// Package agent is the deterministic core of a Sluice agent: the stateless
// Kafka-facing process. It batches produces from all partitions into one
// segment object per flush, uploads it with put-if-absent, asks the
// sequencer to assign offsets, and serves fetches from caches or ranged
// GETs. Everything it holds is soft state; killing it loses nothing that
// was acknowledged.
package agent

import (
	"errors"
	"sort"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/bugs"
	"github.com/ayushhh1010/sluice/internal/cache"
	"github.com/ayushhh1010/sluice/internal/kbatch"
	"github.com/ayushhh1010/sluice/internal/metalog"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/placement"
	"github.com/ayushhh1010/sluice/internal/proc"
	"github.com/ayushhh1010/sluice/internal/segment"
)

// Config holds agent settings. Zero values take defaults.
type Config struct {
	ID         string
	NodeID     int32
	Cluster    string
	Sequencers []string
	Listeners  []*pb.Listener
	HTTPAddr   string

	FlushWindowMs    int64
	FlushMaxBytes    int64
	BufferMaxBytes   int64
	RPCTimeoutMs     int64
	CommitDeadlineMs int64 // give up on a segment this long after creating it
	HeartbeatMs      int64
	IndexTTLms       int64
	HWStaleMs        int64
	TailCacheBytes   int64
	DataCacheBytes   int64
	StoreRetryBaseMs int64
	StoreRetryMaxMs  int64
	LookupMaxBytes   int32
}

func (c *Config) setDefaults() {
	def := func(p *int64, v int64) {
		if *p == 0 {
			*p = v
		}
	}
	def(&c.FlushWindowMs, 250)
	def(&c.FlushMaxBytes, 4<<20)
	def(&c.BufferMaxBytes, 64<<20)
	def(&c.RPCTimeoutMs, 5_000)
	def(&c.CommitDeadlineMs, 60_000)
	def(&c.HeartbeatMs, 1_000)
	def(&c.IndexTTLms, 30_000)
	def(&c.HWStaleMs, 5_000)
	def(&c.TailCacheBytes, 128<<20)
	def(&c.DataCacheBytes, 512<<20)
	def(&c.StoreRetryBaseMs, 50)
	def(&c.StoreRetryMaxMs, 2_000)
	if c.LookupMaxBytes == 0 {
		c.LookupMaxBytes = 8 << 20
	}
}

// Kafka error codes used by the agent.
const (
	ErrNone                    int16 = 0
	ErrOffsetOutOfRange        int16 = 1
	ErrUnknownTopicOrPartition int16 = 3
	ErrNotLeader               int16 = 6
	ErrRequestTimedOut         int16 = 7
	ErrPolicyViolation         int16 = 44
	ErrKafkaStorage            int16 = 56
)

// ---- Events ----

type Start struct{}

// ProducePart is one partition of a Produce request; batches are already
// validated (magic, CRC, size) by the protocol layer.
type ProducePart struct {
	Topic     string
	Partition int32
	Batches   [][]byte
	Headers   []kbatch.Header
}

type ProduceReq struct {
	ID        uint64
	Parts     []ProducePart
	TimeoutMs int64
	Acks      int16
	Public    bool // public listener: cost guard applies
	AnyOwner  bool // skip the ownership check (playground HTTP API)
}

type FetchPart struct {
	Topic     string
	Partition int32
	Offset    int64
	MaxBytes  int32
}

type FetchReq struct {
	ID        uint64
	Parts     []FetchPart
	MaxWaitMs int64
	MinBytes  int32
	MaxBytes  int32
}

// ForwardReq relays a request to the sequencer.
type ForwardReq struct {
	ID        uint64
	Req       *pb.Request
	TimeoutMs int64
}

// Watch delivers a sequencer watch event.
type Watch struct{ Event *pb.WatchEvent }

// ---- Effects ----

type PartResult struct {
	Err        int16
	BaseOffset int64
	LogStart   int64
}

type ProduceResp struct {
	ID    uint64
	Parts []PartResult
}

type FetchPartResult struct {
	Err      int16
	HW       int64
	LogStart int64
	Records  []byte
}

type FetchResp struct {
	ID    uint64
	Parts []FetchPartResult
}

type ForwardResp struct {
	ID   uint64
	Resp *pb.Response
	Err  error
}

// Die tells the driver to exit the process (chaos testing).
type Die struct{}

// ---- Core ----

type tp struct {
	topic [16]byte
	part  uint32
}

type topicInfo struct {
	id         [16]byte
	partitions uint32
}

type Core struct {
	cfg  Config
	keys metalog.Keys
	rnd  *proc.Rand

	now    int64
	out    []any
	timers proc.Timers
	nextID uint64

	storeCont map[uint64]func(proc.StoreRes)
	callCont  map[uint64]func(proc.CallRes)

	seqIdx    int
	epochSeen uint64

	members   []*pb.AgentInfo
	nodes     []int32
	topics    map[string]topicInfo
	topicsVer uint64
	readOnly  bool
	classA    int64

	buf           []*pending
	bufBytes      int64
	bufOldest     int64
	flushArmed    bool
	inflight      int64 // bytes in segments not yet committed
	commitQ       []*flushSeg
	committing    bool
	commitAttempt int
	blocked       []*blockedReq
	inc           string

	topicWaiters     []func()
	topicsRefreshing bool
	topicsRefreshed  int64

	tail    *cache.Tail[tp]
	data    *cache.LRU
	index   map[tp]*indexEntry
	hw      map[tp]*watermark
	lookups map[tp][]*fetchWait
	gets    map[cache.RangeKey][]*fetchWait
	waiting map[tp][]*fetchWait

	Stats Stats
}

// Stats are counters for observability.
type Stats struct {
	Segments, SegmentBytes, CommitRetries, PutRetries uint64
	Produced, ProduceErrors, Fetches, Lookups, Gets   uint64
	AbandonedSegments                                 uint64
}

type pending struct {
	tp    tp
	batch []byte
	hdr   kbatch.Header
	w     *produceWait
	part  int
	idx   int // batch index within the partition
	at    int64
}

type produceWait struct {
	start   int64
	req     *ProduceReq
	results []PartResult
	left    []int // batches outstanding per part
	open    int   // parts outstanding
	done    bool
}

type watermark struct {
	hw, logStart int64
	at           int64
}

type indexEntry struct {
	refs []*pb.BatchRef
	at   int64
}

func New(cfg Config, seed uint64) *Core {
	cfg.setDefaults()
	return &Core{
		cfg:       cfg,
		keys:      metalog.NewKeys(cfg.Cluster),
		rnd:       proc.NewRand(seed),
		storeCont: map[uint64]func(proc.StoreRes){},
		callCont:  map[uint64]func(proc.CallRes){},
		topics:    map[string]topicInfo{},
		tail:      cache.NewTail[tp](cfg.TailCacheBytes),
		data:      cache.NewLRU(cfg.DataCacheBytes),
		index:     map[tp]*indexEntry{},
		hw:        map[tp]*watermark{},
		lookups:   map[tp][]*fetchWait{},
		gets:      map[cache.RangeKey][]*fetchWait{},
		waiting:   map[tp][]*fetchWait{},
	}
}

func (c *Core) Step(now int64, ev any) []any {
	if now > c.now {
		c.now = now
	}
	switch e := ev.(type) {
	case Start:
		c.heartbeat()
	case ProduceReq:
		names := make([]string, len(e.Parts))
		for i, p := range e.Parts {
			names[i] = p.Topic
		}
		c.withTopics(names, func() { c.onProduce(&e) })
	case FetchReq:
		names := make([]string, len(e.Parts))
		for i, p := range e.Parts {
			names[i] = p.Topic
		}
		c.withTopics(names, func() { c.onFetch(&e) })
	case ForwardReq:
		c.forward(e)
	case Watch:
		c.onWatch(e.Event)
	case proc.StoreRes:
		if fn, ok := c.storeCont[e.ID]; ok {
			delete(c.storeCont, e.ID)
			fn(e)
		}
	case proc.CallRes:
		if fn, ok := c.callCont[e.ID]; ok {
			delete(c.callCont, e.ID)
			fn(e)
		}
	case proc.Tick:
	}
	c.timers.Run(c.now)
	if next := c.timers.Next(); next > 0 {
		c.out = append(c.out, proc.WakeAt{At: next})
	}
	out := c.out
	c.out = nil
	return out
}

func (c *Core) emit(e any) { c.out = append(c.out, e) }

func (c *Core) after(ms int64, fn func()) { c.timers.At(c.now+ms, func(int64) { fn() }) }

func (c *Core) store(r proc.StoreReq, fn func(proc.StoreRes)) {
	c.nextID++
	r.ID = c.nextID
	c.storeCont[r.ID] = fn
	if r.Op == proc.OpPut || r.Op == proc.OpPutIfAbsent || r.Op == proc.OpList {
		c.classA++
	}
	c.emit(r)
}

// ---- Sequencer RPC ----

// errRetry marks a response that should be retried against another
// sequencer (not the leader, or a reply from a deposed epoch).
var errRetry = errors.New("agent: retry at another sequencer")

// errUnavailable is a transient sequencer-side failure.
var errUnavailable = errors.New("agent: sequencer unavailable")

func (c *Core) call(req *pb.Request, timeoutMs int64, fn func(*pb.Response, error)) {
	c.nextID++
	id := c.nextID
	req.From = c.cfg.ID
	req.EpochSeen = c.epochSeen
	to := c.cfg.Sequencers[c.seqIdx%len(c.cfg.Sequencers)]
	c.callCont[id] = func(res proc.CallRes) {
		if res.Err != nil {
			c.rotate("")
			fn(nil, res.Err)
			return
		}
		r := res.Resp
		if r.Code == pb.Code_CODE_NOT_LEADER || r.Epoch < c.epochSeen {
			c.rotate(r.LeaderHint)
			fn(nil, errRetry)
			return
		}
		if r.Code == pb.Code_CODE_UNAVAILABLE {
			fn(nil, errUnavailable)
			return
		}
		if r.Epoch > c.epochSeen {
			c.epochSeen = r.Epoch
		}
		fn(r, nil)
	}
	c.emit(proc.Call{ID: id, To: to, Req: req, TimeoutMs: timeoutMs})
}

// rotate moves to the hinted sequencer, or the next one.
func (c *Core) rotate(hint string) {
	for i, s := range c.cfg.Sequencers {
		if s == hint && hint != "" {
			c.seqIdx = i
			return
		}
	}
	c.seqIdx++
}

func (c *Core) forward(f ForwardReq) {
	deadline := c.now + f.TimeoutMs
	var try func(attempt int)
	try = func(attempt int) {
		c.call(f.Req, max(deadline-c.now, 1), func(r *pb.Response, err error) {
			if err == nil {
				c.emit(ForwardResp{ID: f.ID, Resp: r})
				return
			}
			if c.now >= deadline {
				c.emit(ForwardResp{ID: f.ID, Err: err})
				return
			}
			c.after(c.rnd.Backoff(attempt, 20, 500), func() { try(attempt + 1) })
		})
	}
	try(0)
}

func (c *Core) heartbeat() {
	c.sendHeartbeat(func(err error) {
		delay := c.cfg.HeartbeatMs
		if errors.Is(err, errRetry) {
			delay = 50 // found the leader; report in right away
		}
		c.after(delay, c.heartbeat)
	})
}

// sendHeartbeat reports in and refreshes membership and topics.
func (c *Core) sendHeartbeat(done func(error)) {
	ops := c.classA
	c.classA = 0
	req := &pb.Request{Body: &pb.Request_Heartbeat{Heartbeat: &pb.HeartbeatRequest{
		Agent: &pb.AgentInfo{
			Id: c.cfg.ID, NodeId: c.cfg.NodeID, Listeners: c.cfg.Listeners, HttpAddr: c.cfg.HTTPAddr,
			Incarnation: c.incarnation(),
		},
		TopicsVersion: c.topicsVer,
		ClassAOps:     ops,
	}}}
	c.call(req, c.cfg.HeartbeatMs, func(r *pb.Response, err error) {
		hb := r.GetHeartbeat()
		if err == nil && hb == nil {
			err = errUnavailable
		}
		if err != nil {
			c.classA += ops
			done(err)
			return
		}
		c.members = hb.Members
		c.nodes = c.nodes[:0]
		for _, m := range hb.Members {
			c.nodes = append(c.nodes, m.NodeId)
		}
		sort.Slice(c.nodes, func(i, j int) bool { return c.nodes[i] < c.nodes[j] })
		if hb.TopicsIncluded {
			c.topics = map[string]topicInfo{}
			for _, t := range hb.Topics {
				var id [16]byte
				copy(id[:], t.Id)
				c.topics[t.Name] = topicInfo{id: id, partitions: t.Partitions}
			}
			c.topicsVer = hb.TopicsVersion
		}
		c.readOnly = hb.ReadOnly
		if hb.Die {
			c.emit(Die{})
		}
		done(nil)
	})
}

// withTopics runs fn once the agent's topic view includes every named
// topic, refreshing it at most once per request: a topic created a moment
// ago must not look unknown just because the next heartbeat is not due.
func (c *Core) withTopics(names []string, fn func()) {
	missing := false
	for _, n := range names {
		if _, ok := c.topics[n]; !ok {
			missing = true
		}
	}
	if !missing || c.now-c.topicsRefreshed < 100 {
		fn()
		return
	}
	c.topicWaiters = append(c.topicWaiters, fn)
	if c.topicsRefreshing {
		return
	}
	c.topicsRefreshing = true
	var try func(attempt int)
	try = func(attempt int) {
		c.sendHeartbeat(func(err error) {
			if err != nil && attempt < 3 {
				c.after(c.rnd.Backoff(attempt, 10, 100), func() { try(attempt + 1) })
				return
			}
			c.topicsRefreshing = false
			c.topicsRefreshed = c.now
			ws := c.topicWaiters
			c.topicWaiters = nil
			for _, w := range ws {
				w()
			}
		})
	}
	try(0)
}

func (c *Core) incarnation() string {
	if c.inc == "" {
		c.inc = metalog.ULID(c.now, c.rnd.Uint64(), c.rnd.Uint64())
	}
	return c.inc
}

// Members returns the agent's current view of live agents.
func (c *Core) Members() []*pb.AgentInfo { return c.members }

func (c *Core) owner(t tp) int32 { return placement.Owner(c.nodes, t.topic, t.part) }

// ---- Produce ----

func (c *Core) onProduce(req *ProduceReq) {
	w := &produceWait{start: c.now, req: req, results: make([]PartResult, len(req.Parts)), left: make([]int, len(req.Parts))}
	var adds []*pending
	var size int64
	for i, p := range req.Parts {
		ti, ok := c.topics[p.Topic]
		switch {
		case !ok || p.Partition < 0 || uint32(p.Partition) >= ti.partitions:
			w.results[i].Err = ErrUnknownTopicOrPartition
			continue
		case req.Public && c.readOnly:
			w.results[i].Err = ErrPolicyViolation
			continue
		}
		t := tp{ti.id, uint32(p.Partition)}
		if !req.AnyOwner && c.owner(t) != c.cfg.NodeID {
			w.results[i].Err = ErrNotLeader
			continue
		}
		w.left[i] = len(p.Batches)
		if w.left[i] > 0 {
			w.open++
		}
		for j, b := range p.Batches {
			adds = append(adds, &pending{tp: t, batch: b, hdr: p.Headers[j], w: w, part: i, idx: j})
			size += int64(len(b))
		}
	}
	if w.open == 0 {
		c.finishProduce(w)
		return
	}
	if req.Acks == 0 {
		c.finishProduce(w) // nothing is sent back for acks=0; records still flush
	}
	timeout := req.TimeoutMs
	if timeout <= 0 {
		timeout = 30_000
	}
	c.after(timeout, func() {
		if w.done {
			return
		}
		for i, b := range c.blocked {
			if b.w == w {
				c.blocked = append(c.blocked[:i:i], c.blocked[i+1:]...)
				break
			}
		}
		for k := range w.results {
			if w.left[k] > 0 && w.results[k].Err == 0 {
				w.results[k].Err = ErrRequestTimedOut
			}
		}
		c.finishProduce(w)
	})
	if len(c.blocked) > 0 || (c.bufBytes+c.inflight+size > c.cfg.BufferMaxBytes && c.bufBytes+c.inflight > 0) {
		// The object store is slower than producers: hold the request
		// until buffer space frees (or it times out).
		c.blocked = append(c.blocked, &blockedReq{w: w, adds: adds, size: size})
		return
	}
	c.buffer(adds)
}

type blockedReq struct {
	w    *produceWait
	adds []*pending
	size int64
}

func (c *Core) buffer(adds []*pending) {
	if len(c.buf) == 0 {
		c.bufOldest = c.now
	}
	for _, a := range adds {
		a.at = c.now
		c.buf = append(c.buf, a)
		c.bufBytes += int64(len(a.batch))
	}
	c.maybeFlush()
}

func (c *Core) unblock() {
	for len(c.blocked) > 0 {
		b := c.blocked[0]
		if c.bufBytes+c.inflight+b.size > c.cfg.BufferMaxBytes && c.bufBytes+c.inflight > 0 {
			return
		}
		c.blocked = c.blocked[1:]
		if !b.w.done || b.w.req.Acks == 0 {
			c.buffer(b.adds)
		}
	}
}

func (c *Core) finishProduce(w *produceWait) {
	if w.done {
		return
	}
	w.done = true
	if w.req.Acks != 0 {
		c.emit(ProduceResp{ID: w.req.ID, Parts: w.results})
		c.emit(proc.Observe{Name: "produce_total_ms", Value: float64(c.now - w.start)})
	}
	for _, r := range w.results {
		if r.Err != 0 {
			c.Stats.ProduceErrors++
		}
	}
}

func (c *Core) maybeFlush() {
	if len(c.buf) == 0 {
		return
	}
	due := c.bufOldest + c.cfg.FlushWindowMs
	if c.bufBytes < c.cfg.FlushMaxBytes && c.now < due {
		if !c.flushArmed {
			c.flushArmed = true
			c.after(due-c.now, func() {
				c.flushArmed = false
				c.maybeFlush()
			})
		}
		return
	}
	c.flush()
}

type flushSeg struct {
	key     string
	created int64
	items   []*pending // in footer order
	entries []segment.Entry
	obj     []byte
	bytes   int64
	putAt   int64
	putDone bool // object durable; ready to commit
	done    bool // committed or abandoned
}

func (c *Core) flush() {
	b := segment.NewBuilder(c.now, c.cfg.ID)
	for _, p := range c.buf {
		b.Add(segment.Entry{
			TopicID: p.tp.topic, Partition: p.tp.part, Records: uint32(p.hdr.Records()), MaxTS: p.hdr.MaxTimestamp,
			ProducerID: p.hdr.ProducerID, ProducerEpoch: p.hdr.ProducerEpoch, BaseSeq: p.hdr.BaseSequence,
		}, p.batch)
	}
	obj, entries, order := b.Finish()
	seg := &flushSeg{created: c.now, entries: entries, obj: obj, bytes: c.bufBytes}
	for _, i := range order {
		seg.items = append(seg.items, c.buf[i])
	}
	c.inflight += c.bufBytes
	c.buf, c.bufBytes = nil, 0
	c.commitQ = append(c.commitQ, seg)
	c.Stats.Segments++
	c.Stats.SegmentBytes += uint64(len(obj))
	c.emit(proc.Observe{Name: "flush_bytes", Value: float64(len(obj))})
	for _, p := range seg.items {
		c.emit(proc.Observe{Name: "produce_buffer_ms", Value: float64(c.now - p.at)})
	}
	c.putSegment(seg, 0, true)
}

func (c *Core) newKey() string {
	return c.keys.Data(c.now, c.cfg.ID, metalog.ULID(c.now, c.rnd.Uint64(), c.rnd.Uint64()))
}

// putSegment uploads with put-if-absent. The key is fixed across retries,
// so a 412 on a retry means an earlier attempt landed.
func (c *Core) putSegment(seg *flushSeg, attempt int, first bool) {
	if first {
		seg.key = c.newKey()
	}
	c.store(proc.StoreReq{Op: proc.OpPutIfAbsent, Key: seg.key, Data: seg.obj}, func(res proc.StoreRes) {
		switch {
		case res.Err == nil:
			if bugs.On("ack-before-commit") {
				for _, p := range seg.items {
					c.resolve(p, 0, c.logStart(p.tp)+int64(c.Stats.Segments))
				}
			}
			seg.putDone = true
			c.pumpCommits()
		case errors.Is(res.Err, objstore.ErrPrecondition):
			if first {
				c.putSegment(seg, attempt+1, true) // key collision: new key
				return
			}
			seg.putDone = true
			c.pumpCommits()
		default:
			c.Stats.PutRetries++
			if c.now-seg.created > c.cfg.CommitDeadlineMs {
				c.abandon(seg)
				return
			}
			c.after(c.rnd.Backoff(attempt, c.cfg.StoreRetryBaseMs, c.cfg.StoreRetryMaxMs), func() {
				c.putSegment(seg, attempt+1, false)
			})
		}
	})
}

// pumpCommits issues CommitBatches strictly in flush order, one at a time.
// Uploads run in parallel, but a pipelining producer's later batch for a
// partition must never be ordered before its earlier one, which concurrent
// commits could do.
func (c *Core) pumpCommits() {
	for len(c.commitQ) > 0 && c.commitQ[0].done {
		c.commitQ = c.commitQ[1:]
	}
	if c.committing {
		return
	}
	// Every uploaded segment at the head of the queue goes in one request,
	// in flush order; nothing after a segment still uploading.
	var batch []*flushSeg
	for _, s := range c.commitQ {
		if s.done {
			continue
		}
		if !s.putDone || len(batch) == maxSegmentsPerCommit {
			break
		}
		batch = append(batch, s)
	}
	if len(batch) == 0 {
		return
	}
	c.committing = true
	c.commit(batch)
}

// maxSegmentsPerCommit bounds one CommitRequest.
const maxSegmentsPerCommit = 64

func (c *Core) commit(batch []*flushSeg) {
	req := &pb.CommitRequest{}
	for _, seg := range batch {
		if seg.putAt == 0 {
			seg.putAt = c.now
			c.emit(proc.Observe{Name: "produce_put_ms", Value: float64(c.now - seg.created)})
		}
		sc := &pb.SegmentCommit{ObjectKey: seg.key, ObjectCreatedMs: seg.created}
		for _, e := range seg.entries {
			sc.Entries = append(sc.Entries, &pb.FooterEntry{
				TopicId: append([]byte(nil), e.TopicID[:]...), Partition: e.Partition, OffsetInObj: e.Offset, Length: e.Length,
				Records: e.Records, MaxTs: e.MaxTS, ProducerId: e.ProducerID, ProducerEpoch: int32(e.ProducerEpoch), BaseSeq: e.BaseSeq,
			})
		}
		req.Segments = append(req.Segments, sc)
	}
	c.call(&pb.Request{Body: &pb.Request_Commit{Commit: req}}, c.cfg.RPCTimeoutMs, func(r *pb.Response, err error) {
		outcomes := r.GetCommit().GetSegments()
		if err == nil && r.Code == pb.Code_CODE_OK && len(outcomes) == len(batch) {
			retry := false
			for i, o := range outcomes {
				seg := batch[i]
				switch {
				case o.Code == pb.Code_CODE_OK && len(o.Results) == len(seg.items):
					c.committed(seg, o.Results)
				case o.Code == pb.Code_CODE_UNAVAILABLE:
					retry = true
				default:
					c.abandon(seg)
				}
			}
			c.committing = false
			if retry {
				c.retryCommits(errUnavailable)
				return
			}
			c.commitAttempt = 0
			c.pumpCommits()
			return
		}
		c.committing = false
		if err == nil && r.Code == pb.Code_CODE_INVALID {
			for _, seg := range batch {
				c.abandon(seg)
			}
			return
		}
		c.retryCommits(err)
	})
}

// retryCommits backs off, gives up on segments past their deadline, then
// commits whatever is ready again (the same keys: the sequencer dedups).
func (c *Core) retryCommits(err error) {
	c.Stats.CommitRetries++
	c.commitAttempt++
	delay := c.rnd.Backoff(c.commitAttempt, 20, 1000)
	if errors.Is(err, errRetry) {
		delay = c.rnd.Backoff(c.commitAttempt, 5, 200) // found the leader
	}
	c.committing = true // hold the queue until the retry
	c.after(delay, func() {
		for _, seg := range c.commitQ { // still holding the queue
			if !seg.done && c.now-seg.created > c.cfg.CommitDeadlineMs {
				c.abandon(seg)
			}
		}
		c.committing = false
		c.pumpCommits()
	})
}

func (c *Core) committed(seg *flushSeg, results []*pb.BatchResult) {
	if seg.done {
		return
	}
	seg.done = true
	defer c.pumpCommits()
	c.inflight -= seg.bytes
	c.emit(proc.Observe{Name: "produce_commit_ms", Value: float64(c.now - seg.putAt)})
	touched := map[tp]bool{}
	for i, r := range results {
		p := seg.items[i]
		if r.ErrorCode == 0 && !r.Duplicate {
			c.tail.Add(p.tp, cache.Batch{Base: r.BaseOffset, Records: int64(r.Records), Data: seg.obj[seg.entries[i].Offset : seg.entries[i].Offset+seg.entries[i].Length]})
			end := r.BaseOffset + int64(r.Records)
			if w := c.hw[p.tp]; w == nil || w.hw < end {
				c.hw[p.tp] = &watermark{hw: end, logStart: c.logStart(p.tp), at: c.now}
			}
			touched[p.tp] = true
		}
		c.resolve(p, int16(r.ErrorCode), r.BaseOffset)
	}
	c.wakeAll(touched)
	c.unblock()
}

func (c *Core) logStart(t tp) int64 {
	if w := c.hw[t]; w != nil {
		return w.logStart
	}
	return 0
}

func (c *Core) abandon(seg *flushSeg) {
	if seg.done {
		return
	}
	seg.done = true
	defer c.pumpCommits()
	c.Stats.AbandonedSegments++
	c.inflight -= seg.bytes
	for _, p := range seg.items {
		c.resolve(p, ErrKafkaStorage, -1)
	}
	c.unblock()
}

func (c *Core) resolve(p *pending, code int16, base int64) {
	w := p.w
	if w.done {
		return
	}
	r := &w.results[p.part]
	if code != 0 && r.Err == 0 {
		r.Err = code
	}
	if p.idx == 0 {
		r.BaseOffset = base
		r.LogStart = c.logStart(p.tp)
	}
	w.left[p.part]--
	if w.left[p.part] == 0 {
		w.open--
		c.Stats.Produced++
	}
	if w.open == 0 {
		c.finishProduce(w)
	}
}
