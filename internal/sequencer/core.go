// Package sequencer is the deterministic core of the Sluice sequencer: it
// orders every write through a metadata log appended with put-if-absent
// object writes.
//
// Safety rests on one rule: appending entry N is a PUT of meta/log/N with
// If-None-Match: *, so only one writer can ever create N. A sequencer
// writes N only after applying every entry below N; if its write succeeds
// its view of the prefix was complete. On a 412 it reads N and, when N
// belongs to a newer epoch, steps down. Leases (pings) provide liveness
// only.
package sequencer

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/bugs"
	"github.com/ayushhh1010/sluice/internal/metalog"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/proc"
)

// Request is an event: an RPC addressed to this sequencer. The core answers
// with exactly one Reply carrying the same ID (unless it crashes).
type Request struct {
	ID  uint64
	Req *pb.Request
}

// Start is the first event delivered to a fresh core.
type Start struct{}

// Reply is an effect answering a Request.
type Reply struct {
	ID   uint64
	Resp *pb.Response
}

// Notify is an effect: push a watch event to an agent.
type Notify struct {
	Agent string
	Event *pb.WatchEvent
}

// Fatal is an effect: the core found an unrecoverable inconsistency and has
// stopped. Drivers must exit (the simulator reports a violation).
type Fatal struct{ Err error }

// RoleChange is an informational effect for logs and metrics.
type RoleChange struct {
	Role  string
	Epoch uint64
}

type role int

const (
	roleStandby role = iota
	roleRecovering
	roleLeader
	roleFailed
)

func (r role) String() string {
	return [...]string{"standby", "recovering", "leader", "failed"}[r]
}

// Core is the sequencer state machine.
type Core struct {
	cfg  Config
	keys metalog.Keys
	rnd  *proc.Rand

	now    int64
	out    []any
	timers proc.Timers
	nextID uint64
	gen    uint64 // bumped on every role change; stale continuations drop out

	storeCont map[uint64]storeCont
	callCont  map[uint64]callCont

	role            role
	st              *State
	lastHash        [32]byte // SHA-256 of the encoded entry at st.Seq
	leaderHint      string
	epochSeen       uint64
	lastLeader      int64 // standby: last time a peer was seen leading
	recoveringSince int64

	// Leader-only.
	queue         []*queued
	windowArmed   bool
	appending     *inflight
	commitWaiters map[string][]func(*pb.SegmentOutcome)
	lastSnapSeq   uint64
	lastSnapAt    int64
	snapshotting  bool
	gcRunning     bool
	members       map[string]*member
	membersVer    uint64
	topicsVer     uint64
	groups        map[string]*group
	groupGens     map[string]int32
	merge         *mergeJob
	deleting      map[string]bool
	sweeping      bool
	classADay     int64
	classACount   int64
	chaosNext     int64
	chaosTargets  map[string]bool
	barrier       []func(ok bool)
	barrierBusy   bool

	// Metrics readable by drivers.
	Stats Stats
}

// Stats are counters for observability.
type Stats struct {
	Appends, AppendConflicts, StepDowns, Recoveries uint64
	Snapshots, MergedObjects, DeletedObjects        uint64
	GroupRebalances                                 uint64
}

type storeCont struct {
	gen uint64
	fn  func(proc.StoreRes)
}

type callCont struct {
	gen uint64
	fn  func(proc.CallRes)
}

// queued is one unit of work waiting for the next log entry. plan runs when
// the entry is built (against state with every earlier entry applied) and
// adds zero or more ops; done runs after the entry is applied (ok=true) or
// when leadership is lost before that (ok=false).
type queued struct {
	at   int64
	plan func(pl *planner)
	done func(ok bool)
}

type inflight struct {
	start int64
	entry *pb.Entry
	data  []byte
	items []*queued
}

// New creates a core. seed drives every random choice (ULIDs, topic IDs,
// backoff jitter); production passes crypto randomness, the simulator a
// value derived from the run seed.
func New(cfg Config, seed uint64) *Core {
	cfg.setDefaults()
	return &Core{
		cfg:       cfg,
		keys:      metalog.NewKeys(cfg.Cluster),
		rnd:       proc.NewRand(seed),
		storeCont: map[uint64]storeCont{},
		callCont:  map[uint64]callCont{},
	}
}

// Role reports the current role (for drivers and tests).
func (c *Core) Role() string { return c.role.String() }

// Epoch reports the epoch of the applied log prefix.
func (c *Core) Epoch() uint64 {
	if c.st == nil {
		return 0
	}
	return c.st.Epoch
}

// State exposes the applied state for tests and the simulator. Callers must
// not modify it.
func (c *Core) State() *State { return c.st }

// Step advances the core.
func (c *Core) Step(now int64, ev any) []any {
	if now > c.now {
		c.now = now
	}
	if c.role == roleFailed {
		if r, ok := ev.(Request); ok {
			c.reply(r.ID, &pb.Response{Code: pb.Code_CODE_UNAVAILABLE, Message: "sequencer failed"})
		}
		return c.flush()
	}
	switch e := ev.(type) {
	case Start:
		c.becomeStandby(true)
	case proc.StoreRes:
		if k, ok := c.storeCont[e.ID]; ok {
			delete(c.storeCont, e.ID)
			if k.gen == c.gen {
				k.fn(e)
			}
		}
	case proc.CallRes:
		if k, ok := c.callCont[e.ID]; ok {
			delete(c.callCont, e.ID)
			if k.gen == c.gen {
				k.fn(e)
			}
		}
	case Request:
		c.handle(e.ID, e.Req)
	case proc.Tick:
	}
	c.timers.Run(c.now)
	return c.flush()
}

func (c *Core) flush() []any {
	if next := c.timers.Next(); next > 0 {
		c.out = append(c.out, proc.WakeAt{At: next})
	}
	out := c.out
	c.out = nil
	return out
}

func (c *Core) emit(e any) { c.out = append(c.out, e) }

func (c *Core) reply(id uint64, resp *pb.Response) {
	if c.st != nil {
		resp.Epoch = c.st.Epoch
	}
	if resp.LeaderHint == "" {
		resp.LeaderHint = c.leaderHint
	}
	c.emit(Reply{ID: id, Resp: resp})
}

// after schedules fn on the current generation only.
func (c *Core) after(delayMs int64, fn func()) {
	gen := c.gen
	c.timers.At(c.now+delayMs, func(int64) {
		if c.gen == gen {
			fn()
		}
	})
}

func (c *Core) store(r proc.StoreReq, fn func(proc.StoreRes)) {
	c.nextID++
	r.ID = c.nextID
	c.storeCont[r.ID] = storeCont{gen: c.gen, fn: fn}
	if r.Op == proc.OpPut || r.Op == proc.OpPutIfAbsent || r.Op == proc.OpList {
		c.countClassA(1)
	}
	c.emit(r)
}

// storeRetry repeats r until it returns something other than a transient
// error, then calls fn with that result.
func (c *Core) storeRetry(r proc.StoreReq, fn func(proc.StoreRes)) {
	var try func(attempt int)
	try = func(attempt int) {
		c.store(r, func(res proc.StoreRes) {
			if res.Err != nil && !errors.Is(res.Err, objstore.ErrNotFound) && !errors.Is(res.Err, objstore.ErrPrecondition) {
				c.after(c.rnd.Backoff(attempt, c.cfg.StoreRetryBaseMs, c.cfg.StoreRetryMaxMs), func() { try(attempt + 1) })
				return
			}
			fn(res)
		})
	}
	try(0)
}

func (c *Core) call(to string, req *pb.Request, timeoutMs int64, fn func(proc.CallRes)) {
	c.nextID++
	id := c.nextID
	req.From = c.cfg.Node
	req.EpochSeen = c.epochSeen
	c.callCont[id] = callCont{gen: c.gen, fn: fn}
	c.emit(proc.Call{ID: id, To: to, Req: req, TimeoutMs: timeoutMs})
}

func (c *Core) fail(err error) {
	c.role = roleFailed
	c.gen++
	c.timers.Reset()
	c.emit(Fatal{Err: err})
	c.emit(RoleChange{Role: c.role.String()})
}

// ---- Standby and leader election (liveness only) ----

func (c *Core) rank() int {
	nodes := append([]string{c.cfg.Node}, c.cfg.Peers...)
	sort.Strings(nodes)
	for i, n := range nodes {
		if n == c.cfg.Node {
			return i
		}
	}
	return 0
}

func (c *Core) becomeStandby(startup bool) {
	c.abandonLeadership()
	c.role = roleStandby
	c.gen++
	c.timers.Reset()
	c.st = nil
	if startup {
		// Stagger first takeovers by rank so simultaneous starts rarely race.
		// Wait for a couple of ping rounds before a first takeover so a
		// restarted sequencer does not depose a healthy leader, and stagger
		// by rank so simultaneous cold starts rarely race.
		c.lastLeader = c.now - c.cfg.LeaderTimeoutMs + 2*c.cfg.PingIntervalMs + int64(c.rank())*c.cfg.StartupStaggerMs
	} else {
		c.lastLeader = c.now
	}
	c.emit(RoleChange{Role: c.role.String()})
	c.standbyTick()
}

func (c *Core) standbyTick() {
	if c.now-c.lastLeader >= c.cfg.LeaderTimeoutMs {
		c.startRecovery()
		return
	}
	for _, p := range c.cfg.Peers {
		c.call(p, &pb.Request{Body: &pb.Request_Ping{Ping: &pb.PingRequest{}}}, c.cfg.PingIntervalMs, func(res proc.CallRes) {
			ping := res.Resp.GetPing()
			switch {
			case res.Err != nil:
			case ping.GetLeader():
				c.lastLeader = c.now
				c.leaderHint = ping.GetNode()
				c.noteEpoch(res.Resp.Epoch)
			case ping.GetRecoveringMs() > 0 && ping.GetRecoveringMs() < c.cfg.LeaderTimeoutMs:
				// A peer is taking over right now; racing it would only cost
				// a second failover. One stuck recovering stops counting
				// after LeaderTimeout.
				c.lastLeader = c.now
			}
		})
	}
	c.after(c.cfg.PingIntervalMs, c.standbyTick)
}

func (c *Core) noteEpoch(e uint64) {
	if e > c.epochSeen {
		c.epochSeen = e
	}
}

// ---- Recovery ----

func (c *Core) startRecovery() {
	c.role = roleRecovering
	c.recoveringSince = c.now
	c.gen++
	c.timers.Reset()
	c.Stats.Recoveries++
	c.emit(RoleChange{Role: c.role.String()})
	c.findSnapshot()
}

func (c *Core) newState() *State { return NewState(c.cfg.GCGraceMs, c.cfg.RecentCommits) }

func (c *Core) loadSnapshot(seq uint64) {
	if seq == 0 {
		c.st = c.newState()
		c.lastHash = [32]byte{}
		c.probe()
		return
	}
	c.storeRetry(proc.StoreReq{Op: proc.OpGet, Key: c.keys.Snap(seq)}, func(res proc.StoreRes) {
		if errors.Is(res.Err, objstore.ErrNotFound) {
			c.findSnapshot() // deleted between LIST and GET; look again
			return
		}
		snap, err := metalog.DecodeSnapshot(res.Data)
		if err != nil || snap.Seq != seq {
			c.fail(fmt.Errorf("snapshot %d unreadable: %w", seq, err))
			return
		}
		st, err := StateFromSnapshot(snap, c.cfg.GCGraceMs, c.cfg.RecentCommits)
		if err != nil {
			c.fail(err)
			return
		}
		st.PriorSnaps = append(st.PriorSnaps, seq)
		c.st = st
		copy(c.lastHash[:], snap.LastEntrySha256)
		c.lastSnapSeq, c.lastSnapAt = seq, c.now
		c.probe()
	})
}

// findSnapshot starts recovery from the newest snapshot. It uses one LIST
// rather than a head pointer: a zombie can rewrite a pointer to an old
// snapshot whose following log entries were already garbage collected.
func (c *Core) findSnapshot() {
	c.storeRetry(proc.StoreReq{Op: proc.OpList, Prefix: c.keys.SnapPrefix()}, func(res proc.StoreRes) {
		var best uint64
		for _, o := range res.List {
			if s, ok := metalog.SeqFromKey(o.Key); ok && s > best {
				best = s
			}
		}
		if best == 0 {
			c.st = c.newState()
			c.lastHash = [32]byte{}
			c.probe()
			return
		}
		c.loadSnapshot(best)
	})
}

// probe applies log entries forward from the loaded state until a missing
// slot, using GETs (LIST is a billed write-class operation on R2).
func (c *Core) probe() {
	seq := c.st.Seq + 1
	c.storeRetry(proc.StoreReq{Op: proc.OpGet, Key: c.keys.Log(seq)}, func(res proc.StoreRes) {
		if errors.Is(res.Err, objstore.ErrNotFound) {
			c.claim(seq)
			return
		}
		if err := c.applyRaw(seq, res.Data); err != nil {
			c.fail(err)
			return
		}
		c.probe()
	})
}

func (c *Core) applyRaw(seq uint64, data []byte) error {
	e, err := metalog.DecodeEntry(data)
	if err != nil {
		return fmt.Errorf("%w: entry %d undecodable: %w", ErrCorruptLog, seq, err)
	}
	if e.Seq != seq {
		return corrupt("object %d holds entry %d", seq, e.Seq)
	}
	if err := c.st.Apply(e); err != nil {
		return err
	}
	c.lastHash = sha256.Sum256(data)
	c.noteEpoch(c.st.Epoch)
	c.leaderHint = c.st.Leader
	return nil
}

// claim tries to append BecomeLeader at slot seq.
func (c *Core) claim(seq uint64) {
	e := &pb.Entry{
		Seq: seq, Epoch: max(c.st.Epoch, c.epochSeen) + 1, WrittenUnixMs: c.now, Writer: c.cfg.Node,
		Ops: []*pb.Op{{Body: &pb.Op_BecomeLeader{BecomeLeader: &pb.BecomeLeader{Node: c.cfg.Node}}}},
	}
	data := metalog.EncodeEntry(e)
	c.putEntry(seq, data, func(won bool, other []byte) {
		if won {
			c.confirmPredecessor(seq, func(ok bool) {
				if !ok {
					c.becomeStandby(false) // our slot was garbage collected: far behind
					return
				}
				if err := c.applyRaw(seq, data); err != nil {
					c.fail(err)
					return
				}
				c.becomeLeader()
			})
			return
		}
		if err := c.applyRaw(seq, other); err != nil {
			c.fail(err)
			return
		}
		// Someone else just claimed leadership: defer to them rather than
		// immediately deposing a leader that is evidently alive.
		if c.st.Leader != c.cfg.Node && c.st.Seq == seq && isBecomeLeader(other) {
			c.becomeStandby(false)
			return
		}
		c.probe()
	})
}

// confirmPredecessor makes winning slot seq meaningful once old log
// entries can be deleted. Put-if-absent proves the slot was empty, not that
// it was never written: a zombie that resumes after the leader garbage
// collected slot seq would "win" it in a dead zone no recovery reads. GC
// deletes entries in ascending order, so if seq had been deleted, seq-1 was
// deleted before it. Reading seq-1 after our write and finding exactly the
// entry we applied there proves our write is part of the live log.
func (c *Core) confirmPredecessor(seq uint64, done func(ok bool)) {
	if seq == 1 {
		done(true)
		return
	}
	want := c.lastHash
	c.storeRetry(proc.StoreReq{Op: proc.OpGet, Key: c.keys.Log(seq - 1)}, func(res proc.StoreRes) {
		done(res.Err == nil && sha256.Sum256(res.Data) == want)
	})
}

func isBecomeLeader(data []byte) bool {
	e, err := metalog.DecodeEntry(data)
	return err == nil && len(e.Ops) > 0 && e.Ops[0].GetBecomeLeader() != nil
}

// putEntry writes data at slot seq with put-if-absent, retrying transient
// errors with the same bytes. done(true, nil) means our entry is at seq;
// done(false, other) means another writer's entry is there.
func (c *Core) putEntry(seq uint64, data []byte, done func(won bool, other []byte)) {
	key := c.keys.Log(seq)
	var try func(attempt int)
	try = func(attempt int) {
		c.store(proc.StoreReq{Op: proc.OpPutIfAbsent, Key: key, Data: data}, func(res proc.StoreRes) {
			switch {
			case res.Err == nil:
				done(true, nil)
			case errors.Is(res.Err, objstore.ErrPrecondition):
				c.Stats.AppendConflicts++
				// Either an earlier attempt of ours landed (response lost)
				// or someone else won the slot. Read it to find out.
				c.storeRetry(proc.StoreReq{Op: proc.OpGet, Key: key}, func(got proc.StoreRes) {
					if got.Err != nil { // NotFound: cannot happen for a live slot; retry
						c.after(c.rnd.Backoff(attempt, c.cfg.StoreRetryBaseMs, c.cfg.StoreRetryMaxMs), func() { try(attempt + 1) })
						return
					}
					if bytes.Equal(got.Data, data) || bugs.On("ignore-fence") {
						done(true, nil)
						return
					}
					done(false, got.Data)
				})
			default:
				c.after(c.rnd.Backoff(attempt, c.cfg.StoreRetryBaseMs, c.cfg.StoreRetryMaxMs), func() { try(attempt + 1) })
			}
		})
	}
	try(0)
}

// ---- Leader ----

func (c *Core) becomeLeader() {
	c.role = roleLeader
	c.gen++
	c.timers.Reset()
	c.leaderHint = c.cfg.Node
	c.commitWaiters = map[string][]func(*pb.SegmentOutcome){}
	c.members = map[string]*member{}
	c.groups = map[string]*group{}
	c.groupGens = map[string]int32{}
	c.deleting = map[string]bool{}
	c.chaosTargets = map[string]bool{}
	c.queue = nil
	c.appending = nil
	c.barrier, c.barrierBusy = nil, false
	c.windowArmed = false
	c.merge = nil
	c.sweeping = false
	c.snapshotting = false
	c.gcRunning = false
	c.topicsVer++
	c.membersVer++
	c.lastSnapAt = c.now
	c.emit(RoleChange{Role: c.role.String(), Epoch: c.st.Epoch})
	c.emit(proc.Gauge{Name: "epoch", Value: float64(c.st.Epoch)})
	c.emit(proc.Gauge{Name: "metalog_seq", Value: float64(c.st.Seq)})

	c.every(c.cfg.NextSlotCheckMs, c.checkNextSlot)
	c.every(c.cfg.GroupHousekeepingMs, c.groupHousekeeping)
	c.every(c.cfg.MemberTTLms/5, c.expireMembers)
	c.every(c.cfg.GCIntervalMs, c.runGC)
	c.every(c.cfg.RetentionCheckMs, c.runRetention)
	c.every(c.cfg.MergeCheckMs, c.runMerge)
	c.every(c.cfg.OrphanSweepMs, c.runOrphanSweep)
	c.every(c.cfg.SnapshotIntervalMs/10, c.maybeSnapshot)
}

func (c *Core) every(ms int64, fn func()) {
	var loop func()
	loop = func() {
		fn()
		c.after(ms, loop)
	}
	c.after(ms, loop)
}

// abandonLeadership fails everything waiting on this leader.
func (c *Core) abandonLeadership() {
	if c.role != roleLeader {
		return
	}
	items := c.queue
	if c.appending != nil {
		items = append(append([]*queued(nil), c.appending.items...), items...)
	}
	c.queue, c.appending = nil, nil
	for _, q := range items {
		q.done(false)
	}
	for _, g := range c.groups {
		g.failPending(c)
	}
	c.groups = nil
	ws := c.barrier
	c.barrier = nil
	for _, w := range ws {
		w(false)
	}
}

func (c *Core) stepDown(why string) {
	if c.role != roleLeader {
		return
	}
	c.Stats.StepDowns++
	c.becomeStandby(false)
	_ = why
}

func (c *Core) enqueue(q *queued) {
	q.at = c.now
	c.queue = append(c.queue, q)
	c.maybeAppend()
}

func (c *Core) maybeAppend() {
	if c.role != roleLeader || c.appending != nil || len(c.queue) == 0 {
		return
	}
	due := c.queue[0].at + c.cfg.CommitWindowMs
	if c.now < due && len(c.queue) < c.cfg.CommitMaxRequests {
		if !c.windowArmed {
			c.windowArmed = true
			c.after(due-c.now, func() {
				c.windowArmed = false
				c.maybeAppend()
			})
		}
		return
	}
	n := min(len(c.queue), c.cfg.CommitMaxRequests)
	items := c.queue[:n:n]
	c.queue = append([]*queued(nil), c.queue[n:]...)

	pl := newPlanner(c)
	for _, q := range items {
		q.plan(pl)
	}
	ops := pl.ops()
	if len(ops) == 0 {
		for _, q := range items {
			q.done(true)
		}
		c.maybeAppend()
		return
	}
	e := &pb.Entry{Seq: c.st.Seq + 1, Epoch: c.st.Epoch, WrittenUnixMs: c.now, Writer: c.cfg.Node, Ops: ops}
	data := metalog.EncodeEntry(e)
	c.appending = &inflight{start: c.now, entry: e, data: data, items: items}
	c.putEntry(e.Seq, data, func(won bool, other []byte) {
		if !won {
			// Another writer owns this slot; a correct log never holds a
			// competing entry from our own epoch, so it is a newer leader.
			c.stepDown("lost slot")
			return
		}
		c.confirmPredecessor(e.Seq, func(ok bool) {
			if !ok {
				c.stepDown("slot was garbage collected")
				return
			}
			c.appended()
		})
	})
}

func (c *Core) appended() {
	in := c.appending
	c.appending = nil
	if err := c.st.Apply(in.entry); err != nil {
		c.fail(err)
		return
	}
	c.lastHash = sha256.Sum256(in.data)
	c.Stats.Appends++
	c.emit(proc.Observe{Name: "append_ms", Value: float64(c.now - in.start)})
	c.emit(proc.Gauge{Name: "metalog_seq", Value: float64(c.st.Seq)})
	c.notifyWatermarks(in.entry)
	for _, q := range in.items {
		q.done(true)
	}
	if c.role != roleLeader {
		return
	}
	if c.st.Seq-c.lastSnapSeq >= c.cfg.SnapshotEvery {
		c.maybeSnapshot()
	}
	c.maybeAppend()
	c.runBarrier()
}

// readBarrier runs fn once this leader has proven, after fn was requested,
// that nobody has appended past its log: a HEAD of the next slot returned
// 404. Reads that must be linearizable (committed offsets) wait for it, so
// a deposed leader cannot answer them from stale memory.
func (c *Core) readBarrier(fn func(ok bool)) {
	c.barrier = append(c.barrier, fn)
	c.runBarrier()
}

func (c *Core) runBarrier() {
	if c.role != roleLeader || c.barrierBusy || c.appending != nil || len(c.barrier) == 0 {
		return
	}
	ws := c.barrier
	c.barrier = nil
	c.barrierBusy = true
	seq := c.st.Seq + 1
	c.store(proc.StoreReq{Op: proc.OpHead, Key: c.keys.Log(seq)}, func(res proc.StoreRes) {
		c.barrierBusy = false
		switch {
		case errors.Is(res.Err, objstore.ErrNotFound):
			for _, w := range ws {
				w(true)
			}
		case res.Err == nil && c.st.Seq+1 == seq && c.appending == nil:
			c.barrier = append(ws, c.barrier...)
			c.stepDown("next slot taken (read barrier)")
			return
		default: // transient error, or our own append raced the probe
			c.barrier = append(ws, c.barrier...)
		}
		c.runBarrier()
	})
}

// checkNextSlot bounds how long a deposed leader keeps serving reads: if
// our next slot exists, someone else is writing the log.
func (c *Core) checkNextSlot() {
	if c.appending != nil {
		return
	}
	seq := c.st.Seq + 1
	c.store(proc.StoreReq{Op: proc.OpHead, Key: c.keys.Log(seq)}, func(res proc.StoreRes) {
		if res.Err == nil && c.appending == nil && c.st.Seq+1 == seq {
			c.stepDown("next slot taken")
		}
	})
}

func (c *Core) notifyWatermarks(e *pb.Entry) {
	seen := map[TP]bool{}
	var wms []*pb.Watermark
	add := func(id []byte, p uint32) {
		tid, ok := toTopicID(id)
		if !ok || seen[TP{tid, p}] {
			return
		}
		seen[TP{tid, p}] = true
		if part := c.st.Partition(tid, p); part != nil {
			wms = append(wms, &pb.Watermark{TopicId: id, Partition: p, HighWatermark: part.Next, LogStart: part.LogStart})
		}
	}
	for _, op := range e.Ops {
		switch b := op.Body.(type) {
		case *pb.Op_Commit:
			for _, r := range b.Commit.Results {
				if r.ErrorCode == 0 && !r.Duplicate {
					add(r.TopicId, r.Partition)
				}
			}
		case *pb.Op_Truncate:
			for _, s := range b.Truncate.Starts {
				add(s.TopicId, s.Partition)
			}
		}
	}
	if len(wms) == 0 {
		return
	}
	for _, id := range c.sortedMembers() {
		c.emit(Notify{Agent: id, Event: &pb.WatchEvent{Epoch: c.st.Epoch, Watermarks: wms}})
	}
}
