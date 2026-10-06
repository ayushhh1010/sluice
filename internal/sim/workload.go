package sim

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/checker"
	"github.com/ayushhh1010/sluice/internal/kbatch"
)

// The workload: simulated clients that speak to agent cores directly (the
// Kafka codec is covered by the compatibility tests). Each client is a
// small state machine driven by callbacks.

type topicSpec struct {
	name  string
	parts int32
}

type client struct {
	w    *World
	id   int
	name string
	busy bool

	// producer
	idem    bool
	pid     int64
	seqs    map[checker.Partition]int32
	leaders map[checker.Partition]int32
	n       int

	// fetcher
	offsets map[checker.Partition]int64

	// committer
	counter int64

	// group member
	member     string
	generation int32
}

func (w *World) newClient(prefix string) *client {
	c := &client{w: w, id: len(w.clients), seqs: map[checker.Partition]int32{}, leaders: map[checker.Partition]int32{}, offsets: map[checker.Partition]int64{}}
	c.name = fmt.Sprintf("%s-%d", prefix, c.id)
	w.clients = append(w.clients, c)
	return c
}

func (w *World) partitions() []checker.Partition {
	var ps []checker.Partition
	for _, t := range w.topics {
		for p := int32(0); p < t.parts; p++ {
			ps = append(ps, checker.Partition{Topic: t.name, Partition: p})
		}
	}
	return ps
}

func (w *World) randomAgent() *node { return w.agents[w.rnd.Intn(len(w.agents))] }

func (w *World) agentByID(id int32) *node {
	if id >= 1 && int(id) <= len(w.agents) {
		return w.agents[id-1]
	}
	return w.randomAgent()
}

// kafka sends a raw Kafka request through an agent to the sequencer.
func (c *client) kafka(to *node, req kmsg.Request, timeoutMs int64, fn func(kmsg.Response)) {
	body := req.AppendTo(nil)
	c.w.send(c, to, func(id uint64) any {
		return agent.ForwardReq{ID: id, TimeoutMs: timeoutMs, Req: &pb.Request{Body: &pb.Request_Kafka{Kafka: &pb.KafkaRequest{
			ApiKey: int32(req.Key()), ApiVersion: int32(req.GetVersion()), Body: body,
			Principal: "sim", Listener: "sim", ClientId: c.name,
		}}}}
	}, timeoutMs+1_000, func(r any) {
		fr, ok := r.(agent.ForwardResp)
		if !ok || fr.Err != nil || fr.Resp.Code != pb.Code_CODE_OK {
			fn(nil)
			return
		}
		kr := req.ResponseKind()
		if err := kr.ReadFrom(fr.Resp.GetKafka().GetBody()); err != nil {
			c.w.violation("PROTOCOL", "undecodable %s response: %v", kmsg.NameForKey(req.Key()), err)
			fn(nil)
			return
		}
		fn(kr)
	})
}

func (w *World) setupWorkload() {
	nTopics := 1 + w.rnd.Intn(3)
	for i := 0; i < nTopics; i++ {
		w.topics = append(w.topics, topicSpec{name: fmt.Sprintf("t%d", i), parts: int32(1 + w.rnd.Intn(4))})
	}
	admin := w.newClient("admin")
	admin.busy = true
	remaining := len(w.topics)
	var create func(t topicSpec)
	create = func(t topicSpec) {
		req := kmsg.NewPtrCreateTopicsRequest()
		req.Version = 5
		ct := kmsg.NewCreateTopicsRequestTopic()
		ct.Topic, ct.NumPartitions, ct.ReplicationFactor = t.name, t.parts, -1
		req.Topics = append(req.Topics, ct)
		admin.kafka(w.randomAgent(), req, 5_000, func(r kmsg.Response) {
			if r != nil {
				code := r.(*kmsg.CreateTopicsResponse).Topics[0].ErrorCode
				if code == 0 || code == 36 { // created, or created by an earlier attempt
					if remaining--; remaining == 0 {
						admin.busy = false
						w.startClients()
					}
					return
				}
			}
			w.at(w.now+200, func() { create(t) })
		})
	}
	for _, t := range w.topics {
		create(t)
	}
}

func (w *World) startClients() {
	k := w.knobs
	for i := 0; i < k.Producers+k.IdempotentProducers; i++ {
		c := w.newClient("producer")
		c.idem = i >= k.Producers
		c.busy = true
		if c.idem {
			c.initProducerID(c.produceNext)
		} else {
			w.at(w.now+w.rnd.Int63n(100), c.produceNext)
		}
	}
	for i := 0; i < k.Fetchers; i++ {
		c := w.newClient("fetcher")
		c.busy = true
		w.at(w.now+w.rnd.Int63n(100), c.fetchNext)
	}
	for i := 0; i < k.Committers; i++ {
		c := w.newClient("committer")
		c.busy = true
		c.counter = int64(c.id) * 1_000_000
		w.at(w.now+w.rnd.Int63n(100), c.commitNext)
	}
	for i := 0; i < k.GroupMembers; i++ {
		c := w.newClient("member")
		c.busy = true
		w.at(w.now+w.rnd.Int63n(1_000), c.join)
	}
}

// ---- Producers ----

func (c *client) initProducerID(then func()) {
	req := kmsg.NewPtrInitProducerIDRequest()
	req.Version = 4
	c.kafka(c.w.randomAgent(), req, 5_000, func(r kmsg.Response) {
		if r == nil || r.(*kmsg.InitProducerIDResponse).ErrorCode != 0 {
			if c.w.now >= c.w.end {
				c.busy = false
				return
			}
			c.w.at(c.w.now+200, func() { c.initProducerID(then) })
			return
		}
		c.pid = r.(*kmsg.InitProducerIDResponse).ProducerID
		then()
	})
}

func (c *client) refreshMetadata(then func()) {
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 12
	c.kafka(c.w.randomAgent(), req, 3_000, func(r kmsg.Response) {
		if r != nil {
			for _, t := range r.(*kmsg.MetadataResponse).Topics {
				for _, p := range t.Partitions {
					c.leaders[checker.Partition{Topic: *t.Topic, Partition: p.Partition}] = p.Leader
				}
			}
		}
		then()
	})
}

func (c *client) produceNext() {
	w := c.w
	if w.now >= w.end {
		c.busy = false
		return
	}
	ps := w.partitions()
	part := ps[w.rnd.Intn(len(ps))]
	var recs []kbatch.Record
	op := checker.ProduceOp{Client: c.id, Idempotent: c.idem, Part: part, Start: w.now}
	for i := 0; i < 1+w.rnd.Intn(4); i++ {
		c.n++
		v := fmt.Sprintf("%s-%d", c.name, c.n)
		op.Values = append(op.Values, v)
		recs = append(recs, kbatch.Record{Value: []byte(v)})
	}
	batch := kbatch.Build(w.now, recs)
	if c.idem {
		kbatch.SetProducer(batch, c.pid, 0, c.seqs[part])
	}
	hdr, _ := kbatch.Validate(batch)
	maxAttempts := 5
	finish := func() {
		op.End = w.now
		w.hist.Produces = append(w.hist.Produces, op)
		w.ops++
		if op.Acked && c.idem {
			c.seqs[part] += int32(len(recs))
		}
		w.at(w.now+w.rnd.Int63n(50), c.produceNext)
	}
	var attempt func()
	retry := func(delay int64) {
		// Idempotent producers retry until acked; giving up would open a
		// sequence gap. Plain producers give up after a few attempts.
		if (!c.idem && op.Attempts >= maxAttempts) || w.now > w.end+40_000 {
			finish()
			return
		}
		w.at(w.now+delay, attempt)
	}
	attempt = func() {
		op.Attempts++
		to := w.randomAgent()
		if l, ok := c.leaders[part]; ok && l > 0 {
			to = w.agentByID(l)
		}
		timeout := 2_000 + w.rnd.Int63n(4_000)
		w.send(c, to, func(id uint64) any {
			return agent.ProduceReq{ID: id, TimeoutMs: timeout, Acks: -1, Parts: []agent.ProducePart{{
				Topic: part.Topic, Partition: part.Partition, Batches: [][]byte{batch}, Headers: []kbatch.Header{hdr},
			}}}
		}, timeout+2_000, func(r any) {
			resp, ok := r.(agent.ProduceResp)
			if !ok {
				retry(100 + w.rnd.Int63n(400))
				return
			}
			res := resp.Parts[0]
			switch res.Err {
			case 0:
				op.Acked, op.BaseOffset = true, res.BaseOffset
				w.tracef("ACK %s %v -> %s@%d via %s (attempt %d)", c.name, op.Values, part, res.BaseOffset, to.name, op.Attempts)
				finish()
			case agent.ErrNotLeader:
				c.refreshMetadata(func() { retry(w.rnd.Int63n(50)) })
			case 45, 46, 47: // sequence errors cannot happen with one in-flight batch per partition
				w.violation("I2", "idempotent producer %s got error %d for seq %d on %s", c.name, res.Err, c.seqs[part], part)
				finish()
			default:
				retry(50 + w.rnd.Int63n(300))
			}
		})
	}
	attempt()
}

// ---- Fetchers ----

func (c *client) fetchNext() {
	w := c.w
	if w.now >= w.end+5_000 {
		c.busy = false
		return
	}
	ps := w.partitions()
	part := ps[w.rnd.Intn(len(ps))]
	offset := c.offsets[part]
	maxWait := []int64{0, 50, 500, 1_500}[w.rnd.Intn(4)]
	partMax := []int32{1, 1 << 10, 64 << 10, 1 << 20}[w.rnd.Intn(4)]
	start := w.now
	w.send(c, w.randomAgent(), func(id uint64) any {
		return agent.FetchReq{ID: id, MaxWaitMs: maxWait, MinBytes: 1, MaxBytes: 1 << 20,
			Parts: []agent.FetchPart{{Topic: part.Topic, Partition: part.Partition, Offset: offset, MaxBytes: partMax}}}
	}, maxWait+3_000, func(r any) {
		next := w.rnd.Int63n(30)
		defer func() { w.at(w.now+next, c.fetchNext) }()
		resp, ok := r.(agent.FetchResp)
		if !ok {
			return
		}
		w.ops++
		res := resp.Parts[0]
		switch res.Err {
		case 0:
		case agent.ErrOffsetOutOfRange:
			if offset < res.LogStart {
				c.offsets[part] = res.LogStart
			}
			next = 200
			return
		default:
			next = 100
			return
		}
		bs, hs, err := kbatch.Split(res.Records)
		if err != nil {
			w.violation("I4", "fetch %s@%d returned malformed batches: %v", part, offset, err)
			return
		}
		f := checker.FetchOp{Client: c.id, Part: part, Offset: offset, Start: start, End: w.now}
		for i, b := range bs {
			recs, _, ok := kbatch.Records(b)
			if !ok {
				w.violation("I4", "fetch %s@%d returned an undecodable batch", part, offset)
				return
			}
			for j, rec := range recs {
				f.Records = append(f.Records, checker.Rec{Offset: hs[i].BaseOffset + int64(j), Value: string(rec.Value)})
			}
		}
		if len(f.Records) > 0 {
			w.hist.Fetches = append(w.hist.Fetches, f)
			c.offsets[part] = f.Records[len(f.Records)-1].Offset + 1
		}
	})
}

// ---- Offset committers ----

func (c *client) commitNext() {
	w := c.w
	if w.now >= w.end {
		c.busy = false
		return
	}
	ps := w.partitions()
	part := ps[w.rnd.Intn(len(ps))]
	group := fmt.Sprintf("cg-%d", c.id%2)
	start := w.now
	next := func() { w.at(w.now+w.rnd.Int63n(200), c.commitNext) }
	if w.rnd.Intn(2) == 0 {
		c.counter++
		off := c.counter
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Version = 8
		req.Group, req.Generation = group, -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = part.Topic
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition, rp.Offset = part.Partition, off
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		c.kafka(w.randomAgent(), req, 3_000, func(r kmsg.Response) {
			op := checker.CommitOp{Group: group, Part: part, Offset: off, Start: start, End: w.now}
			if r != nil {
				op.Acked = r.(*kmsg.OffsetCommitResponse).Topics[0].Partitions[0].ErrorCode == 0
			}
			w.hist.Commits = append(w.hist.Commits, op)
			w.ops++
			next()
		})
		return
	}
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version = 7
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic, rt.Partitions = part.Topic, []int32{part.Partition}
	req.Topics = append(req.Topics, rt)
	c.kafka(w.randomAgent(), req, 3_000, func(r kmsg.Response) {
		if r != nil {
			resp := r.(*kmsg.OffsetFetchResponse)
			if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 && resp.Topics[0].Partitions[0].ErrorCode == 0 {
				w.hist.OffsetFetches = append(w.hist.OffsetFetches, checker.OffsetFetchOp{
					Group: group, Part: part, Offset: resp.Topics[0].Partitions[0].Offset, Start: start, End: w.now,
				})
				w.ops++
			}
		}
		next()
	})
}

// ---- Consumer group members ----

const memberGroup = "gm"

func (c *client) join() {
	w := c.w
	if w.now >= w.end {
		c.busy = false
		return
	}
	req := kmsg.NewPtrJoinGroupRequest()
	req.Version = 5
	req.Group, req.SessionTimeoutMillis, req.RebalanceTimeoutMillis = memberGroup, 6_000, 8_000
	req.MemberID, req.ProtocolType = c.member, "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name, p.Metadata = "rr", []byte(c.name)
	req.Protocols = append(req.Protocols, p)
	c.kafka(w.randomAgent(), req, 15_000, func(r kmsg.Response) {
		if r == nil {
			w.at(w.now+300, c.join)
			return
		}
		resp := r.(*kmsg.JoinGroupResponse)
		switch resp.ErrorCode {
		case 0:
		case 79: // MEMBER_ID_REQUIRED
			c.member = resp.MemberID
			w.at(w.now, c.join)
			return
		case 25: // UNKNOWN_MEMBER_ID
			c.member = ""
			w.at(w.now+100, c.join)
			return
		default:
			w.at(w.now+200, c.join)
			return
		}
		c.generation = resp.Generation
		var ms []string
		for _, m := range resp.Members {
			ms = append(ms, m.MemberID[len(m.MemberID)-6:])
		}
		w.tracef("JOINED %s as %s gen=%d leader=%s members=%v", c.name, c.member, resp.Generation, resp.LeaderID, ms)
		sync := kmsg.NewPtrSyncGroupRequest()
		sync.Version = 4
		sync.Group, sync.Generation, sync.MemberID = memberGroup, resp.Generation, c.member
		if resp.LeaderID == c.member {
			// Round-robin the first topic's partitions over the members.
			var ids []string
			for _, m := range resp.Members {
				ids = append(ids, m.MemberID)
			}
			sort.Strings(ids)
			assign := map[string][]string{}
			for p := int32(0); p < w.topics[0].parts; p++ {
				m := ids[int(p)%len(ids)]
				assign[m] = append(assign[m], strconv.Itoa(int(p)))
			}
			for _, id := range ids {
				a := kmsg.NewSyncGroupRequestGroupAssignment()
				a.MemberID, a.MemberAssignment = id, []byte(strings.Join(assign[id], ","))
				sync.GroupAssignment = append(sync.GroupAssignment, a)
			}
		}
		c.sync(sync)
	})
}

func (c *client) sync(req *kmsg.SyncGroupRequest) {
	w := c.w
	c.kafka(w.randomAgent(), req, 15_000, func(r kmsg.Response) {
		if r == nil {
			w.at(w.now+200, c.join)
			return
		}
		resp := r.(*kmsg.SyncGroupResponse)
		switch resp.ErrorCode {
		case 0:
		case 25:
			c.member = ""
			w.at(w.now+100, c.join)
			return
		default:
			w.at(w.now+100, c.join)
			return
		}
		s := checker.SyncOp{Group: memberGroup, Generation: req.Generation, Member: c.member}
		for _, p := range strings.Split(string(resp.MemberAssignment), ",") {
			if p == "" {
				continue
			}
			n, _ := strconv.ParseInt(p, 10, 32)
			s.Assigned = append(s.Assigned, checker.Partition{Topic: w.topics[0].name, Partition: int32(n)})
		}
		w.hist.Syncs = append(w.hist.Syncs, s)
		w.tracef("SYNCED %s as %s gen=%d assigned=%s", c.name, c.member, req.Generation, resp.MemberAssignment)
		w.ops++
		w.at(w.now+500, c.heartbeat)
	})
}

func (c *client) heartbeat() {
	w := c.w
	if w.now >= w.end {
		c.busy = false
		return
	}
	if w.rnd.Intn(40) == 0 { // leave and come back later
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Version = 3
		req.Group = memberGroup
		m := kmsg.NewLeaveGroupRequestMember()
		m.MemberID = c.member
		req.Members = append(req.Members, m)
		c.member = ""
		c.kafka(w.randomAgent(), req, 3_000, func(kmsg.Response) { w.at(w.now+2_000, c.join) })
		return
	}
	req := kmsg.NewPtrHeartbeatRequest()
	req.Version = 3
	req.Group, req.Generation, req.MemberID = memberGroup, c.generation, c.member
	c.kafka(w.randomAgent(), req, 3_000, func(r kmsg.Response) {
		if r == nil {
			w.at(w.now+500, c.heartbeat)
			return
		}
		switch r.(*kmsg.HeartbeatResponse).ErrorCode {
		case 0:
			w.at(w.now+1_000, c.heartbeat)
		case 25:
			c.member = ""
			c.join()
		default:
			c.join()
		}
	})
}

// ---- Finish: heal, read the final bucket and check ----

func (w *World) finish() Result {
	res := Result{Seed: w.cfg.Seed, Ops: w.ops, Stats: w.stats, Knobs: w.knobs, History: &w.hist}
	ctx := ctxBackground()
	st, vs, err := checker.CheckBucket(ctx, w.store, "sim", w.knobs.GCGraceMs, w.knobs.RecentCommits)
	res.Violations = append(res.Violations, w.violations...)
	if err != nil {
		res.Violations = append(res.Violations, checker.Violation{Invariant: "RECOVERY", Detail: err.Error()})
		res.Fingerprint = fingerprint(&w.hist, nil, w.stats)
		return res
	}
	res.Violations = append(res.Violations, vs...)
	final, err := checker.ReadFinal(ctx, w.store, st)
	if err != nil {
		res.Violations = append(res.Violations, checker.Violation{Invariant: "I7", Detail: err.Error()})
	} else {
		res.Violations = append(res.Violations, checker.Check(&w.hist, final)...)
	}
	// The live leader, once caught up, must hold exactly the recovered state.
	for _, n := range w.seqs {
		if n.up && n.seq != nil && n.seq.Role() == "leader" {
			if ls := n.seq.State(); ls != nil && ls.Seq == st.Seq && ls.Hash() != st.Hash() {
				res.Violations = append(res.Violations, checker.Violation{Invariant: "I6", Detail: fmt.Sprintf("live leader %s state differs from recovered state at seq %d", n.name, st.Seq)})
			}
		}
	}
	res.Stats["metalog_entries"] = int(st.Seq)
	res.Stats["epoch"] = int(st.Epoch)
	res.Fingerprint = fingerprint(&w.hist, st, w.stats)
	return res
}
