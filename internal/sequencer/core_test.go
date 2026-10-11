package sequencer

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/metalog"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/proc"
)

// driver runs one core synchronously against an in-memory store.
type driver struct {
	t       *testing.T
	c       *Core
	store   *objstore.Mem
	now     int64
	wake    int64
	replies map[uint64]*pb.Response
	nextID  uint64
}

func newDriver(t *testing.T, cfg Config) *driver {
	if cfg.Node == "" {
		cfg.Node = "seq-a"
	}
	cfg.Cluster = "test"
	d := &driver{t: t, c: New(cfg, 1), store: objstore.NewMem(), now: 1_790_000_000_000, replies: map[uint64]*pb.Response{}}
	d.step(Start{})
	d.run(15_000)
	if d.c.Role() != "leader" {
		t.Fatalf("not leader: %s", d.c.Role())
	}
	return d
}

func (d *driver) step(ev any) {
	queue := []any{ev}
	for len(queue) > 0 {
		ev := queue[0]
		queue = queue[1:]
		for _, e := range d.c.Step(d.now, ev) {
			switch e := e.(type) {
			case proc.StoreReq:
				queue = append(queue, d.exec(e))
			case proc.Call:
				queue = append(queue, proc.CallRes{ID: e.ID, Err: context.DeadlineExceeded})
			case proc.WakeAt:
				d.wake = e.At
			case Reply:
				d.replies[e.ID] = e.Resp
			}
		}
	}
}

func (d *driver) exec(r proc.StoreReq) proc.StoreRes {
	ctx := context.Background()
	res := proc.StoreRes{ID: r.ID}
	switch r.Op {
	case proc.OpGet:
		res.Data, res.Err = d.store.Get(ctx, r.Key, r.Off, r.Len)
	case proc.OpPut, proc.OpPutIfAbsent:
		res.Err = d.store.Put(ctx, r.Key, r.Data, r.Op == proc.OpPutIfAbsent)
	case proc.OpHead:
		res.Size, res.Err = d.store.Head(ctx, r.Key)
	case proc.OpDelete:
		res.Err = d.store.Delete(ctx, r.Key)
	case proc.OpList:
		res.List, res.Err = d.store.List(ctx, r.Prefix, r.After, r.Limit)
	}
	return res
}

// run advances virtual time by ms, firing timers.
func (d *driver) run(ms int64) {
	end := d.now + ms
	for d.wake > 0 && d.wake <= end {
		d.now = max(d.now, d.wake)
		d.wake = 0
		d.step(proc.Tick{})
	}
	d.now = end
	d.step(proc.Tick{})
}

func (d *driver) call(req *pb.Request) *pb.Response {
	d.nextID++
	id := d.nextID
	d.step(Request{ID: id, Req: req})
	for i := 0; i < 100 && d.replies[id] == nil; i++ {
		d.run(50)
	}
	r := d.replies[id]
	if r == nil {
		d.t.Fatalf("no reply to %v", req)
	}
	return r
}

func (d *driver) kafka(req kmsg.Request) kmsg.Response {
	r := d.call(&pb.Request{Body: &pb.Request_Kafka{Kafka: &pb.KafkaRequest{
		ApiKey: int32(req.Key()), ApiVersion: int32(req.GetVersion()), Body: req.AppendTo(nil), Listener: "internal", ClientId: "test",
	}}})
	if r.Code != pb.Code_CODE_OK {
		d.t.Fatalf("kafka call: %v %s", r.Code, r.Message)
	}
	kr := req.ResponseKind()
	if err := kr.ReadFrom(r.GetKafka().GetBody()); err != nil {
		d.t.Fatal(err)
	}
	return kr
}

func (d *driver) createTopic(name string, parts int32) TopicID {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Version = 7
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = name, parts, -1
	req.Topics = append(req.Topics, t)
	resp := d.kafka(req).(*kmsg.CreateTopicsResponse)
	if resp.Topics[0].ErrorCode != 0 {
		d.t.Fatalf("create topic: %d", resp.Topics[0].ErrorCode)
	}
	return TopicID(resp.Topics[0].TopicID)
}

func (d *driver) commit(key string, tid TopicID, part uint32, records uint32) *pb.Response {
	return d.call(&pb.Request{Body: &pb.Request_Commit{Commit: &pb.CommitRequest{Segments: []*pb.SegmentCommit{{
		ObjectKey: key, ObjectCreatedMs: d.now,
		Entries: []*pb.FooterEntry{{TopicId: tid[:], Partition: part, OffsetInObj: 32, Length: 100, Records: records, ProducerId: -1}},
	}}}}})
}

func TestCommitAssignsDenseOffsetsAndDedupsRetries(t *testing.T) {
	d := newDriver(t, Config{})
	tid := d.createTopic("orders", 2)
	var want int64
	for i := 0; i < 5; i++ {
		key := metalog.NewKeys("test").Data(d.now, "agent-1", metalog.ULID(d.now, uint64(i), 0))
		r := d.commit(key, tid, 0, 3)
		if got := r.GetCommit().Segments[0].Results[0].BaseOffset; got != want {
			t.Fatalf("commit %d: base %d, want %d", i, got, want)
		}
		// A retried commit returns the original answer, without a new append.
		before := d.c.State().Seq
		if again := d.commit(key, tid, 0, 3); again.GetCommit().Segments[0].Results[0].BaseOffset != want || d.c.State().Seq != before {
			t.Fatalf("retry was not deduplicated")
		}
		want += 3
	}
	lr := d.call(&pb.Request{Body: &pb.Request_Lookup{Lookup: &pb.LookupRequest{Topic: "orders", Partition: 0, Offset: 4, MaxBytes: 1 << 20}}}).GetLookup()
	if lr.HighWatermark != 15 || len(lr.Refs) != 4 || lr.Refs[0].BaseOffset != 3 {
		t.Fatalf("lookup: hw %d, %d refs from %d", lr.HighWatermark, len(lr.Refs), lr.Refs[0].BaseOffset)
	}
	if err := d.c.State().Verify(); err != nil {
		t.Fatal(err)
	}
}

// Regression (BUGS.md #3): a leader that wins a log slot which garbage
// collection already emptied must not acknowledge anything.
func TestWinningAGarbageCollectedSlotStepsDown(t *testing.T) {
	d := newDriver(t, Config{})
	tid := d.createTopic("orders", 1)
	// Simulate GC having deleted our last applied entry: the next slot we
	// write is then in a dead zone no recovery will read.
	_ = d.store.Delete(context.Background(), d.c.keys.Log(d.c.State().Seq))
	r := d.commit(metalog.NewKeys("test").Data(d.now, "agent-1", metalog.ULID(d.now, 9, 9)), tid, 0, 1)
	if r.Code == pb.Code_CODE_OK && r.GetCommit().GetSegments()[0].Code == pb.Code_CODE_OK {
		t.Fatalf("acknowledged a commit written into a garbage-collected slot")
	}
	if d.c.Role() == "leader" {
		t.Fatalf("still leader after losing log continuity")
	}
}

// Regression (BUGS.md #5): grace must outlast every commit retry.
func TestGCGraceCoversCommitRetries(t *testing.T) {
	c := Config{GCGraceMs: 1_000, MaxSegmentAgeMs: 60_000}
	c.setDefaults()
	if c.GCGraceMs < 2*c.MaxSegmentAgeMs || c.OrphanMinAgeMs < c.MaxSegmentAgeMs*3/2 {
		t.Fatalf("unsafe GC timing accepted: %+v", c)
	}
}

// Regression (BUGS.md #1, #2): generations never repeat, even after the
// group empties and its in-memory state is dropped.
func TestGroupGenerationsNeverRepeat(t *testing.T) {
	d := newDriver(t, Config{GroupInitialDelayMs: -1, GroupMinSessionMs: 1000})
	join := func(member string) *kmsg.JoinGroupResponse {
		req := kmsg.NewPtrJoinGroupRequest()
		req.Version = 5
		req.Group, req.SessionTimeoutMillis, req.RebalanceTimeoutMillis = "g", 2000, 2000
		req.MemberID, req.ProtocolType = member, "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name = "range"
		req.Protocols = append(req.Protocols, p)
		return d.kafka(req).(*kmsg.JoinGroupResponse)
	}
	first := join("")
	if first.ErrorCode != 79 {
		t.Fatalf("want MEMBER_ID_REQUIRED, got %d", first.ErrorCode)
	}
	g1 := join(first.MemberID)
	// Let the session expire so the group empties and is cleaned up.
	d.run(5_000)
	again := join("")
	g2 := join(again.MemberID)
	if g2.Generation <= g1.Generation {
		t.Fatalf("generation went from %d to %d", g1.Generation, g2.Generation)
	}
}

func TestOffsetCommitIsDurableAcrossRecovery(t *testing.T) {
	d := newDriver(t, Config{})
	d.createTopic("orders", 1)
	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Version = 8
	oc.Group, oc.Generation = "g", -1
	ot := kmsg.NewOffsetCommitRequestTopic()
	ot.Topic = "orders"
	op := kmsg.NewOffsetCommitRequestTopicPartition()
	op.Offset = 42
	ot.Partitions = append(ot.Partitions, op)
	oc.Topics = append(oc.Topics, ot)
	if code := d.kafka(oc).(*kmsg.OffsetCommitResponse).Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatalf("commit: %d", code)
	}
	// A fresh sequencer recovering from the same bucket sees the offset.
	d2 := &driver{t: t, c: New(Config{Node: "seq-b", Cluster: "test"}, 2), store: d.store, now: d.now, replies: map[uint64]*pb.Response{}}
	d2.step(Start{})
	d2.run(15_000)
	of := kmsg.NewPtrOffsetFetchRequest()
	of.Version = 7
	of.Group = "g"
	of.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0}}}
	if got := d2.kafka(of).(*kmsg.OffsetFetchResponse).Topics[0].Partitions[0].Offset; got != 42 {
		t.Fatalf("recovered offset %d, want 42", got)
	}
}

func TestDeposedLeaderStepsDownOnNextSlotCheck(t *testing.T) {
	d := newDriver(t, Config{})
	// Another sequencer appends past us.
	e := &pb.Entry{Seq: d.c.State().Seq + 1, Epoch: d.c.State().Epoch + 1, Writer: "seq-b",
		Ops: []*pb.Op{{Body: &pb.Op_BecomeLeader{BecomeLeader: &pb.BecomeLeader{Node: "seq-b"}}}}}
	if err := d.store.Put(context.Background(), d.c.keys.Log(e.Seq), metalog.EncodeEntry(e), true); err != nil {
		t.Fatal(err)
	}
	d.run(2_000)
	if d.c.Role() == "leader" {
		t.Fatal("zombie leader did not notice the newer epoch")
	}
}

// Segments in one CommitRequest are ordered as sent: a pipelining
// producer's later batch never gets the lower offset.
func TestMultiSegmentCommitKeepsRequestOrder(t *testing.T) {
	d := newDriver(t, Config{})
	tid := d.createTopic("orders", 1)
	keys := metalog.NewKeys("test")
	req := &pb.CommitRequest{}
	for i := 0; i < 5; i++ {
		req.Segments = append(req.Segments, &pb.SegmentCommit{
			ObjectKey: keys.Data(d.now, "agent-1", metalog.ULID(d.now, uint64(100+i), 0)), ObjectCreatedMs: d.now,
			Entries: []*pb.FooterEntry{{TopicId: tid[:], OffsetInObj: 32, Length: 10, Records: uint32(i + 1), ProducerId: -1}},
		})
	}
	r := d.call(&pb.Request{Body: &pb.Request_Commit{Commit: req}})
	var next int64
	for i, o := range r.GetCommit().GetSegments() {
		if o.Code != pb.Code_CODE_OK || o.Results[0].BaseOffset != next {
			t.Fatalf("segment %d: code %v base %d, want %d", i, o.Code, o.Results[0].BaseOffset, next)
		}
		next += int64(i + 1)
	}
}

// Regression (BUGS.md #17, nightly simulator): a node that listed no
// snapshots, paused, and resumed after log GC found slot 1 empty and led
// from an empty state, because slot 1 has no predecessor to confirm.
func TestClaimingGarbageCollectedSlotOneIsRejected(t *testing.T) {
	d := &driver{t: t, c: New(Config{Node: "seq-b", Cluster: "test"}, 3), store: objstore.NewMem(), now: 1_790_000_000_000, replies: map[uint64]*pb.Response{}}
	// The log grew past slot 1, two snapshots were taken and GC deleted slot 1.
	_ = d.store.Put(context.Background(), d.c.keys.Snap(110), metalog.EncodeSnapshot(&pb.Snapshot{Seq: 110, Epoch: 1}), false)
	d.c.role, d.c.st = roleRecovering, d.c.newState() // stale view from before the snapshot existed
	d.c.claim(1)
	for _, e := range d.c.flush() {
		if r, ok := e.(proc.StoreReq); ok {
			d.step(d.exec(r))
		}
	}
	if d.c.Role() == "leader" {
		t.Fatal("led from an empty state after winning a garbage-collected slot 1")
	}
}
