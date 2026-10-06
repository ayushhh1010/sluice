package sequencer

import (
	"google.golang.org/protobuf/proto"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/bugs"
)

// Op kinds in apply order. Building an entry plans items in queue order but
// emits ops grouped by kind, so planning only needs overlays for what can
// change within one entry: partition next offsets, producer sequence state,
// new topic names and producer ID allocation. Topic creations apply after
// commits and deletions apply last, so a commit planned in the same entry as
// a create or delete of its topic is conservatively rejected.
const (
	kindCommit = iota
	kindOffsets
	kindCreate
	kindAlloc
	kindReplace
	kindTruncate
	kindDelete
	numKinds
)

type planner struct {
	c         *Core
	byKind    [numKinds][]*pb.Op
	next      map[TP]int64
	producers map[producerKey]*producerState
	newNames  map[string]bool
	deleted   map[TopicID]bool
	nextPID   int64
	publicNew int
}

func newPlanner(c *Core) *planner {
	return &planner{
		c:         c,
		next:      map[TP]int64{},
		producers: map[producerKey]*producerState{},
		newNames:  map[string]bool{},
		deleted:   map[TopicID]bool{},
		nextPID:   c.st.NextPID,
	}
}

func (pl *planner) add(kind int, op *pb.Op) { pl.byKind[kind] = append(pl.byKind[kind], op) }

func (pl *planner) ops() []*pb.Op {
	var out []*pb.Op
	for _, ops := range pl.byKind {
		out = append(out, ops...)
	}
	return out
}

// Kafka error codes used by planning.
const (
	errUnknownTopicOrPartition = 3
	errCorruptMessage          = 2
	errOutOfOrderSequence      = 45
	errDuplicateSequence       = 46
	errInvalidProducerEpoch    = 47
)

// planCommit assigns offsets to every batch of one agent segment.
func (pl *planner) planCommit(req *pb.SegmentCommit) *pb.CommitBatches {
	cb := &pb.CommitBatches{ObjectKey: req.ObjectKey, ObjectCreatedMs: req.ObjectCreatedMs}
	appended := 0
	var dropped []int
	for _, fe := range req.Entries {
		r := &pb.BatchResult{
			TopicId: fe.TopicId, Partition: fe.Partition, OffsetInObj: fe.OffsetInObj, Length: fe.Length,
			Records: fe.Records, MaxTs: fe.MaxTs, ProducerId: fe.ProducerId, ProducerEpoch: fe.ProducerEpoch, BaseSeq: fe.BaseSeq,
		}
		cb.Results = append(cb.Results, r)
		id, ok := toTopicID(fe.TopicId)
		part := pl.c.st.Partition(id, fe.Partition)
		if !ok || part == nil || pl.deleted[id] {
			r.ErrorCode = errUnknownTopicOrPartition
			continue
		}
		if fe.Records == 0 || fe.Length == 0 {
			r.ErrorCode = errCorruptMessage
			continue
		}
		tp := TP{id, fe.Partition}
		next, ok := pl.next[tp]
		if !ok {
			next = part.Next
		}
		if fe.ProducerId >= 0 {
			code, dupOffset := pl.checkSequence(producerKey{fe.ProducerId, tp}, fe)
			if code != 0 {
				r.ErrorCode = code
				continue
			}
			if dupOffset >= 0 {
				r.Duplicate = true
				r.BaseOffset = dupOffset
				continue
			}
			pl.recordSequence(producerKey{fe.ProducerId, tp}, fe, next)
		}
		r.BaseOffset = next
		if bugs.On("drop-acked-batch") && next%11 == 5 {
			dropped = append(dropped, len(cb.Results)-1)
			continue // acked below, but never logged
		}
		pl.next[tp] = next + int64(fe.Records)
		appended++
	}
	if appended > 0 || hasDuplicate(cb) {
		logged := cb
		if len(dropped) > 0 {
			logged = proto.Clone(cb).(*pb.CommitBatches)
			for _, i := range dropped {
				logged.Results[i].ErrorCode = errCorruptMessage
			}
		}
		pl.add(kindCommit, &pb.Op{Body: &pb.Op_Commit{Commit: logged}})
	}
	return cb
}

func hasDuplicate(cb *pb.CommitBatches) bool {
	for _, r := range cb.Results {
		if r.Duplicate {
			return true
		}
	}
	return false
}

func (pl *planner) producer(k producerKey) *producerState {
	if ps, ok := pl.producers[k]; ok {
		return ps
	}
	ps := pl.c.st.Producers[k]
	if ps != nil {
		ps = &producerState{Epoch: ps.Epoch, Recent: append([]*pb.SeqRange(nil), ps.Recent...)}
	}
	pl.producers[k] = ps
	return ps
}

// checkSequence implements idempotent-producer deduplication for one batch:
// it returns a Kafka error code, or the base offset of the original batch if
// this one is a retry of an appended batch (-1 otherwise).
func (pl *planner) checkSequence(k producerKey, fe *pb.FooterEntry) (int32, int64) {
	ps := pl.producer(k)
	if ps == nil {
		return 0, -1 // first batch we have seen for this producer and partition
	}
	switch {
	case fe.ProducerEpoch < ps.Epoch:
		return errInvalidProducerEpoch, -1
	case fe.ProducerEpoch > ps.Epoch:
		if fe.BaseSeq != 0 {
			return errOutOfOrderSequence, -1
		}
		return 0, -1
	}
	last := fe.BaseSeq + int32(fe.Records) - 1
	for _, r := range ps.Recent {
		if r.FirstSeq == fe.BaseSeq && r.LastSeq == last {
			return 0, r.BaseOffset
		}
	}
	if len(ps.Recent) == 0 {
		return 0, -1
	}
	newest := ps.Recent[len(ps.Recent)-1]
	if fe.BaseSeq == nextSeq(newest.LastSeq) {
		return 0, -1
	}
	if seqBefore(fe.BaseSeq, ps.Recent[0].FirstSeq) {
		return errDuplicateSequence, -1
	}
	return errOutOfOrderSequence, -1
}

func (pl *planner) recordSequence(k producerKey, fe *pb.FooterEntry, base int64) {
	ps := pl.producers[k]
	if ps == nil || fe.ProducerEpoch != ps.Epoch {
		ps = &producerState{Epoch: fe.ProducerEpoch}
		pl.producers[k] = ps
	}
	ps.Recent = append(ps.Recent, &pb.SeqRange{FirstSeq: fe.BaseSeq, LastSeq: fe.BaseSeq + int32(fe.Records) - 1, BaseOffset: base})
	if len(ps.Recent) > 5 {
		ps.Recent = ps.Recent[1:]
	}
}

// Kafka sequence numbers wrap from MaxInt32 to 0.
func nextSeq(s int32) int32 {
	if s == 1<<31-1 {
		return 0
	}
	return s + 1
}

func seqBefore(a, b int32) bool { return int32(uint32(a)-uint32(b)) < 0 }
