package sequencer

import (
	"testing"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/metalog"
)

// Recovery reads whatever the bucket holds. Decoding and applying any byte
// string must fail cleanly (ErrCorruptLog), never panic.
func FuzzApply(f *testing.F) {
	tid := make([]byte, 16)
	tid[0] = 1
	seed := []*pb.Entry{
		{Seq: 2, Epoch: 1, Ops: []*pb.Op{{Body: &pb.Op_Commit{Commit: &pb.CommitBatches{ObjectKey: "k", Results: []*pb.BatchResult{{TopicId: tid, Records: 2, Length: 9, BaseOffset: 0}}}}}}},
		{Seq: 2, Epoch: 1, Ops: []*pb.Op{{Body: &pb.Op_Truncate{Truncate: &pb.Truncate{Starts: []*pb.LogStart{{TopicId: tid, LogStart: 5}}}}}}},
		{Seq: 2, Epoch: 1, Ops: []*pb.Op{{Body: &pb.Op_Replace{Replace: &pb.ReplaceObjects{Replacements: []*pb.Replacement{{TopicId: tid, OldKey: "k", NewKey: "m"}}}}}}},
		{Seq: 2, Epoch: 1, Ops: []*pb.Op{{Body: &pb.Op_DeleteTopic{DeleteTopic: &pb.DeleteTopic{TopicId: tid}}}, {}}},
	}
	for _, e := range seed {
		f.Add(metalog.EncodeEntry(e))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		s := NewState(1000, 16)
		_ = s.Apply(&pb.Entry{Seq: 1, Epoch: 1, Ops: []*pb.Op{{Body: &pb.Op_CreateTopic{CreateTopic: &pb.CreateTopic{TopicId: tid, Name: "t", Partitions: 2}}}}})
		e, err := metalog.DecodeEntry(b)
		if err != nil {
			return
		}
		if s.Apply(e) == nil {
			if err := s.Verify(); err != nil {
				t.Fatalf("applied entry left inconsistent state: %v", err)
			}
		}
		if snap, err := metalog.DecodeSnapshot(b); err == nil {
			_, _ = StateFromSnapshot(snap, 1000, 16)
		}
	})
}
