package sequencer

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/metalog"
)

// TopicID is a 16-byte topic UUID.
type TopicID [16]byte

func toTopicID(b []byte) (TopicID, bool) {
	var t TopicID
	if len(b) != 16 {
		return t, false
	}
	copy(t[:], b)
	return t, true
}

// MaxPartitionsLimit bounds partitions per topic. Apply enforces it so a
// corrupt or hostile log entry cannot make recovery allocate without bound.
const MaxPartitionsLimit = 1 << 16

// TP names a partition.
type TP struct {
	Topic     TopicID
	Partition uint32
}

// Partition is one partition's log: refs sorted by base offset, covering
// [LogStart, Next) without gaps once the first ref at or below LogStart.
type Partition struct {
	LogStart int64
	Next     int64 // high watermark
	Refs     []*pb.BatchRef
}

// find returns the index of the ref containing offset, or -1.
func (p *Partition) find(offset int64) int {
	i := sort.Search(len(p.Refs), func(i int) bool {
		r := p.Refs[i]
		return r.BaseOffset+int64(r.Records) > offset
	})
	if i < len(p.Refs) && p.Refs[i].BaseOffset <= offset {
		return i
	}
	return -1
}

// findBase returns the index of the ref with exactly this base offset, or -1.
func (p *Partition) findBase(base int64) int {
	i := sort.Search(len(p.Refs), func(i int) bool { return p.Refs[i].BaseOffset >= base })
	if i < len(p.Refs) && p.Refs[i].BaseOffset == base {
		return i
	}
	return -1
}

type Topic struct {
	ID        TopicID
	Name      string
	Retention int64
	Owner     string
	CreatedMs int64
	Parts     []*Partition
}

type groupTP struct {
	Topic     string
	Partition uint32
}

type producerKey struct {
	PID int64
	TP  TP
}

type producerState struct {
	Epoch  int32
	Recent []*pb.SeqRange // oldest first, at most 5
}

// State is everything the metadata log determines. Apply is deterministic:
// replaying the same log always yields the same state (invariant I6).
type State struct {
	Seq    uint64
	Epoch  uint64
	Leader string

	Topics  map[TopicID]*Topic
	ByName  map[string]TopicID
	Offsets map[string]map[groupTP]*pb.OffsetCommit

	Producers map[producerKey]*producerState
	NextPID   int64

	// Live batch refs per object. Reaching zero tombstones the object.
	RefCount   map[string]int
	Tombstones map[string]int64 // key -> delete after (unix ms)

	// Recent commits for CommitBatches retry dedup, oldest first.
	Recent      []*pb.CommitBatches
	RecentByKey map[string]*pb.CommitBatches

	// Bookkeeping not covered by the state hash: snapshot and log GC.
	PriorSnaps []uint64
	LogGCFloor uint64

	gcGraceMs   int64
	recentLimit int
}

func NewState(gcGraceMs int64, recentLimit int) *State {
	return &State{
		Topics:      map[TopicID]*Topic{},
		ByName:      map[string]TopicID{},
		Offsets:     map[string]map[groupTP]*pb.OffsetCommit{},
		Producers:   map[producerKey]*producerState{},
		RefCount:    map[string]int{},
		Tombstones:  map[string]int64{},
		RecentByKey: map[string]*pb.CommitBatches{},
		gcGraceMs:   gcGraceMs,
		recentLimit: recentLimit,
	}
}

// ErrCorruptLog means an entry is inconsistent with the state it applies to.
// It indicates a safety bug or a damaged bucket; the sequencer refuses to run.
var ErrCorruptLog = errors.New("sequencer: metadata log inconsistent with state")

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruptLog, fmt.Sprintf(format, args...))
}

// Apply applies one entry. On error the state must be discarded.
func (s *State) Apply(e *pb.Entry) error {
	if e.Seq != s.Seq+1 {
		return corrupt("entry seq %d after %d", e.Seq, s.Seq)
	}
	if e.Epoch < s.Epoch {
		return corrupt("entry %d epoch %d below %d", e.Seq, e.Epoch, s.Epoch)
	}
	s.Seq = e.Seq
	s.Epoch = e.Epoch
	for _, op := range e.Ops {
		if err := s.applyOp(e, op); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) applyOp(e *pb.Entry, op *pb.Op) error {
	switch b := op.Body.(type) {
	case *pb.Op_BecomeLeader:
		s.Leader = b.BecomeLeader.Node
	case *pb.Op_Commit:
		return s.applyCommit(e, b.Commit)
	case *pb.Op_CreateTopic:
		c := b.CreateTopic
		id, ok := toTopicID(c.TopicId)
		if !ok || c.Partitions == 0 || c.Partitions > MaxPartitionsLimit {
			return corrupt("bad create %v", c)
		}
		if _, dup := s.ByName[c.Name]; dup {
			return corrupt("topic %q exists", c.Name)
		}
		if _, dup := s.Topics[id]; dup {
			return corrupt("topic id %x exists", id)
		}
		t := &Topic{ID: id, Name: c.Name, Retention: c.RetentionMs, Owner: c.Owner, CreatedMs: c.CreatedUnixMs}
		for i := uint32(0); i < c.Partitions; i++ {
			t.Parts = append(t.Parts, &Partition{})
		}
		s.Topics[id] = t
		s.ByName[c.Name] = id
	case *pb.Op_DeleteTopic:
		id, _ := toTopicID(b.DeleteTopic.TopicId)
		t, ok := s.Topics[id]
		if !ok {
			return corrupt("delete of unknown topic %x", id)
		}
		for _, p := range t.Parts {
			for _, r := range p.Refs {
				s.unref(e, r.ObjectKey)
			}
		}
		delete(s.Topics, id)
		delete(s.ByName, t.Name)
		for _, offs := range s.Offsets {
			for k := range offs {
				if k.Topic == t.Name {
					delete(offs, k)
				}
			}
		}
		for k := range s.Producers {
			if k.TP.Topic == id {
				delete(s.Producers, k)
			}
		}
	case *pb.Op_CommitOffsets:
		c := b.CommitOffsets
		offs := s.Offsets[c.Group]
		if offs == nil {
			offs = map[groupTP]*pb.OffsetCommit{}
			s.Offsets[c.Group] = offs
		}
		for _, o := range c.Offsets {
			offs[groupTP{o.Topic, o.Partition}] = o
		}
	case *pb.Op_AllocPids:
		if b.AllocPids.Next <= s.NextPID {
			return corrupt("producer id allocation %d not above %d", b.AllocPids.Next, s.NextPID)
		}
		s.NextPID = b.AllocPids.Next
	case *pb.Op_Replace:
		for _, r := range b.Replace.Replacements {
			id, _ := toTopicID(r.TopicId)
			t := s.Topics[id]
			if t == nil || int(r.Partition) >= len(t.Parts) {
				continue // topic deleted since the merge was planned
			}
			p := t.Parts[r.Partition]
			i := p.findBase(r.BaseOffset)
			if i < 0 || p.Refs[i].ObjectKey != r.OldKey {
				continue // truncated or already replaced
			}
			if _, dead := s.Tombstones[r.NewKey]; dead {
				return corrupt("replacement into tombstoned object %s", r.NewKey)
			}
			nr := proto.Clone(p.Refs[i]).(*pb.BatchRef)
			nr.ObjectKey = r.NewKey
			nr.OffsetInObj = r.NewOffsetInObj
			nr.CommitSeq = e.Seq
			p.Refs[i] = nr
			s.RefCount[r.NewKey]++
			s.unref(e, r.OldKey)
		}
	case *pb.Op_Truncate:
		for _, ls := range b.Truncate.Starts {
			id, _ := toTopicID(ls.TopicId)
			t := s.Topics[id]
			if t == nil || int(ls.Partition) >= len(t.Parts) {
				continue
			}
			p := t.Parts[ls.Partition]
			if ls.LogStart <= p.LogStart {
				continue
			}
			if ls.LogStart > p.Next {
				return corrupt("truncate %x/%d to %d beyond %d", id, ls.Partition, ls.LogStart, p.Next)
			}
			p.LogStart = ls.LogStart
			n := 0
			for n < len(p.Refs) && p.Refs[n].BaseOffset+int64(p.Refs[n].Records) <= ls.LogStart {
				s.unref(e, p.Refs[n].ObjectKey)
				n++
			}
			p.Refs = append([]*pb.BatchRef(nil), p.Refs[n:]...)
		}
	default:
		return corrupt("unknown op %T", op.Body)
	}
	return nil
}

func (s *State) applyCommit(e *pb.Entry, c *pb.CommitBatches) error {
	if _, dup := s.RecentByKey[c.ObjectKey]; dup {
		return corrupt("object %s committed twice", c.ObjectKey)
	}
	if _, dead := s.Tombstones[c.ObjectKey]; dead {
		return corrupt("commit references tombstoned object %s", c.ObjectKey)
	}
	for _, r := range c.Results {
		if r.ErrorCode != 0 || r.Duplicate {
			continue
		}
		id, _ := toTopicID(r.TopicId)
		t := s.Topics[id]
		if t == nil || int(r.Partition) >= len(t.Parts) {
			return corrupt("commit to unknown partition %x/%d", id, r.Partition)
		}
		p := t.Parts[r.Partition]
		if r.BaseOffset != p.Next || r.Records == 0 {
			return corrupt("commit %x/%d base %d, next %d", id, r.Partition, r.BaseOffset, p.Next)
		}
		p.Refs = append(p.Refs, &pb.BatchRef{
			ObjectKey:   c.ObjectKey,
			OffsetInObj: r.OffsetInObj,
			Length:      r.Length,
			Records:     r.Records,
			BaseOffset:  r.BaseOffset,
			MaxTs:       r.MaxTs,
			CommitSeq:   e.Seq,
		})
		p.Next += int64(r.Records)
		s.RefCount[c.ObjectKey]++
		if r.ProducerId >= 0 {
			k := producerKey{r.ProducerId, TP{id, r.Partition}}
			ps := s.Producers[k]
			if ps == nil || r.ProducerEpoch != ps.Epoch {
				ps = &producerState{Epoch: r.ProducerEpoch}
				s.Producers[k] = ps
			}
			ps.Recent = append(ps.Recent, &pb.SeqRange{
				FirstSeq:   r.BaseSeq,
				LastSeq:    r.BaseSeq + int32(r.Records) - 1,
				BaseOffset: r.BaseOffset,
			})
			if len(ps.Recent) > 5 {
				ps.Recent = ps.Recent[len(ps.Recent)-5:]
			}
		}
	}
	s.remember(c)
	return nil
}

func (s *State) remember(c *pb.CommitBatches) {
	s.Recent = append(s.Recent, c)
	s.RecentByKey[c.ObjectKey] = c
	if over := len(s.Recent) - s.recentLimit; over > 0 && s.recentLimit > 0 {
		for _, old := range s.Recent[:over] {
			delete(s.RecentByKey, old.ObjectKey)
		}
		s.Recent = append([]*pb.CommitBatches(nil), s.Recent[over:]...)
	}
}

func (s *State) unref(e *pb.Entry, key string) {
	s.RefCount[key]--
	if s.RefCount[key] <= 0 {
		delete(s.RefCount, key)
		s.Tombstones[key] = e.WrittenUnixMs + s.gcGraceMs
	}
}

// Verify checks internal consistency: reference counts match the refs, and
// no live object is tombstoned. The simulator calls it continuously.
func (s *State) Verify() error {
	counts := map[string]int{}
	for _, t := range s.Topics {
		for i, p := range t.Parts {
			for _, r := range p.Refs {
				counts[r.ObjectKey]++
				if at, dead := s.Tombstones[r.ObjectKey]; dead {
					return fmt.Errorf("%s/%d@%d references tombstoned object %s (delete after %d)", t.Name, i, r.BaseOffset, r.ObjectKey, at)
				}
			}
		}
	}
	for k, n := range counts {
		if s.RefCount[k] != n {
			return fmt.Errorf("object %s: refcount %d, %d refs", k, s.RefCount[k], n)
		}
	}
	for k, n := range s.RefCount {
		if counts[k] != n {
			return fmt.Errorf("object %s: refcount %d, %d refs", k, n, counts[k])
		}
	}
	return nil
}

// Partition returns the partition or nil.
func (s *State) Partition(id TopicID, p uint32) *Partition {
	t := s.Topics[id]
	if t == nil || int(p) >= len(t.Parts) {
		return nil
	}
	return t.Parts[p]
}

func (s *State) sortedTopics() []*Topic {
	ts := make([]*Topic, 0, len(s.Topics))
	for _, t := range s.Topics {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].Name < ts[j].Name })
	return ts
}

// Snapshot captures the state. withVolatile includes GC bookkeeping, which
// the state hash excludes.
func (s *State) Snapshot(withVolatile bool) *pb.Snapshot {
	snap := &pb.Snapshot{Seq: s.Seq, Epoch: s.Epoch, Leader: s.Leader, NextProducerId: s.NextPID}
	for _, t := range s.sortedTopics() {
		ts := &pb.TopicState{Id: t.ID[:], Name: t.Name, RetentionMs: t.Retention, Owner: t.Owner, CreatedUnixMs: t.CreatedMs}
		for _, p := range t.Parts {
			ts.Partitions = append(ts.Partitions, &pb.PartitionState{LogStart: p.LogStart, NextOffset: p.Next, Refs: p.Refs})
		}
		snap.Topics = append(snap.Topics, ts)
	}
	groups := make([]string, 0, len(s.Offsets))
	for g := range s.Offsets {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	for _, g := range groups {
		gs := &pb.GroupOffsets{Group: g}
		for _, o := range s.Offsets[g] {
			gs.Offsets = append(gs.Offsets, o)
		}
		sort.Slice(gs.Offsets, func(i, j int) bool {
			a, b := gs.Offsets[i], gs.Offsets[j]
			if a.Topic != b.Topic {
				return a.Topic < b.Topic
			}
			return a.Partition < b.Partition
		})
		snap.Groups = append(snap.Groups, gs)
	}
	for k, ps := range s.Producers {
		snap.Producers = append(snap.Producers, &pb.ProducerState{
			ProducerId: k.PID, Epoch: ps.Epoch, TopicId: append([]byte(nil), k.TP.Topic[:]...), Partition: k.TP.Partition, Recent: ps.Recent,
		})
	}
	sort.Slice(snap.Producers, func(i, j int) bool {
		a, b := snap.Producers[i], snap.Producers[j]
		if a.ProducerId != b.ProducerId {
			return a.ProducerId < b.ProducerId
		}
		if c := bytes.Compare(a.TopicId, b.TopicId); c != 0 {
			return c < 0
		}
		return a.Partition < b.Partition
	})
	snap.RecentCommits = s.Recent
	if withVolatile {
		for k, at := range s.Tombstones {
			snap.Tombstones = append(snap.Tombstones, &pb.Tombstone{Key: k, DeleteAfterUnixMs: at})
		}
		sort.Slice(snap.Tombstones, func(i, j int) bool { return snap.Tombstones[i].Key < snap.Tombstones[j].Key })
		snap.PriorSnapshots = s.PriorSnaps
		snap.LogGcFloor = s.LogGCFloor
	}
	return snap
}

// Hash is a digest of the deterministic state, used by invariant I6.
func (s *State) Hash() [32]byte {
	return sha256.Sum256(metalog.EncodeSnapshot(s.Snapshot(false)))
}

// StateFromSnapshot rebuilds state, deriving object reference counts.
func StateFromSnapshot(snap *pb.Snapshot, gcGraceMs int64, recentLimit int) (*State, error) {
	s := NewState(gcGraceMs, recentLimit)
	s.Seq, s.Epoch, s.Leader, s.NextPID = snap.Seq, snap.Epoch, snap.Leader, snap.NextProducerId
	for _, ts := range snap.Topics {
		id, ok := toTopicID(ts.Id)
		if !ok {
			return nil, corrupt("snapshot topic id")
		}
		t := &Topic{ID: id, Name: ts.Name, Retention: ts.RetentionMs, Owner: ts.Owner, CreatedMs: ts.CreatedUnixMs}
		for _, ps := range ts.Partitions {
			t.Parts = append(t.Parts, &Partition{LogStart: ps.LogStart, Next: ps.NextOffset, Refs: ps.Refs})
			for _, r := range ps.Refs {
				s.RefCount[r.ObjectKey]++
			}
		}
		s.Topics[id] = t
		s.ByName[t.Name] = id
	}
	for _, g := range snap.Groups {
		offs := map[groupTP]*pb.OffsetCommit{}
		for _, o := range g.Offsets {
			offs[groupTP{o.Topic, o.Partition}] = o
		}
		s.Offsets[g.Group] = offs
	}
	for _, p := range snap.Producers {
		id, _ := toTopicID(p.TopicId)
		s.Producers[producerKey{p.ProducerId, TP{id, p.Partition}}] = &producerState{Epoch: p.Epoch, Recent: p.Recent}
	}
	for _, c := range snap.RecentCommits {
		s.remember(c)
	}
	for _, t := range snap.Tombstones {
		s.Tombstones[t.Key] = t.DeleteAfterUnixMs
	}
	s.PriorSnaps = snap.PriorSnapshots
	s.LogGCFloor = snap.LogGcFloor
	return s, nil
}
