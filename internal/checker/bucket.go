package checker

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/ayushhh1010/sluice/internal/kbatch"
	"github.com/ayushhh1010/sluice/internal/metalog"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

// Replay rebuilds sequencer state from a bucket the way a recovering
// sequencer does: from snapshot snapSeq (0 = the newest; use FromGenesis
// for none) forward through the log until the first missing entry.
func Replay(ctx context.Context, st objstore.Store, cluster string, snapSeq uint64, gcGraceMs int64, recentLimit int) (*sequencer.State, error) {
	keys := metalog.NewKeys(cluster)
	var s *sequencer.State
	if snapSeq == 0 {
		snaps, err := Snapshots(ctx, st, cluster)
		if err != nil {
			return nil, err
		}
		if len(snaps) > 0 {
			snapSeq = snaps[len(snaps)-1]
		}
	}
	if snapSeq == 0 || snapSeq == FromGenesis {
		s = sequencer.NewState(gcGraceMs, recentLimit)
	} else {
		b, err := st.Get(ctx, keys.Snap(snapSeq), 0, 0)
		if err != nil {
			return nil, fmt.Errorf("snapshot %d: %w", snapSeq, err)
		}
		snap, err := metalog.DecodeSnapshot(b)
		if err != nil {
			return nil, err
		}
		if s, err = sequencer.StateFromSnapshot(snap, gcGraceMs, recentLimit); err != nil {
			return nil, err
		}
	}
	for {
		b, err := st.Get(ctx, keys.Log(s.Seq+1), 0, 0)
		if errors.Is(err, objstore.ErrNotFound) {
			return s, nil
		}
		if err != nil {
			return nil, err
		}
		e, err := metalog.DecodeEntry(b)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", s.Seq+1, err)
		}
		if err := s.Apply(e); err != nil {
			return nil, err
		}
	}
}

// FromGenesis replays from an empty state.
const FromGenesis = ^uint64(0)

// Snapshots lists retained snapshot sequence numbers, ascending.
func Snapshots(ctx context.Context, st objstore.Store, cluster string) ([]uint64, error) {
	list, err := st.List(ctx, metalog.NewKeys(cluster).SnapPrefix(), "", 0)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, o := range list {
		if s, ok := metalog.SeqFromKey(o.Key); ok {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// ReadFinal reads every record of every partition. Compressed batches are
// recorded as one placeholder per offset.
func ReadFinal(ctx context.Context, st objstore.Store, s *sequencer.State) (FinalLog, error) {
	final := FinalLog{}
	objs := map[string][]byte{}
	for _, t := range s.Topics {
		for i, p := range t.Parts {
			pl := &PartLog{LogStart: p.LogStart, HW: p.Next}
			final[Partition{t.Name, int32(i)}] = pl
			for _, r := range p.Refs {
				obj, ok := objs[r.ObjectKey]
				if !ok {
					b, err := st.Get(ctx, r.ObjectKey, 0, 0)
					if err != nil {
						return nil, fmt.Errorf("read %s: %w", r.ObjectKey, err)
					}
					obj, objs[r.ObjectKey] = b, b
				}
				if int(r.OffsetInObj+r.Length) > len(obj) {
					return nil, fmt.Errorf("ref %s@%d beyond object end", r.ObjectKey, r.OffsetInObj)
				}
				batch := obj[r.OffsetInObj : r.OffsetInObj+r.Length]
				recs, _, ok := kbatch.Records(batch)
				for j := int64(0); j < int64(r.Records); j++ {
					off := r.BaseOffset + j
					v := fmt.Sprintf("<compressed %s@%d>", r.ObjectKey, off)
					if ok && j < int64(len(recs)) {
						v = string(recs[j].Value)
					}
					if off >= p.LogStart {
						pl.Records = append(pl.Records, Rec{off, v})
					}
				}
			}
		}
	}
	return final, nil
}

// CheckBucket verifies I3, I6 and I7 and returns the recovered state.
func CheckBucket(ctx context.Context, st objstore.Store, cluster string, gcGraceMs int64, recentLimit int) (*sequencer.State, []Violation, error) {
	var r report
	newest, err := Replay(ctx, st, cluster, 0, gcGraceMs, recentLimit)
	if err != nil {
		return nil, nil, err
	}
	// I6: replaying from the oldest retained snapshot (or from genesis when
	// the log is complete) twice must reach the same state as recovering
	// from the newest snapshot.
	snaps, err := Snapshots(ctx, st, cluster)
	if err != nil {
		return nil, nil, err
	}
	// The oldest starting point with an unbroken log after it: genesis if
	// entry 1 survives, else the oldest snapshot whose next entry exists
	// (deleted entries always form a prefix).
	keys := metalog.NewKeys(cluster)
	from := FromGenesis
	if _, err := st.Head(ctx, keys.Log(1)); err != nil && len(snaps) > 0 {
		from = snaps[len(snaps)-1]
		for _, s := range snaps {
			if _, err := st.Head(ctx, keys.Log(s+1)); err == nil {
				from = s
				break
			}
		}
	}
	a, err := Replay(ctx, st, cluster, from, gcGraceMs, recentLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("replay from %d: %w", from, err)
	}
	b, err := Replay(ctx, st, cluster, from, gcGraceMs, recentLimit)
	if err != nil {
		return nil, nil, err
	}
	if a.Hash() != b.Hash() {
		r.add("I6", "two replays from snapshot %d differ", from)
	}
	if a.Hash() != newest.Hash() {
		r.add("I6", "replay from snapshot %d (seq %d) differs from recovery via the newest snapshot (seq %d)", from, a.Seq, newest.Seq)
	}
	// I3: dense offsets.
	for _, t := range newest.Topics {
		for i, p := range t.Parts {
			next := p.LogStart
			if len(p.Refs) > 0 && p.Refs[0].BaseOffset > p.LogStart {
				r.add("I3", "%s/%d: first batch at %d, log start %d", t.Name, i, p.Refs[0].BaseOffset, p.LogStart)
			}
			if len(p.Refs) > 0 {
				next = p.Refs[0].BaseOffset
			}
			for _, ref := range p.Refs {
				if ref.BaseOffset != next {
					r.add("I3", "%s/%d: batch at %d, expected %d", t.Name, i, ref.BaseOffset, next)
				}
				next = ref.BaseOffset + int64(ref.Records)
			}
			if next != p.Next {
				r.add("I3", "%s/%d: batches end at %d, high watermark %d", t.Name, i, next, p.Next)
			}
		}
	}
	// I7: no dangling refs.
	checked := map[string]bool{}
	for _, t := range newest.Topics {
		for i, p := range t.Parts {
			for _, ref := range p.Refs {
				if checked[ref.ObjectKey] {
					continue
				}
				checked[ref.ObjectKey] = true
				if _, err := st.Head(ctx, ref.ObjectKey); errors.Is(err, objstore.ErrNotFound) {
					r.add("I7", "%s/%d@%d references deleted object %s", t.Name, i, ref.BaseOffset, ref.ObjectKey)
				}
			}
		}
	}
	return newest, r.vs, nil
}
