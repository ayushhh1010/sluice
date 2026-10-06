package sim

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/ayushhh1010/sluice/internal/checker"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

func diffStates(a, b *sequencer.State) []string {
	var out []string
	if a.Seq != b.Seq || a.Epoch != b.Epoch || a.Leader != b.Leader || a.NextPID != b.NextPID {
		out = append(out, fmt.Sprintf("header %d/%d/%s/%d vs %d/%d/%s/%d", a.Seq, a.Epoch, a.Leader, a.NextPID, b.Seq, b.Epoch, b.Leader, b.NextPID))
	}
	for id, ta := range a.Topics {
		tb := b.Topics[id]
		if tb == nil {
			out = append(out, "topic missing in b: "+ta.Name)
			continue
		}
		for i := range ta.Parts {
			pa, pb := ta.Parts[i], tb.Parts[i]
			if pa.LogStart != pb.LogStart || pa.Next != pb.Next || len(pa.Refs) != len(pb.Refs) {
				out = append(out, fmt.Sprintf("%s/%d: start %d/%d next %d/%d refs %d/%d", ta.Name, i, pa.LogStart, pb.LogStart, pa.Next, pb.Next, len(pa.Refs), len(pb.Refs)))
				continue
			}
			for j := range pa.Refs {
				if pa.Refs[j].String() != pb.Refs[j].String() {
					out = append(out, fmt.Sprintf("%s/%d ref %d: %v vs %v", ta.Name, i, j, pa.Refs[j], pb.Refs[j]))
					break
				}
			}
		}
	}
	if len(a.Producers) != len(b.Producers) {
		out = append(out, fmt.Sprintf("producers %d vs %d", len(a.Producers), len(b.Producers)))
	}
	if len(a.Recent) != len(b.Recent) {
		out = append(out, fmt.Sprintf("recent %d vs %d", len(a.Recent), len(b.Recent)))
	} else {
		for i := range a.Recent {
			if a.Recent[i].ObjectKey != b.Recent[i].ObjectKey {
				out = append(out, fmt.Sprintf("recent[%d] %s vs %s", i, a.Recent[i].ObjectKey, b.Recent[i].ObjectKey))
				break
			}
		}
	}
	if len(a.Offsets) != len(b.Offsets) {
		out = append(out, fmt.Sprintf("groups %d vs %d", len(a.Offsets), len(b.Offsets)))
	}
	return out
}

// TestDebugSeed: SLUICE_SEED=78 go test ./internal/sim -run TestDebugSeed -v
func TestDebugSeed(t *testing.T) {
	s := os.Getenv("SLUICE_SEED")
	if s == "" {
		t.Skip("set SLUICE_SEED")
	}
	seed, _ := strconv.ParseUint(s, 10, 64)
	var w *World
	captured = func(x *World) { w = x }
	res := Run(Config{Seed: seed, DurationMs: 60_000, Faults: true})
	for _, v := range res.Violations {
		t.Log(v)
	}
	ctx := context.Background()
	snaps, _ := checker.Snapshots(ctx, w.store, "sim")
	t.Log("snapshots", snaps)
	a, err := checker.Replay(ctx, w.store, "sim", snaps[0], w.knobs.GCGraceMs, w.knobs.RecentCommits)
	if err != nil {
		t.Fatal(err)
	}
	b, err := checker.Replay(ctx, w.store, "sim", 0, w.knobs.GCGraceMs, w.knobs.RecentCommits)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range diffStates(a, b) {
		t.Log("DIFF", d)
	}
}

func TestDebugPartition(t *testing.T) {
	s := os.Getenv("SLUICE_SEED")
	if s == "" {
		t.Skip("set SLUICE_SEED")
	}
	seed, _ := strconv.ParseUint(s, 10, 64)
	var w *World
	captured = func(x *World) { w = x }
	Run(Config{Seed: seed, DurationMs: 60_000, Faults: true})
	ctx := context.Background()
	b, err := checker.Replay(ctx, w.store, "sim", 0, w.knobs.GCGraceMs, w.knobs.RecentCommits)
	if err != nil {
		t.Fatal(err)
	}
	topic, part := os.Getenv("SLUICE_TOPIC"), os.Getenv("SLUICE_PART")
	pn, _ := strconv.Atoi(part)
	for _, tp := range b.Topics {
		if tp.Name != topic {
			continue
		}
		for _, r := range tp.Parts[pn].Refs {
			t.Logf("base=%d n=%d seq=%d key=%s", r.BaseOffset, r.Records, r.CommitSeq, r.ObjectKey)
		}
	}
}

func TestDebugLeader(t *testing.T) {
	s := os.Getenv("SLUICE_SEED")
	if s == "" {
		t.Skip("set SLUICE_SEED")
	}
	seed, _ := strconv.ParseUint(s, 10, 64)
	var w *World
	captured = func(x *World) { w = x }
	Run(Config{Seed: seed, DurationMs: 60_000, Faults: true})
	ctx := context.Background()
	b, err := checker.Replay(ctx, w.store, "sim", 0, w.knobs.GCGraceMs, w.knobs.RecentCommits)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range w.seqs {
		if n.seq == nil || n.seq.State() == nil {
			continue
		}
		st := n.seq.State()
		t.Logf("%s role=%s seq=%d replay seq=%d verify=%v", n.name, n.seq.Role(), st.Seq, b.Seq, st.Verify())
		for _, d := range diffStates(st, b) {
			t.Log("DIFF", d)
		}
	}
	key := os.Getenv("SLUICE_KEY")
	for _, tp := range b.Topics {
		for i, p := range tp.Parts {
			for _, r := range p.Refs {
				if key != "" && r.ObjectKey == key {
					t.Logf("replay ref %s/%d@%d seq=%d", tp.Name, i, r.BaseOffset, r.CommitSeq)
				}
			}
		}
	}
}
