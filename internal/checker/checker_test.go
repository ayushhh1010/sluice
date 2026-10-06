package checker

import (
	"testing"
)

func final(vals ...string) FinalLog {
	pl := &PartLog{HW: int64(len(vals))}
	for i, v := range vals {
		pl.Records = append(pl.Records, Rec{int64(i), v})
	}
	return FinalLog{{"t", 0}: pl}
}

var p0 = Partition{"t", 0}

func has(vs []Violation, inv string) bool {
	for _, v := range vs {
		if v.Invariant == inv {
			return true
		}
	}
	return false
}

func TestCleanHistoryPasses(t *testing.T) {
	h := &History{
		Produces: []ProduceOp{{Part: p0, Values: []string{"a", "b"}, Attempts: 1, Acked: true, BaseOffset: 0}},
		Fetches:  []FetchOp{{Part: p0, Offset: 1, Records: []Rec{{0, "a"}, {1, "b"}}}},
	}
	if vs := Check(h, final("a", "b")); len(vs) != 0 {
		t.Fatal(vs)
	}
}

// Each planted bug must be reported by the right invariant.
func TestCheckerCatchesPlantedViolations(t *testing.T) {
	cases := []struct {
		name, inv string
		h         *History
		f         FinalLog
	}{
		{"lost acked record", "I1", &History{Produces: []ProduceOp{{Part: p0, Values: []string{"a"}, Attempts: 1, Acked: true}}}, final("x")},
		{"wrong offset", "I1", &History{Produces: []ProduceOp{{Part: p0, Values: []string{"a"}, Attempts: 1, Acked: true, BaseOffset: 1}}}, final("a", "b")},
		{"duplicate without retry", "I2", &History{Produces: []ProduceOp{{Part: p0, Values: []string{"a"}, Attempts: 1, Acked: true}}}, final("a", "a")},
		{"idempotent duplicate", "I2", &History{Produces: []ProduceOp{{Part: p0, Idempotent: true, Values: []string{"a"}, Attempts: 3, Acked: true}}}, final("a", "a")},
		{"phantom record", "I2", &History{}, final("ghost")},
		{"fetch disagrees with log", "I4", &History{Produces: []ProduceOp{{Part: p0, Values: []string{"a"}, Attempts: 1}}, Fetches: []FetchOp{{Part: p0, Records: []Rec{{0, "zzz"}}}}}, final("a")},
		{"fetch skips offsets", "I4", &History{Produces: []ProduceOp{{Part: p0, Values: []string{"a", "b"}, Attempts: 1}}, Fetches: []FetchOp{{Part: p0, Offset: 0, Records: []Rec{{1, "b"}}}}}, final("a", "b")},
		{"stale offset fetch", "I5", &History{
			Commits:       []CommitOp{{Group: "g", Part: p0, Offset: 5, Acked: true, Start: 0, End: 1}, {Group: "g", Part: p0, Offset: 6, Acked: true, Start: 2, End: 3}},
			OffsetFetches: []OffsetFetchOp{{Group: "g", Part: p0, Offset: 5, Start: 4, End: 5}},
		}, final()},
		{"lost offset commit", "I5", &History{
			Commits:       []CommitOp{{Group: "g", Part: p0, Offset: 5, Acked: true, Start: 0, End: 1}},
			OffsetFetches: []OffsetFetchOp{{Group: "g", Part: p0, Offset: -1, Start: 2, End: 3}},
		}, final()},
		{"double assignment", "G1", &History{Syncs: []SyncOp{
			{Group: "g", Generation: 1, Member: "m1", Assigned: []Partition{p0}},
			{Group: "g", Generation: 1, Member: "m2", Assigned: []Partition{p0}},
		}}, final()},
	}
	for _, c := range cases {
		if vs := Check(c.h, c.f); !has(vs, c.inv) {
			t.Errorf("%s: want %s violation, got %v", c.name, c.inv, vs)
		}
	}
}

func TestUncertainOutcomesAreAllowed(t *testing.T) {
	// A retried, unacked send may appear twice; an unacked commit may win.
	h := &History{
		Produces: []ProduceOp{{Part: p0, Values: []string{"a"}, Attempts: 2}},
		Commits: []CommitOp{
			{Group: "g", Part: p0, Offset: 5, Acked: false, Start: 0, End: 1},
			{Group: "g", Part: p0, Offset: 6, Acked: true, Start: 2, End: 3},
		},
		OffsetFetches: []OffsetFetchOp{{Group: "g", Part: p0, Offset: 5, Start: 4, End: 5}},
	}
	if vs := Check(h, final("a", "a")); len(vs) != 0 {
		t.Fatalf("false positives: %v", vs)
	}
}
