package metalog

import (
	"sort"
	"testing"
)

func TestULIDSortsByTimeAndRoundTrips(t *testing.T) {
	var ids []string
	for i := int64(0); i < 100; i++ {
		ms := 1_790_000_000_000 + i*37
		u := ULID(ms, uint64(i)*0x9e3779b97f4a7c15, ^uint64(i))
		if len(u) != 26 {
			t.Fatalf("length %d", len(u))
		}
		if got, ok := ULIDTime(u); !ok || got != ms {
			t.Fatalf("time %d, want %d", got, ms)
		}
		ids = append(ids, u)
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatal("ULIDs not in time order")
	}
}

func TestKeysSortNumerically(t *testing.T) {
	k := NewKeys("c")
	if k.Log(9) >= k.Log(10) || k.Log(99999) >= k.Log(100000) {
		t.Fatal("zero padding broken: lexical order must equal numeric order")
	}
	if s, ok := SeqFromKey(k.Snap(1234)); !ok || s != 1234 {
		t.Fatalf("SeqFromKey: %d %v", s, ok)
	}
}
