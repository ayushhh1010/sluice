package placement

import "testing"

func TestRendezvousIsBalancedAndMinimallyDisruptive(t *testing.T) {
	nodes := []int32{1, 2, 3}
	counts := map[int32]int{}
	owner := map[uint32]int32{}
	for p := uint32(0); p < 3000; p++ {
		o := Owner(nodes, [16]byte{7}, p)
		counts[o]++
		owner[p] = o
	}
	for n, c := range counts {
		if c < 800 || c > 1200 {
			t.Fatalf("node %d owns %d of 3000 partitions", n, c)
		}
	}
	// Removing a node moves only the partitions it owned.
	for p := uint32(0); p < 3000; p++ {
		o := Owner([]int32{1, 2}, [16]byte{7}, p)
		if owner[p] != 3 && o != owner[p] {
			t.Fatalf("partition %d moved from %d to %d", p, owner[p], o)
		}
	}
	if Owner(nil, [16]byte{}, 0) != -1 {
		t.Fatal("no nodes must mean no owner")
	}
}
