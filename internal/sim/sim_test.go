package sim

import (
	"testing"
)

func TestSeedsHoldInvariants(t *testing.T) {
	n := 40
	if testing.Short() {
		n = 10
	}
	for seed := uint64(1); seed <= uint64(n); seed++ {
		res := Run(Config{Seed: seed, DurationMs: 30_000, Faults: true})
		for _, v := range res.Violations {
			t.Errorf("seed %d: %s (replay: go run ./cmd/sluice-sim --seed %d --duration 30s --trace)", seed, v, seed)
		}
		if res.Ops == 0 {
			t.Errorf("seed %d: workload made no progress", seed)
		}
	}
}

func TestDeterministicReplay(t *testing.T) {
	for _, seed := range []uint64{7, 99, 12345} {
		a := Run(Config{Seed: seed, DurationMs: 20_000, Faults: true})
		b := Run(Config{Seed: seed, DurationMs: 20_000, Faults: true})
		if a.Fingerprint != b.Fingerprint {
			t.Fatalf("seed %d is not deterministic: %s vs %s", seed, a.Fingerprint, b.Fingerprint)
		}
	}
}

func TestFaultFreeRunIsClean(t *testing.T) {
	res := Run(Config{Seed: 1, DurationMs: 20_000, NoBuggify: true})
	if len(res.Violations) > 0 || res.Ops < 100 {
		t.Fatalf("violations %v, ops %d", res.Violations, res.Ops)
	}
}
