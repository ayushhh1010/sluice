//go:build sluice_bugs

package sim

import (
	"testing"

	"github.com/ayushhh1010/sluice/internal/bugs"
)

// TestCheckerCatchesPlantedBugs proves the checker is not vacuous: every
// planted bug must be reported within a handful of seeds.
func TestCheckerCatchesPlantedBugs(t *testing.T) {
	for _, bug := range []string{"ack-wrong-offset", "drop-acked-batch", "ack-before-commit", "ignore-fence"} {
		t.Run(bug, func(t *testing.T) {
			bugs.Set(bug, true)
			defer bugs.Set(bug, false)
			for seed := uint64(1); seed <= 60; seed++ {
				if res := Run(Config{Seed: seed, DurationMs: 30_000, Faults: true}); len(res.Violations) > 0 {
					t.Logf("caught at seed %d: %s", seed, res.Violations[0])
					return
				}
			}
			t.Fatalf("planted bug %q went undetected for 60 seeds", bug)
		})
	}
}
