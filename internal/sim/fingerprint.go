package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/ayushhh1010/sluice/internal/checker"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

func ctxBackground() context.Context { return context.Background() }

// fingerprint digests everything a run observed. Two runs of one seed must
// produce the same fingerprint; CI replays seeds to prove it.
func fingerprint(h *checker.History, st *sequencer.State, stats map[string]int) string {
	d := sha256.New()
	fmt.Fprintf(d, "%v|%v|%v|%v|%v", h.Produces, h.Fetches, h.Commits, h.OffsetFetches, h.Syncs)
	if st != nil {
		hs := st.Hash()
		d.Write(hs[:])
	}
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(d, "%s=%d;", k, stats[k])
	}
	return hex.EncodeToString(d.Sum(nil))[:16]
}
