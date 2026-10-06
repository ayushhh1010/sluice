// Command sluice-check verifies Sluice's invariants offline, straight from
// the bucket: I3 (dense offsets), I6 (deterministic replay) and I7 (no
// dangling refs). Given a client history recorded by sluice-chaos it also
// checks I1, I2, I4 and I5 against the final log.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ayushhh1010/sluice/internal/checker"
	"github.com/ayushhh1010/sluice/internal/objstore"
)

func main() {
	store := flag.String("store", os.Getenv("SLUICE_STORE"), "object store URL")
	cluster := flag.String("cluster", "prod", "cluster name")
	history := flag.String("history", "", "JSON history written by sluice-chaos")
	recent := flag.Int("recent-commits", 8192, "sequencer RecentCommits setting (part of the state hash)")
	flag.Parse()
	if *store == "" {
		fmt.Fprintln(os.Stderr, "--store is required")
		os.Exit(2)
	}
	ctx := context.Background()
	st, err := objstore.Open(ctx, *store)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	state, vs, err := checker.CheckBucket(ctx, st, *cluster, 0, *recent)
	if err != nil {
		fmt.Fprintln(os.Stderr, "recovery failed:", err)
		os.Exit(1)
	}
	fmt.Printf("metadata log: seq %d, epoch %d, leader %s, %d topics\n", state.Seq, state.Epoch, state.Leader, len(state.Topics))
	if *history != "" {
		b, err := os.ReadFile(*history)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var h checker.History
		if err := json.Unmarshal(b, &h); err != nil {
			fmt.Fprintln(os.Stderr, "bad history:", err)
			os.Exit(1)
		}
		final, err := checker.ReadFinal(ctx, st, state)
		if err != nil {
			vs = append(vs, checker.Violation{Invariant: "I7", Detail: err.Error()})
		} else {
			vs = append(vs, checker.Check(&h, final)...)
		}
		fmt.Printf("history: %d sends, %d fetches, %d offset commits\n", len(h.Produces), len(h.Fetches), len(h.Commits))
	}
	if len(vs) == 0 {
		fmt.Println("OK: no invariant violations")
		return
	}
	for _, v := range vs {
		fmt.Println("VIOLATION", v)
	}
	os.Exit(1)
}
