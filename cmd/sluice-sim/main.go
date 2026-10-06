// Command sluice-sim runs the deterministic simulator.
//
//	sluice-sim --seed 42 --trace        replay one seed with an event trace
//	sluice-sim --seeds 200              200 seeds in parallel (CI)
//	sluice-sim --seeds 20000 --budget 45m --determinism
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayushhh1010/sluice/internal/bugs"
	"github.com/ayushhh1010/sluice/internal/sim"
)

func main() {
	seed := flag.Uint64("seed", 0, "run exactly this seed")
	seeds := flag.Int("seeds", 0, "run this many seeds")
	start := flag.Uint64("start", 0, "first seed for --seeds (default: derived from the time)")
	duration := flag.Duration("duration", 60*time.Second, "virtual workload duration per seed")
	faults := flag.Bool("faults", true, "inject faults")
	trace := flag.Bool("trace", false, "print the event trace (single seed)")
	determinism := flag.Bool("determinism", false, "run every seed twice and compare fingerprints")
	parallel := flag.Int("parallel", runtime.NumCPU(), "seeds to run concurrently")
	budget := flag.Duration("budget", 0, "stop starting new seeds after this much wall time")
	jsonOut := flag.String("json", "", "write a JSON summary to this file")
	planted := flag.String("plant", "", "comma-separated planted bugs (needs -tags sluice_bugs)")
	flag.Parse()

	for _, b := range strings.Split(*planted, ",") {
		if b = strings.TrimSpace(b); b != "" {
			bugs.Set(b, true)
		}
	}
	cfg := sim.Config{DurationMs: duration.Milliseconds(), Faults: *faults}

	if *seeds == 0 {
		cfg.Seed = *seed
		if *trace {
			cfg.Trace = os.Stdout
		}
		res := sim.Run(cfg)
		report(res, cfg)
		if *determinism {
			cfg.Trace = nil
			again := sim.Run(cfg)
			if again.Fingerprint != res.Fingerprint {
				fmt.Printf("NONDETERMINISTIC: seed %d fingerprints %s vs %s\n", cfg.Seed, res.Fingerprint, again.Fingerprint)
				os.Exit(1)
			}
		}
		if len(res.Violations) > 0 {
			os.Exit(1)
		}
		return
	}

	first := *start
	if first == 0 {
		first = uint64(time.Now().UnixNano())
	}
	began := time.Now()
	var (
		next     atomic.Int64
		ops      atomic.Int64
		ran      atomic.Int64
		mu       sync.Mutex
		failures []sim.Result
		nondet   []uint64
		wg       sync.WaitGroup
	)
	for w := 0; w < *parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(*seeds) || (*budget > 0 && time.Since(began) > *budget) {
					return
				}
				c := cfg
				c.Seed = first + uint64(i)
				res := sim.Run(c)
				ops.Add(int64(res.Ops))
				ran.Add(1)
				if *determinism {
					if again := sim.Run(c); again.Fingerprint != res.Fingerprint {
						mu.Lock()
						nondet = append(nondet, c.Seed)
						mu.Unlock()
					}
				}
				if len(res.Violations) > 0 {
					mu.Lock()
					failures = append(failures, res)
					mu.Unlock()
					report(res, c)
				}
			}
		}()
	}
	wg.Wait()
	sort.Slice(failures, func(i, j int) bool { return failures[i].Seed < failures[j].Seed })
	elapsed := time.Since(began).Round(time.Second)
	fmt.Printf("\n%d seeds (%d..%d), %d client operations, %d failing, %d nondeterministic, %s\n",
		ran.Load(), first, first+uint64(ran.Load())-1, ops.Load(), len(failures), len(nondet), elapsed)
	for _, s := range nondet {
		fmt.Printf("NONDETERMINISTIC seed %d: sluice-sim --seed %d --determinism\n", s, s)
	}
	if *jsonOut != "" {
		var failing []uint64
		for _, f := range failures {
			failing = append(failing, f.Seed)
		}
		b, _ := json.MarshalIndent(map[string]any{
			"seeds": ran.Load(), "first_seed": first, "operations": ops.Load(), "failing_seeds": failing,
			"nondeterministic_seeds": nondet, "elapsed_seconds": elapsed.Seconds(),
		}, "", "  ")
		_ = os.WriteFile(*jsonOut, b, 0o600)
	}
	if len(failures) > 0 || len(nondet) > 0 {
		os.Exit(1)
	}
}

func report(res sim.Result, cfg sim.Config) {
	if len(res.Violations) == 0 {
		if cfg.Trace != nil || flag.Lookup("seeds").Value.String() == "0" {
			fmt.Printf("seed %d OK: %d ops, fingerprint %s, stats %v\n", res.Seed, res.Ops, res.Fingerprint, res.Stats)
		}
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\nFAIL seed %d (%d ops)\n", res.Seed, res.Ops)
	for _, v := range res.Violations {
		fmt.Fprintf(&b, "  %s\n", v)
	}
	fmt.Fprintf(&b, "  knobs: %+v\n", res.Knobs)
	fmt.Fprintf(&b, "  replay: go run ./cmd/sluice-sim --seed %d --duration %s --trace\n", res.Seed, time.Duration(cfg.DurationMs)*time.Millisecond)
	fmt.Print(b.String())
}
