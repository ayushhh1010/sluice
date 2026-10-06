// Command sluice-bench measures produce latency, end-to-end latency,
// throughput and object-store requests per GB.
//
// Local mode (default) runs a fresh in-process cluster per flush window on
// a store that injects latency, so the latency-vs-cost curve is
// reproducible on any machine:
//
//	sluice-bench -flush-windows 50ms,100ms,250ms,500ms,1s -store-latency 30ms -store-jitter 40ms
//
// Remote mode benchmarks an existing cluster (no request counts):
//
//	sluice-bench -bootstrap localhost:9092 -duration 60s
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/node"
	"github.com/ayushhh1010/sluice/internal/objstore"
)

var linger *time.Duration

type result struct {
	FlushWindow    string  `json:"flush_window"`
	Records        int64   `json:"records"`
	MBps           float64 `json:"mb_per_s"`
	ProduceP50ms   float64 `json:"produce_p50_ms"`
	ProduceP99ms   float64 `json:"produce_p99_ms"`
	ProduceP999ms  float64 `json:"produce_p999_ms"`
	E2EP50ms       float64 `json:"e2e_p50_ms"`
	E2EP99ms       float64 `json:"e2e_p99_ms"`
	PutsPerGB      float64 `json:"puts_per_gb,omitempty"`
	PutsPerSec     float64 `json:"puts_per_s,omitempty"`
	ClassAUSDPerGB float64 `json:"class_a_usd_per_gb,omitempty"`
}

func main() {
	bootstrap := flag.String("bootstrap", "", "benchmark an existing cluster instead of a local one")
	windows := flag.String("flush-windows", "250ms", "comma-separated flush windows (local mode)")
	duration := flag.Duration("duration", 30*time.Second, "measured duration per run")
	warmup := flag.Duration("warmup", 5*time.Second, "warmup before measuring")
	rate := flag.Float64("rate", 2, "target MB/s (0 = as fast as possible)")
	size := flag.Int("record-size", 1024, "record value size in bytes")
	partitions := flag.Int("partitions", 6, "partitions")
	agents := flag.Int("agents", 2, "agents (local mode)")
	latency := flag.Duration("store-latency", 30*time.Millisecond, "injected base object store latency (local mode)")
	jitter := flag.Duration("store-jitter", 40*time.Millisecond, "injected uniform jitter (local mode)")
	jsonOut := flag.String("json", "", "write results as JSON")
	linger = flag.Duration("linger", 50*time.Millisecond, "producer linger (diskless brokers favour fewer, larger requests)")
	flag.Parse()

	var results []result
	if *bootstrap != "" {
		r := run(strings.Split(*bootstrap, ","), nil, "n/a", *duration, *warmup, *rate, *size, *partitions)
		results = append(results, r)
	} else {
		for _, w := range strings.Split(*windows, ",") {
			fw, err := time.ParseDuration(strings.TrimSpace(w))
			if err != nil {
				fmt.Fprintln(os.Stderr, "bad flush window:", w)
				os.Exit(2)
			}
			st := &objstore.Instrumented{Store: objstore.NewMem(), Base: *latency, Jitter: *jitter}
			ctx, cancel := context.WithCancel(context.Background())
			d := &node.DevCluster{Store: st, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
				TuneAgent: func(c *agent.Config) { c.FlushWindowMs = fw.Milliseconds() }}
			if err := d.Start(ctx, 2, *agents); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			results = append(results, run(d.Bootstrap(), st, fw.String(), *duration, *warmup, *rate, *size, *partitions))
			d.Stop()
			cancel()
		}
	}
	fmt.Printf("\n| flush window | MB/s | produce p50 | p99 | p99.9 | e2e p50 | e2e p99 | PUTs/s | PUTs per GB | Class A $/GB |\n|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		fmt.Printf("| %s | %.2f | %.0f ms | %.0f ms | %.0f ms | %.0f ms | %.0f ms | %.1f | %.0f | $%.4f |\n",
			r.FlushWindow, r.MBps, r.ProduceP50ms, r.ProduceP99ms, r.ProduceP999ms, r.E2EP50ms, r.E2EP99ms, r.PutsPerSec, r.PutsPerGB, r.ClassAUSDPerGB)
	}
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(map[string]any{
			"config": map[string]any{"duration": duration.String(), "rate_mb_s": *rate, "record_size": *size, "partitions": *partitions,
				"agents": *agents, "store_latency": latency.String(), "store_jitter": jitter.String(), "bootstrap": *bootstrap},
			"results": results,
		}, "", "  ")
		_ = os.WriteFile(*jsonOut, b, 0o600)
	}
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	slices.Sort(xs)
	return xs[min(len(xs)-1, int(float64(len(xs))*p))]
}

func run(seeds []string, st *objstore.Instrumented, label string, dur, warm time.Duration, rate float64, size, parts int) result {
	ctx := context.Background()
	topic := fmt.Sprintf("bench-%d", rand.Int64())
	admin, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	must(err)
	for i := 0; ; i++ {
		r, err := kadm.NewClient(admin).CreateTopic(ctx, int32(parts), 1, nil, topic)
		if err == nil && r.Err == nil {
			break
		}
		if i > 100 {
			must(fmt.Errorf("create topic: %w", errors.Join(err, r.Err)))
		}
		time.Sleep(200 * time.Millisecond)
	}
	admin.Close()

	prod, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ProducerLinger(*linger),
		kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.MaxBufferedRecords(1<<20))
	must(err)
	defer prod.Close()
	cons, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), kgo.FetchMaxWait(time.Second))
	must(err)
	defer cons.Close()

	var (
		mu        sync.Mutex
		prodLat   []float64
		e2eLat    []float64
		measuring atomic.Bool
		records   atomic.Int64
		stop      = make(chan struct{})
	)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			fs := cons.PollFetches(ctx)
			now := time.Now()
			fs.EachRecord(func(r *kgo.Record) {
				if measuring.Load() {
					mu.Lock()
					e2eLat = append(e2eLat, float64(now.Sub(r.Timestamp).Microseconds())/1000)
					mu.Unlock()
				}
			})
		}
	}()
	time.Sleep(500 * time.Millisecond) // let the consumer start at the end

	val := make([]byte, size)
	for i := range val {
		val[i] = byte(rand.IntN(256))
	}
	interval := time.Duration(0)
	if rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(size) / (rate * 1e6))
	}
	start := time.Now()
	measureAt, end := start.Add(warm), start.Add(warm+dur)
	var putsAt int64
	next := start
	for time.Now().Before(end) {
		if !measuring.Load() && time.Now().After(measureAt) {
			measuring.Store(true)
			if st != nil {
				putsAt = st.ClassA.Load()
			}
		}
		sent := time.Now()
		m := measuring.Load()
		prod.Produce(ctx, &kgo.Record{Topic: topic, Value: val, Timestamp: sent}, func(_ *kgo.Record, err error) {
			if err != nil || !m {
				return
			}
			records.Add(1)
			mu.Lock()
			prodLat = append(prodLat, float64(time.Since(sent).Microseconds())/1000)
			mu.Unlock()
		})
		if interval > 0 {
			next = next.Add(interval)
			if d := time.Until(next); d > 0 {
				time.Sleep(d)
			}
		}
	}
	_ = prod.Flush(ctx)
	time.Sleep(2 * time.Second) // let the tail consumer catch up
	close(stop)

	mu.Lock()
	defer mu.Unlock()
	n := records.Load()
	gb := float64(n) * float64(size) / 1e9
	r := result{
		FlushWindow: label, Records: n, MBps: float64(n) * float64(size) / 1e6 / dur.Seconds(),
		ProduceP50ms: pct(prodLat, 0.5), ProduceP99ms: pct(prodLat, 0.99), ProduceP999ms: pct(prodLat, 0.999),
		E2EP50ms: pct(e2eLat, 0.5), E2EP99ms: pct(e2eLat, 0.99),
	}
	if st != nil && gb > 0 {
		puts := float64(st.ClassA.Load() - putsAt)
		r.PutsPerGB = puts / gb
		r.PutsPerSec = puts / dur.Seconds()
		r.ClassAUSDPerGB = r.PutsPerGB * 4.50 / 1e6
	}
	fmt.Fprintf(os.Stderr, "flush %s: %d records, produce p99 %.0f ms, %.1f PUTs/s\n", label, n, r.ProduceP99ms, r.PutsPerSec)
	return r
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
