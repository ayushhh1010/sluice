// Command sluice-chaos runs a live chaos test against a real cluster: a
// workload of idempotent producers (acks=all) and tail consumers runs while
// agents are killed every few minutes (and the active sequencer once), then
// the recorded history is checked against the final log with the same
// checker the simulator uses.
//
//	sluice-chaos -b localhost:9092 -duration 30m \
//	  -kill 'docker restart sluice-agent-{n}-1' -agents 2 \
//	  -kill-sequencer 'docker restart sluice-sequencer-a-1'
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/ayushhh1010/sluice/internal/checker"
	"github.com/ayushhh1010/sluice/internal/objstore"
)

func main() {
	bootstrap := flag.String("b", "localhost:9092", "bootstrap brokers")
	user := flag.String("user", "", "SASL/PLAIN user")
	pass := flag.String("pass", "", "SASL/PLAIN password")
	useTLS := flag.Bool("tls", false, "use TLS")
	duration := flag.Duration("duration", 30*time.Minute, "chaos duration")
	killCmd := flag.String("kill", "", "shell command that kills agent {n}; empty: no agent kills")
	agents := flag.Int("agents", 2, "number of agents ({n} ranges over 1..agents)")
	killMin := flag.Duration("kill-min", 2*time.Minute, "minimum time between kills")
	killMax := flag.Duration("kill-max", 5*time.Minute, "maximum time between kills")
	seqCmd := flag.String("kill-sequencer", "", "shell command that kills the active sequencer, run once mid-test")
	producers := flag.Int("producers", 4, "producer clients")
	partitions := flag.Int("partitions", 6, "partitions")
	rate := flag.Int("rate", 200, "records per second per producer")
	historyOut := flag.String("history", "chaos-history.json", "write the history here")
	resultOut := flag.String("result", "chaos-result.json", "write the summary here")
	store := flag.String("store", "", "optional object store URL: also check I3, I6 and I7 in the bucket")
	cluster := flag.String("cluster", "prod", "cluster name (with -store)")
	flag.Parse()

	opts := []kgo.Opt{kgo.SeedBrokers(strings.Split(*bootstrap, ",")...), kgo.RetryTimeout(2 * time.Minute)}
	if *user != "" {
		opts = append(opts, kgo.SASL(plain.Auth{User: *user, Pass: *pass}.AsMechanism()))
	}
	if *useTLS {
		opts = append(opts, kgo.DialTLS())
	}
	ctx := context.Background()
	topic := fmt.Sprintf("chaos-%d", time.Now().Unix())
	if *user != "" {
		topic = "pg-" + topic
	}
	admin := client(opts)
	if r, err := kadm.NewClient(admin).CreateTopic(ctx, int32(*partitions), 1, nil, topic); err != nil || r.Err != nil {
		fatal(fmt.Errorf("create topic: %w", errors.Join(err, r.Err)))
	}
	fmt.Println("topic", topic)

	var (
		mu       sync.Mutex
		hist     checker.History
		lastAck  = time.Now()
		maxStall time.Duration
		kills    []string
		stop     = make(chan struct{})
		wg       sync.WaitGroup
		start    = time.Now()
	)
	ms := func(t time.Time) int64 { return t.Sub(start).Milliseconds() }

	// Producers: one ProduceOp per record; franz-go retries internally and
	// is idempotent, so a record may never appear twice.
	for p := 0; p < *producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cl := client(opts)
			defer cl.Close()
			tick := time.NewTicker(time.Second / time.Duration(max(*rate, 1)))
			defer tick.Stop()
			for n := 0; ; n++ {
				select {
				case <-stop:
					_ = cl.Flush(ctx)
					return
				case <-tick.C:
				}
				v := fmt.Sprintf("c%d-%d", id, n)
				sent := time.Now()
				cl.Produce(ctx, &kgo.Record{Topic: topic, Key: []byte(strconv.Itoa(n % 64)), Value: []byte(v)}, func(r *kgo.Record, err error) {
					now := time.Now()
					op := checker.ProduceOp{Client: id, Idempotent: true, Part: checker.Partition{Topic: topic, Partition: r.Partition},
						Values: []string{v}, Attempts: 1, Acked: err == nil, BaseOffset: r.Offset, Start: ms(sent), End: ms(now)}
					mu.Lock()
					hist.Produces = append(hist.Produces, op)
					if err == nil {
						if gap := now.Sub(lastAck); gap > maxStall {
							maxStall = gap
						}
						lastAck = now
					}
					mu.Unlock()
				})
			}
		}(p)
	}
	// Tail consumer recording every fetched slice.
	wg.Add(1)
	go func() {
		defer wg.Done()
		cl := client(append(opts, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())))
		defer cl.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			fs := cl.PollFetches(pctx)
			cancel()
			now := ms(time.Now())
			fs.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p.Records) == 0 {
					return
				}
				f := checker.FetchOp{Part: checker.Partition{Topic: topic, Partition: p.Partition}, Offset: p.Records[0].Offset, Start: now, End: now}
				for _, r := range p.Records {
					f.Records = append(f.Records, checker.Rec{Offset: r.Offset, Value: string(r.Value)})
				}
				mu.Lock()
				hist.Fetches = append(hist.Fetches, f)
				mu.Unlock()
			})
		}
	}()

	// Fault injection.
	deadline := time.Now().Add(*duration)
	seqKilled := *seqCmd == ""
	for time.Now().Before(deadline) {
		wait := *killMin + time.Duration(rand.Int64N(int64(*killMax-*killMin)+1))
		time.Sleep(min(wait, time.Until(deadline)))
		if !time.Now().Before(deadline) {
			break
		}
		if !seqKilled && time.Since(start) > *duration/2 {
			seqKilled = true
			kills = append(kills, run("sequencer", *seqCmd))
			continue
		}
		if *killCmd != "" {
			n := 1 + rand.IntN(*agents)
			kills = append(kills, run(fmt.Sprintf("agent-%d", n), strings.ReplaceAll(*killCmd, "{n}", strconv.Itoa(n))))
		}
	}
	close(stop)
	wg.Wait()
	fmt.Println("workload stopped; reading the final log")

	final := readFinal(ctx, opts, topic, *partitions)
	vs := checker.Check(&hist, final)
	if *store != "" {
		st, err := objstore.Open(ctx, *store)
		fatal(err)
		_, bvs, err := checker.CheckBucket(ctx, st, *cluster, 0, 8192)
		fatal(err)
		vs = append(vs, bvs...)
	}
	acked := 0
	for _, p := range hist.Produces {
		if p.Acked {
			acked++
		}
	}
	summary := map[string]any{
		"topic": topic, "duration": duration.String(), "kills": kills, "sends": len(hist.Produces), "acked": acked,
		"fetches": len(hist.Fetches), "max_produce_stall_ms": maxStall.Milliseconds(), "violations": vs,
	}
	b, _ := json.MarshalIndent(hist, "", " ")
	_ = os.WriteFile(*historyOut, b, 0o600)
	b, _ = json.MarshalIndent(summary, "", "  ")
	_ = os.WriteFile(*resultOut, b, 0o600)
	fmt.Println(string(b))
	if len(vs) > 0 {
		os.Exit(1)
	}
}

func readFinal(ctx context.Context, opts []kgo.Opt, topic string, parts int) checker.FinalLog {
	adm := kadm.NewClient(client(opts))
	starts, err := adm.ListStartOffsets(ctx, topic)
	fatal(err)
	ends, err := adm.ListEndOffsets(ctx, topic)
	fatal(err)
	final := checker.FinalLog{}
	want := 0
	for p := 0; p < parts; p++ {
		s, _ := starts.Lookup(topic, int32(p))
		e, _ := ends.Lookup(topic, int32(p))
		final[checker.Partition{Topic: topic, Partition: int32(p)}] = &checker.PartLog{LogStart: s.Offset, HW: e.Offset}
		want += int(e.Offset - s.Offset)
	}
	cl := client(append(opts, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())))
	defer cl.Close()
	got := 0
	rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for got < want && rctx.Err() == nil {
		fs := cl.PollFetches(rctx)
		fs.EachRecord(func(r *kgo.Record) {
			pl := final[checker.Partition{Topic: topic, Partition: r.Partition}]
			if r.Offset == pl.LogStart+int64(len(pl.Records)) && r.Offset < pl.HW {
				pl.Records = append(pl.Records, checker.Rec{Offset: r.Offset, Value: string(r.Value)})
				got++
			}
		})
	}
	if got < want {
		fatal(fmt.Errorf("final read got %d of %d records", got, want))
	}
	return final
}

func run(what, cmd string) string {
	fmt.Printf("%s killing %s: %s\n", time.Now().Format(time.TimeOnly), what, cmd)
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput() //nolint:gosec // the operator supplies the kill command
	if err != nil {
		fmt.Printf("kill command failed: %v: %s\n", err, out)
	}
	return time.Now().Format(time.RFC3339) + " " + what
}

func client(opts []kgo.Opt) *kgo.Client {
	cl, err := kgo.NewClient(opts...)
	fatal(err)
	return cl
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
