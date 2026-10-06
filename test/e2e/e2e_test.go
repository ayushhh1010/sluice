// Package e2e runs unmodified franz-go clients against an in-process
// cluster over real sockets.
package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ayushhh1010/sluice/internal/node"
	"github.com/ayushhh1010/sluice/internal/objstore"
)

func startCluster(t *testing.T, store objstore.Store, agents int) (*node.DevCluster, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	lvl := slog.LevelWarn
	if os.Getenv("SLUICE_DEBUG") != "" {
		lvl = slog.LevelDebug
	}
	d := &node.DevCluster{Store: store, Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))}
	if err := d.Start(ctx, 2, agents); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Stop(); cancel() })
	return d, ctx
}

func client(t *testing.T, d *node.DevCluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(d.Bootstrap()...), kgo.RetryTimeout(60 * time.Second)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func createTopic(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, parts int32) {
	t.Helper()
	adm := kadm.NewClient(cl)
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := adm.CreateTopic(ctx, parts, 1, nil, topic)
		if err == nil && resp.Err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic: %v %v", err, resp.Err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func produce(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, from, n int) {
	t.Helper()
	var recs []*kgo.Record
	for i := from; i < from+n; i++ {
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(fmt.Sprint(i % 7)), Value: []byte(fmt.Sprintf("v%06d", i))})
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce %d..%d: %v", from, from+n, err)
	}
}

// consumeAll reads every partition from the start until want values are seen.
func consumeAll(t *testing.T, ctx context.Context, d *node.DevCluster, topic string, want int) map[string]int {
	t.Helper()
	cl := client(t, d, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(500*time.Millisecond))
	seen := map[string]int{}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for len(seen) < want {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consumed %d/%d unique values before timeout", len(seen), want)
		}
		fs.EachError(func(tp string, p int32, err error) { t.Logf("fetch error %s/%d: %v", tp, p, err) })
		fs.EachRecord(func(r *kgo.Record) { seen[string(r.Value)]++ })
	}
	return seen
}

func TestProduceConsumeAcrossFailures(t *testing.T) {
	d, ctx := startCluster(t, objstore.NewMem(), 2)
	cl := client(t, d)
	createTopic(t, ctx, cl, "orders", 3)

	produce(t, ctx, cl, "orders", 0, 500)

	// Kill an agent: producers re-route after a metadata refresh.
	d.StopAgent(1)
	produce(t, ctx, cl, "orders", 500, 200)
	if err := d.StartAgent(ctx, 1); err != nil {
		t.Fatal(err)
	}

	// Kill the active sequencer: the standby recovers from the bucket.
	for _, n := range []string{"seq-a", "seq-b"} {
		d.StopSequencer(n)
		produce(t, ctx, cl, "orders", 700+100*map[string]int{"seq-a": 0, "seq-b": 1}[n], 100)
		d.StartSequencer(ctx, n)
	}

	seen := consumeAll(t, ctx, d, "orders", 900)
	var missing []string
	for i := 0; i < 900; i++ {
		if seen[fmt.Sprintf("v%06d", i)] == 0 {
			missing = append(missing, fmt.Sprint(i))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("acked records missing: %v", missing)
	}
}

func TestConsumerGroupRebalance(t *testing.T) {
	d, ctx := startCluster(t, objstore.NewMem(), 2)
	cl := client(t, d)
	createTopic(t, ctx, cl, "events", 4)
	produce(t, ctx, cl, "events", 0, 400)

	group := func() *kgo.Client {
		return client(t, d, kgo.ConsumerGroup("g1"), kgo.ConsumeTopics("events"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(300*time.Millisecond),
			kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(500*time.Millisecond))
	}
	c1, c2 := group(), group()
	seen := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(seen) < 400 && time.Now().Before(deadline) {
		for _, c := range []*kgo.Client{c1, c2} {
			pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			fs := c.PollFetches(pctx)
			cancel()
			fs.EachRecord(func(r *kgo.Record) { seen[string(r.Value)] = true })
		}
	}
	if len(seen) < 400 {
		t.Fatalf("group consumed %d/400", len(seen))
	}
	if err := c1.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := c2.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	adm := kadm.NewClient(cl)
	offs, err := adm.FetchOffsets(ctx, "g1")
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	offs.Each(func(o kadm.OffsetResponse) { total += o.At })
	if total != 400 {
		t.Fatalf("committed offsets sum to %d, want 400", total)
	}
}
