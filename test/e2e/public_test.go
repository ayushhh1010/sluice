package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/node"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/protocol"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

func selfSigned(t *testing.T) *tls.Config {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func TestPublicListenerTLSAndSASL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	store := objstore.NewMem()
	seqReady := make(chan string, 1)
	go func() {
		_ = node.RunSequencer(ctx, node.SequencerOptions{
			Config: sequencer.Config{Node: "seq-a", Cluster: "pub", StartupStaggerMs: 1}, Listen: "127.0.0.1:0",
			Store: store, Log: log, Ready: seqReady,
		})
	}()
	seqAddr := <-seqReady
	ready := make(chan []string, 1)
	go func() {
		_ = node.RunAgent(ctx, node.AgentOptions{
			Config: agent.Config{ID: "agent-1", NodeID: 1, Cluster: "pub", Sequencers: []string{"seq-a"}, FlushWindowMs: 20,
				Listeners: []*pb.Listener{{Name: "public", Host: "127.0.0.1"}}},
			Kafka: []protocol.Listener{{Name: "public", Addr: "127.0.0.1:0", Public: true, TLS: selfSigned(t),
				Users: map[string]string{"demo": "secret"}, ProduceQuota: 64 << 10, MaxConnsPerIP: 20}},
			SeqAddrs: map[string]string{"seq-a": seqAddr}, Store: store, Log: log, Ready: ready,
		})
	}()
	addr := (<-ready)[0]
	tlsCfg := &tls.Config{InsecureSkipVerify: true}

	// Wrong password: authentication fails.
	bad, _ := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTLSConfig(tlsCfg), kgo.SASL(plain.Auth{User: "demo", Pass: "nope"}.AsMechanism()))
	if _, err := kadm.NewClient(bad).ListTopics(ctx); err == nil || !strings.Contains(strings.ToUpper(err.Error()), "SASL") {
		t.Fatalf("bad password accepted: %v", err)
	}
	bad.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTLSConfig(tlsCfg), kgo.SASL(plain.Auth{User: "demo", Pass: "secret"}.AsMechanism()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	// Public users are confined to the pg- namespace and small topics.
	if r, _ := adm.CreateTopic(ctx, 1, 1, nil, "private"); r.Err == nil {
		t.Fatal("public user created a topic outside pg-")
	}
	if r, _ := adm.CreateTopic(ctx, 16, 1, nil, "pg-too-big"); r.Err == nil {
		t.Fatal("public user created a 16-partition topic")
	}
	createTopic(t, ctx, cl, "pg-demo", 2)

	// 256 KiB against a 64 KiB/s quota: produce succeeds but is throttled.
	start := time.Now()
	val := make([]byte, 16<<10)
	_, _ = rand.Read(val) // incompressible
	for i := 0; i < 16; i++ {
		pctx, c := context.WithTimeout(ctx, 60*time.Second)
		if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "pg-demo", Value: val}).FirstErr(); err != nil && !errors.Is(err, context.Canceled) {
			c()
			t.Fatalf("produce: %v", err)
		}
		c()
	}
	if el := time.Since(start); el < 2*time.Second {
		t.Fatalf("256 KiB at 64 KiB/s finished in %s: quota not enforced", el)
	}
}
