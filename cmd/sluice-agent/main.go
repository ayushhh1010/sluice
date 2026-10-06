// Command sluice-agent runs one stateless agent: Kafka listeners, the
// playground HTTP API and an admin endpoint.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/cli"
	"github.com/ayushhh1010/sluice/internal/httpapi"
	"github.com/ayushhh1010/sluice/internal/node"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/protocol"
)

func main() {
	fs := flag.NewFlagSet("sluice-agent", flag.ExitOnError)
	var (
		id         = cli.String(fs, "id", "", "agent ID (e.g. agent-1)")
		nodeID     = cli.Int64(fs, "node-id", 0, "Kafka broker ID, stable across restarts")
		seqs       = cli.String(fs, "sequencers", "seq-a=localhost:7000", "sequencers: name=host:port,...")
		cluster    = cli.String(fs, "cluster", "prod", "cluster name (bucket key prefix)")
		store      = cli.String(fs, "store", "file://./data", "object store URL")
		logLevel   = cli.String(fs, "log-level", "info", "log level")
		internalLn = cli.String(fs, "kafka-internal", ":9092", "plaintext Kafka listener (private network); empty disables")
		internalAd = cli.String(fs, "advertise-internal", "localhost:9092", "advertised host:port for the internal listener")
		publicLn   = cli.String(fs, "kafka-public", "", "public TLS + SASL Kafka listener; empty disables")
		publicAd   = cli.String(fs, "advertise-public", "", "advertised host:port for the public listener")
		tlsCert    = cli.String(fs, "tls-cert", "", "TLS certificate file (reloaded when it changes)")
		tlsKey     = cli.String(fs, "tls-key", "", "TLS key file")
		users      = cli.String(fs, "sasl-users", "", "SASL/PLAIN users for the public listener: user:pass,... or @file")
		admins     = cli.String(fs, "admin-users", "", "users exempt from public limits, comma separated")
		httpAddr   = cli.String(fs, "http", ":8080", "playground HTTP API address; empty disables")
		adminAddr  = cli.String(fs, "admin", ":8081", "metrics, health and pprof address")
		flushWin   = cli.Duration(fs, "flush-window", 250*time.Millisecond, "maximum time a batch waits before its segment is uploaded")
		flushMax   = cli.Int64(fs, "flush-max-bytes", 4<<20, "segment size that triggers an immediate flush")
		prodQuota  = cli.Int64(fs, "public-produce-quota", 256<<10, "public produce bytes/s per user")
		fetchQuota = cli.Int64(fs, "public-fetch-quota", 1<<20, "public fetch bytes/s per user")
		maxBatch   = cli.Int64(fs, "public-max-batch", 64<<10, "largest record batch on the public listener")
		maxConns   = cli.Int64(fs, "public-max-conns-per-ip", 10, "connections per client IP on the public listener")
		restartDel = cli.Duration(fs, "killed-restart-delay", 30*time.Second, "after a chaos kill, wait this long on the next start")
	)
	_ = fs.Parse(os.Args[1:])
	if *id == "" || *nodeID <= 0 {
		fmt.Fprintln(os.Stderr, "--id and --node-id are required")
		os.Exit(2)
	}
	log := cli.Logger(*logLevel).With("proc", *id)

	// A chaos kill leaves a marker so the restarted process stays down for a
	// while, which makes the outage visible in the playground.
	marker := filepath.Join(os.TempDir(), "sluice-killed-"+*id)
	if _, err := os.Stat(marker); err == nil {
		_ = os.Remove(marker)
		log.Warn("restarting after a chaos kill", "delay", *restartDel)
		time.Sleep(*restartDel)
	}

	seqAddrs, err := cli.Map(*seqs)
	if err != nil {
		log.Error("bad --sequencers", "err", err)
		os.Exit(2)
	}
	cfg := agent.Config{ID: *id, NodeID: int32(*nodeID), Cluster: *cluster, HTTPAddr: *httpAddr, FlushWindowMs: flushWin.Milliseconds(), FlushMaxBytes: *flushMax}
	for n := range seqAddrs {
		cfg.Sequencers = append(cfg.Sequencers, n)
	}
	var listeners []protocol.Listener
	if *internalLn != "" {
		h, p, err := cli.HostPort(*internalAd)
		if err != nil {
			log.Error("bad --advertise-internal", "err", err)
			os.Exit(2)
		}
		cfg.Listeners = append(cfg.Listeners, &pb.Listener{Name: "internal", Host: h, Port: p})
		listeners = append(listeners, protocol.Listener{Name: "internal", Addr: *internalLn})
	}
	if *publicLn != "" {
		h, p, err := cli.HostPort(*publicAd)
		if err != nil {
			log.Error("bad --advertise-public", "err", err)
			os.Exit(2)
		}
		l := protocol.Listener{
			Name: "public", Addr: *publicLn, Public: true, MaxBatchBytes: int(*maxBatch), MaxFrameBytes: 1 << 20,
			ProduceQuota: float64(*prodQuota), FetchQuota: float64(*fetchQuota), MaxConnsPerIP: int(*maxConns), Admins: map[string]bool{},
		}
		if *tlsCert != "" {
			// On a fresh VM Caddy may still be obtaining the certificate.
			for wait := 0; ; wait++ {
				if l.TLS, err = cli.ReloadingCert(*tlsCert, *tlsKey); err == nil {
					break
				}
				if wait == 120 {
					log.Error("load TLS certificate", "err", err)
					os.Exit(1)
				}
				log.Warn("waiting for TLS certificate", "err", err)
				time.Sleep(5 * time.Second)
			}
		}
		if *users != "" {
			if l.Users, err = cli.Users(*users); err != nil {
				log.Error("bad --sasl-users", "err", err)
				os.Exit(2)
			}
		}
		for _, u := range strings.Split(*admins, ",") {
			if u = strings.TrimSpace(u); u != "" {
				l.Admins[u] = true
			}
		}
		cfg.Listeners = append(cfg.Listeners, &pb.Listener{Name: "public", Host: h, Port: p})
		listeners = append(listeners, l)
	}
	ctx, cancel := cli.Context()
	defer cancel()
	st, err := objstore.Open(ctx, *store)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	err = node.RunAgent(ctx, node.AgentOptions{
		Config: cfg, Kafka: listeners, HTTP: *httpAddr, Admin: *adminAddr, SeqAddrs: seqAddrs, Store: st, Log: log,
		HTTPOptions: httpapi.Options{TrustProxy: true},
	})
	if errors.Is(err, node.ErrKilled) {
		_ = os.WriteFile(marker, []byte(strconv.FormatInt(time.Now().Unix(), 10)), 0o600)
		os.Exit(3)
	}
	if err != nil {
		log.Error("agent failed", "err", err)
		os.Exit(1)
	}
}
