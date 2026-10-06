// Command sluice-sequencer runs one sequencer (active or standby).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/ayushhh1010/sluice/internal/cli"
	"github.com/ayushhh1010/sluice/internal/node"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

func main() {
	fs := flag.NewFlagSet("sluice-sequencer", flag.ExitOnError)
	var (
		nodeName     = cli.String(fs, "node", "", "this sequencer's node name (e.g. seq-a)")
		listen       = cli.String(fs, "listen", ":7000", "gRPC listen address")
		admin        = cli.String(fs, "admin", ":7001", "metrics, health and pprof address")
		peers        = cli.String(fs, "peers", "", "other sequencers: name=host:port,...")
		cluster      = cli.String(fs, "cluster", "prod", "cluster name (bucket key prefix)")
		store        = cli.String(fs, "store", "file://./data", "object store URL: s3://bucket?endpoint=..., file://dir, mem://")
		logLevel     = cli.String(fs, "log-level", "info", "debug, info, warn or error")
		commitWindow = cli.Duration(fs, "commit-window", 0, "group commit window (default 20ms)")
		leaderTO     = cli.Duration(fs, "leader-timeout", 0, "standby takeover timeout (default 10s)")
		gcGrace      = cli.Duration(fs, "gc-grace", 0, "delay between tombstoning and deleting an object (default 10m)")
		retention    = cli.Duration(fs, "default-retention", 0, "default topic retention (default 24h)")
		costGuard    = cli.Int64(fs, "cost-guard-daily", 0, "Class A operations per UTC day before public listeners turn read-only; 0 disables")
		publicTopics = cli.Int64(fs, "public-max-topics", 0, "maximum public (pg-) topics (default 200)")
	)
	_ = fs.Parse(os.Args[1:])
	if *nodeName == "" {
		fmt.Fprintln(os.Stderr, "--node is required")
		os.Exit(2)
	}
	log := cli.Logger(*logLevel).With("proc", *nodeName)
	peerAddrs, err := cli.Map(*peers)
	if err != nil {
		log.Error("bad --peers", "err", err)
		os.Exit(2)
	}
	ctx, cancel := cli.Context()
	defer cancel()
	st, err := objstore.Open(ctx, *store)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	cfg := sequencer.Config{
		Node: *nodeName, Cluster: *cluster,
		CommitWindowMs: commitWindow.Milliseconds(), LeaderTimeoutMs: leaderTO.Milliseconds(),
		GCGraceMs: gcGrace.Milliseconds(), DefaultRetentionMs: retention.Milliseconds(),
		ClassADailyLimit: *costGuard, PublicMaxTopics: int(*publicTopics),
	}
	for p := range peerAddrs {
		cfg.Peers = append(cfg.Peers, p)
	}
	err = node.RunSequencer(ctx, node.SequencerOptions{Config: cfg, Listen: *listen, Admin: *admin, PeerAddrs: peerAddrs, Store: st, Log: log})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("sequencer failed", "err", err)
		os.Exit(1)
	}
}
