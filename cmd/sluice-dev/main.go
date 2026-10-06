// Command sluice-dev runs a whole cluster (two sequencers, N agents) in one
// process against a local directory or memory. It is the fastest way to try
// Sluice: point kcat or any Kafka client at the printed bootstrap address.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ayushhh1010/sluice/internal/cli"
	"github.com/ayushhh1010/sluice/internal/node"
	"github.com/ayushhh1010/sluice/internal/objstore"
)

func main() {
	fs := flag.NewFlagSet("sluice-dev", flag.ExitOnError)
	store := cli.String(fs, "store", "file://./sluice-data", "object store URL (file://dir, mem://, s3://...)")
	agents := cli.Int64(fs, "agents", 2, "number of agents")
	httpBase := cli.Int64(fs, "http-base", 8080, "first playground HTTP port (agent i listens on base+i-1); 0 disables")
	logLevel := cli.String(fs, "log-level", "warn", "log level")
	kafkaBase := cli.Int64(fs, "kafka-base", 9092, "first Kafka port (agent i listens on base+i-1); 0 picks free ports")
	_ = fs.Parse(os.Args[1:])

	ctx, cancel := cli.Context()
	defer cancel()
	st, err := objstore.Open(ctx, *store)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open store:", err)
		os.Exit(1)
	}
	d := &node.DevCluster{Store: st, Log: cli.Logger(*logLevel), HTTPBase: int(*httpBase), KafkaBase: int(*kafkaBase), RestartKilled: 10 * time.Second}
	if err := d.Start(ctx, 2, int(*agents)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Sluice dev cluster up (store %s)\n  bootstrap: %s\n", *store, strings.Join(d.Bootstrap(), ","))
	if *httpBase > 0 {
		fmt.Printf("  playground API: http://127.0.0.1:%d/api/v1/cluster\n", *httpBase)
	}
	fmt.Printf("  try: kcat -b %s -L\n", d.Bootstrap()[0])
	<-ctx.Done()
	d.Stop()
}
