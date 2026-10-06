// Command sluicectl is the admin CLI.
//
//	sluicectl topics   -b localhost:9092
//	sluicectl groups   -b localhost:9092
//	sluicectl create   -b localhost:9092 -topic orders -partitions 3
//	sluicectl log      -store file://./data [-from 1] [-n 20]
//	sluicectl replay   -store file://./data
//	sluicectl inspect  -store file://./data -topic orders -partition 0 -offset 42
//	sluicectl dump     -store file://./data <segment key> [-records]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ayushhh1010/sluice/internal/checker"
	"github.com/ayushhh1010/sluice/internal/kbatch"
	"github.com/ayushhh1010/sluice/internal/metalog"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/segment"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: sluicectl <topics|groups|create|delete|log|replay|inspect|dump> [flags]
  Kafka commands take -b bootstrap [-user u -pass p -tls]
  bucket commands take -store URL [-cluster name]`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	bootstrap := fs.String("b", envOr("SLUICE_BOOTSTRAP", "localhost:9092"), "bootstrap brokers")
	user := fs.String("user", "", "SASL/PLAIN user")
	pass := fs.String("pass", "", "SASL/PLAIN password")
	useTLS := fs.Bool("tls", false, "use TLS")
	store := fs.String("store", os.Getenv("SLUICE_STORE"), "object store URL")
	cluster := fs.String("cluster", envOr("SLUICE_CLUSTER", "prod"), "cluster name")
	topic := fs.String("topic", "", "topic")
	partitions := fs.Int("partitions", 1, "partitions (create)")
	partition := fs.Int("partition", 0, "partition (inspect)")
	offset := fs.Int64("offset", 0, "offset (inspect)")
	from := fs.Uint64("from", 1, "first entry (log)")
	n := fs.Int("n", 20, "entries to print (log)")
	records := fs.Bool("records", false, "print record values (dump)")
	_ = fs.Parse(os.Args[2:])
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	kafka := func() *kadm.Client {
		opts := []kgo.Opt{kgo.SeedBrokers(strings.Split(*bootstrap, ",")...)}
		if *user != "" {
			opts = append(opts, kgo.SASL(plain.Auth{User: *user, Pass: *pass}.AsMechanism()))
		}
		if *useTLS {
			opts = append(opts, kgo.DialTLS())
		}
		cl, err := kgo.NewClient(opts...)
		die(err)
		return kadm.NewClient(cl)
	}
	bucket := func() objstore.Store {
		if *store == "" {
			die(fmt.Errorf("-store is required"))
		}
		st, err := objstore.Open(ctx, *store)
		die(err)
		return st
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	defer tw.Flush()

	switch cmd {
	case "topics":
		td, err := kafka().ListTopics(ctx)
		die(err)
		fmt.Fprintln(tw, "TOPIC\tPARTITIONS\tID")
		for _, t := range td.Sorted() {
			fmt.Fprintf(tw, "%s\t%d\t%s\n", t.Topic, len(t.Partitions), t.ID)
		}
	case "groups":
		adm := kafka()
		lg, err := adm.ListGroups(ctx)
		die(err)
		dg, err := adm.DescribeGroups(ctx, lg.Groups()...)
		die(err)
		lag, _ := adm.Lag(ctx, lg.Groups()...)
		fmt.Fprintln(tw, "GROUP\tSTATE\tMEMBERS\tLAG")
		for _, g := range dg.Sorted() {
			var total int64
			if l, ok := lag[g.Group]; ok {
				total = l.Lag.Total()
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%d\n", g.Group, g.State, len(g.Members), total)
		}
	case "create":
		resp, err := kafka().CreateTopic(ctx, int32(*partitions), 1, nil, *topic)
		die(err)
		die(resp.Err)
		fmt.Printf("created %s (%d partitions, id %s)\n", resp.Topic, resp.NumPartitions, resp.ID)
	case "delete":
		resp, err := kafka().DeleteTopic(ctx, *topic)
		die(err)
		die(resp.Err)
		fmt.Println("deleted", *topic)
	case "log":
		st, keys := bucket(), metalog.NewKeys(*cluster)
		for seq := *from; seq < *from+uint64(*n); seq++ {
			b, err := st.Get(ctx, keys.Log(seq), 0, 0)
			if err != nil {
				fmt.Printf("entry %d: %v\n", seq, err)
				break
			}
			e, err := metalog.DecodeEntry(b)
			die(err)
			fmt.Println(protojson.MarshalOptions{EmitUnpopulated: false}.Format(e))
		}
	case "replay":
		st := bucket()
		s, vs, err := checker.CheckBucket(ctx, st, *cluster, 0, 8192)
		die(err)
		fmt.Printf("seq %d epoch %d leader %s state %x\n", s.Seq, s.Epoch, s.Leader, s.Hash())
		var names []string
		for _, t := range s.Topics {
			names = append(names, t.Name)
		}
		sort.Strings(names)
		fmt.Fprintln(tw, "TOPIC\tPARTITION\tLOG START\tHIGH WATERMARK\tBATCHES")
		for _, name := range names {
			t := s.Topics[s.ByName[name]]
			for i, p := range t.Parts {
				fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\n", name, i, p.LogStart, p.Next, len(p.Refs))
			}
		}
		tw.Flush()
		for _, v := range vs {
			fmt.Println("VIOLATION", v)
		}
		if len(vs) == 0 {
			fmt.Println("invariants I3, I6, I7 hold")
		}
	case "inspect":
		st := bucket()
		s, err := checker.Replay(ctx, st, *cluster, 0, 0, 8192)
		die(err)
		id, ok := s.ByName[*topic]
		p := s.Partition(id, uint32(*partition))
		if !ok || p == nil {
			die(fmt.Errorf("unknown topic or partition"))
		}
		for _, r := range p.Refs {
			if *offset >= r.BaseOffset && *offset < r.BaseOffset+int64(r.Records) {
				fmt.Printf("offset %d of %s/%d\n  batch:     offsets %d-%d (%d records)\n  object:    %s\n  bytes:     %d-%d\n  committed: metadata log entry %d\n",
					*offset, *topic, *partition, r.BaseOffset, r.BaseOffset+int64(r.Records)-1, r.Records, r.ObjectKey, r.OffsetInObj, r.OffsetInObj+r.Length, r.CommitSeq)
				return
			}
		}
		die(fmt.Errorf("offset %d is outside [%d, %d)", *offset, p.LogStart, p.Next))
	case "dump":
		if fs.NArg() != 1 {
			usage()
		}
		obj, err := bucket().Get(ctx, fs.Arg(0), 0, 0)
		die(err)
		h, entries, err := segment.Parse(obj)
		die(err)
		fmt.Printf("SLC%d segment, %d bytes, created %s by %s, %d batches\n", h.Version, len(obj),
			time.UnixMilli(h.CreatedMs).UTC().Format(time.RFC3339), strings.TrimRight(string(h.AgentID[:]), "\x00"), len(entries))
		fmt.Fprintln(tw, "TOPIC ID\tPARTITION\tBYTES\tRECORDS\tPRODUCER\tSEQ")
		for _, e := range entries {
			fmt.Fprintf(tw, "%x\t%d\t%d-%d\t%d\t%d/%d\t%d\n", e.TopicID, e.Partition, e.Offset, e.Offset+e.Length, e.Records, e.ProducerID, e.ProducerEpoch, e.BaseSeq)
			if *records {
				recs, _, ok := kbatch.Records(segment.Batch(obj, e))
				if !ok {
					fmt.Fprintln(tw, "\t\t(compressed)")
				}
				for _, r := range recs {
					fmt.Fprintf(tw, "\t\tkey=%q value=%q\n", r.Key, r.Value)
				}
			}
		}
	default:
		usage()
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
