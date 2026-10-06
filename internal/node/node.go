// Package node wires cores, transports and listeners into runnable
// processes. The cmd/ binaries and the in-process dev cluster share it.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/httpapi"
	"github.com/ayushhh1010/sluice/internal/metrics"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/protocol"
	"github.com/ayushhh1010/sluice/internal/runtime"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

// SequencerOptions configures a sequencer process.
type SequencerOptions struct {
	Config    sequencer.Config
	Listen    string            // gRPC address
	Admin     string            // metrics, health and pprof; empty disables
	PeerAddrs map[string]string // sequencer node -> gRPC address (peers only)
	Store     objstore.Store
	Log       *slog.Logger
	Ready     chan<- string // receives the bound gRPC address
}

// RunSequencer runs until ctx ends or the core hits an unrecoverable error.
func RunSequencer(ctx context.Context, o SequencerOptions) error {
	m := metrics.New("sequencer")
	peers := runtime.NewPeers(o.PeerAddrs)
	defer peers.Close()
	n := runtime.NewSequencerNode(o.Config, o.Store, peers, m, o.Log)

	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return err
	}
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	pb.RegisterSequencerServiceServer(gs, n)
	go func() {
		if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			o.Log.Error("grpc server", "err", err)
		}
	}()
	defer gs.Stop()
	if o.Admin != "" {
		stop := serveAdmin(o.Admin, m, func() error {
			if r := n.Role(); r == "failed" {
				return fmt.Errorf("sequencer %s", r)
			}
			return nil
		}, o.Log)
		defer stop()
	}
	if o.Ready != nil {
		o.Ready <- ln.Addr().String()
	}
	o.Log.Info("sequencer started", "node", o.Config.Node, "grpc", ln.Addr().String())
	return n.Run(ctx)
}

// AgentOptions configures an agent process.
type AgentOptions struct {
	Config   agent.Config
	Kafka    []protocol.Listener
	HTTP     string // playground HTTP API; empty disables
	Admin    string
	SeqAddrs map[string]string
	Store    objstore.Store
	Log      *slog.Logger
	// Ready receives each Kafka listener's bound address, in order.
	Ready chan<- []string
	// OnDie runs when chaos kills the agent (default: return ErrKilled).
	HTTPOptions httpapi.Options
}

// ErrKilled is returned when the agent was killed by the chaos endpoint.
var ErrKilled = errors.New("agent killed by chaos request")

// RunAgent runs until ctx ends or the agent is killed.
func RunAgent(ctx context.Context, o AgentOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	m := metrics.New("agent")
	peers := runtime.NewPeers(o.SeqAddrs)
	defer peers.Close()

	// Bind listeners first so advertised ports are known (port 0 in tests).
	var lns []net.Listener
	var bound []string
	for i := range o.Kafka {
		ln, err := net.Listen("tcp", o.Kafka[i].Addr)
		if err != nil {
			return err
		}
		defer ln.Close()
		lns = append(lns, ln)
		bound = append(bound, ln.Addr().String())
		if l := o.Config.Listeners; i < len(l) && l[i].Port == 0 {
			l[i].Port = int32(ln.Addr().(*net.TCPAddr).Port)
		}
	}
	n := runtime.NewAgentNode(o.Config, o.Store, peers, m, o.Log)
	go n.Run(ctx)
	for i, l := range o.Kafka {
		s := protocol.NewServer(l, n, m, o.Log)
		go func(ln net.Listener) {
			if err := s.Serve(ctx, ln); err != nil {
				o.Log.Error("kafka listener", "err", err)
			}
		}(lns[i])
	}
	if o.HTTP != "" {
		api := httpapi.New(n, o.HTTPOptions, o.Log)
		srv := &http.Server{Addr: o.HTTP, Handler: api, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				o.Log.Error("http api", "err", err)
			}
		}()
		defer srv.Close()
	}
	if o.Admin != "" {
		stop := serveAdmin(o.Admin, m, func() error { return nil }, o.Log)
		defer stop()
	}
	if o.Ready != nil {
		o.Ready <- bound
	}
	o.Log.Info("agent started", "id", o.Config.ID, "node", o.Config.NodeID, "kafka", bound)
	select {
	case <-ctx.Done():
		return nil
	case <-n.Died:
		return ErrKilled
	}
}

func serveAdmin(addr string, m *metrics.Metrics, health func() error, log *slog.Logger) func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if err := health(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server", "err", err)
		}
	}()
	return func() { srv.Close() }
}
