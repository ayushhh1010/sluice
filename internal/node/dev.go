package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/protocol"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

// DevCluster runs sequencers and agents in one process over real sockets.
// It backs `sluice-dev` and the end-to-end tests, and supports killing and
// restarting any process to exercise recovery.
type DevCluster struct {
	Store objstore.Store
	Log   *slog.Logger
	// Tune lets callers adjust configs before processes start.
	TuneSeq   func(*sequencer.Config)
	TuneAgent func(*agent.Config)
	Cluster   string
	HTTPBase  int // first playground HTTP port; 0 disables HTTP
	KafkaBase int // first Kafka port (agent i uses KafkaBase+i-1); 0 picks free ports
	// RestartKilled restarts an agent killed by the chaos endpoint after this
	// delay, like the production restart policy; 0 leaves it down.
	RestartKilled time.Duration

	mu       sync.Mutex
	seqAddrs map[string]string
	seqs     map[string]context.CancelFunc
	seqDone  map[string]chan struct{}
	agents   map[int]context.CancelFunc
	agDone   map[int]chan struct{}
	kafka    map[int]string
	ports    map[int]int
}

// freePort reserves a TCP port number.
func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// Start launches nSeq sequencers and nAgents agents.
func (d *DevCluster) Start(ctx context.Context, nSeq, nAgents int) error {
	if d.Cluster == "" {
		d.Cluster = "dev"
	}
	d.seqAddrs = map[string]string{}
	d.seqs, d.seqDone = map[string]context.CancelFunc{}, map[string]chan struct{}{}
	d.agents, d.agDone, d.kafka, d.ports = map[int]context.CancelFunc{}, map[int]chan struct{}{}, map[int]string{}, map[int]int{}
	for i := 0; i < nSeq; i++ {
		d.seqAddrs[fmt.Sprintf("seq-%c", 'a'+i)] = fmt.Sprintf("127.0.0.1:%d", freePort())
	}
	for name := range d.seqAddrs {
		d.StartSequencer(ctx, name)
	}
	for i := 1; i <= nAgents; i++ {
		d.ports[i] = freePort()
		if d.KafkaBase > 0 {
			d.ports[i] = d.KafkaBase + i - 1
		}
		if err := d.StartAgent(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

func (d *DevCluster) seqNames() []string {
	var out []string
	for n := range d.seqAddrs {
		out = append(out, n)
	}
	return out
}

// StartSequencer (re)starts the named sequencer.
func (d *DevCluster) StartSequencer(ctx context.Context, name string) {
	cfg := sequencer.Config{Node: name, Cluster: d.Cluster, GroupInitialDelayMs: -1, LeaderTimeoutMs: 3000, StartupStaggerMs: 1000}
	peers := map[string]string{}
	for n, a := range d.seqAddrs {
		if n != name {
			cfg.Peers = append(cfg.Peers, n)
			peers[n] = a
		}
	}
	if d.TuneSeq != nil {
		d.TuneSeq(&cfg)
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	d.mu.Lock()
	d.seqs[name], d.seqDone[name] = cancel, done
	d.mu.Unlock()
	go func() {
		defer close(done)
		err := RunSequencer(ctx, SequencerOptions{Config: cfg, Listen: d.seqAddrs[name], PeerAddrs: peers, Store: d.Store, Log: d.Log.With("proc", name)})
		if err != nil {
			d.Log.Error("sequencer exited", "node", name, "err", err)
		}
	}()
}

// StopSequencer kills a sequencer (all its memory is lost).
func (d *DevCluster) StopSequencer(name string) {
	d.mu.Lock()
	cancel, done := d.seqs[name], d.seqDone[name]
	d.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// StartAgent (re)starts agent i (node ID i) on a stable port.
func (d *DevCluster) StartAgent(ctx context.Context, i int) error {
	port := d.ports[i]
	cfg := agent.Config{
		ID: "agent-" + strconv.Itoa(i), NodeID: int32(i), Cluster: d.Cluster, Sequencers: d.seqNames(),
		Listeners:     []*pb.Listener{{Name: "internal", Host: "127.0.0.1", Port: int32(port)}},
		FlushWindowMs: 50,
	}
	if d.TuneAgent != nil {
		d.TuneAgent(&cfg)
	}
	httpAddr := ""
	if d.HTTPBase > 0 {
		httpAddr = fmt.Sprintf("127.0.0.1:%d", d.HTTPBase+i-1)
	}
	ready := make(chan []string, 1)
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	d.mu.Lock()
	d.agents[i], d.agDone[i] = cancel, done
	d.mu.Unlock()
	go func() {
		defer close(done)
		err := RunAgent(ctx, AgentOptions{
			Config: cfg, Kafka: []protocol.Listener{{Name: "internal", Addr: fmt.Sprintf("127.0.0.1:%d", port)}},
			HTTP: httpAddr, SeqAddrs: d.seqAddrs, Store: d.Store, Log: d.Log.With("proc", cfg.ID), Ready: ready,
		})
		if err != nil {
			d.Log.Warn("agent exited", "agent", cfg.ID, "err", err)
			select {
			case ready <- nil:
			default:
			}
		}
		d.mu.Lock()
		restart := d.RestartKilled
		d.mu.Unlock()
		if errors.Is(err, ErrKilled) && restart > 0 {
			go func() {
				select {
				case <-time.After(restart):
					if err := d.StartAgent(parent, i); err != nil {
						d.Log.Error("restart after chaos kill", "agent", cfg.ID, "err", err)
					}
				case <-parent.Done():
				}
			}()
		}
	}()
	addrs := <-ready
	if addrs == nil {
		return fmt.Errorf("agent %d failed to start", i)
	}
	d.mu.Lock()
	d.kafka[i] = addrs[0]
	d.mu.Unlock()
	return nil
}

// StopAgent kills agent i.
func (d *DevCluster) StopAgent(i int) {
	d.mu.Lock()
	cancel, done := d.agents[i], d.agDone[i]
	d.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// Bootstrap returns every agent's Kafka address.
func (d *DevCluster) Bootstrap() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for i := 1; i <= len(d.kafka); i++ {
		out = append(out, d.kafka[i])
	}
	return out
}

// Stop kills everything.
func (d *DevCluster) Stop() {
	d.mu.Lock()
	d.RestartKilled = 0 // no chaos restarts while shutting down
	var agents []int
	for i := range d.agents {
		agents = append(agents, i)
	}
	var seqs []string
	for n := range d.seqs {
		seqs = append(seqs, n)
	}
	d.mu.Unlock()
	for _, i := range agents {
		d.StopAgent(i)
	}
	for _, n := range seqs {
		d.StopSequencer(n)
	}
}
