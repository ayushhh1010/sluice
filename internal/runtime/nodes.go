package runtime

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/metrics"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

func seed() uint64 {
	var b [8]byte
	_, _ = crand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}

// ---- Sequencer ----

// SequencerNode runs a sequencer core and serves its gRPC API.
type SequencerNode struct {
	pb.UnimplementedSequencerServiceServer

	Core  *sequencer.Core
	loop  *Loop
	log   *slog.Logger
	m     *metrics.Metrics
	fatal chan error

	mu      sync.Mutex
	watches map[string][]chan *pb.WatchEvent
	role    string
}

func NewSequencerNode(cfg sequencer.Config, store objstore.Store, peers *Peers, m *metrics.Metrics, log *slog.Logger) *SequencerNode {
	n := &SequencerNode{Core: sequencer.New(cfg, seed()), log: log, m: m, fatal: make(chan error, 1), watches: map[string][]chan *pb.WatchEvent{}}
	n.loop = NewLoop(n.Core.Step, store, peers, m, log, n.onEffect)
	return n
}

func (n *SequencerNode) onEffect(e any) {
	switch e := e.(type) {
	case sequencer.Reply:
		n.loop.Deliver(e.ID, e.Resp)
	case sequencer.Notify:
		n.mu.Lock()
		for _, ch := range n.watches[e.Agent] {
			select {
			case ch <- e.Event:
			default: // watch events are hints; a slow stream misses some
			}
		}
		n.mu.Unlock()
	case sequencer.RoleChange:
		n.log.Info("sequencer role", "role", e.Role, "epoch", e.Epoch)
		n.mu.Lock()
		n.role = e.Role
		n.mu.Unlock()
		if n.m != nil {
			for _, r := range []string{"standby", "recovering", "leader", "failed"} {
				v := 0.0
				if r == e.Role {
					v = 1
				}
				n.m.Role.WithLabelValues(r).Set(v)
			}
		}
	case sequencer.Fatal:
		n.log.Error("sequencer stopped: unrecoverable state", "err", e.Err)
		select {
		case n.fatal <- e.Err:
		default:
		}
	}
}

// Role reports the current role (safe for concurrent use).
func (n *SequencerNode) Role() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Run drives the core until ctx ends or the core fails.
func (n *SequencerNode) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		n.loop.Run(ctx, sequencer.Start{})
		close(done)
	}()
	select {
	case err := <-n.fatal:
		cancel()
		<-done
		return err
	case <-done:
		return nil
	}
}

func (n *SequencerNode) Call(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	r, err := n.loop.Request(ctx, func(id uint64) any { return sequencer.Request{ID: id, Req: req} })
	if err != nil {
		return nil, err
	}
	return r.(*pb.Response), nil
}

func (n *SequencerNode) Watch(req *pb.WatchRequest, stream pb.SequencerService_WatchServer) error {
	ch := make(chan *pb.WatchEvent, 256)
	n.mu.Lock()
	n.watches[req.AgentId] = append(n.watches[req.AgentId], ch)
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		ws := n.watches[req.AgentId]
		for i, c := range ws {
			if c == ch {
				n.watches[req.AgentId] = append(ws[:i:i], ws[i+1:]...)
				break
			}
		}
		n.mu.Unlock()
	}()
	for {
		select {
		case ev := <-ch:
			if err := stream.Send(ev); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

// ---- Agent ----

// AgentNode runs an agent core.
type AgentNode struct {
	Core  *agent.Core
	cfg   agent.Config
	loop  *Loop
	peers *Peers
	log   *slog.Logger
	Died  chan struct{}
	once  sync.Once
}

func NewAgentNode(cfg agent.Config, store objstore.Store, peers *Peers, m *metrics.Metrics, log *slog.Logger) *AgentNode {
	n := &AgentNode{Core: agent.New(cfg, seed()), cfg: cfg, peers: peers, log: log, Died: make(chan struct{})}
	n.loop = NewLoop(n.Core.Step, store, peers, m, log, n.onEffect)
	return n
}

func (n *AgentNode) onEffect(e any) {
	switch e := e.(type) {
	case agent.ProduceResp:
		n.loop.Deliver(e.ID, e)
	case agent.FetchResp:
		n.loop.Deliver(e.ID, e)
	case agent.ForwardResp:
		n.loop.Deliver(e.ID, e)
	case agent.Die:
		n.log.Warn("chaos: killed by request")
		n.once.Do(func() { close(n.Died) })
	}
}

func (n *AgentNode) Run(ctx context.Context) {
	for _, s := range n.cfg.Sequencers {
		go n.watch(ctx, s)
	}
	n.loop.Run(ctx, agent.Start{})
}

// watch keeps a watch stream open to one sequencer; only the leader sends.
func (n *AgentNode) watch(ctx context.Context, node string) {
	for ctx.Err() == nil {
		st, err := n.peers.Watch(ctx, node, n.cfg.ID)
		if err == nil {
			for {
				ev, err := st.Recv()
				if err != nil {
					break
				}
				n.loop.Submit(agent.Watch{Event: ev})
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

var ErrNoResponse = errors.New("agent: no response")

func (n *AgentNode) Produce(ctx context.Context, req agent.ProduceReq) (agent.ProduceResp, error) {
	if req.Acks == 0 {
		req.ID = n.loop.NewID()
		n.loop.Submit(req)
		return agent.ProduceResp{}, nil
	}
	r, err := n.loop.Request(ctx, func(id uint64) any { req.ID = id; return req })
	if err != nil {
		return agent.ProduceResp{}, err
	}
	return r.(agent.ProduceResp), nil
}

// ProduceStart submits a produce now (preserving the caller's order) and
// returns a function that waits for its result.
func (n *AgentNode) ProduceStart(req agent.ProduceReq) func(ctx context.Context) (agent.ProduceResp, error) {
	if req.Acks == 0 {
		req.ID = n.loop.NewID()
		n.loop.Submit(req)
		return func(context.Context) (agent.ProduceResp, error) { return agent.ProduceResp{}, nil }
	}
	ch, done := n.loop.Begin(func(id uint64) any { req.ID = id; return req })
	return func(ctx context.Context) (agent.ProduceResp, error) {
		defer done()
		select {
		case r := <-ch:
			return r.(agent.ProduceResp), nil
		case <-ctx.Done():
			return agent.ProduceResp{}, ctx.Err()
		}
	}
}

func (n *AgentNode) Fetch(ctx context.Context, req agent.FetchReq) (agent.FetchResp, error) {
	r, err := n.loop.Request(ctx, func(id uint64) any { req.ID = id; return req })
	if err != nil {
		return agent.FetchResp{}, err
	}
	return r.(agent.FetchResp), nil
}

func (n *AgentNode) Forward(ctx context.Context, req *pb.Request, timeout time.Duration) (*pb.Response, error) {
	r, err := n.loop.Request(ctx, func(id uint64) any {
		return agent.ForwardReq{ID: id, Req: req, TimeoutMs: timeout.Milliseconds()}
	})
	if err != nil {
		return nil, err
	}
	fr := r.(agent.ForwardResp)
	if fr.Err != nil {
		return nil, fr.Err
	}
	return fr.Resp, nil
}
