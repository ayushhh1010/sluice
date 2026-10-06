package runtime

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
)

// Peers is a pool of gRPC connections to sequencers, by node name.
type Peers struct {
	addrs map[string]string
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPeers maps node names to host:port addresses.
func NewPeers(addrs map[string]string) *Peers {
	return &Peers{addrs: addrs, conns: map[string]*grpc.ClientConn{}}
}

func (p *Peers) client(node string) (pb.SequencerServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cc := p.conns[node]; cc != nil {
		return pb.NewSequencerServiceClient(cc), nil
	}
	addr, ok := p.addrs[node]
	if !ok {
		return nil, fmt.Errorf("unknown sequencer %q", node)
	}
	// Internal traffic stays on the private network (compose network or
	// localhost); only Kafka and HTTPS listeners are exposed.
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		return nil, err
	}
	p.conns[node] = cc
	return pb.NewSequencerServiceClient(cc), nil
}

func (p *Peers) Call(ctx context.Context, node string, req *pb.Request) (*pb.Response, error) {
	c, err := p.client(node)
	if err != nil {
		return nil, err
	}
	return c.Call(ctx, req)
}

// Nodes lists configured node names.
func (p *Peers) Nodes() []string {
	var out []string
	for n := range p.addrs {
		out = append(out, n)
	}
	return out
}

func (p *Peers) Watch(ctx context.Context, node, agentID string) (pb.SequencerService_WatchClient, error) {
	c, err := p.client(node)
	if err != nil {
		return nil, err
	}
	return c.Watch(ctx, &pb.WatchRequest{AgentId: agentID})
}

func (p *Peers) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cc := range p.conns {
		cc.Close()
	}
}
