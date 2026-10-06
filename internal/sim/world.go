// Package sim is Sluice's deterministic simulator. It runs the real agent
// and sequencer cores on one thread in virtual time, with a simulated
// network and object store, injects faults (crashes, pauses, partitions,
// storage errors, lost responses, slowdowns), drives a client workload and
// hands the resulting history to the checker. Every run is a pure function
// of its 64-bit seed.
package sim

import (
	"container/heap"
	"fmt"
	"io"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/checker"
	"github.com/ayushhh1010/sluice/internal/proc"
	"github.com/ayushhh1010/sluice/internal/sequencer"
)

// Config selects the run. Knobs not set here are randomized from the seed.
type Config struct {
	Seed       uint64
	DurationMs int64 // workload duration in virtual time
	Faults     bool
	Trace      io.Writer // optional event trace
	// NoBuggify keeps default knobs instead of randomizing them.
	NoBuggify bool
}

// Result is the outcome of one run.
type Result struct {
	Seed        uint64
	Violations  []checker.Violation
	Ops         int    // completed client operations
	Fingerprint string // digest of the history and final state, for determinism checks
	Stats       map[string]int
	Knobs       Knobs
	History     *checker.History
}

const epochStart = int64(1_790_000_000_000) // virtual time starts in 2026

type event struct {
	at  int64
	seq uint64
	fn  func()
}

type evHeap []event

func (h evHeap) Len() int { return len(h) }
func (h evHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h evHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *evHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *evHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

type kind int

const (
	kindSeq kind = iota
	kindAgent
)

type node struct {
	name        string
	kind        kind
	inc         int // incarnation: bumps on crash, stale deliveries drop
	up          bool
	pausedUntil int64
	skew        int64
	seq         *sequencer.Core
	ag          *agent.Core
	nodeID      int32
}

func (n *node) step(now int64, ev any) []any {
	if n.kind == kindSeq {
		return n.seq.Step(now, ev)
	}
	return n.ag.Step(now, ev)
}

type pendingCall struct {
	from     *node
	inc      int
	id       uint64
	answered bool
}

type clientCont struct {
	c    *client
	fn   func(any)
	done bool
}

// World is one simulation run.
type World struct {
	cfg   Config
	knobs Knobs
	rnd   *proc.Rand
	now   int64
	q     evHeap
	seq   uint64

	store   *Store
	nodes   map[string]*node
	names   []string
	agents  []*node
	seqs    []*node
	calls   map[uint64]*pendingCall
	reqs    map[uint64]*clientCont
	nextID  uint64
	blocked map[[2]string]bool

	clients             []*client
	hist                checker.History
	topics              []topicSpec
	end                 int64
	faulty              bool
	slowUntil, errUntil int64

	stats                    map[string]int
	verifyEvery, verifyCount int
	verifyFailed             bool
	violations               []checker.Violation
	ops                      int
}

func (w *World) at(t int64, fn func()) {
	w.seq++
	heap.Push(&w.q, event{at: max(t, w.now), seq: w.seq, fn: fn})
}

func (w *World) tracef(format string, args ...any) {
	if w.cfg.Trace != nil {
		fmt.Fprintf(w.cfg.Trace, "%9d %s\n", w.now-epochStart, fmt.Sprintf(format, args...))
	}
}

func (w *World) id() uint64 {
	w.nextID++
	return w.nextID
}

// ---- Nodes ----

func (w *World) newCore(n *node) {
	seed := w.rnd.Uint64()
	k := w.knobs
	if n.kind == kindSeq {
		var peers []string
		for _, s := range w.seqs {
			if s != n {
				peers = append(peers, s.name)
			}
		}
		n.seq = sequencer.New(sequencer.Config{
			Node: n.name, Peers: peers, Cluster: "sim",
			CommitWindowMs: k.CommitWindowMs, CommitMaxRequests: k.CommitMaxRequests,
			LeaderTimeoutMs: k.LeaderTimeoutMs, StartupStaggerMs: 1000, PingIntervalMs: 500,
			SnapshotEvery: k.SnapshotEvery, SnapshotIntervalMs: 20_000,
			MemberTTLms: k.MemberTTLms, GCGraceMs: k.GCGraceMs, GCIntervalMs: 2_000,
			RetentionCheckMs: 3_000, OrphanSweepMs: k.OrphanSweepMs, OrphanMinAgeMs: k.OrphanMinAgeMs,
			MaxSegmentAgeMs: k.MaxSegmentAgeMs, RecentCommits: k.RecentCommits,
			MergeCheckMs: 2_000, MergeMinRefs: k.MergeMinRefs, MergeMinBytes: 1 << 40, MergeMaxBytes: 1 << 20, MergeSmallRefMax: 1 << 20,
			DefaultRetentionMs: k.RetentionMs, GroupMinSessionMs: 1000, GroupInitialDelayMs: -1, GroupHousekeepingMs: 100,
		}, seed)
		return
	}
	var seqNames []string
	for _, s := range w.seqs {
		seqNames = append(seqNames, s.name)
	}
	n.ag = agent.New(agent.Config{
		ID: n.name, NodeID: n.nodeID, Cluster: "sim", Sequencers: seqNames,
		Listeners:     []*pb.Listener{{Name: "sim", Host: n.name, Port: 9092}},
		FlushWindowMs: k.FlushWindowMs, FlushMaxBytes: k.FlushMaxBytes, BufferMaxBytes: k.BufferMaxBytes,
		RPCTimeoutMs: 2_000, CommitDeadlineMs: k.CommitDeadlineMs, HeartbeatMs: 1_000, IndexTTLms: 5_000, HWStaleMs: 2_000,
		TailCacheBytes: k.TailCacheBytes, DataCacheBytes: k.DataCacheBytes,
	}, seed)
}

func (w *World) start(n *node) {
	n.up = true
	n.inc++
	w.newCore(n)
	if n.kind == kindSeq {
		w.deliver(n, n.inc, sequencer.Start{})
	} else {
		w.deliver(n, n.inc, agent.Start{})
	}
}

func (w *World) crash(n *node) {
	if !n.up {
		return
	}
	w.stats["crash_"+kindName(n.kind)]++
	w.tracef("CRASH %s", n.name)
	n.up = false
	n.inc++
	n.seq, n.ag = nil, nil
}

func kindName(k kind) string {
	if k == kindSeq {
		return "sequencer"
	}
	return "agent"
}

// deliver runs ev on n now, unless n crashed since (inc) or is paused.
func (w *World) deliver(n *node, inc int, ev any) {
	if !n.up || n.inc != inc {
		return
	}
	if w.now < n.pausedUntil {
		w.at(n.pausedUntil, func() { w.deliver(n, inc, ev) })
		return
	}
	w.handle(n, n.step(w.now+n.skew, ev))
	if n.kind == kindSeq && n.seq != nil && w.verifyEvery > 0 {
		if w.verifyCount++; w.verifyCount%w.verifyEvery == 0 {
			if st := n.seq.State(); st != nil {
				if err := st.Verify(); err != nil && !w.verifyFailed {
					w.verifyFailed = true
					w.violation("STATE", "%s (%s) at seq %d: %v", n.name, n.seq.Role(), st.Seq, err)
				}
			}
		}
	}
}

func (w *World) handle(n *node, effects []any) {
	inc := n.inc
	for _, e := range effects {
		switch e := e.(type) {
		case proc.WakeAt:
			w.at(e.At-n.skew, func() { w.deliver(n, inc, proc.Tick{}) })
		case proc.StoreReq:
			w.storeOp(n, inc, e)
		case proc.Call:
			w.call(n, inc, e)
		case sequencer.Reply:
			w.reply(n, e)
		case sequencer.Notify:
			w.notify(n, e)
		case sequencer.Fatal:
			w.violation("FATAL", "%s: %v", n.name, e.Err)
		case sequencer.RoleChange:
			w.tracef("ROLE %s %s epoch=%d", n.name, e.Role, e.Epoch)
			w.stats["role_"+e.Role]++
		case agent.ProduceResp:
			w.toClient(n, e.ID, e)
		case agent.FetchResp:
			w.toClient(n, e.ID, e)
		case agent.ForwardResp:
			w.toClient(n, e.ID, e)
		}
	}
}

func (w *World) violation(inv, format string, args ...any) {
	v := checker.Violation{Invariant: inv, Detail: fmt.Sprintf(format, args...)}
	w.tracef("VIOLATION %s", v)
	w.violations = append(w.violations, v)
}

// ---- Object store with faults ----

func (w *World) storeLatency() int64 {
	d := 5 + w.rnd.Int63n(25)
	if w.rnd.Intn(50) == 0 {
		d += w.rnd.Int63n(400) // tail latency
	}
	if w.now < w.slowUntil {
		d *= 10
	}
	return d
}

func (w *World) storeOp(n *node, inc int, r proc.StoreReq) {
	if r.Op == proc.OpDelete && !strings.Contains(r.Key, "/meta/") {
		w.tracef("DELETE %s by %s", r.Key, n.name)
	}
	w.at(w.now+w.storeLatency(), func() {
		var res proc.StoreRes
		errP := w.knobs.StoreErrorPct
		if w.now < w.errUntil {
			errP = 30
		}
		mutating := r.Op == proc.OpPut || r.Op == proc.OpPutIfAbsent || r.Op == proc.OpDelete
		switch {
		case w.faulty && w.rnd.Intn(100) < errP:
			res = proc.StoreRes{ID: r.ID, Err: errUnavailable}
			w.stats["store_error"]++
		case w.faulty && mutating && w.rnd.Intn(100) < w.knobs.LostResponsePct:
			// Applied, but the response is lost: the caller sees a timeout.
			w.store.apply(r)
			res = proc.StoreRes{ID: r.ID, Err: errTimeout}
			w.stats["store_lost_response"]++
		default:
			res = w.store.apply(r)
		}
		w.at(w.now+w.storeLatency(), func() { w.deliver(n, inc, res) })
	})
}

// ---- Network ----

func (w *World) netLatency() int64 {
	d := 1 + w.rnd.Int63n(10)
	if w.rnd.Intn(100) == 0 {
		d += w.rnd.Int63n(200)
	}
	return d
}

func (w *World) linkDown(a, b string) bool {
	return w.blocked[[2]string{a, b}] || w.blocked[[2]string{b, a}]
}

func (w *World) dropped(a, b string) bool {
	if w.linkDown(a, b) {
		return true
	}
	return w.faulty && w.rnd.Intn(1000) < w.knobs.DropPerMille
}

func cloneReq(r *pb.Request) *pb.Request    { return roundTrip(r).(*pb.Request) }
func cloneResp(r *pb.Response) *pb.Response { return roundTrip(r).(*pb.Response) }

// roundTrip marshals and unmarshals a message, as the wire would.
func roundTrip(m proto.Message) proto.Message {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	out := m.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(b, out); err != nil {
		panic(err)
	}
	return out
}

func (w *World) call(n *node, inc int, c proc.Call) {
	id := w.id()
	pc := &pendingCall{from: n, inc: inc, id: c.ID}
	w.calls[id] = pc
	w.at(w.now+c.TimeoutMs, func() {
		if !pc.answered {
			pc.answered = true
			delete(w.calls, id)
			w.deliver(n, inc, proc.CallRes{ID: c.ID, Err: errTimeout})
		}
	})
	target := w.nodes[c.To]
	if target == nil || w.dropped(n.name, c.To) {
		return
	}
	req := cloneReq(c.Req)
	tinc := target.inc
	w.at(w.now+w.netLatency(), func() { w.deliver(target, tinc, sequencer.Request{ID: id, Req: req}) })
}

func (w *World) reply(n *node, r sequencer.Reply) {
	pc := w.calls[r.ID]
	if pc == nil || w.dropped(n.name, pc.from.name) {
		return
	}
	resp := cloneResp(r.Resp)
	w.at(w.now+w.netLatency(), func() {
		if pc.answered {
			return
		}
		pc.answered = true
		delete(w.calls, r.ID)
		w.deliver(pc.from, pc.inc, proc.CallRes{ID: pc.id, Resp: resp})
	})
}

func (w *World) notify(n *node, nt sequencer.Notify) {
	target := w.nodes[nt.Agent]
	if target == nil || w.dropped(n.name, nt.Agent) {
		return
	}
	ev := roundTrip(nt.Event).(*pb.WatchEvent)
	tinc := target.inc
	w.at(w.now+w.netLatency(), func() { w.deliver(target, tinc, agent.Watch{Event: ev}) })
}

// ---- Clients <-> agents ----

// send delivers a client request to an agent. fn gets the response, or nil
// after timeoutMs.
func (w *World) send(c *client, to *node, build func(id uint64) any, timeoutMs int64, fn func(any)) {
	id := w.id()
	cont := &clientCont{c: c, fn: fn}
	w.reqs[id] = cont
	w.at(w.now+timeoutMs, func() {
		if !cont.done {
			cont.done = true
			delete(w.reqs, id)
			fn(nil)
		}
	})
	if w.dropped(c.name, to.name) {
		return
	}
	ev := build(id)
	tinc := to.inc
	w.at(w.now+w.netLatency(), func() { w.deliver(to, tinc, ev) })
}

func (w *World) toClient(n *node, id uint64, resp any) {
	cont := w.reqs[id]
	if cont == nil || w.dropped(n.name, cont.c.name) {
		return
	}
	w.at(w.now+w.netLatency(), func() {
		if cont.done {
			return
		}
		cont.done = true
		delete(w.reqs, id)
		cont.fn(resp)
	})
}

// ---- Faults ----

func (w *World) scheduleFaults() {
	if !w.cfg.Faults {
		return
	}
	var tick func()
	tick = func() {
		if w.now >= w.end {
			return
		}
		w.injectFault()
		w.at(w.now+w.knobs.FaultEveryMs/2+w.rnd.Int63n(w.knobs.FaultEveryMs), tick)
	}
	w.at(w.now+3_000, tick)
}

func (w *World) pick() *node { return w.nodes[w.names[w.rnd.Intn(len(w.names))]] }

func (w *World) injectFault() {
	switch r := w.rnd.Intn(100); {
	case r < 30: // crash and restart later
		n := w.pick()
		if !n.up {
			return
		}
		w.crash(n)
		w.at(w.now+500+w.rnd.Int63n(8_000), func() {
			if !n.up { // heal may have restarted it already
				w.start(n)
				w.tracef("RESTART %s", n.name)
			}
		})
	case r < 50: // pause: a long GC pause or VM freeze makes a zombie
		n := w.pick()
		d := 500 + w.rnd.Int63n(w.knobs.LeaderTimeoutMs*2)
		n.pausedUntil = max(n.pausedUntil, w.now+d)
		w.stats["pause"]++
		w.tracef("PAUSE %s for %dms", n.name, d)
	case r < 70: // partition two nodes from each other
		a, b := w.pick(), w.pick()
		if a == b {
			return
		}
		k := [2]string{a.name, b.name}
		w.blocked[k] = true
		w.stats["partition"]++
		w.tracef("PARTITION %s <-> %s", a.name, b.name)
		w.at(w.now+1_000+w.rnd.Int63n(10_000), func() { delete(w.blocked, k) })
	case r < 80: // isolate a node from every other node (not from storage)
		n := w.pick()
		var ks [][2]string
		for _, o := range w.names {
			if o != n.name {
				k := [2]string{n.name, o}
				w.blocked[k] = true
				ks = append(ks, k)
			}
		}
		for _, c := range w.clients {
			k := [2]string{n.name, c.name}
			w.blocked[k] = true
			ks = append(ks, k)
		}
		w.stats["isolate"]++
		w.tracef("ISOLATE %s", n.name)
		w.at(w.now+1_000+w.rnd.Int63n(15_000), func() {
			for _, k := range ks {
				delete(w.blocked, k)
			}
		})
	case r < 90:
		w.slowUntil = w.now + 2_000 + w.rnd.Int63n(8_000)
		w.stats["store_slowdown"]++
		w.tracef("STORE SLOWDOWN")
	default:
		w.errUntil = w.now + 1_000 + w.rnd.Int63n(4_000)
		w.stats["store_error_burst"]++
		w.tracef("STORE ERROR BURST")
	}
}

// heal ends every fault so the system can quiesce.
func (w *World) heal() {
	w.faulty = false
	w.blocked = map[[2]string]bool{}
	w.slowUntil, w.errUntil = 0, 0
	for _, name := range w.names {
		n := w.nodes[name]
		n.pausedUntil = 0
		if !n.up {
			w.start(n)
		}
	}
}

// Run executes one simulation.
func Run(cfg Config) Result {
	if cfg.DurationMs == 0 {
		cfg.DurationMs = 60_000
	}
	w := &World{
		cfg: cfg, rnd: proc.NewRand(cfg.Seed), now: epochStart,
		nodes: map[string]*node{}, calls: map[uint64]*pendingCall{}, reqs: map[uint64]*clientCont{},
		blocked: map[[2]string]bool{}, stats: map[string]int{}, faulty: cfg.Faults, verifyEvery: 50,
	}
	w.knobs = defaultKnobs()
	if !cfg.NoBuggify {
		w.knobs = buggify(w.rnd)
	}
	w.store = newStore(func() int64 { return w.now })
	w.end = w.now + cfg.DurationMs

	for _, name := range []string{"seq-a", "seq-b"} {
		n := &node{name: name, kind: kindSeq}
		w.nodes[name], w.seqs = n, append(w.seqs, n)
	}
	for i := 1; i <= w.knobs.Agents; i++ {
		n := &node{name: fmt.Sprintf("agent-%d", i), kind: kindAgent, nodeID: int32(i)}
		w.nodes[n.name], w.agents = n, append(w.agents, n)
	}
	for name := range w.nodes {
		w.names = append(w.names, name)
	}
	sort.Strings(w.names)
	if cfg.Faults {
		for _, name := range w.names {
			w.nodes[name].skew = w.rnd.Int63n(2_001) - 1_000 // clocks disagree by up to 1s
		}
	}
	for _, name := range w.names {
		w.start(w.nodes[name])
	}
	w.setupWorkload()
	w.scheduleFaults()

	// Workload phase, then heal and let in-flight work settle.
	healed := false
	quiesceEnd := w.end + 60_000
	for len(w.q) > 0 {
		e := heap.Pop(&w.q).(event)
		w.now = e.at
		if !healed && w.now >= w.end {
			healed = true
			w.heal()
			w.tracef("HEAL")
		}
		if w.now > quiesceEnd {
			break
		}
		e.fn()
		if healed && w.idle() && w.now > w.end+15_000 {
			break
		}
	}
	if captured != nil {
		captured(w)
	}
	return w.finish()
}

// captured lets tests inspect a finished world.
var captured func(*World)

// idle reports whether every client finished its last operation.
func (w *World) idle() bool {
	for _, c := range w.clients {
		if c.busy {
			return false
		}
	}
	return true
}
