// Package proc defines the contract between the deterministic (sans-I/O)
// cores and whatever drives them: the real runtime in production, or the
// single-threaded simulator.
//
// A core is a state machine: Step(now, event) returns effects. Cores never
// read the clock, use global randomness, start goroutines or perform I/O.
// Every effect that asks for a result (StoreReq, Call) is answered by exactly
// one matching result event, possibly an error. Drivers own deadlines.
package proc

import (
	"container/heap"

	"github.com/ayushhh1010/sluice/internal/objstore"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
)

// Op is an object store operation.
type Op uint8

const (
	OpGet Op = iota
	OpPut
	OpPutIfAbsent
	OpHead
	OpDelete
	OpList
)

func (o Op) String() string {
	return [...]string{"get", "put", "put", "head", "delete", "list"}[o]
}

// StoreReq is an effect: perform one object store operation.
type StoreReq struct {
	ID     uint64
	Op     Op
	Key    string
	Off    int64 // OpGet: range start
	Len    int64 // OpGet: range length; 0 = whole object
	Data   []byte
	Prefix string // OpList
	After  string // OpList
	Limit  int    // OpList
}

// StoreRes is the event answering a StoreReq. Err is nil,
// objstore.ErrNotFound, objstore.ErrPrecondition, or a transient error.
type StoreRes struct {
	ID   uint64
	Data []byte
	Size int64
	List []objstore.ObjectInfo
	Err  error
}

// Call is an effect: send an RPC to node To. The driver answers with exactly
// one CallRes, at the latest after TimeoutMs.
type Call struct {
	ID        uint64
	To        string
	Req       *pb.Request
	TimeoutMs int64
}

// CallRes answers a Call.
type CallRes struct {
	ID   uint64
	Resp *pb.Response
	Err  error
}

// WakeAt is an effect: deliver a Tick at or after At (ms). Only the latest
// WakeAt matters; drivers may deliver extra Ticks.
type WakeAt struct{ At int64 }

// Tick is a timer event.
type Tick struct{}

// Observe is an effect: record one histogram sample (milliseconds or bytes)
// for metric Name. The simulator may ignore it.
type Observe struct {
	Name  string
	Value float64
}

// Gauge is an effect: set gauge Name to Value.
type Gauge struct {
	Name  string
	Value float64
}

// Timers is a deterministic timer heap: ties fire in insertion order.
type Timers struct {
	h   timerHeap
	seq uint64
}

type timer struct {
	at  int64
	seq uint64
	fn  func(now int64)
}

type timerHeap []timer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(timer)) }
func (h *timerHeap) Pop() any {
	old := *h
	t := old[len(old)-1]
	*h = old[:len(old)-1]
	return t
}

// At schedules fn to run at time at.
func (t *Timers) At(at int64, fn func(now int64)) {
	t.seq++
	heap.Push(&t.h, timer{at: at, seq: t.seq, fn: fn})
}

// Run fires every timer due at now, including timers scheduled by fired
// timers, and returns the next deadline (0 if none).
func (t *Timers) Run(now int64) int64 {
	for len(t.h) > 0 && t.h[0].at <= now {
		tm := heap.Pop(&t.h).(timer)
		tm.fn(now)
	}
	return t.Next()
}

// Next returns the earliest deadline, or 0 if no timers are pending.
func (t *Timers) Next() int64 {
	if len(t.h) == 0 {
		return 0
	}
	return t.h[0].at
}

// Reset drops all timers.
func (t *Timers) Reset() { t.h = nil }

// Rand is splitmix64: tiny, fast and fully determined by its seed.
type Rand struct{ s uint64 }

func NewRand(seed uint64) *Rand { return &Rand{s: seed} }

func (r *Rand) Uint64() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a value in [0, n).
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}

// Int63n returns a value in [0, n).
func (r *Rand) Int63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(r.Uint64() % uint64(n))
}

// Float64 returns a value in [0, 1).
func (r *Rand) Float64() float64 { return float64(r.Uint64()>>11) / (1 << 53) }

// Backoff returns a jittered exponential delay for attempt (0-based).
func (r *Rand) Backoff(attempt int, baseMs, maxMs int64) int64 {
	d := baseMs << min(attempt, 20)
	if d > maxMs || d <= 0 {
		d = maxMs
	}
	return d/2 + r.Int63n(d/2+1)
}
