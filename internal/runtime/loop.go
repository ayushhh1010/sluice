// Package runtime drives the deterministic cores with real goroutines,
// sockets, clocks and object storage. It is deliberately thin: everything
// that decides anything lives in the cores, which the simulator runs too.
package runtime

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayushhh1010/sluice/internal/metrics"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/proc"
)

// Loop runs one core on a single goroutine. Effects that need I/O run on
// their own goroutines and feed results back as events.
type Loop struct {
	step     func(now int64, ev any) []any
	in       chan any
	store    objstore.Store
	peers    *Peers
	m        *metrics.Metrics
	log      *slog.Logger
	onEffect func(any) // core-specific effects

	storeTimeout time.Duration
	nextID       atomic.Uint64

	mu      sync.Mutex
	waiters map[uint64]chan any

	costMu sync.Mutex
	classA []costSample
	classB []costSample
	aTotal int64
	bTotal int64
}

type costSample struct {
	at time.Time
	n  int64
}

func NewLoop(step func(int64, any) []any, store objstore.Store, peers *Peers, m *metrics.Metrics, log *slog.Logger, onEffect func(any)) *Loop {
	return &Loop{
		step: step, in: make(chan any, 4096), store: store, peers: peers, m: m, log: log, onEffect: onEffect,
		storeTimeout: 30 * time.Second, waiters: map[uint64]chan any{},
	}
}

func now() int64 { return time.Now().UnixMilli() }

// Submit delivers an event to the core.
func (l *Loop) Submit(ev any) { l.in <- ev }

// NewID returns a request ID unique within this loop.
func (l *Loop) NewID() uint64 { return l.nextID.Add(1) }

// Request submits an event built around a fresh ID and waits for the
// matching response delivered with Deliver.
func (l *Loop) Request(ctx context.Context, build func(id uint64) any) (any, error) {
	id := l.NewID()
	ch := make(chan any, 1)
	l.mu.Lock()
	l.waiters[id] = ch
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.waiters, id)
		l.mu.Unlock()
	}()
	l.Submit(build(id))
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Begin submits an event built around a fresh ID from the calling
// goroutine, so events submitted one after another by the same goroutine
// reach the core in that order, and returns a channel for the response.
// done must be called once the caller stops waiting.
func (l *Loop) Begin(build func(id uint64) any) (resp <-chan any, done func()) {
	id := l.NewID()
	ch := make(chan any, 1)
	l.mu.Lock()
	l.waiters[id] = ch
	l.mu.Unlock()
	l.Submit(build(id))
	return ch, func() {
		l.mu.Lock()
		delete(l.waiters, id)
		l.mu.Unlock()
	}
}

// Deliver hands a response to the goroutine waiting on id, if any.
func (l *Loop) Deliver(id uint64, resp any) {
	l.mu.Lock()
	ch := l.waiters[id]
	l.mu.Unlock()
	if ch != nil {
		ch <- resp
	}
}

// Run processes events until ctx is done.
func (l *Loop) Run(ctx context.Context, start any) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	handle := func(effects []any) {
		for _, e := range effects {
			switch e := e.(type) {
			case proc.WakeAt:
				// Each step ends with the core's earliest deadline.
				timer.Reset(max(time.Duration(e.At-now())*time.Millisecond, 0))
			case proc.StoreReq:
				go l.doStore(ctx, e)
			case proc.Call:
				go l.doCall(ctx, e)
			case proc.Observe:
				if l.m != nil {
					l.m.Observe(e.Name, e.Value)
				}
			case proc.Gauge:
				if l.m != nil {
					l.m.Gauge(e.Name, e.Value)
				}
			default:
				l.onEffect(e)
			}
		}
	}
	handle(l.step(now(), start))
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-l.in:
			handle(l.step(now(), ev))
		case <-timer.C:
			handle(l.step(now(), proc.Tick{}))
		}
	}
}

func (l *Loop) doStore(ctx context.Context, r proc.StoreReq) {
	ctx, cancel := context.WithTimeout(ctx, l.storeTimeout)
	defer cancel()
	start := time.Now()
	res := proc.StoreRes{ID: r.ID}
	switch r.Op {
	case proc.OpGet:
		res.Data, res.Err = l.store.Get(ctx, r.Key, r.Off, r.Len)
	case proc.OpPut, proc.OpPutIfAbsent:
		res.Err = l.store.Put(ctx, r.Key, r.Data, r.Op == proc.OpPutIfAbsent)
	case proc.OpHead:
		res.Size, res.Err = l.store.Head(ctx, r.Key)
	case proc.OpDelete:
		res.Err = l.store.Delete(ctx, r.Key)
	case proc.OpList:
		res.List, res.Err = l.store.List(ctx, r.Prefix, r.After, r.Limit)
	}
	op := r.Op.String()
	status := "ok"
	switch {
	case errors.Is(res.Err, objstore.ErrNotFound):
		status = "not_found"
	case errors.Is(res.Err, objstore.ErrPrecondition):
		status = "precondition"
	case res.Err != nil:
		status = "error"
		l.log.Warn("object store error", "op", op, "key", r.Key, "err", res.Err)
	}
	class := objstore.Class(op)
	if l.m != nil {
		l.m.ObjRequests.WithLabelValues(op, class, status).Inc()
		l.m.ObjLatency.WithLabelValues(op).Observe(time.Since(start).Seconds())
	}
	l.countCost(class)
	l.Submit(res)
}

func (l *Loop) doCall(ctx context.Context, c proc.Call) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.TimeoutMs)*time.Millisecond)
	defer cancel()
	resp, err := l.peers.Call(ctx, c.To, c.Req)
	l.Submit(proc.CallRes{ID: c.ID, Resp: resp, Err: err})
}

// ---- Cost estimate ----
//
// R2: Class A (PUT, LIST) $4.50 per million, Class B (GET, HEAD) $0.36 per
// million, with 1M A and 10M B free each month. The estimate projects the
// last ten minutes' request rate over a 30-day month.

const costWindow = 10 * time.Minute

func (l *Loop) countCost(class string) {
	l.costMu.Lock()
	defer l.costMu.Unlock()
	t := time.Now().Truncate(10 * time.Second)
	add := func(s *[]costSample, total *int64) {
		if n := len(*s); n > 0 && (*s)[n-1].at.Equal(t) {
			(*s)[n-1].n++
		} else {
			*s = append(*s, costSample{t, 1})
		}
		*total++
		for len(*s) > 0 && time.Since((*s)[0].at) > costWindow {
			*total -= (*s)[0].n
			*s = (*s)[1:]
		}
	}
	switch class {
	case "A":
		add(&l.classA, &l.aTotal)
	case "B":
		add(&l.classB, &l.bTotal)
	}
	if l.m != nil {
		perMonth := 30 * 24 * time.Hour.Seconds() / costWindow.Seconds()
		a := max(float64(l.aTotal)*perMonth-1e6, 0) / 1e6 * 4.50
		b := max(float64(l.bTotal)*perMonth-10e6, 0) / 1e6 * 0.36
		l.m.CostMonth.Set(a + b)
	}
}
