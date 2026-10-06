package objstore

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// Instrumented wraps a store, adds latency (base plus uniform jitter, to
// model a remote object store on a laptop) and counts requests by billing
// class. sluice-bench uses it to measure PUTs per GB reproducibly.
type Instrumented struct {
	Store
	Base, Jitter time.Duration
	ClassA       atomic.Int64
	ClassB       atomic.Int64
	BytesPut     atomic.Int64
}

func (s *Instrumented) wait(ctx context.Context) error {
	d := s.Base
	if s.Jitter > 0 {
		d += time.Duration(rand.Int64N(int64(s.Jitter)))
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Instrumented) Get(ctx context.Context, key string, off, n int64) ([]byte, error) {
	s.ClassB.Add(1)
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.Store.Get(ctx, key, off, n)
}

func (s *Instrumented) Put(ctx context.Context, key string, data []byte, ifAbsent bool) error {
	s.ClassA.Add(1)
	s.BytesPut.Add(int64(len(data)))
	if err := s.wait(ctx); err != nil {
		return err
	}
	return s.Store.Put(ctx, key, data, ifAbsent)
}

func (s *Instrumented) Head(ctx context.Context, key string) (int64, error) {
	s.ClassB.Add(1)
	if err := s.wait(ctx); err != nil {
		return 0, err
	}
	return s.Store.Head(ctx, key)
}

func (s *Instrumented) List(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, error) {
	s.ClassA.Add(1)
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.Store.List(ctx, prefix, after, limit)
}
