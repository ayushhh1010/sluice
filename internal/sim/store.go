package sim

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/proc"
)

// Store is a linearizable object store with real conditional-PUT semantics.
// Operations apply at one virtual instant; the world adds latency and
// faults around them.
type Store struct {
	objs map[string]simObj
	now  func() int64
	// Counters by op for cost reports.
	Ops map[string]int
}

type simObj struct {
	data []byte
	mod  int64
}

var (
	errUnavailable = errors.New("sim: 503 slow down")
	errTimeout     = errors.New("sim: request timed out")
)

func newStore(now func() int64) *Store {
	return &Store{objs: map[string]simObj{}, now: now, Ops: map[string]int{}}
}

func (s *Store) apply(r proc.StoreReq) proc.StoreRes {
	res := proc.StoreRes{ID: r.ID}
	s.Ops[r.Op.String()]++
	switch r.Op {
	case proc.OpGet:
		o, ok := s.objs[r.Key]
		if !ok {
			res.Err = objstore.ErrNotFound
			break
		}
		b := o.data
		off := min(r.Off, int64(len(b)))
		end := int64(len(b))
		if r.Len > 0 && off+r.Len < end {
			end = off + r.Len
		}
		res.Data = append([]byte(nil), b[off:end]...)
	case proc.OpPut, proc.OpPutIfAbsent:
		if _, ok := s.objs[r.Key]; ok && r.Op == proc.OpPutIfAbsent {
			res.Err = objstore.ErrPrecondition
			break
		}
		s.objs[r.Key] = simObj{append([]byte(nil), r.Data...), s.now()}
	case proc.OpHead:
		o, ok := s.objs[r.Key]
		if !ok {
			res.Err = objstore.ErrNotFound
			break
		}
		res.Size = int64(len(o.data))
	case proc.OpDelete:
		delete(s.objs, r.Key)
	case proc.OpList:
		var keys []string
		for k := range s.objs {
			if strings.HasPrefix(k, r.Prefix) && k > r.After {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		if r.Limit > 0 && len(keys) > r.Limit {
			keys = keys[:r.Limit]
		}
		for _, k := range keys {
			o := s.objs[k]
			res.List = append(res.List, objstore.ObjectInfo{Key: k, Size: int64(len(o.data)), LastModified: time.UnixMilli(o.mod)})
		}
	}
	return res
}

// The Store also implements objstore.Store (synchronously, without faults)
// so the checker can read the final bucket.

func (s *Store) Get(_ context.Context, key string, off, n int64) ([]byte, error) {
	r := s.apply(proc.StoreReq{Op: proc.OpGet, Key: key, Off: off, Len: n})
	return r.Data, r.Err
}

func (s *Store) Put(_ context.Context, key string, data []byte, ifAbsent bool) error {
	op := proc.OpPut
	if ifAbsent {
		op = proc.OpPutIfAbsent
	}
	return s.apply(proc.StoreReq{Op: op, Key: key, Data: data}).Err
}

func (s *Store) Head(_ context.Context, key string) (int64, error) {
	r := s.apply(proc.StoreReq{Op: proc.OpHead, Key: key})
	return r.Size, r.Err
}

func (s *Store) Delete(_ context.Context, key string) error {
	return s.apply(proc.StoreReq{Op: proc.OpDelete, Key: key}).Err
}

func (s *Store) List(_ context.Context, prefix, after string, limit int) ([]objstore.ObjectInfo, error) {
	r := s.apply(proc.StoreReq{Op: proc.OpList, Prefix: prefix, After: after, Limit: limit})
	return r.List, r.Err
}
