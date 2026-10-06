package objstore

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// Mem is a linearizable in-memory store.
type Mem struct {
	mu   sync.Mutex
	objs map[string]memObj
}

type memObj struct {
	data []byte
	mod  time.Time
}

func NewMem() *Mem { return &Mem{objs: map[string]memObj{}} }

func (m *Mem) Get(_ context.Context, key string, off, n int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return nil, ErrNotFound
	}
	return slice(o.data, off, n), nil
}

func (m *Mem) Put(_ context.Context, key string, data []byte, ifAbsent bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[key]; ok && ifAbsent {
		return ErrPrecondition
	}
	m.objs[key] = memObj{append([]byte(nil), data...), time.Now()}
	return nil
}

func (m *Mem) Head(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return 0, ErrNotFound
	}
	return int64(len(o.data)), nil
}

func (m *Mem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

func (m *Mem) List(_ context.Context, prefix, after string, limit int) ([]ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ObjectInfo
	for k, o := range m.objs {
		if strings.HasPrefix(k, prefix) && k > after {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(o.data)), LastModified: o.mod})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// slice returns a copy of n bytes at off (whole object when n <= 0),
// clamped to the object like an HTTP range request.
func slice(b []byte, off, n int64) []byte {
	if off > int64(len(b)) {
		off = int64(len(b))
	}
	end := int64(len(b))
	if n > 0 && off+n < end {
		end = off + n
	}
	return append([]byte(nil), b[off:end]...)
}
