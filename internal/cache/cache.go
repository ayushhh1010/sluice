// Package cache holds the agent's soft state: a write-through tail cache per
// partition and a byte-bounded LRU of object ranges. Both are deterministic
// (no clocks), so they run unchanged inside the simulator.
package cache

import (
	"container/list"
	"sort"
)

// ---- Data LRU ----

// RangeKey names a byte range of an object.
type RangeKey struct {
	Object   string
	Off, Len uint32
}

// LRU is a byte-bounded least-recently-used cache.
type LRU struct {
	max, size int64
	ll        *list.List
	items     map[RangeKey]*list.Element
	Hits      uint64
	Misses    uint64
}

type lruEntry struct {
	k RangeKey
	v []byte
}

func NewLRU(maxBytes int64) *LRU {
	return &LRU{max: maxBytes, ll: list.New(), items: map[RangeKey]*list.Element{}}
}

func (c *LRU) Get(k RangeKey) ([]byte, bool) {
	if e, ok := c.items[k]; ok {
		c.ll.MoveToFront(e)
		c.Hits++
		return e.Value.(*lruEntry).v, true
	}
	c.Misses++
	return nil, false
}

func (c *LRU) Add(k RangeKey, v []byte) {
	if int64(len(v)) > c.max {
		return
	}
	if e, ok := c.items[k]; ok {
		c.ll.MoveToFront(e)
		return
	}
	c.items[k] = c.ll.PushFront(&lruEntry{k, v})
	c.size += int64(len(v))
	for c.size > c.max {
		e := c.ll.Back()
		ent := e.Value.(*lruEntry)
		c.ll.Remove(e)
		delete(c.items, ent.k)
		c.size -= int64(len(ent.v))
	}
}

// Drop removes every cached range of an object (it was deleted).
func (c *LRU) Drop(object string) {
	for k, e := range c.items {
		if k.Object == object {
			c.size -= int64(len(e.Value.(*lruEntry).v))
			c.ll.Remove(e)
			delete(c.items, k)
		}
	}
}

func (c *LRU) Size() int64 { return c.size }

// ---- Tail cache ----

// Batch is one committed batch with its assigned base offset.
type Batch struct {
	Base    int64
	Records int64
	Data    []byte
}

type tail struct {
	batches []Batch // contiguous offsets
	bytes   int64
}

func (t *tail) end() int64 {
	if len(t.batches) == 0 {
		return -1
	}
	b := t.batches[len(t.batches)-1]
	return b.Base + b.Records
}

// Tail keeps the most recent committed batches of each partition so tail
// consumers never touch object storage. A global byte budget evicts the
// oldest insertions first.
type Tail[K comparable] struct {
	max, size int64
	parts     map[K]*tail
	order     []tailRef[K] // insertion order, for eviction
	Hits      uint64
	Misses    uint64
}

type tailRef[K comparable] struct {
	k    K
	base int64
}

func NewTail[K comparable](maxBytes int64) *Tail[K] {
	return &Tail[K]{max: maxBytes, parts: map[K]*tail{}}
}

// Add records a committed batch. A batch that does not extend the cached
// run restarts it.
func (c *Tail[K]) Add(k K, b Batch) {
	t := c.parts[k]
	if t == nil {
		t = &tail{}
		c.parts[k] = t
	}
	if e := t.end(); e != b.Base {
		if e > b.Base {
			return // already cached (or older than the cached run)
		}
		c.size -= t.bytes
		t.batches, t.bytes = nil, 0
	}
	t.batches = append(t.batches, b)
	t.bytes += int64(len(b.Data))
	c.size += int64(len(b.Data))
	c.order = append(c.order, tailRef[K]{k, b.Base})
	for c.size > c.max && len(c.order) > 0 {
		r := c.order[0]
		c.order = c.order[1:]
		if t := c.parts[r.k]; t != nil && len(t.batches) > 0 && t.batches[0].Base == r.base {
			c.size -= int64(len(t.batches[0].Data))
			t.bytes -= int64(len(t.batches[0].Data))
			t.batches = t.batches[1:]
			if len(t.batches) == 0 {
				delete(c.parts, r.k)
			}
		}
	}
	if len(c.order) > 4*len(c.parts)+1024 { // compact stale refs
		live := c.order[:0]
		for _, r := range c.order {
			if t := c.parts[r.k]; t != nil && len(t.batches) > 0 && r.base >= t.batches[0].Base {
				live = append(live, r)
			}
		}
		c.order = live
	}
}

// Read returns cached batches starting with the one containing offset, up
// to maxBytes (at least one batch). ok is false when offset is not cached.
func (c *Tail[K]) Read(k K, offset int64, maxBytes int64) ([]Batch, bool) {
	t := c.parts[k]
	if t == nil || len(t.batches) == 0 || offset < t.batches[0].Base || offset >= t.end() {
		c.Misses++
		return nil, false
	}
	i := sort.Search(len(t.batches), func(i int) bool { return t.batches[i].Base+t.batches[i].Records > offset })
	var out []Batch
	var n int64
	for ; i < len(t.batches); i++ {
		if len(out) > 0 && n+int64(len(t.batches[i].Data)) > maxBytes {
			break
		}
		out = append(out, t.batches[i])
		n += int64(len(t.batches[i].Data))
	}
	c.Hits++
	return out, true
}

// Drop forgets a partition.
func (c *Tail[K]) Drop(k K) {
	if t := c.parts[k]; t != nil {
		c.size -= t.bytes
		delete(c.parts, k)
	}
}

func (c *Tail[K]) Size() int64 { return c.size }
