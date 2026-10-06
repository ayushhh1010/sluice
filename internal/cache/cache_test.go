package cache

import (
	"bytes"
	"testing"
)

func TestTailServesContiguousRunAndEvictsOldest(t *testing.T) {
	c := NewTail[string](10)
	c.Add("p", Batch{Base: 0, Records: 2, Data: []byte("aaaa")})
	c.Add("p", Batch{Base: 2, Records: 1, Data: []byte("bbb")})
	bs, ok := c.Read("p", 1, 100)
	if !ok || len(bs) != 2 || bs[0].Base != 0 {
		t.Fatalf("read from inside first batch: %v %v", bs, ok)
	}
	if _, ok := c.Read("p", 3, 100); ok {
		t.Fatal("read at the end of the run must miss")
	}
	// A gap restarts the run.
	c.Add("p", Batch{Base: 10, Records: 1, Data: []byte("c")})
	if _, ok := c.Read("p", 0, 100); ok {
		t.Fatal("stale run still served after a gap")
	}
	// The byte budget evicts the oldest batches first.
	c.Add("q", Batch{Base: 0, Records: 1, Data: []byte("0123456789")})
	if c.Size() > 10 {
		t.Fatalf("size %d over budget", c.Size())
	}
	if bs, ok := c.Read("q", 0, 1); !ok || !bytes.Equal(bs[0].Data, []byte("0123456789")) {
		t.Fatal("newest batch evicted")
	}
}

func TestLRUBoundedByBytes(t *testing.T) {
	c := NewLRU(8)
	c.Add(RangeKey{"a", 0, 4}, []byte("aaaa"))
	c.Add(RangeKey{"b", 0, 4}, []byte("bbbb"))
	c.Get(RangeKey{"a", 0, 4}) // a is now most recent
	c.Add(RangeKey{"c", 0, 4}, []byte("cccc"))
	if _, ok := c.Get(RangeKey{"b", 0, 4}); ok {
		t.Fatal("least recently used entry survived")
	}
	if _, ok := c.Get(RangeKey{"a", 0, 4}); !ok {
		t.Fatal("recently used entry evicted")
	}
	c.Drop("a")
	if _, ok := c.Get(RangeKey{"a", 0, 4}); ok || c.Size() != 4 {
		t.Fatal("drop failed")
	}
}
