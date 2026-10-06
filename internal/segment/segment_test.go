package segment

import (
	"bytes"
	"testing"

	"github.com/ayushhh1010/sluice/internal/kbatch"
	"pgregory.net/rapid"
)

func TestRoundTripGroupsByPartition(t *testing.T) {
	b := NewBuilder(42, "agent-1")
	t1, t2 := [16]byte{1}, [16]byte{2}
	batches := [][]byte{
		kbatch.Build(1, []kbatch.Record{{Value: []byte("a")}}),
		kbatch.Build(2, []kbatch.Record{{Value: []byte("b")}}),
		kbatch.Build(3, []kbatch.Record{{Value: []byte("c")}}),
	}
	b.Add(Entry{TopicID: t2, Partition: 0, Records: 1}, batches[0])
	b.Add(Entry{TopicID: t1, Partition: 1, Records: 1}, batches[1])
	b.Add(Entry{TopicID: t2, Partition: 0, Records: 1}, batches[2])
	obj, entries, order := b.Finish()

	h, got, err := Parse(obj)
	if err != nil {
		t.Fatal(err)
	}
	if h.CreatedMs != 42 || string(bytes.TrimRight(h.AgentID[:], "\x00")) != "agent-1" {
		t.Fatalf("header %+v", h)
	}
	if len(got) != 3 || got[0].TopicID != t1 || got[1].TopicID != t2 || got[2].TopicID != t2 {
		t.Fatalf("not grouped: %+v", got)
	}
	// Insertion order is kept within a partition.
	if order[1] != 0 || order[2] != 2 || order[0] != 1 {
		t.Fatalf("order %v", order)
	}
	for i, e := range got {
		if e != entries[i] || !bytes.Equal(Batch(obj, e), batches[order[i]]) {
			t.Fatalf("entry %d mismatch", i)
		}
	}
}

func TestParseRejectsCorruption(t *testing.T) {
	b := NewBuilder(1, "a")
	b.Add(Entry{Records: 1}, kbatch.Build(1, []kbatch.Record{{Value: []byte("x")}}))
	obj, _, _ := b.Finish()
	for i := range obj {
		c := append([]byte(nil), obj...)
		c[i] ^= 0x40
		if _, ents, err := Parse(c); err == nil {
			// Flips inside batch bytes are not covered by the footer CRC
			// (the batch has its own CRC); everything else must fail.
			if i < HeaderSize || i >= int(ents[0].Offset+ents[0].Length) {
				if i >= 6 && i < HeaderSize { // flags, created and agent fields are informational
					continue
				}
				t.Fatalf("flip at %d accepted", i)
			}
		}
	}
}

func TestRapidRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 20).Draw(t, "n")
		b := NewBuilder(rapid.Int64().Draw(t, "ts"), "x")
		var want [][]byte
		for i := 0; i < n; i++ {
			var tid [16]byte
			tid[0] = byte(rapid.IntRange(0, 3).Draw(t, "topic"))
			data := rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "data")
			want = append(want, data)
			b.Add(Entry{TopicID: tid, Partition: uint32(rapid.IntRange(0, 3).Draw(t, "p")), Records: 1}, data)
		}
		obj, entries, order := b.Finish()
		_, got, err := Parse(obj)
		if err != nil || len(got) != n {
			t.Fatalf("parse: %v", err)
		}
		for i := range got {
			if got[i] != entries[i] || !bytes.Equal(Batch(obj, got[i]), want[order[i]]) {
				t.Fatalf("mismatch at %d", i)
			}
		}
	})
}

func FuzzParse(f *testing.F) {
	b := NewBuilder(1, "a")
	b.Add(Entry{Records: 1}, []byte("batch"))
	obj, _, _ := b.Finish()
	f.Add(obj)
	f.Fuzz(func(t *testing.T, obj []byte) {
		_, ents, err := Parse(obj)
		if err != nil {
			return
		}
		for _, e := range ents {
			_ = Batch(obj, e)
		}
	})
}
