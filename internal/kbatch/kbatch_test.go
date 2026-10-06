package kbatch

import (
	"bytes"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBuildValidateRoundTrip(t *testing.T) {
	recs := []Record{
		{Key: []byte("k1"), Value: []byte("hello")},
		{Value: []byte("world"), Headers: [][2][]byte{{[]byte("h"), []byte("v")}}},
	}
	b := Build(1700000000000, recs)
	h, err := Validate(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Records() != 2 || h.MaxTimestamp != 1700000000000 || h.ProducerID != -1 {
		t.Fatalf("bad header %+v", h)
	}

	// Cross-check against franz-go's decoder.
	var kb kmsg.RecordBatch
	if err := kb.ReadFrom(b); err != nil {
		t.Fatal(err)
	}
	if kb.NumRecords != 2 || kb.Magic != 2 {
		t.Fatalf("kmsg decode: %+v", kb)
	}

	got, ts, ok := Records(b)
	if !ok || len(got) != 2 || !bytes.Equal(got[0].Value, []byte("hello")) || got[0].Key == nil ||
		got[1].Key != nil || string(got[1].Headers[0][1]) != "v" || ts[1] != 1700000000000 {
		t.Fatalf("records: %v %v %v", got, ts, ok)
	}

	// Patching the base offset must not invalidate the CRC.
	Patch(b, 12345)
	h, err = Validate(b)
	if err != nil || h.BaseOffset != 12345 {
		t.Fatalf("after patch: %v %+v", err, h)
	}

	// Any change inside the CRC-covered region must be caught.
	b[len(b)-1] ^= 0xff
	if _, err := Validate(b); !errors.Is(err, ErrCRC) {
		t.Fatalf("want CRC error, got %v", err)
	}
}

func TestSplit(t *testing.T) {
	a := Build(1, []Record{{Value: []byte("a")}})
	b := Build(2, []Record{{Value: []byte("b")}, {Value: []byte("c")}})
	bs, hs, err := Split(append(append([]byte{}, a...), b...))
	if err != nil || len(bs) != 2 || hs[1].Records() != 2 {
		t.Fatalf("split: %v %d", err, len(bs))
	}
	if _, _, err := Split(append(append([]byte{}, a...), b[:20]...)); err == nil {
		t.Fatal("truncated tail accepted")
	}
}

func FuzzSplit(f *testing.F) {
	f.Add(Build(1, []Record{{Value: []byte("x")}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		bs, _, err := Split(b)
		if err != nil {
			return
		}
		for _, x := range bs {
			Records(x)
		}
	})
}
