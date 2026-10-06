package objstore

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
)

// conformance runs the contract every Store must meet.
func conformance(t *testing.T, s Store) {
	ctx := context.Background()
	if _, err := s.Get(ctx, "a/x", 0, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
	if err := s.Put(ctx, "a/x", []byte("hello world"), true); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "a/x", []byte("other"), true); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second put-if-absent: %v", err)
	}
	if b, _ := s.Get(ctx, "a/x", 6, 5); string(b) != "world" {
		t.Fatalf("range get: %q", b)
	}
	if b, _ := s.Get(ctx, "a/x", 0, 0); string(b) != "hello world" {
		t.Fatalf("get: %q", b)
	}
	if n, err := s.Head(ctx, "a/x"); err != nil || n != 11 {
		t.Fatalf("head: %d %v", n, err)
	}
	_ = s.Put(ctx, "a/y", []byte("1"), false)
	_ = s.Put(ctx, "b/z", []byte("1"), false)
	l, err := s.List(ctx, "a/", "", 0)
	if err != nil || len(l) != 2 || l[0].Key != "a/x" || l[1].Key != "a/y" {
		t.Fatalf("list: %v %v", l, err)
	}
	if l, _ := s.List(ctx, "a/", "a/x", 0); len(l) != 1 || l[0].Key != "a/y" {
		t.Fatalf("list after: %v", l)
	}
	if err := s.Delete(ctx, "a/x"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a/x"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if _, err := s.Head(ctx, "a/x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("head deleted: %v", err)
	}

	// Exactly one concurrent put-if-absent wins.
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Put(ctx, "race", []byte{byte(i)}, true) == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d winners", wins)
	}
}

func TestMem(t *testing.T) { conformance(t, NewMem()) }

func TestDir(t *testing.T) {
	d, err := NewDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	conformance(t, d)
}

// TestS3 runs against a real bucket when SLUICE_TEST_S3 is set, e.g.
// s3://sluice-test?endpoint=http://localhost:9000&path_style=true
func TestS3(t *testing.T) {
	u := os.Getenv("SLUICE_TEST_S3")
	if u == "" {
		t.Skip("SLUICE_TEST_S3 not set")
	}
	s, err := NewS3(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a/x", "a/y", "b/z", "race"} {
		_ = s.Delete(context.Background(), k)
	}
	conformance(t, s)
}
