// Package objstore is the only durable layer: an S3-style object store with
// put-if-absent. Implementations: S3 (R2, MinIO, AWS), a local directory for
// development, and an in-memory map for tests.
package objstore

import (
	"context"
	"errors"
	"strings"
	"time"
)

var (
	// ErrNotFound: the key does not exist.
	ErrNotFound = errors.New("objstore: not found")
	// ErrPrecondition: a conditional write lost (the key already exists).
	ErrPrecondition = errors.New("objstore: precondition failed")
)

// ObjectInfo is one LIST result.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Store is the object store interface. Every error other than ErrNotFound
// and ErrPrecondition is treated as transient: the caller retries the same
// operation, and a retried put-if-absent that returns ErrPrecondition may
// mean an earlier attempt succeeded.
type Store interface {
	// Get reads a whole object, or n bytes from off when n > 0.
	Get(ctx context.Context, key string, off, n int64) ([]byte, error)
	// Put writes an object. With ifAbsent it fails with ErrPrecondition if
	// the key exists (If-None-Match: *).
	Put(ctx context.Context, key string, data []byte, ifAbsent bool) error
	// Head reports an object's size.
	Head(ctx context.Context, key string) (int64, error)
	// Delete removes an object; deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// List returns up to limit keys with the prefix, in lexical order,
	// strictly after startAfter.
	List(ctx context.Context, prefix, startAfter string, limit int) ([]ObjectInfo, error)
}

// Class is the R2/S3 billing class of an operation.
func Class(op string) string {
	switch op {
	case "put", "list":
		return "A"
	case "get", "head":
		return "B"
	default:
		return "free"
	}
}

// Open builds a store from a URL:
//
//	mem://                      in-memory (tests, single process)
//	file:///var/lib/sluice      local directory
//	s3://bucket?endpoint=...&region=auto&path_style=true
//
// S3 credentials come from the standard AWS environment variables.
func Open(ctx context.Context, url string) (Store, error) {
	switch {
	case url == "mem://" || url == "mem":
		return NewMem(), nil
	case strings.HasPrefix(url, "file://"):
		return NewDir(strings.TrimPrefix(url, "file://"))
	case strings.HasPrefix(url, "s3://"):
		return NewS3(ctx, url)
	}
	return nil, errors.New("objstore: unsupported URL " + url)
}
