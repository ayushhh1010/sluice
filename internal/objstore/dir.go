package objstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Dir stores objects as files under a root directory. Put-if-absent is a
// hard link of a fully written temp file, which is atomic and fails if the
// target exists. It is meant for local development and crash tests without
// MinIO.
type Dir struct{ root string }

func NewDir(root string) (*Dir, error) {
	if err := os.MkdirAll(filepath.Join(root, ".tmp"), 0o750); err != nil {
		return nil, err
	}
	return &Dir{root: root}, nil
}

func (d *Dir) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") || strings.HasPrefix(key, ".tmp") {
		return "", errors.New("objstore: invalid key " + key)
	}
	return filepath.Join(d.root, filepath.FromSlash(key)), nil
}

func (d *Dir) Get(_ context.Context, key string, off, n int64) ([]byte, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p) //nolint:gosec // p is built by path(), which rejects traversal
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	defer f.Close()
	if n <= 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			return nil, err
		}
		return io.ReadAll(f)
	}
	buf := make([]byte, n)
	k, err := f.ReadAt(buf, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:k], nil
}

func (d *Dir) Put(_ context.Context, key string, data []byte, ifAbsent bool) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return err
	}
	var suffix [8]byte
	_, _ = rand.Read(suffix[:])
	tmp := filepath.Join(d.root, ".tmp", hex.EncodeToString(suffix[:]))
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // tmp is under our root
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
	if !ifAbsent {
		return os.Rename(tmp, p)
	}
	if err := os.Link(tmp, p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrPrecondition
		}
		return err
	}
	return nil
}

func (d *Dir) Head(_ context.Context, key string) (int64, error) {
	p, err := d.path(key)
	if err != nil {
		return 0, err
	}
	st, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, ErrNotFound
	} else if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (d *Dir) Delete(_ context.Context, key string) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (d *Dir) List(_ context.Context, prefix, after string, limit int) ([]ObjectInfo, error) {
	var out []ObjectInfo
	err := filepath.WalkDir(d.root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(d.root, p)
		rel = filepath.ToSlash(rel)
		if e.IsDir() {
			if rel == ".tmp" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(rel, prefix) && rel > after {
			info, err := e.Info()
			if err != nil {
				return nil // deleted concurrently
			}
			out = append(out, ObjectInfo{Key: rel, Size: info.Size(), LastModified: info.ModTime()})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, err
}
