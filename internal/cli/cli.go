// Package cli holds the small helpers shared by the cmd/ binaries: flags
// with environment fallbacks, logging, and parsing of list-valued options.
package cli

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Env returns the environment variable for flag name (SLUICE_ + upper
// snake case) or def.
func Env(name, def string) string {
	k := "SLUICE_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

// String defines a string flag defaulting to its environment variable.
func String(fs *flag.FlagSet, name, def, usage string) *string {
	return fs.String(name, Env(name, def), usage+" [$SLUICE_"+strings.ToUpper(strings.ReplaceAll(name, "-", "_"))+"]")
}

// Int64 defines an integer flag defaulting to its environment variable.
func Int64(fs *flag.FlagSet, name string, def int64, usage string) *int64 {
	v, err := strconv.ParseInt(Env(name, strconv.FormatInt(def, 10)), 10, 64)
	if err != nil {
		v = def
	}
	return fs.Int64(name, v, usage+" [$SLUICE_"+strings.ToUpper(strings.ReplaceAll(name, "-", "_"))+"]")
}

// Duration defines a duration flag defaulting to its environment variable.
func Duration(fs *flag.FlagSet, name string, def time.Duration, usage string) *time.Duration {
	v, err := time.ParseDuration(Env(name, def.String()))
	if err != nil {
		v = def
	}
	return fs.Duration(name, v, usage+" [$SLUICE_"+strings.ToUpper(strings.ReplaceAll(name, "-", "_"))+"]")
}

// Logger returns a JSON slog logger at the given level.
func Logger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

// Context is cancelled on SIGINT or SIGTERM.
func Context() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// Map parses "a=x,b=y".
func Map(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("expected key=value, got %q", kv)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out, nil
}

// Users parses "user:password,user2:password2", or the same format one per
// line from a file when s starts with @.
func Users(s string) (map[string]string, error) {
	if strings.HasPrefix(s, "@") {
		b, err := os.ReadFile(s[1:])
		if err != nil {
			return nil, err
		}
		s = strings.ReplaceAll(string(b), "\n", ",")
	}
	out := map[string]string{}
	for _, up := range strings.Split(s, ",") {
		up = strings.TrimSpace(up)
		if up == "" || strings.HasPrefix(up, "#") {
			continue
		}
		u, p, ok := strings.Cut(up, ":")
		if !ok || u == "" {
			return nil, fmt.Errorf("expected user:password")
		}
		out[u] = p
	}
	return out, nil
}

// HostPort splits "host:port".
func HostPort(s string) (string, int32, error) {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return "", 0, fmt.Errorf("expected host:port, got %q", s)
	}
	p, err := strconv.ParseInt(s[i+1:], 10, 32)
	return s[:i], int32(p), err
}

// ReloadingCert serves a certificate from files and re-reads them when
// they change, so certificates renewed by Caddy are picked up without a
// restart.
func ReloadingCert(certFile, keyFile string) (*tls.Config, error) {
	r := &certReloader{cert: certFile, key: keyFile}
	if err := r.load(); err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: r.get}, nil
}

type certReloader struct {
	cert, key string
	mu        sync.Mutex
	c         *tls.Certificate
	mod       time.Time
	checked   time.Time
}

func (r *certReloader) load() error {
	st, err := os.Stat(r.cert)
	if err != nil {
		return err
	}
	c, err := tls.LoadX509KeyPair(r.cert, r.key)
	if err != nil {
		return err
	}
	r.c, r.mod = &c, st.ModTime()
	return nil
}

func (r *certReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.checked) > time.Minute {
		r.checked = time.Now()
		if st, err := os.Stat(r.cert); err == nil && st.ModTime().After(r.mod) {
			if err := r.load(); err != nil {
				slog.Warn("certificate reload failed; keeping the old one", "err", err)
			}
		}
	}
	return r.c, nil
}
