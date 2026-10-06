package protocol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/kbatch"
	"github.com/ayushhh1010/sluice/internal/metrics"
	"github.com/ayushhh1010/sluice/internal/runtime"
)

// Listener configures one Kafka listener.
type Listener struct {
	Name   string
	Addr   string
	Public bool        // namespace, quota and cost-guard rules apply
	TLS    *tls.Config // nil: plaintext
	// Users enables SASL/PLAIN when non-nil (user -> password).
	Users map[string]string
	// Admins are users exempt from public-listener restrictions.
	Admins map[string]bool

	MaxFrameBytes int32   // largest request accepted
	MaxBatchBytes int     // largest record batch accepted
	ProduceQuota  float64 // bytes/s per user; 0 = unlimited
	FetchQuota    float64
	MaxConnsPerIP int
	IdleTimeout   time.Duration
}

func (l *Listener) defaults() {
	if l.MaxFrameBytes == 0 {
		l.MaxFrameBytes = 100 << 20
	}
	if l.MaxBatchBytes == 0 {
		l.MaxBatchBytes = 1 << 20
	}
	if l.IdleTimeout == 0 {
		l.IdleTimeout = 10 * time.Minute
	}
}

// Server serves one listener for an agent.
type Server struct {
	L     Listener
	Agent *runtime.AgentNode
	M     *metrics.Metrics
	Log   *slog.Logger

	mu      sync.Mutex
	perIP   map[string]int
	buckets map[string]*bucket
	connIDs atomic.Uint64
}

func NewServer(l Listener, a *runtime.AgentNode, m *metrics.Metrics, log *slog.Logger) *Server {
	l.defaults()
	return &Server{L: l, Agent: a, M: m, Log: log.With("listener", l.Name), perIP: map[string]int{}, buckets: map[string]*bucket{}}
}

// Serve accepts connections until ctx ends.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		go s.handle(ctx, c)
	}
}

type conn struct {
	s        *Server
	c        net.Conn
	id       uint64
	authed   bool
	mech     string
	user     string
	clientID string
	log      *slog.Logger
	// closeAfter closes the connection once the current response is out.
	closeAfter bool
}

func (s *Server) admit(ip string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.L.MaxConnsPerIP > 0 && s.perIP[ip] >= s.L.MaxConnsPerIP {
		return false
	}
	s.perIP[ip]++
	return true
}

func (s *Server) release(ip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.perIP[ip]--; s.perIP[ip] <= 0 {
		delete(s.perIP, ip)
	}
}

func (s *Server) handle(ctx context.Context, raw net.Conn) {
	ip, _, _ := net.SplitHostPort(raw.RemoteAddr().String())
	if !s.admit(ip) {
		raw.Close()
		return
	}
	defer s.release(ip)
	if s.M != nil {
		g := s.M.Connections.WithLabelValues(s.L.Name)
		g.Inc()
		defer g.Dec()
	}
	nc := raw
	if s.L.TLS != nil {
		tc := tls.Server(raw, s.L.TLS)
		_ = tc.SetDeadline(time.Now().Add(10 * time.Second))
		if err := tc.HandshakeContext(ctx); err != nil {
			raw.Close()
			return
		}
		_ = tc.SetDeadline(time.Time{})
		nc = tc
	}
	defer nc.Close()
	c := &conn{s: s, c: nc, id: s.connIDs.Add(1), authed: s.L.Users == nil}
	c.log = s.Log.With("conn", c.id, "remote", ip)
	if err := c.serve(ctx); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		c.log.Debug("connection closed", "err", err)
	}
}

// job is one request whose response is pending.
type job struct {
	corr       int32
	key        int16
	reply      replyFunc
	closeAfter bool
}

// serve pipelines requests: the reader hands each one to the agent in
// arrival order and queues a job; the writer sends responses strictly in
// request order, as the protocol requires. Kafka brokers process one
// request per connection at a time, but every produce here waits for a
// flush window, so serial processing would stack those windows for any
// client with several requests in flight.
func (c *conn) serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan *job, 32)
	werr := make(chan error, 1)
	go func() { werr <- c.writeLoop(ctx, jobs) }()
	rerr := c.readLoop(ctx, jobs)
	close(jobs)
	if rerr != nil {
		cancel()
		c.c.Close()
		<-werr
		return rerr
	}
	return <-werr
}

func (c *conn) readLoop(ctx context.Context, jobs chan<- *job) error {
	br := bufio.NewReaderSize(c.c, 64<<10)
	var size [4]byte
	for {
		_ = c.c.SetReadDeadline(time.Now().Add(c.s.L.IdleTimeout))
		if _, err := io.ReadFull(br, size[:]); err != nil {
			return err
		}
		n := int32(binary.BigEndian.Uint32(size[:]))
		if n < 8 || n > c.s.L.MaxFrameBytes || (!c.authed && n > 64<<10) {
			return fmt.Errorf("bad request size %d", n)
		}
		frame := make([]byte, n)
		if _, err := io.ReadFull(br, frame); err != nil {
			return err
		}
		r := kbin.Reader{Src: frame}
		key, version, corr := r.Int16(), r.Int16(), r.Int32()
		if cid := r.NullableString(); cid != nil {
			c.clientID = *cid
		}
		j := &job{corr: corr, key: key}
		if key == kmsg.ApiVersions.Int16() && version > Versions[key][1] {
			// KIP-511: answer at v0 with our range so the client retries.
			resp := kmsg.NewPtrApiVersionsResponse()
			resp.ErrorCode = kerr.UnsupportedVersion.Code
			k := kmsg.NewApiVersionsResponseApiKey()
			k.ApiKey, k.MinVersion, k.MaxVersion = key, Versions[key][0], Versions[key][1]
			resp.ApiKeys = append(resp.ApiKeys, k)
			body := resp.AppendTo(nil)
			j.reply = func() ([]byte, bool, time.Duration, error) { return body, false, 0, nil }
		} else {
			if !supported(key, version) {
				return fmt.Errorf("unsupported api %d v%d", key, version)
			}
			kreq := kmsg.RequestForKey(key)
			kreq.SetVersion(version)
			if kreq.IsFlexible() {
				kmsg.SkipTags(&r)
			}
			if !r.Ok() {
				return errors.New("malformed request header")
			}
			if !c.authed && key != kmsg.ApiVersions.Int16() && key != kmsg.SASLHandshake.Int16() && key != kmsg.SASLAuthenticate.Int16() {
				return errors.New("request before authentication")
			}
			if c.s.M != nil {
				c.s.M.KafkaRequests.WithLabelValues(kmsg.NameForKey(key), c.s.L.Name).Inc()
			}
			reply, err := c.dispatch(ctx, key, version, kreq, r.Src)
			if err != nil {
				return err
			}
			j.reply, j.closeAfter = reply, c.closeAfter
		}
		select {
		case jobs <- j:
		case <-ctx.Done():
			return ctx.Err()
		}
		if j.closeAfter {
			return nil // the writer sends the failure, then closes
		}
	}
}

func (c *conn) writeLoop(ctx context.Context, jobs <-chan *job) error {
	var out []byte
	fail := func(err error) error {
		c.c.Close()
		for range jobs { // drain so the reader can exit
		}
		return err
	}
	for j := range jobs {
		body, flexible, throttle, err := j.reply()
		if err != nil {
			return fail(err)
		}
		if body == nil {
			continue // acks=0 produce: no response
		}
		out = append(out[:0], 0, 0, 0, 0, 0, 0, 0, 0)
		if flexible && j.key != kmsg.ApiVersions.Int16() {
			out = append(out, 0) // response header tagged fields
		}
		out = append(out, body...)
		binary.BigEndian.PutUint32(out, uint32(len(out)-4))
		binary.BigEndian.PutUint32(out[4:], uint32(j.corr))
		_ = c.c.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := c.c.Write(out); err != nil {
			return fail(err)
		}
		if j.closeAfter {
			return fail(errors.New("closing after failed authentication"))
		}
		if throttle > 0 {
			// KIP-219: the response carries the throttle time and the
			// broker mutes the connection; a stalled writer fills the job
			// queue and stops the reader.
			select {
			case <-time.After(throttle):
			case <-ctx.Done():
				return fail(ctx.Err())
			}
		}
	}
	return nil
}

func (c *conn) public() bool { return c.s.L.Public && !c.s.L.Admins[c.user] }

func (c *conn) principal() string {
	if c.user != "" {
		return c.user
	}
	return "anonymous:" + c.s.L.Name
}

type replyFunc = func() ([]byte, bool, time.Duration, error)

type reply struct {
	body     []byte
	flexible bool
	throttle time.Duration
	err      error
}

// async runs fn on its own goroutine and returns a reply that waits for it.
func async(fn replyFunc) replyFunc {
	ch := make(chan reply, 1)
	go func() {
		b, f, t, err := fn()
		ch <- reply{b, f, t, err}
	}()
	return func() ([]byte, bool, time.Duration, error) {
		r := <-ch
		return r.body, r.flexible, r.throttle, r.err
	}
}

func now(b []byte, f bool, t time.Duration, err error) replyFunc {
	return func() ([]byte, bool, time.Duration, error) { return b, f, t, err }
}

func encoded(resp kmsg.Response, throttle time.Duration, err error) ([]byte, bool, time.Duration, error) {
	if err != nil || resp == nil {
		return nil, false, 0, err
	}
	return resp.AppendTo(nil), resp.IsFlexible(), throttle, nil
}

// dispatch starts a request on the reader goroutine (so produces reach the
// agent in arrival order) and returns a function that waits for the reply.
func (c *conn) dispatch(ctx context.Context, key, version int16, kreq kmsg.Request, raw []byte) (replyFunc, error) {
	if forwarded[key] {
		// Capture per-request state now: the reader overwrites clientID
		// while this request is still being forwarded.
		flexible, meta := kreq.IsFlexible(), c.requestMeta()
		return async(func() ([]byte, bool, time.Duration, error) {
			body, err := c.forward(ctx, key, version, raw, meta)
			return body, flexible, 0, err
		}), nil
	}
	if err := kreq.ReadFrom(raw); err != nil {
		return nil, fmt.Errorf("decode %s: %w", kmsg.NameForKey(key), err)
	}
	switch r := kreq.(type) {
	case *kmsg.ApiVersionsRequest:
		return now(encoded(apiVersions(r), 0, nil)), nil
	case *kmsg.SASLHandshakeRequest:
		return now(encoded(c.saslHandshake(r), 0, nil)), nil
	case *kmsg.SASLAuthenticateRequest:
		return now(encoded(c.saslAuthenticate(r), 0, nil)), nil
	case *kmsg.ProduceRequest:
		wait := c.produce(r)
		return func() ([]byte, bool, time.Duration, error) { return encoded(wait(ctx)) }, nil
	case *kmsg.FetchRequest:
		return async(func() ([]byte, bool, time.Duration, error) { return encoded(c.fetch(ctx, r)) }), nil
	}
	return nil, fmt.Errorf("unhandled api %d", key)
}

func apiVersions(r *kmsg.ApiVersionsRequest) *kmsg.ApiVersionsResponse {
	resp := r.ResponseKind().(*kmsg.ApiVersionsResponse)
	keys := make([]int16, 0, len(Versions))
	for k := range Versions {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		ak := kmsg.NewApiVersionsResponseApiKey()
		ak.ApiKey, ak.MinVersion, ak.MaxVersion = k, Versions[k][0], Versions[k][1]
		resp.ApiKeys = append(resp.ApiKeys, ak)
	}
	return resp
}

// ---- SASL/PLAIN ----

func (c *conn) saslHandshake(r *kmsg.SASLHandshakeRequest) *kmsg.SASLHandshakeResponse {
	resp := r.ResponseKind().(*kmsg.SASLHandshakeResponse)
	resp.SupportedMechanisms = []string{"PLAIN"}
	if c.s.L.Users == nil || r.Mechanism != "PLAIN" {
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
		return resp
	}
	if c.authed {
		resp.ErrorCode = kerr.IllegalSaslState.Code
		return resp
	}
	c.mech = "PLAIN"
	return resp
}

func (c *conn) saslAuthenticate(r *kmsg.SASLAuthenticateRequest) *kmsg.SASLAuthenticateResponse {
	resp := r.ResponseKind().(*kmsg.SASLAuthenticateResponse)
	if c.mech != "PLAIN" || c.authed {
		resp.ErrorCode = kerr.IllegalSaslState.Code
		return resp
	}
	// message = [authzid] UTF8NUL authcid UTF8NUL passwd (RFC 4616)
	parts := bytes.Split(r.SASLAuthBytes, []byte{0})
	ok := false
	var user string
	if len(parts) == 3 {
		user = string(parts[1])
		want, exists := c.s.L.Users[user]
		// Compare even for unknown users so timing does not reveal them.
		match := subtle.ConstantTimeCompare([]byte(want), parts[2]) == 1
		ok = exists && match && (len(parts[0]) == 0 || string(parts[0]) == user)
	}
	if !ok {
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		msg := "authentication failed: invalid username or password"
		resp.ErrorMessage = &msg
		c.closeAfter = true
		c.log.Info("sasl authentication failed", "user", user)
		return resp
	}
	c.authed, c.user = true, user
	return resp
}

// ---- Forwarded APIs ----

// requestMeta is the connection state a forwarded request carries.
type requestMeta struct {
	principal, clientID string
	public              bool
}

func (c *conn) requestMeta() requestMeta {
	return requestMeta{principal: c.principal(), clientID: c.clientID, public: c.public()}
}

func (c *conn) forward(ctx context.Context, key, version int16, body []byte, meta requestMeta) ([]byte, error) {
	timeout := 30 * time.Second
	if key == kmsg.JoinGroup.Int16() || key == kmsg.SyncGroup.Int16() {
		timeout = 6 * time.Minute // waits for the rebalance to complete
	}
	ctx, cancel := context.WithTimeout(ctx, timeout+5*time.Second)
	defer cancel()
	resp, err := c.s.Agent.Forward(ctx, &pb.Request{Body: &pb.Request_Kafka{Kafka: &pb.KafkaRequest{
		ApiKey: int32(key), ApiVersion: int32(version), Body: body,
		Principal: meta.principal, Listener: c.s.L.Name, Public: meta.public, ClientId: meta.clientID,
	}}}, timeout)
	if err != nil {
		// Closing the connection is the one error every client handles for
		// every API: it refreshes metadata and retries.
		return nil, fmt.Errorf("forward %s: %w", kmsg.NameForKey(key), err)
	}
	if resp.Code != pb.Code_CODE_OK {
		return nil, fmt.Errorf("forward %s: %s", kmsg.NameForKey(key), resp.Message)
	}
	return resp.GetKafka().GetBody(), nil
}

// ---- Produce ----

// produce validates and submits the request immediately and returns a
// function that waits for the result.
func (c *conn) produce(r *kmsg.ProduceRequest) func(context.Context) (kmsg.Response, time.Duration, error) {
	resp := r.ResponseKind().(*kmsg.ProduceResponse)
	type slot struct{ t, p int }
	var (
		req   = agent.ProduceReq{TimeoutMs: int64(r.TimeoutMillis), Acks: r.Acks, Public: c.public()}
		slots []slot
		bytes int
	)
	for ti, rt := range r.Topics {
		t := kmsg.NewProduceResponseTopic()
		t.Topic = rt.Topic
		for pi, rp := range rt.Partitions {
			p := kmsg.NewProduceResponseTopicPartition()
			p.Partition = rp.Partition
			p.BaseOffset, p.LogAppendTime, p.LogStartOffset = -1, -1, -1
			code := c.validate(r, rp.Records)
			if code == 0 {
				bs, hs, err := kbatch.Split(rp.Records)
				switch {
				case errors.Is(err, kbatch.ErrMagic), errors.Is(err, kbatch.ErrControl):
					code = kerr.UnsupportedForMessageFormat.Code
				case err != nil:
					code = kerr.CorruptMessage.Code
				default:
					for _, b := range bs {
						if len(b) > c.s.L.MaxBatchBytes {
							code = kerr.MessageTooLarge.Code
						}
					}
					if code == 0 {
						req.Parts = append(req.Parts, agent.ProducePart{Topic: rt.Topic, Partition: rp.Partition, Batches: bs, Headers: hs})
						slots = append(slots, slot{ti, pi})
						bytes += len(rp.Records)
					}
				}
			}
			p.ErrorCode = code
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	throttle := c.s.throttle(c.principal(), "produce", c.s.L.ProduceQuota, bytes)
	var wait func(context.Context) (agent.ProduceResp, error)
	if len(req.Parts) > 0 {
		wait = c.s.Agent.ProduceStart(req)
	}
	return func(ctx context.Context) (kmsg.Response, time.Duration, error) {
		if wait != nil {
			ctx, cancel := context.WithTimeout(ctx, time.Duration(max(r.TimeoutMillis, 1000))*time.Millisecond+10*time.Second)
			defer cancel()
			ar, err := wait(ctx)
			if err != nil {
				return nil, 0, err
			}
			if r.Acks != 0 {
				for i, s := range slots {
					p := &resp.Topics[s.t].Partitions[s.p]
					res := ar.Parts[i]
					p.ErrorCode = res.Err
					if res.Err == 0 {
						p.BaseOffset, p.LogStartOffset = res.BaseOffset, res.LogStart
					}
				}
			}
		}
		if r.Acks == 0 {
			if throttle > 0 {
				time.Sleep(throttle)
			}
			return nil, 0, nil
		}
		resp.ThrottleMillis = int32(throttle.Milliseconds())
		return resp, throttle, nil
	}
}

func (c *conn) validate(r *kmsg.ProduceRequest, records []byte) int16 {
	switch {
	case r.Acks != 0 && r.Acks != 1 && r.Acks != -1:
		return kerr.InvalidRequiredAcks.Code
	case r.TransactionID != nil:
		return kerr.TransactionalIDAuthorizationFailed.Code
	case len(records) == 0:
		return kerr.CorruptMessage.Code
	}
	return 0
}

// ---- Fetch ----

func (c *conn) fetch(ctx context.Context, r *kmsg.FetchRequest) (kmsg.Response, time.Duration, error) {
	resp := r.ResponseKind().(*kmsg.FetchResponse)
	req := agent.FetchReq{MaxWaitMs: int64(min(max(r.MaxWaitMillis, 0), 30_000)), MinBytes: r.MinBytes, MaxBytes: r.MaxBytes}
	if r.Version < 3 || req.MaxBytes <= 0 {
		req.MaxBytes = 50 << 20
	}
	for _, rt := range r.Topics {
		for _, rp := range rt.Partitions {
			req.Parts = append(req.Parts, agent.FetchPart{Topic: rt.Topic, Partition: rp.Partition, Offset: rp.FetchOffset, MaxBytes: rp.PartitionMaxBytes})
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(req.MaxWaitMs)*time.Millisecond+30*time.Second)
	defer cancel()
	ar, err := c.s.Agent.Fetch(ctx, req)
	if err != nil {
		return nil, 0, err
	}
	i, total := 0, 0
	for _, rt := range r.Topics {
		t := kmsg.NewFetchResponseTopic()
		t.Topic = rt.Topic
		for _, rp := range rt.Partitions {
			res := ar.Parts[i]
			i++
			p := kmsg.NewFetchResponseTopicPartition()
			p.Partition = rp.Partition
			p.ErrorCode = res.Err
			p.HighWatermark, p.LastStableOffset, p.LogStartOffset = res.HW, res.HW, res.LogStart
			p.RecordBatches = res.Records
			if p.RecordBatches == nil {
				p.RecordBatches = []byte{} // empty, not null: librdkafka rejects a null record set
			}
			total += len(res.Records)
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	resp.SessionID = 0 // no incremental fetch sessions: always full responses
	throttle := c.s.throttle(c.principal(), "fetch", c.s.L.FetchQuota, total)
	resp.ThrottleMillis = int32(throttle.Milliseconds())
	return resp, throttle, nil
}

// ---- Quotas ----

type bucket struct {
	tokens float64
	last   time.Time
}

// throttle charges n bytes to the user's bucket and returns how long the
// connection must stay muted to bring the user back under the rate.
func (s *Server) throttle(user, dir string, rate float64, n int) time.Duration {
	if rate <= 0 || n == 0 || !s.L.Public || s.L.Admins[user] {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := dir + "/" + user
	b := s.buckets[k]
	now := time.Now()
	if b == nil {
		b = &bucket{tokens: rate, last: now}
		s.buckets[k] = b
	}
	b.tokens = min(rate, b.tokens+now.Sub(b.last).Seconds()*rate)
	b.last = now
	b.tokens -= float64(n)
	if b.tokens >= 0 {
		return 0
	}
	d := time.Duration(-b.tokens / rate * float64(time.Second))
	if s.M != nil {
		s.M.Throttled.WithLabelValues(dir).Add(d.Seconds())
	}
	return min(d, 30*time.Second)
}
