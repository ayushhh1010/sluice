// Package httpapi is the playground's HTTP API, served by every agent.
// Browsers cannot speak Kafka's TCP protocol, so this small JSON/SSE API
// exposes produce, consume, inspection and the chaos button.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/agent"
	"github.com/ayushhh1010/sluice/internal/kbatch"
	"github.com/ayushhh1010/sluice/internal/runtime"
)

// Options configures the API.
type Options struct {
	Prefix        string // public topic prefix, default "pg-"
	AllowOrigin   string // CORS origin; empty = same-origin only
	MaxPartitions int32
	TrustProxy    bool // take the client IP from X-Forwarded-For (behind Caddy)
}

type API struct {
	agent *runtime.AgentNode
	opt   Options
	log   *slog.Logger
	mux   *http.ServeMux
	lim   *limiter
}

func New(a *runtime.AgentNode, opt Options, log *slog.Logger) *API {
	if opt.Prefix == "" {
		opt.Prefix = "pg-"
	}
	if opt.MaxPartitions == 0 {
		opt.MaxPartitions = 4
	}
	api := &API{agent: a, opt: opt, log: log, mux: http.NewServeMux(), lim: newLimiter()}
	api.mux.HandleFunc("POST /api/v1/topics", api.createTopic)
	api.mux.HandleFunc("POST /api/v1/topics/{topic}/records", api.produce)
	api.mux.HandleFunc("GET /api/v1/topics/{topic}/stream", api.stream)
	api.mux.HandleFunc("GET /api/v1/inspect/partitions/{topic}/{partition}", api.inspectPartition)
	api.mux.HandleFunc("GET /api/v1/inspect/objects", api.inspectObjects)
	api.mux.HandleFunc("GET /api/v1/cluster", api.cluster)
	api.mux.HandleFunc("POST /api/v1/chaos/kill-agent", api.chaos)
	return api
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a.opt.AllowOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", a.opt.AllowOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	a.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (a *API) clientIP(r *http.Request) string {
	if a.opt.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

// kafka forwards one Kafka request to the sequencer as the playground user.
func (a *API) kafka(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	resp, err := a.agent.Forward(ctx, &pb.Request{Body: &pb.Request_Kafka{Kafka: &pb.KafkaRequest{
		ApiKey: int32(req.Key()), ApiVersion: int32(req.GetVersion()), Body: req.AppendTo(nil),
		Principal: "playground", Listener: "playground", Public: true, ClientId: "sluice-playground",
	}}}, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if resp.Code != pb.Code_CODE_OK {
		return nil, errors.New(resp.Message)
	}
	kr := req.ResponseKind()
	if err := kr.ReadFrom(resp.GetKafka().GetBody()); err != nil {
		return nil, err
	}
	return kr, nil
}

var (
	adjectives = []string{"amber", "brisk", "calm", "dusty", "eager", "fuzzy", "gentle", "hazy", "icy", "jolly", "keen", "lucky", "mellow", "nimble", "olive", "plucky", "quiet", "rapid", "sunny", "tidy", "urban", "vivid", "witty", "young", "zesty"}
	nouns      = []string{"otter", "falcon", "river", "comet", "maple", "harbor", "pebble", "lantern", "meadow", "canyon", "badger", "glacier", "willow", "ember", "tide", "quartz", "heron", "cedar", "delta", "fjord", "aurora", "lynx", "sparrow", "summit", "brook"}
)

func (a *API) createTopic(w http.ResponseWriter, r *http.Request) {
	if !a.lim.allow("topic:"+a.clientIP(r), 10, time.Hour) {
		fail(w, http.StatusTooManyRequests, "topic creation rate limit reached; try again later")
		return
	}
	var body struct {
		Name       string `json:"name"`
		Partitions int32  `json:"partitions"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			fail(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
	}
	if body.Name == "" {
		body.Name = fmt.Sprintf("%s%s-%s-%d", a.opt.Prefix, adjectives[rand.IntN(len(adjectives))], nouns[rand.IntN(len(nouns))], rand.IntN(1000))
	}
	if !strings.HasPrefix(body.Name, a.opt.Prefix) {
		fail(w, http.StatusBadRequest, "playground topics must start with "+a.opt.Prefix)
		return
	}
	if body.Partitions <= 0 {
		body.Partitions = 2
	}
	if body.Partitions > a.opt.MaxPartitions {
		fail(w, http.StatusBadRequest, fmt.Sprintf("at most %d partitions", a.opt.MaxPartitions))
		return
	}
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Version = 7
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = body.Name, body.Partitions, -1
	req.Topics = append(req.Topics, t)
	kr, err := a.kafka(r.Context(), req)
	if err != nil {
		fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	res := kr.(*kmsg.CreateTopicsResponse).Topics[0]
	if res.ErrorCode != 0 {
		msg := kerr.ErrorForCode(res.ErrorCode).Error()
		if res.ErrorMessage != nil {
			msg = *res.ErrorMessage
		}
		fail(w, http.StatusConflict, msg)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"topic": body.Name, "partitions": body.Partitions})
}

func (a *API) partitions(ctx context.Context, topic string) (int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 12
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = &topic
	req.Topics = append(req.Topics, t)
	kr, err := a.kafka(ctx, req)
	if err != nil {
		return 0, err
	}
	mt := kr.(*kmsg.MetadataResponse).Topics
	if len(mt) != 1 || mt[0].ErrorCode != 0 {
		return 0, errors.New("unknown topic")
	}
	return int32(len(mt[0].Partitions)), nil
}

type recordIn struct {
	Key       *string `json:"key"`
	Value     string  `json:"value"`
	Partition *int32  `json:"partition"`
}

func (a *API) produce(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if !strings.HasPrefix(topic, a.opt.Prefix) {
		fail(w, http.StatusForbidden, "playground can only write to "+a.opt.Prefix+"* topics")
		return
	}
	if !a.lim.allow("produce:"+a.clientIP(r), 600, time.Minute) {
		fail(w, http.StatusTooManyRequests, "produce rate limit reached")
		return
	}
	var body struct {
		Records []recordIn `json:"records"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Records) == 0 || len(body.Records) > 500 {
		fail(w, http.StatusBadRequest, `body must be {"records":[{"key":"k","value":"v"}]} with 1-500 records`)
		return
	}
	n, err := a.partitions(r.Context(), topic)
	if err != nil {
		fail(w, http.StatusNotFound, err.Error())
		return
	}
	// Group records by partition: explicit, by key hash, or round robin.
	byPart := map[int32][]kbatch.Record{}
	var order []int32
	rr := rand.Int32N(n)
	for _, rec := range body.Records {
		var p int32
		switch {
		case rec.Partition != nil:
			p = *rec.Partition
		case rec.Key != nil:
			h := fnv.New32a()
			h.Write([]byte(*rec.Key))
			p = int32(h.Sum32() % uint32(n))
		default:
			p = rr
		}
		if p < 0 || p >= n {
			fail(w, http.StatusBadRequest, "partition out of range")
			return
		}
		kr := kbatch.Record{Value: []byte(rec.Value)}
		if rec.Key != nil {
			kr.Key = []byte(*rec.Key)
		}
		if byPart[p] == nil {
			order = append(order, p)
		}
		byPart[p] = append(byPart[p], kr)
	}
	req := agent.ProduceReq{TimeoutMs: 10_000, Acks: -1, Public: true, AnyOwner: true}
	now := time.Now().UnixMilli()
	for _, p := range order {
		b := kbatch.Build(now, byPart[p])
		h, _ := kbatch.Validate(b)
		req.Parts = append(req.Parts, agent.ProducePart{Topic: topic, Partition: p, Batches: [][]byte{b}, Headers: []kbatch.Header{h}})
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	resp, err := a.agent.Produce(ctx, req)
	if err != nil {
		fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	type result struct {
		Partition  int32  `json:"partition"`
		BaseOffset int64  `json:"base_offset"`
		Count      int    `json:"count"`
		Error      string `json:"error,omitempty"`
	}
	var out []result
	status := http.StatusOK
	for i, p := range order {
		res := result{Partition: p, BaseOffset: resp.Parts[i].BaseOffset, Count: len(byPart[p])}
		if code := resp.Parts[i].Err; code != 0 {
			res.Error = kerr.ErrorForCode(code).Error()
			status = http.StatusServiceUnavailable
		}
		out = append(out, res)
	}
	writeJSON(w, status, map[string]any{"topic": topic, "results": out})
}

// stream serves records as server-sent events.
func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	n, err := a.partitions(r.Context(), topic)
	if err != nil {
		fail(w, http.StatusNotFound, err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		fail(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	offsets := make([]int64, n)
	from := r.URL.Query().Get("from")
	if from == "" || from == "latest" || from == "earliest" {
		ts := int64(-1)
		if from == "earliest" {
			ts = -2
		}
		req := kmsg.NewPtrListOffsetsRequest()
		req.Version = 6
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		for p := int32(0); p < n; p++ {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition, rp.Timestamp = p, ts
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		kr, err := a.kafka(r.Context(), req)
		if err != nil {
			fail(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		for _, p := range kr.(*kmsg.ListOffsetsResponse).Topics[0].Partitions {
			offsets[p.Partition] = max(p.Offset, 0)
		}
	} else {
		v, err := strconv.ParseInt(from, 10, 64)
		if err != nil || v < 0 {
			fail(w, http.StatusBadRequest, "from must be earliest, latest or an offset")
			return
		}
		for i := range offsets {
			offsets[i] = v
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	deadline := time.Now().Add(30 * time.Minute)
	for r.Context().Err() == nil && time.Now().Before(deadline) {
		req := agent.FetchReq{MaxWaitMs: 5000, MinBytes: 1, MaxBytes: 1 << 20}
		for p := int32(0); p < n; p++ {
			req.Parts = append(req.Parts, agent.FetchPart{Topic: topic, Partition: p, Offset: offsets[p], MaxBytes: 256 << 10})
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		resp, err := a.agent.Fetch(ctx, req)
		cancel()
		if err != nil {
			return
		}
		sent := false
		for p, part := range resp.Parts {
			if part.Err == agent.ErrOffsetOutOfRange {
				offsets[p] = part.LogStart
				continue
			}
			bs, hs, err := kbatch.Split(part.Records)
			if err != nil {
				continue
			}
			for i, b := range bs {
				recs, tss, ok := kbatch.Records(b)
				if !ok {
					writeEvent(w, map[string]any{"partition": p, "offset": hs[i].BaseOffset, "compressed": true, "count": hs[i].Records()})
					offsets[p] = hs[i].BaseOffset + hs[i].Records()
					sent = true
					continue
				}
				for j, rec := range recs {
					off := hs[i].BaseOffset + int64(j)
					if off < offsets[p] {
						continue
					}
					ev := map[string]any{"partition": p, "offset": off, "value": string(rec.Value), "timestamp": tss[j]}
					if rec.Key != nil {
						ev["key"] = string(rec.Key)
					}
					writeEvent(w, ev)
					sent = true
				}
				offsets[p] = max(offsets[p], hs[i].BaseOffset+hs[i].Records())
			}
		}
		if !sent {
			fmt.Fprint(w, ": keepalive\n\n")
		}
		flusher.Flush()
	}
}

func writeEvent(w http.ResponseWriter, v any) {
	b, _ := json.Marshal(v)
	fmt.Fprintf(w, "data: %s\n\n", b)
}

func (a *API) inspect(ctx context.Context, req *pb.InspectRequest) (*pb.InspectResponse, error) {
	resp, err := a.agent.Forward(ctx, &pb.Request{Body: &pb.Request_Inspect{Inspect: req}}, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if resp.Code != pb.Code_CODE_OK {
		return nil, errors.New(resp.Message)
	}
	return resp.GetInspect(), nil
}

// agentFromKey extracts the writing agent from a segment key.
func agentFromKey(key string) string {
	base := key[strings.LastIndexByte(key, '/')+1:]
	if i := strings.LastIndexByte(base, '-'); i > 0 && strings.Contains(key, "/data/") {
		return base[:i]
	}
	return "merger"
}

func (a *API) inspectPartition(w http.ResponseWriter, r *http.Request) {
	p, err := strconv.ParseUint(r.PathValue("partition"), 10, 32)
	if err != nil {
		fail(w, http.StatusBadRequest, "bad partition")
		return
	}
	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	res, err := a.inspect(r.Context(), &pb.InspectRequest{What: &pb.InspectRequest_Partition{Partition: &pb.InspectPartition{
		Topic: r.PathValue("topic"), Partition: uint32(p), FromOffset: from, Limit: 200,
	}}})
	if err != nil {
		fail(w, http.StatusNotFound, err.Error())
		return
	}
	type ref struct {
		BaseOffset int64  `json:"base_offset"`
		LastOffset int64  `json:"last_offset"`
		Object     string `json:"object"`
		ByteStart  uint32 `json:"byte_start"`
		ByteEnd    uint32 `json:"byte_end"`
		Agent      string `json:"agent"`
		CommitSeq  uint64 `json:"commit_seq"`
		MaxTs      int64  `json:"max_timestamp"`
	}
	out := []ref{}
	for _, b := range res.Refs {
		out = append(out, ref{b.BaseOffset, b.BaseOffset + int64(b.Records) - 1, b.ObjectKey, b.OffsetInObj, b.OffsetInObj + b.Length, agentFromKey(b.ObjectKey), b.CommitSeq, b.MaxTs})
	}
	writeJSON(w, http.StatusOK, map[string]any{"high_watermark": res.HighWatermark, "log_start": res.LogStart, "batches": out})
}

func (a *API) inspectObjects(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)
	res, err := a.inspect(r.Context(), &pb.InspectRequest{What: &pb.InspectRequest_Objects{Objects: &pb.InspectObjects{Limit: int32(limit)}}})
	if err != nil {
		fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	type part struct {
		TopicID    string `json:"topic_id"`
		Partition  uint32 `json:"partition"`
		BaseOffset int64  `json:"base_offset"`
		Records    uint32 `json:"records"`
		Bytes      uint32 `json:"bytes"`
	}
	type obj struct {
		Key     string `json:"key"`
		Agent   string `json:"agent"`
		Created int64  `json:"created_ms"`
		Batches []part `json:"batches"`
	}
	out := []obj{}
	for _, c := range res.Objects {
		o := obj{Key: c.ObjectKey, Agent: agentFromKey(c.ObjectKey), Created: c.ObjectCreatedMs}
		for _, b := range c.Results {
			if b.ErrorCode == 0 && !b.Duplicate {
				o.Batches = append(o.Batches, part{fmt.Sprintf("%x", b.TopicId), b.Partition, b.BaseOffset, b.Records, b.Length})
			}
		}
		out = append(out, o)
	}
	writeJSON(w, http.StatusOK, map[string]any{"objects": out})
}

func (a *API) cluster(w http.ResponseWriter, r *http.Request) {
	res, err := a.inspect(r.Context(), &pb.InspectRequest{What: &pb.InspectRequest_Cluster{Cluster: &pb.InspectCluster{}}})
	if err != nil {
		fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	ci := res.Cluster
	type ag struct {
		ID     string `json:"id"`
		NodeID int32  `json:"node_id"`
	}
	agents := []ag{}
	for _, a := range ci.Agents {
		agents = append(agents, ag{a.Id, a.NodeId})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sequencer": ci.Leader, "epoch": ci.Epoch, "metalog_seq": ci.MetalogSeq, "agents": agents,
		"class_a_today": ci.ClassAToday, "read_only": ci.ReadOnly, "topics": ci.Topics,
	})
}

func (a *API) chaos(w http.ResponseWriter, r *http.Request) {
	if !a.lim.allow("chaos:"+a.clientIP(r), 3, 10*time.Minute) {
		fail(w, http.StatusTooManyRequests, "chaos rate limit reached")
		return
	}
	resp, err := a.agent.Forward(r.Context(), &pb.Request{Body: &pb.Request_Chaos{Chaos: &pb.ChaosRequest{}}}, 5*time.Second)
	if err != nil {
		fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if resp.Code != pb.Code_CODE_OK {
		fail(w, http.StatusConflict, resp.Message)
		return
	}
	c := resp.GetChaos()
	if c.Killed == "" {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "an agent was killed recently", "retry_after_ms": c.RetryAfterMs})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"killed": c.Killed})
}

// limiter is a fixed-window per-key rate limiter.
type limiter struct {
	mu sync.Mutex
	m  map[string]*window
}

type window struct {
	start time.Time
	n     int
}

func newLimiter() *limiter { return &limiter{m: map[string]*window{}} }

func (l *limiter) allow(key string, n int, per time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if len(l.m) > 100_000 { // bound memory under abuse
		l.m = map[string]*window{}
	}
	w := l.m[key]
	if w == nil || now.Sub(w.start) > per {
		w = &window{start: now}
		l.m[key] = w
	}
	w.n++
	return w.n <= n
}
