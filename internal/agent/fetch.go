package agent

import (
	"errors"
	"sort"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/cache"
	"github.com/ayushhh1010/sluice/internal/kbatch"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/proc"
)

// Fetch planning. Each pass recomputes what every partition can return from
// the tail cache, the index cache and the data LRU, then either responds or
// starts the I/O it is missing (lookups and coalesced ranged GETs) and runs
// again when that I/O completes. Long polls wait for a commit (own or via
// the sequencer's watch stream) or the deadline.

type fetchWait struct {
	start     int64
	req       *FetchReq
	deadline  int64
	done      bool
	errs      []int16 // sticky per-part errors from lookups
	notFound  int     // GETs that hit deleted objects (GC races)
	waitingOn map[tp]bool
}

func (c *Core) onFetch(req *FetchReq) {
	c.Stats.Fetches++
	f := &fetchWait{start: c.now, req: req, deadline: c.now + max(req.MaxWaitMs, 0), errs: make([]int16, len(req.Parts)), waitingOn: map[tp]bool{}}
	c.after(max(req.MaxWaitMs, 0), func() { c.tryFetch(f) })
	c.tryFetch(f)
}

type partPlan struct {
	batches []cache.Batch
	err     int16
}

func (c *Core) tryFetch(f *fetchWait) {
	if f.done {
		return
	}
	final := c.now >= f.deadline
	plans := make([]partPlan, len(f.req.Parts))
	var total int64
	var lookups []int
	missing := map[string][]*pb.BatchRef{}
	maxBytes := int64(f.req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 50 << 20
	}
	for i, p := range f.req.Parts {
		pl := &plans[i]
		if f.errs[i] != 0 {
			pl.err = f.errs[i]
			continue
		}
		ti, ok := c.topics[p.Topic]
		if !ok || p.Partition < 0 || uint32(p.Partition) >= ti.partitions {
			pl.err = ErrUnknownTopicOrPartition
			continue
		}
		t := tp{ti.id, uint32(p.Partition)}
		budget := min(int64(p.MaxBytes), maxBytes-total)
		if budget <= 0 && total > 0 {
			continue
		}
		budget = max(budget, 1) // always return at least one batch
		if bs, ok := c.tail.Read(t, p.Offset, budget); ok {
			pl.batches = bs
			total += batchBytes(bs)
			c.emit(proc.Observe{Name: "cache:tail:hit", Value: 1})
			continue
		}
		c.emit(proc.Observe{Name: "cache:tail:miss", Value: 1})
		if w := c.hw[t]; w != nil && p.Offset == w.hw && c.now-w.at < c.cfg.HWStaleMs {
			continue // at the high watermark: long poll
		}
		refs, ok := c.indexed(t, p.Offset)
		if !ok {
			lookups = append(lookups, i)
			continue
		}
		var size int64
		for _, r := range refs {
			if len(pl.batches) > 0 && size+int64(r.Length) > budget {
				break
			}
			data, ok := c.data.Get(cache.RangeKey{Object: r.ObjectKey, Off: r.OffsetInObj, Len: r.Length})
			if ok {
				c.emit(proc.Observe{Name: "cache:data:hit", Value: 1})
			} else {
				c.emit(proc.Observe{Name: "cache:data:miss", Value: 1})
			}
			if !ok {
				if f.notFound > 2 {
					pl.err = ErrKafkaStorage
				} else if len(pl.batches) == 0 {
					// Read ahead: the GET covers the refs that follow too.
					missing[r.ObjectKey] = append(missing[r.ObjectKey], refsIn(refs, r.ObjectKey, budget)...)
				}
				break // keep returned batches contiguous
			}
			pl.batches = append(pl.batches, cache.Batch{Base: r.BaseOffset, Records: int64(r.Records), Data: data})
			size += int64(len(data))
		}
		total += size
	}

	if final || hasErr(plans) || total >= int64(max(f.req.MinBytes, 1)) {
		c.respondFetch(f, plans)
		return
	}
	for _, i := range lookups {
		c.lookup(f, i)
	}
	objs := make([]string, 0, len(missing))
	for o := range missing {
		objs = append(objs, o)
	}
	sort.Strings(objs)
	for _, o := range objs {
		c.fetchRanges(f, o, missing[o])
	}
	// Also wake on commits to any requested partition.
	for _, p := range f.req.Parts {
		ti, ok := c.topics[p.Topic]
		if !ok {
			continue
		}
		t := tp{ti.id, uint32(p.Partition)}
		if f.waitingOn[t] {
			continue
		}
		f.waitingOn[t] = true
		live := c.waiting[t][:0]
		for _, w := range c.waiting[t] {
			if !w.done {
				live = append(live, w)
			}
		}
		c.waiting[t] = append(live, f)
	}
}

// refsIn returns the refs in object obj, up to budget bytes.
func refsIn(refs []*pb.BatchRef, obj string, budget int64) []*pb.BatchRef {
	var out []*pb.BatchRef
	var n int64
	for _, r := range refs {
		if r.ObjectKey != obj {
			continue
		}
		if len(out) > 0 && n+int64(r.Length) > budget {
			break
		}
		out = append(out, r)
		n += int64(r.Length)
	}
	return out
}

func hasErr(ps []partPlan) bool {
	for _, p := range ps {
		if p.err != 0 {
			return true
		}
	}
	return false
}

func batchBytes(bs []cache.Batch) int64 {
	var n int64
	for _, b := range bs {
		n += int64(len(b.Data))
	}
	return n
}

func (c *Core) respondFetch(f *fetchWait, plans []partPlan) {
	f.done = true
	resp := FetchResp{ID: f.req.ID, Parts: make([]FetchPartResult, len(plans))}
	for i, p := range f.req.Parts {
		r := &resp.Parts[i]
		r.Err = plans[i].err
		if ti, ok := c.topics[p.Topic]; ok {
			if w := c.hw[tp{ti.id, uint32(p.Partition)}]; w != nil {
				r.HW, r.LogStart = w.hw, w.logStart
			}
		}
		if r.Err != 0 {
			continue
		}
		var out []byte
		for _, b := range plans[i].batches {
			start := len(out)
			out = append(out, b.Data...)
			kbatch.Patch(out[start:], b.Base)
			if end := b.Base + b.Records; end > r.HW {
				r.HW = end
			}
		}
		r.Records = out
	}
	c.emit(resp)
	c.emit(proc.Observe{Name: "fetch_ms", Value: float64(c.now - f.start)})
}

// indexed returns cached refs starting at the one containing offset.
func (c *Core) indexed(t tp, offset int64) ([]*pb.BatchRef, bool) {
	e := c.index[t]
	if e == nil || c.now-e.at > c.cfg.IndexTTLms {
		return nil, false
	}
	refs := e.refs
	i := sort.Search(len(refs), func(i int) bool { return refs[i].BaseOffset+int64(refs[i].Records) > offset })
	if i == len(refs) || refs[i].BaseOffset > offset {
		return nil, false
	}
	return refs[i:], true
}

func (c *Core) lookup(f *fetchWait, i int) {
	p := f.req.Parts[i]
	ti := c.topics[p.Topic]
	t := tp{ti.id, uint32(p.Partition)}
	if ws, ok := c.lookups[t]; ok {
		c.lookups[t] = append(ws, f)
		return
	}
	c.lookups[t] = []*fetchWait{f}
	c.Stats.Lookups++
	req := &pb.Request{Body: &pb.Request_Lookup{Lookup: &pb.LookupRequest{
		Topic: p.Topic, Partition: uint32(p.Partition), Offset: p.Offset, MaxBytes: c.cfg.LookupMaxBytes,
	}}}
	c.call(req, c.cfg.RPCTimeoutMs, func(r *pb.Response, err error) {
		ws := c.lookups[t]
		delete(c.lookups, t)
		if err == nil && r.GetLookup() == nil {
			err = errUnavailable
		}
		if err != nil {
			// Retry on the next pass after a short pause.
			c.after(c.rnd.Backoff(0, 20, 200), func() {
				for _, w := range ws {
					c.tryFetch(w)
				}
			})
			return
		}
		lr := r.GetLookup()
		if lr.ErrorCode == 0 || int16(lr.ErrorCode) == ErrOffsetOutOfRange {
			hw := lr.HighWatermark
			if w := c.hw[t]; w != nil && w.hw > hw {
				hw = w.hw // our own newer commit raced the lookup
			}
			c.hw[t] = &watermark{hw: hw, logStart: lr.LogStart, at: c.now}
		}
		if lr.ErrorCode == 0 && len(lr.Refs) > 0 {
			c.mergeIndex(t, lr.Refs)
		}
		for _, w := range ws {
			if lr.ErrorCode != 0 {
				for j, wp := range w.req.Parts {
					if wp.Topic == p.Topic && wp.Partition == p.Partition && (int16(lr.ErrorCode) != ErrOffsetOutOfRange || wp.Offset < lr.LogStart || wp.Offset > lr.HighWatermark) {
						w.errs[j] = int16(lr.ErrorCode)
					}
				}
			}
			c.tryFetch(w)
		}
	})
}

func (c *Core) mergeIndex(t tp, refs []*pb.BatchRef) {
	e := c.index[t]
	if e != nil && c.now-e.at <= c.cfg.IndexTTLms && len(e.refs) > 0 {
		last := e.refs[len(e.refs)-1]
		if last.BaseOffset+int64(last.Records) == refs[0].BaseOffset && len(e.refs) < 10_000 {
			e.refs = append(e.refs, refs...)
			e.at = c.now
			return
		}
	}
	c.index[t] = &indexEntry{refs: refs, at: c.now}
}

// fetchRanges issues one ranged GET per run of adjacent refs in an object.
func (c *Core) fetchRanges(f *fetchWait, obj string, refs []*pb.BatchRef) {
	sort.Slice(refs, func(i, j int) bool { return refs[i].OffsetInObj < refs[j].OffsetInObj })
	// Read ahead: also fetch the refs that follow in the same object.
	type rng struct {
		off, end uint32
		refs     []*pb.BatchRef
	}
	var rs []*rng
	for _, r := range refs {
		if n := len(rs); n > 0 && r.OffsetInObj <= rs[n-1].end+64<<10 {
			rs[n-1].end = max(rs[n-1].end, r.OffsetInObj+r.Length)
			rs[n-1].refs = append(rs[n-1].refs, r)
			continue
		}
		rs = append(rs, &rng{off: r.OffsetInObj, end: r.OffsetInObj + r.Length, refs: []*pb.BatchRef{r}})
	}
	for _, rg := range rs {
		key := cache.RangeKey{Object: obj, Off: rg.off, Len: rg.end - rg.off}
		if ws, ok := c.gets[key]; ok {
			c.gets[key] = append(ws, f)
			continue
		}
		c.gets[key] = []*fetchWait{f}
		c.Stats.Gets++
		c.getRange(key, rg.refs, 0)
	}
}

func (c *Core) getRange(key cache.RangeKey, refs []*pb.BatchRef, attempt int) {
	c.store(proc.StoreReq{Op: proc.OpGet, Key: key.Object, Off: int64(key.Off), Len: int64(key.Len)}, func(res proc.StoreRes) {
		if res.Err != nil && !errors.Is(res.Err, objstore.ErrNotFound) && attempt < 3 {
			c.after(c.rnd.Backoff(attempt, c.cfg.StoreRetryBaseMs, c.cfg.StoreRetryMaxMs), func() { c.getRange(key, refs, attempt+1) })
			return
		}
		ws := c.gets[key]
		delete(c.gets, key)
		switch {
		case res.Err == nil && len(res.Data) == int(key.Len):
			for _, r := range refs {
				s := r.OffsetInObj - key.Off
				c.data.Add(cache.RangeKey{Object: key.Object, Off: r.OffsetInObj, Len: r.Length}, res.Data[s:s+r.Length])
			}
		case errors.Is(res.Err, objstore.ErrNotFound):
			// The object was merged or garbage collected after we cached
			// its refs. Forget them; the next pass looks up again, once.
			c.data.Drop(key.Object)
			for t, e := range c.index {
				for _, r := range e.refs {
					if r.ObjectKey == key.Object {
						delete(c.index, t)
						break
					}
				}
			}
			for _, w := range ws {
				w.notFound++
			}
		}
		for _, w := range ws {
			c.tryFetch(w)
		}
	})
}

// wakeAll re-runs fetches waiting on partitions that just advanced.
func (c *Core) wakeAll(ts map[tp]bool) {
	keys := make([]tp, 0, len(ts))
	for t := range ts {
		keys = append(keys, t)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic != keys[j].topic {
			return string(keys[i].topic[:]) < string(keys[j].topic[:])
		}
		return keys[i].part < keys[j].part
	})
	for _, t := range keys {
		ws := c.waiting[t]
		delete(c.waiting, t)
		for _, w := range ws {
			delete(w.waitingOn, t)
			c.tryFetch(w)
		}
	}
}

func (c *Core) onWatch(ev *pb.WatchEvent) {
	if ev.Epoch < c.epochSeen {
		return
	}
	touched := map[tp]bool{}
	for _, wm := range ev.Watermarks {
		var id [16]byte
		copy(id[:], wm.TopicId)
		t := tp{id, wm.Partition}
		if w := c.hw[t]; w == nil || w.hw <= wm.HighWatermark {
			c.hw[t] = &watermark{hw: wm.HighWatermark, logStart: wm.LogStart, at: c.now}
			touched[t] = true
		}
	}
	c.wakeAll(touched)
}
