package sequencer

import (
	"bytes"
	"errors"
	"sort"
	"strings"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/kbatch"
	"github.com/ayushhh1010/sluice/internal/metalog"
	"github.com/ayushhh1010/sluice/internal/objstore"
	"github.com/ayushhh1010/sluice/internal/proc"
	"github.com/ayushhh1010/sluice/internal/segment"
)

// ---- Snapshots and log GC ----
//
// GC invariant: log entries above the second-newest snapshot are never
// deleted, and recovery always starts from the newest snapshot that exists
// (found with LIST, not a hint). A zombie can re-create an old snapshot
// after GC, but never one newer than every snapshot, so recovery never
// probes into a deleted range, where put-if-absent would let it rewrite
// history.

func (c *Core) maybeSnapshot() {
	if c.role != roleLeader || c.snapshotting || c.st.Seq == c.lastSnapSeq {
		return
	}
	if c.st.Seq-c.lastSnapSeq < c.cfg.SnapshotEvery && c.now-c.lastSnapAt < c.cfg.SnapshotIntervalMs {
		return
	}
	c.snapshotting = true
	seq := c.st.Seq
	snap := c.st.Snapshot(true)
	snap.LastEntrySha256 = append([]byte(nil), c.lastHash[:]...)
	snap.ClassADay, snap.ClassACount = c.classADay, c.classACount
	data := metalog.EncodeSnapshot(snap)
	c.storeRetry(proc.StoreReq{Op: proc.OpPut, Key: c.keys.Snap(seq), Data: data}, func(proc.StoreRes) {
		c.snapshotting = false
		c.lastSnapSeq, c.lastSnapAt = seq, c.now
		c.Stats.Snapshots++
		n := len(c.st.PriorSnaps)
		c.st.PriorSnaps = append(c.st.PriorSnaps, seq)
		if n == 0 || c.gcRunning {
			return
		}
		// Entries up to the previous snapshot (now the second-newest) can go.
		keep := c.st.PriorSnaps[n-1]
		c.gcRunning = true
		c.gcLog(c.st.LogGCFloor+1, keep, func() {
			c.st.LogGCFloor = keep
			c.st.PriorSnaps = trimSnaps(c.st.PriorSnaps, keep)
			// Snapshots below the floor are useless. LIST finds them all,
			// including ones other leaders wrote; it runs once per snapshot.
			c.storeRetry(proc.StoreReq{Op: proc.OpList, Prefix: c.keys.SnapPrefix()}, func(res proc.StoreRes) {
				c.gcRunning = false
				var keys []string
				for _, o := range res.List {
					if s, ok := metalog.SeqFromKey(o.Key); ok && s < keep {
						keys = append(keys, o.Key)
					}
				}
				c.deleteKeys(keys)
			})
		})
	})
}

// gcLog deletes log entries from..to one at a time in ascending order, so
// the deleted entries always form a prefix of the log (confirmPredecessor
// relies on it). The floor advances only once the whole range is gone, so
// the next round, by this leader or any other, starts at or below the
// prefix end.
func (c *Core) gcLog(from, to uint64, done func()) {
	if from > to {
		done()
		return
	}
	c.storeRetry(proc.StoreReq{Op: proc.OpDelete, Key: c.keys.Log(from)}, func(proc.StoreRes) {
		c.gcLog(from+1, to, done)
	})
}

func trimSnaps(snaps []uint64, keep uint64) []uint64 {
	var out []uint64
	for _, s := range snaps {
		if s >= keep {
			out = append(out, s)
		}
	}
	return out
}

// deleteKeys deletes keys with bounded concurrency, best effort.
func (c *Core) deleteKeys(keys []string) {
	const parallel = 16
	next := 0
	var launch func()
	launch = func() {
		if next >= len(keys) {
			return
		}
		k := keys[next]
		next++
		c.storeRetry(proc.StoreReq{Op: proc.OpDelete, Key: k}, func(proc.StoreRes) { launch() })
	}
	for i := 0; i < parallel; i++ {
		launch()
	}
}

// ---- Object GC: tombstones are deleted after their grace period ----

func (c *Core) runGC() {
	var due []string
	for k, at := range c.st.Tombstones {
		if at <= c.now && !c.deleting[k] {
			due = append(due, k)
		}
	}
	sort.Strings(due)
	if len(due) > 256 {
		due = due[:256]
	}
	for _, k := range due {
		k := k
		c.deleting[k] = true
		c.storeRetry(proc.StoreReq{Op: proc.OpDelete, Key: k}, func(res proc.StoreRes) {
			delete(c.deleting, k)
			if res.Err == nil || errors.Is(res.Err, objstore.ErrNotFound) {
				delete(c.st.Tombstones, k)
				c.Stats.DeletedObjects++
			}
		})
	}
}

// ---- Retention and public topic expiry ----

func (c *Core) runRetention() {
	trunc := &pb.Truncate{}
	var expired []TopicID
	for _, t := range c.st.sortedTopics() {
		if strings.HasPrefix(t.Name, c.cfg.PublicPrefix) && c.cfg.PublicTopicTTLms > 0 && c.now-t.CreatedMs > c.cfg.PublicTopicTTLms {
			expired = append(expired, t.ID)
			continue
		}
		if t.Retention <= 0 {
			continue
		}
		cutoff := c.now - t.Retention
		for i, p := range t.Parts {
			n := 0
			for n < len(p.Refs) && p.Refs[n].MaxTs < cutoff {
				n++
			}
			if n == 0 {
				continue
			}
			start := p.Next
			if n < len(p.Refs) {
				start = p.Refs[n].BaseOffset
			}
			if start > p.LogStart {
				trunc.Starts = append(trunc.Starts, &pb.LogStart{TopicId: append([]byte(nil), t.ID[:]...), Partition: uint32(i), LogStart: start})
			}
		}
	}
	if len(trunc.Starts) == 0 && len(expired) == 0 {
		return
	}
	c.enqueue(&queued{
		plan: func(pl *planner) {
			if len(trunc.Starts) > 0 {
				pl.add(kindTruncate, &pb.Op{Body: &pb.Op_Truncate{Truncate: trunc}})
			}
			for _, id := range expired {
				if c.st.Topics[id] != nil && !pl.deleted[id] {
					pl.deleted[id] = true
					pl.add(kindDelete, &pb.Op{Body: &pb.Op_DeleteTopic{DeleteTopic: &pb.DeleteTopic{TopicId: append([]byte(nil), id[:]...)}}})
				}
			}
		},
		done: func(ok bool) {
			if ok && len(expired) > 0 {
				c.topicsVer++
			}
		},
	})
}

// ---- Segment merging ----
//
// Agent segments are small and mix partitions, so one partition's data is
// scattered. The merger copies a run of small refs into one object per
// partition, then appends ReplaceObjects. Batch bytes and offsets never
// change; apply swaps only refs that are still current.

type mergeJob struct {
	tp   TP
	refs []*pb.BatchRef
	data map[*pb.BatchRef][]byte
}

func (c *Core) isSmallDataRef(r *pb.BatchRef) bool {
	return strings.HasPrefix(r.ObjectKey, c.keys.DataPrefix()) && int64(r.Length) < c.cfg.MergeSmallRefMax
}

func (c *Core) pickMerge() *mergeJob {
	for _, t := range c.st.sortedTopics() {
		for i, p := range t.Parts {
			var count int
			var size int64
			for _, r := range p.Refs {
				if c.isSmallDataRef(r) {
					count++
					size += int64(r.Length)
				}
			}
			if count < c.cfg.MergeMinRefs && size < c.cfg.MergeMinBytes {
				continue
			}
			// Longest run of consecutive small data refs, from the oldest.
			var run []*pb.BatchRef
			var runBytes int64
			for _, r := range p.Refs {
				if !c.isSmallDataRef(r) {
					if len(run) >= 2 {
						break
					}
					run, runBytes = nil, 0
					continue
				}
				if runBytes+int64(r.Length) > c.cfg.MergeMaxBytes {
					break
				}
				run = append(run, r)
				runBytes += int64(r.Length)
			}
			if len(run) >= 2 {
				return &mergeJob{tp: TP{t.ID, uint32(i)}, refs: run, data: map[*pb.BatchRef][]byte{}}
			}
		}
	}
	return nil
}

func (c *Core) runMerge() {
	if c.merge != nil {
		return
	}
	job := c.pickMerge()
	if job == nil {
		return
	}
	c.merge = job
	// Coalesce adjacent ranges per object into one ranged GET each.
	byObj := map[string][]*pb.BatchRef{}
	var objs []string
	for _, r := range job.refs {
		if byObj[r.ObjectKey] == nil {
			objs = append(objs, r.ObjectKey)
		}
		byObj[r.ObjectKey] = append(byObj[r.ObjectKey], r)
	}
	sort.Strings(objs)
	type rng struct {
		key      string
		off, end int64
		refs     []*pb.BatchRef
	}
	var ranges []*rng
	for _, k := range objs {
		refs := byObj[k]
		sort.Slice(refs, func(i, j int) bool { return refs[i].OffsetInObj < refs[j].OffsetInObj })
		var cur *rng
		for _, r := range refs {
			off, end := int64(r.OffsetInObj), int64(r.OffsetInObj)+int64(r.Length)
			if cur != nil && off-cur.end <= 64<<10 {
				cur.end = max(cur.end, end)
				cur.refs = append(cur.refs, r)
				continue
			}
			cur = &rng{key: k, off: off, end: end, refs: []*pb.BatchRef{r}}
			ranges = append(ranges, cur)
		}
	}
	remaining := len(ranges)
	failed := false
	for _, rg := range ranges {
		rg := rg
		c.storeRetry(proc.StoreReq{Op: proc.OpGet, Key: rg.key, Off: rg.off, Len: rg.end - rg.off}, func(res proc.StoreRes) {
			if c.merge != job {
				return
			}
			if res.Err != nil || int64(len(res.Data)) != rg.end-rg.off {
				failed = true
			} else {
				for _, r := range rg.refs {
					s := int64(r.OffsetInObj) - rg.off
					job.data[r] = res.Data[s : s+int64(r.Length)]
				}
			}
			if remaining--; remaining == 0 {
				if failed {
					c.merge = nil
					return
				}
				c.writeMerged(job)
			}
		})
	}
}

func (c *Core) writeMerged(job *mergeJob) {
	b := segment.NewBuilder(c.now, c.cfg.Node)
	for _, r := range job.refs {
		data := job.data[r]
		h, err := kbatch.ReadHeader(data)
		if err != nil || h.Length != len(data) {
			c.merge = nil // corrupt source batch; leave it in place
			return
		}
		b.Add(segment.Entry{
			TopicID: job.tp.Topic, Partition: job.tp.Partition, Records: r.Records, MaxTS: r.MaxTs,
			ProducerID: h.ProducerID, ProducerEpoch: h.ProducerEpoch, BaseSeq: h.BaseSequence,
		}, data)
	}
	obj, entries, order := b.Finish()
	key := c.keys.Merged(job.tp.Topic[:], job.tp.Partition, job.refs[0].BaseOffset, metalog.ULID(c.now, c.rnd.Uint64(), c.rnd.Uint64()))
	c.storeRetry(proc.StoreReq{Op: proc.OpPutIfAbsent, Key: key, Data: obj}, func(res proc.StoreRes) {
		if c.merge != job {
			return
		}
		if res.Err != nil {
			// A 412 here means an earlier attempt of ours landed; verify.
			c.storeRetry(proc.StoreReq{Op: proc.OpGet, Key: key}, func(got proc.StoreRes) {
				if c.merge == job && got.Err == nil && bytes.Equal(got.Data, obj) {
					c.commitMerge(job, key, entries, order)
				} else if c.merge == job {
					c.merge = nil
				}
			})
			return
		}
		c.commitMerge(job, key, entries, order)
	})
}

func (c *Core) commitMerge(job *mergeJob, key string, entries []segment.Entry, order []int) {
	rep := &pb.ReplaceObjects{}
	for i, e := range entries {
		r := job.refs[order[i]]
		rep.Replacements = append(rep.Replacements, &pb.Replacement{
			TopicId: append([]byte(nil), job.tp.Topic[:]...), Partition: job.tp.Partition, BaseOffset: r.BaseOffset,
			OldKey: r.ObjectKey, NewKey: key, NewOffsetInObj: e.Offset,
		})
	}
	c.enqueue(&queued{
		plan: func(pl *planner) { pl.add(kindReplace, &pb.Op{Body: &pb.Op_Replace{Replace: rep}}) },
		done: func(ok bool) {
			if c.merge == job {
				c.merge = nil
			}
			if ok {
				c.Stats.MergedObjects++
			}
		},
	})
}

// ---- Orphan sweep ----
//
// Agents that crash between a segment PUT and its commit leave unreferenced
// objects. Hourly, delete objects older than OrphanMinAge that no live ref,
// tombstone or pending commit names. Before deleting, the leader re-checks
// that its next log slot is still empty: a paused zombie with a stale view
// would otherwise delete objects the real leader committed.

func (c *Core) runOrphanSweep() {
	if c.sweeping {
		return
	}
	c.sweeping = true
	var found []objstore.ObjectInfo
	prefixes := []string{c.keys.DataPrefix(), c.keys.MergedPrefix()}
	var list func(pi int, after string)
	list = func(pi int, after string) {
		if pi == len(prefixes) {
			c.confirmAndSweep(found, 0)
			return
		}
		c.storeRetry(proc.StoreReq{Op: proc.OpList, Prefix: prefixes[pi], After: after, Limit: 1000}, func(res proc.StoreRes) {
			for _, o := range res.List {
				if c.now-o.LastModified.UnixMilli() > c.cfg.OrphanMinAgeMs && c.isOrphan(o.Key) {
					found = append(found, o)
				}
			}
			if len(res.List) == 1000 {
				list(pi, res.List[len(res.List)-1].Key)
			} else {
				list(pi+1, "")
			}
		})
	}
	list(0, "")
}

func (c *Core) isOrphan(key string) bool {
	_, waiting := c.commitWaiters[key]
	return c.st.RefCount[key] == 0 && c.st.Tombstones[key] == 0 && !waiting && c.st.RecentByKey[key] == nil
}

func (c *Core) confirmAndSweep(found []objstore.ObjectInfo, attempt int) {
	if len(found) == 0 {
		c.sweeping = false
		return
	}
	if c.appending != nil {
		c.after(500, func() { c.confirmAndSweep(found, attempt+1) })
		return
	}
	seq := c.st.Seq + 1
	c.storeRetry(proc.StoreReq{Op: proc.OpHead, Key: c.keys.Log(seq)}, func(res proc.StoreRes) {
		if c.st.Seq+1 != seq || c.appending != nil {
			c.confirmAndSweep(found, attempt+1) // our own append raced the probe
			return
		}
		if res.Err == nil {
			c.stepDown("next slot taken (orphan sweep)")
			return
		}
		var keys []string
		for _, o := range found {
			if c.isOrphan(o.Key) {
				keys = append(keys, o.Key)
			}
		}
		c.Stats.DeletedObjects += uint64(len(keys))
		c.deleteKeys(keys)
		c.sweeping = false
	})
}
