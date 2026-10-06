// Package checker verifies Sluice's invariants against a history of client
// operations and the final contents of the log. The simulator, the live
// chaos runner and sluicectl all use it.
//
//	I1 Acked-durable: every acked record is in the final log at its acked offset.
//	I2 Unique placement: no record appears twice unless its producer retried
//	   (and never for idempotent producers); nothing appears that was not produced.
//	I3 Dense offsets: each partition's batches tile [log start, high watermark).
//	I4 Prefix consistency: every fetch response is a contiguous slice of the final log.
//	I5 Offset commits: OffsetFetch returns the last acked OffsetCommit (allowing
//	   for commits whose outcome the client never learned).
//	I6 Deterministic replay: replaying the metadata log yields identical state.
//	I7 No dangling refs: no live batch ref points at a deleted object.
//	G1 Group assignments: within one generation no partition has two owners.
package checker

import (
	"fmt"
	"sort"
)

// Partition names a topic partition.
type Partition struct {
	Topic     string
	Partition int32
}

func (p Partition) String() string { return fmt.Sprintf("%s/%d", p.Topic, p.Partition) }

// Rec is one record at an offset.
type Rec struct {
	Offset int64
	Value  string
}

// ProduceOp is one logical send (a batch), including all its retries.
type ProduceOp struct {
	Client     int
	Idempotent bool
	Part       Partition
	Values     []string
	Attempts   int
	Acked      bool
	BaseOffset int64
	Start, End int64
}

// FetchOp is one fetch response for one partition.
type FetchOp struct {
	Client     int
	Part       Partition
	Offset     int64 // requested
	Records    []Rec
	Start, End int64
}

// CommitOp is one OffsetCommit for one partition.
type CommitOp struct {
	Group      string
	Part       Partition
	Offset     int64
	Acked      bool
	Start, End int64
}

// OffsetFetchOp is one OffsetFetch result for one partition (-1: none).
type OffsetFetchOp struct {
	Group      string
	Part       Partition
	Offset     int64
	Start, End int64
}

// SyncOp is one member's SyncGroup result.
type SyncOp struct {
	Group      string
	Generation int32
	Member     string
	Assigned   []Partition
}

// History is everything clients observed.
type History struct {
	Produces      []ProduceOp
	Fetches       []FetchOp
	Commits       []CommitOp
	OffsetFetches []OffsetFetchOp
	Syncs         []SyncOp
}

// PartLog is the final content of one partition: Records are dense from
// LogStart to HW.
type PartLog struct {
	LogStart, HW int64
	Records      []Rec
}

func (p *PartLog) at(off int64) (string, bool) {
	if off < p.LogStart || off >= p.HW {
		return "", false
	}
	i := off - p.LogStart
	if i >= int64(len(p.Records)) {
		return "", false
	}
	return p.Records[i].Value, true
}

// FinalLog is the final content of every partition.
type FinalLog map[Partition]*PartLog

// Violation is one broken invariant.
type Violation struct {
	Invariant string
	Detail    string
}

func (v Violation) String() string { return v.Invariant + ": " + v.Detail }

// maxPerInvariant bounds the report; one example is enough to debug.
const maxPerInvariant = 5

type report struct {
	vs     []Violation
	counts map[string]int
}

func (r *report) add(inv, format string, args ...any) {
	if r.counts == nil {
		r.counts = map[string]int{}
	}
	r.counts[inv]++
	if r.counts[inv] <= maxPerInvariant {
		r.vs = append(r.vs, Violation{inv, fmt.Sprintf(format, args...)})
	}
}

// Check verifies I1, I2, I4, I5 and G1 (I3, I6 and I7 need the bucket; see
// CheckBucket).
func Check(h *History, final FinalLog) []Violation {
	var r report
	checkProduces(&r, h, final)
	checkFetches(&r, h, final)
	checkOffsets(&r, h)
	checkGroups(&r, h)
	sort.Slice(r.vs, func(i, j int) bool {
		if r.vs[i].Invariant != r.vs[j].Invariant {
			return r.vs[i].Invariant < r.vs[j].Invariant
		}
		return r.vs[i].Detail < r.vs[j].Detail
	})
	var invs []string
	for inv, n := range r.counts {
		if n > maxPerInvariant {
			invs = append(invs, inv)
		}
	}
	sort.Strings(invs)
	for _, inv := range invs {
		r.vs = append(r.vs, Violation{inv, fmt.Sprintf("... and %d more", r.counts[inv]-maxPerInvariant)})
	}
	return r.vs
}

func checkProduces(r *report, h *History, final FinalLog) {
	type origin struct {
		op       *ProduceOp
		attempts int
	}
	produced := map[string]origin{}
	for i := range h.Produces {
		op := &h.Produces[i]
		for _, v := range op.Values {
			produced[v] = origin{op, op.Attempts}
		}
		if !op.Acked {
			continue
		}
		pl := final[op.Part]
		if pl == nil {
			r.add("I1", "acked send to %s: partition missing from final log", op.Part)
			continue
		}
		for j, v := range op.Values {
			off := op.BaseOffset + int64(j)
			if off < pl.LogStart {
				continue // removed by retention
			}
			got, ok := pl.at(off)
			if !ok {
				r.add("I1", "acked record %q at %s@%d is beyond the final high watermark %d", v, op.Part, off, pl.HW)
			} else if got != v {
				r.add("I1", "acked record %q at %s@%d, final log has %q", v, op.Part, off, got)
			}
		}
	}
	// I2: duplicates and phantoms.
	seen := map[string]int{}
	for p, pl := range final {
		for _, rec := range pl.Records {
			seen[rec.Value]++
			o, ok := produced[rec.Value]
			if !ok {
				r.add("I2", "record %q at %s@%d was never produced", rec.Value, p, rec.Offset)
				continue
			}
			if o.op.Part != p {
				r.add("I2", "record %q produced to %s appears in %s@%d", rec.Value, o.op.Part, p, rec.Offset)
			}
		}
	}
	for v, n := range seen {
		o, ok := produced[v]
		if !ok || n <= 1 {
			continue
		}
		switch {
		case o.op.Idempotent:
			r.add("I2", "idempotent record %q appears %d times", v, n)
		case n > o.attempts:
			r.add("I2", "record %q appears %d times after %d attempts", v, n, o.attempts)
		}
	}
}

func checkFetches(r *report, h *History, final FinalLog) {
	for _, f := range h.Fetches {
		pl := final[f.Part]
		if len(f.Records) == 0 {
			continue
		}
		if f.Records[0].Offset > f.Offset {
			r.add("I4", "fetch %s@%d started at %d (skipped offsets)", f.Part, f.Offset, f.Records[0].Offset)
		}
		for i, rec := range f.Records {
			if i > 0 && rec.Offset != f.Records[i-1].Offset+1 {
				r.add("I4", "fetch %s@%d not contiguous: %d after %d", f.Part, f.Offset, rec.Offset, f.Records[i-1].Offset)
				break
			}
			if pl == nil {
				r.add("I4", "fetch from %s, which is missing from the final log", f.Part)
				break
			}
			if rec.Offset < pl.LogStart {
				continue
			}
			got, ok := pl.at(rec.Offset)
			if !ok || got != rec.Value {
				r.add("I4", "fetch %s returned %q at %d, final log has %q", f.Part, rec.Value, rec.Offset, got)
				break
			}
		}
	}
}

func checkOffsets(r *report, h *History) {
	type key struct {
		g string
		p Partition
	}
	commits := map[key][]*CommitOp{}
	byValue := map[key]map[int64]*CommitOp{}
	for i := range h.Commits {
		c := &h.Commits[i]
		k := key{c.Group, c.Part}
		commits[k] = append(commits[k], c)
		if byValue[k] == nil {
			byValue[k] = map[int64]*CommitOp{}
		}
		byValue[k][c.Offset] = c
	}
	for _, f := range h.OffsetFetches {
		k := key{f.Group, f.Part}
		// Commits acked before the fetch started.
		var before []*CommitOp
		for _, c := range commits[k] {
			if c.Acked && c.End < f.Start {
				before = append(before, c)
			}
		}
		if f.Offset == -1 {
			if len(before) > 0 {
				r.add("I5", "offset fetch %s/%s returned nothing after a commit of %d was acked", f.Group, f.Part, before[0].Offset)
			}
			continue
		}
		c := byValue[k][f.Offset]
		if c == nil {
			r.add("I5", "offset fetch %s/%s returned %d, which was never committed", f.Group, f.Part, f.Offset)
			continue
		}
		if c.Start > f.End {
			r.add("I5", "offset fetch %s/%s returned %d from a commit that started later", f.Group, f.Part, f.Offset)
			continue
		}
		// Stale if c was acked and another acked commit began after c ended
		// and finished before this fetch began.
		if c.Acked {
			for _, c2 := range before {
				if c2.Start > c.End {
					r.add("I5", "offset fetch %s/%s returned stale %d; %d was acked later, before the fetch", f.Group, f.Part, f.Offset, c2.Offset)
					break
				}
			}
		}
	}
}

func checkGroups(r *report, h *History) {
	type gen struct {
		g string
		n int32
	}
	owner := map[gen]map[Partition]string{}
	for _, s := range h.Syncs {
		k := gen{s.Group, s.Generation}
		if owner[k] == nil {
			owner[k] = map[Partition]string{}
		}
		for _, p := range s.Assigned {
			if o, ok := owner[k][p]; ok && o != s.Member {
				r.add("G1", "group %s generation %d assigned %s to both %s and %s", s.Group, s.Generation, p, o, s.Member)
			}
			owner[k][p] = s.Member
		}
	}
}
