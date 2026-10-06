package sim

import "github.com/ayushhh1010/sluice/internal/proc"

// Knobs are the tunables each run randomizes (FoundationDB's BUGGIFY): odd
// flush windows, tiny caches, frequent snapshots and merges push the
// system into rare paths that default settings would almost never reach.
type Knobs struct {
	Agents            int
	FlushWindowMs     int64
	FlushMaxBytes     int64
	BufferMaxBytes    int64
	CommitDeadlineMs  int64
	TailCacheBytes    int64
	DataCacheBytes    int64
	CommitWindowMs    int64
	CommitMaxRequests int
	LeaderTimeoutMs   int64
	SnapshotEvery     uint64
	MemberTTLms       int64
	GCGraceMs         int64
	OrphanSweepMs     int64
	OrphanMinAgeMs    int64
	MaxSegmentAgeMs   int64
	RecentCommits     int
	MergeMinRefs      int
	RetentionMs       int64

	StoreErrorPct   int
	LostResponsePct int
	DropPerMille    int
	FaultEveryMs    int64

	Producers, IdempotentProducers, Fetchers, Committers, GroupMembers int
}

func defaultKnobs() Knobs {
	return Knobs{
		Agents: 2, FlushWindowMs: 100, FlushMaxBytes: 1 << 20, BufferMaxBytes: 16 << 20, CommitDeadlineMs: 30_000,
		TailCacheBytes: 8 << 20, DataCacheBytes: 16 << 20, CommitWindowMs: 20, CommitMaxRequests: 256,
		LeaderTimeoutMs: 4_000, SnapshotEvery: 50, MemberTTLms: 5_000, GCGraceMs: 20_000,
		OrphanSweepMs: 30_000, OrphanMinAgeMs: 60_000, MaxSegmentAgeMs: 20_000, RecentCommits: 1024,
		MergeMinRefs: 8, RetentionMs: 0,
		StoreErrorPct: 2, LostResponsePct: 2, DropPerMille: 5, FaultEveryMs: 5_000,
		Producers: 3, IdempotentProducers: 2, Fetchers: 2, Committers: 2, GroupMembers: 3,
	}
}

func buggify(r *proc.Rand) Knobs {
	k := defaultKnobs()
	pick := func(xs ...int64) int64 { return xs[r.Intn(len(xs))] }
	k.Agents = 2 + r.Intn(2)
	k.FlushWindowMs = pick(5, 20, 100, 250, 500)
	k.FlushMaxBytes = pick(512, 4<<10, 64<<10, 1<<20)
	k.BufferMaxBytes = pick(16<<10, 1<<20, 16<<20)
	k.CommitDeadlineMs = pick(2_000, 5_000, 15_000, 30_000)
	k.TailCacheBytes = pick(1<<10, 64<<10, 8<<20)
	k.DataCacheBytes = pick(1<<10, 64<<10, 16<<20)
	k.CommitWindowMs = pick(0, 5, 20, 50)
	k.CommitMaxRequests = int(pick(1, 4, 32, 256))
	k.LeaderTimeoutMs = pick(2_000, 4_000, 8_000)
	k.SnapshotEvery = uint64(pick(3, 10, 50, 200))
	k.MemberTTLms = pick(3_000, 5_000)
	k.GCGraceMs = pick(5_000, 20_000, 60_000)
	k.OrphanSweepMs = pick(10_000, 30_000)
	k.OrphanMinAgeMs = pick(30_000, 60_000)
	k.MaxSegmentAgeMs = pick(3_000, 10_000, 20_000)
	k.RecentCommits = int(pick(8, 64, 1024))
	k.MergeMinRefs = int(pick(2, 4, 16))
	k.RetentionMs = pick(0, 0, 20_000, 45_000)
	k.StoreErrorPct = int(pick(0, 1, 3, 8))
	k.LostResponsePct = int(pick(0, 1, 3, 8))
	k.DropPerMille = int(pick(0, 2, 10, 30))
	k.FaultEveryMs = pick(2_000, 5_000, 10_000)
	k.Producers = 1 + r.Intn(4)
	k.IdempotentProducers = r.Intn(3)
	k.Fetchers = 1 + r.Intn(3)
	k.Committers = r.Intn(3)
	k.GroupMembers = int(pick(0, 2, 3))
	return k
}
