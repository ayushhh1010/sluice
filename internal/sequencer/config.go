package sequencer

// Config holds sequencer settings. Zero values are replaced by defaults.
type Config struct {
	Node    string   // this sequencer's node name
	Peers   []string // other sequencer node names
	Cluster string

	CommitWindowMs    int64 // group commit window
	CommitMaxRequests int   // queued items that force an append

	LeaderTimeoutMs    int64 // standby takes over after this long without a leader
	PingIntervalMs     int64
	StartupStaggerMs   int64 // extra startup wait per rank before a first takeover
	NextSlotCheckMs    int64 // leader probes its next log slot this often
	StoreRetryBaseMs   int64
	StoreRetryMaxMs    int64
	SnapshotEvery      uint64
	SnapshotIntervalMs int64

	MemberTTLms int64

	GCGraceMs        int64
	GCIntervalMs     int64
	RetentionCheckMs int64
	OrphanSweepMs    int64
	OrphanMinAgeMs   int64
	MaxSegmentAgeMs  int64 // commits for older segments are refused
	RecentCommits    int

	MergeCheckMs     int64
	MergeMinRefs     int
	MergeMinBytes    int64
	MergeMaxBytes    int64
	MergeSmallRefMax int64 // refs at least this large are not worth merging

	DefaultPartitions   int32
	DefaultRetentionMs  int64
	MaxPartitions       int32
	PublicPrefix        string
	PublicRetentionMs   int64
	PublicMaxPartitions int32
	PublicMaxTopics     int
	PublicTopicTTLms    int64

	GroupMinSessionMs   int32
	GroupMaxSessionMs   int32
	GroupInitialDelayMs int64
	GroupHousekeepingMs int64

	ClassADailyLimit int64 // 0 disables the cost guard
	ChaosIntervalMs  int64
}

func (c *Config) setDefaults() {
	def := func(p *int64, v int64) {
		if *p == 0 {
			*p = v
		}
	}
	def(&c.CommitWindowMs, 20)
	if c.CommitMaxRequests == 0 {
		c.CommitMaxRequests = 256
	}
	def(&c.LeaderTimeoutMs, 10_000)
	def(&c.PingIntervalMs, 1_000)
	def(&c.StartupStaggerMs, 3_000)
	def(&c.NextSlotCheckMs, 1_000)
	def(&c.StoreRetryBaseMs, 50)
	def(&c.StoreRetryMaxMs, 2_000)
	if c.SnapshotEvery == 0 {
		c.SnapshotEvery = 1000
	}
	def(&c.SnapshotIntervalMs, 600_000)
	def(&c.MemberTTLms, 5_000)
	def(&c.GCGraceMs, 600_000)
	def(&c.GCIntervalMs, 10_000)
	def(&c.RetentionCheckMs, 30_000)
	def(&c.OrphanSweepMs, 3_600_000)
	def(&c.OrphanMinAgeMs, 3_600_000)
	def(&c.MaxSegmentAgeMs, 120_000)
	// Safety: the orphan sweep must never see an object young enough to
	// still be committed. Keep a wide margin for clock skew.
	if c.OrphanMinAgeMs < c.MaxSegmentAgeMs*3/2 {
		c.OrphanMinAgeMs = c.MaxSegmentAgeMs * 3 / 2
	}
	// Safety: once an object's tombstone is deleted, a late retry of its
	// commit would look brand new. Retries older than MaxSegmentAge are
	// refused, so deleting only after twice that long closes the window
	// (with room for clock skew) without an unbounded dedup table.
	if c.GCGraceMs < 2*c.MaxSegmentAgeMs {
		c.GCGraceMs = 2 * c.MaxSegmentAgeMs
	}
	if c.RecentCommits == 0 {
		c.RecentCommits = 8192
	}
	def(&c.MergeCheckMs, 10_000)
	if c.MergeMinRefs == 0 {
		c.MergeMinRefs = 16
	}
	def(&c.MergeMinBytes, 32<<20)
	def(&c.MergeMaxBytes, 64<<20)
	def(&c.MergeSmallRefMax, 1<<20)
	if c.DefaultPartitions == 0 {
		c.DefaultPartitions = 1
	}
	def(&c.DefaultRetentionMs, 24*3_600_000)
	if c.MaxPartitions == 0 {
		c.MaxPartitions = 1024
	}
	if c.PublicPrefix == "" {
		c.PublicPrefix = "pg-"
	}
	def(&c.PublicRetentionMs, 3_600_000)
	if c.PublicMaxPartitions == 0 {
		c.PublicMaxPartitions = 4
	}
	if c.PublicMaxTopics == 0 {
		c.PublicMaxTopics = 200
	}
	def(&c.PublicTopicTTLms, 24*3_600_000)
	if c.GroupMinSessionMs == 0 {
		c.GroupMinSessionMs = 6_000
	}
	if c.GroupMaxSessionMs == 0 {
		c.GroupMaxSessionMs = 1_800_000
	}
	def(&c.GroupInitialDelayMs, 1_000)
	def(&c.GroupHousekeepingMs, 100)
	def(&c.ChaosIntervalMs, 120_000)
}
