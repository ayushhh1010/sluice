# ADR-003: One active sequencer

**Status:** accepted

**Decision.** All ordering goes through one active sequencer; a standby takes over after `leader.timeout`.

**Why.** One writer is simple to reason about and to verify: the simulator's invariants are statements about one totally ordered log.

**Ceiling.** Metadata entries per second (one PUT and one GET each) times batches per entry, plus the in-memory batch index. Group
commit makes batches per entry grow with load. DESIGN §17 describes sharding by partition range when the ceiling matters.
