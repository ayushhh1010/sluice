# ADR-007: Recover from the newest snapshot by LIST, and confirm log continuity after every append

**Status:** accepted (supersedes the PRD's `meta/head` hint)

**Context.** Put-if-absent proves a slot is empty, not that it was never written. Once old log entries are garbage-collected, a zombie
leader can "win" a deleted slot (found by the simulator, BUGS.md #3), and a recovery that starts from a stale `meta/head` hint can probe
into the deleted range and claim a slot below the true end of the log (BUGS.md #16).

**Decision.**
1. Log GC deletes entries one at a time in ascending order; the deleted entries always form a prefix of the log.
2. After winning slot N, a writer reads N−1 and requires the exact entry it applied there before acknowledging anything.
3. Recovery LISTs `meta/snap/` and starts from the newest snapshot. There is no head pointer.

**Consequences.** One extra GET per append and one LIST per failover. Safety remains independent of clocks.
