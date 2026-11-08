# ADR-007: Recover from the newest snapshot by LIST, and confirm log continuity after every append

**Status:** accepted (supersedes the PRD's `meta/head` hint)

**Context.** Put-if-absent proves a slot is empty, not that it was never written. Once old log entries are garbage-collected, a zombie
leader can "win" a deleted slot (found by the simulator, BUGS.md #3), and a recovery that starts from a stale `meta/head` hint can probe
into the deleted range and claim a slot below the true end of the log (BUGS.md #16).

**Decision.**
1. Log GC deletes entries one at a time in ascending order; the deleted entries always form a prefix of the log.
2. After winning slot N, a writer reads N−1 and requires the exact entry it applied there before acknowledging anything. Slot 1 has
   no predecessor, so a writer that wins it requires `meta/snap/` to be empty instead (log GC never runs before two snapshots exist;
   BUGS.md #17).
3. Recovery LISTs `meta/snap/` and starts from the newest snapshot. There is no head pointer.
4. A leader that checks whether anyone has appended past it (read barrier, stale-read probe, orphan sweep) treats a 404 on its next
   slot as proof only after reading its own last entry back byte for byte (BUGS.md #20).

**Consequences.** One extra GET per append and per leadership probe, one LIST per failover and per win of slot 1. Safety remains
independent of clocks.
