# Bugs found

Every row is a real bug found while building Sluice, with how it was found, its root cause, the fix and the test that keeps it fixed.

A note on seeds: a seed replays the run that exposed a bug against the code as it was *before* the fix. Code changes alter a seed's
trajectory, so today the same seed simply passes; the regression tests below are what keep each bug fixed. To watch one fail, revert the
fix and run `go run ./cmd/sluice-sim --seed <seed> --trace`.

| # | Found by | Symptom | Root cause | Fix | Regression test |
| --- | --- | --- | --- | --- | --- |
| 1 | Simulator, seeds 54, 86, 92, 94, 97, 100 (G1) | Two members owned the same partition "in generation 1" | Group membership is in memory; a new sequencer leader restarted generation numbers at 1, so different incarnations of a generation collided | New groups start at `epoch << 16` | `TestGroupGenerationsNeverRepeat`, sim G1 |
| 2 | Simulator, seed 5 (G1) | Same as #1, within one leader's term | Housekeeping deleted empty groups from memory; a later join recreated the group at the epoch's base generation | Per-group generation floor survives cleanup | `TestGroupGenerationsNeverRepeat` |
| 3 | Simulator, seeds 78, 150 (I1) | Acknowledged records missing; the log held other records at their offsets | A leader paused mid-append resumed after log GC had deleted its slot. Put-if-absent *succeeded* on the empty slot and the zombie acked offsets in a dead zone | Log GC deletes in ascending order; a writer that wins slot N confirms N−1 is byte-identical to what it applied, else steps down (DESIGN §3) | `TestWinningAGarbageCollectedSlotStepsDown` |
| 4 | Simulator, seed 12 (I6) | Replay from the oldest snapshot disagreed with recovery from the newest | Snapshots written by other leaders were never deleted, so stale snapshots outlived the log entries after them (and the checker replayed from one) | GC lists `meta/snap/` and deletes everything below the floor; the checker replays from the oldest snapshot with an unbroken log after it | sim I6 |
| 5 | Simulator, seed 1037 (I7) | A live batch ref pointed at a deleted object | A commit retry arrived after the segment's refs were merged away, it was tombstoned, deleted and the tombstone dropped; the bounded dedup cache had evicted it, so the retry looked new | Enforce `gc.grace ≥ 2 × max_segment_age`: any retry old enough to race GC is refused by the age check | `TestGCGraceCoversCommitRetries`, sim I7 |
| 6 | Simulator determinism check, seeds 1090, 1096 | Same seed, different fingerprint | The simulator assigned node clock skews while ranging over a Go map | Assign skews in sorted node order | `TestDeterministicReplay`; CI replays every seed twice |
| 7 | Planted-bug run (`ignore-fence`) | Agent panic | A failed sequencer answers `CODE_UNAVAILABLE` with no body; the agent treated any non-`NOT_LEADER` reply as success and dereferenced a nil heartbeat | Treat `UNAVAILABLE` as transient; guard every reply body | `TestCheckerCatchesPlantedBugs` exercises the path |
| 8 | Simulator trace, seed 78 | Every restart of `seq-a` caused a failover | The rank-0 sequencer attempted takeover immediately at startup, before any ping could tell it a leader existed | Wait two ping rounds before a first takeover | sim role statistics |
| 9 | End-to-end test (franz-go) and kafka-python | `UNKNOWN_TOPIC_OR_PARTITION` right after CreateTopics succeeded | Agents learn topics from the 1 s heartbeat; a produce could arrive first | An agent that sees an unknown topic refreshes its topic list (with retries) before answering | `TestConsumerGroupRebalance`, `kafka_python_test.py` |
| 10 | kafka-python compatibility test | `ValueError: Unrecognized ConfigSourceType: -1` | CreateTopics v5+ response configs left `Source` at kmsg's default −1 | Report `DEFAULT_CONFIG` or `DYNAMIC_TOPIC_CONFIG` | `kafka_python_test.py` |
| 11 | confluent-kafka (librdkafka) compatibility test | `invalid MessageSetSize -1`, consumer stalled | Empty partitions in flexible Fetch v12 encoded the record set as null | Always send an empty, non-null record set | `confluent_test.py` |
| 12 | Benchmark | p99 produce latency 2.6 s at 2 MB/s, 270 ms single-shot | One request per connection at a time (Kafka's model) while every produce waits for a flush window: pipelined requests stacked windows | Pipeline requests per connection, reply in order; commits in flush order keep per-partition ordering | `sluice-bench` (p99 now ~0.7 s at the 250 ms window with injected 30–70 ms storage latency) |
| 13 | Benchmark | At 50–100 ms flush windows PUTs flatlined at ~20/s and p99 rose to 1.3 s | Ordering fix #12 serialized commits per agent, capping each agent at one segment per sequencer round trip | Per-agent group commit: every ready segment goes in one ordered `CommitRequest` | `TestMultiSegmentCommitKeepsRequestOrder`, `sluice-bench` |
| 14 | Live chaos run (role log) | Killing the leader caused two failovers 1 s apart | The restarted leader reclaimed leadership just as the standby's timeout expired; standbys ignored a peer that was mid-recovery | Ping replies report `recovering_ms`; a standby treats a recently recovering peer as alive | live chaos result |
| 15 | Design review while specifying I5 | (latent) OffsetFetch could return an offset older than one already acknowledged | A deposed leader answers reads from memory for up to the next-slot check (~1 s) | OffsetFetch waits for a read barrier: a HEAD of the next slot started after the request must return 404 | sim I5 |
| 16 | Design review | (latent) Recovery could claim a slot below the true end of the log | The PRD's `meta/head` hint can be rewritten by a zombie to an old snapshot whose following entries were garbage-collected | Recover from the newest snapshot found with LIST; no head pointer | sim I6, `TestOffsetCommitIsDurableAcrossRecovery` |

## How the checker is kept honest

`go test -tags sluice_bugs -run Planted ./internal/sim` plants four bugs and fails unless the checker reports each within 60 seeds:

| Planted bug | Caught as |
| --- | --- |
| `ack-wrong-offset`: the sequencer acks base offset + 1 for some batches | I1, I4 |
| `drop-acked-batch`: a batch is acked but left out of the log entry | I1, I2, I4 |
| `ack-before-commit`: the agent acks after the segment PUT, before the commit | I1, I2 |
| `ignore-fence`: a leader treats a lost log slot as its own | FATAL (corrupt log on apply), RECOVERY, I1, I2, I4 |

`internal/checker/checker_test.go` also feeds the checker hand-built histories for every invariant, including the cases it must *not*
flag (an unacknowledged retry that lands twice, an unacknowledged offset commit that wins).
