# Benchmarks

Every number here comes with the hardware, configuration, command and raw output that produced it. Raw files live in
[`docs/results/`](results). Numbers that need infrastructure this machine does not have (Cloudflare R2, a 2-vCPU ARM VM) are marked
**not yet measured** rather than estimated.

**Machine:** laptop, Intel Core i5-12450H (8 cores, 12 threads), 16 GB RAM, Windows 11, Go 1.27. All processes run on one machine, so
client, agents, sequencers and the simulated object store compete for the same CPU.

## Latency vs cost

The design trades latency for object-store writes: the flush window sets how long a batch waits before its segment is uploaded. The
benchmark runs a fresh in-process cluster (2 agents, 2 sequencers) per flush window over an in-memory store that injects **30–70 ms
of latency per request** (uniform), roughly the range of a nearby S3-compatible region, and counts Class A requests (PUT, LIST).

Workload: franz-go producer, idempotent, `acks=all`, linger 50 ms, no compression, 1 KiB random records at 2 MB/s across 6 partitions;
a tail consumer measures produce-to-consume latency. 5 s warmup, 30 s measured per window.

```bash
go run ./cmd/sluice-bench -flush-windows 50ms,100ms,250ms,500ms,1s -duration 30s -warmup 5s -rate 2 -linger 50ms
```

| Flush window | Produce p50 | Produce p99 | p99.9 | End-to-end p50 | End-to-end p99 | PUTs/s | PUTs per GB | Class A $ per GB (R2) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 ms | 437 ms | 601 ms | 657 ms | 439 ms | 602 ms | 29.2 | 14,600 | $0.066 |
| 100 ms | 447 ms | 633 ms | 697 ms | 449 ms | 635 ms | 22.2 | 11,100 | $0.050 |
| **250 ms (default)** | **389 ms** | **690 ms** | 767 ms | 392 ms | 694 ms | **11.3** | **5,650** | **$0.025** |
| 500 ms | 697 ms | 1,150 ms | 1,191 ms | 708 ms | 1,157 ms | 6.3 | 3,133 | $0.014 |
| 1 s | 1,483 ms | 2,175 ms | 2,219 ms | 1,510 ms | 2,194 ms | 3.8 | 1,900 | $0.0085 |

Raw: [`bench-curve-2026-10-06.txt`](results/bench-curve-2026-10-06.txt), [`.json`](results/bench-curve-2026-10-06.json).

**Reading the curve.** Above 250 ms, latency tracks the flush window and PUTs fall in proportion. Below 250 ms latency stops improving:
the floor is the metadata pipeline (one append in flight, each a PUT plus a continuity GET, plus the producer's linger), so smaller
windows only buy more PUTs. That is why 250 ms is the default. At a sustained 2 MB/s, 11.3 PUTs/s is about 29 M Class A requests a
month, around $130 above R2's free tier; the public demo stays free because the cost guard caps it at 30,000 a day (DESIGN §15).

### Without injected storage latency

The same workload at the default 250 ms window, with the in-memory store answering immediately (the PRD's "local MinIO, no injected
latency" row; this is an in-memory store, which is faster than MinIO):

```bash
go run ./cmd/sluice-bench -flush-windows 250ms -duration 30s -warmup 5s -rate 2 -linger 50ms -store-latency 0 -store-jitter 0
```

| Produce p50 | Produce p99 | p99.9 | End-to-end p99 | PUTs per GB |
| --- | --- | --- | --- | --- |
| 222 ms | 329 ms | 345 ms | 337 ms | 6,484 |

PRD target: p99 under 400 ms. Raw: [`bench-nolatency-2026-10-06.txt`](results/bench-nolatency-2026-10-06.txt).

### Throughput

Open loop (the producer sends as fast as it can), 250 ms window, injected 30–70 ms latency:

```bash
go run ./cmd/sluice-bench -flush-windows 250ms -duration 30s -warmup 5s -rate 0 -linger 50ms
```

| Sustained throughput | Per agent | PUTs per GB |
| --- | --- | --- |
| 73.3 MB/s | ~36.7 MB/s | 405 |

PRD target: at least 20 MB/s per agent (on a 2-vCPU ARM VM, which this is not). Latency in an open-loop run only measures how much the
client queued, so it is not reported. At full 4 MiB segments the cost per GB falls to 405 PUTs. Raw:
[`bench-max-2026-10-06.txt`](results/bench-max-2026-10-06.txt).

### Not yet measured

| PRD metric | Status |
| --- | --- |
| p99 produce latency on R2 (target < 1.5 s) | Needs the R2 bucket; run `sluice-bench -bootstrap k1.<domain>:9093` once deployed |
| Throughput per agent on a 2-vCPU ARM VM | Needs the VM |
| p99 produce-to-consume on R2 (target < 2 s) | Needs the R2 bucket |

## Correctness

### Deterministic simulation

Each seed runs a 60-second virtual workload (plain and idempotent producers, fetchers, offset committers, group members) against two
sequencers and two or three agents while injecting crashes, pauses longer than the leader timeout (zombies), partitions, node
isolation, object-store errors, slowdowns and applied-but-lost responses, with BUGGIFY-randomized configuration. The checker verifies
I1–I7 and G1 from the history and the final bucket, and every leader's state is checked for internal consistency throughout.

```bash
go run ./cmd/sluice-sim -seeds 6000 -start 1000000 -json sim.json
```

| Run (final code) | Seeds | Client operations | Violations | Wall time (12 threads) |
| --- | --- | --- | --- | --- |
| 2026-10-11, seeds 11,000,000–11,011,999 (after fixes #17–#19) | 12,000 | **6,516,413** | **0** | 29 min |
| 2026-10-06, seeds 1,000,000–1,005,999 (before #17–#19) | 6,000 | 3,293,307 | 0 | 26 min |

Raw: [`sim-confirm-2026-10-11.txt`](results/sim-confirm-2026-10-11.txt), [`sim-final-2026-10-06.txt`](results/sim-final-2026-10-06.txt).

The 2026-10-06 run was clean but not exhaustive: the nightly job's larger daily sweeps then found BUGS.md #17, which needs two sequencer pauses in a narrow window, so per-run counts are evidence, not proof.

Earlier runs on the code as it evolved (each after the fixes listed in BUGS.md up to that point):

| Code version | Seeds | Client operations | Violations | Nondeterministic |
| --- | --- | --- | --- | --- |
| after ordered commits | 3,000 | 1,567,870 | 0 | n/a |
| after election fix (#14), replayed twice | 1,000 | 514,913 | 0 | 0 |
| after per-agent group commit (#13), replayed twice | 1,500 | 800,843 | 0 | 0 |

The checker itself is tested: four planted bugs must each be caught (`go test -tags sluice_bugs -run Planted ./internal/sim`).

### Live chaos

Real OS processes on one machine (two sequencers, two agents, a fresh local-directory object store), with supervisors that restart
killed processes after a second. A 3-minute run killed agents with `kill -9` every 20–40 seconds and the active sequencer once, while
four idempotent franz-go producers (acks=all, 100 records/s each) and a tail consumer ran. The history was then checked against the
final log, and the bucket against I3, I6 and I7.

```bash
sluice-chaos -b 127.0.0.1:19092,127.0.0.1:19093 -duration 180s -kill-min 20s -kill-max 40s   -kill 'kill -9 $(cat /tmp/chaos-agent-{n}.pid)' -kill-sequencer 'kill -9 $(cat /tmp/chaos-seq-a.pid)'   -store file:///tmp/chaos-final -cluster chaos -rate 100
```

| Run | Kills | Sends | Acked | Violations (I1, I2, I4, I3, I6, I7) | Longest produce stall | Failovers |
| --- | --- | --- | --- | --- | --- | --- |
| final code | 4 agents, the active sequencer once | 71,133 | 71,133 | **0** | **3.5 s** | 1 (epoch 1 → 2, ~3.7 s after the kill) |
| before fix #14 | 4 agents, the active sequencer once | 59,968 | 59,968 | 0 | 3.5 s | 2 back to back |

PRD targets: producers resume within 10 s of an agent kill; sequencer failover within 15 s. Raw:
[`chaos-local-final-2026-10-06.json`](results/chaos-local-final-2026-10-06.json),
[`chaos-local-2026-10-06.json`](results/chaos-local-2026-10-06.json). The nightly workflow runs the same tool for 30 minutes against
the deployed cluster once it exists.

### Client compatibility

| Client | Tests | Result |
| --- | --- | --- |
| franz-go (Go) | produce, consume, consumer group with rebalance, offsets, idempotent producer, agent and sequencer kills, TLS + SASL, quotas | pass (`go test ./test/e2e`) |
| confluent-kafka-python (librdkafka 2.15.1) | idempotent produce, consumer group, offsets | pass (`test/compat/confluent_test.py`) |
| kafka-python 3.0 | produce, consume, three-member consumer group, offsets | pass (`test/compat/kafka_python_test.py`) |
| kcat (librdkafka) | 10,000-message round trip | runs in CI through Docker (`test/compat/kcat.sh`); not run on this machine |
