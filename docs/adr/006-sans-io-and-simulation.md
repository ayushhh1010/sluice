# ADR-006: Sans-I/O cores and deterministic simulation

**Status:** accepted

**Decision.** The agent and sequencer are pure state machines (`Step(now, event) → []effect`). A thin runtime executes their effects with
real goroutines, sockets and storage; a single-threaded simulator executes the same cores in virtual time with injected faults, and a
checker verifies invariants from the recorded history.

**Why.** Distributed bugs are rare interleavings. A seeded, single-threaded run turns each into a failure replayable from one 64-bit
seed, and runs thousands of fault schedules per minute. The simulator found every safety bug in BUGS.md that a test found.

**Enforcement.** `internal/sim/purity_test.go` parses the core packages and fails on goroutines, wall-clock calls, global randomness or
I/O imports; `golangci-lint` (forbidigo) repeats the clock and randomness checks. CI replays every seed twice and compares fingerprints.

**Costs.** Callback-style code in the cores, and determinism only at the core boundary: the runtime, the Kafka codec and gRPC are covered
by end-to-end, compatibility and live chaos tests instead.
