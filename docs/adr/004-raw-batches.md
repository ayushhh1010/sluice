# ADR-004: Store raw Kafka batches and rewrite offsets on read

**Status:** accepted

**Decision.** Keep client bytes untouched and patch `baseOffset` (and zero `partitionLeaderEpoch`) at fetch time.

**Why.** Kafka's batch CRC32C covers `attributes` through the end of the batch, not the base offset, so there is no re-encoding, no CRC
recomputation and no decompression on any path. It is also what lets an agent upload a batch before the sequencer has assigned its
offsets.
