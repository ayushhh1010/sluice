# ADR-002: Metadata in object storage, not Postgres or etcd

**Status:** accepted

**Decision.** The metadata log is a sequence of put-if-absent objects (`meta/log/<seq>`) in the same bucket as the data.

**For.** No extra stateful service; fencing without ZooKeeper (storage rejects a zombie's write); disaster recovery needs only the bucket.

**Against.** Each append costs a PUT (Class A) plus one GET to confirm continuity (DESIGN §3) and a storage round trip of latency,
which is why commits are grouped at three levels (DESIGN §6). Deleting old entries needs care: put-if-absent proves a slot is empty,
not that it was never written (BUGS.md #3, #16).

**Fallback.** All storage goes through `objstore.Store`; a Postgres implementation of the log (conditional insert on `seq`) is about a
day's work if R2 misbehaves. The nightly R2 probe checks put-if-absent with 16 concurrent writers.
