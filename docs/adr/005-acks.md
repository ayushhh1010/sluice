# ADR-005: acks=1 behaves like acks=all

**Status:** accepted

**Decision.** Treat `acks=1` and `acks=-1` identically; `acks=0` returns no response but the records are still flushed and committed.

**Why.** Sluice has a single durability level: in object storage and ordered in the metadata log. There is no leader-only replica to
acknowledge early from.
