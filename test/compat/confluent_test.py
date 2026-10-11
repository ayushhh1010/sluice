"""confluent-kafka-python (librdkafka) compatibility: idempotent produce, groups."""
import os, time, uuid
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOT = os.environ.get("BOOTSTRAP", "localhost:9092")
topic = f"compat-ck-{uuid.uuid4().hex[:8]}"
N = 2000

admin = AdminClient({"bootstrap.servers": BOOT})
for f in admin.create_topics([NewTopic(topic, num_partitions=4, replication_factor=1)]).values():
    f.result(30)
time.sleep(1)

delivered, errors = [], []
def cb(err, msg):
    (errors if err else delivered).append(err or msg.offset())
p = Producer({"bootstrap.servers": BOOT, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
for i in range(N):
    p.produce(topic, key=str(i % 13), value=f"v{i}", on_delivery=cb)
    p.poll(0)
p.flush(60)
assert not errors, errors[:3]
assert len(delivered) == N, f"delivered {len(delivered)}/{N}"
print(f"produced {N} records (idempotent) to {topic}")

group = f"g-{uuid.uuid4().hex[:6]}"
def consumer():
    return Consumer({"bootstrap.servers": BOOT, "group.id": group, "auto.offset.reset": "earliest",
                     "enable.auto.commit": False, "session.timeout.ms": 10000})
members = [consumer(), consumer()]
for c in members:
    c.subscribe([topic])
seen = set()
deadline = time.time() + 90
while time.time() < deadline and len(seen) < N:
    for c in members:
        msgs = [m for m in c.consume(num_messages=500, timeout=0.3) if not m.error()]
        for m in msgs:
            seen.add(m.value().decode())
        if msgs:
            try:
                c.commit(asynchronous=False)
            except KafkaException as e:  # REBALANCE_IN_PROGRESS is normal during a rebalance
                print("commit retry:", e)
assert len(seen) == N, f"group consumed {len(seen)}/{N}"
# Commit final positions; retry while the group settles after rebalances.
tps = [TopicPartition(topic, p) for p in range(4)]
committed, deadline = [], time.time() + 30
while time.time() < deadline:
    for c in members:
        # Drain in batches: a member that took a partition over after a
        # rebalance re-reads from the last successful commit.
        c.consume(num_messages=500, timeout=0.5)
        try:
            c.commit(asynchronous=False)
        except KafkaException:
            pass
    committed = members[0].committed(tps, timeout=10)
    if sum(max(tp.offset, 0) for tp in committed) == N:
        break
for c in members:
    c.close()
print("committed:", [tp.offset for tp in committed])
assert sum(max(tp.offset, 0) for tp in committed) == N, "committed offsets do not cover all records"
print("confluent-kafka (librdkafka): PASS")
