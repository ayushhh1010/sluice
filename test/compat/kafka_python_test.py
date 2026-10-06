"""kafka-python compatibility: produce, consume, consumer group, offsets."""
import os, sys, time, uuid, threading
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition
from kafka.admin import NewTopic

BOOT = os.environ.get("BOOTSTRAP", "localhost:9092").split(",")
topic = f"compat-kp-{uuid.uuid4().hex[:8]}"
N = 1000

admin = KafkaAdminClient(bootstrap_servers=BOOT, client_id="compat-admin")
admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])

prod = KafkaProducer(bootstrap_servers=BOOT, acks="all", linger_ms=20, retries=10)
futures = [prod.send(topic, key=str(i % 10).encode(), value=f"v{i}".encode()) for i in range(N)]
prod.flush(60)
offsets = [f.get(timeout=60) for f in futures]
assert all(m.offset >= 0 for m in offsets), "missing offsets"
print(f"produced {N} records to {topic}")

# Plain consumer, manual assignment from the beginning.
cons = KafkaConsumer(bootstrap_servers=BOOT, auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=15000)
tps = [TopicPartition(topic, p) for p in range(3)]
cons.assign(tps)
cons.seek_to_beginning()
seen = set()
for msg in cons:
    seen.add(msg.value.decode())
    if len(seen) == N:
        break
cons.close()
assert len(seen) == N, f"consumed {len(seen)}/{N}"
print("consumed all records")

# Consumer group: three members share the partitions; committed offsets persist.
group = f"g-{uuid.uuid4().hex[:6]}"
got, lock, stop = set(), threading.Lock(), threading.Event()
def member():
    c = KafkaConsumer(topic, bootstrap_servers=BOOT, group_id=group, auto_offset_reset="earliest",
                      enable_auto_commit=False, session_timeout_ms=10000, heartbeat_interval_ms=1000)
    while not stop.is_set():
        batch = c.poll(timeout_ms=500)
        for recs in batch.values():
            with lock:
                for r in recs:
                    got.add(r.value.decode())
        if batch:
            c.commit()
    c.close()
threads = [threading.Thread(target=member) for _ in range(3)]
for t in threads:
    t.start()
deadline = time.time() + 90
while time.time() < deadline and len(got) < N:
    time.sleep(0.5)
stop.set()
for t in threads:
    t.join()
assert len(got) == N, f"group consumed {len(got)}/{N}"
probe = KafkaConsumer(bootstrap_servers=BOOT, group_id=group, enable_auto_commit=False)
total = sum((probe.committed(tp) or 0) for tp in tps)
probe.close()
assert total == N, f"committed offsets sum {total} != {N}"
print("consumer group consumed and committed all records")
print("kafka-python: PASS")
