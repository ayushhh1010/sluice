import { useEffect, useState } from "preact/hooks";
import { api, type StreamRecord } from "../api";
import type { Selection } from "../app";

type Sent = { partition: number; offset: number; count: number; value: string; at: number };

export function TopicBar({ topic, setTopic }: { topic: string | null; setTopic: (t: string, partitions: number) => void }) {
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const create = async () => {
    setBusy(true);
    setErr("");
    try {
      const r = await api.createTopic(2);
      setTopic(r.topic, r.partitions);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div class="topicbar">
      {topic ? (
        <span>
          Topic <code>{topic}</code>
        </span>
      ) : (
        <span class="muted">No topic yet.</span>
      )}
      <button onClick={create} disabled={busy}>
        {busy ? "Creating…" : topic ? "New topic" : "Create a topic"}
      </button>
      {err && <span class="error">{err}</span>}
    </div>
  );
}

export function Produce({ topic, setTopic, onInspect }: { topic: string | null; setTopic: (t: string, partitions: number) => void; onInspect: (s: Selection) => void }) {
  const [key, setKey] = useState("");
  const [value, setValue] = useState("hello, object storage");
  const [sent, setSent] = useState<Sent[]>([]);
  const [records, setRecords] = useState<StreamRecord[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [streamDown, setStreamDown] = useState(false);

  useEffect(() => {
    setRecords([]);
    if (!topic) return;
    return api.stream(
      topic,
      "earliest",
      (r) => {
        setStreamDown(false);
        setRecords((rs) => [r, ...rs].slice(0, 300));
      },
      () => setStreamDown(true),
    );
  }, [topic]);

  const send = async (n: number) => {
    if (!topic) return;
    setBusy(true);
    setErr("");
    const started = performance.now();
    try {
      const recs = Array.from({ length: n }, (_, i) => ({
        key: key || undefined,
        value: n === 1 ? value : `${value} #${i + 1}`,
      }));
      const r = await api.produce(topic, recs);
      const ms = performance.now() - started;
      const out = r.results.filter((x) => !x.error).map((x) => ({ partition: x.partition, offset: x.base_offset, count: x.count, value: recs[0].value, at: ms }));
      setSent((s) => [...out, ...s].slice(0, 20));
      const failed = r.results.find((x) => x.error);
      if (failed) setErr(failed.error!);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <section class="stack">
      <h1>Produce and consume</h1>
      <TopicBar topic={topic} setTopic={setTopic} />
      {topic && (
        <div class="grid two">
          <div class="card">
            <h2>Send</h2>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                send(1);
              }}
            >
              <label>
                Key <span class="muted small">(optional: same key, same partition)</span>
                <input value={key} onInput={(e) => setKey(e.currentTarget.value)} placeholder="user-42" maxLength={200} />
              </label>
              <label>
                Value
                <textarea value={value} onInput={(e) => setValue(e.currentTarget.value)} rows={3} maxLength={4000} required />
              </label>
              <div class="row">
                <button class="primary" type="submit" disabled={busy}>
                  Send
                </button>
                <button type="button" onClick={() => send(100)} disabled={busy}>
                  Send a burst of 100
                </button>
              </div>
            </form>
            {err && <p class="error">{err}</p>}
            {sent.length > 0 && (
              <>
                <h3>Acknowledged</h3>
                <ul class="list">
                  {sent.map((s, i) => (
                    <li key={i}>
                      <button class="link" onClick={() => onInspect({ topic, partition: s.partition, offset: s.offset })}>
                        partition {s.partition}, offset {s.offset}
                        {s.count > 1 ? `–${s.offset + s.count - 1}` : ""}
                      </button>
                      <span class="muted small"> durable after {Math.round(s.at)} ms</span>
                    </li>
                  ))}
                </ul>
                <p class="muted small">The ack arrives only after the segment PUT and the metadata-log append both succeed.</p>
              </>
            )}
          </div>
          <div class="card">
            <h2>
              Live consumer <span class="muted small">{streamDown ? "(reconnecting…)" : `(${records.length} records)`}</span>
            </h2>
            {records.length === 0 ? (
              <p class="muted">Waiting for records…</p>
            ) : (
              <ul class="list records">
                {records.map((r) => (
                  <li key={`${r.partition}-${r.offset}`}>
                    <button class="link mono" onClick={() => onInspect({ topic, partition: r.partition, offset: r.offset })}>
                      p{r.partition}@{r.offset}
                    </button>{" "}
                    {r.compressed ? <span class="muted">compressed batch</span> : <span class="val">{r.key ? <b>{r.key}: </b> : null}{r.value}</span>}
                  </li>
                ))}
              </ul>
            )}
            <p class="muted small">Click a record to see the object and byte range it lives in.</p>
          </div>
        </div>
      )}
    </section>
  );
}
