import { useEffect, useState } from "preact/hooks";
import { api, shortKey, type ObjectView, type PartitionView } from "../api";
import type { Selection } from "../app";

export function Inspector({ topic, partitions, selection }: { topic: string | null; partitions: number; selection: Selection }) {
  const t = selection?.topic ?? topic;
  const [partition, setPartition] = useState(selection?.partition ?? 0);
  const [view, setView] = useState<PartitionView | null>(null);
  const [objects, setObjects] = useState<ObjectView[]>([]);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (selection) setPartition(selection.partition);
  }, [selection]);

  useEffect(() => {
    if (!t) return;
    let alive = true;
    const load = async () => {
      try {
        const [v, o] = await Promise.all([api.partition(t, partition), api.objects(10)]);
        if (alive) {
          setView(v);
          setObjects(o.objects);
          setErr("");
        }
      } catch (e) {
        if (alive) setErr((e as Error).message);
      }
    };
    load();
    const timer = setInterval(load, 3000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [t, partition]);

  if (!t) {
    return (
      <section class="stack">
        <h1>Inspector</h1>
        <p class="muted">
          Create a topic on the <a href="#/produce">Produce</a> page first.
        </p>
      </section>
    );
  }
  const target = selection && selection.topic === t && selection.partition === partition ? selection.offset : null;
  const hit = view?.batches.find((b) => target !== null && target >= b.base_offset && target <= b.last_offset);

  return (
    <section class="stack">
      <h1>Inspector</h1>
      <div class="topicbar">
        <span>
          Topic <code>{t}</code>, partition{" "}
          <select value={partition} onChange={(e) => setPartition(Number(e.currentTarget.value))}>
            {Array.from({ length: Math.max(partitions, partition + 1) }, (_, p) => p).map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </span>
        {view && (
          <span class="muted">
            log start {view.log_start}, high watermark {view.high_watermark}
          </span>
        )}
      </div>
      {err && <p class="error">{err}</p>}

      {hit && target !== null && (
        <div class="card highlight">
          <h2>
            Offset {target} lives in {hit.agent === "merger" ? "a merged object" : `a segment written by ${hit.agent}`}
          </h2>
          <dl class="facts">
            <dt>Object</dt>
            <dd class="mono break">{hit.object}</dd>
            <dt>Byte range</dt>
            <dd class="mono">
              {hit.byte_start}–{hit.byte_end} ({hit.byte_end - hit.byte_start} bytes, stored exactly as the client sent them)
            </dd>
            <dt>Batch</dt>
            <dd>
              offsets {hit.base_offset}–{hit.last_offset}
            </dd>
            <dt>Committed by</dt>
            <dd>
              metadata log entry <span class="mono">#{hit.commit_seq}</span>
            </dd>
          </dl>
          <p class="muted small">
            The batch was uploaded before it had offsets; the sequencer assigned them at commit time, and every fetch patches the base offset
            into the header. Kafka's batch CRC does not cover that field, so nothing is re-encoded.
          </p>
        </div>
      )}

      <div class="card">
        <h2>Batches in this partition</h2>
        {!view || view.batches.length === 0 ? (
          <p class="muted">No batches yet.</p>
        ) : (
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Offsets</th>
                  <th>Object</th>
                  <th>Bytes</th>
                  <th>Writer</th>
                  <th>Log entry</th>
                </tr>
              </thead>
              <tbody>
                {view.batches.slice(-50).reverse().map((b) => (
                  <tr key={b.base_offset} class={b === hit ? "selected" : ""}>
                    <td class="mono">
                      {b.base_offset}
                      {b.last_offset > b.base_offset ? `–${b.last_offset}` : ""}
                    </td>
                    <td class="mono" title={b.object}>
                      {shortKey(b.object)}
                    </td>
                    <td class="mono">
                      {b.byte_start}–{b.byte_end}
                    </td>
                    <td>{b.agent}</td>
                    <td class="mono">#{b.commit_seq}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div class="card">
        <h2>Recent segment objects</h2>
        <p class="muted small">Each upload carries batches for many partitions, which is what keeps object-store writes per GB low.</p>
        <ul class="list">
          {objects.map((o) => (
            <li key={o.key}>
              <span class="mono" title={o.key}>
                {shortKey(o.key)}
              </span>{" "}
              <span class="muted small">
                by {o.agent}: {(o.batches ?? []).length} batches,{" "}
                {(o.batches ?? []).reduce((n, b) => n + b.records, 0)} records,{" "}
                {(o.batches ?? []).reduce((n, b) => n + b.bytes, 0).toLocaleString()} bytes
              </span>
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}
