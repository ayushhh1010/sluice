import { useState } from "preact/hooks";
import type { Cluster } from "../api";

export function Home({ cluster }: { cluster: Cluster | null }) {
  const host = location.hostname || "localhost";
  const local = host === "localhost" || host === "127.0.0.1";
  const bootstrap = local ? "127.0.0.1:9092" : `k1.${host}:9093`;
  const secure = !local;
  const kcat = secure
    ? `kcat -b ${bootstrap} -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=demo -X sasl.password=sluice-demo -L`
    : `kcat -b ${bootstrap} -L`;

  return (
    <section class="stack">
      <div class="hero">
        <h1>A Kafka you can kill.</h1>
        <p class="lede">
          Sluice speaks the Kafka protocol, keeps nothing on disk, and stores every record in object storage. Send a message, see the exact
          object and byte range it landed in, then kill an agent and check that nothing you were promised is gone.
        </p>
        <div class="cta">
          <a class="button primary" href="#/produce">
            Produce a message
          </a>
          <a class="button" href="#/chaos">
            Kill an agent
          </a>
        </div>
      </div>

      <div class="grid stats">
        <Stat label="Agents up" value={cluster ? String(cluster.agents.length) : "…"} />
        <Stat label="Sequencer" value={cluster ? `${cluster.sequencer} (epoch ${cluster.epoch})` : "…"} />
        <Stat label="Metadata log entries" value={cluster ? cluster.metalog_seq.toLocaleString() : "…"} />
        <Stat label="Object-store writes today" value={cluster ? cluster.class_a_today.toLocaleString() : "…"} />
      </div>

      <div class="card">
        <h2>Use your own client</h2>
        <p>Any Kafka client works unmodified. With kcat:</p>
        <Copy text={kcat} />
        {secure && (
          <p class="muted">
            Public credentials: <code>demo</code> / <code>sluice-demo</code>. Topics must start with <code>pg-</code> (at most 4 partitions,
            1 hour retention).
          </p>
        )}
      </div>

      <div class="card">
        <h2>What happens to a record</h2>
        <ol class="steps">
          <li>
            <b>Batch.</b> The agent that owns the partition buffers it with records for other partitions for up to 250 ms.
          </li>
          <li>
            <b>Upload.</b> The buffer becomes one immutable segment object, written with <code>If-None-Match: *</code>.
          </li>
          <li>
            <b>Order.</b> The sequencer assigns offsets and appends one metadata-log object. Only one writer can ever create each log slot,
            so a zombie leader is fenced by storage itself.
          </li>
          <li>
            <b>Acknowledge.</b> Only after both writes succeed does the producer get its offset. Agents keep nothing that matters: killing
            one loses nothing that was acknowledged.
          </li>
        </ol>
      </div>
    </section>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div class="card stat">
      <div class="muted small">{label}</div>
      <div class="big">{value}</div>
    </div>
  );
}

export function Copy({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div class="copy">
      <code>{text}</code>
      <button
        onClick={async () => {
          try {
            await navigator.clipboard.writeText(text);
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
          } catch {
            /* clipboard blocked: the text is selectable */
          }
        }}
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}
