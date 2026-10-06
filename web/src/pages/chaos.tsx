import { useEffect, useRef, useState } from "preact/hooks";
import { api, ApiError, type Cluster } from "../api";
import { TopicBar } from "./produce";

type Event = { at: string; text: string; kind?: "bad" | "ok" };

export function Chaos({ topic, setTopic, cluster }: { topic: string | null; setTopic: (t: string, partitions: number) => void; cluster: Cluster | null }) {
  const [running, setRunning] = useState(false);
  const [acked, setAcked] = useState(0);
  const [failed, setFailed] = useState(0);
  const [readable, setReadable] = useState(0);
  const [missing, setMissing] = useState(0);
  const [events, setEvents] = useState<Event[]>([]);
  const [killing, setKilling] = useState(false);
  const ackedSet = useRef(new Set<string>());
  const readSet = useRef(new Set<string>());
  const run = useRef(`run-${Math.random().toString(36).slice(2, 7)}`);
  const seq = useRef(0);
  const lastAgents = useRef<string>("");

  const log = (text: string, kind?: Event["kind"]) =>
    setEvents((e) => [{ at: new Date().toLocaleTimeString(), text, kind }, ...e].slice(0, 40));

  // Report membership changes as agents die and come back.
  useEffect(() => {
    if (!cluster || !running) return;
    const ids = cluster.agents.map((a) => a.id).sort().join(", ");
    if (lastAgents.current && ids !== lastAgents.current) log(`live agents: ${ids || "none"}`);
    lastAgents.current = ids;
  }, [cluster, running]);

  // Consumer: everything readable from this run.
  useEffect(() => {
    if (!topic || !running) return;
    return api.stream(topic, "earliest", (r) => {
      if (r.value?.startsWith(run.current)) {
        readSet.current.add(r.value);
        setReadable(readSet.current.size);
      }
    });
  }, [topic, running]);

  // Producer: one request at a time, about three records a second; count
  // only acknowledged sends.
  useEffect(() => {
    if (!topic || !running) return;
    let alive = true;
    let timer: ReturnType<typeof setTimeout>;
    const tick = async () => {
      const v = `${run.current}-${++seq.current}`;
      try {
        const r = await api.produce(topic, [{ value: v }]);
        if (alive && r.results.every((x) => !x.error)) {
          ackedSet.current.add(v);
          setAcked(ackedSet.current.size);
        } else if (alive) setFailed((f) => f + 1);
      } catch {
        if (alive) setFailed((f) => f + 1);
      }
      if (alive) timer = setTimeout(tick, 300);
    };
    tick();
    return () => {
      alive = false;
      clearTimeout(timer);
    };
  }, [topic, running]);

  useEffect(() => {
    const t = setInterval(() => {
      let m = 0;
      ackedSet.current.forEach((v) => {
        if (!readSet.current.has(v)) m++;
      });
      setMissing(m);
    }, 500);
    return () => clearInterval(t);
  }, []);

  const kill = async () => {
    setKilling(true);
    try {
      const r = await api.killAgent();
      if (r.killed) log(`killed ${r.killed}; its supervisor restarts it shortly`, "bad");
    } catch (e) {
      const wait = e instanceof ApiError ? Number(e.body.retry_after_ms ?? 0) : 0;
      log(wait > 0 ? `not yet: one kill every two minutes across all visitors (try again in ${Math.ceil(wait / 1000)} s)` : (e as Error).message);
    } finally {
      setKilling(false);
    }
  };

  return (
    <section class="stack">
      <h1>Chaos</h1>
      <p class="lede">
        Start a producer, then kill an agent while it runs. Some sends will fail and be retried by the client, but every acknowledged record
        must stay readable.
      </p>
      <TopicBar topic={topic} setTopic={setTopic} />
      {topic && (
        <>
          <div class="row">
            <button class={running ? "" : "primary"} onClick={() => setRunning(!running)}>
              {running ? "Stop producer" : "Start producer"}
            </button>
            <button class="danger" onClick={kill} disabled={killing || !running}>
              Kill an agent
            </button>
          </div>
          <div class="grid stats">
            <div class="card stat">
              <div class="muted small">Acknowledged</div>
              <div class="big">{acked}</div>
            </div>
            <div class="card stat">
              <div class="muted small">Readable</div>
              <div class="big">{readable}</div>
            </div>
            <div class={`card stat ${missing > 0 ? "warn" : "good"}`}>
              <div class="muted small">Acked, not yet read</div>
              <div class="big">{missing}</div>
              <div class="muted small">{missing > 0 ? "should drain to 0 within seconds" : "nothing promised is missing"}</div>
            </div>
            <div class="card stat">
              <div class="muted small">Failed sends (never acked)</div>
              <div class="big">{failed}</div>
            </div>
          </div>
          <div class="card">
            <h2>Timeline</h2>
            {events.length === 0 ? (
              <p class="muted">Nothing yet.</p>
            ) : (
              <ul class="list">
                {events.map((e, i) => (
                  <li key={i} class={e.kind}>
                    <span class="muted small mono">{e.at}</span> {e.text}
                  </li>
                ))}
              </ul>
            )}
          </div>
          <p class="muted small">
            The button never kills the sequencer and allows one kill every two minutes across all visitors. The nightly chaos run, which also
            kills the sequencer, publishes its result on the Metrics page.
          </p>
        </>
      )}
    </section>
  );
}
