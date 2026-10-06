import { useEffect, useState } from "preact/hooks";
import { api, type Cluster } from "./api";
import { Home } from "./pages/home";
import { Produce } from "./pages/produce";
import { Inspector } from "./pages/inspector";
import { Chaos } from "./pages/chaos";
import { Metrics } from "./pages/metrics";

export type Selection = { topic: string; partition: number; offset: number } | null;

const pages = [
  { id: "", label: "Home" },
  { id: "produce", label: "Produce" },
  { id: "inspect", label: "Inspector" },
  { id: "chaos", label: "Chaos" },
  { id: "metrics", label: "Metrics" },
] as const;

function route(): string {
  return location.hash.replace(/^#\/?/, "").split("?")[0];
}

function load(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function save(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* private mode: the topic just isn't remembered */
  }
}

export function App() {
  const [page, setPage] = useState(route());
  const [cluster, setCluster] = useState<Cluster | null>(null);
  const [down, setDown] = useState(false);
  const [topic, setTopicState] = useState<string | null>(load("sluice.topic"));
  const [partitions, setPartitions] = useState<number>(Number(load("sluice.partitions") ?? 2) || 2);
  const [selection, setSelection] = useState<Selection>(null);

  useEffect(() => {
    const onHash = () => setPage(route());
    addEventListener("hashchange", onHash);
    return () => removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    let alive = true;
    const poll = async () => {
      try {
        const c = await api.cluster();
        if (alive) {
          setCluster(c);
          setDown(false);
        }
      } catch {
        if (alive) setDown(true);
      }
    };
    poll();
    const t = setInterval(poll, 2000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  const setTopic = (t: string, n = 2) => {
    setTopicState(t);
    setPartitions(n);
    save("sluice.topic", t);
    save("sluice.partitions", String(n));
  };
  const inspect = (s: Selection) => {
    setSelection(s);
    location.hash = "#/inspect";
  };

  return (
    <>
      <header class="top">
        <a class="brand" href="#/">
          <svg viewBox="0 0 32 32" width="26" height="26" aria-hidden="true">
            <rect width="32" height="32" rx="7" fill="currentColor" />
            <path d="M6 11h20M6 16h14M6 21h20" stroke="var(--bg)" stroke-width="3" stroke-linecap="round" />
          </svg>
          Sluice
        </a>
        <nav>
          {pages.map((p) => (
            <a key={p.id} href={`#/${p.id}`} class={page === p.id ? "active" : ""}>
              {p.label}
            </a>
          ))}
        </nav>
        <StatusPill cluster={cluster} down={down} />
      </header>
      {cluster?.read_only && (
        <div class="banner">
          The daily object-store budget is used up, so public writes are paused until midnight UTC. Reading still works.
        </div>
      )}
      <main>
        {page === "" && <Home cluster={cluster} />}
        {page === "produce" && <Produce topic={topic} setTopic={setTopic} onInspect={inspect} />}
        {page === "inspect" && <Inspector topic={topic} partitions={partitions} selection={selection} />}
        {page === "chaos" && <Chaos topic={topic} setTopic={setTopic} cluster={cluster} />}
        {page === "metrics" && <Metrics />}
      </main>
      <footer>
        Sluice is a learning-grade, diskless Kafka-compatible log: stateless agents, one fenced sequencer, object storage as the only
        durable layer.
      </footer>
    </>
  );
}

function StatusPill({ cluster, down }: { cluster: Cluster | null; down: boolean }) {
  if (down) return <span class="pill bad">API unreachable</span>;
  if (!cluster) return <span class="pill">connecting…</span>;
  return (
    <span class="pill ok" title={`sequencer ${cluster.sequencer}, epoch ${cluster.epoch}`}>
      {cluster.agents.length} agents · epoch {cluster.epoch}
    </span>
  );
}
