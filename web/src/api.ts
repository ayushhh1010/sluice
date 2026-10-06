// Typed client for the agents' playground HTTP API (internal/httpapi).

export type Cluster = {
  sequencer: string;
  epoch: number;
  metalog_seq: number;
  agents: { id: string; node_id: number }[];
  class_a_today: number;
  read_only: boolean;
  topics: number;
};

export type ProduceResult = {
  topic: string;
  results: { partition: number; base_offset: number; count: number; error?: string }[];
};

export type Batch = {
  base_offset: number;
  last_offset: number;
  object: string;
  byte_start: number;
  byte_end: number;
  agent: string;
  commit_seq: number;
  max_timestamp: number;
};

export type PartitionView = { high_watermark: number; log_start: number; batches: Batch[] };

export type ObjectView = {
  key: string;
  agent: string;
  created_ms: number;
  batches: { topic_id: string; partition: number; base_offset: number; records: number; bytes: number }[] | null;
};

export type StreamRecord = {
  partition: number;
  offset: number;
  key?: string;
  value?: string;
  timestamp?: number;
  compressed?: boolean;
};

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public body: Record<string, unknown> = {},
  ) {
    super(message);
  }
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api/v1${path}`, {
    ...init,
    headers: init?.body ? { "Content-Type": "application/json" } : undefined,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new ApiError(res.status, body.error ?? `HTTP ${res.status}`, body);
  return body as T;
}

export const api = {
  cluster: () => call<Cluster>("/cluster"),
  createTopic: (partitions = 2) =>
    call<{ topic: string; partitions: number }>("/topics", { method: "POST", body: JSON.stringify({ partitions }) }),
  produce: (topic: string, records: { key?: string; value: string; partition?: number }[]) =>
    call<ProduceResult>(`/topics/${encodeURIComponent(topic)}/records`, { method: "POST", body: JSON.stringify({ records }) }),
  partition: (topic: string, p: number) =>
    call<PartitionView>(`/inspect/partitions/${encodeURIComponent(topic)}/${p}`),
  objects: (limit = 12) => call<{ objects: ObjectView[] }>(`/inspect/objects?limit=${limit}`),
  killAgent: () => call<{ killed?: string; error?: string; retry_after_ms?: number }>("/chaos/kill-agent", { method: "POST" }),
  // stream follows a topic over server-sent events. It reconnects by itself,
  // because EventSource gives up for good when a reconnect gets a non-200
  // answer (a proxy's 502 while an agent restarts) and never notices a
  // stream that went silent because the agent behind a proxy died. The
  // server pings every few seconds; 15 s of silence means reconnect.
  // Records already delivered are dropped on replay.
  stream: (topic: string, from: "earliest" | "latest" | number, onRecord: (r: StreamRecord) => void, onError?: () => void) => {
    const seen = new Set<string>();
    let es: EventSource | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let closed = false;
    let start = String(from);
    let last = Date.now();
    let delay = 500;
    const reconnect = () => {
      es?.close();
      onError?.();
      // Resume from the start; a "latest" stream must not skip what arrived
      // while it was disconnected.
      start = "earliest";
      clearTimeout(timer);
      timer = setTimeout(open, delay);
      delay = Math.min(delay * 2, 5000);
    };
    const open = () => {
      if (closed) return;
      last = Date.now();
      es = new EventSource(`/api/v1/topics/${encodeURIComponent(topic)}/stream?from=${start}`);
      es.onopen = () => {
        last = Date.now();
        delay = 500;
      };
      es.addEventListener("ping", () => {
        last = Date.now();
      });
      es.onmessage = (e) => {
        last = Date.now();
        const r: StreamRecord = JSON.parse(e.data);
        const k = `${r.partition}:${r.offset}`;
        if (seen.has(k)) return;
        seen.add(k);
        onRecord(r);
      };
      es.onerror = () => {
        if (es?.readyState === EventSource.CLOSED) reconnect();
      };
    };
    const watchdog = setInterval(() => {
      if (!closed && Date.now() - last > 15_000) reconnect();
    }, 3000);
    open();
    return () => {
      closed = true;
      clearTimeout(timer);
      clearInterval(watchdog);
      es?.close();
    };
  },
};

export function shortKey(key: string): string {
  const base = key.slice(key.lastIndexOf("/") + 1);
  return base.length > 34 ? `${base.slice(0, 14)}…${base.slice(-18)}` : base;
}
