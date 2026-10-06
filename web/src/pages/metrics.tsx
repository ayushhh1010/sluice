export function Metrics() {
  const url = "/grafana/d/sluice/sluice?orgId=1&kiosk&refresh=10s";
  return (
    <section class="stack">
      <h1>Metrics</h1>
      <p class="lede">
        The public, read-only Grafana dashboard: produce latency by stage, object-store request rates by billing class, cache hit rate,
        sequencer epoch and the estimated monthly cost.
      </p>
      <div class="frame">
        <iframe src={url} title="Sluice Grafana dashboard" loading="lazy" />
      </div>
      <p class="muted small">
        <a href={url} target="_blank" rel="noreferrer">
          Open the dashboard in a new tab
        </a>
      </p>
    </section>
  );
}
