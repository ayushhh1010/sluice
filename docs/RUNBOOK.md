# Runbook

Production is one VM running `deploy/docker-compose.yml` (two sequencers, two agents, Caddy, Prometheus, Grafana) plus an R2 bucket.
Every Sluice process is stateless: when in doubt, restarting a process is safe, and a fresh VM pointed at the same bucket is a full
recovery.

## Dashboards and endpoints

| What | Where |
| --- | --- |
| Public dashboard | `https://<domain>/grafana/` (anonymous, read-only) |
| Agent health, metrics, pprof | `127.0.0.1:18081` (agent-1), `127.0.0.1:18083` (agent-2) on the VM: `/healthz`, `/metrics`, `/debug/pprof/` |
| Sequencer health, metrics | `sequencer-a:7001`, `sequencer-b:7001` on the compose network |
| Logs | `docker compose logs -f <service>` (JSON; every line has `proc` and, for connections, `conn`) |
| Offline state | `sluicectl replay -store $SLUICE_STORE -cluster prod` |

## Symptoms

| Symptom | First check | Action |
| --- | --- | --- |
| Produce latency high | "Produce latency by stage": which stage? "Object store latency p99"; R2 status page | `put` or `commit` stage high: storage is slow; raise `--flush-window` to make fewer, larger PUTs and wait it out. `buffer` stage high with small segments: clients are lingering too little (recommend `linger.ms` 50–100). |
| Producers get `REQUEST_TIMED_OUT` | Object store error rate; sequencer role panel | Usually a storage brownout or a failover in progress. Records may still land (at-least-once). If it persists, check that one sequencer is `leader`. |
| Cost guard tripped (playground banner, `POLICY_VIOLATION` on public produce) | "Class A vs Class B" panel; `sluice_objstore_requests_total{class="A"}` by process | Listener stays read-only until 00:00 UTC. Find the heavy user in agent logs; remove abusive credentials from `SLUICE_SASL_USERS`. Restarting the active sequencer resets the counter (deliberately not persisted), so do not restart it to "clear" the guard. |
| Sequencer flapping (epoch climbing) | `sluice_sequencer_epoch`, role panel, logs of both sequencers | Usually the sequencers cannot reach each other (pings) while both reach storage. Fix the network path, or raise `--leader-timeout`. Safety is unaffected: losers step down on their next append. |
| A sequencer exits with "metadata log inconsistent with state" | The log line names the entry | Fail-stop by design: an entry contradicts the state it applies to. Do not delete objects. Run `sluicectl replay` and `sluicectl log -from <seq-2>` against the bucket, keep the bucket intact, and open an issue with the output. |
| Agent restarts in a loop | `docker compose logs agent-N` | "waiting for TLS certificate": Caddy has not obtained `kN.<domain>` yet; check DNS (DNS-only, not proxied) and Caddy logs. "open store": check `.env` credentials and the bucket name. |
| Consumers stuck at an offset | `sluicectl inspect -topic T -partition P -offset O` | Prints the object and byte range. If the object is missing, run `sluice-check` (I7). |
| VM disk full | `docker system df`; Prometheus volume size | Lower `--storage.tsdb.retention.size` in `deploy/docker-compose.yml`; `docker image prune`. Sluice itself writes nothing to disk. |
| Total VM loss | Is the bucket intact? | New VM, clone the repo, copy `.env`, point DNS at it, `docker compose up -d`. The sequencer recovers from the newest snapshot plus the log. |

## Procedures

### First deployment

1. **Bucket.** Create an R2 bucket (for example `sluice-prod`) and an API token scoped to that bucket with object read and write.
2. **VM.** Any small Linux VM with Docker (an Oracle Cloud Always Free ARM instance works). Open TCP 80, 443, 9093 and 9094 only.
3. **DNS.** Point `<domain>`, `k1.<domain>` and `k2.<domain>` at the VM. On Cloudflare, set the `k1`/`k2` records to **DNS only**: the
   proxy does not carry raw Kafka TCP.
4. **Configure.**
   ```bash
   git clone <repo> ~/sluice && cd ~/sluice/deploy && cp .env.example .env && chmod 600 .env
   ```
   Fill in `DOMAIN`, `SLUICE_STORE`, the R2 credentials, `SLUICE_SASL_USERS` (keep the admin password private) and
   `GRAFANA_ADMIN_PASSWORD`.
5. **Build the playground** (no Node on the VM needed):
   ```bash
   docker run --rm -v "$HOME/sluice/web:/w" -w /w node:22-alpine sh -c "npm ci && npm run build"
   ```
6. **Start.**
   ```bash
   docker compose up -d
   ```
   Agents wait for Caddy to obtain the `k1`/`k2` certificates, then start. Check `docker compose ps` and
   `curl -sf 127.0.0.1:18081/healthz`.
7. **Smoke test** from your laptop:
   ```bash
   kcat -b k1.<domain>:9093 -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=demo -X sasl.password=<demo password> -L
   ```

### Deploy a new version

CI builds multi-arch images on every merge to `main` and, when `DEPLOY_ENABLED` is set, restarts the standby sequencer, the active
sequencer, then each agent after the previous one reports healthy. By hand:

```bash
cd ~/sluice/deploy && git -C .. pull --ff-only && docker compose pull
docker compose up -d --no-deps sequencer-b && sleep 5
docker compose up -d --no-deps sequencer-a && sleep 15
docker compose up -d --no-deps agent-1 && curl -sf 127.0.0.1:18081/healthz
docker compose up -d --no-deps agent-2 && curl -sf 127.0.0.1:18083/healthz
```

Clients see a few seconds of retriable errors per agent and no acknowledged loss.

### Rotate credentials

* SASL users: edit `SLUICE_SASL_USERS` in `.env`, then restart agents one at a time.
* R2 token: create a new bucket-scoped token, update `.env`, restart sequencers then agents, then revoke the old token.

### Verify durability after an incident

```bash
sluice-check -store "$SLUICE_STORE" -cluster prod          # I3, I6, I7 straight from the bucket
sluice-chaos -b k1.<domain>:9093 -tls -user admin -pass ... -duration 10m   # optional live re-test
```

### Clocks

Two GC margins assume clocks agree within minutes (DESIGN §7). All processes share the VM clock; keep `systemd-timesyncd` or `chrony`
enabled. If agents ever move to other machines, check NTP there too.
