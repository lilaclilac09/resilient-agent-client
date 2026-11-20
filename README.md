# resilient-agent-client

Resilient network client / proxy middleware for coding agents under high
concurrency.

## The problem

Coding-agent clients combine continuous background traffic (telemetry,
health checks, polling) with interactive RPCs in a single process. Under
transient network faults — a flapping local proxy, an injected TCP RST —
naive clients retry immediately and in lockstep. Each retry opens a new
socket and file descriptor; failed sockets are not reclaimed fast enough;
the storm saturates the ephemeral port range and the process FD limit.
Unrelated subsystems then fail with `EMFILE` / `ECONNREFUSED`, and the
process can crash. The trigger is transient; the failure persists after
the network recovers.

This is a general coding-agent concurrency problem. The motivating case
study is Cursor (`api2.cursor.sh`, `api3.cursor.sh`, `cursorvm`), where
the failure was observed first-hand: socket explosion, FD exhaustion,
`EMFILE`, and an IDE core-thread crash under proxy flapping. See
`docs/SPEC.md` for the full defect analysis and remediation design.

## The four modules

1. **connection-pool** — bounded per-origin connection pool: hard
   `max_sockets` cap of 6–10 per origin plus a global cap; strict
   connect / read / total timeouts; timed-out or errored sockets are
   destroyed, never returned to the pool; idle FDs reclaimed.
2. **retry-budget** — decorrelated jitter exponential backoff
   (`sleep = min(cap, rand(base, sleep * 3))`, base 250 ms, cap 30 s)
   with a sliding-window retry budget (at most 10 retries per origin per
   rolling 60 s); fail fast when exhausted; idempotent-only retries;
   retries coalesced across in-flight requests.
3. **circuit-breaker** — per-origin Closed / Open / Half-Open state
   machine; trips to Open after 5 consecutive underlying network errors
   (RST / `ECONNREFUSED` / timeout); while Open, background work is
   rejected locally with zero socket consumption; 30–60 s jittered
   cooldown, then Half-Open with a single probe.
4. **telemetry-isolation** — telemetry and user-interactive RPC on two
   fully isolated channels / queues; background telemetry frozen while
   any breaker is Open; a global hard switch disables background
   polling entirely.

## Repository layout

```
resilient-agent-client/
├── crates/
│   ├── connection-pool/      # per-origin socket limits, strict timeouts, FD reclamation
│   ├── circuit-breaker/      # Closed / Open / Half-Open state machine
│   ├── retry-budget/         # jittered backoff + sliding-window retry budget
│   └── telemetry-isolation/  # priority queue isolating core interaction from telemetry
├── harness/
│   ├── proxy-flapper/        # toxiproxy-based local network/proxy flapping simulation
│   └── fd-monitor/           # lsof / proc monitoring script with charts
├── docs/
│   └── SPEC.md
└── README.md
```

## Harness

- `harness/proxy-flapper` — timed kill of a loopback server manufactures
  proxy flapping (ECONNREFUSED / TCP RST); a breaker-gated client loop
  asserts the breaker trips, rejects with zero socket cost while Open,
  and closes again after recovery. Run it with
  `bash harness/run-trip-test.sh`.
- `harness/fault-server` — minimal loopback TCP server the flapper kills
  and restarts; holds accepted sockets without reading so a kill sends
  RSTs.
- `harness/fd-monitor` — `lsof` / `/proc` FD and TCP-state monitoring
  script; emits CSV rows for charting (see its README). It samples the
  flapper's FD count out of process and opens no sockets itself, so the
  measurement cannot leak FDs into the runner.

## Before / after proxy latency dashboard

From the repo root, run `python3 dashboard/serve.py` and open
<http://127.0.0.1:8765/>. The dashboard reads `target/proxy-latency.json`
on each page load; use `--results /path/to/results.json` to read an
export from another location. Until a measured export is available,
both columns show dashes and the status is **Pending measurement**.
The dashboard does not run a benchmark or use sample numbers.
For an organization-only preview routed by a private proxy, set
`HOST=0.0.0.0 PORT=8765 python3 dashboard/serve.py`.
`RESULTS_PATH` sets the export location; `--host`, `--port` and
`--results` override their corresponding environment variables.
The dashboard itself does not provide authentication or TLS; do not
expose it directly to the public internet.

The measured export is a JSON object with these fields:

| Field | Meaning |
| --- | --- |
| `scenario` | String describing the shared workload, request timing boundaries, and proxy conditions. |
| `source.environment` | String describing where the run took place (e.g. loopback fixture vs. real proxy app). |
| `source.measured_at` | String containing the measurement timestamp, preferably ISO 8601. |
| `source.command` | String containing the command used to produce the data. Use a reproducible command without secrets. |
| `before`, `after` | Objects with `label` (string identifying the baseline or candidate configuration) and the following fields. |
| `before.samples`, `after.samples` | Non-negative integer counts of successful requests used to compute the percentiles. |
| `before.failures`, `after.failures` | Non-negative integer counts of failed requests. Each configuration must have at least one success or failure. |
| `p50_ms`, `p95_ms`, `p99_ms` in each configuration | Required success-latency percentiles in milliseconds, as finite non-negative numbers in ascending order. Set all three to `null` when the configuration had no successes. |
| `errors` in each configuration (optional) | Object mapping error categories to positive integer counts that sum to `failures`. Omit if the breakdown was not recorded. |

Report the same metric and workload for both configurations; the labels
identify what actually changed rather than assuming the candidate is faster.
The failure rate is `failures / (samples + failures)`; its signed change
is in percentage points. If documented error breakdowns differ, the dashboard
compares the union of categories and leaves unreported counts blank.
Run the dashboard HTTP smoke checks with `python3 -B -m unittest discover -s dashboard`.

## Status

All four modules are implemented and composed in `crates/agent-client`
(one `Client` shared by every agent in a process, optionally through a
local HTTP proxy such as mihomo via `HTTPS_PROXY`). `cargo test
--workspace`: 66 tests, loopback fixtures only.

Measured 2026-11-19:

- `bash harness/run-trip-test.sh 30` (proxy flap every 5 s): 5 trips,
  130 requests rejected while Open, 96/122 attempts succeeded, FD
  10 → 11 max → 11 final, final state Closed.
- `cargo run --release -p multi-agent-proxy -- 24 20 2000` (24 agents,
  one shared client, loopback CONNECT proxy killed 6 times): peak 8
  sockets per origin (cap 8), 65 retry attempts (ceiling 150), 6,641 local
  rejections while Open, recovery 618 ms after the proxy returned,
  process FDs 8 → 8.

- `harness/mihomo/run-mihomo-e2e.sh` (real mihomo v1.19.34, loopback):
  8 agents made 1,960 certificate-validated HTTPS requests through
  `mixed-port` with 0 errors via the raw pool and 2,017 via the public
  `Client::get("https://…")` API (keep-alive TLS: 9 sockets); an
  independent client on the HTTP port stayed connected; `examples/fetch.rs`
  worked as a separate process; untrusted CA and wrong name rejected.

- `agent-fetch-mcp` (stdio MCP server for Cursor and other IDEs) through
  the same real mihomo: 80/80 concurrent verified HTTPS tool calls OK.

Docs: [SPEC](docs/SPEC.md) · [provenance and references](docs/PROVENANCE.md)
· [multi-agent behind mihomo](docs/MIHOMO.md)
· [RSI / reinforcement](docs/RSI-REINFORCEMENT.md)
· [Cursor / MCP integration](docs/CURSOR.md)

## Future research directions

- How to solve the multi-concurrency problem locally: on-device / local
  mitigation for coding agents under high concurrency, so the client
  degrades gracefully without depending on the server or the network path.
- The same resilience patterns (bounded pools, retry budgets, circuit
  breakers, traffic isolation) are candidates for self-custodial local
  tooling later.
