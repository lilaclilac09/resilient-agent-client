# Resilient Agent Client — Remediation Spec

> **Source note.** This spec is adapted from the author's full remediation
> spec: the bug report *"Unbounded retry storms and telemetry polling
> causing FD exhaustion and cascading failures"* sent to Cursor on
> 17 November 2026, plus the author's fault-injection notes. The problem
> is framed generally here: it affects coding-agent clients under high
> concurrency, not one vendor's client. Cursor (`api2.cursor.sh`,
> `api3.cursor.sh`, `cursorvm`) is the motivating case study and the
> first set of origins; the same failure shape applies to any agent
> client that combines background telemetry and polling with unbounded
> retries under a shared process file-descriptor limit. The author's
> pasted fault-injection notes begin mid-section — their opening is
> missing, so no section before the reproduction environment has been
> reconstructed.

## 1. Problem statement

Under transient network faults, a coding-agent client produces connection
storms that exhaust local sockets and file descriptors, which cascades
into process instability. Background telemetry and health-check traffic
adds a constant baseline load. The trigger is transient, but the failure
persists after the network recovers.

## 2. Defect analysis

### 2.1 Background telemetry and polling volume

The client issues continuous background traffic to its API origins (in
the motivating case: `api2.cursor.sh` and `api3.cursor.sh`, including
`cursorvm` polling, telemetry events, and health-check and RPC calls).
The cadence is high relative to idle state and persists when the user is
not using AI features. On a constrained or proxied network, this baseline
load raises socket churn and leaves little headroom for recovery.

### 2.2 No exponential backoff with jitter on transient failures

When the local proxy or upstream link flaps, or the peer sends a TCP RST,
the client retries without exponential backoff or jitter. Retries are not
coordinated across the in-flight request set, so a single fault produces
dozens of concurrent reconnect attempts within a short window. The
synchronized pattern behaves like a thundering herd against the local
proxy.

### 2.3 Resource exhaustion and cascading failure

Each retry opens a new socket and file descriptor, and failed sockets are
not reclaimed fast enough for the rate of new attempts. On a local network
loop or loopback proxy, the storm saturates the ephemeral port range and
the process FD limit. Once the limit is reached, unrelated subsystems
(file watching, language servers, terminal and extension hosts) fail with
`EMFILE` or `ECONNREFUSED`, and the process eventually crashes — in the
observed case, the IDE core thread crashed. The trigger is transient, but
the failure persists after the network recovers, which indicates there is
no circuit breaker shedding load while the fault is active.

## 3. Reproduction (fault injection)

- **Environment:** run a local loopback HTTP(S) proxy (e.g. a local
  clash/mihomo instance). Route the agent client's traffic through it.
- **Fault injection:** use `tc`, toxiproxy, or timed kill to manufacture
  proxy flapping and TCP RSTs (e.g. kill and restart the proxy every few
  seconds).
- **Observability:** `lsof -p <pid> | wc -l` (FD leak monitoring); TCP
  connection rate and state statistics (via `lsof` or `ss`, including
  the connection rate to the proxy port).
- **Observed:** socket explosion, FD exhaustion, `EMFILE`, IDE core
  thread crash.
- **Expected:** bounded retries with backoff. **Observed:** dozens of
  concurrent connections, FD growth, and eventual crash.

## 4. Remediation design

### 4.1 Bounded connection pool

- Use a bounded connection pool per origin (in the motivating case:
  `api2.cursor.sh`, `api3.cursor.sh`, and the `cursorvm` endpoint), with
  `max_sockets` capped at 6–10 per origin and a global cap across all
  origins.
- Enable keep-alive with an explicit idle timeout (for example 15–30
  seconds) and reap idle sockets on a timer so they cannot accumulate.
- Set connect, read, and total-request timeouts on every call. A socket
  that has not completed its request within the budget must be destroyed,
  not left open.
- Destroy sockets explicitly on error paths (RST, `ECONNREFUSED`,
  timeout) instead of relying on garbage collection, and assert FD count
  against a ceiling in debug builds.
- Queue requests above the pool cap instead of opening new sockets.
  Queue depth must be bounded, and overflow should drop low-priority
  work first.

### 4.2 Decorrelated jitter and retry budget

- Remove immediate and linear retries. Use decorrelated jitter
  exponential backoff: `sleep = min(cap, rand(base, sleep * 3))`, with
  `base` 250 ms and `cap` 30 s. Reset `sleep` to `base` on a successful
  response.
- Apply a retry budget per origin: at most 10 retries per rolling
  60-second window. When the budget is exhausted, fail fast and do not
  schedule further attempts until the window rolls over.
- Retry only idempotent operations automatically. Non-idempotent RPCs
  should surface the error to the caller.
- Coalesce retries across in-flight requests to the same origin so that
  one fault produces one reconnect schedule, not one per request.

### 4.3 Per-origin circuit breaker

- Implement a state machine per origin (in the motivating case:
  `api2.cursor.sh`, `api3.cursor.sh`, `cursorvm`) with Closed, Open, and
  Half-Open states.
- Trip to Open after 5 consecutive failures of type RST, `ECONNREFUSED`,
  or timeout. Do not count HTTP 4xx responses toward the trip threshold.
- While Open, reject background requests immediately without opening any
  socket or file descriptor. Hold Open for a jittered duration between
  30 and 60 seconds.
- In Half-Open, permit a single probe request. On success, return to
  Closed and reset counters. On failure, return to Open with a doubled
  hold time, capped at 5 minutes.
- Expose the breaker state as a metric and a log event on every
  transition.

### 4.4 Queue and traffic isolation

- Move telemetry events, `cursorvm` health checks, and polling into a
  background queue that is separate from interactive RPC traffic.
  Interactive user actions must never wait behind background work.
- Pause the background queue entirely while any origin's circuit breaker
  is Open, and while the client detects an offline or
  loopback-proxy-unreachable state. Resume only on a Half-Open success.
- Batch telemetry events and flush on a fixed interval (for example
  every 60 seconds) instead of per event.
- Rate-limit health checks and `cursorvm` polling, with the interval
  configurable and a documented floor.
- Provide a telemetry opt-out that is respected at startup and at
  runtime, and a single switch that disables background polling entirely.

## 5. Acceptance

- Ship the pooling, backoff, and circuit-breaker behavior above as
  defaults, with the numeric parameters exposed as documented
  configuration.
- Surface retry, budget, and circuit state in client logs so these
  events can be diagnosed in the field.

## 6. Repository layout

```
resilient-agent-client/
├── crates/
│   ├── connection-pool/      # per-origin/global socket caps, strict timeouts, FD reclamation, HTTP CONNECT proxy
│   ├── circuit-breaker/      # Closed / Open / Half-Open state machine + per-origin Registry, probe timeout
│   ├── retry-budget/         # decorrelated jitter + rolling per-origin retry budget, coalesced
│   ├── telemetry-isolation/  # interactive vs background workers, gated queue, batched telemetry
│   └── agent-client/         # facade composing the four; one Client shared by all agents
├── harness/
│   ├── proxy-flapper/        # loopback flapping simulation (trip harness)
│   ├── fault-server/         # loopback server killed/restarted on a timer
│   ├── fd-monitor/           # external /proc + lsof FD sampling
│   ├── loopback-fixtures/    # keep-alive upstreams + flappable loopback CONNECT proxy
│   └── multi-agent-proxy/    # N agents through one flapping proxy, checks §5 bounds
├── docs/
│   ├── SPEC.md
│   ├── PROVENANCE.md         # rule → source → code → test, reference ratings
│   ├── MIHOMO.md             # multi-agent use behind mihomo / clash
│   └── RSI-REINFORCEMENT.md  # bounded measure → change → re-measure loop
└── README.md
```

### 6.1 Additions beyond §4

- **Probe timeout** (§4.3): a Half-Open probe that never reports back
  returns the breaker to Open after `probe_timeout` (default 30 s) with a
  doubled hold, instead of blocking the origin forever.
- **Proxy transport**: connections go through `HTTPS_PROXY` /
  `HTTP_PROXY` / `ALL_PROXY` via HTTP `CONNECT`, honouring `NO_PROXY` and
  loopback. Proxy 502/503 count as connection refused and 504 as timeout,
  so a dead upstream behind a live proxy still trips the breaker. Proxy
  credentials in the URL are rejected.
- **Stale keep-alive retry**: a pooled socket that fails before any
  response byte is retried once on a fresh socket without spending retry
  budget or counting toward the breaker.
- **Shared client**: one `agent_client::Client` per process, so the caps
  in §4.1 bound all agents together.

## 7. Closing note

This design can be taken directly into a Rust sandbox or network-layer
implementation; taken as a whole, it is production-grade systems
engineering.
