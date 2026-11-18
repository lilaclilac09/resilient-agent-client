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

- `harness/proxy-flapper` — toxiproxy-based local network / proxy
  flapping simulation (stub).
- `harness/fd-monitor` — `lsof` / `/proc` FD and TCP-state monitoring
  script; emits CSV rows for charting (see its README).

## Status

Scaffold. The four crates and the harness are stubs (doc comments and
TODO markers only) awaiting implementation. No network behavior is
implemented yet.

## Future research directions

- How to solve the multi-concurrency problem locally: on-device / local
  mitigation for coding agents under high concurrency, so the client
  degrades gracefully without depending on the server or the network path.
- The same resilience patterns (bounded pools, retry budgets, circuit
  breakers, traffic isolation) are candidates for self-custodial local
  tooling later.
