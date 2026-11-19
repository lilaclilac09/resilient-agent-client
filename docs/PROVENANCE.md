# Provenance and references

Where every rule in [SPEC.md](SPEC.md) came from, what in this repository
implements it, and which test proves it. All tests run on loopback only;
nothing here contacts a third-party host or changes system proxy settings.

## Requirement → source → code → proof

| SPEC rule | Primary source | Code | Proof (test / harness) |
|---|---|---|---|
| §4.1 per-origin socket cap (1–10, default 8) and global cap (32), queue above the cap | Google SRE Book, *Handling Overload* (bounded concurrency, queue limits) | `crates/connection-pool` `Pool::checkout` | `per_origin_cap_is_never_exceeded_under_concurrency`, `global_cap_spans_origins`; `many_agents_share_bounded_sockets_per_origin`; `multi-agent-proxy` `peak_per_origin` |
| §4.1 strict connect / I/O / total timeouts, destroy on error | Rust `std::net::TcpStream` docs (`connect_timeout`, `set_read_timeout`) | `PoolConfig`, `Conn::arm_deadline`, `Conn::discard`, `Drop for Conn` | `errored_connections_are_destroyed_not_pooled`, pool timeout tests |
| §4.1 idle reaping, FD ceiling | Linux `proc(5)` (`/proc/self/fd`) | `Pool::reap_idle`, `Pool::start_reaper`, `fd_ceiling` | `idle_sockets_are_reaped`, `idle_sockets_are_evicted_to_serve_another_origin`; `multi-agent-proxy` `fd_start == fd_end` |
| §4.2 decorrelated jitter `min(cap, rand(base, prev*3))`, base 250 ms, cap 30 s | AWS Architecture Blog, *Exponential Backoff and Jitter* | `retry-budget` `DecorrelatedJitter` | `jitter_*` tests |
| §4.2 10 retries / origin / 60 s, idempotent only, coalesced per origin | Google SRE Book, *Addressing Cascading Failures* (retry budgets); RFC 9110 §9.2.2 (idempotent methods) | `RetryCoordinator::on_failure`, `Request::is_idempotent` | `budget_allows_ten_per_window_then_refills`, `budget_exhaustion_fails_fast_per_origin`, `concurrent_failures_coalesce_into_one_schedule`, `non_idempotent_requests_are_not_retried` |
| §4.3 breaker Closed/Open/HalfOpen, trip at 5 network failures, 4xx ignored, 30–60 s jittered hold, ×2 up to 5 min, one probe | Martin Fowler, *CircuitBreaker* | `circuit-breaker` `CircuitBreaker`, `Registry` | 11 unit tests; `proxy_flap_trips_breaker_fails_fast_then_recovers`; `harness/run-trip-test.sh` |
| §4.3 probe timeout (added: a lost probe must not wedge HalfOpen) | Fowler *CircuitBreaker* (half-open trial) — gap closed here | `Config::probe_timeout`, `abandon` | `lost_probe_times_out_and_reopens`, `abandoned_probe_frees_the_slot` |
| §4.4 interactive vs background isolation, pause background while any breaker not Closed / offline / disabled, batch telemetry, opt-out, 10 s floor | Google SRE Book, *Handling Overload* (criticality, shed low priority first) | `telemetry-isolation` `Dispatcher`, `Gate`, `BackgroundQueue`, `IntervalLimiter` | 7 unit tests; `background_queue_freezes_during_outage_*` |
| Proxy support: HTTP `CONNECT` via `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY`, `NO_PROXY`, 502/503→refused, 504→timeout | RFC 9110 §9.3.6 (CONNECT); mihomo docs (`mixed-port` serves HTTP and SOCKS5) | `connection-pool/src/proxy.rs` `HttpConnectProxy` | proxy unit tests; `every_origin_is_reachable_through_the_proxy_*` |
| §3 fault injection by killing/restarting the proxy | Shopify Toxiproxy (pattern); implemented with plain loopback sockets instead | `harness/proxy-flapper`, `harness/loopback-fixtures`, `harness/multi-agent-proxy` | `run-trip-test.sh`, `cargo run -p multi-agent-proxy` |

## References: rating and how much each was used

Rating is how authoritative and directly usable the source was for this
repository (5 = normative or exact match). Usage is how much of the
implementation rests on it.

| # | Reference | Type | Rating (1–5) | Usage | What it decided |
|---|---|---|---|---|---|
| 1 | [docs/SPEC.md](SPEC.md) (this repo) | Project spec | 5 | Very high: every constant and acceptance check | Caps, timeouts, budget, breaker numbers, isolation rules |
| 2 | [AWS Architecture Blog — Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) | Engineering article | 5 | High: the whole backoff module | Decorrelated jitter formula and why jitter de-synchronises clients |
| 3 | [Martin Fowler — CircuitBreaker](https://martinfowler.com/bliki/CircuitBreaker.html) | Pattern reference | 5 | High: state machine | Closed/Open/HalfOpen, single trial request |
| 4 | [Google SRE Book — Handling Overload](https://sre.google/sre-book/handling-overload/) | Book chapter | 4 | Medium: pool and queue policy | Bounded queues, criticality, shed background first |
| 5 | [Google SRE Book — Addressing Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/) | Book chapter | 4 | Medium: retry budget | Per-origin retry budgets, no retry amplification |
| 6 | [RFC 9110 — HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110) | IETF standard | 5 | Medium: proxy and retries | CONNECT (§9.3.6), idempotent methods (§9.2.2), 502/503/504/407 meaning |
| 7 | [Rust std `TcpStream`](https://doc.rust-lang.org/std/net/struct.TcpStream.html) | API reference | 5 | High: all socket code | `connect_timeout`, read/write timeouts, `shutdown` |
| 8 | [Linux `proc(5)`](https://man7.org/linux/man-pages/man5/proc.5.html) | OS manual | 4 | Low: FD counting | `/proc/self/fd` for FD ceiling and harness checks |
| 9 | [mihomo docs — inbound ports](https://wiki.metacubex.one/en/config/inbound/port/) | Proxy docs | 3 | Low: usage guide | `mixed-port` accepts HTTP CONNECT and SOCKS5; common local ports |
| 10 | [Shopify Toxiproxy](https://github.com/Shopify/toxiproxy) | Tool | 3 | Low: design idea only | Fault injection by proxy kill/restart; not a dependency |
