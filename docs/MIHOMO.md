# Many agents through one local proxy (mihomo / clash)

`agent-client` talks to the internet through whatever local HTTP proxy
the environment already names. It never edits proxy or system settings
and never routes around the proxy.

## Setup

1. In mihomo, use the `mixed-port` (HTTP CONNECT and SOCKS5 on one port)
   or the HTTP `port`. Check your config for the actual number.
2. Export it for the agent processes, for example:
   `export HTTPS_PROXY=http://127.0.0.1:<mixed-port>` and
   `export NO_PROXY=localhost,127.0.0.1,::1`.
3. Build one `Client` per process with `ProxyMode::FromEnv` (the default)
   and share it (`Arc<Client>`) across every agent in the session.

Only `http://` proxy URLs are supported; SOCKS5-only ports and proxy URLs
with credentials are rejected rather than half-working.

## What the shared client guarantees

| Problem when many agents share the proxy | What the client does |
|---|---|
| Each agent opens its own sockets → FD exhaustion | One pool: ≤ 8 sockets per origin, ≤ 32 total, keep-alive reuse |
| Proxy restarts → every agent retries at once | Retries coalesced per origin, jittered, ≤ 10 per 60 s |
| Proxy down → requests hang | Breaker opens after 5 failures; calls fail locally in < 1 ms with no socket |
| Telemetry and polling crowd out chat/RPC | Background work runs on its own worker and pauses during outages |
| Proxy comes back | One probe per origin; on success traffic resumes, background last |

## Verify locally

`cargo run --release -p multi-agent-proxy -- 24 20 2000` runs 24 agents
for 20 s while a loopback CONNECT proxy is killed/restarted every 2 s and
prints a JSON summary; it exits non-zero if a bound is broken. The fixture
proxy only tunnels to loopback targets.

Measured on 2026-11-19 (24 agents, 20 s, 6 proxy kills): 2,391 requests
OK, 6,641 rejected locally while Open, 69 failed fast with "retry budget
exhausted" (stale keep-alive tunnels after a kill no longer trip the
breaker, so callers hit the budget instead), 65 retry attempts (ceiling
150, coalesced and immediate retries included), peak 8 sockets per origin
(cap 8), 83 sockets opened, recovery 618 ms after the proxy returned,
process FDs 8 → 8. The FD peak (296) includes the in-process fixture
proxy's own tunnel sockets.

## Real mihomo test (`harness/mihomo-e2e`)

`MIHOMO_BIN=/path/to/mihomo bash harness/mihomo/run-mihomo-e2e.sh 8 15 1`
starts a real mihomo child from a generated config (`bind-address:
127.0.0.1`, `allow-lan: false`, `rules: MATCH,DIRECT`, TUN and DNS off,
temp config dir) and a sandbox HTTPS server whose `localhost` cert is
signed by a CA generated at startup and trusted only by the test client.

What it proves, and what it is:

| Check | Who does it | Result (2026-11-19, mihomo v1.19.34, sha256 matches the release digest) |
|---|---|---|
| 8 agents, HTTPS through `mixed-port` via `connection-pool` CONNECT + rustls, cert chain and `localhost` name validated | test client using the repo's pool (not an agent app) | 1,960 OK, 0 errors in 15 s, 535 TLS handshakes |
| Independent client on mihomo's HTTP `port`, own CONNECT, one TLS session kept for the whole run | separate thread, no repo code | 63/63 requests, never disconnected |
| `curl --proxy … --cacert …` | separate process | OK |
| Untrusted CA / wrong server name | test client | both rejected |
| Socket cap | pool | peak 8 per origin (cap 8) |
| mihomo killed and restarted once under load | pool | 61 failed requests during the outage, recovered 3.3 s after restart, 0 sockets left open |

### Public API phase (what a consumer actually calls)

Same run, `agent_client::Client::get("https://localhost:…")` with
`ProxyMode::Http(mixed-port)` and `Tls::only_ca_pem(sandbox CA)`. The
client does its own TLS; nothing in the harness touches the socket.

| Check | Result (2026-11-19) |
|---|---|
| 8 agents sharing one `Client`, 15 s | 2,017 verified HTTPS responses, 0 errors |
| Keep-alive TLS reuse | 9 sockets opened, 2,009 requests on reused TLS sessions |
| Independent client on HTTP `port`, held the whole phase | 63/63, never disconnected |
| `examples/fetch.rs` as a separate process (`--proxy`, `--cacert`) | OK |
| Default roots (sandbox CA not trusted) / `https://127.0.0.1` (name not in cert) | both `ClientError::Tls` |
| Breaker after the phase | Closed |

### Minimal consumer

```rust
use agent_client::{Client, ClientConfig};
let client = Client::new(ClientConfig::default())?; // HTTPS_PROXY etc. + Mozilla roots
let resp = client.get("https://api.example.com/v1/ping")?;
```

or `cargo run -p agent-client --example fetch -- <url> [--cacert ca.pem] [--proxy http://127.0.0.1:7890]`.
With `ProxyMode::FromEnv` loopback hosts always go direct, so use
`--proxy` / `ProxyMode::Http` to send loopback tests through mihomo.

Not verified here: integration into any real agent app or IDE (Cursor or
others; nothing is wired to `agent-client` outside this repo), SOCKS5 traffic, mihomo rule sets other than `DIRECT`, and
real internet origins. Those need the owner's own machine and config.

## Intermittent raw-pool steady-phase error (quantified, not root-caused)

In 14 back-to-back runs (`run-mihomo-e2e.sh 8 15 1`, mihomo v1.19.34):

| Path | Requests | Errors |
|---|---|---|
| Raw-pool steady phase (no retries, by design) | ~27,500 | 2, in 2 runs (1 each) |
| Public `Client` / `send_https` phase | ~28,300 | 0 |

Both raw-pool errors were `UnexpectedEof` on a **fresh** tunnel
(`reused=false`), before the first response arrived. The tunnel was
closed without a TLS alert. That is consistent with mihomo closing a newly
opened CONNECT tunnel, but the logs don't prove which side closed it. The
upstream fixture has no connection limit. The public client retries a GET
on its own retry budget, so the agents never saw the error. The raw-pool
phase reports it because it has no retry path.

The harness now logs each raw-pool error on stderr (`raw-pool error
agent=… at=checkout|request reused=…`), so a recurrence can be
attributed.
