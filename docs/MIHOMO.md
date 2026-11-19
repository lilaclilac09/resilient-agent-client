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

Measured on 2026-11-19 (24 agents, 20 s, 6 proxy kills): 1,681 requests
OK, 7,123 rejected locally while Open, 0 unexplained errors, 31 retries
(ceiling 150), peak 8 sockets per origin (cap 8), 59 sockets opened in
total, recovery 509 ms after the proxy returned, process FDs 8 → 8. The
FD peak (296) includes the in-process fixture proxy's own tunnel sockets.
