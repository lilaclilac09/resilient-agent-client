# Running agents behind BaoLianDeng (macOS)

[BaoLianDeng](https://github.com/madeye/BaoLianDeng) is a macOS
transparent proxy (`NETransparentProxyProvider` + the meow-rs
Clash/mihomo-compatible engine) with per-app proxy/bypass lists. Its
README does not document a local HTTP/mixed port, so this guide does not
assume one exists.

Not tested here: nothing in this repository has run against BaoLianDeng
or meow-rs. The loopback and mihomo results in README/MIHOMO.md do not
prove anything about it.

## Setup (operator settings only)

1. **Direct mode.** Leave `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY` unset for
   `agent-client` and `agent-fetch-mcp`. With no proxy variables, the
   client dials directly, and BaoLianDeng intercepts the sockets at the OS
   level. If one of the variables is set to something unsupported, such
   as `socks5://`, `Client::new` refuses to start rather than silently
   bypass it.
2. **Per-app list.** In BaoLianDeng, make sure these go through the proxy:
   Cursor (for Cursor's own traffic) and the `agent-fetch-mcp` binary (for
   tool calls). That's the owner's setting; nothing here changes it.
3. **Allowlist and TLS are unchanged.** `AGENT_FETCH_ALLOW` still lists
   the exact HTTPS origins, and certificates are always verified. If
   BaoLianDeng's rules route an origin through a TLS-inspecting node,
   calls fail with `ClientError::Tls`. They don't silently trust it.

## What the client does when the extension flaps

A restart or a node switch shows up as resets or refused connections on
direct sockets. The client:

- counts them toward the per-origin breaker, which opens after 5;
- fails calls fast while the breaker is Open (`circuit open …`), with no
  new sockets;
- probes once after 30–60 s and resumes when the probe succeeds;
- never retries more than 10 times per origin per 60 s and never opens
  more than 8 sockets per origin, so the agents can't create a reconnect
  storm.

## Reading a log (diagnosis, not evidence)

| Line on `agent-fetch-mcp` stderr / tool result | Likely cause | What to check |
|---|---|---|
| `breaker origin=… Closed->Open reason="failure threshold reached"` | 5 transport failures in a row to that origin | BaoLianDeng connected? Node reachable? The Logs tab at the same time |
| `retry-budget-exhausted origin=…` | Repeated failures within 60 s | Same as above; the client is protecting the system as designed |
| `circuit open for …; failing fast` | Breaker Open, no request sent | Wait for the half-open probe, or fix the upstream path |
| `TLS error: invalid peer certificate` | Chain or name mismatch (often a TLS-inspecting route) | The rule/node used for that origin |
| `… is not in the operator allowlist` | Origin missing from `AGENT_FETCH_ALLOW` | The operator's `mcp.json` |
| `proxy variable set but unsupported` | A SOCKS or credentialed proxy URL in the environment | Remove it (direct mode) |
| No client lines, and Cursor itself is disconnected | Cursor's own traffic, which this library doesn't cover | Cursor Settings → Network → Run Diagnostics; HTTP Compatibility Mode; BaoLianDeng's per-app list |

To diagnose a real incident, collect BaoLianDeng's Logs tab export and the
`agent-fetch-mcp` stderr from the same time window.
