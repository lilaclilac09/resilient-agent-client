# Running agents behind BaoLianDeng (macOS)

[BaoLianDeng](https://github.com/madeye/BaoLianDeng) is a macOS proxy app
built on the meow-rs Clash/mihomo-compatible engine. Version 6.x has a
**Local Proxy** mode: a mixed HTTP/SOCKS5 listener on loopback, shown as
the active **Proxy Address** in Settings, that needs no system proxy.
Port changes take effect after a restart
([SettingsView.swift](https://github.com/madeye/BaoLianDeng/blob/10cffd10b15d485f2377607d055b01700b04c58b/BaoLianDeng/Views/SettingsView.swift#L112-L147)).
That is source evidence, not a tested run.

Not tested here: nothing in this repository has run against BaoLianDeng
or meow-rs. The loopback and mihomo results don't prove anything about it.

## Setup: Local Proxy mode (the primary route)

1. In BaoLianDeng 6.x, enable Local Proxy and note the **Proxy Address**
   port. If you change the port, restart the app.
2. Point the agents at it, for example in the `agent-fetch-mcp` entry of
   `mcp.json`:
   `"HTTPS_PROXY": "http://127.0.0.1:<Proxy Address port>"`. The mixed
   listener accepts HTTP CONNECT, which is what the client speaks.
   Don't use a `socks5://` URL: `Client::new` refuses it rather than
   silently bypassing the proxy.
3. If the proxy is down, don't switch to direct mode as a workaround.
   Calls are meant to fail fast until the Local Proxy is back.
4. The allowlist and TLS settings stay as they are. `AGENT_FETCH_ALLOW`
   still lists the exact HTTPS origins, and certificates are always
   verified. A route through a TLS-inspecting node fails with
   `ClientError::Tls` rather than being trusted.

## What the client does when the extension flaps

A Local Proxy restart shows up as refused CONNECTs; a node failure shows up
as proxy 502/503/504. The client:

- counts them toward the per-origin breaker, which opens after 5;
- fails calls fast while the breaker is Open (`circuit open …`), with no
  new sockets;
- probes once after 30–60 s and resumes when the probe succeeds;
- never retries more than 10 times per origin per 60 s and never opens
  more than 8 sockets per origin, so the agents can't create a reconnect
  storm.

## Reading a log (diagnosis, not evidence)

| Line on `agent-fetch-mcp` stderr / tool result | Consistent with (not a diagnosis) | What to check |
|---|---|---|
| `breaker origin=… Closed->Open reason="failure threshold reached"` | 5 transport failures in a row to that origin | BaoLianDeng connected? Node reachable? The Logs tab at the same time |
| `retry-budget-exhausted origin=…` | Repeated failures within 60 s | Same as above; the client is protecting the system as designed |
| `circuit open for …; failing fast` | Breaker Open, no request sent | Wait for the half-open probe, or fix the upstream path |
| `TLS error: invalid peer certificate` | Chain or name mismatch (often a TLS-inspecting route) | The rule/node used for that origin |
| `… is not in the operator allowlist` | Origin missing from `AGENT_FETCH_ALLOW` | The operator's `mcp.json` |
| `proxy variable set but unsupported` | A SOCKS or credentialed proxy URL in the environment | Use the `http://127.0.0.1:<port>` Proxy Address |
| No client lines, and Cursor itself is disconnected | Cursor's own traffic, which this library doesn't cover | Cursor Settings → Network → Run Diagnostics; HTTP Compatibility Mode; BaoLianDeng's per-app list |

To diagnose a real incident, collect BaoLianDeng's Logs tab export and the
`agent-fetch-mcp` stderr from the same time window.
