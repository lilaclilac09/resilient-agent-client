# Using the client with Cursor (and other MCP-capable IDEs)

Cursor is the motivating example for this repository, not a confirmed
installation. This page lists only supported, documented integration
points. Nothing here intercepts, patches, or reroutes Cursor's traffic,
and nothing changes system or network settings.

## Can Cursor use `agent-client` for its own traffic?

No. Cursor is a closed-source desktop app. Its chat, agent, Tab and
codebase-search traffic (`api2`/`api5`/`api3`/`repo42.cursor.sh`, per
[Cursor's network configuration docs][net]) goes through Cursor's own
HTTP/2 client. No documented setting or extension point lets a library
replace it. The only way to put this library in that path would be to
intercept, forward, or patch, and those are out of scope on purpose.

## What is supported

| Integration point (documented) | What it does | What it protects | What it does not protect |
|---|---|---|---|
| **`agent-fetch-mcp` as a stdio MCP server** ([Cursor MCP docs][mcp], [MCP 2025-06-18][spec]) | Gives agents a read-only `resilient_fetch` tool (GET/HEAD). One process per Cursor window is shared by every agent in it. | Every request made **through this tool**: ≤ 8 sockets per origin / 32 total, retry budget of 10 per origin per 60 s, a per-origin breaker that fails fast while the proxy is down, verified TLS, and the local proxy from `HTTPS_PROXY`. | Cursor's own model/agent/Tab traffic, Cursor's built-in web search/fetch tools, terminal commands (`curl`, package managers), extensions, and other MCP servers. |
| **Cursor Settings → Network → HTTP Compatibility Mode = HTTP/1.1** ([troubleshooting][ts]) | Makes Cursor use HTTP/1.1 (SSE) instead of HTTP/2 streaming. | Cursor's own streams, when a proxy or proxy rule breaks HTTP/2 streaming. | Retry storms, socket counts and breaker behaviour inside Cursor. None of this library's logic applies. |
| **Cursor Settings → Network → Run Diagnostics** ([troubleshooting][ts]) | Tests Cursor's connection to its backend. | Diagnosis only. | Nothing. It changes no behaviour. |
| **The `curl` streaming checks in [Cursor's network docs][net]** | Shows whether a proxy buffers SSE or HTTP/2 bidi streams. | Diagnosis only. | Nothing. Run them only on your own network if you choose to. |

## Permission boundary of `resilient_fetch`

The tool is an agent-facing network permission, so it stays narrow:

- **Default deny.** It reaches only the exact origins the operator lists
  in `AGENT_FETCH_ALLOW` (`https://host[:port]`, comma-separated, port
  defaults to 443). Unset or empty means every call is refused. The
  server refuses to start on wildcard (`*`), `http://`, credential, path
  or port-0 entries.
- **HTTPS only, GET/HEAD only.** The certificate chain and host name are
  always verified. URLs with embedded credentials are rejected.
- **No redirects followed.** A 3xx comes back as the result; the agent
  has to make a new call, which the allowlist checks again.
- **Tool input can't widen it.** Proxy (`HTTPS_PROXY`,
  `AGENT_FETCH_PROXY`), trust (`AGENT_FETCH_CA_FILE`) and the allowlist
  come only from the server's environment as set by the operator in
  `mcp.json`. Any argument besides `url`, `method` and `max_chars` is
  refused.
- **Host restrictions stay in force.** It's an ordinary process: it goes
  through the configured proxy or the host's normal routing, and
  firewall or mihomo rules still apply. It never bypasses or changes
  them.

## Setup: `agent-fetch-mcp` in Cursor

1. Build it with `cargo build --release -p agent-fetch-mcp`.
2. Add it to `~/.cursor/mcp.json` (global) or `.cursor/mcp.json` (project):

   ```json
   {
     "mcpServers": {
       "resilient-fetch": {
         "type": "stdio",
         "command": "/absolute/path/to/target/release/agent-fetch-mcp",
         "env": {
           "AGENT_FETCH_ALLOW": "https://api.example.com,https://docs.example.com",
           "HTTPS_PROXY": "http://127.0.0.1:<mihomo mixed-port>",
           "NO_PROXY": "localhost,127.0.0.1,::1"
         }
       }
     }
   }
   ```

   Use the mixed-port or HTTP `port` from your own mihomo config. Leave
   `HTTPS_PROXY` out to connect directly. Add `AGENT_FETCH_CA_FILE` for a
   private CA.
3. Ask the agent to use `resilient_fetch` for web requests. Cursor's own
   fetch/search tools still work and stay unprotected.

Each tool result starts with `HTTP <status> (<n> attempt(s))`. While the
proxy is down, calls come back at once as tool errors (`circuit open …;
failing fast`). They don't hang, and they don't open new sockets.

## Verified (loopback only)

- `cargo test -p agent-fetch-mcp` drives the real binary over stdio:
  - The initialize/tools list/tools call lifecycle works.
  - 120 concurrent calls through a loopback CONNECT proxy all succeeded on ≤ 8 tunnels. The server runs at most 40 at once (pool sockets plus queue) and holds the rest for up to 30 s instead of failing them.
  - Default deny works.
  - http://, other ports, credentials, extra `proxy`/`ca_file` arguments and POST are all refused without a connection.
  - Invalid allowlist entries stop the server.
  - Malformed JSON-RPC gets `-32600`.
  - With the proxy killed, calls fail fast with `circuit open`.
- `harness/mihomo/run-mihomo-e2e.sh` starts the server with `AGENT_FETCH_PROXY` pointing at a real mihomo mixed-port and `AGENT_FETCH_CA_FILE` set to the sandbox CA:
  - 80 concurrent verified HTTPS tool calls through real mihomo v1.19.34: 80/80 OK.
  - A wrong host name is refused with a TLS error.

Nothing has been installed on the owner's machine. Not verified: that a specific Cursor build loads it. That needs Cursor on
the owner's machine. The config above follows Cursor's documented
`mcp.json` stdio format.

[net]: https://cursor.com/docs/enterprise/network-configuration
[ts]: https://cursor.com/docs/troubleshooting/common-issues
[mcp]: https://cursor.com/docs/context/mcp
[spec]: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
