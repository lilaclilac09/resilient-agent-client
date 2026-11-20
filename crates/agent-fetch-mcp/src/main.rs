//! `agent-fetch-mcp`: a stdio MCP server (spec 2025-06-18) with one
//! read-only tool, `resilient_fetch`, that performs GET/HEAD requests
//! through one shared `agent_client::Client`.
//!
//! Cursor (or any MCP client) starts one process per window and every
//! agent in that window shares it, so the pool caps, retry budget and
//! per-origin breakers bound all of their tool requests together.
//! It only covers requests made through this tool; the IDE's own traffic
//! is untouched (see docs/CURSOR.md).
//!
//! Environment:
//! - `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` / `NO_PROXY`: proxy, e.g.
//!   mihomo's `http://127.0.0.1:<mixed-port>` (loopback stays direct).
//! - `AGENT_FETCH_PROXY=http://host:port`: use this proxy for every host,
//!   loopback included (used by the local tests).
//! - `AGENT_FETCH_CA_FILE=/path/ca.pem`: extra trusted CA (PEM).
//! - `AGENT_FETCH_ALLOW=https://a.example,https://b.example:8443`: the
//!   only origins the tool may reach (exact match, HTTPS only). Unset or
//!   empty denies every call. Redirects are returned, never followed.

use agent_client::{
    parse_http_proxy_url, Client, ClientConfig, ClientError, HttpConnectProxy, ProxyMode, Request,
    Tls,
};
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
/// Calls accepted at once; beyond this they are refused immediately.
const MAX_PENDING: usize = 256;
/// Calls running at once. Matches the pool's per-origin sockets plus its
/// queue (8 + 32), so a burst waits here instead of hitting `QueueFull`.
const MAX_ACTIVE: usize = 40;
const SLOT_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Counting semaphore with a bounded wait.
struct Slots {
    used: Mutex<usize>,
    cv: std::sync::Condvar,
}

impl Slots {
    fn acquire(&self, max: usize, wait: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + wait;
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        while *used >= max {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return false;
            }
            used = self
                .cv
                .wait_timeout(used, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        *used += 1;
        true
    }
    fn release(&self) {
        *self.used.lock().unwrap_or_else(|e| e.into_inner()) -= 1;
        self.cv.notify_one();
    }
}
const DEFAULT_MAX_CHARS: usize = 20_000;

/// Operator-set exact HTTPS origins the tool may reach
/// (`AGENT_FETCH_ALLOW`, comma-separated `https://host[:port]`). Empty
/// means deny everything. No wildcards, paths, credentials or `http://`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Allowlist(Vec<(String, u16)>);

impl Allowlist {
    pub fn parse(value: &str) -> Result<Self, String> {
        let mut out = Vec::new();
        for entry in value.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let bad = |why: &str| Err(format!("AGENT_FETCH_ALLOW entry {entry:?}: {why}"));
            let Some(rest) = entry.strip_prefix("https://") else {
                return bad("must start with https://");
            };
            let rest = rest.strip_suffix('/').unwrap_or(rest);
            if rest.contains(['*', '@', '/', '?', '#', ' ']) || rest.is_empty() {
                return bad("exact origin only: no wildcard, credentials, path, query or fragment");
            }
            let (host, port) = match rest.rsplit_once(':') {
                Some((h, p)) => match p.parse::<u16>() {
                    Ok(p) if !h.is_empty() && p != 0 => (h, p),
                    _ => return bad("invalid port"),
                },
                None => (rest, 443),
            };
            out.push((host.to_ascii_lowercase(), port));
        }
        Ok(Self(out))
    }

    pub fn allows(&self, host: &str, port: u16) -> bool {
        let host = host.to_ascii_lowercase();
        self.0.iter().any(|(h, p)| *h == host && *p == port)
    }
}

const ALLOWED_ARGS: [&str; 3] = ["url", "method", "max_chars"];

fn build_client() -> Result<Client, String> {
    let mut cfg = ClientConfig {
        logger: Some(Arc::new(|line: &str| eprintln!("agent-fetch-mcp: {line}"))),
        ..ClientConfig::default()
    };
    if let Ok(p) = std::env::var("AGENT_FETCH_PROXY") {
        let origin =
            parse_http_proxy_url(&p).ok_or("AGENT_FETCH_PROXY must be http://host:port")?;
        cfg.proxy = ProxyMode::Http(HttpConnectProxy::new(origin));
    }
    if let Ok(path) = std::env::var("AGENT_FETCH_CA_FILE") {
        let pem = std::fs::read(&path).map_err(|e| format!("{path}: {e}"))?;
        cfg.tls = Tls::with_extra_ca_pem(&pem)?;
    }
    Client::new(cfg)
}

fn tool_definition() -> Value {
    json!({
        "name": "resilient_fetch",
        "title": "Resilient fetch",
        "description": "Fetch an http(s) URL with GET or HEAD through the shared resilient client: bounded sockets per origin, retry budget, circuit breaker, verified TLS, local proxy from HTTPS_PROXY. Returns the status line and the (truncated) body.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "http:// or https:// URL" },
                "method": { "type": "string", "enum": ["GET", "HEAD"], "default": "GET" },
                "max_chars": { "type": "integer", "minimum": 1, "maximum": 1000000, "default": DEFAULT_MAX_CHARS }
            },
            "additionalProperties": false,
            "required": ["url"]
        },
        "annotations": { "readOnlyHint": true, "openWorldHint": true }
    })
}

/// Result of `tools/call` for `resilient_fetch` (MCP tool result object).
pub fn call_fetch(client: &Client, allow: &Allowlist, args: &Value) -> Value {
    let text_result = |text: String, is_error: bool| json!({ "content": [{ "type": "text", "text": text }], "isError": is_error });
    let Some(url) = args.get("url").and_then(Value::as_str) else {
        return text_result("missing string argument `url`".into(), true);
    };
    // Proxy, trust roots and the allowlist come only from the operator's
    // environment; tool input can't carry anything else.
    if let Some(extra) = args
        .as_object()
        .and_then(|o| o.keys().find(|k| !ALLOWED_ARGS.contains(&k.as_str())))
    {
        return text_result(format!("unknown argument `{extra}`"), true);
    }
    match agent_client::parse_url(url) {
        Ok((agent_client::Scheme::Https, origin, _)) if allow.allows(&origin.host, origin.port) => {}
        Ok((agent_client::Scheme::Https, origin, _)) => {
            return text_result(
                format!("{origin} is not in the operator allowlist (AGENT_FETCH_ALLOW); refused without a request"),
                true,
            )
        }
        Ok(_) => return text_result("only https:// URLs are allowed".into(), true),
        Err(e) => return text_result(e.to_string(), true),
    }
    let method = args.get("method").and_then(Value::as_str).unwrap_or("GET");
    if method != "GET" && method != "HEAD" {
        return text_result(
            format!("method {method:?} not allowed: GET or HEAD only"),
            true,
        );
    }
    let max_chars = args
        .get("max_chars")
        .and_then(Value::as_u64)
        .map(|n| n.clamp(1, 1_000_000) as usize)
        .unwrap_or(DEFAULT_MAX_CHARS);
    let mut req = Request::get("/");
    req.method = method.to_string();
    match client.send_url(url, req) {
        Ok(resp) => {
            let body = String::from_utf8_lossy(&resp.body);
            let mut text: String = body.chars().take(max_chars).collect();
            if body.chars().count() > max_chars {
                text.push_str("\n[truncated]");
            }
            text_result(
                format!(
                    "HTTP {} ({} attempt(s))\n\n{text}",
                    resp.status, resp.attempts
                ),
                !(200..400).contains(&resp.status),
            )
        }
        Err(e @ ClientError::CircuitOpen { .. }) => text_result(
            format!("{e}; failing fast without a request, retry later"),
            true,
        ),
        Err(e) => text_result(e.to_string(), true),
    }
}

fn error(id: &Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn ok(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Handle one message. `None` for notifications / responses.
fn handle_sync(msg: &Value) -> Option<Value> {
    let Some(obj) = msg.as_object() else {
        return Some(error(&Value::Null, -32600, "invalid request"));
    };
    if obj.contains_key("result") || obj.contains_key("error") {
        return None; // a response from the client
    }
    let method = match obj.get("method") {
        Some(Value::String(m)) => Some(m.as_str()),
        Some(_) => {
            return Some(error(
                obj.get("id").unwrap_or(&Value::Null),
                -32600,
                "invalid request",
            ))
        }
        None => None,
    };
    let Some(id) = obj.get("id") else {
        return method
            .is_none()
            .then(|| error(&Value::Null, -32600, "invalid request"));
    };
    Some(match method {
        Some("initialize") => {
            let asked = msg
                .pointer("/params/protocolVersion")
                .and_then(Value::as_str);
            let version = asked
                .filter(|v| PROTOCOL_VERSIONS.contains(v))
                .unwrap_or(PROTOCOL_VERSIONS[0]);
            ok(
                id,
                json!({
                    "protocolVersion": version,
                    "capabilities": { "tools": { "listChanged": false } },
                    "serverInfo": { "name": "agent-fetch-mcp", "version": env!("CARGO_PKG_VERSION") },
                    "instructions": "Use resilient_fetch for web requests so every agent shares bounded, retry-budgeted connections through the local proxy."
                }),
            )
        }
        Some("ping") => ok(id, json!({})),
        Some("tools/list") => ok(id, json!({ "tools": [tool_definition()] })),
        Some(_) => error(id, -32601, "method not found"),
        None => error(id, -32600, "invalid request"),
    })
}

fn main() {
    let allow = match Allowlist::parse(&std::env::var("AGENT_FETCH_ALLOW").unwrap_or_default()) {
        Ok(a) => Arc::new(a),
        Err(e) => {
            eprintln!("agent-fetch-mcp: {e}");
            std::process::exit(2);
        }
    };
    if allow.0.is_empty() {
        eprintln!("agent-fetch-mcp: AGENT_FETCH_ALLOW is empty; every fetch is denied");
    }
    let client = match build_client() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("agent-fetch-mcp: {e}");
            std::process::exit(2);
        }
    };
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let send = |out: &Mutex<std::io::Stdout>, v: &Value| {
        let mut o = out.lock().unwrap_or_else(|e| e.into_inner());
        let _ = writeln!(o, "{v}");
        let _ = o.flush();
    };
    let in_flight = Arc::new(AtomicUsize::new(0));
    let slots = Arc::new(Slots {
        used: Mutex::new(0),
        cv: std::sync::Condvar::new(),
    });
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                send(&out, &error(&Value::Null, -32700, "parse error"));
                continue;
            }
        };
        if msg.get("method").and_then(Value::as_str) == Some("tools/call") {
            let Some(id) = msg.get("id").cloned() else {
                continue;
            };
            if msg.pointer("/params/name").and_then(Value::as_str) != Some("resilient_fetch") {
                send(&out, &error(&id, -32602, "unknown tool"));
                continue;
            }
            if in_flight.fetch_add(1, Ordering::SeqCst) >= MAX_PENDING {
                in_flight.fetch_sub(1, Ordering::SeqCst);
                send(
                    &out,
                    &ok(
                        &id,
                        json!({ "content": [{ "type": "text", "text": "too many requests in flight; retry later" }], "isError": true }),
                    ),
                );
                continue;
            }
            let (client, out, in_flight, slots, allow) = (
                client.clone(),
                out.clone(),
                in_flight.clone(),
                slots.clone(),
                allow.clone(),
            );
            let args = msg
                .pointer("/params/arguments")
                .cloned()
                .unwrap_or(json!({}));
            std::thread::spawn(move || {
                let result = if slots.acquire(MAX_ACTIVE, SLOT_WAIT) {
                    let r = call_fetch(&client, &allow, &args);
                    slots.release();
                    r
                } else {
                    json!({ "content": [{ "type": "text", "text": "busy: no slot within 30 s; retry later" }], "isError": true })
                };
                send(&out, &ok(&id, result));
                in_flight.fetch_sub(1, Ordering::SeqCst);
            });
            continue;
        }
        if let Some(resp) = handle_sync(&msg) {
            send(&out, &resp);
        }
    }
}
