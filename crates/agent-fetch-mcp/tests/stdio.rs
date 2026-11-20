//! Drives the real binary over stdio, as Cursor would, against loopback
//! fixtures only.

use loopback_fixtures::{ConnectProxy, Upstream};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

struct Mcp {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Mcp {
    fn start(env: &[(&str, String)]) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_agent-fetch-mcp"));
        for v in [
            "HTTPS_PROXY",
            "https_proxy",
            "HTTP_PROXY",
            "http_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            cmd.env_remove(v);
        }
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
        }
    }
    fn send(&mut self, v: Value) {
        writeln!(self.stdin, "{v}").unwrap();
        self.stdin.flush().unwrap();
    }
    fn recv(&mut self) -> Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        assert!(!line.trim_end().contains('\n'));
        serde_json::from_str(&line).unwrap()
    }
    fn init(&mut self) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}));
        let r = self.recv();
        self.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        r
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn lifecycle_and_tool_listing_follow_the_spec() {
    let mut m = Mcp::start(&[]);
    let init = m.init();
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    assert!(init["result"]["capabilities"]["tools"].is_object());
    m.send(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}));
    let list = m.recv();
    assert_eq!(list["id"], 2);
    assert_eq!(list["result"]["tools"][0]["name"], "resilient_fetch");
    m.send(json!({"jsonrpc":"2.0","id":3,"method":"nope"}));
    assert_eq!(m.recv()["error"]["code"], -32601);
}

#[test]
fn concurrent_tool_calls_share_one_bounded_client_through_the_proxy() {
    let up = Upstream::start("api").unwrap();
    let proxy = ConnectProxy::start(0).unwrap();
    let mut m = Mcp::start(&[(
        "AGENT_FETCH_PROXY",
        format!("http://127.0.0.1:{}", proxy.port),
    )]);
    m.init();
    let n = 120; // more than the pool's 8 sockets + 32 queue: the server must hold the rest
    for i in 0..n {
        m.send(json!({"jsonrpc":"2.0","id":100+i,"method":"tools/call","params":{"name":"resilient_fetch","arguments":{"url":format!("http://127.0.0.1:{}/item/{i}", up.addr.port())}}}));
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..n {
        let r = m.recv();
        assert_eq!(r["result"]["isError"], false, "{r}");
        let text = r["result"]["content"][0]["text"].as_str().unwrap();
        let id = r["id"].as_i64().unwrap();
        assert!(text.starts_with("HTTP 200"), "{text}");
        assert!(text.ends_with(&format!("api /item/{}", id - 100)), "{text}");
        seen.insert(id);
    }
    assert_eq!(seen.len(), n as usize);
    assert!(up.peak_connections() <= 8, "peak {}", up.peak_connections());
    assert!(
        proxy.total_tunnels() <= 8,
        "{} tunnels",
        proxy.total_tunnels()
    );
}

#[test]
fn writes_and_unknown_tools_are_refused() {
    let mut m = Mcp::start(&[]);
    m.init();
    m.send(json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"resilient_fetch","arguments":{"url":"http://127.0.0.1:9/","method":"POST"}}}));
    let r = m.recv();
    assert_eq!(r["result"]["isError"], true);
    m.send(json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"other","arguments":{}}}));
    assert_eq!(m.recv()["error"]["code"], -32602);
}

#[test]
fn proxy_down_fails_fast_with_a_tool_error() {
    let up = Upstream::start("api").unwrap();
    let proxy = ConnectProxy::start(0).unwrap();
    let mut m = Mcp::start(&[(
        "AGENT_FETCH_PROXY",
        format!("http://127.0.0.1:{}", proxy.port),
    )]);
    m.init();
    proxy.kill();
    let url = format!("http://127.0.0.1:{}/x", up.addr.port());
    let mut texts = Vec::new();
    for i in 0..8 {
        m.send(json!({"jsonrpc":"2.0","id":i,"method":"tools/call","params":{"name":"resilient_fetch","arguments":{"url":url}}}));
        let r = m.recv();
        assert_eq!(r["result"]["isError"], true);
        texts.push(
            r["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    assert!(
        texts.iter().any(|t| t.contains("circuit open")),
        "{texts:?}"
    );
}
