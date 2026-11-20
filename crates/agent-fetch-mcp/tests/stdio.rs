//! Drives the real binary over stdio, as Cursor would, against loopback
//! fixtures only.

use loopback_fixtures::{ConnectProxy, Upstream};
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Loopback HTTPS upstream (`localhost` cert from a per-test CA) that
/// answers `secure <path>` on keep-alive connections.
struct TlsUpstream {
    port: u16,
    ca_file: std::path::PathBuf,
    peak: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
}

fn tls_upstream() -> TlsUpstream {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca.self_signed(&ca_key).unwrap();
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .unwrap();
    let ca_file = std::env::temp_dir().join(format!(
        "afm-ca-{}-{}.pem",
        std::process::id(),
        leaf_key.serialize_der().len() ^ rand_suffix()
    ));
    std::fs::write(&ca_file, ca_cert.pem()).unwrap();
    let cfg = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(leaf_key.serialize_der().into()),
            )
            .unwrap(),
    );
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let (open, peak, accepted) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let (p2, a2) = (peak.clone(), accepted.clone());
    std::thread::spawn(move || {
        for sock in l.incoming().flatten() {
            a2.fetch_add(1, Ordering::SeqCst);
            let (cfg, open, peak) = (cfg.clone(), open.clone(), p2.clone());
            std::thread::spawn(move || {
                peak.fetch_max(open.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                let mut tls =
                    rustls::StreamOwned::new(rustls::ServerConnection::new(cfg).unwrap(), sock);
                let mut buf = Vec::new();
                let mut b = [0u8; 1024];
                while let Ok(n) = tls.read(&mut b) {
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&b[..n]);
                    while let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..end]).to_string();
                        buf.drain(..end + 4);
                        let path = head.split_whitespace().nth(1).unwrap_or("/").to_string();
                        let body = format!("secure {path}");
                        let _ = write!(
                            tls,
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = tls.flush();
                    }
                }
                open.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    TlsUpstream {
        port,
        ca_file,
        peak,
        accepted,
    }
}

fn rand_suffix() -> usize {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as usize
}

fn call(id: i64, args: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"resilient_fetch","arguments":args}})
}
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
    let up = tls_upstream();
    let proxy = ConnectProxy::start(0).unwrap();
    let mut m = Mcp::start(&[
        (
            "AGENT_FETCH_PROXY",
            format!("http://127.0.0.1:{}", proxy.port),
        ),
        ("AGENT_FETCH_CA_FILE", up.ca_file.display().to_string()),
        (
            "AGENT_FETCH_ALLOW",
            format!("https://localhost:{}", up.port),
        ),
    ]);
    m.init();
    let n = 120; // more than the pool's 8 sockets + 32 queue: the server must hold the rest
    for i in 0..n {
        m.send(call(
            100 + i,
            json!({"url": format!("https://localhost:{}/item/{i}", up.port)}),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..n {
        let r = m.recv();
        assert_eq!(r["result"]["isError"], false, "{r}");
        let text = r["result"]["content"][0]["text"].as_str().unwrap();
        let id = r["id"].as_i64().unwrap();
        assert!(text.starts_with("HTTP 200"), "{text}");
        assert!(
            text.ends_with(&format!("secure /item/{}", id - 100)),
            "{text}"
        );
        seen.insert(id);
    }
    assert_eq!(seen.len(), n as usize);
    assert!(
        up.peak.load(Ordering::SeqCst) <= 8,
        "peak {}",
        up.peak.load(Ordering::SeqCst)
    );
    assert!(
        proxy.total_tunnels() <= 8,
        "{} tunnels",
        proxy.total_tunnels()
    );
}

#[test]
fn default_deny_and_exact_https_origins_only() {
    let up = tls_upstream();
    let plain = Upstream::start("plain").unwrap();
    let ca = ("AGENT_FETCH_CA_FILE", up.ca_file.display().to_string());
    // No allowlist: everything is denied without a connection.
    let mut m = Mcp::start(std::slice::from_ref(&ca));
    m.init();
    m.send(call(
        1,
        json!({"url": format!("https://localhost:{}/", up.port)}),
    ));
    let r = m.recv();
    assert_eq!(r["result"]["isError"], true);
    assert!(r["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("allowlist"));
    drop(m);
    let mut m = Mcp::start(&[
        ca,
        (
            "AGENT_FETCH_ALLOW",
            format!("https://localhost:{}", up.port),
        ),
    ]);
    m.init();
    let refused = [
        json!({"url": format!("http://127.0.0.1:{}/", plain.addr.port())}),
        json!({"url": format!("https://localhost:{}/", up.port + 1)}),
        json!({"url": format!("https://user:pw@localhost:{}/", up.port)}),
        json!({"url": format!("https://localhost:{}/", up.port), "proxy": "http://127.0.0.1:1"}),
        json!({"url": format!("https://localhost:{}/", up.port), "ca_file": "/tmp/x.pem"}),
        json!({"url": format!("https://localhost:{}/", up.port), "method": "POST"}),
    ];
    for (i, args) in refused.iter().enumerate() {
        m.send(call(10 + i as i64, args.clone()));
        let r = m.recv();
        assert_eq!(r["result"]["isError"], true, "{args} -> {r}");
    }
    m.send(call(
        20,
        json!({"url": format!("https://LOCALHOST:{}/ok", up.port)}),
    ));
    let r = m.recv();
    assert_eq!(r["result"]["isError"], false, "{r}");
    assert_eq!(
        up.accepted.load(Ordering::SeqCst),
        1,
        "a refused call opened a connection"
    );
    assert_eq!(plain.total_connections(), 0);
}

#[test]
fn invalid_allowlist_entries_stop_the_server() {
    for bad in [
        "https://*.example.com",
        "http://example.com",
        "https://u:p@example.com",
        "https://example.com/path",
        "https://example.com:0",
    ] {
        let status = Command::new(env!("CARGO_BIN_EXE_agent-fetch-mcp"))
            .env("AGENT_FETCH_ALLOW", bad)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(2), "{bad} accepted");
    }
}

#[test]
fn invalid_json_rpc_gets_invalid_request() {
    let mut m = Mcp::start(&[]);
    m.init();
    m.send(json!(null));
    assert_eq!(m.recv()["error"]["code"], -32600);
    m.send(json!({"jsonrpc":"2.0","id":7}));
    let r = m.recv();
    assert_eq!(
        (r["id"].clone(), r["error"]["code"].clone()),
        (json!(7), json!(-32600))
    );
}

#[test]
fn writes_and_unknown_tools_are_refused() {
    let mut m = Mcp::start(&[]);
    m.init();
    m.send(json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"other","arguments":{}}}));
    assert_eq!(m.recv()["error"]["code"], -32602);
}

#[test]
fn proxy_down_fails_fast_with_a_tool_error() {
    let up = tls_upstream();
    let proxy = ConnectProxy::start(0).unwrap();
    let mut m = Mcp::start(&[
        (
            "AGENT_FETCH_PROXY",
            format!("http://127.0.0.1:{}", proxy.port),
        ),
        ("AGENT_FETCH_CA_FILE", up.ca_file.display().to_string()),
        (
            "AGENT_FETCH_ALLOW",
            format!("https://localhost:{}", up.port),
        ),
    ]);
    m.init();
    proxy.kill();
    let url = format!("https://localhost:{}/x", up.port);
    let mut texts = Vec::new();
    for i in 0..8 {
        m.send(call(i, json!({"url": url})));
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
