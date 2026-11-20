//! Bounded end-to-end test against a **real mihomo process** on loopback.
//!
//! This is a test client, not an agent app integration: it checks that the
//! `connection-pool` CONNECT connector, used through mihomo's `mixed-port`,
//! carries certificate-validated TLS to a sandbox-owned HTTPS server.
//!
//! - Upstream: rustls HTTPS server on 127.0.0.1 with a leaf cert for
//!   `localhost` signed by a CA generated at startup (never installed in
//!   any system trust store).
//! - mihomo: started as a child from a generated config (`bind-address`
//!   127.0.0.1, `allow-lan: false`, `rules: MATCH,DIRECT`, no TUN, no DNS,
//!   no system proxy). It only ever dials the loopback upstream.
//! - Agents: N threads share one `Pool` (≤ cap sockets) through the mixed
//!   port; each checkout does a TLS handshake that validates the chain and
//!   the `localhost` name, then several keep-alive HTTPS requests.
//! - Independent client: a separate thread that does its own CONNECT on
//!   mihomo's HTTP `port` (no pool code), one TLS session, and a request
//!   every 200 ms for the whole steady phase; it must never disconnect.
//! - `curl` (if installed): one independent process request with
//!   `--proxy` + `--cacert`.
//! - Negative checks: an untrusted CA and a wrong server name must both be
//!   rejected by the client.
//! - Optional restart phase: mihomo is killed and restarted once; the pool
//!   must recover and stay within its caps.
//!
//! Usage: `mihomo-e2e <mihomo-bin> [agents=8] [steady_secs=15] [restart=1]`

use connection_pool::proxy::HttpConnectProxy;
use connection_pool::{Origin, Pool, PoolConfig, Priority};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Pki {
    ca_der: CertificateDer<'static>,
    ca_pem: String,
    leaf_der: CertificateDer<'static>,
    leaf_key: Vec<u8>,
}

fn make_pki(cn: &str) -> Pki {
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name.push(DnType::CommonName, cn);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca.self_signed(&ca_key).unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let leaf_key = KeyPair::generate().unwrap();
    let leaf_cert = leaf.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();
    Pki {
        ca_der: ca_cert.der().clone(),
        ca_pem: ca_cert.pem(),
        leaf_der: leaf_cert.der().clone(),
        leaf_key: leaf_key.serialize_der(),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Keep-alive HTTP/1.1 over TLS; counts concurrent TLS connections.
fn start_https(pki: &Pki, open: Arc<AtomicUsize>, peak: Arc<AtomicUsize>) -> u16 {
    let cfg = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![pki.leaf_der.clone(), pki.ca_der.clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pki.leaf_key.clone())),
            )
            .unwrap(),
    );
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for sock in l.incoming().flatten() {
            let (cfg, open, peak) = (cfg.clone(), open.clone(), peak.clone());
            std::thread::spawn(move || {
                let n = open.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                let _ = sock.set_read_timeout(Some(Duration::from_secs(30)));
                let tls = StreamOwned::new(ServerConnection::new(cfg).unwrap(), sock);
                let _ = serve(tls);
                open.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    port
}

fn serve<S: Read + Write>(s: S) -> io::Result<()> {
    let mut r = BufReader::new(s);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
        let mut close = false;
        loop {
            let mut h = String::new();
            if r.read_line(&mut h)? == 0 {
                return Ok(());
            }
            if h == "\r\n" {
                break;
            }
            if h.to_ascii_lowercase().starts_with("connection:") && h.contains("close") {
                close = true;
            }
        }
        let body = format!("secure {path}");
        let w = r.get_mut();
        write!(
            w,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )?;
        w.flush()?;
        if close {
            return Ok(());
        }
    }
}

fn https_get<S: Read + Write>(s: &mut S, path: &str) -> io::Result<String> {
    write!(s, "GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    s.flush()?;
    let mut r = BufReader::new(s);
    let mut line = String::new();
    r.read_line(&mut line)?;
    if !line.starts_with("HTTP/1.1 200") {
        return Err(io::Error::other(format!("bad status: {line:?}")));
    }
    let mut len = 0;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if h == "\r\n" {
            break;
        }
        if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
            len = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; len];
    r.read_exact(&mut body)?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn client_config(ca: &CertificateDer<'static>) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(ca.clone()).unwrap();
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

struct Mihomo {
    bin: String,
    dir: std::path::PathBuf,
    child: Option<Child>,
    mixed: u16,
    http: u16,
}

impl Mihomo {
    fn new(bin: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("mihomo-e2e-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (mixed, http) = (free_port(), free_port());
        let cfg = format!(
            "mixed-port: {mixed}\nport: {http}\nallow-lan: false\nbind-address: 127.0.0.1\nmode: rule\nlog-level: warning\nipv6: false\nunified-delay: false\ndns:\n  enable: false\ntun:\n  enable: false\nprofile:\n  store-selected: false\n  store-fake-ip: false\nrules:\n  - MATCH,DIRECT\n"
        );
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
        Self {
            bin: bin.into(),
            dir,
            child: None,
            mixed,
            http,
        }
    }
    fn start(&mut self) {
        let child = Command::new(&self.bin)
            .arg("-d")
            .arg(&self.dir)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start mihomo");
        self.child = Some(child);
        let t = Instant::now();
        while TcpStream::connect(("127.0.0.1", self.mixed)).is_err()
            || TcpStream::connect(("127.0.0.1", self.http)).is_err()
        {
            assert!(
                t.elapsed() < Duration::from_secs(15),
                "mihomo did not open its ports"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn stop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for Mihomo {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[derive(Default)]
struct McpPhase {
    ran: bool,
    calls: usize,
    ok: usize,
    errors: usize,
    wrong_name_rejected: bool,
}

fn run_mcp_phase(
    bin: &str,
    mixed: u16,
    ca_file: &std::path::Path,
    up: u16,
    agents: usize,
) -> McpPhase {
    use serde_json::{json, Value};
    let mut child = Command::new(bin)
        .env("AGENT_FETCH_PROXY", format!("http://127.0.0.1:{mixed}"))
        .env("AGENT_FETCH_CA_FILE", ca_file)
        .env(
            "AGENT_FETCH_ALLOW",
            format!("https://localhost:{up},https://127.0.0.1:{up}"),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start agent-fetch-mcp");
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut send = |v: Value| {
        writeln!(stdin, "{v}").unwrap();
        stdin.flush().unwrap();
    };
    let mut recv = || {
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        serde_json::from_str::<Value>(&line).unwrap()
    };
    send(
        json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"mihomo-e2e","version":"0"}}}),
    );
    let _ = recv();
    send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    let calls = agents * 10;
    for i in 0..calls {
        send(
            json!({"jsonrpc":"2.0","id":i + 1,"method":"tools/call","params":{"name":"resilient_fetch","arguments":{"url":format!("https://localhost:{up}/mcp/{i}")}}}),
        );
    }
    let (mut ok, mut errors) = (0, 0);
    for _ in 0..calls {
        let r = recv();
        let i = r["id"].as_u64().unwrap_or(0) as usize - 1;
        let text = r["result"]["content"][0]["text"].as_str().unwrap_or("");
        if r["result"]["isError"] == false && text.ends_with(&format!("secure /mcp/{i}")) {
            ok += 1;
        } else {
            if errors == 0 {
                eprintln!("first mcp error: {text}");
            }
            errors += 1;
        }
    }
    send(
        json!({"jsonrpc":"2.0","id":99999,"method":"tools/call","params":{"name":"resilient_fetch","arguments":{"url":format!("https://127.0.0.1:{up}/neg")}}}),
    );
    let r = recv();
    let wrong_name_rejected = r["result"]["isError"] == true
        && r["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("")
            .contains("TLS");
    let _ = child.kill();
    let _ = child.wait();
    McpPhase {
        ran: true,
        calls,
        ok,
        errors,
        wrong_name_rejected,
    }
}

fn spawn_independent(
    http: u16,
    up: u16,
    cfg: Arc<ClientConfig>,
    stop: Arc<AtomicBool>,
    ok: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut s = independent_tls(http, up, cfg).expect("independent connect");
        while !stop.load(Ordering::SeqCst) {
            match https_get(&mut s, "/independent") {
                Ok(b) if b == "secure /independent" => ok.fetch_add(1, Ordering::SeqCst),
                _ => {
                    err.fetch_add(1, Ordering::SeqCst);
                    break;
                }
            };
            std::thread::sleep(Duration::from_millis(200));
        }
    })
}

/// Independent client: hand-written CONNECT on mihomo's HTTP port.
fn independent_tls(
    http_port: u16,
    up: u16,
    cfg: Arc<ClientConfig>,
) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
    let mut s = TcpStream::connect(("127.0.0.1", http_port))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        s,
        "CONNECT localhost:{up} HTTP/1.1\r\nHost: localhost:{up}\r\n\r\n"
    )?;
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if s.read(&mut b)? == 0 || head.len() > 8192 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        head.push(b[0]);
    }
    if !head.starts_with(b"HTTP/1.1 200") && !head.starts_with(b"HTTP/1.0 200") {
        return Err(io::Error::other(
            String::from_utf8_lossy(&head).into_owned(),
        ));
    }
    let conn = ClientConnection::new(cfg, ServerName::try_from("localhost").unwrap())
        .map_err(io::Error::other)?;
    Ok(StreamOwned::new(conn, s))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let bin = args.first().cloned().unwrap_or_else(|| "mihomo".into());
    let agents: usize = args
        .get(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(8)
        .min(32);
    let steady = Duration::from_secs(
        args.get(2)
            .and_then(|a| a.parse().ok())
            .unwrap_or(15u64)
            .min(60),
    );
    let restart = args.get(3).map(|a| a != "0").unwrap_or(true);
    let _ = rustls::crypto::ring::default_provider().install_default();

    let pki = make_pki("resilient-agent-client sandbox CA");
    let rogue = make_pki("untrusted CA");
    let (tls_open, tls_peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let up = start_https(&pki, tls_open.clone(), tls_peak.clone());
    let mut mihomo = Mihomo::new(&bin);
    mihomo.start();
    let version = Command::new(&bin)
        .arg("-v")
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .next()
                .unwrap_or("")
                .to_string()
        })
        .unwrap_or_default();

    let pool_cfg = PoolConfig {
        connect_timeout: Duration::from_secs(2),
        io_timeout: Duration::from_secs(5),
        total_timeout: Duration::from_secs(30),
        queue_timeout: Duration::from_secs(10),
        ..PoolConfig::default()
    };
    let cap = pool_cfg.max_per_origin;
    let proxy = HttpConnectProxy::new(Origin::new("127.0.0.1", mihomo.mixed));
    let pool = Arc::new(Pool::new(pool_cfg, Arc::new(proxy)).unwrap());
    let origin = Origin::new("localhost", up);
    let trusted = client_config(&pki.ca_der);

    // Negative checks first: validation must actually be enforced.
    let reject = |cfg: Arc<ClientConfig>, name: &str| -> bool {
        let mut c = pool.checkout(&origin, Priority::Interactive).unwrap();
        let mut tls =
            ClientConnection::new(cfg, ServerName::try_from(name.to_string()).unwrap()).unwrap();
        let r = https_get(&mut rustls::Stream::new(&mut tls, c.stream()), "/neg").is_err();
        c.discard();
        r
    };
    let untrusted_rejected = reject(client_config(&rogue.ca_der), "localhost");
    let wrong_name_rejected = reject(trusted.clone(), "not-localhost.test");

    // Independent long-lived client on mihomo's HTTP port.
    let stop = Arc::new(AtomicBool::new(false));
    let (ind_ok, ind_err) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let independent = {
        let (stop, ok, err, cfg, http) = (
            stop.clone(),
            ind_ok.clone(),
            ind_err.clone(),
            trusted.clone(),
            mihomo.http,
        );
        std::thread::spawn(move || {
            let mut s = independent_tls(http, up, cfg).expect("independent connect");
            while !stop.load(Ordering::SeqCst) {
                match https_get(&mut s, "/independent") {
                    Ok(b) if b == "secure /independent" => ok.fetch_add(1, Ordering::SeqCst),
                    _ => {
                        err.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                };
                std::thread::sleep(Duration::from_millis(200));
            }
        })
    };

    let (ok, err, handshakes) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    let run_agents = |until: Instant| {
        let hs: Vec<_> = (0..agents)
            .map(|i| {
                let (pool, origin, cfg) = (pool.clone(), origin.clone(), trusted.clone());
                let (ok, err, handshakes) = (ok.clone(), err.clone(), handshakes.clone());
                std::thread::spawn(move || {
                    while Instant::now() < until {
                        let mut c = match pool.checkout(&origin, Priority::Interactive) {
                            Ok(c) => c,
                            Err(_) => {
                                err.fetch_add(1, Ordering::Relaxed);
                                std::thread::sleep(Duration::from_millis(200));
                                continue;
                            }
                        };
                        let mut tls = ClientConnection::new(
                            cfg.clone(),
                            ServerName::try_from("localhost").unwrap(),
                        )
                        .unwrap();
                        let mut s = rustls::Stream::new(&mut tls, c.stream());
                        handshakes.fetch_add(1, Ordering::Relaxed);
                        for _ in 0..5 {
                            match https_get(&mut s, &format!("/agent/{i}")) {
                                Ok(b) if b == format!("secure /agent/{i}") => {
                                    ok.fetch_add(1, Ordering::Relaxed)
                                }
                                _ => {
                                    err.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            };
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        c.discard();
                    }
                })
            })
            .collect();
        hs
    };

    // Phase 1: steady state.
    let steady_agents = run_agents(Instant::now() + steady);
    std::thread::sleep(steady / 2);
    let ca_file = mihomo.dir.join("sandbox-ca.pem");
    std::fs::write(&ca_file, &pki.ca_pem).unwrap();
    let curl = Command::new("curl")
        .args([
            "-sS",
            "--max-time",
            "10",
            "--proxy",
            &format!("http://127.0.0.1:{}", mihomo.mixed),
            "--cacert",
        ])
        .arg(&ca_file)
        .arg(format!("https://localhost:{up}/curl"))
        .output();
    let curl_result = match curl {
        Ok(o) if o.status.success() && o.stdout == b"secure /curl" => "ok".to_string(),
        Ok(o) => format!("failed: {}", String::from_utf8_lossy(&o.stderr).trim()),
        Err(_) => "not installed".to_string(),
    };
    for h in steady_agents {
        h.join().unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    independent.join().unwrap();
    let (steady_ok, steady_err) = (ok.swap(0, Ordering::SeqCst), err.swap(0, Ordering::SeqCst));
    let steady_peak = pool.stats().peak_per_origin;

    // Phase 1b: the public `agent_client` API, as a consumer would use it:
    // `Client::get("https://…")` through mihomo's mixed-port, TLS verified
    // by the client itself, while a separate independent client stays up.
    let api_cfg = |tls: agent_client::Tls| agent_client::ClientConfig {
        proxy: agent_client::ProxyMode::Http(HttpConnectProxy::new(Origin::new(
            "127.0.0.1",
            mihomo.mixed,
        ))),
        tls,
        ..agent_client::ClientConfig::default()
    };
    let api = Arc::new(
        agent_client::Client::new(api_cfg(
            agent_client::Tls::only_ca_pem(pki.ca_pem.as_bytes()).unwrap(),
        ))
        .unwrap(),
    );
    let stop_b = Arc::new(AtomicBool::new(false));
    let (api_ind_ok, api_ind_err) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let independent_b = spawn_independent(
        mihomo.http,
        up,
        trusted.clone(),
        stop_b.clone(),
        api_ind_ok.clone(),
        api_ind_err.clone(),
    );
    let (api_ok, api_err) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let until = Instant::now() + steady;
    let api_agents: Vec<_> = (0..agents)
        .map(|i| {
            let (api, ok, err) = (api.clone(), api_ok.clone(), api_err.clone());
            std::thread::spawn(move || {
                while Instant::now() < until {
                    match api.get(&format!("https://localhost:{up}/api/{i}")) {
                        Ok(r)
                            if r.status == 200
                                && r.body == format!("secure /api/{i}").as_bytes() =>
                        {
                            ok.fetch_add(1, Ordering::Relaxed)
                        }
                        other => {
                            eprintln!("api agent {i}: {:?}", other.map(|r| r.status));
                            err.fetch_add(1, Ordering::Relaxed)
                        }
                    };
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        })
        .collect();
    let example = match std::env::var("FETCH_EXAMPLE") {
        Ok(path) => match Command::new(&path)
            .arg(format!("https://localhost:{up}/example"))
            .arg("--cacert")
            .arg(&ca_file)
            .args(["--proxy", &format!("http://127.0.0.1:{}", mihomo.mixed)])
            .output()
        {
            Ok(o)
                if o.status.success()
                    && String::from_utf8_lossy(&o.stdout).trim() == "secure /example" =>
            {
                "ok".to_string()
            }
            Ok(o) => format!("failed: {}", String::from_utf8_lossy(&o.stderr).trim()),
            Err(e) => format!("failed: {e}"),
        },
        Err(_) => "not built".to_string(),
    };
    for h in api_agents {
        h.join().unwrap();
    }
    stop_b.store(true, Ordering::SeqCst);
    independent_b.join().unwrap();
    let untrusted = agent_client::Client::new(api_cfg(agent_client::Tls::webpki_roots())).unwrap();
    let api_untrusted_rejected = matches!(
        untrusted.get(&format!("https://localhost:{up}/neg")),
        Err(agent_client::ClientError::Tls(_))
    );
    let api_wrong_name_rejected = matches!(
        api.get(&format!("https://127.0.0.1:{up}/neg")),
        Err(agent_client::ClientError::Tls(_))
    );
    let api_stats = api.pool().stats();
    let api_breaker_closed =
        api.breakers().state(&Origin::new("localhost", up).key()) == agent_client::State::Closed;
    println!(
        "{{\"phase\":\"public-api\",\"agents\":{agents},\"secs\":{},\"https_ok\":{},\"errors\":{},\"sockets_opened\":{},\"tls_sessions_reused\":{},\"peak_per_origin\":{},\"independent_ok\":{},\"independent_errors\":{},\"example\":\"{example}\",\"untrusted_ca_rejected\":{api_untrusted_rejected},\"wrong_name_rejected\":{api_wrong_name_rejected},\"breaker_closed\":{api_breaker_closed}}}",
        steady.as_secs(),
        api_ok.load(Ordering::SeqCst),
        api_err.load(Ordering::SeqCst),
        api_stats.opened,
        api_stats.reused,
        api_stats.peak_per_origin,
        api_ind_ok.load(Ordering::SeqCst),
        api_ind_err.load(Ordering::SeqCst),
    );
    drop(api);

    // Phase 1c: the `agent-fetch-mcp` stdio server, configured the way a
    // `.cursor/mcp.json` entry would start it, driven over stdio.
    let mcp = match std::env::var("MCP_SERVER") {
        Ok(bin) => run_mcp_phase(&bin, mihomo.mixed, &ca_file, up, agents),
        Err(_) => McpPhase::default(),
    };
    println!(
        "{{\"phase\":\"mcp\",\"ran\":{},\"calls\":{},\"ok\":{},\"errors\":{},\"wrong_name_rejected\":{}}}",
        mcp.ran, mcp.calls, mcp.ok, mcp.errors, mcp.wrong_name_rejected
    );

    // Phase 2: one real mihomo restart under load.
    let (mut restart_ok, mut restart_err, mut recovery_ms) = (0, 0, 0u128);
    if restart {
        let until = Instant::now() + Duration::from_secs(6);
        let hs = run_agents(until);
        std::thread::sleep(Duration::from_secs(2));
        mihomo.stop();
        std::thread::sleep(Duration::from_secs(1));
        let back = Instant::now();
        mihomo.start();
        for h in hs {
            h.join().unwrap();
        }
        restart_ok = ok.swap(0, Ordering::SeqCst);
        restart_err = err.swap(0, Ordering::SeqCst);
        // Recovery: first successful validated request after restart.
        loop {
            let mut c = match pool.checkout(&origin, Priority::Interactive) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let mut tls =
                ClientConnection::new(trusted.clone(), ServerName::try_from("localhost").unwrap())
                    .unwrap();
            let r = https_get(&mut rustls::Stream::new(&mut tls, c.stream()), "/recovered");
            c.discard();
            if r.is_ok() {
                recovery_ms = back.elapsed().as_millis();
                break;
            }
            assert!(
                back.elapsed() < Duration::from_secs(20),
                "no recovery after mihomo restart"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    let stats = pool.stats();
    mihomo.stop();

    println!(
        "{{\"mihomo\":\"{version}\",\"agents\":{agents},\"steady_secs\":{},\"steady_https_ok\":{steady_ok},\"steady_errors\":{steady_err},\"tls_handshakes\":{},\"independent_ok\":{},\"independent_errors\":{},\"curl\":\"{curl_result}\",\"untrusted_ca_rejected\":{untrusted_rejected},\"wrong_name_rejected\":{wrong_name_rejected},\"peak_per_origin\":{},\"steady_peak_per_origin\":{steady_peak},\"cap\":{cap},\"upstream_tls_peak\":{},\"restart_https_ok\":{restart_ok},\"restart_errors\":{restart_err},\"recovery_ms\":{recovery_ms},\"open_after\":{}}}",
        steady.as_secs(),
        handshakes.load(Ordering::SeqCst),
        ind_ok.load(Ordering::SeqCst),
        ind_err.load(Ordering::SeqCst),
        stats.peak_per_origin,
        tls_peak.load(Ordering::SeqCst),
        stats.open_now,
    );
    let mut fail = Vec::new();
    if mcp.ran && (mcp.ok != mcp.calls || mcp.errors > 0 || !mcp.wrong_name_rejected) {
        fail.push("agent-fetch-mcp phase failed");
    }
    if api_ok.load(Ordering::SeqCst) == 0 || api_err.load(Ordering::SeqCst) > 0 {
        fail.push("public API HTTPS through mihomo had errors");
    }
    if api_ind_err.load(Ordering::SeqCst) > 0 || api_ind_ok.load(Ordering::SeqCst) == 0 {
        fail.push("independent client disconnected during public-API phase");
    }
    if !api_untrusted_rejected || !api_wrong_name_rejected {
        fail.push("public API did not enforce certificate validation");
    }
    if api_stats.peak_per_origin > cap {
        fail.push("public API exceeded per-origin cap");
    }
    if example.starts_with("failed") {
        fail.push("fetch example failed");
    }
    if steady_ok == 0 || steady_err > 0 {
        fail.push("steady-state HTTPS through mihomo had errors");
    }
    if ind_err.load(Ordering::SeqCst) > 0 || ind_ok.load(Ordering::SeqCst) == 0 {
        fail.push("independent client was disconnected");
    }
    if !untrusted_rejected || !wrong_name_rejected {
        fail.push("certificate validation not enforced");
    }
    if stats.peak_per_origin > cap {
        fail.push("per-origin cap exceeded");
    }
    if curl_result.starts_with("failed") {
        fail.push("curl through mihomo failed");
    }
    if fail.is_empty() {
        eprintln!("PASS");
    } else {
        eprintln!("FAIL: {}", fail.join("; "));
        std::process::exit(1);
    }
}
