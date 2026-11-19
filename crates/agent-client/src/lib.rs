//! Resilient HTTP/1.1 client composing the four modules (SPEC §4):
//! every request passes the origin's circuit breaker (rejected with zero
//! socket cost while Open), takes a socket from the bounded pool
//! (directly or tunnelled through a local HTTP proxy such as mihomo),
//! and on a network failure asks the shared retry coordinator whether
//! and when to retry. One `Client` is meant to be shared by every agent
//! in a process, so the caps bound the whole session, not each agent.

pub use circuit_breaker::{Config as BreakerConfig, Outcome, Registry, State};
pub use connection_pool::proxy::{parse_http_proxy_url, HttpConnectProxy, NoProxy};
pub use connection_pool::{
    Connector, DirectConnector, Origin, Pool, PoolConfig, PoolError, Priority,
};
pub use retry_budget::{
    BackoffConfig, BudgetConfig, Decision, FailFast, RetryConfig, RetryCoordinator, RetryStats,
};
pub use telemetry_isolation as isolation;

use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Certificate verification for `https` requests (rustls). The default
/// trusts the Mozilla root set (`webpki-roots`); add a private CA with
/// [`Tls::with_extra_ca_pem`]. Verification is never disabled.
#[derive(Clone)]
pub struct Tls(Arc<rustls::ClientConfig>);

impl Tls {
    pub fn webpki_roots() -> Self {
        Self::from_roots(webpki_store())
    }

    /// Mozilla roots plus every certificate in `pem`.
    pub fn with_extra_ca_pem(pem: &[u8]) -> Result<Self, String> {
        let mut roots = webpki_store();
        add_pem(&mut roots, pem)?;
        Ok(Self::from_roots(roots))
    }

    /// Only the certificates in `pem` (e.g. a sandbox CA in tests).
    pub fn only_ca_pem(pem: &[u8]) -> Result<Self, String> {
        let mut roots = rustls::RootCertStore::empty();
        add_pem(&mut roots, pem)?;
        Ok(Self::from_roots(roots))
    }

    pub fn from_rustls(config: Arc<rustls::ClientConfig>) -> Self {
        Self(config)
    }

    fn from_roots(roots: rustls::RootCertStore) -> Self {
        let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("ring supports the default TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        Self(Arc::new(cfg))
    }
}

fn webpki_store() -> rustls::RootCertStore {
    rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    }
}

fn add_pem(roots: &mut rustls::RootCertStore, pem: &[u8]) -> Result<(), String> {
    use rustls::pki_types::{pem::PemObject, CertificateDer};
    let mut n = 0;
    for cert in CertificateDer::pem_slice_iter(pem) {
        let cert = cert.map_err(|e| format!("bad CA PEM: {e}"))?;
        roots
            .add(cert)
            .map_err(|e| format!("bad CA certificate: {e}"))?;
        n += 1;
    }
    if n == 0 {
        return Err("no certificate in CA PEM".into());
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Http,
    Https,
}

/// `http(s)://host[:port][/path]` → (scheme, origin, path).
pub fn parse_url(url: &str) -> Result<(Scheme, Origin, String), ClientError> {
    let bad = || ClientError::InvalidUrl(url.to_string());
    let (scheme, rest, default_port) = if let Some(r) = url.strip_prefix("https://") {
        (Scheme::Https, r, 443)
    } else if let Some(r) = url.strip_prefix("http://") {
        (Scheme::Http, r, 80)
    } else {
        return Err(bad());
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() || authority.contains('@') {
        return Err(bad());
    }
    let origin = match authority.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() && !h.contains(':') => {
            Origin::new(h, p.parse().map_err(|_| bad())?)
        }
        Some(_) => return Err(bad()),
        None => Origin::new(authority, default_port),
    };
    Ok((scheme, origin, path))
}

/// How connections are dialled.
#[derive(Debug, Clone)]
pub enum ProxyMode {
    Direct,
    /// `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` + `NO_PROXY`; direct when unset.
    FromEnv,
    Http(HttpConnectProxy),
}

pub type Logger = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct ClientConfig {
    pub pool: PoolConfig,
    pub breaker: BreakerConfig,
    pub retry: RetryConfig,
    /// Attempts per request including the first.
    pub max_attempts: u32,
    /// Largest response body accepted.
    pub max_body: usize,
    pub proxy: ProxyMode,
    /// Trust roots for `https` requests.
    pub tls: Tls,
    /// Receives retry, budget and breaker log lines (SPEC §5).
    pub logger: Option<Logger>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            pool: PoolConfig::default(),
            breaker: BreakerConfig::default(),
            retry: RetryConfig::default(),
            max_attempts: 3,
            max_body: 16 << 20,
            proxy: ProxyMode::FromEnv,
            tls: Tls::webpki_roots(),
            logger: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
    pub priority: Priority,
    /// Defaults to RFC 9110 §9.2.2 by method.
    pub idempotent: Option<bool>,
}

impl Request {
    pub fn get(path: &str) -> Self {
        Self {
            method: "GET".into(),
            path: path.into(),
            body: Vec::new(),
            priority: Priority::Interactive,
            idempotent: None,
        }
    }
    pub fn post(path: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            method: "POST".into(),
            body: body.into(),
            ..Self::get(path)
        }
    }
    pub fn background(mut self) -> Self {
        self.priority = Priority::Background;
        self
    }
    pub fn idempotent(mut self, yes: bool) -> Self {
        self.idempotent = Some(yes);
        self
    }
    fn is_idempotent(&self) -> bool {
        self.idempotent.unwrap_or(matches!(
            self.method.as_str(),
            "GET" | "HEAD" | "OPTIONS" | "TRACE" | "PUT" | "DELETE"
        ))
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
    pub attempts: u32,
}

#[derive(Debug)]
pub enum ClientError {
    /// Rejected locally by the breaker; no socket was opened.
    CircuitOpen {
        origin: String,
    },
    /// Network failure with no retry allowed (budget spent, non-idempotent
    /// or attempts used up).
    Network {
        error: io::Error,
        reason: &'static str,
        attempts: u32,
    },
    Pool(PoolError),
    /// A malformed or oversized response.
    Protocol(io::Error),
    /// TLS failure, e.g. an untrusted certificate or a name mismatch.
    /// Not retried and not counted as a network failure.
    Tls(io::Error),
    InvalidUrl(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::CircuitOpen { origin } => write!(f, "circuit open for {origin}"),
            ClientError::Network {
                error,
                reason,
                attempts,
            } => {
                write!(
                    f,
                    "network error after {attempts} attempt(s) ({reason}): {error}"
                )
            }
            ClientError::Pool(e) => write!(f, "{e}"),
            ClientError::Protocol(e) => write!(f, "protocol error: {e}"),
            ClientError::Tls(e) => write!(f, "TLS error: {e}"),
            ClientError::InvalidUrl(u) => write!(f, "unsupported URL: {u}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// Transport failures that count toward the breaker (SPEC §4.3).
pub fn is_network_error(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionRefused
            | ConnectionReset
            | ConnectionAborted
            | TimedOut
            | WouldBlock
            | BrokenPipe
            | UnexpectedEof
            | NotConnected
    )
}

pub struct Client {
    config: ClientConfig,
    pool: Pool,
    breakers: Arc<Registry>,
    retries: Arc<RetryCoordinator>,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self, String> {
        let connector: Arc<dyn Connector> = match &config.proxy {
            ProxyMode::Direct => Arc::new(DirectConnector),
            ProxyMode::FromEnv => match HttpConnectProxy::from_env() {
                Some(p) => Arc::new(p),
                None if HttpConnectProxy::env_configured(|k| std::env::var(k).ok()) => {
                    return Err("proxy variable set but unsupported (need http:// without credentials); refusing to bypass it".into());
                }
                None => Arc::new(DirectConnector),
            },
            ProxyMode::Http(p) => Arc::new(p.clone()),
        };
        let pool = Pool::new(config.pool, connector)?;
        let mut reg = Registry::new(config.breaker);
        if let Some(log) = config.logger.clone() {
            reg = reg.with_observer(Arc::new(move |origin, t| {
                log(&format!(
                    "breaker origin={origin} {:?}->{:?} state_metric={} reason=\"{}\"",
                    t.from,
                    t.to,
                    t.to.as_metric(),
                    t.reason
                ))
            }));
        }
        Ok(Self {
            retries: Arc::new(RetryCoordinator::new(config.retry)),
            breakers: Arc::new(reg),
            pool,
            config,
        })
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn breakers(&self) -> &Arc<Registry> {
        &self.breakers
    }
    pub fn retries(&self) -> &Arc<RetryCoordinator> {
        &self.retries
    }

    fn log(&self, msg: impl FnOnce() -> String) {
        if let Some(l) = &self.config.logger {
            l(&msg());
        }
    }

    /// Plain-HTTP request to `origin`.
    pub fn send(&self, origin: &Origin, req: &Request) -> Result<Response, ClientError> {
        self.request(Scheme::Http, origin, req)
    }

    /// HTTPS request to `origin`, verified against [`ClientConfig::tls`]
    /// for the name `origin.host`. TLS sessions stay with their pooled
    /// socket, so keep-alive reuse skips the handshake.
    pub fn send_https(&self, origin: &Origin, req: &Request) -> Result<Response, ClientError> {
        self.request(Scheme::Https, origin, req)
    }

    /// GET an `http://` or `https://` URL.
    pub fn get(&self, url: &str) -> Result<Response, ClientError> {
        self.send_url(url, Request::get("/"))
    }

    /// Send `req` to `url`; the URL's path replaces `req.path`.
    pub fn send_url(&self, url: &str, mut req: Request) -> Result<Response, ClientError> {
        let (scheme, origin, path) = parse_url(url)?;
        req.path = path;
        self.request(scheme, &origin, &req)
    }

    pub fn request(
        &self,
        scheme: Scheme,
        origin: &Origin,
        req: &Request,
    ) -> Result<Response, ClientError> {
        let key = origin.key();
        let idempotent = req.is_idempotent();
        let mut attempts = 0u32;
        let mut stale_retries = 0usize;
        let mut mismatched = 0usize;
        loop {
            if !self.breakers.allow_request(&key) {
                return Err(ClientError::CircuitOpen { origin: key });
            }
            let mut conn = match self.pool.checkout(origin, req.priority) {
                Ok(c) => c,
                Err(PoolError::Connect(e)) if !is_network_error(&e) => {
                    self.breakers.abandon(&key);
                    return Err(ClientError::Pool(PoolError::Connect(e)));
                }
                Err(PoolError::Connect(e)) => {
                    attempts += 1;
                    match self.on_network_error(&key, e, idempotent, attempts) {
                        Ok(()) => continue,
                        Err(err) => return Err(err),
                    }
                }
                Err(e) => {
                    self.breakers.abandon(&key);
                    return Err(ClientError::Pool(e));
                }
            };
            let reused = conn.reused();
            let session = conn
                .take_session()
                .and_then(|s| s.downcast::<rustls::ClientConnection>().ok());
            let mut tls = match (scheme, session) {
                (Scheme::Https, Some(s)) => Some(*s),
                (Scheme::Http, None) => None,
                (_, _) if reused && mismatched < self.pool.config().max_per_origin => {
                    // Idle socket pooled for the other scheme: never mix.
                    conn.discard();
                    self.breakers.abandon(&key);
                    mismatched += 1;
                    continue;
                }
                (Scheme::Https, None) => {
                    let name = rustls::pki_types::ServerName::try_from(origin.host.clone())
                        .map_err(|_| ClientError::InvalidUrl(origin.host.clone()));
                    match name.and_then(|n| {
                        rustls::ClientConnection::new(self.config.tls.0.clone(), n)
                            .map_err(|e| ClientError::Tls(io::Error::other(e)))
                    }) {
                        Ok(c) => Some(c),
                        Err(e) => {
                            conn.discard();
                            self.breakers.abandon(&key);
                            return Err(e);
                        }
                    }
                }
                (Scheme::Http, Some(_)) => {
                    conn.discard();
                    self.breakers.abandon(&key);
                    return Err(ClientError::Protocol(io::Error::other(
                        "origin pooled as https; use send_https",
                    )));
                }
            };
            attempts += 1;
            match exchange(
                &mut conn,
                tls.as_mut(),
                origin,
                req,
                self.config.max_body,
                self.config.pool.io_timeout,
            ) {
                Ok((resp, keep_alive)) => {
                    self.breakers.record(&key, Outcome::Success);
                    self.retries.on_success(&key);
                    if keep_alive {
                        if let Some(t) = tls {
                            conn.set_session(Box::new(t));
                        }
                        conn.release();
                    } else {
                        conn.discard();
                    }
                    return Ok(Response { attempts, ..resp });
                }
                Err(Fail::Stale(_))
                    if reused
                        && idempotent
                        && stale_retries < self.pool.config().max_per_origin
                        && self.retries.try_spend_immediate(&key, Instant::now()) =>
                {
                    // A pooled keep-alive socket the peer already closed:
                    // replay at once on another socket. Spends a retry
                    // token but does not count toward the breaker.
                    conn.discard();
                    stale_retries += 1;
                    attempts -= 1;
                    self.breakers.abandon(&key);
                    continue;
                }
                Err(Fail::Stale(e)) | Err(Fail::Network(e)) => {
                    conn.discard();
                    match self.on_network_error(&key, e, idempotent, attempts) {
                        Ok(()) => continue,
                        Err(err) => return Err(err),
                    }
                }
                Err(Fail::Protocol(e)) => {
                    conn.discard();
                    self.breakers.record(&key, Outcome::NonNetworkError);
                    return Err(ClientError::Protocol(e));
                }
                Err(Fail::Tls(e)) => {
                    conn.discard();
                    self.breakers.record(&key, Outcome::NonNetworkError);
                    return Err(ClientError::Tls(e));
                }
            }
        }
    }

    /// Record a network failure; Ok(()) after sleeping means retry.
    fn on_network_error(
        &self,
        key: &str,
        error: io::Error,
        idempotent: bool,
        attempts: u32,
    ) -> Result<(), ClientError> {
        self.breakers.record(key, Outcome::NetworkError);
        if attempts >= self.config.max_attempts {
            return Err(ClientError::Network {
                error,
                reason: "attempts exhausted",
                attempts,
            });
        }
        match self.retries.on_failure(key, idempotent, Instant::now()) {
            Decision::Retry { after, coalesced } => {
                self.log(|| {
                    format!(
                        "retry origin={key} after_ms={} coalesced={coalesced} error=\"{error}\"",
                        after.as_millis()
                    )
                });
                std::thread::sleep(after);
                Ok(())
            }
            Decision::FailFast(FailFast::NonIdempotent) => Err(ClientError::Network {
                error,
                reason: "non-idempotent",
                attempts,
            }),
            Decision::FailFast(FailFast::BudgetExhausted { refill_in }) => {
                self.log(|| {
                    format!(
                        "retry-budget-exhausted origin={key} refill_in_ms={}",
                        refill_in.as_millis()
                    )
                });
                Err(ClientError::Network {
                    error,
                    reason: "retry budget exhausted",
                    attempts,
                })
            }
        }
    }
}

enum Fail {
    /// Failed before any response byte arrived (may be a stale socket).
    Stale(io::Error),
    Network(io::Error),
    Protocol(io::Error),
    Tls(io::Error),
}

fn classify(e: io::Error, got_bytes: bool) -> Fail {
    if e.get_ref().is_some_and(|i| i.is::<rustls::Error>()) {
        Fail::Tls(e)
    } else if !is_network_error(&e) {
        Fail::Protocol(e)
    } else if got_bytes {
        Fail::Network(e)
    } else {
        Fail::Stale(e)
    }
}

/// Reads that never outlive the request's total deadline, even when
/// bytes keep trickling in within each I/O timeout.
struct Deadlined<'a> {
    s: &'a mut std::net::TcpStream,
    deadline: Instant,
    io: Duration,
}

impl Read for Deadlined<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "total request timeout",
            ));
        }
        self.s.set_read_timeout(Some(left.min(self.io)))?;
        self.s.read(buf)
    }
}

impl Write for Deadlined<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "total request timeout",
            ));
        }
        self.s.set_write_timeout(Some(left.min(self.io)))?;
        self.s.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.s.flush()
    }
}

const MAX_HEAD_LINE: u64 = 8 * 1024;
const MAX_HEADERS: usize = 100;

/// `read_line` capped at [`MAX_HEAD_LINE`] bytes.
fn read_head_line(r: &mut impl BufRead, line: &mut String) -> io::Result<usize> {
    let n = r.by_ref().take(MAX_HEAD_LINE).read_line(line)?;
    if n as u64 == MAX_HEAD_LINE && !line.ends_with('\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response header line too long",
        ));
    }
    Ok(n)
}

fn exchange(
    conn: &mut connection_pool::Conn,
    tls: Option<&mut rustls::ClientConnection>,
    origin: &Origin,
    req: &Request,
    max_body: usize,
    io_timeout: Duration,
) -> Result<(Response, bool), Fail> {
    conn.arm_deadline().map_err(|e| classify(e, true))?;
    let deadline = Instant::now() + conn.remaining();
    let mut d = Deadlined {
        s: conn.stream(),
        deadline,
        io: io_timeout,
    };
    match tls {
        Some(t) => http_exchange(&mut rustls::Stream::new(t, &mut d), origin, req, max_body),
        None => http_exchange(&mut d, origin, req, max_body),
    }
}

fn http_exchange<S: Read + Write>(
    s: &mut S,
    origin: &Origin,
    req: &Request,
    max_body: usize,
) -> Result<(Response, bool), Fail> {
    let head = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        req.method,
        req.path,
        origin,
        req.body.len()
    );
    s.write_all(head.as_bytes())
        .and_then(|_| s.write_all(&req.body))
        .and_then(|_| s.flush())
        .map_err(|e| classify(e, false))?;
    let mut r = BufReader::new(s);
    // RFC 9110 §15.2: skip interim 1xx responses (e.g. 103 Early Hints)
    // and read the final one.
    let mut interim = 0;
    let status = loop {
        let mut line = String::new();
        match read_head_line(&mut r, &mut line) {
            Ok(0) if interim == 0 => {
                return Err(Fail::Stale(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "closed before response",
                )))
            }
            Ok(0) => {
                return Err(Fail::Network(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "closed after interim response",
                )))
            }
            Ok(_) => {}
            Err(e) => return Err(classify(e, interim > 0)),
        }
        let status: u16 = line
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| {
                Fail::Protocol(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad status line",
                ))
            })?;
        if !(100..200).contains(&status) {
            break status;
        }
        if status == 101 || interim >= 8 {
            return Err(Fail::Protocol(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported interim response",
            )));
        }
        interim += 1;
        for i in 0..=MAX_HEADERS {
            let mut h = String::new();
            if read_head_line(&mut r, &mut h).map_err(|e| classify(e, true))? == 0 {
                return Err(Fail::Network(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "closed in interim headers",
                )));
            }
            if h == "\r\n" || h == "\n" {
                break;
            }
            if i == MAX_HEADERS {
                return Err(Fail::Protocol(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "too many headers",
                )));
            }
        }
    };
    // RFC 9110 §6.3: no body for HEAD, 204, 304.
    let no_body = req.method == "HEAD" || status == 204 || status == 304;
    let mut len: Option<usize> = None;
    let mut keep_alive = true;
    for _ in 0..MAX_HEADERS {
        let mut h = String::new();
        if read_head_line(&mut r, &mut h).map_err(|e| classify(e, true))? == 0 {
            return Err(Fail::Network(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "closed in headers",
            )));
        }
        if h == "\r\n" || h == "\n" {
            if no_body {
                return Ok((
                    Response {
                        status,
                        body: Vec::new(),
                        attempts: 0,
                    },
                    keep_alive,
                ));
            }
            let len = len.ok_or_else(|| {
                Fail::Protocol(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing Content-Length",
                ))
            })?;
            if len > max_body {
                return Err(Fail::Protocol(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response body too large",
                )));
            }
            let mut body = vec![0u8; len];
            r.read_exact(&mut body).map_err(|e| classify(e, true))?;
            return Ok((
                Response {
                    status,
                    body,
                    attempts: 0,
                },
                keep_alive,
            ));
        }
        let lower = h.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            len = v.trim().parse().ok();
        } else if lower.starts_with("connection:") && lower.contains("close") {
            keep_alive = false;
        }
    }
    Err(Fail::Protocol(io::Error::new(
        io::ErrorKind::InvalidData,
        "too many headers",
    )))
}

/// Short timings for tests and harnesses: same rules, faster clock.
pub fn test_config(proxy: ProxyMode) -> ClientConfig {
    ClientConfig {
        pool: PoolConfig {
            connect_timeout: Duration::from_millis(500),
            io_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(2),
            queue_timeout: Duration::from_secs(3),
            ..PoolConfig::default()
        },
        breaker: BreakerConfig {
            open_hold_min: Duration::from_millis(500),
            open_hold_max: Duration::from_millis(1000),
            max_hold: Duration::from_secs(4),
            probe_timeout: Duration::from_secs(3),
            ..BreakerConfig::default()
        },
        retry: RetryConfig {
            backoff: retry_budget::BackoffConfig {
                base: Duration::from_millis(50),
                cap: Duration::from_millis(400),
            },
            budget: retry_budget::BudgetConfig {
                max_retries: 10,
                window: Duration::from_secs(5),
            },
        },
        max_attempts: 3,
        max_body: 1 << 20,
        proxy,
        tls: Tls::webpki_roots(),
        logger: None,
    }
}
