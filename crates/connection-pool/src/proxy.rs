//! HTTP CONNECT through a local proxy (e.g. mihomo / clash `mixed-port`
//! or `port`), with `NO_PROXY` handling.
//!
//! The pool dials the proxy, issues `CONNECT host:port`, and pools the
//! resulting tunnel under the *origin's* key, so the per-origin caps
//! bound the number of tunnels each origin holds open through the proxy.
//!
//! Proxy replies map onto transport errors so the circuit breaker sees
//! an unreachable upstream as a network failure:
//! 502 → `ConnectionRefused`, 503 → `ConnectionRefused`, 504 → `TimedOut`;
//! 407 → `PermissionDenied` (not a network fault).

use crate::{connect_any, Connector, Origin};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// Hosts that bypass the proxy.
#[derive(Debug, Clone, Default)]
pub struct NoProxy {
    all: bool,
    entries: Vec<String>,
}

impl NoProxy {
    /// Parse a `NO_PROXY` value: comma-separated hosts or domain
    /// suffixes (`.example.com` or `example.com`), `*` for everything.
    /// Ports in entries are ignored.
    pub fn parse(value: &str) -> Self {
        let mut np = NoProxy::default();
        for raw in value.split(',') {
            let e = raw.trim().to_ascii_lowercase();
            if e.is_empty() {
                continue;
            }
            if e == "*" {
                np.all = true;
                continue;
            }
            let host = match Origin::parse(&e) {
                Some(o) => o.host,
                None => e.trim_start_matches('[').trim_end_matches(']').to_string(),
            };
            np.entries.push(
                host.trim_start_matches("*.")
                    .trim_start_matches('.')
                    .to_string(),
            );
        }
        np
    }

    /// Loopback names always bypass a loopback proxy.
    pub fn with_loopback(mut self) -> Self {
        for h in ["localhost", "127.0.0.1", "::1"] {
            if !self.entries.iter().any(|e| e == h) {
                self.entries.push(h.to_string());
            }
        }
        self
    }

    pub fn matches(&self, host: &str) -> bool {
        if self.all {
            return true;
        }
        let h = host.trim_end_matches('.').to_ascii_lowercase();
        self.entries
            .iter()
            .any(|e| h == *e || h.ends_with(&format!(".{e}")))
    }
}

/// [`Connector`] that tunnels through an HTTP proxy with `CONNECT`.
#[derive(Debug, Clone)]
pub struct HttpConnectProxy {
    pub proxy: Origin,
    pub no_proxy: NoProxy,
}

impl HttpConnectProxy {
    pub fn new(proxy: Origin) -> Self {
        Self {
            proxy,
            no_proxy: NoProxy::default(),
        }
    }

    pub fn with_no_proxy(mut self, no_proxy: NoProxy) -> Self {
        self.no_proxy = no_proxy;
        self
    }

    /// Build from the environment: `HTTPS_PROXY`, then `HTTP_PROXY`,
    /// then `ALL_PROXY` (each also lowercase). Only `http://` proxies are
    /// used; `socks5://` values are skipped (point at mihomo's HTTP or
    /// mixed port instead). `NO_PROXY` plus loopback bypass the proxy.
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let pick = |names: &[&str]| {
            names
                .iter()
                .find_map(|n| get(n).filter(|v| !v.trim().is_empty()))
        };
        let proxy = [
            &["HTTPS_PROXY", "https_proxy"][..],
            &["HTTP_PROXY", "http_proxy"][..],
            &["ALL_PROXY", "all_proxy"][..],
        ]
        .iter()
        .filter_map(|names| pick(names))
        .find_map(|v| parse_http_proxy_url(&v))?;
        let no_proxy = pick(&["NO_PROXY", "no_proxy"])
            .map(|v| NoProxy::parse(&v))
            .unwrap_or_default()
            .with_loopback();
        Some(Self { proxy, no_proxy })
    }
}

/// `http://host:port[/]` → `Origin`. Port defaults to 80. Credentials
/// are not supported and make the URL rejected (never logged).
pub fn parse_http_proxy_url(url: &str) -> Option<Origin> {
    let url = url.trim();
    let rest = if let Some(r) = url.strip_prefix("http://") {
        r
    } else if url.contains("://") {
        return None;
    } else {
        url
    };
    let authority = rest.split('/').next()?;
    if authority.contains('@') {
        return None;
    }
    Origin::parse(authority).or_else(|| {
        if authority.is_empty() {
            None
        } else {
            Some(Origin::new(authority, 80))
        }
    })
}

impl Connector for HttpConnectProxy {
    fn connect(&self, origin: &Origin, timeout: Duration) -> io::Result<TcpStream> {
        if self.no_proxy.matches(&origin.host) {
            return connect_any(&origin.host, origin.port, timeout);
        }
        let deadline = Instant::now() + timeout;
        let mut s = connect_any(&self.proxy.host, self.proxy.port, timeout)?;
        let left = deadline
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1));
        s.set_read_timeout(Some(left))?;
        s.set_write_timeout(Some(left))?;
        let target = origin.to_string();
        s.write_all(format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n").as_bytes())?;
        let status = read_connect_reply(&mut s)?;
        match status {
            200..=299 => Ok(s),
            502 | 503 => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("proxy CONNECT {target}: {status}"),
            )),
            504 => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("proxy CONNECT {target}: 504"),
            )),
            407 => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy requires authentication (407)",
            )),
            other => Err(io::Error::other(format!("proxy CONNECT {target}: {other}"))),
        }
    }
}

/// Read the proxy's reply head byte by byte (so no tunnel bytes are
/// consumed) and return its status code. Head capped at 8 KiB.
fn read_connect_reply(s: &mut TcpStream) -> io::Result<u16> {
    let mut head = Vec::with_capacity(128);
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() >= 8192 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy reply head too large",
            ));
        }
        let n = s.read(&mut b)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy closed during CONNECT",
            ));
        }
        head.push(b[0]);
    }
    let line = head.split(|&c| c == b'\n').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line);
    line.split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad proxy status line"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;

    #[test]
    fn no_proxy_matching() {
        let np = NoProxy::parse(" .corp.example, internal:8080 ,10.0.0.1").with_loopback();
        assert!(np.matches("a.corp.example"));
        assert!(np.matches("corp.example"));
        assert!(np.matches("internal"));
        assert!(np.matches("10.0.0.1"));
        assert!(np.matches("localhost"));
        assert!(np.matches("127.0.0.1"));
        assert!(!np.matches("api2.cursor.sh"));
        assert!(!np.matches("notcorp.example"));
        assert!(NoProxy::parse("*").matches("anything"));
    }

    #[test]
    fn env_lookup_prefers_https_and_skips_socks() {
        let env: HashMap<&str, &str> = [
            ("ALL_PROXY", "socks5://127.0.0.1:7891"),
            ("https_proxy", "http://127.0.0.1:7890/"),
            ("NO_PROXY", "intranet"),
        ]
        .into();
        let p = HttpConnectProxy::from_lookup(|k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(p.proxy, Origin::new("127.0.0.1", 7890));
        assert!(p.no_proxy.matches("intranet"));
        assert!(p.no_proxy.matches("localhost"));

        let socks_only: HashMap<&str, &str> = [("ALL_PROXY", "socks5://127.0.0.1:7891")].into();
        assert!(
            HttpConnectProxy::from_lookup(|k| socks_only.get(k).map(|v| v.to_string())).is_none()
        );
        assert_eq!(parse_http_proxy_url("http://user:pw@h:1"), None);
        assert_eq!(
            parse_http_proxy_url("127.0.0.1:7890"),
            Some(Origin::new("127.0.0.1", 7890))
        );
    }

    /// One-shot fake proxy replying with `status` to a CONNECT.
    fn fake_proxy(status: &'static str) -> (Origin, std::thread::JoinHandle<String>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let h = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            let mut first = String::new();
            r.read_line(&mut first).unwrap();
            let mut line = String::new();
            while r.read_line(&mut line).unwrap() > 0 && line != "\r\n" {
                line.clear();
            }
            s.write_all(format!("HTTP/1.1 {status}\r\n\r\ntunnel-bytes").as_bytes())
                .unwrap();
            first
        });
        (Origin::new("127.0.0.1", port), h)
    }

    #[test]
    fn connect_success_leaves_tunnel_bytes_unread() {
        let (proxy, h) = fake_proxy("200 Connection Established");
        let c = HttpConnectProxy::new(proxy);
        let mut s = c
            .connect(&Origin::new("api.example", 443), Duration::from_secs(2))
            .unwrap();
        assert_eq!(h.join().unwrap(), "CONNECT api.example:443 HTTP/1.1\r\n");
        let mut buf = [0u8; 12];
        s.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"tunnel-bytes");
    }

    #[test]
    fn proxy_gateway_errors_map_to_network_errors() {
        for (status, kind) in [
            ("502 Bad Gateway", io::ErrorKind::ConnectionRefused),
            ("504 Gateway Timeout", io::ErrorKind::TimedOut),
            (
                "407 Proxy Authentication Required",
                io::ErrorKind::PermissionDenied,
            ),
        ] {
            let (proxy, h) = fake_proxy(status);
            let err = HttpConnectProxy::new(proxy)
                .connect(&Origin::new("x", 1), Duration::from_secs(2))
                .unwrap_err();
            h.join().unwrap();
            assert_eq!(err.kind(), kind, "{status}");
        }
    }

    #[test]
    fn proxy_down_is_connection_refused() {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let err = HttpConnectProxy::new(Origin::new("127.0.0.1", port))
            .connect(&Origin::new("x", 1), Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }
}
