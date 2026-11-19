//! Loopback-only test fixtures:
//! - [`Upstream`]: a keep-alive HTTP/1.1 server that answers every
//!   request with `200` and counts concurrent connections (its peak is
//!   the externally observed per-origin socket count).
//! - [`ConnectProxy`]: an HTTP CONNECT proxy standing in for a local
//!   mihomo / clash `mixed-port`. [`ConnectProxy::kill`] closes the
//!   listener and every tunnel (flapping); [`ConnectProxy::restart`]
//!   rebinds the same port.
//!
//! The proxy only tunnels to loopback targets, so tests built on it
//! cannot reach the internet.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Default)]
struct Gauge {
    now: AtomicUsize,
    peak: AtomicUsize,
    total: AtomicUsize,
}

impl Gauge {
    fn inc(&self) {
        let n = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(n, Ordering::SeqCst);
        self.total.fetch_add(1, Ordering::SeqCst);
    }
    fn dec(&self) {
        self.now.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Keep-alive HTTP/1.1 upstream on 127.0.0.1.
pub struct Upstream {
    pub addr: SocketAddr,
    name: String,
    conns: Arc<Gauge>,
    requests: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Upstream {
    pub fn start(name: &str) -> io::Result<Self> {
        let l = TcpListener::bind("127.0.0.1:0")?;
        l.set_nonblocking(true)?;
        let addr = l.local_addr()?;
        let conns = Arc::new(Gauge::default());
        let requests = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (c, r, s, n) = (
            conns.clone(),
            requests.clone(),
            stop.clone(),
            name.to_string(),
        );
        let handle = std::thread::spawn(move || {
            while !s.load(Ordering::SeqCst) {
                match l.accept() {
                    Ok((sock, _)) => {
                        let (c, r, n) = (c.clone(), r.clone(), n.clone());
                        c.inc();
                        std::thread::spawn(move || {
                            let _ = serve(sock, &n, &r);
                            c.dec();
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            addr,
            name: name.to_string(),
            conns,
            requests,
            stop,
            handle: Some(handle),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    /// Highest number of simultaneously open connections seen.
    pub fn peak_connections(&self) -> usize {
        self.conns.peak.load(Ordering::SeqCst)
    }
    pub fn open_connections(&self) -> usize {
        self.conns.now.load(Ordering::SeqCst)
    }
    pub fn total_connections(&self) -> usize {
        self.conns.total.load(Ordering::SeqCst)
    }
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn serve(sock: TcpStream, name: &str, requests: &AtomicUsize) -> io::Result<()> {
    sock.set_read_timeout(Some(Duration::from_secs(30)))?;
    let mut w = sock.try_clone()?;
    let mut r = BufReader::new(sock);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
        let mut len = 0usize;
        let mut close = false;
        loop {
            let mut h = String::new();
            if r.read_line(&mut h)? == 0 {
                return Ok(());
            }
            if h == "\r\n" || h == "\n" {
                break;
            }
            let lower = h.to_ascii_lowercase();
            if let Some(v) = lower.strip_prefix("content-length:") {
                len = v.trim().parse().unwrap_or(0);
            }
            if lower.starts_with("connection:") && lower.contains("close") {
                close = true;
            }
        }
        let mut body = vec![0u8; len.min(1 << 20)];
        r.read_exact(&mut body)?;
        requests.fetch_add(1, Ordering::SeqCst);
        let reply = format!("{name} {path}");
        write!(
            w,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n{reply}",
            reply.len(),
            if close { "close" } else { "keep-alive" }
        )?;
        if close {
            return Ok(());
        }
    }
}

/// Flappable loopback HTTP CONNECT proxy.
pub struct ConnectProxy {
    pub port: u16,
    inner: Arc<ProxyInner>,
    accept: Mutex<Option<(Arc<AtomicBool>, JoinHandle<()>)>>,
}

#[derive(Default)]
struct ProxyInner {
    tunnels: Gauge,
    sockets: Mutex<Vec<TcpStream>>,
    refused_targets: AtomicUsize,
}

impl ConnectProxy {
    /// Bind 127.0.0.1:`port` (0 picks a free port) and start accepting.
    pub fn start(port: u16) -> io::Result<Self> {
        let l = TcpListener::bind(("127.0.0.1", port))?;
        let port = l.local_addr()?.port();
        let p = Self {
            port,
            inner: Arc::new(ProxyInner::default()),
            accept: Mutex::new(None),
        };
        p.spawn_accept(l)?;
        Ok(p)
    }

    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
    }

    fn spawn_accept(&self, l: TcpListener) -> io::Result<()> {
        l.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let (s, inner) = (stop.clone(), self.inner.clone());
        let h = std::thread::spawn(move || {
            while !s.load(Ordering::SeqCst) {
                match l.accept() {
                    Ok((sock, _)) => {
                        let inner = inner.clone();
                        std::thread::spawn(move || {
                            let _ = tunnel(sock, &inner);
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2))
                    }
                    Err(_) => break,
                }
            }
            // Listener dropped here: new connects get ECONNREFUSED.
        });
        *self.accept.lock().unwrap() = Some((stop, h));
        Ok(())
    }

    /// Simulate the proxy dying: stop listening and tear down every
    /// tunnel (both legs).
    pub fn kill(&self) {
        if let Some((stop, h)) = self.accept.lock().unwrap().take() {
            stop.store(true, Ordering::SeqCst);
            let _ = h.join();
        }
        for s in self.inner.sockets.lock().unwrap().drain(..) {
            let _ = s.shutdown(Shutdown::Both);
        }
    }

    /// Rebind the same port after [`ConnectProxy::kill`].
    pub fn restart(&self) -> io::Result<()> {
        let mut tries = 0;
        loop {
            match TcpListener::bind(("127.0.0.1", self.port)) {
                Ok(l) => return self.spawn_accept(l),
                Err(e) if tries < 50 => {
                    let _ = e;
                    tries += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn is_up(&self) -> bool {
        self.accept.lock().unwrap().is_some()
    }
    pub fn peak_tunnels(&self) -> usize {
        self.inner.tunnels.peak.load(Ordering::SeqCst)
    }
    pub fn total_tunnels(&self) -> usize {
        self.inner.tunnels.total.load(Ordering::SeqCst)
    }
    pub fn open_tunnels(&self) -> usize {
        self.inner.tunnels.now.load(Ordering::SeqCst)
    }
    /// CONNECTs refused because the target was not loopback.
    pub fn refused_targets(&self) -> usize {
        self.inner.refused_targets.load(Ordering::SeqCst)
    }
}

impl Drop for ConnectProxy {
    fn drop(&mut self) {
        self.kill();
    }
}

fn is_loopback_target(target: &str) -> Option<SocketAddr> {
    let addr = target.to_socket_addrs().ok()?.next()?;
    let lo = match addr.ip() {
        IpAddr::V4(v) => v.is_loopback(),
        IpAddr::V6(v) => v.is_loopback(),
    };
    lo.then_some(addr)
}

fn tunnel(client: TcpStream, inner: &ProxyInner) -> io::Result<()> {
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut r = BufReader::new(client.try_clone()?);
    let mut line = String::new();
    r.read_line(&mut line)?;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h)? == 0 || h == "\r\n" || h == "\n" {
            break;
        }
    }
    let mut w = client.try_clone()?;
    let mut parts = line.split_whitespace();
    let (Some("CONNECT"), Some(target)) = (parts.next(), parts.next()) else {
        w.write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")?;
        return Ok(());
    };
    let Some(addr) = is_loopback_target(target) else {
        inner.refused_targets.fetch_add(1, Ordering::SeqCst);
        w.write_all(b"HTTP/1.1 403 Forbidden\r\n\r\n")?;
        return Ok(());
    };
    let upstream = match TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
        Ok(u) => u,
        Err(_) => {
            w.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")?;
            return Ok(());
        }
    };
    w.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")?;
    client.set_read_timeout(None)?;
    {
        let mut socks = inner.sockets.lock().unwrap();
        socks.push(client.try_clone()?);
        socks.push(upstream.try_clone()?);
    }
    inner.tunnels.inc();
    // BufReader may hold bytes the client sent right after CONNECT.
    let buffered = r.buffer().to_vec();
    let mut up_w = upstream.try_clone()?;
    up_w.write_all(&buffered)?;
    let mut up_r = upstream.try_clone()?;
    let mut cl_w = client.try_clone()?;
    let down = std::thread::spawn(move || {
        let _ = io::copy(&mut up_r, &mut cl_w);
        let _ = cl_w.shutdown(Shutdown::Write);
    });
    let mut cl_r = client;
    let _ = io::copy(&mut cl_r, &mut up_w);
    let _ = up_w.shutdown(Shutdown::Write);
    let _ = down.join();
    inner.tunnels.dec();
    let mut socks = inner.sockets.lock().unwrap();
    let ids = [cl_r.peer_addr().ok(), cl_r.local_addr().ok()];
    socks.retain(|s| {
        let same_client = s.peer_addr().ok() == ids[0] && s.local_addr().ok() == ids[1];
        let same_upstream = s.peer_addr().ok() == upstream.peer_addr().ok()
            && s.local_addr().ok() == upstream.local_addr().ok();
        !(same_client || same_upstream)
    });
    Ok(())
}
