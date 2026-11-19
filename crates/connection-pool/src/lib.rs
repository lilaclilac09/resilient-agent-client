//! Bounded per-origin connection pool (SPEC §4.1).
//!
//! - Hard per-origin socket cap (`max_per_origin`, 6–10 in production)
//!   plus a global cap across origins. Idle sockets count against both.
//! - Connect timeout on every dial; read/write timeouts on every socket;
//!   a total per-checkout deadline. A connection that overran its
//!   deadline or saw an error is destroyed, never returned to the pool.
//! - Requests above the cap queue (bounded, interactive first); overflow
//!   sheds background work before interactive work.
//! - Idle sockets expire after `idle_timeout` and are reaped on checkout
//!   and by an optional reaper thread.
//! - Debug builds assert the process FD count against `fd_ceiling`.
//!
//! Connections are dialled through a [`Connector`]: [`DirectConnector`]
//! or [`proxy::HttpConnectProxy`] (HTTP CONNECT through a local proxy
//! such as mihomo's `mixed-port`).

pub mod proxy;

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// A request target: `host:port`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Origin {
    pub host: String,
    pub port: u16,
}

impl Origin {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Parse `host:port` (IPv6 hosts in brackets: `[::1]:443`).
    pub fn parse(s: &str) -> Option<Self> {
        let (host, port) = s.rsplit_once(':')?;
        let port = port.parse().ok()?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return None;
        }
        Some(Self::new(host, port))
    }

    pub fn key(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Request priority. Interactive work is served first when queued and
/// is the last to be shed on overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    Interactive,
    Background,
}

/// Pool configuration. Defaults follow SPEC §4.1.
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// Open sockets (in use + idle) per origin. SPEC: 6–10.
    pub max_per_origin: usize,
    /// Open sockets across all origins.
    pub global_max: usize,
    pub connect_timeout: Duration,
    /// Read and write timeout applied to every socket.
    pub io_timeout: Duration,
    /// Total time a checked-out connection may be held for one request.
    pub total_timeout: Duration,
    /// Keep-alive idle timeout; idle sockets older than this are closed.
    pub idle_timeout: Duration,
    /// Waiters allowed per origin above the cap.
    pub max_queue_per_origin: usize,
    /// How long a queued checkout waits for a slot.
    pub queue_timeout: Duration,
    /// Debug builds assert the process FD count stays at or below this.
    pub fd_ceiling: Option<usize>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_per_origin: 8,
            global_max: 32,
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(15),
            total_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(20),
            max_queue_per_origin: 32,
            queue_timeout: Duration::from_secs(10),
            fd_ceiling: None,
        }
    }
}

impl PoolConfig {
    /// Hard upper bound on `max_per_origin` (SPEC §4.1).
    pub const MAX_PER_ORIGIN_LIMIT: usize = 10;

    pub fn validate(&self) -> Result<(), String> {
        if self.max_per_origin == 0 || self.max_per_origin > Self::MAX_PER_ORIGIN_LIMIT {
            return Err(format!(
                "max_per_origin must be 1..={}, got {}",
                Self::MAX_PER_ORIGIN_LIMIT,
                self.max_per_origin
            ));
        }
        if self.global_max < self.max_per_origin {
            return Err("global_max must be >= max_per_origin".into());
        }
        for (name, d) in [
            ("connect_timeout", self.connect_timeout),
            ("io_timeout", self.io_timeout),
            ("total_timeout", self.total_timeout),
        ] {
            if d.is_zero() {
                return Err(format!("{name} must be non-zero"));
            }
        }
        Ok(())
    }
}

/// Dials a connection to an origin.
pub trait Connector: Send + Sync {
    fn connect(&self, origin: &Origin, timeout: Duration) -> io::Result<TcpStream>;
}

/// Dial the origin directly.
#[derive(Debug, Default, Clone, Copy)]
pub struct DirectConnector;

impl Connector for DirectConnector {
    fn connect(&self, origin: &Origin, timeout: Duration) -> io::Result<TcpStream> {
        connect_any(&origin.host, origin.port, timeout)
    }
}

/// Resolve and try each address with a connect timeout.
pub fn connect_any(host: &str, port: u16, timeout: Duration) -> io::Result<TcpStream> {
    let mut last = io::Error::new(io::ErrorKind::NotFound, "no addresses resolved");
    for addr in (host, port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(s) => return Ok(s),
            Err(e) => last = e,
        }
    }
    Err(last)
}

#[derive(Debug)]
pub enum PoolError {
    /// The per-origin queue is full; background work is shed first.
    QueueFull,
    /// Waited `queue_timeout` without getting a slot.
    QueueTimeout,
    /// Dialling failed; the reserved slot was released.
    Connect(io::Error),
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolError::QueueFull => write!(f, "connection pool queue full"),
            PoolError::QueueTimeout => write!(f, "timed out waiting for a pooled connection"),
            PoolError::Connect(e) => write!(f, "connect failed: {e}"),
        }
    }
}

impl std::error::Error for PoolError {}

/// Pool counters for logs and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub opened: u64,
    pub reused: u64,
    pub returned: u64,
    pub destroyed: u64,
    pub idle_reaped: u64,
    pub idle_evicted: u64,
    pub connect_failures: u64,
    pub queue_full: u64,
    pub queue_timeouts: u64,
    /// Highest `open` seen for any single origin.
    pub peak_per_origin: usize,
    pub peak_global: usize,
    /// Sockets currently open (in use + idle).
    pub open_now: usize,
    pub idle_now: usize,
}

struct Idle {
    stream: TcpStream,
    since: Instant,
    session: Option<Session>,
}

/// Per-connection state a caller keeps with a pooled socket (e.g. a TLS
/// session), returned with the socket on the next checkout.
pub type Session = Box<dyn std::any::Any + Send>;

#[derive(Default)]
struct Slot {
    open: usize,
    idle: Vec<Idle>,
    waiting_interactive: usize,
    waiting_background: usize,
}

struct Inner {
    slots: HashMap<String, Slot>,
    global_open: usize,
    stats: PoolStats,
}

struct Shared {
    config: PoolConfig,
    connector: Arc<dyn Connector>,
    inner: Mutex<Inner>,
    cv: Condvar,
}

/// Bounded per-origin connection pool. Cheap to clone; clones share state.
#[derive(Clone)]
pub struct Pool {
    shared: Arc<Shared>,
}

impl Pool {
    pub fn new(config: PoolConfig, connector: Arc<dyn Connector>) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            shared: Arc::new(Shared {
                config,
                connector,
                inner: Mutex::new(Inner {
                    slots: HashMap::new(),
                    global_open: 0,
                    stats: PoolStats::default(),
                }),
                cv: Condvar::new(),
            }),
        })
    }

    pub fn config(&self) -> PoolConfig {
        self.shared.config
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.shared.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Check out a connection: reuse an idle one, open a new one within
    /// the caps, or queue (bounded) until one frees up.
    pub fn checkout(&self, origin: &Origin, priority: Priority) -> Result<Conn, PoolError> {
        let cfg = self.shared.config;
        let key = origin.key();
        let deadline = Instant::now() + cfg.queue_timeout;
        let mut g = self.lock();
        let mut queued = false;
        loop {
            let now = Instant::now();
            reap_slot(&mut g, &key, cfg.idle_timeout, now);
            let inner = &mut *g;
            let slot = inner.slots.entry(key.clone()).or_default();

            let yield_idle = priority == Priority::Background && slot.waiting_interactive > 0;
            if let Some(idle) = (!yield_idle).then(|| slot.idle.pop()).flatten() {
                inner.stats.reused += 1;
                inner.stats.idle_now -= 1;
                drop(g);
                return Ok(self.make_conn(key, idle.stream, true, idle.session));
            }

            let interactive_ahead = slot.waiting_interactive > 0;
            let may_go = priority == Priority::Interactive || !interactive_ahead;
            if may_go && slot.open < cfg.max_per_origin {
                if inner.global_open >= cfg.global_max {
                    if evict_one_idle(inner) {
                        continue;
                    }
                } else {
                    slot.open += 1;
                    inner.global_open += 1;
                    inner.stats.peak_per_origin = inner.stats.peak_per_origin.max(slot.open);
                    inner.stats.peak_global = inner.stats.peak_global.max(inner.global_open);
                    inner.stats.open_now = inner.global_open;
                    drop(g);
                    return self.dial(origin, key);
                }
            }

            // Must queue. Overflow sheds background first.
            let slot = g.slots.get_mut(&key).expect("slot exists");
            let waiting = slot.waiting_interactive + slot.waiting_background;
            let full = match priority {
                Priority::Background => waiting >= cfg.max_queue_per_origin,
                Priority::Interactive => slot.waiting_interactive >= cfg.max_queue_per_origin,
            };
            if full && !queued {
                g.stats.queue_full += 1;
                return Err(PoolError::QueueFull);
            }
            if queued && priority == Priority::Background && waiting > cfg.max_queue_per_origin {
                // Interactive arrivals pushed the queue past its bound.
                g.stats.queue_full += 1;
                return Err(PoolError::QueueFull);
            }
            let now = Instant::now();
            if now >= deadline {
                g.stats.queue_timeouts += 1;
                return Err(PoolError::QueueTimeout);
            }
            let slot = g.slots.get_mut(&key).expect("slot exists");
            match priority {
                Priority::Interactive => slot.waiting_interactive += 1,
                Priority::Background => slot.waiting_background += 1,
            }
            queued = true;
            let (ng, _) = self
                .shared
                .cv
                .wait_timeout(g, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            g = ng;
            let slot = g.slots.get_mut(&key).expect("slot exists");
            match priority {
                Priority::Interactive => slot.waiting_interactive -= 1,
                Priority::Background => slot.waiting_background -= 1,
            }
        }
    }

    fn dial(&self, origin: &Origin, key: String) -> Result<Conn, PoolError> {
        let cfg = self.shared.config;
        match self.shared.connector.connect(origin, cfg.connect_timeout) {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let _ = stream.set_read_timeout(Some(cfg.io_timeout));
                let _ = stream.set_write_timeout(Some(cfg.io_timeout));
                self.lock().stats.opened += 1;
                if let Some(ceiling) = cfg.fd_ceiling {
                    if let Some(n) = process_fd_count() {
                        debug_assert!(n <= ceiling, "process FD count {n} above ceiling {ceiling}");
                    }
                }
                Ok(self.make_conn(key, stream, false, None))
            }
            Err(e) => {
                let mut g = self.lock();
                g.stats.connect_failures += 1;
                release_slot(&mut g, &key);
                drop(g);
                self.shared.cv.notify_all();
                Err(PoolError::Connect(e))
            }
        }
    }

    fn make_conn(
        &self,
        key: String,
        stream: TcpStream,
        reused: bool,
        session: Option<Session>,
    ) -> Conn {
        Conn {
            session,
            pool: Arc::downgrade(&self.shared),
            key,
            stream: Some(stream),
            started: Instant::now(),
            total_timeout: self.shared.config.total_timeout,
            io_timeout: self.shared.config.io_timeout,
            reused,
        }
    }

    /// Close every idle socket older than `idle_timeout`.
    pub fn reap_idle(&self) -> usize {
        let cfg = self.shared.config;
        let now = Instant::now();
        let mut g = self.lock();
        let keys: Vec<String> = g.slots.keys().cloned().collect();
        let n: usize = keys
            .iter()
            .map(|k| reap_slot(&mut g, k, cfg.idle_timeout, now))
            .sum();
        drop(g);
        if n > 0 {
            self.shared.cv.notify_all();
        }
        n
    }

    /// Close every idle socket now (e.g. after the proxy restarted, so
    /// stale tunnels are not reused).
    pub fn close_idle(&self) -> usize {
        let mut g = self.lock();
        let mut n = 0;
        while evict_one_idle(&mut g) {
            n += 1;
        }
        drop(g);
        self.shared.cv.notify_all();
        n
    }

    /// Spawn a thread that reaps idle sockets every `interval` until the
    /// handle is dropped.
    pub fn start_reaper(&self, interval: Duration) -> Reaper {
        let weak: Weak<Shared> = Arc::downgrade(&self.shared);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::Builder::new()
            .name("pool-reaper".into())
            .spawn(move || {
                while !stop2.load(Ordering::Relaxed) {
                    std::thread::park_timeout(interval);
                    match weak.upgrade() {
                        Some(shared) => {
                            Pool { shared }.reap_idle();
                        }
                        None => break,
                    }
                }
            })
            .expect("spawn reaper");
        Reaper {
            stop,
            handle: Some(handle),
        }
    }

    pub fn stats(&self) -> PoolStats {
        self.lock().stats
    }

    /// Open sockets (in use + idle) for one origin.
    pub fn open_for(&self, origin: &Origin) -> usize {
        self.lock()
            .slots
            .get(&origin.key())
            .map(|s| s.open)
            .unwrap_or(0)
    }
}

fn release_slot(g: &mut Inner, key: &str) {
    if let Some(slot) = g.slots.get_mut(key) {
        slot.open = slot.open.saturating_sub(1);
    }
    g.global_open = g.global_open.saturating_sub(1);
    g.stats.open_now = g.global_open;
}

fn reap_slot(g: &mut Inner, key: &str, idle_timeout: Duration, now: Instant) -> usize {
    let Some(slot) = g.slots.get_mut(key) else {
        return 0;
    };
    let before = slot.idle.len();
    slot.idle.retain(|i| {
        let keep = now.saturating_duration_since(i.since) < idle_timeout;
        if !keep {
            let _ = i.stream.shutdown(Shutdown::Both);
        }
        keep
    });
    let n = before - slot.idle.len();
    slot.open -= n;
    g.global_open -= n;
    g.stats.idle_reaped += n as u64;
    g.stats.destroyed += n as u64;
    g.stats.idle_now -= n;
    g.stats.open_now = g.global_open;
    n
}

/// Close the oldest idle socket of any origin to free a global slot.
fn evict_one_idle(g: &mut Inner) -> bool {
    let mut oldest: Option<(String, usize, Instant)> = None;
    for (k, s) in &g.slots {
        for (i, idle) in s.idle.iter().enumerate() {
            if oldest.as_ref().is_none_or(|o| idle.since < o.2) {
                oldest = Some((k.clone(), i, idle.since));
            }
        }
    }
    let Some((k, i, _)) = oldest else {
        return false;
    };
    let slot = g.slots.get_mut(&k).expect("slot exists");
    let idle = slot.idle.remove(i);
    let _ = idle.stream.shutdown(Shutdown::Both);
    slot.open -= 1;
    g.global_open -= 1;
    g.stats.idle_evicted += 1;
    g.stats.destroyed += 1;
    g.stats.idle_now -= 1;
    g.stats.open_now = g.global_open;
    true
}

/// Stops the reaper thread when dropped.
pub struct Reaper {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for Reaper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            h.thread().unpark();
            let _ = h.join();
        }
    }
}

/// A checked-out connection. Dropping it destroys the socket; call
/// [`Conn::release`] to return a healthy one to the pool.
pub struct Conn {
    pool: Weak<Shared>,
    key: String,
    stream: Option<TcpStream>,
    started: Instant,
    total_timeout: Duration,
    io_timeout: Duration,
    reused: bool,
    session: Option<Session>,
}

impl Conn {
    /// Take the state stored with this socket by [`Conn::set_session`].
    pub fn take_session(&mut self) -> Option<Session> {
        self.session.take()
    }

    /// Keep `session` with the socket; it comes back on reuse and is
    /// dropped with the socket on discard.
    pub fn set_session(&mut self, session: Session) {
        self.session = Some(session);
    }

    pub fn stream(&mut self) -> &mut TcpStream {
        self.stream
            .as_mut()
            .expect("stream present until release/discard")
    }

    /// True when this socket came from the idle list.
    pub fn reused(&self) -> bool {
        self.reused
    }

    /// Time left before the total-request deadline.
    pub fn remaining(&self) -> Duration {
        self.total_timeout.saturating_sub(self.started.elapsed())
    }

    /// Tighten the socket's read/write timeouts to the remaining total
    /// budget; `TimedOut` if the budget is spent.
    pub fn arm_deadline(&mut self) -> io::Result<()> {
        let rem = self.remaining();
        if rem.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "total request timeout",
            ));
        }
        let t = std::cmp::min(rem, self.io_timeout);
        let s = self.stream();
        s.set_read_timeout(Some(t))?;
        s.set_write_timeout(Some(t))?;
        Ok(())
    }

    /// Return a healthy connection to the idle list. One that overran
    /// its total deadline is destroyed instead.
    pub fn release(mut self) {
        let Some(stream) = self.stream.take() else {
            return;
        };
        let Some(shared) = self.pool.upgrade() else {
            return;
        };
        if self.started.elapsed() > self.total_timeout {
            destroy(&shared, &self.key, stream);
            return;
        }
        let _ = stream.set_read_timeout(Some(shared.config.io_timeout));
        let _ = stream.set_write_timeout(Some(shared.config.io_timeout));
        let mut g = shared.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.stats.returned += 1;
        g.stats.idle_now += 1;
        g.slots
            .entry(self.key.clone())
            .or_default()
            .idle
            .push(Idle {
                stream,
                since: Instant::now(),
                session: self.session.take(),
            });
        drop(g);
        shared.cv.notify_all();
    }

    /// Destroy the socket (error paths: RST, ECONNREFUSED, timeout).
    pub fn discard(mut self) {
        self.destroy_now();
    }

    fn destroy_now(&mut self) {
        if let Some(stream) = self.stream.take() {
            match self.pool.upgrade() {
                Some(shared) => destroy(&shared, &self.key, stream),
                None => {
                    let _ = stream.shutdown(Shutdown::Both);
                }
            }
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.destroy_now();
    }
}

fn destroy(shared: &Shared, key: &str, stream: TcpStream) {
    let _ = stream.shutdown(Shutdown::Both);
    drop(stream);
    let mut g = shared.inner.lock().unwrap_or_else(|e| e.into_inner());
    g.stats.destroyed += 1;
    release_slot(&mut g, key);
    drop(g);
    shared.cv.notify_all();
}

/// Open FDs of this process (Linux `/proc/self/fd`); `None` elsewhere.
pub fn process_fd_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::atomic::AtomicUsize;

    /// Loopback listener that accepts and holds connections, counting
    /// how many are open at once.
    struct Sink {
        addr: Origin,
        _listener: Arc<TcpListener>,
    }

    fn sink() -> Sink {
        let l = Arc::new(TcpListener::bind("127.0.0.1:0").unwrap());
        let port = l.local_addr().unwrap().port();
        let l2 = l.clone();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for s in l2.incoming() {
                match s {
                    Ok(s) => held.push(s),
                    Err(_) => break,
                }
            }
        });
        Sink {
            addr: Origin::new("127.0.0.1", port),
            _listener: l,
        }
    }

    fn cfg(per: usize, global: usize) -> PoolConfig {
        PoolConfig {
            max_per_origin: per,
            global_max: global,
            connect_timeout: Duration::from_millis(500),
            io_timeout: Duration::from_millis(500),
            total_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_millis(200),
            max_queue_per_origin: 4,
            queue_timeout: Duration::from_millis(300),
            fd_ceiling: None,
        }
    }

    fn pool(c: PoolConfig) -> Pool {
        Pool::new(c, Arc::new(DirectConnector)).unwrap()
    }

    #[test]
    fn origin_parse_and_display() {
        assert_eq!(
            Origin::parse("api2.cursor.sh:443"),
            Some(Origin::new("api2.cursor.sh", 443))
        );
        assert_eq!(Origin::parse("[::1]:80").unwrap().to_string(), "[::1]:80");
        assert_eq!(Origin::parse("nope"), None);
    }

    #[test]
    fn config_rejects_caps_above_spec() {
        assert!(PoolConfig {
            max_per_origin: 11,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(PoolConfig {
            max_per_origin: 0,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(PoolConfig {
            max_per_origin: 8,
            global_max: 4,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(PoolConfig::default().validate().is_ok());
    }

    #[test]
    fn per_origin_cap_is_never_exceeded_under_concurrency() {
        let s = sink();
        let p = pool(PoolConfig {
            max_queue_per_origin: 64,
            queue_timeout: Duration::from_secs(5),
            ..cfg(3, 10)
        });
        let peak = Arc::new(AtomicUsize::new(0));
        let mut hs = Vec::new();
        for _ in 0..24 {
            let p = p.clone();
            let o = s.addr.clone();
            let peak = peak.clone();
            hs.push(std::thread::spawn(move || {
                let c = p.checkout(&o, Priority::Interactive).expect("checkout");
                peak.fetch_max(p.open_for(&o), Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
                c.release();
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        assert!(peak.load(Ordering::SeqCst) <= 3);
        assert_eq!(p.stats().peak_per_origin, 3);
        assert!(
            p.stats().opened <= 3,
            "opened {} sockets, cap 3",
            p.stats().opened
        );
    }

    #[test]
    fn global_cap_spans_origins() {
        let a = sink();
        let b = sink();
        let p = pool(cfg(3, 4));
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(p.checkout(&a.addr, Priority::Interactive).unwrap());
        }
        held.push(p.checkout(&b.addr, Priority::Interactive).unwrap());
        let t = Instant::now();
        assert!(matches!(
            p.checkout(&b.addr, Priority::Interactive),
            Err(PoolError::QueueTimeout)
        ));
        assert!(t.elapsed() >= Duration::from_millis(250));
        assert_eq!(p.stats().peak_global, 4);
    }

    #[test]
    fn idle_sockets_are_evicted_to_serve_another_origin() {
        let a = sink();
        let b = sink();
        let p = pool(cfg(2, 2));
        p.checkout(&a.addr, Priority::Interactive)
            .unwrap()
            .release();
        p.checkout(&a.addr, Priority::Interactive)
            .unwrap()
            .release();
        assert_eq!(
            p.stats().open_now,
            1,
            "second checkout reuses the idle socket"
        );
        let c1 = p.checkout(&a.addr, Priority::Interactive).unwrap();
        let c2 = p.checkout(&a.addr, Priority::Interactive).unwrap();
        c1.release();
        c2.release();
        assert_eq!(p.stats().idle_now, 2);
        let _cb = p.checkout(&b.addr, Priority::Interactive).unwrap();
        assert_eq!(p.stats().idle_evicted, 1);
        assert_eq!(p.stats().open_now, 2);
    }

    #[test]
    fn errored_connections_are_destroyed_not_pooled() {
        let s = sink();
        let p = pool(cfg(2, 4));
        let c = p.checkout(&s.addr, Priority::Interactive).unwrap();
        c.discard();
        let st = p.stats();
        assert_eq!((st.open_now, st.idle_now, st.destroyed), (0, 0, 1));
        // Dropping without release also destroys.
        drop(p.checkout(&s.addr, Priority::Interactive).unwrap());
        assert_eq!(p.stats().destroyed, 2);
        assert_eq!(p.stats().reused, 0);
    }

    #[test]
    fn connection_over_total_deadline_is_not_returned() {
        let s = sink();
        let p = pool(PoolConfig {
            total_timeout: Duration::from_millis(50),
            ..cfg(2, 4)
        });
        let mut c = p.checkout(&s.addr, Priority::Interactive).unwrap();
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(
            c.arm_deadline().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        c.release();
        assert_eq!(p.stats().idle_now, 0);
        assert_eq!(p.stats().open_now, 0);
    }

    #[test]
    fn idle_sockets_are_reaped() {
        let s = sink();
        let p = pool(cfg(2, 4));
        p.checkout(&s.addr, Priority::Interactive)
            .unwrap()
            .release();
        assert_eq!(p.stats().idle_now, 1);
        let _r = p.start_reaper(Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(400));
        let st = p.stats();
        assert_eq!((st.idle_now, st.open_now, st.idle_reaped), (0, 0, 1));
    }

    #[test]
    fn refused_connect_releases_its_slot() {
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let o = Origin::new("127.0.0.1", port);
        let p = pool(cfg(2, 4));
        for _ in 0..10 {
            match p.checkout(&o, Priority::Interactive) {
                Err(PoolError::Connect(e)) => {
                    assert_eq!(e.kind(), io::ErrorKind::ConnectionRefused)
                }
                other => panic!("expected refused, got {:?}", other.map(|_| ())),
            }
        }
        assert_eq!(p.stats().open_now, 0);
        assert_eq!(p.stats().connect_failures, 10);
    }

    #[test]
    fn background_is_shed_before_interactive_when_queue_is_full() {
        let s = sink();
        let p = pool(PoolConfig {
            max_queue_per_origin: 2,
            queue_timeout: Duration::from_secs(2),
            ..cfg(1, 4)
        });
        let held = p.checkout(&s.addr, Priority::Interactive).unwrap();
        // Two background waiters fill the queue.
        let mut bg = Vec::new();
        for _ in 0..2 {
            let p = p.clone();
            let o = s.addr.clone();
            bg.push(std::thread::spawn(move || {
                p.checkout(&o, Priority::Background).map(|c| c.release())
            }));
        }
        std::thread::sleep(Duration::from_millis(100));
        // A third background request is refused immediately.
        assert!(matches!(
            p.checkout(&s.addr, Priority::Background),
            Err(PoolError::QueueFull)
        ));
        // Interactive requests still queue, and are served first.
        let pi = p.clone();
        let o = s.addr.clone();
        let inter = std::thread::spawn(move || {
            let t = Instant::now();
            pi.checkout(&o, Priority::Interactive).map(|c| {
                c.release();
                t.elapsed()
            })
        });
        std::thread::sleep(Duration::from_millis(100));
        held.release();
        let waited = inter.join().unwrap().expect("interactive served");
        assert!(waited < Duration::from_secs(1));
        let results: Vec<_> = bg.into_iter().map(|h| h.join().unwrap()).collect();
        for r in results {
            assert!(matches!(
                r,
                Ok(()) | Err(PoolError::QueueFull) | Err(PoolError::QueueTimeout)
            ));
        }
    }
}
