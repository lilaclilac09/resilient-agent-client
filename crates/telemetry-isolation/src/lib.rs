//! Queue and traffic isolation for telemetry vs. interactive RPC
//! (SPEC §4.4).
//!
//! - Interactive work runs on its own worker threads fed by its own
//!   channel; it never shares a queue or a thread with background work.
//! - Background work (health checks, polling, telemetry flushes) sits in
//!   a bounded queue that sheds telemetry first, then polling, then
//!   health checks.
//! - The background queue is paused while any circuit breaker is Open or
//!   Half-Open, while the client is marked offline / proxy-unreachable,
//!   or while the global background switch is off. It resumes only once
//!   every breaker is Closed again, i.e. after a Half-Open probe succeeds.
//! - Telemetry events are batched and flushed on a fixed interval.
//! - Health checks and polling are rate-limited with a documented floor.
//! - Telemetry opt-out is honoured at startup and at runtime.

use circuit_breaker::Registry;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Smallest health-check / polling interval accepted (SPEC §4.4 floor).
pub const BACKGROUND_INTERVAL_FLOOR: Duration = Duration::from_secs(10);

/// Runtime switches. All default to the safe, spec-compliant setting.
#[derive(Debug)]
pub struct Controls {
    telemetry_enabled: AtomicBool,
    background_enabled: AtomicBool,
    offline: AtomicBool,
}

impl Controls {
    /// `telemetry_enabled` is the startup opt-out value.
    pub fn new(telemetry_enabled: bool) -> Self {
        Self {
            telemetry_enabled: AtomicBool::new(telemetry_enabled),
            background_enabled: AtomicBool::new(true),
            offline: AtomicBool::new(false),
        }
    }

    /// Telemetry opt-out at runtime. Disabling also drops buffered events.
    pub fn set_telemetry_enabled(&self, on: bool) {
        self.telemetry_enabled.store(on, Ordering::SeqCst)
    }
    pub fn telemetry_enabled(&self) -> bool {
        self.telemetry_enabled.load(Ordering::SeqCst)
    }
    /// The single switch that disables all background polling.
    pub fn set_background_enabled(&self, on: bool) {
        self.background_enabled.store(on, Ordering::SeqCst)
    }
    pub fn background_enabled(&self) -> bool {
        self.background_enabled.load(Ordering::SeqCst)
    }
    /// Mark offline / loopback-proxy-unreachable.
    pub fn set_offline(&self, offline: bool) {
        self.offline.store(offline, Ordering::SeqCst)
    }
    pub fn offline(&self) -> bool {
        self.offline.load(Ordering::SeqCst)
    }
}

impl Default for Controls {
    fn default() -> Self {
        Self::new(true)
    }
}

/// Decides whether background work may run right now.
#[derive(Clone)]
pub struct Gate {
    pub controls: Arc<Controls>,
    pub breakers: Arc<Registry>,
}

impl Gate {
    pub fn background_allowed(&self) -> bool {
        self.controls.background_enabled() && !self.controls.offline() && self.breakers.all_closed()
    }
}

/// Background work kinds, in shedding order (telemetry first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BackgroundKind {
    Telemetry = 0,
    Polling = 1,
    HealthCheck = 2,
}

pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// Bounded queue that evicts the lowest-priority, oldest job on overflow.
pub struct BackgroundQueue {
    capacity: usize,
    jobs: VecDeque<(BackgroundKind, Option<String>, Job)>,
    dropped: u64,
}

impl BackgroundQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            jobs: VecDeque::new(),
            dropped: 0,
        }
    }

    /// Enqueue; returns false when the job itself was dropped.
    pub fn push(&mut self, kind: BackgroundKind, job: Job) -> bool {
        self.push_for(kind, None, job)
    }

    /// Enqueue a job that talks to `origin`, so it can serve as that
    /// origin's cooldown probe.
    pub fn push_for(&mut self, kind: BackgroundKind, origin: Option<String>, job: Job) -> bool {
        if self.jobs.len() >= self.capacity {
            let victim = self
                .jobs
                .iter()
                .enumerate()
                .min_by_key(|(i, (k, _, _))| (*k, *i))
                .map(|(i, (k, _, _))| (i, *k));
            match victim {
                Some((i, k)) if k <= kind => {
                    self.jobs.remove(i);
                    self.dropped += 1;
                }
                _ => {
                    self.dropped += 1;
                    return false;
                }
            }
        }
        self.jobs.push_back((kind, origin, job));
        true
    }

    pub fn pop(&mut self) -> Option<(BackgroundKind, Job)> {
        self.jobs.pop_front().map(|(k, _, j)| (k, j))
    }

    /// Remove the oldest origin-tagged job matching `pred`.
    pub fn pop_for(
        &mut self,
        pred: impl Fn(BackgroundKind, &str) -> bool,
    ) -> Option<(BackgroundKind, String, Job)> {
        let i = self
            .jobs
            .iter()
            .position(|(k, o, _)| o.as_deref().is_some_and(|o| pred(*k, o)))?;
        let (k, o, j) = self.jobs.remove(i)?;
        Some((k, o.expect("tagged"), j))
    }
    pub fn len(&self) -> usize {
        self.jobs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

/// Fixed-interval limiter with a floor.
#[derive(Debug, Clone)]
pub struct IntervalLimiter {
    interval: Duration,
    last: Option<Instant>,
}

impl IntervalLimiter {
    /// `interval` is raised to `floor` if below it.
    pub fn new(interval: Duration, floor: Duration) -> Self {
        Self {
            interval: interval.max(floor),
            last: None,
        }
    }
    pub fn interval(&self) -> Duration {
        self.interval
    }
    pub fn try_acquire(&mut self, now: Instant) -> bool {
        match self.last {
            Some(t) if now.saturating_duration_since(t) < self.interval => false,
            _ => {
                self.last = Some(now);
                true
            }
        }
    }
}

/// Dispatcher configuration. Defaults follow SPEC §4.4.
#[derive(Debug, Clone, Copy)]
pub struct DispatcherConfig {
    pub interactive_workers: usize,
    pub background_capacity: usize,
    pub telemetry_flush_interval: Duration,
    /// Buffered telemetry events kept between flushes (oldest dropped).
    pub telemetry_buffer: usize,
    /// How often the paused background worker re-checks the gate.
    pub gate_poll: Duration,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            interactive_workers: 4,
            background_capacity: 256,
            telemetry_flush_interval: Duration::from_secs(60),
            telemetry_buffer: 1000,
            gate_poll: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejected {
    /// Background switch is off.
    BackgroundDisabled,
    /// Telemetry opted out.
    TelemetryDisabled,
    /// Queue full of equal-or-higher priority work.
    QueueFull,
    /// Dispatcher shut down.
    ShutDown,
}

/// Dispatcher counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DispatcherStats {
    pub interactive_run: u64,
    pub background_run: u64,
    pub background_dropped: u64,
    pub telemetry_batches: u64,
    pub telemetry_events_flushed: u64,
    pub telemetry_events_dropped: u64,
    /// Times the background worker found the gate closed with work waiting.
    pub paused_checks: u64,
    /// Origin-tagged jobs run as a cooldown probe while paused.
    pub background_probes: u64,
}

type TelemetrySink = Arc<dyn Fn(Vec<String>) + Send + Sync>;

struct BgState {
    queue: BackgroundQueue,
    telemetry: VecDeque<String>,
    last_flush: Instant,
    shutdown: bool,
    /// Cooldown epoch already probed, per origin.
    probed: std::collections::HashMap<String, u64>,
}

struct BgShared {
    state: Mutex<BgState>,
    cv: Condvar,
    background_run: AtomicU64,
    telemetry_batches: AtomicU64,
    telemetry_flushed: AtomicU64,
    telemetry_dropped: AtomicU64,
    paused_checks: AtomicU64,
    probes: AtomicU64,
}

/// Runs interactive and background work on isolated workers.
pub struct Dispatcher {
    config: DispatcherConfig,
    gate: Gate,
    interactive_tx: Mutex<Option<mpsc::Sender<Job>>>,
    interactive_run: Arc<AtomicU64>,
    bg: Arc<BgShared>,
    threads: Vec<JoinHandle<()>>,
}

impl Dispatcher {
    /// `sink` receives each telemetry batch; it runs on the background
    /// worker, so it is paused with the rest of background work.
    pub fn new(
        config: DispatcherConfig,
        gate: Gate,
        sink: impl Fn(Vec<String>) + Send + Sync + 'static,
    ) -> Self {
        let sink: TelemetrySink = Arc::new(sink);
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let interactive_run = Arc::new(AtomicU64::new(0));
        let mut threads = Vec::new();
        for i in 0..config.interactive_workers.max(1) {
            let rx = rx.clone();
            let ran = interactive_run.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("interactive-{i}"))
                    .spawn(move || loop {
                        let job = { rx.lock().unwrap_or_else(|e| e.into_inner()).recv() };
                        match job {
                            Ok(job) => {
                                job();
                                ran.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => break,
                        }
                    })
                    .expect("spawn interactive worker"),
            );
        }
        let bg = Arc::new(BgShared {
            state: Mutex::new(BgState {
                queue: BackgroundQueue::new(config.background_capacity),
                telemetry: VecDeque::new(),
                last_flush: Instant::now(),
                shutdown: false,
                probed: Default::default(),
            }),
            cv: Condvar::new(),
            background_run: AtomicU64::new(0),
            telemetry_batches: AtomicU64::new(0),
            telemetry_flushed: AtomicU64::new(0),
            telemetry_dropped: AtomicU64::new(0),
            paused_checks: AtomicU64::new(0),
            probes: AtomicU64::new(0),
        });
        {
            let bg = bg.clone();
            let gate = gate.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("background".into())
                    .spawn(move || background_loop(config, gate, bg, sink))
                    .expect("spawn background worker"),
            );
        }
        Self {
            config,
            gate,
            interactive_tx: Mutex::new(Some(tx)),
            interactive_run,
            bg,
            threads,
        }
    }

    pub fn gate(&self) -> &Gate {
        &self.gate
    }

    /// Run user-interactive work. Never queued behind background work.
    pub fn submit_interactive(&self, job: impl FnOnce() + Send + 'static) -> Result<(), Rejected> {
        let g = self
            .interactive_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match g.as_ref() {
            Some(tx) => tx.send(Box::new(job)).map_err(|_| Rejected::ShutDown),
            None => Err(Rejected::ShutDown),
        }
    }

    /// Queue background work. Refused outright while the background
    /// switch is off; otherwise queued (and held while the gate is
    /// closed).
    pub fn submit_background(
        &self,
        kind: BackgroundKind,
        job: impl FnOnce() + Send + 'static,
    ) -> Result<(), Rejected> {
        self.submit(kind, None, Box::new(job))
    }

    /// Like [`Dispatcher::submit_background`] for a job whose requests go
    /// to `origin`. While paused, it may run as that origin's single
    /// cooldown probe (see SPEC §6.1), which is how a background-only
    /// origin's breaker gets back to Closed.
    pub fn submit_background_for(
        &self,
        kind: BackgroundKind,
        origin: &str,
        job: impl FnOnce() + Send + 'static,
    ) -> Result<(), Rejected> {
        self.submit(kind, Some(origin.to_string()), Box::new(job))
    }

    fn submit(
        &self,
        kind: BackgroundKind,
        origin: Option<String>,
        job: Job,
    ) -> Result<(), Rejected> {
        if !self.gate.controls.background_enabled() {
            return Err(Rejected::BackgroundDisabled);
        }
        if kind == BackgroundKind::Telemetry && !self.gate.controls.telemetry_enabled() {
            return Err(Rejected::TelemetryDisabled);
        }
        let mut st = self.bg.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.shutdown {
            return Err(Rejected::ShutDown);
        }
        let ok = st.queue.push_for(kind, origin, job);
        drop(st);
        self.bg.cv.notify_all();
        if ok {
            Ok(())
        } else {
            Err(Rejected::QueueFull)
        }
    }

    /// Buffer one telemetry event for the next batch flush.
    pub fn record_telemetry(&self, event: impl Into<String>) -> Result<(), Rejected> {
        if !self.gate.controls.telemetry_enabled() {
            return Err(Rejected::TelemetryDisabled);
        }
        if !self.gate.controls.background_enabled() {
            return Err(Rejected::BackgroundDisabled);
        }
        let mut st = self.bg.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.telemetry.len() >= self.config.telemetry_buffer {
            st.telemetry.pop_front();
            self.bg.telemetry_dropped.fetch_add(1, Ordering::Relaxed);
        }
        st.telemetry.push_back(event.into());
        Ok(())
    }

    /// Background jobs waiting (paused or not yet run).
    pub fn background_pending(&self) -> usize {
        self.bg
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queue
            .len()
    }

    pub fn stats(&self) -> DispatcherStats {
        let st = self.bg.state.lock().unwrap_or_else(|e| e.into_inner());
        DispatcherStats {
            interactive_run: self.interactive_run.load(Ordering::Relaxed),
            background_run: self.bg.background_run.load(Ordering::Relaxed),
            background_dropped: st.queue.dropped(),
            telemetry_batches: self.bg.telemetry_batches.load(Ordering::Relaxed),
            telemetry_events_flushed: self.bg.telemetry_flushed.load(Ordering::Relaxed),
            telemetry_events_dropped: self.bg.telemetry_dropped.load(Ordering::Relaxed),
            paused_checks: self.bg.paused_checks.load(Ordering::Relaxed),
            background_probes: self.bg.probes.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.interactive_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        self.bg
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .shutdown = true;
        self.bg.cv.notify_all();
        for h in self.threads.drain(..) {
            let _ = h.join();
        }
    }
}

/// While paused: the oldest queued job tagged with an origin whose
/// breaker is Half-Open with a free probe slot, at most once per cooldown
/// epoch. Never while offline or background is off, never opted-out
/// telemetry.
fn take_probe(gate: &Gate, st: &mut BgState) -> Option<(String, u64, Job)> {
    if !gate.controls.background_enabled() || gate.controls.offline() {
        return None;
    }
    let fresh: Vec<(String, u64)> = gate
        .breakers
        .probe_candidates()
        .into_iter()
        .filter(|(o, e)| st.probed.get(o) != Some(e))
        .collect();
    if fresh.is_empty() {
        return None;
    }
    let telemetry_on = gate.controls.telemetry_enabled();
    let (_, origin, job) = st.queue.pop_for(|k, o| {
        (k != BackgroundKind::Telemetry || telemetry_on) && fresh.iter().any(|(f, _)| f == o)
    })?;
    let epoch = fresh.iter().find(|(f, _)| *f == origin)?.1;
    Some((origin, epoch, job))
}

fn background_loop(config: DispatcherConfig, gate: Gate, bg: Arc<BgShared>, sink: TelemetrySink) {
    let mut st = bg.state.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        if st.shutdown {
            return;
        }
        if !gate.controls.telemetry_enabled() && !st.telemetry.is_empty() {
            let n = st.telemetry.len() as u64;
            st.telemetry.clear();
            bg.telemetry_dropped.fetch_add(n, Ordering::Relaxed);
        }
        let has_work = !st.queue.is_empty() || !st.telemetry.is_empty();
        if has_work && !gate.background_allowed() {
            if let Some((origin, epoch, job)) = take_probe(&gate, &mut st) {
                st.probed.insert(origin, epoch);
                drop(st);
                job();
                bg.probes.fetch_add(1, Ordering::Relaxed);
                bg.background_run.fetch_add(1, Ordering::Relaxed);
                st = bg.state.lock().unwrap_or_else(|e| e.into_inner());
                continue;
            }
            bg.paused_checks.fetch_add(1, Ordering::Relaxed);
            st = bg
                .cv
                .wait_timeout(st, config.gate_poll)
                .unwrap_or_else(|e| e.into_inner())
                .0;
            continue;
        }
        if let Some((kind, job)) = st.queue.pop() {
            if kind == BackgroundKind::Telemetry && !gate.controls.telemetry_enabled() {
                bg.telemetry_dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            drop(st);
            job();
            bg.background_run.fetch_add(1, Ordering::Relaxed);
            st = bg.state.lock().unwrap_or_else(|e| e.into_inner());
            continue;
        }
        let due = st.last_flush.elapsed() >= config.telemetry_flush_interval;
        if due && !st.telemetry.is_empty() {
            let batch: Vec<String> = st.telemetry.drain(..).collect();
            st.last_flush = Instant::now();
            drop(st);
            let n = batch.len() as u64;
            sink(batch);
            bg.telemetry_batches.fetch_add(1, Ordering::Relaxed);
            bg.telemetry_flushed.fetch_add(n, Ordering::Relaxed);
            st = bg.state.lock().unwrap_or_else(|e| e.into_inner());
            continue;
        }
        if due {
            st.last_flush = Instant::now();
        }
        let wait = config
            .telemetry_flush_interval
            .saturating_sub(st.last_flush.elapsed())
            .min(config.gate_poll)
            .max(Duration::from_millis(1));
        st = bg
            .cv
            .wait_timeout(st, wait)
            .unwrap_or_else(|e| e.into_inner())
            .0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use circuit_breaker::{Config, Outcome, State};
    use std::sync::atomic::AtomicUsize;

    fn registry() -> Arc<Registry> {
        Arc::new(Registry::new(Config {
            failure_threshold: 5,
            open_hold_min: Duration::from_millis(100),
            open_hold_max: Duration::from_millis(150),
            max_hold: Duration::from_secs(2),
            probe_timeout: Duration::from_secs(1),
        }))
    }

    fn fast() -> DispatcherConfig {
        DispatcherConfig {
            interactive_workers: 2,
            background_capacity: 8,
            telemetry_flush_interval: Duration::from_millis(100),
            telemetry_buffer: 5,
            gate_poll: Duration::from_millis(10),
        }
    }

    fn wait_until(mut f: impl FnMut() -> bool, max: Duration) -> bool {
        let t = Instant::now();
        while t.elapsed() < max {
            if f() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        f()
    }

    #[test]
    fn queue_sheds_telemetry_before_health_checks() {
        let mut q = BackgroundQueue::new(2);
        assert!(q.push(BackgroundKind::HealthCheck, Box::new(|| {})));
        assert!(q.push(BackgroundKind::Telemetry, Box::new(|| {})));
        assert!(
            q.push(BackgroundKind::Polling, Box::new(|| {})),
            "evicts telemetry"
        );
        assert!(
            !q.push(BackgroundKind::Telemetry, Box::new(|| {})),
            "telemetry dropped"
        );
        let kinds: Vec<_> = std::iter::from_fn(|| q.pop().map(|(k, _)| k)).collect();
        assert_eq!(
            kinds,
            vec![BackgroundKind::HealthCheck, BackgroundKind::Polling]
        );
        assert_eq!(q.dropped(), 2);
    }

    #[test]
    fn interval_limiter_enforces_floor() {
        let mut l = IntervalLimiter::new(Duration::from_secs(1), BACKGROUND_INTERVAL_FLOOR);
        assert_eq!(l.interval(), BACKGROUND_INTERVAL_FLOOR);
        let t = Instant::now();
        assert!(l.try_acquire(t));
        assert!(!l.try_acquire(t + Duration::from_secs(9)));
        assert!(l.try_acquire(t + Duration::from_secs(10)));
    }

    #[test]
    fn interactive_never_waits_behind_blocked_background_work() {
        let gate = Gate {
            controls: Arc::new(Controls::default()),
            breakers: registry(),
        };
        let d = Dispatcher::new(fast(), gate, |_| {});
        let (release_tx, release_rx) = mpsc::channel::<()>();
        d.submit_background(BackgroundKind::Polling, move || {
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let (tx, rx) = mpsc::channel();
        let t = Instant::now();
        d.submit_interactive(move || tx.send(()).unwrap()).unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("interactive ran");
        assert!(t.elapsed() < Duration::from_millis(200));
        release_tx.send(()).unwrap();
    }

    #[test]
    fn background_pauses_while_breaker_open_and_resumes_after_probe_success() {
        let reg = registry();
        let gate = Gate {
            controls: Arc::new(Controls::default()),
            breakers: reg.clone(),
        };
        let d = Dispatcher::new(fast(), gate, |_| {});
        for _ in 0..5 {
            reg.record("api", Outcome::NetworkError);
        }
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        d.submit_background(BackgroundKind::HealthCheck, move || {
            r.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(250));
        assert_eq!(ran.load(Ordering::SeqCst), 0, "ran while Open/Half-Open");
        assert_eq!(d.background_pending(), 1);
        assert!(d.stats().paused_checks > 0);
        // Hold expired: Half-Open. Still paused until the probe succeeds.
        assert!(reg.allow_request("api"));
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
        reg.record("api", Outcome::Success);
        assert!(wait_until(
            || ran.load(Ordering::SeqCst) == 1,
            Duration::from_secs(1)
        ));
    }

    #[test]
    fn offline_flag_pauses_background() {
        let controls = Arc::new(Controls::default());
        let gate = Gate {
            controls: controls.clone(),
            breakers: registry(),
        };
        let d = Dispatcher::new(fast(), gate, |_| {});
        controls.set_offline(true);
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        d.submit_background(BackgroundKind::Polling, move || {
            r.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
        controls.set_offline(false);
        assert!(wait_until(
            || ran.load(Ordering::SeqCst) == 1,
            Duration::from_secs(1)
        ));
    }

    #[test]
    fn background_switch_and_telemetry_opt_out() {
        let controls = Arc::new(Controls::new(false));
        let gate = Gate {
            controls: controls.clone(),
            breakers: registry(),
        };
        let d = Dispatcher::new(fast(), gate, |_| {});
        assert_eq!(d.record_telemetry("e"), Err(Rejected::TelemetryDisabled));
        assert_eq!(
            d.submit_background(BackgroundKind::Telemetry, || {}),
            Err(Rejected::TelemetryDisabled)
        );
        assert!(d
            .submit_background(BackgroundKind::HealthCheck, || {})
            .is_ok());
        controls.set_background_enabled(false);
        assert_eq!(
            d.submit_background(BackgroundKind::HealthCheck, || {}),
            Err(Rejected::BackgroundDisabled)
        );
        assert!(
            d.submit_interactive(|| {}).is_ok(),
            "interactive unaffected"
        );
    }

    #[test]
    fn telemetry_is_batched_on_interval_and_dropped_on_runtime_opt_out() {
        let batches = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
        let b = batches.clone();
        let controls = Arc::new(Controls::default());
        let gate = Gate {
            controls: controls.clone(),
            breakers: registry(),
        };
        let d = Dispatcher::new(fast(), gate, move |batch| b.lock().unwrap().push(batch));
        for i in 0..7 {
            d.record_telemetry(format!("e{i}")).unwrap();
        }
        assert!(wait_until(
            || !batches.lock().unwrap().is_empty(),
            Duration::from_secs(1)
        ));
        {
            let got = batches.lock().unwrap();
            assert_eq!(got.len(), 1, "one batch, not one flush per event");
            assert_eq!(
                got[0],
                vec!["e2", "e3", "e4", "e5", "e6"],
                "buffer keeps newest 5"
            );
        }
        assert_eq!(d.stats().telemetry_events_dropped, 2);
        d.record_telemetry("late").unwrap();
        controls.set_telemetry_enabled(false);
        std::thread::sleep(Duration::from_millis(250));
        assert_eq!(batches.lock().unwrap().len(), 1, "opt-out flushed nothing");
    }

    fn tripped(reg: &Registry, origin: &str) {
        for _ in 0..5 {
            assert!(reg.allow_request(origin));
            reg.record(origin, Outcome::NetworkError);
        }
        assert_eq!(reg.state(origin), State::Open);
    }

    fn probing_job(
        reg: &Arc<Registry>,
        ran: &Arc<AtomicUsize>,
        succeed: &Arc<AtomicBool>,
    ) -> impl FnOnce() + Send + 'static {
        let (reg, ran, succeed) = (reg.clone(), ran.clone(), succeed.clone());
        move || {
            ran.fetch_add(1, Ordering::SeqCst);
            if reg.allow_request("bg-only") {
                let ok = succeed.load(Ordering::SeqCst);
                reg.record(
                    "bg-only",
                    if ok {
                        Outcome::Success
                    } else {
                        Outcome::NetworkError
                    },
                );
            }
        }
    }

    fn fast_dispatcher(controls: &Arc<Controls>, reg: &Arc<Registry>) -> Dispatcher {
        Dispatcher::new(
            DispatcherConfig {
                gate_poll: Duration::from_millis(10),
                ..Default::default()
            },
            Gate {
                controls: controls.clone(),
                breakers: reg.clone(),
            },
            |_| {},
        )
    }

    #[test]
    fn background_only_origin_gets_one_probe_per_cooldown() {
        let reg = registry();
        let controls = Arc::new(Controls::default());
        let d = fast_dispatcher(&controls, &reg);
        tripped(&reg, "bg-only");
        let ran = Arc::new(AtomicUsize::new(0));
        let succeed = Arc::new(AtomicBool::new(false));
        for _ in 0..3 {
            d.submit_background_for(
                BackgroundKind::Polling,
                "bg-only",
                probing_job(&reg, &ran, &succeed),
            )
            .unwrap();
        }
        assert!(wait_until(
            || ran.load(Ordering::SeqCst) == 1,
            Duration::from_secs(2)
        ));
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(
            ran.load(Ordering::SeqCst),
            1,
            "more than one probe in one cooldown"
        );
        assert_eq!(reg.state("bg-only"), State::Open, "failed probe re-opens");
        succeed.store(true, Ordering::SeqCst);
        assert!(wait_until(
            || ran.load(Ordering::SeqCst) == 3,
            Duration::from_secs(3)
        ));
        assert_eq!(reg.state("bg-only"), State::Closed);
        assert_eq!(d.stats().background_probes, 2);
    }

    #[test]
    fn no_probe_while_offline_disabled_or_opted_out() {
        let reg = registry();
        let controls = Arc::new(Controls::default());
        let d = fast_dispatcher(&controls, &reg);
        tripped(&reg, "bg-only");
        let ran = Arc::new(AtomicUsize::new(0));
        let succeed = Arc::new(AtomicBool::new(true));
        d.submit_background_for(
            BackgroundKind::Telemetry,
            "bg-only",
            probing_job(&reg, &ran, &succeed),
        )
        .unwrap();
        controls.set_telemetry_enabled(false);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            ran.load(Ordering::SeqCst),
            0,
            "opted-out telemetry used as probe"
        );
        d.submit_background_for(
            BackgroundKind::Polling,
            "bg-only",
            probing_job(&reg, &ran, &succeed),
        )
        .unwrap();
        controls.set_offline(true);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(ran.load(Ordering::SeqCst), 0, "probed while offline");
        controls.set_offline(false);
        controls.set_background_enabled(false);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            ran.load(Ordering::SeqCst),
            0,
            "probed while background disabled"
        );
        assert_eq!(reg.state("bg-only"), State::HalfOpen);
        controls.set_background_enabled(true);
        assert!(wait_until(
            || ran.load(Ordering::SeqCst) == 1,
            Duration::from_secs(2)
        ));
        assert_eq!(d.stats().background_probes, 1);
        assert_eq!(reg.state("bg-only"), State::Closed);
    }
}
