//! Per-origin circuit breaker: Closed / Open / Half-Open.
//!
//! Trips to Open after 5 consecutive underlying network errors
//! (RST, `ECONNREFUSED`, timeout); HTTP 4xx does not count. While Open,
//! background work is rejected locally with zero socket cost; after a
//! jittered 30–60 s hold (doubled on each failed Half-Open probe, capped
//! at 5 minutes) a single probe is allowed. See `docs/SPEC.md` §4.3.
//!
//! No external dependencies: jitter uses a small xorshift PRNG seeded
//! from the system clock, so the crate builds offline.

use std::time::{Duration, Instant};

/// Configuration for [`CircuitBreaker`]. Defaults follow SPEC §4.3.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Consecutive counted failures that trip Closed -> Open.
    pub failure_threshold: u32,
    /// Lower bound of the jittered Open hold.
    pub open_hold_min: Duration,
    /// Upper bound of the jittered Open hold.
    pub open_hold_max: Duration,
    /// Absolute cap on the Open hold after repeated failed probes.
    pub max_hold: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            open_hold_min: Duration::from_secs(30),
            open_hold_max: Duration::from_secs(60),
            max_hold: Duration::from_secs(300),
        }
    }
}

impl Config {
    /// Short holds for tests and the trip harness. Same threshold and
    /// doubling rules as the spec defaults; only the clock is faster.
    pub fn test_config() -> Self {
        Self {
            failure_threshold: 5,
            open_hold_min: Duration::from_millis(2000),
            open_hold_max: Duration::from_millis(4000),
            max_hold: Duration::from_secs(30),
        }
    }
}

/// What a request produced, as the breaker sees it.
///
/// Callers classify: any response from the server (any HTTP status,
/// including 4xx) is [`Outcome::Success`] — the network path works.
/// Only transport-level failures count toward the trip threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The server responded (any HTTP status). Resets the consecutive
    /// failure count; in Half-Open it closes the breaker.
    Success,
    /// Underlying network error: TCP RST, `ECONNREFUSED`, or timeout.
    /// Counts toward the trip threshold.
    NetworkError,
    /// A failure that is neither a network error nor a server response
    /// (e.g. a local validation error). Never counts toward the trip
    /// threshold and never resets the failure count — except in
    /// Half-Open, where any outcome other than a network error proves
    /// the path works and closes the breaker.
    NonNetworkError,
}

/// Breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
}

/// One recorded state transition, with the reason it happened.
#[derive(Debug, Clone)]
pub struct Transition {
    pub from: State,
    pub to: State,
    pub at: Instant,
    pub reason: &'static str,
}

/// Per-origin circuit breaker (SPEC §4.3).
pub struct CircuitBreaker {
    config: Config,
    state: State,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    hold: Duration,
    probe_in_flight: bool,
    trips: u64,
    rejected: u64,
    events: Vec<Transition>,
    rng_state: u64,
}

impl CircuitBreaker {
    pub fn new(config: Config) -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9e3779b97f4a7c15)
            ^ (std::process::id() as u64).wrapping_mul(0x2545f4914f6cdd1d);
        let mut cb = Self {
            config,
            state: State::Closed,
            consecutive_failures: 0,
            opened_at: None,
            hold: Duration::from_secs(0),
            probe_in_flight: false,
            trips: 0,
            rejected: 0,
            events: Vec::new(),
            rng_state: seed | 1,
        };
        cb.hold = cb.jittered_hold(config.open_hold_min, config.open_hold_max);
        cb
    }

    /// Current state. Applies lazy expiry: an Open breaker whose hold
    /// has expired is reported (and recorded) as Half-Open.
    pub fn state(&mut self) -> State {
        self.refresh();
        self.state
    }

    /// Whether a request may proceed. Closed: yes. Open: no (the call
    /// is rejected locally — no socket is opened). Half-Open: exactly
    /// one probe is allowed; further calls are rejected until the probe
    /// reports via [`CircuitBreaker::record`].
    pub fn allow_request(&mut self) -> bool {
        self.refresh();
        match self.state {
            State::Closed => true,
            State::Open => {
                self.rejected += 1;
                false
            }
            State::HalfOpen => {
                if self.probe_in_flight {
                    self.rejected += 1;
                    false
                } else {
                    self.probe_in_flight = true;
                    true
                }
            }
        }
    }

    /// Report the outcome of an allowed request.
    pub fn record(&mut self, outcome: Outcome) {
        self.refresh();
        match (self.state, outcome) {
            (State::HalfOpen, Outcome::NetworkError) => {
                self.probe_in_flight = false;
                self.hold = std::cmp::min(self.hold * 2, self.config.max_hold);
                self.transition(State::Open, "half-open probe failed; hold doubled");
                self.trips += 1;
            }
            (State::HalfOpen, _) => {
                // Any non-network outcome proves the path works.
                self.probe_in_flight = false;
                self.consecutive_failures = 0;
                self.hold =
                    self.jittered_hold(self.config.open_hold_min, self.config.open_hold_max);
                self.transition(State::Closed, "half-open probe succeeded");
            }
            (State::Closed, Outcome::Success) => {
                self.consecutive_failures = 0;
            }
            (State::Closed, Outcome::NetworkError) => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= self.config.failure_threshold {
                    self.hold =
                        self.jittered_hold(self.config.open_hold_min, self.config.open_hold_max);
                    self.transition(State::Open, "failure threshold reached");
                    self.trips += 1;
                }
            }
            (State::Closed, Outcome::NonNetworkError) => {
                // Neutral: does not count, does not reset.
            }
            (State::Open, _) => {
                // Unreachable: no request is allowed while Open.
            }
        }
    }

    /// Consecutive counted failures since the last success (Closed only).
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Number of transitions into Open (trips + failed Half-Open probes).
    pub fn trips(&self) -> u64 {
        self.trips
    }

    /// Number of requests rejected locally while Open / probe in flight.
    pub fn rejected(&self) -> u64 {
        self.rejected
    }

    /// The hold currently in effect (jittered on trip, doubled on each
    /// failed probe, capped at `config.max_hold`).
    pub fn current_hold(&self) -> Duration {
        self.hold
    }

    /// Recorded state transitions, in order.
    pub fn events(&self) -> &[Transition] {
        &self.events
    }

    fn refresh(&mut self) {
        if self.state == State::Open {
            if let Some(opened) = self.opened_at {
                if opened.elapsed() >= self.hold {
                    self.probe_in_flight = false;
                    self.transition(State::HalfOpen, "open hold expired");
                }
            }
        }
    }

    fn transition(&mut self, to: State, reason: &'static str) {
        let from = self.state;
        if to == State::Open {
            self.opened_at = Some(Instant::now());
        }
        self.state = to;
        self.events.push(Transition {
            from,
            to,
            at: Instant::now(),
            reason,
        });
    }

    fn jittered_hold(&mut self, min: Duration, max: Duration) -> Duration {
        let span_nanos = max.saturating_sub(min).as_nanos() as u64;
        if span_nanos == 0 {
            return min;
        }
        min + Duration::from_nanos(self.next_u64() % (span_nanos + 1))
    }

    fn next_u64(&mut self) -> u64 {
        // xorshift64star; no external RNG dependency.
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        x.wrapping_mul(0x2545f4914f6cdd1d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    fn test_breaker() -> CircuitBreaker {
        CircuitBreaker::new(Config {
            failure_threshold: 5,
            open_hold_min: Duration::from_millis(50),
            open_hold_max: Duration::from_millis(100),
            max_hold: Duration::from_secs(5),
        })
    }

    fn trip(b: &mut CircuitBreaker) {
        for _ in 0..5 {
            b.record(Outcome::NetworkError);
        }
        assert_eq!(b.state(), State::Open);
    }

    #[test]
    fn trips_after_five_consecutive_network_errors() {
        let mut b = test_breaker();
        for i in 1..5 {
            b.record(Outcome::NetworkError);
            assert_eq!(b.state(), State::Closed, "tripped early at {i}");
            assert_eq!(b.consecutive_failures(), i);
        }
        b.record(Outcome::NetworkError);
        assert_eq!(b.state(), State::Open);
        assert_eq!(b.trips(), 1);
        assert!(!b.allow_request());
    }

    #[test]
    fn http_4xx_does_not_trip() {
        let mut b = test_breaker();
        for _ in 0..20 {
            b.record(Outcome::NonNetworkError);
        }
        assert_eq!(b.state(), State::Closed);
        assert_eq!(b.consecutive_failures(), 0);
        assert_eq!(b.trips(), 0);
    }

    #[test]
    fn success_resets_failure_count() {
        let mut b = test_breaker();
        for _ in 0..4 {
            b.record(Outcome::NetworkError);
        }
        b.record(Outcome::Success);
        assert_eq!(b.consecutive_failures(), 0);
        for _ in 0..4 {
            b.record(Outcome::NetworkError);
        }
        assert_eq!(b.state(), State::Closed);
    }

    #[test]
    fn open_rejects_and_counts() {
        let mut b = test_breaker();
        trip(&mut b);
        for _ in 0..3 {
            assert!(!b.allow_request());
        }
        assert_eq!(b.rejected(), 3);
        assert_eq!(b.state(), State::Open);
    }

    #[test]
    fn half_open_probe_success_closes() {
        let mut b = test_breaker();
        trip(&mut b);
        sleep(Duration::from_millis(150)); // past the 50–100 ms hold
        assert!(b.allow_request(), "probe should be allowed after hold");
        assert!(!b.allow_request(), "only one probe in flight");
        assert_eq!(b.state(), State::HalfOpen);
        b.record(Outcome::Success);
        assert_eq!(b.state(), State::Closed);
        assert_eq!(b.consecutive_failures(), 0);
        assert!(b.allow_request());
    }

    #[test]
    fn half_open_probe_failure_reopens_with_doubled_hold() {
        let mut b = test_breaker();
        trip(&mut b);
        let first_hold = b.current_hold();
        sleep(first_hold + Duration::from_millis(20));
        assert!(b.allow_request(), "probe should be allowed after hold");
        b.record(Outcome::NetworkError);
        assert_eq!(b.state(), State::Open);
        assert_eq!(b.trips(), 2);
        let doubled = std::cmp::min(first_hold * 2, Duration::from_secs(5));
        assert_eq!(b.current_hold(), doubled);
    }

    #[test]
    fn jittered_hold_stays_within_bounds() {
        for _ in 0..20 {
            let mut b = test_breaker();
            trip(&mut b);
            let hold = b.current_hold();
            assert!(hold >= Duration::from_millis(50), "hold {hold:?} below min");
            assert!(hold <= Duration::from_millis(100), "hold {hold:?} above max");
        }
    }

    #[test]
    fn transitions_are_recorded() {
        let mut b = test_breaker();
        trip(&mut b);
        assert_eq!(b.events().len(), 1);
        assert_eq!(b.events()[0].from, State::Closed);
        assert_eq!(b.events()[0].to, State::Open);
    }
}
