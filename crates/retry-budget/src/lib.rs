//! Decorrelated-jitter backoff and sliding-window retry budget.
//!
//! See `docs/SPEC.md` §4.2:
//! - Decorrelated jitter: `sleep = min(cap, rand(base, sleep * 3))`,
//!   base 250 ms, cap 30 s; reset to base on success.
//! - At most 10 retries per origin per rolling 60 s window; fail fast
//!   when exhausted. Idempotent-only retries; retries coalesced across
//!   in-flight requests to the same origin (one reconnect schedule per
//!   origin, not one per request).
//!
//! No external dependencies: jitter uses a xorshift PRNG seeded from the
//! system clock.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Backoff parameters. Defaults follow SPEC §4.2.
#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    pub base: Duration,
    pub cap: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(250),
            cap: Duration::from_secs(30),
        }
    }
}

/// Retry budget parameters. Defaults follow SPEC §4.2.
#[derive(Debug, Clone, Copy)]
pub struct BudgetConfig {
    /// Retries allowed per origin inside one rolling window.
    pub max_retries: u32,
    pub window: Duration,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            window: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RetryConfig {
    pub backoff: BackoffConfig,
    pub budget: BudgetConfig,
}

/// Decorrelated jitter: `sleep = min(cap, rand(base, sleep * 3))`.
#[derive(Debug, Clone)]
pub struct DecorrelatedJitter {
    config: BackoffConfig,
    sleep: Duration,
    rng: u64,
}

impl DecorrelatedJitter {
    pub fn new(config: BackoffConfig) -> Self {
        Self::with_seed(config, clock_seed())
    }

    pub fn with_seed(config: BackoffConfig, seed: u64) -> Self {
        Self {
            config,
            sleep: config.base,
            rng: seed | 1,
        }
    }

    /// The next delay; also becomes the `sleep` the following call
    /// grows from.
    pub fn next_delay(&mut self) -> Duration {
        let lo = self.config.base;
        let hi = std::cmp::max(lo, self.sleep.saturating_mul(3));
        let span = hi.saturating_sub(lo).as_nanos() as u64;
        let pick = if span == 0 {
            lo
        } else {
            lo + Duration::from_nanos(self.next_u64() % (span + 1))
        };
        self.sleep = std::cmp::min(self.config.cap, pick);
        self.sleep
    }

    /// Reset to `base` after a successful response.
    pub fn reset(&mut self) {
        self.sleep = self.config.base;
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545f4914f6cdd1d)
    }
}

/// Sliding-window retry budget: at most `max_retries` per `window`.
#[derive(Debug, Clone)]
pub struct RetryBudget {
    config: BudgetConfig,
    spent: VecDeque<Instant>,
}

impl RetryBudget {
    pub fn new(config: BudgetConfig) -> Self {
        Self {
            config,
            spent: VecDeque::new(),
        }
    }

    fn expire(&mut self, now: Instant) {
        while let Some(&t) = self.spent.front() {
            if now.saturating_duration_since(t) >= self.config.window {
                self.spent.pop_front();
            } else {
                break;
            }
        }
    }

    /// Spend one retry if the window has room.
    pub fn try_spend(&mut self, now: Instant) -> bool {
        self.expire(now);
        if (self.spent.len() as u32) < self.config.max_retries {
            self.spent.push_back(now);
            true
        } else {
            false
        }
    }

    pub fn remaining(&mut self, now: Instant) -> u32 {
        self.expire(now);
        self.config
            .max_retries
            .saturating_sub(self.spent.len() as u32)
    }

    /// When the oldest spent retry leaves the window (zero if there is room).
    pub fn refill_in(&mut self, now: Instant) -> Duration {
        self.expire(now);
        if (self.spent.len() as u32) < self.config.max_retries {
            return Duration::ZERO;
        }
        self.spent
            .front()
            .map(|&t| {
                self.config
                    .window
                    .saturating_sub(now.saturating_duration_since(t))
            })
            .unwrap_or(Duration::ZERO)
    }
}

/// Why a failed request must not be retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailFast {
    /// Non-idempotent operations surface the error to the caller.
    NonIdempotent,
    /// The origin's rolling-window budget is spent; `refill_in` says
    /// when the next retry becomes possible.
    BudgetExhausted { refill_in: Duration },
}

/// What the caller should do after a retryable (network) failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Wait `after`, then retry. `coalesced` is true when the request
    /// joined a reconnect schedule another in-flight request already
    /// created (no budget spent, backoff not advanced).
    Retry {
        after: Duration,
        coalesced: bool,
    },
    FailFast(FailFast),
}

/// Per-origin retry counters, for logs and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryStats {
    pub scheduled: u64,
    pub coalesced: u64,
    pub budget_exhausted: u64,
    pub non_idempotent: u64,
    pub successes: u64,
}

struct OriginRetry {
    jitter: DecorrelatedJitter,
    budget: RetryBudget,
    next_attempt: Option<Instant>,
    stats: RetryStats,
}

/// Thread-safe per-origin retry coordinator shared by every in-flight
/// request of a client.
pub struct RetryCoordinator {
    config: RetryConfig,
    origins: Mutex<HashMap<String, OriginRetry>>,
}

impl RetryCoordinator {
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            origins: Mutex::new(HashMap::new()),
        }
    }

    pub fn config(&self) -> RetryConfig {
        self.config
    }

    fn with_origin<T>(&self, origin: &str, f: impl FnOnce(&mut OriginRetry) -> T) -> T {
        let mut map = self.origins.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map
            .entry(origin.to_string())
            .or_insert_with(|| OriginRetry {
                jitter: DecorrelatedJitter::new(self.config.backoff),
                budget: RetryBudget::new(self.config.budget),
                next_attempt: None,
                stats: RetryStats::default(),
            });
        f(entry)
    }

    /// Decide what to do after a network failure on `origin`.
    pub fn on_failure(&self, origin: &str, idempotent: bool, now: Instant) -> Decision {
        self.with_origin(origin, |o| {
            if !idempotent {
                o.stats.non_idempotent += 1;
                return Decision::FailFast(FailFast::NonIdempotent);
            }
            if let Some(t) = o.next_attempt {
                if t > now {
                    o.stats.coalesced += 1;
                    return Decision::Retry {
                        after: t - now,
                        coalesced: true,
                    };
                }
            }
            if !o.budget.try_spend(now) {
                o.stats.budget_exhausted += 1;
                return Decision::FailFast(FailFast::BudgetExhausted {
                    refill_in: o.budget.refill_in(now),
                });
            }
            let after = o.jitter.next_delay();
            o.next_attempt = Some(now + after);
            o.stats.scheduled += 1;
            Decision::Retry {
                after,
                coalesced: false,
            }
        })
    }

    /// A response arrived: reset the backoff and the pending schedule.
    pub fn on_success(&self, origin: &str) {
        self.with_origin(origin, |o| {
            o.jitter.reset();
            o.next_attempt = None;
            o.stats.successes += 1;
        })
    }

    pub fn remaining_budget(&self, origin: &str, now: Instant) -> u32 {
        self.with_origin(origin, |o| o.budget.remaining(now))
    }

    pub fn stats(&self, origin: &str) -> RetryStats {
        self.with_origin(origin, |o| o.stats)
    }
}

fn clock_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9e3779b97f4a7c15)
        ^ (std::process::id() as u64).wrapping_mul(0x2545f4914f6cdd1d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_base_and_cap_and_grows() {
        let cfg = BackoffConfig::default();
        let mut j = DecorrelatedJitter::with_seed(cfg, 42);
        let mut prev = cfg.base;
        let mut max_seen = Duration::ZERO;
        for _ in 0..200 {
            let d = j.next_delay();
            assert!(d >= cfg.base, "{d:?} below base");
            assert!(d <= cfg.cap, "{d:?} above cap");
            assert!(
                d <= std::cmp::max(cfg.base, prev * 3),
                "{d:?} above 3x previous"
            );
            prev = d;
            max_seen = max_seen.max(d);
        }
        assert!(
            max_seen > Duration::from_secs(5),
            "never grew: {max_seen:?}"
        );
    }

    #[test]
    fn jitter_reset_returns_to_base_range() {
        let cfg = BackoffConfig::default();
        let mut j = DecorrelatedJitter::with_seed(cfg, 7);
        for _ in 0..50 {
            j.next_delay();
        }
        j.reset();
        let d = j.next_delay();
        assert!(d <= cfg.base * 3, "after reset {d:?} > 3x base");
    }

    #[test]
    fn jitter_is_decorrelated_across_seeds() {
        let cfg = BackoffConfig::default();
        let a: Vec<_> = {
            let mut j = DecorrelatedJitter::with_seed(cfg, 1);
            (0..5).map(|_| j.next_delay()).collect()
        };
        let b: Vec<_> = {
            let mut j = DecorrelatedJitter::with_seed(cfg, 2);
            (0..5).map(|_| j.next_delay()).collect()
        };
        assert_ne!(a, b);
    }

    #[test]
    fn budget_allows_ten_per_window_then_refills() {
        let mut b = RetryBudget::new(BudgetConfig::default());
        let t0 = Instant::now();
        for i in 0..10 {
            assert!(
                b.try_spend(t0 + Duration::from_secs(i)),
                "retry {i} refused"
            );
        }
        assert!(!b.try_spend(t0 + Duration::from_secs(30)));
        assert_eq!(
            b.refill_in(t0 + Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        // The first spend leaves the window at t0 + 60 s.
        assert!(b.try_spend(t0 + Duration::from_secs(60)));
        assert!(!b.try_spend(t0 + Duration::from_secs(60)));
    }

    #[test]
    fn non_idempotent_fails_fast_without_spending_budget() {
        let c = RetryCoordinator::new(RetryConfig::default());
        let now = Instant::now();
        assert_eq!(
            c.on_failure("o", false, now),
            Decision::FailFast(FailFast::NonIdempotent)
        );
        assert_eq!(c.remaining_budget("o", now), 10);
    }

    #[test]
    fn concurrent_failures_coalesce_into_one_schedule() {
        let c = RetryCoordinator::new(RetryConfig::default());
        let now = Instant::now();
        let first = c.on_failure("o", true, now);
        let Decision::Retry { after, coalesced } = first else {
            panic!("expected retry, got {first:?}")
        };
        assert!(!coalesced);
        for i in 1..50u64 {
            let t = now + Duration::from_millis(i % 5);
            match c.on_failure("o", true, t) {
                Decision::Retry {
                    after: a,
                    coalesced: true,
                } => assert!(a <= after),
                other => panic!("request {i} not coalesced: {other:?}"),
            }
        }
        assert_eq!(
            c.remaining_budget("o", now),
            9,
            "coalesced requests spent budget"
        );
        assert_eq!(c.stats("o").scheduled, 1);
        assert_eq!(c.stats("o").coalesced, 49);
    }

    #[test]
    fn budget_exhaustion_fails_fast_per_origin() {
        // Short cap so 11 sequential retries land inside one 60 s window.
        let c = RetryCoordinator::new(RetryConfig {
            backoff: BackoffConfig {
                base: Duration::from_millis(250),
                cap: Duration::from_secs(1),
            },
            budget: BudgetConfig::default(),
        });
        let mut now = Instant::now();
        let mut scheduled = 0;
        for _ in 0..100 {
            match c.on_failure("a", true, now) {
                Decision::Retry {
                    after,
                    coalesced: false,
                } => {
                    scheduled += 1;
                    now += after;
                }
                Decision::Retry { .. } => unreachable!("now is past every schedule"),
                Decision::FailFast(FailFast::BudgetExhausted { .. }) => break,
                Decision::FailFast(f) => panic!("{f:?}"),
            }
        }
        assert!(scheduled <= 10);
        assert!(matches!(
            c.on_failure("a", true, now),
            Decision::FailFast(FailFast::BudgetExhausted { .. })
        ));
        // Other origins keep their own budget.
        assert!(matches!(
            c.on_failure("b", true, now),
            Decision::Retry { .. }
        ));
    }

    #[test]
    fn success_clears_schedule() {
        let c = RetryCoordinator::new(RetryConfig::default());
        let now = Instant::now();
        c.on_failure("o", true, now);
        c.on_success("o");
        assert!(matches!(
            c.on_failure("o", true, now),
            Decision::Retry {
                coalesced: false,
                ..
            }
        ));
    }
}
