//! Decorrelated-jitter backoff and sliding-window retry budget.
//!
//! TODO: stub — not implemented yet.
//!
//! Responsibility (see `docs/SPEC.md`, section 4.2):
//! - Decorrelated jitter: `sleep = min(cap, rand(base, sleep * 3))`,
//!   base 250 ms, cap 30 s; reset to base on success.
//! - At most 10 retries per origin per rolling 60 s window; fail fast
//!   when exhausted. Idempotent-only retries; retries coalesced across
//!   in-flight requests.
