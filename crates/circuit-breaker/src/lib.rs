//! Per-origin circuit breaker: Closed / Open / Half-Open.
//!
//! TODO: stub — not implemented yet.
//!
//! Responsibility (see `docs/SPEC.md`, section 4.3):
//! - Trip to Open after 5 consecutive underlying network errors
//!   (RST, ECONNREFUSED, timeout); HTTP 4xx does not count.
//! - While Open, reject background work locally with zero socket cost;
//!   jittered 30–60 s hold, then Half-Open with a single probe.
