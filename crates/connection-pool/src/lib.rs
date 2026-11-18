//! Bounded per-origin connection pool.
//!
//! TODO: stub — not implemented yet.
//!
//! Responsibility (see `docs/SPEC.md`, section 4.1):
//! - Hard per-origin socket cap (`max_sockets` 6–10) plus a global cap.
//! - Strict connect / read / total timeouts; a socket that exceeds its
//!   budget is destroyed, never returned to the pool.
//! - Explicit socket destruction on error paths (RST, ECONNREFUSED,
//!   timeout) and reclamation of idle file descriptors.
