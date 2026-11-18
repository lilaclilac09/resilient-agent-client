//! Queue and traffic isolation for telemetry vs. interactive RPC.
//!
//! TODO: stub — not implemented yet.
//!
//! Responsibility (see `docs/SPEC.md`, section 4.4):
//! - Telemetry and user-interactive RPC on two fully isolated
//!   channels/queues; interactive work never waits behind background work.
//! - Freeze background telemetry while any breaker is Open; a global
//!   hard switch disables background polling entirely.
