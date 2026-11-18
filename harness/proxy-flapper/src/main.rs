//! TODO: stub — proxy flapper harness (not implemented yet).
//!
//! Intended behavior (see `docs/SPEC.md`, section 3): drive toxiproxy
//! (or `tc`, or timed kill) against a local loopback HTTP(S) proxy to
//! manufacture proxy flapping and TCP RSTs, so the client crates can be
//! exercised under the same fault conditions that produced the
//! socket/FD avalanche.

fn main() {
    eprintln!("proxy-flapper: TODO — not implemented yet");
    std::process::exit(2);
}
