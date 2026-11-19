//! Proxy flapper + breaker trip test (see `docs/SPEC.md` §3).
//!
//! What it does, end to end:
//! 1. Spawns `fault-server` (a loopback TCP listener) as a child process.
//! 2. Kills and restarts it on a schedule — timed kill manufactures
//!    proxy flapping: new connects get ECONNREFUSED, established
//!    connections with unread data get TCP RSTs.
//! 3. A client loop attempts `TcpStream::connect_timeout` against the
//!    server, gated by the circuit breaker. Every socket is dropped
//!    immediately after its attempt, so the client holds at most one
//!    socket at a time.
//! 4. Asserts the breaker trips (Closed -> Open after 5 consecutive
//!    network errors), rejects work with zero socket cost while Open,
//!    and closes again once the server recovers.
//!
//! FD hygiene: the flapper is the system under test. Its FD count is
//! measured OUTSIDE this process by `harness/fd-monitor/fd-monitor.sh`,
//! which reads `/proc/<pid>/fd` (or `lsof`) and opens no sockets itself,
//! so the measurement cannot leak FDs into the runner. The flapper also
//! reports its own `/proc/self/fd` count as a cross-check.
//!
//! Usage:
//!   proxy-flapper [--port 18099] [--duration-secs 60]
//!                 [--flap-every-secs 5] [--outage-secs 1]
//!                 [--attempt-interval-ms 100]
//! Exits 0 when the breaker behavior asserts pass, 1 otherwise.

use circuit_breaker::{CircuitBreaker, Config, Outcome, State};
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct Args {
    port: u16,
    duration: Duration,
    flap_every: Duration,
    outage: Duration,
    attempt_interval: Duration,
}

fn parse_args() -> Args {
    let mut args = Args {
        port: 18099,
        duration: Duration::from_secs(60),
        flap_every: Duration::from_secs(5),
        outage: Duration::from_secs(1),
        attempt_interval: Duration::from_millis(100),
    };
    let mut it = std::env::args().skip(1).peekable();
    while let Some(a) = it.next() {
        let mut v = || it.next().unwrap_or_else(|| panic!("missing value for {a}"));
        match a.as_str() {
            "--port" => args.port = v().parse().expect("--port must be a number"),
            "--duration-secs" => {
                args.duration = Duration::from_secs(v().parse().expect("--duration-secs"))
            }
            "--flap-every-secs" => {
                args.flap_every = Duration::from_secs(v().parse().expect("--flap-every-secs"))
            }
            "--outage-secs" => {
                args.outage = Duration::from_secs(v().parse().expect("--outage-secs"))
            }
            "--attempt-interval-ms" => {
                args.attempt_interval =
                    Duration::from_millis(v().parse().expect("--attempt-interval-ms"))
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    args
}

fn fault_server_path() -> PathBuf {
    if let Ok(p) = std::env::var("FAULT_SERVER_BIN") {
        return p.into();
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let cand = dir.join("fault-server");
            if cand.exists() {
                return cand;
            }
        }
    }
    "fault-server".into()
}

fn spawn_server(path: &PathBuf, port: u16) -> Child {
    Command::new(path)
        .arg(port.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn fault-server; build the workspace first (cargo build)")
}

fn classify(e: &std::io::Error) -> Outcome {
    use std::io::ErrorKind::*;
    match e.kind() {
        ConnectionRefused | ConnectionReset | TimedOut => Outcome::NetworkError,
        _ => Outcome::NonNetworkError,
    }
}

fn self_fd_count() -> u64 {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count() as u64)
        .unwrap_or(0)
}

fn main() {
    let args = parse_args();
    let addr: SocketAddr = format!("127.0.0.1:{}", args.port)
        .parse()
        .expect("invalid port");
    let server_bin = fault_server_path();
    let mut breaker = CircuitBreaker::new(Config::test_config());
    let mut server = spawn_server(&server_bin, args.port);
    eprintln!(
        "flapper: fault-server pid={} port={} (breaker test config: 2–4 s holds)",
        server.id(),
        args.port
    );

    let start = Instant::now();
    let mut next_flap = start + args.flap_every;
    let mut server_down_until: Option<Instant> = None;
    let mut attempts = 0u64;
    let mut successes = 0u64;
    let mut failures = 0u64;
    let mut other_errors = 0u64;
    let mut last_state = breaker.state();
    let mut max_self_fd = 0u64;
    let mut last_fd_sample = Instant::now() - Duration::from_secs(10);

    while start.elapsed() < args.duration {
        let now = Instant::now();

        // Flap on schedule: kill the server; the loop keeps attempting
        // during the outage, so connects fail and the breaker must trip.
        if server_down_until.is_none() && now >= next_flap {
            eprintln!("flapper: flap — killing fault-server pid={}", server.id());
            let _ = server.kill();
            let _ = server.wait(); // reap: no zombie, no leaked child handle
            server_down_until = Some(now + args.outage);
            next_flap = now + args.flap_every;
        }
        if let Some(until) = server_down_until {
            if now >= until {
                server = spawn_server(&server_bin, args.port);
                eprintln!("flapper: fault-server respawned pid={}", server.id());
                server_down_until = None;
            }
        }

        if breaker.allow_request() {
            attempts += 1;
            match TcpStream::connect_timeout(&addr, Duration::from_millis(500)) {
                Ok(mut s) => {
                    // One byte the server never reads: if the server is
                    // killed mid-connection, close sends RST to us.
                    let _ = s.write_all(b"ping\n");
                    std::thread::sleep(Duration::from_millis(50));
                    drop(s); // explicit: socket destroyed, never pooled
                    breaker.record(Outcome::Success);
                    successes += 1;
                }
                Err(e) => {
                    let outcome = classify(&e);
                    match outcome {
                        Outcome::NetworkError => failures += 1,
                        _ => other_errors += 1,
                    }
                    breaker.record(outcome);
                    eprintln!("flapper: connect failed: {e} ({outcome:?})");
                }
            }
        } else {
            // Rejected locally: zero socket cost, no connect attempted.
        }

        let st = breaker.state();
        if st != last_state {
            eprintln!(
                "flapper: transition {last_state:?} -> {st:?} at {:?} (trips={}, rejected={})",
                start.elapsed(),
                breaker.trips(),
                breaker.rejected()
            );
            last_state = st;
        }

        // Cross-check our own FD count once a second (transient /proc
        // read). Authoritative measurement is fd-monitor.sh, out of
        // process.
        if last_fd_sample.elapsed() >= Duration::from_secs(1) {
            let n = self_fd_count();
            if n > max_self_fd {
                max_self_fd = n;
            }
            last_fd_sample = Instant::now();
        }

        std::thread::sleep(args.attempt_interval);
    }

    // Recovery: make sure the server is up, then keep attempting until
    // the breaker is Closed (or the grace period expires), so the final
    // state does not depend on where the run happened to stop.
    if server_down_until.is_some() {
        server = spawn_server(&server_bin, args.port);
    }
    let grace = Instant::now();
    while breaker.state() != State::Closed && grace.elapsed() < Duration::from_secs(15) {
        if breaker.allow_request() {
            attempts += 1;
            match TcpStream::connect_timeout(&addr, Duration::from_millis(500)) {
                Ok(mut s) => {
                    let _ = s.write_all(b"ping\n");
                    drop(s);
                    breaker.record(Outcome::Success);
                    successes += 1;
                }
                Err(e) => {
                    let outcome = classify(&e);
                    if outcome == Outcome::NetworkError {
                        failures += 1;
                    } else {
                        other_errors += 1;
                    }
                    breaker.record(outcome);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let _ = server.kill();
    let _ = server.wait();

    let final_state = breaker.state();
    let ok = breaker.trips() >= 1
        && breaker.rejected() > 0
        && successes > 0
        && final_state == State::Closed;
    println!("=== TRIP TEST SUMMARY ===");
    println!("duration_secs: {}", args.duration.as_secs());
    println!("attempts: {attempts}");
    println!("successes: {successes}");
    println!("network_failures: {failures}");
    println!("other_errors: {other_errors}");
    println!("rejected_while_open: {}", breaker.rejected());
    println!("trips: {}", breaker.trips());
    println!("final_state: {final_state:?}");
    println!("max_self_fd: {max_self_fd}");
    println!("result: {}", if ok { "PASS" } else { "FAIL" });
    std::process::exit(if ok { 0 } else { 1 });
}
