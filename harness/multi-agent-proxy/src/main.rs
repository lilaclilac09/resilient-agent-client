//! `cargo run -p multi-agent-proxy -- [agents] [seconds] [flap_ms]`
//!
//! Starts three loopback upstreams (named after the motivating origins)
//! and one loopback CONNECT proxy, then runs `agents` threads sharing a
//! single `agent_client::Client` while the proxy is killed and restarted
//! every `flap_ms`. Prints one JSON summary line and exits non-zero if a
//! bound from SPEC §5 is violated. Never touches a non-loopback address.

use agent_client::*;
use loopback_fixtures::{ConnectProxy, Upstream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn fd_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count())
}

fn main() {
    let args: Vec<u64> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let agents = *args.first().unwrap_or(&16) as usize;
    let secs = (*args.get(1).unwrap_or(&20)).min(120);
    let flap_ms = (*args.get(2).unwrap_or(&2000)).max(200);

    let ups: Vec<_> = ["api2", "api3", "cursorvm"]
        .iter()
        .map(|n| Upstream::start(n).unwrap())
        .collect();
    let proxy = Arc::new(ConnectProxy::start(0).unwrap());
    let retries_logged = Arc::new(AtomicU64::new(0));
    let rl = retries_logged.clone();
    let mut cfg = test_config(ProxyMode::Http(HttpConnectProxy::new(Origin::new(
        "127.0.0.1",
        proxy.port,
    ))));
    cfg.logger = Some(Arc::new(move |line: &str| {
        if line.starts_with("retry ") {
            rl.fetch_add(1, Ordering::Relaxed);
        } else if line.starts_with("breaker") {
            eprintln!("{line}");
        }
    }));
    let client = Arc::new(Client::new(cfg).unwrap());
    let fd_start = fd_count();
    let stop = Arc::new(AtomicBool::new(false));
    let (ok, open_rej, net_err, other) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );

    let flapper = {
        let (p, s) = (proxy.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut flaps = 0u64;
            while !s.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(flap_ms));
                if p.is_up() {
                    p.kill();
                    flaps += 1;
                } else {
                    p.restart().expect("rebind proxy port");
                }
            }
            if !p.is_up() {
                p.restart().expect("rebind proxy port");
            }
            flaps
        })
    };

    let origins: Vec<Origin> = ups
        .iter()
        .map(|u| Origin::new("127.0.0.1", u.addr.port()))
        .collect();
    let mut fd_peak = fd_start.unwrap_or(0);
    let workers: Vec<_> = (0..agents)
        .map(|i| {
            let (c, s, o) = (
                client.clone(),
                stop.clone(),
                origins[i % origins.len()].clone(),
            );
            let (ok, open_rej, net_err, other) =
                (ok.clone(), open_rej.clone(), net_err.clone(), other.clone());
            std::thread::spawn(move || {
                while !s.load(Ordering::SeqCst) {
                    let req = if i % 4 == 3 {
                        Request::post("/telemetry", "e")
                            .background()
                            .idempotent(true)
                    } else {
                        Request::get("/rpc")
                    };
                    match c.send(&o, &req) {
                        Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                        Err(ClientError::CircuitOpen { .. }) => {
                            std::thread::sleep(Duration::from_millis(50));
                            open_rej.fetch_add(1, Ordering::Relaxed)
                        }
                        Err(ClientError::Network { .. }) => net_err.fetch_add(1, Ordering::Relaxed),
                        Err(_) => other.fetch_add(1, Ordering::Relaxed),
                    };
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        })
        .collect();

    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(secs) {
        std::thread::sleep(Duration::from_millis(100));
        fd_peak = fd_peak.max(fd_count().unwrap_or(0));
    }
    stop.store(true, Ordering::SeqCst);
    for w in workers {
        w.join().unwrap();
    }
    let flaps = flapper.join().unwrap();

    // Recovery: after the proxy is back every origin must answer.
    let rec = Instant::now();
    let mut recovered = true;
    for o in &origins {
        loop {
            if client.send(o, &Request::get("/health")).is_ok() {
                break;
            }
            if rec.elapsed() > Duration::from_secs(15) {
                recovered = false;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    let recovery_ms = rec.elapsed().as_millis();
    client.pool().close_idle();
    std::thread::sleep(Duration::from_millis(200));
    let fd_end = fd_count();
    let stats = client.pool().stats();
    let cap = client.pool().config().max_per_origin;
    let peak_upstream = ups.iter().map(|u| u.peak_connections()).max().unwrap_or(0);
    let retry_budget_max = test_config(ProxyMode::Direct).retry.budget.max_retries as u64;
    let window_secs = test_config(ProxyMode::Direct)
        .retry
        .budget
        .window
        .as_secs()
        .max(1);
    let retry_ceiling = retry_budget_max * origins.len() as u64 * (secs / window_secs + 1);
    let retries: u64 = origins
        .iter()
        .map(|o| client.retries().stats(&o.key()).scheduled)
        .sum();

    println!(
        "{{\"agents\":{agents},\"seconds\":{secs},\"flaps\":{flaps},\"ok\":{},\"circuit_open_rejections\":{},\"network_errors\":{},\"other_errors\":{},\"retries\":{retries},\"retry_ceiling\":{retry_ceiling},\"peak_per_origin\":{},\"peak_upstream_conns\":{peak_upstream},\"cap\":{cap},\"sockets_opened\":{},\"proxy_tunnels\":{},\"fd_start\":{},\"fd_peak\":{fd_peak},\"fd_end\":{},\"recovered\":{recovered},\"recovery_ms\":{recovery_ms}}}",
        ok.load(Ordering::Relaxed),
        open_rej.load(Ordering::Relaxed),
        net_err.load(Ordering::Relaxed),
        other.load(Ordering::Relaxed),
        stats.peak_per_origin,
        stats.opened,
        proxy.total_tunnels(),
        fd_start.unwrap_or(0),
        fd_end.unwrap_or(0),
    );
    let mut fail = Vec::new();
    if stats.peak_per_origin > cap || peak_upstream > cap {
        fail.push("per-origin socket cap exceeded");
    }
    if retries > retry_ceiling {
        fail.push("retry budget exceeded");
    }
    if !recovered {
        fail.push("did not recover after proxy came back");
    }
    if let (Some(a), Some(b)) = (fd_start, fd_end) {
        if b > a + 4 {
            fail.push("file descriptors leaked");
        }
    }
    if !fail.is_empty() {
        eprintln!("FAIL: {}", fail.join("; "));
        std::process::exit(1);
    }
    eprintln!("PASS");
}
