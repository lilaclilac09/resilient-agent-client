//! Multi-agent clients through a flapping loopback CONNECT proxy
//! (stand-in for mihomo's mixed-port). Local fixtures only.

use agent_client::*;
use loopback_fixtures::{ConnectProxy, Upstream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn origin(u: &Upstream) -> Origin {
    Origin::new("127.0.0.1", u.addr.port())
}

fn proxied_client(proxy: &ConnectProxy) -> Client {
    Client::new(test_config(ProxyMode::Http(HttpConnectProxy::new(
        Origin::new("127.0.0.1", proxy.port),
    ))))
    .unwrap()
}

#[test]
fn every_origin_is_reachable_through_the_proxy_and_sockets_are_reused() {
    let ups: Vec<_> = ["api2", "api3", "vm"]
        .iter()
        .map(|n| Upstream::start(n).unwrap())
        .collect();
    let proxy = ConnectProxy::start(0).unwrap();
    let c = proxied_client(&proxy);
    for _ in 0..20 {
        for u in &ups {
            let r = c.send(&origin(u), &Request::get("/ping")).unwrap();
            assert_eq!(r.status, 200);
            assert!(String::from_utf8_lossy(&r.body).starts_with(u.name()));
        }
    }
    assert_eq!(
        proxy.total_tunnels(),
        3,
        "keep-alive: one tunnel per origin"
    );
    assert!(c.pool().stats().reused >= 57);
}

#[test]
fn many_agents_share_bounded_sockets_per_origin() {
    let ups: Vec<_> = ["api2", "api3"]
        .iter()
        .map(|n| Upstream::start(n).unwrap())
        .collect();
    let proxy = ConnectProxy::start(0).unwrap();
    let c = Arc::new(proxied_client(&proxy));
    let max = c.pool().config().max_per_origin;
    let hs: Vec<_> = (0..24)
        .map(|i| {
            let c = c.clone();
            let o = origin(&ups[i % 2]);
            std::thread::spawn(move || {
                for _ in 0..30 {
                    c.send(&o, &Request::get("/work")).expect("request");
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    for u in &ups {
        assert!(
            u.peak_connections() <= max,
            "{} peaked at {}",
            u.name(),
            u.peak_connections()
        );
        assert_eq!(u.requests(), 360);
    }
    assert!(c.pool().stats().peak_per_origin <= max);
}

#[test]
fn proxy_flap_trips_breaker_fails_fast_then_recovers() {
    let up = Upstream::start("api2").unwrap();
    let proxy = ConnectProxy::start(0).unwrap();
    let c = proxied_client(&proxy);
    let o = origin(&up);
    c.send(&o, &Request::get("/")).unwrap();

    proxy.kill();
    let mut open_rejections = 0;
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(2) {
        match c.send(&o, &Request::get("/")) {
            Err(ClientError::CircuitOpen { .. }) => {
                let t0 = Instant::now();
                let _ = c.send(&o, &Request::get("/"));
                assert!(
                    t0.elapsed() < Duration::from_millis(20),
                    "open rejection must be local"
                );
                open_rejections += 1;
                break;
            }
            Err(ClientError::Network { .. }) => {}
            other => panic!("unexpected while proxy down: {other:?}"),
        }
    }
    assert!(open_rejections > 0, "breaker never opened");
    assert_eq!(c.breakers().state(&o.key()), State::Open);
    assert_eq!(c.pool().stats().open_now, 0, "no sockets held while down");

    proxy.restart().unwrap();
    let t = Instant::now();
    let ok = loop {
        if let Ok(r) = c.send(&o, &Request::get("/")) {
            break r.status == 200;
        }
        assert!(t.elapsed() < Duration::from_secs(10), "never recovered");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(ok);
    assert_eq!(c.breakers().state(&o.key()), State::Closed);
}

#[test]
fn non_idempotent_requests_are_not_retried() {
    let up = Upstream::start("api2").unwrap();
    let proxy = ConnectProxy::start(0).unwrap();
    let c = proxied_client(&proxy);
    proxy.kill();
    match c.send(&origin(&up), &Request::post("/rpc", "x")) {
        Err(ClientError::Network {
            reason, attempts, ..
        }) => {
            assert_eq!(reason, "non-idempotent");
            assert_eq!(attempts, 1);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(c.retries().stats(&origin(&up).key()).non_idempotent, 1);
}

#[test]
fn background_queue_freezes_during_outage_while_interactive_fails_fast() {
    use agent_client::isolation::*;
    let up = Upstream::start("telemetry").unwrap();
    let proxy = ConnectProxy::start(0).unwrap();
    let c = Arc::new(proxied_client(&proxy));
    let o = origin(&up);
    let gate = Gate {
        controls: Arc::new(Controls::default()),
        breakers: c.breakers().clone(),
    };
    let sent = Arc::new(AtomicU64::new(0));
    let (c2, o2, s2) = (c.clone(), o.clone(), sent.clone());
    let d = Dispatcher::new(
        DispatcherConfig {
            telemetry_flush_interval: Duration::from_millis(200),
            gate_poll: Duration::from_millis(20),
            ..Default::default()
        },
        gate,
        move |batch| {
            if c2
                .send(
                    &o2,
                    &Request::post("/telemetry", batch.join("\n")).background(),
                )
                .is_ok()
            {
                s2.fetch_add(1, Ordering::SeqCst);
            }
        },
    );
    proxy.kill();
    // Trip the breaker with interactive traffic.
    for _ in 0..10 {
        let _ = c.send(&o, &Request::get("/"));
    }
    assert_ne!(c.breakers().state(&o.key()), State::Closed);
    let before = proxy.total_tunnels();
    let stop = Arc::new(AtomicBool::new(false));
    for i in 0..50 {
        d.record_telemetry(format!("event {i}")).unwrap();
    }
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        sent.load(Ordering::SeqCst),
        0,
        "telemetry flushed during outage"
    );
    assert_eq!(proxy.total_tunnels(), before);
    proxy.restart().unwrap();
    let t = Instant::now();
    while c.send(&o, &Request::get("/")).is_err() {
        assert!(t.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(100));
    }
    while sent.load(Ordering::SeqCst) == 0 {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "telemetry never resumed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::SeqCst);
    assert_eq!(
        d.stats().telemetry_batches,
        1,
        "events batched into one flush"
    );
}

#[test]
fn fixture_proxy_refuses_non_loopback_targets() {
    let proxy = ConnectProxy::start(0).unwrap();
    let c = Client::new(ClientConfig {
        max_attempts: 1,
        ..test_config(ProxyMode::Http(HttpConnectProxy::new(Origin::new(
            "127.0.0.1",
            proxy.port,
        ))))
    })
    .unwrap();
    assert!(c
        .send(&Origin::new("192.0.2.1", 443), &Request::get("/"))
        .is_err());
    assert_eq!(proxy.refused_targets(), 1);
}

#[test]
fn concurrent_retries_never_exceed_the_per_origin_budget() {
    // Upstream that accepts and immediately closes: every attempt is a
    // real outbound connection and a network failure.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicU64::new(0));
    let a = accepted.clone();
    std::thread::spawn(move || {
        for s in l.incoming().flatten() {
            a.fetch_add(1, Ordering::SeqCst);
            drop(s);
        }
    });
    let mut cfg = test_config(ProxyMode::Direct);
    cfg.breaker.failure_threshold = 10_000; // isolate the budget from the breaker
    cfg.max_attempts = 5;
    cfg.retry.budget.window = Duration::from_secs(60);
    cfg.retry.backoff = retry_budget_cfg();
    let c = Arc::new(Client::new(cfg).unwrap());
    let o = Origin::new("127.0.0.1", port);
    let callers = 20u64;
    let hs: Vec<_> = (0..callers)
        .map(|_| {
            let (c, o) = (c.clone(), o.clone());
            std::thread::spawn(move || c.send(&o, &Request::get("/")).is_err())
        })
        .collect();
    assert!(hs.into_iter().all(|h| h.join().unwrap()));
    std::thread::sleep(Duration::from_millis(100));
    let retries = c.retries().stats(&o.key()).attempts();
    assert!(retries <= 10, "{retries} retries authorised");
    let outbound = accepted.load(Ordering::SeqCst);
    assert!(
        outbound <= callers + 10,
        "{outbound} outbound attempts for {callers} requests"
    );
}

fn retry_budget_cfg() -> BackoffConfig {
    BackoffConfig {
        base: Duration::from_millis(10),
        cap: Duration::from_millis(50),
    }
}
