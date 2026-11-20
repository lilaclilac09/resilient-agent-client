use agent_client::*;
use loopback_fixtures::{ConnectProxy, Upstream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn pct(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() { return 0; }
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let scenario = a.get(1).cloned().unwrap_or("steady".into());
    let agents: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(24);
    let secs: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
    let up = Upstream::start("bench").unwrap();
    let proxy = Arc::new(ConnectProxy::start(0).unwrap());
    let mut cfg = test_config(ProxyMode::Http(HttpConnectProxy::new(Origin::new("127.0.0.1", proxy.port))));
    cfg.logger = Some(Arc::new(|_: &str| {}));
    let client = Arc::new(Client::new(cfg).unwrap());
    let origin = Origin::new("127.0.0.1", up.addr.port());
    let until = Instant::now() + Duration::from_secs(secs);
    let stop = Arc::new(AtomicBool::new(false));
    let down_since = Arc::new(Mutex::new(Vec::<(Instant, Instant)>::new()));
    let flapper = (scenario == "flap").then(|| {
        let (p, stop, d) = (proxy.clone(), stop.clone(), down_since.clone());
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(2));
            while !stop.load(Ordering::SeqCst) {
                let t0 = Instant::now();
                p.kill();
                std::thread::sleep(Duration::from_millis(1000));
                p.restart().unwrap();
                d.lock().unwrap().push((t0, Instant::now()));
                std::thread::sleep(Duration::from_millis(2000));
            }
        })
    });
    let res: Vec<_> = (0..agents).map(|i| {
        let (c, o) = (client.clone(), origin.clone());
        std::thread::spawn(move || {
            let (mut lat, mut errs, mut ev) = (Vec::new(), 0u64, Vec::new());
            let req = Request::get(&format!("/a/{i}"));
            while Instant::now() < until {
                let t = Instant::now();
                let r = c.send(&o, &req);
                let us = t.elapsed().as_micros() as u64;
                ev.push((t, r.is_ok()));
                match r { Ok(_) => lat.push(us), Err(_) => { errs += 1; std::thread::sleep(Duration::from_millis(50)); } }
                std::thread::sleep(Duration::from_millis(5));
            }
            (lat, errs, ev)
        })
    }).collect();
    let (mut lat, mut errs, mut ev) = (Vec::new(), 0, Vec::new());
    for h in res { let (l, e, v) = h.join().unwrap(); lat.extend(l); errs += e; ev.extend(v); }
    stop.store(true, Ordering::SeqCst);
    if let Some(f) = flapper { let _ = f.join(); }
    // recovery: per outage, time from proxy restart to the first later successful request start
    let mut rec = Vec::new();
    for (_, up_at) in down_since.lock().unwrap().iter() {
        if let Some(t) = ev.iter().filter(|(t, ok)| *ok && t >= up_at).map(|(t, _)| *t).min() {
            rec.push(t.duration_since(*up_at).as_millis() as u64);
        }
    }
    let ok = lat.len();
    let rps = ok as f64 / secs as f64;
    println!("{{\"scenario\":\"{scenario}\",\"agents\":{agents},\"secs\":{secs},\"ok\":{ok},\"errors\":{errs},\"rps\":{rps:.0},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},\"max_us\":{},\"outages\":{},\"recovery_ms_max\":{},\"upstream_peak_conns\":{},\"proxy_tunnels\":{},\"upstream_conns\":{}}}",
        pct(&mut lat, 0.5), pct(&mut lat, 0.95), pct(&mut lat, 0.99), pct(&mut lat, 1.0),
        rec.len(), rec.iter().max().copied().unwrap_or(0), up.peak_connections(), proxy.total_tunnels(), up.total_connections());
}
