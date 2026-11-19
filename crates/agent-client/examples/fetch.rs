//! Minimal supported consumer of the public `agent_client` API.
//!
//! ```text
//! cargo run -p agent-client --example fetch -- <url> [--cacert ca.pem] [--proxy http://127.0.0.1:7890]
//! ```
//!
//! Without `--proxy` it uses `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY`
//! and `NO_PROXY` (loopback always direct). HTTPS certificates are
//! verified against the Mozilla roots plus `--cacert`, if given.

use agent_client::{parse_http_proxy_url, Client, ClientConfig, HttpConnectProxy, ProxyMode, Tls};
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let mut args = std::env::args().skip(1);
    let (mut url, mut cacert, mut proxy) = (None, None, None);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--cacert" => cacert = args.next(),
            "--proxy" => proxy = args.next(),
            _ => url = Some(a),
        }
    }
    let url = url.ok_or("usage: fetch <url> [--cacert FILE] [--proxy http://HOST:PORT]")?;
    let mut config = ClientConfig::default();
    if let Some(p) = proxy {
        let origin = parse_http_proxy_url(&p).ok_or("--proxy must be http://host:port")?;
        config.proxy = ProxyMode::Http(HttpConnectProxy::new(origin));
    }
    if let Some(path) = cacert {
        let pem = std::fs::read(&path).map_err(|e| format!("{path}: {e}"))?;
        config.tls = Tls::with_extra_ca_pem(&pem)?;
    }
    let client = Client::new(config)?;
    let resp = client.get(&url).map_err(|e| e.to_string())?;
    println!("{}", String::from_utf8_lossy(&resp.body));
    eprintln!("status {} after {} attempt(s)", resp.status, resp.attempts);
    Ok(if (200..300).contains(&resp.status) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
