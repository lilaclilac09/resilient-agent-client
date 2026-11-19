//! Minimal loopback TCP server for fault-injection tests.
//!
//! Listens on 127.0.0.1:<port>, accepts connections and holds each one
//! for a few seconds WITHOUT reading from it. The unread data left in
//! the socket's receive buffer is what makes a kill produce TCP RSTs on
//! the client side (closing a socket with unread received data sends
//! RST). The proxy-flapper kills and restarts this process to
//! manufacture proxy flapping. Exits on SIGTERM/SIGINT via the default
//! disposition.
//!
//! Usage: fault-server [port]  (default 18099)

use std::env;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

fn main() {
    let port: u16 = env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(18099);
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap_or_else(|e| {
        eprintln!("fault-server: bind 127.0.0.1:{port} failed: {e}");
        std::process::exit(1);
    });
    eprintln!("fault-server: listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                // Hold the socket without reading, then close cleanly.
                thread::spawn(move || {
                    thread::sleep(Duration::from_secs(5));
                    drop(s);
                });
            }
            Err(e) => {
                eprintln!("fault-server: accept error: {e}");
                break;
            }
        }
    }
}
