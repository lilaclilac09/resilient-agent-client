//! Scripted loopback server checks for HTTP/1.1 response framing.

use agent_client::*;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

/// Serve one keep-alive connection, answering the n-th request head
/// with `replies[n]` verbatim.
fn scripted(replies: Vec<&'static str>) -> Origin {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        let mut w = s.try_clone().unwrap();
        let mut r = BufReader::new(s);
        for reply in replies {
            loop {
                let mut h = String::new();
                if r.read_line(&mut h).unwrap_or(0) == 0 {
                    return;
                }
                if h == "\r\n" {
                    break;
                }
            }
            w.write_all(reply.as_bytes()).unwrap();
        }
        std::thread::sleep(Duration::from_secs(2));
    });
    Origin::new("127.0.0.1", port)
}

#[test]
fn interim_1xx_is_skipped_and_the_connection_stays_in_sync() {
    let o = scripted(vec![
        "HTTP/1.1 103 Early Hints\r\nLink: </a.css>; rel=preload\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst",
        "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond",
    ]);
    let c = Client::new(test_config(ProxyMode::Direct)).unwrap();
    let a = c.send(&o, &Request::get("/a")).unwrap();
    assert_eq!((a.status, a.body.as_slice()), (200, &b"first"[..]));
    let b = c.send(&o, &Request::get("/b")).unwrap();
    assert_eq!((b.status, b.body.as_slice()), (200, &b"second"[..]));
    assert_eq!(c.pool().stats().reused, 1);
}

#[test]
fn head_response_does_not_wait_for_a_body() {
    let o = scripted(vec![
        "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n",
        "HTTP/1.1 204 No Content\r\n\r\n",
    ]);
    let c = Client::new(test_config(ProxyMode::Direct)).unwrap();
    let t = Instant::now();
    let mut head = Request::get("/");
    head.method = "HEAD".into();
    let r = c.send(&o, &head).unwrap();
    assert!(r.body.is_empty());
    let r = c.send(&o, &Request::get("/")).unwrap();
    assert_eq!(r.status, 204);
    assert!(t.elapsed() < Duration::from_secs(1), "waited for a body");
}

#[test]
fn urls_parse_into_scheme_origin_and_path() {
    let (s, o, p) = parse_url("https://localhost:8443/v1/x?y=1").unwrap();
    assert_eq!(
        (s, o.key(), p.as_str()),
        (Scheme::Https, "localhost:8443".to_string(), "/v1/x?y=1")
    );
    let (s, o, p) = parse_url("http://example.test").unwrap();
    assert_eq!((s, o.port, p.as_str()), (Scheme::Http, 80, "/"));
    assert_eq!(parse_url("https://h.test").unwrap().1.port, 443);
    let (_, o, p) = parse_url("https://example.com?x=1#frag").unwrap();
    assert_eq!(
        (o.key(), p.as_str()),
        ("example.com:443".to_string(), "/?x=1")
    );
    let (_, o, p) = parse_url("http://h.test:8080/a?b#c").unwrap();
    assert_eq!((o.port, p.as_str()), (8080, "/a?b"));
    for bad in ["ftp://h/", "https://user:pw@h/", "https://", "https://h:x/"] {
        assert!(parse_url(bad).is_err(), "{bad}");
    }
}

#[test]
fn crlf_in_url_or_method_is_rejected_before_any_socket() {
    let c = Client::new(test_config(ProxyMode::Direct)).unwrap();
    for url in [
        "http://127.0.0.1:9/a\r\nX-Injected: 1",
        "http://127.0.0.1:9/a b",
    ] {
        assert!(
            matches!(c.get(url), Err(ClientError::InvalidUrl(_))),
            "{url:?}"
        );
    }
    let mut req = Request::get("/");
    req.method = "GET /x HTTP/1.1\r\nHost: evil\r\n\r\nGET".into();
    assert!(matches!(
        c.send(&Origin::new("127.0.0.1", 9), &req),
        Err(ClientError::InvalidUrl(_))
    ));
    assert_eq!(c.pool().stats().opened, 0);
}
