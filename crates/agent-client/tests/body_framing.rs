use agent_client::*;
use loopback_fixtures::ConnectProxy;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

fn origin(replies: Vec<&'static str>) -> Origin {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        for reply in replies {
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    return;
                }
                if line == "\r\n" {
                    break;
                }
            }
            if socket.write_all(reply.as_bytes()).is_err() {
                return;
            }
        }
    });
    Origin::new("127.0.0.1", port)
}

const CHUNKED: &str = "HTTP/1.1 200 OK\r\nTransfer-Encoding: ChUnKeD\r\n\r\n2;foo=bar\r\nhe\r\n3;quoted=\"a;b\"\r\nllo\r\n0;end=yes\r\nX-Trailer: value\r\n\r\n";
const LENGTH: &str = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
const EOF: &str = "HTTP/1.1 200 OK\r\n\r\nhello";

fn check(reply: &'static str, proxy: ProxyMode, limit: usize) -> Result<Response, ClientError> {
    let mut config = test_config(proxy);
    config.max_attempts = 1;
    config.max_body = limit;
    Client::new(config)
        .unwrap()
        .send(&origin(vec![reply]), &Request::get("/"))
}

#[test]
fn chunked_with_extensions_trailers_and_reuse() {
    let server = origin(vec![CHUNKED, LENGTH]);
    let client = Client::new(test_config(ProxyMode::Direct)).unwrap();
    for _ in 0..2 {
        assert_eq!(
            client.send(&server, &Request::get("/")).unwrap().body,
            b"hello"
        );
    }
    assert_eq!(client.pool().stats().reused, 1);
}

#[test]
fn close_delimited() {
    assert_eq!(check(EOF, ProxyMode::Direct, 5).unwrap().body, b"hello");
}

#[test]
fn content_length() {
    assert_eq!(check(LENGTH, ProxyMode::Direct, 5).unwrap().body, b"hello");
}

#[test]
fn chunked_over_proxy() {
    let proxy = ConnectProxy::start(0).unwrap();
    let mode = ProxyMode::Http(HttpConnectProxy::new(Origin::new("127.0.0.1", proxy.port)));
    assert_eq!(check(CHUNKED, mode, 5).unwrap().body, b"hello");
    assert_eq!(proxy.total_tunnels(), 1);
}

#[test]
fn body_limits_apply_to_every_framing() {
    for reply in [CHUNKED, EOF, LENGTH] {
        let error = check(reply, ProxyMode::Direct, 4).unwrap_err();
        assert!(matches!(error, ClientError::Protocol(_)), "{error:?}");
        assert!(error.to_string().contains("response body too large"));
    }
}

#[test]
fn empty_bodies() {
    for reply in [
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        "HTTP/1.1 200 OK\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    ] {
        assert!(check(reply, ProxyMode::Direct, 0).unwrap().body.is_empty());
    }
}

#[test]
fn malformed_or_truncated_bodies_fail() {
    for reply in [
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\ng\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nx",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx!!0\r\n\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nx",
        "HTTP/1.1 200 OK\r\nContent-Length: invalid\r\n\r\nx",
    ] {
        assert!(check(reply, ProxyMode::Direct, 5).is_err(), "{reply:?}");
    }
}

#[test]
fn no_body_responses_leave_connection_in_sync() {
    let server = origin(vec![
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
        "HTTP/1.1 204 No Content\r\n\r\n",
        "HTTP/1.1 304 Not Modified\r\nContent-Length: 100\r\n\r\n",
        LENGTH,
    ]);
    let client = Client::new(test_config(ProxyMode::Direct)).unwrap();
    let mut head = Request::get("/");
    head.method = "HEAD".into();
    assert!(client.send(&server, &head).unwrap().body.is_empty());
    for status in [204, 304] {
        let response = client.send(&server, &Request::get("/")).unwrap();
        assert_eq!(response.status, status);
        assert!(response.body.is_empty());
    }
    assert_eq!(
        client.send(&server, &Request::get("/")).unwrap().body,
        b"hello"
    );
}
