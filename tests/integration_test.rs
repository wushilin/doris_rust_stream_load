use doris_rust_stream_load::{AuthenticationType, Client, Config, Mode, ValidationMode};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

#[test]
fn fake_send_csv_batch_succeeds() {
    let mut cfg = Config::default();
    cfg.stream_load_url =
        Some("http://doris.example.com/api/test_db/test_table/_stream_load".to_string());
    cfg.columns = vec!["id".to_string(), "name".to_string()];
    cfg.mode = Mode::Csv;
    cfg.validation = ValidationMode::Syntax;
    cfg.fake_send = true;
    cfg.fake_send_delay = Duration::from_millis(10);
    cfg.fake_send_delay_set = true;

    let client = Client::new(cfg).expect("client should build");
    let handle = client
        .send("1,alice".to_string())
        .expect("send should succeed");
    let result = handle.wait();
    assert!(result.success());
    assert_eq!(result.status_code, 200);
    assert_eq!(result.response.unwrap().status.unwrap(), "Success");
    client.close().expect("close should succeed");
}

#[test]
fn fake_send_json_batch_strict_validation() {
    let mut cfg = Config::default();
    cfg.stream_load_url =
        Some("http://doris.example.com/api/test_db/test_table/_stream_load".to_string());
    cfg.columns = vec!["id".to_string(), "name".to_string()];
    cfg.mode = Mode::Json;
    cfg.validation = ValidationMode::Strict;
    cfg.fake_send = true;
    cfg.fake_send_delay = Duration::ZERO;
    cfg.fake_send_delay_set = true;

    let client = Client::new(cfg).expect("client should build");
    let handle = client
        .send("{\"id\":1,\"name\":\"alice\"}".to_string())
        .expect("send should succeed");
    let result = handle.wait();
    assert!(result.success());
    client.close().expect("close should succeed");
}

#[test]
fn fake_send_completes_all_handles_in_merged_batch() {
    let mut cfg = Config::default();
    cfg.stream_load_url =
        Some("http://doris.example.com/api/test_db/test_table/_stream_load".to_string());
    cfg.columns = vec!["id".to_string(), "name".to_string()];
    cfg.mode = Mode::Csv;
    cfg.validation = ValidationMode::Syntax;
    cfg.fake_send = true;
    cfg.fake_send_delay = Duration::from_millis(10);
    cfg.fake_send_delay_set = true;
    cfg.batch_bytes = 1024 * 1024;
    cfg.linger = Duration::from_millis(50);

    let client = Client::new(cfg).expect("client should build");
    let handles: Vec<_> = (0..20)
        .map(|i| {
            client
                .send(format!("{i},user{i}"))
                .expect("send should succeed")
        })
        .collect();

    for handle in handles {
        let result = handle.wait();
        assert!(result.success());
        assert_eq!(result.status_code, 200);
    }
    client.close().expect("close should succeed");
}

#[test]
fn close_drains_and_rejects_new_records() {
    let mut cfg = Config::default();
    cfg.stream_load_url =
        Some("http://doris.example.com/api/test_db/test_table/_stream_load".to_string());
    cfg.columns = vec!["id".to_string(), "name".to_string()];
    cfg.mode = Mode::Csv;
    cfg.validation = ValidationMode::None;
    cfg.fake_send = true;
    cfg.fake_send_delay = Duration::ZERO;
    cfg.fake_send_delay_set = true;

    let client = Client::new(cfg).expect("client should build");
    let handle = client
        .send("1,alice".to_string())
        .expect("send should succeed");

    client.close().expect("close should drain workers");
    assert!(client.closed());
    assert!(handle.is_done());
    assert!(client.send("2,bob".to_string()).is_err());
}

#[test]
fn rejects_batch_size_above_go_hard_cap() {
    let mut cfg = Config::default();
    cfg.stream_load_url =
        Some("http://doris.example.com/api/test_db/test_table/_stream_load".to_string());
    cfg.columns = vec!["id".to_string(), "name".to_string()];
    cfg.batch_bytes = 91 * 1024 * 1024;

    assert!(Config::builder()
        .stream_load_url("http://doris.example.com/api/test_db/test_table/_stream_load")
        .with_columns(["id"])
        .batch_bytes(91 * 1024 * 1024)
        .build()
        .is_err());
    assert!(Client::new(cfg).is_err());
}

#[test]
fn csv_validation_rejects_malformed_and_multirow_record() {
    let client =
        Client::new(fake_cfg(Mode::Csv, ValidationMode::Syntax)).expect("client should build");

    assert!(client.send("\"unterminated,alice".to_string()).is_err());
    assert!(client.send("1,alice\n2,bob".to_string()).is_err());
    assert!(client.send("1".to_string()).is_err());
    client.close().expect("close should succeed");
}

#[test]
fn csv_validation_uses_configured_separator_and_quote() {
    let mut cfg = fake_cfg(Mode::Csv, ValidationMode::Syntax);
    cfg.csv_separator = "|".to_string();
    cfg.csv_quote = "'".to_string();
    let client = Client::new(cfg).expect("client should build");

    assert!(client.send("1|'alice|bob'".to_string()).is_ok());
    assert!(client.send("1,alice".to_string()).is_err());
    client.close().expect("close should succeed");
}

#[test]
fn json_validation_modes_match_go_semantics() {
    let syntax_client =
        Client::new(fake_cfg(Mode::Json, ValidationMode::Syntax)).expect("client should build");
    assert!(syntax_client.send("[{\"id\":1}]".to_string()).is_err());
    assert!(syntax_client.send("{\"id\":".to_string()).is_err());
    assert!(syntax_client
        .send("{\"id\":1,\"extra\":true}".to_string())
        .is_ok());
    syntax_client.close().expect("close should succeed");

    let strict_client =
        Client::new(fake_cfg(Mode::Json, ValidationMode::Strict)).expect("client should build");
    assert!(strict_client
        .send("{\"id\":1,\"name\":\"alice\"}".to_string())
        .is_ok());
    assert!(strict_client.send("{\"id\":1}".to_string()).is_err());
    assert!(strict_client
        .send("{\"id\":1,\"name\":\"alice\",\"extra\":true}".to_string())
        .is_err());
    strict_client.close().expect("close should succeed");

    let none_client =
        Client::new(fake_cfg(Mode::Json, ValidationMode::None)).expect("client should build");
    let handle = none_client
        .send("{\"id\":".to_string())
        .expect("invalid json should enqueue when validation is disabled");
    assert!(handle.wait().success());
    none_client.close().expect("close should succeed");
}

#[test]
fn callback_runs_once_per_submitted_batch() {
    let client =
        Client::new(fake_cfg(Mode::Csv, ValidationMode::Syntax)).expect("client should build");
    let callbacks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callbacks_clone = callbacks.clone();

    let handle = client
        .send_batch_with_callback(
            move |result| {
                assert!(result.success());
                callbacks_clone.fetch_add(1, Ordering::SeqCst);
            },
            vec![
                "1,alice".to_string(),
                "2,bob".to_string(),
                "3,carl".to_string(),
            ],
        )
        .expect("send should succeed");

    assert!(handle.wait().success());
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    client.close().expect("close should succeed");
}

#[test]
fn callback_panic_does_not_kill_worker() {
    let client =
        Client::new(fake_cfg(Mode::Csv, ValidationMode::Syntax)).expect("client should build");

    let panicking = client
        .send_with_callback(
            |_| {
                panic!("callback should be contained");
            },
            "1,alice".to_string(),
        )
        .expect("send should succeed");
    assert!(panicking.wait().success());

    let after_panic = client
        .send("2,bob".to_string())
        .expect("worker should still accept later work");
    assert!(after_panic.wait().success());
    client.close().expect("close should succeed");
}

#[test]
fn wait_timeout_returns_none_before_completion_and_result_after_completion() {
    let mut cfg = fake_cfg(Mode::Csv, ValidationMode::Syntax);
    cfg.fake_send_delay = Duration::from_millis(50);
    cfg.fake_send_delay_set = true;
    let client = Client::new(cfg).expect("client should build");

    let handle = client
        .send("1,alice".to_string())
        .expect("send should succeed");
    assert!(handle
        .clone()
        .wait_timeout(Duration::from_millis(1))
        .is_none());
    assert!(handle
        .wait_timeout(Duration::from_secs(1))
        .unwrap()
        .success());
    client.close().expect("close should succeed");
}

#[test]
fn basic_auth_is_preserved_on_stream_load_redirect() {
    let auth_seen = Arc::new(AtomicBool::new(false));
    let be_listener = TcpListener::bind("127.0.0.1:0").expect("bind BE");
    let be_addr = be_listener.local_addr().expect("BE addr");
    let be_auth_seen = auth_seen.clone();
    let be = thread::spawn(move || {
        let (mut stream, _) = be_listener.accept().expect("BE accept");
        let request = read_http_request(&mut stream);
        if has_header_value(&request, "authorization", "Basic dXNlcjpwYXNz") {
            be_auth_seen.store(true, Ordering::SeqCst);
        }
        write_response(
            &mut stream,
            200,
            "OK",
            &[("Content-Type", "application/json")],
            r#"{"Status":"Success","Label":"redirected"}"#,
        );
    });

    let fe_listener = TcpListener::bind("127.0.0.1:0").expect("bind FE");
    let fe_addr = fe_listener.local_addr().expect("FE addr");
    let fe = thread::spawn(move || {
        let (mut stream, _) = fe_listener.accept().expect("FE accept");
        let _ = read_http_request(&mut stream);
        let location = format!("http://{be_addr}/api/test_db/test_table/_stream_load");
        write_response(
            &mut stream,
            307,
            "Temporary Redirect",
            &[("Location", &location)],
            "",
        );
    });

    let cfg = Config::builder()
        .stream_load_url(format!(
            "http://{fe_addr}/api/test_db/test_table/_stream_load"
        ))
        .with_columns(["id", "name"])
        .mode(Mode::Csv)
        .validation(ValidationMode::Syntax)
        .authentication_type(AuthenticationType::Basic)
        .authentication_token("user:pass")
        .doris_upload_request_timeout(Duration::from_secs(10))
        .build()
        .expect("config should build");
    let client = Client::new(cfg).expect("client should build");
    let result = client
        .send("1,alice".to_string())
        .expect("send should succeed")
        .wait();
    assert!(result.success(), "result={result:?}");

    client.close().expect("close should succeed");
    fe.join().expect("FE thread should finish");
    be.join().expect("BE thread should finish");
    assert!(auth_seen.load(Ordering::SeqCst));
}

#[test]
fn basic_auth_is_not_preserved_after_first_redirect() {
    let first_redirect_auth_seen = Arc::new(AtomicBool::new(false));
    let final_auth_seen = Arc::new(AtomicBool::new(false));

    let final_listener = TcpListener::bind("127.0.0.1:0").expect("bind final");
    let final_addr = final_listener.local_addr().expect("final addr");
    let final_auth_seen_clone = final_auth_seen.clone();
    let final_server = thread::spawn(move || {
        let (mut stream, _) = final_listener.accept().expect("final accept");
        let request = read_http_request(&mut stream);
        if has_header_value(&request, "authorization", "Basic dXNlcjpwYXNz") {
            final_auth_seen_clone.store(true, Ordering::SeqCst);
        }
        write_response(
            &mut stream,
            200,
            "OK",
            &[("Content-Type", "application/json")],
            r#"{"Status":"Success","Label":"redirected-twice"}"#,
        );
    });

    let redirect_listener = TcpListener::bind("127.0.0.1:0").expect("bind redirect");
    let redirect_addr = redirect_listener.local_addr().expect("redirect addr");
    let first_redirect_auth_seen_clone = first_redirect_auth_seen.clone();
    let redirect_server = thread::spawn(move || {
        let (mut stream, _) = redirect_listener.accept().expect("redirect accept");
        let request = read_http_request(&mut stream);
        if has_header_value(&request, "authorization", "Basic dXNlcjpwYXNz") {
            first_redirect_auth_seen_clone.store(true, Ordering::SeqCst);
        }
        let location = format!("http://{final_addr}/api/test_db/test_table/_stream_load");
        write_response(
            &mut stream,
            307,
            "Temporary Redirect",
            &[("Location", &location)],
            "",
        );
    });

    let fe_listener = TcpListener::bind("127.0.0.1:0").expect("bind FE");
    let fe_addr = fe_listener.local_addr().expect("FE addr");
    let fe = thread::spawn(move || {
        let (mut stream, _) = fe_listener.accept().expect("FE accept");
        let _ = read_http_request(&mut stream);
        let location = format!("http://{redirect_addr}/api/test_db/test_table/_stream_load");
        write_response(
            &mut stream,
            307,
            "Temporary Redirect",
            &[("Location", &location)],
            "",
        );
    });

    let cfg = Config::builder()
        .stream_load_url(format!(
            "http://{fe_addr}/api/test_db/test_table/_stream_load"
        ))
        .with_columns(["id", "name"])
        .mode(Mode::Csv)
        .validation(ValidationMode::Syntax)
        .authentication_type(AuthenticationType::Basic)
        .authentication_token("user:pass")
        .doris_upload_request_timeout(Duration::from_secs(10))
        .build()
        .expect("config should build");
    let client = Client::new(cfg).expect("client should build");
    let result = client
        .send("1,alice".to_string())
        .expect("send should succeed")
        .wait();
    assert!(result.success(), "result={result:?}");

    client.close().expect("close should succeed");
    fe.join().expect("FE thread should finish");
    redirect_server
        .join()
        .expect("redirect thread should finish");
    final_server.join().expect("final thread should finish");
    assert!(first_redirect_auth_seen.load(Ordering::SeqCst));
    assert!(!final_auth_seen.load(Ordering::SeqCst));
}

#[test]
fn label_already_exists_finished_is_success() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind server");
    let addr = listener.local_addr().expect("server addr");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let _ = read_http_request(&mut stream);
        write_response(
            &mut stream,
            200,
            "OK",
            &[("Content-Type", "application/json")],
            r#"{"Status":"Label Already Exists","ExistingJobStatus":"FINISHED","Label":"already"}"#,
        );
    });

    let cfg = Config::builder()
        .stream_load_url(format!("http://{addr}/api/test_db/test_table/_stream_load"))
        .with_columns(["id", "name"])
        .mode(Mode::Csv)
        .validation(ValidationMode::Syntax)
        .doris_upload_request_timeout(Duration::from_secs(10))
        .build()
        .expect("config should build");
    let client = Client::new(cfg).expect("client should build");
    let result = client
        .send("1,alice".to_string())
        .expect("send should succeed")
        .wait();
    assert!(result.success(), "result={result:?}");
    client.close().expect("close should succeed");
    server.join().expect("server should finish");
}

fn fake_cfg(mode: Mode, validation: ValidationMode) -> Config {
    let mut cfg = Config::default();
    cfg.stream_load_url =
        Some("http://doris.example.com/api/test_db/test_table/_stream_load".to_string());
    cfg.columns = vec!["id".to_string(), "name".to_string()];
    cfg.mode = mode;
    cfg.validation = validation;
    cfg.fake_send = true;
    cfg.fake_send_delay = Duration::ZERO;
    cfg.fake_send_delay_set = true;
    cfg
}

fn read_http_request(stream: &mut TcpStream) -> String {
    let mut data = Vec::new();
    let mut buf = [0; 1024];
    let header_end;
    loop {
        let n = stream.read(&mut buf).expect("read request");
        assert!(n > 0, "connection closed before request headers");
        data.extend_from_slice(&buf[..n]);
        if let Some(pos) = data.windows(4).position(|window| window == b"\r\n\r\n") {
            header_end = pos + 4;
            break;
        }
    }
    let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let already_read = data.len().saturating_sub(header_end);
    let mut remaining = content_length.saturating_sub(already_read);
    while remaining > 0 {
        let n = stream.read(&mut buf).expect("read request body");
        assert!(n > 0, "connection closed before request body");
        remaining = remaining.saturating_sub(n);
    }
    headers
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    headers: &[(&str, &str)],
    body: &str,
) {
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )
    .expect("write response status");
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n").expect("write response header");
    }
    write!(stream, "\r\n{body}").expect("write response body");
}

fn has_header_value(request: &str, expected_name: &str, expected_value: &str) -> bool {
    request.lines().any(|line| {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        name.trim().eq_ignore_ascii_case(expected_name) && value.trim() == expected_value
    })
}
