use doris_rust_stream_load::{AuthenticationType, Client, Config, LogLevel, Mode, ValidationMode};
use reqwest::header::{HeaderMap, HeaderValue};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();
    headers.insert("x-app", HeaderValue::from_static("rust-stream-load"));

    let cfg = Config::builder()
        .stream_load_url("http://example.invalid/api/demo/events/_stream_load")
        .with_columns(["id", "name"])
        .headers(headers)
        .mode(Mode::Csv)
        .validation(ValidationMode::Syntax)
        .authentication_type(AuthenticationType::Basic)
        .authentication_token("user:password")
        .max_queue_size(10_000)
        .max_upload_queue_size(2)
        .batch_bytes(1 << 20)
        .linger(Duration::from_millis(50))
        .max_queue_wait_time(Duration::from_secs(1))
        .doris_upload_workers(2)
        .doris_upload_timeout(Duration::from_secs(60))
        .doris_upload_request_timeout(Duration::from_secs(30))
        .status_poll_timeout(Duration::from_secs(20))
        .label_prefix("rust_demo")
        .fake_send(true)
        .fake_send_delay(Duration::from_millis(10))
        .log_level(LogLevel::Debug)
        .build()?;

    let client = Client::new(cfg)?;

    let records = vec![
        "1,alice".to_string(),
        "2,bob".to_string(),
        "3,charlie".to_string(),
    ];

    let batch_handle = client.send_batch(records)?;
    let batch_result = batch_handle.wait();

    println!(
        "batch send result: success={} status_code={} response={:?}",
        batch_result.success(),
        batch_result.status_code,
        batch_result.response
    );

    let callback_handle = client.send_with_callback(
        |result| {
            println!(
                "callback result: success={} status_code={}",
                result.success(),
                result.status_code
            );
        },
        "4,dave".to_string(),
    )?;
    let callback_result = callback_handle.wait();

    println!(
        "callback send finished: success={} response={:?}",
        callback_result.success(),
        callback_result.response
    );

    client.close()?;
    let stats = client.stats();
    println!(
        "stats: total_workers={} busy_workers={} total_load_jobs={}",
        stats.total_workers, stats.busy_workers, stats.total_load_jobs
    );

    Ok(())
}
