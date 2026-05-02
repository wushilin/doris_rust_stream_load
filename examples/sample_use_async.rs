use doris_rust_stream_load::{
    AsyncClient, AsyncHandle, AuthenticationType, Config, LogLevel, Mode, ValidationMode,
};
use reqwest::header::{HeaderMap, HeaderValue};
use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

/// Async counterpart of sample_use.rs.
///
/// AsyncHandle is Send, so handles can be forwarded to a plain OS thread for
/// reaping.  The thread uses AsyncHandle::is_done() — a sync, lock-free read
/// of the underlying watch channel — to poll without needing a Tokio runtime.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();
    headers.insert("x-app", HeaderValue::from_static("rust-stream-load-async"));

    let cfg = Config::builder()
        .endpoint("https://doris.example.com")
        .database("sample_db")
        .table("sample_table")
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

    let client = AsyncClient::new(cfg)?;

    // Channel from the async task to the reaper OS thread.
    // Capacity mirrors max_queue_size so senders never block under normal load.
    let (handle_tx, handle_rx) = mpsc::sync_channel::<AsyncHandle>(10_000);

    // Spawn a plain OS thread.  It polls handles with is_done(), which is a
    // sync read and requires no Tokio runtime.
    let reaper = thread::spawn(move || {
        reap_handles(handle_rx);
    });

    // ── send_batch ────────────────────────────────────────────────────────────
    let records = vec![
        "1,alice".to_string(),
        "2,bob".to_string(),
        "3,charlie".to_string(),
    ];
    let batch_handle = client.send_batch(records).await?;
    handle_tx.send(batch_handle)?;

    // ── send_with_callback ────────────────────────────────────────────────────
    let callback_handle = client
        .send_with_callback(
            |result| {
                println!(
                    "callback result: success={} status_code={}",
                    result.success(),
                    result.status_code
                );
            },
            "4,dave".to_string(),
        )
        .await?;
    handle_tx.send(callback_handle)?;

    // Signal the reaper that no more handles will arrive.
    drop(handle_tx);

    // Flush all pending records and wait for every in-flight upload to finish.
    client.close().await?;

    reaper.join().expect("reaper thread should not panic");

    let stats = client.stats();
    println!(
        "stats: total_workers={} busy_workers={} total_load_jobs={}",
        stats.total_workers, stats.busy_workers, stats.total_load_jobs
    );

    Ok(())
}

/// Polls AsyncHandles from a sync channel until all are delivered.
///
/// AsyncHandle::is_done() and ::result() are non-async: they read the shared
/// watch state under a read lock, so no Tokio context is required.
fn reap_handles(handle_rx: Receiver<AsyncHandle>) {
    let mut pending = VecDeque::<AsyncHandle>::new();
    let mut channel_closed = false;

    loop {
        // Drain all newly arrived handles without blocking.
        while !channel_closed {
            match handle_rx.try_recv() {
                Ok(handle) => pending.push_back(handle),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    channel_closed = true;
                    break;
                }
            }
        }

        // Scan pending handles; put unfinished ones back.
        let count = pending.len();
        for _ in 0..count {
            let handle = pending.pop_front().unwrap();
            if handle.is_done() {
                if let Some(result) = handle.result() {
                    println!(
                        "delivered: success={} status_code={} response={:?}",
                        result.success(),
                        result.status_code,
                        result.response
                    );
                }
            } else {
                pending.push_back(handle);
            }
        }

        if channel_closed && pending.is_empty() {
            return;
        }

        thread::sleep(Duration::from_millis(10));
    }
}
