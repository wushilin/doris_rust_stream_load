use doris_rust_stream_load::{AsyncClient, AsyncHandle, Config, LogLevel, Mode, ValidationMode};
use reqwest::header::{HeaderMap, HeaderValue};
use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

const HANDLE_REAPER_QUEUE_SIZE: usize = 500_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-custom-header",
        HeaderValue::from_static("rust-example-async"),
    );

    let cfg = Config::builder()
        .endpoint("http://doris.example.com")
        .database("test_db")
        .table("test_table")
        .with_columns(["id", "name"])
        .mode(Mode::Json)
        .validation(ValidationMode::Syntax)
        .fake_send(true)
        .fake_send_delay(Duration::from_millis(50))
        .doris_upload_workers(2)
        .batch_bytes(20 * 1024 * 1024)
        .linger(Duration::from_millis(100))
        .max_queue_wait_time(Duration::from_secs(2))
        .status_poll_timeout(Duration::from_secs(10))
        .log_level(LogLevel::Debug)
        .headers(headers)
        .build()?;

    let client = AsyncClient::new(cfg)?;

    let total_messages = 20_000_000;
    let acked_messages = Arc::new(AtomicUsize::new(0));
    let completed_handles = Arc::new(AtomicUsize::new(0));
    let failed_handles = Arc::new(AtomicUsize::new(0));
    let (handle_tx, handle_rx) = mpsc::sync_channel(HANDLE_REAPER_QUEUE_SIZE);
    let reaper_completed_handles = completed_handles.clone();
    let reaper_failed_handles = failed_handles.clone();
    let handle_reaper = thread::spawn(move || {
        reap_handles(handle_rx, reaper_completed_handles, reaper_failed_handles);
    });

    for i in 1..=total_messages {
        let record = format!(r#"{{"id":"{i}","name":"{i}"}}"#);
        let acked_messages_for_callback = acked_messages.clone();

        let handle = client
            .send_with_callback(
                move |result| {
                    if result.success() {
                        acked_messages_for_callback.fetch_add(1, Ordering::Relaxed);
                    }
                },
                record,
            )
            .await?;
        handle_tx.send(handle)?;

        if i % 100_000 == 0 {
            println!(
                "enqueued {} messages, acked {} messages, completed_handles {}",
                i,
                acked_messages.load(Ordering::Relaxed),
                completed_handles.load(Ordering::Relaxed)
            );
        }
    }

    drop(handle_tx);
    client.close().await?;
    handle_reaper
        .join()
        .expect("handle reaper thread should not panic");
    let stats = client.stats();
    let elapsed = started.elapsed();
    println!(
        "finished: enqueued={} acked={} completed_handles={} failed_handles={} elapsed={:.2?}",
        total_messages,
        acked_messages.load(Ordering::Relaxed),
        completed_handles.load(Ordering::Relaxed),
        failed_handles.load(Ordering::Relaxed),
        elapsed
    );
    println!(
        "stats: total_workers={} idle_workers={} busy_workers={} total_load_jobs={} error_jobs={} error_rate={:.4} total_upload_attempts={} total_bytes_sent={} records_sent={} avg_load_size={:.2} avg_bytes_rate={:.2}/s avg_records_rate={:.2}/s avg_load_time={:.2?} p50={:.2?} p90={:.2?} p99={:.2?} avg_retries={:.2}",
        stats.total_workers,
        stats.idle_workers,
        stats.busy_workers,
        stats.total_load_jobs,
        stats.error_jobs,
        stats.error_rate,
        stats.total_upload_attempts,
        stats.total_bytes_sent,
        stats.records_sent,
        stats.average_load_size,
        stats.average_bytes_rate,
        stats.average_records_rate,
        stats.average_load_time,
        stats.p50_load_time,
        stats.p90_load_time,
        stats.p99_load_time,
        stats.average_retries
    );
    Ok(())
}

fn reap_handles(
    handle_rx: Receiver<AsyncHandle>,
    completed_handles: Arc<AtomicUsize>,
    failed_handles: Arc<AtomicUsize>,
) {
    let mut pending = VecDeque::new();
    let mut channel_closed = false;

    loop {
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

        let pending_count = pending.len();
        for _ in 0..pending_count {
            let handle = pending.pop_front().unwrap();
            if handle.is_done() {
                if let Some(result) = handle.result() {
                    if !result.success() {
                        failed_handles.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "batch failed: status={} response={:?}",
                            result.status_code, result.response
                        );
                    }
                }
                completed_handles.fetch_add(1, Ordering::Relaxed);
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
