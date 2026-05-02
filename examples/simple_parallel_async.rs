use doris_rust_stream_load::{AsyncClient, Config, LogLevel, Mode, ValidationMode};
use reqwest::header::{HeaderMap, HeaderValue};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const TOTAL_MESSAGES: usize = 20_000_000;
const DEFAULT_PARALLEL_TASKS: usize = 50_000;
const PROGRESS_INTERVAL: usize = 100_000;

fn parse_args() -> usize {
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--threads" {
            if let Some(val) = args.get(i + 1) {
                if let Ok(n) = val.parse::<usize>() {
                    return n;
                }
            }
        }
    }
    DEFAULT_PARALLEL_TASKS
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let parallel_tasks = parse_args();
    let started = Instant::now();

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-custom-header",
        HeaderValue::from_static("rust-example-parallel-async"),
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
        .max_queue_size(parallel_tasks * 2)
        .max_queue_wait_time(Duration::from_secs(2))
        .status_poll_timeout(Duration::from_secs(10))
        .log_level(LogLevel::Debug)
        .headers(headers)
        .build()?;

    let client = Arc::new(AsyncClient::new(cfg)?);

    // enqueued: send() was called (attempt count, before we know the outcome)
    //
    // sent:     send() returned Ok — the record was accepted into the client
    //           queue and will be uploaded in a future batch. The upload has
    //           NOT happened yet at this point.
    //           send() returns Err when the record is rejected *before* the
    //           queue: validation failure, record too large, queue-full
    //           timeout, or client already closed.
    //
    // acked:    handle.wait() resolved with a successful DeliveryResult —
    //           the batch containing this record was uploaded to Doris.
    let enqueued = Arc::new(AtomicUsize::new(0));
    let sent = Arc::new(AtomicUsize::new(0));
    let acked = Arc::new(AtomicUsize::new(0));

    let records_per_task = TOTAL_MESSAGES.div_ceil(parallel_tasks);
    let mut set = JoinSet::new();

    for task_id in 0..parallel_tasks {
        let client = client.clone();
        let enqueued = enqueued.clone();
        let sent = sent.clone();
        let acked = acked.clone();
        let base = task_id * records_per_task;

        set.spawn(async move {
            let end = (base + records_per_task).min(TOTAL_MESSAGES);
            for i in base..end {
                let id = i + 1;
                let record = format!(r#"{{"id":"{id}","name":"{id}"}}"#);

                enqueued.fetch_add(1, Ordering::Relaxed);

                match client.send(record).await {
                    Err(_) => {}
                    Ok(handle) => {
                        let new_sent = sent.fetch_add(1, Ordering::Relaxed) + 1;
                        if new_sent % PROGRESS_INTERVAL == 0 {
                            println!(
                                "progress: enqueued={} sent={} acked={}",
                                enqueued.load(Ordering::Relaxed),
                                new_sent,
                                acked.load(Ordering::Relaxed),
                            );
                        }

                        let result = handle.wait().await;
                        if result.success() {
                            acked.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }

    set.join_all().await;
    client.close().await?;

    let stats = client.stats();
    let elapsed = started.elapsed();
    println!(
        "finished: total={} enqueued={} sent={} acked={} elapsed={:.2?}",
        TOTAL_MESSAGES,
        enqueued.load(Ordering::Relaxed),
        sent.load(Ordering::Relaxed),
        acked.load(Ordering::Relaxed),
        elapsed,
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
        stats.average_retries,
    );
    Ok(())
}
