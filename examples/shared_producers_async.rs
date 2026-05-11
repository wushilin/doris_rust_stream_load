use doris_rust_stream_load::{AsyncClient, Config, Mode, ValidationMode};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const PRODUCER_TASKS: usize = 10;
const RECORDS_PER_TASK: usize = 100_000;
const PROGRESS_INTERVAL: usize = 100_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let cfg = Config::builder()
        .endpoint("http://doris.example.com")
        .database("test_db")
        .table("test_table")
        .with_columns(["id", "name"])
        .mode(Mode::Json)
        .validation(ValidationMode::Syntax)
        .fake_send(true)
        .fake_send_delay(Duration::from_millis(5))
        .doris_upload_workers(4)
        .batch_bytes(1024 * 1024)
        .linger(Duration::from_millis(10))
        .max_queue_size(100_000)
        .max_queue_wait_time(Duration::from_secs(2))
        .build()?;

    let client = Arc::new(AsyncClient::new(cfg)?);
    let accepted = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicUsize::new(0));

    let mut producers = JoinSet::new();
    for producer_id in 0..PRODUCER_TASKS {
        let client = client.clone();
        let accepted = accepted.clone();
        let delivered = delivered.clone();

        producers.spawn(async move {
            let mut handles = Vec::with_capacity(RECORDS_PER_TASK);
            for offset in 0..RECORDS_PER_TASK {
                let id = producer_id * RECORDS_PER_TASK + offset + 1;
                let record = format!(r#"{{"id":{id},"name":"user-{id}"}}"#);
                let handle = client.send(record).await.map_err(|e| e.to_string())?;
                let accepted_now = accepted.fetch_add(1, Ordering::Relaxed) + 1;
                if accepted_now % PROGRESS_INTERVAL == 0 {
                    println!("accepted {accepted_now} records");
                }
                handles.push(handle);
            }

            for handle in handles {
                let result = handle.wait().await;
                if result.success() {
                    delivered.fetch_add(1, Ordering::Relaxed);
                } else {
                    return Err(format!("delivery failed: {result:?}"));
                }
            }
            Ok::<(), String>(())
        });
    }

    while let Some(result) = producers.join_next().await {
        result.map_err(|e| format!("producer task failed to join: {e}"))??;
    }

    client.close().await?;
    let expected = PRODUCER_TASKS * RECORDS_PER_TASK;
    println!(
        "finished: producers={} expected={} accepted={} delivered={} elapsed={:.2?}",
        PRODUCER_TASKS,
        expected,
        accepted.load(Ordering::Relaxed),
        delivered.load(Ordering::Relaxed),
        started.elapsed()
    );
    println!("stats: {:?}", client.stats());

    Ok(())
}
