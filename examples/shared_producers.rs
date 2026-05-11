use doris_rust_stream_load::{Client, Config, Mode, ValidationMode};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

const PRODUCER_THREADS: usize = 10;
const RECORDS_PER_THREAD: usize = 100_000;
const PROGRESS_INTERVAL: usize = 100_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let client = Arc::new(Client::new(cfg)?);
    let accepted = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicUsize::new(0));

    let mut producers = Vec::with_capacity(PRODUCER_THREADS);
    for producer_id in 0..PRODUCER_THREADS {
        let client = client.clone();
        let accepted = accepted.clone();
        let delivered = delivered.clone();

        producers.push(thread::spawn(move || -> Result<(), String> {
            let mut handles = Vec::with_capacity(RECORDS_PER_THREAD);
            for offset in 0..RECORDS_PER_THREAD {
                let id = producer_id * RECORDS_PER_THREAD + offset + 1;
                let record = format!(r#"{{"id":{id},"name":"user-{id}"}}"#);
                let handle = client.send(record).map_err(|e| e.to_string())?;
                let accepted_now = accepted.fetch_add(1, Ordering::Relaxed) + 1;
                if accepted_now % PROGRESS_INTERVAL == 0 {
                    println!("accepted {accepted_now} records");
                }
                handles.push(handle);
            }

            for handle in handles {
                let result = handle.wait();
                if result.success() {
                    delivered.fetch_add(1, Ordering::Relaxed);
                } else {
                    return Err(format!("delivery failed: {result:?}"));
                }
            }
            Ok(())
        }));
    }

    for producer in producers {
        producer
            .join()
            .map_err(|_| "producer thread panicked".to_string())??;
    }

    client.close()?;
    let expected = PRODUCER_THREADS * RECORDS_PER_THREAD;
    println!(
        "finished: producers={} expected={} accepted={} delivered={} elapsed={:.2?}",
        PRODUCER_THREADS,
        expected,
        accepted.load(Ordering::Relaxed),
        delivered.load(Ordering::Relaxed),
        started.elapsed()
    );
    println!("stats: {:?}", client.stats());

    Ok(())
}
