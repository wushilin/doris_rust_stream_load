# doris_rust_stream_load

Rust port of a Doris Stream Load client, modeled after the original Go stream load example.

`doris_rust_stream_load` provides a high-throughput, concurrent stream load client for Apache Doris with batching, validation, retry/backoff, TLS support, and optional fake-send mode for local testing.

Two client variants are provided:

- **`Client`** — synchronous, thread-based. Suitable for blocking code, batch pipelines, and cases where you want simple `handle.wait()` semantics with no async runtime.
- **`AsyncClient`** — fully async, Tokio-based. Designed for high-concurrency async applications where tens of thousands of concurrent senders share one client.

---

## Features

- Bounded request queue with optional enqueue timeout
- Batch assembly by record count and configurable `batch_bytes`
- CSV and JSON payload support
- CSV syntax validation and optional JSON strict validation
- Concurrent upload workers with configurable worker count
- Fake send mode for local testing and development
- HTTP sender with optional TLS certificate configuration
- Custom request headers and basic auth support
- Retry/backoff semantics for upload failures
- Label polling when the response is ambiguous
- Handle-based completion notification for send results
- Runtime statistics via `ClientStats`

---

## Sync Client — Quick Start

```rust
use doris_rust_stream_load::{Client, Config, Mode, ValidationMode};
use std::time::Duration;

let cfg = Config::builder()
    .endpoint("http://doris.example.com")
    .database("test_db")
    .table("test_table")
    .with_columns(["id", "name"])
    .mode(Mode::Csv)
    .validation(ValidationMode::Syntax)
    .fake_send(true)
    .fake_send_delay(Duration::from_millis(10))
    .build()?;

let client = Client::new(cfg)?;
let handle = client.send("1,alice".to_string())?;
let result = handle.wait();
assert!(result.success());
```

---

## Async Client — Quick Start

`AsyncClient` requires a Tokio multi-thread runtime.

```rust
use doris_rust_stream_load::{AsyncClient, Config, Error, Mode, ValidationMode};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::builder()
        .endpoint("http://doris.example.com")
        .database("test_db")
        .table("test_table")
        .with_columns(["id", "name"])
        .mode(Mode::Json)
        .validation(ValidationMode::Syntax)
        .doris_upload_workers(4)
        .batch_bytes(8 * 1024 * 1024)
        .linger(Duration::from_millis(50))
        .build()?;

    let client = AsyncClient::new(cfg)?;

    // send() returns Ok(handle) when the record is accepted into the queue.
    // Err means the record was rejected before any upload was attempted.
    let handle = match client.send(r#"{"id":"1","name":"alice"}"#.to_string()).await {
        Ok(h) => h,
        Err(Error::InvalidRecord(msg)) => {
            eprintln!("record rejected — validation: {msg}");
            return Ok(());
        }
        Err(Error::SendTooLarge) => {
            eprintln!("record exceeds batch_bytes limit");
            return Ok(());
        }
        Err(Error::QueueFull) => {
            // Intake queue was full for longer than max_queue_wait_time.
            // Options: retry with backoff, drop and count, or surface as an error.
            eprintln!("queue full — increase max_queue_size or max_queue_wait_time");
            return Ok(());
        }
        Err(Error::ClientClosed) => {
            eprintln!("client is already closed");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    // wait() resolves as soon as the batch containing this record is uploaded.
    // The DeliveryResult tells you whether the upload itself succeeded.
    let result = handle.wait().await;
    if result.success() {
        println!("delivered in {} attempt(s)", result.attempts);
    } else {
        eprintln!("upload failed after {} attempt(s): {:?}", result.attempts, result.err);
    }

    // Always close before dropping to flush pending records and wait for
    // all in-flight uploads to finish.
    client.close().await?;
    Ok(())
}
```

### send() return semantics

`send().await` (and `send_batch`, `send_with_callback`, `send_batch_with_callback`) separate enqueuing from uploading:

- **`Ok(handle)`** — the record was accepted into the client's internal queue. The upload has **not** happened yet. Await the handle to learn the delivery outcome.
- **`Err(_)`** — the record was **rejected before entering the queue**. Possible reasons:
  - Record is empty or fails format / size validation (`Error::InvalidRecord`, `Error::SendTooLarge`)
  - Queue was full and `max_queue_wait_time` elapsed (`Error::QueueFull`)
  - Client has already been closed (`Error::ClientClosed`)
  
  No upload was attempted in the `Err` case.

### AsyncHandle

```rust
// Async wait — resolves immediately if the batch already completed.
let result: DeliveryResult = handle.wait().await;

// Sync poll — safe to call from a plain OS thread, no Tokio runtime needed.
if handle.is_done() {
    let result = handle.result();
}
```

`AsyncHandle` uses a `tokio::sync::watch` channel internally. `wait()` calls `changed().await`, which compares receiver and sender versions before registering a waker — if the batch already completed, it resolves **without suspending**, making it safe to call at any point after `send()` returns without fear of missing the signal.

---

## Performance Guidance

### Worker count

`doris_upload_workers` controls how many concurrent HTTP upload tasks run simultaneously. A value between 2 and 8 is typical for most Doris clusters. More workers help when upload latency is high (large batches, slow network); too many workers will saturate the Doris frontend.

```rust
.doris_upload_workers(4)
```

### Batch size and linger

Records are coalesced into batches before upload. Two parameters control when a batch is flushed:

- `batch_bytes` — flush as soon as accumulated bytes reach this threshold. 4–20 MB is a common range.
- `linger` — flush after this duration even if `batch_bytes` has not been reached. 50–200 ms balances latency and throughput.

```rust
.batch_bytes(8 * 1024 * 1024)   // 8 MB
.linger(Duration::from_millis(100))
```

### Queue sizing

`max_queue_size` is the number of enqueued submissions the client will buffer before applying backpressure. For high-throughput producers, set this to at least 2× the number of concurrent senders to avoid spurious `QueueFull` errors.

```rust
.max_queue_size(100_000)
.max_queue_wait_time(Duration::from_secs(2))
```

### Async vs sync

- Use `AsyncClient` when you already have a Tokio runtime and want to drive thousands of concurrent senders from async tasks.
- Use `Client` when you want a simple blocking API, e.g. from a Rayon worker pool or a plain OS thread.
- **Do not** call `Client::send` from an async context without `spawn_blocking` — it can block the Tokio thread pool.

### Callback vs handle

`send_with_callback` fires a closure on the Tokio worker thread that completes the batch. Use it when you want fire-and-forget accounting (increment counters, write to a channel) without holding an `AsyncHandle`. Use `handle.wait().await` when you need the result to drive further logic in the same async task.

---

## Shutdown Semantics

Both clients share the same shutdown contract. There are two ways to stop a client:

### `close()` — graceful, confirmed shutdown

```rust
client.close().await?;   // AsyncClient
client.close()?;         // Client
```

- Signals the batcher to stop accepting new submissions.
- Waits for the batcher to flush all queued records into batches.
- Waits for all upload workers to finish their current batch.
- Returns only after every enqueued record has been delivered (or failed with an error).
- Every `handle.wait()` / `handle.wait().await` that was issued before `close()` is guaranteed to have resolved by the time `close()` returns.
- Preferred for production code where you need a clean handoff.

### `drop()` without `close()` — fire-and-forget shutdown

```rust
drop(client);   // or just let it go out of scope
```

- Drops the channel senders, signalling the batcher and workers to drain and stop.
- Returns immediately; background threads/tasks are **detached**, not cancelled.
- The batcher drains the entire intake queue before exiting — no enqueued record is silently discarded.
- Every `Handle` / `AsyncHandle` **will eventually resolve**, with either a successful delivery result or an error (including `Error::Timeout` if the Doris backend is unreachable and the upload/poll timeouts expire).
- For `AsyncClient`, the detached tasks run on the Tokio runtime. If the runtime itself is dropped before the tasks complete, Tokio cancels all remaining tasks and any unresolved handles will hang. As long as the runtime outlives the detached work, all handles resolve.
- Suitable for applications where you want to fire-and-forget without blocking on shutdown, and you can tolerate waiting for timeouts in the worst case.

### Summary

| | `close()` | `drop()` |
|---|---|---|
| Returns | After all work is done | Immediately |
| Pending records | Guaranteed delivered or errored | Delivered or errored eventually |
| Handles resolve | Guaranteed before `close()` returns | Eventually, subject to timeouts |
| Callbacks fire | Guaranteed | Eventually (requires runtime/process to stay alive) |
| New `send()` calls | Rejected (`ClientClosed`) | Not possible (struct is gone) |

---

## Best Practices

**Call `close()` when you need a confirmed, clean shutdown.**

```rust
client.close().await?;
```

**Share one client across all tasks.**
`AsyncClient` is designed to be shared behind an `Arc`. One client per process is the normal pattern; it manages its own internal worker pool.

```rust
let client = Arc::new(AsyncClient::new(cfg)?);
// clone the Arc into each spawned task
```

**Forward handles to a reaper thread for high-throughput fire-and-forget.**
When you need to send tens of millions of records but don't need to block each sender on delivery, pass handles through a channel to a dedicated reaper:

```rust
let (tx, rx) = std::sync::mpsc::sync_channel::<AsyncHandle>(10_000);

// Sender loop
let handle = client.send(record).await?;
tx.send(handle)?;

// Reaper OS thread — no Tokio runtime required
std::thread::spawn(move || {
    let mut pending = std::collections::VecDeque::new();
    loop {
        // drain new handles
        while let Ok(h) = rx.try_recv() { pending.push_back(h); }
        // poll ready ones
        pending.retain(|h| {
            if h.is_done() {
                // inspect h.result()
                false
            } else {
                true
            }
        });
        if pending.is_empty() { break; }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
});
```

**Use `send_with_callback` for lightweight per-record accounting.**
Callbacks run on the Tokio thread that completes the batch. Keep them short — increment an atomic, send to a channel. Do not block inside a callback.

```rust
match client.send_with_callback(
    |result| { if result.success() { counter.fetch_add(1, Ordering::Relaxed); } },
    record,
).await {
    Ok(_handle) => {}  // enqueued; callback fires on delivery
    Err(Error::QueueFull)          => { /* backpressure: retry or drop */ }
    Err(Error::InvalidRecord(msg)) => { eprintln!("bad record: {msg}"); }
    Err(Error::SendTooLarge)       => { eprintln!("record too large"); }
    Err(Error::ClientClosed)       => { /* shut down in progress */ }
    Err(e)                         => { eprintln!("unexpected: {e}"); }
}
```

**Match `max_queue_size` to your concurrency.**
If you spawn N async tasks each calling `send().await`, set `max_queue_size >= N` so producers are never artificially throttled.

---

## Async Usage Sample

```rust
use doris_rust_stream_load::{AsyncClient, Config, Error, LogLevel, Mode, ValidationMode};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;
use tokio::task::JoinSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::builder()
        .endpoint("http://doris.example.com")
        .database("mydb")
        .table("events")
        .with_columns(["id", "name"])
        .mode(Mode::Json)
        .validation(ValidationMode::Syntax)
        .doris_upload_workers(4)
        .batch_bytes(8 * 1024 * 1024)
        .linger(Duration::from_millis(100))
        .max_queue_size(200_000)
        .max_queue_wait_time(Duration::from_secs(5))
        .log_level(LogLevel::Info)
        .build()?;

    let client = Arc::new(AsyncClient::new(cfg)?);
    let acked  = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));

    let mut set = JoinSet::new();
    for i in 0..1_000u32 {
        let client = client.clone();
        let acked  = acked.clone();
        let failed = failed.clone();
        set.spawn(async move {
            let record = format!(r#"{{"id":"{i}","name":"item-{i}"}}"#);

            // ── enqueue ────────────────────────────────────────────────────
            // Ok  → record accepted into the queue; upload is pending.
            // Err → rejected before the queue; no upload was attempted.
            let handle = match client.send(record).await {
                Ok(h) => h,
                Err(Error::InvalidRecord(msg)) => {
                    // The record failed format or column-count validation.
                    // Fix the record before retrying.
                    eprintln!("task {i}: invalid record — {msg}");
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(Error::SendTooLarge) => {
                    // The record alone exceeds batch_bytes. Split it or
                    // increase batch_bytes in the config.
                    eprintln!("task {i}: record too large");
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(Error::QueueFull) => {
                    // The intake queue was full for longer than
                    // max_queue_wait_time. Options: retry with backoff,
                    // drop and count, or propagate as an application error.
                    eprintln!("task {i}: queue full — dropping record");
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(Error::ClientClosed) => {
                    // The client shut down while tasks were still running.
                    // No point retrying; abort the task.
                    return;
                }
                Err(e) => {
                    eprintln!("task {i}: unexpected enqueue error: {e}");
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            // ── delivery ───────────────────────────────────────────────────
            // wait() resolves once the batch containing this record is
            // uploaded. DeliveryResult carries the HTTP status, Doris
            // response body, attempt count, and any upload-level error.
            let result = handle.wait().await;
            if result.success() {
                acked.fetch_add(1, Ordering::Relaxed);
            } else {
                eprintln!(
                    "task {i}: upload failed after {} attempt(s): {:?}",
                    result.attempts, result.err,
                );
                failed.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    set.join_all().await;

    client.close().await?;

    let stats = client.stats();
    println!(
        "acked={} failed={} jobs={} error_rate={:.4}",
        acked.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        stats.total_load_jobs,
        stats.error_rate,
    );
    Ok(())
}
```

---

## Configuration Builder Reference

| Method | Default | Description |
|---|---|---|
| `endpoint(url)` | required | Base URL of the Doris FE, e.g. `http://host:8030` |
| `database(db)` | required | Target database |
| `table(t)` | required | Target table |
| `stream_load_url(url)` | derived | Override the full stream load URL (makes `endpoint`, `database`, `table` optional) |
| `with_columns(iter)` | required | Column list, e.g. `["id", "name"]` |
| `headers(map)` | none | Extra HTTP headers forwarded to every request |
| `mode(Mode)` | `Csv` | `Mode::Csv` or `Mode::Json` |
| `validation(ValidationMode)` | `Syntax` | `None`, `Syntax`, or `Strict` |
| `authentication_type(t)` | `None` | `AuthenticationType::None` or `AuthenticationType::Basic` |
| `authentication_token(s)` | none | `"user:password"` for basic auth |
| `max_queue_size(n)` | 100 000 | Max pending submissions in the intake queue |
| `max_upload_queue_size(n)` | 1 | Dispatch queue depth between the batcher and workers |
| `batch_bytes(n)` | 90 MB | Flush batch when accumulated bytes reach this threshold (hard cap: 90 MB) |
| `linger(d)` | 5 ms | Flush batch after this duration even if `batch_bytes` has not been reached |
| `max_queue_wait_time(d)` | 0 (wait forever) | How long `send()` blocks when the queue is full before returning `QueueFull`; 0 means wait indefinitely |
| `doris_upload_workers(n)` | 4 | Concurrent HTTP upload workers |
| `doris_upload_timeout(d)` | 300 s | Total time budget for a batch including all retries |
| `doris_upload_request_timeout(d)` | 300 s | Per-request HTTP timeout (minimum 10 s) |
| `status_poll_timeout(d)` | 300 s | Timeout for label state polling on ambiguous responses |
| `label_prefix(s)` | `"go_stream_load"` | Prefix for generated stream load labels |
| `fake_send(bool)` | `false` | Skip real HTTP; return a synthetic success response |
| `fake_send_delay(d)` | 500 ms | Simulated latency in fake send mode |
| `csv_separator(s)` | `,` | CSV field separator |
| `csv_quote(s)` | `"` | CSV quote character |
| `slow_callback_warn(d)` | 10 ms | Log a warning when a delivery callback exceeds this duration |
| `logger(fn)` | `eprintln!` | Custom log sink; receives `(LogLevel, &str)` |
| `log_level(LogLevel)` | `Info` | Minimum level to emit: `Error`, `Info`, or `Debug` |
| `tls_skip_verify(bool)` | `false` | Disable TLS certificate verification |
| `tls_ca_cert_path(p)` | none | Custom CA certificate bundle (PEM) |

---

## Project Layout

| Path | Contents |
|---|---|
| `src/lib.rs` | Public API exports |
| `src/client.rs` | Sync client, batching, workers, delivery |
| `src/async_client.rs` | Async client, batcher task, fixed worker pool |
| `src/config.rs` | Configuration, validation, builder |
| `src/queue.rs` | Internal queue and batching semantics |
| `src/sender.rs` | HTTP request construction, response parsing, retry classification |
| `src/types.rs` | API types, handles, results, statistics |
| `tests/` | Integration tests |
| `examples/` | Runnable usage examples |

### Examples

| Example | Description |
|---|---|
| `simple.rs` | Minimal sync example |
| `sample_use.rs` | Full sync example with handle reaping |
| `simple_async.rs` | High-throughput async benchmark (20 M records, callback + OS reaper) |
| `sample_use_async.rs` | Full async example with OS thread reaper |
| `simple_parallel_async.rs` | 50 K concurrent Tokio tasks, configurable via `--threads N` |

```bash
cargo run --example simple
cargo run --example simple_async
cargo run --example simple_parallel_async -- --threads 100000
```

---

## Testing

```bash
cargo test
```
