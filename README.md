# Doris Stream Load Rust SDK

`doris_rust_stream_load` is a Rust SDK for sending CSV rows or JSON objects to Apache Doris Stream Load.

It keeps the public model small:

- `Mode::Csv` and `Mode::Json`
- string input only
- `send(...)` for one item
- `send_batch(...)` for many items
- batching, queueing, retry, label polling, callback, handle, and stats are handled inside the SDK
- sync `Client` and Tokio-based `AsyncClient`

## Requirements

Rust `1.75` or newer.

## Install

Add the crate from crates.io:

```sh
cargo add doris_rust_stream_load@0.1
```

Or add it manually:

```toml
[dependencies]
doris_rust_stream_load = "0.1"
```

Then import the SDK:

```rust
use doris_rust_stream_load::{Client, Config, Mode, ValidationMode};
```

For async applications, enable Tokio in your application:

```toml
[dependencies]
doris_rust_stream_load = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## FakeSend Quick Start

This example is local runnable. It does not require a Doris cluster because `fake_send(true)` bypasses real HTTP upload and returns a successful Stream Load result.

```sh
mkdir doris-rust-stream-load-quickstart
cd doris-rust-stream-load-quickstart
cargo init
cargo add doris_rust_stream_load@0.1
```

Create `src/main.rs`:

```rust
use doris_rust_stream_load::{Client, Config, Mode, ValidationMode};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::builder()
        .stream_load_url("http://example.invalid/api/demo/events/_stream_load")
        .with_columns(["event_time", "user_id", "event_name"])
        .mode(Mode::Csv)
        .validation(ValidationMode::Syntax)
        .batch_bytes(1024 * 1024)
        .linger(Duration::from_millis(10))
        .doris_upload_workers(1)
        .doris_upload_request_timeout(Duration::from_secs(30))
        .doris_upload_timeout(Duration::from_secs(30))
        .fake_send(true)
        .fake_send_delay(Duration::from_millis(20))
        .build()?;

    let client = Client::new(cfg)?;
    let handle = client.send_batch(vec![
        "2026-05-01T10:00:00Z,1,login".to_string(),
        "2026-05-01T10:00:01Z,2,logout".to_string(),
    ])?;

    let result = handle.wait();
    if !result.success() {
        return Err(format!("delivery failed: {:?}", result.err).into());
    }

    println!(
        "success label={:?} attempts={} records={}",
        result.response.and_then(|r| r.label),
        result.attempts,
        client.stats().records_sent,
    );

    client.close()?;
    Ok(())
}
```

Run it:

```sh
cargo run
```

From this repository, the recommended first example is also local-only:

```sh
cargo run --example simple
```

## Async Quick Start

`AsyncClient` requires a Tokio runtime. This quick start also uses FakeSend, so it does not contact a real Doris cluster.

```rust
use doris_rust_stream_load::{AsyncClient, Config, Error, Mode, ValidationMode};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::builder()
        .stream_load_url("http://example.invalid/api/demo/events/_stream_load")
        .with_columns(["event_time", "user_id", "event_name"])
        .mode(Mode::Json)
        .validation(ValidationMode::Syntax)
        .batch_bytes(1024 * 1024)
        .linger(Duration::from_millis(10))
        .doris_upload_workers(1)
        .fake_send(true)
        .fake_send_delay(Duration::from_millis(20))
        .build()?;

    let client = AsyncClient::new(cfg)?;

    let handle = match client
        .send(r#"{"event_time":"2026-05-01T10:00:00Z","user_id":1,"event_name":"login"}"#.to_string())
        .await
    {
        Ok(handle) => handle,
        Err(Error::InvalidRecord(msg)) => {
            eprintln!("record rejected: {msg}");
            return Ok(());
        }
        Err(Error::SendTooLarge) => {
            eprintln!("record exceeds batch_bytes");
            return Ok(());
        }
        Err(Error::QueueFull) => {
            eprintln!("queue full");
            return Ok(());
        }
        Err(Error::ClientClosed) => {
            eprintln!("client closed");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let result = handle.wait().await;
    println!("success={} attempts={}", result.success(), result.attempts);

    client.close().await?;
    Ok(())
}
```

## Real Doris Cluster

For a real cluster, create a table first. The following DDL assumes append-only event data, common time-range queries, and a small/general-purpose cluster. Tune partitions, buckets, and replication for your own data volume and cluster size.

```sql
CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.events (
    event_time DATETIME NOT NULL,
    user_id BIGINT NOT NULL,
    event_name VARCHAR(64) NOT NULL
)
DUPLICATE KEY(event_time, user_id)
PARTITION BY RANGE(event_time) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1",
    "compression" = "zstd"
);
```

Configure the SDK with either a full Stream Load URL:

```rust
use doris_rust_stream_load::{AuthenticationType, Config, Mode};

let cfg = Config::builder()
    .stream_load_url("http://127.0.0.1:8030/api/demo/events/_stream_load")
    .with_columns(["event_time", "user_id", "event_name"])
    .mode(Mode::Csv)
    .authentication_type(AuthenticationType::Basic)
    .authentication_token("root:password")
    .build()?;
```

Or configure endpoint, database, and table separately:

```rust
let cfg = Config::builder()
    .endpoint("http://127.0.0.1:8030")
    .database("demo")
    .table("events")
    .with_columns(["event_time", "user_id", "event_name"])
    .mode(Mode::Json)
    .authentication_type(AuthenticationType::Basic)
    .authentication_token("root:password")
    .build()?;
```

For basic auth, use `authentication_token("user:password")`. If Doris does not require auth, leave `authentication_type` and `authentication_token` unset.

## CSV And JSON

Each submitted string is one logical item.

In `Mode::Csv`, one string is one CSV row:

```rust
client.send("2026-05-01T10:00:00Z,1,login".to_string())?;
client.send_batch(vec![
    "2026-05-01T10:00:01Z,2,logout".to_string(),
    "2026-05-01T10:00:02Z,3,purchase".to_string(),
])?;
```

When several CSV items are coalesced into one outbound Doris request, the body is newline joined.

In `Mode::Json`, one string is one JSON object:

```rust
client.send(r#"{"event_time":"2026-05-01T10:00:00Z","user_id":1,"event_name":"login"}"#.to_string())?;
client.send_batch(vec![
    r#"{"event_time":"2026-05-01T10:00:01Z","user_id":2,"event_name":"logout"}"#.to_string(),
    r#"{"event_time":"2026-05-01T10:00:02Z","user_id":3,"event_name":"purchase"}"#.to_string(),
])?;
```

When several JSON items are coalesced into one outbound Doris request, the body is one JSON array.

Validation is controlled by `Config::builder().validation(...)`:

| Value | CSV behavior | JSON behavior |
|---|---|---|
| `ValidationMode::None` | No parsing before queue admission | No parsing before queue admission |
| `ValidationMode::Syntax` | Non-blank, parses as one row, field count matches `with_columns` | Non-blank, valid JSON object |
| `ValidationMode::Strict` | Same as syntax today | Valid object, every configured column present, no extra keys |

CSV formatting defaults to separator `,` and quote `"`. Override with `csv_separator(...)` and `csv_quote(...)`.

## Callback, Handle, Stats

Every accepted send returns a handle. `send(...)` returning `Ok(handle)` means the record entered the SDK queue; it does not mean Doris has accepted the upload yet. Await or block on the handle for the delivery result.

```rust
let handle = client.send_batch(vec![
    "2026-05-01T10:00:00Z,1,login".to_string(),
    "2026-05-01T10:00:01Z,2,logout".to_string(),
])?;

let result = handle.wait();
if !result.success() {
    return Err(format!("delivery failed: {:?}", result.err).into());
}
```

Supported sync handle methods:

- `wait() -> DeliveryResult`
- `wait_timeout(Duration) -> Option<DeliveryResult>`
- `is_done() -> bool`
- `result() -> Option<DeliveryResult>`

Supported async handle methods:

- `wait().await -> DeliveryResult`
- `is_done() -> bool`
- `result() -> Option<DeliveryResult>`

Callbacks run when the batch containing the submitted item completes:

```rust
let handle = client.send_with_callback(
    |result| {
        if result.success() {
            println!("delivered label={:?}", result.response.and_then(|r| r.label));
        }
    },
    "2026-05-01T10:00:00Z,1,login".to_string(),
)?;
```

`DeliveryResult` includes `err`, `attempts`, `status_code`, `response`, `started_at`, and `finished_at`.

Use `client.stats()` for a lifetime snapshot:

```rust
let stats = client.stats();
println!(
    "jobs={} errors={} records={} bytes={} p99={:?}",
    stats.total_load_jobs,
    stats.error_jobs,
    stats.records_sent,
    stats.total_bytes_sent,
    stats.p99_load_time,
);
```

Stats include worker counts, job counts, error rate, retry average, total upload attempts, bytes/records sent, average rates, and p50/p90/p99 load time.

## Parameters

Required:

| Builder method | Description |
|---|---|
| `with_columns(iter)` | Doris target columns in record order |
| `mode(Mode)` | `Mode::Csv` or `Mode::Json`; defaults to `Mode::Csv` |
| `stream_load_url(url)` | Full URL like `http://host:8030/api/db/table/_stream_load` |
| `endpoint(url)` + `database(db)` + `table(t)` | Alternative to `stream_load_url(url)` |

Connection and auth:

| Builder method | Default | Description |
|---|---|---|
| `authentication_type(t)` | `AuthenticationType::None` | Use `AuthenticationType::Basic` for basic auth |
| `authentication_token(s)` | empty | For basic auth, `user:password` |
| `headers(map)` | empty | Extra HTTP headers sent to Doris |
| `tls_skip_verify(bool)` | `false` | Skip TLS certificate verification |
| `tls_ca_cert_path(path)` | empty | Custom CA certificate bundle or certificate in PEM/DER format |

Batching and queueing:

| Builder method | Default | Description |
|---|---|---|
| `batch_bytes(n)` | `90 MiB` | Max outbound request body size; also the per-send admission limit |
| `linger(d)` | `5ms` | Age at which an open batch is offered to a worker; if every worker is busy and the upload queue is full, the batch keeps accumulating in further `linger` windows until it reaches `batch_bytes` |
| `max_queue_size(n)` | `100000` | Max submitted items in the intake queue |
| `max_queue_wait_time(d)` | `0` | How long `send` waits for queue space; `0` waits indefinitely |
| `max_upload_queue_size(n)` | `1` | Channel depth between batcher and upload workers |
| `doris_upload_workers(n)` | `4` | Concurrent upload workers |

Retry and timing:

| Builder method | Default | Description |
|---|---|---|
| `doris_upload_request_timeout(d)` | `300s` | HTTP deadline for one upload or label-poll request; minimum `10s` |
| `doris_upload_timeout(d)` | `300s` | Total retry budget per batch, covering uploads, label checks, and backoff |
| `max_retries(n)` | `0` (unlimited) | Cap on re-uploads per batch; `0` leaves retries bounded only by `doris_upload_timeout` |
| `status_poll_timeout(d)` | `300s` | Max time spent polling a label after an ambiguous outcome |
| `slow_callback_warn(d)` | `10ms` | Slow callback warning threshold |

Behavior:

| Builder method | Default | Description |
|---|---|---|
| `validation(v)` | `ValidationMode::Syntax` | `None`, `Syntax`, or `Strict` |
| `label_prefix(s)` | `rust_stream_load` | Prefix for generated Doris labels |
| `fake_send(bool)` | `false` | Bypass real HTTP upload and return fake success |
| `fake_send_delay(d)` | `500ms` | Artificial fake-send delay |
| `csv_separator(s)` | `,` | CSV field separator |
| `csv_quote(s)` | `"` | CSV quote character |
| `logger(fn)` | `eprintln!` | Optional logger receiving `(LogLevel, &str)` |
| `log_level(level)` | `LogLevel::Info` | `Error`, `Info`, or `Debug` |

`batch_bytes` and `linger` work together like Kafka `batch.size` and `linger.ms`: the SDK dispatches when the payload reaches `batch_bytes` or the open batch reaches `linger`, whichever happens first.

Dispatch at `linger` is opportunistic. When the open batch reaches `linger`, the batcher hands it over only if a worker or a free `max_upload_queue_size` slot can take it without blocking. If all workers are busy, the batch stays open and keeps absorbing records for another `linger` window, so a saturated cluster receives fewer, larger loads instead of a stream of tiny ones. Only a batch that has reached `batch_bytes` (or cannot fit the next record) makes the batcher wait for a worker.

## Retries and Label Checks

Every upload uses a unique Doris label. When an upload attempt does not come back as a success, the SDK decides what to do in the same way the Flink Doris connector does:

1. A connection failure that never reached Doris is retried directly.
2. An HTTP 401/403 is treated as permanent and fails immediately.
3. Any other failure, including unrecognised Doris errors such as `too many versions` and ambiguous transport errors, triggers a label check via `GET /api/{db}/get_load_state?label=...`:
   - `VISIBLE` / `COMMITTED`: the data landed; the batch is reported as a success.
   - `ABORTED` / `UNKNOWN`: the attempt is dead; the batch is re-uploaded under a fresh label after a backoff of 1s, 2s, 4s, 4s, ...
   - `PREPARE` / `PRECOMMITTED`: the SDK keeps polling up to `status_poll_timeout`.
   - A definitive non-state reply (for example a 4xx from the label endpoint) fails the batch with the original upload error.

Retries stop when `doris_upload_timeout` elapses (the result carries `Error::Timeout` plus the last Doris response) or, if `max_retries` is set, after that many re-uploads (the result carries the last Doris error). Each retry is logged at `LogLevel::Info`.

## Common Errors

`send(...)` and `send_batch(...)` can fail before data enters the SDK queue:

| Error | Meaning | Typical action |
|---|---|---|
| `Error::ClientClosed` | The client is closed | Stop sending or create a new client |
| `Error::QueueFull` | The intake queue stayed full longer than `max_queue_wait_time` | Increase workers, queue size, or timeout; reduce producer rate |
| `Error::SendTooLarge` | One submitted item or batch is larger than `batch_bytes` | Split the caller-side batch or raise `batch_bytes` up to the 90 MiB limit |
| `Error::InvalidRecord` | CSV/JSON failed configured validation | Fix the row/object or loosen validation |

Delivery can fail after queue admission; check `DeliveryResult.err` from the handle or callback:

| Failure | Meaning | Typical action |
|---|---|---|
| HTTP 401/403 from Doris | Bad credentials or missing privileges | Fails immediately without retry; fix `authentication_token` |
| Doris `Status: Fail` or other HTTP 4xx/5xx | Schema, data format, `too many versions`, memory limit, BE unavailable, ... | The SDK checks the label and re-uploads under a fresh label until `doris_upload_timeout` / `max_retries`; inspect `response` for the last Doris message |
| Connection failure | Doris/FE/network unreachable | The SDK retries within `doris_upload_timeout`; check cluster health |
| Ambiguous transport error | Request may have reached Doris but response was lost | The SDK polls the load label to decide whether it became visible |
| Label state `ABORTED` / `UNKNOWN` | The attempt did not commit | The batch is retried with a fresh label |
| Status poll timeout | Doris did not reach a final visible/failed state in time | Increase `status_poll_timeout` or inspect Doris load jobs |
| `Error::Timeout` | Retries ran out of `doris_upload_timeout` | `response` holds the last Doris reply; check cluster health, compaction, or data |
| Slow callback | Callback exceeded `slow_callback_warn` and produced a log | Keep callbacks small; hand work to another thread/task if needed |

## Shutdown

`close()` stops intake, drains accepted work, and waits for in-flight deliveries:

```rust
client.close()?;        // Client
client.close().await?;  // AsyncClient
```

After `close()`, new sends return `Error::ClientClosed`; already accepted handles still complete.

Dropping a client without `close()` signals background workers to drain, but the caller does not wait for confirmation. For production code, call `close()`.

## Examples

All repository examples are configured with `fake_send(true)` by default. They are safe to run locally and do not send data to Doris, even when an example contains a Doris-looking endpoint.

To use any example for a real Stream Load request, you must change the configuration:

- Set `fake_send(false)` or remove the `fake_send(true)` line
- Replace `stream_load_url(...)`, or `endpoint(...)` + `database(...)` + `table(...)`, with your real Doris FE address and target table
- Configure authentication if your Doris cluster requires it
- Review record counts before running benchmark examples

| Example | Purpose |
|---|---|
| `simple.rs` | Small local FakeSend sync example; recommended first run |
| `async_full_config.rs` | Async configuration, callbacks, handle reaping, and stats |
| `shared_producers.rs` | Shared sync client used by multiple producer threads |
| `shared_producers_async.rs` | Shared async client used by multiple producer tasks |
| `sync_throughput_benchmark.rs` | High-throughput sync benchmark-style FakeSend run |
| `async_throughput_benchmark.rs` | High-throughput async benchmark-style FakeSend run |
| `async_parallel_benchmark.rs` | Many concurrent Tokio tasks; configurable with `--threads N` |

```sh
cargo run --example simple
cargo run --example async_full_config
cargo run --example sync_throughput_benchmark
cargo run --example async_throughput_benchmark
cargo run --example async_parallel_benchmark -- --threads 100000
```

## TLS

The SDK uses reqwest with rustls by default (`default-features = false`, `rustls-tls`). It does not require OpenSSL to build.

Custom CA files passed to `tls_ca_cert_path(...)` are treated as a truststore. Supported formats:

- PEM bundle with one or more certificates
- Single DER-encoded certificate

PKCS#12 / PFX truststores are not supported.

Example PEM bundle:

```pem
-----BEGIN CERTIFICATE-----
... root CA certificate ...
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
... intermediate CA certificate ...
-----END CERTIFICATE-----
```

Use it from config:

```rust
let cfg = Config::builder()
    .stream_load_url("https://doris.example.com/api/demo/events/_stream_load")
    .with_columns(["event_time", "user_id", "event_name"])
    .tls_ca_cert_path("certs/doris-ca-bundle.pem")
    .build()?;
```

## Testing

```sh
cargo test
```

## Changelog

### 0.1.3 - 2026-09-08

- Consult the load label after any failed upload and retry with a fresh
  label when Doris reports the label as `ABORTED` or `UNKNOWN`. Unrecognised
  Doris errors such as `too many versions` no longer fail the batch on the
  first attempt; only 401/403 fail immediately.
- Add `max_retries` to cap re-uploads per batch (default unlimited within
  `doris_upload_timeout`) and log each retry at `Info`.
- Stop label polling early on definitive non-state replies instead of
  waiting for `status_poll_timeout`.
- Offer a lingered batch to workers without blocking. When every worker is
  busy and the upload queue is full, the batch keeps accumulating in further
  `linger` windows until it reaches `batch_bytes`, which avoids many small
  loads under sustained producer pressure.
- Export `ConfigBuilder`.

### 0.1.2 - 2026-06-10

- Make successful `AsyncClient` queue admission participate in Tokio's
  cooperative task budget, preventing sustained producers from starving the
  batcher and uploader tasks.
- Add a single-threaded Tokio regression test for async send fairness.
