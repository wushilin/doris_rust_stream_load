use crate::config::{AuthenticationType, Config, LogLevel, Mode, ValidationMode};
use crate::errors::{Error, Result};
use crate::queue::{generate_label, DeliveryBatch, QueueItem, QueuedSubmission};
use crate::sender::{
    classify_response_error, classify_transport_error, is_http_success, is_redirect, load_ca_certs,
    resolve_redirect_url, LoadStateResponse, SendOutcome, StreamLoadError,
};
use crate::types::{
    ClientStats, CompletionSink, DeliveryCallback, DeliveryResult, StreamLoadResponse,
};
use reqwest::header::{CONTENT_LENGTH, LOCATION};
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

// ── Async completion ──────────────────────────────────────────────────────────

pub(crate) struct AsyncBatchCompletion {
    tx: watch::Sender<Option<DeliveryResult>>,
}

impl CompletionSink for AsyncBatchCompletion {
    fn complete(&self, result: DeliveryResult) {
        let _ = self.tx.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(result);
            true
        });
    }
}

/// Handle returned by [`AsyncClient`] send methods.
///
/// - [`AsyncHandle::wait`] — async, consumes the handle, returns [`DeliveryResult`].
/// - [`AsyncHandle::is_done`] — sync, no Tokio runtime required.
/// - [`AsyncHandle::result`] — sync, returns the result if already done.
#[derive(Clone)]
pub struct AsyncHandle {
    rx: watch::Receiver<Option<DeliveryResult>>,
}

impl AsyncHandle {
    fn new() -> (Self, Arc<AsyncBatchCompletion>) {
        let (tx, rx) = watch::channel(None);
        (Self { rx }, Arc::new(AsyncBatchCompletion { tx }))
    }

    /// Await delivery completion and return the result.
    ///
    /// Non-consuming — safe to call multiple times and from cloned handles.
    /// Clones the internal `watch::Receiver` so concurrent calls on the same
    /// handle do not race on version state. Resolves immediately if the batch
    /// already completed before this call.
    pub async fn wait(&self) -> DeliveryResult {
        let mut rx = self.rx.clone();
        if let Some(r) = rx.borrow_and_update().clone() {
            return r;
        }
        rx.changed()
            .await
            .expect("completion sender dropped without completing");
        let result = rx.borrow().clone();
        result.expect("value must be Some after watch change")
    }

    /// Returns `true` if the delivery result is available.
    pub fn is_done(&self) -> bool {
        self.rx.borrow().is_some()
    }

    /// Returns the delivery result if already done, or `None` if still pending.
    pub fn result(&self) -> Option<DeliveryResult> {
        self.rx.borrow().clone()
    }
}

// ── Async senders ─────────────────────────────────────────────────────────────

struct AsyncFakeSender {
    delay: Duration,
}

impl AsyncFakeSender {
    async fn send(
        &self,
        batch: &DeliveryBatch,
        _timeout: Duration,
    ) -> std::result::Result<SendOutcome, StreamLoadError> {
        if self.delay > Duration::ZERO {
            tokio::time::sleep(self.delay).await;
        }
        Ok(SendOutcome {
            status_code: 200,
            response: Some(StreamLoadResponse {
                label: Some(batch.label.clone()),
                status: Some("Success".to_string()),
                message: Some("fake send success".to_string()),
                number_total_rows: Some(batch.len() as i64),
                number_loaded_rows: Some(batch.len() as i64),
                load_bytes: Some(batch.byte_size as i64),
                load_time_ms: Some(self.delay.as_millis() as i64),
                ..Default::default()
            }),
        })
    }

    async fn poll_label(
        &self,
        _label: &str,
        _timeout: Duration,
    ) -> std::result::Result<LoadStateResponse, StreamLoadError> {
        Ok(LoadStateResponse {
            status_code: 200,
            state: "VISIBLE".to_string(),
        })
    }
}

struct AsyncHttpSender {
    cfg: Config,
    client: reqwest::Client,
}

impl AsyncHttpSender {
    fn new(cfg: Config) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .danger_accept_invalid_certs(cfg.tls_skip_verify)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(path) = cfg.tls_ca_cert_path.as_ref() {
            let data = std::fs::read(path)
                .map_err(|e| Error::InvalidConfig(format!("failed to read tls ca cert: {e}")))?;
            for cert in load_ca_certs(&data)? {
                builder = builder.add_root_certificate(cert);
            }
        }
        let client = builder
            .build()
            .map_err(|e| Error::InvalidConfig(format!("failed to build http client: {e}")))?;
        Ok(Self { cfg, client })
    }

    async fn send(
        &self,
        batch: &DeliveryBatch,
        timeout: Duration,
    ) -> std::result::Result<SendOutcome, StreamLoadError> {
        let body_bytes = batch.encode_body();
        let mut url = self.cfg.stream_load_url();
        let mut redirect_count = 0u32;

        let response = loop {
            let builder = self.apply_stream_load_headers(
                self.client
                    .put(&url)
                    .body(body_bytes.clone())
                    .timeout(timeout),
                batch,
                body_bytes.len(),
                redirect_count <= 1,
            );
            let response = builder.send().await.map_err(classify_transport_error)?;

            if !is_redirect(response.status().as_u16()) {
                break response;
            }
            if redirect_count >= 10 {
                return Err(StreamLoadError::Error {
                    status_code: response.status().as_u16(),
                    message: "too many redirects".to_string(),
                    retriable: false,
                    ambiguous: true,
                    response: None,
                });
            }
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or(StreamLoadError::Error {
                    status_code: response.status().as_u16(),
                    message: "redirect response missing Location header".to_string(),
                    retriable: false,
                    ambiguous: true,
                    response: None,
                })?
                .to_string();
            url = resolve_redirect_url(&url, &location)?;
            redirect_count += 1;
        };

        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        let parsed: Option<StreamLoadResponse> = serde_json::from_str(&body).ok();

        if (200..300).contains(&status) {
            if body.is_empty() {
                return Err(StreamLoadError::Error {
                    status_code: status,
                    message: "missing stream load response body".to_string(),
                    retriable: false,
                    ambiguous: true,
                    response: None,
                });
            }
            if parsed.is_none() {
                return Err(StreamLoadError::Error {
                    status_code: status,
                    message: format!("invalid stream load response body: {}", body.trim()),
                    retriable: false,
                    ambiguous: true,
                    response: None,
                });
            }
        }

        let outcome = SendOutcome {
            status_code: status,
            response: parsed.clone(),
        };
        if !is_http_success(status, parsed.as_ref()) {
            return Err(classify_response_error(status, body, parsed));
        }
        Ok(outcome)
    }

    async fn poll_label(
        &self,
        label: &str,
        timeout: Duration,
    ) -> std::result::Result<LoadStateResponse, StreamLoadError> {
        let database = self.cfg.poll_database().ok_or(StreamLoadError::Error {
            status_code: 0,
            message: "database is required to poll load label state".to_string(),
            retriable: false,
            ambiguous: false,
            response: None,
        })?;
        let url = self.cfg.load_state_url(&database, label);
        let mut builder = self.client.get(&url).timeout(timeout);
        if let AuthenticationType::Basic = self.cfg.authentication_type {
            if let Some(token) = self.cfg.authentication_token.as_ref() {
                if let Some((username, password)) = token.split_once(':') {
                    builder = builder.basic_auth(username, Some(password));
                }
            }
        }
        for (key, value) in self.cfg.headers.iter() {
            builder = builder.header(key, value.clone());
        }
        let response = builder.send().await.map_err(classify_transport_error)?;
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();

        #[derive(serde::Deserialize)]
        struct PollResponse {
            msg: Option<String>,
            code: Option<serde_json::Value>,
            data: Option<String>,
        }

        let parsed: PollResponse =
            serde_json::from_str(&body).map_err(|_| StreamLoadError::Error {
                status_code: status,
                message: format!("invalid load state response: {}", body.trim()),
                retriable: false,
                ambiguous: false,
                response: None,
            })?;

        let state = parsed.data.unwrap_or_default();
        if !state.trim().is_empty() {
            return Ok(LoadStateResponse {
                status_code: status,
                state: state.trim().to_string(),
            });
        }
        Err(StreamLoadError::Error {
            status_code: status,
            message: format!(
                "load state request failed: msg={} code={}",
                parsed.msg.unwrap_or_default(),
                parsed.code.map(|v| v.to_string()).unwrap_or_default()
            ),
            retriable: false,
            ambiguous: false,
            response: None,
        })
    }

    fn apply_stream_load_headers(
        &self,
        mut builder: reqwest::RequestBuilder,
        batch: &DeliveryBatch,
        content_length: usize,
        include_basic_auth: bool,
    ) -> reqwest::RequestBuilder {
        let content_type = match batch.mode {
            Mode::Csv => "text/csv",
            Mode::Json => "application/json",
        };
        builder = builder.header("Content-Type", content_type);
        builder = builder.header("columns", self.cfg.columns.join(","));
        builder = builder.header("Expect", "100-continue");
        builder = builder.header("label", batch.label.clone());
        builder = builder.header("format", batch.header_format());
        builder = builder.header(CONTENT_LENGTH, content_length.to_string());
        if let Mode::Json = batch.mode {
            builder = builder.header("strip_outer_array", "true");
            builder = builder.header("read_json_by_line", "false");
        } else {
            builder = builder.header("column_separator", self.cfg.csv_separator.clone());
            builder = builder.header("enclose", self.cfg.csv_quote.clone());
        }
        for (key, value) in self.cfg.headers.iter() {
            builder = builder.header(key, value.clone());
        }
        if include_basic_auth {
            if let AuthenticationType::Basic = self.cfg.authentication_type {
                if let Some(token) = self.cfg.authentication_token.as_ref() {
                    if let Some((username, password)) = token.split_once(':') {
                        builder = builder.basic_auth(username, Some(password));
                    }
                }
            }
        }
        builder
    }
}

// Wraps both sender kinds so they can be stored in an Arc.
enum AsyncSenderKind {
    Http(AsyncHttpSender),
    Fake(AsyncFakeSender),
}

impl AsyncSenderKind {
    async fn send(
        &self,
        batch: &DeliveryBatch,
        timeout: Duration,
    ) -> std::result::Result<SendOutcome, StreamLoadError> {
        match self {
            Self::Http(s) => s.send(batch, timeout).await,
            Self::Fake(s) => s.send(batch, timeout).await,
        }
    }

    async fn poll_label(
        &self,
        label: &str,
        timeout: Duration,
    ) -> std::result::Result<LoadStateResponse, StreamLoadError> {
        match self {
            Self::Http(s) => s.poll_label(label, timeout).await,
            Self::Fake(s) => s.poll_label(label, timeout).await,
        }
    }
}

// ── Stats ─────────────────────────────────────────────────────────────────────

struct AsyncStatsCollector {
    started_at: SystemTime,
    busy_workers: AtomicI64,
    total_load_jobs: AtomicI64,
    error_jobs: AtomicI64,
    total_retries: AtomicI64,
    total_bytes_sent: AtomicI64,
    total_records_sent: AtomicI64,
    total_upload_attempts: AtomicI64,
    total_load_time_nanos: AtomicI64,
    durations: Mutex<VecDeque<Duration>>,
}

impl AsyncStatsCollector {
    fn new(started_at: SystemTime) -> Self {
        Self {
            started_at,
            busy_workers: AtomicI64::new(0),
            total_load_jobs: AtomicI64::new(0),
            error_jobs: AtomicI64::new(0),
            total_retries: AtomicI64::new(0),
            total_bytes_sent: AtomicI64::new(0),
            total_records_sent: AtomicI64::new(0),
            total_upload_attempts: AtomicI64::new(0),
            total_load_time_nanos: AtomicI64::new(0),
            durations: Mutex::new(VecDeque::new()),
        }
    }

    fn record_upload_attempt(&self, bytes: i64, records: i64) {
        self.total_upload_attempts.fetch_add(1, Ordering::SeqCst);
        self.total_bytes_sent.fetch_add(bytes, Ordering::SeqCst);
        self.total_records_sent.fetch_add(records, Ordering::SeqCst);
    }

    fn record_completion(&self, result: &DeliveryResult) {
        self.total_load_jobs.fetch_add(1, Ordering::SeqCst);
        if result.err.is_some() {
            self.error_jobs.fetch_add(1, Ordering::SeqCst);
        }
        if result.attempts > 1 {
            self.total_retries
                .fetch_add((result.attempts - 1) as i64, Ordering::SeqCst);
        }
        if let Ok(d) = result.finished_at.duration_since(result.started_at) {
            self.total_load_time_nanos
                .fetch_add(d.as_nanos() as i64, Ordering::SeqCst);
            let mut durations = self.durations.lock().unwrap();
            if durations.len() == crate::client::MAX_STATS_SAMPLES {
                durations.pop_front();
            }
            durations.push_back(d);
        }
    }

    fn snapshot(&self, total_workers: usize) -> ClientStats {
        let total_jobs = self.total_load_jobs.load(Ordering::SeqCst);
        let error_jobs = self.error_jobs.load(Ordering::SeqCst);
        let busy_workers = self.busy_workers.load(Ordering::SeqCst).max(0) as usize;
        let elapsed = self
            .started_at
            .elapsed()
            .unwrap_or(Duration::from_secs(1))
            .as_secs_f64()
            .max(1.0);
        let total_bytes_sent = self.total_bytes_sent.load(Ordering::SeqCst);
        let total_records_sent = self.total_records_sent.load(Ordering::SeqCst);
        let total_upload_attempts = self.total_upload_attempts.load(Ordering::SeqCst);
        let average_load_size = if total_upload_attempts > 0 {
            total_bytes_sent as f64 / total_upload_attempts as f64
        } else {
            0.0
        };
        let average_retries = if total_jobs > 0 {
            self.total_retries.load(Ordering::SeqCst) as f64 / total_jobs as f64
        } else {
            0.0
        };
        let average_load_time = if total_jobs > 0 {
            Duration::from_nanos(
                (self.total_load_time_nanos.load(Ordering::SeqCst) / total_jobs) as u64,
            )
        } else {
            Duration::ZERO
        };
        let mut durations: Vec<_> = self.durations.lock().unwrap().iter().copied().collect();
        durations.sort();
        let p50 = crate::client::percentile_duration(&durations, 0.50);
        let p90 = crate::client::percentile_duration(&durations, 0.90);
        let p99 = crate::client::percentile_duration(&durations, 0.99);
        let p999 = crate::client::percentile_duration(&durations, 0.999);

        ClientStats {
            started_at: self.started_at,
            total_workers,
            idle_workers: total_workers.saturating_sub(busy_workers),
            busy_workers,
            total_load_jobs: total_jobs,
            error_jobs,
            error_rate: if total_jobs > 0 {
                error_jobs as f64 / total_jobs as f64
            } else {
                0.0
            },
            average_load_time,
            p50_load_time: p50,
            p90_load_time: p90,
            p99_load_time: p99,
            p999_load_time: p999,
            average_retries,
            total_bytes_sent,
            average_load_size,
            average_bytes_rate: total_bytes_sent as f64 / elapsed,
            records_sent: total_records_sent,
            average_records_rate: total_records_sent as f64 / elapsed,
            total_upload_attempts,
        }
    }
}

// ── AsyncClient ───────────────────────────────────────────────────────────────

/// Async version of [`Client`](crate::Client). Requires a Tokio runtime.
///
/// Spawn with [`AsyncClient::new`] from within a running runtime, then call
/// `send`/`send_batch` and their `_with_callback` variants. Call
/// [`AsyncClient::close`] before dropping for graceful shutdown.
pub struct AsyncClient {
    cfg: Config,
    intake_tx: Mutex<Option<mpsc::Sender<QueuedSubmission>>>,
    stats: Arc<AsyncStatsCollector>,
    closed: AtomicBool,
    batcher_handle: Mutex<Option<JoinHandle<()>>>,
    worker_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl AsyncClient {
    /// Create a new async client. Must be called from within a Tokio runtime.
    pub fn new(mut cfg: Config) -> Result<Self> {
        cfg = cfg.with_defaults();
        cfg.validate()?;
        if cfg.stream_load_url.is_some() && !cfg.stream_load_url_has_suffix() {
            cfg.log(
                LogLevel::Info,
                "stream_load_url does not end with _stream_load; \
                 this may not be a valid Doris stream load endpoint",
            );
        }
        let sender = Arc::new(if cfg.fake_send {
            AsyncSenderKind::Fake(AsyncFakeSender {
                delay: cfg.fake_send_delay,
            })
        } else {
            AsyncSenderKind::Http(AsyncHttpSender::new(cfg.clone())?)
        });
        let (intake_tx, intake_rx) = mpsc::channel(cfg.max_queue_size);
        let (dispatch_tx, dispatch_rx) =
            mpsc::channel::<DeliveryBatch>(cfg.max_upload_queue_size.max(1));
        let dispatch_rx = Arc::new(AsyncMutex::new(dispatch_rx));

        let n_workers = cfg.doris_upload_workers.max(1);
        let stats = Arc::new(AsyncStatsCollector::new(SystemTime::now()));
        let mut worker_handles = Vec::with_capacity(n_workers);
        for _ in 0..n_workers {
            let rx = dispatch_rx.clone();
            let s = sender.clone();
            let st = stats.clone();
            let c = cfg.clone();
            worker_handles.push(tokio::spawn(run_async_worker(rx, s, st, c)));
        }

        let batcher_handle = tokio::spawn(run_async_batcher(
            intake_rx,
            dispatch_tx,
            stats.clone(),
            cfg.clone(),
        ));
        Ok(Self {
            cfg,
            intake_tx: Mutex::new(Some(intake_tx)),
            stats,
            closed: AtomicBool::new(false),
            batcher_handle: Mutex::new(Some(batcher_handle)),
            worker_handles: Mutex::new(worker_handles),
        })
    }

    /// Enqueue a single record for batched delivery.
    ///
    /// # Return value
    /// - `Ok(handle)` — the record was **accepted into the client queue**.
    ///   The upload has not happened yet. Call [`AsyncHandle::wait`] to block
    ///   until the batch containing this record has been uploaded and to
    ///   inspect the [`DeliveryResult`].
    /// - `Err(_)` — the record was **rejected before it entered the queue**.
    ///   Possible reasons: record is empty or fails format/size validation
    ///   ([`Error::InvalidRecord`], [`Error::SendTooLarge`]); the queue was
    ///   full and the configured `max_queue_wait_time` elapsed
    ///   ([`Error::QueueFull`]); or the client has already been closed
    ///   ([`Error::ClientClosed`]).  No upload was attempted.
    pub async fn send(&self, record: String) -> Result<AsyncHandle> {
        self.send_internal(vec![record], None, None).await
    }

    /// Enqueue a batch of records for delivery as a single logical unit.
    ///
    /// # Return value
    /// - `Ok(handle)` — all records were **accepted into the client queue**.
    ///   The handle resolves once the entire batch has been uploaded.
    /// - `Err(_)` — rejected before entering the queue (validation, size,
    ///   queue-full timeout, or client closed). No upload was attempted.
    pub async fn send_batch(&self, records: Vec<String>) -> Result<AsyncHandle> {
        self.send_internal(records, None, None).await
    }

    /// Enqueue a single record; `callback` is invoked once delivery completes.
    ///
    /// # Return value
    /// Same semantics as [`send`](Self::send): `Ok` means enqueued, `Err`
    /// means rejected before the queue.  The callback fires regardless of
    /// upload success or failure and receives the [`DeliveryResult`].
    pub async fn send_with_callback<F>(&self, callback: F, record: String) -> Result<AsyncHandle>
    where
        F: Fn(DeliveryResult) + Send + Sync + 'static,
    {
        self.send_internal(vec![record], Some(Arc::new(callback)), None)
            .await
    }

    /// Enqueue a batch of records; `callback` is invoked once delivery completes.
    ///
    /// # Return value
    /// Same semantics as [`send_batch`](Self::send_batch): `Ok` means
    /// enqueued, `Err` means rejected before the queue.
    pub async fn send_batch_with_callback<F>(
        &self,
        callback: F,
        records: Vec<String>,
    ) -> Result<AsyncHandle>
    where
        F: Fn(DeliveryResult) + Send + Sync + 'static,
    {
        self.send_internal(records, Some(Arc::new(callback)), None)
            .await
    }

    /// Gracefully shut down: flushes all pending records and waits for
    /// in-flight uploads to complete.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Signal batcher to stop accepting new submissions.
        let _ = self.intake_tx.lock().unwrap().take();

        // Wait for the batcher to flush all queued batches and exit.
        let batcher_handle = self.batcher_handle.lock().unwrap().take();
        if let Some(h) = batcher_handle {
            h.await
                .map_err(|_| Error::Internal("batcher task panicked".into()))?;
        }

        // Wait for all workers to drain.
        let handles = std::mem::take(&mut *self.worker_handles.lock().unwrap());
        for h in handles {
            h.await
                .map_err(|_| Error::Internal("worker task panicked".into()))?;
        }
        Ok(())
    }

    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> ClientStats {
        self.stats.snapshot(self.cfg.doris_upload_workers)
    }

    async fn send_internal(
        &self,
        records: Vec<String>,
        callback: Option<DeliveryCallback>,
        enqueue_timeout: Option<Duration>,
    ) -> Result<AsyncHandle> {
        if records.is_empty() {
            return Err(Error::InvalidRecord(
                "at least one record is required".into(),
            ));
        }
        self.validate_records(&records)
            .map_err(Error::InvalidRecord)?;

        let (handle, completion) = AsyncHandle::new();
        let payload_bytes: usize = records.iter().map(|r| r.len()).sum();
        let items: Vec<QueueItem> = records
            .into_iter()
            .map(|r| QueueItem {
                byte_size: r.len(),
                payload: r,
            })
            .collect();
        let submission =
            QueuedSubmission::new(&self.cfg.mode, items, payload_bytes, completion, callback);

        if self.cfg.batch_bytes > 0 && submission.standalone_byte_size > self.cfg.batch_bytes {
            return Err(Error::SendTooLarge);
        }
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::ClientClosed);
        }

        let tx = self
            .intake_tx
            .lock()
            .unwrap()
            .clone()
            .ok_or(Error::ClientClosed)?;

        // Always await channel admission so sustained producers participate in
        // Tokio's cooperative scheduling even while the queue has capacity.
        let timeout = enqueue_timeout.or_else(|| {
            (!self.cfg.max_queue_wait_time.is_zero()).then_some(self.cfg.max_queue_wait_time)
        });
        match timeout {
            None => tx.send(submission).await.map_err(|_| Error::ClientClosed)?,
            Some(t) => match tokio::time::timeout(t, tx.send(submission)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(Error::ClientClosed),
                Err(_) => return Err(Error::QueueFull),
            },
        }

        Ok(handle)
    }

    fn validate_records(&self, records: &[String]) -> std::result::Result<(), String> {
        for record in records {
            if record.trim().is_empty() {
                return Err("record cannot be empty".to_string());
            }
        }
        match self.cfg.mode {
            Mode::Csv => {
                if self.cfg.validation != ValidationMode::None {
                    crate::client::validate_csv_records(
                        records,
                        self.cfg.columns.len(),
                        self.cfg.csv_separator.as_bytes()[0],
                        self.cfg.csv_quote.as_bytes()[0],
                    )
                    .map_err(|e| e.to_string())?;
                }
            }
            Mode::Json => {
                if self.cfg.validation != ValidationMode::None {
                    for record in records {
                        crate::client::validate_json_record(
                            record,
                            &self.cfg.columns,
                            self.cfg.validation == ValidationMode::Strict,
                        )
                        .map_err(|e| e.to_string())?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for AsyncClient {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        // Drop intake to signal shutdown. The batcher owns the only dispatch
        // sender, so workers see dispatch EOF after the batcher drains and
        // exits. JoinHandles are dropped (detached), not aborted, so tasks run
        // to completion on the runtime. The runtime must outlive the tasks; if
        // it is dropped first, all pending tasks are cancelled by Tokio.
        let _ = self.intake_tx.lock().map(|mut g| g.take());
        // batcher_handle and worker_handles are dropped (detached) here.
    }
}

// ── Batcher task ──────────────────────────────────────────────────────────────

async fn run_async_batcher(
    mut intake_rx: mpsc::Receiver<QueuedSubmission>,
    dispatch_tx: mpsc::Sender<DeliveryBatch>,
    stats: Arc<AsyncStatsCollector>,
    cfg: Config,
) {
    let mut current: Option<DeliveryBatch> = None;

    // Single pinned sleep reused for every linger window; reset on each new
    // batch so we never allocate a fresh Future per iteration.
    let sleep = tokio::time::sleep(Duration::from_secs(86_400));
    tokio::pin!(sleep);
    let mut linger_armed = false;

    loop {
        let submission = tokio::select! {
            msg = intake_rx.recv() => match msg {
                None => break,
                Some(s) => s,
            },
            _ = &mut sleep, if linger_armed => {
                if let Some(batch) = current.take() {
                    send_or_complete_batch(batch, &dispatch_tx, &stats, &cfg).await;
                }
                linger_armed = false;
                continue;
            },
        };

        // Overflow: flush the current batch before adding the new submission.
        if let Some(batch) = current.as_ref() {
            if batch.len() > 0
                && cfg.batch_bytes > 0
                && batch.byte_size + submission.append_byte_size > cfg.batch_bytes
            {
                send_or_complete_batch(current.take().unwrap(), &dispatch_tx, &stats, &cfg).await;
                linger_armed = false;
            }
        }

        let was_empty = current.is_none();
        let batch = current.get_or_insert_with(DeliveryBatch::new);
        batch.add_submission(submission, &cfg);

        // Arm linger once when the first item lands in a fresh batch.
        if was_empty {
            sleep
                .as_mut()
                .reset(tokio::time::Instant::now() + cfg.linger);
            linger_armed = true;
        }

        if cfg.batch_bytes > 0 && batch.byte_size >= cfg.batch_bytes {
            send_or_complete_batch(current.take().unwrap(), &dispatch_tx, &stats, &cfg).await;
            linger_armed = false;
        }
    }

    // Intake channel closed: flush whatever remains, then return.
    // Intake channel closed: flush whatever remains, then return. Dropping
    // dispatch_tx here signals workers that no more batches are coming.
    if let Some(batch) = current.take() {
        send_or_complete_batch(batch, &dispatch_tx, &stats, &cfg).await;
    }
}

async fn send_or_complete_batch(
    batch: DeliveryBatch,
    dispatch_tx: &mpsc::Sender<DeliveryBatch>,
    stats: &Arc<AsyncStatsCollector>,
    cfg: &Config,
) {
    if let Err(err) = dispatch_tx.send(batch).await {
        complete_async_batch(
            err.0,
            stats,
            DeliveryResult {
                err: Some(Error::Internal(
                    "dispatch channel closed before batch delivery".into(),
                )),
                attempts: 0,
                status_code: 0,
                response: None,
                started_at: SystemTime::now(),
                finished_at: SystemTime::now(),
            },
            cfg,
        );
    }
}

async fn run_async_worker(
    dispatch_rx: Arc<AsyncMutex<mpsc::Receiver<DeliveryBatch>>>,
    sender: Arc<AsyncSenderKind>,
    stats: Arc<AsyncStatsCollector>,
    cfg: Config,
) {
    loop {
        let batch = {
            let mut rx = dispatch_rx.lock().await;
            rx.recv().await
        };
        match batch {
            None => return,
            Some(batch) => {
                stats.busy_workers.fetch_add(1, Ordering::SeqCst);
                deliver_batch_async(batch, &sender, &stats, &cfg).await;
                stats.busy_workers.fetch_add(-1, Ordering::SeqCst);
            }
        }
    }
}

// ── Delivery task ─────────────────────────────────────────────────────────────

async fn deliver_batch_async(
    mut batch: DeliveryBatch,
    sender: &Arc<AsyncSenderKind>,
    stats: &Arc<AsyncStatsCollector>,
    cfg: &Config,
) {
    let started = SystemTime::now();
    let mut attempts = 0usize;
    let mut retry_deadline: Option<Instant> = if cfg.doris_upload_timeout > Duration::ZERO {
        Some(Instant::now() + cfg.doris_upload_timeout)
    } else {
        None
    };

    loop {
        if attempts > 0 {
            if let Some(deadline) = retry_deadline {
                if Instant::now() > deadline {
                    complete_async_batch(
                        batch,
                        stats,
                        DeliveryResult {
                            err: Some(Error::Timeout),
                            attempts,
                            status_code: 0,
                            response: None,
                            started_at: started,
                            finished_at: SystemTime::now(),
                        },
                        cfg,
                    );
                    return;
                }
            }
        }

        attempts += 1;
        stats.record_upload_attempt(batch.byte_size as i64, batch.len() as i64);

        match sender.send(&batch, cfg.doris_upload_request_timeout).await {
            Ok(outcome) => {
                complete_async_batch(
                    batch,
                    stats,
                    DeliveryResult {
                        err: None,
                        attempts,
                        status_code: outcome.status_code,
                        response: outcome.response,
                        started_at: started,
                        finished_at: SystemTime::now(),
                    },
                    cfg,
                );
                return;
            }
            Err(err) => {
                let mut retriable = err.retriable();
                let ambiguous = err.ambiguous();
                let mut final_err = err;

                if ambiguous {
                    match poll_label_async(&batch.label, started, attempts, sender, cfg).await {
                        Ok(result) => {
                            complete_async_batch(batch, stats, result, cfg);
                            return;
                        }
                        Err(err2) => {
                            retriable = err2.retriable();
                            final_err = err2;
                            if retriable {
                                batch.label = generate_label(&cfg.label_prefix);
                            }
                        }
                    }
                }

                if !retriable {
                    complete_async_batch(
                        batch,
                        stats,
                        DeliveryResult {
                            err: Some(Error::Http(final_err.message())),
                            attempts,
                            status_code: final_err.status_code(),
                            response: final_err.response().cloned(),
                            started_at: started,
                            finished_at: SystemTime::now(),
                        },
                        cfg,
                    );
                    return;
                }

                if retry_deadline.is_none() {
                    retry_deadline = Some(Instant::now() + cfg.doris_upload_timeout);
                }
                if retry_deadline.is_some_and(|d| Instant::now() > d) {
                    complete_async_batch(
                        batch,
                        stats,
                        DeliveryResult {
                            err: Some(Error::Timeout),
                            attempts,
                            status_code: final_err.status_code(),
                            response: final_err.response().cloned(),
                            started_at: started,
                            finished_at: SystemTime::now(),
                        },
                        cfg,
                    );
                    return;
                }

                tokio::time::sleep(retry_backoff_delay(attempts)).await;
            }
        }
    }
}

async fn poll_label_async(
    label: &str,
    started: SystemTime,
    attempts: usize,
    sender: &Arc<AsyncSenderKind>,
    cfg: &Config,
) -> std::result::Result<DeliveryResult, StreamLoadError> {
    let deadline = Instant::now() + cfg.status_poll_timeout;
    let mut backoff = Duration::from_millis(500);

    loop {
        match sender
            .poll_label(label, cfg.doris_upload_request_timeout)
            .await
        {
            Ok(state) => match state.state.to_uppercase().as_str() {
                "VISIBLE" | "COMMITTED" => {
                    return Ok(DeliveryResult {
                        err: None,
                        attempts,
                        status_code: state.status_code,
                        response: Some(StreamLoadResponse {
                            label: Some(label.to_string()),
                            status: Some("Success".to_string()),
                            message: Some(state.state),
                            ..Default::default()
                        }),
                        started_at: started,
                        finished_at: SystemTime::now(),
                    });
                }
                "ABORTED" => {
                    return Err(StreamLoadError::Error {
                        status_code: state.status_code,
                        message: format!("load label {label} concluded as ABORTED"),
                        retriable: true,
                        ambiguous: false,
                        response: None,
                    });
                }
                "UNKNOWN" => {
                    return Err(StreamLoadError::Error {
                        status_code: state.status_code,
                        message: format!("load label {label} not found in Doris (state=UNKNOWN)"),
                        retriable: true,
                        ambiguous: false,
                        response: None,
                    });
                }
                _ => {}
            },
            Err(err) => {
                if Instant::now() > deadline {
                    return Err(err);
                }
            }
        }

        if Instant::now() > deadline {
            return Err(StreamLoadError::Error {
                status_code: 0,
                message: format!(
                    "load label {label} did not reach a terminal state before poll timeout"
                ),
                retriable: false,
                ambiguous: true,
                response: None,
            });
        }

        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, Duration::from_secs(4));
    }
}

fn complete_async_batch(
    batch: DeliveryBatch,
    stats: &Arc<AsyncStatsCollector>,
    result: DeliveryResult,
    cfg: &Config,
) {
    stats.record_completion(&result);
    for submission in &batch.submissions {
        submission.completion.complete(result.clone());
    }
    if !batch.has_callback {
        return;
    }
    for submission in batch.submissions {
        if let Some(callback) = submission.callback {
            let t = Instant::now();
            if catch_unwind(AssertUnwindSafe(|| callback(result.clone()))).is_err() {
                cfg.log(LogLevel::Error, "delivery callback panicked");
            }
            let elapsed = t.elapsed();
            if elapsed > cfg.slow_callback_warn {
                cfg.log(LogLevel::Info, &format!("callback took {elapsed:?}"));
            }
        }
    }
}

fn retry_backoff_delay(attempt: usize) -> Duration {
    let mut delay = Duration::from_secs(1);
    for _ in 1..attempt {
        delay = delay.saturating_mul(2);
        if delay >= Duration::from_secs(4) {
            return Duration::from_secs(4);
        }
    }
    delay
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    let next = current.saturating_mul(2);
    if next > max {
        max
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn send_yields_during_a_burst_with_spare_queue_capacity() {
        let mut cfg = Config::default();
        cfg.stream_load_url =
            Some("http://doris.example.com/api/test_db/test_table/_stream_load".to_string());
        cfg.columns = vec!["id".to_string(), "name".to_string()];
        cfg.mode = Mode::Csv;
        cfg.validation = ValidationMode::None;
        cfg.fake_send = true;
        cfg.fake_send_delay_set = true;
        cfg.max_queue_size = 10_000;
        cfg.batch_bytes = 90 * 1024 * 1024;
        cfg.linger = Duration::from_secs(60);

        let client = AsyncClient::new(cfg).expect("client should build");
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_ran_clone = peer_ran.clone();
        let peer = tokio::spawn(async move {
            peer_ran_clone.store(true, Ordering::SeqCst);
        });

        for i in 0..1_000 {
            client
                .send(format!("{i},user{i}"))
                .await
                .expect("send should succeed");
        }

        assert!(
            peer_ran.load(Ordering::SeqCst),
            "successful sends should consume Tokio task budget"
        );
        peer.await.expect("peer task should finish");
        client.close().await.expect("close should succeed");
    }
}
