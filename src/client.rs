use crate::config::{Config, LogLevel, Mode, ValidationMode};
use crate::errors::{Error, Result};
use crate::queue::{DeliveryBatch, DequeueWaitResult, QueueItem, QueuedSubmission, RequestQueue};
use crate::sender::{FakeSender, HttpSender, Sender, StreamLoadError};
use crate::types::{ClientStats, DeliveryCallback, DeliveryResult, Handle, StreamLoadResponse};
use crossbeam_channel::{bounded, Receiver, Sender as ChannelSender};
use serde::de::{
    DeserializeSeed, Deserializer, Error as SerdeError, IgnoredAny, MapAccess, Visitor,
};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

pub(crate) const MAX_STATS_SAMPLES: usize = 1_000;

pub struct Client {
    cfg: Config,
    intake: Arc<RequestQueue>,
    worker_handles: Mutex<Vec<JoinHandle<()>>>,
    batcher_handle: Mutex<Option<JoinHandle<()>>>,
    stats: Arc<ClientStatsCollector>,
    closed: AtomicBool,
}

struct ClientStatsCollector {
    started_at: SystemTime,
    busy_workers: std::sync::atomic::AtomicI64,
    total_load_jobs: std::sync::atomic::AtomicI64,
    error_jobs: std::sync::atomic::AtomicI64,
    total_retries: std::sync::atomic::AtomicI64,
    total_bytes_sent: std::sync::atomic::AtomicI64,
    total_records_sent: std::sync::atomic::AtomicI64,
    total_upload_attempts: std::sync::atomic::AtomicI64,
    total_load_time_nanos: std::sync::atomic::AtomicI64,
    durations: Mutex<VecDeque<Duration>>,
}

impl Client {
    pub fn new(mut cfg: Config) -> Result<Self> {
        cfg = cfg.with_defaults();
        cfg.validate()?;
        if cfg.stream_load_url.is_some() && !cfg.stream_load_url_has_suffix() {
            cfg.log(
                LogLevel::Info,
                "stream_load_url does not end with _stream_load; this may not be a valid Doris stream load endpoint",
            );
        }
        let intake = Arc::new(RequestQueue::new(cfg.max_queue_size));
        let (dispatch_tx, dispatch_rx) = bounded(cfg.max_upload_queue_size);
        let sender: Arc<dyn Sender> = if cfg.fake_send {
            Arc::new(FakeSender::new(cfg.fake_send_delay))
        } else {
            Arc::new(HttpSender::new(cfg.clone())?)
        };
        let stats = Arc::new(ClientStatsCollector::new(SystemTime::now()));
        let mut worker_handles = Vec::with_capacity(cfg.doris_upload_workers);
        for worker_id in 0..cfg.doris_upload_workers {
            let receiver = dispatch_rx.clone();
            let sender = sender.clone();
            let stats = stats.clone();
            let cfg = cfg.clone();
            let handle = thread::spawn(move || run_worker(worker_id, receiver, sender, stats, cfg));
            worker_handles.push(handle);
        }
        let intake_clone = intake.clone();
        let sender_clone = sender.clone();
        let stats_clone = stats.clone();
        let cfg_clone = cfg.clone();
        let batcher = thread::spawn(move || {
            run_batcher(
                intake_clone,
                dispatch_tx,
                sender_clone,
                stats_clone,
                cfg_clone,
            )
        });

        Ok(Self {
            cfg,
            intake,
            worker_handles: Mutex::new(worker_handles),
            batcher_handle: Mutex::new(Some(batcher)),
            stats,
            closed: AtomicBool::new(false),
        })
    }

    pub fn send(&self, record: String) -> Result<Handle> {
        self.send_batch_internal(vec![record], None, None)
    }

    pub fn send_batch(&self, records: Vec<String>) -> Result<Handle> {
        self.send_batch_internal(records, None, None)
    }

    pub fn send_timeout(&self, record: String, timeout: Duration) -> Result<Handle> {
        self.send_batch_internal(vec![record], None, Some(timeout))
    }

    pub fn send_batch_timeout(&self, records: Vec<String>, timeout: Duration) -> Result<Handle> {
        self.send_batch_internal(records, None, Some(timeout))
    }

    pub fn send_with_callback<F>(&self, callback: F, record: String) -> Result<Handle>
    where
        F: Fn(DeliveryResult) + Send + Sync + 'static,
    {
        self.send_batch_internal(vec![record], Some(Arc::new(callback)), None)
    }

    pub fn send_batch_with_callback<F>(&self, callback: F, records: Vec<String>) -> Result<Handle>
    where
        F: Fn(DeliveryResult) + Send + Sync + 'static,
    {
        self.send_batch_internal(records, Some(Arc::new(callback)), None)
    }

    pub fn send_with_callback_timeout<F>(
        &self,
        callback: F,
        record: String,
        timeout: Duration,
    ) -> Result<Handle>
    where
        F: Fn(DeliveryResult) + Send + Sync + 'static,
    {
        self.send_batch_internal(vec![record], Some(Arc::new(callback)), Some(timeout))
    }

    pub fn send_batch_with_callback_timeout<F>(
        &self,
        callback: F,
        records: Vec<String>,
        timeout: Duration,
    ) -> Result<Handle>
    where
        F: Fn(DeliveryResult) + Send + Sync + 'static,
    {
        self.send_batch_internal(records, Some(Arc::new(callback)), Some(timeout))
    }

    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.intake.close();

        let batcher_handle = self.batcher_handle.lock().unwrap().take();
        if let Some(handle) = batcher_handle {
            handle
                .join()
                .map_err(|_| Error::Internal("batcher thread panicked".into()))?;
        }

        let worker_handles = std::mem::take(&mut *self.worker_handles.lock().unwrap());
        for handle in worker_handles {
            handle
                .join()
                .map_err(|_| Error::Internal("worker thread panicked".into()))?;
        }
        Ok(())
    }

    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn buffered_records(&self) -> usize {
        self.intake.len()
    }

    pub fn stats(&self) -> ClientStats {
        self.stats.snapshot(self.cfg.doris_upload_workers)
    }

    fn send_batch_internal(
        &self,
        records: Vec<String>,
        callback: Option<DeliveryCallback>,
        enqueue_timeout: Option<Duration>,
    ) -> Result<Handle> {
        if records.is_empty() {
            return Err(Error::InvalidRecord(
                "at least one record is required".into(),
            ));
        }
        let handle = Handle::new();
        self.validate_records(&records)
            .map_err(Error::InvalidRecord)?;
        let payload_bytes = records.iter().map(|record| record.len()).sum();
        let items = records
            .into_iter()
            .map(|record| QueueItem {
                byte_size: record.len(),
                payload: record,
            })
            .collect();
        let submission = QueuedSubmission::new(
            &self.cfg.mode,
            items,
            payload_bytes,
            handle.completion(),
            callback,
        );
        if self.cfg.batch_bytes > 0 && submission.standalone_byte_size > self.cfg.batch_bytes {
            return Err(Error::SendTooLarge);
        }
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::ClientClosed);
        }
        let timeout = if let Some(timeout) = enqueue_timeout {
            Some(timeout)
        } else if self.cfg.max_queue_wait_time.is_zero() {
            None
        } else {
            Some(self.cfg.max_queue_wait_time)
        };
        self.intake.enqueue(submission, timeout)?;
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
                    validate_csv_records(
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
                        validate_json_record(
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

pub(crate) fn validate_csv_records(
    records: &[String],
    expected_columns: usize,
    delimiter: u8,
    quote: u8,
) -> std::result::Result<(), csv::Error> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .delimiter(delimiter)
        .quote(quote)
        .from_reader(CsvRecordsReader::new(records));
    let mut count = 0;
    for result in reader.records() {
        let row = result?;
        count += 1;
        if row.len() != expected_columns {
            return Err(invalid_csv(format!(
                "invalid csv record: expected {} columns, got {}",
                expected_columns,
                row.len()
            )));
        }
    }
    if count != records.len() {
        return Err(invalid_csv(format!(
            "invalid csv record: expected {} rows, got {}",
            records.len(),
            count
        )));
    }
    Ok(())
}

struct CsvRecordsReader<'a> {
    records: &'a [String],
    index: usize,
    offset: usize,
    needs_newline: bool,
}

impl<'a> CsvRecordsReader<'a> {
    fn new(records: &'a [String]) -> Self {
        Self {
            records,
            index: 0,
            offset: 0,
            needs_newline: false,
        }
    }
}

impl Read for CsvRecordsReader<'_> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut written = 0;
        while !buf.is_empty() {
            if self.needs_newline {
                buf[0] = b'\n';
                buf = &mut buf[1..];
                written += 1;
                self.needs_newline = false;
                if buf.is_empty() {
                    break;
                }
            }

            let Some(record) = self.records.get(self.index) else {
                break;
            };
            let bytes = record.as_bytes();
            let remaining = &bytes[self.offset..];
            let to_copy = remaining.len().min(buf.len());
            buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
            buf = &mut buf[to_copy..];
            written += to_copy;
            self.offset += to_copy;

            if self.offset == bytes.len() {
                self.index += 1;
                self.offset = 0;
                self.needs_newline = self.index < self.records.len();
            }
        }

        Ok(written)
    }
}
fn invalid_csv(message: impl Into<String>) -> csv::Error {
    csv::Error::from(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

pub(crate) fn validate_json_record(
    record: &str,
    columns: &[String],
    strict: bool,
) -> serde_json::Result<()> {
    if strict {
        return JSON_STRICT_VALIDATOR
            .with(|validator| validator.borrow_mut().validate(record, columns));
    }

    let mut deserializer = serde_json::Deserializer::from_str(record);
    deserializer.deserialize_any(JsonObjectSyntaxValidator)?;
    deserializer.end()
}

struct JsonObjectSyntaxValidator;

impl<'de> DeserializeSeed<'de> for JsonObjectSyntaxValidator {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for JsonObjectSyntaxValidator {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some((_key, _value)) = map.next_entry::<IgnoredAny, IgnoredAny>()? {}
        Ok(())
    }
}

thread_local! {
    static JSON_STRICT_VALIDATOR: RefCell<JsonStrictValidator> = RefCell::new(JsonStrictValidator::new());
}

struct JsonStrictValidator {
    columns: Vec<String>,
    seen: Vec<bool>,
}

impl JsonStrictValidator {
    fn new() -> Self {
        Self {
            columns: Vec::new(),
            seen: Vec::new(),
        }
    }

    fn validate(&mut self, record: &str, columns: &[String]) -> serde_json::Result<()> {
        self.prepare_columns(columns);
        self.seen.fill(false);

        let mut deserializer = serde_json::Deserializer::from_str(record);
        deserializer.deserialize_any(JsonObjectStrictValidator {
            columns: &self.columns,
            seen: &mut self.seen,
            seen_count: 0,
        })?;
        deserializer.end()
    }

    fn prepare_columns(&mut self, columns: &[String]) {
        if self.columns.len() == columns.len()
            && self
                .columns
                .iter()
                .zip(columns)
                .all(|(cached, current)| cached == current)
        {
            return;
        }
        self.columns.clear();
        self.columns.extend(columns.iter().cloned());
        self.seen.resize(self.columns.len(), false);
    }
}

struct JsonObjectStrictValidator<'a> {
    columns: &'a [String],
    seen: &'a mut [bool],
    seen_count: usize,
}

impl<'de> Visitor<'de> for JsonObjectStrictValidator<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(mut self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            let Some(index) = self.columns.iter().position(|column| column == &key) else {
                return Err(SerdeError::custom(format!(
                    "invalid json object payload: unexpected column '{key}'"
                )));
            };
            if !self.seen[index] {
                self.seen[index] = true;
                self.seen_count += 1;
            }
            map.next_value::<IgnoredAny>()?;
        }

        if self.seen_count != self.columns.len() {
            return Err(SerdeError::custom(format!(
                "invalid json object payload: expected exactly {} columns, got {}",
                self.columns.len(),
                self.seen_count
            )));
        }
        for (index, column) in self.columns.iter().enumerate() {
            if !self.seen[index] {
                return Err(SerdeError::custom(format!(
                    "invalid json object payload: missing column '{column}'"
                )));
            }
        }
        Ok(())
    }
}

fn run_batcher(
    intake: Arc<RequestQueue>,
    dispatch: ChannelSender<DeliveryBatch>,
    _sender: Arc<dyn Sender>,
    stats: Arc<ClientStatsCollector>,
    cfg: Config,
) {
    let mut current: Option<DeliveryBatch> = None;

    let flush = |batch: &mut Option<DeliveryBatch>| {
        if let Some(batch) = batch.take() {
            if let Err(err) = dispatch.send(batch) {
                complete_batch(
                    err.0,
                    stats.clone(),
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
                    &cfg,
                );
            }
        }
    };

    loop {
        let result = intake.dequeue_batch(cfg.batch_bytes);
        if result.is_none() {
            flush(&mut current);
            drop(dispatch);
            return;
        }
        let (submissions, _) = result.unwrap();
        for submission in submissions {
            let need_flush = current
                .as_ref()
                .map(|batch| {
                    batch.len() > 0
                        && cfg.batch_bytes > 0
                        && batch.byte_size + submission.append_byte_size > cfg.batch_bytes
                })
                .unwrap_or(false);
            if need_flush {
                flush(&mut current);
            }
            if current.is_none() {
                current = Some(DeliveryBatch::new());
            }
            if let Some(batch) = current.as_mut() {
                batch.add_submission(submission, &cfg);
                if cfg.batch_bytes > 0 && batch.byte_size >= cfg.batch_bytes {
                    flush(&mut current);
                }
            }
        }

        loop {
            if current.as_ref().map_or(true, |b| b.len() == 0) {
                break;
            }
            let elapsed = current
                .as_ref()
                .unwrap()
                .created_at
                .elapsed()
                .unwrap_or(Duration::ZERO);
            if elapsed >= cfg.linger {
                flush(&mut current);
                break;
            }
            let remaining = if cfg.batch_bytes > 0 {
                cfg.batch_bytes
                    .saturating_sub(current.as_ref().unwrap().byte_size)
            } else {
                usize::MAX
            };
            if cfg.batch_bytes > 0 && remaining == 0 {
                flush(&mut current);
                break;
            }
            match intake.dequeue_batch_wait(remaining, cfg.linger - elapsed) {
                DequeueWaitResult::Batch(submissions) => {
                    for submission in submissions {
                        let need_flush = current
                            .as_ref()
                            .map(|batch| {
                                batch.len() > 0
                                    && cfg.batch_bytes > 0
                                    && batch.byte_size + submission.append_byte_size
                                        > cfg.batch_bytes
                            })
                            .unwrap_or(false);
                        if need_flush {
                            flush(&mut current);
                        }
                        if current.is_none() {
                            current = Some(DeliveryBatch::new());
                        }
                        if let Some(batch) = current.as_mut() {
                            batch.add_submission(submission, &cfg);
                            if cfg.batch_bytes > 0 && batch.byte_size >= cfg.batch_bytes {
                                flush(&mut current);
                            }
                        }
                    }
                }
                DequeueWaitResult::Timeout => {
                    flush(&mut current);
                    break;
                }
                DequeueWaitResult::Closed => {
                    flush(&mut current);
                    drop(dispatch);
                    return;
                }
            }
        }
    }
}

fn run_worker(
    _id: usize,
    dispatch: Receiver<DeliveryBatch>,
    sender: Arc<dyn Sender>,
    stats: Arc<ClientStatsCollector>,
    cfg: Config,
) {
    for batch in dispatch {
        stats.change_busy_workers(1);
        deliver_batch(batch, sender.clone(), stats.clone(), cfg.clone());
        stats.change_busy_workers(-1);
    }
}

fn deliver_batch(
    mut batch: DeliveryBatch,
    sender: Arc<dyn Sender>,
    stats: Arc<ClientStatsCollector>,
    cfg: Config,
) {
    let started = SystemTime::now();
    let mut attempts = 0;
    let mut retry_deadline = if cfg.doris_upload_timeout > Duration::ZERO {
        Some(Instant::now() + cfg.doris_upload_timeout)
    } else {
        None
    };

    loop {
        if attempts > 0 {
            if let Some(deadline) = retry_deadline {
                if Instant::now() > deadline {
                    complete_batch(
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
                        &cfg,
                    );
                    return;
                }
            }
        }

        attempts += 1;
        stats.record_upload_attempt(batch.byte_size as i64, batch.len() as i64);
        let outcome = sender.send(&batch, cfg.doris_upload_request_timeout);
        match outcome {
            Ok(outcome) => {
                let result = DeliveryResult {
                    err: None,
                    attempts,
                    status_code: outcome.status_code,
                    response: outcome.response,
                    started_at: started,
                    finished_at: SystemTime::now(),
                };
                complete_batch(batch, stats, result, &cfg);
                return;
            }
            Err(err) => {
                let mut retriable = err.retriable();
                let mut ambiguous = err.ambiguous();
                let mut final_err = err;
                if ambiguous {
                    match poll_label_until_conclusion(
                        &batch.label,
                        started,
                        attempts,
                        sender.clone(),
                        &cfg,
                    ) {
                        Ok(result) => {
                            complete_batch(batch, stats, result, &cfg);
                            return;
                        }
                        Err(err2) => {
                            retriable = err2.retriable();
                            ambiguous = err2.ambiguous();
                            final_err = err2;
                            if retriable {
                                batch.label = crate::queue::generate_label(&cfg.label_prefix);
                            }
                        }
                    }
                }
                if !retriable {
                    let result = DeliveryResult {
                        err: Some(Error::Http(final_err.message())),
                        attempts,
                        status_code: final_err.status_code(),
                        response: final_err.response().cloned(),
                        started_at: started,
                        finished_at: SystemTime::now(),
                    };
                    complete_batch(batch, stats, result, &cfg);
                    return;
                }
                if retry_deadline.is_none() {
                    retry_deadline = Some(Instant::now() + cfg.doris_upload_timeout);
                }
                if let Some(deadline) = retry_deadline {
                    if Instant::now() > deadline {
                        let result = DeliveryResult {
                            err: Some(Error::Timeout),
                            attempts,
                            status_code: final_err.status_code(),
                            response: final_err.response().cloned(),
                            started_at: started,
                            finished_at: SystemTime::now(),
                        };
                        complete_batch(batch, stats, result, &cfg);
                        return;
                    }
                }
                let backoff = retry_backoff_delay(attempts);
                std::thread::sleep(backoff);
            }
        }
    }
}

fn poll_label_until_conclusion(
    label: &str,
    started: SystemTime,
    attempts: usize,
    sender: Arc<dyn Sender>,
    cfg: &Config,
) -> std::result::Result<DeliveryResult, StreamLoadError> {
    let deadline = Instant::now() + cfg.status_poll_timeout;
    let mut backoff = Duration::from_millis(500);
    loop {
        let response = sender.poll_label(label, cfg.doris_upload_request_timeout);
        match response {
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
                        message: format!("load label {} concluded as ABORTED", label),
                        retriable: true,
                        ambiguous: false,
                        response: None,
                    });
                }
                "PREPARE" | "PRECOMMITTED" => {}
                "UNKNOWN" => {
                    return Err(StreamLoadError::Error {
                        status_code: state.status_code,
                        message: format!("load label {} not found in Doris (state=UNKNOWN)", label),
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
                    "load label {} did not reach a terminal state before poll timeout",
                    label
                ),
                retriable: false,
                ambiguous: true,
                response: None,
            });
        }

        std::thread::sleep(backoff);
        backoff = next_backoff(backoff, Duration::from_secs(4));
    }
}

fn complete_batch(
    batch: DeliveryBatch,
    stats: Arc<ClientStatsCollector>,
    result: DeliveryResult,
    cfg: &Config,
) {
    stats.record_completion(result.clone());
    for submission in &batch.submissions {
        submission.completion.complete(result.clone());
    }
    if !batch.has_callback {
        return;
    }
    for submission in batch.submissions {
        if let Some(callback) = submission.callback {
            let started = Instant::now();
            if catch_unwind(AssertUnwindSafe(|| callback(result.clone()))).is_err() {
                cfg.log(LogLevel::Error, "delivery callback panicked");
            }
            let elapsed = started.elapsed();
            if elapsed > cfg.slow_callback_warn {
                cfg.log(LogLevel::Info, &format!("callback took {:?}", elapsed));
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        self.intake.close();
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

impl ClientStatsCollector {
    fn new(started_at: SystemTime) -> Self {
        Self {
            started_at,
            busy_workers: std::sync::atomic::AtomicI64::new(0),
            total_load_jobs: std::sync::atomic::AtomicI64::new(0),
            error_jobs: std::sync::atomic::AtomicI64::new(0),
            total_retries: std::sync::atomic::AtomicI64::new(0),
            total_bytes_sent: std::sync::atomic::AtomicI64::new(0),
            total_records_sent: std::sync::atomic::AtomicI64::new(0),
            total_upload_attempts: std::sync::atomic::AtomicI64::new(0),
            total_load_time_nanos: std::sync::atomic::AtomicI64::new(0),
            durations: Mutex::new(VecDeque::new()),
        }
    }

    fn change_busy_workers(&self, delta: i64) {
        self.busy_workers.fetch_add(delta, Ordering::SeqCst);
    }

    fn record_upload_attempt(&self, bytes: i64, records: i64) {
        self.total_upload_attempts.fetch_add(1, Ordering::SeqCst);
        self.total_bytes_sent.fetch_add(bytes, Ordering::SeqCst);
        self.total_records_sent.fetch_add(records, Ordering::SeqCst);
    }

    fn record_completion(&self, result: DeliveryResult) {
        self.total_load_jobs.fetch_add(1, Ordering::SeqCst);
        if result.err.is_some() {
            self.error_jobs.fetch_add(1, Ordering::SeqCst);
        }
        if result.attempts > 1 {
            self.total_retries
                .fetch_add((result.attempts - 1) as i64, Ordering::SeqCst);
        }
        if let Ok(duration) = result.finished_at.duration_since(result.started_at) {
            self.total_load_time_nanos
                .fetch_add(duration.as_nanos() as i64, Ordering::SeqCst);
            let mut durations = self.durations.lock().unwrap();
            if durations.len() == MAX_STATS_SAMPLES {
                durations.pop_front();
            }
            durations.push_back(duration);
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
        let p50 = percentile_duration(&durations, 0.50);
        let p90 = percentile_duration(&durations, 0.90);
        let p99 = percentile_duration(&durations, 0.99);
        let p999 = percentile_duration(&durations, 0.999);

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

pub(crate) fn percentile_duration(values: &[Duration], quantile: f64) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    if quantile <= 0.0 {
        return values[0];
    }
    if quantile >= 1.0 {
        return *values.last().unwrap();
    }
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index.min(values.len() - 1)]
}
