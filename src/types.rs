use crate::errors::Error;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime};

pub(crate) trait CompletionSink: Send + Sync {
    fn complete(&self, result: DeliveryResult);
}

pub type DeliveryCallback = Arc<dyn Fn(DeliveryResult) + Send + Sync + 'static>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamLoadResponse {
    #[serde(rename = "TxnId")]
    pub txn_id: Option<i64>,
    #[serde(rename = "Label")]
    pub label: Option<String>,
    #[serde(rename = "Status")]
    pub status: Option<String>,
    #[serde(rename = "ExistingJobStatus")]
    pub existing_job_status: Option<String>,
    #[serde(rename = "Message")]
    pub message: Option<String>,
    #[serde(rename = "ErrorURL")]
    pub error_url: Option<String>,
    #[serde(rename = "NumberTotalRows")]
    pub number_total_rows: Option<i64>,
    #[serde(rename = "NumberLoadedRows")]
    pub number_loaded_rows: Option<i64>,
    #[serde(rename = "NumberFilteredRows")]
    pub number_filtered_rows: Option<i64>,
    #[serde(rename = "NumberUnselectedRows")]
    pub number_unselected: Option<i64>,
    #[serde(rename = "LoadBytes")]
    pub load_bytes: Option<i64>,
    #[serde(rename = "LoadTimeMs")]
    pub load_time_ms: Option<i64>,
}

impl Default for StreamLoadResponse {
    fn default() -> Self {
        Self {
            txn_id: None,
            label: None,
            status: None,
            existing_job_status: None,
            message: None,
            error_url: None,
            number_total_rows: None,
            number_loaded_rows: None,
            number_filtered_rows: None,
            number_unselected: None,
            load_bytes: None,
            load_time_ms: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeliveryResult {
    pub err: Option<Error>,
    pub attempts: usize,
    pub status_code: u16,
    pub response: Option<StreamLoadResponse>,
    pub started_at: SystemTime,
    pub finished_at: SystemTime,
}

impl DeliveryResult {
    pub fn success(&self) -> bool {
        self.err.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct ClientStats {
    pub started_at: SystemTime,

    pub total_workers: usize,
    pub idle_workers: usize,
    pub busy_workers: usize,

    pub total_load_jobs: i64,
    pub error_jobs: i64,
    pub error_rate: f64,

    pub average_load_time: Duration,
    pub p50_load_time: Duration,
    pub p90_load_time: Duration,
    pub p99_load_time: Duration,
    pub p999_load_time: Duration,

    pub average_retries: f64,

    pub total_bytes_sent: i64,
    pub average_load_size: f64,
    pub average_bytes_rate: f64,

    pub records_sent: i64,
    pub average_records_rate: f64,

    pub total_upload_attempts: i64,
}

impl Default for ClientStats {
    fn default() -> Self {
        Self {
            started_at: SystemTime::now(),
            total_workers: 0,
            idle_workers: 0,
            busy_workers: 0,
            total_load_jobs: 0,
            error_jobs: 0,
            error_rate: 0.0,
            average_load_time: Duration::ZERO,
            p50_load_time: Duration::ZERO,
            p90_load_time: Duration::ZERO,
            p99_load_time: Duration::ZERO,
            p999_load_time: Duration::ZERO,
            average_retries: 0.0,
            total_bytes_sent: 0,
            average_load_size: 0.0,
            average_bytes_rate: 0.0,
            records_sent: 0,
            average_records_rate: 0.0,
            total_upload_attempts: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchCompletion {
    inner: Mutex<Option<DeliveryResult>>,
    cvar: Condvar,
}

impl BatchCompletion {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(None),
            cvar: Condvar::new(),
        }
    }

    pub(crate) fn complete(&self, result: DeliveryResult) {
        let mut inner = self.inner.lock().unwrap();
        if inner.is_some() {
            return;
        }
        *inner = Some(result);
        self.cvar.notify_all();
    }
}

impl CompletionSink for BatchCompletion {
    fn complete(&self, result: DeliveryResult) {
        BatchCompletion::complete(self, result);
    }
}

impl BatchCompletion {
    fn wait(&self) -> DeliveryResult {
        let mut inner = self.inner.lock().unwrap();
        while inner.is_none() {
            inner = self.cvar.wait(inner).unwrap();
        }
        inner.clone().unwrap()
    }

    fn wait_timeout(&self, timeout: Duration) -> Option<DeliveryResult> {
        let deadline = std::time::Instant::now() + timeout;
        let mut inner = self.inner.lock().unwrap();
        while inner.is_none() {
            let remaining = deadline.checked_duration_since(std::time::Instant::now())?;
            let (guard, wait_result) = self.cvar.wait_timeout(inner, remaining).unwrap();
            inner = guard;
            if wait_result.timed_out() && inner.is_none() {
                return None;
            }
        }
        inner.clone()
    }

    fn try_result(&self) -> Option<DeliveryResult> {
        let inner = self.inner.lock().unwrap();
        inner.clone()
    }

    fn is_done(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct Handle {
    completion: Arc<BatchCompletion>,
}

impl Handle {
    pub fn new() -> Self {
        Self {
            completion: Arc::new(BatchCompletion::new()),
        }
    }

    pub fn wait(&self) -> DeliveryResult {
        self.completion.wait()
    }

    pub fn wait_timeout(&self, timeout: Duration) -> Option<DeliveryResult> {
        self.completion.wait_timeout(timeout)
    }

    pub fn result(&self) -> Option<DeliveryResult> {
        self.completion.try_result()
    }

    pub fn is_done(&self) -> bool {
        self.completion.is_done()
    }
}

impl Default for Handle {
    fn default() -> Self {
        Self::new()
    }
}

impl Handle {
    pub(crate) fn completion(&self) -> Arc<dyn CompletionSink> {
        self.completion.clone()
    }
}
