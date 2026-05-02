use crate::config::Config;
use crate::types::{CompletionSink, DeliveryCallback};
use crossbeam_channel::{bounded, Receiver, SendTimeoutError, Sender, TryRecvError};
use std::sync::{atomic::AtomicBool, atomic::Ordering, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug)]
pub(crate) struct QueueItem {
    pub payload: String,
    pub byte_size: usize,
}

pub(crate) struct QueuedSubmission {
    pub items: Vec<QueueItem>,
    pub standalone_byte_size: usize,
    pub append_byte_size: usize,
    pub completion: std::sync::Arc<dyn CompletionSink>,
    pub callback: Option<DeliveryCallback>,
}

impl QueuedSubmission {
    pub(crate) fn new(
        mode: &crate::config::Mode,
        items: Vec<QueueItem>,
        payload_bytes: usize,
        completion: std::sync::Arc<dyn CompletionSink>,
        callback: Option<DeliveryCallback>,
    ) -> Self {
        let count = items.len();
        let (standalone_byte_size, append_byte_size) = match mode {
            crate::config::Mode::Csv => (
                payload_bytes + count.saturating_sub(1),
                payload_bytes + count,
            ),
            crate::config::Mode::Json => (payload_bytes + count + 1, payload_bytes + count),
        };
        Self {
            items,
            standalone_byte_size,
            append_byte_size,
            completion,
            callback,
        }
    }
}

pub(crate) struct DeliveryBatch {
    pub submissions: Vec<QueuedSubmission>,
    pub items: Vec<QueueItem>,
    pub label: String,
    pub mode: crate::config::Mode,
    pub created_at: SystemTime,
    pub has_callback: bool,
    pub byte_size: usize,
}

impl DeliveryBatch {
    pub(crate) fn new() -> Self {
        Self {
            submissions: Vec::new(),
            items: Vec::new(),
            label: String::new(),
            mode: crate::config::Mode::Csv,
            created_at: SystemTime::now(),
            has_callback: false,
            byte_size: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn add_submission(&mut self, mut submission: QueuedSubmission, cfg: &Config) {
        if submission.callback.is_some() {
            self.has_callback = true;
        }

        for item in std::mem::take(&mut submission.items) {
            self.add(item, cfg);
        }

        self.submissions.push(submission);
    }

    fn add(&mut self, item: QueueItem, cfg: &Config) {
        if self.items.is_empty() {
            self.label = generate_label(&cfg.label_prefix);
            self.mode = cfg.mode.clone();
            self.created_at = SystemTime::now();
        }

        match self.mode {
            crate::config::Mode::Csv => {
                if !self.items.is_empty() {
                    self.byte_size += 1;
                }
                self.byte_size += item.byte_size;
            }
            crate::config::Mode::Json => {
                if self.items.is_empty() {
                    self.byte_size = 2 + item.byte_size;
                } else {
                    self.byte_size += 1 + item.byte_size;
                }
            }
        }

        self.items.push(item);
    }

    pub(crate) fn encode_body(&self) -> Vec<u8> {
        match self.mode {
            crate::config::Mode::Csv => {
                let mut body = String::with_capacity(self.byte_size);
                for (i, item) in self.items.iter().enumerate() {
                    if i > 0 {
                        body.push('\n');
                    }
                    body.push_str(&item.payload);
                }
                body.into_bytes()
            }
            crate::config::Mode::Json => {
                let mut body = String::with_capacity(self.byte_size);
                body.push('[');
                for (i, item) in self.items.iter().enumerate() {
                    if i > 0 {
                        body.push(',');
                    }
                    body.push_str(&item.payload);
                }
                body.push(']');
                body.into_bytes()
            }
        }
    }

    pub(crate) fn header_format(&self) -> &'static str {
        match self.mode {
            crate::config::Mode::Csv => "csv",
            crate::config::Mode::Json => "json",
        }
    }
}

pub(crate) fn generate_label(prefix: &str) -> String {
    let suffix: String = (0..12)
        .map(|_| {
            const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
            let idx = rand::random::<usize>() % LETTERS.len();
            LETTERS[idx] as char
        })
        .collect();
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{}_{}_{}", prefix, nanos, suffix)
}

pub(crate) struct RequestQueue {
    sender: Mutex<Option<Sender<QueuedSubmission>>>,
    receiver: Receiver<QueuedSubmission>,
    pending: Mutex<Option<QueuedSubmission>>,
    closed: AtomicBool,
}

pub(crate) enum DequeueWaitResult {
    Batch(Vec<QueuedSubmission>),
    Timeout,
    Closed,
}

impl RequestQueue {
    pub(crate) fn new(max_requests: usize) -> Self {
        let (sender, receiver) = bounded(max_requests);
        Self {
            sender: Mutex::new(Some(sender)),
            receiver,
            pending: Mutex::new(None),
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn enqueue(
        &self,
        submission: QueuedSubmission,
        timeout: Option<Duration>,
    ) -> crate::errors::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(crate::errors::Error::ClientClosed);
        }

        let sender = {
            let sender_guard = self.sender.lock().unwrap();
            sender_guard
                .as_ref()
                .cloned()
                .ok_or(crate::errors::Error::ClientClosed)?
        };

        let result = match timeout {
            Some(timeout) => sender.send_timeout(submission, timeout),
            None => sender
                .send(submission)
                .map_err(|err| SendTimeoutError::Disconnected(err.0)),
        };

        match result {
            Ok(()) => Ok(()),
            Err(SendTimeoutError::Timeout(_)) => Err(crate::errors::Error::QueueFull),
            Err(SendTimeoutError::Disconnected(_)) => Err(crate::errors::Error::ClientClosed),
        }
    }

    pub(crate) fn dequeue_batch(&self, max_bytes: usize) -> Option<(Vec<QueuedSubmission>, usize)> {
        let submission = self.wait_for_item()?;
        let (submissions, bytes) = self.collect_batch(submission, max_bytes);
        Some((submissions, bytes))
    }

    pub(crate) fn dequeue_batch_wait(
        &self,
        max_bytes: usize,
        timeout: Duration,
    ) -> DequeueWaitResult {
        if let Some(submission) = self.take_pending_or_next() {
            let (submissions, _bytes) = self.collect_batch(submission, max_bytes);
            return DequeueWaitResult::Batch(submissions);
        }

        match self.receiver.recv_timeout(timeout) {
            Ok(submission) => {
                let (submissions, _bytes) = self.collect_batch(submission, max_bytes);
                DequeueWaitResult::Batch(submissions)
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => DequeueWaitResult::Timeout,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                if let Some(submission) = self.take_pending_or_next() {
                    let (submissions, _bytes) = self.collect_batch(submission, max_bytes);
                    DequeueWaitResult::Batch(submissions)
                } else {
                    DequeueWaitResult::Closed
                }
            }
        }
    }

    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut sender_guard = self.sender.lock().unwrap();
        *sender_guard = None;
    }

    pub(crate) fn len(&self) -> usize {
        self.receiver.len() + self.pending.lock().unwrap().as_ref().map_or(0, |_| 1)
    }

    fn wait_for_item(&self) -> Option<QueuedSubmission> {
        if let Some(submission) = self.take_pending_or_next() {
            return Some(submission);
        }
        self.receiver.recv().ok()
    }

    fn take_pending_or_next(&self) -> Option<QueuedSubmission> {
        let mut pending_guard = self.pending.lock().unwrap();
        if pending_guard.is_some() {
            return pending_guard.take();
        }
        match self.receiver.try_recv() {
            Ok(submission) => Some(submission),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => None,
        }
    }

    fn collect_batch(
        &self,
        first: QueuedSubmission,
        max_bytes: usize,
    ) -> (Vec<QueuedSubmission>, usize) {
        let mut submissions = vec![first];
        let mut bytes = submissions[0].standalone_byte_size;
        if max_bytes > 0 && bytes >= max_bytes {
            return (submissions, bytes);
        }

        loop {
            match self.receiver.try_recv() {
                Ok(next) => {
                    if max_bytes > 0 && bytes + next.append_byte_size > max_bytes {
                        let mut pending_guard = self.pending.lock().unwrap();
                        *pending_guard = Some(next);
                        break;
                    }
                    bytes += next.append_byte_size;
                    submissions.push(next);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        (submissions, bytes)
    }
}

