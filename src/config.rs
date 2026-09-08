use crate::errors::Error;
use reqwest::header::HeaderMap;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

pub const DEFAULT_MAX_QUEUE_SIZE: usize = 100_000;
pub const DEFAULT_MAX_UPLOAD_QUEUE_SIZE: usize = 1;
pub const DEFAULT_BATCH_BYTES: usize = 90 * 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 90 * 1024 * 1024;
pub const DEFAULT_DORIS_UPLOAD_WORKERS: usize = 4;
pub const DEFAULT_MAX_RETRIES: usize = 0;
pub const DEFAULT_LINGER: Duration = Duration::from_millis(5);
pub const DEFAULT_DORIS_UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_DORIS_UPLOAD_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_STATUS_POLL_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_SLOW_CALLBACK_WARN: Duration = Duration::from_millis(10);
pub const DEFAULT_FAKE_SEND_DELAY: Duration = Duration::from_millis(500);
pub const DEFAULT_CSV_SEPARATOR: &str = ",";
pub const DEFAULT_CSV_QUOTE: &str = "\"";
pub const DEFAULT_LABEL_PREFIX: &str = "rust_stream_load";

pub type Logger = Arc<dyn Fn(LogLevel, &str) + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    Csv,
    Json,
}

impl Default for Mode {
    fn default() -> Self {
        Self::Csv
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Csv => write!(f, "csv"),
            Mode::Json => write!(f, "json"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationMode {
    None,
    Syntax,
    Strict,
}

impl Default for ValidationMode {
    fn default() -> Self {
        Self::Syntax
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthenticationType {
    None,
    Basic,
}

impl Default for AuthenticationType {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error,
    Info,
    Debug,
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Info
    }
}

#[derive(Clone)]
pub struct Config {
    pub endpoint: Option<String>,
    pub database: Option<String>,
    pub table: Option<String>,
    pub stream_load_url: Option<String>,
    pub columns: Vec<String>,
    pub headers: HeaderMap,
    pub mode: Mode,
    pub authentication_type: AuthenticationType,
    pub authentication_token: Option<String>,
    pub max_queue_size: usize,
    pub max_upload_queue_size: usize,
    pub batch_bytes: usize,
    pub linger: Duration,
    pub max_queue_wait_time: Duration,
    pub doris_upload_workers: usize,
    pub validation: ValidationMode,
    pub doris_upload_timeout: Duration,
    pub doris_upload_request_timeout: Duration,
    /// Maximum number of re-uploads after a failed attempt. `0` means no
    /// count limit; retries are then bounded only by `doris_upload_timeout`.
    pub max_retries: usize,
    pub slow_callback_warn: Duration,
    pub status_poll_timeout: Duration,
    pub label_prefix: String,
    pub fake_send: bool,
    pub fake_send_delay: Duration,
    pub fake_send_delay_set: bool,
    pub csv_separator: String,
    pub csv_quote: String,
    pub tls_skip_verify: bool,
    pub tls_ca_cert_path: Option<String>,
    pub logger: Option<Logger>,
    pub log_level: LogLevel,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: None,
            database: None,
            table: None,
            stream_load_url: None,
            columns: Vec::new(),
            headers: HeaderMap::new(),
            mode: Mode::default(),
            authentication_type: AuthenticationType::default(),
            authentication_token: None,
            max_queue_size: DEFAULT_MAX_QUEUE_SIZE,
            max_upload_queue_size: DEFAULT_MAX_UPLOAD_QUEUE_SIZE,
            batch_bytes: DEFAULT_BATCH_BYTES,
            linger: DEFAULT_LINGER,
            max_queue_wait_time: Duration::ZERO,
            doris_upload_workers: DEFAULT_DORIS_UPLOAD_WORKERS,
            validation: ValidationMode::default(),
            doris_upload_timeout: DEFAULT_DORIS_UPLOAD_TIMEOUT,
            doris_upload_request_timeout: DEFAULT_DORIS_UPLOAD_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            slow_callback_warn: DEFAULT_SLOW_CALLBACK_WARN,
            status_poll_timeout: DEFAULT_STATUS_POLL_TIMEOUT,
            label_prefix: DEFAULT_LABEL_PREFIX.to_string(),
            fake_send: false,
            fake_send_delay: DEFAULT_FAKE_SEND_DELAY,
            fake_send_delay_set: false,
            csv_separator: DEFAULT_CSV_SEPARATOR.to_string(),
            csv_quote: DEFAULT_CSV_QUOTE.to_string(),
            tls_skip_verify: false,
            tls_ca_cert_path: None,
            logger: None,
            log_level: LogLevel::default(),
        }
    }
}

pub struct ConfigBuilder {
    config: Config,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder {
            config: Config::default(),
        }
    }
}

impl ConfigBuilder {
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.endpoint = Some(endpoint.into());
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.config.database = Some(database.into());
        self
    }

    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.config.table = Some(table.into());
        self
    }

    pub fn stream_load_url(mut self, url: impl Into<String>) -> Self {
        self.config.stream_load_url = Some(url.into());
        self
    }

    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.config.columns = columns.into_iter().map(|c| c.into()).collect();
        self
    }

    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.config.headers = headers;
        self
    }

    pub fn mode(mut self, mode: Mode) -> Self {
        self.config.mode = mode;
        self
    }

    pub fn authentication_type(mut self, authentication_type: AuthenticationType) -> Self {
        self.config.authentication_type = authentication_type;
        self
    }

    pub fn authentication_token(mut self, token: impl Into<String>) -> Self {
        self.config.authentication_token = Some(token.into());
        self
    }

    pub fn max_queue_size(mut self, size: usize) -> Self {
        self.config.max_queue_size = size;
        self
    }

    pub fn max_upload_queue_size(mut self, size: usize) -> Self {
        self.config.max_upload_queue_size = size;
        self
    }

    pub fn batch_bytes(mut self, bytes: usize) -> Self {
        self.config.batch_bytes = bytes;
        self
    }

    pub fn linger(mut self, linger: Duration) -> Self {
        self.config.linger = linger;
        self
    }

    pub fn max_queue_wait_time(mut self, timeout: Duration) -> Self {
        self.config.max_queue_wait_time = timeout;
        self
    }

    pub fn doris_upload_workers(mut self, workers: usize) -> Self {
        self.config.doris_upload_workers = workers;
        self
    }

    pub fn validation(mut self, validation: ValidationMode) -> Self {
        self.config.validation = validation;
        self
    }

    pub fn doris_upload_timeout(mut self, timeout: Duration) -> Self {
        self.config.doris_upload_timeout = timeout;
        self
    }

    pub fn doris_upload_request_timeout(mut self, timeout: Duration) -> Self {
        self.config.doris_upload_request_timeout = timeout;
        self
    }

    /// Cap the number of re-uploads per batch. `0` (default) disables the
    /// count limit; retries then stop only when `doris_upload_timeout` elapses.
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.config.max_retries = retries;
        self
    }

    pub fn slow_callback_warn(mut self, timeout: Duration) -> Self {
        self.config.slow_callback_warn = timeout;
        self
    }

    pub fn status_poll_timeout(mut self, timeout: Duration) -> Self {
        self.config.status_poll_timeout = timeout;
        self
    }

    pub fn label_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.label_prefix = prefix.into();
        self
    }

    pub fn fake_send(mut self, fake_send: bool) -> Self {
        self.config.fake_send = fake_send;
        self
    }

    pub fn fake_send_delay(mut self, delay: Duration) -> Self {
        self.config.fake_send_delay = delay;
        self.config.fake_send_delay_set = true;
        self
    }

    pub fn csv_separator(mut self, separator: impl Into<String>) -> Self {
        self.config.csv_separator = separator.into();
        self
    }

    pub fn csv_quote(mut self, quote: impl Into<String>) -> Self {
        self.config.csv_quote = quote.into();
        self
    }

    pub fn tls_skip_verify(mut self, skip: bool) -> Self {
        self.config.tls_skip_verify = skip;
        self
    }

    pub fn tls_ca_cert_path(mut self, path: impl Into<String>) -> Self {
        self.config.tls_ca_cert_path = Some(path.into());
        self
    }

    pub fn logger<F>(mut self, logger: F) -> Self
    where
        F: Fn(LogLevel, &str) + Send + Sync + 'static,
    {
        self.config.logger = Some(Arc::new(logger));
        self
    }

    pub fn log_level(mut self, log_level: LogLevel) -> Self {
        self.config.log_level = log_level;
        self
    }

    pub fn build(self) -> Result<Config, Error> {
        let config = self.config.with_defaults();
        config.validate()?;
        Ok(config)
    }
}

impl Config {
    pub fn with_defaults(mut self) -> Self {
        if self.max_queue_size == 0 {
            self.max_queue_size = DEFAULT_MAX_QUEUE_SIZE;
        }
        if self.max_upload_queue_size == 0 {
            self.max_upload_queue_size = DEFAULT_MAX_UPLOAD_QUEUE_SIZE;
        }
        if self.batch_bytes == 0 {
            self.batch_bytes = DEFAULT_BATCH_BYTES;
        }
        if self.doris_upload_workers == 0 {
            self.doris_upload_workers = DEFAULT_DORIS_UPLOAD_WORKERS;
        }
        if self.linger.is_zero() {
            self.linger = DEFAULT_LINGER;
        }
        if self.doris_upload_timeout.is_zero() {
            self.doris_upload_timeout = DEFAULT_DORIS_UPLOAD_TIMEOUT;
        }
        if self.doris_upload_request_timeout.is_zero() {
            self.doris_upload_request_timeout = DEFAULT_DORIS_UPLOAD_REQUEST_TIMEOUT;
        }
        if self.slow_callback_warn.is_zero() {
            self.slow_callback_warn = DEFAULT_SLOW_CALLBACK_WARN;
        }
        if self.status_poll_timeout.is_zero() {
            self.status_poll_timeout = DEFAULT_STATUS_POLL_TIMEOUT;
        }
        if !self.fake_send_delay_set {
            self.fake_send_delay = DEFAULT_FAKE_SEND_DELAY;
        }
        if self.label_prefix.is_empty() {
            self.label_prefix = DEFAULT_LABEL_PREFIX.to_string();
        }
        if self.csv_separator.is_empty() {
            self.csv_separator = DEFAULT_CSV_SEPARATOR.to_string();
        }
        if self.csv_quote.is_empty() {
            self.csv_quote = DEFAULT_CSV_QUOTE.to_string();
        }
        self
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.columns.is_empty() {
            return Err(Error::InvalidConfig("columns must be configured".into()));
        }

        match self.mode {
            Mode::Csv | Mode::Json => {}
        }

        match self.validation {
            ValidationMode::None | ValidationMode::Syntax | ValidationMode::Strict => {}
        }

        match self.authentication_type {
            AuthenticationType::None | AuthenticationType::Basic => {}
        }

        if let AuthenticationType::Basic = self.authentication_type {
            let token = self.authentication_token.as_deref().unwrap_or("").trim();
            if token.is_empty() {
                return Err(Error::InvalidConfig(
                    "authentication token is required for basic authentication".into(),
                ));
            }
            if !token.contains(':') {
                return Err(Error::InvalidConfig(
                    "basic authentication token must use user:password format".into(),
                ));
            }
        }

        if self.stream_load_url.is_none() {
            let endpoint = self.endpoint.as_deref().map(str::trim).unwrap_or("");
            if endpoint.is_empty() {
                return Err(Error::InvalidConfig(
                    "endpoint is required when stream_load_url is not set".into(),
                ));
            }
            validate_endpoint_url(endpoint)?;
            if self
                .database
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
            {
                return Err(Error::InvalidConfig(
                    "database is required when stream_load_url is not set".into(),
                ));
            }
            if self
                .table
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
            {
                return Err(Error::InvalidConfig(
                    "table is required when stream_load_url is not set".into(),
                ));
            }
        } else if let Some(url) = &self.stream_load_url {
            validate_stream_load_url(url)?;
        }

        if self.doris_upload_request_timeout < Duration::from_secs(10) {
            return Err(Error::InvalidConfig(
                "doris_upload_request_timeout must be at least 10s".into(),
            ));
        }
        if self.batch_bytes > MAX_BATCH_BYTES {
            return Err(Error::InvalidConfig(
                "batch_bytes cannot be greater than 90 MiB".into(),
            ));
        }
        if self.label_prefix.trim().is_empty() {
            return Err(Error::InvalidConfig("label_prefix cannot be empty".into()));
        }
        if self.csv_separator.as_bytes().len() != 1 {
            return Err(Error::InvalidConfig(
                "csv_separator must be exactly one byte".into(),
            ));
        }
        if self.csv_quote.as_bytes().len() != 1 {
            return Err(Error::InvalidConfig(
                "csv_quote must be exactly one byte".into(),
            ));
        }
        if self.csv_separator.as_bytes()[0] == self.csv_quote.as_bytes()[0] {
            return Err(Error::InvalidConfig(
                "csv_separator and csv_quote must be different".into(),
            ));
        }
        if self.fake_send_delay_set && self.fake_send_delay.as_nanos() > i64::MAX as u128 {
            return Err(Error::InvalidConfig("fake_send_delay is too large".into()));
        }

        Ok(())
    }

    pub fn stream_load_url(&self) -> String {
        if let Some(url) = &self.stream_load_url {
            return url.clone();
        }
        let endpoint = self.endpoint.as_deref().unwrap_or("");
        let database = self.database.as_deref().unwrap_or("");
        let table = self.table.as_deref().unwrap_or("");
        let base = Url::parse(endpoint).unwrap_or_else(|_| Url::parse("http://localhost").unwrap());
        let mut url = base.clone();
        url.path_segments_mut().ok().map(|mut segments| {
            segments.push("api");
            segments.push(database);
            segments.push(table);
            segments.push("_stream_load");
        });
        url.to_string()
    }

    pub fn load_state_url(&self, database: &str, label: &str) -> String {
        let base_url = self
            .stream_load_url
            .as_deref()
            .or_else(|| self.endpoint.as_deref())
            .unwrap_or("");
        let base = Url::parse(base_url).unwrap_or_else(|_| Url::parse("http://localhost").unwrap());
        let mut url = base.clone();
        url.path_segments_mut().ok().map(|mut segments| {
            segments.clear();
            segments.push("api");
            segments.push(database);
            segments.push("get_load_state");
        });
        url.query_pairs_mut().append_pair("label", label);
        url.to_string()
    }

    pub fn poll_database(&self) -> Option<String> {
        if let Some(database) = self.database.as_ref().filter(|s| !s.trim().is_empty()) {
            return Some(database.clone());
        }
        if let Some(stream_url) = self.stream_load_url.as_ref() {
            if let Ok(parsed) = Url::parse(stream_url) {
                let parts: Vec<_> = parsed.path().trim_start_matches('/').split('/').collect();
                for window in parts.windows(4) {
                    if window[0] == "api" && window[3] == "_stream_load" {
                        return Some(window[1].to_string());
                    }
                }
            }
        }
        None
    }

    pub fn log(&self, level: LogLevel, message: &str) {
        if level <= self.log_level {
            if let Some(logger) = &self.logger {
                logger(level, message);
            } else {
                eprintln!("{message}");
            }
        }
    }

    pub fn stream_load_url_has_suffix(&self) -> bool {
        let Some(raw) = self.stream_load_url.as_ref() else {
            return true;
        };
        Url::parse(raw)
            .map(|url| url.path().trim_end_matches('/').ends_with("_stream_load"))
            .unwrap_or(false)
    }
}

fn validate_endpoint_url(endpoint: &str) -> Result<(), Error> {
    let parsed =
        Url::parse(endpoint).map_err(|e| Error::InvalidConfig(format!("invalid endpoint: {e}")))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(Error::InvalidConfig(
            "endpoint must use http or https".into(),
        ));
    }
    if parsed.host_str().is_none() {
        return Err(Error::InvalidConfig(
            "endpoint must include host[:port]".into(),
        ));
    }
    if parsed.username() != "" || parsed.password().is_some() {
        return Err(Error::InvalidConfig(
            "endpoint must not contain embedded credentials".into(),
        ));
    }
    if parsed.path() != "" && parsed.path() != "/" {
        return Err(Error::InvalidConfig(
            "endpoint must not contain a path".into(),
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(Error::InvalidConfig(
            "endpoint must not contain query or fragment".into(),
        ));
    }
    Ok(())
}

fn validate_stream_load_url(value: &str) -> Result<(), Error> {
    let parsed = Url::parse(value)
        .map_err(|e| Error::InvalidConfig(format!("invalid stream_load_url: {e}")))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(Error::InvalidConfig(
            "stream_load_url must use http or https".into(),
        ));
    }
    if parsed.host_str().is_none() {
        return Err(Error::InvalidConfig(
            "stream_load_url must include host[:port]".into(),
        ));
    }
    if parsed.fragment().is_some() {
        return Err(Error::InvalidConfig(
            "stream_load_url must not contain fragment".into(),
        ));
    }
    Ok(())
}
