use crate::config::Config;
use crate::errors::Error;
use crate::queue::DeliveryBatch;
use crate::types::StreamLoadResponse;
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::header::{CONTENT_LENGTH, LOCATION};
use std::fs;
use std::time::Duration;
use url::Url;

pub(crate) struct SendOutcome {
    pub status_code: u16,
    pub response: Option<StreamLoadResponse>,
}

pub(crate) struct LoadStateResponse {
    pub status_code: u16,
    pub state: String,
}

pub(crate) enum StreamLoadError {
    Error {
        status_code: u16,
        message: String,
        retriable: bool,
        ambiguous: bool,
        response: Option<StreamLoadResponse>,
    },
}

impl StreamLoadError {
    pub fn status_code(&self) -> u16 {
        match self {
            StreamLoadError::Error { status_code, .. } => *status_code,
        }
    }

    pub fn retriable(&self) -> bool {
        match self {
            StreamLoadError::Error { retriable, .. } => *retriable,
        }
    }

    pub fn ambiguous(&self) -> bool {
        match self {
            StreamLoadError::Error { ambiguous, .. } => *ambiguous,
        }
    }

    pub fn message(&self) -> String {
        match self {
            StreamLoadError::Error { message, .. } => message.clone(),
        }
    }

    pub fn response(&self) -> Option<&StreamLoadResponse> {
        match self {
            StreamLoadError::Error { response, .. } => response.as_ref(),
        }
    }
}

pub(crate) trait Sender: Send + Sync {
    fn send(
        &self,
        batch: &DeliveryBatch,
        timeout: Duration,
    ) -> Result<SendOutcome, StreamLoadError>;
    fn poll_label(
        &self,
        label: &str,
        timeout: Duration,
    ) -> Result<LoadStateResponse, StreamLoadError>;
}

pub(crate) struct HttpSender {
    cfg: Config,
    client: Client,
}

pub(crate) struct FakeSender {
    delay: Duration,
}

impl FakeSender {
    pub(crate) fn new(delay: Duration) -> Self {
        Self { delay }
    }
}

impl Sender for FakeSender {
    fn send(
        &self,
        batch: &DeliveryBatch,
        _timeout: Duration,
    ) -> Result<SendOutcome, StreamLoadError> {
        if self.delay > Duration::ZERO {
            std::thread::sleep(self.delay);
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

    fn poll_label(
        &self,
        _label: &str,
        _timeout: Duration,
    ) -> Result<LoadStateResponse, StreamLoadError> {
        Ok(LoadStateResponse {
            status_code: 200,
            state: "VISIBLE".to_string(),
        })
    }
}

impl HttpSender {
    pub(crate) fn new(cfg: Config) -> Result<Self, Error> {
        let mut builder = Client::builder()
            .danger_accept_invalid_certs(cfg.tls_skip_verify)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(path) = cfg.tls_ca_cert_path.as_ref() {
            let data = fs::read(path)
                .map_err(|e| Error::InvalidConfig(format!("failed to read tls ca cert: {e}")))?;
            let certs = load_ca_certs(&data)?;
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        let client = builder
            .build()
            .map_err(|e| Error::InvalidConfig(format!("failed to build http client: {e}")))?;
        Ok(Self { cfg, client })
    }

    fn build_request(
        &self,
        batch: &DeliveryBatch,
        url: &str,
        body: Vec<u8>,
        timeout: Duration,
        include_basic_auth: bool,
    ) -> Result<reqwest::blocking::Request, StreamLoadError> {
        let content_length = body.len();
        let mut request = self.client.put(url).body(body);
        request =
            self.apply_stream_load_headers(request, batch, content_length, include_basic_auth);
        request = request.timeout(timeout);
        request.build().map_err(|e| StreamLoadError::Error {
            status_code: 0,
            message: e.to_string(),
            retriable: false,
            ambiguous: false,
            response: None,
        })
    }

    fn apply_stream_load_headers(
        &self,
        mut request: RequestBuilder,
        batch: &DeliveryBatch,
        content_length: usize,
        include_basic_auth: bool,
    ) -> RequestBuilder {
        request = request.header("Content-Type", self.content_type(batch));
        request = request.header("columns", self.cfg.columns.join(","));
        request = request.header("Expect", "100-continue");
        request = request.header("label", batch.label.clone());
        request = request.header("format", batch.header_format());
        request = request.header(CONTENT_LENGTH, content_length.to_string());
        if let crate::config::Mode::Json = batch.mode {
            request = request.header("strip_outer_array", "true");
            request = request.header("read_json_by_line", "false");
        } else {
            request = request.header("column_separator", self.cfg.csv_separator.clone());
            request = request.header("enclose", self.cfg.csv_quote.clone());
        }
        for (key, value) in self.cfg.headers.iter() {
            request = request.header(key, value.clone());
        }
        if include_basic_auth {
            if let crate::config::AuthenticationType::Basic = self.cfg.authentication_type {
                if let Some(token) = self.cfg.authentication_token.as_ref() {
                    if let Some((username, password)) = token.split_once(':') {
                        request = request.basic_auth(username, Some(password));
                    }
                }
            }
        }
        request
    }

    fn content_type(&self, batch: &DeliveryBatch) -> String {
        match batch.mode {
            crate::config::Mode::Csv => "text/csv".into(),
            crate::config::Mode::Json => "application/json".into(),
        }
    }
}

fn include_basic_auth_for_redirect_count(redirect_count: usize) -> bool {
    redirect_count <= 1
}

pub(crate) fn load_ca_certs(data: &[u8]) -> Result<Vec<reqwest::Certificate>, Error> {
    if let Ok(certs) = reqwest::Certificate::from_pem_bundle(data) {
        return Ok(certs);
    }
    if let Ok(cert) = reqwest::Certificate::from_der(data) {
        return Ok(vec![cert]);
    }

    Err(Error::InvalidConfig(
        "tls ca cert file must be a valid PEM bundle or DER certificate".into(),
    ))
}

impl Sender for HttpSender {
    fn send(
        &self,
        batch: &DeliveryBatch,
        timeout: Duration,
    ) -> Result<SendOutcome, StreamLoadError> {
        let body_bytes = batch.encode_body();
        let mut url = self.cfg.stream_load_url();
        let mut redirect_count = 0;

        let response = loop {
            let request = self.build_request(
                batch,
                &url,
                body_bytes.clone(),
                timeout,
                include_basic_auth_for_redirect_count(redirect_count),
            )?;
            let response = self
                .client
                .execute(request)
                .map_err(|err| classify_transport_error(err))?;
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
                .and_then(|value| value.to_str().ok())
                .ok_or(StreamLoadError::Error {
                    status_code: response.status().as_u16(),
                    message: "redirect response missing Location header".to_string(),
                    retriable: false,
                    ambiguous: true,
                    response: None,
                })?;
            url = resolve_redirect_url(&url, location)?;
            redirect_count += 1;
        };
        let status = response.status().as_u16();
        let body = response.text().unwrap_or_default();
        let parsed: Option<StreamLoadResponse> = match serde_json::from_str(&body) {
            Ok(parsed) => Some(parsed),
            Err(_) => None,
        };

        if status >= 200 && status < 300 {
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

    fn poll_label(
        &self,
        label: &str,
        timeout: Duration,
    ) -> Result<LoadStateResponse, StreamLoadError> {
        let database = self.cfg.poll_database().ok_or(StreamLoadError::Error {
            status_code: 0,
            message: "database is required to poll load label state".to_string(),
            retriable: false,
            ambiguous: false,
            response: None,
        })?;

        let url = self.cfg.load_state_url(&database, label);
        let mut request = self.client.get(&url).timeout(timeout);
        if let crate::config::AuthenticationType::Basic = self.cfg.authentication_type {
            if let Some(token) = self.cfg.authentication_token.as_ref() {
                if let Some((username, password)) = token.split_once(':') {
                    request = request.basic_auth(username, Some(password));
                }
            }
        }
        for (key, value) in self.cfg.headers.iter() {
            request = request.header(key, value.clone());
        }
        let response = request
            .send()
            .map_err(|err| classify_transport_error(err))?;
        let status = response.status().as_u16();
        let body = response.text().unwrap_or_default();

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
}

pub(crate) fn is_redirect(status_code: u16) -> bool {
    matches!(status_code, 301 | 302 | 303 | 307 | 308)
}

pub(crate) fn resolve_redirect_url(
    current: &str,
    location: &str,
) -> Result<String, StreamLoadError> {
    let location = location.trim();
    if Url::parse(location).is_ok() {
        return Ok(location.to_string());
    }
    let base = Url::parse(current).map_err(|e| StreamLoadError::Error {
        status_code: 0,
        message: format!("invalid redirect base url: {e}"),
        retriable: false,
        ambiguous: true,
        response: None,
    })?;
    base.join(location)
        .map(|url| url.to_string())
        .map_err(|e| StreamLoadError::Error {
            status_code: 0,
            message: format!("invalid redirect location: {e}"),
            retriable: false,
            ambiguous: true,
            response: None,
        })
}

pub(crate) fn classify_transport_error(err: reqwest::Error) -> StreamLoadError {
    if err.is_connect() {
        StreamLoadError::Error {
            status_code: 0,
            message: err.to_string(),
            retriable: true,
            ambiguous: false,
            response: None,
        }
    } else {
        StreamLoadError::Error {
            status_code: 0,
            message: err.to_string(),
            retriable: false,
            ambiguous: true,
            response: None,
        }
    }
}

pub(crate) fn classify_response_error(
    status_code: u16,
    body: String,
    response: Option<StreamLoadResponse>,
) -> StreamLoadError {
    let message = response
        .as_ref()
        .and_then(|r| r.message.clone())
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| body.trim().to_string());
    let mut retriable = false;
    if status_code == 429 || status_code == 408 || status_code >= 500 {
        retriable = true;
    }

    if let Some(response) = response.clone() {
        match response.status.as_deref().unwrap_or_default() {
            "Success" | "Publish Timeout" | "" => retriable = false,
            "Label Already Exists" => {
                retriable = false;
                if let Some(existing) = response.existing_job_status.as_deref() {
                    if existing.eq_ignore_ascii_case("RUNNING") {
                        return StreamLoadError::Error {
                            status_code,
                            message,
                            retriable: false,
                            ambiguous: true,
                            response: Some(response),
                        };
                    }
                }
            }
            "Fail" => {
                if status_code >= 500 {
                    retriable = true;
                }
            }
            _ => {}
        }
    }

    StreamLoadError::Error {
        status_code,
        message,
        retriable,
        ambiguous: false,
        response,
    }
}

pub(crate) fn is_http_success(status_code: u16, response: Option<&StreamLoadResponse>) -> bool {
    if status_code < 200 || status_code >= 300 {
        return false;
    }
    if let Some(response) = response {
        match response.status.as_deref().unwrap_or_default() {
            "" | "Success" | "Publish Timeout" => true,
            "Label Already Exists" => response
                .existing_job_status
                .as_deref()
                .map(|v| v.eq_ignore_ascii_case("FINISHED"))
                .unwrap_or(false),
            _ => false,
        }
    } else {
        true
    }
}
