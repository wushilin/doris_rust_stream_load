use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("client is closed")]
    ClientClosed,
    #[error("queue is full")]
    QueueFull,
    #[error("send exceeds batch bytes limit")]
    SendTooLarge,
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("invalid record: {0}")]
    InvalidRecord(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("request timeout")]
    Timeout,
    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;
