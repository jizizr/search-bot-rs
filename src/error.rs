use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AppError {
    #[error("Elasticsearch error: {0}")]
    Elasticsearch(#[from] elasticsearch::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Telegram API error: {0}")]
    Telegram(#[from] teloxide::RequestError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Bulk index failed (status {status_code}): {details}")]
    BulkIndexFailure { status_code: u16, details: String },
}
