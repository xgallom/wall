use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("I/O System Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Log Processing Parsing Failure: {0}")]
    ParseError(String),

    #[error("Internal Concurrency Component Drop Failure")]
    ConcurrencyError,
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for PipelineError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        PipelineError::ConcurrencyError
    }
}

