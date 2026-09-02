use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Transport error: {0}")]
    Transport(#[from] crate::transport::error::TransportError),

    #[error("Integrity mismatch: expected {expected}, got {actual}")]
    IntegrityMismatch { expected: String, actual: String },

    #[error("Size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("Transfer rejected: {0}")]
    Rejected(String),

    #[error("Invalid chunk index: expected {expected}, got {actual}")]
    InvalidChunkIndex { expected: u32, actual: u32 },

    #[error("Unexpected message: {0}")]
    UnexpectedMessage(String),

    #[error("Invalid filename: {0}")]
    InvalidFilename(String),
}

pub type Result<T> = std::result::Result<T, TransferError>;
