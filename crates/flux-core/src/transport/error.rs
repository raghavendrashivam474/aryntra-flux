use std::io;
use thiserror::Error;

/// Transport layer errors
#[derive(Error, Debug)]
pub enum TransportError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection timeout")]
    Timeout,

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    #[error("Connection closed")]
    Disconnected,

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, TransportError>;
