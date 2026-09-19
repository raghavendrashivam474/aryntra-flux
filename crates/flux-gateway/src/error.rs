use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Node not ready: {0}")]
    NodeNotReady(String),

    #[error("Peer not found: {0}")]
    PeerNotFound(String),

    #[error("Path not found: {0}")]
    PathNotFound(String),

    #[error("Transfer not found: {0}")]
    TransferNotFound(String),

    #[error("Transfer error: {0}")]
    TransferError(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

impl GatewayError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            GatewayError::NodeNotReady(_) => StatusCode::SERVICE_UNAVAILABLE,
            GatewayError::PeerNotFound(_) => StatusCode::NOT_FOUND,
            GatewayError::PathNotFound(_) => StatusCode::NOT_FOUND,
            GatewayError::TransferNotFound(_) => StatusCode::NOT_FOUND,
            GatewayError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            GatewayError::TransferError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            GatewayError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            GatewayError::NodeNotReady(_) => "NODE_NOT_READY",
            GatewayError::PeerNotFound(_) => "PEER_NOT_FOUND",
            GatewayError::PathNotFound(_) => "PATH_NOT_FOUND",
            GatewayError::TransferNotFound(_) => "TRANSFER_NOT_FOUND",
            GatewayError::InvalidRequest(_) => "INVALID_REQUEST",
            GatewayError::TransferError(_) => "TRANSFER_ERROR",
            GatewayError::Internal(_) => "INTERNAL_ERROR",
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = Json(ErrorResponse {
            error: ErrorBody {
                code: self.error_code().to_string(),
                message: self.to_string(),
                detail: None,
            },
        });
        (status, body).into_response()
    }
}

pub type GatewayResult<T> = Result<T, GatewayError>;
