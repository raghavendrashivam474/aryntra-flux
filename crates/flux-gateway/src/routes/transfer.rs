use crate::error::{GatewayError, GatewayResult};
use crate::state::GatewayState;
use crate::transfer_tracker::GatewayTransferInfo;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use flux_core::identity::PeerId;
use flux_core::path::PathId;
use flux_core::session::Session;
use flux_core::transfer::{
    TransferCancellation, TransferCarrier, TransferError, TransferManager, TransferPlan,
    TransferProgress,
};
use flux_core::transport::TcpTransport;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Deserialize)]
pub struct StartTransferRequest {
    pub peer_id: String,
    pub file_paths: Vec<String>,
}

#[derive(Serialize)]
pub struct StartTransferResponse {
    pub transfer_id: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct CancelTransferResponse {
    pub transfer_id: String,
    pub cancelled: bool,
}

pub fn routes() -> Router<GatewayState> {
    Router::new()
        .route("/transfer", post(start_transfer))
        .route("/transfer/:transfer_id", get(get_transfer_status))
        .route("/transfer/:transfer_id/cancel", post(cancel_transfer))
}

/// Guard to guarantee the Session is returned back to the active sessions map
/// even if the transfer task is completed, failed, panicked, or cancelled.
///
/// S3.7: Wraps a TransferCarrier instead of a raw Session so that after
/// autonomous migration the *current* (possibly replacement) session is returned.
struct CarrierReturnGuard {
    peer_id: String,
    carrier: Option<TransferCarrier<TcpTransport>>,
    sessions_map: Arc<Mutex<HashMap<String, Session>>>,
}

impl Drop for CarrierReturnGuard {
    fn drop(&mut self) {
        if let Some(carrier) = self.carrier.take() {
            let session = carrier.session;
            let peer_id = self.peer_id.clone();
            let sessions_map = self.sessions_map.clone();
            tokio::spawn(async move {
                let mut guard = sessions_map.lock().await;
                guard.insert(peer_id, session);
            });
        }
    }
}

async fn start_transfer(
    State(state): State<GatewayState>,
    Json(payload): Json<StartTransferRequest>,
) -> GatewayResult<Json<StartTransferResponse>> {
    // 1. Take Session out of the active sessions pool to run exclusively
    let mut sessions_guard = state.sessions.lock().await;
    let session = sessions_guard.remove(&payload.peer_id).ok_or_else(|| {
        GatewayError::InvalidRequest(format!(
            "No active session found for peer_id: {}. Please run /connect first.",
            payload.peer_id
        ))
    })?;
    drop(sessions_guard);

    // 2. Resolve the target PeerId for path registry lookup
    let target_uuid = uuid::Uuid::parse_str(&payload.peer_id)
        .map_err(|e| GatewayError::InvalidRequest(format!("Invalid UUID: {}", e)))?;
    let target_peer_id: PeerId = serde_json::from_str(&format!("\"{}\"", target_uuid))
        .map_err(|e| GatewayError::Internal(format!("Failed to reconstruct PeerId: {}", e)))?;

    // 3. Determine the PathId for the current session by matching remote address
    let session_addr = session.remote_addr();
    let current_path_id = state
        .node
        .path_registry
        .get_paths(&target_peer_id)
        .and_then(|paths| {
            paths
                .list()
                .iter()
                .find(|p| p.remote_addr == session_addr)
                .map(|p| p.id.clone())
        })
        .unwrap_or_else(PathId::new);

    // 4. Resolve paths on disk and build TransferPlan
    let paths: Vec<PathBuf> = payload.file_paths.iter().map(PathBuf::from).collect();
    let plan = TransferPlan::from_paths(&paths).map_err(|e| {
        GatewayError::InvalidRequest(format!("Failed to build transfer plan: {:?}", e))
    })?;
    if plan.is_empty() {
        let mut sg = state.sessions.lock().await;
        sg.insert(payload.peer_id.clone(), session);
        return Err(GatewayError::InvalidRequest(
            "Transfer plan contains zero files.".to_string(),
        ));
    }

    // 5. Compute total size for progress tracking
    let mut total_bytes = 0u64;
    for item in &plan.items {
        if let Ok(meta) = tokio::fs::metadata(&item.source_path).await {
            total_bytes += meta.len();
        }
    }
    let transfer_id = uuid::Uuid::new_v4().to_string();
    let total_files = plan.len();
    let cancel_token = TransferCancellation::new();
    let progress = TransferProgress::new();

    // 6. Construct TransferCarrier — wraps session with autonomous migration
    let carrier = TransferCarrier::new(
        target_peer_id,
        current_path_id,
        session,
        state.node.path_registry.clone(),
        state.transport.clone(),
        state.node.identity.clone(),
    );

    // 7. Initialize return guard for the carrier
    let mut guard = CarrierReturnGuard {
        peer_id: payload.peer_id.clone(),
        carrier: None,
        sessions_map: state.sessions.clone(),
    };
    let tracker_clone = state.tracker.clone();
    let transfer_id_clone = transfer_id.clone();
    let cancel_clone = cancel_token.clone();
    let progress_clone = progress.clone();

    // 8. Spawn background task using carrier-aware transfer (S3.7 integration)
    let task_handle = tokio::spawn(async move {
        guard.carrier = Some(carrier);
        let carrier_ref = guard.carrier.as_mut().unwrap();
        match TransferManager::send_collection_with_carrier(
            carrier_ref,
            &plan,
            &cancel_clone,
            &progress_clone,
        )
        .await
        {
            Ok(()) => {
                tracing::info!("Transfer {} completed successfully.", transfer_id_clone);
                tracker_clone.mark_completed(&transfer_id_clone).await;
            }
            Err(TransferError::Cancelled) => {
                tracing::info!("Transfer {} cancelled cooperatively.", transfer_id_clone);
                tracker_clone.mark_cancelled(&transfer_id_clone).await;
            }
            Err(e) => {
                tracing::error!("Transfer {} failed: {}", transfer_id_clone, e);
                tracker_clone
                    .mark_failed(&transfer_id_clone, e.to_string())
                    .await;
            }
        }
    });

    // 9. Register running transfer in Tracker
    state
        .tracker
        .register(
            transfer_id.clone(),
            payload.peer_id,
            total_bytes,
            total_files,
            cancel_token,
            progress,
            Some(task_handle),
        )
        .await;

    Ok(Json(StartTransferResponse {
        transfer_id,
        status: "RUNNING".to_string(),
    }))
}

async fn get_transfer_status(
    State(state): State<GatewayState>,
    Path(transfer_id): Path<String>,
) -> GatewayResult<Json<GatewayTransferInfo>> {
    state
        .tracker
        .get(&transfer_id)
        .await
        .map(Json)
        .ok_or(GatewayError::TransferNotFound(transfer_id))
}

async fn cancel_transfer(
    State(state): State<GatewayState>,
    Path(transfer_id): Path<String>,
) -> GatewayResult<Json<CancelTransferResponse>> {
    let cancelled = state.tracker.cancel(&transfer_id).await;
    Ok(Json(CancelTransferResponse {
        transfer_id,
        cancelled,
    }))
}
