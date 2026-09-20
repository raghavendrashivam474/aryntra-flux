use crate::error::{GatewayError, GatewayResult};
use crate::state::GatewayState;
use axum::{extract::State, routing::post, Json, Router};
use flux_core::identity::PeerId;
use flux_core::path::selector::PathSelector;
use flux_core::session::SessionBuilder;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Deserialize)]
pub struct ConnectRequest {
    pub peer_id: String,
}

#[derive(Serialize)]
pub struct ConnectResponse {
    pub success: bool,
    pub peer_id: String,
    pub connected_address: String,
    pub message: String,
}

pub fn routes() -> Router<GatewayState> {
    Router::new().route("/connect", post(connect_peer))
}

async fn connect_peer(
    State(state): State<GatewayState>,
    Json(payload): Json<ConnectRequest>,
) -> GatewayResult<Json<ConnectResponse>> {
    let target_uuid = uuid::Uuid::parse_str(&payload.peer_id)
        .map_err(|e| GatewayError::InvalidRequest(format!("Invalid UUID: {}", e)))?;

    let target_peer_id = serde_json::from_str::<PeerId>(&format!("\"{}\"", target_uuid))
        .map_err(|e| GatewayError::Internal(format!("Failed to reconstruct PeerId: {}", e)))?;

    let peer_list = state.node.registry.list();
    let registry_peer = peer_list
        .iter()
        .find(|p| p.id == target_peer_id)
        .ok_or_else(|| GatewayError::PeerNotFound(payload.peer_id.clone()))?;

    let selector = PathSelector::new(state.node.path_registry.clone());

    let (target_addr, source) = if let Some(optimal_path) = selector.select_path(&target_peer_id) {
        tracing::info!(
            "Selected optimal path for {}: {}",
            target_peer_id,
            optimal_path.remote_addr
        );
        (optimal_path.remote_addr, "optimal_path_selector")
    } else if let Some(path_set) = state.node.path_registry.get_paths(&target_peer_id) {
        if let Some(path) = path_set.list().iter().find(|path| {
            matches!(
                path.state,
                flux_core::path::PathState::Discovered
                    | flux_core::path::PathState::Candidate
                    | flux_core::path::PathState::Connecting
            )
        }) {
            tracing::info!(
                "Using discovered path for {}: {}",
                target_peer_id,
                path.remote_addr
            );
            (path.remote_addr, "discovered_path")
        } else {
            let parsed_addr: SocketAddr = registry_peer.address.parse().map_err(|e| {
                GatewayError::InvalidRequest(format!(
                    "Invalid peer registry address '{}': {}",
                    registry_peer.address, e
                ))
            })?;
            tracing::info!(
                "No usable path available; falling back to peer registry address: {}",
                parsed_addr
            );
            (parsed_addr, "peer_registry_fallback")
        }
    } else {
        let parsed_addr: SocketAddr = registry_peer.address.parse().map_err(|e| {
            GatewayError::InvalidRequest(format!(
                "Invalid peer registry address '{}': {}",
                registry_peer.address, e
            ))
        })?;
        tracing::info!(
            "No path registry entry; falling back to peer registry address: {}",
            parsed_addr
        );
        (parsed_addr, "peer_registry_fallback")
    };

    let session_builder = SessionBuilder::new(&*state.transport, state.node.identity.clone());
    let session = session_builder
        .connect(&target_peer_id, target_addr)
        .await
        .map_err(|e| GatewayError::Internal(format!("Failed to connect: {}", e)))?;

    let mut sessions_guard = state.sessions.lock().await;
    sessions_guard.insert(payload.peer_id.clone(), session);

    Ok(Json(ConnectResponse {
        success: true,
        peer_id: payload.peer_id,
        connected_address: target_addr.to_string(),
        message: format!("Session successfully established via {}.", source),
    }))
}
