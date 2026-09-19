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
    // 1. Parse and validate Target Peer ID
    let target_uuid = uuid::Uuid::parse_str(&payload.peer_id)
        .map_err(|e| GatewayError::InvalidRequest(format!("Invalid UUID: {}", e)))?;

    // We recreate the exact PeerId wrapping this Uuid to query registries
    let target_peer_id = serde_json::from_str::<PeerId>(&format!("\"{}\"", target_uuid))
        .map_err(|e| GatewayError::Internal(format!("Failed to reconstruct PeerId: {}", e)))?;

    // 2. Validate peer is present in our PeerRegistry
    let peer_list = state.node.registry.list();
    let registry_peer = peer_list
        .iter()
        .find(|p| p.id == target_peer_id)
        .ok_or_else(|| GatewayError::PeerNotFound(payload.peer_id.clone()))?;

    // 3. Optimal Path Selection via PathSelector
    let selector = PathSelector::new(state.node.path_registry.clone());
    let (target_addr, source) = match selector.select_path(&target_peer_id) {
        Some(optimal_path) => {
            tracing::info!(
                "Selected optimal path for {}: {}",
                target_peer_id,
                optimal_path.remote_addr
            );
            (optimal_path.remote_addr, "optimal_path_selector")
        }
        None => {
            // Fall back to primary registry address
            let parsed_addr: SocketAddr = registry_peer.address.parse().map_err(|e| {
                GatewayError::InvalidRequest(format!(
                    "Invalid peer registry address '{}': {}",
                    registry_peer.address, e
                ))
            })?;
            tracing::info!("No metrics-optimal path available yet; falling back to primary registry address: {}", parsed_addr);
            (parsed_addr, "peer_registry_fallback")
        }
    };

    // 4. Connect and Establish Session
    let session_builder = SessionBuilder::new(&*state.transport, state.node.identity.clone());
    let session = session_builder
        .connect(&target_peer_id, target_addr)
        .await
        .map_err(|e| GatewayError::Internal(format!("Failed to connect: {}", e)))?;

    // 5. Store Session in Active Session Pool
    let mut sessions_guard = state.sessions.lock().await;
    sessions_guard.insert(payload.peer_id.clone(), session);

    Ok(Json(ConnectResponse {
        success: true,
        peer_id: payload.peer_id,
        connected_address: target_addr.to_string(),
        message: format!("Session successfully established via {}.", source),
    }))
}
