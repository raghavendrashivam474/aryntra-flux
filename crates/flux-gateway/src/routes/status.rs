use crate::error::GatewayResult;
use crate::state::GatewayState;
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;

#[derive(Serialize)]
pub struct StatusResponse {
    pub state: String,
    pub peer_id: String,
    pub discovered_peer_count: usize,
    pub active_path_count: usize,
    pub active_transfer_count: usize,
}

pub fn routes() -> Router<GatewayState> {
    Router::new().route("/status", get(get_status))
}

async fn get_status(State(state): State<GatewayState>) -> GatewayResult<Json<StatusResponse>> {
    let node = &state.node;

    let state_name = match node.state {
        flux_core::node::NodeState::Starting => "starting",
        flux_core::node::NodeState::Running => "running",
        flux_core::node::NodeState::Stopped => "stopped",
    };

    let discovered_peer_count = node.registry.list().len();
    let active_path_count = node.path_registry.all_peers().len();
    let active_transfer_count = state.tracker.active_count().await;

    Ok(Json(StatusResponse {
        state: state_name.to_string(),
        peer_id: node.identity.to_string(),
        discovered_peer_count,
        active_path_count,
        active_transfer_count,
    }))
}
