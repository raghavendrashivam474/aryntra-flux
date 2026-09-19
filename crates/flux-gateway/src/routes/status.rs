use crate::error::GatewayResult;
use crate::state::GatewayState;
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;

#[derive(Serialize)]
pub struct StatusResponse {
    pub status: String,
    pub peers_known: usize,
    pub paths_tracked: usize,
}

pub fn routes() -> Router<GatewayState> {
    Router::new().route("/status", get(get_status))
}

async fn get_status(State(state): State<GatewayState>) -> GatewayResult<Json<StatusResponse>> {
    let node = &state.node;
    let status = match node.state {
        flux_core::node::NodeState::Starting => "starting",
        flux_core::node::NodeState::Running => "running",
        flux_core::node::NodeState::Stopped => "stopped",
    };
    let peers_known = node.registry.list().len();
    let paths_tracked = node.path_registry.all_peers().len();

    Ok(Json(StatusResponse {
        status: status.to_string(),
        peers_known,
        paths_tracked,
    }))
}
