use crate::error::{GatewayError, GatewayResult};
use crate::state::GatewayState;
use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use serde::Serialize;

#[derive(Serialize)]
pub struct PeerSummary {
    pub peer_id: String,
    pub address: String,
    pub last_seen_secs_ago: f64,
}

#[derive(Serialize)]
pub struct PeerListResponse {
    pub peers: Vec<PeerSummary>,
}

pub fn routes() -> Router<GatewayState> {
    Router::new()
        .route("/peers", get(list_peers))
        .route("/peers/:peer_id", get(get_peer))
}

fn peer_to_summary(p: &flux_core::peer::Peer) -> PeerSummary {
    PeerSummary {
        peer_id: p.id.to_string(),
        address: p.address.clone(),
        last_seen_secs_ago: p.last_seen.elapsed().as_secs_f64(),
    }
}

async fn list_peers(State(state): State<GatewayState>) -> GatewayResult<Json<PeerListResponse>> {
    let peers = state.node.registry.list();
    let summaries = peers.iter().map(peer_to_summary).collect();
    Ok(Json(PeerListResponse { peers: summaries }))
}

async fn get_peer(
    State(state): State<GatewayState>,
    Path(peer_id): Path<String>,
) -> GatewayResult<Json<PeerSummary>> {
    let peers = state.node.registry.list();
    let peer = peers
        .iter()
        .find(|p| p.id.to_string() == peer_id)
        .ok_or_else(|| GatewayError::PeerNotFound(peer_id.clone()))?;
    Ok(Json(peer_to_summary(peer)))
}
