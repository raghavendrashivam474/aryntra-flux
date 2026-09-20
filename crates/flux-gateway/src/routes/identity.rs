use crate::error::GatewayResult;
use crate::state::GatewayState;
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;

#[derive(Serialize)]
pub struct IdentityResponse {
    pub peer_id: String,
    pub version: String,
    pub protocol_version: String,
}

pub fn routes() -> Router<GatewayState> {
    Router::new().route("/identity", get(get_identity))
}

async fn get_identity(State(state): State<GatewayState>) -> GatewayResult<Json<IdentityResponse>> {
    Ok(Json(IdentityResponse {
        peer_id: state.node.identity.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: flux_core::protocol::PROTOCOL_VERSION.to_string(),
    }))
}
