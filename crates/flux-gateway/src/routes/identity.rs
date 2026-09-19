use crate::error::GatewayResult;
use crate::state::GatewayState;
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;

#[derive(Serialize)]
pub struct IdentityResponse {
    pub peer_id: String,
}

pub fn routes() -> Router<GatewayState> {
    Router::new().route("/identity", get(get_identity))
}

async fn get_identity(State(state): State<GatewayState>) -> GatewayResult<Json<IdentityResponse>> {
    Ok(Json(IdentityResponse {
        peer_id: state.node.identity.to_string(),
    }))
}
