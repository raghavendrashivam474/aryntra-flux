pub mod connect;
pub mod identity;
pub mod peers;
pub mod status;
pub mod transfer;

use crate::state::GatewayState;
use axum::Router;

pub fn create_router(state: GatewayState) -> Router {
    Router::new().nest("/flux/v1", api_routes(state))
}

fn api_routes(state: GatewayState) -> Router {
    Router::new()
        .merge(identity::routes())
        .merge(status::routes())
        .merge(peers::routes())
        .merge(connect::routes())
        .merge(transfer::routes())
        .with_state(state)
}
