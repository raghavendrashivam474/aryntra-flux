use crate::routes::create_router;
use crate::state::GatewayState;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub struct GatewayServer {
    state: GatewayState,
    bind_addr: SocketAddr,
}

impl GatewayServer {
    pub fn new(state: GatewayState, bind_addr: SocketAddr) -> Self {
        Self { state, bind_addr }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let app = create_router(self.state)
            .layer(TraceLayer::new_for_http())
            .layer(CorsLayer::permissive());

        let listener = TcpListener::bind(self.bind_addr).await?;
        tracing::info!("Flux Gateway listening on http://{}", self.bind_addr);
        axum::serve(listener, app).await?;
        Ok(())
    }
}
