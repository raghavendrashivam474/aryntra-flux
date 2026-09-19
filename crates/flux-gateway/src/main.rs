use flux_core::node::FluxNode;
use flux_gateway::{GatewayServer, GatewayState};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,flux_gateway=debug,flux_core=debug".into()),
        )
        .init();

    info!("Initializing Flux Node runtime for Gateway...");
    let mut node = FluxNode::new("default");

    // Start discovery and underlying node capabilities
    if let Err(e) = node.start().await {
        tracing::warn!(
            "Discovery start warning (non-fatal in isolated environments): {}",
            e
        );
    }

    let node_arc = Arc::new(node);
    let state = GatewayState::new(node_arc);

    let bind_addr: SocketAddr = "127.0.0.1:9100".parse()?;
    info!("Starting Aryntra Flux HTTP Gateway on http://{}", bind_addr);

    let server = GatewayServer::new(state, bind_addr);
    server.run().await?;

    Ok(())
}
