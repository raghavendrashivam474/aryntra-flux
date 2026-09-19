use crate::transfer_tracker::GatewayTransferTracker;
use flux_core::node::FluxNode;
use flux_core::session::Session;
use flux_core::transport::TcpTransport;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct GatewayState {
    pub node: Arc<FluxNode>,
    pub transport: Arc<TcpTransport>,
    pub tracker: GatewayTransferTracker,
    pub sessions: Arc<Mutex<HashMap<String, Session>>>,
}

impl GatewayState {
    pub fn new(node: Arc<FluxNode>) -> Self {
        Self {
            node,
            transport: Arc::new(TcpTransport::new()),
            tracker: GatewayTransferTracker::new(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
