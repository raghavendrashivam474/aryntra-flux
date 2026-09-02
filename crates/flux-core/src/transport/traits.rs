use crate::identity::PeerId;
use crate::protocol::FluxMessage;
use crate::transport::error::Result;
use async_trait::async_trait;
use std::any::Any;
use std::net::SocketAddr;

/// Abstract transport interface
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to a peer at the given address
    async fn connect(&self, peer_id: &PeerId, addr: SocketAddr) -> Result<Box<dyn Connection>>;

    /// Start listening on the given address
    async fn listen(&self, addr: SocketAddr) -> Result<Box<dyn Listener>>;
}

/// Active connection - now message-oriented
#[async_trait]
pub trait Connection: Send + Sync {
    /// Get remote peer ID (after handshake)
    fn peer_id(&self) -> Option<&PeerId>;

    /// Get remote address
    fn remote_addr(&self) -> SocketAddr;

    /// Send a message (handles framing internally)
    async fn send_message(&mut self, msg: &FluxMessage) -> Result<()>;

    /// Receive a message (handles framing internally)
    async fn recv_message(&mut self) -> Result<FluxMessage>;

    /// Close connection gracefully
    async fn close(self: Box<Self>) -> Result<()>;

    /// Downcast helper for accessing concrete type
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Connection listener
#[async_trait]
pub trait Listener: Send + Sync {
    /// Accept incoming connection
    async fn accept(&mut self) -> Result<(Box<dyn Connection>, SocketAddr)>;

    /// Get local listening address
    fn local_addr(&self) -> SocketAddr;
}
