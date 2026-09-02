use crate::identity::PeerId;
use crate::protocol::{read_message, write_message, FluxMessage, PROTOCOL_VERSION};
use crate::transport::error::{Result, TransportError};
use crate::transport::traits::{Connection, Listener, Transport};
use async_trait::async_trait;
use log::{debug, info, warn};
use std::any::Any;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// TCP transport implementation
#[derive(Debug, Clone)]
pub struct TcpTransport {
    connect_timeout: Duration,
    handshake_timeout: Duration,
}

impl TcpTransport {
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            handshake_timeout: Duration::from_secs(60),
        }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(&self, peer_id: &PeerId, addr: SocketAddr) -> Result<Box<dyn Connection>> {
        info!("[TRANSPORT] Connecting to {} at {}", peer_id, addr);

        let stream = timeout(self.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        debug!("[TRANSPORT] TCP connection established");

        let mut conn = TcpConnection::new(stream, addr);

        timeout(self.handshake_timeout, conn.client_handshake(peer_id))
            .await
            .map_err(|_| TransportError::Timeout)??;

        info!("[TRANSPORT] Handshake completed");

        Ok(Box::new(conn))
    }

    async fn listen(&self, addr: SocketAddr) -> Result<Box<dyn Listener>> {
        info!("[TRANSPORT] Starting TCP listener on {}", addr);

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        let actual_addr = listener.local_addr()?;
        info!("[TRANSPORT] TCP listener active on {}", actual_addr);

        Ok(Box::new(TcpConnectionListener {
            listener,
            handshake_timeout: self.handshake_timeout,
        }))
    }
}

/// TCP connection
pub struct TcpConnection {
    stream: TcpStream,
    remote_addr: SocketAddr,
    peer_id: Option<PeerId>,
}

impl TcpConnection {
    fn new(stream: TcpStream, remote_addr: SocketAddr) -> Self {
        Self {
            stream,
            remote_addr,
            peer_id: None,
        }
    }

    async fn client_handshake(&mut self, local_peer_id: &PeerId) -> Result<()> {
        debug!("[PROTOCOL] Sending handshake");

        let hello = FluxMessage::hello(local_peer_id.clone());
        write_message(&mut self.stream, &hello).await?;

        let response = read_message(&mut self.stream).await?;

        match response {
            FluxMessage::HelloAck { version, peer_id } => {
                if version != PROTOCOL_VERSION {
                    return Err(TransportError::HandshakeFailed(format!(
                        "Protocol version mismatch: {} != {}",
                        version, PROTOCOL_VERSION
                    )));
                }

                debug!("[PROTOCOL] Handshake accepted by {}", peer_id);
                self.peer_id = Some(peer_id);
                Ok(())
            }
            _ => Err(TransportError::HandshakeFailed(
                "Expected HelloAck".to_string(),
            )),
        }
    }

    pub async fn server_handshake(&mut self, local_peer_id: &PeerId) -> Result<()> {
        debug!("[PROTOCOL] Awaiting handshake");

        let request = read_message(&mut self.stream).await?;

        match request {
            FluxMessage::Hello { version, peer_id } => {
                if version != PROTOCOL_VERSION {
                    return Err(TransportError::HandshakeFailed(format!(
                        "Protocol version mismatch: {} != {}",
                        version, PROTOCOL_VERSION
                    )));
                }

                debug!("[PROTOCOL] Handshake received from {}", peer_id);

                let ack = FluxMessage::hello_ack(local_peer_id.clone());
                write_message(&mut self.stream, &ack).await?;

                self.peer_id = Some(peer_id);
                Ok(())
            }
            _ => Err(TransportError::HandshakeFailed(
                "Expected Hello".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Connection for TcpConnection {
    fn peer_id(&self) -> Option<&PeerId> {
        self.peer_id.as_ref()
    }

    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    async fn send_message(&mut self, msg: &FluxMessage) -> Result<()> {
        debug!("[TRANSPORT] Sending message: {:?}", msg);
        write_message(&mut self.stream, msg).await
    }

    async fn recv_message(&mut self) -> Result<FluxMessage> {
        let msg = read_message(&mut self.stream).await?;
        debug!("[TRANSPORT] Received message: {:?}", msg);
        Ok(msg)
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let goodbye = FluxMessage::Goodbye;
        if let Err(e) = write_message(&mut self.stream, &goodbye).await {
            warn!("[TRANSPORT] Error sending goodbye: {}", e);
        }

        self.stream.shutdown().await?;
        debug!("[TRANSPORT] Connection closed");
        Ok(())
    }
}

#[allow(dead_code)]
pub struct TcpConnectionListener {
    listener: TcpListener,
    handshake_timeout: Duration,
}

#[async_trait]
impl Listener for TcpConnectionListener {
    async fn accept(&mut self) -> Result<(Box<dyn Connection>, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        debug!("[TRANSPORT] Accepted connection from {}", addr);

        let conn = TcpConnection::new(stream, addr);

        Ok((Box::new(conn), addr))
    }

    fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }
}
