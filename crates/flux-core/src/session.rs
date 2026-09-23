use crate::identity::PeerId;
use crate::protocol::FluxMessage;
use crate::transport::error::{Result, TransportError};
use crate::transport::{Connection, Transport};
use log::{debug, info};
use std::net::SocketAddr;

/// Communication session state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Connecting,
    Handshaking,
    Established,
    Closing,
    Closed,
}

/// Active communication session
pub struct Session {
    connection: Box<dyn Connection>,
    state: SessionState,
    #[allow(dead_code)]
    local_peer_id: PeerId,
}

impl Session {
    /// Create session from established connection
    pub fn from_connection(connection: Box<dyn Connection>, local_peer_id: PeerId) -> Self {
        Self {
            connection,
            state: SessionState::Established,
            local_peer_id,
        }
    }

    /// Get current session state
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Check if session is currently established and active
    pub fn is_established(&self) -> bool {
        self.state == SessionState::Established
    }

    /// Get remote peer ID
    pub fn peer_id(&self) -> Option<&PeerId> {
        self.connection.peer_id()
    }

    /// Get remote address
    pub fn remote_addr(&self) -> SocketAddr {
        self.connection.remote_addr()
    }

    /// Send a message
    pub async fn send_message(&mut self, msg: &FluxMessage) -> Result<()> {
        if self.state != SessionState::Established {
            return Err(TransportError::ProtocolError(
                "Session not established".to_string(),
            ));
        }

        debug!("[SESSION] Sending message: {:?}", msg);
        self.connection.send_message(msg).await?;
        Ok(())
    }

    /// Receive a message
    pub async fn recv_message(&mut self) -> Result<FluxMessage> {
        if self.state != SessionState::Established {
            return Err(TransportError::ProtocolError(
                "Session not established".to_string(),
            ));
        }

        let msg = self.connection.recv_message().await?;
        debug!("[SESSION] Received message: {:?}", msg);
        Ok(msg)
    }

    /// Controlled cooperative session shutdown without consuming ownership.
    ///
    /// Sends a Goodbye message if established and marks state as Closed.
    /// Idempotent if already closing or closed.
    pub async fn shutdown(&mut self) -> Result<()> {
        if self.state == SessionState::Closed || self.state == SessionState::Closing {
            return Ok(());
        }

        self.state = SessionState::Closing;
        let _ = self.connection.send_message(&FluxMessage::Goodbye).await;
        self.state = SessionState::Closed;
        info!("[SESSION] Session shut down cooperatively");
        Ok(())
    }

    /// Close session gracefully, consuming the session and releasing underlying resources.
    pub async fn close(mut self) -> Result<()> {
        self.state = SessionState::Closing;
        self.connection.close().await?;
        self.state = SessionState::Closed;
        info!("[SESSION] Session closed");
        Ok(())
    }
}

/// Session builder
pub struct SessionBuilder<'a, T: Transport> {
    transport: &'a T,
    #[allow(dead_code)]
    local_peer_id: PeerId,
}

impl<'a, T: Transport> SessionBuilder<'a, T> {
    pub fn new(transport: &'a T, local_peer_id: PeerId) -> Self {
        Self {
            transport,
            local_peer_id,
        }
    }

    /// Connect to a peer and establish session
    pub async fn connect(&self, peer_id: &PeerId, addr: SocketAddr) -> Result<Session> {
        info!(
            "[SESSION] Establishing session with {} at {}",
            peer_id, addr
        );

        let connection = self.transport.connect(&self.local_peer_id, addr).await?;

        Ok(Session {
            connection,
            state: SessionState::Established,
            local_peer_id: self.local_peer_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FluxMessage;
    use crate::transport::traits::Connection;
    use async_trait::async_trait;
    use std::any::Any;

    struct DummyConnection {
        peer_id: PeerId,
        addr: SocketAddr,
    }

    #[async_trait]
    impl Connection for DummyConnection {
        fn peer_id(&self) -> Option<&PeerId> {
            Some(&self.peer_id)
        }

        fn remote_addr(&self) -> SocketAddr {
            self.addr
        }

        async fn send_message(&mut self, _msg: &FluxMessage) -> Result<()> {
            Ok(())
        }

        async fn recv_message(&mut self) -> Result<FluxMessage> {
            Ok(FluxMessage::pong(1, "dummy".to_string()))
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_session_lifecycle() {
        let local_id = PeerId::new();
        let remote_id = PeerId::new();
        let addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

        let dummy = DummyConnection {
            peer_id: remote_id.clone(),
            addr,
        };

        let mut session = Session::from_connection(Box::new(dummy), local_id);
        assert_eq!(session.state(), SessionState::Established);
        assert!(session.is_established());
        assert_eq!(session.peer_id(), Some(&remote_id));
        assert_eq!(session.remote_addr(), addr);

        let ping = FluxMessage::ping(1, "test".to_string());
        assert!(session.send_message(&ping).await.is_ok());

        let recv_res = session.recv_message().await;
        assert!(recv_res.is_ok());

        assert!(session.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_session_shutdown() {
        let local_id = PeerId::new();
        let remote_id = PeerId::new();
        let addr: SocketAddr = "127.0.0.1:9003".parse().unwrap();

        let dummy = DummyConnection {
            peer_id: remote_id.clone(),
            addr,
        };

        let mut session = Session::from_connection(Box::new(dummy), local_id);
        assert!(session.is_established());

        // Cooperative shutdown without consuming
        assert!(session.shutdown().await.is_ok());
        assert_eq!(session.state(), SessionState::Closed);
        assert!(!session.is_established());

        // Repeated shutdown is idempotent
        assert!(session.shutdown().await.is_ok());

        // Sending or receiving on shutdown session returns error
        let ping = FluxMessage::ping(1, "test".to_string());
        assert!(session.send_message(&ping).await.is_err());
        assert!(session.recv_message().await.is_err());
    }
}