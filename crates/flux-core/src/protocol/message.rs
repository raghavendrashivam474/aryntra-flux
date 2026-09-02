use crate::identity::PeerId;
use serde::{Deserialize, Serialize};

/// Protocol version
pub const PROTOCOL_VERSION: u16 = 1;

/// Protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FluxMessage {
    /// Initial handshake from client
    Hello { version: u16, peer_id: PeerId },

    /// Handshake acknowledgment from server
    HelloAck { version: u16, peer_id: PeerId },

    /// Test message for S1.3 demonstration
    Ping { sequence: u32, payload: String },

    /// Response to test message
    Pong { sequence: u32, payload: String },

    /// Graceful connection close
    Goodbye,
}

impl FluxMessage {
    pub fn hello(peer_id: PeerId) -> Self {
        Self::Hello {
            version: PROTOCOL_VERSION,
            peer_id,
        }
    }

    pub fn hello_ack(peer_id: PeerId) -> Self {
        Self::HelloAck {
            version: PROTOCOL_VERSION,
            peer_id,
        }
    }

    pub fn ping(sequence: u32, payload: String) -> Self {
        Self::Ping { sequence, payload }
    }

    pub fn pong(sequence: u32, payload: String) -> Self {
        Self::Pong { sequence, payload }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        // Use PeerId::new() instead of generate()
        let peer_id = PeerId::new();
        let hello = FluxMessage::hello(peer_id.clone());

        match hello {
            FluxMessage::Hello {
                version,
                peer_id: id,
            } => {
                assert_eq!(version, PROTOCOL_VERSION);
                assert_eq!(id, peer_id);
            }
            _ => panic!("Expected Hello message"),
        }
    }

    #[test]
    fn test_ping_pong() {
        let ping = FluxMessage::ping(42, "test".to_string());
        match ping {
            FluxMessage::Ping { sequence, payload } => {
                assert_eq!(sequence, 42);
                assert_eq!(payload, "test");
            }
            _ => panic!("Expected Ping message"),
        }
    }
}
