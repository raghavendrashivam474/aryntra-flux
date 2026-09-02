use crate::identity::PeerId;
use crate::transfer::{TransferId, TransferMetadata};
use serde::{Deserialize, Serialize};

/// Protocol version
pub const PROTOCOL_VERSION: u16 = 1;

/// Protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FluxMessage {
    // --- S1.3 messages ---
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

    // --- S1.4: File transfer messages ---
    /// Request to initiate a file transfer
    TransferRequest { metadata: TransferMetadata },

    /// Accept an incoming transfer
    TransferAccept { transfer_id: TransferId },

    /// Reject an incoming transfer
    TransferReject {
        transfer_id: TransferId,
        reason: String,
    },

    /// A chunk of file data
    TransferChunk {
        transfer_id: TransferId,
        index: u32,
        data: Vec<u8>,
    },

    /// Signal that all chunks have been sent
    TransferComplete { transfer_id: TransferId },

    /// Final result of a transfer
    TransferResult {
        transfer_id: TransferId,
        success: bool,
        message: String,
    },
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

    #[test]
    fn test_transfer_messages_serialize() {
        use crate::transfer::TransferMetadata;

        let meta = TransferMetadata::new("test.bin".to_string(), 1024, [0xAB; 32]);
        let req = FluxMessage::TransferRequest {
            metadata: meta.clone(),
        };

        // Round-trip through bincode
        let encoded = crate::protocol::framing::encode_message(&req).unwrap();
        let decoded = crate::protocol::framing::decode_message(&encoded[4..]).unwrap();
        assert_eq!(req, decoded);

        // Chunk with binary data
        let chunk = FluxMessage::TransferChunk {
            transfer_id: meta.transfer_id,
            index: 0,
            data: vec![0xFF, 0x00, 0x42, 0xDE],
        };
        let encoded = crate::protocol::framing::encode_message(&chunk).unwrap();
        let decoded = crate::protocol::framing::decode_message(&encoded[4..]).unwrap();
        assert_eq!(chunk, decoded);
    }
}
