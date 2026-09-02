use crate::protocol::message::FluxMessage;
use crate::transport::error::{Result, TransportError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum message size (1 MB for now)
const MAX_MESSAGE_SIZE: u32 = 1_024 * 1_024;

/// Encode a message with length prefix
pub fn encode_message(msg: &FluxMessage) -> Result<Vec<u8>> {
    let payload =
        bincode::serialize(msg).map_err(|e| TransportError::Serialization(e.to_string()))?;

    let len = payload.len() as u32;
    if len > MAX_MESSAGE_SIZE {
        return Err(TransportError::InvalidFrame(format!(
            "Message too large: {} bytes",
            len
        )));
    }

    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&payload);

    Ok(frame)
}

/// Decode a message from length-prefixed frame
pub fn decode_message(data: &[u8]) -> Result<FluxMessage> {
    bincode::deserialize(data).map_err(|e| TransportError::Serialization(e.to_string()))
}

/// Read a framed message from async stream
pub async fn read_message<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<FluxMessage> {
    // Read length prefix
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes);

    if len > MAX_MESSAGE_SIZE {
        return Err(TransportError::InvalidFrame(format!(
            "Message too large: {} bytes",
            len
        )));
    }

    // Read payload
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;

    decode_message(&payload)
}

/// Write a framed message to async stream
pub async fn write_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &FluxMessage,
) -> Result<()> {
    let frame = encode_message(msg)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PeerId;

    #[test]
    fn test_encode_decode() {
        let peer_id = PeerId::new();
        let msg = FluxMessage::hello(peer_id.clone());

        let encoded = encode_message(&msg).unwrap();

        let len = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(len as usize, encoded.len() - 4);

        let decoded = decode_message(&encoded[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_message_too_large() {
        let huge_payload = "x".repeat((MAX_MESSAGE_SIZE + 1) as usize);
        let msg = FluxMessage::ping(1, huge_payload);

        let result = encode_message(&msg);
        assert!(result.is_err());

        match result {
            Err(TransportError::InvalidFrame(_)) => {}
            _ => panic!("Expected InvalidFrame error"),
        }
    }

    #[test]
    fn test_ping_pong_encode() {
        let ping = FluxMessage::ping(123, "hello".to_string());
        let encoded = encode_message(&ping).unwrap();
        let decoded = decode_message(&encoded[4..]).unwrap();

        assert_eq!(ping, decoded);
    }

    #[tokio::test]
    async fn test_read_write_message_stream() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let msg = FluxMessage::ping(42, "Stream test".to_string());

        tokio::spawn(async move {
            write_message(&mut client, &msg).await.unwrap();
        });

        let received = read_message(&mut server).await.unwrap();
        match received {
            FluxMessage::Ping { sequence, payload } => {
                assert_eq!(sequence, 42);
                assert_eq!(payload, "Stream test");
            }
            _ => panic!("Unexpected message type"),
        }
    }

    #[tokio::test]
    async fn test_read_oversized_frame() {
        let (mut client, mut server) = tokio::io::duplex(1024);

        tokio::spawn(async move {
            let oversized_len: u32 = MAX_MESSAGE_SIZE + 100;
            client
                .write_all(&oversized_len.to_be_bytes())
                .await
                .unwrap();
        });

        let result = read_message(&mut server).await;
        assert!(result.is_err());
        match result {
            Err(TransportError::InvalidFrame(msg)) => {
                assert!(msg.contains("Message too large"));
            }
            other => panic!("Expected InvalidFrame, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_invalid_data() {
        let invalid_bytes = vec![0xFF, 0xFF, 0x00, 0x12];
        let result = decode_message(&invalid_bytes);
        assert!(result.is_err());
        match result {
            Err(TransportError::Serialization(_)) => {}
            other => panic!("Expected Serialization error, got {:?}", other),
        }
    }
}
