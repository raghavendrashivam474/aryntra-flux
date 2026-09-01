# Flux Wire Protocol (v0.1.0-draft)

## 1. Overview
The Flux protocol is a binary-efficient messaging system designed to operate over multiple transports (TCP, QUIC, UDP).

## 2. Identity
Every node is identified by a **PeerID**, which is a SHA-256 hash of its Public Key (Ed25519).

## 3. Message Framing
All messages follow a basic Length-Prefix-Type format:
- **Length (4 bytes)**: Total size of the payload.
- **Type (1 byte)**: Message category (Control, Data, Handshake).
- **Payload**: The encoded data (Protobuf or Bincode).

## 4. Handshake Sequence
1. **SYN**: Initiator sends PeerID and Capabilities.
2. **ACK**: Receiver validates identity and responds with chosen parameters.
3. **EST**: Connection established; Encryption keys rotated.

## 5. Transfer State
Flux supports **Resume-by-Hash**. Every file is chunked, and every chunk is verified via BLAKE3.

## 6. Local Discovery (v0.1.0)
### 6.1 mDNS Parameters
- **Service Type**: \_flux._udp.local.\
- **Instance Name**: \[PeerID]\
- **Port**: 9000 (Placeholder)

### 6.2 UDP Heartbeat
- **Port**: 9001
- **Payload**: Raw UTF-8 String of the PeerID.
- **Interval**: 2 Seconds.
