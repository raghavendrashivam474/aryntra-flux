# Flux Wire Protocol (v0.1.0)

## 1. Overview
The Flux wire protocol is a message-oriented protocol operating over reliable binary stream transports (like TCP or QUIC).

## 2. Identity
Every node is uniquely identified by a UUID-v4 based `PeerId`, persistence-cached locally or generated dynamically using a multi-profile schema.

## 3. Message Framing
To maintain distinct message boundaries over continuous byte streams, all messages follow a **Length-Prefix-Type** framing pattern:
- **Length prefix (4 bytes)**: Big-endian representation of the serialized payload's size.
- **Maximum Message Guard**: Enforced limit of **1 MB** (`1,048,576` bytes) to prevent resource allocation exhaustion attacks.
- **Payload**: Serialized representation of a `FluxMessage` utilizing `bincode`.

## 4. Handshake Sequence
Prior to establishing an active communication channel, peers execute a two-way identity validation handshake:
1. **Hello**: The client sends a `Hello` message detailing its protocol version and unique `PeerId`.
2. **HelloAck**: The server validates the protocol version, records the client's `PeerId` inside the transport connection, and responds with its own protocol version and `PeerId`.
3. **Established**: The session transitions to the `Established` state, enabling general message exchange.

## 5. Message Catalog
- **Hello (Type: Hello)**: `version: u16`, `peer_id: PeerId` (client handshake initiator).
- **HelloAck (Type: HelloAck)**: `version: u16`, `peer_id: PeerId` (server response).
- **Ping (Type: Ping)**: `sequence: u32`, `payload: String` (test transmission).
- **Pong (Type: Pong)**: `sequence: u32`, `payload: String` (test echo).
- **Goodbye (Type: Goodbye)**: Graceful close notification.

## 6. Ports and Addresses
- **mDNS Service discovery**: `_flux._udp.local.` (Port: 9000)
- **UDP Heartbeat Fallback**: Broadcast (Port: 9001)
- **Direct TCP Connection**: Communication port (Port: 9002)
