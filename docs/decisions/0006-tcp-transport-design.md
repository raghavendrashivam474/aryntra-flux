# ADR 006: TCP Transport and Framing Design

## Status
Accepted

## Context
In S1.3, Flux requires its first concrete, reliable communication transport between discovered peers. TCP is a stream-oriented protocol without native message boundaries, requiring an explicit framing layer. Higher layers must remain decoupled from TCP socket specifics per ADR 002.

## Decision
1. **Transport Abstraction**: Implement `Transport`, `Connection`, and `Listener` asynchronous traits using `async-trait` to encapsulate connection establishment, bi-directional message exchange, and teardown.
2. **Concrete TCP Implementation**: Provide `TcpTransport`, `TcpConnection`, and `TcpConnectionListener` operating over `tokio::net::TcpListener` and `tokio::net::TcpStream`.
3. **Length-Prefixed Framing**: Frame each message with a 4-byte big-endian payload length prefix, enforced with a maximum message size limit of 1 MB (`MAX_MESSAGE_SIZE`) to prevent allocation exhaustion.
4. **Serialization**: Use `bincode` for binary serialization of `FluxMessage` variants (`Hello`, `HelloAck`, `Ping`, `Pong`, `Goodbye`).
5. **Default TCP Port**: Assign port 9002 for direct TCP peer communication, distinct from discovery ports (9000 mDNS, 9001 UDP broadcast).

## Consequences
- Clean separation between transport mechanism, framing, session management, and application logic.
- Robust handling of packet fragmentation and TCP stream boundaries.
- Simple swap-in of future transports (QUIC, Relay, WebSockets) conforming to the `Transport` trait.
