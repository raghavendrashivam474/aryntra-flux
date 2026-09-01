# ADR 002: Transport Abstraction

## Status
Accepted

## Context
Networking environments change (Local Wi-Fi, 5G, Hotel Firewalls). Hard-coding TCP or QUIC would limit Flux's adaptability.

## Decision
All networking logic in \lux-core\ must operate against a \Transport\ Trait rather than concrete types.

## Consequences
- Allows swapping QUIC for WebRTC or Relay paths without changing file transfer logic.
