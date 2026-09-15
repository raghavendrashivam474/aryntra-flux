# ADR 0010: Multi-Path Foundation & Path Representation

## Status
Accepted

## Context
In Phase 1 (S1.1 through S1.6), Aryntra Flux established a single-path model where a discovered peer was identified by its `PeerId` and associated with exactly one active IP endpoint (`Peer.address`).

Sprint S2.1 introduces the foundational multi-path model. A physical node or remote peer may expose multiple IP addresses across network interfaces (e.g., Ethernet, Wi-Fi, VPN, cellular). Higher layers require the capability to discover, register, and represent all viable candidate routes independently of dynamic routing or path selection decisions.

## Decision
1. **Explicit Path Abstraction**: Introduce `Path`, `PathId`, `PathSet`, and `PathRegistry` in `crates/flux-core/src/path/mod.rs`.
2. **Identity Decoupling**: A `PeerId` uniquely identifies a peer node, while a `PathId` uniquely identifies a concrete candidate communication route to that peer. Routes are deduplicated by the `(PeerId, TransportKind, SocketAddr)` tuple.
3. **Multi-Endpoint Ingestion**: Discovery mechanisms (both mDNS multi-address records and UDP fallback beacons) now ingest all announced endpoints and register each as an independent candidate `Path` in the `PathRegistry`.
4. **Lifecycle Segregation**: Introduce `PathState` (`Discovered`, `Candidate`, `Connecting`, `Available`, `Unavailable`) separate from `SessionState` and transfer progress states.
5. **Preserved Boundaries**: The `Session` and `TransferManager` abstractions remain untouched; a `Session` is established against a concrete endpoint provided by a `Path`, maintaining strict separation of concerns without premature traffic multiplexing or routing heuristics.

## Consequences
- Peers can be associated with multiple independently verifiable paths.
- S1 file and directory transfer semantics remain 100% backward-compatible.
- Paves the way for S2.2 (Path Measurement) and S2.3 (Relay / Selection).
