# ADR 0011: Path Measurement & Deterministic Auto-Selection

## Status
Accepted

## Context
Sprint S2.1 established the foundational multi-path representation (`Path`, `PathId`, `PathSet`, and `PathRegistry`), enabling Flux to represent multiple network routes per peer. However, candidate paths lacked reachability verification, active latency metrics, stale-route expiry, and deterministic selection.

Sprint S2.2 adds path measurement and deterministic selection capabilities without altering the underlying `Session`, `Transport`, or `TransferManager` architectures.

## Decision
1. **Decoupled Metrics Representation**: Introduce `PathMetrics` in `crates/flux-core/src/path/metrics.rs` holding round-trip time (`rtt_ms: Option<u64>`) and probing timestamp (`last_probed: Option<Instant>`). `Path` holds `metrics: Option<PathMetrics>` to keep telemetry decoupled from core path identity.
2. **Session-Level Active Probing**: Introduce `PathProber` in `crates/flux-core/src/path/prober.rs`. The prober establishes connections exclusively via the existing `SessionBuilder` and executes Ping/Pong exchanges to compute end-to-end RTT. Raw socket manipulation is explicitly avoided.
3. **State Machine & Expiry Integration**:
   - Successful probes transition candidate paths to `PathState::Available` and update metrics.
   - Unsuccessful probes transition paths to `PathState::Unavailable`.
   - `PathSet::expire_stale` / `PathRegistry::expire_stale_paths` transition paths exceeding a configured TTL to `PathState::Unavailable`.
4. **Deterministic Selection Policy**: Introduce `PathSelector` in `crates/flux-core/src/path/selector.rs`. The selector evaluates only `PathState::Available` paths with recorded RTT metrics and selects the path with the lowest RTT. Ties are deterministically resolved by stable `PathId` lexical ordering (`Ord`).
5. **Preserved Boundaries**:
   - `TransferManager` remains unaware of multi-path routing mechanics.
   - Wire framing and message definitions (`protocol/framing.rs`, `protocol/message.rs`) are untouched.
   - Sessions are established cleanly over the selected path endpoint.

## Consequences
- Flux can dynamically probe all candidate interfaces and deterministically auto-select the lowest-latency route to any peer.
- The path layer maintains zero dependency on transfer logic and avoids premature adaptive routing heuristics or complex graph scoring.
- Paves the way for S2.3 (Dynamic Path Switching & Relay Fallback).
