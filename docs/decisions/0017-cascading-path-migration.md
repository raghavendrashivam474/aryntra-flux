# ADR-0017: Cascading Path Migration & Multi-Fallback Transfer Recovery

*Historical Note: Originally designated ADR-0019; chronologically normalized to ADR-0017 following S3.6–S3.8 architecture audit.*

## Context
In Flux v0.18.0, when a transport failure is detected during an active transfer, the TransferCarrier attempts to migrate once. 
If the newly selected candidate path fails to connect, the migrate() operation returns an error immediately, causing the entire transfer to fail terminal.
To survive sequential or simultaneous failures of multiple alternative paths (A -> B -> C -> D), the migration mechanism must be capable of falling back to other candidate paths iteratively during a single migration request.

## Decision
We will modify the internal implementation of TransferCarrier::migrate() in crates/flux-core/src/transfer/migration.rs to run an internal selection and connection retry loop.

### Core Architectural Invariants:
1. **Single Entry Point**: The TransferCarrier::migrate() signature remains unchanged (pub async fn migrate(&mut self) -> Result<()>). No new public methods like migrate_again will be introduced.
2. **Deterministic Candidate Exhaustion**: In each iteration of the connection failure, the failed candidate path will be marked as PathState::Unavailable in the existing PathRegistry. The deterministic PathSelector is then called again to pick the next best path.
3. **No Wire Protocol Changes**: S3.9 will utilize existing TransferResume and chunk checkpoint messages to continue transfers smoothly. No new wire-level messages are defined.
4. **Cooperative Cancellation First**: If the transfer cancellation token is tripped before a connection attempt completes, the migration loop terminates immediately and returns TransferError::Cancelled.
5. **No Infinite Loops**: Since candidates are marked as Unavailable in the shared registry during connection failures, and there are a finite number of registered paths, the loop is guaranteed to terminate when candidate paths are exhausted.

## Consequences
- The Transfer Manager remains simple and decoupled: it still calls carrier.migrate().await? once on carrier transport failures.
- Multiple sequential failures during active transfer and connection setup are handled transparently inside the carrier.
- All S3.8 chunk checkpointing guarantees remain perfectly preserved because the same transfer progress/checkpoints are maintained.
- All existing tests in the workspace must continue to compile and pass without regressions.

