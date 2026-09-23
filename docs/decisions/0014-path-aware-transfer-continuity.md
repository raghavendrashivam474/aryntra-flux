# ADR-0014: Path-Aware Transfer Continuity

## Status
Accepted

## Context
Prior to Flux v0.3.6 (Sprint S3.5), transfer execution was coupled to the lifetime of an individual network session. While single-file resume (S1.5/S3.3) and progress observability (S3.4) allowed a interrupted transfer to pick up from disk checkpoints (`.part` and `.part.meta`), there was no first-class architectural abstraction to express the continuity of an in-flight logical transfer across successive carrier sessions or alternate network paths.

Specifically:
1. `TransferManager::send_collection` operated as a monolithic single-session loop. If a session dropped mid-collection, the entire execution failed without a direct re-entry API.
2. In-memory `TransferProgress` snapshots could be skewed if unconfirmed socket buffers from a failed session were not calibrated against verified on-disk checkpoints before resuming over a new session.
3. The Gateway transfer tracker lacked an explicit API to attach a new carrier task to an existing logical `TransferId` without creating duplicate transfer records or resetting observed metrics.

## Decision
1. **Decouple Logical Transfer from Carrier Session**:
   - Introduce `TransferContinuation` in `flux_core::transfer::continuity`, holding the collection `TransferPlan`, shared `TransferProgress`, cooperative `TransferCancellation` token, and `completed_files` index.
   - Introduce `TransferManager::continue_collection(&mut Session, &TransferContinuation)` enabling deterministic continuation of collections on fresh sessions.
2. **Progress Baseline Calibration**:
   - Add `TransferProgress::set_files` for symmetric state initialization.
   - Calibrate `TransferProgress` upon continuation by computing verified byte lengths of completed items before initiating subsequent file transmissions.
3. **Gateway Tracker Continuation**:
   - Provide `GatewayTransferTracker::attach_continuation` allowing the HTTP Gateway to bind replacement sessions/tasks to the same logical `TransferId` while preserving continuous client-observable progress and lifecycle state.
4. **Preserve Content-Driven Resume**:
   - Reuse existing S1.5/S3.3 `.part` and `.part.meta` checkpointing mechanisms without modification.
   - Maintain SHA-256 integrity verification across all continued and multi-path file streams.

## Non-Goals
- **No Automatic Path Migration**: Automated detection of degraded paths and dynamic carrier switching is deferred to Sprint S3.6. S3.5 establishes the underlying continuity capability.
- **No Concurrent Multipath Chunk Striping**: File chunks continue to be sent sequentially per session.

## Consequences
- **Positive**: Logical transfers survive carrier session death and path failovers.
- **Positive**: Gateway clients query the identical `TransferId` and receive non-resetting monotonic progress.
- **Positive**: Multi-file collections resume from the exact interrupted file without retransmitting finished items.
- **Positive**: Zero breaking changes to existing S3.3 cancellation or S3.4 observability contracts.
