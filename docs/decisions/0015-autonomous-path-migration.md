# ADR 0015: Autonomous Path-Aware Transfer Migration

## Status

Accepted — Implemented in S3.6 (v0.16.0)

## Context

S3.5 introduced path-aware transfer continuity: the ability to explicitly
resume an interrupted transfer on a different session/path using
`TransferContinuation`. However, S3.5 required the *caller* to detect the
failure, construct the continuation, and manually invoke `continue_collection`
on a new session.

In a real local-network environment, paths can fail mid-transfer due to
interface changes, cable disconnects, or Wi-Fi roaming. The transfer layer
should autonomously detect carrier failure and migrate to an alternate path
without losing verified progress.

## Problem

When an active transfer's underlying session/path fails during chunk
transmission:

1. The sender receives a `TransportError` from the session.
2. Without autonomous migration, the entire transfer aborts.
3. The caller must manually detect the failure, select a new path, establish
   a new session, and resume — duplicating logic that belongs in the core.

## Decision

Introduce a **TransferCarrier** abstraction that wraps an active `Session`
with path-awareness and autonomous migration capability.

### Key Components

- **`MigrationState`** enum: `Active`, `Migrating`, `Paused`, `Completed`,
  `Failed`. Makes the carrier lifecycle explicit and testable.
- **`TransferCarrier<T: Transport>`** struct: Holds the active session,
  current path identity, `PathRegistry`, and transport reference. Provides
  `migrate()` which marks the failed path unavailable, queries
  `PathSelector` for the best alternate, connects a replacement session,
  and transitions state.
- **`TransferManager::send_collection_with_carrier()`**: A new entry point
  that drives the collection loop through a `TransferCarrier`. On
  `TransferError::Transport`, it calibrates the progress checkpoint to the
  last verified completed-file boundary, invokes `carrier.migrate()`, and
  retries the current file on the replacement session. The receiver's
  existing `TransferResume` protocol handles chunk-level resumption
  transparently.

### Architectural Invariants Preserved

- **Transfer ≠ Path**: The transfer is the durable logical operation; paths
  and sessions are replaceable carriers.
- **No new wire protocol**: Migration reuses the existing `TransferResume`
  handshake. No new `FluxMessage` variants were introduced.
- **No duplicate path-health system**: The carrier delegates to the existing
  `PathRegistry`, `PathSelector`, and `PathHealthMonitor`.
- **Gateway remains an adapter**: Migration logic lives in `flux-core`. The
  Gateway does not own path selection or session replacement.
- **Checkpoint authority**: Progress is calibrated from
  `TransferProgress.files_completed()` and actual file metadata, not from
  UI or Gateway counters.

## Consequences

### Positive

- Transfers survive single-path failures automatically.
- Multi-path environments get resilience without caller intervention.
- Migration state is observable and testable via `MigrationState`.
- Zero protocol changes — fully backward compatible with S3.5 receivers.

### Negative

- `TransferCarrier` is generic over `T: Transport`, adding a type parameter
  to the call site. Callers must hold an `Arc<T>` and `PathRegistry`.
- File-level checkpoint granularity: if a large file fails mid-transfer,
  the entire file restarts on the new path (chunk-level resume is handled
  by the receiver's existing `TransferResume` mechanism, but the sender
  re-sends from chunk 0 of that file).

### Risks

- Concurrent migration race: mitigated by the single-threaded carrier loop
  (one `send_collection_with_carrier` call per carrier instance).
- Replacement path also fails: carrier enters `Paused` state; caller can
  retry or abort.

## Alternatives Considered

1. **Multipath simultaneous transfer**: Sending chunks across multiple paths
   concurrently. Rejected — significantly harder, different sprint scope.
2. **Gateway-driven migration**: Having the Gateway detect failures and
   orchestrate path switches. Rejected — violates the core/adapter boundary.
3. **New wire protocol for migration signaling**: Rejected — existing
   `TransferResume` is sufficient.

## Compatibility

- Fully backward compatible with S3.5 `TransferContinuation` and all
  existing `TransferManager` methods.
- No changes to `FluxMessage`, framing, or transport interfaces.
- Gateway continues to use `send_collection_with_cancel_and_progress`
  unchanged.
