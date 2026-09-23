# ADR-0013: Transfer Progress Observability & Propagation

## Status
Accepted

## Context
In Sprint S3.3, Flux established cooperative cancellation (`TransferCancellation`) at chunk boundaries and exposed a basic transfer lifecycle state machine (`Created`, `Running`, `Completed`, `Failed`, `Cancelled`) via the Flux Gateway.

However, while `GatewayTransferInfo` included placeholder fields for `bytes_transferred` and `files_transferred`, these values were initialized to zero upon transfer start and never updated dynamically during transmission. Callers such as the Shyam desktop shell had no visibility into actual byte-level or file-level progress of an in-flight transfer.

Sprint S3.4 requires making an active Flux transfer observable in real time without altering transfer algorithms, wire formats, or session lifecycle semantics.

## Decision

1. **Progress Model (`TransferProgress` in `flux-core`)**:
   - Introduce `TransferProgress` in `flux_core::transfer::progress`.
   - Implement `TransferProgress` using lock-free atomics (`Arc<AtomicU64>` for bytes, `Arc<AtomicUsize>` for files).
   - Cheap to clone (single atomic reference increment) and thread-safe across asynchronous task boundaries.

2. **Boundary Updates**:
   - `TransferProgress` is updated at chunk transmission boundaries (`add_bytes(chunk_len)`) immediately after successful frame dispatch/receipt.
   - File counts (`add_file()`) are incremented immediately upon completing each file in a collection plan.
   - For resumed transfers, any pre-existing verified bytes (from `resume_from_chunk`) are credited to the transfer progress upon receipt of `TransferResume`, ensuring logical continuity.

3. **Additive API Evolution**:
   - Retain full backwards compatibility with all existing `TransferManager` public APIs:
     - `send_collection(session, plan)`
     - `send_collection_with_cancel(session, plan, cancel)`
     - `receive_collection(session, output_dir)`
     - `receive_collection_with_cancel(session, output_dir, cancel)`
   - Introduce additive variants:
     - `send_collection_with_cancel_and_progress(session, plan, cancel, progress)`
     - `receive_collection_with_cancel_and_progress(session, output_dir, cancel, progress)`

4. **Gateway State Synchronization**:
   - Store `TransferProgress` within `ActiveTransfer` in `GatewayTransferTracker`.
   - When `GET /flux/v1/transfer/{id}` is queried, `GatewayTransferTracker::get()` snapshots the live atomic values from `TransferProgress` to populate `bytes_transferred` and `files_transferred`.
   - On terminal states (`Cancelled`, `Failed`), the snapshot freezes at the last recorded byte count. On `Completed`, it reports full byte/file totals.

5. **Transport & Protocol Invariance**:
   - No new wire protocol messages are introduced. Observability is entirely an in-memory runtime concern.
   - No WebSockets or persistent streaming connections are added. Polling `GET /flux/v1/transfer/{id}` satisfies all Shyam observability requirements with low overhead.

## Consequences
- Zero breaking changes to `flux-core` or `flux-gateway` existing interfaces.
- Lock-free, zero-allocation runtime overhead on the chunk transmission hot path.
- Consistent progress reporting across cancellations, failures, and resumable transfers.
