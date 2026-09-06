# ADR-008: Transfer Resume Protocol Design

## Status
Accepted (S1.5)

## Context
S1.4 introduced end-to-end file transfer with chunking and SHA-256 verification. However, any connection interruption forces the sender to retransmit the entire file from the beginning. For large files on unreliable networks, this is unacceptable.

## Decision
Implement sequential, chunk-boundary-based transfer resume using a persistent metadata sidecar (`.part.meta`) and a single new protocol message (`TransferResume`).

### Key Design Choices

1. **Resume matching uses file metadata, not TransferId.** `TransferMetadata::new()` generates a fresh UUID per transfer. A reconnection creates a new TransferId. Matching uses the stable tuple `(file_name, file_size, chunk_size, total_chunks, sha256)`.

2. **Sequential contiguous resume only.** The receiver tracks the highest contiguous chunk received. No sparse chunk maps. This matches the existing sequential transfer model and avoids unnecessary complexity.

3. **Disk-based SHA-256 at finalization.** Instead of persisting incremental hasher state, the complete file is hashed from disk at finalization. This works identically for fresh and resumed transfers.

4. **Write-then-record ordering.** Chunk data is flushed to disk before `.part.meta` is updated, preventing false resume horizons on crash.

5. **Single new protocol variant.** `TransferResume { transfer_id, resume_from_chunk }` is sent by the receiver instead of `TransferAccept` when valid partial state exists. The sender interprets this as "start from chunk N."

## Consequences

### Positive
- Interrupted transfers no longer waste bandwidth retransmitting received data.
- No changes to Transport or Session layers.
- Backward compatible: fresh transfers work identically when no partial state exists.
- Corrupt partial state is automatically detected and cleaned up.

### Negative
- `.part.meta` sidecar files add minor filesystem clutter during active transfers (cleaned up on completion).
- Disk-based final hashing reads the entire file from disk, adding I/O at finalization (acceptable for correctness).

## Alternatives Considered

1. **Incremental hasher persistence:** Serialize the SHA-256 hasher state to disk. Rejected because it couples the resume logic to a specific hash library's internal state format.

2. **Sparse chunk bitmaps:** Track individual chunk receipt. Rejected because the sequential transfer model makes this unnecessary; contiguous tracking is simpler and sufficient.

3. **TransferId-based matching:** Maintain a persistent registry mapping TransferIds to partial files. Rejected because TransferIds are ephemeral per-transfer; file metadata is the stable identifier.

## Files Affected
- `crates/flux-core/src/transfer/metadata.rs`
- `crates/flux-core/src/transfer/chunker.rs`
- `crates/flux-core/src/transfer/receiver.rs`
- `crates/flux-core/src/transfer/manager.rs`
- `crates/flux-core/src/protocol/message.rs`
