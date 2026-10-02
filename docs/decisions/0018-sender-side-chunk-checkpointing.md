# ADR 0018: Sender-Side Chunk Checkpointing & Efficient Migration

## Context
Prior to Sprint S3.8, carrier migration (introduced in S3.6 and integrated with the Gateway in S3.7) was file-level on the sender side. When a transport failure occurred mid-file, the sender-side `Chunker` was dropped. Upon migration, the transfer would resume starting from the beginning of the interrupted file, relying on the receiver to discard duplicate bytes. While correct, this re-read, re-hashed, and re-sent bytes over the network unnecessarily, reducing migration efficiency.

## Problem
How do we introduce sender-side chunk-level progress awareness during carrier migration without:
1. Violating the "one source of truth" principle.
2. Creating a competing progress authority with `TransferProgress` or the receiver's authoritative `.part.meta`.
3. Breaking backward compatibility with legacy and Gateway-driven transfers.

## Decision
We introduce a lightweight, precise `ChunkCheckpoint` state model inside `continuity.rs`.
- `ChunkCheckpoint` tracks `file_index`, `completed_chunks`, `next_chunk`, and `bytes_completed`.
- It is updated sequentially as soon as `session.send_message(TransferChunk)` completes successfully.
- It is owned by `TransferContinuation` and `TransferManager::send_collection_with_carrier`.
- On carrier failure, the exact `bytes_completed` for the active chunk boundary is preserved, the `TransferProgress` is calibrated, and the replacement session begins sending from the receiver-verified chunk offset.
- The receiver's `TransferResume` response remains the final authority for the starting chunk index, ensuring total synchronization and preventing any possibility of data corruption.

## Consequences
- **Correctness**: Preserved. SHA-256 remains authoritative and matches byte-for-byte.
- **Efficiency**: Already completed chunks are never re-transmitted or re-read from disk.
- **Simplicity**: No changes to the wire protocol or Gateway are required. No competing state machines.
