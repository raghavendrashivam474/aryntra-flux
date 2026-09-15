# ADR 0009: Multi-File Collection Transfer (S1.6)

## Status
Accepted

## Context
Flux S1.4 and S1.5 established a reliable single-file transfer primitive with chunked streaming, SHA-256 verification, and resume-from-interruption recovery. However, users must still treat each file as an independent transfer operation, establishing a new session for every file. This is inefficient and prevents batch workflows.

## Problem
How do we extend Flux to transfer multiple files and directories over a single session without rewriting the existing protocol, breaking resume semantics, or introducing premature concurrency?

## Options Considered

### Option A: New batch wire protocol
Introduce `BatchRequest`, `BatchChunk`, `BatchComplete` messages that multiplex files within a single protocol stream.

**Rejected.** This duplicates the entire S1.4/S1.5 transfer machinery, increases protocol surface area, and creates a parallel code path that must independently maintain resume and integrity guarantees.

### Option B: Parallel transfers over multiple sessions
Open N simultaneous TCP connections, one per file.

**Rejected.** Concurrency/multiplexing is a separate architectural problem (future Phase 2 milestone). Introducing it now would complicate the codebase before the collection abstraction is proven.

### Option C: Sequential orchestration over one session (chosen)
Reuse the existing per-file protocol messages (`TransferRequest` → `TransferAccept/Resume` → `TransferChunk` → `TransferComplete` → `TransferResult`) in a loop over a single established session. Signal collection completion using the existing `Goodbye` message.

**Accepted.** This composes the existing reliable primitive rather than replacing it. Zero protocol changes required. Resume semantics are preserved per-file.

## Decision
S1.6 introduces a `TransferPlan` / `TransferItem` abstraction layer above `TransferManager`. The plan enumerates files (from explicit paths or recursive directory walks), sorts them deterministically by relative path, and orchestrates sequential single-file transfers over one session.

Key design choices:
- **No new wire protocol messages.** Collection termination uses `Goodbye` (S1.3).
- **`TransferMetadata.relative_path: Option<String>`** added to support nested directory reconstruction on the receiver side.
- **`PartialTransferState.relative_path`** added to maintain resume identity for directory contents.
- **Path sanitization** via `sanitize_relative_path()` rejects traversal (`../../`), absolute paths, and empty components.
- **Deterministic ordering** by sorted relative path ensures reproducible transfers and predictable resume behavior.
- **Receiver-side directory creation** happens automatically via `fs::create_dir_all` before file writes.

## Consequences

### Positive
- Multi-file and directory transfers work over a single TCP session.
- Existing single-file transfers remain fully backward-compatible.
- Per-file resume continues to work independently within a collection.
- Zero protocol version bump required.
- Clean separation: collection logic lives in `collection.rs`, transfer mechanics remain in `manager.rs`.

### Negative
- Sequential-only: large collections of small files may be slower than parallel transfers (addressed in future Phase 2).
- `Goodbye` is overloaded as both a session-close and collection-end signal; a dedicated `CollectionComplete` message could be cleaner in the future but is not justified at this scale.

### Neutral
- The `relative_path` field is `Option<String>` to preserve backward compatibility with S1.4/S1.5 metadata that lacks it.
