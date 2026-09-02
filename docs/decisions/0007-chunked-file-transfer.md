# ADR-007: Chunked File Transfer Architecture

## Status
Accepted

## Context
In Sprint 1.3, Aryntra Flux successfully decoupled its connection lifecycle into an abstract Transport layer (TCP) and an identity-verifying Session layer. With Sprint 1.4, we introduce the first true application utility: **end-to-end file transfer**.

Moving files over a session network introduces several critical architectural challenges:
1. **Memory Safety:** Loading entire multi-gigabyte files into RAM causes memory exhaustion and crashes.
2. **Protocol Framing Limits:** S1.3 enforces a strict `1 MB` framing guard on protocol payloads to prevent denial-of-service (DoS) memory allocations.
3. **Network Efficiency:** Interrupted network pipes can corrupt incomplete data unless integrity checks are verified cryptographically.
4. **Path Traversal Security:** A malicious peer could send a filename containing relative path qualifiers (e.g., `../../etc/passwd` or `..\..\Windows\System32\cmd.exe`) to overwrite sensitive host system files.

## Decisions
To resolve these requirements safely and cleanly without modifying S1.3 core transport rules, we chose the following architecture:

1. **Layering Architecture:**
   The `TransferManager` is positioned strictly **above the Session layer**. The underlying transport remains unaware of "files"; it only knows it is carrying framed messages. The Session layer acts as a stable, identity-verified transport pipe.

2. **Fixed 64 KiB Chunking Stream:**
   We enforce a default chunk size of `64 KiB`.
   * This is well below the `1 MB` frame-guard threshold.
   * Both sender and receiver process file chunks sequentially using buffered asynchronous reading (`tokio::fs::File`), maintaining a constant memory overhead of `<64 KiB` regardless of total file size.

3. **In-Protocol Serialization (Unified Message Space):**
   Instead of introducing an auxiliary transfer protocol, we extended the existing S1.3 `FluxMessage` enum with transfer-specific variants:
   * `TransferRequest { metadata }`
   * `TransferAccept { transfer_id }`
   * `TransferReject { transfer_id, reason }`
   * `TransferChunk { transfer_id, index, data }`
   * `TransferComplete { transfer_id }`
   * `TransferResult { transfer_id, success, message }`

4. **Temporary File Sandboxing (`.part` Reassembly):**
   We isolate all incoming file chunks inside a secure, relative folder called `received/`. Files are assembled sequentially inside a `.part` temporary file (e.g., `received/file.zip.part`). Only after successful sizing and cryptographic matching is the file safely renamed to its destination name.

5. **Triple-Point Cryptographic Verification:**
   Before marking a file transfer as successful, the receiver performs three consecutive assertions:
   * **Total Chunks Received:** Ensures all packets in the sequence arrived without drops.
   * **Exact Byte Count Comparison:** Verifies physical file size matches metadata.
   * **SHA-256 Hash Matching:** Real-time hashing of arriving bytes must match the pre-calculated SHA-256 checksum sent in the `TransferRequest`.

6. **Filename Path Sanitization:**
   The receiver filters and cleans all remote filenames using regex/string filtering, preserving only alphanumeric values, dots, underscores, and hyphens. Relative or absolute paths are strictly rejected with an instant `TransferError::InvalidFilename` exception.

## Consequences
* **Unified Sessions:** The protocol remains unified, meaning future features (like multi-file sync or chat) can run simultaneously over the same peer session without port collisions.
* **Network Fail-Safe:** The design explicitly prepares for future feature iterations such as Transfer Resume (S1.5) because every chunk is indexed and uniquely tied to a session-independent `TransferId` (UUID-v4).
* **Resource Optimization:** Bounded stream pipelines protect low-power/embedded nodes from running out of heap space.
