# ADR-0012: Cooperative Transfer Cancellation and Session Control

## Status
Accepted

## Context
In Sprint S3.2, raw tokio task abortion was used to terminate file transfers. While behavioral, this had two primary drawbacks:

* It left connection protocol states ambiguous and was non-cooperative over the network.
* It forced session teardowns instead of allowing the underlying connection to remain established for subsequent control messages or consecutive file transfers.

With future adaptive path migration (S3.5) and path-aware transfer continuity (S3.4) on the horizon, transfers must be treated as independent logical operations that can be paused, observed, or cancelled, independently of the underlying communication session.

## Decision
* **Wire-level Cooperative Cancellation:** Introduce `FluxMessage::TransferCancel { transfer_id: TransferId }` to allow either peer to trigger cancellation cooperatively.
* **TransferCancellation Token:** Implement a thread-safe atomic cancellation handle (`TransferCancellation`) inside `flux-core` that allows clean, out-of-band triggering without ownership-consuming API limits.
* **Session Non-consuming Control:** Introduce `Session::shutdown(&mut self)` to gracefully shut down session channels cooperatively (by sending a final `Goodbye`) without consuming session ownership, maintaining complete alignment with our collection-level lifecycle structures.
* **Resumable State Preservation:** When cancellation is triggered (either locally or over-the-wire), the receiver explicitly preserves partial files (`.part` and `.part.meta`) rather than purging them, ensuring that the existing sequence-based resume machinery remains functional for subsequent transfer attempts.

## Consequences
* Clean, non-destructive cancellations can be invoked dynamically from Gateway endpoints.
* Underlying sessions remain intact and stable even when an individual transfer fails or is cancelled.
* The same transfer control abstractions can scale to Android-specific logic (S3.5+) without breaking wire-level semantics.