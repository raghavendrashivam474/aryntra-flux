# Flux Architecture

## Blended Model
Flux Core remains agnostic of the specific transport layer.

## Layers
1. **Application**: UI (Tauri/React)
2. **Flux Core (Transfer / Logic)**:
   - **Collection Layer (S1.6)**: `TransferPlan` and `TransferItem` build recursive directory structures and multiple items with deterministic sorted order and security sanitization.
   - **Transfer Layer (S1.4 + S1.5)**: Chunking, Seek operations, Verification, and Sidecar Resume State.
3. **Transport Abstraction**: Traits defining Send/Receive
4. **Transport Implementation**: QUIC, TCP, Bluetooth, etc.

## Transfer Subsystem (S1.4 + S1.5 + S1.6)

```text
TransferPlan / TransferItem (S1.6 Collection Model)
↓
TransferManager (send_collection / receive_collection)
├── Chunker (streaming read + seek for resume)
├── FileReceiver (reassembly + verification + state persistence + relative folders)
└── Resume State (.part.meta sidecar with relative_path)
↓
Session (send_message / recv_message - reused sequentially)
↓
Protocol (FluxMessage: bincode + 4-byte length prefix)
↓
Transport (TCP)
```

## Path Hierarchy

- Local (LAN/Wi-Fi)
- Direct P2P
- Relay (Fallback)
rn
## Multi-Path Architecture (S2.1)

Flux models peer connectivity using an explicit multi-path abstraction layer:

```text
                  ┌──────────────┐
                  │    PeerId    │
                  └──────┬───────┘
                         │ 1:N
                  ┌──────▼───────┐
                  │   PathSet    │
                  └──────┬───────┘
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
   ┌───────────┐   ┌───────────┐   ┌───────────┐
   │  Path A   │   │  Path B   │   │  Path C   │
   │ (LAN TCP) │   │(Wi-Fi TCP)│   │ (Relay)   │
   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
         │               │               │
         ▼               ▼               ▼
   ┌───────────┐   ┌───────────┐   ┌───────────┐
   │  Session  │   │  Session  │   │  Session  │
   └─────┬─────┘   └───────────┘   └───────────┘
         │
         ▼
   ┌───────────┐
   │ Transfer  │
   └───────────┘
```

Peer Identity vs Route Identity: PeerId tracks who the node is. PathId tracks a specific route (TransportKind + SocketAddr) to reach them.
Path Registry: PathRegistry maintains thread-safe collections of candidate paths per peer, updated dynamically via discovery.
Path States: Discovered -> Candidate -> Connecting -> Available / Unavailable.
## Path-Aware Transfer Migration (S3.6)

S3.6 establishes the formal separation of the durable **logical transfer operation** from the replaceable **physical communication route (carrier)**:

```text
               ┌──────────────────────┐
               │    TransferManager   │
               └──────────┬───────────┘
                          │
                  logical progress
                          │
                          ▼
               ┌──────────────────────┐
               │   TransferCarrier    │
               └──────────┬───────────┘
                          │
             ┌────────────┴────────────┐
             │                         │
       current_path             replacement_path
             │                         │
             ▼                         ▼
         Session A                 Session B
             │                         │
             ▼                         ▼
          Path A                    Path B
             │
             X (carrier failure)
```
- **Transfer ≠ Path**: A transfer represents a high-level logical transaction (source files, destination directory, progress). The path and its associated session are merely temporary transport carriers.
- **`TransferCarrier<T: Transport>`**: Orchestrates active session tracking, alternate path querying, and autonomous connection establishment.
- **Autonomous Migration State Machine**:
  - `Active`: Transmitting chunks over the active path.
  - `Migrating`: Active path failed; registry updated to `Unavailable`, session shut down, `PathSelector` queried, and connection to a replacement path initiated.
  - `Paused`: No backup paths available; logical checkpoint safely preserved.
  - `Completed`: Complete collection transferred and verified.
- **Resumption Semantics**: Resumes using the receiver's existing `TransferResume` sidecar checkpoints. Progress is calculated from the last fully completed file, and the chunk-level resume completes the file transmission.

## Gateway-Aware Autonomous Transfer (S3.7)
S3.7 integrates the S3.6 `TransferCarrier` migration mechanism into the Flux Gateway HTTP transfer flow, making Gateway-initiated transfers resilient to path/session failure without the Gateway implementing any migration logic.
```text
                     ┌──────────────┐
                     │    Client    │
                     └──────┬───────┘
                            │ HTTP
                            ▼
                 ┌──────────────────────┐
                 │   Gateway Adapter    │
                 │  (routes/transfer)   │
                 │                      │
                 │  • transfer_id       │
                 │  • cancel token      │
                 │  • progress tracker  │
                 │  • CarrierReturnGrd  │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │   TransferManager    │
                 │ send_collection_     │
                 │   with_carrier(...)  │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │  TransferCarrier     │
                 │  (owns current       │
                 │   Session + state)   │
                 └──────────┬───────────┘
                            │
                 ┌──────────┴───────────┐
                 ▼                      ▼
              Session              PathSelector
                 │                      │
                 ▼                      ▼
             Transport             PathRegistry
                 │
          ┌──────┴──────┐
          ▼             ▼
       Path A         Path B
       (dead)        (active)
```

* **Gateway is a thin adapter:** The Gateway constructs a `btTransferCarrier`, passes it to `btTransferManager::sendcollectionwithcarrier()`, and is otherwise unaware of migration events.

* **CarrierReturnGuard:** An RAII guard that holds the carrier (not a bare Session) and returns `btcarrier.session` to the pool on drop — ensuring the current (possibly post-migration) session is returned, never a stale one.

* **Lazy PathId resolution:** The Gateway resolves the active `btPathId` by matching `btsession.remoteaddr()` against `btPathRegistry` entries at transfer start time. No persistent session-to-path mapping required.

* **Transfer identity stability:** The UUID-based `bttransferid`, cancellation token, and progress tracker remain unchanged across any number of internal migrations. The Gateway client sees one continuous transfer.

* **Ownership boundary preserved:** Path selection, health monitoring, migration state, and session replacement all remain exclusively within Flux Core. The Gateway knows only: transfer exists, progressed, completed, failed, or was cancelled.