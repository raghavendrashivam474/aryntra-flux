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
Peer Identity vs Route Identity: PeerId tracks who the node is. PathId tracks a specific route (TransportKind + SocketAddr) to reach them.
Path Registry: PathRegistry maintains thread-safe collections of candidate paths per peer, updated dynamically via discovery.
Path States: Discovered -> Candidate -> Connecting -> Available / Unavailable.