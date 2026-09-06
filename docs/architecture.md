# Flux Architecture

## Blended Model
Flux Core remains agnostic of the specific transport layer.

## Layers
1. **Application**: UI (Tauri/React)
2. **Flux Core**: Logic, Chunking, Routing, Resume State
3. **Transport Abstraction**: Traits defining Send/Receive
4. **Transport Implementation**: QUIC, TCP, Bluetooth, etc.

## Transfer Subsystem (S1.4 + S1.5)
```text
TransferManager
├── Chunker (streaming read + seek for resume)
├── FileReceiver (reassembly + verification + state persistence)
└── Resume State (.part.meta sidecar)
↓
Session (send_message / recv_message)
↓
Protocol (FluxMessage: bincode + 4-byte length prefix)
↓
Transport (TCP)
```


## Path Hierarchy
- Local (LAN/Wi-Fi)
- Direct P2P
- Relay (Fallback)
