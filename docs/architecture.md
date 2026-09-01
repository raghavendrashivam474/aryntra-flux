# Flux Architecture

## Blended Model
Flux Core remains agnostic of the specific transport layer.

## Layers
1. **Application**: UI (Tauri/React)
2. **Flux Core**: Logic, Chunking, Routing
3. **Transport Abstraction**: Traits defining Send/Receive
4. **Transport Implementation**: QUIC, TCP, Bluetooth, etc.

## Path Hierarchy
- Local (LAN/Wi-Fi)
- Direct P2P
- Relay (Fallback)
