# Aryntra Flux

Aryntra Flux is an adaptive, cross-device data-transfer system designed for resilience and performance.

## The Core Concept
Flux abstracts the complexity of networking. Instead of choosing between Wi-Fi, Bluetooth, or Cloud, Flux evaluates available paths (Local LAN, Direct P2P, Relays) and moves data through the most efficient route, adapting in real-time if conditions change.

## Project Structure
- **/crates/flux-core**: The engine logic, transport traits, and routing.
- **/crates/flux-node**: The executable binary for running a Flux instance.
- **/docs**: Detailed architectural and design documentation.
- **/clients**: Platform-specific client applications (Desktop/Mobile).

## Getting Started

### Prerequisites
- Rust (Latest Stable)
- Node.js & npm (for Desktop client)

### Environment Check
To ensure your machine is ready for development, run:
\\\ash
cargo run --package flux-node -- doctor
\\\

## Development
See [CONTRIBUTING.md](./CONTRIBUTING.md) for toolchain requirements and workflow.
