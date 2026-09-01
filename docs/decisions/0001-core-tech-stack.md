# ADR 001: Core Technology Stack

## Status
Accepted

## Context
We need a system that is memory-safe, high-performance, and capable of handling low-level networking across Windows, Linux, macOS, and Mobile.

## Decision
We will use **Rust** for the Flux Core.
We will use **Tauri (React/TS)** for the Desktop Client.

## Consequences
- High performance and safety.
- Cross-platform binary compatibility.
- Steep learning curve for networking (QUIC/mDNS), but high long-term stability.
