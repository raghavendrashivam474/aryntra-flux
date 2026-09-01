# ADR 005: Socket Address Reuse

Status: Accepted

Context: Port contention during local testing.

Decision: Use SO_REUSEADDR via socket2 crate.
