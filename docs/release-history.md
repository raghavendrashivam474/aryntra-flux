# Aryntra Flux Release History

This document provides the canonical map of sprint iterations, milestone objectives, release tags, and associated git commit SHAs in the Aryntra Flux repository.

---

## 1. Release Mapping Table

| Sprint | Milestone / Description | Release Tag | Target Commit SHA | Status |
|---|---|---|---|---|
| **S1.1** | Foundation & Peer Identity | `v1.1` | *(Historical)* | Released |
| **S1.2** | Identity and Discovery | `v1.2` | *(Historical)* | Released |
| **S1.3** | Transport & Framing | `v1.3` | *(Historical)* | Released |
| **S1.4** | Chunked File Transfer | `v1.4` | *(Historical)* | Released |
| **S1.5** | Transfer Resume & Recovery | `v1.5` | *(Historical)* | Released |
| **S1.6** | Multi-File & Directory Transfer | `v1.6.0` | *(Historical)* | Released |
| **S2.1** | Multi-Path Connectivity Foundation | `v2.1.0` | `37ed3b4` | Released |
| **S2.2** | Path Measurement & Auto-Selection | `v2.2.0` | `1661e45` | Released |
| **S2.3** | Probing & Continuous Path Health | `v2.3.0` | `a1894a9` | Released |
| **S3.1** | Shyam Contract Mapping | `v0.3.1` | `c93f1ab` | Released |
| **S3.2** | Flux HTTP Gateway Foundation | `v0.3.2` | `ecb5cf8` | Released |
| **S3.2+**| Gateway Runtime Hardening | `v0.3.3` | `85f8899` | Released |
| **S3.3** | Reliable Transfer Control | `v0.3.4` | `d6706a1` | Released |
| **S3.4** | Transfer Observability | `v0.3.5` | `db2cea5` | Released |
| **S3.5** | Path-Aware Transfer Continuity | `v0.3.6` | `788c109` | Released (Current Baseline) |
| **S3.5.1**| Versioning & Release Hygiene | — | *Current HEAD* | Micro-Sprint (Docs only) |
| **S3.6** | Path-Aware Transfer Migration | `v0.16.0` | *Current HEAD* | Completed |

---

## 2. Historical Tag Streams

The repository reflects three evolutionary tagging phases:

1. **Sprint 1 Series (`v1.1` – `v1.6.0`)**: Initial single-path P2P protocol development.
2. **Sprint 2 Series (`v2.1.0` – `v2.3.0`)**: Multi-path discovery, measurement, probing, and health management.
3. **Sprint 3 Series (`v0.3.1` – `v0.3.6`)**: Gateway architecture, reliable control, transfer observability, and path continuity.
4. **Normalized Future Stream (`v0.4.0` onward)**: Uniform SemVer `vX.Y.Z` where release tag numbers decouple from sprint numbers.

---

## 3. History Preservation Principle

Historical tags and commits remain immutable. No historical tags will be renamed, deleted, or backfilled.
