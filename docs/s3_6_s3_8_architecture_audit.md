# S3.6 – S3.8 Architecture Audit & ADR Normalization Record

**Date:** 2026-10-02
**Baseline:** v0.19.0 / S3.9
**Scope:** S3.6 (v0.15.0 → v0.16.0) through S3.8 (v0.17.0 → v0.18.0), extended through S3.9 for ADR normalization
**Objective:** Determine whether undocumented architectural decisions exist in S3.6–S3.8 and chronologically normalize ADR numbering.

---

## 1. Historical Ledger

| Sprint | Release | Commits | Major Changes | Architectural Changes |
|--------|---------|---------|---------------|----------------------|
| S3.6 | v0.16.0 | 6 | TransferCarrier, MigrationState, autonomous migration | New abstraction: TransferCarrier separates transfer from path |
| S3.7 | v0.17.0 | 4 | Gateway CarrierReturnGuard, lazy PathId resolution | Integration of existing architecture into Gateway adapter |
| S3.8 | v0.18.0 | 5 | ChunkCheckpoint, TransferContinuation extension | Sender-side chunk-level progress during migration |

---

## 2. S3.6 Analysis (v0.15.0 → v0.16.0)

### Commits Investigated

| Hash | Subject | Files |
|------|---------|-------|
| 5ac69c8 | fix(gateway): read bind address from FLUX_GATEWAY_BIND env var | crates/flux-gateway/src/main.rs |
| 78fe571 | docs(versioning): document release version convention | docs/release-history.md, docs/versioning.md |
| 4284737 | feat(transfer): introduce TransferCarrier and MigrationState | crates/flux-core/src/transfer/migration.rs, mod.rs |
| 10f71ef | feat(transfer): implement autonomous carrier migration | crates/flux-core/src/lib.rs, transfer/manager.rs |
| 2a9580a | test(transfer): verify path failure migration | crates/flux-core/tests/transfer_test.rs |
| b2c20c2 | docs: document S3.6 architecture | docs/S3.6-completion-report.md, ADR-0015, architecture.md |

### Existing ADR Coverage

**ADR-0015: Autonomous Path-Aware Transfer Migration** — ✅ Comprehensive

Covers:
- Context (S3.5 caller-driven resumption limitation)
- Problem (transfer aborts on path failure)
- Decision (TransferCarrier abstraction with MigrationState machine)
- Key Components (MigrationState, TransferCarrier<T>, send_collection_with_carrier)
- Architectural Invariants (Transfer ≠ Path, no new wire protocol, Gateway remains adapter)
- Consequences (positive, negative, risks)
- Alternatives Considered (multipath simultaneous, Gateway-driven, new protocol)
- Compatibility (backward compatible with S3.5)

### Audit Finding

**No missing architectural decisions.** ADR-0015 accurately and thoroughly captures the architecture implemented in S3.6. Non-architectural commits (gateway bind fix, versioning docs) are correctly excluded.

---

## 3. S3.7 Analysis (v0.16.0 → v0.17.0)

### Commits Investigated

| Hash | Subject | Files |
|------|---------|-------|
| 30bd9dc | feat(gateway): integrate TransferCarrier into Gateway transfer flow | flux-gateway/Cargo.toml, routes/transfer.rs |
| d4e618d | test(gateway): add E2E autonomous migration integration tests | flux-gateway/tests/integration.rs |
| 6df0d8c | docs: update architecture, roadmap, release history | docs/S3.7-completion-report.md, architecture.md, release-history.md, roadmap.md |
| 17588d4 | docs: include detailed S3.7 post-completion report | docs/S3.7_post_completion_report.md |

### Key Implementation Changes

1. **SessionReturnGuard → CarrierReturnGuard**: RAII guard holds TransferCarrier instead of bare Session. On drop, returns carrier.session (current, possibly post-migration) to pool.
2. **Lazy PathId resolution**: Gateway resolves PathId by matching session.remote_addr() against PathRegistry at transfer start.
3. **Transfer invocation rewire**: send_collection_with_cancel_and_progress() → send_collection_with_carrier().

### Critical Evidence: Implementation Team's Own Assessment

From docs/S3.7_post_completion_report.md, Section 5 ("What Was NOT Done"):

> **"No ADR created — the existing ownership model proved sufficient; no architectural decision was required"**

### Architectural Classification

| Change | Category | Reason |
|--------|----------|--------|
| CarrierReturnGuard | Category B (Design) | Internal RAII pattern change within existing Gateway adapter boundary |
| Lazy PathId resolution | Category C (Implementation) | Minimal adapter wiring, no new contract |
| send_collection_with_carrier() call | Category C (Implementation) | Call-site change to use existing API |
| Gateway-Core ownership boundary | Category A (Architecture) | **Already documented** in ADR-0015 and architecture.md |

### Audit Finding

**No missing architectural decision warranting a new ADR.** S3.7 was an integration sprint applying S3.6 architecture to the Gateway adapter. The Gateway-as-thin-adapter invariant was pre-established in ADR-0015.

---

## 4. S3.8 Analysis (v0.17.0 → v0.18.0)

### Commits Investigated

| Hash | Subject | Files |
|------|---------|-------|
| 93581b6 | feat(transfer): introduce ChunkCheckpoint model | flux-core/src/transfer/continuity.rs, mod.rs |
| 47e81c8 | feat(transfer): integrate chunk checkpointing, prioritize cancellation | flux-core/src/transfer/manager.rs |
| 81934a0 | test(transfer): S3.8 unit and E2E migration tests | flux-core/tests/transfer_test.rs |
| cf1e821 | docs(transfer): add ADR-0018 and S3.8 completion report | docs/decisions/0016-sender-side-chunk-checkpointing.md, docs/s3_8_completion_report.md |
| d688252 | docs(transfer): add finalized S3.8 post-completion report | docs/s3.8_post_completion_report.md |

### Existing ADR Coverage

**ADR-0016 (formerly designated ADR-0018): Sender-Side Chunk Checkpointing & Efficient Migration** — ✅ Adequate

Covers:
- Context (file-level sender checkpoint limitation)
- Problem (unnecessary re-transmission on migration)
- Decision (ChunkCheckpoint state model in continuity.rs)
- Invariant (receiver .part.meta remains authoritative)
- Consequences (correctness, efficiency, simplicity)

---

## 5. ADR Numbering Normalization

### Rationale

ADR numbers in Flux represent **sequential architectural decisions**, not sprint numbers. Because S3.7 was an integration sprint that established no new architectural boundaries, no ADR exists for S3.7.

Consequently, the decisions authored in S3.8 and S3.9 have been chronologically normalized into continuous sequence:

| Original Designator | Sprint | Normalized ADR | Decision Title |
|---------------------|--------|----------------|----------------|
| ADR-0014 | S3.5 | **ADR-0014** | Path-Aware Transfer Continuity |
| ADR-0015 | S3.6 | **ADR-0015** | Autonomous Path-Aware Transfer Migration |
| *(None)* | S3.7 | *(None)* | *Gateway Integration (Adapter only; adheres to ADR-0015)* |
| ADR-0018 | S3.8 | **ADR-0016** | Sender-Side Chunk Checkpointing & Efficient Migration |
| ADR-0019 | S3.9 | **ADR-0017** | Cascading Path Migration & Multi-Fallback Transfer Recovery |

### Integrity Safeguards

- No historical decisions were fabricated or added.
- No ADR content was altered beyond the title number and a clarifying historical origin note.
- All internal repository references have been audited and updated.

---

## 6. Cross-Sprint Architectural Evolution

```text
S3.5 (v0.15.0) — Path-Aware Continuity
  Explicit caller-driven resume via TransferContinuation.
  [ADR-0014]
      ↓
S3.6 (v0.16.0) — Autonomous Migration
  TransferCarrier abstraction separates transfer from path.
  MigrationState machine manages lifecycle.
  File-level sender checkpoint; receiver handles chunk resume.
  [ADR-0015]
      ↓
S3.7 (v0.17.0) — Gateway Integration
  TransferCarrier wired into Gateway adapter via CarrierReturnGuard.
  Gateway remains thin adapter; zero migration logic in Gateway.
  [architecture.md S3.7 section]
      ↓
S3.8 (v0.18.0) — Chunk-Efficient Migration
  ChunkCheckpoint model enables sender-side chunk-level resume.
  Receiver .part.meta remains authoritative.
  [ADR-0016 (formerly 0018)]
      ↓
S3.9 (v0.19.0) — Cascading Migration
  Multi-fallback path migration with cascading retry loop.
  [ADR-0017 (formerly 0019)]
```
## 7. Final Audit Table

| Sprint | Decision | Evidence | Normalized ADR | Classification | Action Taken |
|--------|----------|----------|----------------|----------------|--------------|
| S3.6 | Autonomous path migration via TransferCarrier | commits 4284737, 10f71ef; migration.rs | ADR-0015 | Category A — Architecture | Retained as ADR-0015 |
| S3.7 | Gateway CarrierReturnGuard integration | commit 30bd9dc; routes/transfer.rs | N/A | Category B — Design (adapter) | Documented in architecture.md |
| S3.8 | Sender-side ChunkCheckpoint | commits 93581b6, 47e81c8; continuity.rs | ADR-0016 | Category A — Architecture | Renormalized from 0018 → 0016 |
| S3.9 | Cascading multi-fallback path migration | manager.rs, transfer_test.rs | ADR-0017 | Category A — Architecture | Renormalized from 0019 → 0017 |

---

## 8. Definition of Done

- [x] S3.6 history reconstructed
- [x] S3.7 history reconstructed
- [x] S3.8 history reconstructed
- [x] ADR-0015 audited (comprehensive, accurate)
- [x] ADR-0016 (formerly 0018) audited (adequate, accurate)
- [x] ADR-0017 (formerly 0019) audited (acknowledged as current S3.9 decision)
- [x] ADR sequence normalized (0014 → 0015 → 0016 → 0017)
- [x] Every candidate decision classified (Categories A–D)
- [x] Evidence recorded (commit hashes, file paths, document references)
- [x] Stale cross-references audited and updated across docs/
- [x] No production code changed
- [x] No Git history rewritten
