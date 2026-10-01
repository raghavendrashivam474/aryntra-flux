# Aryntra Flux Versioning & Release Policy

## 1. Purpose

This document defines the canonical versioning, tagging, and release policies for the Aryntra Flux repository. It establishes a uniform, deterministic convention for all future releases while preserving historical tags and commit integrity.

---

## 2. Release Tag Format

All future releases follow Semantic Versioning (SemVer 2.0.0) prefixed with `v`:

v<MAJOR>.<MINOR>.<PATCH>

### Examples:
- `v0.4.0` (Next milestone release)
- `v0.4.1` (Patch release)
- `v0.5.0` (Subsequent milestone release)

### Prohibited Tag Formats:
- Short versions: `v4.0`, `v4.1`
- Prefixed names: `release-4`, `flux-v0.4.0`
- Sprint identifiers as tags: `s3.6`, `S3.6`

---

## 3. Sprint Identifier vs. Release Version

Sprint milestones and product releases are distinct concepts:

| Concept | Purpose | Format | Example |
|---|---|---|---|
| **Sprint** | Internal engineering iteration | `S<Track>.<Iteration>` | `S3.6 — Path-Aware Transfer Migration` |
| **Release** | Distributable artifact / Git tag | `v<MAJOR>.<MINOR>.<PATCH>` | `v0.4.0` |

A sprint completion report must reference both concepts clearly:
- **Sprint**: `S3.6 — Path-Aware Transfer Migration`
- **Release Tag**: `v0.4.0`

---

## 4. Version Increment Policy

During pre-1.0 development (`0.x.y`), breaking or substantial architecture changes may occur between minor versions.

### 4.1 PATCH (`v0.4.0` → `v0.4.1`)
- Backward-compatible bug fixes.
- Documentation corrections accompanying a release fix.
- Non-breaking implementation or test corrections.

### 4.2 MINOR (`v0.4.0` → `v0.5.0`)
- Major planned sprint milestones.
- New features, protocol extensions, or capability additions.
- Significant module expansions.

### 4.3 MAJOR (`v0.x.y` → `v1.0.0`)
- Reserved for the formal public API stability milestone.
- Flux remains in `0.x` development until stability criteria are formally met.

---

## 5. Tag Immutability & Safety Rules

1. **Immutability**: Once created and pushed, a release tag must never be moved, amended, or overwritten.
2. **No Deletion**: Historical tags must never be deleted from local or remote repositories.
3. **No Force Pushing**: Force-pushing tags (`git push --force origin <tag>`) is strictly forbidden.
4. **No Backfilling**: Do not invent retroactive tags to fix historical inconsistencies.
5. **No Premature Tagging**: Release tags are created only **after** sprint implementation, verification, and formatting are complete.

---

## 6. Current Baseline & Next Release

- **Current Baseline Release**: `v0.3.6` (Commit `788c109`)
- **Current Sprint Baseline**: `S3.5 — Path-Aware Transfer Continuity`
- **Next Planned Release**: `v0.4.0` (Associated with Sprint `S3.6 — Path-Aware Transfer Migration`)