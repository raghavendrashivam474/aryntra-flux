# S3.9 Investigation: Cascading Path Migration

## A. How Migration Works Today (v0.18.0)

1. send_collection_with_carrier() in manager.rs (L673) runs a file loop.
2. On transport error during a file transfer, it checks cancellation (L730).
3. Calls carrier.migrate().await? (L745) — **single-shot**.
4. migrate() in migration.rs (L73):
   - Sets state to Migrating
   - Calls selector.select_path(&self.peer_id) for ONE candidate
   - Attempts 	ransport.connect() to that candidate
   - On success: installs new session, sets state to Active
   - On failure: returns Err — **no fallback to next candidate**
5. The ? in manager.rs propagates the error, terminating the transfer.

## B. What Prevents Cascading Today

- migrate() has no internal retry loop over candidates.
- If the selected candidate's connection fails, migrate() returns Err immediately.
- PathSelector::select_path() accepts no exclusion list — it picks the best Available path.
- There is no mechanism to mark a just-failed candidate as Unavailable before re-querying.

## C. Where Cascading Should Live

**Primary: TransferCarrier::migrate() in migration.rs**

The migrate() method should internally loop over candidates:
- Select candidate via PathSelector
- Attempt connection
- On connection failure: mark candidate unavailable, select next
- On success: install session, return Ok
- On exhaustion: return Err (terminal)

This keeps the manager.rs loop unchanged — it still calls migrate() once per failure event.

## D. State Machine Support

MigrationState::Migrating already contains migration_count.
The Active → Migrating → Active transition is already legal.
Repeated transitions should work if migrate() succeeds on a later candidate.
**No state machine change required** — just internal looping within migrate().

## E. PathSelector Candidate Support

select_path() filters by PathState::Available only.
**No API change needed** if we mark failed candidates as Unavailable in the
PathRegistry/PathHealthMonitor between selection attempts.
The selector will naturally skip them on the next call.

## F. S3.8 Checkpoint Support for Repeated Migration

**Yes, already supported.** The ChunkCheckpoint lives in the manager loop
and is NOT reset on migration. Each retry of the same file index reuses the
existing checkpoint. The receiver's FileReceiver::try_resume() reads the
.part.meta and returns the authoritative esume_from_chunk.

## G. Wire Protocol Changes Required

**No.** The existing TransferResume message and .part/.part.meta
mechanism handles resume after any number of migrations.

## H. Implementation Plan

1. Add internal candidate loop to TransferCarrier::migrate()
2. Mark failed candidates as Unavailable via existing path health API
3. Add migration attempt tracking to prevent infinite loops
4. Preserve cancellation priority (check before each candidate attempt)
5. Add cascading migration tests (A→B→C, A→B→C→D, connection-fail-fallback)
6. Verify all S3.8 tests remain green
