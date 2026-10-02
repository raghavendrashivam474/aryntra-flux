use super::collection::TransferPlan;
use super::control::TransferCancellation;
use super::progress::TransferProgress;

/// Sender-side checkpoint representing verified chunk-level progress
/// for an in-flight file transfer within a collection or single-file transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkCheckpoint {
    /// Index of the file in the `TransferPlan` items list.
    pub file_index: usize,
    /// Total number of chunks successfully completed/sent for this file.
    pub completed_chunks: u32,
    /// The next chunk index to be transmitted (0-indexed).
    pub next_chunk: u32,
    /// Total bytes successfully transferred for this file so far.
    pub bytes_completed: u64,
}

impl ChunkCheckpoint {
    /// Create a new, fresh checkpoint for a file at `file_index`.
    pub fn new(file_index: usize) -> Self {
        Self {
            file_index,
            completed_chunks: 0,
            next_chunk: 0,
            bytes_completed: 0,
        }
    }

    /// Record a successfully completed chunk transmission.
    ///
    /// # Arguments
    /// * `chunk_index` - The 0-based sequence index of the chunk just completed.
    /// * `chunk_bytes` - Number of bytes in this chunk.
    #[inline]
    pub fn record_chunk(&mut self, chunk_index: u32, chunk_bytes: u64) {
        self.completed_chunks = chunk_index + 1;
        self.next_chunk = chunk_index + 1;
        self.bytes_completed += chunk_bytes;
    }

    /// Return true if no chunks have been completed yet.
    #[inline]
    pub fn is_initial(&self) -> bool {
        self.completed_chunks == 0
    }
}

/// Captures the state of an interrupted collection transfer so it can
/// be continued on a new session/path without losing progress.
///
/// This is the S3.5 / S3.8 continuity primitive. It records both completed
/// files and intra-file chunk checkpoints so a replacement carrier or caller
/// can resume with chunk-level precision.
#[derive(Clone)]
pub struct TransferContinuation {
    /// The original transfer plan (file list, paths, etc.)
    pub plan: TransferPlan,
    /// Shared progress tracker — preserves bytes/files across sessions.
    pub progress: TransferProgress,
    /// Shared cancellation token — survives session replacement.
    pub cancel: TransferCancellation,
    /// Number of files that were fully completed before interruption.
    /// The file at this index is the first one that needs (re)sending.
    pub completed_files: usize,
    /// Intra-file chunk checkpoint for the file at `completed_files`, if any progress occurred.
    pub active_checkpoint: Option<ChunkCheckpoint>,
}

impl TransferContinuation {
    /// Create a continuation from an interrupted transfer with file-level granularity.
    ///
    /// # Arguments
    /// * `plan` — The original `TransferPlan` for the collection.
    /// * `progress` — The `TransferProgress` that was tracking the transfer.
    /// * `cancel` — The `TransferCancellation` token for the transfer.
    /// * `completed_files` — How many files finished before the interruption.
    pub fn from_interrupted(
        plan: TransferPlan,
        progress: TransferProgress,
        cancel: TransferCancellation,
        completed_files: usize,
    ) -> Self {
        Self {
            plan,
            progress,
            cancel,
            completed_files,
            active_checkpoint: None,
        }
    }

    /// Create a continuation with explicit intra-file chunk-level checkpoint state.
    pub fn with_checkpoint(
        plan: TransferPlan,
        progress: TransferProgress,
        cancel: TransferCancellation,
        completed_files: usize,
        active_checkpoint: Option<ChunkCheckpoint>,
    ) -> Self {
        Self {
            plan,
            progress,
            cancel,
            completed_files,
            active_checkpoint,
        }
    }

    /// Number of files still needing transfer (including any partial file).
    pub fn remaining_files(&self) -> usize {
        self.plan.items.len().saturating_sub(self.completed_files)
    }

    /// Whether the entire collection has already been transferred.
    pub fn is_complete(&self) -> bool {
        self.completed_files >= self.plan.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::collection::TransferItem;
    use std::path::PathBuf;

    fn dummy_plan(n: usize) -> TransferPlan {
        TransferPlan {
            items: (0..n)
                .map(|i| TransferItem {
                    source_path: PathBuf::from(format!("file_{}.bin", i)),
                    relative_path: PathBuf::from(format!("file_{}.bin", i)),
                })
                .collect(),
        }
    }

    #[test]
    fn test_chunk_checkpoint_lifecycle() {
        let mut cp = ChunkCheckpoint::new(0);
        assert!(cp.is_initial());
        assert_eq!(cp.file_index, 0);
        assert_eq!(cp.completed_chunks, 0);
        assert_eq!(cp.next_chunk, 0);
        assert_eq!(cp.bytes_completed, 0);

        // Record 1st chunk (64 KiB)
        cp.record_chunk(0, 64 * 1024);
        assert!(!cp.is_initial());
        assert_eq!(cp.completed_chunks, 1);
        assert_eq!(cp.next_chunk, 1);
        assert_eq!(cp.bytes_completed, 64 * 1024);

        // Record chunks up to index 59 (60 total chunks)
        for i in 1..60 {
            cp.record_chunk(i, 64 * 1024);
        }
        assert_eq!(cp.completed_chunks, 60);
        assert_eq!(cp.next_chunk, 60);
        assert_eq!(cp.bytes_completed, 60 * 64 * 1024);
    }

    #[test]
    fn test_continuation_from_interrupted() {
        let plan = dummy_plan(5);
        let progress = TransferProgress::new();
        let cancel = TransferCancellation::new();
        let cont = TransferContinuation::from_interrupted(plan, progress, cancel, 2);
        assert_eq!(cont.completed_files, 2);
        assert_eq!(cont.remaining_files(), 3);
        assert!(!cont.is_complete());
        assert!(cont.active_checkpoint.is_none());
    }

    #[test]
    fn test_continuation_with_checkpoint() {
        let plan = dummy_plan(5);
        let progress = TransferProgress::new();
        let cancel = TransferCancellation::new();
        let mut cp = ChunkCheckpoint::new(2);
        cp.record_chunk(0, 64 * 1024);
        cp.record_chunk(1, 64 * 1024);

        let cont = TransferContinuation::with_checkpoint(plan, progress, cancel, 2, Some(cp));
        assert_eq!(cont.completed_files, 2);
        assert_eq!(cont.remaining_files(), 3);
        assert!(!cont.is_complete());
        assert!(cont.active_checkpoint.is_some());
        let saved_cp = cont.active_checkpoint.unwrap();
        assert_eq!(saved_cp.completed_chunks, 2);
        assert_eq!(saved_cp.next_chunk, 2);
    }

    #[test]
    fn test_continuation_complete() {
        let plan = dummy_plan(3);
        let progress = TransferProgress::new();
        let cancel = TransferCancellation::new();
        let cont = TransferContinuation::from_interrupted(plan, progress, cancel, 3);
        assert_eq!(cont.remaining_files(), 0);
        assert!(cont.is_complete());
    }

    #[test]
    fn test_continuation_zero_completed() {
        let plan = dummy_plan(4);
        let progress = TransferProgress::new();
        let cancel = TransferCancellation::new();
        let cont = TransferContinuation::from_interrupted(plan, progress, cancel, 0);
        assert_eq!(cont.remaining_files(), 4);
        assert!(!cont.is_complete());
    }
}
