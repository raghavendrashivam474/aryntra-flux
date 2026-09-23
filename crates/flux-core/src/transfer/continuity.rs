use super::collection::TransferPlan;
use super::control::TransferCancellation;
use super::progress::TransferProgress;

/// Captures the state of an interrupted collection transfer so it can
/// be continued on a new session/path without losing progress.
///
/// This is the S3.5 continuity primitive. It does NOT implement automatic
/// migration — that is S3.6. It simply records where the transfer left off
/// so a caller can explicitly resume on a different session.
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
}

impl TransferContinuation {
    /// Create a continuation from an interrupted transfer.
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
    fn test_continuation_from_interrupted() {
        let plan = dummy_plan(5);
        let progress = TransferProgress::new();
        let cancel = TransferCancellation::new();
        let cont = TransferContinuation::from_interrupted(plan, progress, cancel, 2);

        assert_eq!(cont.completed_files, 2);
        assert_eq!(cont.remaining_files(), 3);
        assert!(!cont.is_complete());
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
