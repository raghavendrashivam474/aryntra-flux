use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Thread-safe, lock-free transfer progress tracker.
///
/// Designed to be cheaply cloned (single Arc reference clone) and shared across
/// async task boundaries between the transfer engine (writer) and the gateway/caller (reader).
///
/// Updated at chunk and file boundaries inside `TransferManager`.
#[derive(Clone, Debug, Default)]
pub struct TransferProgress {
    bytes_transferred: Arc<AtomicU64>,
    files_completed: Arc<AtomicUsize>,
}

impl TransferProgress {
    /// Create a new progress tracker initialized to 0 bytes and 0 files.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add bytes after a chunk is successfully sent or received.
    #[inline]
    pub fn add_bytes(&self, bytes: u64) {
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Set the byte count directly (e.g. for pre-existing bytes on resume).
    #[inline]
    pub fn set_bytes(&self, bytes: u64) {
        self.bytes_transferred.store(bytes, Ordering::Relaxed);
    }

    /// Increment the count of completed files by one.
    #[inline]
    pub fn add_file(&self) {
        self.files_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Return the current snapshot of total bytes transferred.
    #[inline]
    pub fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred.load(Ordering::Relaxed)
    }

    /// Return the current snapshot of total files completed.
    #[inline]
    pub fn files_completed(&self) -> usize {
        self.files_completed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let p = TransferProgress::new();
        assert_eq!(p.bytes_transferred(), 0);
        assert_eq!(p.files_completed(), 0);
    }

    #[test]
    fn test_add_bytes() {
        let p = TransferProgress::new();
        p.add_bytes(1024);
        p.add_bytes(2048);
        assert_eq!(p.bytes_transferred(), 3072);
    }

    #[test]
    fn test_set_bytes() {
        let p = TransferProgress::new();
        p.add_bytes(500);
        p.set_bytes(1000);
        assert_eq!(p.bytes_transferred(), 1000);
    }

    #[test]
    fn test_add_file() {
        let p = TransferProgress::new();
        p.add_file();
        p.add_file();
        p.add_file();
        assert_eq!(p.files_completed(), 3);
    }

    #[test]
    fn test_clone_shares_state() {
        let p1 = TransferProgress::new();
        let p2 = p1.clone();

        p1.add_bytes(4096);
        p1.add_file();

        assert_eq!(p2.bytes_transferred(), 4096);
        assert_eq!(p2.files_completed(), 1);
    }

    #[test]
    fn test_concurrent_updates() {
        use std::thread;

        let p = TransferProgress::new();
        let mut handles = vec![];

        for _ in 0..10 {
            let p_clone = p.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    p_clone.add_bytes(64);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(p.bytes_transferred(), 10 * 1000 * 64);
    }
}
