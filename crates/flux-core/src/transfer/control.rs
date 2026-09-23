use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Thread-safe cooperative cancellation handle for in-flight transfers.
#[derive(Debug, Clone, Default)]
pub struct TransferCancellation {
    cancelled: Arc<AtomicBool>,
}

impl TransferCancellation {
    /// Create a new, non-cancelled token.
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal cooperative cancellation to all holders of this token.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancellation_lifecycle() {
        let token = TransferCancellation::new();
        assert!(!token.is_cancelled());

        let cloned = token.clone();
        cloned.cancel();

        assert!(token.is_cancelled());
        assert!(cloned.is_cancelled());
    }
}
