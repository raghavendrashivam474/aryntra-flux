use std::time::{Duration, Instant};

/// Measurement metrics for a candidate communication path.
///
/// Keeps latency and reachability data decoupled from the core
/// identity of `Path`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathMetrics {
    /// Measured round-trip time in milliseconds.
    pub rtt_ms: Option<u64>,
    /// Timestamp when this path was last probed.
    pub last_probed: Option<Instant>,
}

impl PathMetrics {
    /// Create an unmeasured metrics record.
    pub fn new() -> Self {
        Self {
            rtt_ms: None,
            last_probed: None,
        }
    }

    /// Create a metrics record with an initial RTT measurement.
    pub fn with_rtt(rtt: Duration) -> Self {
        Self {
            rtt_ms: Some(rtt.as_millis() as u64),
            last_probed: Some(Instant::now()),
        }
    }

    /// Update with a newly measured round-trip time.
    pub fn update_rtt(&mut self, rtt: Duration) {
        self.rtt_ms = Some(rtt.as_millis() as u64);
        self.last_probed = Some(Instant::now());
    }

    /// Return measured RTT as a `Duration` if available.
    pub fn rtt(&self) -> Option<Duration> {
        self.rtt_ms.map(Duration::from_millis)
    }
}

impl Default for PathMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_metrics_initial_state() {
        let metrics = PathMetrics::new();
        assert_eq!(metrics.rtt_ms, None);
        assert_eq!(metrics.last_probed, None);
        assert_eq!(metrics.rtt(), None);
    }

    #[test]
    fn test_path_metrics_update() {
        let mut metrics = PathMetrics::new();
        metrics.update_rtt(Duration::from_millis(15));
        assert_eq!(metrics.rtt_ms, Some(15));
        assert!(metrics.last_probed.is_some());
        assert_eq!(metrics.rtt(), Some(Duration::from_millis(15)));

        metrics.update_rtt(Duration::from_millis(5));
        assert_eq!(metrics.rtt_ms, Some(5));
    }

    #[test]
    fn test_path_metrics_with_rtt() {
        let metrics = PathMetrics::with_rtt(Duration::from_millis(42));
        assert_eq!(metrics.rtt_ms, Some(42));
        assert!(metrics.last_probed.is_some());
    }
}
