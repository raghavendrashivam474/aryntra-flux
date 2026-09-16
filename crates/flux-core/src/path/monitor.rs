use crate::identity::PeerId;
use crate::path::{PathHealthConfig, PathProber, PathRegistry};
use crate::transport::Transport;
use log::{debug, info};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

/// Continuous path health monitor.
///
/// Runs a background loop to periodically probe candidate paths concurrently
/// and expire stale paths based on configured intervals.
pub struct PathHealthMonitor<T: Transport + 'static> {
    transport: Arc<T>,
    local_peer_id: PeerId,
    registry: PathRegistry,
    config: PathHealthConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl<T: Transport + 'static> PathHealthMonitor<T> {
    /// Create a new monitor.
    pub fn new(
        transport: Arc<T>,
        local_peer_id: PeerId,
        registry: PathRegistry,
        config: PathHealthConfig,
    ) -> Self {
        Self {
            transport,
            local_peer_id,
            registry,
            config,
            shutdown_tx: None,
            join_handle: None,
        }
    }

    /// Check if the monitor loop is actively running.
    pub fn is_running(&self) -> bool {
        self.join_handle.is_some()
    }

    /// Start the background monitoring loop.
    pub fn start(&mut self) {
        if self.join_handle.is_some() {
            return;
        }

        let (tx, mut rx) = oneshot::channel::<()>();
        self.shutdown_tx = Some(tx);

        let transport = self.transport.clone();
        let local_peer_id = self.local_peer_id.clone();
        let registry = self.registry.clone();
        let config = self.config;

        let handle = tokio::spawn(async move {
            info!("[MONITOR] Starting background path health loop");
            let mut interval = time::interval(config.probe_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = &mut rx => {
                        info!("[MONITOR] Received shutdown signal; stopping health loop");
                        break;
                    }
                    _ = interval.tick() => {
                        debug!("[MONITOR] Evaluating path health...");

                        // 1. Expire stale paths in registry (marking them Unavailable)
                        let expired = registry.expire_stale_paths(config.stale_after);
                        if expired > 0 {
                            debug!("[MONITOR] Expired {} stale paths", expired);
                        }

                        // 2. Instantiate prober and probe all known paths concurrently
                        let prober = PathProber::new(&*transport, local_peer_id.clone(), registry.clone())
                            .with_timeout(config.probe_timeout);

                        let results = prober.probe_all_concurrent().await;
                        debug!("[MONITOR] Concurrently probed {} paths", results.len());
                    }
                }
            }
        });

        self.join_handle = Some(handle);
    }

    /// Stop the background monitor loop gracefully and wait for completion.
    pub async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::{Path, PathState, TransportKind};
    use crate::transport::TcpTransport;
    use std::net::SocketAddr;
    use std::time::Duration;

    #[tokio::test]
    async fn test_monitor_lifecycle_and_shutdown() {
        let peer_id = PeerId::new();
        let registry = PathRegistry::new();
        let transport = Arc::new(TcpTransport::default());

        let config = PathHealthConfig {
            probe_interval: Duration::from_millis(50),
            probe_timeout: Duration::from_millis(20),
            stale_after: Duration::from_millis(100),
        };

        let mut monitor = PathHealthMonitor::new(transport, peer_id, registry, config);
        assert!(!monitor.is_running());

        monitor.start();
        assert!(monitor.is_running());

        // Let it tick a couple times
        tokio::time::sleep(Duration::from_millis(150)).await;

        monitor.shutdown().await;
        assert!(!monitor.is_running());
    }

    #[tokio::test]
    async fn test_monitor_expires_stale_paths() {
        let local_peer = PeerId::new();
        let remote_peer = PeerId::new();
        let registry = PathRegistry::new();
        let transport = Arc::new(TcpTransport::default());

        // Register a path with old timestamp
        let addr: SocketAddr = "127.0.0.1:9099".parse().unwrap();
        let mut path = Path::new(remote_peer.clone(), TransportKind::Tcp, addr);
        path.state = PathState::Available;
        // Make it stale
        path.last_seen = std::time::Instant::now() - Duration::from_secs(5);
        let path_id = path.id.clone();
        registry.register_path(path);

        // Run monitor with small tick interval and aggressive staleness threshold
        let config = PathHealthConfig {
            probe_interval: Duration::from_millis(10),
            probe_timeout: Duration::from_millis(5),
            stale_after: Duration::from_millis(100),
        };

        let mut monitor = PathHealthMonitor::new(transport, local_peer, registry.clone(), config);
        monitor.start();

        // Give the loop time to tick
        tokio::time::sleep(Duration::from_millis(30)).await;

        let paths = registry.get_paths(&remote_peer).unwrap();
        let p_rec = paths.get(&path_id).unwrap();
        assert!(
            p_rec.state == PathState::Unavailable || p_rec.state == PathState::Connecting,
            "Stale path should have expired or transitioned to Connecting (actual: {:?})",
            p_rec.state
        );

        monitor.shutdown().await;
    }
}
