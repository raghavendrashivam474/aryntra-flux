use crate::identity::PeerId;
use crate::path::metrics::PathMetrics;
use crate::path::{Path, PathId, PathRegistry, PathState};
use crate::protocol::FluxMessage;
use crate::session::SessionBuilder;
use crate::transport::Transport;
use log::{debug, warn};
use std::time::{Duration, Instant};
use thiserror::Error;

/// Errors that can occur during path probing.
#[derive(Debug, Error)]
pub enum ProberError {
    #[error("Connection timed out")]
    Timeout,
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Protocol error: expected Pong, received {0:?}")]
    UnexpectedMessage(String),
    #[error("Path not found in registry")]
    PathNotFound,
}

/// Active prober responsible for measuring path reachability and latency.
///
/// Uses the existing `Session` abstraction without raw socket manipulation.
pub struct PathProber<'a, T: Transport> {
    transport: &'a T,
    local_peer_id: PeerId,
    registry: PathRegistry,
    timeout: Duration,
}

impl<'a, T: Transport> PathProber<'a, T> {
    /// Create a new prober with default 2-second timeout per probe.
    pub fn new(transport: &'a T, local_peer_id: PeerId, registry: PathRegistry) -> Self {
        Self {
            transport,
            local_peer_id,
            registry,
            timeout: Duration::from_secs(2),
        }
    }

    /// Override the probing timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Probe a single path: connect, ping/pong, calculate RTT, update registry.
    pub async fn probe_path(&self, path: &Path) -> Result<PathMetrics, ProberError> {
        let peer_id = &path.peer_id;
        let path_id = &path.id;

        debug!(
            "[PROBER] Probing path {} ({} @ {})",
            path_id, peer_id, path.remote_addr
        );

        // 1. Transition state to Connecting
        self.registry
            .set_path_state(peer_id, path_id, PathState::Connecting);

        let builder = SessionBuilder::new(self.transport, self.local_peer_id.clone());

        // 2. Connect within configured timeout
        let connect_fut = builder.connect(peer_id, path.remote_addr);
        let mut session = match tokio::time::timeout(self.timeout, connect_fut).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!("[PROBER] Path {} connection failed: {}", path_id, e);
                self.registry
                    .set_path_state(peer_id, path_id, PathState::Unavailable);
                return Err(ProberError::Transport(e.to_string()));
            }
            Err(_) => {
                warn!("[PROBER] Path {} connection timed out", path_id);
                self.registry
                    .set_path_state(peer_id, path_id, PathState::Unavailable);
                return Err(ProberError::Timeout);
            }
        };

        // 3. Measure RTT via Ping / Pong
        let seq = 1;
        let ping_msg = FluxMessage::ping(seq, "flux-probe".to_string());
        let t0 = Instant::now();

        let ping_pong_res: Result<Duration, ProberError> = async {
            session
                .send_message(&ping_msg)
                .await
                .map_err(|e| ProberError::Transport(e.to_string()))?;

            let reply = session
                .recv_message()
                .await
                .map_err(|e| ProberError::Transport(e.to_string()))?;

            let rtt = t0.elapsed();

            match reply {
                FluxMessage::Pong { sequence, .. } if sequence == seq => Ok(rtt),
                other => Err(ProberError::UnexpectedMessage(format!("{:?}", other))),
            }
        }
        .await;

        // 4. Graceful session cleanup
        let _ = session.close().await;

        match ping_pong_res {
            Ok(rtt) => {
                let metrics = PathMetrics::with_rtt(rtt);
                self.registry.set_path_metrics(peer_id, path_id, metrics);
                self.registry
                    .set_path_state(peer_id, path_id, PathState::Available);

                debug!(
                    "[PROBER] Path {} available (RTT: {} ms)",
                    path_id,
                    metrics.rtt_ms.unwrap_or(0)
                );
                Ok(metrics)
            }
            Err(err) => {
                warn!("[PROBER] Path {} ping/pong failed: {}", path_id, err);
                self.registry
                    .set_path_state(peer_id, path_id, PathState::Unavailable);
                Err(err)
            }
        }
    }

    /// Probe all registered paths for a given peer.
    pub async fn probe_peer_paths(
        &self,
        peer_id: &PeerId,
    ) -> Vec<(PathId, Result<PathMetrics, ProberError>)> {
        let paths = match self.registry.get_paths(peer_id) {
            Some(set) => set.list().to_vec(),
            None => return Vec::new(),
        };

        let mut results = Vec::new();
        for path in paths {
            let res = self.probe_path(&path).await;
            results.push((path.id, res));
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::TransportKind;
    use crate::transport::traits::Connection;
    use crate::transport::TcpTransport;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_probe_reachable_path() {
        let peer_a = PeerId::new();
        let peer_b = PeerId::new();

        let registry_a = PathRegistry::new();
        let addr_b: SocketAddr = "127.0.0.1:9181".parse().unwrap();

        let path = Path::new(peer_b.clone(), TransportKind::Tcp, addr_b);
        let path_id = path.id.clone();
        registry_a.register_path(path);

        // Server responder on Node B
        let transport_b = TcpTransport::default();
        let listener_b = transport_b.listen(addr_b).await.unwrap();
        let p_b = peer_b.clone();

        tokio::spawn(async move {
            let mut l = listener_b;
            if let Ok((mut conn, _)) = l.accept().await {
                let tcp_conn = conn
                    .as_any_mut()
                    .downcast_mut::<crate::transport::TcpConnection>()
                    .unwrap();
                if tcp_conn.server_handshake(&p_b).await.is_ok() {
                    if let Ok(FluxMessage::Ping { sequence, payload }) =
                        tcp_conn.recv_message().await
                    {
                        let _ = tcp_conn
                            .send_message(&FluxMessage::pong(sequence, payload))
                            .await;
                    }
                }
            }
        });

        let transport_a = TcpTransport::default();
        let prober = PathProber::new(&transport_a, peer_a, registry_a.clone())
            .with_timeout(Duration::from_millis(500));

        let paths = registry_a.get_paths(&peer_b).unwrap();
        let target_path = paths.get(&path_id).unwrap();

        let res = prober.probe_path(target_path).await;
        assert!(res.is_ok(), "Probe should succeed");

        let updated_paths = registry_a.get_paths(&peer_b).unwrap();
        let updated_path = updated_paths.get(&path_id).unwrap();
        assert_eq!(updated_path.state, PathState::Available);
        assert!(updated_path.metrics.is_some());
    }

    #[tokio::test]
    async fn test_probe_unreachable_path() {
        let peer_a = PeerId::new();
        let peer_b = PeerId::new();

        let registry_a = PathRegistry::new();
        // Unopened port
        let addr_unreachable: SocketAddr = "127.0.0.1:9199".parse().unwrap();

        let path = Path::new(peer_b.clone(), TransportKind::Tcp, addr_unreachable);
        let path_id = path.id.clone();
        registry_a.register_path(path);

        let transport_a = TcpTransport::default();
        let prober = PathProber::new(&transport_a, peer_a, registry_a.clone())
            .with_timeout(Duration::from_millis(200));

        let paths = registry_a.get_paths(&peer_b).unwrap();
        let target_path = paths.get(&path_id).unwrap();

        let res = prober.probe_path(target_path).await;
        assert!(res.is_err(), "Probe should fail for unreachable endpoint");

        let updated_paths = registry_a.get_paths(&peer_b).unwrap();
        let updated_path = updated_paths.get(&path_id).unwrap();
        assert_eq!(updated_path.state, PathState::Unavailable);
        assert!(updated_path.metrics.is_none());
    }
}
