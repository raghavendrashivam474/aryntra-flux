use log::{error, info, warn};
use std::sync::Arc;

use crate::identity::PeerId;
use crate::path::selector::PathSelector;
use crate::path::{PathId, PathRegistry, PathState};
use crate::session::{Session, SessionBuilder};
use crate::transfer::error::{Result, TransferError};
use crate::transport::traits::Transport;

/// Autonomous path migration state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationState {
    /// Active transmission on the given path.
    Active { path_id: PathId },
    /// Transient state during carrier migration from an old path to an alternate path.
    Migrating {
        from_path: PathId,
        to_path: Option<PathId>,
        attempt: usize,
    },
    /// Transfer paused because no healthy alternative path could be selected.
    Paused { reason: String },
    /// Transfer completed successfully.
    Completed,
    /// Terminal failure where migration or recovery could not succeed.
    Failed { reason: String },
}

/// A path-aware wrapper around an active transfer `Session`.
///
/// If a carrier failure (e.g. transport disconnect or timeout) occurs during file transmission,
/// `TransferCarrier` coordinates with the `PathRegistry` and `PathSelector` to autonomously
/// discover and switch to an alternate healthy path without aborting the logical transfer.
pub struct TransferCarrier<T: Transport> {
    pub peer_id: PeerId,
    pub current_path_id: PathId,
    pub session: Session,
    pub registry: PathRegistry,
    pub transport: Arc<T>,
    pub local_peer_id: PeerId,
    pub state: MigrationState,
    pub migration_count: usize,
}

impl<T: Transport> TransferCarrier<T> {
    /// Create a new carrier initialized to the `Active` state with an initial session and path.
    pub fn new(
        peer_id: PeerId,
        current_path_id: PathId,
        session: Session,
        registry: PathRegistry,
        transport: Arc<T>,
        local_peer_id: PeerId,
    ) -> Self {
        Self {
            peer_id,
            current_path_id: current_path_id.clone(),
            session,
            registry,
            transport,
            local_peer_id,
            state: MigrationState::Active {
                path_id: current_path_id,
            },
            migration_count: 0,
        }
    }

    /// Returns the current state of the carrier state machine.
    pub fn state(&self) -> &MigrationState {
        &self.state
    }

    /// Autonomously migrates the transfer session to an alternate candidate path.
    ///
    /// This method performs cascading fallback: if a candidate path fails to connect,
    /// it marks that candidate as `Unavailable` and attempts the next best available candidate
    /// until a healthy session is established or all paths are exhausted.
    pub async fn migrate(&mut self) -> Result<()> {
        let failed_path_id = self.current_path_id.clone();
        self.migration_count += 1;

        warn!(
            "[MIGRATION] Carrier failure detected on path {} (attempt #{}). Initiating cascading migration for peer {}",
            failed_path_id, self.migration_count, self.peer_id
        );

        // 1. Mark the failed active path as Unavailable in registry
        self.registry
            .set_path_state(&self.peer_id, &failed_path_id, PathState::Unavailable);

        // 2. Transition state to Migrating
        self.state = MigrationState::Migrating {
            from_path: failed_path_id.clone(),
            to_path: None,
            attempt: self.migration_count,
        };

        // 3. Gracefully shutdown the broken session
        let _ = self.session.shutdown().await;

        // 4. Cascading Candidate Fallback Loop
        loop {
            let selector = PathSelector::new(self.registry.clone());
            let candidate_path = match selector.select_path(&self.peer_id) {
                Some(path) => path,
                None => {
                    let reason = format!(
                        "No alternative healthy path found for peer {} after carrier failure",
                        self.peer_id
                    );
                    error!("[MIGRATION] Migration aborted: {}", reason);
                    self.state = MigrationState::Paused {
                        reason: reason.clone(),
                    };
                    return Err(TransferError::UnexpectedMessage(reason));
                }
            };

            info!(
                "[MIGRATION] Selected candidate replacement path: {} ({})",
                candidate_path.id, candidate_path.remote_addr
            );

            self.state = MigrationState::Migrating {
                from_path: failed_path_id.clone(),
                to_path: Some(candidate_path.id.clone()),
                attempt: self.migration_count,
            };

            // Attempt connection to candidate
            let session_builder =
                SessionBuilder::new(self.transport.as_ref(), self.local_peer_id.clone());
            match session_builder
                .connect(&self.peer_id, candidate_path.remote_addr)
                .await
            {
                Ok(new_session) => {
                    // Success - update active carrier state
                    self.current_path_id = candidate_path.id.clone();
                    self.session = new_session;
                    self.state = MigrationState::Active {
                        path_id: candidate_path.id.clone(),
                    };

                    info!(
                        "[MIGRATION] Successfully migrated carrier to path {}",
                        candidate_path.id
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[MIGRATION] Connection failed to candidate replacement path {}: {}. Retrying next available candidate...",
                        candidate_path.id, e
                    );
                    // Mark this candidate as Unavailable so selector will bypass it on subsequent iterations
                    self.registry.set_path_state(
                        &self.peer_id,
                        &candidate_path.id,
                        PathState::Unavailable,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::metrics::PathMetrics;
    use crate::path::{Path, TransportKind};
    use crate::protocol::FluxMessage;
    use crate::transport::error::{Result as TransResult, TransportError};
    use crate::transport::traits::{Connection, Transport};
    use crate::transport::Listener;
    use async_trait::async_trait;
    use std::any::Any;
    use std::net::SocketAddr;
    use std::time::Duration;

    struct MockConnection {
        peer_id: PeerId,
        addr: SocketAddr,
    }

    #[async_trait]
    impl Connection for MockConnection {
        fn peer_id(&self) -> Option<&PeerId> {
            Some(&self.peer_id)
        }
        fn remote_addr(&self) -> SocketAddr {
            self.addr
        }
        async fn send_message(&mut self, _msg: &FluxMessage) -> TransResult<()> {
            Ok(())
        }
        async fn recv_message(&mut self) -> TransResult<FluxMessage> {
            Ok(FluxMessage::Goodbye)
        }
        async fn close(self: Box<Self>) -> TransResult<()> {
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    struct MockTransport {
        pub fail_addrs: Vec<SocketAddr>,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                fail_addrs: Vec::new(),
            }
        }

        fn with_failing_addrs(fail_addrs: Vec<SocketAddr>) -> Self {
            Self { fail_addrs }
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn listen(&self, _addr: SocketAddr) -> TransResult<Box<dyn Listener>> {
            Err(TransportError::Io(std::io::Error::other("unsupported")))
        }
        async fn connect(
            &self,
            _local_id: &PeerId,
            addr: SocketAddr,
        ) -> TransResult<Box<dyn Connection>> {
            if self.fail_addrs.contains(&addr) {
                return Err(TransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "mock connection refused",
                )));
            }
            Ok(Box::new(MockConnection {
                peer_id: PeerId::new(),
                addr,
            }))
        }
    }

    #[tokio::test]
    async fn test_carrier_initial_state() {
        let local_id = PeerId::new();
        let remote_id = PeerId::new();
        let path_id = PathId::new();
        let addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();

        let conn = Box::new(MockConnection {
            peer_id: remote_id.clone(),
            addr,
        });
        let session = Session::from_connection(conn, local_id.clone());
        let registry = PathRegistry::new();
        let transport = Arc::new(MockTransport::new());

        let carrier = TransferCarrier::new(
            remote_id,
            path_id.clone(),
            session,
            registry,
            transport,
            local_id,
        );

        assert_eq!(
            carrier.state(),
            &MigrationState::Active {
                path_id: path_id.clone()
            }
        );
        assert_eq!(carrier.migration_count, 0);
    }

    #[tokio::test]
    async fn test_carrier_migration_success() {
        let local_id = PeerId::new();
        let remote_id = PeerId::new();
        let addr_a: SocketAddr = "127.0.0.1:9101".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:9102".parse().unwrap();

        let registry = PathRegistry::new();

        let mut path_a = Path::new(remote_id.clone(), TransportKind::Tcp, addr_a);
        path_a.state = PathState::Available;
        path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(50)));
        let path_a_id = path_a.id.clone();
        registry.register_path(path_a);

        let mut path_b = Path::new(remote_id.clone(), TransportKind::Tcp, addr_b);
        path_b.state = PathState::Available;
        path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(20)));
        let path_b_id = path_b.id.clone();
        registry.register_path(path_b);

        let conn_a = Box::new(MockConnection {
            peer_id: remote_id.clone(),
            addr: addr_a,
        });
        let session_a = Session::from_connection(conn_a, local_id.clone());
        let transport = Arc::new(MockTransport::new());

        let mut carrier = TransferCarrier::new(
            remote_id.clone(),
            path_a_id.clone(),
            session_a,
            registry.clone(),
            transport,
            local_id,
        );

        // Migrate from Path A to Path B
        carrier.migrate().await.unwrap();

        assert_eq!(
            carrier.state(),
            &MigrationState::Active {
                path_id: path_b_id.clone()
            }
        );
        assert_eq!(carrier.current_path_id, path_b_id);
        assert_eq!(carrier.migration_count, 1);

        // Verify Path A is marked Unavailable
        let paths = registry.get_paths(&remote_id).unwrap();
        let p_a = paths.get(&path_a_id).unwrap();
        assert_eq!(p_a.state, PathState::Unavailable);
    }

    #[tokio::test]
    async fn test_carrier_migration_fails_when_no_alternate_path() {
        let local_id = PeerId::new();
        let remote_id = PeerId::new();
        let addr_a: SocketAddr = "127.0.0.1:9103".parse().unwrap();

        let registry = PathRegistry::new();
        let mut path_a = Path::new(remote_id.clone(), TransportKind::Tcp, addr_a);
        path_a.state = PathState::Available;
        path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(50)));
        let path_a_id = path_a.id.clone();
        registry.register_path(path_a);

        let conn_a = Box::new(MockConnection {
            peer_id: remote_id.clone(),
            addr: addr_a,
        });
        let session_a = Session::from_connection(conn_a, local_id.clone());
        let transport = Arc::new(MockTransport::new());

        let mut carrier = TransferCarrier::new(
            remote_id,
            path_a_id.clone(),
            session_a,
            registry,
            transport,
            local_id,
        );

        let res = carrier.migrate().await;
        assert!(res.is_err());
        assert!(matches!(carrier.state(), MigrationState::Paused { .. }));
    }

    #[tokio::test]
    async fn test_carrier_cascading_migration_multiple_paths() {
        let local_id = PeerId::new();
        let remote_id = PeerId::new();
        let addr_a: SocketAddr = "127.0.0.1:9104".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:9105".parse().unwrap();
        let addr_c: SocketAddr = "127.0.0.1:9106".parse().unwrap();

        let registry = PathRegistry::new();

        let mut path_a = Path::new(remote_id.clone(), TransportKind::Tcp, addr_a);
        path_a.state = PathState::Available;
        path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(50)));
        let path_a_id = path_a.id.clone();
        registry.register_path(path_a);

        let mut path_b = Path::new(remote_id.clone(), TransportKind::Tcp, addr_b);
        path_b.state = PathState::Available;
        path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(30)));
        let path_b_id = path_b.id.clone();
        registry.register_path(path_b);

        let mut path_c = Path::new(remote_id.clone(), TransportKind::Tcp, addr_c);
        path_c.state = PathState::Available;
        path_c.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
        let path_c_id = path_c.id.clone();
        registry.register_path(path_c);

        let conn_a = Box::new(MockConnection {
            peer_id: remote_id.clone(),
            addr: addr_a,
        });
        let session_a = Session::from_connection(conn_a, local_id.clone());
        let transport = Arc::new(MockTransport::new());

        let mut carrier = TransferCarrier::new(
            remote_id.clone(),
            path_a_id.clone(),
            session_a,
            registry.clone(),
            transport,
            local_id,
        );

        // First migration: Path A fails -> best available is Path C (10ms RTT)
        carrier.migrate().await.unwrap();
        assert_eq!(carrier.current_path_id, path_c_id);
        assert_eq!(carrier.migration_count, 1);

        // Second migration: Path C fails -> remaining available is Path B (30ms RTT)
        carrier.migrate().await.unwrap();
        assert_eq!(carrier.current_path_id, path_b_id);
        assert_eq!(carrier.migration_count, 2);

        // Third migration: Path B fails -> no more paths available -> error & paused
        let res = carrier.migrate().await;
        assert!(res.is_err());
        assert!(matches!(carrier.state(), MigrationState::Paused { .. }));
        assert_eq!(carrier.migration_count, 3);
    }

    #[tokio::test]
    async fn test_carrier_cascading_migration_connection_fallback() {
        let local_id = PeerId::new();
        let remote_id = PeerId::new();
        let addr_a: SocketAddr = "127.0.0.1:9107".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:9108".parse().unwrap();
        let addr_c: SocketAddr = "127.0.0.1:9109".parse().unwrap();

        let registry = PathRegistry::new();

        let mut path_a = Path::new(remote_id.clone(), TransportKind::Tcp, addr_a);
        path_a.state = PathState::Available;
        path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(50)));
        let path_a_id = path_a.id.clone();
        registry.register_path(path_a);

        let mut path_b = Path::new(remote_id.clone(), TransportKind::Tcp, addr_b);
        path_b.state = PathState::Available;
        path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
        let path_b_id = path_b.id.clone();
        registry.register_path(path_b);

        let mut path_c = Path::new(remote_id.clone(), TransportKind::Tcp, addr_c);
        path_c.state = PathState::Available;
        path_c.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(20)));
        let path_c_id = path_c.id.clone();
        registry.register_path(path_c);

        let conn_a = Box::new(MockConnection {
            peer_id: remote_id.clone(),
            addr: addr_a,
        });
        let session_a = Session::from_connection(conn_a, local_id.clone());

        // Transport will reject connection to Path B (addr_b)
        let transport = Arc::new(MockTransport::with_failing_addrs(vec![addr_b]));

        let mut carrier = TransferCarrier::new(
            remote_id.clone(),
            path_a_id.clone(),
            session_a,
            registry.clone(),
            transport,
            local_id,
        );

        // Path A fails -> Selector tries Path B (10ms), fails to connect,
        // automatically falls back to Path C (20ms) and succeeds in a single migrate() call!
        carrier.migrate().await.unwrap();

        assert_eq!(carrier.current_path_id, path_c_id);
        assert_eq!(carrier.migration_count, 1);

        let paths = registry.get_paths(&remote_id).unwrap();
        assert_eq!(paths.get(&path_a_id).unwrap().state, PathState::Unavailable);
        assert_eq!(paths.get(&path_b_id).unwrap().state, PathState::Unavailable);
        assert_eq!(paths.get(&path_c_id).unwrap().state, PathState::Available);
    }
}
