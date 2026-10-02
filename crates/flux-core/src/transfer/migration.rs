use crate::identity::PeerId;
use crate::path::selector::PathSelector;
use crate::path::{PathId, PathRegistry, PathState};
use crate::session::{Session, SessionBuilder};
use crate::transfer::error::{Result, TransferError};
use crate::transport::Transport;
use log::{error, info, warn};
use std::sync::Arc;

/// Explicit lifecycle states of a migratable transfer carrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationState {
    /// Actively transmitting data over the designated path.
    Active { path_id: PathId },
    /// Active carrier failed; currently attempting migration to replacement path.
    Migrating {
        from_path: PathId,
        to_path: Option<PathId>,
        attempt: usize,
    },
    /// No alternate path available; progress preserved on disk.
    Paused { reason: String },
    /// Transfer has completed successfully across carriers.
    Completed,
    /// Unrecoverable carrier or migration error.
    Failed { reason: String },
}

/// A path-aware carrier that wraps an active Session and orchestrates
/// autonomous migration to alternate paths upon connection failure.
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

    /// Return current migration state.
    pub fn state(&self) -> &MigrationState {
        &self.state
    }

    /// Mark the current path as failed, query PathRegistry/PathSelector for
    /// the optimal alternate path, establish a replacement Session, and transition state.
    pub async fn migrate(&mut self) -> Result<()> {
        let failed_path_id = self.current_path_id.clone();
        self.migration_count += 1;

        warn!(
            "[MIGRATION] Carrier failure detected on path {} (attempt #{}). Initiating migration for peer {}",
            failed_path_id, self.migration_count, self.peer_id
        );

        // 1. Mark failed path as Unavailable in registry
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

        // 4. Select alternate path using deterministic PathSelector
        let selector = PathSelector::new(self.registry.clone());
        let candidate_path = match selector.select_path(&self.peer_id) {
            Some(path) => path,
            None => {
                let reason = format!(
                    "No alternative healthy path found for peer {} after failure of {}",
                    self.peer_id, failed_path_id
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
            from_path: failed_path_id,
            to_path: Some(candidate_path.id.clone()),
            attempt: self.migration_count,
        };

        // 5. Connect to replacement path
        let session_builder =
            SessionBuilder::new(self.transport.as_ref(), self.local_peer_id.clone());
        let new_session = match session_builder
            .connect(&self.peer_id, candidate_path.remote_addr)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                let reason = format!(
                    "Failed to connect to replacement path {}: {}",
                    candidate_path.id, e
                );
                error!("[MIGRATION] {}", reason);
                self.registry.set_path_state(
                    &self.peer_id,
                    &candidate_path.id,
                    PathState::Unavailable,
                );
                self.state = MigrationState::Failed {
                    reason: reason.clone(),
                };
                return Err(TransferError::UnexpectedMessage(reason));
            }
        };

        // 6. Success - update active carrier state
        self.current_path_id = candidate_path.id.clone();
        self.session = new_session;
        self.state = MigrationState::Active {
            path_id: candidate_path.id.clone(),
        };

        info!(
            "[MIGRATION] Successfully migrated carrier to path {}",
            candidate_path.id
        );
        Ok(())
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

    struct MockTransport;

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
        let transport = Arc::new(MockTransport);

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
        let transport = Arc::new(MockTransport);

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
        let transport = Arc::new(MockTransport);

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
}
