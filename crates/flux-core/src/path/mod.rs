use crate::identity::PeerId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use uuid::Uuid;

// ─── Path Identity ───────────────────────────────────────────

/// Unique identifier for a communication path.
/// Distinct from PeerId: one peer can have many paths.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathId(Uuid);

impl PathId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for PathId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ─── Transport Kind ──────────────────────────────────────────

/// The transport mechanism used by a path.
/// Currently only Tcp; future variants: Quic, Relay, Bluetooth, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransportKind {
    Tcp,
}

impl std::fmt::Display for TransportKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportKind::Tcp => write!(f, "TCP"),
        }
    }
}

// ─── Path State ──────────────────────────────────────────────

/// Lifecycle state of a path.
/// Separate from SessionState and transfer state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathState {
    Discovered,
    Candidate,
    Connecting,
    Available,
    Unavailable,
}

// ─── Path ────────────────────────────────────────────────────

/// A concrete communication route to a peer.
///
/// A path is NOT a transfer and NOT a session.
/// It represents a reachable endpoint that *can* produce a session.
#[derive(Debug, Clone)]
pub struct Path {
    pub id: PathId,
    pub peer_id: PeerId,
    pub transport: TransportKind,
    pub remote_addr: SocketAddr,
    pub state: PathState,
    pub last_seen: Instant,
}

impl Path {
    /// Create a new path in the Discovered state.
    pub fn new(peer_id: PeerId, transport: TransportKind, remote_addr: SocketAddr) -> Self {
        Self {
            id: PathId::new(),
            peer_id,
            transport,
            remote_addr,
            state: PathState::Discovered,
            last_seen: Instant::now(),
        }
    }

    /// Two paths represent the same route if they share
    /// peer identity, transport type, and remote address.
    pub fn same_route(&self, other: &Path) -> bool {
        self.peer_id == other.peer_id
            && self.transport == other.transport
            && self.remote_addr == other.remote_addr
    }

    /// Transition to a new state, refreshing last_seen.
    pub fn set_state(&mut self, state: PathState) {
        self.state = state;
        self.last_seen = Instant::now();
    }
}

// ─── PathSet ─────────────────────────────────────────────────

/// Collection of paths to a single peer.
#[derive(Debug, Clone, Default)]
pub struct PathSet {
    paths: Vec<Path>,
}

impl PathSet {
    pub fn new() -> Self {
        Self { paths: Vec::new() }
    }

    /// Add a path. If the same route already exists, refresh last_seen
    /// and revive it from Unavailable if needed. No duplicates.
    pub fn add_or_update(&mut self, path: Path) {
        if let Some(existing) = self.paths.iter_mut().find(|p| p.same_route(&path)) {
            existing.last_seen = Instant::now();
            if existing.state == PathState::Unavailable {
                existing.state = PathState::Discovered;
            }
        } else {
            self.paths.push(path);
        }
    }

    pub fn list(&self) -> &[Path] {
        &self.paths
    }

    pub fn get(&self, id: &PathId) -> Option<&Path> {
        self.paths.iter().find(|p| &p.id == id)
    }

    pub fn available(&self) -> Vec<&Path> {
        self.paths
            .iter()
            .filter(|p| p.state == PathState::Available)
            .collect()
    }

    pub fn count(&self) -> usize {
        self.paths.len()
    }

    /// Set state for a specific path ID inside this set.
    pub fn set_state(&mut self, id: &PathId, state: PathState) {
        if let Some(path) = self.paths.iter_mut().find(|p| &p.id == id) {
            path.set_state(state);
        }
    }
}

// ─── PathRegistry ────────────────────────────────────────────

/// Thread-safe registry of all known paths, keyed by peer.
///
/// Lives alongside PeerRegistry. Does not replace it.
/// PeerRegistry tracks "known peers"; PathRegistry tracks
/// "known routes to peers."
#[derive(Debug, Clone, Default)]
pub struct PathRegistry {
    paths: Arc<RwLock<HashMap<PeerId, PathSet>>>,
}

impl PathRegistry {
    pub fn new() -> Self {
        Self {
            paths: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a path for a peer. Deduplicates by route.
    pub fn register_path(&self, path: Path) {
        let mut map = self.paths.write().unwrap();
        let set = map.entry(path.peer_id.clone()).or_default();
        set.add_or_update(path);
    }

    /// Get all known paths to a specific peer.
    pub fn get_paths(&self, peer_id: &PeerId) -> Option<PathSet> {
        let map = self.paths.read().unwrap();
        map.get(peer_id).cloned()
    }

    /// List all peers that have at least one known path.
    pub fn all_peers(&self) -> Vec<PeerId> {
        let map = self.paths.read().unwrap();
        map.keys().cloned().collect()
    }

    /// Set state for a specific path of a specific peer.
    pub fn set_path_state(&self, peer_id: &PeerId, path_id: &PathId, state: PathState) {
        let mut map = self.paths.write().unwrap();
        if let Some(set) = map.get_mut(peer_id) {
            set.set_state(path_id, state);
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    #[test]
    fn test_path_identity_distinct_from_peer() {
        let peer = PeerId::new();
        let p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        let p2 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9002));
        // Same peer, different paths
        assert_ne!(p1.id, p2.id);
        assert_eq!(p1.peer_id, p2.peer_id);
    }

    #[test]
    fn test_same_route_detection() {
        let peer = PeerId::new();
        let p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        let p2 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        let p3 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9002));
        assert!(p1.same_route(&p2));
        assert!(!p1.same_route(&p3));
    }

    #[test]
    fn test_pathset_deduplication() {
        let peer = PeerId::new();
        let mut set = PathSet::new();
        set.add_or_update(Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001)));
        set.add_or_update(Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001)));
        set.add_or_update(Path::new(peer.clone(), TransportKind::Tcp, test_addr(9002)));
        assert_eq!(
            set.count(),
            2,
            "Duplicate route should not create a new entry"
        );
    }

    #[test]
    fn test_path_lifecycle_transitions() {
        let peer = PeerId::new();
        let mut path = Path::new(peer, TransportKind::Tcp, test_addr(9001));
        assert_eq!(path.state, PathState::Discovered);

        path.set_state(PathState::Candidate);
        assert_eq!(path.state, PathState::Candidate);

        path.set_state(PathState::Connecting);
        assert_eq!(path.state, PathState::Connecting);

        path.set_state(PathState::Available);
        assert_eq!(path.state, PathState::Available);

        path.set_state(PathState::Unavailable);
        assert_eq!(path.state, PathState::Unavailable);
    }

    #[test]
    fn test_pathset_available_filter() {
        let peer = PeerId::new();
        let mut set = PathSet::new();

        let mut p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        let mut p2 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9002));
        let p3 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9003));

        p1.set_state(PathState::Available);
        p2.set_state(PathState::Available);
        // p3 stays Discovered

        set.add_or_update(p1);
        set.add_or_update(p2);
        set.add_or_update(p3);

        assert_eq!(set.available().len(), 2);
        assert_eq!(set.count(), 3);
    }

    #[test]
    fn test_pathset_revives_unavailable() {
        let peer = PeerId::new();
        let mut set = PathSet::new();

        let mut p = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        p.set_state(PathState::Unavailable);
        set.add_or_update(p);
        assert_eq!(set.list()[0].state, PathState::Unavailable);

        // Re-discovering the same route should revive it
        let p2 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        set.add_or_update(p2);
        assert_eq!(set.count(), 1, "Should not duplicate");
        assert_eq!(
            set.list()[0].state,
            PathState::Discovered,
            "Should revive from Unavailable"
        );
    }

    #[test]
    fn test_registry_multi_peer_multi_path() {
        let registry = PathRegistry::new();
        let peer_a = PeerId::new();
        let peer_b = PeerId::new();

        registry.register_path(Path::new(
            peer_a.clone(),
            TransportKind::Tcp,
            test_addr(9001),
        ));
        registry.register_path(Path::new(
            peer_a.clone(),
            TransportKind::Tcp,
            test_addr(9002),
        ));
        registry.register_path(Path::new(
            peer_b.clone(),
            TransportKind::Tcp,
            test_addr(9003),
        ));

        assert_eq!(registry.get_paths(&peer_a).unwrap().count(), 2);
        assert_eq!(registry.get_paths(&peer_b).unwrap().count(), 1);
        assert_eq!(registry.all_peers().len(), 2);
    }

    #[test]
    fn test_single_path_backward_compat() {
        // The one-peer-one-path case must work identically
        let registry = PathRegistry::new();
        let peer = PeerId::new();
        registry.register_path(Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001)));

        let paths = registry.get_paths(&peer).unwrap();
        assert_eq!(paths.count(), 1);
        assert_eq!(paths.list()[0].transport, TransportKind::Tcp);
    }

    #[test]
    fn test_unknown_peer_returns_none() {
        let registry = PathRegistry::new();
        let ghost = PeerId::new();
        assert!(registry.get_paths(&ghost).is_none());
    }
}
