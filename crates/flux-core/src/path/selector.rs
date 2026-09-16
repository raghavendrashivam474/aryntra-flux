use crate::identity::PeerId;
use crate::path::{Path, PathRegistry, PathSet, PathState};

/// Deterministic path selector.
///
/// Evaluates candidate paths and selects the optimal path based on:
/// 1. Only considering `PathState::Available` paths with measured RTT.
/// 2. Selecting the lowest measured round-trip time (RTT).
/// 3. Breaking ties deterministically using stable `PathId` ordering.
pub struct PathSelector {
    registry: PathRegistry,
}

impl PathSelector {
    /// Create a new selector referencing the given path registry.
    pub fn new(registry: PathRegistry) -> Self {
        Self { registry }
    }

    /// Select the best usable path for a peer.
    /// Returns `None` if no path is available or measured.
    pub fn select_path(&self, peer_id: &PeerId) -> Option<Path> {
        let path_set = self.registry.get_paths(peer_id)?;
        Self::select_from_set(&path_set)
    }

    /// Pure selection function over a `PathSet`.
    pub fn select_from_set(set: &PathSet) -> Option<Path> {
        set.list()
            .iter()
            .filter(|p| p.state == PathState::Available)
            .filter_map(|p| {
                let rtt = p.metrics.as_ref()?.rtt_ms?;
                Some((rtt, &p.id, p))
            })
            .min_by(|(rtt_a, id_a, _), (rtt_b, id_b, _)| {
                rtt_a.cmp(rtt_b).then_with(|| id_a.cmp(id_b))
            })
            .map(|(_, _, path)| path.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::metrics::PathMetrics;
    use crate::path::TransportKind;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    #[test]
    fn test_selector_returns_none_when_no_usable_path() {
        let registry = PathRegistry::new();
        let selector = PathSelector::new(registry.clone());
        let peer = PeerId::new();

        // 1. Unknown peer
        assert!(selector.select_path(&peer).is_none());

        // 2. Peer with only Discovered path
        let p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        registry.register_path(p1);
        assert!(selector.select_path(&peer).is_none());
    }

    #[test]
    fn test_selector_ignores_unavailable() {
        let registry = PathRegistry::new();
        let selector = PathSelector::new(registry.clone());
        let peer = PeerId::new();

        let mut p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        p1.state = PathState::Unavailable;
        p1.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(5)));

        registry.register_path(p1);
        assert!(selector.select_path(&peer).is_none());
    }

    #[test]
    fn test_selector_ignores_unmeasured() {
        let registry = PathRegistry::new();
        let selector = PathSelector::new(registry.clone());
        let peer = PeerId::new();

        let mut p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        p1.state = PathState::Available;
        // metrics remains None

        registry.register_path(p1);
        assert!(selector.select_path(&peer).is_none());
    }

    #[test]
    fn test_selector_selects_lowest_rtt() {
        let registry = PathRegistry::new();
        let selector = PathSelector::new(registry.clone());
        let peer = PeerId::new();

        let mut p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        p1.state = PathState::Available;
        p1.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(45)));
        let p1_id = p1.id.clone();

        let mut p2 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9002));
        p2.state = PathState::Available;
        p2.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(5)));
        let p2_id = p2.id.clone();

        let mut p3 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9003));
        p3.state = PathState::Available;
        p3.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(20)));

        registry.register_path(p1);
        registry.register_path(p2);
        registry.register_path(p3);

        let selected = selector
            .select_path(&peer)
            .expect("Path should be selected");
        assert_eq!(selected.id, p2_id);
        assert_ne!(selected.id, p1_id);
        assert_eq!(selected.metrics.unwrap().rtt_ms, Some(5));
    }

    #[test]
    fn test_selector_is_deterministic_on_tie() {
        let registry = PathRegistry::new();
        let selector = PathSelector::new(registry.clone());
        let peer = PeerId::new();

        let mut p1 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9001));
        p1.state = PathState::Available;
        p1.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));

        let mut p2 = Path::new(peer.clone(), TransportKind::Tcp, test_addr(9002));
        p2.state = PathState::Available;
        p2.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));

        let smaller_id = if p1.id < p2.id {
            p1.id.clone()
        } else {
            p2.id.clone()
        };

        registry.register_path(p1);
        registry.register_path(p2);

        for _ in 0..10 {
            let selected = selector
                .select_path(&peer)
                .expect("Path should be selected");
            assert_eq!(
                selected.id, smaller_id,
                "Tie-break must consistently choose the smaller PathId"
            );
        }
    }
}
