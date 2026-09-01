use crate::identity::PeerId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: PeerId,
    pub address: String,
    pub last_seen: std::time::Instant,
}

#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    peers: Arc<RwLock<HashMap<PeerId, Peer>>>,
}

impl PeerRegistry {
    pub fn update(&self, peer: Peer) {
        let mut peers = self.peers.write().unwrap();
        peers.insert(peer.id.clone(), peer);
    }

    pub fn list(&self) -> Vec<Peer> {
        let peers = self.peers.read().unwrap();
        peers.values().cloned().collect()
    }
}
