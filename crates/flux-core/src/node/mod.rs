use crate::discovery;
use crate::identity::PeerId;
use crate::path::PathRegistry;
use crate::peer::PeerRegistry;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Starting,
    Running,
    Stopped,
}

pub struct FluxNode {
    pub identity: PeerId,
    pub registry: PeerRegistry,
    pub path_registry: PathRegistry,
    pub state: NodeState,
}

impl FluxNode {
    pub fn new(profile: &str) -> Self {
        Self {
            identity: PeerId::load_or_generate(profile),
            registry: PeerRegistry::default(),
            path_registry: PathRegistry::default(),
            state: NodeState::Starting,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        discovery::start_discovery(
            self.identity.clone(),
            self.registry.clone(),
            self.path_registry.clone(),
        )?;
        self.state = NodeState::Running;
        Ok(())
    }
}
