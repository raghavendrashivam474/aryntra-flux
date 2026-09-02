pub mod discovery;
pub mod identity;
pub mod node;
pub mod peer;

// S1.3: Transport abstraction
pub mod transport;
pub use transport::{Connection, Listener, TcpConnection, TcpTransport, Transport, TransportError};

// S1.3: Protocol definitions
pub mod protocol;
pub use protocol::{FluxMessage, PROTOCOL_VERSION};

// S1.3: Session management
pub mod session;
pub use session::{Session, SessionBuilder, SessionState};
