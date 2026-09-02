use clap::{Parser, Subcommand};
use flux_core::{
    identity::PeerId,
    node::FluxNode,
    protocol::FluxMessage,
    session::SessionBuilder,
    transport::{TcpConnection, TcpTransport, Transport}, // Added Transport trait
};
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "flux")]
#[command(version)]
struct Cli {
    #[arg(short, long, default_value = "")]
    profile: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Show node identity
    Identity,

    /// Run discovery and show peers
    Run,

    /// Listen for incoming connections
    Listen {
        /// Port to listen on
        #[arg(short, long, default_value = "9002")]
        port: u16,
    },

    /// Connect to a peer and send a test message
    Connect {
        /// Peer address (IP:PORT)
        #[arg(short, long)]
        addr: String,

        /// Message to send
        #[arg(short, long, default_value = "Hello from Flux!")]
        message: String,
    },

    /// Health check
    Doctor,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    let mut node = FluxNode::new(&cli.profile);

    match &cli.command {
        Some(Commands::Identity) => {
            println!("Peer ID: {}", node.identity);
        }

        Some(Commands::Doctor) => {
            println!("✅ Flux node is healthy");
            println!("Identity: {}", node.identity);
        }

        Some(Commands::Run) => {
            println!("Starting Aryntra Flux Node [Profile: '{}']...", cli.profile);
            println!("Identity: {}", node.identity);

            node.start().await?;
            println!("State: Running (mDNS Discovery Active)\n");

            loop {
                let peers = node.registry.list();
                if peers.is_empty() {
                    println!("[...] Searching for peers...");
                } else {
                    println!("--- Discovered Peers ({}) ---", peers.len());
                    for peer in peers {
                        println!("ID: {} | Addr: {}", peer.id, peer.address);
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }

        Some(Commands::Listen { port }) => {
            println!("🎧 Aryntra Flux - Listening Mode");
            println!(
                "Profile: {}",
                if cli.profile.is_empty() {
                    "default"
                } else {
                    &cli.profile
                }
            );
            println!("Identity: {}", node.identity);
            println!("Port: {}\n", port);

            let transport = TcpTransport::new();
            let listen_addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

            let mut listener = transport.listen(listen_addr).await?;
            println!("✅ Listening on {}", listener.local_addr());
            println!("Waiting for connections...\n");

            loop {
                match listener.accept().await {
                    Ok((mut conn, addr)) => {
                        println!("📥 Incoming connection from {}", addr);

                        // Spawn handler for this connection
                        let local_peer_id = node.identity.clone();
                        tokio::spawn(async move {
                            // Handle server-side handshake
                            if let Some(tcp_conn) =
                                conn.as_any_mut().downcast_mut::<TcpConnection>()
                            {
                                if let Err(e) = tcp_conn.server_handshake(&local_peer_id).await {
                                    eprintln!("❌ Handshake failed: {}", e);
                                    return;
                                }
                            }

                            if let Some(peer_id) = conn.peer_id() {
                                println!("✅ Handshake completed with {}", peer_id);
                            }

                            // Receive message
                            match conn.recv_message().await {
                                Ok(msg) => {
                                    println!("📨 Received: {:?}", msg);

                                    // Send response
                                    match msg {
                                        FluxMessage::Ping { sequence, payload } => {
                                            println!("   Ping #{}: {}", sequence, payload);
                                            let response = FluxMessage::pong(
                                                sequence,
                                                "Pong from server!".to_string(),
                                            );

                                            if let Err(e) = conn.send_message(&response).await {
                                                eprintln!("❌ Failed to send response: {}", e);
                                            } else {
                                                println!("📤 Sent pong response");
                                            }
                                        }
                                        _ => {
                                            println!("   (Other message type)");
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("❌ Error receiving message: {}", e);
                                }
                            }

                            // Close connection
                            if let Err(e) = conn.close().await {
                                eprintln!("❌ Error closing connection: {}", e);
                            } else {
                                println!("👋 Connection closed\n");
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to accept connection: {}", e);
                    }
                }
            }
        }

        Some(Commands::Connect { addr, message }) => {
            println!("🔌 Aryntra Flux - Client Mode");
            println!(
                "Profile: {}",
                if cli.profile.is_empty() {
                    "default"
                } else {
                    &cli.profile
                }
            );
            println!("Identity: {}", node.identity);
            println!("Target: {}", addr);
            println!("Message: {}\n", message);

            let transport = TcpTransport::new();
            let target_addr: SocketAddr = addr.parse()?;

            // Create a dummy peer ID for connection (we don't know the real one yet)
            let target_peer_id = PeerId::new();

            println!("📡 Connecting to {}...", target_addr);

            let session_builder = SessionBuilder::new(&transport, node.identity.clone());
            let mut session = session_builder
                .connect(&target_peer_id, target_addr)
                .await?;

            println!("✅ Connected!");

            if let Some(peer_id) = session.peer_id() {
                println!("🤝 Remote peer: {}", peer_id);
            }

            // Send ping message
            println!("📤 Sending message...");
            let ping = FluxMessage::ping(1, message.clone());
            session.send_message(&ping).await?;

            // Wait for response
            println!("⏳ Waiting for response...");
            match tokio::time::timeout(Duration::from_secs(10), session.recv_message()).await {
                Ok(Ok(response)) => {
                    println!("📨 Received response: {:?}", response);

                    match response {
                        FluxMessage::Pong { sequence, payload } => {
                            println!("   Pong #{}: {}", sequence, payload);
                        }
                        _ => {
                            println!("   (Unexpected response type)");
                        }
                    }
                }
                Ok(Err(e)) => {
                    eprintln!("❌ Error receiving response: {}", e);
                }
                Err(_) => {
                    eprintln!("❌ Timeout waiting for response");
                }
            }

            // Close session
            println!("👋 Closing connection...");
            session.close().await?;
            println!("✅ Done!");
        }

        None => {
            println!("Use --help for available commands");
        }
    }

    Ok(())
}
