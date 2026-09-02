use clap::{Parser, Subcommand};
use flux_core::{
    identity::PeerId,
    node::FluxNode,
    protocol::FluxMessage,
    session::{Session, SessionBuilder},
    transfer::TransferManager,
    transport::{TcpConnection, TcpTransport, Transport},
};
use std::net::SocketAddr;
use std::path::PathBuf;
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

    /// Listen for incoming connections and transfers
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

    /// Send a file to a peer
    Send {
        /// Peer address (IP:PORT)
        #[arg(short, long)]
        addr: String,

        /// Path to the file to send
        #[arg(short, long)]
        file: String,
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
            println!("Flux node is healthy");
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
            println!("Aryntra Flux - Listening Mode");
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
            println!("Listening on {}", listener.local_addr());
            println!("Waiting for connections...\n");

            loop {
                match listener.accept().await {
                    Ok((mut conn, addr)) => {
                        println!("Incoming connection from {}", addr);

                        let local_peer_id = node.identity.clone();
                        tokio::spawn(async move {
                            // Server-side handshake
                            if let Some(tcp_conn) =
                                conn.as_any_mut().downcast_mut::<TcpConnection>()
                            {
                                if let Err(e) = tcp_conn.server_handshake(&local_peer_id).await {
                                    eprintln!("Handshake failed: {}", e);
                                    return;
                                }
                            }

                            if let Some(peer_id) = conn.peer_id() {
                                println!("Handshake completed with {}", peer_id);
                            }

                            // Receive first message to decide what to do
                            let first_msg = conn.recv_message().await;

                            match first_msg {
                                Ok(FluxMessage::Ping { sequence, payload }) => {
                                    println!("Received Ping #{}: {}", sequence, payload);
                                    let response = FluxMessage::pong(
                                        sequence,
                                        "Pong from server!".to_string(),
                                    );
                                    if let Err(e) = conn.send_message(&response).await {
                                        eprintln!("Failed to send pong: {}", e);
                                    } else {
                                        println!("Sent pong response");
                                    }
                                    let _ = conn.close().await;
                                    println!("Connection closed\n");
                                }

                                Ok(FluxMessage::TransferRequest { metadata }) => {
                                    println!("Transfer request: {}", metadata.file_name);
                                    let mut session = Session::from_connection(conn, local_peer_id);
                                    let output_dir = PathBuf::from("received");
                                    match TransferManager::receive_transfer(
                                        &mut session,
                                        metadata,
                                        &output_dir,
                                    )
                                    .await
                                    {
                                        Ok(()) => println!("Transfer complete\n"),
                                        Err(e) => {
                                            eprintln!("Transfer failed: {}\n", e)
                                        }
                                    }
                                    let _ = session.close().await;
                                }

                                Ok(other) => {
                                    println!("Unhandled message: {:?}", other);
                                    let _ = conn.close().await;
                                }

                                Err(e) => {
                                    eprintln!("Error receiving message: {}", e);
                                    let _ = conn.close().await;
                                }
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
        }

        Some(Commands::Connect { addr, message }) => {
            println!("Aryntra Flux - Client Mode");
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
            let target_peer_id = PeerId::new();

            println!("Connecting to {}...", target_addr);

            let session_builder = SessionBuilder::new(&transport, node.identity.clone());
            let mut session = session_builder
                .connect(&target_peer_id, target_addr)
                .await?;

            println!("Connected!");

            if let Some(peer_id) = session.peer_id() {
                println!("Remote peer: {}", peer_id);
            }

            println!("Sending message...");
            let ping = FluxMessage::ping(1, message.clone());
            session.send_message(&ping).await?;

            println!("Waiting for response...");
            match tokio::time::timeout(Duration::from_secs(10), session.recv_message()).await {
                Ok(Ok(response)) => {
                    println!("Received response: {:?}", response);
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
                    eprintln!("Error receiving response: {}", e);
                }
                Err(_) => {
                    eprintln!("Timeout waiting for response");
                }
            }

            println!("Closing connection...");
            session.close().await?;
            println!("Done!");
        }

        Some(Commands::Send { addr, file }) => {
            println!("Aryntra Flux - Send Mode");
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
            println!("File: {}\n", file);

            let file_path = PathBuf::from(file);
            if !file_path.exists() {
                eprintln!("File not found: {}", file_path.display());
                return Ok(());
            }

            let transport = TcpTransport::new();
            let target_addr: SocketAddr = addr.parse()?;
            let target_peer_id = PeerId::new();

            println!("Connecting to {}...", target_addr);

            let session_builder = SessionBuilder::new(&transport, node.identity.clone());
            let mut session = session_builder
                .connect(&target_peer_id, target_addr)
                .await?;

            println!("Connected!\n");

            match TransferManager::send_file(&mut session, &file_path).await {
                Ok(()) => println!("\nTransfer successful!"),
                Err(e) => eprintln!("\nTransfer failed: {}", e),
            }

            session.close().await?;
        }

        None => {
            println!("Use --help for available commands");
        }
    }

    Ok(())
}
