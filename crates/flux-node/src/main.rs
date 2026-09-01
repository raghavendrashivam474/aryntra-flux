use clap::{Parser, Subcommand};
use flux_core::node::FluxNode;
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
    Doctor,
    Run,
    Identity,
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
        _ => println!("Use --help for commands"),
    }
    Ok(())
}
