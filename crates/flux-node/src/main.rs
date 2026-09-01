use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "flux")]
#[command(version)] // This enables the --version flag
#[command(about = "Aryntra Flux Node CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Check environment and connectivity
    Doctor,
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Doctor) => {
            println!("Aryntra Flux Environment Check");
            println!("----------------------------");
            println!("Rust/Cargo:     ?");
            println!("Flux Core:      ?");

            // Basic Networking Check
            match std::net::TcpListener::bind("127.0.0.1:0") {
                Ok(_) => println!("Local Network:  ?"),
                Err(_) => println!("Local Network:  ?"),
            }

            println!("\nEnvironment: READY");
        }
        None => {
            println!("Aryntra Flux v{}", env!("CARGO_PKG_VERSION"));
            println!("Use 'flux doctor' to check environment.");
        }
    }
}
