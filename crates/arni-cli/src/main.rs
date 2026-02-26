use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "arni")]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Connect to a database
    Connect {
        /// Connection profile name from config
        #[arg(short, long)]
        profile: String,
    },
    /// Execute a query
    Query {
        /// SQL query to execute
        query: String,
    },
    /// Show metadata
    Metadata {
        /// Show tables
        #[arg(short, long)]
        tables: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Connect { profile } => {
            println!("Connecting to profile: {}", profile);
            println!("Not yet implemented");
        }
        Commands::Query { query } => {
            println!("Executing query: {}", query);
            println!("Not yet implemented");
        }
        Commands::Metadata { tables } => {
            if tables {
                println!("Listing tables");
                println!("Not yet implemented");
            }
        }
    }

    Ok(())
}
