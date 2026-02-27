use clap::{Parser, Subcommand};
use colored::*;
use figlet_rs::FIGfont;
use std::error::Error;
use std::process::Command;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const TAGLINE: &str = "Unified database access for Rust";
const COMPOSE_FILE: &str = "compose.yml";

#[derive(Parser)]
#[command(name = "arni")]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Skip ASCII banner display
    #[arg(long, global = true)]
    no_banner: bool,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage development containers
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
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

        /// Connection profile name
        #[arg(short, long)]
        profile: String,

        /// Output format (table, json, csv, parquet)
        #[arg(short, long, default_value = "table")]
        format: String,
    },
    /// Show metadata
    Metadata {
        /// Connection profile name
        #[arg(short, long)]
        profile: String,

        /// Show tables
        #[arg(long)]
        tables: bool,

        /// Show columns
        #[arg(long)]
        columns: bool,

        /// Show schemas
        #[arg(long)]
        schemas: bool,

        /// Show views
        #[arg(long)]
        views: bool,

        /// Show indexes
        #[arg(long)]
        indexes: bool,
    },
    /// Export data
    Export {
        /// SQL query for export
        query: String,

        /// Connection profile name
        #[arg(short, long)]
        profile: String,

        /// Output format (json, csv, parquet)
        #[arg(short, long, default_value = "json")]
        format: String,

        /// Output file path
        #[arg(short, long)]
        output: String,
    },
}

#[derive(Subcommand)]
enum DevAction {
    /// Start development containers
    Start,
    /// Stop development containers
    Stop,
    /// Show container status
    Status,
    /// Show container logs
    Logs {
        /// Service name (postgres, mysql, mongodb, sqlserver, oracle)
        #[arg(short, long)]
        service: Option<String>,
    },
    /// Clean up containers and volumes
    Clean {
        /// Remove volumes
        #[arg(short, long)]
        volumes: bool,
    },
}

fn print_banner() {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("ARNI");

    if let Some(fig) = figure {
        println!("{}", fig.to_string().bright_cyan());
    }

    println!("{}", TAGLINE.bright_yellow());
    println!("{} {}\n", "Version".bright_blue(), VERSION.bright_white());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // Initialize arni-logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    arni_logging::init_default_with_filter(log_level)?;

    // Show banner unless suppressed
    if !cli.no_banner {
        print_banner();
    }

    match cli.command {
        Commands::Dev { action } => {
            handle_dev_command(action).await?;
        }
        Commands::Connect { profile } => {
            println!(
                "{} {}",
                "Connecting to profile:".green(),
                profile.bright_white()
            );
            println!("{}", "Not yet implemented".yellow());
        }
        Commands::Query {
            query,
            profile,
            format,
        } => {
            println!(
                "{} {} {} {}",
                "Executing query on".green(),
                profile.bright_white(),
                "with format".green(),
                format.bright_white()
            );
            println!("{} {}", "Query:".cyan(), query);
            println!("{}", "Not yet implemented".yellow());
        }
        Commands::Metadata {
            profile,
            tables,
            columns,
            schemas,
            views,
            indexes,
        } => {
            println!(
                "{} {}",
                "Fetching metadata from".green(),
                profile.bright_white()
            );

            if tables {
                println!("{}", "  • Tables".cyan());
            }
            if columns {
                println!("{}", "  • Columns".cyan());
            }
            if schemas {
                println!("{}", "  • Schemas".cyan());
            }
            if views {
                println!("{}", "  • Views".cyan());
            }
            if indexes {
                println!("{}", "  • Indexes".cyan());
            }

            println!("{}", "Not yet implemented".yellow());
        }
        Commands::Export {
            query,
            profile,
            format,
            output,
        } => {
            println!(
                "{} {} {} {} {} {}",
                "Exporting from".green(),
                profile.bright_white(),
                "to".green(),
                output.bright_white(),
                "as".green(),
                format.bright_white()
            );
            println!("{} {}", "Query:".cyan(), query);
            println!("{}", "Not yet implemented".yellow());
        }
    }

    Ok(())
}

async fn handle_dev_command(action: DevAction) -> Result<(), Box<dyn Error>> {
    // Check if podman-compose is available
    if !is_podman_compose_available() {
        eprintln!(
            "{}",
            "Error: podman-compose is not installed or not in PATH".red()
        );
        eprintln!(
            "{}",
            "Install with: brew install podman-compose (macOS) or pip install podman-compose"
                .yellow()
        );
        return Err("podman-compose not found".into());
    }

    match action {
        DevAction::Start => {
            println!("{}", "Starting development containers...".green());
            run_compose_command(&["up", "-d"])?;
            println!("{}", "✓ Containers started successfully".bright_green());
        }
        DevAction::Stop => {
            println!("{}", "Stopping development containers...".green());
            run_compose_command(&["down"])?;
            println!("{}", "✓ Containers stopped successfully".bright_green());
        }
        DevAction::Status => {
            println!("{}", "Container status:".green());
            run_compose_command(&["ps"])?;
        }
        DevAction::Logs { service } => {
            if let Some(svc) = service {
                println!("{} {}", "Showing logs for".green(), svc.bright_white());
                run_compose_command(&["logs", "--tail=50", &svc])?;
            } else {
                println!("{}", "Showing logs for all containers".green());
                run_compose_command(&["logs", "--tail=50"])?;
            }
        }
        DevAction::Clean { volumes } => {
            println!("{}", "Cleaning up containers...".green());
            if volumes {
                println!("{}", "  • Removing volumes".cyan());
                run_compose_command(&["down", "-v"])?;
            } else {
                run_compose_command(&["down"])?;
            }
            println!("{}", "✓ Cleanup completed".bright_green());
        }
    }

    Ok(())
}

fn is_podman_compose_available() -> bool {
    Command::new("podman-compose")
        .arg("--version")
        .output()
        .is_ok()
}

fn run_compose_command(args: &[&str]) -> Result<(), Box<dyn Error>> {
    let output = Command::new("podman-compose")
        .arg("-f")
        .arg(COMPOSE_FILE)
        .args(args)
        .output()?;

    // Print stdout
    if !output.stdout.is_empty() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }

    // Print stderr
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }

    if !output.status.success() {
        return Err(format!("Command failed with exit code: {:?}", output.status.code()).into());
    }

    Ok(())
}
