mod config;
mod logging_config;

use arni_data::adapter::{ConnectionConfig, DatabaseType};
use clap::{Parser, Subcommand};
use colored::*;
use config::{ConfigStore, ConnectionEntry};
use figlet_rs::FIGfont;
use std::collections::HashMap;
use std::error::Error;
use std::net::TcpStream;
use std::process::Command;
use std::time::Duration;

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
    /// Manage database connection profiles
    Config {
        #[command(subcommand)]
        action: ConfigAction,
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

#[derive(Subcommand)]
enum ConfigAction {
    /// Add or update a named connection profile
    Add {
        /// Unique profile name — letters, numbers, hyphens, underscores only
        #[arg(long)]
        name: String,

        /// Database type: postgres, mysql, sqlite, mongodb, sqlserver, oracle, duckdb
        #[arg(long = "type", value_name = "TYPE")]
        db_type: String,

        /// Hostname or IP address (required for network databases)
        #[arg(long)]
        host: Option<String>,

        /// Port number (defaults to the standard port for the database type)
        #[arg(long)]
        port: Option<u16>,

        /// Database name, or file path for SQLite/DuckDB (use ':memory:' for in-memory DuckDB)
        #[arg(long)]
        database: Option<String>,

        /// Username for authentication
        #[arg(long)]
        username: Option<String>,

        /// Password (leave unset to be prompted at runtime; avoid committing passwords)
        #[arg(long)]
        password: Option<String>,

        /// Enable SSL/TLS for the connection
        #[arg(long)]
        ssl: bool,

        /// Additional driver parameters as KEY=VALUE pairs (repeatable)
        #[arg(long, value_name = "KEY=VALUE")]
        param: Vec<String>,
    },
    /// List all configured connection profiles
    List,
    /// Remove a named connection profile
    Remove {
        /// Profile name to remove
        #[arg(long)]
        name: String,
    },
    /// Test a connection profile (validates config + checks TCP/file reachability)
    Test {
        /// Profile name to test
        #[arg(long)]
        name: String,
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

    // Initialize logging from ~/.arni/logging.yml (seed defaults on first run).
    let arni_home = config::arni_home();
    if let Err(e) = logging_config::write_default_logging_config(&arni_home) {
        // Non-fatal — proceed without a config file.
        eprintln!("arni: warning: could not write default logging config: {e}");
    }
    let log_cfg = logging_config::load_logging_config(&arni_home).unwrap_or_default();

    // --verbose overrides the level from the config file.
    let effective_level = if cli.verbose {
        "debug".to_string()
    } else {
        log_cfg.level.clone()
    };

    let rotation = match log_cfg.rolling.strategy {
        logging_config::RollingStrategy::Daily => arni_logging::RotationPolicy::Daily,
        logging_config::RollingStrategy::Hourly => arni_logging::RotationPolicy::Hourly,
        logging_config::RollingStrategy::Never => arni_logging::RotationPolicy::Never,
    };

    let log_dir = log_cfg.resolved_log_dir();

    // _log_guard MUST be held until process exit to flush the async file writer.
    let _log_guard = match arni_logging::init_arni_logging(&log_dir, &effective_level, rotation) {
        Ok(guard) => Some(guard),
        Err(e) => {
            eprintln!("arni: warning: file logging unavailable: {e}");
            let _ = arni_logging::init_default_with_filter(&effective_level);
            None
        }
    };

    tracing::info!("arni v{VERSION} started");

    // Show banner unless suppressed
    if !cli.no_banner {
        print_banner();
    }

    match cli.command {
        Commands::Config { action } => {
            handle_config_command(action).await?;
        }
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

// ─── Config command handler ───────────────────────────────────────────────────

async fn handle_config_command(action: ConfigAction) -> Result<(), Box<dyn Error>> {
    match action {
        ConfigAction::Add {
            name,
            db_type,
            host,
            port,
            database,
            username,
            password,
            ssl,
            param,
        } => {
            let parsed_type = parse_db_type(&db_type)?;

            // Validate database field — required for all adapter types.
            let database = database.ok_or_else(|| {
                let hint = match parsed_type {
                    DatabaseType::SQLite | DatabaseType::DuckDB => {
                        " (provide a file path or ':memory:' for DuckDB)"
                    }
                    _ => " (provide the database name)",
                };
                format!("--database is required{}", hint)
            })?;

            // Network databases need a host.
            if !matches!(parsed_type, DatabaseType::SQLite | DatabaseType::DuckDB) && host.is_none()
            {
                return Err(format!("--host is required for {} connections", parsed_type).into());
            }

            // Parse KEY=VALUE parameters.
            let mut parameters: HashMap<String, String> = HashMap::new();
            for kv in &param {
                let (k, v) = kv.split_once('=').ok_or_else(|| {
                    format!("Invalid --param '{}': expected KEY=VALUE format", kv)
                })?;
                parameters.insert(k.to_string(), v.to_string());
            }

            let entry = ConnectionEntry {
                db_type: parsed_type,
                host,
                port,
                database,
                username,
                password,
                ssl,
                parameters,
            };

            let mut store = ConfigStore::load(None)?;
            store.add(name.clone(), entry)?;
            store.save()?;

            println!(
                "{} {} {}",
                "✓".bright_green(),
                "Saved connection profile:".green(),
                name.bright_white()
            );
            println!(
                "  Config: {}",
                store.config_dir().join("connections.yml").display()
            );
        }

        ConfigAction::List => {
            let store = ConfigStore::load(None)?;
            let profiles = store.list();
            if profiles.is_empty() {
                println!("{}", "No connection profiles configured.".yellow());
                println!(
                    "  Run {} to add one.",
                    "arni config add --help".bright_white()
                );
                return Ok(());
            }
            print_config_table(&profiles);
        }

        ConfigAction::Remove { name } => {
            let mut store = ConfigStore::load(None)?;
            store.remove(&name)?;
            store.save()?;
            println!(
                "{} {} {}",
                "✓".bright_green(),
                "Removed connection profile:".green(),
                name.bright_white()
            );
        }

        ConfigAction::Test { name } => {
            let store = ConfigStore::load(None)?;
            let cfg = store.get(&name)?;

            println!(
                "Testing '{}' ({}) ...",
                name.bright_white(),
                cfg.db_type.to_string().cyan()
            );

            match test_connection(&cfg) {
                Ok(detail) => {
                    println!("  {} {}", "✓ OK:".bright_green(), detail);
                }
                Err(e) => {
                    println!("  {} {}", "✗ FAILED:".bright_red(), e);
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

/// Parse a user-supplied database type string into [`DatabaseType`].
fn parse_db_type(s: &str) -> Result<DatabaseType, Box<dyn Error>> {
    match s.to_lowercase().as_str() {
        "postgres" | "postgresql" => Ok(DatabaseType::Postgres),
        "mysql" => Ok(DatabaseType::MySQL),
        "sqlite" => Ok(DatabaseType::SQLite),
        "mongodb" => Ok(DatabaseType::MongoDB),
        "sqlserver" | "mssql" => Ok(DatabaseType::SQLServer),
        "oracle" => Ok(DatabaseType::Oracle),
        "duckdb" => Ok(DatabaseType::DuckDB),
        other => Err(format!(
            "Unknown database type '{}'. Valid: postgres, mysql, sqlite, mongodb, sqlserver, oracle, duckdb",
            other
        )
        .into()),
    }
}

/// Test reachability for a connection config.
///
/// - File-based (SQLite, DuckDB): checks the database file exists, or accepts `:memory:`.
/// - Network-based: performs a TCP connect with a 5-second timeout.
fn test_connection(cfg: &ConnectionConfig) -> Result<String, Box<dyn Error>> {
    match cfg.db_type {
        DatabaseType::SQLite | DatabaseType::DuckDB => {
            if cfg.database == ":memory:" {
                return Ok("In-memory database — no file path to verify.".to_string());
            }
            let path = std::path::Path::new(&cfg.database);
            if path.exists() {
                Ok(format!("File exists: {}", cfg.database))
            } else {
                Err(format!("Database file not found: {}", cfg.database).into())
            }
        }
        _ => {
            use std::net::ToSocketAddrs;
            let host = cfg.host.as_deref().unwrap_or("localhost");
            let port = cfg
                .port
                .unwrap_or_else(|| cfg.db_type.default_port().unwrap_or(5432));
            let addr_str = format!("{}:{}", host, port);

            let addrs: Vec<_> = addr_str
                .to_socket_addrs()
                .map_err(|e| format!("Cannot resolve '{}': {}", host, e))?
                .collect();

            if addrs.is_empty() {
                return Err(format!("No addresses found for '{}'", host).into());
            }

            let mut last_err: Option<std::io::Error> = None;
            for addr in &addrs {
                match TcpStream::connect_timeout(addr, Duration::from_secs(5)) {
                    Ok(_) => {
                        return Ok(format!("TCP connection to {}:{} succeeded.", host, port));
                    }
                    Err(e) => last_err = Some(e),
                }
            }

            Err(format!(
                "Cannot connect to {}:{} — {}",
                host,
                port,
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "connection failed".to_string())
            )
            .into())
        }
    }
}

/// Print a formatted table of connection profiles.
fn print_config_table(profiles: &[(&str, &ConnectionEntry)]) {
    const W_NAME: usize = 20;
    const W_TYPE: usize = 12;
    const W_HOST: usize = 20;
    const W_PORT: usize = 6;

    // Pad before colorizing so ANSI codes don't break column widths.
    println!(
        "{}  {}  {}  {}  {}  {}",
        format!("{:<W_NAME$}", "NAME").bright_white().bold(),
        format!("{:<W_TYPE$}", "TYPE").bright_white().bold(),
        format!("{:<W_HOST$}", "HOST").bright_white().bold(),
        format!("{:<W_PORT$}", "PORT").bright_white().bold(),
        "DATABASE".bright_white().bold(),
        "SSL".bright_white().bold(),
    );
    println!(
        "{}",
        "─".repeat(W_NAME + W_TYPE + W_HOST + W_PORT + 30).dimmed()
    );

    for (name, entry) in profiles {
        let type_str = entry.db_type.to_string();
        let host_str = entry.host.as_deref().unwrap_or("—");
        let port_str = entry
            .port
            .or_else(|| entry.db_type.default_port())
            .map(|p| p.to_string())
            .unwrap_or_else(|| "—".to_string());
        let ssl_str = if entry.ssl { "yes" } else { "no" };

        // Truncate long values to avoid wrapping.
        let name_s = &name[..name.len().min(W_NAME)];
        let type_s = &type_str[..type_str.len().min(W_TYPE)];
        let host_s = &host_str[..host_str.len().min(W_HOST)];
        let port_s = &port_str[..port_str.len().min(W_PORT)];

        // Pad before colorizing so ANSI codes don't inflate column width.
        let name_col = format!("{:<W_NAME$}", name_s).bright_cyan();
        let type_col = format!("{:<W_TYPE$}", type_s).yellow();
        let host_col = format!("{:<W_HOST$}", host_s);
        let port_col = format!("{:<W_PORT$}", port_s);
        println!(
            "{name_col}  {type_col}  {host_col}  {port_col}  {}  {}",
            entry.database.as_str(),
            ssl_str,
        );
    }

    println!(
        "\n{} profile(s) configured.",
        profiles.len().to_string().bright_white()
    );
}

// ─── Dev command handler ──────────────────────────────────────────────────────

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
