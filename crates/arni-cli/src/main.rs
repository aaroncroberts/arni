mod app_config;
mod config;
mod db;
mod logging_config;

use arni_data::adapter::{ConnectionConfig, DatabaseType, TableSearchMode};
use clap::{Parser, Subcommand};
use colored::*;
use comfy_table::{presets, Attribute, Cell, Color, ContentArrangement, Table as CTable};
use config::{ConfigStore, ConnectionEntry};
use figlet_rs::FIGfont;
use polars::prelude::{CsvWriter, DataFrame, JsonFormat, JsonWriter, ParquetWriter, SerWriter};
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

        /// List all tables in the database
        #[arg(long)]
        tables: bool,

        /// Describe columns for a specific table (requires --table)
        #[arg(long)]
        columns: bool,

        /// List databases/schemas on the server
        #[arg(long)]
        schemas: bool,

        /// List views
        #[arg(long)]
        views: bool,

        /// List indexes for a specific table (requires --table)
        #[arg(long)]
        indexes: bool,

        /// Table name used with --columns and --indexes
        #[arg(long)]
        table: Option<String>,

        /// Search for tables whose name starts with, contains, or ends with this literal string
        #[arg(long)]
        search: Option<String>,

        /// How to match the search pattern: starts, contains, or ends [default: contains]
        #[arg(long, default_value = "contains")]
        search_mode: String,
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

    // Load application config (~/.arni/config.yml) and apply native library paths
    // before anything else so adapters can find libclntsh / libduckdb at dlopen time.
    let arni_home = config::arni_home();
    let app_cfg = app_config::load_app_config(&arni_home).unwrap_or_default();
    app_cfg.apply_lib_paths();

    // Initialize logging from ~/.arni/logging.yml (seed defaults on first run).
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
            handle_connect_command(profile).await?;
        }
        Commands::Query {
            query,
            profile,
            format,
        } => {
            handle_query_command(query, profile, format).await?;
        }
        Commands::Metadata {
            profile,
            tables,
            columns,
            schemas,
            views,
            indexes,
            table,
            search,
            search_mode,
        } => {
            handle_metadata_command(
                profile,
                tables,
                columns,
                schemas,
                views,
                indexes,
                table,
                search,
                search_mode,
            )
            .await?;
        }
        Commands::Export {
            query,
            profile,
            format,
            output,
        } => {
            handle_export_command(query, profile, format, output).await?;
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

// ─── Connect command handler ──────────────────────────────────────────────────

async fn handle_connect_command(profile: String) -> Result<(), Box<dyn Error>> {
    let store = ConfigStore::load(None)?;
    let cfg = store.get(&profile)?;

    println!(
        "Connecting to '{}' ({})...",
        profile.bright_white(),
        cfg.db_type.to_string().cyan()
    );

    let adapter = db::connect(&store, &profile).await?;

    println!("{} Connected.", "✓".bright_green());

    // Server info (best-effort — not all adapters implement it).
    if let Ok(info) = adapter.get_server_info().await {
        println!(
            "  {} {} {}",
            "Server:".dimmed(),
            info.server_type.cyan(),
            info.version.bright_white()
        );
        for (k, v) in &info.extra_info {
            println!("  {}: {}", k.dimmed(), v);
        }
    }

    // Quick table summary.
    if let Ok(tables) = adapter.metadata().list_tables(None).await {
        println!(
            "  {} {} table(s) visible",
            "Tables:".dimmed(),
            tables.len().to_string().bright_white()
        );
    }

    Ok(())
}

// ─── Query command handler ────────────────────────────────────────────────────

async fn handle_query_command(
    query: String,
    profile: String,
    format: String,
) -> Result<(), Box<dyn Error>> {
    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await?;

    let mut df = adapter
        .query_df(&query)
        .await
        .map_err(|e| format!("Query failed: {}", e))?;

    match format.to_lowercase().as_str() {
        "table" | "t" => {
            println!("{}", df_to_table(&df));
            println!(
                "\n{} row(s) × {} column(s)",
                df.height().to_string().bright_white(),
                df.width().to_string().bright_white()
            );
        }
        "json" => {
            let json = df_to_json(&mut df)?;
            println!("{}", json);
        }
        "csv" => {
            let csv = df_to_csv(&mut df)?;
            print!("{}", csv);
        }
        other => {
            return Err(format!("Unknown format '{}'. Valid: table, json, csv", other).into());
        }
    }

    Ok(())
}

// ─── Metadata command handler ─────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn handle_metadata_command(
    profile: String,
    tables: bool,
    columns: bool,
    schemas: bool,
    views: bool,
    indexes: bool,
    table: Option<String>,
    search: Option<String>,
    search_mode: String,
) -> Result<(), Box<dyn Error>> {
    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await?;
    let meta = adapter.metadata();

    // --search: find tables by literal name fragment.
    if let Some(ref pattern) = search {
        let mode = match search_mode.to_lowercase().as_str() {
            "starts" | "starts-with" | "startswith" => TableSearchMode::StartsWith,
            "ends" | "ends-with" | "endswith" => TableSearchMode::EndsWith,
            _ => TableSearchMode::Contains,
        };
        let results = meta
            .find_tables(pattern, None, mode.clone())
            .await
            .map_err(|e| format!("find_tables failed: {}", e))?;

        let mode_label = match mode {
            TableSearchMode::StartsWith => "starts with",
            TableSearchMode::Contains => "contains",
            TableSearchMode::EndsWith => "ends with",
        };
        println!(
            "Tables matching '{}' ({}):",
            pattern.bright_white(),
            mode_label.cyan()
        );
        if results.is_empty() {
            println!("  {}", "(none found)".dimmed());
        } else {
            for t in &results {
                println!("  • {}", t.bright_cyan());
            }
            println!(
                "\n{} table(s) found.",
                results.len().to_string().bright_white()
            );
        }
        return Ok(());
    }

    let any_flag = tables || columns || schemas || views || indexes;

    // --schemas: list databases/schemas.
    if schemas {
        let dbs = meta
            .list_databases()
            .await
            .map_err(|e| format!("list_databases failed: {}", e))?;
        println!("{}", "Databases / Schemas:".bright_white().bold());
        for db in &dbs {
            println!("  • {}", db);
        }
        println!();
    }

    // --tables (or default when no flag is given).
    if tables || !any_flag {
        let tbl_list = meta
            .list_tables(None)
            .await
            .map_err(|e| format!("list_tables failed: {}", e))?;
        println!("{}", "Tables:".bright_white().bold());
        if tbl_list.is_empty() {
            println!("  {}", "(no tables found)".dimmed());
        } else {
            for t in &tbl_list {
                println!("  • {}", t.bright_cyan());
            }
            println!("\n{} table(s).", tbl_list.len().to_string().bright_white());
        }
        if !any_flag {
            return Ok(());
        }
        println!();
    }

    // --views: list views.
    if views {
        let view_list = meta
            .get_views(None)
            .await
            .map_err(|e| format!("get_views failed: {}", e))?;
        println!("{}", "Views:".bright_white().bold());
        if view_list.is_empty() {
            println!("  {}", "(no views found)".dimmed());
        } else {
            for v in &view_list {
                println!("  • {}", v.name.bright_cyan());
            }
        }
        println!();
    }

    // --columns: describe a table's columns.
    if columns {
        let tbl = table
            .as_deref()
            .ok_or("--columns requires --table <table_name>")?;
        let info = meta
            .describe_table(tbl, None)
            .await
            .map_err(|e| format!("describe_table('{}') failed: {}", tbl, e))?;
        print_table_info(&info);
        println!();
    }

    // --indexes: show indexes for a table.
    if indexes {
        let tbl = table
            .as_deref()
            .ok_or("--indexes requires --table <table_name>")?;
        let idx_list = meta
            .get_indexes(tbl, None)
            .await
            .map_err(|e| format!("get_indexes('{}') failed: {}", tbl, e))?;
        println!("{}", format!("Indexes on '{}':", tbl).bright_white().bold());
        if idx_list.is_empty() {
            println!("  {}", "(no indexes found)".dimmed());
        } else {
            for idx in &idx_list {
                println!(
                    "  • {} ({}) — [{}]",
                    idx.name.bright_cyan(),
                    if idx.is_unique {
                        "unique"
                    } else {
                        "non-unique"
                    }
                    .dimmed(),
                    idx.columns.join(", ")
                );
            }
        }
    }

    Ok(())
}

// ─── Export command handler ───────────────────────────────────────────────────

async fn handle_export_command(
    query: String,
    profile: String,
    format: String,
    output: String,
) -> Result<(), Box<dyn Error>> {
    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await?;

    println!("{}", "Executing query...".dimmed());
    let mut df = adapter
        .query_df(&query)
        .await
        .map_err(|e| format!("Query failed: {}", e))?;

    let rows = df.height();
    println!(
        "Fetched {} row(s). Writing {} to '{}'...",
        rows.to_string().bright_white(),
        format.cyan(),
        output.bright_white()
    );

    match format.to_lowercase().as_str() {
        "json" => {
            let json = df_to_json(&mut df)?;
            std::fs::write(&output, json)?;
        }
        "csv" => {
            let csv = df_to_csv(&mut df)?;
            std::fs::write(&output, csv)?;
        }
        "parquet" => {
            df_to_parquet(&mut df, &output)?;
        }
        other => {
            return Err(format!(
                "Unknown export format '{}'. Valid: json, csv, parquet",
                other
            )
            .into());
        }
    }

    println!(
        "{} Exported {} row(s) to '{}'.",
        "✓".bright_green(),
        rows,
        output.bright_white()
    );
    Ok(())
}

// ─── DataFrame formatting helpers ─────────────────────────────────────────────

/// Render a DataFrame as a pretty UTF-8 table using comfy-table.
fn df_to_table(df: &DataFrame) -> String {
    let mut table = CTable::new();
    table.load_preset(presets::UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);

    // Bold cyan headers.
    let headers: Vec<Cell> = df
        .get_column_names()
        .iter()
        .map(|name| {
            Cell::new(*name)
                .add_attribute(Attribute::Bold)
                .fg(Color::Cyan)
        })
        .collect();
    table.set_header(headers);

    // Data rows.
    for i in 0..df.height() {
        let cells: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|name| {
                df.column(name)
                    .ok()
                    .and_then(|s| s.get(i).ok())
                    .map(|v| format!("{}", v))
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect();
        table.add_row(cells);
    }

    table.to_string()
}

/// Serialize a DataFrame to a JSON array string.
fn df_to_json(df: &mut DataFrame) -> Result<String, Box<dyn Error>> {
    let mut buf: Vec<u8> = Vec::new();
    JsonWriter::new(&mut buf)
        .with_json_format(JsonFormat::Json)
        .finish(df)?;
    Ok(String::from_utf8(buf)?)
}

/// Serialize a DataFrame to CSV.
fn df_to_csv(df: &mut DataFrame) -> Result<String, Box<dyn Error>> {
    let mut buf: Vec<u8> = Vec::new();
    CsvWriter::new(&mut buf).finish(df)?;
    Ok(String::from_utf8(buf)?)
}

/// Write a DataFrame to a Parquet file at `path`.
fn df_to_parquet(df: &mut DataFrame, path: &str) -> Result<(), Box<dyn Error>> {
    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(df)?;
    Ok(())
}

/// Print a formatted describe-table view.
fn print_table_info(info: &arni_data::TableInfo) {
    const W_NAME: usize = 25;
    const W_TYPE: usize = 20;

    println!("{}", format!("Table: {}", info.name).bright_white().bold());
    if let Some(schema) = &info.schema {
        println!("  Schema: {}", schema.dimmed());
    }
    if let Some(rows) = info.row_count {
        println!("  Rows: ~{}", rows.to_string().bright_white());
    }
    println!();
    println!(
        "  {}  {}  {}  {}",
        format!("{:<W_NAME$}", "COLUMN").bright_white().bold(),
        format!("{:<W_TYPE$}", "TYPE").bright_white().bold(),
        format!("{:<8}", "NULLABLE").bright_white().bold(),
        "PK".bright_white().bold(),
    );
    println!("  {}", "─".repeat(W_NAME + W_TYPE + 20).dimmed());
    for col in &info.columns {
        let name_s = if col.name.len() > W_NAME {
            col.name[..W_NAME].to_string()
        } else {
            col.name.clone()
        };
        let type_s = if col.data_type.len() > W_TYPE {
            col.data_type[..W_TYPE].to_string()
        } else {
            col.data_type.clone()
        };
        let name_col = format!("{:<W_NAME$}", name_s).bright_cyan();
        let type_col = format!("{:<W_TYPE$}", type_s);
        let null_col = format!("{:<8}", if col.nullable { "yes" } else { "no" }).dimmed();
        let pk_col = if col.is_primary_key { "✓" } else { "" }.bright_yellow();
        println!("  {name_col}  {type_col}  {null_col}  {pk_col}");
    }
    println!(
        "\n{} column(s).",
        info.columns.len().to_string().bright_white()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_db_type ────────────────────────────────────────────────────────

    #[test]
    fn test_parse_db_type_postgres() {
        assert_eq!(parse_db_type("postgres").unwrap(), DatabaseType::Postgres);
    }

    #[test]
    fn test_parse_db_type_postgresql_alias() {
        assert_eq!(parse_db_type("postgresql").unwrap(), DatabaseType::Postgres);
    }

    #[test]
    fn test_parse_db_type_mysql() {
        assert_eq!(parse_db_type("mysql").unwrap(), DatabaseType::MySQL);
    }

    #[test]
    fn test_parse_db_type_sqlite() {
        assert_eq!(parse_db_type("sqlite").unwrap(), DatabaseType::SQLite);
    }

    #[test]
    fn test_parse_db_type_mongodb() {
        assert_eq!(parse_db_type("mongodb").unwrap(), DatabaseType::MongoDB);
    }

    #[test]
    fn test_parse_db_type_sqlserver() {
        assert_eq!(parse_db_type("sqlserver").unwrap(), DatabaseType::SQLServer);
    }

    #[test]
    fn test_parse_db_type_mssql_alias() {
        assert_eq!(parse_db_type("mssql").unwrap(), DatabaseType::SQLServer);
    }

    #[test]
    fn test_parse_db_type_oracle() {
        assert_eq!(parse_db_type("oracle").unwrap(), DatabaseType::Oracle);
    }

    #[test]
    fn test_parse_db_type_duckdb() {
        assert_eq!(parse_db_type("duckdb").unwrap(), DatabaseType::DuckDB);
    }

    #[test]
    fn test_parse_db_type_case_insensitive() {
        assert_eq!(parse_db_type("POSTGRES").unwrap(), DatabaseType::Postgres);
        assert_eq!(parse_db_type("MySQL").unwrap(), DatabaseType::MySQL);
        assert_eq!(parse_db_type("DuckDB").unwrap(), DatabaseType::DuckDB);
    }

    #[test]
    fn test_parse_db_type_unknown_returns_error() {
        let err = parse_db_type("redis").unwrap_err();
        assert!(err.to_string().contains("Unknown database type"));
        assert!(err.to_string().contains("redis"));
    }

    #[test]
    fn test_parse_db_type_empty_string_returns_error() {
        assert!(parse_db_type("").is_err());
    }

    // ── test_connection (file-based paths only — no TCP) ─────────────────────

    fn make_cfg(db_type: DatabaseType, database: impl Into<String>) -> ConnectionConfig {
        ConnectionConfig {
            id: "test".to_string(),
            name: "test".to_string(),
            db_type,
            host: None,
            port: None,
            database: database.into(),
            username: None,
            use_ssl: false,
            parameters: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_connection_sqlite_memory() {
        let result = test_connection(&make_cfg(DatabaseType::SQLite, ":memory:")).unwrap();
        assert!(result.contains("In-memory"));
    }

    #[test]
    fn test_connection_duckdb_memory() {
        let result = test_connection(&make_cfg(DatabaseType::DuckDB, ":memory:")).unwrap();
        assert!(result.contains("In-memory"));
    }

    #[test]
    fn test_connection_sqlite_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        std::fs::write(&db_path, b"").unwrap();

        let result =
            test_connection(&make_cfg(DatabaseType::SQLite, db_path.to_str().unwrap())).unwrap();
        assert!(result.contains("File exists"));
    }

    #[test]
    fn test_connection_sqlite_missing_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("nonexistent.db");

        let err = test_connection(&make_cfg(DatabaseType::SQLite, db_path.to_str().unwrap()))
            .unwrap_err();
        assert!(err.to_string().contains("Database file not found"));
    }

    #[test]
    fn test_connection_duckdb_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        std::fs::write(&db_path, b"").unwrap();

        let result =
            test_connection(&make_cfg(DatabaseType::DuckDB, db_path.to_str().unwrap())).unwrap();
        assert!(result.contains("File exists"));
    }
}
