mod app_config;
mod config;
mod db;
mod discovery;
mod filter;
mod json_output;
mod logging_config;

use arni::adapter::{ConnectionConfig, DatabaseType, QueryValue, TableSearchMode};
#[cfg(feature = "polars")]
use arni::{to_bytes, to_file, DataFormat};
use clap::{CommandFactory, Parser, Subcommand};
use colored::*;
use comfy_table::{presets, Attribute, Cell, Color, ContentArrangement, Table as CTable};
use config::{ConfigStore, ConnectionEntry};
use figlet_rs::FIGfont;
use filter::{json_to_query_value, parse_bulk_insert_data, parse_filter_json};
#[cfg(feature = "polars")]
use arni::polars::prelude::DataFrame;
use std::collections::HashMap;
use std::error::Error;
use std::process::Command;
use std::time::Duration;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const TAGLINE: &str = "Unified database access for Rust";
const COMPOSE_FILE: &str = "compose.yml";

#[derive(Parser)]
#[command(name = "arni")]
#[command(
    author,
    version,
    about = "Unified database access — query any database, get QueryResult or DataFrame"
)]
#[command(
    long_about = "Unified database access for Rust.\n\nConnect to PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, DuckDB, or SQLite\nthrough a single trait-based API and receive every result as a Polars DataFrame.\n\nFor agent/script use, add --json to any command to receive a machine-readable\n{ok, …} envelope instead of human-readable formatted output.\n\nDiscovery flags (require no subcommand):\n  arni --list-tools       JSON list of all commands with argument schemas\n  arni --capabilities     JSON of supported database types and features\n  arni --schema <cmd>     JSON input/output schema for a specific command"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Skip ASCII banner display
    #[arg(long, global = true)]
    no_banner: bool,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Output machine-readable JSON (`{ok, …}` envelope) for agent/script use.
    /// Suppresses all human-readable formatting; errors are written to stdout
    /// as `{ok:false, error:{code, message}}` and exit with code 1.
    #[arg(long = "json", global = true)]
    json_output: bool,

    /// List all available commands with their argument schemas (JSON).
    /// Useful for agents that need to self-discover what arni can do.
    /// Output: [{name, description, args:[{name, type, required, description}]}]
    #[arg(long, conflicts_with = "capabilities")]
    list_tools: bool,

    /// Describe supported database types and features (JSON).
    /// Output: {version, database_types:[], features:[]}
    #[arg(long, conflicts_with = "list_tools")]
    capabilities: bool,

    /// Show the input/output JSON schema for a specific command.
    /// Example: arni --schema query
    /// Output: {command, input:{…}, output:{…}}
    #[arg(long, value_name = "COMMAND")]
    schema: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage development containers
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
    /// Connect to a database and print server info (version, host, database)
    ///
    /// Verifies credentials and prints a one-line server info summary.
    /// Useful for checking connectivity before running queries.
    ///
    /// Example:
    ///   arni connect --profile dev-postgres
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

        /// Output format: table, json, csv, xml
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

        /// How to match the search pattern: starts, contains, or ends
        #[arg(long, default_value = "contains")]
        search_mode: String,

        /// List foreign keys for a specific table (requires --table)
        #[arg(long)]
        fkeys: bool,

        /// List stored procedures/functions
        #[arg(long)]
        procs: bool,

        /// Show server version and configuration info
        #[arg(long)]
        server: bool,
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

        /// Output format: json, csv, xml, parquet, excel
        #[arg(short, long, default_value = "json")]
        format: String,

        /// Output file path
        #[arg(short, long)]
        output: String,
    },
    /// Start arni as an MCP server (JSON-RPC 2.0 over stdio)
    ///
    /// Exposes all 14 DbAdapter operations as AI-callable MCP tools.
    /// Reads connection profiles from ~/.arni/config.yaml (same as CLI).
    ///
    /// Register with Claude Desktop or Claude Code in one line:
    ///   {"command": "arni", "args": ["mcp"]}
    ///
    /// See docs/mcp.md for the full tool reference and registration guide.
    Mcp,
    /// Insert rows into a table from a JSON file
    ///
    /// The data file must contain a JSON array of objects where each object is a row.
    /// All objects must have the same keys (column names).
    ///
    /// Example data file:
    ///   [{"name":"Alice","score":92.5},{"name":"Bob","score":87.0}]
    #[command(name = "bulk-insert")]
    BulkInsert {
        /// Connection profile name
        #[arg(short, long)]
        profile: String,
        /// Target table name
        #[arg(long)]
        table: String,
        /// Path to JSON file: array of row objects, e.g. [{"col":"val"}, ...]
        #[arg(long)]
        data: String,
        /// Schema/database name (optional)
        #[arg(long)]
        schema: Option<String>,
    },
    /// Update rows matching a filter with new column values
    ///
    /// Filter JSON format: {"col":{"op":"value"}}
    /// Ops: eq, ne, gt, gte, lt, lte, in (array), is_null, is_not_null
    /// Compound: {"and":[...]}, {"or":[...]}, {"not":{...}}
    ///
    /// Examples:
    ///   --filter '{"id":{"eq":42}}'
    ///   --filter '{"and":[{"score":{"gte":80}},{"active":{"eq":true}}]}'
    #[command(name = "bulk-update")]
    BulkUpdate {
        /// Connection profile name
        #[arg(short, long)]
        profile: String,
        /// Target table name
        #[arg(long)]
        table: String,
        /// Filter expression as JSON (see command help for format)
        #[arg(long)]
        filter: String,
        /// Column values to set as JSON object: {"col":"value", ...}
        #[arg(long)]
        values: String,
        /// Schema/database name (optional)
        #[arg(long)]
        schema: Option<String>,
    },
    /// Delete rows matching a filter
    ///
    /// Filter JSON format: {"col":{"op":"value"}}
    /// Ops: eq, ne, gt, gte, lt, lte, in (array), is_null, is_not_null
    /// Compound: {"and":[...]}, {"or":[...]}, {"not":{...}}
    ///
    /// Examples:
    ///   --filter '{"active":{"eq":false}}'
    ///   --filter '{"score":{"lt":50}}'
    #[command(name = "bulk-delete")]
    BulkDelete {
        /// Connection profile name
        #[arg(short, long)]
        profile: String,
        /// Target table name
        #[arg(long)]
        table: String,
        /// Filter expression as JSON (see command help for format)
        #[arg(long)]
        filter: String,
        /// Schema/database name (optional)
        #[arg(long)]
        schema: Option<String>,
    },
    /// Search for tables whose names match a pattern
    #[command(name = "find-tables")]
    FindTables {
        /// Connection profile name
        #[arg(short, long)]
        profile: String,
        /// Name pattern to search for
        pattern: String,
        /// Match strategy: contains (default), starts, ends
        #[arg(long, default_value = "contains")]
        mode: String,
        /// Schema/database name (optional)
        #[arg(long)]
        schema: Option<String>,
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
    if let Ok(standard_font) = FIGfont::standard() {
        if let Some(fig) = standard_font.convert("ARNI") {
            println!("{}", fig.to_string().bright_cyan());
        }
    } else {
        println!("{}", "ARNI".bright_cyan());
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

    let json_mode = cli.json_output;

    // ── Discovery flags: short-circuit before any subcommand dispatch ─────────
    if cli.list_tools {
        json_output::emit(&discovery::list_tools());
        return Ok(());
    }
    if cli.capabilities {
        json_output::emit(&discovery::capabilities());
        return Ok(());
    }
    if let Some(ref cmd_name) = cli.schema {
        match discovery::schema(cmd_name) {
            Some(v) => {
                json_output::emit(&v);
                return Ok(());
            }
            None => {
                let msg = format!(
                    "Unknown command '{}'. Valid: connect, query, metadata, export, config, mcp, dev",
                    cmd_name
                );
                if json_mode {
                    json_output::emit(&json_output::error("UNKNOWN_COMMAND", &msg));
                    std::process::exit(1);
                } else {
                    return Err(msg.into());
                }
            }
        }
    }

    // Suppress banner in JSON mode (stdout must contain only valid JSON).
    if !cli.no_banner && !json_mode {
        print_banner();
    }

    // Require a subcommand when no discovery flag was supplied.
    let Some(command) = cli.command else {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    };

    let result = run_command(command, json_mode).await;

    if let Err(e) = result {
        if json_mode {
            json_output::emit(&json_output::error("COMMAND_ERROR", &e.to_string()));
            std::process::exit(1);
        }
        return Err(e);
    }

    Ok(())
}

// ─── Top-level command dispatcher ─────────────────────────────────────────────

async fn run_command(command: Commands, json_mode: bool) -> Result<(), Box<dyn Error>> {
    match command {
        Commands::Config { action } => handle_config_command(action, json_mode).await,
        Commands::Dev { action } => handle_dev_command(action).await,
        Commands::Connect { profile } => handle_connect_command(profile, json_mode).await,
        Commands::Query {
            query,
            profile,
            format,
        } => handle_query_command(query, profile, format, json_mode).await,
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
            fkeys,
            procs,
            server,
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
                fkeys,
                procs,
                server,
                json_mode,
            )
            .await
        }
        Commands::Export {
            query,
            profile,
            format,
            output,
        } => handle_export_command(query, profile, format, output, json_mode).await,
        Commands::Mcp => arni_mcp::serve().await.map_err(|e| e.into()),
        Commands::BulkInsert {
            profile,
            table,
            data,
            schema,
        } => handle_bulk_insert_command(profile, table, data, schema, json_mode).await,
        Commands::BulkUpdate {
            profile,
            table,
            filter,
            values,
            schema,
        } => handle_bulk_update_command(profile, table, filter, values, schema, json_mode).await,
        Commands::BulkDelete {
            profile,
            table,
            filter,
            schema,
        } => handle_bulk_delete_command(profile, table, filter, schema, json_mode).await,
        Commands::FindTables {
            profile,
            pattern,
            mode,
            schema,
        } => handle_find_tables_command(profile, pattern, mode, schema, json_mode).await,
    }
}

// ─── Config command handler ───────────────────────────────────────────────────

async fn handle_config_command(
    action: ConfigAction,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
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
                pool_config: None,
            };

            let mut store = ConfigStore::load(None)?;
            store.add(name.clone(), entry)?;
            store.save()?;

            if json_mode {
                json_output::emit(&serde_json::json!({
                    "ok": true,
                    "name": name,
                    "saved_to": store.config_dir().join("connections.yml").to_string_lossy()
                }));
            } else {
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
        }

        ConfigAction::List => {
            let store = ConfigStore::load(None)?;
            let profiles = store.list();
            if json_mode {
                let arr: Vec<serde_json::Value> = profiles
                    .iter()
                    .map(|(name, entry)| {
                        serde_json::json!({
                            "name": name,
                            "type": entry.db_type.to_string(),
                            "host": entry.host,
                            "port": entry.port.or_else(|| entry.db_type.default_port()),
                            "database": entry.database,
                            "ssl": entry.ssl,
                        })
                    })
                    .collect();
                json_output::emit(&serde_json::json!({ "ok": true, "profiles": arr }));
            } else {
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
        }

        ConfigAction::Remove { name } => {
            let mut store = ConfigStore::load(None)?;
            store.remove(&name)?;
            store.save()?;
            if json_mode {
                json_output::emit(&serde_json::json!({ "ok": true, "name": name }));
            } else {
                println!(
                    "{} {} {}",
                    "✓".bright_green(),
                    "Removed connection profile:".green(),
                    name.bright_white()
                );
            }
        }

        ConfigAction::Test { name } => {
            let store = ConfigStore::load(None)?;
            let cfg = store.get(&name)?;

            if !json_mode {
                println!(
                    "Testing '{}' ({}) ...",
                    name.bright_white(),
                    cfg.db_type.to_string().cyan()
                );
            }

            match test_connection(&cfg).await {
                Ok(detail) => {
                    if json_mode {
                        json_output::emit(
                            &serde_json::json!({ "ok": true, "name": name, "detail": detail }),
                        );
                    } else {
                        println!("  {} {}", "✓ OK:".bright_green(), detail);
                    }
                }
                Err(e) => {
                    if json_mode {
                        // Let the error propagate — main() will emit the JSON error envelope.
                        return Err(e);
                    } else {
                        println!("  {} {}", "✗ FAILED:".bright_red(), e);
                        return Err(e);
                    }
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
/// - Network-based: performs a TCP connect with a 5-second timeout using the async runtime,
///   avoiding blocking thread-pool starvation under Tokio.
async fn test_connection(cfg: &ConnectionConfig) -> Result<String, Box<dyn Error>> {
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

            let mut last_err: Option<String> = None;
            for addr in &addrs {
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio::net::TcpStream::connect(addr),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        return Ok(format!("TCP connection to {}:{} succeeded.", host, port));
                    }
                    Ok(Err(e)) => last_err = Some(e.to_string()),
                    Err(_elapsed) => last_err = Some("timeout after 5s".to_string()),
                }
            }

            Err(format!(
                "Cannot connect to {}:{} — {}",
                host,
                port,
                last_err.unwrap_or_else(|| "connection failed".to_string())
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

async fn handle_connect_command(profile: String, json_mode: bool) -> Result<(), Box<dyn Error>> {
    let store = ConfigStore::load(None)?;
    let cfg = store.get(&profile).map_err(|e| {
        format!(
            "{}\nhint: run `arni config list` to see available profiles",
            e
        )
    })?;

    if !json_mode {
        println!(
            "Connecting to '{}' ({})...",
            profile.bright_white(),
            cfg.db_type.to_string().cyan()
        );
    }

    let adapter = db::connect(&store, &profile).await?;

    if json_mode {
        // Collect server info and table count for the JSON envelope.
        let (server_type, version, extra_info) = if let Ok(info) = adapter.get_server_info().await {
            (
                info.server_type.clone(),
                info.version.clone(),
                serde_json::to_value(&info.extra_info).unwrap_or(serde_json::Value::Null),
            )
        } else {
            (
                cfg.db_type.to_string(),
                String::new(),
                serde_json::Value::Null,
            )
        };
        let table_count = adapter
            .metadata()
            .list_tables(None)
            .await
            .map(|t| t.len())
            .unwrap_or(0);
        json_output::emit(&serde_json::json!({
            "ok": true,
            "server_type": server_type,
            "version": version,
            "extra_info": extra_info,
            "table_count": table_count,
        }));
    } else {
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
    }

    Ok(())
}

// ─── Query command handler ────────────────────────────────────────────────────

async fn handle_query_command(
    query: String,
    profile: String,
    format: String,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await.map_err(|e| {
        format!(
            "{}\nhint: run `arni config list` to see available profiles",
            e
        )
    })?;

    #[cfg(feature = "polars")]
    {
        let mut df = adapter
            .query_df(&query)
            .await
            .map_err(|e| format!("Query failed: {}", e))?;

        // --json overrides --format: always emit the agent envelope.
        if json_mode {
            json_output::emit(&json_output::query_result(&df));
            return Ok(());
        }

        // Human-readable output — validate and apply --format.
        // "table"/"t" is a CLI-only display mode; all other values must be a valid DataFormat.
        let fmt = format.to_lowercase();
        let is_table = matches!(fmt.as_str(), "table" | "t");
        let data_fmt: Option<DataFormat> = if is_table {
            None
        } else {
            let parsed: DataFormat = fmt.parse().map_err(|_: String| {
                format!("Unknown format '{}'. Valid: table, json, csv, xml", format)
            })?;
            // Binary formats can only be used with `arni export`.
            if matches!(parsed, DataFormat::Parquet | DataFormat::Excel) {
                return Err(format!(
                    "'{}' is a binary format; use `arni export --format {} --output file.{}` instead",
                    fmt,
                    fmt,
                    parsed.extension()
                )
                .into());
            }
            Some(parsed)
        };

        match data_fmt {
            None => {
                // "table" / "t"
                println!("{}", df_to_table(&df));
                println!(
                    "\n{} row(s) × {} column(s)",
                    df.height().to_string().bright_white(),
                    df.width().to_string().bright_white()
                );
            }
            Some(DataFormat::Json) => {
                let bytes = to_bytes(&mut df, DataFormat::Json)?;
                println!("{}", String::from_utf8(bytes)?);
            }
            Some(DataFormat::Csv) => {
                let bytes = to_bytes(&mut df, DataFormat::Csv)?;
                print!("{}", String::from_utf8(bytes)?);
            }
            Some(DataFormat::Xml) => {
                let bytes = to_bytes(&mut df, DataFormat::Xml)?;
                println!("{}", String::from_utf8(bytes)?);
            }
            Some(_) => unreachable!("binary formats already rejected above"),
        }
    }

    #[cfg(not(feature = "polars"))]
    {
        let qr = adapter
            .execute_query(&query)
            .await
            .map_err(|e| format!("Query failed: {}", e))?;

        if json_mode {
            json_output::emit(&json_output::query_result_from_qr(&qr));
            return Ok(());
        }

        println!("{}", qr_to_table(&qr));
        println!(
            "\n{} row(s) × {} column(s)",
            qr.rows.len().to_string().bright_white(),
            qr.columns.len().to_string().bright_white()
        );
        let _ = format; // suppress unused warning
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
    fkeys: bool,
    procs: bool,
    server: bool,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
    // Validate flags that require --table before connecting (fast-fail).
    if columns && table.is_none() {
        return Err("--columns requires --table <table_name>".into());
    }
    if indexes && table.is_none() {
        return Err("--indexes requires --table <table_name>".into());
    }
    if fkeys && table.is_none() {
        return Err("--fkeys requires --table <table_name>".into());
    }

    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await.map_err(|e| {
        format!(
            "{}\nhint: run `arni config list` to see available profiles",
            e
        )
    })?;
    let meta = adapter.metadata();

    // ── --search ──────────────────────────────────────────────────────────────
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

        if json_mode {
            let mode_str = match mode {
                TableSearchMode::StartsWith => "starts",
                TableSearchMode::Contains => "contains",
                TableSearchMode::EndsWith => "ends",
            };
            json_output::emit(&serde_json::json!({
                "ok": true,
                "pattern": pattern,
                "mode": mode_str,
                "tables": results,
            }));
        } else {
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
        }
        return Ok(());
    }

    let any_flag = tables || columns || schemas || views || indexes || fkeys || procs || server;

    // ── JSON mode: collect all requested sections into one envelope ───────────
    if json_mode {
        let mut out = serde_json::Map::new();
        out.insert("ok".into(), serde_json::Value::Bool(true));

        if schemas || (!any_flag) {
            let dbs = meta
                .list_databases()
                .await
                .map_err(|e| format!("list_databases failed: {}", e))?;
            out.insert("databases".into(), serde_json::json!(dbs));
        }
        if tables || !any_flag {
            let tbl_list = meta
                .list_tables(None)
                .await
                .map_err(|e| format!("list_tables failed: {}", e))?;
            let arr: Vec<serde_json::Value> = tbl_list
                .iter()
                .map(|name| serde_json::json!({ "name": name }))
                .collect();
            out.insert("tables".into(), serde_json::json!(arr));
        }
        if views {
            let view_list = meta
                .get_views(None)
                .await
                .map_err(|e| format!("get_views failed: {}", e))?;
            out.insert(
                "views".into(),
                serde_json::to_value(&view_list).unwrap_or(serde_json::Value::Array(vec![])),
            );
        }
        if columns {
            let tbl = table.as_deref().unwrap(); // validated above
            let info = meta
                .describe_table(tbl, None)
                .await
                .map_err(|e| format!("describe_table('{}') failed: {}", tbl, e))?;
            out.insert("table".into(), serde_json::json!(tbl));
            out.insert(
                "columns".into(),
                serde_json::to_value(&info.columns).unwrap_or_default(),
            );
        }
        if indexes {
            let tbl = table.as_deref().unwrap(); // validated above
            let idx_list = meta
                .get_indexes(tbl, None)
                .await
                .map_err(|e| format!("get_indexes('{}') failed: {}", tbl, e))?;
            out.insert("table".into(), serde_json::json!(tbl));
            out.insert(
                "indexes".into(),
                serde_json::to_value(&idx_list).unwrap_or_default(),
            );
        }
        if fkeys {
            let tbl = table.as_deref().unwrap(); // validated above
            let fk_list = meta
                .get_foreign_keys(tbl, None)
                .await
                .map_err(|e| format!("get_foreign_keys('{}') failed: {}", tbl, e))?;
            out.insert("table".into(), serde_json::json!(tbl));
            out.insert(
                "foreign_keys".into(),
                serde_json::to_value(&fk_list).unwrap_or_default(),
            );
        }
        if procs {
            let proc_list = meta
                .list_stored_procedures(None)
                .await
                .map_err(|e| format!("list_stored_procedures failed: {}", e))?;
            out.insert(
                "procedures".into(),
                serde_json::to_value(&proc_list).unwrap_or_default(),
            );
        }
        if server {
            let info = meta
                .get_server_info()
                .await
                .map_err(|e| format!("get_server_info failed: {}", e))?;
            out.insert(
                "server".into(),
                serde_json::to_value(&info).unwrap_or_default(),
            );
        }

        json_output::emit(&serde_json::Value::Object(out));
        return Ok(());
    }

    // ── Human-readable mode ───────────────────────────────────────────────────

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
        println!();
    }

    // --fkeys: show foreign keys for a table.
    if fkeys {
        let tbl = table
            .as_deref()
            .ok_or("--fkeys requires --table <table_name>")?;
        let fk_list = meta
            .get_foreign_keys(tbl, None)
            .await
            .map_err(|e| format!("get_foreign_keys('{}') failed: {}", tbl, e))?;
        println!(
            "{}",
            format!("Foreign keys on '{}':", tbl).bright_white().bold()
        );
        if fk_list.is_empty() {
            println!("  {}", "(no foreign keys found)".dimmed());
        } else {
            for fk in &fk_list {
                let on_delete = fk
                    .on_delete
                    .as_deref()
                    .map(|s| format!(" ON DELETE {}", s))
                    .unwrap_or_default();
                println!(
                    "  • {} [{}] → {}.{}{}",
                    fk.name.bright_cyan(),
                    fk.columns.join(", "),
                    fk.referenced_table.bright_white(),
                    fk.referenced_columns.join(", "),
                    on_delete.dimmed(),
                );
            }
        }
        println!();
    }

    // --procs: list stored procedures/functions.
    if procs {
        let proc_list = meta
            .list_stored_procedures(None)
            .await
            .map_err(|e| format!("list_stored_procedures failed: {}", e))?;
        println!("{}", "Stored Procedures / Functions:".bright_white().bold());
        if proc_list.is_empty() {
            println!("  {}", "(none found)".dimmed());
        } else {
            for p in &proc_list {
                let ret = p
                    .return_type
                    .as_deref()
                    .map(|r| format!(" → {}", r))
                    .unwrap_or_default();
                println!("  • {}{}", p.name.bright_cyan(), ret.dimmed());
            }
            println!(
                "\n{} procedure(s).",
                proc_list.len().to_string().bright_white()
            );
        }
        println!();
    }

    // --server: show server version and configuration info.
    if server {
        let info = meta
            .get_server_info()
            .await
            .map_err(|e| format!("get_server_info failed: {}", e))?;
        println!("{}", "Server Info:".bright_white().bold());
        println!("  {} {}", "Type:".dimmed(), info.server_type.cyan());
        println!("  {} {}", "Version:".dimmed(), info.version.bright_white());
        for (k, v) in &info.extra_info {
            println!("  {}: {}", k.dimmed(), v);
        }
        println!();
    }

    Ok(())
}

// ─── Export command handler ───────────────────────────────────────────────────

// The `return Ok(())` inside the #[cfg(feature = "polars")] block is required:
// without it, `Ok(())` would be the block's value (discarded), not the function's return.
// Clippy flags it as "needless" when polars is enabled because it's the last expression,
// but it IS needed for the cfg-dual-path pattern to work correctly in both compile modes.
#[allow(clippy::needless_return)]
async fn handle_export_command(
    query: String,
    profile: String,
    format: String,
    output: String,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "polars")]
    {
        // Parse and validate format before connecting (fast fail, no wasted connection).
        let data_fmt: DataFormat = format.parse().map_err(|e: String| e)?;

        let store = ConfigStore::load(None)?;
        let adapter = db::connect(&store, &profile).await.map_err(|e| {
            format!(
                "{}\nhint: run `arni config list` to see available profiles",
                e
            )
        })?;

        if !json_mode {
            println!("{}", "Executing query...".dimmed());
        }
        let mut df = adapter
            .query_df(&query)
            .await
            .map_err(|e| format!("Query failed: {}", e))?;

        let rows = df.height();

        if !json_mode {
            println!(
                "Fetched {} row(s). Writing {} to '{}'...",
                rows.to_string().bright_white(),
                data_fmt.to_string().cyan(),
                output.bright_white()
            );
        }

        match data_fmt {
            DataFormat::Json => {
                let bytes = to_bytes(&mut df, DataFormat::Json)?;
                std::fs::write(&output, bytes)?;
            }
            DataFormat::Csv => {
                let bytes = to_bytes(&mut df, DataFormat::Csv)?;
                std::fs::write(&output, bytes)?;
            }
            DataFormat::Xml => {
                to_file(&mut df, DataFormat::Xml, std::path::Path::new(&output))?;
            }
            DataFormat::Parquet => {
                to_file(&mut df, DataFormat::Parquet, std::path::Path::new(&output))?;
            }
            DataFormat::Excel => {
                to_file(&mut df, DataFormat::Excel, std::path::Path::new(&output))?;
            }
        }

        if json_mode {
            json_output::emit(&serde_json::json!({
                "ok": true,
                "file": output,
                "rows": rows,
                "format": format,
            }));
        } else {
            println!(
                "{} Exported {} row(s) to '{}'.",
                "✓".bright_green(),
                rows,
                output.bright_white()
            );
        }

        return Ok(());
    }

    #[cfg(not(feature = "polars"))]
    {
        let _ = (query, profile, format, output, json_mode);
        Err("The 'export' command requires the 'polars' feature. \
             Rebuild with: cargo install arni --features polars"
            .into())
    }
}

// ─── Bulk-insert command handler ──────────────────────────────────────────────

async fn handle_bulk_insert_command(
    profile: String,
    table: String,
    data_path: String,
    schema: Option<String>,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
    let raw = std::fs::read_to_string(&data_path)
        .map_err(|e| format!("Cannot read data file '{}': {}", data_path, e))?;
    let data: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("Data file '{}' is not valid JSON: {}", data_path, e))?;
    let (columns, rows) = parse_bulk_insert_data(&data)?;

    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await.map_err(|e| {
        format!(
            "{}\nhint: run `arni config list` to see available profiles",
            e
        )
    })?;

    if !json_mode {
        println!(
            "Inserting {} row(s) into '{}'...",
            rows.len().to_string().bright_white(),
            table.bright_cyan()
        );
    }

    let inserted = adapter
        .bulk_insert(&table, &columns, &rows, schema.as_deref())
        .await
        .map_err(|e| format!("bulk_insert failed: {}", e))?;

    tracing::info!(table = %table, rows = inserted, "bulk_insert completed");

    if json_mode {
        json_output::emit(&serde_json::json!({
            "ok": true,
            "table": table,
            "rows_affected": inserted,
        }));
    } else {
        println!(
            "{} Inserted {} row(s) into '{}'.",
            "✓".bright_green(),
            inserted.to_string().bright_white(),
            table.bright_cyan()
        );
    }
    Ok(())
}

// ─── Bulk-update command handler ──────────────────────────────────────────────

async fn handle_bulk_update_command(
    profile: String,
    table: String,
    filter: String,
    values: String,
    schema: Option<String>,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
    let filter_expr = parse_filter_json(&filter)?;

    let values_json: serde_json::Value =
        serde_json::from_str(&values).map_err(|e| format!("--values JSON parse error: {}", e))?;
    let values_obj = values_json
        .as_object()
        .ok_or("--values must be a JSON object: {\"col\": value, ...}")?;
    let mut col_values: HashMap<String, QueryValue> = HashMap::new();
    for (k, v) in values_obj {
        col_values.insert(k.clone(), json_to_query_value(v)?);
    }

    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await.map_err(|e| {
        format!(
            "{}\nhint: run `arni config list` to see available profiles",
            e
        )
    })?;

    if !json_mode {
        println!(
            "Updating rows in '{}' matching filter...",
            table.bright_cyan()
        );
    }

    let updated = adapter
        .bulk_update(&table, &[(col_values, filter_expr)], schema.as_deref())
        .await
        .map_err(|e| format!("bulk_update failed: {}", e))?;

    tracing::info!(table = %table, rows = updated, "bulk_update completed");

    if json_mode {
        json_output::emit(&serde_json::json!({
            "ok": true,
            "table": table,
            "rows_affected": updated,
        }));
    } else {
        println!(
            "{} Updated {} row(s) in '{}'.",
            "✓".bright_green(),
            updated.to_string().bright_white(),
            table.bright_cyan()
        );
    }
    Ok(())
}

// ─── Bulk-delete command handler ──────────────────────────────────────────────

async fn handle_bulk_delete_command(
    profile: String,
    table: String,
    filter: String,
    schema: Option<String>,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
    let filter_expr = parse_filter_json(&filter)?;

    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await.map_err(|e| {
        format!(
            "{}\nhint: run `arni config list` to see available profiles",
            e
        )
    })?;

    if !json_mode {
        println!(
            "Deleting rows from '{}' matching filter...",
            table.bright_cyan()
        );
    }

    let deleted = adapter
        .bulk_delete(&table, &[filter_expr], schema.as_deref())
        .await
        .map_err(|e| format!("bulk_delete failed: {}", e))?;

    tracing::info!(table = %table, rows = deleted, "bulk_delete completed");

    if json_mode {
        json_output::emit(&serde_json::json!({
            "ok": true,
            "table": table,
            "rows_affected": deleted,
        }));
    } else {
        println!(
            "{} Deleted {} row(s) from '{}'.",
            "✓".bright_green(),
            deleted.to_string().bright_white(),
            table.bright_cyan()
        );
    }
    Ok(())
}

// ─── Find-tables command handler ──────────────────────────────────────────────

async fn handle_find_tables_command(
    profile: String,
    pattern: String,
    mode: String,
    schema: Option<String>,
    json_mode: bool,
) -> Result<(), Box<dyn Error>> {
    let search_mode = match mode.to_lowercase().as_str() {
        "starts" | "starts-with" | "startswith" => TableSearchMode::StartsWith,
        "ends" | "ends-with" | "endswith" => TableSearchMode::EndsWith,
        _ => TableSearchMode::Contains,
    };

    let store = ConfigStore::load(None)?;
    let adapter = db::connect(&store, &profile).await.map_err(|e| {
        format!(
            "{}\nhint: run `arni config list` to see available profiles",
            e
        )
    })?;

    let results = adapter
        .metadata()
        .find_tables(&pattern, schema.as_deref(), search_mode.clone())
        .await
        .map_err(|e| format!("find_tables failed: {}", e))?;

    if json_mode {
        let mode_str = match search_mode {
            TableSearchMode::StartsWith => "starts",
            TableSearchMode::Contains => "contains",
            TableSearchMode::EndsWith => "ends",
        };
        json_output::emit(&serde_json::json!({
            "ok": true,
            "pattern": pattern,
            "mode": mode_str,
            "tables": results,
        }));
    } else {
        let mode_label = match search_mode {
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
    }
    Ok(())
}

// ─── Table formatting helpers ─────────────────────────────────────────────────

/// Render a [`QueryResult`] as a pretty UTF-8 table (no polars required).
#[cfg(not(feature = "polars"))]
fn qr_to_table(qr: &arni::QueryResult) -> String {
    let mut table = CTable::new();
    table.load_preset(presets::UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let headers: Vec<Cell> = qr
        .columns
        .iter()
        .map(|name| {
            Cell::new(name)
                .add_attribute(Attribute::Bold)
                .fg(Color::Cyan)
        })
        .collect();
    table.set_header(headers);

    for row in &qr.rows {
        let cells: Vec<String> = row.iter().map(|v| v.to_string()).collect();
        table.add_row(cells);
    }

    table.to_string()
}

/// Render a DataFrame as a pretty UTF-8 table using comfy-table.
#[cfg(feature = "polars")]
fn df_to_table(df: &DataFrame) -> String {
    let mut table = CTable::new();
    table.load_preset(presets::UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);

    // Cache column names once — used for both headers and each data row.
    let col_names = df.get_column_names();

    // Bold cyan headers.
    let headers: Vec<Cell> = col_names
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
        let cells: Vec<String> = col_names
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

/// Print a formatted describe-table view.
fn print_table_info(info: &arni::TableInfo) {
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
    use crate::filter::{json_to_query_value, parse_bulk_insert_data, parse_filter_json};
    use arni::adapter::{FilterExpr, QueryValue};

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
            pool_config: None,
        }
    }

    #[tokio::test]
    async fn test_connection_sqlite_memory() {
        let result = test_connection(&make_cfg(DatabaseType::SQLite, ":memory:"))
            .await
            .unwrap();
        assert!(result.contains("In-memory"));
    }

    #[tokio::test]
    async fn test_connection_duckdb_memory() {
        let result = test_connection(&make_cfg(DatabaseType::DuckDB, ":memory:"))
            .await
            .unwrap();
        assert!(result.contains("In-memory"));
    }

    #[tokio::test]
    async fn test_connection_sqlite_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        std::fs::write(&db_path, b"").unwrap();

        let result = test_connection(&make_cfg(DatabaseType::SQLite, db_path.to_str().unwrap()))
            .await
            .unwrap();
        assert!(result.contains("File exists"));
    }

    #[tokio::test]
    async fn test_connection_sqlite_missing_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("nonexistent.db");

        let err = test_connection(&make_cfg(DatabaseType::SQLite, db_path.to_str().unwrap()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Database file not found"));
    }

    #[tokio::test]
    async fn test_connection_duckdb_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        std::fs::write(&db_path, b"").unwrap();

        let result = test_connection(&make_cfg(DatabaseType::DuckDB, db_path.to_str().unwrap()))
            .await
            .unwrap();
        assert!(result.contains("File exists"));
    }

    // ── json_to_query_value ──────────────────────────────────────────────────

    #[test]
    fn test_json_to_query_value_null() {
        let v = json_to_query_value(&serde_json::Value::Null).unwrap();
        assert!(matches!(v, QueryValue::Null));
    }

    #[test]
    fn test_json_to_query_value_bool() {
        assert!(matches!(
            json_to_query_value(&serde_json::json!(true)).unwrap(),
            QueryValue::Bool(true)
        ));
        assert!(matches!(
            json_to_query_value(&serde_json::json!(false)).unwrap(),
            QueryValue::Bool(false)
        ));
    }

    #[test]
    fn test_json_to_query_value_int() {
        let v = json_to_query_value(&serde_json::json!(42)).unwrap();
        assert!(matches!(v, QueryValue::Int(42)));
    }

    #[test]
    fn test_json_to_query_value_float() {
        let v = json_to_query_value(&serde_json::json!(1.5)).unwrap();
        assert!(matches!(v, QueryValue::Float(f) if (f - 1.5f64).abs() < f64::EPSILON));
    }

    #[test]
    fn test_json_to_query_value_string() {
        let v = json_to_query_value(&serde_json::json!("hello")).unwrap();
        assert!(matches!(v, QueryValue::Text(s) if s == "hello"));
    }

    #[test]
    fn test_json_to_query_value_array_returns_error() {
        let result = json_to_query_value(&serde_json::json!([1, 2, 3]));
        assert!(result.is_err());
    }

    // ── parse_filter_json ────────────────────────────────────────────────────

    #[test]
    fn test_parse_filter_eq_int() {
        let f = parse_filter_json(r#"{"id": {"eq": 42}}"#).unwrap();
        assert!(matches!(f, FilterExpr::Eq(col, QueryValue::Int(42)) if col == "id"));
    }

    #[test]
    fn test_parse_filter_ne_string() {
        let f = parse_filter_json(r#"{"status": {"ne": "active"}}"#).unwrap();
        assert!(matches!(f, FilterExpr::Ne(col, QueryValue::Text(_)) if col == "status"));
    }

    #[test]
    fn test_parse_filter_gt_float() {
        let f = parse_filter_json(r#"{"score": {"gt": 80.5}}"#).unwrap();
        assert!(matches!(f, FilterExpr::Gt(col, QueryValue::Float(_)) if col == "score"));
    }

    #[test]
    fn test_parse_filter_lt() {
        let f = parse_filter_json(r#"{"age": {"lt": 30}}"#).unwrap();
        assert!(matches!(f, FilterExpr::Lt(col, QueryValue::Int(30)) if col == "age"));
    }

    #[test]
    fn test_parse_filter_gte_lte() {
        let f_gte = parse_filter_json(r#"{"x": {"gte": 1}}"#).unwrap();
        let f_lte = parse_filter_json(r#"{"x": {"lte": 10}}"#).unwrap();
        assert!(matches!(f_gte, FilterExpr::Gte(_, _)));
        assert!(matches!(f_lte, FilterExpr::Lte(_, _)));
    }

    #[test]
    fn test_parse_filter_in() {
        let f = parse_filter_json(r#"{"id": {"in": [1, 2, 3]}}"#).unwrap();
        assert!(matches!(f, FilterExpr::In(col, values) if col == "id" && values.len() == 3));
    }

    #[test]
    fn test_parse_filter_is_null() {
        let f = parse_filter_json(r#"{"deleted_at": "is_null"}"#).unwrap();
        assert!(matches!(f, FilterExpr::IsNull(col) if col == "deleted_at"));
    }

    #[test]
    fn test_parse_filter_is_not_null() {
        let f = parse_filter_json(r#"{"email": "is_not_null"}"#).unwrap();
        assert!(matches!(f, FilterExpr::IsNotNull(col) if col == "email"));
    }

    #[test]
    fn test_parse_filter_and() {
        let f = parse_filter_json(r#"{"and": [{"score": {"gte": 80}}, {"active": {"eq": true}}]}"#)
            .unwrap();
        assert!(matches!(f, FilterExpr::And(v) if v.len() == 2));
    }

    #[test]
    fn test_parse_filter_or() {
        let f = parse_filter_json(r#"{"or": [{"status": {"eq": "a"}}, {"status": {"eq": "b"}}]}"#)
            .unwrap();
        assert!(matches!(f, FilterExpr::Or(v) if v.len() == 2));
    }

    #[test]
    fn test_parse_filter_not() {
        let f = parse_filter_json(r#"{"not": {"active": {"eq": false}}}"#).unwrap();
        assert!(matches!(f, FilterExpr::Not(_)));
    }

    #[test]
    fn test_parse_filter_invalid_json_returns_error() {
        assert!(parse_filter_json("not json").is_err());
    }

    #[test]
    fn test_parse_filter_unknown_op_returns_error() {
        assert!(parse_filter_json(r#"{"col": {"between": [1, 5]}}"#).is_err());
    }

    // ── parse_bulk_insert_data ───────────────────────────────────────────────

    #[test]
    fn test_parse_bulk_insert_data_basic() {
        let data = serde_json::json!([
            {"name": "Alice", "score": 92},
            {"name": "Bob",   "score": 87},
        ]);
        let (cols, rows) = parse_bulk_insert_data(&data).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 2);
        assert!(cols.contains(&"name".to_string()));
        assert!(cols.contains(&"score".to_string()));
    }

    #[test]
    fn test_parse_bulk_insert_data_empty_array_returns_error() {
        let data = serde_json::json!([]);
        assert!(parse_bulk_insert_data(&data).is_err());
    }

    #[test]
    fn test_parse_bulk_insert_data_not_array_returns_error() {
        let data = serde_json::json!({"name": "Alice"});
        assert!(parse_bulk_insert_data(&data).is_err());
    }

    #[test]
    fn test_parse_bulk_insert_data_missing_column_uses_null() {
        let data = serde_json::json!([
            {"name": "Alice", "score": 92},
            {"name": "Bob"},
        ]);
        let (cols, rows) = parse_bulk_insert_data(&data).unwrap();
        let score_idx = cols.iter().position(|c| c == "score").unwrap();
        assert!(matches!(rows[1][score_idx], QueryValue::Null));
    }
}
