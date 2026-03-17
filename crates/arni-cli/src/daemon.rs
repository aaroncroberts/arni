//! Unix socket IPC daemon for arni.
//!
//! The daemon listens on a Unix socket and processes newline-delimited JSON
//! command messages. Multiple clients can connect concurrently; all share a
//! single [`ConnectionRegistry`] so database connections are established at
//! most once per profile.
//!
//! # Protocol (v1.0)
//!
//! Each message is a JSON object terminated by `\n`. The server responds with
//! a JSON object followed by `\n`. All responses use `{"ok":true|false, ...}`.
//!
//! ## Core Commands
//!
//! ### `connect`
//! ```json
//! {"cmd":"connect","profile":"my-db"}
//! ```
//! Response: `{"ok":true}` or `{"ok":false,"error":"..."}`.
//! Note: explicit `connect` is optional — all other commands connect lazily.
//!
//! ### `query`
//! ```json
//! {"cmd":"query","profile":"my-db","sql":"SELECT 1 AS n"}
//! ```
//! Response: `{"ok":true,"columns":["n"],"rows":[[1]]}`
//!
//! ### `tables`
//! ```json
//! {"cmd":"tables","profile":"my-db"}
//! ```
//! Response: `{"ok":true,"tables":["users","orders"]}`
//!
//! ### `disconnect`
//! ```json
//! {"cmd":"disconnect","profile":"my-db"}
//! ```
//! Evicts `profile` from the registry. Response: `{"ok":true}`.
//!
//! ### `shutdown`
//! ```json
//! {"cmd":"shutdown"}
//! ```
//! Response: `{"ok":true}` then the daemon exits.
//!
//! ## Metadata Commands
//!
//! ### `describe_table`
//! ```json
//! {"cmd":"describe_table","profile":"my-db","table":"users","schema":null}
//! ```
//! Response: `{"ok":true,"table":"users","columns":[{"name":"id","data_type":"int4",...}],...}`
//!
//! ### `list_databases`
//! ```json
//! {"cmd":"list_databases","profile":"my-db"}
//! ```
//! Response: `{"ok":true,"databases":["public","myschema"]}`
//!
//! ### `get_indexes`
//! ```json
//! {"cmd":"get_indexes","profile":"my-db","table":"users","schema":null}
//! ```
//! Response: `{"ok":true,"table":"users","indexes":[{"name":"users_pkey","columns":["id"],...}]}`
//!
//! ### `get_foreign_keys`
//! ```json
//! {"cmd":"get_foreign_keys","profile":"my-db","table":"orders","schema":null}
//! ```
//! Response: `{"ok":true,"table":"orders","foreign_keys":[...]}`
//!
//! ### `get_views`
//! ```json
//! {"cmd":"get_views","profile":"my-db","schema":null}
//! ```
//! Response: `{"ok":true,"views":[{"name":"active_users","schema":null,"definition":"..."}]}`
//!
//! ### `get_server_info`
//! ```json
//! {"cmd":"get_server_info","profile":"my-db"}
//! ```
//! Response: `{"ok":true,"server":{"version":"16.1","server_type":"PostgreSQL","extra_info":{}}}`
//!
//! ### `list_stored_procedures`
//! ```json
//! {"cmd":"list_stored_procedures","profile":"my-db","schema":null}
//! ```
//! Response: `{"ok":true,"procedures":[{"name":"my_proc","return_type":"void",...}]}`
//!
//! ### `find_tables`
//! ```json
//! {"cmd":"find_tables","profile":"my-db","pattern":"user","mode":"contains","schema":null}
//! ```
//! Mode: `"contains"` (default), `"starts"`, `"ends"`.
//! Response: `{"ok":true,"pattern":"user","mode":"contains","tables":["users","user_roles"]}`
//!
//! ## Bulk Operation Commands
//!
//! ### `bulk_insert`
//! ```json
//! {"cmd":"bulk_insert","profile":"my-db","table":"users","columns":["name","age"],"rows":[["Alice",30]],"schema":null}
//! ```
//! Response: `{"ok":true,"rows_affected":1}`
//!
//! ### `bulk_update`
//! ```json
//! {"cmd":"bulk_update","profile":"my-db","table":"users","filter":{"id":{"eq":1}},"values":{"name":"Bob"},"schema":null}
//! ```
//! Response: `{"ok":true,"rows_affected":1}`
//!
//! ### `bulk_delete`
//! ```json
//! {"cmd":"bulk_delete","profile":"my-db","table":"users","filter":{"active":{"eq":false}},"schema":null}
//! ```
//! Response: `{"ok":true,"rows_affected":3}`
//!
//! ## Utility Commands
//!
//! ### `version`
//! ```json
//! {"cmd":"version"}
//! ```
//! Response: `{"ok":true,"protocol":"1.0","arni_version":"0.1.0"}`

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arni_data::adapter::{
    ForeignKeyInfo, IndexInfo, ProcedureInfo, ServerInfo, TableSearchMode, ViewInfo,
};
use arni_data::{ConnectionRegistry, QueryValue, SharedAdapter};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::config::ConfigStore;
use crate::db::create_adapter;
use crate::filter::{json_to_query_value, parse_filter_value};

// ─── Protocol types ───────────────────────────────────────────────────────────

const ARNI_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum Command {
    // ── Core ──────────────────────────────────────────────────────────────────
    Connect { profile: String },
    Disconnect { profile: String },
    Query { profile: String, sql: String },
    Tables { profile: String },
    Shutdown,
    Version,
    // ── Metadata ──────────────────────────────────────────────────────────────
    DescribeTable {
        profile: String,
        table: String,
        schema: Option<String>,
    },
    ListDatabases {
        profile: String,
    },
    GetIndexes {
        profile: String,
        table: String,
        schema: Option<String>,
    },
    GetForeignKeys {
        profile: String,
        table: String,
        schema: Option<String>,
    },
    GetViews {
        profile: String,
        schema: Option<String>,
    },
    GetServerInfo {
        profile: String,
    },
    ListStoredProcedures {
        profile: String,
        schema: Option<String>,
    },
    FindTables {
        profile: String,
        pattern: String,
        /// "contains" (default), "starts", "ends"
        mode: Option<String>,
        schema: Option<String>,
    },
    // ── Bulk ops ──────────────────────────────────────────────────────────────
    BulkInsert {
        profile: String,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
        schema: Option<String>,
    },
    BulkUpdate {
        profile: String,
        table: String,
        filter: serde_json::Value,
        values: serde_json::Value,
        schema: Option<String>,
    },
    BulkDelete {
        profile: String,
        table: String,
        filter: serde_json::Value,
        schema: Option<String>,
    },
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Response {
    Ok {
        ok: bool,
    },
    Error {
        ok: bool,
        error: String,
    },
    QueryOk {
        ok: bool,
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    TablesOk {
        ok: bool,
        tables: Vec<String>,
    },
    DescribeTableOk {
        ok: bool,
        table: String,
        columns: Vec<serde_json::Value>,
        row_count: Option<i64>,
        size_bytes: Option<i64>,
        created_at: Option<String>,
    },
    DatabasesOk {
        ok: bool,
        databases: Vec<String>,
    },
    IndexesOk {
        ok: bool,
        table: String,
        indexes: Vec<IndexInfo>,
    },
    ForeignKeysOk {
        ok: bool,
        table: String,
        foreign_keys: Vec<ForeignKeyInfo>,
    },
    ViewsOk {
        ok: bool,
        views: Vec<ViewInfo>,
    },
    ServerInfoOk {
        ok: bool,
        server: ServerInfo,
    },
    ProceduresOk {
        ok: bool,
        procedures: Vec<ProcedureInfo>,
    },
    FindTablesOk {
        ok: bool,
        pattern: String,
        mode: String,
        tables: Vec<String>,
    },
    RowsAffectedOk {
        ok: bool,
        rows_affected: u64,
    },
    VersionOk {
        ok: bool,
        protocol: String,
        arni_version: String,
    },
}

impl Response {
    fn ok() -> Self {
        Self::Ok { ok: true }
    }
    fn err(msg: impl Into<String>) -> Self {
        Self::Error {
            ok: false,
            error: msg.into(),
        }
    }
}

// ─── Daemon entry point ───────────────────────────────────────────────────────

/// Start the daemon, listen on `socket_path`, and block until shutdown.
pub async fn run_daemon(socket_path: PathBuf) -> Result<()> {
    // Remove stale socket file
    let _ = tokio::fs::remove_file(&socket_path).await;

    let listener = UnixListener::bind(&socket_path)
        .map_err(|e| anyhow::anyhow!("Failed to bind to {:?}: {}", socket_path, e))?;

    // Print socket path so callers know where to connect
    println!("{}", socket_path.display());
    info!(socket = %socket_path.display(), "Daemon listening");

    let registry = Arc::new(ConnectionRegistry::new());
    let store = Arc::new(ConfigStore::load(None)?);

    // Broadcast channel for graceful shutdown
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // SIGTERM / SIGINT handler
    let shutdown_tx_signal = shutdown_tx.clone();
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received — shutting down"),
            _ = sigint.recv() => info!("SIGINT received — shutting down"),
        }
        let _ = shutdown_tx_signal.send(());
    });

    let mut shutdown_rx = shutdown_tx.subscribe();

    loop {
        tokio::select! {
            // Accept a new client connection
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => {
                        let registry = registry.clone();
                        let store = store.clone();
                        let tx = shutdown_tx.clone();
                        let mut rx = shutdown_tx.subscribe();
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = handle_client(stream, registry, store, tx) => {},
                                _ = rx.recv() => debug!("Client handler cancelled by shutdown"),
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Accept error");
                    }
                }
            }
            // Shutdown signal
            _ = shutdown_rx.recv() => {
                info!("Daemon shutting down");
                break;
            }
        }
    }

    // Clean up socket file
    let _ = tokio::fs::remove_file(&socket_path).await;
    Ok(())
}

// ─── Per-connection handler ───────────────────────────────────────────────────

async fn handle_client(
    stream: UnixStream,
    registry: Arc<ConnectionRegistry>,
    store: Arc<ConfigStore>,
    shutdown_tx: broadcast::Sender<()>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        debug!(line = %line, "Received command");

        let (response, should_shutdown) = match serde_json::from_str::<Command>(&line) {
            Ok(cmd) => dispatch_command(cmd, &registry, &store).await,
            Err(e) => (Response::err(format!("Invalid command: {}", e)), false),
        };

        let mut json = serde_json::to_string(&response)
            .unwrap_or_else(|_| r#"{"ok":false,"error":"serialization error"}"#.to_string());
        json.push('\n');

        if let Err(e) = writer.write_all(json.as_bytes()).await {
            warn!(error = %e, "Failed to write response");
            break;
        }

        if should_shutdown {
            let _ = shutdown_tx.send(());
            break;
        }
    }
}

// ─── Command dispatch ─────────────────────────────────────────────────────────

/// Returns `(response, should_shutdown)`.
async fn dispatch_command(
    cmd: Command,
    registry: &Arc<ConnectionRegistry>,
    store: &Arc<ConfigStore>,
) -> (Response, bool) {
    match cmd {
        Command::Connect { profile } => {
            let resp = match get_or_connect(registry, store, &profile).await {
                Ok(_) => Response::ok(),
                Err(e) => Response::err(e.to_string()),
            };
            (resp, false)
        }

        Command::Disconnect { profile } => {
            registry.evict(&profile);
            (Response::ok(), false)
        }

        Command::Query { profile, sql } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let resp = match adapter.execute_query(&sql).await {
                Ok(result) => {
                    let rows: Vec<Vec<serde_json::Value>> = result
                        .rows
                        .iter()
                        .map(|row| row.iter().map(query_value_to_json).collect())
                        .collect();
                    Response::QueryOk {
                        ok: true,
                        columns: result.columns,
                        rows,
                    }
                }
                Err(e) => Response::err(e.to_string()),
            };
            (resp, false)
        }

        Command::Tables { profile } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let resp = match adapter.list_tables(None).await {
                Ok(tables) => Response::TablesOk { ok: true, tables },
                Err(e) => Response::err(e.to_string()),
            };
            (resp, false)
        }

        Command::Shutdown => (Response::ok(), true),

        Command::Version => (
            Response::VersionOk {
                ok: true,
                protocol: "1.0".to_string(),
                arni_version: ARNI_VERSION.to_string(),
            },
            false,
        ),

        // ── Metadata commands ─────────────────────────────────────────────────

        Command::DescribeTable { profile, table, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.describe_table(&table, schema.as_deref()).await {
                Ok(info) => {
                    let cols: Vec<serde_json::Value> = info
                        .columns
                        .iter()
                        .map(|c| serde_json::to_value(c).unwrap_or(serde_json::Value::Null))
                        .collect();
                    Response::DescribeTableOk {
                        ok: true,
                        table: info.name,
                        columns: cols,
                        row_count: info.row_count,
                        size_bytes: info.size_bytes,
                        created_at: info.created_at,
                    }
                }
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "describe_table", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::ListDatabases { profile } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.list_databases().await {
                Ok(databases) => Response::DatabasesOk { ok: true, databases },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "list_databases", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::GetIndexes { profile, table, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.get_indexes(&table, schema.as_deref()).await {
                Ok(indexes) => Response::IndexesOk { ok: true, table, indexes },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "get_indexes", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::GetForeignKeys { profile, table, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.get_foreign_keys(&table, schema.as_deref()).await {
                Ok(foreign_keys) => Response::ForeignKeysOk { ok: true, table, foreign_keys },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "get_foreign_keys", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::GetViews { profile, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.get_views(schema.as_deref()).await {
                Ok(views) => Response::ViewsOk { ok: true, views },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "get_views", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::GetServerInfo { profile } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.get_server_info().await {
                Ok(server) => Response::ServerInfoOk { ok: true, server },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "get_server_info", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::ListStoredProcedures { profile, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.list_stored_procedures(schema.as_deref()).await {
                Ok(procedures) => Response::ProceduresOk { ok: true, procedures },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "list_stored_procedures", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::FindTables { profile, pattern, mode, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            let mode_str = mode.as_deref().unwrap_or("contains").to_string();
            let search_mode = match mode_str.as_str() {
                "starts" => TableSearchMode::StartsWith,
                "ends" => TableSearchMode::EndsWith,
                _ => TableSearchMode::Contains,
            };
            let t0 = Instant::now();
            let resp = match adapter.find_tables(&pattern, schema.as_deref(), search_mode).await {
                Ok(tables) => Response::FindTablesOk {
                    ok: true,
                    pattern,
                    mode: mode_str,
                    tables,
                },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "find_tables", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        // ── Bulk operation commands ───────────────────────────────────────────

        Command::BulkInsert { profile, table, columns, rows, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            // Convert Vec<Vec<serde_json::Value>> → Vec<Vec<QueryValue>>
            // Use String errors so the future remains Send across awaits.
            let converted: Result<Vec<Vec<QueryValue>>, String> = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|v| json_to_query_value(v).map_err(|e| e.to_string()))
                        .collect::<Result<Vec<QueryValue>, String>>()
                })
                .collect();
            let qv_rows = match converted {
                Ok(r) => r,
                Err(e) => return (Response::err(format!("Row conversion error: {}", e)), false),
            };
            let t0 = Instant::now();
            let resp = match adapter.bulk_insert(&table, &columns, &qv_rows, schema.as_deref()).await {
                Ok(rows_affected) => Response::RowsAffectedOk { ok: true, rows_affected },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "bulk_insert", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::BulkUpdate { profile, table, filter, values, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            // Map to String so the future stays Send across awaits.
            let filter_expr = match parse_filter_value(&filter).map_err(|e| e.to_string()) {
                Ok(f) => f,
                Err(e) => return (Response::err(format!("Filter error: {}", e)), false),
            };
            // `values` must be a JSON object mapping column name → value
            let obj = match values.as_object() {
                Some(o) => o,
                None => return (Response::err("BulkUpdate 'values' must be a JSON object".to_string()), false),
            };
            let col_values: Result<HashMap<String, QueryValue>, String> = obj
                .iter()
                .map(|(k, v)| {
                    json_to_query_value(v)
                        .map_err(|e| e.to_string())
                        .map(|qv| (k.clone(), qv))
                })
                .collect();
            let col_values = match col_values {
                Ok(m) => m,
                Err(e) => return (Response::err(format!("Values error: {}", e)), false),
            };
            let updates = [(col_values, filter_expr)];
            let t0 = Instant::now();
            let resp = match adapter.bulk_update(&table, &updates, schema.as_deref()).await {
                Ok(rows_affected) => Response::RowsAffectedOk { ok: true, rows_affected },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "bulk_update", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }

        Command::BulkDelete { profile, table, filter, schema } => {
            let adapter = match get_or_connect(registry, store, &profile).await {
                Ok(a) => a,
                Err(e) => return (Response::err(e.to_string()), false),
            };
            // Map to String so the future stays Send across awaits.
            let filter_expr = match parse_filter_value(&filter).map_err(|e| e.to_string()) {
                Ok(f) => f,
                Err(e) => return (Response::err(format!("Filter error: {}", e)), false),
            };
            let filters = [filter_expr];
            let t0 = Instant::now();
            let resp = match adapter.bulk_delete(&table, &filters, schema.as_deref()).await {
                Ok(rows_affected) => Response::RowsAffectedOk { ok: true, rows_affected },
                Err(e) => Response::err(e.to_string()),
            };
            info!(cmd = "bulk_delete", profile, duration_ms = t0.elapsed().as_millis() as u64);
            (resp, false)
        }
    }
}

// ─── Helper: connect via registry ─────────────────────────────────────────────

async fn get_or_connect(
    registry: &Arc<ConnectionRegistry>,
    store: &Arc<ConfigStore>,
    profile: &str,
) -> Result<SharedAdapter> {
    let store = store.clone();
    let profile_owned = profile.to_string();

    registry
        .get_or_connect(profile, move || {
            let store = store.clone();
            let profile = profile_owned.clone();
            async move {
                let config = store
                    .get(&profile)
                    .map_err(|e| arni_data::DataError::Config(e.to_string()))?;
                let password = config.parameters.get("password").cloned();
                let mut adapter = create_adapter(config.clone())
                    .map_err(|e| arni_data::DataError::Config(e.to_string()))?;
                adapter
                    .connect(&config, password.as_deref())
                    .await
                    .map_err(|e| arni_data::DataError::Connection(e.to_string()))?;
                Ok(Arc::from(adapter) as SharedAdapter)
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
}

// ─── QueryValue → serde_json::Value ──────────────────────────────────────────

fn query_value_to_json(v: &QueryValue) -> serde_json::Value {
    match v {
        QueryValue::Null => serde_json::Value::Null,
        QueryValue::Bool(b) => serde_json::Value::Bool(*b),
        QueryValue::Int(i) => serde_json::Value::Number((*i).into()),
        QueryValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        QueryValue::Text(s) => serde_json::Value::String(s.clone()),
        // Encode binary data as a JSON array of byte values
        QueryValue::Bytes(b) => {
            serde_json::Value::Array(b.iter().map(|byte| serde_json::json!(byte)).collect())
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    async fn send_recv(stream: &mut UnixStream, msg: &str) -> String {
        stream
            .write_all(format!("{}\n", msg).as_bytes())
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).trim_end().to_string()
    }

    #[tokio::test]
    async fn daemon_bad_command_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");
        let sock2 = sock.clone();

        let registry = Arc::new(ConnectionRegistry::new());
        // Use a temp dir for the config so the test doesn't depend on ~/.arni
        let cfg_dir = dir.path().join("config");
        let store = Arc::new(ConfigStore::load(Some(&cfg_dir)).unwrap());
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let shutdown_tx2 = shutdown_tx.clone();

        tokio::spawn(async move {
            let _ = tokio::fs::remove_file(&sock2).await;
            let listener = UnixListener::bind(&sock2).unwrap();
            if let Ok((stream, _)) = listener.accept().await {
                handle_client(stream, registry, store, shutdown_tx2).await;
            }
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut client = UnixStream::connect(&sock).await.unwrap();
        let resp = send_recv(&mut client, r#"{"cmd":"unknown"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["ok"], false);
    }

    #[tokio::test]
    async fn command_deserialization() {
        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"query","profile":"p","sql":"SELECT 1"}"#).unwrap();
        assert!(matches!(cmd, Command::Query { .. }));

        let cmd: Command = serde_json::from_str(r#"{"cmd":"tables","profile":"p"}"#).unwrap();
        assert!(matches!(cmd, Command::Tables { .. }));

        let cmd: Command = serde_json::from_str(r#"{"cmd":"shutdown"}"#).unwrap();
        assert!(matches!(cmd, Command::Shutdown));
    }

    #[test]
    fn command_deserialization_new_variants() {
        // Metadata commands
        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"describe_table","profile":"p","table":"users","schema":null}"#)
                .unwrap();
        assert!(matches!(cmd, Command::DescribeTable { .. }));

        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"list_databases","profile":"p"}"#).unwrap();
        assert!(matches!(cmd, Command::ListDatabases { .. }));

        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"get_indexes","profile":"p","table":"users","schema":null}"#)
                .unwrap();
        assert!(matches!(cmd, Command::GetIndexes { .. }));

        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"get_foreign_keys","profile":"p","table":"orders","schema":null}"#)
                .unwrap();
        assert!(matches!(cmd, Command::GetForeignKeys { .. }));

        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"get_views","profile":"p","schema":null}"#).unwrap();
        assert!(matches!(cmd, Command::GetViews { .. }));

        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"get_server_info","profile":"p"}"#).unwrap();
        assert!(matches!(cmd, Command::GetServerInfo { .. }));

        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"list_stored_procedures","profile":"p","schema":null}"#)
                .unwrap();
        assert!(matches!(cmd, Command::ListStoredProcedures { .. }));

        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"find_tables","profile":"p","pattern":"user","mode":"contains","schema":null}"#)
                .unwrap();
        assert!(matches!(cmd, Command::FindTables { .. }));

        // Bulk operation commands
        let cmd: Command = serde_json::from_str(
            r#"{"cmd":"bulk_insert","profile":"p","table":"users","columns":["name"],"rows":[["Alice"]],"schema":null}"#,
        )
        .unwrap();
        assert!(matches!(cmd, Command::BulkInsert { .. }));

        let cmd: Command = serde_json::from_str(
            r#"{"cmd":"bulk_update","profile":"p","table":"users","filter":{"id":{"eq":1}},"values":{"name":"Bob"},"schema":null}"#,
        )
        .unwrap();
        assert!(matches!(cmd, Command::BulkUpdate { .. }));

        let cmd: Command = serde_json::from_str(
            r#"{"cmd":"bulk_delete","profile":"p","table":"users","filter":{"active":{"eq":false}},"schema":null}"#,
        )
        .unwrap();
        assert!(matches!(cmd, Command::BulkDelete { .. }));

        // Version command
        let cmd: Command = serde_json::from_str(r#"{"cmd":"version"}"#).unwrap();
        assert!(matches!(cmd, Command::Version));
    }

    #[tokio::test]
    async fn version_command_returns_protocol_and_version() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("ver.sock");
        let sock2 = sock.clone();

        let registry = Arc::new(ConnectionRegistry::new());
        let cfg_dir = dir.path().join("config");
        let store = Arc::new(ConfigStore::load(Some(&cfg_dir)).unwrap());
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let shutdown_tx2 = shutdown_tx.clone();

        tokio::spawn(async move {
            let _ = tokio::fs::remove_file(&sock2).await;
            let listener = UnixListener::bind(&sock2).unwrap();
            if let Ok((stream, _)) = listener.accept().await {
                handle_client(stream, registry, store, shutdown_tx2).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut client = UnixStream::connect(&sock).await.unwrap();
        let resp = send_recv(&mut client, r#"{"cmd":"version"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["protocol"], "1.0");
        assert!(v["arni_version"].is_string());
    }

    #[test]
    fn query_value_json_roundtrip() {
        assert_eq!(
            query_value_to_json(&QueryValue::Null),
            serde_json::Value::Null
        );
        assert_eq!(
            query_value_to_json(&QueryValue::Bool(true)),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            query_value_to_json(&QueryValue::Int(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            query_value_to_json(&QueryValue::Text("hi".into())),
            serde_json::json!("hi")
        );
    }
}
