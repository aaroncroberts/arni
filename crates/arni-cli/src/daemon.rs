//! Unix socket IPC daemon for arni.
//!
//! The daemon listens on a Unix socket and processes newline-delimited JSON
//! command messages. Multiple clients can connect concurrently; all share a
//! single [`ConnectionRegistry`] so database connections are established at
//! most once per profile.
//!
//! # Protocol
//!
//! Each message is a JSON object terminated by `\n`. The server responds with
//! a JSON object followed by `\n`.
//!
//! ## Commands
//!
//! ### `connect`
//! ```json
//! {"cmd":"connect","profile":"my-db"}
//! ```
//! Response: `{"ok":true}` or `{"ok":false,"error":"..."}`.
//! Note: explicit `connect` is optional — `query` and `tables` connect lazily.
//!
//! ### `query`
//! ```json
//! {"cmd":"query","profile":"my-db","sql":"SELECT 1 AS n"}
//! ```
//! Response:
//! ```json
//! {"ok":true,"columns":["n"],"rows":[[1]]}
//! ```
//!
//! ### `tables`
//! ```json
//! {"cmd":"tables","profile":"my-db"}
//! ```
//! Response:
//! ```json
//! {"ok":true,"tables":["users","orders"]}
//! ```
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
//! Causes the daemon to stop accepting new connections and exit.
//! Response: `{"ok":true}`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use arni_data::{ConnectionRegistry, QueryValue, SharedAdapter};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::config::ConfigStore;
use crate::db::create_adapter;

// ─── Protocol types ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "lowercase")]
enum Command {
    Connect { profile: String },
    Disconnect { profile: String },
    Query { profile: String, sql: String },
    Tables { profile: String },
    Shutdown,
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
