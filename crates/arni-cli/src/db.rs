//! Adapter factory and connection helpers for arni-cli.
//!
//! Bridges the CLI's connection profile system to the concrete adapter
//! implementations in `arni-data`. All adapter coupling lives here, keeping
//! `main.rs` free of per-database imports.

use anyhow::{anyhow, Result};
use arni_data::{ConnectionConfig, DatabaseType, DbAdapter};

use crate::config::ConfigStore;

// ─── Adapter factory ──────────────────────────────────────────────────────────

/// Instantiate the concrete adapter matching `config.db_type`.
///
/// The adapter is created but **not yet connected**. Call `adapter.connect()`
/// afterwards to establish the database connection.
pub fn create_adapter(config: ConnectionConfig) -> Result<Box<dyn DbAdapter>> {
    let adapter: Box<dyn DbAdapter> = match config.db_type {
        DatabaseType::Postgres => {
            Box::new(arni_data::adapters::postgres::PostgresAdapter::new(config))
        }
        DatabaseType::MySQL => Box::new(arni_data::adapters::mysql::MySqlAdapter::new(config)),
        DatabaseType::SQLite => Box::new(arni_data::adapters::sqlite::SqliteAdapter::new(config)),
        DatabaseType::MongoDB => {
            Box::new(arni_data::adapters::mongodb::MongoDbAdapter::new(config))
        }
        DatabaseType::SQLServer => {
            Box::new(arni_data::adapters::mssql::SqlServerAdapter::new(config))
        }
        DatabaseType::Oracle => Box::new(arni_data::adapters::oracle::OracleAdapter::new(config)),
        DatabaseType::DuckDB => Box::new(arni_data::adapters::duckdb::DuckDbAdapter::new(config)),
    };
    Ok(adapter)
}

// ─── Connection helper ────────────────────────────────────────────────────────

/// Returns `true` for database types that require user credentials.
fn needs_auth(db_type: &DatabaseType) -> bool {
    !matches!(db_type, DatabaseType::SQLite | DatabaseType::DuckDB)
}

/// Load a named connection profile, obtain a password (stored or prompted),
/// create the matching adapter, and connect it.
///
/// # Password resolution
/// 1. If `parameters["password"]` was injected by [`ConfigStore::get`] → use it.
/// 2. If no password is stored and the database type requires auth → prompt.
/// 3. SQLite and DuckDB never need a password → skip prompting.
pub async fn connect(store: &ConfigStore, profile: &str) -> Result<Box<dyn DbAdapter>> {
    let config = store.get(profile).map_err(|e| anyhow!("{}", e))?;

    let password = match config.parameters.get("password") {
        Some(pw) if !pw.is_empty() => Some(pw.clone()),
        _ if needs_auth(&config.db_type) => {
            let pw = rpassword::prompt_password(format!("Password for '{}': ", profile))?;
            if pw.is_empty() {
                None
            } else {
                Some(pw)
            }
        }
        _ => None,
    };

    let mut adapter = create_adapter(config.clone())?;
    adapter
        .connect(&config, password.as_deref())
        .await
        .map_err(|e| anyhow!("Failed to connect to '{}': {}", profile, e))?;

    Ok(adapter)
}
