//! Adapter factory and connection helpers for arni-cli.
//!
//! Bridges the CLI's connection profile system to the concrete adapter
//! implementations in `arni-data`. All adapter coupling lives here, keeping
//! `main.rs` free of per-database imports.

use anyhow::{anyhow, Result};
use arni::{ConnectionConfig, DatabaseType, DbAdapter, SharedAdapter};
use std::io::IsTerminal;

use crate::config::ConfigStore;

// ─── Adapter factory ──────────────────────────────────────────────────────────

/// Instantiate the concrete adapter matching `config.db_type`.
///
/// The adapter is created but **not yet connected**. Call `adapter.connect()`
/// afterwards to establish the database connection.
///
/// Returns `Box<dyn DbAdapter + Send + Sync + 'static>` so callers can wrap
/// the result in `Arc::from(adapter)` to obtain a [`SharedAdapter`].
pub fn create_adapter(
    config: ConnectionConfig,
) -> Result<Box<dyn DbAdapter + Send + Sync + 'static>> {
    let adapter: Box<dyn DbAdapter + Send + Sync + 'static> = match config.db_type {
        DatabaseType::Postgres => Box::new(arni::adapters::postgres::PostgresAdapter::new(config)),
        DatabaseType::MySQL => Box::new(arni::adapters::mysql::MySqlAdapter::new(config)),
        DatabaseType::SQLite => Box::new(arni::adapters::sqlite::SqliteAdapter::new(config)),
        DatabaseType::MongoDB => Box::new(arni::adapters::mongodb::MongoDbAdapter::new(config)),
        DatabaseType::SQLServer => Box::new(arni::adapters::mssql::SqlServerAdapter::new(config)),
        DatabaseType::Oracle => Box::new(arni::adapters::oracle::OracleAdapter::new(config)),
        DatabaseType::DuckDB => Box::new(arni::adapters::duckdb::DuckDbAdapter::new(config)),
    };
    Ok(adapter)
}

// ─── Connection helper ────────────────────────────────────────────────────────

/// Returns `true` for database types that require user credentials.
fn needs_auth(db_type: &DatabaseType) -> bool {
    !matches!(db_type, DatabaseType::SQLite | DatabaseType::DuckDB)
}

/// Load a named connection profile, obtain a password (stored or prompted),
/// create the matching adapter, connect it, and return a [`SharedAdapter`].
///
/// # Password resolution
/// 1. If `parameters["password"]` was injected by [`ConfigStore::get`] → use it.
/// 2. If no password is stored and the database type requires auth → prompt
///    (only when stdin is a TTY; returns a clear error otherwise).
/// 3. SQLite and DuckDB never need a password → skip prompting.
pub async fn connect(store: &ConfigStore, profile: &str) -> Result<SharedAdapter> {
    let config = store.get(profile).map_err(|e| anyhow!("{}", e))?;

    let password = match config.parameters.get("password") {
        Some(pw) if !pw.is_empty() => Some(pw.clone()),
        _ if needs_auth(&config.db_type) => {
            if !IsTerminal::is_terminal(&std::io::stdin()) {
                return Err(anyhow!(
                    "stdin is not a terminal; provide password via 'arni config add --param password=…'"
                ));
            }
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

    Ok(std::sync::Arc::from(adapter))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(db_type: DatabaseType) -> ConnectionConfig {
        ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type,
            host: Some("localhost".into()),
            port: None,
            database: "testdb".into(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        }
    }

    // ── needs_auth ────────────────────────────────────────────────────────────

    #[test]
    fn test_needs_auth_sqlite_false() {
        assert!(!needs_auth(&DatabaseType::SQLite));
    }

    #[test]
    fn test_needs_auth_duckdb_false() {
        assert!(!needs_auth(&DatabaseType::DuckDB));
    }

    #[test]
    fn test_needs_auth_postgres_true() {
        assert!(needs_auth(&DatabaseType::Postgres));
    }

    #[test]
    fn test_needs_auth_mysql_true() {
        assert!(needs_auth(&DatabaseType::MySQL));
    }

    #[test]
    fn test_needs_auth_mongodb_true() {
        assert!(needs_auth(&DatabaseType::MongoDB));
    }

    #[test]
    fn test_needs_auth_sqlserver_true() {
        assert!(needs_auth(&DatabaseType::SQLServer));
    }

    #[test]
    fn test_needs_auth_oracle_true() {
        assert!(needs_auth(&DatabaseType::Oracle));
    }

    // ── create_adapter dispatch ───────────────────────────────────────────────

    #[test]
    fn test_create_adapter_postgres() {
        let adapter = create_adapter(make_config(DatabaseType::Postgres));
        assert!(adapter.is_ok());
        assert_eq!(adapter.unwrap().database_type(), DatabaseType::Postgres);
    }

    #[test]
    fn test_create_adapter_mysql() {
        let adapter = create_adapter(make_config(DatabaseType::MySQL));
        assert!(adapter.is_ok());
        assert_eq!(adapter.unwrap().database_type(), DatabaseType::MySQL);
    }

    #[test]
    fn test_create_adapter_sqlite() {
        let adapter = create_adapter(make_config(DatabaseType::SQLite));
        assert!(adapter.is_ok());
        assert_eq!(adapter.unwrap().database_type(), DatabaseType::SQLite);
    }

    #[test]
    fn test_create_adapter_mongodb() {
        let adapter = create_adapter(make_config(DatabaseType::MongoDB));
        assert!(adapter.is_ok());
        assert_eq!(adapter.unwrap().database_type(), DatabaseType::MongoDB);
    }

    #[test]
    fn test_create_adapter_sqlserver() {
        let adapter = create_adapter(make_config(DatabaseType::SQLServer));
        assert!(adapter.is_ok());
        assert_eq!(adapter.unwrap().database_type(), DatabaseType::SQLServer);
    }

    #[test]
    fn test_create_adapter_oracle() {
        let adapter = create_adapter(make_config(DatabaseType::Oracle));
        assert!(adapter.is_ok());
        assert_eq!(adapter.unwrap().database_type(), DatabaseType::Oracle);
    }

    #[test]
    fn test_create_adapter_duckdb() {
        let adapter = create_adapter(make_config(DatabaseType::DuckDB));
        assert!(adapter.is_ok());
        assert_eq!(adapter.unwrap().database_type(), DatabaseType::DuckDB);
    }

    // ── password resolution (logic-only, no live connections) ─────────────────

    /// Stored password in parameters is extracted without prompting.
    #[test]
    fn test_stored_password_in_parameters() {
        let mut config = make_config(DatabaseType::Postgres);
        config.parameters.insert("password".into(), "s3cr3t".into());
        // Verify the lookup logic directly
        let pw = config.parameters.get("password").cloned();
        assert_eq!(pw.as_deref(), Some("s3cr3t"));
    }

    /// Empty stored password is treated as absent.
    #[test]
    fn test_empty_stored_password_treated_as_absent() {
        let mut config = make_config(DatabaseType::Postgres);
        config.parameters.insert("password".into(), String::new());
        // Match arm `Some(pw) if !pw.is_empty()` must NOT match
        let matched = matches!(config.parameters.get("password"), Some(pw) if !pw.is_empty());
        assert!(
            !matched,
            "empty password should not match the stored-pw arm"
        );
    }

    /// SQLite/DuckDB adapters never require auth.
    #[test]
    fn test_file_dbs_skip_auth() {
        for db_type in [DatabaseType::SQLite, DatabaseType::DuckDB] {
            assert!(
                !needs_auth(&db_type),
                "{:?} should not require auth",
                db_type
            );
        }
    }
}
