//! Adapter factory for arni-mcp.
//!
//! Mirrors `arni-cli`'s `db.rs` but omits TTY password prompting — in an MCP
//! server there is no interactive terminal. Passwords must be stored in the
//! connection config via `parameters["password"]` or injected through
//! environment variables before the server starts.

use anyhow::{anyhow, Result};
use arni::{ConnectionConfig, DatabaseType, DbAdapter, SharedAdapter};

/// Instantiate the concrete adapter matching `config.db_type`.
///
/// The adapter is **not yet connected**. Call `adapter.connect()` afterwards.
pub fn create_adapter(
    config: ConnectionConfig,
) -> Result<Box<dyn DbAdapter + Send + Sync + 'static>> {
    #[allow(unreachable_patterns)]
    let adapter: Box<dyn DbAdapter + Send + Sync + 'static> = match config.db_type {
        #[cfg(feature = "postgres")]
        DatabaseType::Postgres => Box::new(arni::adapters::postgres::PostgresAdapter::new(config)),
        #[cfg(feature = "mysql")]
        DatabaseType::MySQL => Box::new(arni::adapters::mysql::MySqlAdapter::new(config)),
        #[cfg(feature = "sqlite")]
        DatabaseType::SQLite => Box::new(arni::adapters::sqlite::SqliteAdapter::new(config)),
        #[cfg(feature = "mongodb")]
        DatabaseType::MongoDB => Box::new(arni::adapters::mongodb::MongoDbAdapter::new(config)),
        #[cfg(feature = "mssql")]
        DatabaseType::SQLServer => Box::new(arni::adapters::mssql::SqlServerAdapter::new(config)),
        #[cfg(feature = "oracle")]
        DatabaseType::Oracle => Box::new(arni::adapters::oracle::OracleAdapter::new(config)),
        #[cfg(feature = "duckdb")]
        DatabaseType::DuckDB => Box::new(arni::adapters::duckdb::DuckDbAdapter::new(config)),
        db_type => {
            return Err(anyhow!(
                "Database type {:?} is not compiled in. \
                 Rebuild with the appropriate feature flag, e.g.: \
                 cargo build -p arni-mcp --features {:?}",
                db_type,
                db_type
            ))
        }
    };
    Ok(adapter)
}

/// Connect to a named profile from the given `ArniConfig`.
///
/// The profile name maps to a top-level key in `config.profiles`. The first
/// connection entry in that profile is used. The password (if any) must be
/// stored in `parameters["password"]`.
pub async fn connect_profile(config: &arni::ArniConfig, profile: &str) -> Result<SharedAdapter> {
    let conn_config = config
        .profiles
        .get(profile)
        .and_then(|p| p.connections.first())
        .ok_or_else(|| anyhow!("Profile '{}' not found in configuration", profile))?
        .clone();

    let password = conn_config
        .parameters
        .get("password")
        .filter(|pw| !pw.is_empty())
        .cloned();

    let mut adapter = create_adapter(conn_config.clone())?;
    adapter
        .connect(&conn_config, password.as_deref())
        .await
        .map_err(|e| anyhow!("Failed to connect to '{}': {}", profile, e))?;

    Ok(std::sync::Arc::from(adapter))
}
