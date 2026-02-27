//! Dev-container configuration and `ConnectionConfig` factories.
//!
//! This module provides pre-built [`ConnectionConfig`] values that match the
//! containers started by `arni dev start` (via `compose.yml`). Use these in
//! integration tests instead of hard-coding credentials.
//!
//! # Usage
//!
//! ```ignore
//! use common::containers;
//!
//! #[tokio::test]
//! async fn test_postgres() {
//!     if !containers::is_running("postgres") { return; }
//!     let cfg = containers::postgres_config();
//!     // connect with cfg...
//! }
//! ```
//!
//! # Starting containers
//!
//! ```bash
//! arni dev start          # start all containers (podman-compose)
//! arni dev stop           # stop and remove containers
//! arni dev status         # show container health
//! ```
//!
//! For CI, the `integration-tests` job in `.github/workflows/ci.yml`
//! starts the required services automatically via `podman-compose`.

#![allow(dead_code)]

use arni_data::adapter::{ConnectionConfig, DatabaseType};
use std::collections::HashMap;

// ─── Dev-container constants ───────────────────────────────────────────────────
//
// These match the values in compose.yml exactly. If you change credentials in
// compose.yml you must update these constants too.

/// PostgreSQL dev-container defaults (compose.yml: service `postgres`)
pub mod pg {
    pub const HOST: &str = "localhost";
    pub const PORT: u16 = 5432;
    pub const DATABASE: &str = "test_db";
    pub const USER: &str = "test_user";
    pub const PASSWORD: &str = "test_password";
    /// Profile name used in `~/.arni/connections.yml` and CI env vars
    pub const PROFILE: &str = "pg-dev";
    /// `TEST_POSTGRES_AVAILABLE` availability guard key
    pub const AVAILABLE_KEY: &str = "postgres";
}

/// MySQL dev-container defaults (compose.yml: service `mysql`)
pub mod mysql {
    pub const HOST: &str = "localhost";
    pub const PORT: u16 = 3306;
    pub const DATABASE: &str = "test_db";
    pub const USER: &str = "test_user";
    pub const PASSWORD: &str = "test_password";
    pub const PROFILE: &str = "mysql-dev";
    pub const AVAILABLE_KEY: &str = "mysql";
}

/// SQL Server dev-container defaults (compose.yml: service `mssql`)
/// Host port 1434 avoids conflict with other local MSSQL instances.
pub mod mssql {
    pub const HOST: &str = "localhost";
    pub const PORT: u16 = 1434;
    pub const DATABASE: &str = "master";
    pub const USER: &str = "sa";
    pub const PASSWORD: &str = "Test_Password123!";
    pub const PROFILE: &str = "mssql-dev";
    pub const AVAILABLE_KEY: &str = "mssql";
}

/// MongoDB dev-container defaults (compose.yml: service `mongodb`)
/// Host port 27018 avoids conflict with other local MongoDB instances.
pub mod mongodb {
    pub const HOST: &str = "localhost";
    pub const PORT: u16 = 27018;
    pub const DATABASE: &str = "test_db";
    pub const USER: &str = "test_user";
    pub const PASSWORD: &str = "test_password";
    pub const PROFILE: &str = "mongo-dev";
    pub const AVAILABLE_KEY: &str = "mongodb";
}

/// Oracle dev-container defaults (compose.yml: service `oracle`)
///
/// **NOTE**: Oracle requires ~2 GB shared memory and ~60 s startup time.
/// It is excluded from CI. Run locally with `arni dev start`.
/// Host port 1522 avoids conflict with other local Oracle instances.
pub mod oracle {
    pub const HOST: &str = "localhost";
    pub const PORT: u16 = 1522;
    /// Oracle service name (used as the "database" in connection strings)
    pub const SERVICE: &str = "FREE";
    pub const USER: &str = "system";
    pub const PASSWORD: &str = "test_password";
    pub const PROFILE: &str = "oracle-dev";
    pub const AVAILABLE_KEY: &str = "oracle";
}

// ─── Availability check ────────────────────────────────────────────────────────

/// Returns `true` when the named container is declared available via
/// `TEST_<NAME>_AVAILABLE=true` in the environment.
///
/// Equivalent to [`super::is_adapter_available`] but more explicit about the
/// container concept.
///
/// ```bash
/// export TEST_POSTGRES_AVAILABLE=true
/// export TEST_MYSQL_AVAILABLE=true
/// ```
pub fn is_running(name: &str) -> bool {
    super::is_adapter_available(name)
}

// ─── ConnectionConfig factories ────────────────────────────────────────────────

/// Build a [`ConnectionConfig`] for the PostgreSQL dev-container.
///
/// Prefers values from environment / `~/.arni/connections.yml` (via
/// [`super::load_test_config`]) so that non-default setups work without code
/// changes. Falls back to the hardcoded dev-container defaults.
pub fn postgres_config() -> ConnectionConfig {
    super::load_test_config(pg::PROFILE).unwrap_or_else(|| {
        build_config(
            DatabaseType::Postgres,
            pg::PROFILE,
            pg::HOST,
            pg::PORT,
            pg::DATABASE,
            pg::USER,
            pg::PASSWORD,
        )
    })
}

/// Build a [`ConnectionConfig`] for the MySQL dev-container.
pub fn mysql_config() -> ConnectionConfig {
    super::load_test_config(mysql::PROFILE).unwrap_or_else(|| {
        build_config(
            DatabaseType::MySQL,
            mysql::PROFILE,
            mysql::HOST,
            mysql::PORT,
            mysql::DATABASE,
            mysql::USER,
            mysql::PASSWORD,
        )
    })
}

/// Build a [`ConnectionConfig`] for the SQL Server dev-container.
pub fn mssql_config() -> ConnectionConfig {
    super::load_test_config(mssql::PROFILE).unwrap_or_else(|| {
        build_config(
            DatabaseType::SQLServer,
            mssql::PROFILE,
            mssql::HOST,
            mssql::PORT,
            mssql::DATABASE,
            mssql::USER,
            mssql::PASSWORD,
        )
    })
}

/// Build a [`ConnectionConfig`] for the MongoDB dev-container.
pub fn mongodb_config() -> ConnectionConfig {
    super::load_test_config(mongodb::PROFILE).unwrap_or_else(|| {
        build_config(
            DatabaseType::MongoDB,
            mongodb::PROFILE,
            mongodb::HOST,
            mongodb::PORT,
            mongodb::DATABASE,
            mongodb::USER,
            mongodb::PASSWORD,
        )
    })
}

/// Build a [`ConnectionConfig`] for the Oracle dev-container.
///
/// **NOTE**: Oracle is excluded from CI; use locally after `arni dev start`.
pub fn oracle_config() -> ConnectionConfig {
    super::load_test_config(oracle::PROFILE).unwrap_or_else(|| {
        build_config(
            DatabaseType::Oracle,
            oracle::PROFILE,
            oracle::HOST,
            oracle::PORT,
            oracle::SERVICE,
            oracle::USER,
            oracle::PASSWORD,
        )
    })
}

/// Build an in-memory SQLite [`ConnectionConfig`] (no container needed).
pub fn sqlite_memory_config() -> ConnectionConfig {
    ConnectionConfig {
        id: "sqlite-test".to_string(),
        name: "SQLite Test".to_string(),
        db_type: DatabaseType::SQLite,
        host: None,
        port: None,
        database: ":memory:".to_string(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
    }
}

/// Build an in-memory DuckDB [`ConnectionConfig`] (no container needed).
pub fn duckdb_memory_config() -> ConnectionConfig {
    ConnectionConfig {
        id: "duckdb-test".to_string(),
        name: "DuckDB Test".to_string(),
        db_type: DatabaseType::DuckDB,
        host: None,
        port: None,
        database: ":memory:".to_string(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
    }
}

// ─── Internal helpers ──────────────────────────────────────────────────────────

fn build_config(
    db_type: DatabaseType,
    id: &str,
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> ConnectionConfig {
    let mut parameters = HashMap::new();
    parameters.insert("password".to_string(), password.to_string());
    ConnectionConfig {
        id: id.to_string(),
        name: id.to_string(),
        db_type,
        host: Some(host.to_string()),
        port: Some(port),
        database: database.to_string(),
        username: Some(username.to_string()),
        use_ssl: false,
        parameters,
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arni_data::adapter::DatabaseType;

    #[test]
    fn test_postgres_config_defaults() {
        // Without env vars set the factory uses hardcoded dev-container values.
        let cfg = postgres_config();
        assert_eq!(cfg.db_type, DatabaseType::Postgres);
        assert_eq!(cfg.database, pg::DATABASE);
        assert_eq!(cfg.port, Some(pg::PORT));
        assert_eq!(cfg.username.as_deref(), Some(pg::USER));
        assert_eq!(
            cfg.parameters.get("password").map(|s| s.as_str()),
            Some(pg::PASSWORD)
        );
    }

    #[test]
    fn test_mysql_config_defaults() {
        let cfg = mysql_config();
        assert_eq!(cfg.db_type, DatabaseType::MySQL);
        assert_eq!(cfg.port, Some(mysql::PORT));
        assert_eq!(cfg.database, mysql::DATABASE);
    }

    #[test]
    fn test_mssql_config_defaults() {
        let cfg = mssql_config();
        assert_eq!(cfg.db_type, DatabaseType::SQLServer);
        assert_eq!(cfg.port, Some(mssql::PORT));
        assert_eq!(cfg.username.as_deref(), Some(mssql::USER));
    }

    #[test]
    fn test_mongodb_config_defaults() {
        let cfg = mongodb_config();
        assert_eq!(cfg.db_type, DatabaseType::MongoDB);
        assert_eq!(cfg.port, Some(mongodb::PORT));
    }

    #[test]
    fn test_oracle_config_defaults() {
        let cfg = oracle_config();
        assert_eq!(cfg.db_type, DatabaseType::Oracle);
        assert_eq!(cfg.port, Some(oracle::PORT));
        assert_eq!(cfg.database, oracle::SERVICE);
    }

    #[test]
    fn test_sqlite_memory_config() {
        let cfg = sqlite_memory_config();
        assert_eq!(cfg.db_type, DatabaseType::SQLite);
        assert_eq!(cfg.database, ":memory:");
        assert!(cfg.host.is_none());
    }

    #[test]
    fn test_duckdb_memory_config() {
        let cfg = duckdb_memory_config();
        assert_eq!(cfg.db_type, DatabaseType::DuckDB);
        assert_eq!(cfg.database, ":memory:");
        assert!(cfg.host.is_none());
    }

    #[test]
    fn test_is_running_false_by_default() {
        assert!(!is_running("__no_such_container__"));
    }

    #[test]
    fn test_is_running_true_when_env_set() {
        std::env::set_var("TEST_CONTAINERS_TEST_AVAILABLE", "true");
        assert!(is_running("containers_test"));
        std::env::remove_var("TEST_CONTAINERS_TEST_AVAILABLE");
    }

    #[test]
    fn test_postgres_config_env_override() {
        // load_test_config falls back to env vars when the profile is not in
        // ~/.arni/connections.yml.  Use a synthetic profile name that cannot
        // appear in any real connections file.
        let profile = "__arni_containers_test_pg__";
        let p = format!("TEST_{}", profile.to_uppercase().replace('-', "_"));
        std::env::set_var(format!("{p}_TYPE"), "postgres");
        std::env::set_var(format!("{p}_DATABASE"), "override_db");
        std::env::set_var(format!("{p}_HOST"), "db.example.com");

        let cfg = super::super::load_test_config(profile);
        assert!(cfg.is_some(), "env-var fallback should produce a config");
        let cfg = cfg.unwrap();
        assert_eq!(cfg.database, "override_db");
        assert_eq!(cfg.host.as_deref(), Some("db.example.com"));

        std::env::remove_var(format!("{p}_TYPE"));
        std::env::remove_var(format!("{p}_DATABASE"));
        std::env::remove_var(format!("{p}_HOST"));
    }
}
