//! Integration test harness for arni-data adapters.
//!
//! Provides helpers for loading connection profiles from `~/.arni/connections.yml`
//! or environment variables, and for conditionally skipping tests when adapters
//! are unavailable.
//!
//! # Quick Setup
//!
//! 1. Add a connection profile to `~/.arni/connections.yml`
//! 2. Set `TEST_<DB>_AVAILABLE=true` in your environment
//! 3. Run: `cargo test -p arni-data --features <db-feature>`
//!
//! See `tests/README.md` for full configuration instructions.

#![allow(dead_code)]

pub mod containers;

use arni_data::adapter::{ConnectionConfig, DatabaseType};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

// ─── Path helpers ──────────────────────────────────────────────────────────────

fn arni_home() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .expect("HOME or USERPROFILE environment variable must be set");
    PathBuf::from(home).join(".arni")
}

fn default_connections_path() -> PathBuf {
    arni_home().join("connections.yml")
}

// ─── YAML mirror types ─────────────────────────────────────────────────────────

/// Minimal mirror of `arni-cli`'s `ConnectionEntry` for test-only YAML parsing.
/// Kept separate from the CLI crate to avoid test/runtime coupling.
#[derive(Debug, Clone, Deserialize)]
struct TestConnectionEntry {
    #[serde(rename = "type")]
    db_type: String,
    host: Option<String>,
    port: Option<u16>,
    database: String,
    username: Option<String>,
    password: Option<String>,
    #[serde(default)]
    ssl: bool,
    #[serde(default)]
    parameters: HashMap<String, String>,
}

/// Top-level `connections.yml` document.
#[derive(Debug, Deserialize)]
struct TestConnectionsFile {
    #[serde(flatten)]
    connections: HashMap<String, TestConnectionEntry>,
}

// ─── DB type parsing ───────────────────────────────────────────────────────────

/// Parse a database type string (accepts the same aliases as arni-cli).
fn parse_db_type(s: &str) -> Option<DatabaseType> {
    match s.to_lowercase().as_str() {
        "postgres" | "postgresql" => Some(DatabaseType::Postgres),
        "mysql" => Some(DatabaseType::MySQL),
        "sqlite" => Some(DatabaseType::SQLite),
        "mongodb" | "mongo" => Some(DatabaseType::MongoDB),
        "sqlserver" | "mssql" => Some(DatabaseType::SQLServer),
        "oracle" => Some(DatabaseType::Oracle),
        "duckdb" => Some(DatabaseType::DuckDB),
        _ => None,
    }
}

// ─── Availability guards ───────────────────────────────────────────────────────

/// Returns `true` if the given adapter is declared available in the environment.
///
/// Checks `TEST_<DB>_AVAILABLE`, where `<DB>` is the uppercase canonical name
/// of the database type (e.g. `POSTGRES`, `MYSQL`, `SQLITE`).
///
/// ```bash
/// export TEST_POSTGRES_AVAILABLE=true
/// cargo test -p arni-data --features postgres
/// ```
pub fn is_adapter_available(db_type: &str) -> bool {
    let key = format!(
        "TEST_{}_AVAILABLE",
        db_type.to_uppercase().replace('-', "_")
    );
    matches!(
        std::env::var(&key).as_deref(),
        Ok("true") | Ok("1") | Ok("yes")
    )
}

/// Returns `true` (meaning "skip this test") when the adapter is unavailable.
///
/// Prints a human-readable notice so it is visible in test output.
///
/// ```ignore
/// #[test]
/// fn test_postgres_connect() {
///     if skip_if_unavailable("postgres") { return; }
///     // ... real test body
/// }
/// ```
pub fn skip_if_unavailable(db_type: &str) -> bool {
    if is_adapter_available(db_type) {
        return false;
    }
    println!(
        "[SKIP] {} adapter not available. Set TEST_{}_AVAILABLE=true to enable.",
        db_type,
        db_type.to_uppercase().replace('-', "_")
    );
    true
}

// ─── Config loading ────────────────────────────────────────────────────────────

/// Derive an environment-variable prefix from a connection profile name.
///
/// Converts to uppercase, replacing `-` and `.` with `_`, then prepends `TEST_`.
///
/// - `"pg-dev"` → `"TEST_PG_DEV"`
/// - `"mysql.local"` → `"TEST_MYSQL_LOCAL"`
fn env_prefix(profile_name: &str) -> String {
    format!(
        "TEST_{}",
        profile_name
            .to_uppercase()
            .replace('-', "_")
            .replace('.', "_")
    )
}

/// Try to load a [`ConnectionConfig`] from `~/.arni/connections.yml`.
///
/// Returns `None` if the file does not exist, the profile is missing,
/// or the entry cannot be converted into a valid `ConnectionConfig`.
fn load_from_yaml(profile_name: &str) -> Option<ConnectionConfig> {
    let path = default_connections_path();
    if !path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(&path).ok()?;
    let file: TestConnectionsFile = serde_yaml::from_str(&contents).ok()?;
    let entry = file.connections.get(profile_name)?;

    let db_type = parse_db_type(&entry.db_type)?;
    let port = entry.port.or_else(|| db_type.default_port());

    let mut parameters = entry.parameters.clone();
    if let Some(pwd) = &entry.password {
        parameters.insert("password".to_string(), pwd.clone());
    }

    Some(ConnectionConfig {
        id: profile_name.to_string(),
        name: profile_name.to_string(),
        db_type,
        host: entry.host.clone(),
        port,
        database: entry.database.clone(),
        username: entry.username.clone(),
        use_ssl: entry.ssl,
        parameters,
    })
}

/// Try to load a [`ConnectionConfig`] from environment variables.
///
/// Reads from `TEST_<PREFIX>_*` variables where the prefix is derived from
/// `profile_name` via [`env_prefix`].
///
/// | Variable              | Required | Description                     |
/// |-----------------------|----------|---------------------------------|
/// | `TEST_<P>_TYPE`       | Yes      | Database type string            |
/// | `TEST_<P>_DATABASE`   | Yes      | Database / schema name          |
/// | `TEST_<P>_HOST`       | No       | Hostname or IP                  |
/// | `TEST_<P>_PORT`       | No       | Port (defaults to DB default)   |
/// | `TEST_<P>_USER`       | No       | Username                        |
/// | `TEST_<P>_PASSWORD`   | No       | Password (injected at connect)  |
/// | `TEST_<P>_SSL`        | No       | `true` / `1` to enable SSL      |
fn load_from_env(profile_name: &str) -> Option<ConnectionConfig> {
    let p = env_prefix(profile_name);

    let type_str = std::env::var(format!("{p}_TYPE")).ok()?;
    let db_type = parse_db_type(&type_str)?;
    let database = std::env::var(format!("{p}_DATABASE")).ok()?;

    let host = std::env::var(format!("{p}_HOST")).ok();
    let port = std::env::var(format!("{p}_PORT"))
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .or_else(|| db_type.default_port());
    let username = std::env::var(format!("{p}_USER")).ok();
    let ssl = matches!(
        std::env::var(format!("{p}_SSL")).as_deref(),
        Ok("true") | Ok("1")
    );

    let mut parameters = HashMap::new();
    if let Ok(pwd) = std::env::var(format!("{p}_PASSWORD")) {
        parameters.insert("password".to_string(), pwd);
    }

    Some(ConnectionConfig {
        id: profile_name.to_string(),
        name: profile_name.to_string(),
        db_type,
        host,
        port,
        database,
        username,
        use_ssl: ssl,
        parameters,
    })
}

/// Load a [`ConnectionConfig`] for the given connection profile name.
///
/// Resolution order:
/// 1. `~/.arni/connections.yml` entry matching `profile_name`
/// 2. Environment variables with prefix `TEST_<PROFILE>_*`
///
/// Returns `None` if neither source provides a complete configuration.
///
/// ```ignore
/// let cfg = load_test_config("pg-dev").expect("pg-dev profile required");
/// ```
pub fn load_test_config(profile_name: &str) -> Option<ConnectionConfig> {
    load_from_yaml(profile_name).or_else(|| load_from_env(profile_name))
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── env_prefix ──

    #[test]
    fn test_env_prefix_simple() {
        assert_eq!(env_prefix("postgres"), "TEST_POSTGRES");
    }

    #[test]
    fn test_env_prefix_hyphenated() {
        assert_eq!(env_prefix("pg-dev"), "TEST_PG_DEV");
    }

    #[test]
    fn test_env_prefix_dotted() {
        assert_eq!(env_prefix("mysql.local"), "TEST_MYSQL_LOCAL");
    }

    // ── parse_db_type ──

    #[test]
    fn test_parse_db_type_canonical() {
        assert_eq!(parse_db_type("postgres"), Some(DatabaseType::Postgres));
        assert_eq!(parse_db_type("mysql"), Some(DatabaseType::MySQL));
        assert_eq!(parse_db_type("sqlite"), Some(DatabaseType::SQLite));
        assert_eq!(parse_db_type("mongodb"), Some(DatabaseType::MongoDB));
        assert_eq!(parse_db_type("sqlserver"), Some(DatabaseType::SQLServer));
        assert_eq!(parse_db_type("oracle"), Some(DatabaseType::Oracle));
        assert_eq!(parse_db_type("duckdb"), Some(DatabaseType::DuckDB));
    }

    #[test]
    fn test_parse_db_type_aliases() {
        assert_eq!(parse_db_type("postgresql"), Some(DatabaseType::Postgres));
        assert_eq!(parse_db_type("mssql"), Some(DatabaseType::SQLServer));
        assert_eq!(parse_db_type("mongo"), Some(DatabaseType::MongoDB));
    }

    #[test]
    fn test_parse_db_type_case_insensitive() {
        assert_eq!(parse_db_type("POSTGRES"), Some(DatabaseType::Postgres));
        assert_eq!(parse_db_type("MySQL"), Some(DatabaseType::MySQL));
    }

    #[test]
    fn test_parse_db_type_unknown() {
        assert_eq!(parse_db_type("redis"), None);
        assert_eq!(parse_db_type(""), None);
    }

    // ── is_adapter_available / skip_if_unavailable ──

    #[test]
    fn test_is_adapter_available_false_by_default() {
        assert!(!is_adapter_available("__nonexistent_db__"));
    }

    #[test]
    fn test_is_adapter_available_when_set_true() {
        let key = "TEST___ARNI_TESTDB___AVAILABLE";
        std::env::set_var(key, "true");
        assert!(is_adapter_available("__arni_testdb__"));
        std::env::remove_var(key);
    }

    #[test]
    fn test_is_adapter_available_when_set_1() {
        let key = "TEST___ARNI_TESTDB2___AVAILABLE";
        std::env::set_var(key, "1");
        assert!(is_adapter_available("__arni_testdb2__"));
        std::env::remove_var(key);
    }

    #[test]
    fn test_skip_if_unavailable_returns_true_when_not_set() {
        assert!(skip_if_unavailable("__nonexistent_db__"));
    }

    #[test]
    fn test_skip_if_unavailable_returns_false_when_available() {
        let key = "TEST___ARNI_SKIP_TEST___AVAILABLE";
        std::env::set_var(key, "true");
        assert!(!skip_if_unavailable("__arni_skip_test__"));
        std::env::remove_var(key);
    }

    // ── load_from_env ──

    #[test]
    fn test_load_from_env_minimal() {
        let p = "TEST_ARNI_ENVTEST1";
        std::env::set_var(format!("{p}_TYPE"), "postgres");
        std::env::set_var(format!("{p}_DATABASE"), "testdb");
        std::env::set_var(format!("{p}_HOST"), "localhost");

        let cfg = load_from_env("arni-envtest1");
        assert!(cfg.is_some());
        let cfg = cfg.unwrap();
        assert_eq!(cfg.db_type, DatabaseType::Postgres);
        assert_eq!(cfg.database, "testdb");
        assert_eq!(cfg.host.as_deref(), Some("localhost"));
        assert_eq!(cfg.port, Some(5432));

        std::env::remove_var(format!("{p}_TYPE"));
        std::env::remove_var(format!("{p}_DATABASE"));
        std::env::remove_var(format!("{p}_HOST"));
    }

    #[test]
    fn test_load_from_env_with_password() {
        let p = "TEST_ARNI_ENVTEST2";
        std::env::set_var(format!("{p}_TYPE"), "mysql");
        std::env::set_var(format!("{p}_DATABASE"), "mydb");
        std::env::set_var(format!("{p}_PASSWORD"), "secret");

        let cfg = load_from_env("arni-envtest2").unwrap();
        assert_eq!(
            cfg.parameters.get("password").map(|s| s.as_str()),
            Some("secret")
        );

        std::env::remove_var(format!("{p}_TYPE"));
        std::env::remove_var(format!("{p}_DATABASE"));
        std::env::remove_var(format!("{p}_PASSWORD"));
    }

    #[test]
    fn test_load_from_env_missing_required_returns_none() {
        let cfg = load_from_env("__arni_missing_profile__");
        assert!(cfg.is_none());
    }

    // ── load_test_config (fallback chain) ──

    #[test]
    fn test_load_test_config_falls_back_to_env() {
        // YAML won't have this profile, so env fallback must fire.
        let p = "TEST_ARNI_FALLBACK";
        std::env::set_var(format!("{p}_TYPE"), "sqlite");
        std::env::set_var(format!("{p}_DATABASE"), ":memory:");

        let cfg = load_test_config("arni-fallback");
        assert!(cfg.is_some());
        let cfg = cfg.unwrap();
        assert_eq!(cfg.db_type, DatabaseType::SQLite);
        assert_eq!(cfg.database, ":memory:");

        std::env::remove_var(format!("{p}_TYPE"));
        std::env::remove_var(format!("{p}_DATABASE"));
    }

    #[test]
    fn test_load_test_config_returns_none_for_unknown() {
        let cfg = load_test_config("__arni_definitely_not_in_yaml_or_env__");
        assert!(cfg.is_none());
    }
}
