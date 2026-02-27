//! Connection configuration management for arni-cli.
//!
//! Manages `~/.arni/connections.yml` (or a custom path) containing named database
//! connection profiles. Each connection has a unique name used as a lookup key.
//!
//! # File Format
//!
//! ```yaml
//! # ~/.arni/connections.yml
//!
//! dev-postgres:
//!   type: postgres
//!   host: localhost
//!   port: 5432
//!   database: mydb
//!   username: myuser
//!   password: ~        # null = prompt at runtime
//!   ssl: false
//!   parameters:
//!     connect_timeout: "10"
//!
//! local-sqlite:
//!   type: sqlite
//!   database: /tmp/mydb.db
//!
//! analytics:
//!   type: duckdb
//!   database: ":memory:"
//! ```

use anyhow::{bail, Context, Result};
use arni_data::adapter::{ConnectionConfig, DatabaseType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// ─── Path helpers ────────────────────────────────────────────────────────────

/// Returns the arni home directory: `~/.arni/`
pub fn arni_home() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .expect("HOME or USERPROFILE environment variable must be set");
    PathBuf::from(home).join(".arni")
}

/// Returns the default connections config path: `~/.arni/connections.yml`
pub fn default_connections_path(config_dir: &Path) -> PathBuf {
    config_dir.join("connections.yml")
}

// ─── Schema types ─────────────────────────────────────────────────────────────

/// Top-level structure for `connections.yml`.
///
/// The file is a flat YAML mapping where each key is the unique connection name
/// and the value is a [`ConnectionEntry`]. Connection names must match
/// `[a-zA-Z0-9_-]+`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionsFile {
    #[serde(flatten)]
    pub connections: HashMap<String, ConnectionEntry>,
}

/// A single named connection entry in `connections.yml`.
///
/// `database` doubles as the file path for SQLite and DuckDB (use `":memory:"`
/// for DuckDB in-memory databases). Network-based adapters require `host`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionEntry {
    /// Database type. One of: `postgres`, `mysql`, `sqlite`, `mongodb`,
    /// `sqlserver`, `oracle`, `duckdb`.
    #[serde(rename = "type")]
    pub db_type: DatabaseType,

    /// Hostname or IP address. Required for network-based databases.
    /// Not used for file-based databases (sqlite, duckdb).
    pub host: Option<String>,

    /// Port number. Omit to use the default port for the database type.
    pub port: Option<u16>,

    /// Database name. For SQLite/DuckDB this is the file path or `":memory:"`.
    pub database: String,

    /// Username for authentication.
    pub username: Option<String>,

    /// Password for authentication.
    ///
    /// - Non-empty string: used directly (avoid committing to version control).
    /// - `~` / absent: the CLI will prompt the user at runtime.
    pub password: Option<String>,

    /// Enable SSL/TLS for the connection. Defaults to `false`.
    #[serde(default)]
    pub ssl: bool,

    /// Additional driver-specific key/value parameters (e.g. `connect_timeout: "10"`).
    #[serde(default)]
    pub parameters: HashMap<String, String>,
}

// ─── Name validation ──────────────────────────────────────────────────────────

/// Validates that a connection name matches `[a-zA-Z0-9_-]+`.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("Connection name must not be empty");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!(
            "Connection name '{}' contains invalid characters. \
             Only [a-zA-Z0-9_-] are allowed.",
            name
        );
    }
    Ok(())
}

// ─── ConnectionEntry impl ─────────────────────────────────────────────────────

impl ConnectionEntry {
    /// Convert this entry into a [`ConnectionConfig`] using `name` as the id.
    ///
    /// Password is injected into `parameters["password"]` so adapters can
    /// retrieve it at connection time. It is never stored in `ConnectionConfig`
    /// directly, preserving the separation between schema and runtime state.
    ///
    /// Returns an error if the resolved host is absent for network-based
    /// databases.
    pub fn into_connection_config(self, name: &str) -> Result<ConnectionConfig> {
        let requires_host =
            !matches!(self.db_type, DatabaseType::SQLite | DatabaseType::DuckDB);
        if requires_host && self.host.is_none() {
            bail!(
                "Connection '{}' (type: {}) requires a 'host' field",
                name,
                self.db_type
            );
        }

        let mut parameters = self.parameters;
        if let Some(pw) = self.password {
            if !pw.is_empty() {
                parameters.insert("password".to_string(), pw);
            }
        }

        let port = self.port.or_else(|| self.db_type.default_port());

        Ok(ConnectionConfig {
            id: name.to_string(),
            name: name.to_string(),
            db_type: self.db_type,
            host: self.host,
            port,
            database: self.database,
            username: self.username,
            use_ssl: self.ssl,
            parameters,
        })
    }
}

// ─── ConnectionsFile impl ─────────────────────────────────────────────────────

impl ConnectionsFile {
    /// Load connections from a YAML file.
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read connections file: {}", path.display()))?;
        let file: ConnectionsFile = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse connections YAML: {}", path.display()))?;
        for name in file.connections.keys() {
            validate_name(name)?;
        }
        Ok(file)
    }

    /// Load connections from the default path, returning an empty config if the
    /// file does not exist.
    pub fn load_or_default(config_dir: &Path) -> Result<Self> {
        let path = default_connections_path(config_dir);
        if !path.exists() {
            return Ok(Self::default());
        }
        Self::load(&path)
    }

    /// Save connections to a YAML file, creating parent directories as needed.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create config directory: {}", parent.display())
            })?;
        }
        let contents = serde_yaml::to_string(self).context("Failed to serialize connections")?;
        std::fs::write(path, contents)
            .with_context(|| format!("Failed to write connections file: {}", path.display()))?;
        Ok(())
    }

    /// Retrieve a connection entry by name.
    pub fn get(&self, name: &str) -> Option<&ConnectionEntry> {
        self.connections.get(name)
    }

    /// Add or replace a connection. Validates the name before inserting.
    pub fn upsert(&mut self, name: String, entry: ConnectionEntry) -> Result<()> {
        validate_name(&name)?;
        self.connections.insert(name, entry);
        Ok(())
    }

    /// Remove a connection by name, returning the entry if it existed.
    pub fn remove(&mut self, name: &str) -> Option<ConnectionEntry> {
        self.connections.remove(name)
    }

    /// List all connection names in alphabetical order.
    pub fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.connections.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_name("my-db").is_ok());
        assert!(validate_name("prod_postgres").is_ok());
        assert!(validate_name("DB123").is_ok());
        assert!(validate_name("a").is_ok());
    }

    #[test]
    fn test_validate_name_invalid() {
        assert!(validate_name("").is_err());
        assert!(validate_name("my db").is_err()); // space
        assert!(validate_name("my.db").is_err()); // dot
        assert!(validate_name("my/db").is_err()); // slash
        assert!(validate_name("my@db").is_err()); // at-sign
    }

    #[test]
    fn test_into_connection_config_sqlite_no_host_required() {
        let entry = ConnectionEntry {
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: "/tmp/test.db".to_string(),
            username: None,
            password: None,
            ssl: false,
            parameters: HashMap::new(),
        };
        let cfg = entry.into_connection_config("local-sqlite").unwrap();
        assert_eq!(cfg.id, "local-sqlite");
        assert_eq!(cfg.database, "/tmp/test.db");
        assert_eq!(cfg.db_type, DatabaseType::SQLite);
        assert!(cfg.host.is_none());
    }

    #[test]
    fn test_into_connection_config_duckdb_memory() {
        let entry = ConnectionEntry {
            db_type: DatabaseType::DuckDB,
            host: None,
            port: None,
            database: ":memory:".to_string(),
            username: None,
            password: None,
            ssl: false,
            parameters: HashMap::new(),
        };
        let cfg = entry.into_connection_config("analytics").unwrap();
        assert_eq!(cfg.database, ":memory:");
    }

    #[test]
    fn test_into_connection_config_postgres_missing_host_fails() {
        let entry = ConnectionEntry {
            db_type: DatabaseType::Postgres,
            host: None,
            port: None,
            database: "mydb".to_string(),
            username: Some("user".to_string()),
            password: None,
            ssl: false,
            parameters: HashMap::new(),
        };
        assert!(entry.into_connection_config("dev-pg").is_err());
    }

    #[test]
    fn test_into_connection_config_password_injected() {
        let entry = ConnectionEntry {
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "mydb".to_string(),
            username: Some("user".to_string()),
            password: Some("s3cret".to_string()),
            ssl: false,
            parameters: HashMap::new(),
        };
        let cfg = entry.into_connection_config("test-pg").unwrap();
        assert_eq!(
            cfg.parameters.get("password").map(String::as_str),
            Some("s3cret")
        );
    }

    #[test]
    fn test_into_connection_config_default_port() {
        let entry = ConnectionEntry {
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: None, // should default to 5432
            database: "mydb".to_string(),
            username: None,
            password: None,
            ssl: false,
            parameters: HashMap::new(),
        };
        let cfg = entry.into_connection_config("pg").unwrap();
        assert_eq!(cfg.port, Some(5432));
    }

    #[test]
    fn test_connections_file_yaml_roundtrip() {
        let yaml = r#"
dev-postgres:
  type: postgres
  host: localhost
  port: 5432
  database: mydb
  username: admin
  password: ~
  ssl: false

local-sqlite:
  type: sqlite
  database: /tmp/test.db
"#;
        let file: ConnectionsFile = serde_yaml::from_str(yaml).unwrap();
        assert!(file.connections.contains_key("dev-postgres"));
        assert!(file.connections.contains_key("local-sqlite"));

        let pg = &file.connections["dev-postgres"];
        assert_eq!(pg.db_type, DatabaseType::Postgres);
        assert_eq!(pg.host.as_deref(), Some("localhost"));
        assert_eq!(pg.port, Some(5432));
        assert!(pg.password.is_none());

        let sq = &file.connections["local-sqlite"];
        assert_eq!(sq.db_type, DatabaseType::SQLite);
        assert_eq!(sq.database, "/tmp/test.db");
    }

    #[test]
    fn test_connections_file_names_sorted() {
        let yaml = r#"
zebra:
  type: sqlite
  database: z.db
alpha:
  type: sqlite
  database: a.db
"#;
        let file: ConnectionsFile = serde_yaml::from_str(yaml).unwrap();
        let names = file.names();
        assert_eq!(names, vec!["alpha", "zebra"]);
    }

    #[test]
    fn test_connections_file_upsert_invalid_name() {
        let mut file = ConnectionsFile::default();
        let entry = ConnectionEntry {
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: "x.db".to_string(),
            username: None,
            password: None,
            ssl: false,
            parameters: HashMap::new(),
        };
        assert!(file.upsert("bad name".to_string(), entry).is_err());
    }
}
