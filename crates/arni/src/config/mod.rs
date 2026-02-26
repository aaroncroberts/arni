//! Configuration file loader and types

use serde::{Deserialize, Serialize};

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Configuration profiles
    pub profiles: std::collections::HashMap<String, Profile>,
}

/// Connection profile
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Profile {
    /// PostgreSQL configuration
    Postgres {
        /// Host
        host: String,
        /// Port
        port: u16,
        /// Database name
        database: String,
        /// Username
        username: String,
        /// Password
        password: String,
    },
    /// MongoDB configuration
    MongoDB {
        /// Connection URI
        uri: String,
        /// Database name
        database: String,
    },
    /// Oracle configuration
    Oracle {
        /// Connection string
        connection_string: String,
    },
    /// SQL Server configuration
    MsSql {
        /// Host
        host: String,
        /// Port
        port: u16,
        /// Database name
        database: String,
        /// Username
        username: String,
        /// Password
        password: String,
    },
    /// DuckDB configuration
    DuckDb {
        /// File path (or ":memory:" for in-memory)
        path: String,
    },
}

impl Config {
    /// Load configuration from YAML file
    pub fn from_yaml_file(_path: &str) -> Result<Self, crate::Error> {
        todo!("Configuration loading not yet implemented")
    }

    /// Load configuration from TOML file
    pub fn from_toml_file(_path: &str) -> Result<Self, crate::Error> {
        todo!("Configuration loading not yet implemented")
    }
}
