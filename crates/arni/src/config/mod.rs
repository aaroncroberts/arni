//! Configuration file loader and types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::{Error, Result};

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Configuration profiles
    pub profiles: HashMap<String, Profile>,
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

impl Profile {
    /// Validate profile configuration
    pub fn validate(&self) -> Result<()> {
        match self {
            Profile::Postgres { host, port, database, username, .. } => {
                if host.is_empty() {
                    return Err(Error::Config("Postgres host cannot be empty".to_string()));
                }
                if *port == 0 {
                    return Err(Error::Config("Postgres port must be non-zero".to_string()));
                }
                if database.is_empty() {
                    return Err(Error::Config("Postgres database cannot be empty".to_string()));
                }
                if username.is_empty() {
                    return Err(Error::Config("Postgres username cannot be empty".to_string()));
                }
            }
            Profile::MongoDB { uri, database } => {
                if uri.is_empty() {
                    return Err(Error::Config("MongoDB URI cannot be empty".to_string()));
                }
                if database.is_empty() {
                    return Err(Error::Config("MongoDB database cannot be empty".to_string()));
                }
            }
            Profile::Oracle { connection_string } => {
                if connection_string.is_empty() {
                    return Err(Error::Config("Oracle connection string cannot be empty".to_string()));
                }
            }
            Profile::MsSql { host, port, database, username, .. } => {
                if host.is_empty() {
                    return Err(Error::Config("MsSql host cannot be empty".to_string()));
                }
                if *port == 0 {
                    return Err(Error::Config("MsSql port must be non-zero".to_string()));
                }
                if database.is_empty() {
                    return Err(Error::Config("MsSql database cannot be empty".to_string()));
                }
                if username.is_empty() {
                    return Err(Error::Config("MsSql username cannot be empty".to_string()));
                }
            }
            Profile::DuckDb { path } => {
                if path.is_empty() {
                    return Err(Error::Config("DuckDb path cannot be empty".to_string()));
                }
            }
        }
        Ok(())
    }

    /// Get the database type as a string
    pub fn db_type(&self) -> &str {
        match self {
            Profile::Postgres { .. } => "postgres",
            Profile::MongoDB { .. } => "mongodb",
            Profile::Oracle { .. } => "oracle",
            Profile::MsSql { .. } => "mssql",
            Profile::DuckDb { .. } => "duckdb",
        }
    }
}

impl Config {
    /// Load configuration from YAML file
    pub fn from_yaml_file(path: &str) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("Failed to read config file '{}': {}", path, e)))?;
        
        let config: Config = serde_yaml::from_str(&contents)
            .map_err(|e| Error::Config(format!("Failed to parse YAML config: {}", e)))?;
        
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from TOML file
    pub fn from_toml_file(path: &str) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("Failed to read config file '{}': {}", path, e)))?;
        
        let config: Config = toml::from_str(&contents)
            .map_err(|e| Error::Config(format!("Failed to parse TOML config: {}", e)))?;
        
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from file, auto-detecting format from extension
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let extension = path_ref
            .extension()
            .and_then(|e| e.to_str())
            .ok_or_else(|| Error::Config("Config file must have .yaml, .yml, or .toml extension".to_string()))?;
        
        match extension {
            "yaml" | "yml" => Self::from_yaml_file(path_ref.to_str().unwrap()),
            "toml" => Self::from_toml_file(path_ref.to_str().unwrap()),
            _ => Err(Error::Config(format!("Unsupported config file extension: .{}", extension))),
        }
    }

    /// Get a profile by name
    pub fn get_profile(&self, name: &str) -> Option<&Profile> {
        self.profiles.get(name)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.profiles.is_empty() {
            return Err(Error::Config("Configuration must contain at least one profile".to_string()));
        }

        for (name, profile) in &self.profiles {
            profile.validate().map_err(|e| {
                Error::Config(format!("Invalid profile '{}': {}", name, e))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_postgres_profile_validation() {
        let profile = Profile::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            database: "test".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn test_postgres_profile_validation_empty_host() {
        let profile = Profile::Postgres {
            host: "".to_string(),
            port: 5432,
            database: "test".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn test_postgres_profile_validation_zero_port() {
        let profile = Profile::Postgres {
            host: "localhost".to_string(),
            port: 0,
            database: "test".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn test_mongodb_profile_validation() {
        let profile = Profile::MongoDB {
            uri: "mongodb://localhost:27017".to_string(),
            database: "test".to_string(),
        };
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn test_duckdb_profile_validation() {
        let profile = Profile::DuckDb {
            path: ":memory:".to_string(),
        };
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn test_duckdb_profile_validation_empty_path() {
        let profile = Profile::DuckDb {
            path: "".to_string(),
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn test_profile_db_type() {
        let pg = Profile::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            database: "test".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert_eq!(pg.db_type(), "postgres");

        let mongo = Profile::MongoDB {
            uri: "mongodb://localhost".to_string(),
            database: "test".to_string(),
        };
        assert_eq!(mongo.db_type(), "mongodb");
    }

    #[test]
    fn test_config_validation_empty_profiles() {
        let config = Config {
            profiles: HashMap::new(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_get_profile() {
        let mut profiles = HashMap::new();
        profiles.insert(
            "test".to_string(),
            Profile::DuckDb {
                path: ":memory:".to_string(),
            },
        );
        let config = Config { profiles };
        
        assert!(config.get_profile("test").is_some());
        assert!(config.get_profile("missing").is_none());
    }

    #[test]
    fn test_yaml_config_parsing() {
        let yaml_content = r#"
profiles:
  test_pg:
    type: postgres
    host: localhost
    port: 5432
    database: testdb
    username: testuser
    password: testpass
"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_yaml_file(temp_file.path().to_str().unwrap());
        assert!(config.is_ok());
        
        let config = config.unwrap();
        assert_eq!(config.profiles.len(), 1);
        assert!(config.get_profile("test_pg").is_some());
    }

    #[test]
    fn test_toml_config_parsing() {
        let toml_content = r#"
[profiles.test_pg]
type = "postgres"
host = "localhost"
port = 5432
database = "testdb"
username = "testuser"
password = "testpass"
"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(toml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_toml_file(temp_file.path().to_str().unwrap());
        assert!(config.is_ok());
        
        let config = config.unwrap();
        assert_eq!(config.profiles.len(), 1);
        assert!(config.get_profile("test_pg").is_some());
    }

    #[test]
    fn test_config_from_file_yaml() {
        let yaml_content = r#"
profiles:
  test:
    type: duckdb
    path: ":memory:"
"#;
        let mut temp_file = NamedTempFile::with_suffix(".yaml").unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path());
        assert!(config.is_ok());
    }

    #[test]
    fn test_config_from_file_toml() {
        let toml_content = r#"
[profiles.test]
type = "duckdb"
path = ":memory:"
"#;
        let mut temp_file = NamedTempFile::with_suffix(".toml").unwrap();
        temp_file.write_all(toml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path());
        assert!(config.is_ok());
    }

    #[test]
    fn test_config_from_file_unsupported_extension() {
        let temp_file = NamedTempFile::with_suffix(".json").unwrap();
        let config = Config::from_file(temp_file.path());
        assert!(config.is_err());
    }
}
