//! Configuration schema and validation for database connections
//!
//! This module defines the configuration file format for arni, supporting:
//! - Multiple connection profiles (development, production, etc.)
//! - YAML and TOML file formats
//! - Environment variable substitution
//! - Connection validation
//!
//! # Examples
//!
//! ## YAML Configuration
//!
//! ```yaml
//! default_profile: development
//!
//! profiles:
//!   development:
//!     connections:
//!       - id: dev-postgres
//!         name: Development PostgreSQL
//!         db_type: postgres
//!         host: localhost
//!         port: 5432
//!         database: arni_dev
//!         username: ${POSTGRES_USER}
//!         use_ssl: false
//!
//!   production:
//!     connections:
//!       - id: prod-postgres
//!         name: Production PostgreSQL
//!         db_type: postgres
//!         host: ${DB_HOST}
//!         port: 5432
//!         database: arni_prod
//!         username: ${DB_USER}
//!         use_ssl: true
//! ```
//!
//! ## TOML Configuration
//!
//! ```toml
//! default_profile = "development"
//!
//! [profiles.development]
//! [[profiles.development.connections]]
//! id = "dev-postgres"
//! name = "Development PostgreSQL"
//! db_type = "postgres"
//! host = "localhost"
//! port = 5432
//! database = "arni_dev"
//! username = "${POSTGRES_USER}"
//! use_ssl = false
//! ```

use crate::adapter::ConnectionConfig;
use crate::error::{DataError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;

/// Top-level configuration file structure
///
/// Supports multiple named profiles for different environments
/// (development, staging, production, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArniConfig {
    /// Default profile to use if none specified
    #[serde(default = "default_profile_name")]
    pub default_profile: String,

    /// Named profiles with their connection configurations
    pub profiles: HashMap<String, ConfigProfile>,
}

/// A named configuration profile
///
/// Each profile can contain multiple database connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigProfile {
    /// List of database connections in this profile
    pub connections: Vec<ConnectionConfig>,
}

impl Default for ArniConfig {
    fn default() -> Self {
        Self {
            default_profile: default_profile_name(),
            profiles: HashMap::new(),
        }
    }
}

fn default_profile_name() -> String {
    "default".to_string()
}

impl ArniConfig {
    /// Create a new empty configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a profile by name
    pub fn get_profile(&self, name: &str) -> Option<&ConfigProfile> {
        self.profiles.get(name)
    }

    /// Get the default profile
    pub fn default_profile(&self) -> Option<&ConfigProfile> {
        self.get_profile(&self.default_profile)
    }

    /// Get a specific connection from a profile
    pub fn get_connection(&self, profile: &str, connection_id: &str) -> Option<&ConnectionConfig> {
        self.get_profile(profile)?
            .connections
            .iter()
            .find(|c| c.id == connection_id)
    }

    /// Validate the entire configuration
    pub fn validate(&self) -> Result<()> {
        // Check that default profile exists
        if !self.profiles.contains_key(&self.default_profile) {
            return Err(DataError::Config(format!(
                "Default profile '{}' not found in configuration",
                self.default_profile
            )));
        }

        // Validate each profile
        for (profile_name, profile) in &self.profiles {
            profile
                .validate()
                .map_err(|e| DataError::Config(format!("Profile '{}': {}", profile_name, e)))?;
        }

        Ok(())
    }

    /// Substitute environment variables in the configuration
    ///
    /// Replaces `${VAR_NAME}` or `$VAR_NAME` syntax with actual environment values
    pub fn substitute_env_vars(mut self) -> Result<Self> {
        for profile in self.profiles.values_mut() {
            for connection in &mut profile.connections {
                // Substitute in host
                if let Some(ref mut host) = connection.host {
                    *host = substitute_env_var(host)?;
                }

                // Substitute in database
                connection.database = substitute_env_var(&connection.database)?;

                // Substitute in username
                if let Some(ref mut username) = connection.username {
                    *username = substitute_env_var(username)?;
                }

                // Substitute in parameters
                for value in connection.parameters.values_mut() {
                    *value = substitute_env_var(value)?;
                }
            }
        }

        Ok(self)
    }
}

impl ConfigProfile {
    /// Create a new empty profile
    pub fn new() -> Self {
        Self {
            connections: Vec::new(),
        }
    }

    /// Add a connection to the profile
    pub fn add_connection(&mut self, connection: ConnectionConfig) {
        self.connections.push(connection);
    }

    /// Validate all connections in the profile
    pub fn validate(&self) -> Result<()> {
        validate_connections(&self.connections)
    }
}

impl Default for ConfigProfile {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate a list of connections
///
/// Checks for:
/// - Duplicate connection IDs
/// - Valid individual connection configurations
pub fn validate_connections(connections: &[ConnectionConfig]) -> Result<()> {
    // Check for duplicate IDs
    let mut seen_ids = std::collections::HashSet::new();
    for conn in connections {
        if !seen_ids.insert(&conn.id) {
            return Err(DataError::Config(format!(
                "Duplicate connection ID: {}",
                conn.id
            )));
        }

        // Validate individual connection
        validate_connection(conn)?;
    }

    Ok(())
}

/// Validate a single connection configuration
///
/// Checks:
/// - Non-empty ID and database name
/// - Required host/port for server-based databases
/// - Valid port range
pub fn validate_connection(config: &ConnectionConfig) -> Result<()> {
    use crate::adapter::DatabaseType;

    // Validate ID is not empty
    if config.id.trim().is_empty() {
        return Err(DataError::Config(
            "Connection ID cannot be empty".to_string(),
        ));
    }

    // Validate database name is not empty
    if config.database.trim().is_empty() {
        return Err(DataError::Config(
            "Database name cannot be empty".to_string(),
        ));
    }

    // For non-file-based databases, host is required
    match config.db_type {
        DatabaseType::SQLite | DatabaseType::DuckDB => {
            // File-based databases use database field as file path
            // host/port are optional
        }
        DatabaseType::Postgres
        | DatabaseType::MySQL
        | DatabaseType::MongoDB
        | DatabaseType::SQLServer
        | DatabaseType::Oracle => {
            if config.host.is_none() || config.host.as_ref().unwrap().trim().is_empty() {
                return Err(DataError::Config(format!(
                    "{} requires a host address",
                    config.db_type
                )));
            }
        }
    }

    // Validate port range if provided
    if let Some(port) = config.port {
        if port == 0 {
            return Err(DataError::Config(
                "Invalid port number: 0. Must be between 1 and 65535".to_string(),
            ));
        }
    }

    // For server databases, port should be specified
    match config.db_type {
        DatabaseType::Postgres
        | DatabaseType::MySQL
        | DatabaseType::MongoDB
        | DatabaseType::SQLServer
        | DatabaseType::Oracle => {
            if config.port.is_none() {
                return Err(DataError::Config(format!(
                    "{} requires a port number",
                    config.db_type
                )));
            }
        }
        DatabaseType::SQLite | DatabaseType::DuckDB => {
            // Port not required for file-based databases
        }
    }

    Ok(())
}

/// Substitute environment variables in a string
///
/// Supports both `${VAR}` and `$VAR` syntax.
/// Returns error if referenced variable is not found.
fn substitute_env_var(value: &str) -> Result<String> {
    let mut result = value.to_string();
    let mut changed = true;

    // Iterate until no more substitutions are made (handles nested vars)
    while changed {
        changed = false;
        let original = result.clone();

        // Handle ${VAR} syntax
        if let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_name = &result[start + 2..start + end];
                let env_value = env::var(var_name).map_err(|_| {
                    DataError::Config(format!(
                        "Environment variable '{}' not found (referenced in '{}')",
                        var_name, value
                    ))
                })?;

                result = format!(
                    "{}{}{}",
                    &result[..start],
                    env_value,
                    &result[start + end + 1..]
                );
                changed = true;
            }
        }

        // Handle $VAR syntax (single pass after ${ } handled)
        if !changed && result.contains('$') {
            let parts: Vec<&str> = result.split('$').collect();
            let mut new_result = parts[0].to_string();

            for part in &parts[1..] {
                // Find where variable name ends (non-alphanumeric or underscore)
                let end_pos = part
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(part.len());

                if end_pos == 0 {
                    // Just a $ character, keep it
                    new_result.push('$');
                    new_result.push_str(part);
                } else {
                    let var_name = &part[..end_pos];
                    let env_value = env::var(var_name).map_err(|_| {
                        DataError::Config(format!(
                            "Environment variable '{}' not found (referenced in '{}')",
                            var_name, value
                        ))
                    })?;

                    new_result.push_str(&env_value);
                    new_result.push_str(&part[end_pos..]);
                }
            }

            if new_result != original {
                result = new_result;
                changed = true;
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::DatabaseType;
    use std::collections::HashMap;

    #[test]
    fn test_default_config() {
        let config = ArniConfig::default();
        assert_eq!(config.default_profile, "default");
        assert_eq!(config.profiles.len(), 0);
    }

    #[test]
    fn test_config_profile() {
        let mut profile = ConfigProfile::new();
        assert_eq!(profile.connections.len(), 0);

        let conn = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "testdb".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        profile.add_connection(conn);
        assert_eq!(profile.connections.len(), 1);
    }

    #[test]
    fn test_config_get_profile() {
        let mut config = ArniConfig::new();
        let mut profile = ConfigProfile::new();
        profile.add_connection(ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "testdb".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        });

        config.profiles.insert("dev".to_string(), profile);

        assert!(config.get_profile("dev").is_some());
        assert!(config.get_profile("prod").is_none());
    }

    #[test]
    fn test_validate_empty_id() {
        let config = ConnectionConfig {
            id: "".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "testdb".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("ID cannot be empty"));
    }

    #[test]
    fn test_validate_empty_database() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Database name cannot be empty"));
    }

    #[test]
    fn test_validate_missing_host_for_postgres() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: None,
            port: Some(5432),
            database: "testdb".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a host"));
    }

    #[test]
    fn test_validate_missing_port_for_mysql() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::MySQL,
            host: Some("localhost".to_string()),
            port: None,
            database: "testdb".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a port"));
    }

    #[test]
    fn test_validate_invalid_port() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(0),
            database: "testdb".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid port"));
    }

    #[test]
    fn test_validate_sqlite_no_host_required() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test SQLite".to_string(),
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: "/path/to/database.db".to_string(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_duckdb_no_host_required() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test DuckDB".to_string(),
            db_type: DatabaseType::DuckDB,
            host: None,
            port: None,
            database: "/path/to/database.duckdb".to_string(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_duplicate_ids() {
        let connections = vec![
            ConnectionConfig {
                id: "duplicate".to_string(),
                name: "First".to_string(),
                db_type: DatabaseType::Postgres,
                host: Some("localhost".to_string()),
                port: Some(5432),
                database: "db1".to_string(),
                username: Some("user".to_string()),
                use_ssl: false,
                parameters: HashMap::new(),
            },
            ConnectionConfig {
                id: "duplicate".to_string(),
                name: "Second".to_string(),
                db_type: DatabaseType::MySQL,
                host: Some("localhost".to_string()),
                port: Some(3306),
                database: "db2".to_string(),
                username: Some("user".to_string()),
                use_ssl: false,
                parameters: HashMap::new(),
            },
        ];

        let result = validate_connections(&connections);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Duplicate connection ID"));
    }

    #[test]
    fn test_substitute_env_var_braces() {
        env::set_var("TEST_VAR", "test_value");

        let result = substitute_env_var("prefix_${TEST_VAR}_suffix").unwrap();
        assert_eq!(result, "prefix_test_value_suffix");

        env::remove_var("TEST_VAR");
    }

    #[test]
    fn test_substitute_env_var_no_braces() {
        env::set_var("TEST_VAR2", "another_value");

        // Note: Without braces, use at end of string to avoid ambiguity
        let result = substitute_env_var("prefix_$TEST_VAR2").unwrap();
        assert_eq!(result, "prefix_another_value");

        // Or use braces when followed by more text
        let result2 = substitute_env_var("prefix_${TEST_VAR2}_suffix").unwrap();
        assert_eq!(result2, "prefix_another_value_suffix");

        env::remove_var("TEST_VAR2");
    }

    #[test]
    fn test_substitute_env_var_missing() {
        let result = substitute_env_var("prefix_${MISSING_VAR}_suffix");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("MISSING_VAR"));
    }

    #[test]
    fn test_substitute_env_var_multiple() {
        env::set_var("VAR1", "first");
        env::set_var("VAR2", "second");

        let result = substitute_env_var("${VAR1}_and_${VAR2}").unwrap();
        assert_eq!(result, "first_and_second");

        env::remove_var("VAR1");
        env::remove_var("VAR2");
    }

    #[test]
    fn test_config_validate_default_profile_missing() {
        let mut config = ArniConfig::new();
        config.default_profile = "nonexistent".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_config_validate_profile_connections() {
        let mut config = ArniConfig::new();
        let mut profile = ConfigProfile::new();

        // Add invalid connection (empty ID)
        profile.add_connection(ConnectionConfig {
            id: "".to_string(),
            name: "Invalid".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "db".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        });

        config.profiles.insert("default".to_string(), profile);

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_postgres_connection() {
        let config = ConnectionConfig {
            id: "valid-pg".to_string(),
            name: "Valid Postgres".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "testdb".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let result = validate_connection(&config);
        assert!(result.is_ok());
    }
}
