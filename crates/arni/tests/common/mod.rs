//! Common utilities for integration tests
//!
//! This module provides shared utilities, fixtures, and helpers
//! used across all integration tests.

use std::env;

/// Test database configuration
#[derive(Debug, Clone)]
pub struct TestDbConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

impl TestDbConfig {
    /// Create PostgreSQL test configuration from environment
    pub fn postgres_from_env() -> Self {
        Self {
            host: env::var("TEST_POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("TEST_POSTGRES_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            database: env::var("TEST_POSTGRES_DB").unwrap_or_else(|_| "arni_test".to_string()),
            username: env::var("TEST_POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: env::var("TEST_POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
        }
    }

    /// Get connection string for PostgreSQL
    pub fn postgres_connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }

    /// Create MongoDB test configuration from environment
    pub fn mongodb_from_env() -> Self {
        Self {
            host: env::var("TEST_MONGODB_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("TEST_MONGODB_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(27017),
            database: env::var("TEST_MONGODB_DB").unwrap_or_else(|_| "arni_test".to_string()),
            username: env::var("TEST_MONGODB_USER").unwrap_or_else(|_| "mongo".to_string()),
            password: env::var("TEST_MONGODB_PASSWORD").unwrap_or_else(|_| "mongo".to_string()),
        }
    }

    /// Get connection string for MongoDB
    pub fn mongodb_connection_string(&self) -> String {
        format!(
            "mongodb://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

/// Check if a test database is available
///
/// Returns true if the database is reachable, false otherwise.
/// This is used to skip tests when the database is not available.
pub fn is_postgres_available() -> bool {
    env::var("TEST_POSTGRES_AVAILABLE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(false)
}

/// Check if MongoDB is available for testing
pub fn is_mongodb_available() -> bool {
    env::var("TEST_MONGODB_AVAILABLE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(false)
}

/// Test data cleanup utility
pub struct TestCleanup {
    tables: Vec<String>,
}

impl TestCleanup {
    /// Create a new cleanup utility
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    /// Register a table for cleanup
    pub fn register_table(&mut self, table: &str) {
        self.tables.push(table.to_string());
    }

    /// Get cleanup SQL statements
    pub fn cleanup_sql(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| format!("DROP TABLE IF EXISTS {} CASCADE", table))
            .collect()
    }
}

impl Default for TestCleanup {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_config() {
        let config = TestDbConfig::postgres_from_env();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        let conn_str = config.postgres_connection_string();
        assert!(conn_str.starts_with("postgresql://"));
    }

    #[test]
    fn test_mongodb_config() {
        let config = TestDbConfig::mongodb_from_env();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 27017);
        let conn_str = config.mongodb_connection_string();
        assert!(conn_str.starts_with("mongodb://"));
    }

    #[test]
    fn test_cleanup_utility() {
        let mut cleanup = TestCleanup::new();
        cleanup.register_table("test_table");
        cleanup.register_table("another_table");

        let sql = cleanup.cleanup_sql();
        assert_eq!(sql.len(), 2);
        assert!(sql[0].contains("DROP TABLE IF EXISTS test_table"));
        assert!(sql[1].contains("DROP TABLE IF EXISTS another_table"));
    }
}
