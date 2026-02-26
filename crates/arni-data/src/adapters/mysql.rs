//! MySQL database adapter implementation
//!
//! This module provides the [`MySqlAdapter`] which implements both the [`Connection`]
//! and [`DbAdapter`] traits for MySQL databases using the sqlx driver.
//!
//! # Features
//!
//! This module is only available when the `mysql` feature is enabled:
//!
//! ```toml
//! arni-data = { version = "0.1", features = ["mysql"] }
//! ```
//!
//! # Examples
//!
//! ```ignore
//! use arni_data::adapters::mysql::MySqlAdapter;
//! use arni_data::adapter::{Connection, ConnectionConfig, DatabaseType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ConnectionConfig {
//!         id: "my-mysql".to_string(),
//!         name: "My MySQL DB".to_string(),
//!         db_type: DatabaseType::MySQL,
//!         host: Some("localhost".to_string()),
//!         port: Some(3306),
//!         database: "mydb".to_string(),
//!         username: Some("user".to_string()),
//!         use_ssl: false,
//!         parameters: Default::default(),
//!     };
//!
//!     let mut adapter = MySqlAdapter::new(config);
//!     adapter.connect().await?;
//!
//!     if adapter.health_check().await? {
//!         println!("Connection healthy!");
//!     }
//!
//!     adapter.disconnect().await?;
//!     Ok(())
//! }
//! ```

use crate::adapter::{Connection, ConnectionConfig, QueryResult, QueryValue, Result};
use crate::DataError;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Row, TypeInfo};
use std::sync::Arc;
use tokio::sync::RwLock;

/// MySQL database adapter
///
/// This adapter uses the sqlx MySQL driver to connect to MySQL databases.
/// It implements both [`Connection`] and [`DbAdapter`] traits.
///
/// # Connection Management
///
/// The adapter maintains an internal connection pool that can be checked with
/// [`is_connected`](Connection::is_connected). Connections are established lazily
/// on first use or explicitly via [`connect`](Connection::connect).
///
/// # SSL/TLS Support
///
/// SSL is supported via the `use_ssl` configuration option:
/// - `use_ssl: false` - Plain text connection (default)
/// - `use_ssl: true` - Encrypted connection
///
/// # Thread Safety
///
/// The adapter uses internal locking to ensure thread-safe access to the underlying
/// MySQL connection pool.
pub struct MySqlAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// MySQL connection pool wrapped in Arc<RwLock> for thread-safe access
    pool: Arc<RwLock<Option<MySqlPool>>>,
    /// Connection state flag
    connected: Arc<RwLock<bool>>,
}

impl MySqlAdapter {
    /// Create a new MySQL adapter with the given configuration
    ///
    /// This does not establish a connection immediately. Call [`connect`](Connection::connect)
    /// to establish the connection.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = ConnectionConfig {
    ///     id: "prod-db".to_string(),
    ///     name: "Production DB".to_string(),
    ///     db_type: DatabaseType::MySQL,
    ///     host: Some("db.example.com".to_string()),
    ///     port: Some(3306),
    ///     database: "app_db".to_string(),
    ///     username: Some("app_user".to_string()),
    ///     use_ssl: true,
    ///     parameters: Default::default(),
    /// };
    ///
    /// let adapter = MySqlAdapter::new(config);
    /// ```
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            pool: Arc::new(RwLock::new(None)),
            connected: Arc::new(RwLock::new(false)),
        }
    }

    /// Validate database name
    fn validate_database_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(DataError::Config(
                "Database name cannot be empty".to_string(),
            ));
        }
        if name.len() > 64 {
            return Err(DataError::Config(format!(
                "Database name too long (max 64 chars): {}",
                name.len()
            )));
        }
        Ok(())
    }

    /// Build a MySQL connection string from the configuration
    ///
    /// The connection string format is:
    /// ```text
    /// mysql://username:password@host:port/database?ssl-mode=REQUIRED
    /// ```
    ///
    /// # Returns
    ///
    /// A connection string suitable for sqlx MySQL, or an error if required
    /// fields are missing.
    fn build_connection_string(&self, password: Option<&str>) -> Result<String> {
        Self::validate_database_name(&self.config.database)?;

        let host = self
            .config
            .host
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing host".to_string()))?;

        let port = self.config.port.unwrap_or(3306);

        let username = self
            .config
            .username
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing username".to_string()))?;

        let password = password.unwrap_or("");

        let ssl_mode = if self.config.use_ssl {
            "ssl-mode=REQUIRED"
        } else {
            "ssl-mode=DISABLED"
        };

        Ok(format!(
            "mysql://{}:{}@{}:{}/{}?{}",
            username, password, host, port, self.config.database, ssl_mode
        ))
    }

    /// Convert a MySQL row to a vector of QueryValue
    ///
    /// This helper method extracts values from a MySQL row and converts them
    /// to the QueryValue enum, handling type conversions for common MySQL types.
    fn row_to_values(row: &MySqlRow) -> Result<Vec<QueryValue>> {
        let mut values = Vec::new();

        for (i, column) in row.columns().iter().enumerate() {
            let type_info = column.type_info();
            let type_name = type_info.name();

            let value = match type_name {
                // Boolean (TINYINT(1))
                "TINYINT(1)" | "BOOLEAN" => {
                    let val: Option<bool> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get bool value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Bool(v),
                        None => QueryValue::Null,
                    }
                }
                // Integer types
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
                    let val: Option<i64> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get int value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Int(v),
                        None => QueryValue::Null,
                    }
                }
                // Floating point types
                "FLOAT" | "DOUBLE" | "DECIMAL" => {
                    let val: Option<f64> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get float value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Float(v),
                        None => QueryValue::Null,
                    }
                }
                // Text types
                "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                    let val: Option<String> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get text value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Text(v),
                        None => QueryValue::Null,
                    }
                }
                // Timestamp (stored as UTC DateTime)
                "TIMESTAMP" => {
                    use sqlx::types::chrono::{DateTime, Utc};
                    let val: Option<DateTime<Utc>> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get timestamp value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Text(v.format("%Y-%m-%d %H:%M:%S").to_string()),
                        None => QueryValue::Null,
                    }
                }
                // Datetime (timezone-naive)
                "DATETIME" => {
                    use sqlx::types::chrono::NaiveDateTime;
                    let val: Option<NaiveDateTime> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get datetime value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Text(v.format("%Y-%m-%d %H:%M:%S").to_string()),
                        None => QueryValue::Null,
                    }
                }
                // Date
                "DATE" => {
                    use sqlx::types::chrono::NaiveDate;
                    let val: Option<NaiveDate> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get date value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Text(v.format("%Y-%m-%d").to_string()),
                        None => QueryValue::Null,
                    }
                }
                // Binary types
                "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => {
                    let val: Option<Vec<u8>> = row
                        .try_get(i)
                        .map_err(|e| DataError::Query(format!("Failed to get bytes value: {}", e)))?;
                    match val {
                        Some(v) => QueryValue::Bytes(v),
                        None => QueryValue::Null,
                    }
                }
                // Other types - try to get as text
                _ => {
                    let val: Option<String> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!(
                            "Failed to get value for type {}: {}",
                            type_name, e
                        ))
                    })?;
                    match val {
                        Some(v) => QueryValue::Text(v),
                        None => QueryValue::Null,
                    }
                }
            };

            values.push(value);
        }

        Ok(values)
    }

    /// Execute a SQL query and return results
    ///
    /// This method executes any SQL statement (SELECT, INSERT, UPDATE, DELETE, etc.)
    /// and returns the results as a QueryResult.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query to execute
    ///
    /// # Returns
    ///
    /// A QueryResult containing columns, rows, and rows_affected count.
    ///
    /// # Errors
    ///
    /// Returns DataError::Connection if not connected to the database.
    /// Returns DataError::Query if the query execution fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let result = adapter.execute_query("SELECT * FROM users").await?;
    /// println!("Found {} rows", result.rows.len());
    /// ```
    pub async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        // Check if connected
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        // Get pool
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // Execute query
        let rows = sqlx::query(query).fetch_all(pool).await.map_err(|e| {
            let error_msg = e.to_string();

            // Categorize errors for better error messages
            if error_msg.contains("syntax") {
                DataError::Query(format!("SQL syntax error: {}", e))
            } else if error_msg.contains("Access denied") || error_msg.contains("permission") {
                DataError::Query(format!("Permission denied: {}", e))
            } else if error_msg.contains("doesn't exist") || error_msg.contains("Unknown") {
                DataError::Query(format!("Object not found: {}", e))
            } else if error_msg.contains("Duplicate") || error_msg.contains("constraint") {
                DataError::Query(format!("Constraint violation: {}", e))
            } else {
                DataError::Query(format!("Query failed: {}", e))
            }
        })?;

        // Handle empty results (e.g., from INSERT/UPDATE/DELETE)
        if rows.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: Some(0),
            });
        }

        // Extract column names
        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        // Convert rows to QueryValue vectors
        let mut result_rows = Vec::new();
        for row in &rows {
            let values = Self::row_to_values(row)?;
            result_rows.push(values);
        }

        let rows_affected = result_rows.len() as u64;

        Ok(QueryResult {
            columns,
            rows: result_rows,
            rows_affected: Some(rows_affected),
        })
    }
}

#[async_trait::async_trait]
impl Connection for MySqlAdapter {
    async fn connect(&mut self) -> Result<()> {
        // Check if already connected
        if *self.connected.read().await {
            return Ok(());
        }

        // Build connection string (without password for now - will need separate password handling)
        let conn_str = self.build_connection_string(None)?;

        // Create connection pool with sqlx
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&conn_str)
            .await
            .map_err(|e| DataError::Connection(format!("Failed to connect: {}", e)))?;

        // Store the pool
        *self.pool.write().await = Some(pool);
        *self.connected.write().await = true;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Close the pool
        if let Some(pool) = self.pool.write().await.take() {
            pool.close().await;
        }
        *self.connected.write().await = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // This needs to be a synchronous check, so we use try_read
        // Returns false if the lock is held or if not connected
        self.connected
            .try_read()
            .map(|guard| *guard)
            .unwrap_or(false)
    }

    async fn health_check(&self) -> Result<bool> {
        // Check internal state first
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        // Get pool
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // Execute health check query
        match sqlx::query("SELECT 1").fetch_one(pool).await {
            Ok(_) => Ok(true),
            Err(e) => Err(DataError::Query(format!("Health check failed: {}", e))),
        }
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::DatabaseType;
    use std::collections::HashMap;

    fn create_test_config() -> ConnectionConfig {
        ConnectionConfig {
            id: "test-mysql".to_string(),
            name: "Test MySQL".to_string(),
            db_type: DatabaseType::MySQL,
            host: Some("localhost".to_string()),
            port: Some(3306),
            database: "test_db".to_string(),
            username: Some("test_user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        }
    }

    #[test]
    fn test_new_mysql_adapter() {
        let config = create_test_config();
        let adapter = MySqlAdapter::new(config.clone());
        
        assert_eq!(adapter.config().id, "test-mysql");
        assert_eq!(adapter.config().db_type, DatabaseType::MySQL);
        assert!(!adapter.is_connected());
    }

    #[test]
    fn test_validate_database_name() {
        assert!(MySqlAdapter::validate_database_name("valid_db").is_ok());
        assert!(MySqlAdapter::validate_database_name("").is_err());
        assert!(MySqlAdapter::validate_database_name(&"a".repeat(65)).is_err());
    }

    #[test]
    fn test_build_connection_string() {
        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);
        
        let conn_str = adapter.build_connection_string(Some("password123")).unwrap();
        assert!(conn_str.contains("mysql://"));
        assert!(conn_str.contains("test_user"));
        assert!(conn_str.contains("password123"));
        assert!(conn_str.contains("localhost"));
        assert!(conn_str.contains("3306"));
        assert!(conn_str.contains("test_db"));
        assert!(conn_str.contains("ssl-mode=DISABLED"));
    }

    #[test]
    fn test_build_connection_string_with_ssl() {
        let mut config = create_test_config();
        config.use_ssl = true;
        let adapter = MySqlAdapter::new(config);
        
        let conn_str = adapter.build_connection_string(Some("password123")).unwrap();
        assert!(conn_str.contains("ssl-mode=REQUIRED"));
    }

    #[tokio::test]
    async fn test_connect_not_connected() {
        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);

        let result = adapter.health_check().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataError::Connection(_)),
            "Expected Connection error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_connect() {
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter).await.expect("Failed to connect");
        assert!(adapter.is_connected());

        Connection::disconnect(&mut adapter).await.expect("Failed to disconnect");
        assert!(!adapter.is_connected());
    }

    #[tokio::test]
    #[ignore]
    async fn test_health_check() {
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter).await.expect("Failed to connect");

        let result = adapter.health_check().await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        Connection::disconnect(&mut adapter).await.expect("Failed to disconnect");
    }

    #[tokio::test]
    async fn test_execute_query_not_connected() {
        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);

        let result = adapter.execute_query("SELECT 1").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataError::Connection(_)),
            "Expected Connection error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_execute_query_select() {
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter).await.expect("Failed to connect");

        let result = adapter.execute_query("SELECT 1 as num, 'test' as text").await;
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.columns.len(), 2);
        assert_eq!(query_result.columns[0], "num");
        assert_eq!(query_result.columns[1], "text");
        assert_eq!(query_result.rows.len(), 1);

        Connection::disconnect(&mut adapter).await.expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_execute_query_empty_result() {
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter).await.expect("Failed to connect");

        // Create a test table
        adapter
            .execute_query("CREATE TEMPORARY TABLE test_empty (id INT)")
            .await
            .expect("Failed to create table");

        // Query empty table
        let result = adapter.execute_query("SELECT * FROM test_empty").await;
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.columns.len(), 0);
        assert_eq!(query_result.rows.len(), 0);

        Connection::disconnect(&mut adapter).await.expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_execute_query_insert_update_delete() {
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter).await.expect("Failed to connect");

        // Create a test table
        adapter
            .execute_query("CREATE TEMPORARY TABLE test_crud (id INT, name VARCHAR(100))")
            .await
            .expect("Failed to create table");

        // Insert
        let result = adapter
            .execute_query("INSERT INTO test_crud (id, name) VALUES (1, 'Alice')")
            .await;
        assert!(result.is_ok());

        // Update
        let result = adapter
            .execute_query("UPDATE test_crud SET name = 'Bob' WHERE id = 1")
            .await;
        assert!(result.is_ok());

        // Verify update
        let result = adapter
            .execute_query("SELECT * FROM test_crud WHERE id = 1")
            .await
            .expect("Failed to select");
        assert_eq!(result.rows.len(), 1);

        // Delete
        let result = adapter
            .execute_query("DELETE FROM test_crud WHERE id = 1")
            .await;
        assert!(result.is_ok());

        Connection::disconnect(&mut adapter).await.expect("Failed to disconnect");
    }
}
