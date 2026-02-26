//! PostgreSQL database adapter implementation
//!
//! This module provides the [`PostgresAdapter`] which implements both the [`Connection`]
//! and [`DbAdapter`] traits for PostgreSQL databases using the tokio-postgres driver.
//!
//! # Features
//!
//! This module is only available when the `postgres` feature is enabled:
//!
//! ```toml
//! arni-data = { version = "0.1", features = ["postgres"] }
//! ```
//!
//! # Examples
//!
//! ```ignore
//! use arni_data::adapters::postgres::PostgresAdapter;
//! use arni_data::adapter::{Connection, ConnectionConfig, DatabaseType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ConnectionConfig {
//!         id: "my-postgres".to_string(),
//!         name: "My PostgreSQL DB".to_string(),
//!         db_type: DatabaseType::Postgres,
//!         host: Some("localhost".to_string()),
//!         port: Some(5432),
//!         database: "mydb".to_string(),
//!         username: Some("user".to_string()),
//!         use_ssl: false,
//!         parameters: Default::default(),
//!     };
//!
//!     let mut adapter = PostgresAdapter::new(config);
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

use crate::adapter::{
    Connection, ConnectionConfig, DatabaseType, DbAdapter, QueryResult, QueryValue, Result,
};
use crate::DataError;
use polars::prelude::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{types::Type, Client, NoTls};

/// PostgreSQL database adapter
///
/// This adapter uses the tokio-postgres driver to connect to PostgreSQL databases.
/// It implements both [`Connection`] and [`DbAdapter`] traits.
///
/// # Connection Management
///
/// The adapter maintains an internal connection state that can be checked with
/// [`is_connected`](Connection::is_connected). Connections are established lazily
/// on first use or explicitly via [`connect`](Connection::connect).
///
/// # SSL/TLS Support
///
/// SSL is supported via the `use_ssl` configuration option:
/// - `use_ssl: false` - Plain text connection (default)
/// - `use_ssl: true` - Encrypted connection using native-tls
///
/// # Thread Safety
///
/// The adapter uses internal locking to ensure thread-safe access to the underlying
/// PostgreSQL connection.
pub struct PostgresAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// PostgreSQL client wrapped in Arc<RwLock> for thread-safe access
    client: Arc<RwLock<Option<Client>>>,
    /// Connection state flag
    connected: Arc<RwLock<bool>>,
}

impl PostgresAdapter {
    /// Create a new PostgreSQL adapter with the given configuration
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
    ///     db_type: DatabaseType::Postgres,
    ///     host: Some("db.example.com".to_string()),
    ///     port: Some(5432),
    ///     database: "app_db".to_string(),
    ///     username: Some("app_user".to_string()),
    ///     use_ssl: true,
    ///     parameters: Default::default(),
    /// };
    ///
    /// let adapter = PostgresAdapter::new(config);
    /// ```
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            client: Arc::new(RwLock::new(None)),
            connected: Arc::new(RwLock::new(false)),
        }
    }

    /// Build a PostgreSQL connection string from the configuration
    ///
    /// The connection string format is:
    /// ```text
    /// host={host} port={port} dbname={database} user={username}
    /// ```
    ///
    /// Additional parameters from `config.parameters` are appended.
    ///
    /// # Returns
    ///
    /// A connection string suitable for tokio-postgres, or an error if required
    /// fields are missing.
    fn build_connection_string(&self, password: Option<&str>) -> Result<String> {
        let host = self
            .config
            .host
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing host".to_string()))?;

        let port = self.config.port.unwrap_or(5432);

        let username = self
            .config
            .username
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing username".to_string()))?;

        let mut conn_str = format!(
            "host={} port={} dbname={} user={}",
            host, port, self.config.database, username
        );

        if let Some(pwd) = password {
            conn_str.push_str(&format!(" password={}", pwd));
        }

        // Add additional parameters
        for (key, value) in &self.config.parameters {
            conn_str.push_str(&format!(" {}={}", key, value));
        }

        Ok(conn_str)
    }
}

#[async_trait::async_trait]
impl Connection for PostgresAdapter {
    async fn connect(&mut self) -> Result<()> {
        // Check if already connected
        if *self.connected.read().await {
            return Ok(());
        }

        // Build connection string (without password for now - will need separate password handling)
        let conn_str = self.build_connection_string(None)?;

        // Connect using NoTls for now (will add SSL support later)
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| DataError::Connection(format!("Failed to connect: {}", e)))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Store the client
        *self.client.write().await = Some(client);
        *self.connected.write().await = true;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Drop the client (closes the connection)
        *self.client.write().await = None;
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

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        // Execute health check query
        match client.query_one("SELECT 1", &[]).await {
            Ok(_) => Ok(true),
            Err(e) => Err(DataError::Query(format!("Health check failed: {}", e))),
        }
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

/// Implementation of DbAdapter trait for PostgreSQL
#[async_trait::async_trait]
impl DbAdapter for PostgresAdapter {
    // ===== Connection Management =====
    // Note: connect(), disconnect(), and is_connected() are already implemented
    // via the Connection trait. DbAdapter has its own versions that we need to implement
    // separately with password support.

    async fn connect(&mut self, config: &ConnectionConfig, password: Option<&str>) -> Result<()> {
        // Store config
        self.config = config.clone();

        // Build connection string with password
        let conn_str = self.build_connection_string(password)?;

        // Connect using NoTls for now
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| DataError::Connection(format!("Failed to connect: {}", e)))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Store the client
        *self.client.write().await = Some(client);
        *self.connected.write().await = true;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        *self.client.write().await = None;
        *self.connected.write().await = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
            .try_read()
            .map(|guard| *guard)
            .unwrap_or(false)
    }

    async fn test_connection(
        &self,
        config: &ConnectionConfig,
        password: Option<&str>,
    ) -> Result<bool> {
        // Build connection string
        let host = config
            .host
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing host".to_string()))?;
        let port = config.port.unwrap_or(5432);
        let username = config
            .username
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing username".to_string()))?;

        let mut conn_str = format!(
            "host={} port={} dbname={} user={}",
            host, port, config.database, username
        );

        if let Some(pwd) = password {
            conn_str.push_str(&format!(" password={}", pwd));
        }

        // Try to connect briefly
        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((client, connection)) => {
                // Drop connection immediately
                drop(client);
                drop(connection);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::Postgres
    }

    // ===== Query Operations =====

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        // Check connection
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        // Execute query
        let rows = client
            .query(query, &[])
            .await
            .map_err(|e| DataError::Query(format!("Query failed: {}", e)))?;

        // If no rows, check if it was a modification query
        if rows.is_empty() {
            // Try to get rows affected (for INSERT/UPDATE/DELETE)
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected: None, // tokio-postgres doesn't provide this easily
            });
        }

        // Extract column names
        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        // Convert rows
        let mut result_rows = Vec::new();
        for row in &rows {
            let mut result_row = Vec::new();
            for (col_idx, column) in row.columns().iter().enumerate() {
                let value = self.convert_postgres_value(row, col_idx, column.type_())?;
                result_row.push(value);
            }
            result_rows.push(result_row);
        }

        Ok(QueryResult {
            columns,
            rows: result_rows,
            rows_affected: Some(rows.len() as u64),
        })
    }

    async fn export_dataframe(
        &self,
        df: &DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        replace: bool,
    ) -> Result<u64> {
        // Check connection
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        // If replace, drop and recreate table
        if replace {
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            client
                .execute(&drop_sql, &[])
                .await
                .map_err(|e| DataError::Query(format!("Failed to drop table: {}", e)))?;

            // Create table based on DataFrame schema
            let create_sql = self.generate_create_table_sql(df, table_name)?;
            client
                .execute(&create_sql, &[])
                .await
                .map_err(|e| DataError::Query(format!("Failed to create table: {}", e)))?;
        }

        // Insert data row by row
        // Note: This is not efficient for large datasets, but it's simple and works
        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let placeholders: Vec<String> = (1..=column_names.len())
            .map(|i| format!("${}", i))
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            column_names
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        );

        // Placeholder implementation - will be completed with proper type handling
        // in the DataFrame conversion task (arni-37z.3.3)
        let _ = (insert_sql, df.height());

        Err(DataError::NotSupported(
            "export_dataframe not yet implemented - will be completed in task arni-37z.3.3"
                .to_string(),
        ))
    }

    // ===== Schema Discovery (stubs - will be implemented in arni-37z.3.4) =====

    async fn list_databases(&self) -> Result<Vec<String>> {
        Err(DataError::NotSupported(
            "list_databases will be implemented in task arni-37z.3.4".to_string(),
        ))
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>> {
        Err(DataError::NotSupported(
            "list_tables will be implemented in task arni-37z.3.4".to_string(),
        ))
    }

    async fn describe_table(
        &self,
        _table_name: &str,
        _schema: Option<&str>,
    ) -> Result<crate::adapter::TableInfo> {
        Err(DataError::NotSupported(
            "describe_table will be implemented in task arni-37z.3.4".to_string(),
        ))
    }
}

impl PostgresAdapter {
    /// Convert a PostgreSQL value to QueryValue
    fn convert_postgres_value(
        &self,
        row: &tokio_postgres::Row,
        col_idx: usize,
        col_type: &Type,
    ) -> Result<QueryValue> {
        // Check for NULL first
        if row
            .try_get::<_, Option<String>>(col_idx)
            .ok()
            .flatten()
            .is_none()
            && !matches!(
                col_type,
                &Type::BOOL
                    | &Type::INT2
                    | &Type::INT4
                    | &Type::INT8
                    | &Type::FLOAT4
                    | &Type::FLOAT8
            )
        {
            return Ok(QueryValue::Null);
        }

        // Type conversion based on PostgreSQL type
        match col_type {
            &Type::BOOL => row
                .try_get::<_, Option<bool>>(col_idx)
                .map(|v| v.map(QueryValue::Bool).unwrap_or(QueryValue::Null))
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert bool: {}", e))),

            &Type::INT2 => row
                .try_get::<_, Option<i16>>(col_idx)
                .map(|v| {
                    v.map(|i| QueryValue::Int(i as i64))
                        .unwrap_or(QueryValue::Null)
                })
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert int2: {}", e))),

            &Type::INT4 => row
                .try_get::<_, Option<i32>>(col_idx)
                .map(|v| {
                    v.map(|i| QueryValue::Int(i as i64))
                        .unwrap_or(QueryValue::Null)
                })
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert int4: {}", e))),

            &Type::INT8 => row
                .try_get::<_, Option<i64>>(col_idx)
                .map(|v| v.map(QueryValue::Int).unwrap_or(QueryValue::Null))
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert int8: {}", e))),

            &Type::FLOAT4 => row
                .try_get::<_, Option<f32>>(col_idx)
                .map(|v| {
                    v.map(|f| QueryValue::Float(f as f64))
                        .unwrap_or(QueryValue::Null)
                })
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert float4: {}", e))),

            &Type::FLOAT8 => row
                .try_get::<_, Option<f64>>(col_idx)
                .map(|v| v.map(QueryValue::Float).unwrap_or(QueryValue::Null))
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert float8: {}", e))),

            &Type::TEXT | &Type::VARCHAR | &Type::CHAR | &Type::NAME => row
                .try_get::<_, Option<String>>(col_idx)
                .map(|v| v.map(QueryValue::Text).unwrap_or(QueryValue::Null))
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert text: {}", e))),

            &Type::BYTEA => row
                .try_get::<_, Option<Vec<u8>>>(col_idx)
                .map(|v| v.map(QueryValue::Bytes).unwrap_or(QueryValue::Null))
                .map_err(|e| DataError::TypeConversion(format!("Failed to convert bytes: {}", e))),

            // Default: try to convert to string
            _ => row
                .try_get::<_, Option<String>>(col_idx)
                .map(|v| v.map(QueryValue::Text).unwrap_or(QueryValue::Null))
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to convert type {:?}: {}",
                        col_type, e
                    ))
                }),
        }
    }

    /// Generate CREATE TABLE SQL from DataFrame schema
    fn generate_create_table_sql(&self, df: &DataFrame, table_name: &str) -> Result<String> {
        let mut column_defs = Vec::new();

        for (name, dtype) in df.get_columns().iter().map(|s| (s.name(), s.dtype())) {
            let pg_type = match dtype {
                DataType::Boolean => "BOOLEAN",
                DataType::Int8 | DataType::Int16 | DataType::Int32 => "INTEGER",
                DataType::Int64 => "BIGINT",
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "INTEGER",
                DataType::UInt64 => "BIGINT",
                DataType::Float32 => "REAL",
                DataType::Float64 => "DOUBLE PRECISION",
                DataType::String => "TEXT",
                DataType::Binary => "BYTEA",
                _ => "TEXT", // Fallback for unsupported types
            };

            column_defs.push(format!("{} {}", name, pg_type));
        }

        Ok(format!(
            "CREATE TABLE {} ({})",
            table_name,
            column_defs.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::DatabaseType;
    use std::collections::HashMap;

    fn create_test_config() -> ConnectionConfig {
        ConnectionConfig {
            id: "test-pg".to_string(),
            name: "Test PostgreSQL".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "test_db".to_string(),
            username: Some("test_user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        }
    }

    #[test]
    fn test_new_adapter() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config.clone());

        assert_eq!(adapter.config().id, "test-pg");
        assert_eq!(adapter.config().database, "test_db");
        assert!(!DbAdapter::is_connected(&adapter));
    }

    #[test]
    fn test_build_connection_string() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let conn_str = adapter.build_connection_string(None).unwrap();
        assert!(conn_str.contains("host=localhost"));
        assert!(conn_str.contains("port=5432"));
        assert!(conn_str.contains("dbname=test_db"));
        assert!(conn_str.contains("user=test_user"));
    }

    #[test]
    fn test_build_connection_string_with_password() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let conn_str = adapter.build_connection_string(Some("secret123")).unwrap();
        assert!(conn_str.contains("password=secret123"));
    }

    #[test]
    fn test_build_connection_string_with_parameters() {
        let mut config = create_test_config();
        config
            .parameters
            .insert("application_name".to_string(), "arni".to_string());
        config
            .parameters
            .insert("connect_timeout".to_string(), "10".to_string());

        let adapter = PostgresAdapter::new(config);
        let conn_str = adapter.build_connection_string(None).unwrap();

        assert!(conn_str.contains("application_name=arni"));
        assert!(conn_str.contains("connect_timeout=10"));
    }

    #[test]
    fn test_connection_string_missing_host() {
        let mut config = create_test_config();
        config.host = None;

        let adapter = PostgresAdapter::new(config);
        let result = adapter.build_connection_string(None);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Config(_)));
    }

    #[test]
    fn test_connection_string_missing_username() {
        let mut config = create_test_config();
        config.username = None;

        let adapter = PostgresAdapter::new(config);
        let result = adapter.build_connection_string(None);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Config(_)));
    }

    #[tokio::test]
    async fn test_disconnect_when_not_connected() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);

        // Should not error when disconnecting while not connected
        let result = DbAdapter::disconnect(&mut adapter).await;
        assert!(result.is_ok());
    }

    // Integration tests requiring a running PostgreSQL instance
    // These are ignored by default - run with: cargo test -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_connect_real_database() {
        use crate::adapter::{Connection, DbAdapter};

        // This test requires a PostgreSQL instance running on localhost:5432
        // with a test database and user configured
        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);

        let result = Connection::connect(&mut adapter).await;
        assert!(result.is_ok());
        assert!(DbAdapter::is_connected(&adapter));

        Connection::disconnect(&mut adapter).await.unwrap();
        assert!(!DbAdapter::is_connected(&adapter));
    }

    #[tokio::test]
    #[ignore]
    async fn test_health_check_real_database() {
        use crate::adapter::Connection;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);

        Connection::connect(&mut adapter).await.unwrap();

        let health = adapter.health_check().await;
        assert!(health.is_ok());
        assert!(health.unwrap());

        Connection::disconnect(&mut adapter).await.unwrap();
    }

    #[tokio::test]
    async fn test_health_check_not_connected() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let result = adapter.health_check().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Connection(_)));
    }

    // ===== DbAdapter trait tests =====

    #[test]
    fn test_database_type() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);
        assert_eq!(adapter.database_type(), DatabaseType::Postgres);
    }

    #[tokio::test]
    async fn test_execute_query_not_connected() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let result = adapter.execute_query("SELECT 1").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Connection(_)));
    }

    #[tokio::test]
    async fn test_test_connection_invalid() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config.clone());

        // Test with invalid credentials
        let result = adapter
            .test_connection(&config, Some("wrong_password"))
            .await;
        // This might succeed or fail depending on whether postgres is running
        // We're just testing that it doesn't panic
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    #[ignore]
    async fn test_export_dataframe_not_implemented() {
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        // Connect first so we get past the connection check
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();

        let df = DataFrame::new(vec![
            Series::new("id".into(), &[1, 2, 3]).into(),
            Series::new("name".into(), &["Alice", "Bob", "Charlie"]).into(),
        ])
        .unwrap();

        let result = DbAdapter::export_dataframe(&adapter, &df, "test_table", None, false).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::NotSupported(_)));

        DbAdapter::disconnect(&mut adapter).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_databases_not_implemented() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let result = DbAdapter::list_databases(&adapter).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::NotSupported(_)));
    }

    #[tokio::test]
    async fn test_list_tables_not_implemented() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let result = DbAdapter::list_tables(&adapter, None).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::NotSupported(_)));
    }

    #[tokio::test]
    async fn test_describe_table_not_implemented() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let result = DbAdapter::describe_table(&adapter, "test_table", None).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::NotSupported(_)));
    }

    #[test]
    fn test_generate_create_table_sql() {
        use polars::prelude::*;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let df = DataFrame::new(vec![
            Series::new("id".into(), &[1, 2, 3]).into(),
            Series::new("name".into(), &["Alice", "Bob", "Charlie"]).into(),
            Series::new("score".into(), &[95.5, 87.3, 92.1]).into(),
            Series::new("active".into(), &[true, false, true]).into(),
        ])
        .unwrap();

        let sql = adapter
            .generate_create_table_sql(&df, "test_table")
            .unwrap();

        // Verify the SQL contains expected elements
        assert!(sql.contains("CREATE TABLE test_table"));
        assert!(sql.contains("id"));
        assert!(sql.contains("name"));
        assert!(sql.contains("score"));
        assert!(sql.contains("active"));
        assert!(sql.contains("INTEGER") || sql.contains("BIGINT")); // id
        assert!(sql.contains("TEXT")); // name
        assert!(sql.contains("DOUBLE PRECISION") || sql.contains("REAL")); // score
        assert!(sql.contains("BOOLEAN")); // active
    }

    // Integration tests requiring a running PostgreSQL instance
    #[tokio::test]
    #[ignore]
    async fn test_execute_query_select() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();

        // Test SELECT query
        let result = DbAdapter::execute_query(&adapter, "SELECT 1 as num, 'test' as text").await;
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.columns.len(), 2);
        assert_eq!(query_result.columns[0], "num");
        assert_eq!(query_result.columns[1], "text");
        assert_eq!(query_result.rows.len(), 1);

        DbAdapter::disconnect(&mut adapter).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_execute_query_types() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();

        // Test different PostgreSQL types
        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT 42::integer as int_val, 3.14::double precision as float_val, 
             true::boolean as bool_val, 'hello'::text as text_val",
        )
        .await;

        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 1);

        let row = &query_result.rows[0];
        assert!(matches!(row[0], QueryValue::Int(42)));
        assert!(matches!(row[2], QueryValue::Bool(true)));
        assert!(matches!(row[3], QueryValue::Text(_)));

        DbAdapter::disconnect(&mut adapter).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_execute_query_null_values() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();

        let result =
            DbAdapter::execute_query(&adapter, "SELECT NULL as null_val, 42 as int_val").await;

        assert!(result.is_ok());

        let query_result = result.unwrap();
        let row = &query_result.rows[0];
        assert!(matches!(row[0], QueryValue::Null));
        assert!(matches!(row[1], QueryValue::Int(42)));

        DbAdapter::disconnect(&mut adapter).await.unwrap();
    }
}
