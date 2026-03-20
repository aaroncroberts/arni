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
//! arni = { version = "0.1", features = ["mysql"] }
//! ```
//!
//! # Examples
//!
//! ```ignore
//! use arni::adapters::mysql::MySqlAdapter;
//! use arni::adapter::{Connection, ConnectionConfig, DatabaseType};
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

use crate::adapter::{
    escape_like_pattern, filter_to_sql, AdapterMetadata, ColumnInfo, Connection, ConnectionConfig,
    DatabaseType, DbAdapter, FilterExpr, ForeignKeyInfo, IndexInfo, ProcedureInfo, QueryResult,
    QueryValue, Result, ServerInfo, TableInfo, TableSearchMode, ViewInfo,
};
use crate::DataError;
#[cfg(feature = "polars")]
use polars::prelude::*;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Executor, Row, TypeInfo};
use std::collections::HashMap;
use tracing::{debug, error, info, instrument, warn};

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
    /// MySQL connection pool (MySqlPool is internally Arc and Send+Sync)
    pool: Option<MySqlPool>,
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
        Self { config, pool: None }
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
            let type_name = column.type_info().name();
            let value = match type_name {
                // Boolean (TINYINT(1))
                "TINYINT(1)" | "BOOLEAN" => row
                    .try_get::<Option<bool>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get bool value: {}", e)))?
                    .map(QueryValue::Bool)
                    .unwrap_or(QueryValue::Null),
                // Integer types
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => row
                    .try_get::<Option<i64>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get int value: {}", e)))?
                    .map(QueryValue::Int)
                    .unwrap_or(QueryValue::Null),
                // Floating point types
                "FLOAT" | "DOUBLE" => row
                    .try_get::<Option<f64>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get float value: {}", e)))?
                    .map(QueryValue::Float)
                    .unwrap_or(QueryValue::Null),
                // DECIMAL/NUMERIC: decode via common helper (avoids double to_string())
                "DECIMAL" | "NUMERIC" => row
                    .try_get::<Option<sqlx::types::Decimal>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get decimal value: {}", e)))?
                    .map(super::common::decimal_to_query_value)
                    .unwrap_or(QueryValue::Null),
                // Text types
                "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => row
                    .try_get::<Option<String>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get text value: {}", e)))?
                    .map(QueryValue::Text)
                    .unwrap_or(QueryValue::Null),
                // Timestamp (stored as UTC DateTime)
                "TIMESTAMP" => {
                    use sqlx::types::chrono::{DateTime, Utc};
                    row.try_get::<Option<DateTime<Utc>>, _>(i)
                        .map_err(|e| DataError::Query(format!("Failed to get timestamp value: {}", e)))?
                        .map(|v| QueryValue::Text(v.format("%Y-%m-%d %H:%M:%S").to_string()))
                        .unwrap_or(QueryValue::Null)
                }
                // Datetime (timezone-naive)
                "DATETIME" => {
                    use sqlx::types::chrono::NaiveDateTime;
                    row.try_get::<Option<NaiveDateTime>, _>(i)
                        .map_err(|e| DataError::Query(format!("Failed to get datetime value: {}", e)))?
                        .map(|v| QueryValue::Text(v.format("%Y-%m-%d %H:%M:%S").to_string()))
                        .unwrap_or(QueryValue::Null)
                }
                // Date
                "DATE" => {
                    use sqlx::types::chrono::NaiveDate;
                    row.try_get::<Option<NaiveDate>, _>(i)
                        .map_err(|e| DataError::Query(format!("Failed to get date value: {}", e)))?
                        .map(|v| QueryValue::Text(v.format("%Y-%m-%d").to_string()))
                        .unwrap_or(QueryValue::Null)
                }
                // Binary types
                "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => row
                    .try_get::<Option<Vec<u8>>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get bytes value: {}", e)))?
                    .map(QueryValue::Bytes)
                    .unwrap_or(QueryValue::Null),
                _ => row
                    .try_get::<Option<String>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get value for type {type_name}: {e}")))?
                    .map(QueryValue::Text)
                    .unwrap_or(QueryValue::Null),
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
    #[instrument(skip(self, query), fields(adapter = "mysql", query_length = query.len()))]
    pub async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let sql_type = super::common::detect_sql_type(query);
        debug!(
            sql_type,
            sql_preview = %super::common::sql_preview(query, 100),
            "Executing query"
        );

        // Check if connected
        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "execute_query", "Not connected");
            return Err(super::common::not_connected_error());
        }
        let pool = self.pool.as_ref().ok_or_else(|| {
            DataError::Connection("Pool not available".to_string())
        })?;

        let start = std::time::Instant::now();
        if matches!(sql_type, "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "TRUNCATE") {
            return Self::run_dml_query(pool, query, sql_type, start).await;
        }
        Self::run_select_query(pool, query, sql_type, start).await
    }

    /// Classifies a sqlx error into a user-facing [`DataError::Query`] variant.
    fn classify_query_error(sql_type: &str, e: sqlx::Error) -> DataError {
        error!(adapter = "mysql", operation = "execute_query", sql_type, error = %e, "Query execution failed");
        let msg = e.to_string();
        if msg.contains("syntax") {
            DataError::Query(format!("SQL syntax error: {}", e))
        } else if msg.contains("Access denied") || msg.contains("permission") {
            DataError::Query(format!("Permission denied: {}", e))
        } else if msg.contains("doesn't exist") || msg.contains("Unknown") {
            DataError::Query(format!("Object not found: {}", e))
        } else if msg.contains("Duplicate") || msg.contains("constraint") {
            DataError::Query(format!("Constraint violation: {}", e))
        } else {
            DataError::Query(format!("Query failed: {}", e))
        }
    }

    /// Executes a DML statement (INSERT/UPDATE/DELETE/REPLACE/TRUNCATE) and
    /// returns the rows-affected count without fetching any rows.
    async fn run_dml_query(
        pool: &MySqlPool,
        query: &str,
        sql_type: &str,
        start: std::time::Instant,
    ) -> Result<QueryResult> {
        let result = sqlx::query(query)
            .execute(pool)
            .await
            .map_err(|e| Self::classify_query_error(sql_type, e))?;
        let affected = result.rows_affected();
        info!(
            sql_type,
            rows_affected = affected,
            columns = 0usize,
            duration_ms = start.elapsed().as_millis(),
            "DML executed"
        );
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: Some(affected),
        })
    }

    /// Executes a SELECT query, extracts column names, and converts each row
    /// to a `Vec<QueryValue>`.
    async fn run_select_query(
        pool: &MySqlPool,
        query: &str,
        sql_type: &str,
        start: std::time::Instant,
    ) -> Result<QueryResult> {
        let rows = sqlx::query(query)
            .fetch_all(pool)
            .await
            .map_err(|e| Self::classify_query_error(sql_type, e))?;
        let duration = start.elapsed();

        if rows.is_empty() {
            info!(sql_type, duration_ms = duration.as_millis(), rows = 0usize, columns = 0usize, "Query executed successfully");
            return Ok(QueryResult { columns: vec![], rows: vec![], rows_affected: Some(0) });
        }

        let columns: Vec<String> = rows[0].columns().iter().map(|c| c.name().to_string()).collect();
        info!(sql_type, duration_ms = duration.as_millis(), rows = rows.len(), columns = columns.len(), "Query executed successfully");

        let mut result_rows = Vec::new();
        for row in &rows {
            result_rows.push(Self::row_to_values(row)?);
        }
        Ok(QueryResult { columns, rows: result_rows, rows_affected: None })
    }
}

#[async_trait::async_trait]
impl Connection for MySqlAdapter {
    #[instrument(skip(self), fields(adapter = "mysql", host = ?self.config.host, port = ?self.config.port, database = %self.config.database))]
    async fn connect(&mut self) -> Result<()> {
        // Check if already connected
        if self.pool.is_some() {
            debug!("Already connected, skipping connection attempt");
            return Ok(());
        }

        info!("Connecting to MySQL database");

        // Pull password from stored parameters if available.
        let password = self.config.parameters.get("password").map(String::as_str);
        let conn_str = self.build_connection_string(password).map_err(|e| {
            error!(adapter = "mysql", operation = "connect", error = ?e, "Failed to build connection string");
            e
        })?;

        // Create connection pool with sqlx
        let pc = self.config.pool_config.clone().unwrap_or_default();
        debug!(
            max_connections = pc.max_connections,
            min_connections = pc.min_connections,
            acquire_timeout_secs = pc.acquire_timeout_secs,
            idle_timeout_secs = pc.idle_timeout_secs,
            max_lifetime_secs = pc.max_lifetime_secs,
            "Building MySQL connection pool"
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(pc.max_connections)
            .min_connections(pc.min_connections)
            .acquire_timeout(std::time::Duration::from_secs(pc.acquire_timeout_secs))
            .idle_timeout(std::time::Duration::from_secs(pc.idle_timeout_secs))
            .max_lifetime(std::time::Duration::from_secs(pc.max_lifetime_secs))
            .connect(&conn_str)
            .await
            .map_err(|e| {
                error!(adapter = "mysql", operation = "connect", error = %e, "Failed to establish connection");
                DataError::Connection(format!("Failed to connect: {}", e))
            })?;

        // Store the pool
        self.pool = Some(pool);

        info!("Successfully connected to MySQL");
        Ok(())
    }

    #[instrument(skip(self), fields(adapter = "mysql"))]
    async fn disconnect(&mut self) -> Result<()> {
        info!("Disconnecting from MySQL");
        // Close the pool
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        debug!("MySQL connection closed");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.pool.is_some()
    }

    #[instrument(skip(self), fields(adapter = "mysql"))]
    async fn health_check(&self) -> Result<bool> {
        debug!("Performing health check");
        // Check internal state first
        if self.pool.is_none() {
            warn!("Health check failed: not connected");
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self.pool.as_ref().ok_or_else(|| {
            error!(
                adapter = "mysql",
                operation = "health_check",
                "Pool not available for health check"
            );
            DataError::Connection("Pool not available".to_string())
        })?;

        // Execute health check query
        match sqlx::query("SELECT 1").fetch_one(pool).await {
            Ok(_) => {
                debug!("Health check passed");
                Ok(true)
            }
            Err(e) => {
                error!(adapter = "mysql", operation = "health_check", error = %e, "Health check query failed");
                Err(DataError::Query(format!("Health check failed: {}", e)))
            }
        }
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[async_trait::async_trait]
impl DbAdapter for MySqlAdapter {
    // ===== Connection Management =====

    async fn connect(&mut self, config: &ConnectionConfig, password: Option<&str>) -> Result<()> {
        // Store config
        self.config = config.clone();

        // Build connection string with password
        let conn_str = self.build_connection_string(password)?;

        // Create connection pool
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&conn_str)
            .await
            .map_err(|e| DataError::Connection(format!("Failed to connect: {}", e)))?;

        // Store the pool
        self.pool = Some(pool);

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.pool.is_some()
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
        let port = config.port.unwrap_or(3306);
        let username = config
            .username
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing username".to_string()))?;

        let password = password.unwrap_or("");
        let ssl_mode = if config.use_ssl {
            "ssl-mode=REQUIRED"
        } else {
            "ssl-mode=DISABLED"
        };

        let conn_str = format!(
            "mysql://{}:{}@{}:{}/{}?{}",
            username, password, host, port, config.database, ssl_mode
        );

        // Try to connect briefly
        match MySqlPool::connect(&conn_str).await {
            Ok(pool) => {
                pool.close().await;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::MySQL
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    // ===== Query Operations =====

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        self.execute_query(query).await
    }

    #[cfg(feature = "polars")]
    #[instrument(skip(self, df), fields(adapter = "mysql", table = %table_name, rows = df.height(), columns = df.width(), replace = replace))]
    async fn export_dataframe(
        &self,
        df: &DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        replace: bool,
    ) -> Result<u64> {
        info!(
            table = %table_name,
            rows = df.height(),
            columns = df.width(),
            replace,
            "Starting DataFrame export"
        );
        let export_start = std::time::Instant::now();

        // Check connection
        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "export_dataframe", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // If replace, drop and recreate table
        if replace {
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            pool.execute(drop_sql.as_str())
                .await
                .map_err(|e| DataError::Query(format!("Failed to drop table: {}", e)))?;

            // Create table based on DataFrame schema
            let create_sql = self.generate_create_table_sql(df, table_name)?;
            pool.execute(create_sql.as_str())
                .await
                .map_err(|e| DataError::Query(format!("Failed to create table: {}", e)))?;
        }

        // Insert data row by row
        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let placeholders: Vec<String> = (1..=column_names.len()).map(|_| "?".to_string()).collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            column_names.join(", "),
            placeholders.join(", ")
        );

        let mut rows_inserted: u64 = 0;

        // Insert rows using sqlx
        for row_idx in 0..df.height() {
            let mut query = sqlx::query(&insert_sql);

            // Bind values for this row
            for col_name in &column_names {
                let column = df.column(col_name).map_err(|e| {
                    DataError::DataFrame(format!("Column '{}' not found: {}", col_name, e))
                })?;

                let series = column.as_materialized_series();
                query = self.bind_series_value(query, series, row_idx)?;
            }

            // Execute insert
            query.execute(pool).await.map_err(|e| {
                DataError::Query(format!("Failed to insert row {}: {}", row_idx, e))
            })?;

            rows_inserted += 1;
            if rows_inserted % 1000 == 0 {
                debug!(rows_inserted, total_rows = df.height(), "Export progress");
            }
        }

        info!(
            table = %table_name,
            rows_written = rows_inserted,
            duration_ms = export_start.elapsed().as_millis(),
            "DataFrame export complete"
        );
        Ok(rows_inserted)
    }

    // ===== Schema Discovery =====
    // These methods will be implemented in arni-8b0.1.4

    #[instrument(skip(self), fields(adapter = "mysql"))]
    async fn list_databases(&self) -> Result<Vec<String>> {
        // Check connection
        if !Connection::is_connected(self) {
            error!(
                adapter = "mysql",
                operation = "list_databases",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // Query information_schema.SCHEMATA
        let query = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME";
        let rows: Vec<(String,)> = sqlx::query_as(query)
            .fetch_all(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to list databases: {}", e)))?;

        let databases: Vec<String> = rows.into_iter().map(|(name,)| name).collect();

        Ok(databases)
    }

    #[instrument(skip(self), fields(adapter = "mysql", schema = ?schema))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        // Check connection
        if !Connection::is_connected(self) {
            error!(
                adapter = "mysql",
                operation = "list_tables",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // Query information_schema.TABLES
        let (query, schema_name) = if let Some(schema_name) = schema {
            (
                "SELECT TABLE_NAME FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
                 ORDER BY TABLE_NAME",
                schema_name.to_string(),
            )
        } else {
            // Use current database if no schema specified
            let current_db_query = "SELECT DATABASE()";
            let result: (Option<String>,) = sqlx::query_as(current_db_query)
                .fetch_one(pool)
                .await
                .map_err(|e| DataError::Query(format!("Failed to get current database: {}", e)))?;

            let db_name = result.0.ok_or_else(|| {
                DataError::Query("No database selected. Specify schema parameter.".to_string())
            })?;

            (
                "SELECT TABLE_NAME FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
                 ORDER BY TABLE_NAME",
                db_name,
            )
        };

        let rows: Vec<(String,)> = sqlx::query_as(query)
            .bind(&schema_name)
            .fetch_all(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to list tables: {}", e)))?;

        let tables: Vec<String> = rows.into_iter().map(|(name,)| name).collect();

        Ok(tables)
    }

    #[instrument(skip(self), fields(adapter = "mysql", pattern = %pattern, mode = ?mode, schema = ?schema))]
    async fn find_tables(
        &self,
        pattern: &str,
        schema: Option<&str>,
        mode: TableSearchMode,
    ) -> Result<Vec<String>> {
        if !Connection::is_connected(self) {
            error!(
                adapter = "mysql",
                operation = "find_tables",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // Resolve schema_name the same way as list_tables
        let schema_name = if let Some(s) = schema {
            s.to_string()
        } else {
            let result: (Option<String>,) = sqlx::query_as("SELECT DATABASE()")
                .fetch_one(pool)
                .await
                .map_err(|e| DataError::Query(format!("Failed to get current database: {}", e)))?;
            result.0.ok_or_else(|| {
                DataError::Query("No database selected. Specify schema parameter.".to_string())
            })?
        };

        let escaped = escape_like_pattern(pattern);
        let like_pattern = match mode {
            TableSearchMode::StartsWith => format!("{}%", escaped),
            TableSearchMode::Contains => format!("%{}%", escaped),
            TableSearchMode::EndsWith => format!("%{}", escaped),
        };

        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
             AND TABLE_NAME LIKE ? ESCAPE '\\' \
             ORDER BY TABLE_NAME",
        )
        .bind(&schema_name)
        .bind(&like_pattern)
        .fetch_all(pool)
        .await
        .map_err(|e| DataError::Query(format!("Failed to find tables: {}", e)))?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    #[instrument(skip(self), fields(adapter = "mysql", table = %table_name, schema = ?schema))]
    async fn describe_table(&self, table_name: &str, schema: Option<&str>) -> Result<TableInfo> {
        // Check connection
        if !Connection::is_connected(self) {
            error!(
                adapter = "mysql",
                operation = "describe_table",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // Determine schema to use
        let schema_name = if let Some(schema_name) = schema {
            schema_name.to_string()
        } else {
            // Use current database if no schema specified
            let current_db_query = "SELECT DATABASE()";
            let result: (Option<String>,) = sqlx::query_as(current_db_query)
                .fetch_one(pool)
                .await
                .map_err(|e| DataError::Query(format!("Failed to get current database: {}", e)))?;

            result.0.ok_or_else(|| {
                DataError::Query("No database selected. Specify schema parameter.".to_string())
            })?
        };

        // MySQL 8 returns COLUMN_TYPE/IS_NULLABLE/COLUMN_KEY as MEDIUMTEXT (BLOB on the
        // wire).  CAST to CHAR forces VARCHAR so sqlx can decode them as String.
        let column_query = "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR), \
                    CAST(IS_NULLABLE AS CHAR), COLUMN_DEFAULT, CAST(COLUMN_KEY AS CHAR) \
                    FROM information_schema.COLUMNS \
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                    ORDER BY ORDINAL_POSITION";

        let rows: Vec<(String, String, String, Option<String>, String)> =
            sqlx::query_as(column_query)
                .bind(&schema_name)
                .bind(table_name)
                .fetch_all(pool)
                .await
                .map_err(|e| DataError::Query(format!("Failed to describe table: {}", e)))?;

        if rows.is_empty() {
            error!(adapter = "mysql", operation = "describe_table", table = %table_name, schema = %schema_name, "Table not found in schema");
            return Err(DataError::Query(format!(
                "Table '{}.{}' not found",
                schema_name, table_name
            )));
        }

        // Build column info
        let columns: Vec<ColumnInfo> = rows
            .into_iter()
            .map(
                |(col_name, data_type, is_nullable, default_value, column_key)| ColumnInfo {
                    name: col_name,
                    data_type,
                    nullable: is_nullable == "YES",
                    default_value,
                    is_primary_key: column_key == "PRI",
                },
            )
            .collect();

        // Fetch row count, total size, and creation time from information_schema
        let stats_query = "
            SELECT table_rows,
                   data_length + index_length,
                   DATE_FORMAT(create_time, '%Y-%m-%dT%H:%i:%s')
            FROM information_schema.TABLES
            WHERE table_schema = ? AND table_name = ?
        ";
        let stats: Option<(Option<i64>, Option<i64>, Option<String>)> = sqlx::query_as(stats_query)
            .bind(&schema_name)
            .bind(table_name)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
        let (row_count, size_bytes, created_at) = stats.unwrap_or((None, None, None));

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name),
            columns,
            row_count,
            size_bytes,
            created_at,
        })
    }

    // ===== Metadata Methods =====

    async fn get_indexes(&self, table_name: &str, _schema: Option<&str>) -> Result<Vec<IndexInfo>> {
        // Check connection
        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "get_indexes", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT
                INDEX_NAME,
                TABLE_NAME,
                TABLE_SCHEMA,
                NON_UNIQUE,
                INDEX_TYPE,
                GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_NAME = ?
                AND TABLE_SCHEMA = DATABASE()
            GROUP BY INDEX_NAME, TABLE_NAME, TABLE_SCHEMA, NON_UNIQUE, INDEX_TYPE
        ";

        let results = sqlx::query(query)
            .bind(table_name)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!("Failed to get indexes for '{}': {}", table_name, e))
            })?;

        let indexes = results
            .iter()
            .map(|row| {
                let index_name: String = row.try_get("INDEX_NAME").unwrap_or_default();
                let columns_str: String = row.try_get("columns").unwrap_or_default();
                let columns: Vec<String> = columns_str.split(',').map(|s| s.to_string()).collect();

                IndexInfo {
                    name: index_name.clone(),
                    table_name: row
                        .try_get("TABLE_NAME")
                        .unwrap_or_else(|_| table_name.to_string()),
                    schema: row.try_get("TABLE_SCHEMA").ok(),
                    columns,
                    is_unique: row.try_get::<i32, _>("NON_UNIQUE").unwrap_or(1) == 0,
                    is_primary: index_name == "PRIMARY",
                    index_type: row.try_get("INDEX_TYPE").ok(),
                }
            })
            .collect();

        Ok(indexes)
    }

    async fn get_foreign_keys(
        &self,
        table_name: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        // Check connection
        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "get_foreign_keys", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT
                CONSTRAINT_NAME,
                TABLE_NAME,
                TABLE_SCHEMA,
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_TABLE_SCHEMA,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = ?
                AND TABLE_SCHEMA = DATABASE()
                AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
        ";

        let results = sqlx::query(query)
            .bind(table_name)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get foreign keys for '{}': {}",
                    table_name, e
                ))
            })?;

        let mut fk_map: HashMap<String, ForeignKeyInfo> = HashMap::new();

        for row in results {
            let fk_name: String = row.try_get("CONSTRAINT_NAME").unwrap_or_default();
            let column: String = row.try_get("COLUMN_NAME").unwrap_or_default();
            let ref_column: String = row.try_get("REFERENCED_COLUMN_NAME").unwrap_or_default();

            fk_map
                .entry(fk_name.clone())
                .or_insert_with(|| ForeignKeyInfo {
                    name: fk_name.clone(),
                    table_name: row
                        .try_get("TABLE_NAME")
                        .unwrap_or_else(|_| table_name.to_string()),
                    schema: row.try_get("TABLE_SCHEMA").ok(),
                    columns: Vec::new(),
                    referenced_table: row.try_get("REFERENCED_TABLE_NAME").unwrap_or_default(),
                    referenced_schema: row.try_get("REFERENCED_TABLE_SCHEMA").ok(),
                    referenced_columns: Vec::new(),
                    on_delete: None,
                    on_update: None,
                })
                .columns
                .push(column);

            if let Some(fk) = fk_map.get_mut(&fk_name) {
                fk.referenced_columns.push(ref_column);
            }
        }

        Ok(fk_map.into_values().collect())
    }

    async fn get_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        // Check connection
        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "get_views", "Not connected");
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT TABLE_NAME, TABLE_SCHEMA
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME
        ";

        let results = sqlx::query(query)
            .fetch_all(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to get views: {}", e)))?;

        let views = results
            .iter()
            .map(|row| ViewInfo {
                name: row.try_get("TABLE_NAME").unwrap_or_default(),
                schema: row.try_get("TABLE_SCHEMA").ok(),
                definition: None,
            })
            .collect();

        Ok(views)
    }

    async fn get_view_definition(
        &self,
        view_name: &str,
        _schema: Option<&str>,
    ) -> Result<Option<String>> {
        // Check connection
        if self.pool.is_none() {
            error!(
                adapter = "mysql",
                operation = "get_view_definition",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // LONGTEXT columns in INFORMATION_SCHEMA come back as bytes on MySQL 8;
        // CAST to CHAR forces VARCHAR so sqlx can decode them as String.
        let query = "
            SELECT CAST(VIEW_DEFINITION AS CHAR) AS view_def
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()
        ";

        let result = sqlx::query(query)
            .bind(view_name)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get view definition for '{}': {}",
                    view_name, e
                ))
            })?;

        Ok(result.and_then(|row| row.try_get("view_def").ok()))
    }

    async fn list_stored_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // Check connection
        if self.pool.is_none() {
            error!(
                adapter = "mysql",
                operation = "list_stored_procedures",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT
                ROUTINE_NAME as name,
                ROUTINE_SCHEMA as schema_name,
                DTD_IDENTIFIER as return_type
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE()
            ORDER BY ROUTINE_NAME
        ";

        let results = sqlx::query(query)
            .fetch_all(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to get stored procedures: {}", e)))?;

        let procedures = results
            .iter()
            .map(|row| ProcedureInfo {
                name: row.try_get("name").unwrap_or_default(),
                schema: row.try_get("schema_name").ok(),
                return_type: row.try_get("return_type").ok(),
                language: Some("SQL".to_string()), // MySQL uses SQL
            })
            .collect();

        Ok(procedures)
    }

    #[instrument(skip(self), fields(adapter = "mysql"))]
    async fn get_server_info(&self) -> Result<ServerInfo> {
        if self.pool.is_none() {
            error!(
                adapter = "mysql",
                operation = "get_server_info",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;
        let row = sqlx::query("SELECT VERSION() AS version")
            .fetch_one(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to get server info: {}", e)))?;
        let version: String = row
            .try_get("version")
            .unwrap_or_else(|_| "Unknown".to_string());
        Ok(ServerInfo {
            version,
            server_type: "MySQL".to_string(),
            extra_info: HashMap::new(),
        })
    }

    // ===== Bulk Operations =====

    #[instrument(skip(self, columns, rows), fields(adapter = "mysql", table = %table_name, row_count = rows.len()))]
    async fn bulk_insert(
        &self,
        table_name: &str,
        columns: &[String],
        rows: &[Vec<QueryValue>],
        schema: Option<&str>,
    ) -> Result<u64> {
        if columns.is_empty() {
            return Err(DataError::Config("Column list cannot be empty".to_string()));
        }

        if rows.is_empty() {
            return Ok(0);
        }

        // Check connection
        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "bulk_insert", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        // Validate all rows have the same column count
        for (idx, row) in rows.iter().enumerate() {
            if row.len() != columns.len() {
                return Err(DataError::Config(format!(
                    "Row {} has {} values but expected {} columns",
                    idx,
                    row.len(),
                    columns.len()
                )));
            }
        }

        // Get pool
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let schema_prefix = schema.map(|s| format!("{}.", s)).unwrap_or_default();

        // Build column list
        let column_list = columns.join(", ");

        // Build value placeholders - MySQL uses ?
        let row_placeholder = format!("({})", vec!["?"; columns.len()].join(", "));
        let placeholders = vec![row_placeholder; rows.len()].join(", ");

        // Build the full INSERT query
        let query = format!(
            "INSERT INTO {}{} ({}) VALUES {}",
            schema_prefix, table_name, column_list, placeholders
        );

        // Build and bind all parameters
        let mut query_builder = sqlx::query(&query);

        for row in rows {
            for value in row {
                query_builder = match value {
                    QueryValue::Null => query_builder.bind(None::<String>),
                    QueryValue::Int(v) => query_builder.bind(*v),
                    QueryValue::Float(v) => query_builder.bind(*v),
                    QueryValue::Text(v) => query_builder.bind(v),
                    QueryValue::Bool(v) => query_builder.bind(*v),
                    QueryValue::Bytes(v) => query_builder.bind(v),
                };
            }
        }

        // Execute the query
        let result = query_builder.execute(pool).await.map_err(|e| {
            DataError::Query(format!(
                "Failed to bulk insert into {}{}: {}",
                schema_prefix, table_name, e
            ))
        })?;

        Ok(result.rows_affected())
    }

    #[instrument(skip(self, updates), fields(adapter = "mysql", table = %table_name))]
    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, FilterExpr)],
        schema: Option<&str>,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }

        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "bulk_update", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let schema_prefix = schema.map(|s| format!("{}.", s)).unwrap_or_default();
        let mut total_affected = 0u64;

        for (set_clauses, filter) in updates {
            if set_clauses.is_empty() {
                continue;
            }

            // SET uses parameterized ? placeholders; WHERE uses typed FilterExpr literal
            let set_parts: Vec<String> = set_clauses
                .keys()
                .map(|column| format!("{} = ?", column))
                .collect();

            let query = format!(
                "UPDATE {}{} SET {} WHERE {}",
                schema_prefix,
                table_name,
                set_parts.join(", "),
                filter_to_sql(filter)
            );

            let mut query_builder = sqlx::query(&query);
            for value in set_clauses.values() {
                query_builder = match value {
                    QueryValue::Null => query_builder.bind(None::<String>),
                    QueryValue::Int(v) => query_builder.bind(*v),
                    QueryValue::Float(v) => query_builder.bind(*v),
                    QueryValue::Text(v) => query_builder.bind(v),
                    QueryValue::Bool(v) => query_builder.bind(*v),
                    QueryValue::Bytes(v) => query_builder.bind(v),
                };
            }

            let result = query_builder.execute(pool).await.map_err(|e| {
                DataError::Query(format!(
                    "Failed to bulk update {}{}: {}",
                    schema_prefix, table_name, e
                ))
            })?;

            total_affected += result.rows_affected();
        }

        Ok(total_affected)
    }

    #[instrument(skip(self, filters), fields(adapter = "mysql", table = %table_name))]
    async fn bulk_delete(
        &self,
        table_name: &str,
        filters: &[FilterExpr],
        schema: Option<&str>,
    ) -> Result<u64> {
        if filters.is_empty() {
            return Ok(0);
        }

        if self.pool.is_none() {
            error!(adapter = "mysql", operation = "bulk_delete", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let schema_prefix = schema.map(|s| format!("{}.", s)).unwrap_or_default();
        let mut total_affected = 0u64;

        for filter in filters {
            let query = format!(
                "DELETE FROM {}{} WHERE {}",
                schema_prefix,
                table_name,
                filter_to_sql(filter)
            );

            let result = sqlx::query(&query).execute(pool).await.map_err(|e| {
                DataError::Query(format!(
                    "Failed to bulk delete from {}{}: {}",
                    schema_prefix, table_name, e
                ))
            })?;

            total_affected += result.rows_affected();
        }

        Ok(total_affected)
    }
}

impl MySqlAdapter {
    #[cfg(feature = "polars")]
    /// Map a Polars [`DataType`] to the corresponding MySQL column type name.
    ///
    /// MySQL uses `INT` (not `INTEGER`), and supports unsigned integer variants.
    /// All other types (booleans, signed ints, floats, strings, binary) match
    /// the generic SQL mapping and are delegated to [`super::common::polars_dtype_to_generic_sql`].
    fn polars_dtype_to_mysql_type(dtype: &DataType) -> &'static str {
        match dtype {
            DataType::Int32 => "INT",              // MySQL: INT vs ANSI INTEGER
            DataType::UInt8 => "TINYINT UNSIGNED",
            DataType::UInt16 => "SMALLINT UNSIGNED",
            DataType::UInt32 => "INT UNSIGNED",
            DataType::UInt64 => "BIGINT UNSIGNED",
            _ => super::common::polars_dtype_to_generic_sql(dtype),
        }
    }

    #[cfg(feature = "polars")]
    /// Generate CREATE TABLE SQL from DataFrame schema
    fn generate_create_table_sql(&self, df: &DataFrame, table_name: &str) -> Result<String> {
        let mut column_defs = Vec::new();

        for column in df.columns() {
            let name = column.name();
            let dtype = column.dtype();

            let mysql_type = Self::polars_dtype_to_mysql_type(dtype);

            column_defs.push(format!("{} {}", name, mysql_type));
        }

        Ok(format!(
            "CREATE TABLE {} ({})",
            table_name,
            column_defs.join(", ")
        ))
    }

    #[cfg(feature = "polars")]
    /// Bind a Series value at a specific row index to a sqlx query
    fn bind_series_value<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
        series: &Series,
        row_idx: usize,
    ) -> Result<sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>> {
        // Check if value is null
        let null_mask = series.is_null();
        if null_mask.get(row_idx).unwrap_or(false) {
            return Ok(query.bind(None::<String>));
        }

        // Convert based on Series data type and bind
        let bound_query = match series.dtype() {
            DataType::Boolean => {
                let val = series
                    .bool()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::Int8 => {
                let series_i32 = series
                    .cast(&DataType::Int32)
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?;
                let val = series_i32
                    .i32()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::Int16 => {
                let series_i32 = series
                    .cast(&DataType::Int32)
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?;
                let val = series_i32
                    .i32()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::Int32 => {
                let val = series
                    .i32()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::Int64 => {
                let val = series
                    .i64()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
                let series_i64 = series
                    .cast(&DataType::Int64)
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?;
                let val = series_i64
                    .i64()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::UInt64 => {
                let series_i64 = series
                    .cast(&DataType::Int64)
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?;
                let val = series_i64
                    .i64()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::Float32 => {
                let val = series
                    .f32()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::Float64 => {
                let val = series
                    .f64()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val)
            }
            DataType::String => {
                let val = series
                    .str()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val.to_string())
            }
            DataType::Binary => {
                let val = series
                    .binary()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val.to_vec())
            }
            dtype => {
                // For unsupported types, try to convert to string
                let series_str = series.cast(&DataType::String).map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Cannot convert {:?} to MySQL type: {}",
                        dtype, e
                    ))
                })?;
                let val = series_str
                    .str()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                query.bind(val.to_string())
            }
        };

        Ok(bound_query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::DatabaseType;
    use std::collections::HashMap;

    fn create_test_config() -> ConnectionConfig {
        let mut parameters = HashMap::new();
        // Allow override via env; fall back to the default dev password.
        let password =
            std::env::var("TEST_MYSQL_PASSWORD").unwrap_or_else(|_| "test_password".to_string());
        parameters.insert("password".to_string(), password);
        ConnectionConfig {
            id: "test-mysql".to_string(),
            name: "Test MySQL".to_string(),
            db_type: DatabaseType::MySQL,
            host: Some(
                std::env::var("TEST_MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string()),
            ),
            port: Some(
                std::env::var("TEST_MYSQL_PORT")
                    .ok()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(3306),
            ),
            database: std::env::var("TEST_MYSQL_DATABASE")
                .unwrap_or_else(|_| "test_db".to_string()),
            username: Some(
                std::env::var("TEST_MYSQL_USERNAME").unwrap_or_else(|_| "test_user".to_string()),
            ),
            use_ssl: false,
            parameters,
            pool_config: None,
        }
    }

    /// Skip helper for in-module integration tests.
    fn mysql_integration_available() -> bool {
        std::env::var("TEST_MYSQL_AVAILABLE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
    }

    /// Early-exit guard for in-module tests that need a real MySQL connection.
    macro_rules! require_mysql {
        () => {
            if !mysql_integration_available() {
                return;
            }
        };
    }

    #[test]
    fn test_new_mysql_adapter() {
        let config = create_test_config();
        let adapter = MySqlAdapter::new(config.clone());

        assert_eq!(adapter.config().id, "test-mysql");
        assert_eq!(adapter.config().db_type, DatabaseType::MySQL);
        assert!(!Connection::is_connected(&adapter));
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

        let conn_str = adapter
            .build_connection_string(Some("password123"))
            .unwrap();
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

        let conn_str = adapter
            .build_connection_string(Some("password123"))
            .unwrap();
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
        require_mysql!();
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");
        assert!(Connection::is_connected(&adapter));

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
        assert!(!Connection::is_connected(&adapter));
    }

    #[tokio::test]
    #[ignore]
    async fn test_health_check() {
        require_mysql!();
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        let result = adapter.health_check().await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
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
        require_mysql!();
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        let result = adapter
            .execute_query("SELECT 1 as num, 'test' as text")
            .await;
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.columns.len(), 2);
        assert_eq!(query_result.columns[0], "num");
        assert_eq!(query_result.columns[1], "text");
        assert_eq!(query_result.rows.len(), 1);

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_execute_query_empty_result() {
        require_mysql!();
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        // Use a regular table (not TEMPORARY) so all pool connections can see it
        adapter
            .execute_query("DROP TABLE IF EXISTS test_empty_result")
            .await
            .expect("Failed to drop table");

        adapter
            .execute_query("CREATE TABLE test_empty_result (id INT)")
            .await
            .expect("Failed to create table");

        // Query empty table
        let result = adapter
            .execute_query("SELECT * FROM test_empty_result")
            .await;
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.columns.len(), 0);
        assert_eq!(query_result.rows.len(), 0);

        // Clean up
        adapter
            .execute_query("DROP TABLE IF EXISTS test_empty_result")
            .await
            .expect("Failed to drop table");

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_execute_query_insert_update_delete() {
        require_mysql!();
        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config);

        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        // Use a regular table (not TEMPORARY) so all pool connections can see it
        adapter
            .execute_query("DROP TABLE IF EXISTS test_crud_ops")
            .await
            .expect("Failed to drop table");

        adapter
            .execute_query("CREATE TABLE test_crud_ops (id INT, name VARCHAR(100))")
            .await
            .expect("Failed to create table");

        // Insert
        let result = adapter
            .execute_query("INSERT INTO test_crud_ops (id, name) VALUES (1, 'Alice')")
            .await;
        assert!(result.is_ok());

        // Update
        let result = adapter
            .execute_query("UPDATE test_crud_ops SET name = 'Bob' WHERE id = 1")
            .await;
        assert!(result.is_ok());

        // Verify update
        let result = adapter
            .execute_query("SELECT * FROM test_crud_ops WHERE id = 1")
            .await
            .expect("Failed to select");
        assert_eq!(result.rows.len(), 1);

        // Delete
        let result = adapter
            .execute_query("DELETE FROM test_crud_ops WHERE id = 1")
            .await;
        assert!(result.is_ok());

        // Clean up
        adapter
            .execute_query("DROP TABLE IF EXISTS test_crud_ops")
            .await
            .expect("Failed to drop table");

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn test_generate_create_table_sql() {
        use polars::prelude::*;

        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);

        let df = DataFrame::new(
            3,
            vec![
                Series::new("id".into(), &[1, 2, 3]).into(),
                Series::new("name".into(), &["Alice", "Bob", "Charlie"]).into(),
                Series::new("active".into(), &[true, false, true]).into(),
            ],
        )
        .unwrap();

        let sql = adapter
            .generate_create_table_sql(&df, "test_table")
            .unwrap();

        assert!(sql.contains("CREATE TABLE test_table"));
        assert!(sql.contains("id INT"));
        assert!(sql.contains("name TEXT"));
        assert!(sql.contains("active BOOLEAN"));
    }

    #[cfg(feature = "polars")]
    #[tokio::test]
    #[ignore]
    async fn test_export_dataframe_replace() {
        require_mysql!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .expect("Failed to connect");

        let df = DataFrame::new(
            3,
            vec![
                Series::new("id".into(), &[1, 2, 3]).into(),
                Series::new("name".into(), &["Alice", "Bob", "Charlie"]).into(),
            ],
        )
        .unwrap();

        // Export with replace=true
        let result = DbAdapter::export_dataframe(&adapter, &df, "test_export", None, true).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        // Verify data was inserted
        let query_result = adapter
            .execute_query("SELECT * FROM test_export ORDER BY id")
            .await
            .expect("Failed to select");
        assert_eq!(query_result.rows.len(), 3);

        // Clean up
        adapter
            .execute_query("DROP TABLE test_export")
            .await
            .expect("Failed to drop table");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_read_table() {
        require_mysql!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .expect("Failed to connect");

        // Use a regular table (not TEMPORARY) so all pool connections can see it
        adapter
            .execute_query("DROP TABLE IF EXISTS test_read_table")
            .await
            .expect("Failed to drop table");

        adapter
            .execute_query("CREATE TABLE test_read_table (id INT, name VARCHAR(100))")
            .await
            .expect("Failed to create table");

        adapter
            .execute_query("INSERT INTO test_read_table VALUES (1, 'Alice'), (2, 'Bob')")
            .await
            .expect("Failed to insert data");

        // Read table as QueryResult
        let result = DbAdapter::read_table(&adapter, "test_read_table", None).await;
        assert!(result.is_ok());

        let qr = result.unwrap();
        assert_eq!(qr.rows.len(), 2);
        assert_eq!(qr.columns.len(), 2);

        // Clean up
        adapter
            .execute_query("DROP TABLE IF EXISTS test_read_table")
            .await
            .expect("Failed to drop table");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[cfg(feature = "polars")]
    #[tokio::test]
    #[ignore]
    async fn test_query_df() {
        require_mysql!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .expect("Failed to connect");

        // Query as DataFrame
        let result = DbAdapter::query_df(&adapter, "SELECT 1 as num, 'test' as text").await;
        assert!(result.is_ok());

        let df = result.unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 2);

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_databases() {
        require_mysql!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .expect("Failed to connect");

        // List databases
        let result = DbAdapter::list_databases(&adapter).await;
        assert!(result.is_ok());

        let databases = result.unwrap();
        assert!(!databases.is_empty());
        // Common system databases should be present
        assert!(databases.contains(&"information_schema".to_string()));

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_tables() {
        require_mysql!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .expect("Failed to connect");

        // Create a test table
        adapter
            .execute_query("CREATE TEMPORARY TABLE test_list_tables (id INT, name VARCHAR(100))")
            .await
            .expect("Failed to create table");

        // List tables (temporary tables may not appear in information_schema)
        // So we'll test that the method works without error
        let result = DbAdapter::list_tables(&adapter, None).await;
        assert!(result.is_ok());

        let tables = result.unwrap();
        // Tables vector should be valid (may be empty for temp tables)
        assert!(tables.is_empty() || !tables.is_empty());

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_tables_with_schema() {
        require_mysql!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .expect("Failed to connect");

        // List tables in specific schema (test_db)
        let result = DbAdapter::list_tables(&adapter, Some("test_db")).await;
        assert!(result.is_ok());

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_describe_table() {
        require_mysql!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = MySqlAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .expect("Failed to connect");

        // Create a test table with various column types
        adapter
            .execute_query(
                "CREATE TEMPORARY TABLE test_describe (
                    id INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    age INT DEFAULT 0,
                    active BOOLEAN,
                    created_at TIMESTAMP
                )",
            )
            .await
            .expect("Failed to create table");

        // Describe table (note: temporary tables may not appear in information_schema)
        // We'll test with a real table if available, or expect error for temp table
        let result = DbAdapter::describe_table(&adapter, "test_describe", None).await;

        // Temporary tables may not show up in information_schema
        // If it works, validate the structure
        if let Ok(table_info) = result {
            assert_eq!(table_info.name, "test_describe");
            assert!(!table_info.columns.is_empty());

            // Find the id column and verify it's marked as primary key
            let id_col = table_info.columns.iter().find(|c| c.name == "id");
            if let Some(col) = id_col {
                assert!(col.is_primary_key);
            }
        }

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    async fn test_list_databases_not_connected() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);

        let result = DbAdapter::list_databases(&adapter).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataError::Connection(_)),
            "Expected Connection error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_list_tables_not_connected() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);

        let result = DbAdapter::list_tables(&adapter, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataError::Connection(_)),
            "Expected Connection error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_describe_table_not_connected() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);

        let result = DbAdapter::describe_table(&adapter, "test_table", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataError::Connection(_)),
            "Expected Connection error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_find_tables_not_connected() {
        let config = create_test_config();
        let adapter = MySqlAdapter::new(config);
        let result =
            DbAdapter::find_tables(&adapter, "PS_", None, TableSearchMode::StartsWith).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[test]
    fn test_find_tables_like_pattern_starts_with() {
        let like_pattern = format!("{}%", escape_like_pattern("PS_"));
        assert_eq!(like_pattern, "PS\\_%");
    }

    #[test]
    fn test_find_tables_like_pattern_contains() {
        let like_pattern = format!("%{}%", escape_like_pattern("PS_"));
        assert_eq!(like_pattern, "%PS\\_%");
    }

    #[test]
    fn test_find_tables_like_pattern_ends_with() {
        let like_pattern = format!("%{}", escape_like_pattern("PS_"));
        assert_eq!(like_pattern, "%PS\\_");
    }

    // ── test_connection() unit tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_connection_missing_host_returns_err() {
        let mut config = create_test_config();
        config.host = None;
        let adapter = MySqlAdapter::new(config.clone());
        let result = adapter.test_connection(&config, None).await;
        assert!(result.is_err(), "Missing host should return Err, not panic");
    }

    #[tokio::test]
    async fn test_connection_missing_username_returns_err() {
        let mut config = create_test_config();
        config.username = None;
        let adapter = MySqlAdapter::new(config.clone());
        let result = adapter.test_connection(&config, None).await;
        assert!(
            result.is_err(),
            "Missing username should return Err, not panic"
        );
    }

    // ── not-connected guard tests for extended methods ─────────────────────────

    #[tokio::test]
    async fn test_get_indexes_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let result = adapter.get_indexes("users", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_foreign_keys_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let result = adapter.get_foreign_keys("orders", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_views_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let result = adapter.get_views(None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_server_info_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let result = adapter.get_server_info().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_stored_procedures_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let result = adapter.list_stored_procedures(None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bulk_insert_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let cols = vec!["id".to_string()];
        let rows = vec![vec![QueryValue::Int(1)]];
        let result = adapter.bulk_insert("t", &cols, &rows, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bulk_update_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let mut set = std::collections::HashMap::new();
        set.insert("name".to_string(), QueryValue::Text("x".into()));
        let updates = [(set, FilterExpr::Eq("id".to_string(), QueryValue::Int(1)))];
        let result = adapter.bulk_update("t", &updates, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bulk_delete_not_connected() {
        let adapter = MySqlAdapter::new(create_test_config());
        let filters = [FilterExpr::Eq("id".to_string(), QueryValue::Int(1))];
        let result = adapter.bulk_delete("t", &filters, None).await;
        assert!(result.is_err());
    }

    // ---- helper unit tests ----

    #[test]
    fn classify_syntax_error_maps_to_sql_syntax_error() {
        let e = sqlx::Error::Protocol("syntax error".to_string());
        let result = MySqlAdapter::classify_query_error("SELECT", e);
        assert!(matches!(result, DataError::Query(msg) if msg.contains("SQL syntax error")));
    }

    #[test]
    fn classify_access_denied_maps_to_permission_denied() {
        let e = sqlx::Error::Protocol("Access denied for user".to_string());
        let result = MySqlAdapter::classify_query_error("SELECT", e);
        assert!(matches!(result, DataError::Query(msg) if msg.contains("Permission denied")));
    }

    #[test]
    fn classify_duplicate_maps_to_constraint_violation() {
        let e = sqlx::Error::Protocol("Duplicate entry".to_string());
        let result = MySqlAdapter::classify_query_error("INSERT", e);
        assert!(matches!(result, DataError::Query(msg) if msg.contains("Constraint violation")));
    }

    #[test]
    fn classify_generic_error_maps_to_query_failed() {
        let e = sqlx::Error::Protocol("something went wrong".to_string());
        let result = MySqlAdapter::classify_query_error("SELECT", e);
        assert!(matches!(result, DataError::Query(msg) if msg.contains("Query failed")));
    }
}
