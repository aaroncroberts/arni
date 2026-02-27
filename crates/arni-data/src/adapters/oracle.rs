//! Oracle database adapter implementation
//!
//! This module provides the [`OracleAdapter`] which implements both the [`Connection`]
//! and [`DbAdapter`] traits for Oracle databases using the oracle driver.
//!
//! # Features
//!
//! This module is only available when the `oracle` feature is enabled:
//!
//! ```toml
//! arni-data = { version = "0.1", features = ["oracle"] }
//! ```
//!
//! # Examples
//!
//! ```ignore
//! use arni_data::adapters::oracle::OracleAdapter;
//! use arni_data::adapter::{Connection, ConnectionConfig, DatabaseType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ConnectionConfig {
//!         id: "my-oracle".to_string(),
//!         name: "My Oracle DB".to_string(),
//!         db_type: DatabaseType::Oracle,
//!         host: Some("localhost".to_string()),
//!         port: Some(1521),
//!         database: "FREE".to_string(),
//!         username: Some("user".to_string()),
//!         use_ssl: false,
//!         parameters: Default::default(),
//!     };
//!
//!     let mut adapter = OracleAdapter::new(config);
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
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, ForeignKeyInfo, IndexInfo, ProcedureInfo, QueryResult, QueryValue, Result,
    TableInfo, ViewInfo,
};
use crate::DataError;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

/// Oracle database adapter
///
/// This adapter uses the oracle driver to connect to Oracle databases.
/// The oracle crate is synchronous, so we use tokio::task::spawn_blocking
/// for async operations.
///
/// # Connection Management
///
/// The adapter maintains an internal connection wrapped in Arc<RwLock> for thread-safe access.
/// Connections are established when `connect()` is called.
///
/// # Thread Safety
///
/// The adapter uses internal locking to ensure thread-safe access to the underlying
/// Oracle connection.
pub struct OracleAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// Oracle connection wrapped in Arc<RwLock> for thread-safe access
    /// Note: oracle::Connection is not Send, so we'll use Option<String> for connection string
    /// and reconnect as needed
    connection: Arc<RwLock<Option<oracle::Connection>>>,
    /// Connection state flag
    connected: Arc<RwLock<bool>>,
}

impl OracleAdapter {
    /// Create a new Oracle adapter with the given configuration
    ///
    /// This does not establish a connection immediately. Call [`connect`](ConnectionTrait::connect)
    /// to establish the connection.
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            connection: Arc::new(RwLock::new(None)),
            connected: Arc::new(RwLock::new(false)),
        }
    }

    /// Build a connection string from the configuration
    /// Returns (username, password, connect_string) tuple
    fn build_connection_params(
        config: &ConnectionConfig,
        password: Option<&str>,
    ) -> (String, String, String) {
        let host = config.host.as_deref().unwrap_or("localhost");
        let port = config.port.unwrap_or(1521);
        let database = &config.database; // Service name or SID
        let username = config.username.as_deref().unwrap_or("system").to_string();
        let password = password.unwrap_or("").to_string();
        let connect_string = format!("{}:{}/{}", host, port, database);

        (username, password, connect_string)
    }

    /// Convert an Oracle row to a vector of QueryValues
    fn row_to_values(row: &oracle::Row, column_count: usize) -> Result<Vec<QueryValue>> {
        let mut values = Vec::new();

        for i in 0..column_count {
            // Try to get the value as various types
            // Oracle crate requires knowing the type at compile time
            let value = if let Ok(Some(s)) = row.get::<_, Option<String>>(i) {
                QueryValue::Text(s)
            } else if let Ok(Some(n)) = row.get::<_, Option<i64>>(i) {
                QueryValue::Int(n)
            } else if let Ok(Some(f)) = row.get::<_, Option<f64>>(i) {
                QueryValue::Float(f)
            } else if let Ok(Some(b)) = row.get::<_, Option<bool>>(i) {
                QueryValue::Bool(b)
            } else if let Ok(Some(bytes)) = row.get::<_, Option<Vec<u8>>>(i) {
                QueryValue::Bytes(bytes)
            } else {
                QueryValue::Null
            };

            values.push(value);
        }

        Ok(values)
    }

    /// Convert a [`QueryValue`] to an Oracle SQL literal
    fn query_value_to_sql_literal(value: &QueryValue) -> String {
        match value {
            QueryValue::Null => "NULL".to_string(),
            QueryValue::Bool(b) => if *b { "1".to_string() } else { "0".to_string() },
            QueryValue::Int(n) => n.to_string(),
            QueryValue::Float(f) => {
                if f.is_nan() || f.is_infinite() {
                    "NULL".to_string()
                } else {
                    f.to_string()
                }
            }
            QueryValue::Text(s) => {
                let escaped = s.replace('\'', "''");
                format!("'{}'", escaped)
            }
            QueryValue::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                if hex.is_empty() {
                    "NULL".to_string()
                } else {
                    format!("HEXTORAW('{}')", hex)
                }
            }
        }
    }

    /// Map a Polars [`DataType`] to an Oracle SQL type string
    fn polars_dtype_to_oracle_type(dtype: &DataType) -> &'static str {
        match dtype {
            DataType::Boolean => "NUMBER(1)",
            DataType::Int8 | DataType::Int16 | DataType::Int32 => "NUMBER(10)",
            DataType::Int64 => "NUMBER(19)",
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "NUMBER(10)",
            DataType::UInt64 => "NUMBER(20)",
            DataType::Float32 => "BINARY_FLOAT",
            DataType::Float64 => "BINARY_DOUBLE",
            DataType::String => "VARCHAR2(4000)",
            DataType::Binary => "BLOB",
            _ => "VARCHAR2(4000)",
        }
    }

    /// Extract a value from a Series at `row_idx` as an Oracle SQL literal
    fn series_value_to_sql_literal(series: &Series, row_idx: usize) -> Result<String> {
        if series.is_null().get(row_idx).unwrap_or(false) {
            return Ok("NULL".to_string());
        }
        let lit = match series.dtype() {
            DataType::Boolean => {
                let v = series
                    .bool()
                    .map_err(|e| DataError::Query(e.to_string()))?
                    .get(row_idx)
                    .unwrap_or(false);
                if v { "1".to_string() } else { "0".to_string() }
            }
            DataType::Int8 => series
                .i8()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Int16 => series
                .i16()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Int32 => series
                .i32()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Int64 => series
                .i64()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt8 => series
                .u8()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt16 => series
                .u16()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt32 => series
                .u32()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt64 => series
                .u64()
                .map_err(|e| DataError::Query(e.to_string()))?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Float32 => {
                let v = series
                    .f32()
                    .map_err(|e| DataError::Query(e.to_string()))?
                    .get(row_idx)
                    .unwrap_or(0.0);
                if v.is_nan() || v.is_infinite() { "NULL".to_string() } else { v.to_string() }
            }
            DataType::Float64 => {
                let v = series
                    .f64()
                    .map_err(|e| DataError::Query(e.to_string()))?
                    .get(row_idx)
                    .unwrap_or(0.0);
                if v.is_nan() || v.is_infinite() { "NULL".to_string() } else { v.to_string() }
            }
            DataType::String => {
                let v = series
                    .str()
                    .map_err(|e| DataError::Query(e.to_string()))?
                    .get(row_idx)
                    .unwrap_or("");
                format!("'{}'", v.replace('\'', "''"))
            }
            DataType::Binary => {
                let v = series
                    .binary()
                    .map_err(|e| DataError::Query(e.to_string()))?
                    .get(row_idx)
                    .unwrap_or(&[]);
                let hex: String = v.iter().map(|b| format!("{:02X}", b)).collect();
                if hex.is_empty() { "NULL".to_string() } else { format!("HEXTORAW('{}')", hex) }
            }
            _ => {
                let cast = series
                    .cast(&DataType::String)
                    .map_err(|e| DataError::Query(e.to_string()))?;
                match cast
                    .str()
                    .map_err(|e| DataError::Query(e.to_string()))?
                    .get(row_idx)
                {
                    Some(s) => format!("'{}'", s.replace('\'', "''")),
                    None => "NULL".to_string(),
                }
            }
        };
        Ok(lit)
    }

    /// Execute a DML or DDL statement in blocking context, returning rows affected
    async fn execute_statement_blocking(&self, sql: String) -> Result<u64> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            let conn_guard = handle.block_on(connection.read());
            let conn = conn_guard.as_ref().ok_or_else(|| {
                DataError::Connection("Not connected".to_string())
            })?;
            let mut stmt = conn.statement(&sql).build().map_err(|e| {
                DataError::Query(format!("Failed to prepare statement: {}", e))
            })?;
            stmt.execute(&[]).map_err(|e| {
                DataError::Query(format!("Statement execution failed: {}", e))
            })?;
            // row_count is valid for DML; DDL returns 0 or an error (ignored)
            let count = stmt.row_count().unwrap_or(0);
            // commit DML changes; DDL auto-commits in Oracle
            let _ = conn.commit();
            Ok(count)
        })
        .await
        .map_err(|e| DataError::Connection(format!("Task join error: {}", e)))?
    }

    /// Execute a query in blocking context
    #[instrument(skip(self, query), fields(adapter = "oracle", query_length = query.len()))]
    async fn execute_query_blocking(&self, query: String) -> Result<QueryResult> {
        debug!("Executing query in blocking context");

        // Get the connection outside of spawn_blocking to avoid lifetime issues
        let connection = self.connection.clone();

        let start = std::time::Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            // Use tokio runtime handle to block on async operations within spawn_blocking
            let handle = tokio::runtime::Handle::current();
            let conn_guard = handle.block_on(connection.read());
            let conn = conn_guard.as_ref().ok_or_else(|| {
                error!("Connection not available");
                DataError::Connection("Not connected".to_string())
            })?;

            // Prepare and execute the statement
            let mut stmt = conn.statement(&query).build().map_err(|e| {
                error!(error = %e, "Failed to prepare statement");
                DataError::Query(format!("Failed to prepare statement: {}", e))
            })?;

            let result_set = stmt.query(&[]).map_err(|e| {
                error!(error = %e, "Query execution failed");
                DataError::Query(format!("Query execution failed: {}", e))
            })?;

            // Get column information
            let column_info = result_set.column_info();
            let columns: Vec<String> = column_info
                .iter()
                .map(|col| col.name().to_string())
                .collect();

            let column_count = columns.len();

            // Collect rows
            let mut rows = Vec::new();
            for row_result in result_set {
                let row = row_result
                    .map_err(|e| DataError::Query(format!("Failed to fetch row: {}", e)))?;
                let values = Self::row_to_values(&row, column_count)?;
                rows.push(values);
            }

            Ok::<QueryResult, DataError>(QueryResult {
                columns,
                rows,
                rows_affected: None,
            })
        })
        .await
        .map_err(|e| {
            error!(error = %e, "Task join error");
            DataError::Connection(format!("Task join error: {}", e))
        })??;

        let duration = start.elapsed();
        let row_count = result.rows.len();
        info!(
            rows = row_count,
            duration_ms = duration.as_millis(),
            "Query executed successfully"
        );

        Ok(result)
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for OracleAdapter {
    #[instrument(skip(self), fields(adapter = "oracle", host = ?self.config.host, port = ?self.config.port, database = %self.config.database))]
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to Oracle database");

        if self.config.db_type != DatabaseType::Oracle {
            error!("Invalid database type configuration");
            return Err(DataError::Config(format!(
                "Invalid database type: expected Oracle, got {:?}",
                self.config.db_type
            )));
        }

        let (username, password, connect_string) =
            Self::build_connection_params(&self.config, None);
        let connection = self.connection.clone();
        let connected = self.connected.clone();

        tokio::task::spawn_blocking(move || {
            let conn = oracle::Connection::connect(&username, &password, &connect_string).map_err(
                |e| {
                    error!(error = %e, "Failed to establish Oracle connection");
                    DataError::Connection(format!("Failed to connect: {}", e))
                },
            )?;

            let handle = tokio::runtime::Handle::current();
            let mut conn_guard = handle.block_on(connection.write());
            *conn_guard = Some(conn);

            let mut connected_guard = handle.block_on(connected.write());
            *connected_guard = true;

            info!("Successfully connected to Oracle");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!(error = %e, "Task join error during connection");
            DataError::Connection(format!("Task join error: {}", e))
        })?
    }

    #[instrument(skip(self), fields(adapter = "oracle"))]
    async fn disconnect(&mut self) -> Result<()> {
        info!("Disconnecting from Oracle");
        let mut conn_guard = self.connection.write().await;
        *conn_guard = None;

        let mut connected_guard = self.connected.write().await;
        *connected_guard = false;

        debug!("Oracle connection closed");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // Check synchronously without blocking
        match self.connected.try_read() {
            Ok(guard) => *guard,
            Err(_) => false,
        }
    }

    #[instrument(skip(self), fields(adapter = "oracle"))]
    async fn health_check(&self) -> Result<bool> {
        debug!("Performing health check");

        if !*self.connected.read().await {
            warn!("Health check failed: not connected");
            return Ok(false);
        }

        // Execute a simple query to verify connection
        match self
            .execute_query_blocking("SELECT 1 FROM DUAL".to_string())
            .await
        {
            Ok(_) => {
                debug!("Health check passed");
                Ok(true)
            }
            Err(e) => {
                error!(error = ?e, "Health check query failed");
                Ok(false)
            }
        }
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[async_trait::async_trait]
impl DbAdapter for OracleAdapter {
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<&str>) -> Result<()> {
        self.config = config.clone();

        let (username, password_str, connect_string) =
            Self::build_connection_params(config, password);
        let connection = self.connection.clone();
        let connected = self.connected.clone();

        tokio::task::spawn_blocking(move || {
            let conn = oracle::Connection::connect(&username, &password_str, &connect_string)
                .map_err(|e| DataError::Connection(format!("Failed to connect: {}", e)))?;

            let handle = tokio::runtime::Handle::current();
            let mut conn_guard = handle.block_on(connection.write());
            *conn_guard = Some(conn);

            let mut connected_guard = handle.block_on(connected.write());
            *connected_guard = true;

            Ok(())
        })
        .await
        .map_err(|e| DataError::Connection(format!("Task join error: {}", e)))?
    }

    async fn disconnect(&mut self) -> Result<()> {
        ConnectionTrait::disconnect(self).await
    }

    fn is_connected(&self) -> bool {
        ConnectionTrait::is_connected(self)
    }

    async fn test_connection(
        &self,
        config: &ConnectionConfig,
        password: Option<&str>,
    ) -> Result<bool> {
        let (username, password_str, connect_string) =
            Self::build_connection_params(config, password);

        let result = tokio::task::spawn_blocking(move || {
            oracle::Connection::connect(&username, &password_str, &connect_string)
                .map(|_| true)
                .map_err(|_| false)
        })
        .await
        .map_err(|e| DataError::Connection(format!("Task join error: {}", e)))?;

        Ok(result.unwrap_or(false))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::Oracle
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        self.execute_query_blocking(query.to_string()).await
    }

    async fn export_dataframe(
        &self,
        df: &DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        replace: bool,
    ) -> Result<u64> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let table_upper = table_name.to_uppercase();

        if replace {
            // Oracle has no DROP TABLE IF EXISTS — use PL/SQL to suppress ORA-00942
            let drop_sql = format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {}'; \
                 EXCEPTION WHEN OTHERS THEN \
                 IF SQLCODE != -942 THEN RAISE; END IF; END;",
                table_upper
            );
            self.execute_statement_blocking(drop_sql).await?;

            let col_defs: Vec<String> = df
                .get_columns()
                .iter()
                .map(|col| {
                    format!(
                        "{} {}",
                        col.name().to_uppercase(),
                        Self::polars_dtype_to_oracle_type(col.dtype())
                    )
                })
                .collect();
            let create_sql =
                format!("CREATE TABLE {} ({})", table_upper, col_defs.join(", "));
            self.execute_statement_blocking(create_sql).await?;
        }

        let nrows = df.height();
        if nrows == 0 {
            return Ok(0);
        }

        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|n| n.to_uppercase())
            .collect();
        let cols_str = column_names.join(", ");
        let mut total: u64 = 0;

        for row_idx in 0..nrows {
            let mut literals = Vec::with_capacity(column_names.len());
            for col_name in &column_names {
                let series = df
                    .column(col_name)
                    .map_err(|e| DataError::Query(e.to_string()))?
                    .as_materialized_series();
                literals.push(Self::series_value_to_sql_literal(series, row_idx)?);
            }
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_upper,
                cols_str,
                literals.join(", ")
            );
            total += self.execute_statement_blocking(insert_sql).await?;
        }

        Ok(total)
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        Err(DataError::NotSupported(
            "list_databases not supported for Oracle (use service names/SIDs)".to_string(),
        ))
    }

    #[instrument(skip(self), fields(adapter = "oracle", schema = ?schema))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        debug!("Listing tables");

        if !*self.connected.read().await {
            error!("List tables failed: not connected");
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let owner = schema
            .map(|s| s.to_uppercase())
            .or_else(|| self.config.username.as_ref().map(|u| u.to_uppercase()))
            .unwrap_or_else(|| "USER".to_string());

        let query = format!(
            "SELECT table_name FROM all_tables WHERE owner = '{}' ORDER BY table_name",
            owner
        );

        let result = self.execute_query_blocking(query).await?;
        let tables = result
            .rows
            .iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.first() {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        info!(count = tables.len(), owner = %owner, "Listed tables successfully");
        Ok(tables)
    }

    async fn describe_table(&self, table_name: &str, schema: Option<&str>) -> Result<TableInfo> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let owner = schema
            .map(|s| s.to_uppercase())
            .or_else(|| self.config.username.as_ref().map(|u| u.to_uppercase()))
            .unwrap_or_else(|| "USER".to_string());

        let table_upper = table_name.to_uppercase();

        // Query column information
        let query = format!(
            "SELECT column_name, data_type, nullable, data_default \
             FROM all_tab_columns \
             WHERE owner = '{}' AND table_name = '{}' \
             ORDER BY column_id",
            owner, table_upper
        );

        let result = self.execute_query_blocking(query).await?;

        if result.rows.is_empty() {
            return Err(DataError::Query(format!(
                "Table '{}.{}' not found",
                owner, table_name
            )));
        }

        // Query primary key constraints
        let pk_query = format!(
            "SELECT cols.column_name \
             FROM all_constraints cons \
             JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name \
             WHERE cons.constraint_type = 'P' \
             AND cons.owner = '{}' \
             AND cons.table_name = '{}'",
            owner, table_upper
        );

        let pk_result = self.execute_query_blocking(pk_query).await?;
        let primary_keys: std::collections::HashSet<String> = pk_result
            .rows
            .iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.first() {
                    Some(name.to_uppercase())
                } else {
                    None
                }
            })
            .collect();

        // Build column info
        let columns: Vec<ColumnInfo> = result
            .rows
            .iter()
            .map(|row| {
                let col_name = match &row[0] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let data_type = match &row[1] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let nullable = match &row[2] {
                    QueryValue::Text(s) => s == "Y",
                    _ => false,
                };
                let default_value = match &row[3] {
                    QueryValue::Text(s) => Some(s.clone()),
                    _ => None,
                };

                ColumnInfo {
                    name: col_name.clone(),
                    data_type,
                    nullable,
                    default_value,
                    is_primary_key: primary_keys.contains(&col_name.to_uppercase()),
                }
            })
            .collect();

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: Some(owner),
            columns,
        })
    }

    // Metadata methods will use default implementations from trait for now
    // These can be enhanced with Oracle-specific queries later

    async fn get_indexes(&self, table_name: &str, schema: Option<&str>) -> Result<Vec<IndexInfo>> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let owner = schema
            .map(|s| s.to_uppercase())
            .or_else(|| self.config.username.as_ref().map(|u| u.to_uppercase()))
            .unwrap_or_else(|| "USER".to_string());

        let table_upper = table_name.to_uppercase();

        let query = format!(
            "SELECT i.index_name, i.uniqueness, \
             LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) as columns \
             FROM all_indexes i \
             JOIN all_ind_columns ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner \
             WHERE i.table_owner = '{}' AND i.table_name = '{}' \
             GROUP BY i.index_name, i.uniqueness \
             ORDER BY i.index_name",
            owner, table_upper
        );

        let result = self.execute_query_blocking(query).await?;

        let indexes = result
            .rows
            .iter()
            .map(|row| {
                let index_name = match &row[0] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let uniqueness = match &row[1] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let columns_str = match &row[2] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                };

                IndexInfo {
                    name: index_name.clone(),
                    table_name: table_name.to_string(),
                    schema: Some(owner.clone()),
                    columns: columns_str.split(',').map(|s| s.to_string()).collect(),
                    is_unique: uniqueness == "UNIQUE",
                    is_primary: index_name.contains("PK"),
                    index_type: Some("BTREE".to_string()),
                }
            })
            .collect();

        Ok(indexes)
    }

    async fn get_foreign_keys(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let owner = schema
            .map(|s| s.to_uppercase())
            .or_else(|| self.config.username.as_ref().map(|u| u.to_uppercase()))
            .unwrap_or_else(|| "USER".to_string());

        let table_upper = table_name.to_uppercase();

        let query = format!(
            "SELECT \
             a.constraint_name, \
             a.table_name, \
             c.column_name, \
             b.table_name as referenced_table, \
             d.column_name as referenced_column, \
             a.delete_rule \
             FROM all_constraints a \
             JOIN all_constraints b ON a.r_constraint_name = b.constraint_name \
             JOIN all_cons_columns c ON a.constraint_name = c.constraint_name \
             JOIN all_cons_columns d ON b.constraint_name = d.constraint_name \
             WHERE a.constraint_type = 'R' \
             AND a.owner = '{}' \
             AND a.table_name = '{}' \
             ORDER BY a.constraint_name, c.position",
            owner, table_upper
        );

        let result = self.execute_query_blocking(query).await?;

        let mut fk_map: HashMap<String, ForeignKeyInfo> = HashMap::new();

        for row in result.rows {
            let fk_name = match &row[0] {
                QueryValue::Text(s) => s.clone(),
                _ => continue,
            };
            let column = match &row[2] {
                QueryValue::Text(s) => s.clone(),
                _ => continue,
            };
            let ref_table = match &row[3] {
                QueryValue::Text(s) => s.clone(),
                _ => String::new(),
            };
            let ref_column = match &row[4] {
                QueryValue::Text(s) => s.clone(),
                _ => continue,
            };
            let delete_rule = match &row[5] {
                QueryValue::Text(s) => Some(s.clone()),
                _ => None,
            };

            fk_map
                .entry(fk_name.clone())
                .or_insert_with(|| ForeignKeyInfo {
                    name: fk_name.clone(),
                    table_name: table_name.to_string(),
                    schema: Some(owner.clone()),
                    columns: Vec::new(),
                    referenced_table: ref_table,
                    referenced_schema: Some(owner.clone()),
                    referenced_columns: Vec::new(),
                    on_delete: delete_rule,
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

    async fn get_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let owner = schema
            .map(|s| s.to_uppercase())
            .or_else(|| self.config.username.as_ref().map(|u| u.to_uppercase()))
            .unwrap_or_else(|| "USER".to_string());

        let query = format!(
            "SELECT view_name FROM all_views WHERE owner = '{}' ORDER BY view_name",
            owner
        );

        let result = self.execute_query_blocking(query).await?;

        let views = result
            .rows
            .iter()
            .map(|row| ViewInfo {
                name: match &row[0] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                },
                schema: Some(owner.clone()),
                definition: None,
            })
            .collect();

        Ok(views)
    }

    async fn get_view_definition(
        &self,
        view_name: &str,
        schema: Option<&str>,
    ) -> Result<Option<String>> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let owner = schema
            .map(|s| s.to_uppercase())
            .or_else(|| self.config.username.as_ref().map(|u| u.to_uppercase()))
            .unwrap_or_else(|| "USER".to_string());

        let view_upper = view_name.to_uppercase();

        let query = format!(
            "SELECT text FROM all_views WHERE owner = '{}' AND view_name = '{}'",
            owner, view_upper
        );

        let result = self.execute_query_blocking(query).await?;

        Ok(result.rows.first().and_then(|row| match &row[0] {
            QueryValue::Text(s) => Some(s.clone()),
            _ => None,
        }))
    }

    async fn list_stored_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let owner = schema
            .map(|s| s.to_uppercase())
            .or_else(|| self.config.username.as_ref().map(|u| u.to_uppercase()))
            .unwrap_or_else(|| "USER".to_string());

        let query = format!(
            "SELECT object_name, object_type \
             FROM all_procedures \
             WHERE owner = '{}' \
             ORDER BY object_name",
            owner
        );

        let result = self.execute_query_blocking(query).await?;

        let procedures = result
            .rows
            .iter()
            .map(|row| ProcedureInfo {
                name: match &row[0] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                },
                schema: Some(owner.clone()),
                return_type: None,
                language: Some("PL/SQL".to_string()),
            })
            .collect();

        Ok(procedures)
    }

    async fn bulk_insert(
        &self,
        table_name: &str,
        columns: &[String],
        rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        if columns.is_empty() {
            return Err(DataError::Query("No columns specified".to_string()));
        }
        for (i, row) in rows.iter().enumerate() {
            if row.len() != columns.len() {
                return Err(DataError::Query(format!(
                    "Row {} has {} values but {} columns specified",
                    i,
                    row.len(),
                    columns.len()
                )));
            }
        }

        let table_upper = table_name.to_uppercase();
        let cols_str = columns
            .iter()
            .map(|c| c.to_uppercase())
            .collect::<Vec<_>>()
            .join(", ");

        let mut total: u64 = 0;
        for row in rows {
            let literals: Vec<String> =
                row.iter().map(Self::query_value_to_sql_literal).collect();
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_upper,
                cols_str,
                literals.join(", ")
            );
            total += self.execute_statement_blocking(sql).await?;
        }
        Ok(total)
    }

    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, String)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }
        let table_upper = table_name.to_uppercase();
        let mut total: u64 = 0;
        for (set_values, where_clause) in updates {
            if set_values.is_empty() {
                continue;
            }
            let set_parts: Vec<String> = set_values
                .iter()
                .map(|(col, val)| {
                    format!(
                        "{} = {}",
                        col.to_uppercase(),
                        Self::query_value_to_sql_literal(val)
                    )
                })
                .collect();
            let sql = format!(
                "UPDATE {} SET {} WHERE {}",
                table_upper,
                set_parts.join(", "),
                where_clause
            );
            total += self.execute_statement_blocking(sql).await?;
        }
        Ok(total)
    }

    async fn bulk_delete(
        &self,
        table_name: &str,
        where_clauses: &[String],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if where_clauses.is_empty() {
            return Ok(0);
        }
        let table_upper = table_name.to_uppercase();
        let mut total: u64 = 0;
        for where_clause in where_clauses {
            let sql = format!("DELETE FROM {} WHERE {}", table_upper, where_clause);
            total += self.execute_statement_blocking(sql).await?;
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{Connection as ConnectionTrait, DatabaseType, DbAdapter, QueryValue};

    fn make_config(service_name: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: "test-oracle".to_string(),
            name: "Test Oracle".to_string(),
            db_type: DatabaseType::Oracle,
            host: Some("localhost".to_string()),
            port: Some(1521),
            database: service_name.to_string(),
            username: Some("system".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        }
    }

    #[test]
    fn test_new_adapter_stores_config() {
        let config = make_config("FREE");
        let adapter = OracleAdapter::new(config);
        assert_eq!(adapter.config.database, "FREE");
        assert_eq!(adapter.config.db_type, DatabaseType::Oracle);
    }

    #[test]
    fn test_is_connected_initially_false() {
        let adapter = OracleAdapter::new(make_config("FREE"));
        assert!(!ConnectionTrait::is_connected(&adapter));
    }

    #[test]
    fn test_build_connection_params_defaults() {
        let config = make_config("ORCL");
        let (user, _pass, connect_str) = OracleAdapter::build_connection_params(&config, Some("pw"));
        assert_eq!(user, "system");
        assert!(connect_str.contains("1521"));
        assert!(connect_str.ends_with("/ORCL"));
    }

    #[test]
    fn test_build_connection_params_custom_host_port() {
        let mut config = make_config("FREE");
        config.host = Some("db.example.com".to_string());
        config.port = Some(1522);
        let (_user, _pass, connect_str) =
            OracleAdapter::build_connection_params(&config, None);
        assert!(connect_str.starts_with("db.example.com:1522/"));
    }

    #[test]
    fn test_config_accessor() {
        let config = make_config("FREE");
        let adapter = OracleAdapter::new(config.clone());
        assert_eq!(adapter.config().id, config.id);
        assert_eq!(adapter.config().port, Some(1521));
    }

    // ── SQL literal helpers ──────────────────────────────────────────────────

    #[test]
    fn test_sql_literal_null() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Null),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_bool_true() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Bool(true)),
            "1"
        );
    }

    #[test]
    fn test_sql_literal_bool_false() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Bool(false)),
            "0"
        );
    }

    #[test]
    fn test_sql_literal_int() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Int(-42)),
            "-42"
        );
    }

    #[test]
    fn test_sql_literal_float() {
        let lit = OracleAdapter::query_value_to_sql_literal(&QueryValue::Float(3.14));
        assert!(lit.starts_with("3.14"), "got: {}", lit);
    }

    #[test]
    fn test_sql_literal_float_nan_becomes_null() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Float(f64::NAN)),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_float_inf_becomes_null() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Float(f64::INFINITY)),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_text_plain() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Text("hello".to_string())),
            "'hello'"
        );
    }

    #[test]
    fn test_sql_literal_text_with_single_quote() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Text(
                "O'Brien".to_string()
            )),
            "'O''Brien'"
        );
    }

    #[test]
    fn test_sql_literal_bytes() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Bytes(vec![0xFF, 0x00])),
            "HEXTORAW('FF00')"
        );
    }

    #[test]
    fn test_sql_literal_empty_bytes_is_null() {
        assert_eq!(
            OracleAdapter::query_value_to_sql_literal(&QueryValue::Bytes(vec![])),
            "NULL"
        );
    }

    // ── dtype mapping helpers ────────────────────────────────────────────────

    #[test]
    fn test_dtype_mapping_int_types() {
        use polars::prelude::DataType;
        assert_eq!(OracleAdapter::polars_dtype_to_oracle_type(&DataType::Int8), "NUMBER(10)");
        assert_eq!(OracleAdapter::polars_dtype_to_oracle_type(&DataType::Int16), "NUMBER(10)");
        assert_eq!(OracleAdapter::polars_dtype_to_oracle_type(&DataType::Int32), "NUMBER(10)");
        assert_eq!(OracleAdapter::polars_dtype_to_oracle_type(&DataType::Int64), "NUMBER(19)");
        assert_eq!(OracleAdapter::polars_dtype_to_oracle_type(&DataType::UInt64), "NUMBER(20)");
    }

    #[test]
    fn test_dtype_mapping_float_types() {
        use polars::prelude::DataType;
        assert_eq!(
            OracleAdapter::polars_dtype_to_oracle_type(&DataType::Float32),
            "BINARY_FLOAT"
        );
        assert_eq!(
            OracleAdapter::polars_dtype_to_oracle_type(&DataType::Float64),
            "BINARY_DOUBLE"
        );
    }

    #[test]
    fn test_dtype_mapping_string_and_bool() {
        use polars::prelude::DataType;
        assert_eq!(
            OracleAdapter::polars_dtype_to_oracle_type(&DataType::Boolean),
            "NUMBER(1)"
        );
        assert_eq!(
            OracleAdapter::polars_dtype_to_oracle_type(&DataType::String),
            "VARCHAR2(4000)"
        );
        assert_eq!(
            OracleAdapter::polars_dtype_to_oracle_type(&DataType::Binary),
            "BLOB"
        );
    }

    #[test]
    fn test_dtype_mapping_unknown_falls_back_to_varchar2() {
        use polars::prelude::DataType;
        assert_eq!(
            OracleAdapter::polars_dtype_to_oracle_type(&DataType::Date),
            "VARCHAR2(4000)"
        );
    }
}
