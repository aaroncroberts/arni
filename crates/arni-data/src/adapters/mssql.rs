//! Microsoft SQL Server database adapter implementation.
//!
//! This module provides the [`SqlServerAdapter`] which implements both the [`Connection`]
//! and [`DbAdapter`] traits for SQL Server databases using the `tiberius` async driver.
//!
//! Connections are managed through a `bb8` connection pool backed by `bb8-tiberius`,
//! which handles TCP setup and the `tokio-util` compat layer internally.
//!
//! # SSL/TLS Support
//!
//! TLS is controlled by the `use_ssl` configuration option:
//! - `use_ssl: false` — plain connection (`EncryptionLevel::NotSupported`). Certificate
//!   validation is automatically skipped because no TLS handshake occurs.
//! - `use_ssl: true` — encrypted connection (`EncryptionLevel::Required`). The server
//!   certificate is validated using the system trust store **unless** the connection
//!   parameter `trust_server_certificate=true` is set (useful for dev/CI with
//!   self-signed certificates).
//!
//! Example with self-signed cert:
//! ```text
//! arni config add dev-sql \
//!   --type sqlserver --host localhost --database mydb \
//!   --param use_ssl=true --param trust_server_certificate=true
//! ```

use crate::adapter::{
    escape_like_pattern, filter_to_sql, AdapterMetadata, ColumnInfo, Connection as ConnectionTrait,
    ConnectionConfig, DatabaseType, DbAdapter, FilterExpr, ForeignKeyInfo, IndexInfo,
    ProcedureInfo, QueryResult, QueryValue, ServerInfo, TableInfo, TableSearchMode, ViewInfo,
};
use crate::DataError;
use polars::prelude::*;
use std::collections::HashMap;
use tiberius::{AuthMethod, Config};
use tracing::{debug, error, info, instrument, warn};

type MssqlPool = bb8::Pool<bb8_tiberius::ConnectionManager>;

type Result<T> = std::result::Result<T, DataError>;

/// Microsoft SQL Server database adapter using tiberius + bb8 pool
///
/// This adapter uses tiberius with a bb8 connection pool to connect to SQL Server.
///
/// # Connection Management
///
/// `connect()` builds a `bb8::Pool<bb8_tiberius::ConnectionManager>`. Individual
/// queries acquire a pooled connection for their duration and release it back
/// immediately, enabling true concurrent access without serialisation.
///
/// # Thread Safety
///
/// `bb8::Pool` is `Clone + Send + Sync`. No additional locking is needed.
pub struct SqlServerAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// Password stored for pool creation (tiberius config holds credentials)
    password: Option<String>,
    /// bb8 connection pool (None until connect() is called)
    pool: Option<MssqlPool>,
}

impl SqlServerAdapter {
    /// Create a new SQL Server adapter with the given configuration
    ///
    /// This does not establish a connection immediately. Call [`connect`](ConnectionTrait::connect)
    /// to establish the connection.
    pub fn new(config: ConnectionConfig) -> Self {
        debug!(database = %config.database, "Creating SQL Server adapter");
        Self {
            config,
            password: None,
            pool: None,
        }
    }

    /// Validate database name
    fn validate_database_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(DataError::Config(
                "Database name cannot be empty".to_string(),
            ));
        }
        if name.len() > 128 {
            return Err(DataError::Config(format!(
                "Database name too long (max 128 chars): {}",
                name.len()
            )));
        }
        Ok(())
    }

    /// Build a tiberius config from connection configuration
    fn build_config(config: &ConnectionConfig, password: Option<&str>) -> Result<Config> {
        let host = config.host.as_deref().unwrap_or("localhost");
        let port = config.port.unwrap_or(1433);
        let username = config.username.as_deref().unwrap_or("sa");
        let password = password.unwrap_or("");
        let database = &config.database;

        let mut tiberius_config = Config::new();
        tiberius_config.host(host);
        tiberius_config.port(port);
        tiberius_config.database(database);
        tiberius_config.authentication(AuthMethod::sql_server(username, password));

        if config.use_ssl {
            tiberius_config.encryption(tiberius::EncryptionLevel::Required);
        } else {
            tiberius_config.encryption(tiberius::EncryptionLevel::NotSupported);
        }

        // trust_server_certificate=true skips TLS certificate validation.
        // This is safe when use_ssl=false (no TLS at all) and useful for
        // dev/CI environments with self-signed certs when use_ssl=true.
        // In production with use_ssl=true, leave this unset (default: false)
        // so the system trust store validates the server certificate.
        let trust_cert = !config.use_ssl
            || config
                .parameters
                .get("trust_server_certificate")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false);
        if trust_cert {
            tiberius_config.trust_cert();
        }

        Ok(tiberius_config)
    }

    /// Build a bb8 connection pool for the given config and password
    async fn build_pool(config: &ConnectionConfig, password: Option<&str>) -> Result<MssqlPool> {
        let tiberius_config = Self::build_config(config, password)?;
        let manager = bb8_tiberius::ConnectionManager::new(tiberius_config);
        let pc = config.pool_config.clone().unwrap_or_default();
        debug!(
            max_size = pc.max_connections,
            min_idle = pc.min_connections,
            connection_timeout_secs = pc.acquire_timeout_secs,
            idle_timeout_secs = pc.idle_timeout_secs,
            max_lifetime_secs = pc.max_lifetime_secs,
            "Building SQL Server connection pool (bb8)"
        );
        bb8::Pool::builder()
            .max_size(pc.max_connections)
            .min_idle(Some(pc.min_connections))
            .connection_timeout(std::time::Duration::from_secs(pc.acquire_timeout_secs))
            .idle_timeout(Some(std::time::Duration::from_secs(pc.idle_timeout_secs)))
            .max_lifetime(Some(std::time::Duration::from_secs(pc.max_lifetime_secs)))
            .build(manager)
            .await
            .map_err(|e| {
                error!(adapter = "mssql", operation = "connect", error = %e, "Failed to create connection pool");
                DataError::Connection(format!("Failed to create pool: {}", e))
            })
    }

    /// Convert a [`QueryValue`] to a SQL Server SQL literal
    fn query_value_to_sql_literal(value: &QueryValue) -> String {
        match value {
            QueryValue::Null => "NULL".to_string(),
            QueryValue::Bool(b) => {
                if *b {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
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
                format!("N'{}'", escaped)
            }
            QueryValue::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                if hex.is_empty() {
                    "NULL".to_string()
                } else {
                    format!("0x{}", hex)
                }
            }
        }
    }

    /// Return true when `sql` is a DDL statement that must run as a standalone
    /// batch (i.e. cannot be wrapped in `sp_executesql`).
    ///
    /// tiberius 0.12.3 sends *every* query through `sp_executesql`, but SQL
    /// Server requires CREATE VIEW / PROCEDURE / FUNCTION / TRIGGER to be the
    /// only statement in the batch.  Wrapping them with `EXEC(N'...')` creates
    /// a nested batch that satisfies this constraint.
    fn needs_exec_wrapper(sql: &str) -> bool {
        let upper = sql.trim_start().to_uppercase();
        upper.starts_with("CREATE VIEW")
            || upper.starts_with("ALTER VIEW")
            || upper.starts_with("CREATE PROCEDURE")
            || upper.starts_with("ALTER PROCEDURE")
            || upper.starts_with("CREATE PROC ")
            || upper.starts_with("ALTER PROC ")
            || upper.starts_with("CREATE FUNCTION")
            || upper.starts_with("ALTER FUNCTION")
            || upper.starts_with("CREATE TRIGGER")
            || upper.starts_with("ALTER TRIGGER")
    }

    /// Map a Polars [`DataType`] to a SQL Server type string
    fn polars_dtype_to_mssql_type(dtype: &DataType) -> &'static str {
        match dtype {
            DataType::Boolean => "BIT",
            DataType::Int8 | DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INT",
            DataType::Int64 => "BIGINT",
            DataType::UInt8 | DataType::UInt16 => "SMALLINT",
            DataType::UInt32 => "INT",
            DataType::UInt64 => "BIGINT",
            DataType::Float32 => "REAL",
            DataType::Float64 => "FLOAT",
            DataType::String => "NVARCHAR(MAX)",
            DataType::Binary => "VARBINARY(MAX)",
            _ => "NVARCHAR(MAX)",
        }
    }

    /// Extract a value from a Series at `row_idx` as a SQL Server SQL literal
    fn series_value_to_sql_literal(series: &Series, row_idx: usize) -> Result<String> {
        if series.is_null().get(row_idx).unwrap_or(false) {
            return Ok("NULL".to_string());
        }
        let lit = match series.dtype() {
            DataType::Boolean => {
                let v = series
                    .bool()
                    .map_err(|e| {
                        DataError::TypeConversion(format!(
                            "Failed to read column \'{}\' at row {}: {}",
                            series.name(),
                            row_idx,
                            e
                        ))
                    })?
                    .get(row_idx)
                    .unwrap_or(false);
                if v {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            DataType::Int8 => series
                .i8()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Int16 => series
                .i16()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Int32 => series
                .i32()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Int64 => series
                .i64()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt8 => series
                .u8()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt16 => series
                .u16()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt32 => series
                .u32()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::UInt64 => series
                .u64()
                .map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?
                .get(row_idx)
                .unwrap_or(0)
                .to_string(),
            DataType::Float32 => {
                let v = series
                    .f32()
                    .map_err(|e| {
                        DataError::TypeConversion(format!(
                            "Failed to read column \'{}\' at row {}: {}",
                            series.name(),
                            row_idx,
                            e
                        ))
                    })?
                    .get(row_idx)
                    .unwrap_or(0.0);
                if v.is_nan() || v.is_infinite() {
                    "NULL".to_string()
                } else {
                    v.to_string()
                }
            }
            DataType::Float64 => {
                let v = series
                    .f64()
                    .map_err(|e| {
                        DataError::TypeConversion(format!(
                            "Failed to read column \'{}\' at row {}: {}",
                            series.name(),
                            row_idx,
                            e
                        ))
                    })?
                    .get(row_idx)
                    .unwrap_or(0.0);
                if v.is_nan() || v.is_infinite() {
                    "NULL".to_string()
                } else {
                    v.to_string()
                }
            }
            DataType::String => {
                let v = series
                    .str()
                    .map_err(|e| {
                        DataError::TypeConversion(format!(
                            "Failed to read column \'{}\' at row {}: {}",
                            series.name(),
                            row_idx,
                            e
                        ))
                    })?
                    .get(row_idx)
                    .unwrap_or("");
                format!("N'{}'", v.replace('\'', "''"))
            }
            DataType::Binary => {
                let v = series
                    .binary()
                    .map_err(|e| {
                        DataError::TypeConversion(format!(
                            "Failed to read column \'{}\' at row {}: {}",
                            series.name(),
                            row_idx,
                            e
                        ))
                    })?
                    .get(row_idx)
                    .unwrap_or(&[]);
                let hex: String = v.iter().map(|b| format!("{:02X}", b)).collect();
                if hex.is_empty() {
                    "NULL".to_string()
                } else {
                    format!("0x{}", hex)
                }
            }
            _ => {
                let cast = series.cast(&DataType::String).map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Failed to read column \'{}\' at row {}: {}",
                        series.name(),
                        row_idx,
                        e
                    ))
                })?;
                match cast
                    .str()
                    .map_err(|e| {
                        DataError::TypeConversion(format!(
                            "Failed to read column \'{}\' at row {}: {}",
                            series.name(),
                            row_idx,
                            e
                        ))
                    })?
                    .get(row_idx)
                {
                    Some(s) => format!("N'{}'", s.replace('\'', "''")),
                    None => "NULL".to_string(),
                }
            }
        };
        Ok(lit)
    }

    /// Execute a DML or DDL statement, returning rows affected
    async fn execute_statement(&self, sql: &str) -> Result<u64> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            DataError::Connection("Not connected - call connect() first".to_string())
        })?;
        let mut conn = pool.get().await.map_err(|e| {
            DataError::Connection(format!("Failed to acquire connection: {}", e))
        })?;
        let result = conn
            .execute(sql, &[])
            .await
            .map_err(|e| DataError::Query(format!("Statement execution failed: {}", e)))?;
        Ok(result.rows_affected().iter().sum::<u64>())
    }

    /// Convert a SQL Server row to QueryValue vector
    fn row_to_values(row: &tiberius::Row) -> Result<Vec<QueryValue>> {
        let mut values = Vec::new();

        for i in 0..row.len() {
            // Try to get the value as different types
            let value = if let Ok(Some(v)) = row.try_get::<&str, usize>(i) {
                QueryValue::Text(v.to_string())
            } else if let Ok(Some(v)) = row.try_get::<i32, usize>(i) {
                QueryValue::Int(v as i64)
            } else if let Ok(Some(v)) = row.try_get::<i64, usize>(i) {
                QueryValue::Int(v)
            } else if let Ok(Some(v)) = row.try_get::<f64, usize>(i) {
                QueryValue::Float(v)
            } else if let Ok(Some(v)) = row.try_get::<bool, usize>(i) {
                QueryValue::Bool(v)
            } else if let Ok(Some(v)) = row.try_get::<&[u8], usize>(i) {
                QueryValue::Bytes(v.to_vec())
            } else {
                // NULL or unsupported type
                QueryValue::Null
            };

            values.push(value);
        }

        Ok(values)
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for SqlServerAdapter {
    #[instrument(skip(self), fields(adapter = "sqlserver", database = %self.config.database))]
    async fn connect(&mut self) -> Result<()> {
        if self.config.db_type != DatabaseType::SQLServer {
            let err = DataError::Config(format!(
                "Invalid database type: expected SQLServer, got {:?}",
                self.config.db_type
            ));
            error!(adapter = "mssql", operation = "connect", error = %err, "Invalid database type");
            return Err(err);
        }

        Self::validate_database_name(&self.config.database)?;

        let host = self.config.host.as_deref().unwrap_or("localhost");
        let port = self.config.port.unwrap_or(1433);
        info!(host, port, database = %self.config.database, "Connecting to SQL Server");

        self.pool = Some(Self::build_pool(&self.config, self.password.as_deref()).await?);

        info!("Connected to SQL Server successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(adapter = "sqlserver"))]
    async fn disconnect(&mut self) -> Result<()> {
        debug!("Disconnecting from SQL Server");
        if self.pool.take().is_some() {
            info!("Disconnected from SQL Server");
        } else {
            debug!("Disconnect called but already disconnected");
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.pool.is_some()
    }

    #[instrument(skip(self), fields(adapter = "sqlserver"))]
    async fn health_check(&self) -> Result<bool> {
        debug!("Performing health check");
        let pool = match self.pool.as_ref() {
            Some(p) => p,
            None => {
                warn!("Health check called but not connected");
                return Ok(false);
            }
        };
        let mut conn = pool.get().await.map_err(|e| {
            warn!(error = %e, "Health check: failed to acquire connection");
            DataError::Connection(format!("Health check failed: {}", e))
        })?;
        conn.query("SELECT 1", &[])
            .await
            .map(|_| {
                debug!("Health check passed");
                true
            })
            .map_err(|e| {
                warn!(error = %e, "Health check failed");
                DataError::Connection(format!("Health check failed: {}", e))
            })
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[async_trait::async_trait]
impl DbAdapter for SqlServerAdapter {
    #[instrument(skip(self, config, password), fields(adapter = "sqlserver", database = %config.database))]
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<&str>) -> Result<()> {
        self.config = config.clone();
        self.password = password.map(|p| p.to_string());
        ConnectionTrait::connect(self).await
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
        Self::validate_database_name(&config.database)?;
        let result = Self::build_pool(config, password).await;
        Ok(result.is_ok())
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::SQLServer
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    #[instrument(skip(self, query), fields(adapter = "sqlserver", query_length = query.len()))]
    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let sql_type = super::common::detect_sql_type(query);
        debug!(
            sql_type,
            sql_preview = %super::common::sql_preview(query, 100),
            "Executing query"
        );
        let start = std::time::Instant::now();

        let pool = self.pool.as_ref().ok_or_else(|| {
            error!(adapter = "mssql", operation = "execute_query", "Not connected");
            DataError::Connection("Not connected - call connect() first".to_string())
        })?;
        let mut conn = pool.get().await.map_err(|e| {
            error!(adapter = "mssql", operation = "execute_query", "Failed to acquire connection from pool");
            DataError::Connection(format!("Failed to acquire connection: {}", e))
        })?;

        // tiberius wraps every call in sp_executesql.  DDL like CREATE VIEW
        // must be the sole statement in its batch, so we wrap it with EXEC(N'...')
        // which provides a nested batch scope that satisfies SQL Server's parser.
        let owned;
        let effective_query = if Self::needs_exec_wrapper(query) {
            let escaped = query.replace('\'', "''");
            owned = format!("EXEC(N'{}')", escaped);
            debug!("Wrapping DDL in EXEC() for standalone-batch requirement");
            owned.as_str()
        } else {
            query
        };

        let stream = conn.query(effective_query, &[]).await.map_err(|e| {
            error!(adapter = "mssql", operation = "execute_query", sql_type, error = %e, "Query execution failed");
            DataError::Query(format!("Query failed: {}", e))
        })?;

        let rows: Vec<tiberius::Row> = stream
            .into_results()
            .await
            .map_err(|e| {
                error!(adapter = "mssql", operation = "execute_query", sql_type, error = %e, "Failed to fetch results");
                DataError::Query(format!("Failed to fetch results: {}", e))
            })?
            .into_iter()
            .flatten()
            .collect();

        if rows.is_empty() {
            debug!("Query returned no rows");
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: None,
            });
        }

        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let mut result_rows = Vec::new();
        for row in &rows {
            result_rows.push(Self::row_to_values(row)?);
        }

        let duration = start.elapsed();
        info!(
            sql_type,
            duration_ms = duration.as_millis(),
            rows = result_rows.len(),
            columns = columns.len(),
            "Query executed successfully"
        );

        Ok(QueryResult {
            columns,
            rows: result_rows,
            rows_affected: None,
        })
    }

    #[instrument(skip(self), fields(adapter = "sqlserver"))]
    async fn list_databases(&self) -> Result<Vec<String>> {
        let query = "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name";
        let result = self.execute_query(query).await?;

        let databases = result
            .rows
            .into_iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.first() {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        Ok(databases)
    }

    #[instrument(skip(self), fields(adapter = "sqlserver", schema = ?schema))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        let schema_filter = schema.unwrap_or("dbo");
        let query = format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = '{}' AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
            schema_filter
        );
        let result = self.execute_query(&query).await?;

        let tables = result
            .rows
            .into_iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.first() {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        Ok(tables)
    }

    #[instrument(skip(self), fields(adapter = "sqlserver", pattern = %pattern, mode = ?mode, schema = ?schema))]
    async fn find_tables(
        &self,
        pattern: &str,
        schema: Option<&str>,
        mode: TableSearchMode,
    ) -> Result<Vec<String>> {
        let schema_filter = schema.unwrap_or("dbo");

        let escaped = escape_like_pattern(pattern);
        let like_pattern = match mode {
            TableSearchMode::StartsWith => format!("{}%", escaped),
            TableSearchMode::Contains => format!("%{}%", escaped),
            TableSearchMode::EndsWith => format!("%{}", escaped),
        };
        // Escape single quotes for safe inline SQL formatting
        let safe_pattern = like_pattern.replace('\'', "''");

        let query = format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = '{}' AND table_type = 'BASE TABLE' \
             AND table_name LIKE '{}' ESCAPE '\\' \
             ORDER BY table_name",
            schema_filter, safe_pattern
        );

        let result = self.execute_query(&query).await?;
        let tables = result
            .rows
            .into_iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.into_iter().next() {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();

        Ok(tables)
    }

    #[instrument(skip(self), fields(adapter = "sqlserver", table = %table_name, schema = ?schema))]
    async fn describe_table(&self, table_name: &str, schema: Option<&str>) -> Result<TableInfo> {
        let schema_name = schema.unwrap_or("dbo");

        let query = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = '{}' AND table_name = '{}' \
             ORDER BY ordinal_position",
            schema_name, table_name
        );

        let result = self.execute_query(&query).await?;

        if result.rows.is_empty() {
            return Err(DataError::Query(format!(
                "Table '{}.{}' not found",
                schema_name, table_name
            )));
        }

        let mut columns = Vec::new();
        for row in result.rows {
            if row.len() >= 3 {
                if let (
                    QueryValue::Text(name),
                    QueryValue::Text(data_type),
                    QueryValue::Text(nullable),
                ) = (&row[0], &row[1], &row[2])
                {
                    columns.push(ColumnInfo {
                        name: name.clone(),
                        data_type: data_type.clone(),
                        nullable: nullable == "YES",
                        default_value: None,
                        is_primary_key: false, // Would need additional query
                    });
                }
            }
        }

        // Fetch row count, total size, and creation date from sys catalog
        let stats_query = format!(
            "SELECT SUM(p.rows), SUM(a.total_pages) * 8192, \
                    CONVERT(NVARCHAR(30), t.create_date, 127) \
             FROM sys.tables t \
             JOIN sys.schemas s ON t.schema_id = s.schema_id \
             JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1) \
             JOIN sys.allocation_units a ON p.partition_id = a.container_id \
             WHERE t.name = '{}' AND s.name = '{}' \
             GROUP BY t.create_date",
            table_name, schema_name
        );
        let stats_result = self.execute_query(&stats_query).await.ok();
        let (row_count, size_bytes, created_at) = stats_result
            .as_ref()
            .and_then(|r| r.rows.first())
            .map(|row| {
                let rc = match row.first() {
                    Some(QueryValue::Int(n)) => Some(*n),
                    _ => None,
                };
                let sz = match row.get(1) {
                    Some(QueryValue::Int(n)) => Some(*n),
                    _ => None,
                };
                let ca = match row.get(2) {
                    Some(QueryValue::Text(s)) => Some(s.clone()),
                    _ => None,
                };
                (rc, sz, ca)
            })
            .unwrap_or((None, None, None));

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name.to_string()),
            columns,
            row_count,
            size_bytes,
            created_at,
        })
    }

    #[instrument(skip(self), fields(adapter = "sqlserver"))]
    async fn get_server_info(&self) -> Result<ServerInfo> {
        let query = "SELECT @@VERSION AS version";
        let result = self.execute_query(query).await?;

        let version = result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|val| match val {
                QueryValue::Text(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "Unknown".to_string());

        Ok(ServerInfo {
            version,
            server_type: "SQL Server".to_string(),
            extra_info: HashMap::new(),
        })
    }

    async fn get_indexes(&self, table_name: &str, schema: Option<&str>) -> Result<Vec<IndexInfo>> {
        let schema_name = schema.unwrap_or("dbo");

        let query = format!(
            "SELECT i.name AS index_name, \
                    c.name AS column_name, \
                    i.is_unique, \
                    i.is_primary_key, \
                    i.type_desc \
             FROM sys.indexes i \
             INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
             INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
             INNER JOIN sys.tables t ON i.object_id = t.object_id \
             INNER JOIN sys.schemas s ON t.schema_id = s.schema_id \
             WHERE t.name = '{}' AND s.name = '{}' \
             ORDER BY i.name, ic.key_ordinal",
            table_name, schema_name
        );

        let result = self.execute_query(&query).await?;

        let mut indexes: HashMap<String, (Vec<String>, bool, bool, String)> = HashMap::new();

        for row in result.rows {
            if row.len() >= 5 {
                if let (
                    QueryValue::Text(idx_name),
                    QueryValue::Text(col_name),
                    QueryValue::Bool(is_unique),
                    QueryValue::Bool(is_primary),
                    QueryValue::Text(idx_type),
                ) = (&row[0], &row[1], &row[2], &row[3], &row[4])
                {
                    indexes
                        .entry(idx_name.clone())
                        .or_insert_with(|| (Vec::new(), *is_unique, *is_primary, idx_type.clone()))
                        .0
                        .push(col_name.clone());
                }
            }
        }

        let result_indexes = indexes
            .into_iter()
            .map(
                |(name, (columns, is_unique, is_primary, idx_type))| IndexInfo {
                    name,
                    table_name: table_name.to_string(),
                    schema: Some(schema_name.to_string()),
                    columns,
                    is_unique,
                    is_primary,
                    index_type: Some(idx_type),
                },
            )
            .collect();

        Ok(result_indexes)
    }

    async fn get_foreign_keys(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let schema_name = schema.unwrap_or("dbo");

        let query = format!(
            "SELECT fk.name AS fk_name, \
                    c1.name AS column_name, \
                    t2.name AS ref_table, \
                    c2.name AS ref_column \
             FROM sys.foreign_keys fk \
             INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id \
             INNER JOIN sys.tables t1 ON fkc.parent_object_id = t1.object_id \
             INNER JOIN sys.schemas s1 ON t1.schema_id = s1.schema_id \
             INNER JOIN sys.columns c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id \
             INNER JOIN sys.tables t2 ON fkc.referenced_object_id = t2.object_id \
             INNER JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id \
             WHERE t1.name = '{}' AND s1.name = '{}' \
             ORDER BY fk.name",
            table_name, schema_name
        );

        let result = self.execute_query(&query).await?;

        let mut fks: HashMap<String, (Vec<String>, String, Vec<String>)> = HashMap::new();

        for row in result.rows {
            if row.len() >= 4 {
                if let (
                    QueryValue::Text(fk_name),
                    QueryValue::Text(col_name),
                    QueryValue::Text(ref_table),
                    QueryValue::Text(ref_col),
                ) = (&row[0], &row[1], &row[2], &row[3])
                {
                    let entry = fks
                        .entry(fk_name.clone())
                        .or_insert_with(|| (Vec::new(), ref_table.clone(), Vec::new()));
                    entry.0.push(col_name.clone());
                    entry.2.push(ref_col.clone());
                }
            }
        }

        let result_fks = fks
            .into_iter()
            .map(|(name, (columns, ref_table, ref_columns))| ForeignKeyInfo {
                name,
                table_name: table_name.to_string(),
                schema: Some(schema_name.to_string()),
                columns,
                referenced_table: ref_table,
                referenced_schema: Some(schema_name.to_string()),
                referenced_columns: ref_columns,
                on_delete: None,
                on_update: None,
            })
            .collect();

        Ok(result_fks)
    }

    async fn get_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let schema_filter = schema.unwrap_or("dbo");
        let query = format!(
            "SELECT table_name FROM information_schema.views \
             WHERE table_schema = '{}' \
             ORDER BY table_name",
            schema_filter
        );
        let result = self.execute_query(&query).await?;

        let views = result
            .rows
            .into_iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.first() {
                    Some(ViewInfo {
                        name: name.clone(),
                        schema: Some(schema_filter.to_string()),
                        definition: None,
                    })
                } else {
                    None
                }
            })
            .collect();

        Ok(views)
    }

    async fn get_view_definition(
        &self,
        view_name: &str,
        schema: Option<&str>,
    ) -> Result<Option<String>> {
        let schema_name = schema.unwrap_or("dbo");
        let query = format!(
            "SELECT view_definition FROM information_schema.views \
             WHERE table_schema = '{}' AND table_name = '{}'",
            schema_name, view_name
        );
        let result = self.execute_query(&query).await?;

        let definition =
            result
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(|val| match val {
                    QueryValue::Text(s) => Some(s.clone()),
                    _ => None,
                });

        Ok(definition)
    }

    async fn list_stored_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        let schema_filter = schema.unwrap_or("dbo");
        let query = format!(
            "SELECT routine_name, routine_type \
             FROM information_schema.routines \
             WHERE routine_schema = '{}' \
             ORDER BY routine_name",
            schema_filter
        );
        let result = self.execute_query(&query).await?;

        let procedures = result
            .rows
            .into_iter()
            .filter_map(|row| {
                if row.len() >= 2 {
                    if let (QueryValue::Text(name), QueryValue::Text(routine_type)) =
                        (&row[0], &row[1])
                    {
                        Some(ProcedureInfo {
                            name: name.clone(),
                            schema: Some(schema_filter.to_string()),
                            return_type: None,
                            language: Some(routine_type.clone()),
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        Ok(procedures)
    }

    #[instrument(skip(self, df), fields(adapter = "sqlserver", table = %table_name, rows = df.height(), columns = df.width(), replace = replace))]
    async fn export_dataframe(
        &self,
        df: &DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        replace: bool,
    ) -> Result<u64> {
        if self.pool.is_none() {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let nrows = df.height();
        info!(
            table = %table_name,
            rows = nrows,
            columns = df.width(),
            replace,
            "Starting DataFrame export"
        );
        let export_start = std::time::Instant::now();

        if replace {
            let drop_sql = format!(
                "IF OBJECT_ID(N'{}', N'U') IS NOT NULL DROP TABLE {}",
                table_name, table_name
            );
            self.execute_statement(&drop_sql).await?;

            let col_defs: Vec<String> = df
                .columns()
                .iter()
                .map(|col| {
                    format!(
                        "{} {}",
                        col.name(),
                        Self::polars_dtype_to_mssql_type(col.dtype())
                    )
                })
                .collect();
            let create_sql = format!("CREATE TABLE {} ({})", table_name, col_defs.join(", "));
            self.execute_statement(&create_sql).await?;
        }

        if nrows == 0 {
            info!(table = %table_name, rows_written = 0u64, duration_ms = export_start.elapsed().as_millis(), "DataFrame export complete");
            return Ok(0);
        }

        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect();
        let cols_str = column_names.join(", ");
        let mut total: u64 = 0;

        for row_idx in 0..nrows {
            let mut literals = Vec::with_capacity(column_names.len());
            for col_name in &column_names {
                let series = df
                    .column(col_name)
                    .map_err(|e| {
                        DataError::TypeConversion(format!(
                            "Failed to read column \'{}\' at row {}: {}",
                            col_name, row_idx, e
                        ))
                    })?
                    .as_materialized_series();
                literals.push(Self::series_value_to_sql_literal(series, row_idx)?);
            }
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                cols_str,
                literals.join(", ")
            );
            total += self.execute_statement(&insert_sql).await?;
            if (row_idx + 1) % 1000 == 0 {
                debug!(rows_inserted = total, total_rows = nrows, "Export progress");
            }
        }

        info!(
            table = %table_name,
            rows_written = total,
            duration_ms = export_start.elapsed().as_millis(),
            "DataFrame export complete"
        );
        Ok(total)
    }

    #[instrument(skip(self, columns, rows), fields(adapter = "sqlserver", table = %table_name, row_count = rows.len()))]
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

        let cols_str = columns.join(", ");
        let mut total: u64 = 0;
        for row in rows {
            let literals: Vec<String> = row.iter().map(Self::query_value_to_sql_literal).collect();
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                cols_str,
                literals.join(", ")
            );
            total += self.execute_statement(&sql).await?;
        }
        Ok(total)
    }

    #[instrument(skip(self, updates), fields(adapter = "sqlserver", table = %table_name))]
    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, FilterExpr)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }
        let mut total: u64 = 0;
        for (set_values, filter) in updates {
            if set_values.is_empty() {
                continue;
            }
            let set_parts: Vec<String> = set_values
                .iter()
                .map(|(col, val)| format!("{} = {}", col, Self::query_value_to_sql_literal(val)))
                .collect();
            let sql = format!(
                "UPDATE {} SET {} WHERE {}",
                table_name,
                set_parts.join(", "),
                filter_to_sql(filter)
            );
            total += self.execute_statement(&sql).await?;
        }
        Ok(total)
    }

    #[instrument(skip(self, filters), fields(adapter = "sqlserver", table = %table_name))]
    async fn bulk_delete(
        &self,
        table_name: &str,
        filters: &[FilterExpr],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if filters.is_empty() {
            return Ok(0);
        }
        let mut total: u64 = 0;
        for filter in filters {
            let sql = format!("DELETE FROM {} WHERE {}", table_name, filter_to_sql(filter));
            total += self.execute_statement(&sql).await?;
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{Connection as ConnectionTrait, DatabaseType, QueryValue};

    fn make_config(database: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: "test-mssql".to_string(),
            name: "Test SQL Server".to_string(),
            db_type: DatabaseType::SQLServer,
            host: Some("localhost".to_string()),
            port: Some(1433),
            database: database.to_string(),
            username: Some("sa".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        }
    }

    #[test]
    fn test_new_adapter_stores_config() {
        let config = make_config("test_db");
        let adapter = SqlServerAdapter::new(config);
        assert_eq!(adapter.config.database, "test_db");
        assert_eq!(adapter.config.db_type, DatabaseType::SQLServer);
    }

    #[test]
    fn test_is_connected_initially_false() {
        let adapter = SqlServerAdapter::new(make_config("test_db"));
        assert!(!ConnectionTrait::is_connected(&adapter));
    }

    #[test]
    fn test_validate_database_name_valid() {
        assert!(SqlServerAdapter::validate_database_name("test_db").is_ok());
        assert!(SqlServerAdapter::validate_database_name("MyDatabase").is_ok());
    }

    #[test]
    fn test_validate_database_name_empty_fails() {
        let err = SqlServerAdapter::validate_database_name("").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_validate_database_name_too_long_fails() {
        let long_name = "a".repeat(129);
        let err = SqlServerAdapter::validate_database_name(&long_name).unwrap_err();
        assert!(err.to_string().contains("too long"));
    }

    #[test]
    fn test_build_config_default_values() {
        let config = make_config("master");
        let result = SqlServerAdapter::build_config(&config, Some("password"));
        assert!(result.is_ok());
    }

    // ── SSL / trust_cert logic ───────────────────────────────────────────────

    #[test]
    fn test_build_config_no_ssl_builds_ok() {
        // use_ssl=false: encryption disabled, trust_cert called (harmless, no TLS)
        let mut config = make_config("master");
        config.use_ssl = false;
        assert!(SqlServerAdapter::build_config(&config, Some("pw")).is_ok());
    }

    #[test]
    fn test_build_config_ssl_no_trust_param_builds_ok() {
        // use_ssl=true, no trust param → cert validation ON (trust_cert NOT called)
        let mut config = make_config("master");
        config.use_ssl = true;
        assert!(SqlServerAdapter::build_config(&config, Some("pw")).is_ok());
    }

    #[test]
    fn test_build_config_ssl_with_trust_param_true() {
        // use_ssl=true, trust_server_certificate=true → trust_cert called
        let mut config = make_config("master");
        config.use_ssl = true;
        config
            .parameters
            .insert("trust_server_certificate".into(), "true".into());
        assert!(SqlServerAdapter::build_config(&config, Some("pw")).is_ok());
    }

    #[test]
    fn test_build_config_ssl_with_trust_param_one() {
        // trust_server_certificate=1 also accepted
        let mut config = make_config("master");
        config.use_ssl = true;
        config
            .parameters
            .insert("trust_server_certificate".into(), "1".into());
        assert!(SqlServerAdapter::build_config(&config, Some("pw")).is_ok());
    }

    #[test]
    fn test_build_config_ssl_trust_cert_logic() {
        // When use_ssl=false, trust_cert is always set (no-op, no TLS)
        let mut config = make_config("master");
        config.use_ssl = false;
        let trust = !config.use_ssl
            || config
                .parameters
                .get("trust_server_certificate")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false);
        assert!(trust, "use_ssl=false → trust_cert=true (harmless)");

        // When use_ssl=true without param, trust_cert is NOT set
        config.use_ssl = true;
        let trust = !config.use_ssl
            || config
                .parameters
                .get("trust_server_certificate")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false);
        assert!(!trust, "use_ssl=true without param → trust_cert=false (validates cert)");

        // When use_ssl=true with param=true, trust_cert IS set
        config
            .parameters
            .insert("trust_server_certificate".into(), "true".into());
        let trust = !config.use_ssl
            || config
                .parameters
                .get("trust_server_certificate")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false);
        assert!(trust, "use_ssl=true with param=true → trust_cert=true");
    }

    #[test]
    fn test_config_accessor() {
        let config = make_config("test_db");
        let adapter = SqlServerAdapter::new(config.clone());
        assert_eq!(adapter.config().id, config.id);
    }

    // ── SQL literal helpers ──────────────────────────────────────────────────

    #[test]
    fn test_sql_literal_null() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Null),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_bool_true() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Bool(true)),
            "1"
        );
    }

    #[test]
    fn test_sql_literal_bool_false() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Bool(false)),
            "0"
        );
    }

    #[test]
    fn test_sql_literal_int() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Int(100)),
            "100"
        );
    }

    #[test]
    fn test_sql_literal_float() {
        let lit = SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Float(2.5));
        assert!(lit.starts_with("2.5"), "got: {}", lit);
    }

    #[test]
    fn test_sql_literal_float_nan_becomes_null() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Float(f64::NAN)),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_float_inf_becomes_null() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Float(f64::NEG_INFINITY)),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_text_plain() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Text("hello".to_string())),
            "N'hello'"
        );
    }

    #[test]
    fn test_sql_literal_text_with_single_quote() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Text("it's".to_string())),
            "N'it''s'"
        );
    }

    #[test]
    fn test_sql_literal_bytes() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Bytes(vec![0xDE, 0xAD])),
            "0xDEAD"
        );
    }

    #[test]
    fn test_sql_literal_empty_bytes_is_null() {
        assert_eq!(
            SqlServerAdapter::query_value_to_sql_literal(&QueryValue::Bytes(vec![])),
            "NULL"
        );
    }

    // ── dtype mapping helpers ────────────────────────────────────────────────

    #[test]
    fn test_dtype_mapping_int_types() {
        use polars::prelude::DataType;
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Int8),
            "SMALLINT"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Int16),
            "SMALLINT"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Int32),
            "INT"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Int64),
            "BIGINT"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::UInt32),
            "INT"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::UInt64),
            "BIGINT"
        );
    }

    #[test]
    fn test_dtype_mapping_float_types() {
        use polars::prelude::DataType;
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Float32),
            "REAL"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Float64),
            "FLOAT"
        );
    }

    #[test]
    fn test_dtype_mapping_string_and_bool() {
        use polars::prelude::DataType;
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Boolean),
            "BIT"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::String),
            "NVARCHAR(MAX)"
        );
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Binary),
            "VARBINARY(MAX)"
        );
    }

    #[test]
    fn test_dtype_mapping_unknown_falls_back_to_nvarchar_max() {
        use polars::prelude::DataType;
        assert_eq!(
            SqlServerAdapter::polars_dtype_to_mssql_type(&DataType::Date),
            "NVARCHAR(MAX)"
        );
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
}
