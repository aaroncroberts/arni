//! PostgreSQL database adapter implementation
//!
//! This module provides the [`PostgresAdapter`] which implements both the [`Connection`]
//! and [`DbAdapter`] traits for PostgreSQL databases using the sqlx driver.
//!
//! # Features
//!
//! This module is only available when the `postgres` feature is enabled:
//!
//! ```toml
//! arni = { version = "0.1", features = ["postgres"] }
//! ```
//!
//! # Examples
//!
//! ```ignore
//! use arni::adapters::postgres::PostgresAdapter;
//! use arni::adapter::{Connection, ConnectionConfig, DatabaseType};
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
    escape_like_pattern, filter_to_sql, query_value_to_sql_literal, AdapterMetadata, Connection,
    ConnectionConfig, DatabaseType, DbAdapter, FilterExpr, ForeignKeyInfo, IndexInfo,
    ProcedureInfo, QueryResult, QueryValue, Result, RowStream, ServerInfo, TableSearchMode,
    ViewInfo,
};
use crate::DataError;
#[cfg(feature = "polars")]
use polars::prelude::*;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row, TypeInfo};
#[cfg(feature = "polars")]
use sqlx::Executor;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

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
/// **Note**: TLS/SSL is not yet implemented. The adapter always uses an unencrypted
/// (`NoTls`) connection regardless of the `use_ssl` configuration option. Setting
/// `use_ssl: true` currently has no effect. TLS support will be added in a future
/// release.
///
/// # Thread Safety
///
/// The adapter uses `Arc<RwLock>` internally so it can be cloned and shared across
/// concurrent tokio tasks.
pub struct PostgresAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// sqlx connection pool wrapped in Arc<RwLock> for thread-safe access
    pool: Arc<RwLock<Option<PgPool>>>,
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
            pool: Arc::new(RwLock::new(None)),
            connected: Arc::new(RwLock::new(false)),
        }
    }

    /// Build a PostgreSQL libpq-style connection string for sqlx.
    ///
    /// Format: `host=H port=P dbname=D user=U password=P`
    fn build_connect_options(&self, password: Option<&str>) -> Result<PgConnectOptions> {
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

        let mut opts = PgConnectOptions::new()
            .host(host)
            .port(port)
            .database(&self.config.database)
            .username(username);

        // Prefer the explicitly-passed password, then fall back to parameters map.
        let pwd = password.or_else(|| self.config.parameters.get("password").map(|s| s.as_str()));
        if let Some(pwd) = pwd {
            opts = opts.password(pwd);
        }

        Ok(opts)
    }

    /// Build a postgres:// URL for display/logging purposes (password redacted).
    ///
    /// Also validates that host and username are present — returns `Err` for missing fields.
    #[cfg_attr(not(test), allow(dead_code))]
    fn build_connection_string(&self, _password: Option<&str>) -> Result<String> {
        let host = self
            .config
            .host
            .as_deref()
            .ok_or_else(|| DataError::Config("Missing host".to_string()))?;
        let port = self.config.port.unwrap_or(5432);
        let username = self
            .config
            .username
            .as_deref()
            .ok_or_else(|| DataError::Config("Missing username".to_string()))?;
        Ok(format!(
            "postgres://{}@{}:{}/{}",
            username, host, port, self.config.database
        ))
    }

}

#[async_trait::async_trait]
impl Connection for PostgresAdapter {
    #[instrument(skip(self), fields(adapter = "postgres", host = ?self.config.host, port = ?self.config.port, database = %self.config.database))]
    async fn connect(&mut self) -> Result<()> {
        // Check if already connected
        if *self.connected.read().await {
            debug!("Already connected, skipping connection attempt");
            return Ok(());
        }

        info!("Connecting to PostgreSQL database");

        let password = self.config.parameters.get("password").map(String::as_str);
        let connect_opts = self.build_connect_options(password).map_err(|e| {
            error!(adapter = "postgres", operation = "connect", error = ?e, "Failed to build connection options");
            e
        })?;

        let pc = self.config.pool_config.clone().unwrap_or_default();
        debug!(
            max_connections = pc.max_connections,
            min_connections = pc.min_connections,
            acquire_timeout_secs = pc.acquire_timeout_secs,
            idle_timeout_secs = pc.idle_timeout_secs,
            max_lifetime_secs = pc.max_lifetime_secs,
            "Building PostgreSQL connection pool"
        );
        let pool = PgPoolOptions::new()
            .max_connections(pc.max_connections)
            .min_connections(pc.min_connections)
            .acquire_timeout(std::time::Duration::from_secs(pc.acquire_timeout_secs))
            .idle_timeout(std::time::Duration::from_secs(pc.idle_timeout_secs))
            .max_lifetime(std::time::Duration::from_secs(pc.max_lifetime_secs))
            .connect_with(connect_opts)
            .await
            .map_err(|e| {
                error!(adapter = "postgres", operation = "connect", error = %e, "Failed to establish connection");
                DataError::Connection(format!("Failed to connect: {}", e))
            })?;

        *self.pool.write().await = Some(pool);
        *self.connected.write().await = true;

        info!("Successfully connected to PostgreSQL");
        Ok(())
    }

    #[instrument(skip(self), fields(adapter = "postgres"))]
    async fn disconnect(&mut self) -> Result<()> {
        info!("Disconnecting from PostgreSQL");
        let mut pool_guard = self.pool.write().await;
        if let Some(pool) = pool_guard.take() {
            pool.close().await;
            info!("Disconnected from PostgreSQL");
        } else {
            debug!("Disconnect called but already disconnected");
        }
        *self.connected.write().await = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.pool.try_read().map(|g| g.is_some()).unwrap_or(false)
    }

    #[instrument(skip(self), fields(adapter = "postgres"))]
    async fn health_check(&self) -> Result<bool> {
        debug!("Performing health check");
        if !*self.connected.read().await {
            warn!("Health check failed: not connected");
            return Err(super::common::not_connected_error());
        }

        let pool_guard = self.pool.read().await;
        let pool = pool_guard.as_ref().ok_or_else(|| {
            error!(
                adapter = "postgres",
                operation = "health_check",
                "Pool not available for health check"
            );
            DataError::Connection("Pool not available".to_string())
        })?;

        match sqlx::query("SELECT 1").execute(pool).await {
            Ok(_) => {
                debug!("Health check passed");
                Ok(true)
            }
            Err(e) => {
                error!(adapter = "postgres", operation = "health_check", error = %e, "Health check query failed");
                Err(DataError::Query(format!("Health check failed: {}", e)))
            }
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
        self.config = config.clone();

        let connect_opts = self.build_connect_options(password)?;

        let pc = self.config.pool_config.clone().unwrap_or_default();
        debug!(
            max_connections = pc.max_connections,
            min_connections = pc.min_connections,
            acquire_timeout_secs = pc.acquire_timeout_secs,
            idle_timeout_secs = pc.idle_timeout_secs,
            max_lifetime_secs = pc.max_lifetime_secs,
            "Building PostgreSQL connection pool"
        );
        let pool = PgPoolOptions::new()
            .max_connections(pc.max_connections)
            .min_connections(pc.min_connections)
            .acquire_timeout(std::time::Duration::from_secs(pc.acquire_timeout_secs))
            .idle_timeout(std::time::Duration::from_secs(pc.idle_timeout_secs))
            .max_lifetime(std::time::Duration::from_secs(pc.max_lifetime_secs))
            .connect_with(connect_opts)
            .await
            .map_err(|e| DataError::Connection(format!("Failed to connect: {}", e)))?;

        *self.pool.write().await = Some(pool);
        *self.connected.write().await = true;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        let mut pool_guard = self.pool.write().await;
        if let Some(pool) = pool_guard.take() {
            pool.close().await;
        }
        *self.connected.write().await = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.pool.try_read().map(|g| g.is_some()).unwrap_or(false)
    }

    async fn test_connection(
        &self,
        config: &ConnectionConfig,
        password: Option<&str>,
    ) -> Result<bool> {
        let host = config
            .host
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing host".to_string()))?;
        let port = config.port.unwrap_or(5432);
        let username = config
            .username
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing username".to_string()))?;

        let mut opts = PgConnectOptions::new()
            .host(host)
            .port(port)
            .database(&config.database)
            .username(username);

        let pwd = password.or_else(|| config.parameters.get("password").map(|s| s.as_str()));
        if let Some(pwd) = pwd {
            opts = opts.password(pwd);
        }

        match PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect_with(opts)
            .await
        {
            Ok(pool) => {
                pool.close().await;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::Postgres
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    // ===== Query Operations =====

    #[instrument(skip(self, query), fields(adapter = "postgres", query_length = query.len()))]
    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let sql_type = super::common::detect_sql_type(query);
        debug!(
            sql_type,
            sql_preview = %super::common::sql_preview(query, 100),
            "Executing query"
        );

        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "execute_query",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        let pool_guard = self.pool.read().await;
        let pool = pool_guard.as_ref().ok_or_else(|| {
            error!(
                adapter = "postgres",
                operation = "execute_query",
                "Pool not available"
            );
            DataError::Connection("Pool not available".to_string())
        })?;

        let map_err = |e: sqlx::Error| {
            error!(adapter = "postgres", operation = "execute_query", sql_type, error = %e, "Query execution failed");
            let msg = e.to_string();
            if msg.contains("syntax") {
                DataError::Query(format!("SQL syntax error: {}", e))
            } else if msg.contains("permission") || msg.contains("denied") {
                DataError::Query(format!("Permission denied: {}", e))
            } else if msg.contains("does not exist") {
                DataError::Query(format!("Object not found: {}", e))
            } else if msg.contains("violates") || msg.contains("constraint") {
                DataError::Query(format!("Constraint violation: {}", e))
            } else {
                DataError::Query(format!("Query failed: {}", e))
            }
        };

        let start = std::time::Instant::now();

        if matches!(sql_type, "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE") {
            let result = sqlx::query(query).execute(pool).await.map_err(map_err)?;
            let affected = result.rows_affected();
            info!(
                sql_type,
                rows_affected = affected,
                columns = 0usize,
                duration_ms = start.elapsed().as_millis(),
                "DML executed"
            );
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: Some(affected),
            });
        }

        let rows = sqlx::query(query).fetch_all(pool).await.map_err(map_err)?;
        let duration = start.elapsed();

        if rows.is_empty() {
            info!(
                sql_type,
                duration_ms = duration.as_millis(),
                rows = 0usize,
                columns = 0usize,
                "Query executed successfully"
            );
            debug!("Query returned no rows");
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: Some(0),
            });
        }

        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        info!(
            sql_type,
            duration_ms = duration.as_millis(),
            rows = rows.len(),
            columns = columns.len(),
            "Query executed successfully"
        );
        debug!(columns = columns.len(), "Extracted column metadata");

        let mut result_rows = Vec::new();
        for row in &rows {
            let values = Self::row_to_values(row)?;
            result_rows.push(values);
        }

        Ok(QueryResult {
            columns,
            rows: result_rows,
            rows_affected: Some(rows.len() as u64),
        })
    }

    async fn execute_query_stream(
        &self,
        query: &str,
    ) -> Result<RowStream<Vec<QueryValue>>> {
        // sqlx::query().fetch() supports true cursor-level streaming, but its lifetime
        // is tied to the pool reference, preventing a 'static RowStream without an
        // additional crate (e.g. async-stream). We materialise via execute_query and
        // stream from the resulting Vec — identical in semantics at the API level.
        // A true cursor-streaming path can replace this once async-stream is added.
        let result = self.execute_query(query).await?;
        let stream = futures_util::stream::iter(result.rows.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    #[cfg(feature = "polars")]
    #[instrument(skip(self, df), fields(adapter = "postgres", table = %table_name, rows = df.height(), columns = df.width(), replace = replace))]
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
        if !*self.connected.read().await {
            error!(adapter = "postgres", operation = "export_dataframe", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        let pool_guard = self.pool.read().await;
        let pool = pool_guard.as_ref().ok_or_else(|| {
            error!(adapter = "postgres", operation = "export_dataframe", table = %table_name, "Pool not available");
            DataError::Connection("Pool not available".to_string())
        })?;

        if replace {
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            pool.execute(drop_sql.as_str())
                .await
                .map_err(|e| DataError::Query(format!("Failed to drop table: {}", e)))?;

            let create_sql = self.generate_create_table_sql(df, table_name)?;
            pool.execute(create_sql.as_str())
                .await
                .map_err(|e| DataError::Query(format!("Failed to create table: {}", e)))?;
        }

        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Postgres uses $1, $2, … placeholders
        let placeholders: Vec<String> = (1..=column_names.len())
            .map(|i| format!("${}", i))
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            column_names.join(", "),
            placeholders.join(", ")
        );

        let mut rows_inserted: u64 = 0;

        for row_idx in 0..df.height() {
            let mut query = sqlx::query(&insert_sql);

            for col_name in &column_names {
                let column = df.column(col_name).map_err(|e| {
                    DataError::DataFrame(format!("Column '{}' not found: {}", col_name, e))
                })?;
                let series = column.as_materialized_series();
                query = self.bind_series_value(query, series, row_idx)?;
            }

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

    #[instrument(skip(self), fields(adapter = "postgres"))]
    async fn list_databases(&self) -> Result<Vec<String>> {
        debug!("Listing databases");

        // Check connection
        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "list_databases",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard.as_ref().ok_or_else(|| {
            error!(
                adapter = "postgres",
                operation = "list_databases",
                "Pool not available"
            );
            DataError::Connection("Pool not available".to_string())
        })?;

        // Query pg_database catalog
        let query = "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname";
        let rows = sqlx::query(query).fetch_all(pool).await.map_err(|e| {
            error!(adapter = "postgres", operation = "list_databases", error = %e, "Failed to query databases");
            DataError::Query(format!("Failed to list databases: {}", e))
        })?;

        let databases: Vec<String> = rows
            .iter()
            .map(|row| row.try_get::<String, _>(0).unwrap_or_default())
            .collect();

        info!(count = databases.len(), "Listed databases successfully");
        Ok(databases)
    }

    #[instrument(skip(self), fields(adapter = "postgres", schema = ?schema))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        debug!("Listing tables");

        // Check connection
        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "list_tables",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard.as_ref().ok_or_else(|| {
            error!(
                adapter = "postgres",
                operation = "list_tables",
                "Pool not available"
            );
            DataError::Connection("Pool not available".to_string())
        })?;

        // Query information_schema.tables
        let query = if let Some(schema_name) = schema {
            format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = '{}' AND table_type = 'BASE TABLE' \
                 ORDER BY table_name",
                schema_name
            )
        } else {
            // Default to 'public' schema if none specified
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type = 'BASE TABLE' \
             ORDER BY table_name"
                .to_string()
        };

        let rows = sqlx::query(&query).fetch_all(pool).await.map_err(|e| {
            error!(adapter = "postgres", operation = "list_tables", error = %e, "Failed to query tables");
            DataError::Query(format!("Failed to list tables: {}", e))
        })?;

        let tables: Vec<String> = rows
            .iter()
            .map(|row| row.try_get::<String, _>(0).unwrap_or_default())
            .collect();

        info!(count = tables.len(), "Listed tables successfully");
        Ok(tables)
    }

    #[instrument(skip(self), fields(adapter = "postgres", pattern = %pattern, mode = ?mode, schema = ?schema))]
    async fn find_tables(
        &self,
        pattern: &str,
        schema: Option<&str>,
        mode: TableSearchMode,
    ) -> Result<Vec<String>> {
        debug!("Finding tables by pattern");

        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "find_tables",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        let pool_guard = self.pool.read().await;
        let pool = pool_guard.as_ref().ok_or_else(|| {
            error!(
                adapter = "postgres",
                operation = "find_tables",
                "Pool not available"
            );
            DataError::Connection("Pool not available".to_string())
        })?;

        let escaped = escape_like_pattern(pattern);
        let like_pattern = match mode {
            TableSearchMode::StartsWith => format!("{}%", escaped),
            TableSearchMode::Contains => format!("%{}%", escaped),
            TableSearchMode::EndsWith => format!("%{}", escaped),
        };

        let schema_name = schema.unwrap_or("public");
        let query = format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = '{}' AND table_type = 'BASE TABLE' \
             AND table_name LIKE $1 ESCAPE '\\' \
             ORDER BY table_name",
            schema_name
        );

        let rows = sqlx::query(&query).bind(like_pattern).fetch_all(pool).await.map_err(|e| {
            error!(adapter = "postgres", operation = "find_tables", error = %e, "Failed to find tables");
            DataError::Query(format!("Failed to find tables: {}", e))
        })?;

        let tables: Vec<String> = rows
            .iter()
            .map(|row| row.try_get::<String, _>(0).unwrap_or_default())
            .collect();
        info!(count = tables.len(), "Found tables successfully");
        Ok(tables)
    }

    #[instrument(skip(self), fields(adapter = "postgres", table = %table_name, schema = ?schema))]
    async fn describe_table(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<crate::adapter::TableInfo> {
        // Check connection
        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "describe_table",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        // Use provided schema or default to 'public'
        let schema_name = schema.unwrap_or("public");

        // Query information_schema.columns for column details
        let column_query = format!(
            "SELECT column_name, data_type, is_nullable, column_default \
             FROM information_schema.columns \
             WHERE table_schema = '{}' AND table_name = '{}' \
             ORDER BY ordinal_position",
            schema_name, table_name
        );

        let rows = sqlx::query(&column_query)
            .fetch_all(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to describe table: {}", e)))?;

        if rows.is_empty() {
            error!(adapter = "postgres", operation = "describe_table", table = %table_name, schema = %schema_name, "Table not found in schema");
            return Err(DataError::Query(format!(
                "Table '{}.{}' not found",
                schema_name, table_name
            )));
        }

        // Query for primary key constraints
        let pk_query = format!(
            "SELECT a.attname \
             FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = '\"{}\".\"{}\"'::regclass AND i.indisprimary",
            schema_name, table_name
        );

        let pk_rows = sqlx::query(&pk_query)
            .fetch_all(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to query primary keys: {}", e)))?;

        let primary_keys: std::collections::HashSet<String> = pk_rows
            .iter()
            .map(|row| row.try_get::<String, _>(0).unwrap_or_default())
            .collect();

        // Build column info
        let columns: Vec<crate::adapter::ColumnInfo> = rows
            .iter()
            .map(|row| {
                let col_name: String = row.try_get(0).unwrap_or_default();
                let data_type: String = row.try_get(1).unwrap_or_default();
                let is_nullable: String = row.try_get(2).unwrap_or_default();
                let default_value: Option<String> = row.try_get(3).ok().flatten();

                crate::adapter::ColumnInfo {
                    name: col_name.clone(),
                    data_type,
                    nullable: is_nullable == "YES",
                    default_value,
                    is_primary_key: primary_keys.contains(&col_name),
                }
            })
            .collect();

        // Fetch row count (approximate via pg_class) and total size in one query
        let stats_query = "
            SELECT reltuples::BIGINT, pg_total_relation_size(c.oid)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
        ";
        let stats = sqlx::query(stats_query)
            .bind(schema_name)
            .bind(table_name)
            .fetch_all(pool)
            .await
            .ok();
        let (row_count, size_bytes) = stats
            .as_ref()
            .and_then(|rows| rows.first())
            .map(|row| {
                let rc: i64 = row.try_get(0).unwrap_or(0);
                let sz: i64 = row.try_get(1).unwrap_or(0);
                (Some(rc.max(0)), Some(sz))
            })
            .unwrap_or((None, None));

        Ok(crate::adapter::TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name.to_string()),
            columns,
            row_count,
            size_bytes,
            created_at: None, // PostgreSQL does not natively track table creation time
        })
    }

    // ===== Metadata Methods =====

    async fn get_indexes(&self, table_name: &str, schema: Option<&str>) -> Result<Vec<IndexInfo>> {
        // Check connection
        if !*self.connected.read().await {
            error!(adapter = "postgres", operation = "get_indexes", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT
                i.relname as index_name,
                t.relname as table_name,
                n.nspname as schema_name,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary,
                am.amname as index_type,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = $1 AND n.nspname = $2
            GROUP BY i.relname, t.relname, n.nspname, ix.indisunique, ix.indisprimary, am.amname
        ";

        let rows = sqlx::query(query)
            .bind(table_name)
            .bind(schema_name)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get indexes for '{}.{}': {}",
                    schema_name, table_name, e
                ))
            })?;

        let indexes = rows
            .iter()
            .map(|row| IndexInfo {
                name: row.try_get::<String, _>("index_name").unwrap_or_default(),
                table_name: row.try_get::<String, _>("table_name").unwrap_or_default(),
                schema: Some(row.try_get::<String, _>("schema_name").unwrap_or_default()),
                columns: row.try_get::<Vec<String>, _>("columns").unwrap_or_default(),
                is_unique: row.try_get::<bool, _>("is_unique").unwrap_or(false),
                is_primary: row.try_get::<bool, _>("is_primary").unwrap_or(false),
                index_type: row.try_get::<String, _>("index_type").ok(),
            })
            .collect();

        Ok(indexes)
    }

    async fn get_foreign_keys(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        // Check connection
        if !*self.connected.read().await {
            error!(adapter = "postgres", operation = "get_foreign_keys", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT
                tc.constraint_name,
                tc.table_name,
                tc.table_schema,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.column_name AS foreign_column_name,
                rc.update_rule,
                rc.delete_rule
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            JOIN information_schema.referential_constraints AS rc
                ON rc.constraint_name = tc.constraint_name
                AND rc.constraint_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name = $1
                AND tc.table_schema = $2
            ORDER BY tc.constraint_name, kcu.ordinal_position
        ";

        let rows = sqlx::query(query)
            .bind(table_name)
            .bind(schema_name)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get foreign keys for '{}.{}': {}",
                    schema_name, table_name, e
                ))
            })?;

        // Group by constraint name since one FK can span multiple columns
        let mut fk_map: HashMap<String, ForeignKeyInfo> = HashMap::new();

        for row in rows {
            let fk_name: String = row
                .try_get::<String, _>("constraint_name")
                .unwrap_or_default();
            let column: String = row.try_get::<String, _>("column_name").unwrap_or_default();
            let ref_column: String = row
                .try_get::<String, _>("foreign_column_name")
                .unwrap_or_default();

            fk_map
                .entry(fk_name.clone())
                .or_insert_with(|| ForeignKeyInfo {
                    name: fk_name.clone(),
                    table_name: row.try_get::<String, _>("table_name").unwrap_or_default(),
                    schema: Some(row.try_get::<String, _>("table_schema").unwrap_or_default()),
                    columns: Vec::new(),
                    referenced_table: row
                        .try_get::<String, _>("foreign_table_name")
                        .unwrap_or_default(),
                    referenced_schema: Some(
                        row.try_get::<String, _>("foreign_table_schema")
                            .unwrap_or_default(),
                    ),
                    referenced_columns: Vec::new(),
                    on_delete: Some(row.try_get::<String, _>("delete_rule").unwrap_or_default()),
                    on_update: Some(row.try_get::<String, _>("update_rule").unwrap_or_default()),
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
        // Check connection
        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "get_views",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT
                table_name,
                table_schema
            FROM information_schema.views
            WHERE table_schema = $1
            ORDER BY table_name
        ";

        let rows = sqlx::query(query)
            .bind(schema_name)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get views for schema '{}': {}",
                    schema_name, e
                ))
            })?;

        let views = rows
            .iter()
            .map(|row| ViewInfo {
                name: row.try_get::<String, _>("table_name").unwrap_or_default(),
                schema: Some(row.try_get::<String, _>("table_schema").unwrap_or_default()),
                definition: None, // Definition retrieved separately via get_view_definition
            })
            .collect();

        Ok(views)
    }

    async fn get_view_definition(
        &self,
        view_name: &str,
        schema: Option<&str>,
    ) -> Result<Option<String>> {
        // Check connection
        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "get_view_definition",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT view_definition
            FROM information_schema.views
            WHERE table_name = $1 AND table_schema = $2
        ";

        let rows = sqlx::query(query)
            .bind(view_name)
            .bind(schema_name)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get view definition for '{}.{}': {}",
                    schema_name, view_name, e
                ))
            })?;

        Ok(rows
            .first()
            .and_then(|row| row.try_get::<String, _>("view_definition").ok()))
    }

    async fn list_stored_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // Check connection
        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "list_stored_procedures",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let query = "
            SELECT
                p.proname as name,
                n.nspname as schema,
                pg_get_function_result(p.oid) as return_type,
                l.lanname as language
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_language l ON l.oid = p.prolang
            WHERE n.nspname = $1
            ORDER BY p.proname
        ";

        let rows = sqlx::query(query)
            .bind(schema_name)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get stored procedures for schema '{}': {}",
                    schema_name, e
                ))
            })?;

        let procedures = rows
            .iter()
            .map(|row| ProcedureInfo {
                name: row.try_get::<String, _>("name").unwrap_or_default(),
                schema: Some(row.try_get::<String, _>("schema").unwrap_or_default()),
                return_type: Some(row.try_get::<String, _>("return_type").unwrap_or_default()),
                language: Some(row.try_get::<String, _>("language").unwrap_or_default()),
            })
            .collect();

        Ok(procedures)
    }

    #[instrument(skip(self), fields(adapter = "postgres"))]
    async fn get_server_info(&self) -> Result<ServerInfo> {
        if !*self.connected.read().await {
            error!(
                adapter = "postgres",
                operation = "get_server_info",
                "Not connected"
            );
            return Err(super::common::not_connected_error());
        }
        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;
        let rows = sqlx::query("SELECT version()")
            .fetch_all(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to get server info: {}", e)))?;
        let version = rows
            .first()
            .map(|row| row.try_get::<String, _>(0).unwrap_or_default())
            .unwrap_or_else(|| "Unknown".to_string());
        Ok(ServerInfo {
            version,
            server_type: "PostgreSQL".to_string(),
            extra_info: HashMap::new(),
        })
    }

    // ===== Bulk Operations =====

    #[instrument(skip(self, columns, rows), fields(adapter = "postgres", table = %table_name, row_count = rows.len()))]
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
        if !*self.connected.read().await {
            error!(adapter = "postgres", operation = "bulk_insert", table = %table_name, "Not connected");
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

        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let schema_prefix = schema.map(|s| format!("{}.", s)).unwrap_or_default();
        let column_list = columns.join(", ");

        // PostgreSQL uses globally numbered $1, $2, ... placeholders
        let mut param_idx = 1usize;
        let row_placeholders: Vec<String> = rows
            .iter()
            .map(|_| {
                let ph = (0..columns.len())
                    .map(|_| {
                        let p = format!("${}", param_idx);
                        param_idx += 1;
                        p
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({})", ph)
            })
            .collect();

        let query_str = format!(
            "INSERT INTO {}{} ({}) VALUES {}",
            schema_prefix,
            table_name,
            column_list,
            row_placeholders.join(", ")
        );

        let mut query_builder = sqlx::query(&query_str);
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

        let result = query_builder.execute(pool).await.map_err(|e| {
            DataError::Query(format!(
                "Failed to bulk insert into {}{}: {}",
                schema_prefix, table_name, e
            ))
        })?;

        Ok(result.rows_affected())
    }

    #[instrument(skip(self, updates), fields(adapter = "postgres", table = %table_name))]
    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, FilterExpr)],
        schema: Option<&str>,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }

        if !*self.connected.read().await {
            error!(adapter = "postgres", operation = "bulk_update", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        let schema_name = schema.unwrap_or("public");

        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let mut total_affected = 0u64;

        for (set_clauses, filter) in updates {
            if set_clauses.is_empty() {
                continue;
            }

            let set_parts: Vec<String> = set_clauses
                .iter()
                .map(|(col, val)| format!("{} = {}", col, query_value_to_sql_literal(val)))
                .collect();

            let query = format!(
                "UPDATE {}.{} SET {} WHERE {}",
                schema_name,
                table_name,
                set_parts.join(", "),
                filter_to_sql(filter)
            );

            let result = sqlx::query(&query).execute(pool).await.map_err(|e| {
                DataError::Query(format!(
                    "Failed to bulk update {}.{}: {}",
                    schema_name, table_name, e
                ))
            })?;

            total_affected += result.rows_affected();
        }

        Ok(total_affected)
    }

    #[instrument(skip(self, filters), fields(adapter = "postgres", table = %table_name))]
    async fn bulk_delete(
        &self,
        table_name: &str,
        filters: &[FilterExpr],
        schema: Option<&str>,
    ) -> Result<u64> {
        if filters.is_empty() {
            return Ok(0);
        }

        if !*self.connected.read().await {
            error!(adapter = "postgres", operation = "bulk_delete", table = %table_name, "Not connected");
            return Err(super::common::not_connected_error());
        }

        let schema_name = schema.unwrap_or("public");

        let pool_guard = self.pool.read().await;
        let pool = pool_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Pool not available".to_string()))?;

        let mut total_affected = 0u64;

        for filter in filters {
            let query = format!(
                "DELETE FROM {}.{} WHERE {}",
                schema_name,
                table_name,
                filter_to_sql(filter)
            );

            let result = sqlx::query(&query).execute(pool).await.map_err(|e| {
                DataError::Query(format!(
                    "Failed to bulk delete from {}.{}: {}",
                    schema_name, table_name, e
                ))
            })?;

            total_affected += result.rows_affected();
        }

        Ok(total_affected)
    }
}

impl PostgresAdapter {
    /// Convert a PostgreSQL row to a vector of QueryValues using sqlx PgRow
    fn row_to_values(row: &PgRow) -> Result<Vec<QueryValue>> {
        let mut values = Vec::new();

        for (i, column) in row.columns().iter().enumerate() {
            let type_name = column.type_info().name();

            let value = match type_name {
                "BOOL" => {
                    let val: Option<bool> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get bool value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Bool(v),
                        None => QueryValue::Null,
                    }
                }
                "INT2" => {
                    let val: Option<i16> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get int2 value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Int(v as i64),
                        None => QueryValue::Null,
                    }
                }
                "INT4" => {
                    let val: Option<i32> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get int4 value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Int(v as i64),
                        None => QueryValue::Null,
                    }
                }
                "INT8" => {
                    let val: Option<i64> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get int8 value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Int(v),
                        None => QueryValue::Null,
                    }
                }
                "FLOAT4" => {
                    let val: Option<f32> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get float4 value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Float(v as f64),
                        None => QueryValue::Null,
                    }
                }
                "FLOAT8" => {
                    let val: Option<f64> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get float8 value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Float(v),
                        None => QueryValue::Null,
                    }
                }
                "NUMERIC" => {
                    let val: Option<sqlx::types::Decimal> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get numeric value: {}", e))
                    })?;
                    match val {
                        Some(d) => d
                            .to_string()
                            .parse::<f64>()
                            .map(QueryValue::Float)
                            .unwrap_or_else(|_| QueryValue::Text(d.to_string())),
                        None => QueryValue::Null,
                    }
                }
                "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "UNKNOWN" => {
                    let val: Option<String> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get text value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Text(v),
                        None => QueryValue::Null,
                    }
                }
                "BYTEA" => {
                    let val: Option<Vec<u8>> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get bytes value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Bytes(v),
                        None => QueryValue::Null,
                    }
                }
                "DATE" => {
                    use sqlx::types::chrono::NaiveDate;
                    let val: Option<NaiveDate> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get date value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Text(v.format("%Y-%m-%d").to_string()),
                        None => QueryValue::Null,
                    }
                }
                "TIMESTAMP" => {
                    use sqlx::types::chrono::NaiveDateTime;
                    let val: Option<NaiveDateTime> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get timestamp value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Text(v.format("%Y-%m-%d %H:%M:%S").to_string()),
                        None => QueryValue::Null,
                    }
                }
                "TIMESTAMPTZ" => {
                    use sqlx::types::chrono::{DateTime, Utc};
                    let val: Option<DateTime<Utc>> = row.try_get(i).map_err(|e| {
                        DataError::Query(format!("Failed to get timestamptz value: {}", e))
                    })?;
                    match val {
                        Some(v) => QueryValue::Text(v.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
                        None => QueryValue::Null,
                    }
                }
                // Default: try as string
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

    #[cfg(feature = "polars")]
    /// Bind a Series value at a specific row index to a sqlx postgres query
    fn bind_series_value<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
        series: &Series,
        row_idx: usize,
    ) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
        let null_mask = series.is_null();
        if null_mask.get(row_idx).unwrap_or(false) {
            return Ok(query.bind(None::<String>));
        }

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
            DataType::Int8 | DataType::Int16 => {
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
                // Cast unsigned to signed i32 for PostgreSQL INTEGER
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
            DataType::UInt64 => {
                // Cast unsigned u64 to signed i64 for PostgreSQL BIGINT
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
                // Fallback: cast to String
                let series_str = series.cast(&DataType::String).map_err(|e| {
                    DataError::TypeConversion(format!(
                        "Cannot convert {:?} to PostgreSQL type: {}",
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

    #[cfg(feature = "polars")]
    /// Generate CREATE TABLE SQL from DataFrame schema
    fn generate_create_table_sql(&self, df: &DataFrame, table_name: &str) -> Result<String> {
        let mut column_defs = Vec::new();

        for (name, dtype) in df.columns().iter().map(|s| (s.name(), s.dtype())) {
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
        let mut parameters = HashMap::new();
        let password =
            std::env::var("TEST_POSTGRES_PASSWORD").unwrap_or_else(|_| "test_password".to_string());
        parameters.insert("password".to_string(), password);
        ConnectionConfig {
            id: "test-pg".to_string(),
            name: "Test PostgreSQL".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some(
                std::env::var("TEST_POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
            ),
            port: Some(
                std::env::var("TEST_POSTGRES_PORT")
                    .ok()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(5432),
            ),
            database: std::env::var("TEST_POSTGRES_DATABASE")
                .unwrap_or_else(|_| "test_db".to_string()),
            username: Some(
                std::env::var("TEST_POSTGRES_USERNAME").unwrap_or_else(|_| "test_user".to_string()),
            ),
            use_ssl: false,
            parameters,
            pool_config: None,
        }
    }

    fn postgres_integration_available() -> bool {
        std::env::var("TEST_POSTGRES_AVAILABLE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
    }

    macro_rules! require_postgres {
        () => {
            if !postgres_integration_available() {
                return;
            }
        };
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

        // build_connection_string now returns a postgres:// URL (password redacted).
        let conn_str = adapter.build_connection_string(None).unwrap();
        assert!(
            conn_str.starts_with("postgres://"),
            "expected URL format, got: {conn_str}"
        );
        assert!(conn_str.contains("localhost"), "URL should contain host");
        assert!(conn_str.contains("5432"), "URL should contain port");
        assert!(conn_str.contains("test_db"), "URL should contain database");
        assert!(
            conn_str.contains("test_user"),
            "URL should contain username"
        );
    }

    #[test]
    fn test_build_connection_string_with_password() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        // Password is intentionally omitted from the display URL for safety.
        let conn_str = adapter.build_connection_string(Some("secret123")).unwrap();
        assert!(conn_str.starts_with("postgres://"), "expected URL format");
        assert!(
            !conn_str.contains("secret123"),
            "password should be redacted from display URL"
        );
    }

    #[test]
    fn test_build_connect_options_includes_host_port_db() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);
        // build_connect_options must not error for a complete config.
        let opts = adapter.build_connect_options(Some("test_password"));
        assert!(
            opts.is_ok(),
            "build_connect_options should succeed for a complete config"
        );
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
        require_postgres!();
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
        require_postgres!();
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

    #[cfg(feature = "polars")]
    #[tokio::test]
    #[ignore]
    async fn test_export_dataframe_basic() {
        require_postgres!();
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        // Connect first
        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .unwrap();

        // Create test DataFrame with various types
        let df = DataFrame::new(
            3,
            vec![
                Series::new("id".into(), &[1i32, 2, 3]).into(),
                Series::new("name".into(), &["Alice", "Bob", "Charlie"]).into(),
                Series::new("score".into(), &[95.5f64, 87.3, 92.1]).into(),
                Series::new("active".into(), &[true, false, true]).into(),
            ],
        )
        .unwrap();

        // Export with replace=true
        let result =
            DbAdapter::export_dataframe(&adapter, &df, "test_export_basic", None, true).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3); // 3 rows inserted

        // Verify data was inserted by reading back
        let read_result =
            DbAdapter::execute_query(&adapter, "SELECT * FROM test_export_basic ORDER BY id").await;
        assert!(read_result.is_ok());
        let query_result = read_result.unwrap();
        assert_eq!(query_result.rows.len(), 3);
        assert_eq!(query_result.columns, vec!["id", "name", "score", "active"]);

        // Clean up
        DbAdapter::execute_query(&adapter, "DROP TABLE test_export_basic")
            .await
            .unwrap();
        DbAdapter::disconnect(&mut adapter).await.unwrap();
    }

    #[cfg(feature = "polars")]
    #[tokio::test]
    #[ignore]
    async fn test_export_dataframe_with_nulls() {
        require_postgres!();
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .unwrap();

        // Create DataFrame with NULL values
        let id_series = Series::new("id".into(), &[Some(1i32), Some(2), Some(3)]).into();
        let name_series =
            Series::new("name".into(), &[Some("Alice"), None, Some("Charlie")]).into();
        let score_series = Series::new("score".into(), &[Some(95.5f64), Some(87.3), None]).into();

        let df = DataFrame::new(3, vec![id_series, name_series, score_series]).unwrap();

        // Export with NULL values
        let result =
            DbAdapter::export_dataframe(&adapter, &df, "test_export_nulls", None, true).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        // Verify NULLs were preserved
        let read_result =
            DbAdapter::execute_query(&adapter, "SELECT * FROM test_export_nulls ORDER BY id").await;
        assert!(read_result.is_ok());
        let query_result = read_result.unwrap();

        // Check that NULL values are present
        assert!(matches!(query_result.rows[1][1], QueryValue::Null)); // name is NULL for row 2
        assert!(matches!(query_result.rows[2][2], QueryValue::Null)); // score is NULL for row 3

        // Clean up
        DbAdapter::execute_query(&adapter, "DROP TABLE test_export_nulls")
            .await
            .unwrap();
        DbAdapter::disconnect(&mut adapter).await.unwrap();
    }

    #[cfg(feature = "polars")]
    #[tokio::test]
    #[ignore]
    async fn test_export_dataframe_replace_table() {
        require_postgres!();
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
        .await
        .unwrap();

        // First export
        let df1 =
            DataFrame::new(3, vec![Series::new("value".into(), &[1i32, 2, 3]).into()]).unwrap();

        DbAdapter::export_dataframe(&adapter, &df1, "test_replace", None, true)
            .await
            .unwrap();

        // Second export with replace=true (should drop and recreate)
        let df2 =
            DataFrame::new(2, vec![Series::new("value".into(), &[10i32, 20]).into()]).unwrap();

        let result = DbAdapter::export_dataframe(&adapter, &df2, "test_replace", None, true).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        // Verify only new data exists
        let read_result = DbAdapter::execute_query(&adapter, "SELECT * FROM test_replace")
            .await
            .unwrap();
        assert_eq!(read_result.rows.len(), 2); // Only 2 rows from df2, not 3 from df1

        // Clean up
        DbAdapter::execute_query(&adapter, "DROP TABLE test_replace")
            .await
            .unwrap();
        DbAdapter::disconnect(&mut adapter).await.unwrap();
    }

    #[cfg(feature = "polars")]
    #[tokio::test]
    async fn test_export_dataframe_not_connected() {
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let df = DataFrame::new(3, vec![Series::new("id".into(), &[1i32, 2, 3]).into()]).unwrap();

        let result = DbAdapter::export_dataframe(&adapter, &df, "test_table", None, false).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Connection(_)));
    }

    #[tokio::test]
    async fn test_list_databases_not_connected() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

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
    #[ignore]
    async fn test_list_databases() {
        require_postgres!();
        use crate::adapter::{Connection, DbAdapter};

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);
        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        let result = DbAdapter::list_databases(&adapter).await;
        assert!(result.is_ok());

        let databases = result.unwrap();
        assert!(!databases.is_empty());
        assert!(databases.contains(&"postgres".to_string()));

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    async fn test_list_tables_not_connected() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

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
    #[ignore]
    async fn test_list_tables_default_schema() {
        require_postgres!();
        use crate::adapter::{Connection, DbAdapter};

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);
        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        let result = DbAdapter::list_tables(&adapter, None).await;
        assert!(result.is_ok());

        let tables = result.unwrap();
        // Should list tables from 'public' schema by default
        for table in &tables {
            println!("Found table: {}", table);
        }

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_tables_custom_schema() {
        require_postgres!();
        use crate::adapter::{Connection, DbAdapter};

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);
        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        // Create a table in a custom schema to verify schema-filtering works.
        // Note: information_schema contains VIEWs, not BASE TABLEs, so list_tables
        // (which filters by table_type = 'BASE TABLE') returns empty for it.
        // We use the public schema instead.
        adapter
            .execute_query("CREATE TABLE IF NOT EXISTS test_schema_table (id INT)")
            .await
            .expect("Failed to create test table");

        let result = DbAdapter::list_tables(&adapter, Some("public")).await;
        assert!(result.is_ok());

        let tables = result.unwrap();
        // public schema should contain the table we just created
        assert!(
            tables.contains(&"test_schema_table".to_string()),
            "Expected test_schema_table in public schema, got: {:?}",
            tables
        );

        // Clean up
        adapter
            .execute_query("DROP TABLE IF EXISTS test_schema_table")
            .await
            .expect("Failed to drop test table");

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    async fn test_describe_table_not_connected() {
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

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
    #[ignore]
    async fn test_describe_table_not_found() {
        require_postgres!();
        use crate::adapter::{Connection, DbAdapter};

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);
        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        let result = DbAdapter::describe_table(&adapter, "nonexistent_table_xyz", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataError::Query(_)),
            "Expected Query error, got: {:?}",
            err
        );

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[tokio::test]
    #[ignore]
    async fn test_describe_table() {
        require_postgres!();
        use crate::adapter::{Connection, DbAdapter};

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);
        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        // Describe a standard information_schema table
        let result =
            DbAdapter::describe_table(&adapter, "tables", Some("information_schema")).await;
        assert!(result.is_ok());

        let table_info = result.unwrap();
        assert_eq!(table_info.name, "tables");
        assert_eq!(table_info.schema, Some("information_schema".to_string()));
        assert!(!table_info.columns.is_empty());

        // Verify column structure
        for col in &table_info.columns {
            assert!(!col.name.is_empty());
            assert!(!col.data_type.is_empty());
            println!(
                "Column: {} ({}){}{} {}",
                col.name,
                col.data_type,
                if col.nullable { " NULL" } else { " NOT NULL" },
                if col.is_primary_key {
                    " PRIMARY KEY"
                } else {
                    ""
                },
                col.default_value
                    .as_ref()
                    .map(|d| format!("DEFAULT {}", d))
                    .unwrap_or_default()
            );
        }

        Connection::disconnect(&mut adapter)
            .await
            .expect("Failed to disconnect");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn test_generate_create_table_sql() {
        use polars::prelude::*;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let df = DataFrame::new(
            3,
            vec![
                Series::new("id".into(), &[1, 2, 3]).into(),
                Series::new("name".into(), &["Alice", "Bob", "Charlie"]).into(),
                Series::new("score".into(), &[95.5, 87.3, 92.1]).into(),
                Series::new("active".into(), &[true, false, true]).into(),
            ],
        )
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
        require_postgres!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
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
        require_postgres!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
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
        require_postgres!();
        use crate::adapter::DbAdapter;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(
            &mut adapter,
            &config,
            config.parameters.get("password").map(String::as_str),
        )
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

    #[tokio::test]
    async fn test_find_tables_not_connected() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);
        let result =
            DbAdapter::find_tables(&adapter, "PS_", None, TableSearchMode::StartsWith).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[test]
    fn test_find_tables_like_pattern_starts_with() {
        let like_pattern = format!("{}%", escape_like_pattern("PS_"));
        // PS_ escapes _ → \_ so the LIKE pattern is PS\_% which won't match PSA
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
        let adapter = PostgresAdapter::new(config.clone());
        let result = adapter.test_connection(&config, None).await;
        assert!(result.is_err(), "Missing host should return Err, not panic");
        assert!(
            result.unwrap_err().to_string().contains("host"),
            "Error should mention 'host'"
        );
    }

    #[tokio::test]
    async fn test_connection_missing_username_returns_err() {
        let mut config = create_test_config();
        config.username = None;
        let adapter = PostgresAdapter::new(config.clone());
        let result = adapter.test_connection(&config, None).await;
        assert!(
            result.is_err(),
            "Missing username should return Err, not panic"
        );
    }

    // ── not-connected guard tests for extended methods ─────────────────────────

    #[tokio::test]
    async fn test_get_indexes_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let result = adapter.get_indexes("users", None).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[tokio::test]
    async fn test_get_foreign_keys_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let result = adapter.get_foreign_keys("orders", None).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[tokio::test]
    async fn test_get_views_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let result = adapter.get_views(None).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[tokio::test]
    async fn test_get_server_info_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let result = adapter.get_server_info().await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[tokio::test]
    async fn test_list_stored_procedures_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let result = adapter.list_stored_procedures(None).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[tokio::test]
    async fn test_bulk_insert_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let cols = vec!["id".to_string()];
        let rows = vec![vec![QueryValue::Int(1)]];
        let result = adapter.bulk_insert("t", &cols, &rows, None).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[tokio::test]
    async fn test_bulk_update_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let mut set = std::collections::HashMap::new();
        set.insert("name".to_string(), QueryValue::Text("x".into()));
        let updates = [(set, FilterExpr::Eq("id".to_string(), QueryValue::Int(1)))];
        let result = adapter.bulk_update("t", &updates, None).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }

    #[tokio::test]
    async fn test_bulk_delete_not_connected() {
        let adapter = PostgresAdapter::new(create_test_config());
        let filters = [FilterExpr::Eq("id".to_string(), QueryValue::Int(1))];
        let result = adapter.bulk_delete("t", &filters, None).await;
        assert!(matches!(result, Err(DataError::Connection(_))));
    }
}
