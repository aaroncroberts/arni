//! SQLite database adapter implementation.
//!
//! This module provides the [`SqliteAdapter`] which implements both the [`Connection`]
//! and [`DbAdapter`] traits for SQLite databases using the sqlx driver.
//!
//! Both file-based databases (`sqlite:///path/to/file.db`) and in-memory databases
//! (`":memory:"`) are supported.

use crate::adapter::{
    escape_like_pattern, filter_to_sql, AdapterMetadata, ColumnInfo, Connection as ConnectionTrait,
    ConnectionConfig, DatabaseType, DbAdapter, FilterExpr, ForeignKeyInfo, IndexInfo,
    ProcedureInfo, QueryResult, QueryValue, RowStream, ServerInfo, TableInfo, TableSearchMode,
    ViewInfo,
};
use crate::DataError;
#[cfg(feature = "polars")]
use polars::prelude::*;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Row, TypeInfo};
use std::collections::HashMap;
use tracing::{debug, error, info, instrument, warn};

type Result<T> = std::result::Result<T, DataError>;

/// SQLite database adapter using sqlx
///
/// This adapter uses sqlx to connect to SQLite databases.
/// SQLite is a file-based database, so the `database` field in ConnectionConfig
/// should contain the file path (or ":memory:" for in-memory databases).
///
/// # Connection Management
///
/// The adapter maintains an optional connection pool. Connections are established
/// when `connect()` is called.
///
/// # Thread Safety
///
/// `SqlitePool` is internally reference-counted and `Send + Sync`, so no additional
/// locking is needed.
pub struct SqliteAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// SQLite connection pool (SqlitePool is internally Arc and Send+Sync)
    pool: Option<SqlitePool>,
}

impl SqliteAdapter {
    /// Create a new SQLite adapter with the given configuration
    ///
    /// This does not establish a connection immediately. Call [`connect`](ConnectionTrait::connect)
    /// to establish the connection.
    pub fn new(config: ConnectionConfig) -> Self {
        debug!(database = %config.database, "Creating SQLite adapter");
        Self { config, pool: None }
    }

    /// Validate database path
    fn validate_database_path(path: &str) -> Result<()> {
        if path.is_empty() {
            return Err(DataError::Config(
                "Database path cannot be empty".to_string(),
            ));
        }
        // Allow :memory: for in-memory databases
        if path == ":memory:" {
            return Ok(());
        }
        // Check for path length limits
        if path.len() > 4096 {
            return Err(DataError::Config(format!(
                "Database path too long (max 4096 chars): {}",
                path.len()
            )));
        }
        Ok(())
    }

    /// Build a connection string from the configuration
    fn build_connection_string(config: &ConnectionConfig) -> String {
        // For SQLite, the database field contains the file path
        // Special case: ":memory:" for in-memory database
        if config.database == ":memory:" {
            "sqlite::memory:".to_string()
        } else {
            format!("sqlite://{}", config.database)
        }
    }

    /// Execute a DML/DDL statement and return rows affected.
    ///
    /// Uses `sqlx::query::execute` rather than `fetch_all` so DML operations
    /// correctly return the row count instead of an empty result set.
    async fn execute_statement(&self, sql: &str) -> Result<u64> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;

        let result = sqlx::query(sql)
            .execute(pool)
            .await
            .map_err(|e| DataError::Query(format!("Failed to execute statement: {}", e)))?;

        Ok(result.rows_affected())
    }

    /// Convert a [`QueryValue`] to a SQL literal suitable for inline SQLite SQL.
    ///
    /// Delegates to [`super::common::query_value_to_sql_literal`] with `bool_as_int = true`
    /// (SQLite has no native BOOLEAN type; uses `1`/`0` instead).
    fn query_value_to_sql_literal(value: &QueryValue) -> String {
        super::common::query_value_to_sql_literal(value, true)
    }

    #[cfg(feature = "polars")]
    /// Map a Polars [`DataType`] to the corresponding SQLite type affinity.
    ///
    /// SQLite has four storage classes: INTEGER, REAL, TEXT, BLOB.
    /// All boolean and integer variants collapse to INTEGER; both float
    /// variants map to REAL; everything else delegates to the generic SQL
    /// fallback (String→TEXT, Binary→BLOB, unknowns→TEXT).
    fn polars_dtype_to_sqlite_type(dtype: &DataType) -> &'static str {
        match dtype {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => "INTEGER",
            DataType::Float32 | DataType::Float64 => "REAL",
            _ => super::common::polars_dtype_to_generic_sql(dtype),
        }
    }

    #[cfg(feature = "polars")]
    /// Extract the value at `row_idx` from `series` as a SQLite SQL literal.
    ///
    /// Delegates to the shared implementation in [`super::common`], with booleans
    /// rendered as `1`/`0` (SQLite has no native BOOLEAN type).
    fn series_value_to_sql_literal(series: &Series, row_idx: usize) -> Result<String> {
        super::common::series_value_to_sql_literal(series, row_idx, true)
    }

    /// Convert a SQLite row to QueryValue vector
    fn row_to_values(row: &SqliteRow) -> Result<Vec<QueryValue>> {
        let mut values = Vec::new();

        for (i, column) in row.columns().iter().enumerate() {
            // SQLite has a simpler type system: NULL, INTEGER, REAL, TEXT, BLOB
            let type_name = column.type_info().name();
            let value = match type_name {
                "BOOLEAN" | "BOOL" => row
                    .try_get::<Option<bool>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get bool value: {}", e)))?
                    .map(QueryValue::Bool)
                    .unwrap_or(QueryValue::Null),
                "INTEGER" | "INT" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT" => row
                    .try_get::<Option<i64>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get int value: {}", e)))?
                    .map(QueryValue::Int)
                    .unwrap_or(QueryValue::Null),
                "REAL" | "DOUBLE" | "FLOAT" => row
                    .try_get::<Option<f64>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get float value: {}", e)))?
                    .map(QueryValue::Float)
                    .unwrap_or(QueryValue::Null),
                "TEXT" | "VARCHAR" | "CHAR" | "CLOB" => row
                    .try_get::<Option<String>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get text value: {}", e)))?
                    .map(QueryValue::Text)
                    .unwrap_or(QueryValue::Null),
                "BLOB" => row
                    .try_get::<Option<Vec<u8>>, _>(i)
                    .map_err(|e| DataError::Query(format!("Failed to get bytes value: {}", e)))?
                    .map(QueryValue::Bytes)
                    .unwrap_or(QueryValue::Null),
                "NULL" => {
                    // sqlx reports undeclared-type columns (e.g. PRAGMA results) as
                    // type "NULL". The actual SQLite storage class may be INTEGER,
                    // TEXT, or NULL. Try each in order.
                    if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(i) {
                        QueryValue::Int(v)
                    } else if let Ok(Some(v)) = row.try_get::<Option<String>, _>(i) {
                        QueryValue::Text(v)
                    } else {
                        QueryValue::Null
                    }
                }
                _ => row
                    .try_get::<Option<String>, _>(i)
                    .map_err(|e| {
                        DataError::Query(format!("Failed to get value for type {type_name}: {e}"))
                    })?
                    .map(QueryValue::Text)
                    .unwrap_or(QueryValue::Null),
            };

            values.push(value);
        }

        Ok(values)
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for SqliteAdapter {
    #[instrument(skip(self), fields(adapter = "sqlite", database = %self.config.database))]
    async fn connect(&mut self) -> Result<()> {
        if self.config.db_type != DatabaseType::SQLite {
            let err = DataError::Config(format!(
                "Invalid database type: expected SQLite, got {:?}",
                self.config.db_type
            ));
            error!(adapter = "sqlite", operation = "connect", error = %err, "Invalid database type");
            return Err(err);
        }

        Self::validate_database_path(&self.config.database)?;

        info!(database = %self.config.database, "Connecting to SQLite");

        let conn_str = Self::build_connection_string(&self.config);

        let pc = self.config.pool_config.clone().unwrap_or_default();
        debug!(
            max_connections = pc.max_connections,
            min_connections = pc.min_connections,
            acquire_timeout_secs = pc.acquire_timeout_secs,
            idle_timeout_secs = pc.idle_timeout_secs,
            max_lifetime_secs = pc.max_lifetime_secs,
            "Building SQLite connection pool"
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(pc.max_connections)
            .min_connections(pc.min_connections)
            .acquire_timeout(std::time::Duration::from_secs(pc.acquire_timeout_secs))
            .idle_timeout(std::time::Duration::from_secs(pc.idle_timeout_secs))
            .max_lifetime(std::time::Duration::from_secs(pc.max_lifetime_secs))
            .connect(&conn_str)
            .await
            .map_err(|e| {
                error!(adapter = "sqlite", operation = "connect", error = %e, "Failed to connect to SQLite");
                DataError::Connection(format!("Failed to connect: {}", e))
            })?;

        self.pool = Some(pool);

        info!("Connected to SQLite successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(adapter = "sqlite"))]
    async fn disconnect(&mut self) -> Result<()> {
        debug!("Disconnecting from SQLite");
        if let Some(pool) = self.pool.take() {
            pool.close().await;
            info!("Disconnected from SQLite");
        } else {
            debug!("Disconnect called but no active pool");
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.pool.is_some()
    }

    #[instrument(skip(self), fields(adapter = "sqlite"))]
    async fn health_check(&self) -> Result<bool> {
        debug!("Performing health check");
        if let Some(pool) = self.pool.as_ref() {
            sqlx::query("SELECT 1")
                .execute(pool)
                .await
                .map(|_| {
                    debug!("Health check passed");
                    true
                })
                .map_err(|e| {
                    warn!(error = %e, "Health check failed");
                    DataError::Connection(format!("Health check failed: {}", e))
                })
        } else {
            warn!("Health check called but pool not initialized");
            Ok(false)
        }
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[async_trait::async_trait]
impl DbAdapter for SqliteAdapter {
    async fn connect(&mut self, config: &ConnectionConfig, _password: Option<&str>) -> Result<()> {
        self.config = config.clone();
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
        _password: Option<&str>,
    ) -> Result<bool> {
        Self::validate_database_path(&config.database)?;

        let conn_str = Self::build_connection_string(config);

        let result = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&conn_str)
            .await;

        match result {
            Ok(pool) => {
                pool.close().await;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::SQLite
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    #[instrument(skip(self, query), fields(adapter = "sqlite", query_length = query.len()))]
    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let sql_type = super::common::detect_sql_type(query);
        debug!(
            sql_type,
            sql_preview = %super::common::sql_preview(query, 100),
            "Executing query"
        );
        let start = std::time::Instant::now();

        let pool = self.pool.as_ref().ok_or_else(|| {
            error!(
                adapter = "sqlite",
                operation = "execute_query",
                "Not connected"
            );
            super::common::not_connected_error()
        })?;

        let rows = sqlx::query(query).fetch_all(pool).await.map_err(|e| {
            error!(adapter = "sqlite", operation = "execute_query", sql_type, error = %e, "Query execution failed");
            DataError::Query(format!("Query failed: {}", e))
        })?;

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
        for row in rows {
            result_rows.push(Self::row_to_values(&row)?);
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

    async fn execute_query_stream(&self, query: &str) -> Result<RowStream<Vec<QueryValue>>> {
        use futures_util::TryStreamExt;
        if self.pool.is_none() {
            return Err(super::common::not_connected_error());
        }
        let pool = self
            .pool
            .clone()
            .ok_or_else(super::common::not_connected_error)?;
        let query = query.to_string();
        let stream = async_stream::try_stream! {
            let mut cursor = sqlx::query(&query).fetch(&pool);
            while let Some(row) = cursor.try_next().await.map_err(|e| DataError::Query(format!("Stream fetch error: {}", e)))? {
                yield SqliteAdapter::row_to_values(&row)?;
            }
        };
        Ok(Box::pin(stream))
    }

    #[instrument(skip(self), fields(adapter = "sqlite"))]
    async fn list_databases(&self) -> Result<Vec<String>> {
        // SQLite is file-based, so there's only the current database
        // Return the database path as the single "database"
        Ok(vec![self.config.database.clone()])
    }

    #[instrument(skip(self), fields(adapter = "sqlite"))]
    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>> {
        let query = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name";
        let result = self.execute_query(query).await?;

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

    #[instrument(skip(self), fields(adapter = "sqlite", pattern = %pattern, mode = ?mode))]
    async fn find_tables(
        &self,
        pattern: &str,
        _schema: Option<&str>,
        mode: TableSearchMode,
    ) -> Result<Vec<String>> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            error!(
                adapter = "sqlite",
                operation = "find_tables",
                "Not connected"
            );
            super::common::not_connected_error()
        })?;

        let escaped = escape_like_pattern(pattern);
        let like_pattern = match mode {
            TableSearchMode::StartsWith => format!("{}%", escaped),
            TableSearchMode::Contains => format!("%{}%", escaped),
            TableSearchMode::EndsWith => format!("%{}", escaped),
        };

        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             AND name LIKE ?1 ESCAPE '\\' \
             ORDER BY name",
        )
        .bind(&like_pattern)
        .fetch_all(pool)
        .await
        .map_err(|e| DataError::Query(format!("Failed to find tables: {}", e)))?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    #[instrument(skip(self), fields(adapter = "sqlite", table = %table_name, schema = ?_schema))]
    async fn describe_table(&self, table_name: &str, _schema: Option<&str>) -> Result<TableInfo> {
        // Get column information
        let pragma_query = format!("PRAGMA table_info({})", table_name);
        let result = self.execute_query(&pragma_query).await?;

        let mut columns = Vec::new();

        for row in result.rows {
            if row.len() >= 6 {
                // PRAGMA table_info returns columns without declared types in SQLite's
                // schema, so sqlx may report them as untyped (falling to catch-all Text).
                // Accept both Int and Text representations for notnull and pk.
                let name = match &row[1] {
                    QueryValue::Text(s) => s.clone(),
                    _ => continue,
                };
                let data_type = match &row[2] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let nullable = match &row[3] {
                    QueryValue::Int(i) => *i == 0,
                    QueryValue::Text(s) => s == "0",
                    _ => true,
                };
                let is_primary_key = match &row[5] {
                    QueryValue::Int(i) => *i > 0,
                    QueryValue::Text(s) => s != "0",
                    _ => false,
                };
                columns.push(ColumnInfo {
                    name,
                    data_type,
                    nullable,
                    default_value: None,
                    is_primary_key,
                });
            }
        }

        // Row count via COUNT(*); size and creation time are not tracked per-table in SQLite
        let count_query = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
        let row_count = self
            .execute_query(&count_query)
            .await
            .ok()
            .and_then(|r| r.rows.into_iter().next())
            .and_then(|row| match row.into_iter().next() {
                Some(QueryValue::Int(n)) => Some(n),
                _ => None,
            });

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: None,
            columns,
            row_count,
            size_bytes: None,
            created_at: None,
        })
    }

    #[instrument(skip(self), fields(adapter = "sqlite"))]
    async fn get_server_info(&self) -> Result<ServerInfo> {
        let version_result = self.execute_query("SELECT sqlite_version()").await?;
        let version = version_result
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
            server_type: "SQLite".to_string(),
            extra_info: HashMap::new(),
        })
    }

    async fn get_indexes(&self, table_name: &str, _schema: Option<&str>) -> Result<Vec<IndexInfo>> {
        let query = format!("PRAGMA index_list({})", table_name);
        let result = self.execute_query(&query).await?;

        let mut indexes = Vec::new();

        for row in result.rows {
            if row.len() >= 3 {
                if let (QueryValue::Text(name), QueryValue::Int(unique), QueryValue::Text(origin)) =
                    (&row[1], &row[2], &row[3])
                {
                    // Get columns for this index
                    let index_info_query = format!("PRAGMA index_info({})", name);
                    let index_result = self.execute_query(&index_info_query).await?;

                    let columns: Vec<String> = index_result
                        .rows
                        .iter()
                        .filter_map(|r| {
                            if r.len() >= 3 {
                                if let QueryValue::Text(col_name) = &r[2] {
                                    Some(col_name.clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                    indexes.push(IndexInfo {
                        name: name.clone(),
                        table_name: table_name.to_string(),
                        schema: None,
                        columns,
                        is_unique: *unique == 1,
                        is_primary: origin == "pk",
                        index_type: Some(if *unique == 1 { "UNIQUE" } else { "INDEX" }.to_string()),
                    });
                }
            }
        }

        Ok(indexes)
    }

    async fn get_foreign_keys(
        &self,
        table_name: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let query = format!("PRAGMA foreign_key_list({})", table_name);
        let result = self.execute_query(&query).await?;

        let mut foreign_keys: HashMap<i64, Vec<(String, String)>> = HashMap::new();
        let mut fk_tables: HashMap<i64, String> = HashMap::new();

        for row in result.rows {
            if row.len() >= 4 {
                if let (
                    QueryValue::Int(id),
                    QueryValue::Text(ref_table),
                    QueryValue::Text(from_col),
                    QueryValue::Text(to_col),
                ) = (&row[0], &row[2], &row[3], &row[4])
                {
                    foreign_keys
                        .entry(*id)
                        .or_default()
                        .push((from_col.clone(), to_col.clone()));
                    fk_tables.insert(*id, ref_table.clone());
                }
            }
        }

        let mut result_fks = Vec::new();
        for (id, columns) in foreign_keys {
            let referenced_table = fk_tables.get(&id).cloned().unwrap_or_default();
            let (from_cols, to_cols): (Vec<_>, Vec<_>) = columns.into_iter().unzip();

            result_fks.push(ForeignKeyInfo {
                name: format!("fk_{}_{}", table_name, id),
                table_name: table_name.to_string(),
                schema: None,
                columns: from_cols,
                referenced_table,
                referenced_schema: None,
                referenced_columns: to_cols,
                on_delete: None,
                on_update: None,
            });
        }

        Ok(result_fks)
    }

    async fn get_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let query = "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name";
        let result = self.execute_query(query).await?;

        let views = result
            .rows
            .into_iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.first() {
                    Some(ViewInfo {
                        name: name.clone(),
                        schema: None,
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
        _schema: Option<&str>,
    ) -> Result<Option<String>> {
        let query = format!(
            "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = '{}'",
            view_name
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

    async fn list_stored_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // SQLite doesn't support stored procedures
        Ok(vec![])
    }

    #[cfg(feature = "polars")]
    #[instrument(skip(self, df), fields(adapter = "sqlite", table = %table_name, rows = df.height(), columns = df.width(), replace = replace))]
    async fn export_dataframe(
        &self,
        df: &DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        replace: bool,
    ) -> Result<u64> {
        {
            if self.pool.is_none() {
                error!(adapter = "sqlite", operation = "export_dataframe", table = %table_name, "Not connected");
                return Err(super::common::not_connected_error());
            }
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
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            self.execute_statement(&drop_sql).await?;

            let column_defs: Vec<String> = df
                .columns()
                .iter()
                .map(|col| {
                    format!(
                        "{} {}",
                        col.name(),
                        Self::polars_dtype_to_sqlite_type(col.dtype())
                    )
                })
                .collect();
            let create_sql = format!("CREATE TABLE {} ({})", table_name, column_defs.join(", "));
            self.execute_statement(&create_sql).await?;
        }

        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        if column_names.is_empty() || nrows == 0 {
            info!(table = %table_name, rows_written = 0u64, duration_ms = export_start.elapsed().as_millis(), "DataFrame export complete");
            return Ok(0);
        }

        let cols_clause = column_names.join(", ");
        let mut rows_inserted = 0u64;

        for row_idx in 0..nrows {
            let mut literals = Vec::with_capacity(column_names.len());
            for col_name in &column_names {
                let col = df.column(col_name).map_err(|e| {
                    DataError::DataFrame(format!("Column '{}' not found: {}", col_name, e))
                })?;
                let series = col.as_materialized_series();
                literals.push(Self::series_value_to_sql_literal(series, row_idx)?);
            }
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                cols_clause,
                literals.join(", ")
            );
            self.execute_statement(&insert_sql).await?;
            rows_inserted += 1;
            if rows_inserted % 1000 == 0 {
                debug!(rows_inserted, total_rows = nrows, "Export progress");
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

    #[instrument(skip(self, columns, rows), fields(adapter = "sqlite", table = %table_name, row_count = rows.len()))]
    async fn bulk_insert(
        &self,
        table_name: &str,
        columns: &[String],
        rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if columns.is_empty() {
            return Err(DataError::Config("Column list cannot be empty".to_string()));
        }
        if rows.is_empty() {
            return Ok(0);
        }
        {
            if self.pool.is_none() {
                error!(adapter = "sqlite", operation = "bulk_insert", table = %table_name, "Not connected");
                return Err(super::common::not_connected_error());
            }
        }

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

        let cols_clause = columns.join(", ");
        let mut total_inserted = 0u64;

        const BATCH_SIZE: usize = 500;
        for chunk in rows.chunks(BATCH_SIZE) {
            let value_rows: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let literals: Vec<String> =
                        row.iter().map(Self::query_value_to_sql_literal).collect();
                    format!("({})", literals.join(", "))
                })
                .collect();
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_name,
                cols_clause,
                value_rows.join(", ")
            );
            let rows_affected = self.execute_statement(&insert_sql).await?;
            total_inserted += rows_affected;
        }

        Ok(total_inserted)
    }

    #[instrument(skip(self, updates), fields(adapter = "sqlite", table = %table_name))]
    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, FilterExpr)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }
        {
            if self.pool.is_none() {
                error!(adapter = "sqlite", operation = "bulk_update", table = %table_name, "Not connected");
                return Err(super::common::not_connected_error());
            }
        }

        let mut total_affected = 0u64;

        for (set_values, filter) in updates {
            if set_values.is_empty() {
                continue;
            }
            let set_clause: String = set_values
                .iter()
                .map(|(col, val)| format!("{} = {}", col, Self::query_value_to_sql_literal(val)))
                .collect::<Vec<_>>()
                .join(", ");
            let update_sql = format!(
                "UPDATE {} SET {} WHERE {}",
                table_name,
                set_clause,
                filter_to_sql(filter)
            );
            let rows_affected = self.execute_statement(&update_sql).await?;
            total_affected += rows_affected;
        }

        Ok(total_affected)
    }

    #[instrument(skip(self, filters), fields(adapter = "sqlite", table = %table_name))]
    async fn bulk_delete(
        &self,
        table_name: &str,
        filters: &[FilterExpr],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if filters.is_empty() {
            return Ok(0);
        }
        {
            if self.pool.is_none() {
                error!(adapter = "sqlite", operation = "bulk_delete", table = %table_name, "Not connected");
                return Err(super::common::not_connected_error());
            }
        }

        let mut total_affected = 0u64;

        for filter in filters {
            let delete_sql = format!("DELETE FROM {} WHERE {}", table_name, filter_to_sql(filter));
            let rows_affected = self.execute_statement(&delete_sql).await?;
            total_affected += rows_affected;
        }

        Ok(total_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{Connection as ConnectionTrait, DatabaseType};

    fn make_config(database: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: "test-sqlite".to_string(),
            name: "Test SQLite".to_string(),
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: database.to_string(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        }
    }

    #[test]
    fn test_new_adapter_stores_config() {
        let config = make_config(":memory:");
        let adapter = SqliteAdapter::new(config);
        assert_eq!(adapter.config.database, ":memory:");
        assert_eq!(adapter.config.db_type, DatabaseType::SQLite);
    }

    #[test]
    fn test_is_connected_initially_false() {
        let adapter = SqliteAdapter::new(make_config(":memory:"));
        assert!(!ConnectionTrait::is_connected(&adapter));
    }

    #[test]
    fn test_validate_database_path_memory() {
        assert!(SqliteAdapter::validate_database_path(":memory:").is_ok());
    }

    #[test]
    fn test_validate_database_path_empty_fails() {
        let err = SqliteAdapter::validate_database_path("").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_validate_database_path_too_long_fails() {
        let long_path = "a".repeat(4097);
        let err = SqliteAdapter::validate_database_path(&long_path).unwrap_err();
        assert!(err.to_string().contains("too long"));
    }

    #[test]
    fn test_validate_database_path_valid_file() {
        assert!(SqliteAdapter::validate_database_path("/tmp/test.db").is_ok());
        assert!(SqliteAdapter::validate_database_path("relative/path.db").is_ok());
    }

    #[test]
    fn test_build_connection_string_memory() {
        let config = make_config(":memory:");
        let conn_str = SqliteAdapter::build_connection_string(&config);
        assert_eq!(conn_str, "sqlite::memory:");
    }

    #[test]
    fn test_build_connection_string_absolute_path() {
        let config = make_config("/tmp/mydb.db");
        let conn_str = SqliteAdapter::build_connection_string(&config);
        assert!(conn_str.starts_with("sqlite://"));
        assert!(conn_str.contains("/tmp/mydb.db"));
    }

    #[test]
    fn test_build_connection_string_relative_path() {
        let config = make_config("./mydb.db");
        let conn_str = SqliteAdapter::build_connection_string(&config);
        assert!(conn_str.starts_with("sqlite://"));
    }

    #[test]
    fn test_config_accessor() {
        let config = make_config(":memory:");
        let adapter = SqliteAdapter::new(config.clone());
        assert_eq!(adapter.config().id, config.id);
        assert_eq!(adapter.config().database, ":memory:");
    }

    #[tokio::test]
    async fn test_find_tables_not_connected() {
        let config = make_config(":memory:");
        let adapter = SqliteAdapter::new(config);
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
    async fn test_connection_empty_path_returns_err() {
        // validate_database_path("") is called first — no network I/O needed.
        let config = make_config("");
        let adapter = SqliteAdapter::new(config.clone());
        let result = adapter.test_connection(&config, None).await;
        assert!(
            result.is_err(),
            "Empty path should return Err before attempting connection"
        );
    }

    #[tokio::test]
    async fn test_connection_memory_returns_true() {
        let config = make_config(":memory:");
        let adapter = SqliteAdapter::new(config.clone());
        let result = adapter.test_connection(&config, None).await;
        assert!(result.unwrap(), ":memory: should connect successfully");
    }

    // ── disconnect ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_disconnect_clears_connection() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        assert!(ConnectionTrait::is_connected(&adapter));
        ConnectionTrait::disconnect(&mut adapter).await.unwrap();
        assert!(!ConnectionTrait::is_connected(&adapter));
    }

    // ── get_server_info ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_server_info_returns_sqlite_version() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        let info = adapter.get_server_info().await.unwrap();
        assert_eq!(info.server_type, "SQLite");
        assert!(
            !info.version.is_empty(),
            "SQLite version should not be empty"
        );
    }

    // ── get_views ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_views_empty_when_no_views() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        let views = adapter.get_views(None).await.unwrap();
        assert!(views.is_empty());
    }

    #[tokio::test]
    async fn test_get_views_returns_created_view() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE src (x INTEGER)")
            .await
            .unwrap();
        adapter
            .execute_query("CREATE VIEW v_src AS SELECT x FROM src")
            .await
            .unwrap();
        let views = adapter.get_views(None).await.unwrap();
        assert!(
            views.iter().any(|v| v.name == "v_src"),
            "created view should appear in get_views result"
        );
    }

    // ── get_indexes ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_indexes_empty_for_table_with_no_index() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE idx0 (id INTEGER)")
            .await
            .unwrap();
        let indexes = adapter.get_indexes("idx0", None).await.unwrap();
        assert!(indexes.is_empty());
    }

    #[tokio::test]
    async fn test_get_indexes_returns_created_index() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE idx1 (id INTEGER, email TEXT)")
            .await
            .unwrap();
        adapter
            .execute_query("CREATE UNIQUE INDEX idx1_email ON idx1(email)")
            .await
            .unwrap();
        let indexes = adapter.get_indexes("idx1", None).await.unwrap();
        assert!(
            indexes
                .iter()
                .any(|i| i.name == "idx1_email" && i.is_unique),
            "unique index should be returned with is_unique = true"
        );
    }

    // ── get_foreign_keys ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_foreign_keys_returns_empty_when_no_fks() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE no_fk (id INTEGER)")
            .await
            .unwrap();
        let fks = adapter.get_foreign_keys("no_fk", None).await.unwrap();
        assert!(fks.is_empty());
    }

    #[tokio::test]
    async fn test_get_foreign_keys_returns_declared_fk() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        adapter
            .execute_query(
                "CREATE TABLE child (id INTEGER, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id))",
            )
            .await
            .unwrap();
        let fks = adapter.get_foreign_keys("child", None).await.unwrap();
        assert!(!fks.is_empty(), "child table should have one FK");
        assert_eq!(fks[0].referenced_table, "parent");
    }

    // ── list_stored_procedures ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_list_stored_procedures_returns_empty_for_sqlite() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        let procs = adapter.list_stored_procedures(None).await.unwrap();
        assert!(procs.is_empty(), "SQLite has no stored procedures");
    }

    // ── bulk_insert ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_bulk_insert_not_connected_returns_error() {
        let adapter = SqliteAdapter::new(make_config(":memory:"));
        let cols = vec!["id".to_string()];
        let rows = vec![vec![QueryValue::Int(1)]];
        let result = adapter.bulk_insert("t", &cols, &rows, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bulk_insert_inserts_rows() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE ins_t (id INTEGER, name TEXT)")
            .await
            .unwrap();
        let cols = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![QueryValue::Int(1), QueryValue::Text("Alice".into())],
            vec![QueryValue::Int(2), QueryValue::Text("Bob".into())],
        ];
        let n = adapter
            .bulk_insert("ins_t", &cols, &rows, None)
            .await
            .unwrap();
        assert_eq!(n, 2, "should insert 2 rows");
    }

    // ── bulk_update ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_bulk_update_updates_matching_rows() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE upd_t (id INTEGER, name TEXT)")
            .await
            .unwrap();
        adapter
            .execute_query("INSERT INTO upd_t VALUES (1, 'Alice'), (2, 'Bob')")
            .await
            .unwrap();
        let mut set = HashMap::new();
        set.insert("name".to_string(), QueryValue::Text("AliceX".into()));
        let updates = [(set, FilterExpr::Eq("id".to_string(), QueryValue::Int(1)))];
        let n = adapter.bulk_update("upd_t", &updates, None).await.unwrap();
        assert_eq!(n, 1);
        let result = adapter
            .execute_query("SELECT name FROM upd_t WHERE id = 1")
            .await
            .unwrap();
        assert!(matches!(
            result.rows.first().and_then(|r| r.first()),
            Some(QueryValue::Text(s)) if s == "AliceX"
        ));
    }

    // ── bulk_delete ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_bulk_delete_deletes_matching_rows() {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE del_t (id INTEGER, name TEXT)")
            .await
            .unwrap();
        adapter
            .execute_query("INSERT INTO del_t VALUES (1, 'A'), (2, 'B'), (3, 'C')")
            .await
            .unwrap();
        let filters = [FilterExpr::Eq("id".to_string(), QueryValue::Int(2))];
        let n = adapter.bulk_delete("del_t", &filters, None).await.unwrap();
        assert_eq!(n, 1);
        let result = adapter
            .execute_query("SELECT count(*) FROM del_t")
            .await
            .unwrap();
        let count = match result.rows.first().and_then(|r| r.first()) {
            Some(QueryValue::Int(c)) => *c,
            _ => -1,
        };
        assert_eq!(count, 2);
    }

    // ── execute_query_stream ───────────────────────────────────────────────────

    /// Helper: connect a fresh in-memory adapter.
    async fn connected_adapter() -> SqliteAdapter {
        let mut adapter = SqliteAdapter::new(make_config(":memory:"));
        let config = adapter.config.clone();
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
    }

    #[tokio::test]
    async fn stream_rows_match_execute_query() {
        use futures_util::TryStreamExt;

        let adapter = connected_adapter().await;
        adapter
            .execute_query("CREATE TABLE st (id INTEGER, name TEXT)")
            .await
            .unwrap();
        adapter
            .execute_query("INSERT INTO st VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma')")
            .await
            .unwrap();

        let expected = adapter
            .execute_query("SELECT id, name FROM st")
            .await
            .unwrap();

        let mut stream = adapter
            .execute_query_stream("SELECT id, name FROM st")
            .await
            .unwrap();

        let mut streamed_rows: Vec<Vec<QueryValue>> = Vec::new();
        while let Some(row) = stream.try_next().await.unwrap() {
            streamed_rows.push(row);
        }

        assert_eq!(streamed_rows.len(), expected.rows.len());
        for (got, want) in streamed_rows.iter().zip(expected.rows.iter()) {
            assert_eq!(got, want);
        }
    }

    #[tokio::test]
    async fn stream_maps_to_struct_via_from_query_row() {
        use crate::adapter::DbAdapterExt;

        #[derive(Debug, PartialEq)]
        struct Row {
            id: i64,
            label: String,
        }

        impl crate::adapter::FromQueryRow for Row {
            fn from_row(row: Vec<QueryValue>) -> std::result::Result<Self, DataError> {
                let id = match row.first() {
                    Some(QueryValue::Int(n)) => *n,
                    _ => return Err(DataError::TypeConversion("id".into())),
                };
                let label = match row.get(1) {
                    Some(QueryValue::Text(s)) => s.clone(),
                    _ => return Err(DataError::TypeConversion("label".into())),
                };
                Ok(Row { id, label })
            }
        }

        let adapter = connected_adapter().await;
        adapter
            .execute_query("CREATE TABLE mapped (id INTEGER, label TEXT)")
            .await
            .unwrap();
        adapter
            .execute_query("INSERT INTO mapped VALUES (10, 'X'), (20, 'Y')")
            .await
            .unwrap();

        let rows: Vec<Row> = adapter
            .execute_query_mapped("SELECT id, label FROM mapped")
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            Row {
                id: 10,
                label: "X".to_string()
            }
        );
        assert_eq!(
            rows[1],
            Row {
                id: 20,
                label: "Y".to_string()
            }
        );
    }

    #[tokio::test]
    async fn stream_propagates_from_query_row_error() {
        use crate::adapter::DbAdapterExt;

        #[derive(Debug)]
        struct BadMapper;

        impl crate::adapter::FromQueryRow for BadMapper {
            fn from_row(_row: Vec<QueryValue>) -> std::result::Result<Self, DataError> {
                Err(DataError::TypeConversion("always fails".into()))
            }
        }

        let adapter = connected_adapter().await;
        adapter
            .execute_query("CREATE TABLE errtest (x INTEGER)")
            .await
            .unwrap();
        adapter
            .execute_query("INSERT INTO errtest VALUES (1)")
            .await
            .unwrap();

        let result: Result<Vec<BadMapper>> =
            adapter.execute_query_mapped("SELECT x FROM errtest").await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::TypeConversion(_)));
    }
}
