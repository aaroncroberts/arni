use crate::adapter::{
    escape_like_pattern, filter_to_sql, AdapterMetadata, ColumnInfo, Connection as ConnectionTrait,
    ConnectionConfig, DatabaseType, DbAdapter, FilterExpr, ForeignKeyInfo, IndexInfo,
    ProcedureInfo, QueryResult, QueryValue, ServerInfo, TableInfo, TableSearchMode, ViewInfo,
};
use crate::DataError;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, instrument, warn};

type Result<T> = std::result::Result<T, DataError>;

/// DuckDB database adapter
///
/// This adapter uses the duckdb crate to connect to DuckDB databases.
/// DuckDB is an analytical database optimized for OLAP workloads.
/// It supports both file-based and in-memory databases.
///
/// # Connection Management
///
/// The adapter maintains a connection wrapped in Arc<RwLock> for thread-safe access.
/// DuckDB is synchronous, so we use tokio::task::spawn_blocking for async operations.
///
/// # Thread Safety
///
/// The adapter uses internal locking to ensure thread-safe access to the underlying
/// DuckDB connection.
pub struct DuckDbAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// DuckDB connection wrapped in Arc<Mutex> for thread-safe access
    /// Using Mutex instead of RwLock because DuckDB Connection is not Sync
    connection: Arc<Mutex<Option<duckdb::Connection>>>,
}

impl DuckDbAdapter {
    /// Create a new DuckDB adapter with the given configuration
    ///
    /// This does not establish a connection immediately. Call [`connect`](ConnectionTrait::connect)
    /// to establish the connection.
    pub fn new(config: ConnectionConfig) -> Self {
        debug!(database = %config.database, "Creating DuckDB adapter");
        Self {
            config,
            connection: Arc::new(Mutex::new(None)),
        }
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

    /// Execute a query in a blocking context
    #[instrument(skip(self, query), fields(adapter = "duckdb", query_length = query.len()))]
    async fn execute_query_blocking(&self, query: String) -> Result<QueryResult> {
        debug!("Executing query in blocking context");
        let start = std::time::Instant::now();
        let connection = self.connection.clone();

        let result = tokio::task::spawn_blocking(move || {
            let conn_guard = connection
                .lock()
                .map_err(|_| DataError::Connection("Lock poisoned".to_string()))?;

            let conn = conn_guard.as_ref().ok_or_else(|| {
                error!("Query attempted while not connected");
                DataError::Connection("Not connected".to_string())
            })?;

            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DataError::Query(format!("Failed to prepare query: {}", e)))?;

            // In duckdb-rs 1.x, column_count()/column_names() require the statement to
            // have been executed first. Use query() to execute and get a Rows iterator,
            // then read column names from rows.as_ref() (owned, no extra borrow).
            let mut rows_iter = stmt
                .query([])
                .map_err(|e| DataError::Query(format!("Failed to execute query: {}", e)))?;

            // column_names() is available after query() executes the statement.
            let columns: Vec<String> = rows_iter
                .as_ref()
                .map(|s| s.column_names())
                .unwrap_or_default();

            let n = columns.len();
            let mut rows = Vec::new();
            while let Some(row) = rows_iter
                .next()
                .map_err(|e| DataError::Query(format!("Failed to read row: {}", e)))?
            {
                let mut values = Vec::new();
                for i in 0..n {
                    let value = Self::get_value(row, i).map_err(|e| {
                        DataError::Query(format!("Failed to get column {}: {}", i, e))
                    })?;
                    values.push(value);
                }
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
        info!(
            rows = result.rows.len(),
            duration_ms = duration.as_millis(),
            "Query executed successfully"
        );

        Ok(result)
    }

    /// Execute a DML/DDL statement (INSERT/UPDATE/DELETE/CREATE/DROP) and return rows affected.
    ///
    /// Uses `Connection::execute` rather than `prepare` + `query` so it works
    /// correctly for non-SELECT statements where we want a row count.
    async fn execute_statement_blocking(&self, sql: String) -> Result<u64> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let conn_guard = connection
                .lock()
                .map_err(|_| DataError::Connection("Lock poisoned".to_string()))?;
            let conn = conn_guard
                .as_ref()
                .ok_or_else(|| DataError::Connection("Not connected".to_string()))?;
            conn.execute(&sql, [])
                .map(|n| n as u64)
                .map_err(|e| DataError::Query(format!("Failed to execute statement: {}", e)))
        })
        .await
        .map_err(|e| DataError::Connection(format!("Task join error: {}", e)))?
    }

    /// Convert a [`QueryValue`] to a SQL literal suitable for inline DuckDB SQL.
    ///
    /// Delegates to [`super::common::query_value_to_sql_literal`] with `bool_as_int = false`
    /// (DuckDB has a native BOOLEAN type; uses `TRUE`/`FALSE`).
    fn query_value_to_sql_literal(value: &QueryValue) -> String {
        super::common::query_value_to_sql_literal(value, false)
    }

    /// Map a Polars [`DataType`] to the corresponding DuckDB SQL type name.
    fn polars_dtype_to_duckdb_type(dtype: &DataType) -> &'static str {
        match dtype {
            DataType::Boolean => "BOOLEAN",
            DataType::Int8 => "TINYINT",
            DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INTEGER",
            DataType::Int64 => "BIGINT",
            DataType::UInt8 => "UTINYINT",
            DataType::UInt16 => "USMALLINT",
            DataType::UInt32 => "UINTEGER",
            DataType::UInt64 => "UBIGINT",
            DataType::Float32 => "FLOAT",
            DataType::Float64 => "DOUBLE",
            DataType::String => "VARCHAR",
            DataType::Binary => "BLOB",
            _ => "VARCHAR", // Safe fallback for dates, enums, structs, etc.
        }
    }

    /// Extract the value at `row_idx` from `series` as a DuckDB SQL literal string.
    ///
    /// Delegates to the shared implementation in [`super::common`], with booleans
    /// rendered as `TRUE`/`FALSE` (DuckDB has a native BOOLEAN type).
    fn series_value_to_sql_literal(series: &Series, row_idx: usize) -> Result<String> {
        super::common::series_value_to_sql_literal(series, row_idx, false)
    }

    /// Get a value from a DuckDB row
    fn get_value(row: &duckdb::Row, idx: usize) -> std::result::Result<QueryValue, duckdb::Error> {
        use duckdb::types::ValueRef;

        // Use get_ref() to obtain the actual storage-class value from DuckDB.
        // This avoids the type-guessing order problem (e.g. every non-zero int
        // being misread as Bool(true)) by inspecting the value's real type.
        match row.get_ref(idx)? {
            ValueRef::Null => Ok(QueryValue::Null),
            ValueRef::Boolean(b) => Ok(QueryValue::Bool(b)),
            ValueRef::TinyInt(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::SmallInt(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::Int(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::BigInt(v) => Ok(QueryValue::Int(v)),
            ValueRef::HugeInt(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::UTinyInt(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::USmallInt(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::UInt(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::UBigInt(v) => Ok(QueryValue::Int(v as i64)),
            ValueRef::Float(v) => Ok(QueryValue::Float(v as f64)),
            ValueRef::Double(v) => Ok(QueryValue::Float(v)),
            ValueRef::Text(bytes) => Ok(QueryValue::Text(
                String::from_utf8_lossy(bytes).into_owned(),
            )),
            ValueRef::Blob(bytes) => Ok(QueryValue::Bytes(bytes.to_vec())),
            // All other DuckDB types (Timestamp, Date, Decimal, etc.)
            // fall back to a string representation for now.
            other => Ok(QueryValue::Text(format!("{:?}", other))),
        }
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for DuckDbAdapter {
    #[instrument(skip(self), fields(adapter = "duckdb", database = %self.config.database))]
    async fn connect(&mut self) -> Result<()> {
        if self.config.db_type != DatabaseType::DuckDB {
            let err = DataError::Config(format!(
                "Invalid database type: expected DuckDB, got {:?}",
                self.config.db_type
            ));
            error!(error = %err, "Invalid database type");
            return Err(err);
        }

        Self::validate_database_path(&self.config.database)?;

        info!(database = %self.config.database, "Connecting to DuckDB");

        let path = self.config.database.clone();
        let connection = self.connection.clone();

        tokio::task::spawn_blocking(move || {
            let conn = if path == ":memory:" {
                duckdb::Connection::open_in_memory()
            } else {
                duckdb::Connection::open(&path)
            }
            .map_err(|e| {
                error!(error = %e, "Failed to open DuckDB connection");
                DataError::Connection(format!("Failed to connect: {}", e))
            })?;

            let mut conn_guard = connection
                .lock()
                .map_err(|_| DataError::Connection("Lock poisoned".to_string()))?;
            *conn_guard = Some(conn);

            Ok(())
        })
        .await
        .map_err(|e| {
            error!(error = %e, "Task join error during connect");
            DataError::Connection(format!("Task join error: {}", e))
        })?
        .map(|()| {
            info!("Connected to DuckDB successfully");
        })
    }

    #[instrument(skip(self), fields(adapter = "duckdb"))]
    async fn disconnect(&mut self) -> Result<()> {
        debug!("Disconnecting from DuckDB");
        let mut conn_guard = self
            .connection
            .lock()
            .map_err(|_| DataError::Connection("Lock poisoned".to_string()))?;
        *conn_guard = None;
        info!("Disconnected from DuckDB");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // Check synchronously without blocking
        match self.connection.lock() {
            Ok(guard) => guard.is_some(),
            Err(_) => false,
        }
    }

    #[instrument(skip(self), fields(adapter = "duckdb"))]
    async fn health_check(&self) -> Result<bool> {
        debug!("Performing health check");
        if !ConnectionTrait::is_connected(self) {
            warn!("Health check called but not connected");
            return Ok(false);
        }

        // Execute a simple query to verify connection
        match self.execute_query_blocking("SELECT 1".to_string()).await {
            Ok(_) => {
                debug!("Health check passed");
                Ok(true)
            }
            Err(e) => {
                warn!(error = %e, "Health check failed");
                Ok(false)
            }
        }
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[async_trait::async_trait]
impl DbAdapter for DuckDbAdapter {
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
        let path = config.database.clone();
        let result = tokio::task::spawn_blocking(move || {
            let conn = if path == ":memory:" {
                duckdb::Connection::open_in_memory()
            } else {
                duckdb::Connection::open(&path)
            };
            conn.is_ok()
        })
        .await
        .map_err(|e| DataError::Connection(format!("Task join error: {}", e)))?;

        Ok(result)
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::DuckDB
    }

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        self.execute_query_blocking(query.to_string()).await
    }

    #[instrument(skip(self, df), fields(adapter = "duckdb", table = %table_name, rows = df.height(), columns = df.width(), replace = replace))]
    async fn export_dataframe(
        &self,
        df: &DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        replace: bool,
    ) -> Result<u64> {
        if !ConnectionTrait::is_connected(self) {
            error!("duckdb connection error - not connected");
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        if replace {
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            self.execute_statement_blocking(drop_sql).await?;

            let column_defs: Vec<String> = df
                .columns()
                .iter()
                .map(|col| {
                    format!(
                        "{} {}",
                        col.name(),
                        Self::polars_dtype_to_duckdb_type(col.dtype())
                    )
                })
                .collect();
            let create_sql = format!("CREATE TABLE {} ({})", table_name, column_defs.join(", "));
            self.execute_statement_blocking(create_sql).await?;
        }

        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        if column_names.is_empty() || df.height() == 0 {
            return Ok(0);
        }

        let cols_clause = column_names.join(", ");
        let mut rows_inserted = 0u64;

        for row_idx in 0..df.height() {
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
            self.execute_statement_blocking(insert_sql).await?;
            rows_inserted += 1;
        }

        Ok(rows_inserted)
    }

    #[instrument(skip(self), fields(adapter = "duckdb", table = %table_name))]
    async fn read_table(&self, table_name: &str, _schema: Option<&str>) -> Result<DataFrame> {
        let query = format!("SELECT * FROM {}", table_name);
        let result = self.execute_query(&query).await?;
        result.to_dataframe()
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    #[instrument(skip(self, columns, rows), fields(adapter = "duckdb", table = %table_name, row_count = rows.len()))]
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
        if !ConnectionTrait::is_connected(self) {
            error!("duckdb connection error - not connected");
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
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

        // Batch rows to avoid excessively long individual SQL statements.
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
            let rows_affected = self.execute_statement_blocking(insert_sql).await?;
            total_inserted += rows_affected;
        }

        Ok(total_inserted)
    }

    #[instrument(skip(self, updates), fields(adapter = "duckdb", table = %table_name))]
    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, FilterExpr)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }
        if !ConnectionTrait::is_connected(self) {
            error!("duckdb connection error - not connected");
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
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
            let rows_affected = self.execute_statement_blocking(update_sql).await?;
            total_affected += rows_affected;
        }

        Ok(total_affected)
    }

    #[instrument(skip(self, filters), fields(adapter = "duckdb", table = %table_name))]
    async fn bulk_delete(
        &self,
        table_name: &str,
        filters: &[FilterExpr],
        _schema: Option<&str>,
    ) -> Result<u64> {
        if filters.is_empty() {
            return Ok(0);
        }
        if !ConnectionTrait::is_connected(self) {
            error!("duckdb connection error - not connected");
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let mut total_affected = 0u64;

        for filter in filters {
            let delete_sql = format!("DELETE FROM {} WHERE {}", table_name, filter_to_sql(filter));
            let rows_affected = self.execute_statement_blocking(delete_sql).await?;
            total_affected += rows_affected;
        }

        Ok(total_affected)
    }

    #[instrument(skip(self), fields(adapter = "duckdb"))]
    async fn get_server_info(&self) -> Result<ServerInfo> {
        // Read the bundled DuckDB version directly from the C library — avoids any
        // SQL compatibility issues across DuckDB engine versions.
        let version = unsafe {
            let ptr = duckdb::ffi::duckdb_library_version();
            if ptr.is_null() {
                "unknown".to_string()
            } else {
                std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };

        let mut extra_info = HashMap::new();
        extra_info.insert("database".to_string(), self.config.database.clone());

        Ok(ServerInfo {
            version,
            server_type: "DuckDB".to_string(),
            extra_info,
        })
    }

    #[instrument(skip(self), fields(adapter = "duckdb"))]
    async fn list_databases(&self) -> Result<Vec<String>> {
        // DuckDB can attach multiple databases
        let query = "SELECT database_name FROM duckdb_databases()";
        let result = self.execute_query_blocking(query.to_string()).await?;

        let databases = result
            .rows
            .iter()
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

    #[instrument(skip(self), fields(adapter = "duckdb", schema = ?schema))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        let query = if let Some(schema_name) = schema {
            format!(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'",
                schema_name
            )
        } else {
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                .to_string()
        };

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
            .collect();

        Ok(tables)
    }

    #[instrument(skip(self), fields(adapter = "duckdb", pattern = %pattern, mode = ?mode, schema = ?schema))]
    async fn find_tables(
        &self,
        pattern: &str,
        schema: Option<&str>,
        mode: TableSearchMode,
    ) -> Result<Vec<String>> {
        let escaped = escape_like_pattern(pattern);
        let like_pattern = match mode {
            TableSearchMode::StartsWith => format!("{}%", escaped),
            TableSearchMode::Contains => format!("%{}%", escaped),
            TableSearchMode::EndsWith => format!("%{}", escaped),
        };
        // Escape single quotes for safe inline SQL formatting
        let safe_pattern = like_pattern.replace('\'', "''");
        let schema_name = schema.unwrap_or("main");

        let query = format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = '{}' AND table_name LIKE '{}' ESCAPE '\\' \
             ORDER BY table_name",
            schema_name, safe_pattern
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
            .collect();

        Ok(tables)
    }

    #[instrument(skip(self), fields(adapter = "duckdb", table = %table_name, schema = ?schema))]
    async fn describe_table(&self, table_name: &str, schema: Option<&str>) -> Result<TableInfo> {
        let schema_name = schema.unwrap_or("main");
        let query = format!(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}' ORDER BY ordinal_position",
            schema_name, table_name
        );

        let result = self.execute_query_blocking(query).await?;

        let columns = result
            .rows
            .iter()
            .map(|row| {
                let name = if let Some(QueryValue::Text(n)) = row.first() {
                    n.clone()
                } else {
                    "unknown".to_string()
                };

                let data_type = if let Some(QueryValue::Text(dt)) = row.get(1) {
                    dt.clone()
                } else {
                    "unknown".to_string()
                };

                let nullable = if let Some(QueryValue::Text(n)) = row.get(2) {
                    n.to_uppercase() == "YES"
                } else {
                    true
                };

                ColumnInfo {
                    name,
                    data_type,
                    nullable,
                    default_value: None,
                    is_primary_key: false,
                }
            })
            .collect();

        // Row count via COUNT(*); size and creation time are not tracked for in-memory tables
        let count_query = format!(
            "SELECT COUNT(*) FROM \"{}\".\"{}\"",
            schema_name, table_name
        );
        let row_count = self
            .execute_query_blocking(count_query)
            .await
            .ok()
            .and_then(|r| r.rows.into_iter().next())
            .and_then(|row| match row.into_iter().next() {
                Some(QueryValue::Int(n)) => Some(n),
                _ => None,
            });

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name.to_string()),
            columns,
            row_count,
            size_bytes: None,
            created_at: None,
        })
    }

    async fn get_indexes(&self, _table_name: &str, schema: Option<&str>) -> Result<Vec<IndexInfo>> {
        let _schema_name = schema.unwrap_or("main");

        // DuckDB doesn't have a standard information_schema for indexes
        // Use PRAGMA to get index information
        let query = "PRAGMA show_tables".to_string();
        let _result = self.execute_query_blocking(query).await?;

        // For now, return empty - DuckDB index introspection is limited
        // Would need to query duckdb_indexes() function if available
        Ok(Vec::new())
    }

    async fn get_foreign_keys(
        &self,
        _table_name: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        // DuckDB supports foreign keys but they're not enforced by default
        // Return empty for now
        Ok(Vec::new())
    }

    async fn get_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let query = if let Some(schema_name) = schema {
            format!(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_type = 'VIEW'",
                schema_name
            )
        } else {
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'VIEW'"
                .to_string()
        };

        let result = self.execute_query_blocking(query).await?;

        let views = result
            .rows
            .iter()
            .map(|row| {
                let name = if let Some(QueryValue::Text(n)) = row.first() {
                    n.clone()
                } else {
                    "unknown".to_string()
                };

                ViewInfo {
                    name,
                    schema: schema.map(|s| s.to_string()),
                    definition: None,
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
        let schema_name = schema.unwrap_or("main");
        let query = format!(
            "SELECT view_definition FROM information_schema.views WHERE table_schema = '{}' AND table_name = '{}'",
            schema_name, view_name
        );

        let result = self.execute_query_blocking(query).await?;

        if let Some(row) = result.rows.first() {
            if let Some(QueryValue::Text(def)) = row.first() {
                return Ok(Some(def.clone()));
            }
        }

        Ok(None)
    }

    async fn list_stored_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // DuckDB doesn't have traditional stored procedures
        // It has macros which are similar
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{Connection as ConnectionTrait, DatabaseType, QueryValue};

    fn make_config(database: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: "test-duckdb".to_string(),
            name: "Test DuckDB".to_string(),
            db_type: DatabaseType::DuckDB,
            host: None,
            port: None,
            database: database.to_string(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
        }
    }

    #[test]
    fn test_new_adapter_stores_config() {
        let config = make_config(":memory:");
        let adapter = DuckDbAdapter::new(config);
        assert_eq!(adapter.config.database, ":memory:");
        assert_eq!(adapter.config.db_type, DatabaseType::DuckDB);
    }

    #[test]
    fn test_is_connected_initially_false() {
        let adapter = DuckDbAdapter::new(make_config(":memory:"));
        assert!(!ConnectionTrait::is_connected(&adapter));
    }

    #[test]
    fn test_validate_database_path_memory() {
        assert!(DuckDbAdapter::validate_database_path(":memory:").is_ok());
    }

    #[test]
    fn test_validate_database_path_empty_fails() {
        let err = DuckDbAdapter::validate_database_path("").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_validate_database_path_too_long_fails() {
        let long_path = "a".repeat(4097);
        let err = DuckDbAdapter::validate_database_path(&long_path).unwrap_err();
        assert!(err.to_string().contains("too long"));
    }

    #[test]
    fn test_validate_database_path_valid_file() {
        assert!(DuckDbAdapter::validate_database_path("/tmp/test.duckdb").is_ok());
        assert!(DuckDbAdapter::validate_database_path("relative/path.db").is_ok());
    }

    #[test]
    fn test_config_accessor() {
        let config = make_config("/tmp/test.duckdb");
        let adapter = DuckDbAdapter::new(config.clone());
        assert_eq!(adapter.config().id, config.id);
        assert_eq!(adapter.config().database, "/tmp/test.duckdb");
    }

    // ── query_value_to_sql_literal ──────────────────────────────────────────

    #[test]
    fn test_sql_literal_null() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Null),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_bool_true() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Bool(true)),
            "TRUE"
        );
    }

    #[test]
    fn test_sql_literal_bool_false() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Bool(false)),
            "FALSE"
        );
    }

    #[test]
    fn test_sql_literal_int() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Int(42)),
            "42"
        );
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Int(-7)),
            "-7"
        );
    }

    #[test]
    fn test_sql_literal_float() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Float(3.14)),
            "3.14"
        );
    }

    #[test]
    fn test_sql_literal_float_nan_becomes_null() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Float(f64::NAN)),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_float_inf_becomes_null() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Float(f64::INFINITY)),
            "NULL"
        );
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Float(f64::NEG_INFINITY)),
            "NULL"
        );
    }

    #[test]
    fn test_sql_literal_text_plain() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Text("hello".to_string())),
            "'hello'"
        );
    }

    #[test]
    fn test_sql_literal_text_with_single_quote() {
        // Single quotes in strings are escaped as ''
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Text("it's".to_string())),
            "'it''s'"
        );
    }

    #[test]
    fn test_sql_literal_bytes() {
        let bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Bytes(bytes)),
            "X'deadbeef'"
        );
    }

    #[test]
    fn test_sql_literal_empty_bytes() {
        assert_eq!(
            DuckDbAdapter::query_value_to_sql_literal(&QueryValue::Bytes(vec![])),
            "X''"
        );
    }

    // ── polars_dtype_to_duckdb_type ─────────────────────────────────────────

    #[test]
    fn test_dtype_mapping_int_types() {
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Int8),
            "TINYINT"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Int16),
            "SMALLINT"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Int32),
            "INTEGER"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Int64),
            "BIGINT"
        );
    }

    #[test]
    fn test_dtype_mapping_uint_types() {
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::UInt8),
            "UTINYINT"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::UInt16),
            "USMALLINT"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::UInt32),
            "UINTEGER"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::UInt64),
            "UBIGINT"
        );
    }

    #[test]
    fn test_dtype_mapping_float_types() {
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Float32),
            "FLOAT"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Float64),
            "DOUBLE"
        );
    }

    #[test]
    fn test_dtype_mapping_string_and_bool() {
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::String),
            "VARCHAR"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Boolean),
            "BOOLEAN"
        );
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Binary),
            "BLOB"
        );
    }

    #[test]
    fn test_dtype_mapping_unknown_falls_back_to_varchar() {
        // Date, Datetime, etc. fall back to VARCHAR
        assert_eq!(
            DuckDbAdapter::polars_dtype_to_duckdb_type(&DataType::Date),
            "VARCHAR"
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
