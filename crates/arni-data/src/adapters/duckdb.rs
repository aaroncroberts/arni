use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, ForeignKeyInfo, IndexInfo, ProcedureInfo, QueryResult, QueryValue, ServerInfo,
    TableInfo, ViewInfo,
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

            let column_count = stmt.column_count();
            let mut columns = Vec::new();
            for i in 0..column_count {
                let col_name = stmt
                    .column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "unknown".to_string());
                columns.push(col_name);
            }

            let rows_result = stmt
                .query_map([], |row| {
                    let mut values = Vec::new();
                    for i in 0..column_count {
                        let value = Self::get_value(row, i)?;
                        values.push(value);
                    }
                    Ok(values)
                })
                .map_err(|e| DataError::Query(format!("Failed to execute query: {}", e)))?;

            let mut rows = Vec::new();
            for row_result in rows_result {
                let row = row_result
                    .map_err(|e| DataError::Query(format!("Failed to read row: {}", e)))?;
                rows.push(row);
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

    /// Get a value from a DuckDB row
    fn get_value(row: &duckdb::Row, idx: usize) -> std::result::Result<QueryValue, duckdb::Error> {
        // Try to get the value as different types
        // DuckDB supports many types, but we'll focus on common ones
        if let Ok(val) = row.get::<_, Option<bool>>(idx) {
            return Ok(val.map(QueryValue::Bool).unwrap_or(QueryValue::Null));
        }
        if let Ok(val) = row.get::<_, Option<i64>>(idx) {
            return Ok(val.map(QueryValue::Int).unwrap_or(QueryValue::Null));
        }
        if let Ok(val) = row.get::<_, Option<f64>>(idx) {
            return Ok(val.map(QueryValue::Float).unwrap_or(QueryValue::Null));
        }
        if let Ok(val) = row.get::<_, Option<String>>(idx) {
            return Ok(val.map(QueryValue::Text).unwrap_or(QueryValue::Null));
        }
        if let Ok(val) = row.get::<_, Option<Vec<u8>>>(idx) {
            return Ok(val.map(QueryValue::Bytes).unwrap_or(QueryValue::Null));
        }

        // If all else fails, return null
        Ok(QueryValue::Null)
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

    async fn export_dataframe(
        &self,
        _df: &DataFrame,
        _table_name: &str,
        _schema: Option<&str>,
        _replace: bool,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "export_dataframe not yet implemented for DuckDB".to_string(),
        ))
    }

    async fn read_table(&self, table_name: &str, _schema: Option<&str>) -> Result<DataFrame> {
        let query = format!("SELECT * FROM {}", table_name);
        let result = self.execute_query(&query).await?;
        result.to_dataframe()
    }

    async fn query_df(&self, query: &str) -> Result<DataFrame> {
        let result = self.execute_query(query).await?;
        result.to_dataframe()
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    async fn bulk_insert(
        &self,
        _table_name: &str,
        _columns: &[String],
        _rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_insert not yet implemented for DuckDB".to_string(),
        ))
    }

    async fn bulk_update(
        &self,
        _table_name: &str,
        _updates: &[(HashMap<String, QueryValue>, String)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_update not yet implemented for DuckDB".to_string(),
        ))
    }

    async fn bulk_delete(
        &self,
        _table_name: &str,
        _where_clauses: &[String],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_delete not yet implemented for DuckDB".to_string(),
        ))
    }

    async fn get_server_info(&self) -> Result<ServerInfo> {
        let query = "SELECT library_version() as version";
        let result = self.execute_query_blocking(query.to_string()).await?;

        let version = if let Some(row) = result.rows.first() {
            if let Some(QueryValue::Text(v)) = row.first() {
                v.clone()
            } else {
                "unknown".to_string()
            }
        } else {
            "unknown".to_string()
        };

        let mut extra_info = HashMap::new();
        extra_info.insert("database".to_string(), self.config.database.clone());

        Ok(ServerInfo {
            version,
            server_type: "DuckDB".to_string(),
            extra_info,
        })
    }

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

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name.to_string()),
            columns,
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
