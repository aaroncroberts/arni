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
            let conn = conn_guard
                .as_ref()
                .ok_or_else(|| {
                    error!("Connection not available");
                    DataError::Connection("Not connected".to_string())
                })?;

            // Prepare and execute the statement
            let mut stmt = conn
                .statement(&query)
                .build()
                .map_err(|e| {
                    error!(error = %e, "Failed to prepare statement");
                    DataError::Query(format!("Failed to prepare statement: {}", e))
                })?;

            let result_set = stmt
                .query(&[])
                .map_err(|e| {
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
        info!(rows = row_count, duration_ms = duration.as_millis(), "Query executed successfully");
        
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
            let conn = oracle::Connection::connect(&username, &password, &connect_string)
                .map_err(|e| {
                    error!(error = %e, "Failed to establish Oracle connection");
                    DataError::Connection(format!("Failed to connect: {}", e))
                })?;

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
    async fn disconnect(&mut self) ->Result<()> {
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
        _df: &DataFrame,
        _table_name: &str,
        _schema: Option<&str>,
        _replace: bool,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "DataFrame export not yet implemented for Oracle".to_string(),
        ))
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
        _table_name: &str,
        _columns: &[String],
        _rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_insert not yet implemented for Oracle".to_string(),
        ))
    }

    async fn bulk_update(
        &self,
        _table_name: &str,
        _updates: &[(HashMap<String, QueryValue>, String)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_update not yet implemented for Oracle".to_string(),
        ))
    }

    async fn bulk_delete(
        &self,
        _table_name: &str,
        _where_clauses: &[String],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_delete not yet implemented for Oracle".to_string(),
        ))
    }
}
