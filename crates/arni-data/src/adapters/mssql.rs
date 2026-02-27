use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, ForeignKeyInfo, IndexInfo, ProcedureInfo, QueryResult, QueryValue, ServerInfo,
    TableInfo, ViewInfo,
};
use crate::DataError;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, error, info, instrument, warn};

type Result<T> = std::result::Result<T, DataError>;

/// Microsoft SQL Server database adapter using tiberius
///
/// This adapter uses tiberius to connect to SQL Server databases.
///
/// # Connection Management
///
/// The adapter maintains a tiberius client wrapped in Arc<RwLock> for thread-safe access.
/// Connections are established when `connect()` is called.
///
/// # Thread Safety
///
/// The adapter uses internal locking to ensure thread-safe access to the underlying
/// SQL Server connection.
pub struct SqlServerAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// Tiberius client wrapped in Arc<RwLock> for thread-safe access
    client: Arc<RwLock<Option<Client<Compat<TcpStream>>>>>,
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
            client: Arc::new(RwLock::new(None)),
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

        // Trust server certificate for development
        tiberius_config.trust_cert();

        Ok(tiberius_config)
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
            error!(error = %err, "Invalid database type");
            return Err(err);
        }

        Self::validate_database_name(&self.config.database)?;

        let host = self.config.host.as_deref().unwrap_or("localhost");
        let port = self.config.port.unwrap_or(1433);
        info!(host, port, database = %self.config.database, "Connecting to SQL Server");

        let tiberius_config = Self::build_config(&self.config, None)?;

        let tcp = TcpStream::connect(tiberius_config.get_addr())
            .await
            .map_err(|e| {
                error!(error = %e, "TCP connection failed");
                DataError::Connection(format!("Failed to connect: {}", e))
            })?;

        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .map_err(|e| {
                error!(error = %e, "Authentication failed");
                DataError::Connection(format!("Failed to authenticate: {}", e))
            })?;

        let mut client_guard = self.client.write().await;
        *client_guard = Some(client);

        info!("Connected to SQL Server successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(adapter = "sqlserver"))]
    async fn disconnect(&mut self) -> Result<()> {
        debug!("Disconnecting from SQL Server");
        let mut client_guard = self.client.write().await;
        *client_guard = None;
        info!("Disconnected from SQL Server");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // Simplified - would need async implementation for proper check
        false
    }

    #[instrument(skip(self), fields(adapter = "sqlserver"))]
    async fn health_check(&self) -> Result<bool> {
        debug!("Performing health check");
        let mut client_guard = self.client.write().await;
        if let Some(client) = client_guard.as_mut() {
            client
                .query("SELECT 1", &[])
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
            warn!("Health check called but not connected");
            Ok(false)
        }
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

        let host = config.host.as_deref().unwrap_or("localhost");
        let port = config.port.unwrap_or(1433);
        info!(host, port, database = %config.database, "Connecting to SQL Server");

        Self::validate_database_name(&config.database)?;

        let tiberius_config = Self::build_config(config, password)?;

        let tcp = TcpStream::connect(tiberius_config.get_addr())
            .await
            .map_err(|e| {
                error!(error = %e, "TCP connection failed");
                DataError::Connection(format!("Failed to connect: {}", e))
            })?;

        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .map_err(|e| {
                error!(error = %e, "Authentication failed");
                DataError::Connection(format!("Failed to authenticate: {}", e))
            })?;

        let mut client_guard = self.client.write().await;
        *client_guard = Some(client);

        info!("Connected to SQL Server successfully");
        Ok(())
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

        let tiberius_config = Self::build_config(config, password)?;

        let tcp_result = TcpStream::connect(tiberius_config.get_addr()).await;
        if tcp_result.is_err() {
            return Ok(false);
        }

        let client_result =
            Client::connect(tiberius_config, tcp_result.unwrap().compat_write()).await;
        Ok(client_result.is_ok())
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::SQLServer
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    #[instrument(skip(self, query), fields(adapter = "sqlserver", query_length = query.len()))]
    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        debug!("Executing query");
        let start = std::time::Instant::now();

        let mut client_guard = self.client.write().await;
        let client = client_guard.as_mut().ok_or_else(|| {
            error!("Query attempted while not connected");
            DataError::Connection("Not connected - call connect() first".to_string())
        })?;

        let stream = client.query(query, &[]).await.map_err(|e| {
            error!(error = %e, "Query execution failed");
            DataError::Query(format!("Query failed: {}", e))
        })?;

        let rows: Vec<tiberius::Row> = stream
            .into_results()
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to fetch results");
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
            rows = result_rows.len(),
            duration_ms = duration.as_millis(),
            "Query executed successfully"
        );

        Ok(QueryResult {
            columns,
            rows: result_rows,
            rows_affected: None,
        })
    }

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

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name.to_string()),
            columns,
        })
    }

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

    async fn export_dataframe(
        &self,
        _df: &DataFrame,
        _table_name: &str,
        _schema: Option<&str>,
        _replace: bool,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "export_dataframe not yet implemented for SQL Server".to_string(),
        ))
    }

    async fn bulk_insert(
        &self,
        _table_name: &str,
        _columns: &[String],
        _rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_insert not yet implemented for SQL Server".to_string(),
        ))
    }

    async fn bulk_update(
        &self,
        _table_name: &str,
        _updates: &[(HashMap<String, QueryValue>, String)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_update not yet implemented for SQL Server".to_string(),
        ))
    }

    async fn bulk_delete(
        &self,
        _table_name: &str,
        _where_clauses: &[String],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(DataError::NotSupported(
            "bulk_delete not yet implemented for SQL Server".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{Connection as ConnectionTrait, DatabaseType};

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

    #[test]
    fn test_config_accessor() {
        let config = make_config("test_db");
        let adapter = SqlServerAdapter::new(config.clone());
        assert_eq!(adapter.config().id, config.id);
    }
}
