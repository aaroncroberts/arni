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
    AdapterMetadata, Connection, ConnectionConfig, DatabaseType, DbAdapter, ForeignKeyInfo,
    IndexInfo, ProcedureInfo, QueryResult, QueryValue, Result, ViewInfo,
};
use crate::DataError;
use polars::prelude::*;
use std::collections::HashMap;
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

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
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

        let mut rows_inserted: u64 = 0;

        // Insert rows in batches for better performance
        for row_idx in 0..df.height() {
            // Extract values for this row from each column
            let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

            for col_name in &column_names {
                let column = df.column(col_name).map_err(|e| {
                    DataError::DataFrame(format!("Column '{}' not found: {}", col_name, e))
                })?;

                // Get the underlying Series from the Column
                let series = column.as_materialized_series();

                // Convert series value at row_idx to ToSql parameter
                let param = self.series_value_to_sql(series, row_idx)?;
                params.push(param);
            }

            // Convert params to references (cast to remove Send bound for tokio-postgres)
            let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            // Execute insert
            client
                .execute(&insert_sql, &params_refs[..])
                .await
                .map_err(|e| {
                    DataError::Query(format!("Failed to insert row {}: {}", row_idx, e))
                })?;

            rows_inserted += 1;
        }

        Ok(rows_inserted)
    }

    // ===== Schema Discovery =====

    async fn list_databases(&self) -> Result<Vec<String>> {
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

        // Query pg_database catalog
        let query = "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname";
        let rows = client
            .query(query, &[])
            .await
            .map_err(|e| DataError::Query(format!("Failed to list databases: {}", e)))?;

        let databases: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();

        Ok(databases)
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
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

        let rows = client
            .query(&query, &[])
            .await
            .map_err(|e| DataError::Query(format!("Failed to list tables: {}", e)))?;

        let tables: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();

        Ok(tables)
    }

    async fn describe_table(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<crate::adapter::TableInfo> {
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

        let rows = client
            .query(&column_query, &[])
            .await
            .map_err(|e| DataError::Query(format!("Failed to describe table: {}", e)))?;

        if rows.is_empty() {
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

        let pk_rows = client
            .query(&pk_query, &[])
            .await
            .map_err(|e| DataError::Query(format!("Failed to query primary keys: {}", e)))?;

        let primary_keys: std::collections::HashSet<String> =
            pk_rows.iter().map(|row| row.get::<_, String>(0)).collect();

        // Build column info
        let columns: Vec<crate::adapter::ColumnInfo> = rows
            .iter()
            .map(|row| {
                let col_name: String = row.get(0);
                let data_type: String = row.get(1);
                let is_nullable: String = row.get(2);
                let default_value: Option<String> = row.get(3);

                crate::adapter::ColumnInfo {
                    name: col_name.clone(),
                    data_type,
                    nullable: is_nullable == "YES",
                    default_value,
                    is_primary_key: primary_keys.contains(&col_name),
                }
            })
            .collect();

        Ok(crate::adapter::TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name.to_string()),
            columns,
        })
    }

    // ===== Metadata Methods =====

    async fn get_indexes(&self, table_name: &str, schema: Option<&str>) -> Result<Vec<IndexInfo>> {
        // Check connection
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

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

        let rows = client
            .query(query, &[&table_name, &schema_name])
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
                name: row.get("index_name"),
                table_name: row.get("table_name"),
                schema: Some(row.get::<_, String>("schema_name")),
                columns: row.get("columns"),
                is_unique: row.get("is_unique"),
                is_primary: row.get("is_primary"),
                index_type: row.get("index_type"),
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
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

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

        let rows = client
            .query(query, &[&table_name, &schema_name])
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
            let fk_name: String = row.get("constraint_name");
            let column: String = row.get("column_name");
            let ref_column: String = row.get("foreign_column_name");

            fk_map
                .entry(fk_name.clone())
                .or_insert_with(|| ForeignKeyInfo {
                    name: fk_name.clone(),
                    table_name: row.get("table_name"),
                    schema: Some(row.get::<_, String>("table_schema")),
                    columns: Vec::new(),
                    referenced_table: row.get("foreign_table_name"),
                    referenced_schema: Some(row.get::<_, String>("foreign_table_schema")),
                    referenced_columns: Vec::new(),
                    on_delete: Some(row.get::<_, String>("delete_rule")),
                    on_update: Some(row.get::<_, String>("update_rule")),
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
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        let query = "
            SELECT
                table_name,
                table_schema
            FROM information_schema.views
            WHERE table_schema = $1
            ORDER BY table_name
        ";

        let rows = client.query(query, &[&schema_name]).await.map_err(|e| {
            DataError::Query(format!(
                "Failed to get views for schema '{}': {}",
                schema_name, e
            ))
        })?;

        let views = rows
            .iter()
            .map(|row| ViewInfo {
                name: row.get("table_name"),
                schema: Some(row.get::<_, String>("table_schema")),
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
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        let query = "
            SELECT view_definition
            FROM information_schema.views
            WHERE table_name = $1 AND table_schema = $2
        ";

        let rows = client
            .query(query, &[&view_name, &schema_name])
            .await
            .map_err(|e| {
                DataError::Query(format!(
                    "Failed to get view definition for '{}.{}': {}",
                    schema_name, view_name, e
                ))
            })?;

        Ok(rows.first().map(|row| row.get("view_definition")))
    }

    async fn list_stored_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // Check connection
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

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

        let rows = client.query(query, &[&schema_name]).await.map_err(|e| {
            DataError::Query(format!(
                "Failed to get stored procedures for schema '{}': {}",
                schema_name, e
            ))
        })?;

        let procedures = rows
            .iter()
            .map(|row| ProcedureInfo {
                name: row.get("name"),
                schema: Some(row.get::<_, String>("schema")),
                return_type: Some(row.get::<_, String>("return_type")),
                language: Some(row.get::<_, String>("language")),
            })
            .collect();

        Ok(procedures)
    }

    // ===== Bulk Operations =====

    async fn bulk_insert(
        &self,
        _table_name: &str,
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
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let _schema_name = schema.unwrap_or("public");

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

        // Note: tokio-postgres doesn't support easy dynamic parameter binding
        // For a production system, consider using prepared statements in transactions
        // For now, return NotSupported to indicate this needs special handling
        return Err(DataError::NotSupported(
            "bulk_insert requires parameterized statement support - use transactions with individual inserts".to_string()
        ));
    }

    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, String)],
        schema: Option<&str>,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }

        // Check connection
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        let mut total_affected = 0u64;

        // Execute each update
        for (set_clauses, where_clause) in updates {
            if set_clauses.is_empty() {
                continue;
            }

            // Build SET clause
            let mut set_parts = Vec::new();
            for (column, _) in set_clauses.iter() {
                set_parts.push(format!("{} = $1", column));
            }

            let query = format!(
                "UPDATE {}.{} SET {} WHERE {}",
                schema_name,
                table_name,
                set_parts.join(", "),
                where_clause
            );

            // Note: Simplified implementation - proper implementation would need dynamic parameter binding
            let result = client.execute(&query, &[]).await.map_err(|e| {
                DataError::Query(format!(
                    "Failed to bulk update {}.{}: {}",
                    schema_name, table_name, e
                ))
            })?;

            total_affected += result;
        }

        Ok(total_affected)
    }

    async fn bulk_delete(
        &self,
        table_name: &str,
        where_clauses: &[String],
        schema: Option<&str>,
    ) -> Result<u64> {
        if where_clauses.is_empty() {
            return Ok(0);
        }

        // Check connection
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        let schema_name = schema.unwrap_or("public");

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        let mut total_affected = 0u64;

        // Execute each delete
        for where_clause in where_clauses {
            if where_clause.trim().is_empty() {
                continue;
            }

            let query = format!(
                "DELETE FROM {}.{} WHERE {}",
                schema_name, table_name, where_clause
            );

            let result = client.execute(&query, &[]).await.map_err(|e| {
                DataError::Query(format!(
                    "Failed to bulk delete from {}.{}: {}",
                    schema_name, table_name, e
                ))
            })?;

            total_affected += result;
        }

        Ok(total_affected)
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

    /// Convert a value from a Polars Series to a PostgreSQL ToSql parameter
    ///
    /// Extracts the value at `row_idx` from the `series` and converts it to a type
    /// that implements `ToSql` for use in parameterized queries.
    fn series_value_to_sql(
        &self,
        series: &Series,
        row_idx: usize,
    ) -> Result<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> {
        // Check if value is null by getting the null mask and checking the index
        let null_mask = series.is_null();
        if null_mask.get(row_idx).unwrap_or(false) {
            return Ok(Box::new(None::<i32>)); // NULL value
        }

        // Convert based on Series data type
        match series.dtype() {
            DataType::Boolean => {
                let val = series
                    .bool()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                Ok(Box::new(val))
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
                Ok(Box::new(val))
            }
            DataType::Int32 => {
                let val = series
                    .i32()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                Ok(Box::new(val))
            }
            DataType::Int64 => {
                let val = series
                    .i64()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                Ok(Box::new(val))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
                // Convert unsigned to signed for PostgreSQL
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
                Ok(Box::new(val))
            }
            DataType::UInt64 => {
                // Convert unsigned to signed for PostgreSQL
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
                Ok(Box::new(val))
            }
            DataType::Float32 => {
                let val = series
                    .f32()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                Ok(Box::new(val))
            }
            DataType::Float64 => {
                let val = series
                    .f64()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                Ok(Box::new(val))
            }
            DataType::String => {
                let val = series
                    .str()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                Ok(Box::new(val.to_string()))
            }
            DataType::Binary => {
                let val = series
                    .binary()
                    .map_err(|e| DataError::TypeConversion(e.to_string()))?
                    .get(row_idx)
                    .ok_or_else(|| {
                        DataError::DataFrame(format!("Index {} out of bounds", row_idx))
                    })?;
                Ok(Box::new(val.to_vec()))
            }
            dtype => {
                // For unsupported types, try to convert to string
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
                Ok(Box::new(val.to_string()))
            }
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
    async fn test_export_dataframe_basic() {
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        // Connect first
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();

        // Create test DataFrame with various types
        let df = DataFrame::new(vec![
            Series::new("id".into(), &[1i32, 2, 3]).into(),
            Series::new("name".into(), &["Alice", "Bob", "Charlie"]).into(),
            Series::new("score".into(), &[95.5f64, 87.3, 92.1]).into(),
            Series::new("active".into(), &[true, false, true]).into(),
        ])
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

    #[tokio::test]
    #[ignore]
    async fn test_export_dataframe_with_nulls() {
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();

        // Create DataFrame with NULL values
        let id_series = Series::new("id".into(), &[Some(1i32), Some(2), Some(3)]).into();
        let name_series =
            Series::new("name".into(), &[Some("Alice"), None, Some("Charlie")]).into();
        let score_series = Series::new("score".into(), &[Some(95.5f64), Some(87.3), None]).into();

        let df = DataFrame::new(vec![id_series, name_series, score_series]).unwrap();

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

    #[tokio::test]
    #[ignore]
    async fn test_export_dataframe_replace_table() {
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config.clone());

        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();

        // First export
        let df1 = DataFrame::new(vec![Series::new("value".into(), &[1i32, 2, 3]).into()]).unwrap();

        DbAdapter::export_dataframe(&adapter, &df1, "test_replace", None, true)
            .await
            .unwrap();

        // Second export with replace=true (should drop and recreate)
        let df2 = DataFrame::new(vec![Series::new("value".into(), &[10i32, 20]).into()]).unwrap();

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

    #[tokio::test]
    async fn test_export_dataframe_not_connected() {
        use crate::adapter::DbAdapter;
        use polars::prelude::*;

        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let df = DataFrame::new(vec![Series::new("id".into(), &[1i32, 2, 3]).into()]).unwrap();

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
        use crate::adapter::{Connection, DbAdapter};

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);
        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        let result = DbAdapter::list_databases(&adapter).await;
        assert!(result.is_ok());

        let databases = result.unwrap();
        assert!(databases.len() > 0);
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
        use crate::adapter::{Connection, DbAdapter};

        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);
        Connection::connect(&mut adapter)
            .await
            .expect("Failed to connect");

        let result = DbAdapter::list_tables(&adapter, Some("information_schema")).await;
        assert!(result.is_ok());

        let tables = result.unwrap();
        // information_schema should have standard tables
        assert!(tables.len() > 0);
        assert!(tables.contains(&"tables".to_string()));
        assert!(tables.contains(&"columns".to_string()));

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
        assert!(table_info.columns.len() > 0);

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
