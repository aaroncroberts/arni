//! Database adapter trait and related types
//!
//! This module defines the core [`DbAdapter`] trait that all database adapters must implement,
//! along with supporting types for configuration, query results, and schema information.
//!
//! The trait is designed around Polars DataFrames as the primary data interchange format,
//! while maintaining support for traditional row-based queries through [`QueryResult`].

use async_trait::async_trait;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// Re-export error types (will be defined in error.rs)
pub type Result<T> = std::result::Result<T, crate::DataError>;

/// Represents a database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// Unique identifier for this connection
    pub id: String,
    /// Display name for the connection
    pub name: String,
    /// Database type (postgres, mysql, sqlite, etc.)
    pub db_type: DatabaseType,
    /// Host address (not used for file-based databases like SQLite)
    pub host: Option<String>,
    /// Port number
    pub port: Option<u16>,
    /// Database name
    pub database: String,
    /// Username for authentication
    pub username: Option<String>,
    /// Whether to use SSL/TLS
    pub use_ssl: bool,
    /// Additional connection parameters
    pub parameters: HashMap<String, String>,
}

/// Supported database types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    Postgres,
    MySQL,
    SQLite,
    MongoDB,
    SQLServer,
    Oracle,
    DuckDB,
}

impl DatabaseType {
    /// Returns the default port for this database type
    pub fn default_port(&self) -> Option<u16> {
        match self {
            DatabaseType::Postgres => Some(5432),
            DatabaseType::MySQL => Some(3306),
            DatabaseType::SQLite => None,
            DatabaseType::MongoDB => Some(27017),
            DatabaseType::SQLServer => Some(1433),
            DatabaseType::Oracle => Some(1521),
            DatabaseType::DuckDB => None,
        }
    }
}

impl fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseType::Postgres => write!(f, "PostgreSQL"),
            DatabaseType::MySQL => write!(f, "MySQL"),
            DatabaseType::SQLite => write!(f, "SQLite"),
            DatabaseType::MongoDB => write!(f, "MongoDB"),
            DatabaseType::SQLServer => write!(f, "SQL Server"),
            DatabaseType::Oracle => write!(f, "Oracle"),
            DatabaseType::DuckDB => write!(f, "DuckDB"),
        }
    }
}

/// Represents a value in a query result
///
/// This enum is used for bulk operations and internal query processing.
/// For general data interchange, use Polars DataFrames instead.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum QueryValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl fmt::Display for QueryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryValue::Null => write!(f, "NULL"),
            QueryValue::Bool(b) => write!(f, "{}", b),
            QueryValue::Int(i) => write!(f, "{}", i),
            QueryValue::Float(fl) => write!(f, "{}", fl),
            QueryValue::Text(s) => write!(f, "{}", s),
            QueryValue::Bytes(bytes) => write!(f, "<{} bytes>", bytes.len()),
        }
    }
}

/// Result of a query execution (traditional row-based format)
///
/// For most operations, prefer using [`DataFrame`] methods instead.
/// This type is provided for compatibility and bulk operations.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryValue>>,
    pub rows_affected: Option<u64>,
}

impl QueryResult {
    /// Convert QueryResult to a Polars DataFrame
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        if self.rows.is_empty() {
            // Create empty DataFrame with column names
            let series: Vec<Series> = self
                .columns
                .iter()
                .map(|name| Series::new(name.as_str().into(), Vec::<i64>::new()))
                .collect();
            let columns: Vec<_> = series.into_iter().map(|s| s.into()).collect();
            return Ok(DataFrame::new(columns)?);
        }

        // Build series for each column
        let mut series_vec: Vec<Series> = Vec::new();

        for (col_idx, col_name) in self.columns.iter().enumerate() {
            // Collect values for this column from all rows
            let values: Vec<&QueryValue> = self.rows.iter().map(|row| &row[col_idx]).collect();

            // Determine the series type from the first non-null value
            let series = Self::values_to_series(col_name, &values)?;
            series_vec.push(series);
        }

        let columns: Vec<_> = series_vec.into_iter().map(|s| s.into()).collect();
        Ok(DataFrame::new(columns)?)
    }

    /// Helper to convert a column of QueryValues to a Series
    fn values_to_series(col_name: &str, values: &[&QueryValue]) -> Result<Series> {
        // Find first non-null to determine type
        let sample = values.iter().find(|v| !matches!(v, QueryValue::Null));

        match sample {
            Some(QueryValue::Bool(_)) => {
                let data: Vec<Option<bool>> = values
                    .iter()
                    .map(|v| match v {
                        QueryValue::Bool(b) => Some(*b),
                        QueryValue::Null => None,
                        _ => None, // Type mismatch
                    })
                    .collect();
                Ok(Series::new(col_name.into(), data))
            }
            Some(QueryValue::Int(_)) => {
                let data: Vec<Option<i64>> = values
                    .iter()
                    .map(|v| match v {
                        QueryValue::Int(i) => Some(*i),
                        QueryValue::Null => None,
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(col_name.into(), data))
            }
            Some(QueryValue::Float(_)) => {
                let data: Vec<Option<f64>> = values
                    .iter()
                    .map(|v| match v {
                        QueryValue::Float(f) => Some(*f),
                        QueryValue::Null => None,
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(col_name.into(), data))
            }
            Some(QueryValue::Text(_)) => {
                let data: Vec<Option<&str>> = values
                    .iter()
                    .map(|v| match v {
                        QueryValue::Text(s) => Some(s.as_str()),
                        QueryValue::Null => None,
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(col_name.into(), data))
            }
            Some(QueryValue::Bytes(_)) => {
                // For bytes, we'll use a binary type or string representation
                let data: Vec<Option<String>> = values
                    .iter()
                    .map(|v| match v {
                        QueryValue::Bytes(b) => Some(format!("<{} bytes>", b.len())),
                        QueryValue::Null => None,
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(col_name.into(), data))
            }
            None | Some(QueryValue::Null) => {
                // All nulls - create null string series
                let data: Vec<Option<&str>> = vec![None; values.len()];
                Ok(Series::new(col_name.into(), data))
            }
        }
    }
}

/// Schema information about a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<ColumnInfo>,
}

/// Column information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

/// Server information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    pub version: String,
    pub server_type: String,
    pub extra_info: HashMap<String, String>,
}

/// Database metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    pub name: String,
    pub size_bytes: Option<i64>,
    pub owner: Option<String>,
    pub encoding: Option<String>,
    pub created_at: Option<String>,
    pub extra_info: HashMap<String, String>,
}

/// Index information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub schema: Option<String>,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_type: Option<String>,
}

/// Foreign key information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub table_name: String,
    pub schema: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_schema: Option<String>,
    pub referenced_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

/// View information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub name: String,
    pub schema: Option<String>,
    pub definition: Option<String>,
}

/// Stored procedure/function information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureInfo {
    pub name: String,
    pub schema: Option<String>,
    pub return_type: Option<String>,
    pub language: Option<String>,
}

/// Enhanced table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub schema: Option<String>,
    pub size_bytes: Option<i64>,
    pub row_count: Option<i64>,
    pub created_at: Option<String>,
    pub table_type: Option<String>,
}

/// Main trait that all database adapters must implement
///
/// This trait provides a unified interface for database access with two data formats:
/// - **DataFrame-based**: Primary format using Polars DataFrames for efficient data manipulation
/// - **Row-based**: Traditional QueryResult format for compatibility and bulk operations
///
/// # Examples
///
/// ```ignore
/// use arni_data::adapter::{DbAdapter, ConnectionConfig};
///
/// async fn example(adapter: &mut impl DbAdapter) -> Result<()> {
///     let config = ConnectionConfig { /* ... */ };
///     adapter.connect(&config, Some("password")).await?;
///     
///     // Read data as DataFrame
///     let df = adapter.read_table("users", None).await?;
///     println!("Rows: {}", df.height());
///     
///     // Export DataFrame to database
///     adapter.export_dataframe(&df, "users_backup", None, false).await?;
///     
///     adapter.disconnect().await?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait DbAdapter: Send + Sync {
    // ===== Connection Management =====

    /// Connect to the database using the provided configuration and credentials
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<&str>) -> Result<()>;

    /// Disconnect from the database
    async fn disconnect(&mut self) -> Result<()>;

    /// Check if currently connected
    fn is_connected(&self) -> bool;

    /// Test the connection without fully connecting
    async fn test_connection(
        &self,
        config: &ConnectionConfig,
        password: Option<&str>,
    ) -> Result<bool>;

    /// Get the database type this adapter handles
    fn database_type(&self) -> DatabaseType;

    // ===== DataFrame Operations (Primary Interface) =====

    /// Read a table as a Polars DataFrame
    ///
    /// This is the primary method for reading data from the database.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to read
    /// * `schema` - Optional schema/database name
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let df = adapter.read_table("users", None).await?;
    /// println!("Loaded {} rows", df.height());
    /// ```
    async fn read_table(&self, table_name: &str, schema: Option<&str>) -> Result<DataFrame> {
        // Default implementation: SELECT * and convert to DataFrame
        let schema_prefix = schema.map(|s| format!("{}.", s)).unwrap_or_default();
        let query = format!("SELECT * FROM {}{}", schema_prefix, table_name);
        let result = self.execute_query(&query).await?;
        result.to_dataframe()
    }

    /// Export a Polars DataFrame to a database table
    ///
    /// This is the primary method for writing data to the database.
    ///
    /// # Arguments
    /// * `df` - The DataFrame to export
    /// * `table_name` - Name of the target table
    /// * `schema` - Optional schema/database name
    /// * `if_exists` - If true, replace existing table; if false, append
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let df = DataFrame::new(vec![
    ///     Series::new("id", &[1, 2, 3]),
    ///     Series::new("name", &["Alice", "Bob", "Charlie"]),
    /// ])?;
    /// adapter.export_dataframe(&df, "users", None, false).await?;
    /// ```
    async fn export_dataframe(
        &self,
        df: &DataFrame,
        table_name: &str,
        schema: Option<&str>,
        replace: bool,
    ) -> Result<u64>;

    /// Execute a SQL query and return results as a DataFrame
    ///
    /// For SELECT queries, returns the result set as a DataFrame.
    /// For other queries (INSERT, UPDATE, DELETE), returns an empty DataFrame.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let df = adapter.query_df("SELECT * FROM users WHERE age > 25").await?;
    /// ```
    async fn query_df(&self, query: &str) -> Result<DataFrame> {
        let result = self.execute_query(query).await?;
        result.to_dataframe()
    }

    // ===== Traditional Query Operations =====

    /// Execute a query and return results in traditional row format
    ///
    /// This method is provided for compatibility and internal use.
    /// For most operations, prefer DataFrame-based methods.
    async fn execute_query(&self, query: &str) -> Result<QueryResult>;

    // ===== Schema Discovery =====

    /// List all databases on the server
    async fn list_databases(&self) -> Result<Vec<String>>;

    /// List all tables in the current database
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>>;

    /// Get detailed information about a table
    async fn describe_table(&self, table_name: &str, schema: Option<&str>) -> Result<TableInfo>;

    // ===== Server & Database Introspection Methods =====

    /// Get server version and configuration information
    async fn get_server_info(&self) -> Result<ServerInfo> {
        // Default implementation returns basic info
        Ok(ServerInfo {
            version: "Unknown".to_string(),
            server_type: format!("{}", self.database_type()),
            extra_info: HashMap::new(),
        })
    }

    /// Get metadata about a specific database
    async fn get_database_metadata(&self, database_name: &str) -> Result<DatabaseMetadata> {
        // Default implementation returns minimal info
        Ok(DatabaseMetadata {
            name: database_name.to_string(),
            size_bytes: None,
            owner: None,
            encoding: None,
            created_at: None,
            extra_info: HashMap::new(),
        })
    }

    /// Get metadata about a specific table
    async fn get_table_metadata(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<TableMetadata> {
        // Default implementation returns minimal info
        Ok(TableMetadata {
            name: table_name.to_string(),
            schema: schema.map(|s| s.to_string()),
            size_bytes: None,
            row_count: None,
            created_at: None,
            table_type: None,
        })
    }

    /// Get all indexes for a table
    async fn get_indexes(
        &self,
        _table_name: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<IndexInfo>> {
        // Default implementation returns empty list
        Ok(Vec::new())
    }

    /// Get all foreign keys for a table
    async fn get_foreign_keys(
        &self,
        _table_name: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        // Default implementation returns empty list
        Ok(Vec::new())
    }

    /// List all views in a schema
    async fn get_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        // Default implementation returns empty list
        Ok(Vec::new())
    }

    /// Get view definition
    async fn get_view_definition(
        &self,
        _view_name: &str,
        _schema: Option<&str>,
    ) -> Result<Option<String>> {
        // Default implementation returns None
        Ok(None)
    }

    /// List all stored procedures/functions in a schema
    async fn list_stored_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // Default implementation returns empty list
        Ok(Vec::new())
    }

    // ===== Bulk Operations =====

    /// Bulk insert multiple rows efficiently
    ///
    /// For DataFrame-based bulk operations, use [`export_dataframe`](Self::export_dataframe) instead.
    ///
    /// Returns the number of rows inserted.
    async fn bulk_insert(
        &self,
        _table_name: &str,
        _columns: &[String],
        _rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(crate::DataError::NotSupported(
            "Bulk insert not implemented for this adapter".to_string(),
        ))
    }

    /// Bulk update multiple rows efficiently
    ///
    /// Returns the number of rows updated.
    async fn bulk_update(
        &self,
        _table_name: &str,
        _updates: &[(HashMap<String, QueryValue>, String)], // (column_values, where_clause)
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(crate::DataError::NotSupported(
            "Bulk update not implemented for this adapter".to_string(),
        ))
    }

    /// Bulk delete multiple rows efficiently
    ///
    /// Returns the number of rows deleted.
    async fn bulk_delete(
        &self,
        _table_name: &str,
        _where_clauses: &[String],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(crate::DataError::NotSupported(
            "Bulk delete not implemented for this adapter".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_type_default_ports() {
        assert_eq!(DatabaseType::Postgres.default_port(), Some(5432));
        assert_eq!(DatabaseType::MySQL.default_port(), Some(3306));
        assert_eq!(DatabaseType::SQLite.default_port(), None);
        assert_eq!(DatabaseType::MongoDB.default_port(), Some(27017));
        assert_eq!(DatabaseType::SQLServer.default_port(), Some(1433));
        assert_eq!(DatabaseType::Oracle.default_port(), Some(1521));
        assert_eq!(DatabaseType::DuckDB.default_port(), None);
    }

    #[test]
    fn test_database_type_display() {
        assert_eq!(format!("{}", DatabaseType::Postgres), "PostgreSQL");
        assert_eq!(format!("{}", DatabaseType::MySQL), "MySQL");
        assert_eq!(format!("{}", DatabaseType::SQLite), "SQLite");
        assert_eq!(format!("{}", DatabaseType::MongoDB), "MongoDB");
        assert_eq!(format!("{}", DatabaseType::SQLServer), "SQL Server");
        assert_eq!(format!("{}", DatabaseType::Oracle), "Oracle");
        assert_eq!(format!("{}", DatabaseType::DuckDB), "DuckDB");
    }

    #[test]
    fn test_query_value_display() {
        assert_eq!(format!("{}", QueryValue::Null), "NULL");
        assert_eq!(format!("{}", QueryValue::Bool(true)), "true");
        assert_eq!(format!("{}", QueryValue::Int(42)), "42");
        assert_eq!(format!("{}", QueryValue::Float(3.14)), "3.14");
        assert_eq!(format!("{}", QueryValue::Text("hello".to_string())), "hello");
        assert_eq!(format!("{}", QueryValue::Bytes(vec![1, 2, 3])), "<3 bytes>");
    }

    #[test]
    fn test_query_value_equality() {
        assert_eq!(QueryValue::Null, QueryValue::Null);
        assert_eq!(QueryValue::Bool(true), QueryValue::Bool(true));
        assert_eq!(QueryValue::Int(42), QueryValue::Int(42));
        assert_eq!(QueryValue::Float(3.14), QueryValue::Float(3.14));
        assert_eq!(
            QueryValue::Text("hello".to_string()),
            QueryValue::Text("hello".to_string())
        );
        assert_eq!(QueryValue::Bytes(vec![1, 2, 3]), QueryValue::Bytes(vec![1, 2, 3]));
    }

    #[test]
    fn test_connection_config_creation() {
        let config = ConnectionConfig {
            id: "test-connection".to_string(),
            name: "Test Database".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "test_db".to_string(),
            username: Some("test_user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        assert_eq!(config.id, "test-connection");
        assert_eq!(config.name, "Test Database");
        assert_eq!(config.db_type, DatabaseType::Postgres);
        assert_eq!(config.host, Some("localhost".to_string()));
        assert_eq!(config.port, Some(5432));
        assert_eq!(config.database, "test_db");
        assert_eq!(config.username, Some("test_user".to_string()));
        assert!(!config.use_ssl);
    }

    #[test]
    fn test_query_result_to_dataframe_empty() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![],
            rows_affected: Some(0),
        };

        let df = result.to_dataframe().unwrap();
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_query_result_to_dataframe_with_data() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![QueryValue::Int(1), QueryValue::Text("Alice".to_string())],
                vec![QueryValue::Int(2), QueryValue::Text("Bob".to_string())],
                vec![QueryValue::Int(3), QueryValue::Text("Charlie".to_string())],
            ],
            rows_affected: Some(3),
        };

        let df = result.to_dataframe().unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
        assert_eq!(df.get_column_names(), vec!["id", "name"]);
    }

    #[test]
    fn test_query_result_to_dataframe_with_nulls() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "value".to_string()],
            rows: vec![
                vec![QueryValue::Int(1), QueryValue::Null],
                vec![QueryValue::Int(2), QueryValue::Int(42)],
                vec![QueryValue::Int(3), QueryValue::Null],
            ],
            rows_affected: Some(3),
        };

        let df = result.to_dataframe().unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_query_result_to_dataframe_different_types() {
        let result = QueryResult {
            columns: vec![
                "id".to_string(),
                "active".to_string(),
                "score".to_string(),
                "name".to_string(),
            ],
            rows: vec![
                vec![
                    QueryValue::Int(1),
                    QueryValue::Bool(true),
                    QueryValue::Float(98.5),
                    QueryValue::Text("Alice".to_string()),
                ],
                vec![
                    QueryValue::Int(2),
                    QueryValue::Bool(false),
                    QueryValue::Float(87.3),
                    QueryValue::Text("Bob".to_string()),
                ],
            ],
            rows_affected: Some(2),
        };

        let df = result.to_dataframe().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 4);
    }
}
