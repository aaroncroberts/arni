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
    #[serde(default)]
    pub parameters: HashMap<String, String>,
}

/// Supported database types
/// Controls how [`DbAdapter::find_tables`] matches table names against a search pattern.
///
/// In all modes the pattern is matched **literally** — characters with special meaning
/// in SQL `LIKE` expressions (underscore `_` and percent `%`) are automatically escaped
/// so that, for example, searching for `"PS_"` finds tables whose names contain the
/// exact two characters `P`, `S`, `_`, not tables whose names match the SQL wildcard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableSearchMode {
    /// Table name **starts with** the pattern (e.g. `"PS_"` → `LIKE 'PS\_%'`)
    StartsWith,
    /// Table name **contains** the pattern anywhere (e.g. `"PS_"` → `LIKE '%PS\_%'`)
    Contains,
    /// Table name **ends with** the pattern (e.g. `"PS_"` → `LIKE '%PS\_'`)
    EndsWith,
}

/// Escape a user-supplied search pattern so that it is safe to embed in a SQL
/// `LIKE` expression using backslash as the escape character (`ESCAPE '\'`).
///
/// Both `_` (single-character wildcard) and `%` (multi-character wildcard) are
/// prefixed with `\` so they are treated as literal characters by the database.
///
/// # Examples
/// ```
/// use arni_data::adapter::escape_like_pattern;
/// assert_eq!(escape_like_pattern("PS_"), "PS\\_");
/// assert_eq!(escape_like_pattern("50%"), "50\\%");
/// assert_eq!(escape_like_pattern("plain"), "plain");
/// ```
pub fn escape_like_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 4);
    for ch in pattern.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '_' => out.push_str("\\_"),
            '%' => out.push_str("\\%"),
            c => out.push(c),
        }
    }
    out
}

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

/// Adapter-agnostic filter expression for `bulk_update` and `bulk_delete` operations.
///
/// Each adapter translates `FilterExpr` into its native query language:
/// SQL `WHERE` clauses for relational databases, BSON documents for MongoDB, etc.
///
/// # Examples
///
/// ```ignore
/// use arni_data::{FilterExpr, QueryValue};
///
/// // id = 42
/// let f = FilterExpr::Eq("id".to_string(), QueryValue::Int(42));
///
/// // status = 'active' AND score >= 80
/// let f = FilterExpr::And(vec![
///     FilterExpr::Eq("status".to_string(), QueryValue::Text("active".to_string())),
///     FilterExpr::Gte("score".to_string(), QueryValue::Int(80)),
/// ]);
/// ```
#[derive(Debug, Clone)]
pub enum FilterExpr {
    /// `col = value`
    Eq(String, QueryValue),
    /// `col <> value`
    Ne(String, QueryValue),
    /// `col > value`
    Gt(String, QueryValue),
    /// `col >= value`
    Gte(String, QueryValue),
    /// `col < value`
    Lt(String, QueryValue),
    /// `col <= value`
    Lte(String, QueryValue),
    /// `col IN (v1, v2, …)`
    In(String, Vec<QueryValue>),
    /// `col IS NULL`
    IsNull(String),
    /// `col IS NOT NULL`
    IsNotNull(String),
    /// `(expr1 AND expr2 AND …)` — empty list renders as `1=1` (always true)
    And(Vec<FilterExpr>),
    /// `(expr1 OR expr2 OR …)` — empty list renders as `1=0` (always false)
    Or(Vec<FilterExpr>),
    /// `NOT (expr)`
    Not(Box<FilterExpr>),
}

/// Render a [`QueryValue`] as a SQL literal safe for inline embedding.
///
/// - Strings are single-quoted with internal `'` escaped as `''`.
/// - Booleans render as `TRUE` / `FALSE` (SQL standard, supported by all SQL adapters).
/// - NaN / infinite floats map to `NULL`.
/// - Byte arrays render as a hex literal `X'...'`.
pub fn query_value_to_sql_literal(value: &QueryValue) -> String {
    match value {
        QueryValue::Null => "NULL".to_string(),
        QueryValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        QueryValue::Int(i) => i.to_string(),
        QueryValue::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                "NULL".to_string()
            } else {
                format!("{}", f)
            }
        }
        QueryValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
        QueryValue::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("X'{}'", hex)
        }
    }
}

/// Render a [`FilterExpr`] as a SQL `WHERE`-clause fragment.
///
/// The output is self-contained (no bind parameters) and can be embedded directly
/// in any SQL `WHERE` clause across all SQL-based adapters.
///
/// # Example
///
/// ```ignore
/// let sql = format!("DELETE FROM users WHERE {}", filter_to_sql(&filter));
/// ```
pub fn filter_to_sql(expr: &FilterExpr) -> String {
    match expr {
        FilterExpr::Eq(col, val) => format!("{} = {}", col, query_value_to_sql_literal(val)),
        FilterExpr::Ne(col, val) => format!("{} <> {}", col, query_value_to_sql_literal(val)),
        FilterExpr::Gt(col, val) => format!("{} > {}", col, query_value_to_sql_literal(val)),
        FilterExpr::Gte(col, val) => format!("{} >= {}", col, query_value_to_sql_literal(val)),
        FilterExpr::Lt(col, val) => format!("{} < {}", col, query_value_to_sql_literal(val)),
        FilterExpr::Lte(col, val) => format!("{} <= {}", col, query_value_to_sql_literal(val)),
        FilterExpr::In(col, vals) => {
            let literals: Vec<String> = vals.iter().map(query_value_to_sql_literal).collect();
            format!("{} IN ({})", col, literals.join(", "))
        }
        FilterExpr::IsNull(col) => format!("{} IS NULL", col),
        FilterExpr::IsNotNull(col) => format!("{} IS NOT NULL", col),
        FilterExpr::And(exprs) => {
            if exprs.is_empty() {
                "1=1".to_string()
            } else {
                let parts: Vec<String> = exprs.iter().map(filter_to_sql).collect();
                format!("({})", parts.join(" AND "))
            }
        }
        FilterExpr::Or(exprs) => {
            if exprs.is_empty() {
                "1=0".to_string()
            } else {
                let parts: Vec<String> = exprs.iter().map(filter_to_sql).collect();
                format!("({})", parts.join(" OR "))
            }
        }
        FilterExpr::Not(expr) => format!("NOT ({})", filter_to_sql(expr)),
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
    /// Approximate row count (may be None if unavailable or not yet analyzed)
    pub row_count: Option<i64>,
    /// Total on-disk size in bytes including indexes (None for in-memory or unsupported)
    pub size_bytes: Option<i64>,
    /// Table creation timestamp as an ISO-8601 string (None if the DB does not track it)
    pub created_at: Option<String>,
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

/// Trait for managing database connection lifecycle
///
/// This trait provides a focused interface for connection management, separate from
/// the higher-level [`DbAdapter`] trait. Implementations handle the low-level details
/// of establishing, maintaining, and closing database connections.
///
/// # Lifecycle
///
/// 1. **Connection**: Call [`connect()`](Connection::connect) to establish a connection
/// 2. **Validation**: Use [`health_check()`](Connection::health_check) to verify the connection is working
/// 3. **Usage**: The connection is ready for database operations
/// 4. **Disconnection**: Call [`disconnect()`](Connection::disconnect) when done
///
/// # Health Checks
///
/// The [`health_check()`](Connection::health_check) method should verify the connection is:
/// - Active and responsive
/// - Capable of executing queries
/// - Within acceptable latency bounds
///
/// # Examples
///
/// ```ignore
/// use arni_data::adapter::{Connection, ConnectionConfig, DatabaseType};
///
/// async fn example(mut conn: impl Connection) -> Result<()> {
///     // Establish connection
///     conn.connect().await?;
///     
///     // Verify it's working
///     if conn.is_connected() {
///         let healthy = conn.health_check().await?;
///         println!("Connection healthy: {}", healthy);
///     }
///     
///     // Clean up
///     conn.disconnect().await?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait Connection: Send + Sync {
    /// Establish a connection to the database
    ///
    /// This method should:
    /// - Validate the connection configuration
    /// - Establish network connection (if applicable)
    /// - Authenticate with provided credentials
    /// - Prepare the connection for queries
    ///
    /// # Errors
    ///
    /// Returns [`DataError::Connection`] if:
    /// - Configuration is invalid
    /// - Network connection fails
    /// - Authentication fails
    /// - Database is unreachable
    async fn connect(&mut self) -> Result<()>;

    /// Close the database connection
    ///
    /// This method should:
    /// - Gracefully close the connection
    /// - Release any resources
    /// - Clean up internal state
    ///
    /// Calling disconnect on an already-closed connection should be a no-op.
    async fn disconnect(&mut self) -> Result<()>;

    /// Check if the connection is currently active
    ///
    /// Returns `true` if connected, `false` otherwise.
    /// This is a fast, non-blocking check of internal state.
    fn is_connected(&self) -> bool;

    /// Perform a health check on the connection
    ///
    /// This method should:
    /// - Execute a lightweight query (e.g., `SELECT 1`)
    /// - Verify the response is received within acceptable time
    /// - Return `true` if the connection is healthy
    ///
    /// # Errors
    ///
    /// Returns [`DataError::Connection`] if:
    /// - The connection is closed
    /// - The query fails
    /// - The response times out
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if conn.health_check().await? {
    ///     println!("Connection is healthy");
    /// } else {
    ///     println!("Connection needs attention");
    /// }
    /// ```
    async fn health_check(&self) -> Result<bool>;

    /// Get the connection configuration
    ///
    /// Returns a reference to the configuration used to establish this connection.
    fn config(&self) -> &ConnectionConfig;
}

/// Accessor for database metadata operations
///
/// This struct provides organized access to all metadata-related operations
/// on a database adapter. It groups methods for introspecting:
/// - Server and database information
/// - Table structure and constraints
/// - Indexes and foreign keys
/// - Views and stored procedures
///
/// # Design Pattern
///
/// This struct follows the adapter pattern for metadata organization,
/// separating metadata operations from CRUD operations for better code organization.
/// Inspired by the skidbladnir-data architecture.
///
/// # Examples
///
/// ```ignore
/// // Access metadata through the adapter
/// let databases = adapter.metadata().list_databases().await?;
/// let tables = adapter.metadata().list_tables(None).await?;
/// let indexes = adapter.metadata().get_indexes("users", None).await?;
/// ```
pub struct AdapterMetadata<'a> {
    adapter: &'a dyn DbAdapter,
}

impl<'a> AdapterMetadata<'a> {
    /// Create a new metadata accessor wrapping an adapter reference
    pub fn new(adapter: &'a dyn DbAdapter) -> Self {
        Self { adapter }
    }

    /// List all databases on the server
    pub async fn list_databases(&self) -> Result<Vec<String>> {
        self.adapter.list_databases().await
    }

    /// List all tables in the current database or schema
    pub async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        self.adapter.list_tables(schema).await
    }

    /// Get detailed information about a table
    pub async fn describe_table(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<TableInfo> {
        self.adapter.describe_table(table_name, schema).await
    }

    /// Get server version and configuration information
    pub async fn get_server_info(&self) -> Result<ServerInfo> {
        self.adapter.get_server_info().await
    }

    /// Get all indexes for a table
    pub async fn get_indexes(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<Vec<IndexInfo>> {
        self.adapter.get_indexes(table_name, schema).await
    }

    /// Get all foreign keys for a table
    pub async fn get_foreign_keys(
        &self,
        table_name: &str,
        schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>> {
        self.adapter.get_foreign_keys(table_name, schema).await
    }

    /// List all views in a schema
    pub async fn get_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        self.adapter.get_views(schema).await
    }

    /// Get view definition SQL
    pub async fn get_view_definition(
        &self,
        view_name: &str,
        schema: Option<&str>,
    ) -> Result<Option<String>> {
        self.adapter.get_view_definition(view_name, schema).await
    }

    /// List all stored procedures/functions in a schema
    pub async fn list_stored_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        self.adapter.list_stored_procedures(schema).await
    }

    /// Search for tables whose names start with, contain, or end with `pattern`.
    ///
    /// The `pattern` is matched **literally**: SQL wildcard characters (`_` and `%`)
    /// in the pattern are treated as ordinary characters, not wildcards.
    ///
    /// # Arguments
    /// * `pattern` – the string fragment to search for (e.g. `"PS_"`)
    /// * `schema`  – optional schema/owner filter (same as [`list_tables`](Self::list_tables))
    /// * `mode`    – controls whether to match at the start, anywhere, or at the end
    ///
    /// # Examples
    /// ```no_run
    /// # async fn example(adapter: &dyn arni_data::DbAdapter) -> arni_data::Result<()> {
    /// use arni_data::adapter::TableSearchMode;
    /// let metadata = adapter.metadata();
    /// // find all tables whose name starts with the literal string "PS_"
    /// let tables = metadata.find_tables("PS_", None, TableSearchMode::StartsWith).await?;
    /// # Ok(()) }
    /// ```
    pub async fn find_tables(
        &self,
        pattern: &str,
        schema: Option<&str>,
        mode: TableSearchMode,
    ) -> Result<Vec<String>> {
        self.adapter.find_tables(pattern, schema, mode).await
    }
}

/// Main trait that all database adapters must implement
///
/// This trait provides a unified interface for database access with two data formats:
/// - **DataFrame-based**: Primary format using Polars DataFrames for efficient data manipulation
/// - **Row-based**: Traditional QueryResult format for compatibility and bulk operations
///
/// # Metadata Access
///
/// Use the [`metadata()`](Self::metadata) method to access organized metadata operations:
/// ```ignore
/// let databases = adapter.metadata().list_databases().await?;
/// let indexes = adapter.metadata().get_indexes("users", None).await?;
/// ```
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
///     // Access metadata
///     let tables = adapter.metadata().list_tables(None).await?;
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

    /// Access metadata operations
    ///
    /// Returns an [`AdapterMetadata`] accessor that provides organized access to
    /// database introspection methods: server info, schemas, tables, indexes, views, etc.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // List all databases
    /// let databases = adapter.metadata().list_databases().await?;
    ///
    /// // Get table structure
    /// let table_info = adapter.metadata().describe_table("users", None).await?;
    ///
    /// // Inspect indexes
    /// let indexes = adapter.metadata().get_indexes("users", None).await?;
    /// ```
    fn metadata(&self) -> AdapterMetadata<'_>;

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

    /// Search for tables by name fragment.
    ///
    /// See [`AdapterMetadata::find_tables`] for the full contract and examples.
    /// Adapters should escape `pattern` with [`escape_like_pattern`] before
    /// embedding it in a `LIKE` expression.
    async fn find_tables(
        &self,
        _pattern: &str,
        _schema: Option<&str>,
        _mode: TableSearchMode,
    ) -> Result<Vec<String>> {
        Err(crate::DataError::NotSupported(
            "find_tables not implemented for this adapter".to_string(),
        ))
    }

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
    /// Each entry in `updates` is `(column_values, filter)` where `filter` selects the rows
    /// to update. The adapter translates [`FilterExpr`] to its native query language.
    ///
    /// Returns the total number of rows updated.
    async fn bulk_update(
        &self,
        _table_name: &str,
        _updates: &[(HashMap<String, QueryValue>, FilterExpr)],
        _schema: Option<&str>,
    ) -> Result<u64> {
        Err(crate::DataError::NotSupported(
            "Bulk update not implemented for this adapter".to_string(),
        ))
    }

    /// Bulk delete multiple rows efficiently
    ///
    /// Each entry in `filters` selects a set of rows to delete. The adapter translates
    /// each [`FilterExpr`] to its native query language.
    ///
    /// Returns the total number of rows deleted.
    async fn bulk_delete(
        &self,
        _table_name: &str,
        _filters: &[FilterExpr],
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
        assert_eq!(
            format!("{}", QueryValue::Text("hello".to_string())),
            "hello"
        );
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
        assert_eq!(
            QueryValue::Bytes(vec![1, 2, 3]),
            QueryValue::Bytes(vec![1, 2, 3])
        );
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

    // ===== Metadata Info Struct Tests =====

    #[test]
    fn test_table_info_construction() {
        let col = ColumnInfo {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            default_value: None,
            is_primary_key: true,
        };
        let table = TableInfo {
            name: "users".to_string(),
            schema: Some("public".to_string()),
            columns: vec![col.clone()],
            row_count: Some(42),
            size_bytes: Some(8192),
            created_at: Some("2024-01-01T00:00:00".to_string()),
        };
        assert_eq!(table.name, "users");
        assert_eq!(table.schema.as_deref(), Some("public"));
        assert_eq!(table.columns.len(), 1);
        assert_eq!(table.columns[0].name, "id");
        assert!(table.columns[0].is_primary_key);
        // Verify Clone and Debug work
        let cloned = table.clone();
        assert_eq!(cloned.name, table.name);
        assert!(!format!("{:?}", table).is_empty());
    }

    #[test]
    fn test_column_info_defaults() {
        let col = ColumnInfo {
            name: "email".to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            default_value: Some("''".to_string()),
            is_primary_key: false,
        };
        assert_eq!(col.name, "email");
        assert!(col.nullable);
        assert_eq!(col.default_value.as_deref(), Some("''"));
        assert!(!col.is_primary_key);
        let cloned = col.clone();
        assert_eq!(cloned.data_type, col.data_type);
    }

    #[test]
    fn test_server_info_construction() {
        let mut extra = HashMap::new();
        extra.insert("max_connections".to_string(), "100".to_string());
        let info = ServerInfo {
            version: "16.0".to_string(),
            server_type: "PostgreSQL".to_string(),
            extra_info: extra,
        };
        assert_eq!(info.version, "16.0");
        assert_eq!(info.extra_info.get("max_connections").unwrap(), "100");
        let cloned = info.clone();
        assert_eq!(cloned.server_type, info.server_type);
    }

    #[test]
    fn test_index_info_construction() {
        let idx = IndexInfo {
            name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            schema: Some("public".to_string()),
            columns: vec!["email".to_string()],
            is_unique: true,
            is_primary: false,
            index_type: Some("BTREE".to_string()),
        };
        assert_eq!(idx.name, "idx_users_email");
        assert!(idx.is_unique);
        assert!(!idx.is_primary);
        assert_eq!(idx.columns.len(), 1);
        let cloned = idx.clone();
        assert_eq!(cloned.table_name, idx.table_name);
    }

    #[test]
    fn test_foreign_key_info_construction() {
        let fk = ForeignKeyInfo {
            name: "fk_orders_user".to_string(),
            table_name: "orders".to_string(),
            schema: None,
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        };
        assert_eq!(fk.referenced_table, "users");
        assert_eq!(fk.on_delete.as_deref(), Some("CASCADE"));
        assert!(fk.on_update.is_none());
        assert!(!format!("{:?}", fk).is_empty());
    }

    #[test]
    fn test_view_info_construction() {
        let view = ViewInfo {
            name: "active_users".to_string(),
            schema: Some("public".to_string()),
            definition: Some("SELECT * FROM users WHERE active = true".to_string()),
        };
        assert_eq!(view.name, "active_users");
        assert!(view.definition.is_some());
        let cloned = view.clone();
        assert_eq!(cloned.schema, view.schema);
    }

    #[test]
    fn test_procedure_info_construction() {
        let proc = ProcedureInfo {
            name: "get_user_count".to_string(),
            schema: Some("public".to_string()),
            return_type: Some("INTEGER".to_string()),
            language: Some("plpgsql".to_string()),
        };
        assert_eq!(proc.name, "get_user_count");
        assert_eq!(proc.language.as_deref(), Some("plpgsql"));
        assert!(!format!("{:?}", proc).is_empty());
    }

    #[test]
    fn test_connection_config_clone_and_debug() {
        let config = ConnectionConfig {
            id: "pg-dev".to_string(),
            name: "Dev Postgres".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "mydb".to_string(),
            username: Some("user".to_string()),
            use_ssl: true,
            parameters: HashMap::new(),
        };
        let cloned = config.clone();
        assert_eq!(cloned.id, config.id);
        assert_eq!(cloned.use_ssl, config.use_ssl);
        assert!(!format!("{:?}", config).is_empty());
    }

    #[test]
    fn test_query_result_rows_affected() {
        let result = QueryResult {
            columns: vec!["id".to_string()],
            rows: vec![],
            rows_affected: Some(5),
        };
        assert_eq!(result.rows_affected, Some(5));
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_query_value_clone_and_debug() {
        let values = vec![
            QueryValue::Null,
            QueryValue::Bool(false),
            QueryValue::Int(-1),
            QueryValue::Float(0.0),
            QueryValue::Text(String::new()),
            QueryValue::Bytes(vec![]),
        ];
        for v in &values {
            let cloned = v.clone();
            assert_eq!(&cloned, v);
            assert!(!format!("{:?}", v).is_empty());
        }
    }

    // ===== Connection Trait Tests =====

    /// Mock connection for testing the Connection trait
    struct MockConnection {
        config: ConnectionConfig,
        connected: bool,
        fail_health_check: bool,
    }

    impl MockConnection {
        fn new(config: ConnectionConfig) -> Self {
            Self {
                config,
                connected: false,
                fail_health_check: false,
            }
        }

        #[allow(dead_code)]
        fn set_fail_health_check(&mut self, fail: bool) {
            self.fail_health_check = fail;
        }
    }

    #[async_trait]
    impl Connection for MockConnection {
        async fn connect(&mut self) -> Result<()> {
            if self.connected {
                return Err(crate::DataError::Connection(
                    "Already connected".to_string(),
                ));
            }
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            self.connected = false;
            Ok(())
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        async fn health_check(&self) -> Result<bool> {
            if !self.connected {
                return Err(crate::DataError::Connection("Not connected".to_string()));
            }
            if self.fail_health_check {
                return Ok(false);
            }
            Ok(true)
        }

        fn config(&self) -> &ConnectionConfig {
            &self.config
        }
    }

    #[tokio::test]
    async fn test_connection_lifecycle() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "test".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let mut conn = MockConnection::new(config);

        // Initially not connected
        assert!(!conn.is_connected());

        // Connect
        conn.connect().await.unwrap();
        assert!(conn.is_connected());

        // Health check should pass
        assert!(conn.health_check().await.unwrap());

        // Disconnect
        conn.disconnect().await.unwrap();
        assert!(!conn.is_connected());
    }

    #[tokio::test]
    async fn test_connection_double_connect() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "test".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let mut conn = MockConnection::new(config);

        // First connect succeeds
        conn.connect().await.unwrap();

        // Second connect should fail
        let result = conn.connect().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connection_health_check_not_connected() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "test".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let conn = MockConnection::new(config);

        // Health check on disconnected connection should error
        let result = conn.health_check().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connection_config_access() {
        let config = ConnectionConfig {
            id: "test-id".to_string(),
            name: "Test Name".to_string(),
            db_type: DatabaseType::MySQL,
            host: Some("db.example.com".to_string()),
            port: Some(3306),
            database: "mydb".to_string(),
            username: Some("admin".to_string()),
            use_ssl: true,
            parameters: HashMap::new(),
        };

        let conn = MockConnection::new(config.clone());

        // Should be able to access config
        let retrieved_config = conn.config();
        assert_eq!(retrieved_config.id, "test-id");
        assert_eq!(retrieved_config.name, "Test Name");
        assert_eq!(retrieved_config.db_type, DatabaseType::MySQL);
        assert_eq!(retrieved_config.host, Some("db.example.com".to_string()));
        assert_eq!(retrieved_config.port, Some(3306));
        assert_eq!(retrieved_config.database, "mydb");
        assert_eq!(retrieved_config.username, Some("admin".to_string()));
        assert!(retrieved_config.use_ssl);
    }

    #[tokio::test]
    async fn test_connection_disconnect_idempotent() {
        let config = ConnectionConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "test".to_string(),
            username: Some("user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };

        let mut conn = MockConnection::new(config);

        // Disconnect when not connected should be no-op
        conn.disconnect().await.unwrap();
        assert!(!conn.is_connected());

        // Connect then disconnect
        conn.connect().await.unwrap();
        conn.disconnect().await.unwrap();
        assert!(!conn.is_connected());

        // Disconnect again should be no-op
        conn.disconnect().await.unwrap();
        assert!(!conn.is_connected());
    }

    #[test]
    fn test_query_value_to_sql_literal() {
        assert_eq!(query_value_to_sql_literal(&QueryValue::Null), "NULL");
        assert_eq!(query_value_to_sql_literal(&QueryValue::Bool(true)), "TRUE");
        assert_eq!(
            query_value_to_sql_literal(&QueryValue::Bool(false)),
            "FALSE"
        );
        assert_eq!(query_value_to_sql_literal(&QueryValue::Int(42)), "42");
        assert_eq!(query_value_to_sql_literal(&QueryValue::Float(3.14)), "3.14");
        assert_eq!(
            query_value_to_sql_literal(&QueryValue::Text("hello".to_string())),
            "'hello'"
        );
        // Single quotes inside strings are escaped
        assert_eq!(
            query_value_to_sql_literal(&QueryValue::Text("it's".to_string())),
            "'it''s'"
        );
        // NaN becomes NULL
        assert_eq!(
            query_value_to_sql_literal(&QueryValue::Float(f64::NAN)),
            "NULL"
        );
    }

    #[test]
    fn test_filter_to_sql_simple() {
        assert_eq!(
            filter_to_sql(&FilterExpr::Eq("id".to_string(), QueryValue::Int(5))),
            "id = 5"
        );
        assert_eq!(
            filter_to_sql(&FilterExpr::Ne(
                "status".to_string(),
                QueryValue::Text("x".to_string())
            )),
            "status <> 'x'"
        );
        assert_eq!(
            filter_to_sql(&FilterExpr::IsNull("email".to_string())),
            "email IS NULL"
        );
        assert_eq!(
            filter_to_sql(&FilterExpr::IsNotNull("email".to_string())),
            "email IS NOT NULL"
        );
    }

    #[test]
    fn test_filter_to_sql_in() {
        let f = FilterExpr::In(
            "id".to_string(),
            vec![QueryValue::Int(1), QueryValue::Int(2), QueryValue::Int(3)],
        );
        assert_eq!(filter_to_sql(&f), "id IN (1, 2, 3)");
    }

    #[test]
    fn test_filter_to_sql_compound() {
        let f = FilterExpr::And(vec![
            FilterExpr::Gt("score".to_string(), QueryValue::Int(0)),
            FilterExpr::Lte("score".to_string(), QueryValue::Int(100)),
        ]);
        assert_eq!(filter_to_sql(&f), "(score > 0 AND score <= 100)");

        let f = FilterExpr::Or(vec![
            FilterExpr::Eq("a".to_string(), QueryValue::Bool(true)),
            FilterExpr::Eq("b".to_string(), QueryValue::Bool(true)),
        ]);
        assert_eq!(filter_to_sql(&f), "(a = TRUE OR b = TRUE)");

        let f = FilterExpr::Not(Box::new(FilterExpr::IsNull("x".to_string())));
        assert_eq!(filter_to_sql(&f), "NOT (x IS NULL)");
    }

    #[test]
    fn test_filter_to_sql_empty_and_or() {
        assert_eq!(filter_to_sql(&FilterExpr::And(vec![])), "1=1");
        assert_eq!(filter_to_sql(&FilterExpr::Or(vec![])), "1=0");
    }

    // ── escape_like_pattern ────────────────────────────────────────────────────

    #[test]
    fn test_escape_like_plain_string_unchanged() {
        assert_eq!(escape_like_pattern("hello"), "hello");
        assert_eq!(escape_like_pattern("TABLE_NAME"), "TABLE\\_NAME");
    }

    #[test]
    fn test_escape_like_underscore_escaped() {
        // "PS_" must become "PS\_" so SQL LIKE treats _ as a literal character
        assert_eq!(escape_like_pattern("PS_"), "PS\\_");
    }

    #[test]
    fn test_escape_like_percent_escaped() {
        assert_eq!(escape_like_pattern("50%"), "50\\%");
    }

    #[test]
    fn test_escape_like_mixed_pattern() {
        // "PS_%_data" -> "PS\_%\_data"
        assert_eq!(escape_like_pattern("PS_%_data"), "PS\\_\\%\\_data");
    }

    #[test]
    fn test_escape_like_backslash_escaped() {
        // An existing backslash must also be escaped so it doesn't become a spurious escape char
        assert_eq!(escape_like_pattern("a\\b"), "a\\\\b");
    }

    #[test]
    fn test_escape_like_empty_string() {
        assert_eq!(escape_like_pattern(""), "");
    }

    #[test]
    fn test_escape_like_multiple_underscores() {
        assert_eq!(escape_like_pattern("__"), "\\_\\_");
    }

    // ── TableSearchMode ────────────────────────────────────────────────────────

    #[test]
    fn test_table_search_mode_debug() {
        assert_eq!(format!("{:?}", TableSearchMode::StartsWith), "StartsWith");
        assert_eq!(format!("{:?}", TableSearchMode::Contains), "Contains");
        assert_eq!(format!("{:?}", TableSearchMode::EndsWith), "EndsWith");
    }

    #[test]
    fn test_table_search_mode_eq() {
        assert_eq!(TableSearchMode::StartsWith, TableSearchMode::StartsWith);
        assert_ne!(TableSearchMode::StartsWith, TableSearchMode::Contains);
    }
}
