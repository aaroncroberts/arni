//! Arni Data - Unified database access with Polars DataFrames
//!
//! This crate provides a consistent interface for accessing multiple database systems
//! using Polars DataFrames as the primary data interchange format.
//!
//! # Features
//!
//! - **Unified Interface**: Single [`DbAdapter`] trait for all databases
//! - **DataFrame-First**: Polars DataFrames as the primary data format
//! - **Multiple Databases**: PostgreSQL, MySQL, SQLite, MongoDB, SQL Server, Oracle, DuckDB
//! - **Type Safety**: Strong typing with compile-time guarantees
//! - **Async-First**: All I/O operations are async using Tokio
//! - **Schema Discovery**: Comprehensive metadata and introspection capabilities
//!
//! # Quick Start
//!
//! ```ignore
//! use arni::adapter::{DbAdapter, ConnectionConfig, DatabaseType};
//! use polars::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create connection config
//!     let config = ConnectionConfig {
//!         id: "my-db".to_string(),
//!         name: "My Database".to_string(),
//!         db_type: DatabaseType::Postgres,
//!         host: Some("localhost".to_string()),
//!         port: Some(5432),
//!         database: "mydb".to_string(),
//!         username: Some("user".to_string()),
//!         use_ssl: false,
//!         parameters: std::collections::HashMap::new(),
//!     };
//!     
//!     // Connect to database (adapter implementation needed)
//!     // let mut adapter = PostgresAdapter::new();
//!     // adapter.connect(&config, Some("password")).await?;
//!     
//!     // Read table as DataFrame
//!     // let df = adapter.read_table("users", None).await?;
//!     // println!("Loaded {} rows", df.height());
//!     
//!     // Export DataFrame to database
//!     // adapter.export_dataframe(&df, "users_backup", None, false).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! # Architecture
//!
//! The crate is organized around the [`DbAdapter`] trait, which defines the contract
//! for all database adapters. Each database type has its own adapter implementation.
//!
//! ## Core Components
//!
//! - [`adapter`]: Database adapter trait and configuration types
//! - [`error`]: Error types for database operations
//!
//! ## Data Flow
//!
//! 1. **Reading**: Database → QueryResult → DataFrame
//! 2. **Writing**: DataFrame → QueryValue rows → Database
//! 3. **Querying**: SQL → QueryResult → DataFrame (optional)
//!
//! # Examples
//!
//! ## Reading Data
//!
//! ```ignore
//! // Read entire table
//! let df = adapter.read_table("users", None).await?;
//!
//! // Execute custom query
//! let df = adapter.query_df("SELECT * FROM users WHERE age > 25").await?;
//! ```
//!
//! ## Writing Data
//!
//! ```ignore
//! use polars::prelude::*;
//!
//! // Create DataFrame
//! let df = DataFrame::new(vec![
//!     Series::new("id", &[1, 2, 3]),
//!     Series::new("name", &["Alice", "Bob", "Charlie"]),
//!     Series::new("age", &[30, 25, 35]),
//! ])?;
//!
//! // Export to database (append mode)
//! adapter.export_dataframe(&df, "users", None, false).await?;
//!
//! // Export to database (replace mode)
//! adapter.export_dataframe(&df, "users", None, true).await?;
//! ```
//!
//! ## Schema Discovery
//!
//! ```ignore
//! // List databases
//! let databases = adapter.list_databases().await?;
//!
//! // List tables
//! let tables = adapter.list_tables(None).await?;
//!
//! // Get table structure
//! let table_info = adapter.describe_table("users", None).await?;
//! for col in table_info.columns {
//!     println!("{}: {} (nullable: {})", col.name, col.data_type, col.nullable);
//! }
//! ```

pub mod adapter;
pub mod adapters;
pub mod config;
pub mod error;
pub mod export;
pub mod registry;

// Re-export commonly used types
pub use adapter::{
    escape_like_pattern, filter_to_sql, ColumnInfo, Connection, ConnectionConfig, DatabaseType,
    DbAdapter, FilterExpr, QueryResult, QueryValue, TableInfo, TableSearchMode,
};
pub use config::{ArniConfig, ConfigProfile};
pub use error::{DataError, Result};
pub use export::{to_bytes, to_file, DataFormat};
pub use registry::ConnectionRegistry;

/// A thread-safe, cheaply-cloneable handle to any database adapter.
///
/// All concrete adapters implement `DbAdapter + Send + Sync + 'static`, so they
/// can be wrapped in `Arc` and shared across async tasks without serialisation.
///
/// # Example
///
/// ```ignore
/// use arni::SharedAdapter;
/// let adapter: SharedAdapter = std::sync::Arc::new(my_adapter);
/// let clone = Arc::clone(&adapter); // zero-cost clone
/// ```
pub type SharedAdapter = std::sync::Arc<dyn DbAdapter + Send + Sync + 'static>;

#[cfg(test)]
mod shared_adapter_tests {
    /// Compile-time proof that a concrete adapter satisfies the SharedAdapter bounds.
    /// These tests have no runtime cost — they exist solely to catch bound regressions.
    fn _assert_shared<T: super::DbAdapter + Send + Sync + 'static>() {}

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_adapter_is_shared() {
        use crate::adapters::postgres::PostgresAdapter;
        _assert_shared::<PostgresAdapter>();
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_adapter_is_shared() {
        use crate::adapters::mysql::MySqlAdapter;
        _assert_shared::<MySqlAdapter>();
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_adapter_is_shared() {
        use crate::adapters::sqlite::SqliteAdapter;
        _assert_shared::<SqliteAdapter>();
    }

    #[cfg(feature = "mssql")]
    #[test]
    fn sqlserver_adapter_is_shared() {
        use crate::adapters::mssql::SqlServerAdapter;
        _assert_shared::<SqlServerAdapter>();
    }

    #[cfg(feature = "mongodb")]
    #[test]
    fn mongodb_adapter_is_shared() {
        use crate::adapters::mongodb::MongoDbAdapter;
        _assert_shared::<MongoDbAdapter>();
    }

    #[cfg(feature = "oracle")]
    #[test]
    fn oracle_adapter_is_shared() {
        use crate::adapters::oracle::OracleAdapter;
        _assert_shared::<OracleAdapter>();
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn duckdb_adapter_is_shared() {
        use crate::adapters::duckdb::DuckDbAdapter;
        _assert_shared::<DuckDbAdapter>();
    }
}
