//! # Arni — Multi-database adapter library for Rust
//!
//! This crate is a thin re-export facade over [`arni_data`].
//! All implementation lives in `arni-data`; this crate provides the public-facing
//! `arni::` namespace so downstream users write:
//!
//! ```rust,ignore
//! use arni::{DbAdapter, ConnectionConfig, DatabaseType};
//! ```
//!
//! ## Feature flags
//!
//! Enable database drivers with Cargo features (all off by default):
//!
//! | Feature | Database |
//! |---------|----------|
//! | `postgres` | PostgreSQL |
//! | `mysql` | MySQL / MariaDB |
//! | `sqlite` | SQLite |
//! | `mongodb` | MongoDB |
//! | `mssql` | Microsoft SQL Server |
//! | `oracle` | Oracle DB |
//! | `duckdb` | DuckDB |
//! | `all-databases` | All of the above |
//!
//! ## Example
//!
//! ```toml
//! [dependencies]
//! arni = { version = "0.1", features = ["postgres"] }
//! ```

pub use arni_data::adapter::{
    escape_like_pattern, filter_to_sql, ColumnInfo, Connection, ConnectionConfig, DatabaseType,
    DbAdapter, FilterExpr, QueryResult, QueryValue, TableInfo, TableSearchMode,
};
pub use arni_data::config::{ArniConfig, ConfigProfile};
pub use arni_data::error::{DataError, Result};
pub use arni_data::export::{to_bytes, to_file, DataFormat};
pub use arni_data::registry::ConnectionRegistry;
pub use arni_data::SharedAdapter;

/// All adapter implementations, re-exported from `arni_data::adapters`.
pub mod adapters {
    pub use arni_data::adapters::*;
}
