//! # Arni - Multi-Database Adapter Library
//!
//! Arni provides a unified interface for working with multiple database systems,
//! returning results as Polars DataFrames. Inspired by the Python skidbladnir library.
//!
//! ## Supported Databases
//!
//! - PostgreSQL
//! - MongoDB
//! - Oracle
//! - Microsoft SQL Server
//! - DuckDB
//!
//! ## Example
//!
//! ```rust,ignore
//! use arni::{DbAdapter, PostgresAdapter};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let adapter = PostgresAdapter::connect("postgresql://localhost/mydb").await?;
//!     let df = adapter.query("SELECT * FROM users").await?;
//!     println!("{}", df);
//!     Ok(())
//! }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod adapters;
pub mod config;
pub mod error;
pub mod traits;
pub mod types;

#[cfg(test)]
pub mod testing;

// Re-exports
pub use error::{Error, Result};
pub use traits::{Connection, DbAdapter};
pub use types::DataFrame;
