//! Database adapter implementations
//!
//! This module contains concrete implementations of the [`DbAdapter`](crate::adapter::DbAdapter)
//! and [`Connection`](crate::adapter::Connection) traits for various database systems.
//!
//! Each adapter is feature-gated and only available when the corresponding feature is enabled.
//!
//! # Available Adapters
//!
//! - [`postgres`] - PostgreSQL adapter (requires `postgres` feature)
//! - [`mysql`] - MySQL adapter (requires `mysql` feature)
//!
//! # Examples
//!
//! ```toml
//! # Enable specific database support
//! arni-data = { version = "0.1", features = ["postgres"] }
//!
//! # Or enable all databases
//! arni-data = { version = "0.1", features = ["all-databases"] }
//! ```

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "oracle")]
pub mod oracle;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "mssql")]
pub mod mssql;
