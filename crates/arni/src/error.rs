//! Error types for Arni

use thiserror::Error;

/// Result type alias
pub type Result<T> = std::result::Result<T, Error>;

/// Arni error types
#[derive(Error, Debug)]
pub enum Error {
    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Query execution error
    #[error("Query error: {0}")]
    Query(String),

    /// Data conversion error
    #[error("Conversion error: {0}")]
    Conversion(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Not implemented
    #[error("Not yet implemented: {0}")]
    NotImplemented(String),

    /// Other errors
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
