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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_error() {
        let err = Error::Connection("Failed to connect".to_string());
        assert!(err.to_string().contains("Connection error"));
        assert!(err.to_string().contains("Failed to connect"));
    }

    #[test]
    fn test_query_error() {
        let err = Error::Query("Invalid SQL".to_string());
        assert!(err.to_string().contains("Query error"));
        assert!(err.to_string().contains("Invalid SQL"));
    }

    #[test]
    fn test_conversion_error() {
        let err = Error::Conversion("Type mismatch".to_string());
        assert!(err.to_string().contains("Conversion error"));
        assert!(err.to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_config_error() {
        let err = Error::Config("Invalid config".to_string());
        assert!(err.to_string().contains("Configuration error"));
        assert!(err.to_string().contains("Invalid config"));
    }

    #[test]
    fn test_not_implemented_error() {
        let err = Error::NotImplemented("Feature X".to_string());
        assert!(err.to_string().contains("Not yet implemented"));
        assert!(err.to_string().contains("Feature X"));
    }

    #[test]
    fn test_other_error() {
        let anyhow_err = anyhow::anyhow!("Something went wrong");
        let err = Error::Other(anyhow_err);
        assert!(err.to_string().contains("Something went wrong"));
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Error>();
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        assert!(ok_result.is_ok());
        assert_eq!(ok_result.unwrap(), 42);

        let err_result: Result<i32> = Err(Error::Query("Test".to_string()));
        assert!(err_result.is_err());
    }
}
