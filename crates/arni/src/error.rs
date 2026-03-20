//! Error types for arni operations
//!
//! This module provides the [`DataError`] type for representing all possible errors
//! that can occur during database operations.

use thiserror::Error;

/// Result type for arni operations
pub type Result<T> = std::result::Result<T, DataError>;

/// Main error type for data operations
#[derive(Error, Debug)]
pub enum DataError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("DataFrame error: {0}")]
    DataFrame(String),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Adapter not found: {0}")]
    AdapterNotFound(String),

    #[error("Authentication failed: {0}")]
    Authentication(String),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[cfg(feature = "polars")]
    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<toml::de::Error> for DataError {
    fn from(err: toml::de::Error) -> Self {
        DataError::Serialization(err.to_string())
    }
}

impl From<toml::ser::Error> for DataError {
    fn from(err: toml::ser::Error) -> Self {
        DataError::Serialization(err.to_string())
    }
}

impl From<serde_json::Error> for DataError {
    fn from(err: serde_json::Error) -> Self {
        DataError::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_config_error() {
        let err = DataError::Config("invalid config".to_string());
        assert_eq!(err.to_string(), "Configuration error: invalid config");
    }

    #[test]
    fn test_connection_error() {
        let err = DataError::Connection("connection refused".to_string());
        assert_eq!(err.to_string(), "Connection error: connection refused");
    }

    #[test]
    fn test_query_error() {
        let err = DataError::Query("syntax error".to_string());
        assert_eq!(err.to_string(), "Query error: syntax error");
    }

    #[test]
    fn test_dataframe_error() {
        let err = DataError::DataFrame("invalid schema".to_string());
        assert_eq!(err.to_string(), "DataFrame error: invalid schema");
    }

    #[test]
    fn test_type_conversion_error() {
        let err = DataError::TypeConversion("cannot convert string to int".to_string());
        assert_eq!(
            err.to_string(),
            "Type conversion error: cannot convert string to int"
        );
    }

    #[test]
    fn test_encryption_error() {
        let err = DataError::Encryption("decryption failed".to_string());
        assert_eq!(err.to_string(), "Encryption error: decryption failed");
    }

    #[test]
    fn test_serialization_error() {
        let err = DataError::Serialization("invalid JSON".to_string());
        assert_eq!(err.to_string(), "Serialization error: invalid JSON");
    }

    #[test]
    fn test_adapter_not_found_error() {
        let err = DataError::AdapterNotFound("postgres".to_string());
        assert_eq!(err.to_string(), "Adapter not found: postgres");
    }

    #[test]
    fn test_authentication_error() {
        let err = DataError::Authentication("wrong password".to_string());
        assert_eq!(err.to_string(), "Authentication failed: wrong password");
    }

    #[test]
    fn test_not_supported_error() {
        let err = DataError::NotSupported("bulk insert".to_string());
        assert_eq!(err.to_string(), "Operation not supported: bulk insert");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: DataError = io_err.into();

        match err {
            DataError::Io(_) => assert!(err.to_string().contains("file not found")),
            _ => panic!("Expected Io error variant"),
        }
    }

    #[test]
    fn test_toml_de_error_conversion() {
        let toml_str = "invalid = toml = syntax";
        let toml_err = toml::from_str::<toml::Value>(toml_str).unwrap_err();
        let err: DataError = toml_err.into();

        match err {
            DataError::Serialization(msg) => assert!(!msg.is_empty()),
            _ => panic!("Expected Serialization error variant"),
        }
    }

    #[test]
    fn test_toml_ser_error_conversion() {
        use std::collections::HashMap;

        // Create a value that might fail TOML serialization
        let mut map = HashMap::new();
        map.insert("key".to_string(), f64::NAN);

        match toml::to_string(&map) {
            Err(toml_err) => {
                let err: DataError = toml_err.into();
                match err {
                    DataError::Serialization(msg) => assert!(!msg.is_empty()),
                    _ => panic!("Expected Serialization error variant"),
                }
            }
            Ok(_) => {
                // If serialization succeeds, the From impl exists — nothing to assert
            }
        }
    }

    #[test]
    fn test_serde_json_error_conversion() {
        let json_str = "{invalid json}";
        let json_err = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
        let err: DataError = json_err.into();

        match err {
            DataError::Serialization(msg) => assert!(!msg.is_empty()),
            _ => panic!("Expected Serialization error variant"),
        }
    }

    #[test]
    fn test_anyhow_error_conversion() {
        let anyhow_err = anyhow::anyhow!("generic error");
        let err: DataError = anyhow_err.into();

        match err {
            DataError::Other(_) => assert!(err.to_string().contains("generic error")),
            _ => panic!("Expected Other error variant"),
        }
    }

    #[test]
    fn test_result_type_alias() {
        fn returns_result() -> Result<i32> {
            Ok(42)
        }

        assert_eq!(returns_result().unwrap(), 42);
    }

    #[test]
    fn test_result_type_with_error() {
        fn returns_error() -> Result<i32> {
            Err(DataError::Config("test error".to_string()))
        }

        assert!(returns_error().is_err());
    }
}
