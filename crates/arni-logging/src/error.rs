use std::fmt;

/// Result type for arni-logging operations
pub type Result<T> = std::result::Result<T, LoggingError>;

/// Errors that can occur during logging configuration and initialization
#[derive(Debug)]
pub enum LoggingError {
    /// Error occurred during logging configuration
    ConfigError(String),

    /// Error occurred during logging initialization
    InitError(String),

    /// File I/O error
    IoError(std::io::Error),

    /// Invalid log level or filter
    FilterError(String),
}

impl fmt::Display for LoggingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoggingError::ConfigError(msg) => write!(f, "Configuration error: {}", msg),
            LoggingError::InitError(msg) => write!(f, "Initialization error: {}", msg),
            LoggingError::IoError(e) => write!(f, "I/O error: {}", e),
            LoggingError::FilterError(msg) => write!(f, "Filter error: {}", msg),
        }
    }
}

impl std::error::Error for LoggingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LoggingError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for LoggingError {
    fn from(err: std::io::Error) -> Self {
        LoggingError::IoError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_error_display() {
        let err = LoggingError::ConfigError("test error".to_string());
        assert_eq!(err.to_string(), "Configuration error: test error");
    }

    #[test]
    fn test_init_error_display() {
        let err = LoggingError::InitError("init failed".to_string());
        assert_eq!(err.to_string(), "Initialization error: init failed");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: LoggingError = io_err.into();
        assert!(matches!(err, LoggingError::IoError(_)));
    }

    #[test]
    fn test_filter_error_display() {
        let err = LoggingError::FilterError("invalid filter".to_string());
        assert_eq!(err.to_string(), "Filter error: invalid filter");
    }
}
