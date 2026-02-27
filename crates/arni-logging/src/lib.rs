//! arni-logging - Centralized logging infrastructure for arni
//!
//! This crate provides structured, async-aware logging using the `tracing` ecosystem
//! with support for multiple output formats (console and files) and flexible configuration.
//!
//! # Features
//!
//! - **Structured Logging**: Key-value fields, spans, and events
//! - **Multiple Outputs**: Console (pretty/compact) and files (.log/.jsonl)
//! - **Dual Output**: Simultaneous console + file logging
//! - **Log Levels**: TRACE, DEBUG, INFO, WARN, ERROR
//! - **Categories**: Via tracing targets and spans
//! - **Environment Config**: RUST_LOG environment variable
//! - **File Rotation**: Daily, hourly, minutely, or never
//!
//! # Quick Start
//!
//! ```no_run
//! use arni_logging::LoggingConfig;
//!
//! // Simple console logging
//! arni_logging::init_default();
//!
//! // Console with custom format
//! LoggingConfig::builder()
//!     .with_console_pretty()
//!     .build()
//!     .unwrap()
//!     .apply()
//!     .expect("Failed to initialize logging");
//!
//! // File output with daily rotation
//! LoggingConfig::builder()
//!     .with_file_text()
//!     .with_file_directory("./logs")
//!     .with_file_prefix("myapp")
//!     .build()
//!     .unwrap()
//!     .apply()
//!     .expect("Failed to initialize logging");
//!
//! // Dual output: console + JSON file
//! LoggingConfig::builder()
//!     .with_console_compact()
//!     .with_file_json()
//!     .with_rotation_policy(arni_logging::RotationPolicy::Hourly)
//!     .build()
//!     .unwrap()
//!     .apply()
//!     .expect("Failed to initialize logging");
//! ```

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

pub mod config;
pub mod error;

pub use config::{
    ConsoleFormat, ConsoleWriter, FileFormat, LoggingConfig, LoggingConfigBuilder, RotationPolicy,
};
pub use error::{LoggingError, Result};
// Re-export WorkerGuard so callers don't need a direct tracing-appender dep.
pub use tracing_appender::non_blocking::WorkerGuard;

/// Initialize logging with default settings (pretty console output, INFO level)
///
/// This is a convenience function that sets up logging with sensible defaults:
/// - Pretty console format (colorized)
/// - INFO log level (can be overridden with RUST_LOG environment variable)
/// - Output to stderr
///
/// # Examples
///
/// ```no_run
/// arni_logging::init_default();
/// tracing::info!("Application started");
/// ```
///
/// # Panics
///
/// Panics if logging has already been initialized or if initialization fails.
pub fn init_default() {
    init_default_with_filter("info").expect("Failed to initialize logging");
}

/// Initialize logging with default settings and a custom filter
///
/// # Arguments
///
/// * `filter` - Log level filter (e.g., "debug", "info", "warn")
///
/// # Examples
///
/// ```no_run
/// arni_logging::init_default_with_filter("debug").unwrap();
/// tracing::debug!("Debug message");
/// ```
pub fn init_default_with_filter(filter: &str) -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(filter))
        .map_err(|e| LoggingError::ConfigError(format!("Invalid filter: {}", e)))?;

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .pretty()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true),
        )
        .try_init()
        .map_err(|e| LoggingError::InitError(format!("Failed to initialize subscriber: {}", e)))?;

    Ok(())
}

/// Initialize logging with a custom configuration
///
/// # Arguments
///
/// * `config` - Custom logging configuration
///
/// # Examples
///
/// ```no_run
/// use arni_logging::LoggingConfig;
///
/// let config = LoggingConfig::builder()
///     .with_console_pretty()
///     .build()
///     .unwrap();
///
/// arni_logging::init(config).unwrap();
/// ```
pub fn init(config: LoggingConfig) -> Result<()> {
    config.apply()
}

/// Initialize arni's standard CLI logging.
///
/// Sets up two output layers:
/// - **File** (async, non-blocking): all events at `level` → `log_dir/arni.<date>`
/// - **Console** (stderr): WARN and above, compact format for interactive output
///
/// The returned [`WorkerGuard`] **must be bound to a variable** in the caller and
/// kept alive until the program exits. Dropping it early flushes and stops the
/// async file writer.
///
/// # Arguments
///
/// * `log_dir` — Directory for log files (created if absent)
/// * `level`   — Minimum log level (e.g. `"info"`, `"debug"`)
/// * `rotation` — File rotation policy
///
/// # Errors
///
/// Returns an error if the log directory cannot be created, the level is invalid,
/// or a global subscriber is already installed.
///
/// # Examples
///
/// ```no_run
/// use arni_logging::RotationPolicy;
/// use std::path::Path;
///
/// let _guard = arni_logging::init_arni_logging(
///     Path::new("/var/log/arni"),
///     "info",
///     RotationPolicy::Daily,
/// ).expect("failed to init logging");
/// ```
pub fn init_arni_logging(
    log_dir: &std::path::Path,
    level: &str,
    rotation: RotationPolicy,
) -> Result<WorkerGuard> {
    use tracing_appender::non_blocking;
    use tracing_appender::rolling::{RollingFileAppender, Rotation};

    // Ensure the log directory exists.
    std::fs::create_dir_all(log_dir)?;

    let appender_rotation = match rotation {
        RotationPolicy::Daily => Rotation::DAILY,
        RotationPolicy::Hourly => Rotation::HOURLY,
        RotationPolicy::Minutely => Rotation::MINUTELY,
        RotationPolicy::Never => Rotation::NEVER,
    };

    let file_appender = RollingFileAppender::new(appender_rotation, log_dir, "arni");
    let (non_blocking_writer, guard) = non_blocking(file_appender);

    let file_filter = EnvFilter::try_new(level).map_err(|e| {
        LoggingError::FilterError(format!("Invalid log level '{}': {}", level, e))
    })?;

    // Console only shows WARN+ to avoid cluttering interactive output.
    let console_filter = EnvFilter::try_new("warn")
        .map_err(|e| LoggingError::FilterError(format!("Cannot build warn filter: {}", e)))?;

    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(non_blocking_writer)
        .with_filter(file_filter);

    let console_layer = fmt::layer()
        .compact()
        .with_target(false)
        .with_writer(std::io::stderr)
        .with_filter(console_filter);

    tracing_subscriber::registry()
        .with(file_layer)
        .with(console_layer)
        .try_init()
        .map_err(|e| LoggingError::InitError(format!("Failed to install subscriber: {}", e)))?;

    Ok(guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_filter_parsing() {
        // Test valid filter levels
        let filter = EnvFilter::try_new("info");
        assert!(filter.is_ok());

        let filter = EnvFilter::try_new("debug");
        assert!(filter.is_ok());

        let filter = EnvFilter::try_new("warn");
        assert!(filter.is_ok());
    }

    #[test]
    fn test_env_filter_with_target() {
        // Test filter with target specification
        let filter = EnvFilter::try_new("rusty_data=debug,info");
        assert!(filter.is_ok());
    }

    #[test]
    fn test_logging_config_builder() {
        // Test that config builder can be created
        let builder = LoggingConfig::builder();
        assert!(builder.build().is_ok());
    }
}
