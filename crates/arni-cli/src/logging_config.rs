//! Logging configuration management for arni-cli.
//!
//! Always reads from `~/.arni/logging.yml`. The log level can be overridden
//! at runtime via the `--log-level` global CLI flag without touching the file.
//!
//! # File Format
//!
//! ```yaml
//! # ~/.arni/logging.yml
//!
//! # Default log level: error | warn | info | debug | trace
//! level: info
//!
//! rolling:
//!   # Rotation strategy: daily | hourly | never
//!   strategy: daily
//!   # Number of rotated files to retain (0 = unlimited)
//!   max_files: 30
//!   # Max file size in MB before rotation (0 = no size limit)
//!   max_size_mb: 0
//!
//! # Log file directory. Tilde (~) is expanded to $HOME.
//! # Defaults to ~/.arni/logs when absent.
//! # log_dir: ~/.arni/logs
//! ```

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

// ─── Path helpers ─────────────────────────────────────────────────────────────

/// Returns the default logging config path: `~/.arni/logging.yml`
pub fn default_logging_path(config_dir: &Path) -> PathBuf {
    config_dir.join("logging.yml")
}

/// Expand a leading `~` to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        return home_dir();
    }
    if let Some(rest) = path.strip_prefix("~/") {
        return home_dir().join(rest);
    }
    PathBuf::from(path)
}

fn home_dir() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .expect("HOME or USERPROFILE environment variable must be set");
    PathBuf::from(home)
}

// ─── Schema types ─────────────────────────────────────────────────────────────

/// Top-level structure for `~/.arni/logging.yml`.
///
/// The file is **always** read from the fixed path `~/.arni/logging.yml`
/// (or the equivalent under a custom config dir). Unlike connections, the
/// log config path is not overridable — only the level can be overridden at
/// runtime with `--log-level`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Default log level. One of: `error`, `warn`, `info`, `debug`, `trace`.
    ///
    /// Overridable at runtime via the `--log-level` global CLI flag without
    /// modifying this file.
    #[serde(default = "default_level")]
    pub level: String,

    /// Rolling file appender settings.
    #[serde(default)]
    pub rolling: RollingConfig,

    /// Log file directory. Tilde (`~`) is expanded to `$HOME`.
    ///
    /// Defaults to `~/.arni/logs` when absent.
    pub log_dir: Option<String>,
}

/// Configuration for the rolling log file appender.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingConfig {
    /// Rotation strategy.
    ///
    /// - `daily`:  Rotate at midnight — one file per calendar day.
    /// - `hourly`: Rotate at the top of each hour.
    /// - `never`:  No rotation — a single continuous log file.
    #[serde(default = "default_strategy")]
    pub strategy: RollingStrategy,

    /// Maximum number of rotated log files to retain.
    ///
    /// When the limit is exceeded the oldest file is deleted. `0` = unlimited.
    #[serde(default = "default_max_files")]
    pub max_files: u32,

    /// Maximum log file size in megabytes before triggering rotation.
    ///
    /// `0` = no size-based limit (rotation is driven only by time strategy).
    #[serde(default)]
    pub max_size_mb: u64,
}

/// Log rotation strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RollingStrategy {
    /// Rotate daily (one file per calendar day).
    Daily,
    /// Rotate hourly.
    Hourly,
    /// Never rotate — single continuous log file.
    Never,
}

// ─── Defaults ─────────────────────────────────────────────────────────────────

fn default_level() -> String {
    "info".to_string()
}

fn default_strategy() -> RollingStrategy {
    RollingStrategy::Daily
}

fn default_max_files() -> u32 {
    30
}

impl Default for RollingConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            max_files: default_max_files(),
            max_size_mb: 0,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_level(),
            rolling: RollingConfig::default(),
            log_dir: None,
        }
    }
}

// ─── LoggingConfig impl ───────────────────────────────────────────────────────

impl LoggingConfig {
    /// Resolve the effective log directory path with tilde expansion.
    ///
    /// Priority:
    /// 1. `log_dir` from config (tilde expanded)
    /// 2. `~/.arni/logs` (built-in default)
    pub fn resolved_log_dir(&self) -> PathBuf {
        match &self.log_dir {
            Some(raw) => expand_tilde(raw),
            None => home_dir().join(".arni").join("logs"),
        }
    }

    /// Validate config values.
    ///
    /// Returns an error if `level` is not a recognised tracing level.
    pub fn validate(&self) -> Result<()> {
        match self.level.to_lowercase().as_str() {
            "error" | "warn" | "info" | "debug" | "trace" => Ok(()),
            other => bail!(
                "Invalid log level '{}'. Must be one of: error, warn, info, debug, trace",
                other
            ),
        }
    }
}

// ─── Load / write helpers ─────────────────────────────────────────────────────

/// Load logging config from `<config_dir>/logging.yml`.
///
/// Returns [`LoggingConfig::default`] if the file does not exist so the CLI
/// is fully functional without any config file.
pub fn load_logging_config(config_dir: &Path) -> Result<LoggingConfig> {
    let path = default_logging_path(config_dir);
    if !path.exists() {
        return Ok(LoggingConfig::default());
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read logging config: {}", path.display()))?;
    let cfg: LoggingConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("Failed to parse logging YAML: {}", path.display()))?;
    cfg.validate()?;
    Ok(cfg)
}

/// Write a default `logging.yml` to `config_dir` if none exists.
///
/// Safe to call on every startup — does nothing if the file is already present.
pub fn write_default_logging_config(config_dir: &Path) -> Result<()> {
    let path = default_logging_path(config_dir);
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create config dir: {}", parent.display()))?;
    }
    let default_yaml = "\
# arni-cli logging configuration
# Docs: https://github.com/aaroncroberts/arni

# Default log level (override at runtime with: arni --log-level debug <command>)
# Options: error | warn | info | debug | trace
level: info

rolling:
  # Rotation strategy: daily | hourly | never
  strategy: daily
  # Number of rotated files to keep (0 = unlimited)
  max_files: 30
  # Max file size in MB before rotation (0 = no size limit)
  max_size_mb: 0

# Log file directory (default: ~/.arni/logs)
# log_dir: ~/.arni/logs
";
    std::fs::write(&path, default_yaml)
        .with_context(|| format!("Failed to write default logging config: {}", path.display()))?;
    Ok(())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = LoggingConfig::default();
        assert_eq!(cfg.level, "info");
        assert_eq!(cfg.rolling.strategy, RollingStrategy::Daily);
        assert_eq!(cfg.rolling.max_files, 30);
        assert_eq!(cfg.rolling.max_size_mb, 0);
        assert!(cfg.log_dir.is_none());
    }

    #[test]
    fn test_validate_valid_levels() {
        for level in &["error", "warn", "info", "debug", "trace"] {
            let cfg = LoggingConfig {
                level: level.to_string(),
                ..Default::default()
            };
            assert!(cfg.validate().is_ok(), "Level '{}' should be valid", level);
        }
    }

    #[test]
    fn test_validate_invalid_level() {
        let cfg = LoggingConfig {
            level: "verbose".to_string(),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_yaml_roundtrip() {
        let yaml = "\
level: debug
rolling:
  strategy: hourly
  max_files: 7
  max_size_mb: 50
log_dir: /var/log/arni
";
        let cfg: LoggingConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.level, "debug");
        assert_eq!(cfg.rolling.strategy, RollingStrategy::Hourly);
        assert_eq!(cfg.rolling.max_files, 7);
        assert_eq!(cfg.rolling.max_size_mb, 50);
        assert_eq!(cfg.log_dir.as_deref(), Some("/var/log/arni"));
    }

    #[test]
    fn test_yaml_never_strategy() {
        let yaml = "rolling:\n  strategy: never\n";
        let cfg: LoggingConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.rolling.strategy, RollingStrategy::Never);
    }

    #[test]
    fn test_resolved_log_dir_default() {
        let cfg = LoggingConfig::default();
        let dir = cfg.resolved_log_dir();
        assert!(
            dir.to_string_lossy().ends_with(".arni/logs"),
            "Expected path ending with .arni/logs, got: {}",
            dir.display()
        );
    }

    #[test]
    fn test_resolved_log_dir_custom() {
        let cfg = LoggingConfig {
            log_dir: Some("/var/log/arni".to_string()),
            ..Default::default()
        };
        assert_eq!(cfg.resolved_log_dir(), PathBuf::from("/var/log/arni"));
    }

    #[test]
    fn test_expand_tilde() {
        let expanded = expand_tilde("~/foo/bar");
        let home = home_dir();
        assert_eq!(expanded, home.join("foo/bar"));
    }

    #[test]
    fn test_expand_tilde_exact() {
        let expanded = expand_tilde("~");
        assert_eq!(expanded, home_dir());
    }

    #[test]
    fn test_expand_tilde_absolute_unchanged() {
        let expanded = expand_tilde("/absolute/path");
        assert_eq!(expanded, PathBuf::from("/absolute/path"));
    }

    // ── load_logging_config edge cases ───────────────────────────────────────

    #[test]
    fn test_load_logging_config_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        // No logging.yml written — should silently return default.
        let cfg = load_logging_config(dir.path()).unwrap();
        assert_eq!(cfg.level, "info");
        assert_eq!(cfg.rolling.strategy, RollingStrategy::Daily);
    }

    #[test]
    fn test_load_logging_config_invalid_yaml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logging.yml");
        std::fs::write(&path, b": this is: not: valid yaml :::").unwrap();
        let result = load_logging_config(dir.path());
        assert!(result.is_err(), "Expected error for invalid YAML");
    }

    #[test]
    fn test_load_logging_config_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logging.yml");
        std::fs::write(&path, b"level: debug\nrolling:\n  strategy: never\n").unwrap();
        let cfg = load_logging_config(dir.path()).unwrap();
        assert_eq!(cfg.level, "debug");
        assert_eq!(cfg.rolling.strategy, RollingStrategy::Never);
    }

    // ── write_default_logging_config ─────────────────────────────────────────

    #[test]
    fn test_write_default_logging_config_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        write_default_logging_config(dir.path()).unwrap();
        let path = dir.path().join("logging.yml");
        assert!(path.exists(), "logging.yml should be created");
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.contains("level:"));
    }

    #[test]
    fn test_write_default_logging_config_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        // Write custom content first.
        let path = dir.path().join("logging.yml");
        std::fs::write(&path, b"level: trace\n").unwrap();
        // Second call should not overwrite the existing file.
        write_default_logging_config(dir.path()).unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            contents, "level: trace\n",
            "Existing file must not be overwritten"
        );
    }
}
