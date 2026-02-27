//! Application-level configuration: `~/.arni/config.yml`.
//!
//! This is distinct from `connections.yml` (database credentials) and
//! `logging.yml` (log output settings). It holds user-specific paths and
//! preferences that affect how arni locates native libraries and chooses
//! default behaviour.
//!
//! # File format
//!
//! ```yaml
//! # ~/.arni/config.yml
//! oracle_lib_dir: ~/Oracle/instantclient_23_3
//! # duckdb_lib_dir: /opt/homebrew/lib
//! # default_connection: pg-dev
//! ```
//!
//! Missing keys fall back to sensible defaults. The file itself is optional —
//! arni works without it (library paths are then sourced from the environment).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

// ─── Path helpers ──────────────────────────────────────────────────────────────

pub fn default_app_config_path(config_dir: &Path) -> PathBuf {
    config_dir.join("config.yml")
}

fn home_dir() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .expect("HOME or USERPROFILE must be set");
    PathBuf::from(home)
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

// ─── Schema ───────────────────────────────────────────────────────────────────

/// Top-level structure for `~/.arni/config.yml`.
///
/// All fields are optional — defaults are used when absent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// Path to the Oracle Instant Client directory containing `libclntsh.dylib`
    /// (macOS) or `libclntsh.so` (Linux).
    ///
    /// Overrides the `ORACLE_LIB_DIR` / `DYLD_LIBRARY_PATH` environment
    /// variables when set here, unless those variables are already set in the
    /// shell (shell env always wins, matching `force = false` semantics).
    ///
    /// Example: `~/Oracle/instantclient_23_3`
    pub oracle_lib_dir: Option<String>,

    /// Path to the directory containing the DuckDB shared library
    /// (`libduckdb.dylib` / `libduckdb.so`).
    ///
    /// Overrides `DUCKDB_LIB_DIR` when not already set in the shell.
    ///
    /// Default on macOS (Homebrew ARM64): `/opt/homebrew/lib`
    pub duckdb_lib_dir: Option<String>,

    /// Name of the connection profile to use when none is specified on the CLI.
    /// Must match a key in `~/.arni/connections.yml`.
    pub default_connection: Option<String>,

    /// Override the default connections file location.
    /// Default: `~/.arni/connections.yml`
    pub connections_file: Option<String>,

    /// Override the default log output directory.
    /// Default: `~/.arni/logs`
    pub log_dir: Option<String>,
}

impl AppConfig {
    // ── Resolved accessors ────────────────────────────────────────────────────

    /// Resolved Oracle Instant Client directory (tilde expanded).
    ///
    /// Resolution order:
    /// 1. `ORACLE_LIB_DIR` environment variable (set in shell)
    /// 2. `oracle_lib_dir` from `~/.arni/config.yml`
    /// 3. `None` — Oracle adapter will fail at runtime if invoked
    pub fn resolved_oracle_lib_dir(&self) -> Option<PathBuf> {
        if let Ok(v) = std::env::var("ORACLE_LIB_DIR") {
            if !v.is_empty() {
                return Some(expand_tilde(&v));
            }
        }
        self.oracle_lib_dir
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(expand_tilde)
    }

    /// Resolved DuckDB library directory (tilde expanded).
    ///
    /// Resolution order:
    /// 1. `DUCKDB_LIB_DIR` environment variable
    /// 2. `duckdb_lib_dir` from `~/.arni/config.yml`
    /// 3. Homebrew ARM64 default (`/opt/homebrew/lib`)
    pub fn resolved_duckdb_lib_dir(&self) -> PathBuf {
        if let Ok(v) = std::env::var("DUCKDB_LIB_DIR") {
            if !v.is_empty() {
                return expand_tilde(&v);
            }
        }
        if let Some(dir) = self.duckdb_lib_dir.as_deref().filter(|s| !s.is_empty()) {
            return expand_tilde(dir);
        }
        // Homebrew ARM64 default
        PathBuf::from("/opt/homebrew/lib")
    }

    /// Apply library path configuration to the process environment so that
    /// downstream code (e.g. the oracle crate's dlopen call) can find the
    /// native libraries.
    ///
    /// Only sets variables that are not already present in the environment
    /// (`force = false` semantics — shell env always wins).
    pub fn apply_lib_paths(&self) {
        // Oracle — DYLD_LIBRARY_PATH (macOS) / LD_LIBRARY_PATH (Linux)
        if let Some(dir) = self.resolved_oracle_lib_dir() {
            let dir_str = dir.to_string_lossy().to_string();
            #[cfg(target_os = "macos")]
            if std::env::var("DYLD_LIBRARY_PATH").is_err() {
                std::env::set_var("DYLD_LIBRARY_PATH", &dir_str);
            }
            #[cfg(not(target_os = "macos"))]
            if std::env::var("LD_LIBRARY_PATH").is_err() {
                std::env::set_var("LD_LIBRARY_PATH", &dir_str);
            }
            if std::env::var("ORACLE_LIB_DIR").is_err() {
                std::env::set_var("ORACLE_LIB_DIR", &dir_str);
            }
        }

        // DuckDB — build-time env var read by the duckdb crate's build script
        if std::env::var("DUCKDB_LIB_DIR").is_err() {
            let dir = self.resolved_duckdb_lib_dir();
            std::env::set_var("DUCKDB_LIB_DIR", dir.to_string_lossy().as_ref());
        }
    }
}

// ─── Load helpers ─────────────────────────────────────────────────────────────

/// Load `AppConfig` from `<config_dir>/config.yml`.
///
/// Returns [`AppConfig::default`] if the file does not exist so the CLI
/// is fully functional without any config file.
pub fn load_app_config(config_dir: &Path) -> Result<AppConfig> {
    let path = default_app_config_path(config_dir);
    if !path.exists() {
        return Ok(AppConfig::default());
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read app config: {}", path.display()))?;
    serde_yaml::from_str(&contents)
        .with_context(|| format!("Failed to parse app config YAML: {}", path.display()))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_empty() {
        let cfg = AppConfig::default();
        assert!(cfg.oracle_lib_dir.is_none());
        assert!(cfg.duckdb_lib_dir.is_none());
        assert!(cfg.default_connection.is_none());
    }

    #[test]
    fn test_resolved_duckdb_lib_dir_default() {
        // Clear env to test config-only path
        let original = std::env::var("DUCKDB_LIB_DIR").ok();
        std::env::remove_var("DUCKDB_LIB_DIR");

        let cfg = AppConfig::default();
        assert_eq!(cfg.resolved_duckdb_lib_dir(), PathBuf::from("/opt/homebrew/lib"));

        if let Some(v) = original {
            std::env::set_var("DUCKDB_LIB_DIR", v);
        }
    }

    #[test]
    fn test_resolved_duckdb_lib_dir_from_config() {
        let original = std::env::var("DUCKDB_LIB_DIR").ok();
        std::env::remove_var("DUCKDB_LIB_DIR");

        let cfg = AppConfig {
            duckdb_lib_dir: Some("/usr/local/lib".to_string()),
            ..Default::default()
        };
        assert_eq!(cfg.resolved_duckdb_lib_dir(), PathBuf::from("/usr/local/lib"));

        if let Some(v) = original {
            std::env::set_var("DUCKDB_LIB_DIR", v);
        }
    }

    #[test]
    fn test_resolved_oracle_lib_dir_from_config() {
        let original = std::env::var("ORACLE_LIB_DIR").ok();
        std::env::remove_var("ORACLE_LIB_DIR");

        let cfg = AppConfig {
            oracle_lib_dir: Some("/opt/oracle/instantclient".to_string()),
            ..Default::default()
        };
        assert_eq!(
            cfg.resolved_oracle_lib_dir(),
            Some(PathBuf::from("/opt/oracle/instantclient"))
        );

        if let Some(v) = original {
            std::env::set_var("ORACLE_LIB_DIR", v);
        }
    }

    #[test]
    fn test_env_var_wins_over_config() {
        std::env::set_var("ORACLE_LIB_DIR", "/from/env");
        let cfg = AppConfig {
            oracle_lib_dir: Some("/from/config".to_string()),
            ..Default::default()
        };
        assert_eq!(
            cfg.resolved_oracle_lib_dir(),
            Some(PathBuf::from("/from/env"))
        );
        std::env::remove_var("ORACLE_LIB_DIR");
    }

    #[test]
    fn test_tilde_expansion() {
        let home = home_dir();
        let cfg = AppConfig {
            oracle_lib_dir: Some("~/Oracle/instantclient".to_string()),
            ..Default::default()
        };
        let original = std::env::var("ORACLE_LIB_DIR").ok();
        std::env::remove_var("ORACLE_LIB_DIR");

        let resolved = cfg.resolved_oracle_lib_dir().unwrap();
        assert_eq!(resolved, home.join("Oracle/instantclient"));

        if let Some(v) = original {
            std::env::set_var("ORACLE_LIB_DIR", v);
        }
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = load_app_config(dir.path()).unwrap();
        assert!(cfg.oracle_lib_dir.is_none());
    }

    #[test]
    fn test_load_valid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yml");
        std::fs::write(
            &path,
            "oracle_lib_dir: /opt/oracle/ic\nduckdb_lib_dir: /usr/local/lib\n",
        )
        .unwrap();
        let cfg = load_app_config(dir.path()).unwrap();
        assert_eq!(cfg.oracle_lib_dir.as_deref(), Some("/opt/oracle/ic"));
        assert_eq!(cfg.duckdb_lib_dir.as_deref(), Some("/usr/local/lib"));
    }

    #[test]
    fn test_load_invalid_yaml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yml");
        std::fs::write(&path, b": broken: yaml:::").unwrap();
        assert!(load_app_config(dir.path()).is_err());
    }

    #[test]
    fn test_unknown_fields_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yml");
        std::fs::write(&path, b"unknown_key: value\n").unwrap();
        assert!(
            load_app_config(dir.path()).is_err(),
            "Unknown fields should be rejected by deny_unknown_fields"
        );
    }
}
