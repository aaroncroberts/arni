//! Typed output abstraction for `--json` vs human-readable mode.
//!
//! Every command handler receives `&OutputFormatter` instead of a raw
//! `json_mode: bool`. This keeps the output-mode decision in one place
//! and eliminates repeated `if json_mode { … } else { … }` branches.
//!
//! ## Method cheat-sheet
//!
//! | Method | JSON mode | Plain mode |
//! |--------|-----------|------------|
//! | `info(msg)` | _(suppressed)_ | `println!("{msg}")` |
//! | `success(val, plain)` | emits `val` | `println!("✓ {plain}")` |
//! | `emit(val)` | emits `val` | _(suppressed)_ |
//! | `is_json()` | `true` | `false` |

use crate::json_output;
use colored::Colorize;
use serde_json::Value;

/// Wraps the `--json` flag and provides typed output methods for handlers.
///
/// Construct once in `main()` and pass `&OutputFormatter` to every handler.
pub struct OutputFormatter {
    json_mode: bool,
}

impl OutputFormatter {
    /// Create a new formatter. `json_mode` mirrors the `--json` CLI flag.
    pub fn new(json_mode: bool) -> Self {
        Self { json_mode }
    }

    /// Returns `true` when JSON output mode is active.
    ///
    /// Prefer the typed methods (`info`, `success`, `emit`) where possible.
    /// Use `is_json()` only when the two modes require fundamentally different
    /// computation (e.g. fetching extra server metadata only for JSON output).
    pub fn is_json(&self) -> bool {
        self.json_mode
    }

    /// Print a progress or status message — suppressed in JSON mode.
    ///
    /// Suitable for "Connecting to...", "Querying..." etc.
    /// The message is printed as-is, so callers can pre-apply color codes.
    pub fn info(&self, msg: impl std::fmt::Display) {
        if !self.json_mode {
            println!("{msg}");
        }
    }

    /// Emit a custom JSON value (JSON mode) or print `✓ {plain}` (plain mode).
    ///
    /// The `value` should include `"ok": true` at the top level so agents
    /// can parse it uniformly. The `plain` string is shown with a leading
    /// green `✓` prefix and may contain pre-applied color codes.
    pub fn success(&self, value: Value, plain: impl std::fmt::Display) {
        if self.json_mode {
            json_output::emit(&value);
        } else {
            println!("{} {plain}", "✓".bright_green());
        }
    }

    /// Emit a JSON value only — no plain-text equivalent.
    ///
    /// Use when the plain-mode output is handled separately (e.g. via
    /// direct `println!` calls or a table renderer).
    pub fn emit(&self, value: Value) {
        if self.json_mode {
            json_output::emit(&value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn is_json_reflects_constructor_arg() {
        assert!(OutputFormatter::new(true).is_json());
        assert!(!OutputFormatter::new(false).is_json());
    }

    #[test]
    fn info_suppressed_in_json_mode() {
        // info() in json mode must not panic and returns without printing.
        // We can't capture stdout easily here, but we verify no panic occurs.
        OutputFormatter::new(true).info("should be suppressed");
    }

    #[test]
    fn emit_suppressed_in_plain_mode() {
        OutputFormatter::new(false).emit(json!({"ok": true}));
    }
}
