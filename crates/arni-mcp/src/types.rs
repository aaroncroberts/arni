//! Input parameter and response types for all arni MCP tools.
//!
//! Every struct derives [`schemars::JsonSchema`] so `rmcp` can auto-generate
//! the JSON Schema exposed in `tools/list`. All types also derive
//! `Serialize`/`Deserialize` for wire encoding.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Query / Execute ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct QueryParams {
    /// Name of the connection profile (from ~/.arni/connections.yml).
    pub profile: String,
    /// SQL statement to execute and return rows for.
    pub sql: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExecuteParams {
    /// Name of the connection profile.
    pub profile: String,
    /// SQL statement to execute (INSERT / UPDATE / DELETE / DDL).
    pub sql: String,
}

// ── Table listing / describe ──────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProfileParams {
    /// Name of the connection profile.
    pub profile: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct TableParams {
    /// Name of the connection profile.
    pub profile: String,
    /// Table name.
    pub table: String,
    /// Schema / namespace (uses database default when omitted).
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SchemaParams {
    /// Name of the connection profile.
    pub profile: String,
    /// Schema / namespace (uses database default when omitted).
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct FindTablesParams {
    /// Name of the connection profile.
    pub profile: String,
    /// Pattern to match against table names.
    pub pattern: String,
    /// Match mode: "contains" (default), "starts", or "ends".
    pub mode: Option<String>,
    /// Schema / namespace (uses database default when omitted).
    pub schema: Option<String>,
}

// ── Bulk operations ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct BulkInsertParams {
    /// Name of the connection profile.
    pub profile: String,
    /// Target table name.
    pub table: String,
    /// Column names in the same order as each row's values.
    pub columns: Vec<String>,
    /// Rows to insert — each inner array maps to `columns`.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Schema / namespace (uses database default when omitted).
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct BulkUpdateParams {
    /// Name of the connection profile.
    pub profile: String,
    /// Target table name.
    pub table: String,
    /// Filter DSL expression — e.g. `{"id": {"eq": 42}}`.
    pub filter: serde_json::Value,
    /// Column→value map of fields to set — e.g. `{"name": "Alice"}`.
    pub values: serde_json::Value,
    /// Schema / namespace (uses database default when omitted).
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct BulkDeleteParams {
    /// Name of the connection profile.
    pub profile: String,
    /// Target table name.
    pub table: String,
    /// Filter DSL expression — e.g. `{"id": {"in": [1, 2, 3]}}`.
    pub filter: serde_json::Value,
    /// Schema / namespace (uses database default when omitted).
    pub schema: Option<String>,
}

// ── Generic response ──────────────────────────────────────────────────────────

/// Wraps any successful tool response value.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RowsAffected {
    pub rows_affected: u64,
}
