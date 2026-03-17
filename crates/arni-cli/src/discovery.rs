//! Static discovery endpoints for agent/MCP self-discovery.
//!
//! Three JSON output types are supported:
//!
//! * [`list_tools`] — every command with its argument schema
//! * [`capabilities`] — supported database types and feature set
//! * [`schema`] — input/output shape for a named command
//!
//! All output is derived from the CLI's own definition, so it stays in sync
//! with the implementation automatically as commands are added.

use crate::VERSION;
use serde_json::{json, Value};

// ─── Public API ───────────────────────────────────────────────────────────────

/// Return a JSON array describing every available command with its arguments.
///
/// Shape: `[{name, description, args:[{name, type, required, description}]}]`
pub fn list_tools() -> Value {
    json!([
        {
            "name": "connect",
            "description": "Connect to a database profile and display server info and table count. Use --json for machine-readable output.",
            "args": [
                { "name": "--profile", "type": "string", "required": true,  "description": "Connection profile name from ~/.arni/connections.yml" },
                { "name": "--json",    "type": "flag",   "required": false, "description": "Emit {ok, server_type, version, table_count} envelope" },
            ]
        },
        {
            "name": "query",
            "description": "Execute a SQL (or MQL) query and return results. Use --json for {ok, columns, rows} envelope matching the daemon protocol.",
            "args": [
                { "name": "query",     "type": "string", "required": true,  "description": "SQL query to execute" },
                { "name": "--profile", "type": "string", "required": true,  "description": "Connection profile name" },
                { "name": "--format",  "type": "string", "required": false, "description": "Output format: table (default), json, csv, xml" },
                { "name": "--json",    "type": "flag",   "required": false, "description": "Emit {ok, columns:[], rows:[[]]} — overrides --format" },
            ]
        },
        {
            "name": "metadata",
            "description": "Inspect database structure: tables, columns, views, indexes, schemas. Combine flags; use --json for structured output.",
            "args": [
                { "name": "--profile",     "type": "string", "required": true,  "description": "Connection profile name" },
                { "name": "--tables",      "type": "flag",   "required": false, "description": "List all tables" },
                { "name": "--columns",     "type": "flag",   "required": false, "description": "Describe columns (requires --table)" },
                { "name": "--schemas",     "type": "flag",   "required": false, "description": "List databases/schemas" },
                { "name": "--views",       "type": "flag",   "required": false, "description": "List views" },
                { "name": "--indexes",     "type": "flag",   "required": false, "description": "List indexes (requires --table)" },
                { "name": "--table",       "type": "string", "required": false, "description": "Table name for --columns and --indexes" },
                { "name": "--search",      "type": "string", "required": false, "description": "Search tables by name fragment" },
                { "name": "--search-mode", "type": "string", "required": false, "description": "Match strategy: contains (default), starts, ends" },
                { "name": "--json",        "type": "flag",   "required": false, "description": "Emit {ok, tables/columns/views/indexes/…} envelope" },
            ]
        },
        {
            "name": "export",
            "description": "Execute a query and write results to a file. Supports csv, json, xml, parquet, excel.",
            "args": [
                { "name": "query",     "type": "string", "required": true,  "description": "SQL query for export" },
                { "name": "--profile", "type": "string", "required": true,  "description": "Connection profile name" },
                { "name": "--format",  "type": "string", "required": false, "description": "Output format: json (default), csv, xml, parquet, excel" },
                { "name": "--output",  "type": "string", "required": true,  "description": "Destination file path" },
                { "name": "--json",    "type": "flag",   "required": false, "description": "Emit {ok, file, rows, format} envelope" },
            ]
        },
        {
            "name": "config",
            "description": "Manage connection profiles stored in ~/.arni/connections.yml.",
            "args": [
                {
                    "name": "add",
                    "type": "subcommand",
                    "required": false,
                    "description": "Add or update a profile",
                    "sub_args": [
                        { "name": "--name",     "type": "string", "required": true,  "description": "Profile name" },
                        { "name": "--type",     "type": "string", "required": true,  "description": "Database type: postgres|mysql|sqlite|mongodb|sqlserver|oracle|duckdb" },
                        { "name": "--host",     "type": "string", "required": false, "description": "Hostname (required for network databases)" },
                        { "name": "--port",     "type": "number", "required": false, "description": "Port (defaults to database standard)" },
                        { "name": "--database", "type": "string", "required": true,  "description": "Database name, or file path / :memory: for embedded" },
                        { "name": "--username", "type": "string", "required": false, "description": "Username" },
                        { "name": "--password", "type": "string", "required": false, "description": "Password (omit to prompt at runtime)" },
                        { "name": "--ssl",      "type": "flag",   "required": false, "description": "Enable SSL/TLS" },
                        { "name": "--param",    "type": "string", "required": false, "description": "KEY=VALUE extra driver parameter (repeatable)" },
                    ]
                },
                { "name": "list",   "type": "subcommand", "required": false, "description": "List all configured profiles" },
                { "name": "remove", "type": "subcommand", "required": false, "description": "Remove a named profile" },
                { "name": "test",   "type": "subcommand", "required": false, "description": "Test TCP/file reachability for a profile" },
            ]
        },
        {
            "name": "daemon",
            "description": "Start a persistent background daemon on a Unix domain socket. Accepts NDJSON commands; use for long-lived connections.",
            "args": [
                { "name": "--socket", "type": "string", "required": false, "description": "Unix socket path (default: /tmp/arni.sock)" },
            ]
        },
        {
            "name": "dev",
            "description": "Manage development database containers via podman-compose.",
            "args": [
                { "name": "start",  "type": "subcommand", "required": false, "description": "Start all dev containers" },
                { "name": "stop",   "type": "subcommand", "required": false, "description": "Stop all dev containers" },
                { "name": "status", "type": "subcommand", "required": false, "description": "Show container status" },
                { "name": "logs",   "type": "subcommand", "required": false, "description": "Show container logs (--service to filter)" },
                { "name": "clean",  "type": "subcommand", "required": false, "description": "Remove containers and optionally volumes (--volumes)" },
            ]
        },
    ])
}

/// Return a JSON object describing supported database types and features.
///
/// Shape: `{version, database_types:[], features:[]}`
pub fn capabilities() -> Value {
    json!({
        "version": VERSION,
        "database_types": [
            { "id": "postgres",    "name": "PostgreSQL",           "embedded": false },
            { "id": "mysql",       "name": "MySQL / MariaDB",      "embedded": false },
            { "id": "sqlite",      "name": "SQLite",               "embedded": true  },
            { "id": "mongodb",     "name": "MongoDB",              "embedded": false },
            { "id": "sqlserver",   "name": "Microsoft SQL Server", "embedded": false },
            { "id": "oracle",      "name": "Oracle Database",      "embedded": false },
            { "id": "duckdb",      "name": "DuckDB",               "embedded": true  },
        ],
        "features": [
            "execute_query",
            "export_dataframe",
            "import_dataframe",
            "list_tables",
            "describe_table",
            "list_databases",
            "get_views",
            "get_indexes",
            "get_foreign_keys",
            "get_server_info",
            "list_stored_procedures",
            "bulk_insert",
            "bulk_update",
            "bulk_delete",
            "find_tables",
            "test_connection",
            "connection_profiles",
            "daemon_mode",
            "json_output",
            "dataframe_export_csv",
            "dataframe_export_json",
            "dataframe_export_parquet",
            "dataframe_export_excel",
            "dataframe_export_xml",
        ]
    })
}

/// Return the input/output JSON schema for a named command, or `None` if unknown.
///
/// Shape: `{command, input:{…properties…}, output:{…properties…}}`
pub fn schema(command: &str) -> Option<Value> {
    let v = match command {
        "connect" => json!({
            "command": "connect",
            "input": {
                "--profile": { "type": "string", "required": true, "description": "Connection profile name" },
                "--json":    { "type": "flag",   "required": false }
            },
            "output": {
                "ok":          { "type": "boolean" },
                "server_type": { "type": "string",  "description": "Database engine name" },
                "version":     { "type": "string",  "description": "Server version string" },
                "extra_info":  { "type": "object",  "description": "Engine-specific metadata" },
                "table_count": { "type": "integer", "description": "Number of tables visible to this user" }
            }
        }),
        "query" => json!({
            "command": "query",
            "input": {
                "query":     { "type": "string", "required": true,  "description": "SQL query string" },
                "--profile": { "type": "string", "required": true,  "description": "Connection profile name" },
                "--format":  { "type": "string", "required": false, "enum": ["table","json","csv","xml"], "default": "table" },
                "--json":    { "type": "flag",   "required": false, "description": "Overrides --format; emits agent envelope" }
            },
            "output": {
                "ok":      { "type": "boolean" },
                "columns": { "type": "array", "items": { "type": "string" } },
                "rows":    { "type": "array",  "items": { "type": "array" }, "description": "Row-major; values are JSON primitives or null" }
            }
        }),
        "metadata" => json!({
            "command": "metadata",
            "input": {
                "--profile":     { "type": "string",  "required": true  },
                "--tables":      { "type": "flag",    "required": false },
                "--columns":     { "type": "flag",    "required": false, "note": "requires --table" },
                "--schemas":     { "type": "flag",    "required": false },
                "--views":       { "type": "flag",    "required": false },
                "--indexes":     { "type": "flag",    "required": false, "note": "requires --table" },
                "--table":       { "type": "string",  "required": false },
                "--search":      { "type": "string",  "required": false },
                "--search-mode": { "type": "string",  "required": false, "enum": ["contains","starts","ends"], "default": "contains" },
                "--json":        { "type": "flag",    "required": false }
            },
            "output": {
                "ok":        { "type": "boolean" },
                "tables":    { "type": "array",  "description": "Present when --tables or no flag; [{name}]" },
                "databases": { "type": "array",  "description": "Present when --schemas; list of schema names" },
                "views":     { "type": "array",  "description": "Present when --views; [{name, schema, definition}]" },
                "table":     { "type": "string", "description": "Present when --columns or --indexes" },
                "columns":   { "type": "array",  "description": "Present when --columns; [{name, data_type, nullable, is_primary_key, default_value}]" },
                "indexes":   { "type": "array",  "description": "Present when --indexes; [{name, columns, is_unique, is_primary, index_type}]" },
                "pattern":   { "type": "string", "description": "Present when --search" },
                "mode":      { "type": "string", "description": "Present when --search; contains|starts|ends" }
            }
        }),
        "export" => json!({
            "command": "export",
            "input": {
                "query":     { "type": "string", "required": true },
                "--profile": { "type": "string", "required": true },
                "--format":  { "type": "string", "required": false, "enum": ["json","csv","xml","parquet","excel"], "default": "json" },
                "--output":  { "type": "string", "required": true, "description": "Destination file path" },
                "--json":    { "type": "flag",   "required": false }
            },
            "output": {
                "ok":     { "type": "boolean" },
                "file":   { "type": "string",  "description": "Absolute path to the written file" },
                "rows":   { "type": "integer", "description": "Number of rows exported" },
                "format": { "type": "string" }
            }
        }),
        "config" => json!({
            "command": "config",
            "input": {
                "subcommand": { "type": "string", "required": true, "enum": ["add","list","remove","test"] }
            },
            "output": {
                "add":    { "ok": "boolean", "name": "string", "saved_to": "string" },
                "list":   { "ok": "boolean", "profiles": "array of {name, type, host, port, database, ssl}" },
                "remove": { "ok": "boolean", "name": "string" },
                "test":   { "ok": "boolean", "name": "string", "detail": "string" }
            }
        }),
        "daemon" => json!({
            "command": "daemon",
            "input": {
                "--socket": { "type": "string", "required": false, "default": "/tmp/arni.sock" }
            },
            "output": {
                "description": "Daemon runs until interrupted. Accepts NDJSON on the socket.",
                "protocol": "NDJSON: {command:'query', sql:'…', profile:'…'} → {ok:bool, columns:[], rows:[[]]}"
            }
        }),
        "dev" => json!({
            "command": "dev",
            "input": {
                "subcommand": { "type": "string", "required": true, "enum": ["start","stop","status","logs","clean"] },
                "--service":  { "type": "string", "required": false, "description": "Filter logs to a single service (logs subcommand)" },
                "--volumes":  { "type": "flag",   "required": false, "description": "Also remove volumes (clean subcommand)" }
            },
            "output": {
                "description": "Passes through podman-compose stdout/stderr. No JSON envelope."
            }
        }),
        _ => return None,
    };
    Some(v)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_tools_is_array_of_seven_commands() {
        let v = list_tools();
        let arr = v.as_array().expect("list_tools must be an array");
        assert_eq!(arr.len(), 7);
    }

    #[test]
    fn test_list_tools_each_entry_has_required_fields() {
        for tool in list_tools().as_array().unwrap() {
            assert!(tool["name"].is_string(), "missing 'name' in tool");
            assert!(
                tool["description"].is_string(),
                "missing 'description' in tool"
            );
            assert!(tool["args"].is_array(), "missing 'args' in tool");
        }
    }

    #[test]
    fn test_capabilities_has_expected_keys() {
        let v = capabilities();
        assert!(v["version"].is_string());
        let types = v["database_types"].as_array().unwrap();
        assert_eq!(types.len(), 7);
        let features = v["features"].as_array().unwrap();
        assert!(!features.is_empty());
    }

    #[test]
    fn test_capabilities_version_matches_cargo_pkg() {
        let v = capabilities();
        assert_eq!(v["version"].as_str().unwrap(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_schema_query_has_input_output() {
        let v = schema("query").expect("query schema should exist");
        assert_eq!(v["command"], "query");
        assert!(v["input"].is_object());
        assert!(v["output"]["columns"].is_object());
    }

    #[test]
    fn test_schema_unknown_returns_none() {
        assert!(schema("nonexistent_command").is_none());
    }

    #[test]
    fn test_schema_all_known_commands() {
        for cmd in [
            "connect", "query", "metadata", "export", "config", "daemon", "dev",
        ] {
            assert!(schema(cmd).is_some(), "schema missing for command '{cmd}'");
        }
    }
}
