//! JSON output helpers for agent-readable `--json` mode.
//!
//! Every command that accepts `--json` uses this module to emit a consistent
//! envelope so agents can parse `arni` output without per-command format
//! knowledge.
//!
//! ## Shapes
//!
//! | Context | Shape |
//! |---------|-------|
//! | Generic success | `{"ok":true, …command-specific fields…}` |
//! | Query result | `{"ok":true,"columns":[…],"rows":[[…]]}` |
//! | Error | `{"ok":false,"error":{"code":"…","message":"…"}}` |
//!
//! Fields are inlined at the top level (not nested under a `data` key) so the
//! shape matches the daemon's NDJSON query-response protocol.

#[cfg(feature = "polars")]
use arni::polars::prelude::{AnyValue, DataFrame};
use arni::QueryResult;
use serde_json::{json, Value};

// ─── Public helpers ───────────────────────────────────────────────────────────

/// Build the query-result envelope from a Polars DataFrame:
/// `{"ok": true, "columns": [...], "rows": [[...]]}`.
///
/// Only available when the `polars` feature is enabled.
#[cfg(feature = "polars")]
pub fn query_result(df: &DataFrame) -> Value {
    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let row: Vec<Value> = columns
            .iter()
            .map(|name| {
                df.column(name)
                    .ok()
                    .and_then(|s| s.get(i).ok())
                    .map(anyvalue_to_json)
                    .unwrap_or(Value::Null)
            })
            .collect();
        rows.push(row);
    }

    json!({ "ok": true, "columns": columns, "rows": rows })
}

/// Build the query-result envelope from a lightweight [`QueryResult`]:
/// `{"ok": true, "columns": [...], "rows": [[...]]}`.
///
/// Available regardless of the `polars` feature.
pub fn query_result_from_qr(qr: &QueryResult) -> Value {
    let columns: Vec<String> = qr.columns.clone();
    let rows: Vec<Vec<Value>> = qr
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| json!(v.to_string()))
                .collect()
        })
        .collect();
    json!({ "ok": true, "columns": columns, "rows": rows })
}

/// Build an error envelope:
/// `{"ok": false, "error": {"code": "…", "message": "…"}}`.
pub fn error(code: &str, message: &str) -> Value {
    json!({ "ok": false, "error": { "code": code, "message": message } })
}

/// Print a JSON value as a single line to stdout.
///
/// Callers should use this instead of `println!("{}", v)` so all JSON output
/// uses the same compact single-line format.
pub fn emit(v: &Value) {
    println!("{v}");
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Convert a Polars [`AnyValue`] to a [`serde_json::Value`].
///
/// Numeric, boolean, and string variants map to their natural JSON types.
/// All other variants (dates, durations, nested lists, …) fall back to their
/// `Display` representation as a JSON string.
#[cfg(feature = "polars")]
fn anyvalue_to_json(v: AnyValue<'_>) -> Value {
    match v {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => json!(b),
        AnyValue::Int8(i) => json!(i),
        AnyValue::Int16(i) => json!(i),
        AnyValue::Int32(i) => json!(i),
        AnyValue::Int64(i) => json!(i),
        AnyValue::UInt8(u) => json!(u),
        AnyValue::UInt16(u) => json!(u),
        AnyValue::UInt32(u) => json!(u),
        AnyValue::UInt64(u) => json!(u),
        AnyValue::Float32(f) => json!(f),
        AnyValue::Float64(f) => json!(f),
        AnyValue::String(s) => json!(s),
        AnyValue::StringOwned(ref s) => json!(s.as_str()),
        _ => Value::String(format!("{v}")),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_shape() {
        let v = error("CONNECT_FAILED", "cannot reach host");
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"]["code"], "CONNECT_FAILED");
        assert_eq!(v["error"]["message"], "cannot reach host");
    }

    #[cfg(feature = "polars")]
    mod polars_tests {
        use super::super::*;
        use arni::polars::prelude::*;

        #[test]
        fn test_query_result_columns_and_rows() {
            let df = df![
                "id"   => [1i64, 2, 3],
                "name" => ["Alice", "Bob", "Carol"]
            ]
            .unwrap();
            let v = query_result(&df);
            assert_eq!(v["ok"], true);
            let cols = v["columns"].as_array().unwrap();
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0], "id");
            let rows = v["rows"].as_array().unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][0], 1);
            assert_eq!(rows[0][1], "Alice");
        }

        #[test]
        fn test_query_result_empty_dataframe() {
            let df = DataFrame::empty();
            let v = query_result(&df);
            assert_eq!(v["ok"], true);
            assert_eq!(v["columns"].as_array().unwrap().len(), 0);
            assert_eq!(v["rows"].as_array().unwrap().len(), 0);
        }

        #[test]
        fn test_anyvalue_null_maps_to_json_null() {
            let v = anyvalue_to_json(AnyValue::Null);
            assert!(v.is_null());
        }

        #[test]
        fn test_anyvalue_bool_maps_correctly() {
            assert_eq!(anyvalue_to_json(AnyValue::Boolean(true)), json!(true));
            assert_eq!(anyvalue_to_json(AnyValue::Boolean(false)), json!(false));
        }

        #[test]
        fn test_anyvalue_int64_maps_to_json_number() {
            assert_eq!(anyvalue_to_json(AnyValue::Int64(42)), json!(42i64));
        }

        #[test]
        fn test_anyvalue_float64_maps_to_json_number() {
            let v = anyvalue_to_json(AnyValue::Float64(1.5));
            assert_eq!(v, json!(1.5f64));
        }

        #[test]
        fn test_anyvalue_string_maps_to_json_string() {
            assert_eq!(anyvalue_to_json(AnyValue::String("hello")), json!("hello"));
        }
    }
}
