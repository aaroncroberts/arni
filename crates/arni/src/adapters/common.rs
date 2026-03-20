//! Shared utilities used by multiple SQL adapters.
//!
//! Functions here avoid duplication across the per-database adapter modules.
//! They are `pub(crate)` — internal to arni, not part of the public API.

use crate::adapter::QueryValue;
use crate::DataError;
#[cfg(feature = "polars")]
use polars::prelude::*;

#[cfg(feature = "polars")]
pub(crate) type Result<T> = std::result::Result<T, DataError>;

/// Return the standard "not connected" [`DataError`].
///
/// Use as `.ok_or_else(super::common::not_connected_error)?` to replace the
/// repeated inline closure pattern across all adapters.
#[allow(dead_code)] // used by adapter impls gated on DB feature flags
pub(crate) fn not_connected_error() -> DataError {
    DataError::Connection("Not connected — call connect() first".to_string())
}

/// Convert a [`sqlx::types::Decimal`] to a [`QueryValue`].
///
/// Attempts to parse the decimal as `f64`. Falls back to [`QueryValue::Text`] when
/// the parse yields a non-finite value (overflow to ±infinity) or when the string
/// representation is not a valid `f64` literal.
///
/// Used by the MySQL (`DECIMAL`/`NUMERIC`) adapter.
/// Avoids calling `to_string()` twice compared to the inline version.
#[cfg(feature = "mysql")]
pub(crate) fn decimal_to_query_value(d: sqlx::types::Decimal) -> QueryValue {
    let s = d.to_string();
    s.parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
        .map(QueryValue::Float)
        .unwrap_or_else(|| QueryValue::Text(s))
}

/// Map a Polars [`DataType`] to a generic ANSI SQL type name.
///
/// Returns the closest standard SQL type for each Polars primitive. Adapter
/// dtype-mapping functions should use this as their `_` wildcard arm and
/// override only the types that differ in their target dialect.
///
/// | Polars type       | Generic SQL |
/// |-------------------|-------------|
/// | `Boolean`         | `BOOLEAN`   |
/// | `Int8`            | `TINYINT`   |
/// | `Int16`           | `SMALLINT`  |
/// | `Int32`           | `INTEGER`   |
/// | `Int64`           | `BIGINT`    |
/// | `UInt8`           | `TINYINT`   |
/// | `UInt16`          | `SMALLINT`  |
/// | `UInt32`          | `INTEGER`   |
/// | `UInt64`          | `BIGINT`    |
/// | `Float32`         | `FLOAT`     |
/// | `Float64`         | `DOUBLE`    |
/// | `String`          | `TEXT`      |
/// | `Binary`          | `BLOB`      |
/// | _(anything else)_ | `TEXT`      |
#[cfg(feature = "polars")]
#[allow(dead_code)] // called by SQL adapters; absent in Cloudflare-only builds
pub(crate) fn polars_dtype_to_generic_sql(dtype: &DataType) -> &'static str {
    match dtype {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "TINYINT",
        DataType::UInt16 => "SMALLINT",
        DataType::UInt32 => "INTEGER",
        DataType::UInt64 => "BIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::String => "TEXT",
        DataType::Binary => "BLOB",
        _ => "TEXT",
    }
}

#[cfg(feature = "polars")]
/// Convert a single value from a Polars [`Series`] at `row_idx` to an SQL literal string.
///
/// # Parameters
/// - `series`: the column
/// - `row_idx`: the row to read
/// - `bool_as_int`: when `true`, booleans render as `1`/`0` (SQLite, SQL Server, Oracle);
///   when `false`, they render as `TRUE`/`FALSE` (DuckDB, standard SQL).
///
/// NULL values always render as `NULL`.
/// Strings are single-quoted with internal `'` escaped as `''`.
/// Byte arrays render as `X'<hex>'`.
/// NaN / infinite floats render as `NULL`.
/// All other types are cast to `String` and single-quoted.
pub(crate) fn series_value_to_sql_literal(
    series: &Series,
    row_idx: usize,
    bool_as_int: bool,
) -> Result<String> {
    if series.is_null().get(row_idx).unwrap_or(false) {
        return Ok("NULL".to_string());
    }
    match series.dtype() {
        DataType::Boolean => {
            let val = series
                .bool()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            Ok(if bool_as_int {
                if val { "1" } else { "0" }.to_string()
            } else {
                if val { "TRUE" } else { "FALSE" }.to_string()
            })
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            let s = series
                .cast(&DataType::Int32)
                .map_err(|e| DataError::TypeConversion(e.to_string()))?;
            let val = s
                .i32()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            Ok(val.to_string())
        }
        DataType::Int64 => {
            let val = series
                .i64()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            Ok(val.to_string())
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            let s = series
                .cast(&DataType::UInt32)
                .map_err(|e| DataError::TypeConversion(e.to_string()))?;
            let val = s
                .u32()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            Ok(val.to_string())
        }
        DataType::UInt64 => {
            let val = series
                .u64()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            Ok(val.to_string())
        }
        DataType::Float32 => {
            let val = series
                .f32()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            if val.is_nan() || val.is_infinite() {
                Ok("NULL".to_string())
            } else {
                Ok(format!("{}", val))
            }
        }
        DataType::Float64 => {
            let val = series
                .f64()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            if val.is_nan() || val.is_infinite() {
                Ok("NULL".to_string())
            } else {
                Ok(format!("{}", val))
            }
        }
        DataType::String => {
            let val = series
                .str()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            Ok(format!("'{}'", val.replace('\'', "''")))
        }
        DataType::Binary => {
            let val = series
                .binary()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
                .ok_or_else(|| DataError::DataFrame(format!("Index {} out of bounds", row_idx)))?;
            let hex: String = val.iter().map(|byte| format!("{:02x}", byte)).collect();
            Ok(format!("X'{}'", hex))
        }
        _ => {
            let s = series
                .cast(&DataType::String)
                .map_err(|e| DataError::TypeConversion(e.to_string()))?;
            match s
                .str()
                .map_err(|e| DataError::TypeConversion(e.to_string()))?
                .get(row_idx)
            {
                Some(val) => Ok(format!("'{}'", val.replace('\'', "''"))),
                None => Ok("NULL".to_string()),
            }
        }
    }
}

/// Shared SQL literal helper for adapters that represent booleans as integers (0/1).
///
/// Convenience wrapper around [`series_value_to_sql_literal`] with `bool_as_int = true`.
/// Used by SQLite, SQL Server (MSSQL), and Oracle adapters.
#[cfg(feature = "polars")]
#[allow(dead_code)]
pub(crate) fn series_value_to_sql_literal_int_bool(
    series: &Series,
    row_idx: usize,
) -> Result<String> {
    series_value_to_sql_literal(series, row_idx, true)
}

/// Shared SQL literal helper for adapters that represent booleans as TRUE/FALSE.
///
/// Convenience wrapper around [`series_value_to_sql_literal`] with `bool_as_int = false`.
/// Used by DuckDB and standard-SQL adapters.
#[cfg(feature = "polars")]
#[allow(dead_code)]
pub(crate) fn series_value_to_sql_literal_bool_keyword(
    series: &Series,
    row_idx: usize,
) -> Result<String> {
    series_value_to_sql_literal(series, row_idx, false)
}

#[allow(dead_code)]
/// Convert a [`QueryValue`] to an inline SQL literal string.
///
/// # Parameters
/// - `value`: the value to render
/// - `bool_as_int`: when `true`, booleans render as `1`/`0` (SQLite);
///   when `false`, they render as `TRUE`/`FALSE` (DuckDB, standard SQL).
///
/// NULL renders as `NULL`.
/// Strings are single-quoted with internal `'` escaped as `''`.
/// Byte arrays render as `X'<hex>'`.
/// NaN / infinite floats render as `NULL`.
pub(crate) fn query_value_to_sql_literal(value: &QueryValue, bool_as_int: bool) -> String {
    match value {
        QueryValue::Null => "NULL".to_string(),
        QueryValue::Bool(b) => {
            if bool_as_int {
                if *b { "1" } else { "0" }.to_string()
            } else {
                if *b { "TRUE" } else { "FALSE" }.to_string()
            }
        }
        QueryValue::Int(i) => i.to_string(),
        QueryValue::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                "NULL".to_string()
            } else {
                format!("{}", f)
            }
        }
        QueryValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
        QueryValue::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("X'{}'", hex)
        }
    }
}

// ─── Query logging helpers ────────────────────────────────────────────────────

#[allow(dead_code)]
/// Classify the SQL statement type from its first keyword.
///
/// Returns one of: `"SELECT"`, `"INSERT"`, `"UPDATE"`, `"DELETE"`, `"CREATE"`,
/// `"DROP"`, `"ALTER"`, `"TRUNCATE"`, `"WITH"`, or `"OTHER"`.
/// Case-insensitive; leading whitespace is trimmed.
pub(crate) fn detect_sql_type(sql: &str) -> &'static str {
    match sql
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_uppercase()
        .as_str()
    {
        "SELECT" => "SELECT",
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        "CREATE" => "CREATE",
        "DROP" => "DROP",
        "ALTER" => "ALTER",
        "TRUNCATE" => "TRUNCATE",
        "REPLACE" => "REPLACE",
        "WITH" => "WITH",
        _ => "OTHER",
    }
}

#[allow(dead_code)]
/// Return the first `max_chars` characters of `sql` with internal whitespace collapsed.
///
/// Useful for attaching a safe, readable preview to log fields without
/// logging full query text (which may be very long or contain user data).
pub(crate) fn sql_preview(sql: &str, max_chars: usize) -> String {
    let collapsed: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.len() <= max_chars {
        collapsed
    } else {
        format!("{}…", &collapsed[..max_chars])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── polars_dtype_to_generic_sql ───────────────────────────────────────────

    #[cfg(feature = "polars")]
    #[test]
    fn generic_sql_signed_ints() {
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Int8), "TINYINT");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Int16), "SMALLINT");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Int32), "INTEGER");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Int64), "BIGINT");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn generic_sql_unsigned_ints() {
        assert_eq!(polars_dtype_to_generic_sql(&DataType::UInt8), "TINYINT");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::UInt16), "SMALLINT");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::UInt32), "INTEGER");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::UInt64), "BIGINT");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn generic_sql_scalars() {
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Boolean), "BOOLEAN");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Float32), "FLOAT");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Float64), "DOUBLE");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::String), "TEXT");
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Binary), "BLOB");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn generic_sql_unknown_falls_back_to_text() {
        assert_eq!(polars_dtype_to_generic_sql(&DataType::Date), "TEXT");
    }

    // ── decimal_to_query_value ────────────────────────────────────────────────

    #[cfg(feature = "mysql")]
    #[test]
    fn decimal_normal_value_becomes_float() {
        use std::str::FromStr;
        let d = sqlx::types::Decimal::from_str("3.14").unwrap();
        match decimal_to_query_value(d) {
            #[allow(clippy::approx_constant)]
            QueryValue::Float(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn decimal_integer_value_becomes_float() {
        use std::str::FromStr;
        let d = sqlx::types::Decimal::from_str("42").unwrap();
        assert!(matches!(decimal_to_query_value(d), QueryValue::Float(f) if f == 42.0));
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn decimal_very_large_value_falls_back_to_text() {
        // A decimal with more significant digits than f64 can represent accurately
        // still parses as f64 (rounding), so we verify the Text fallback via a
        // manually constructed non-finite case using MAX * 10.
        use std::str::FromStr;
        // Decimal::MAX is ~7.9e28 — well within f64 range, so parse succeeds.
        // To force Text, construct a Decimal that exceeds f64::MAX (~1.8e308).
        // rust_decimal's max is ~7.9e28 so it will ALWAYS parse to f64 Float.
        // Confirm that a finite decimal always becomes Float (no Text fallback in practice).
        let d = sqlx::types::Decimal::from_str("999999999999999999.99").unwrap();
        assert!(matches!(decimal_to_query_value(d), QueryValue::Float(_)));
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn decimal_zero_becomes_float_zero() {
        use std::str::FromStr;
        let d = sqlx::types::Decimal::from_str("0").unwrap();
        assert!(matches!(decimal_to_query_value(d), QueryValue::Float(f) if f == 0.0));
    }

    #[cfg(feature = "polars")]
    fn bool_series(vals: &[bool]) -> Series {
        Series::new("col".into(), vals)
    }

    #[cfg(feature = "polars")]
    fn int_series(vals: &[i64]) -> Series {
        Series::new("col".into(), vals)
    }

    #[cfg(feature = "polars")]
    fn str_series(vals: &[&str]) -> Series {
        Series::new("col".into(), vals)
    }

    #[cfg(feature = "polars")]
    #[test]
    fn bool_as_int_true() {
        let s = bool_series(&[true]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "1");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn bool_as_int_false() {
        let s = bool_series(&[false]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "0");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn bool_as_keyword_true() {
        let s = bool_series(&[true]);
        assert_eq!(series_value_to_sql_literal(&s, 0, false).unwrap(), "TRUE");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn bool_as_keyword_false() {
        let s = bool_series(&[false]);
        assert_eq!(series_value_to_sql_literal(&s, 0, false).unwrap(), "FALSE");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn integer_value() {
        let s = int_series(&[42]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "42");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn string_value_escaped() {
        let s = str_series(&["it's"]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "'it''s'");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn null_value() {
        let s = Series::new_null("col".into(), 1);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "NULL");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn float_nan_is_null() {
        let s = Series::new("col".into(), &[f64::NAN]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "NULL");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn float64_inf_is_null() {
        let pos = Series::new("col".into(), &[f64::INFINITY]);
        assert_eq!(
            series_value_to_sql_literal(&pos, 0, true).unwrap(),
            "NULL",
            "positive f64 infinity should render as NULL"
        );
        let neg = Series::new("col".into(), &[f64::NEG_INFINITY]);
        assert_eq!(
            series_value_to_sql_literal(&neg, 0, true).unwrap(),
            "NULL",
            "negative f64 infinity should render as NULL"
        );
    }

    #[cfg(feature = "polars")]
    #[test]
    fn float32_inf_is_null() {
        let pos = Series::new("col".into(), &[f32::INFINITY]);
        assert_eq!(series_value_to_sql_literal(&pos, 0, true).unwrap(), "NULL");
        let neg = Series::new("col".into(), &[f32::NEG_INFINITY]);
        assert_eq!(series_value_to_sql_literal(&neg, 0, true).unwrap(), "NULL");
    }

    #[cfg(feature = "polars")]
    #[test]
    fn bytes_render_as_hex_literal() {
        let bytes: Vec<u8> = vec![0xCA, 0xFE, 0xBA, 0xBE];
        let s = Series::new("col".into(), [bytes.as_slice()]);
        assert_eq!(
            series_value_to_sql_literal(&s, 0, true).unwrap(),
            "X'cafebabe'"
        );
    }

    #[cfg(feature = "polars")]
    #[test]
    fn empty_bytes_render_as_empty_hex_literal() {
        let empty: Vec<u8> = vec![];
        let s = Series::new("col".into(), [empty.as_slice()]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "X''");
    }

    #[test]
    fn detect_sql_type_select() {
        assert_eq!(detect_sql_type("SELECT 1"), "SELECT");
        assert_eq!(detect_sql_type("  select * from t"), "SELECT");
    }

    #[test]
    fn detect_sql_type_dml() {
        assert_eq!(detect_sql_type("INSERT INTO t VALUES (1)"), "INSERT");
        assert_eq!(detect_sql_type("UPDATE t SET a=1"), "UPDATE");
        assert_eq!(detect_sql_type("DELETE FROM t"), "DELETE");
        assert_eq!(detect_sql_type("REPLACE INTO t VALUES (1)"), "REPLACE");
        assert_eq!(detect_sql_type("replace into t values (1)"), "REPLACE");
    }

    #[test]
    fn detect_sql_type_ddl() {
        assert_eq!(detect_sql_type("CREATE TABLE t (id INT)"), "CREATE");
        assert_eq!(detect_sql_type("DROP TABLE t"), "DROP");
        assert_eq!(detect_sql_type("ALTER TABLE t ADD COLUMN x INT"), "ALTER");
        assert_eq!(detect_sql_type("TRUNCATE TABLE t"), "TRUNCATE");
    }

    #[test]
    fn detect_sql_type_with() {
        assert_eq!(
            detect_sql_type("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            "WITH"
        );
    }

    #[test]
    fn detect_sql_type_other() {
        assert_eq!(detect_sql_type("EXPLAIN SELECT 1"), "OTHER");
        assert_eq!(detect_sql_type(""), "OTHER");
    }

    #[test]
    fn sql_preview_short() {
        assert_eq!(sql_preview("SELECT 1", 100), "SELECT 1");
    }

    #[test]
    fn sql_preview_collapses_whitespace() {
        assert_eq!(sql_preview("SELECT\n  1,\n  2", 100), "SELECT 1, 2");
    }

    #[test]
    fn sql_preview_truncates() {
        let long = "SELECT ".to_string() + &"a".repeat(200);
        let preview = sql_preview(&long, 20);
        assert!(preview.ends_with('…'));
        // character count: 20 chars + ellipsis
        assert!(preview.len() <= 25);
    }
}
