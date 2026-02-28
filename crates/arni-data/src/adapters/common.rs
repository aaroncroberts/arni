//! Shared utilities used by multiple SQL adapters.
//!
//! Functions here avoid duplication across the per-database adapter modules.
//! They are `pub(crate)` — internal to arni-data, not part of the public API.

use crate::adapter::QueryValue;
use crate::DataError;
use polars::prelude::*;

pub(crate) type Result<T> = std::result::Result<T, DataError>;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn bool_series(vals: &[bool]) -> Series {
        Series::new("col".into(), vals)
    }

    fn int_series(vals: &[i64]) -> Series {
        Series::new("col".into(), vals)
    }

    fn str_series(vals: &[&str]) -> Series {
        Series::new("col".into(), vals)
    }

    #[test]
    fn bool_as_int_true() {
        let s = bool_series(&[true]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "1");
    }

    #[test]
    fn bool_as_int_false() {
        let s = bool_series(&[false]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "0");
    }

    #[test]
    fn bool_as_keyword_true() {
        let s = bool_series(&[true]);
        assert_eq!(series_value_to_sql_literal(&s, 0, false).unwrap(), "TRUE");
    }

    #[test]
    fn bool_as_keyword_false() {
        let s = bool_series(&[false]);
        assert_eq!(series_value_to_sql_literal(&s, 0, false).unwrap(), "FALSE");
    }

    #[test]
    fn integer_value() {
        let s = int_series(&[42]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "42");
    }

    #[test]
    fn string_value_escaped() {
        let s = str_series(&["it's"]);
        assert_eq!(
            series_value_to_sql_literal(&s, 0, true).unwrap(),
            "'it''s'"
        );
    }

    #[test]
    fn null_value() {
        let s = Series::new_null("col".into(), 1);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "NULL");
    }

    #[test]
    fn float_nan_is_null() {
        let s = Series::new("col".into(), &[f64::NAN]);
        assert_eq!(series_value_to_sql_literal(&s, 0, true).unwrap(), "NULL");
    }
}
