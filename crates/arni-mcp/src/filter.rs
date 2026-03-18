//! JSON → [`FilterExpr`] / [`QueryValue`] parsing utilities for MCP tool inputs.
//!
//! Accepts the same filter DSL used by the arni CLI so any JSON filter
//! expression valid in the CLI is equally valid in an MCP tool call.
//!
//! # Filter JSON format
//!
//! | Shape | Meaning |
//! |-------|---------|
//! | `{"col": {"eq": value}}` | `col = value` |
//! | `{"col": {"ne": value}}` | `col <> value` |
//! | `{"col": {"gt": value}}` | `col > value` |
//! | `{"col": {"gte": value}}` | `col >= value` |
//! | `{"col": {"lt": value}}` | `col < value` |
//! | `{"col": {"lte": value}}` | `col <= value` |
//! | `{"col": {"in": [v1, v2]}}` | `col IN (v1, v2)` |
//! | `{"col": "is_null"}` | `col IS NULL` |
//! | `{"col": "is_not_null"}` | `col IS NOT NULL` |
//! | `{"and": [expr, ...]}` | `(expr AND ...)` |
//! | `{"or": [expr, ...]}` | `(expr OR ...)` |
//! | `{"not": expr}` | `NOT expr` |

use std::error::Error;

use arni::adapter::{FilterExpr, QueryValue};

// ─── Public API ───────────────────────────────────────────────────────────────

/// Convert a `serde_json::Value` to a [`QueryValue`].
pub fn json_to_query_value(v: &serde_json::Value) -> Result<QueryValue, Box<dyn Error>> {
    match v {
        serde_json::Value::Null => Ok(QueryValue::Null),
        serde_json::Value::Bool(b) => Ok(QueryValue::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(QueryValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(QueryValue::Float(f))
            } else {
                Err("Unsupported numeric value in filter".into())
            }
        }
        serde_json::Value::String(s) => Ok(QueryValue::Text(s.clone())),
        _ => Err(format!("Cannot convert JSON value to QueryValue: {}", v).into()),
    }
}

/// Parse a [`FilterExpr`] from a `serde_json::Value`.
///
/// See module-level docs for the accepted JSON shapes.
pub fn parse_filter_value(v: &serde_json::Value) -> Result<FilterExpr, Box<dyn Error>> {
    let obj = v.as_object().ok_or("Filter must be a JSON object")?;
    if obj.len() != 1 {
        return Err(format!(
            "Filter object must have exactly one key, got {}",
            obj.len()
        )
        .into());
    }
    let (key, val) = obj.iter().next().unwrap();
    match key.as_str() {
        "and" => {
            let arr = val.as_array().ok_or("'and' value must be a JSON array")?;
            let exprs: Result<Vec<FilterExpr>, _> = arr.iter().map(parse_filter_value).collect();
            Ok(FilterExpr::And(exprs?))
        }
        "or" => {
            let arr = val.as_array().ok_or("'or' value must be a JSON array")?;
            let exprs: Result<Vec<FilterExpr>, _> = arr.iter().map(parse_filter_value).collect();
            Ok(FilterExpr::Or(exprs?))
        }
        "not" => {
            let expr = parse_filter_value(val)?;
            Ok(FilterExpr::Not(Box::new(expr)))
        }
        col => {
            // Shorthand: {"col": "is_null"} / {"col": "is_not_null"}
            if let Some(s) = val.as_str() {
                return match s {
                    "is_null" | "isnull" => Ok(FilterExpr::IsNull(col.to_string())),
                    "is_not_null" | "isnotnull" => Ok(FilterExpr::IsNotNull(col.to_string())),
                    _ => Err(format!(
                        "Unknown string op '{}' for column '{}'. Use 'is_null' or 'is_not_null'.",
                        s, col
                    )
                    .into()),
                };
            }
            // Normal: {"col": {"op": value}}
            let op_obj = val.as_object().ok_or_else(|| {
                format!(
                    "Column '{}' value must be an object like {{\"eq\": value}} or \
                     the string \"is_null\"/\"is_not_null\"",
                    col
                )
            })?;
            if op_obj.len() != 1 {
                return Err(format!(
                    "Column '{}' filter must have exactly one op, got {}",
                    col,
                    op_obj.len()
                )
                .into());
            }
            let (op, op_val) = op_obj.iter().next().unwrap();
            match op.as_str() {
                "eq" => Ok(FilterExpr::Eq(
                    col.to_string(),
                    json_to_query_value(op_val)?,
                )),
                "ne" => Ok(FilterExpr::Ne(
                    col.to_string(),
                    json_to_query_value(op_val)?,
                )),
                "gt" => Ok(FilterExpr::Gt(
                    col.to_string(),
                    json_to_query_value(op_val)?,
                )),
                "gte" => Ok(FilterExpr::Gte(
                    col.to_string(),
                    json_to_query_value(op_val)?,
                )),
                "lt" => Ok(FilterExpr::Lt(
                    col.to_string(),
                    json_to_query_value(op_val)?,
                )),
                "lte" => Ok(FilterExpr::Lte(
                    col.to_string(),
                    json_to_query_value(op_val)?,
                )),
                "in" => {
                    let arr = op_val
                        .as_array()
                        .ok_or("'in' value must be a JSON array")?;
                    let values: Result<Vec<QueryValue>, _> =
                        arr.iter().map(json_to_query_value).collect();
                    Ok(FilterExpr::In(col.to_string(), values?))
                }
                _ => Err(format!(
                    "Unknown op '{}' for column '{}'. Valid: eq, ne, gt, gte, lt, lte, in",
                    op, col
                )
                .into()),
            }
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_query_value_types() {
        assert!(matches!(
            json_to_query_value(&serde_json::Value::Null).unwrap(),
            QueryValue::Null
        ));
        assert!(matches!(
            json_to_query_value(&serde_json::json!(true)).unwrap(),
            QueryValue::Bool(true)
        ));
        assert!(matches!(
            json_to_query_value(&serde_json::json!(42)).unwrap(),
            QueryValue::Int(42)
        ));
        assert!(matches!(
            json_to_query_value(&serde_json::json!(1.5)).unwrap(),
            QueryValue::Float(_)
        ));
        assert!(matches!(
            json_to_query_value(&serde_json::json!("hi")).unwrap(),
            QueryValue::Text(s) if s == "hi"
        ));
        assert!(json_to_query_value(&serde_json::json!([1, 2])).is_err());
    }

    #[test]
    fn test_parse_filter_eq() {
        let f =
            parse_filter_value(&serde_json::json!({"id": {"eq": 42}})).unwrap();
        assert!(matches!(f, FilterExpr::Eq(col, QueryValue::Int(42)) if col == "id"));
    }

    #[test]
    fn test_parse_filter_in() {
        let f =
            parse_filter_value(&serde_json::json!({"id": {"in": [1, 2, 3]}})).unwrap();
        assert!(matches!(f, FilterExpr::In(col, v) if col == "id" && v.len() == 3));
    }

    #[test]
    fn test_parse_filter_is_null() {
        let f = parse_filter_value(&serde_json::json!({"col": "is_null"})).unwrap();
        assert!(matches!(f, FilterExpr::IsNull(_)));
    }

    #[test]
    fn test_parse_filter_and() {
        let f = parse_filter_value(
            &serde_json::json!({"and": [{"a": {"eq": 1}}, {"b": {"gt": 0}}]}),
        )
        .unwrap();
        assert!(matches!(f, FilterExpr::And(v) if v.len() == 2));
    }

    #[test]
    fn test_parse_filter_not() {
        let f =
            parse_filter_value(&serde_json::json!({"not": {"active": {"eq": false}}})).unwrap();
        assert!(matches!(f, FilterExpr::Not(_)));
    }

    #[test]
    fn test_parse_filter_invalid_op_returns_error() {
        let r = parse_filter_value(&serde_json::json!({"col": {"between": [1, 5]}}));
        assert!(r.is_err());
    }
}
