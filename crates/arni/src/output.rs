//! Lightweight output convenience layers for [`DbAdapter`](crate::DbAdapter).
//!
//! This module provides two optional output helpers that work on any adapter
//! implementing [`execute_query_stream`](crate::DbAdapter::execute_query_stream):
//!
//! | Feature flag | Method | Output |
//! |---|---|---|
//! | `json` | [`DbAdapterOutputExt::execute_query_json`] | `Vec<serde_json::Value>` |
//! | `csv` | [`DbAdapterOutputExt::execute_query_csv`] | writes to `impl std::io::Write` |
//!
//! Both are blanket impls over [`DbAdapter`] — adapters get them automatically once
//! they implement `execute_query_stream`.

use crate::adapter::DbAdapter;

// ─── QueryValue → JSON conversion ────────────────────────────────────────────

#[cfg(feature = "json")]
fn query_value_to_json(v: &crate::QueryValue) -> serde_json::Value {
    use serde_json::{json, Value};
    match v {
        crate::QueryValue::Null => Value::Null,
        crate::QueryValue::Bool(b) => json!(b),
        crate::QueryValue::Int(i) => json!(i),
        crate::QueryValue::Float(f) => json!(f),
        crate::QueryValue::Text(s) => json!(s),
        crate::QueryValue::Bytes(b) => {
            // Encode as a hex string — compact and unambiguous.
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            json!(hex)
        }
    }
}

// ─── Extension trait ─────────────────────────────────────────────────────────

/// Output convenience methods available on all adapters implementing
/// [`execute_query_stream`](DbAdapter::execute_query_stream).
///
/// This trait is a blanket impl — you do not implement it yourself.
pub trait DbAdapterOutputExt: DbAdapter {
    /// Execute `query` and return each row as a [`serde_json::Value`] object.
    ///
    /// Column names become keys; [`QueryValue`](crate::QueryValue)s are mapped to their natural
    /// JSON types. Requires the `json` feature flag.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use arni::output::DbAdapterOutputExt;
    ///
    /// let rows = adapter.execute_query_json("SELECT id, name FROM users").await?;
    /// println!("{}", serde_json::to_string_pretty(&rows).unwrap());
    /// ```
    #[cfg(feature = "json")]
    fn execute_query_json<'a>(
        &'a self,
        query: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<Vec<serde_json::Value>, crate::DataError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let qr = self.execute_query(query).await?;
            let rows = qr
                .rows
                .iter()
                .map(|row| {
                    let obj: serde_json::Map<String, serde_json::Value> = qr
                        .columns
                        .iter()
                        .zip(row.iter())
                        .map(|(col, val)| (col.clone(), query_value_to_json(val)))
                        .collect();
                    serde_json::Value::Object(obj)
                })
                .collect();
            Ok(rows)
        })
    }

    /// Execute `query` and write results as CSV into `writer`.
    ///
    /// The first row written is the header (column names). Rows are written in
    /// arrival order. Requires the `csv` feature flag.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use arni::output::DbAdapterOutputExt;
    ///
    /// let mut buf = Vec::<u8>::new();
    /// adapter.execute_query_csv("SELECT id, name FROM users", &mut buf).await?;
    /// println!("{}", String::from_utf8(buf).unwrap());
    /// ```
    #[cfg(feature = "csv")]
    fn execute_query_csv<'a, W>(
        &'a self,
        query: &'a str,
        writer: W,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<(), crate::DataError>> + Send + 'a,
        >,
    >
    where
        W: std::io::Write + Send + 'a,
    {
        Box::pin(async move {
            let qr = self.execute_query(query).await?;
            let mut wtr = csv::Writer::from_writer(writer);

            // Header row
            wtr.write_record(&qr.columns)
                .map_err(|e| crate::DataError::Query(format!("CSV write header error: {}", e)))?;

            // Data rows
            for row in &qr.rows {
                let record: Vec<String> = row
                    .iter()
                    .map(|v: &crate::QueryValue| v.to_string())
                    .collect();
                wtr.write_record(&record)
                    .map_err(|e| crate::DataError::Query(format!("CSV write row error: {}", e)))?;
            }

            wtr.flush()
                .map_err(|e| crate::DataError::Query(format!("CSV flush error: {}", e)))?;
            Ok(())
        })
    }
}

/// Blanket impl: every type that implements [`DbAdapter`] automatically gets
/// [`DbAdapterOutputExt`] with no additional code.
impl<A: DbAdapter + ?Sized> DbAdapterOutputExt for A {}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #[cfg(feature = "json")]
    use super::query_value_to_json;
    #[cfg(feature = "json")]
    use crate::QueryValue;

    #[cfg(feature = "json")]
    #[test]
    fn query_value_null_maps_to_json_null() {
        let v = query_value_to_json(&QueryValue::Null);
        assert!(v.is_null());
    }

    #[cfg(feature = "json")]
    #[test]
    fn query_value_bool_maps_to_json_bool() {
        assert_eq!(
            query_value_to_json(&QueryValue::Bool(true)),
            serde_json::json!(true)
        );
        assert_eq!(
            query_value_to_json(&QueryValue::Bool(false)),
            serde_json::json!(false)
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn query_value_int_maps_to_json_number() {
        assert_eq!(
            query_value_to_json(&QueryValue::Int(42)),
            serde_json::json!(42i64)
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn query_value_float_maps_to_json_number() {
        assert_eq!(
            query_value_to_json(&QueryValue::Float(1.5)),
            serde_json::json!(1.5f64)
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn query_value_text_maps_to_json_string() {
        assert_eq!(
            query_value_to_json(&QueryValue::Text("hello".to_string())),
            serde_json::json!("hello")
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn query_value_bytes_maps_to_hex_string() {
        let v = query_value_to_json(&QueryValue::Bytes(vec![0xDE, 0xAD]));
        assert_eq!(v, serde_json::json!("dead"));
    }

    // ── execute_query_json integration (SQLite in-memory) ─────────────────────

    #[cfg(all(feature = "json", feature = "sqlite"))]
    #[tokio::test]
    async fn execute_query_json_round_trips_rows() {
        use crate::adapter::{ConnectionConfig, DatabaseType, DbAdapter};
        use crate::adapters::sqlite::SqliteAdapter;
        use crate::output::DbAdapterOutputExt;
        use std::collections::HashMap;

        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: ":memory:".into(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let mut adapter = SqliteAdapter::new(config.clone());
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE jt (id INTEGER, name TEXT)")
            .await
            .unwrap();
        adapter
            .execute_query("INSERT INTO jt VALUES (1, 'A'), (2, 'B')")
            .await
            .unwrap();

        let rows = adapter
            .execute_query_json("SELECT id, name FROM jt")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], serde_json::json!(1i64));
        assert_eq!(rows[0]["name"], serde_json::json!("A"));
        assert_eq!(rows[1]["id"], serde_json::json!(2i64));
    }

    #[cfg(all(feature = "json", feature = "sqlite"))]
    #[tokio::test]
    async fn execute_query_json_empty_result() {
        use crate::adapter::{ConnectionConfig, DatabaseType, DbAdapter};
        use crate::adapters::sqlite::SqliteAdapter;
        use crate::output::DbAdapterOutputExt;
        use std::collections::HashMap;

        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: ":memory:".into(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let mut adapter = SqliteAdapter::new(config.clone());
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE jt2 (id INTEGER)")
            .await
            .unwrap();

        let rows = adapter
            .execute_query_json("SELECT id FROM jt2")
            .await
            .unwrap();
        assert!(rows.is_empty());
    }

    // ── execute_query_csv integration (SQLite in-memory) ──────────────────────

    #[cfg(all(feature = "csv", feature = "sqlite"))]
    #[tokio::test]
    async fn execute_query_csv_round_trips_rows() {
        use crate::adapter::{ConnectionConfig, DatabaseType, DbAdapter};
        use crate::adapters::sqlite::SqliteAdapter;
        use crate::output::DbAdapterOutputExt;
        use std::collections::HashMap;

        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: ":memory:".into(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let mut adapter = SqliteAdapter::new(config.clone());
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE ct (id INTEGER, label TEXT)")
            .await
            .unwrap();
        adapter
            .execute_query("INSERT INTO ct VALUES (10, 'X'), (20, 'Y')")
            .await
            .unwrap();

        let mut buf = Vec::<u8>::new();
        adapter
            .execute_query_csv("SELECT id, label FROM ct", &mut buf)
            .await
            .unwrap();
        let csv_str = String::from_utf8(buf).unwrap();

        let mut rdr = csv::Reader::from_reader(csv_str.as_bytes());
        let headers: Vec<String> = rdr.headers().unwrap().iter().map(String::from).collect();
        assert_eq!(headers, vec!["id", "label"]);
        let records: Vec<csv::StringRecord> = rdr.records().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 2);
        assert_eq!(&records[0][0], "10");
        assert_eq!(&records[0][1], "X");
    }

    #[cfg(all(feature = "csv", feature = "sqlite"))]
    #[tokio::test]
    async fn execute_query_csv_empty_result_no_data_rows() {
        // Note: SQLite's execute_query returns empty columns when there are no rows
        // (it reads column names from the first returned row). The CSV will be valid
        // but the header record will be empty. This is a known adapter limitation.
        use crate::adapter::{ConnectionConfig, DatabaseType, DbAdapter};
        use crate::adapters::sqlite::SqliteAdapter;
        use crate::output::DbAdapterOutputExt;
        use std::collections::HashMap;

        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: ":memory:".into(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let mut adapter = SqliteAdapter::new(config.clone());
        DbAdapter::connect(&mut adapter, &config, None)
            .await
            .unwrap();
        adapter
            .execute_query("CREATE TABLE ct2 (id INTEGER, name TEXT)")
            .await
            .unwrap();

        let mut buf = Vec::<u8>::new();
        adapter
            .execute_query_csv("SELECT id, name FROM ct2", &mut buf)
            .await
            .unwrap();
        // Output is valid CSV (succeeds without error); no data rows.
        let csv_str = String::from_utf8(buf).unwrap();
        let mut rdr = csv::Reader::from_reader(csv_str.as_bytes());
        assert_eq!(rdr.records().count(), 0);
    }
}
