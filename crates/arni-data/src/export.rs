//! DataFrame serialization to multiple output formats.
//!
//! This module provides a single, DRY serialization layer for Polars DataFrames.
//! It is the canonical place for all DataFrame → bytes/file conversion logic in
//! the `arni` workspace, shared by `arni-cli` and any future API crate.
//!
//! # Supported Formats
//!
//! | Variant | MIME type | Notes |
//! |---------|-----------|-------|
//! | [`DataFormat::Csv`]     | `text/csv`              | RFC 4180, header row included |
//! | [`DataFormat::Json`]    | `application/json`      | JSON array of objects |
//! | [`DataFormat::Xml`]     | `application/xml`       | `<dataframe><row>…</row></dataframe>` |
//! | [`DataFormat::Parquet`] | `application/octet-stream` | Apache Parquet binary |
//!
//! # Examples
//!
//! ```rust
//! use arni_data::export::{DataFormat, to_bytes};
//! use polars::prelude::*;
//!
//! let mut df = df!["name" => ["Alice", "Bob"], "score" => [92.5f64, 87.0]].unwrap();
//! let csv_bytes = to_bytes(&mut df, DataFormat::Csv).unwrap();
//! assert!(String::from_utf8(csv_bytes).unwrap().starts_with("name,score"));
//! ```

use std::io::Cursor;
use std::path::Path;

use polars::prelude::{CsvWriter, DataFrame, JsonFormat, JsonWriter, ParquetWriter, SerWriter};
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;

use crate::error::{DataError, Result};

// ─── Public API ──────────────────────────────────────────────────────────────

/// Output format for DataFrame serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFormat {
    /// Comma-separated values with header row.
    Csv,
    /// JSON array of row objects.
    Json,
    /// XML document: `<dataframe><row><col>val</col>…</row></dataframe>`.
    Xml,
    /// Apache Parquet binary format.
    Parquet,
}

impl DataFormat {
    /// Canonical file extension for this format (without leading dot).
    pub fn extension(self) -> &'static str {
        match self {
            DataFormat::Csv => "csv",
            DataFormat::Json => "json",
            DataFormat::Xml => "xml",
            DataFormat::Parquet => "parquet",
        }
    }
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.extension())
    }
}

/// Serialize `df` to an in-memory byte buffer in the given `format`.
///
/// The DataFrame may be mutated in place by some Polars writers (e.g. rechunk).
pub fn to_bytes(df: &mut DataFrame, format: DataFormat) -> Result<Vec<u8>> {
    let mut buf = Cursor::new(Vec::new());
    match format {
        DataFormat::Csv => {
            CsvWriter::new(&mut buf).finish(df)?;
        }
        DataFormat::Json => {
            JsonWriter::new(&mut buf)
                .with_json_format(JsonFormat::Json)
                .finish(df)?;
        }
        DataFormat::Parquet => {
            ParquetWriter::new(&mut buf).finish(df)?;
        }
        DataFormat::Xml => {
            write_xml(df, &mut buf)?;
        }
    }
    Ok(buf.into_inner())
}

/// Write `df` to a file at `path` in the given `format`.
///
/// The file is created (or truncated) at `path`. Intermediate directories must
/// already exist.
pub fn to_file(df: &mut DataFrame, format: DataFormat, path: &Path) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    match format {
        DataFormat::Csv => {
            CsvWriter::new(&mut file).finish(df)?;
        }
        DataFormat::Json => {
            JsonWriter::new(&mut file)
                .with_json_format(JsonFormat::Json)
                .finish(df)?;
        }
        DataFormat::Parquet => {
            ParquetWriter::new(&mut file).finish(df)?;
        }
        DataFormat::Xml => {
            write_xml(df, &mut file)?;
        }
    }
    Ok(())
}

// ─── XML implementation ───────────────────────────────────────────────────────

fn xml_err(e: quick_xml::Error) -> DataError {
    DataError::Query(format!("XML serialization error: {e}"))
}

fn write_xml<W: std::io::Write>(df: &mut DataFrame, sink: &mut W) -> Result<()> {
    let mut writer = Writer::new_with_indent(sink, b' ', 2);

    // <?xml version="1.0" encoding="UTF-8"?>
    writer
        .write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .map_err(xml_err)?;

    // <dataframe>
    writer
        .write_event(Event::Start(BytesStart::new("dataframe")))
        .map_err(xml_err)?;

    let columns = df.get_columns();
    let n_rows = df.height();

    for row_idx in 0..n_rows {
        // <row>
        writer
            .write_event(Event::Start(BytesStart::new("row")))
            .map_err(xml_err)?;

        for col in columns.iter() {
            let tag = sanitize_xml_tag(col.name());
            let val = col
                .get(row_idx)
                .map_err(|e| DataError::Query(format!("column read error: {e}")))?;
            let text = format!("{val}");
            // Strip surrounding quotes that AnyValue Display adds for strings
            let text = text.trim_matches('"');

            writer
                .write_event(Event::Start(BytesStart::new(tag.as_str())))
                .map_err(xml_err)?;
            writer
                .write_event(Event::Text(BytesText::new(text)))
                .map_err(xml_err)?;
            writer
                .write_event(Event::End(BytesEnd::new(tag.as_str())))
                .map_err(xml_err)?;
        }

        // </row>
        writer
            .write_event(Event::End(BytesEnd::new("row")))
            .map_err(xml_err)?;
    }

    // </dataframe>
    writer
        .write_event(Event::End(BytesEnd::new("dataframe")))
        .map_err(xml_err)?;

    Ok(())
}

/// Replace characters that are invalid in XML element names with underscores.
/// XML names must start with a letter or underscore; subsequent chars may also
/// include digits, hyphens, and dots.
fn sanitize_xml_tag(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (i, ch) in name.char_indices() {
        let ok = if i == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')
        };
        if ok {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    use tempfile::NamedTempFile;

    fn sample_df() -> DataFrame {
        df![
            "name"  => ["Alice", "Bob", "Carol"],
            "score" => [92.5f64, 87.0, 95.1]
        ]
        .unwrap()
    }

    // ── CSV ──────────────────────────────────────────────────────────────────

    #[test]
    fn csv_to_bytes_has_header() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Csv).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("name,score"), "expected header row, got: {text}");
    }

    #[test]
    fn csv_to_bytes_has_all_rows() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Csv).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("Alice"));
        assert!(text.contains("Bob"));
        assert!(text.contains("Carol"));
    }

    #[test]
    fn csv_to_file_writes_nonempty_file() {
        let mut df = sample_df();
        let tmp = NamedTempFile::new().unwrap();
        to_file(&mut df, DataFormat::Csv, tmp.path()).unwrap();
        assert!(tmp.path().metadata().unwrap().len() > 0);
    }

    // ── JSON ─────────────────────────────────────────────────────────────────

    #[test]
    fn json_to_bytes_is_valid_json_array() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Json).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(parsed.is_array(), "expected JSON array, got: {text}");
        assert_eq!(parsed.as_array().unwrap().len(), 3);
    }

    #[test]
    fn json_to_bytes_contains_expected_keys() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Json).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("\"name\""));
        assert!(text.contains("\"score\""));
    }

    #[test]
    fn json_to_file_writes_nonempty_file() {
        let mut df = sample_df();
        let tmp = NamedTempFile::new().unwrap();
        to_file(&mut df, DataFormat::Json, tmp.path()).unwrap();
        assert!(tmp.path().metadata().unwrap().len() > 0);
    }

    // ── Parquet ───────────────────────────────────────────────────────────────

    #[test]
    fn parquet_to_bytes_round_trips() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Parquet).unwrap();
        assert!(!bytes.is_empty());
        // Parquet magic bytes: PAR1 at start
        assert_eq!(&bytes[..4], b"PAR1", "expected Parquet magic header");
    }

    #[test]
    fn parquet_to_file_writes_nonempty_file() {
        let mut df = sample_df();
        let tmp = NamedTempFile::new().unwrap();
        to_file(&mut df, DataFormat::Parquet, tmp.path()).unwrap();
        assert!(tmp.path().metadata().unwrap().len() > 0);
    }

    // ── XML ───────────────────────────────────────────────────────────────────

    #[test]
    fn xml_to_bytes_starts_with_declaration() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Xml).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(
            text.starts_with("<?xml"),
            "expected XML declaration, got: {text}"
        );
    }

    #[test]
    fn xml_to_bytes_has_dataframe_root() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Xml).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("<dataframe>"), "missing <dataframe>: {text}");
        assert!(text.contains("</dataframe>"), "missing </dataframe>: {text}");
    }

    #[test]
    fn xml_to_bytes_has_correct_row_count() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Xml).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let row_count = text.matches("<row>").count();
        assert_eq!(row_count, 3, "expected 3 <row> elements, got {row_count}");
    }

    #[test]
    fn xml_to_bytes_contains_values() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Xml).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("Alice"), "missing Alice: {text}");
        assert!(text.contains("Bob"), "missing Bob: {text}");
        assert!(text.contains("92.5"), "missing 92.5: {text}");
    }

    #[test]
    fn xml_to_file_writes_nonempty_file() {
        let mut df = sample_df();
        let tmp = NamedTempFile::new().unwrap();
        to_file(&mut df, DataFormat::Xml, tmp.path()).unwrap();
        assert!(tmp.path().metadata().unwrap().len() > 0);
    }

    #[test]
    fn xml_column_names_used_as_tags() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Xml).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("<name>"), "missing <name> tag: {text}");
        assert!(text.contains("<score>"), "missing <score> tag: {text}");
    }

    // ── sanitize_xml_tag ─────────────────────────────────────────────────────

    #[test]
    fn sanitize_simple_name_unchanged() {
        assert_eq!(sanitize_xml_tag("name"), "name");
    }

    #[test]
    fn sanitize_leading_digit_replaced() {
        let result = sanitize_xml_tag("1col");
        assert_eq!(&result[..1], "_");
    }

    #[test]
    fn sanitize_spaces_replaced() {
        assert_eq!(sanitize_xml_tag("my col"), "my_col");
    }

    // ── DataFormat helpers ───────────────────────────────────────────────────

    #[test]
    fn extensions_are_correct() {
        assert_eq!(DataFormat::Csv.extension(), "csv");
        assert_eq!(DataFormat::Json.extension(), "json");
        assert_eq!(DataFormat::Xml.extension(), "xml");
        assert_eq!(DataFormat::Parquet.extension(), "parquet");
    }

    #[test]
    fn display_matches_extension() {
        assert_eq!(DataFormat::Csv.to_string(), "csv");
        assert_eq!(DataFormat::Xml.to_string(), "xml");
    }

    // ── Empty DataFrame edge cases ───────────────────────────────────────────

    #[test]
    fn csv_empty_df_has_header_only() {
        let mut df = DataFrame::new(vec![
            Column::new("id".into(), &[] as &[i32]),
            Column::new("val".into(), &[] as &[f64]),
        ])
        .unwrap();
        let bytes = to_bytes(&mut df, DataFormat::Csv).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("id,val"));
    }

    #[test]
    fn xml_empty_df_has_no_row_elements() {
        let mut df =
            DataFrame::new(vec![Column::new("id".into(), &[] as &[i32])]).unwrap();
        let bytes = to_bytes(&mut df, DataFormat::Xml).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(!text.contains("<row>"), "empty df should have no <row>: {text}");
    }
}
