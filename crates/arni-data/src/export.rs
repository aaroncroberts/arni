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
//! | [`DataFormat::Excel`]   | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | `.xlsx` workbook with bold headers |
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

use polars::prelude::{AnyValue, CsvWriter, DataFrame, JsonFormat, JsonWriter, ParquetWriter, SerWriter};
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;
use rust_xlsxwriter::{Format, Workbook, XlsxError};

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
    /// Microsoft Excel Open XML workbook (.xlsx).
    ///
    /// Column names are written as bold headers in row 0; data starts in row 1.
    Excel,
}

impl DataFormat {
    /// Canonical file extension for this format (without leading dot).
    pub fn extension(self) -> &'static str {
        match self {
            DataFormat::Csv => "csv",
            DataFormat::Json => "json",
            DataFormat::Xml => "xml",
            DataFormat::Parquet => "parquet",
            DataFormat::Excel => "xlsx",
        }
    }
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.extension())
    }
}

impl std::str::FromStr for DataFormat {
    type Err = String;

    /// Parse a format name (case-insensitive) to a [`DataFormat`].
    ///
    /// Accepted aliases: `csv`, `json`, `xml`, `parquet`, `excel`, `xlsx`.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(DataFormat::Csv),
            "json" => Ok(DataFormat::Json),
            "xml" => Ok(DataFormat::Xml),
            "parquet" => Ok(DataFormat::Parquet),
            "excel" | "xlsx" => Ok(DataFormat::Excel),
            other => Err(format!(
                "Unknown format '{}'. Valid: csv, json, xml, parquet, excel",
                other
            )),
        }
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
        DataFormat::Excel => {
            return write_excel_bytes(df);
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
        DataFormat::Excel => {
            let bytes = write_excel_bytes(df)?;
            use std::io::Write;
            file.write_all(&bytes)?;
        }
    }
    Ok(())
}

// ─── XML implementation ───────────────────────────────────────────────────────

fn xml_err(e: std::io::Error) -> DataError {
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

    let columns = df.columns();
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

// ─── Excel implementation ─────────────────────────────────────────────────────

fn xlsx_err(e: XlsxError) -> DataError {
    DataError::Query(format!("Excel serialization error: {e}"))
}

/// Serialize `df` to an in-memory `.xlsx` workbook byte buffer.
///
/// Row 0 contains bold column headers. Data rows start at row 1.
/// Values are written with their native Excel types:
/// integers and floats as numbers, booleans as booleans, everything else as strings.
fn write_excel_bytes(df: &mut DataFrame) -> Result<Vec<u8>> {
    let mut workbook = Workbook::new();
    let bold = Format::new().set_bold();
    let worksheet = workbook.add_worksheet();

    let col_names = df.get_column_names();

    // Header row (bold)
    for (col_idx, name) in col_names.iter().enumerate() {
        worksheet
            .write_with_format(0, col_idx as u16, name.as_str(), &bold)
            .map_err(xlsx_err)?;
    }

    let columns = df.columns();
    let n_rows = df.height();

    for row_idx in 0..n_rows {
        let xlsx_row = (row_idx + 1) as u32; // offset by 1 for header
        for (col_idx, col) in columns.iter().enumerate() {
            let xlsx_col = col_idx as u16;
            let val = col
                .get(row_idx)
                .map_err(|e| DataError::Query(format!("column read error: {e}")))?;
            write_any_value(worksheet, xlsx_row, xlsx_col, &val)?;
        }
    }

    workbook.save_to_buffer().map_err(xlsx_err)
}

/// Write a single `AnyValue` cell to the worksheet, choosing the most
/// appropriate Excel type for each Polars variant.
fn write_any_value(
    ws: &mut rust_xlsxwriter::Worksheet,
    row: u32,
    col: u16,
    val: &AnyValue<'_>,
) -> Result<()> {
    match val {
        AnyValue::Null => {
            ws.write_blank(row, col, &Format::new()).map_err(xlsx_err)?;
        }
        AnyValue::Boolean(b) => {
            ws.write_boolean(row, col, *b).map_err(xlsx_err)?;
        }
        AnyValue::Int8(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::Int16(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::Int32(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::Int64(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::UInt8(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::UInt16(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::UInt32(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::UInt64(n) => {
            ws.write_number(row, col, *n as f64).map_err(xlsx_err)?;
        }
        AnyValue::Float32(f) => {
            ws.write_number(row, col, *f as f64).map_err(xlsx_err)?;
        }
        AnyValue::Float64(f) => {
            ws.write_number(row, col, *f).map_err(xlsx_err)?;
        }
        // All other variants (strings, dates, categoricals, …) fall back to Display
        other => {
            let text = format!("{other}");
            let text = text.trim_matches('"');
            ws.write_string(row, col, text).map_err(xlsx_err)?;
        }
    }
    Ok(())
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
    fn from_str_parses_all_formats() {
        assert_eq!("csv".parse::<DataFormat>().unwrap(), DataFormat::Csv);
        assert_eq!("json".parse::<DataFormat>().unwrap(), DataFormat::Json);
        assert_eq!("xml".parse::<DataFormat>().unwrap(), DataFormat::Xml);
        assert_eq!("parquet".parse::<DataFormat>().unwrap(), DataFormat::Parquet);
        assert_eq!("excel".parse::<DataFormat>().unwrap(), DataFormat::Excel);
        assert_eq!("xlsx".parse::<DataFormat>().unwrap(), DataFormat::Excel);
        assert_eq!("CSV".parse::<DataFormat>().unwrap(), DataFormat::Csv);  // case-insensitive
    }

    #[test]
    fn from_str_unknown_format_returns_err() {
        assert!("tsv".parse::<DataFormat>().is_err());
        assert!("".parse::<DataFormat>().is_err());
    }

    #[test]
    fn extensions_are_correct() {
        assert_eq!(DataFormat::Csv.extension(), "csv");
        assert_eq!(DataFormat::Json.extension(), "json");
        assert_eq!(DataFormat::Xml.extension(), "xml");
        assert_eq!(DataFormat::Parquet.extension(), "parquet");
        assert_eq!(DataFormat::Excel.extension(), "xlsx");
    }

    #[test]
    fn display_matches_extension() {
        assert_eq!(DataFormat::Csv.to_string(), "csv");
        assert_eq!(DataFormat::Xml.to_string(), "xml");
        assert_eq!(DataFormat::Excel.to_string(), "xlsx");
    }

    // ── Excel ─────────────────────────────────────────────────────────────────

    #[test]
    fn excel_to_bytes_has_xlsx_magic() {
        let mut df = sample_df();
        let bytes = to_bytes(&mut df, DataFormat::Excel).unwrap();
        // .xlsx files are ZIP archives; ZIP magic bytes are PK\x03\x04
        assert!(bytes.len() > 4, "xlsx bytes should be non-trivial");
        assert_eq!(&bytes[..2], b"PK", "expected ZIP/xlsx magic 'PK', got {:?}", &bytes[..4]);
    }

    #[test]
    fn excel_to_file_writes_nonempty_file() {
        let mut df = sample_df();
        let tmp = NamedTempFile::new().unwrap();
        to_file(&mut df, DataFormat::Excel, tmp.path()).unwrap();
        let size = tmp.path().metadata().unwrap().len();
        assert!(size > 0, "excel file should be non-empty");
    }

    #[test]
    fn excel_with_mixed_types() {
        // Exercises the numeric, boolean, and string match arms
        let mut df = DataFrame::new(3, vec![
            Column::new("id".into(), &[1i32, 2, 3]),
            Column::new("active".into(), &[true, false, true]),
            Column::new("name".into(), &["Alice", "Bob", "Carol"]),
            Column::new("score".into(), &[92.5f64, 87.0, 95.1]),
        ])
        .unwrap();
        let bytes = to_bytes(&mut df, DataFormat::Excel).unwrap();
        assert_eq!(&bytes[..2], b"PK");
    }

    #[test]
    fn excel_empty_df_does_not_panic() {
        let mut df =
            DataFrame::new(0, vec![Column::new("id".into(), &[] as &[i32])]).unwrap();
        let bytes = to_bytes(&mut df, DataFormat::Excel).unwrap();
        // Should produce a valid (header-only) workbook without panicking
        assert_eq!(&bytes[..2], b"PK");
    }

    // ── Empty DataFrame edge cases ───────────────────────────────────────────

    #[test]
    fn csv_empty_df_has_header_only() {
        let mut df = DataFrame::new(0, vec![
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
            DataFrame::new(0, vec![Column::new("id".into(), &[] as &[i32])]).unwrap();
        let bytes = to_bytes(&mut df, DataFormat::Xml).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(!text.contains("<row>"), "empty df should have no <row>: {text}");
    }

    // ── Excel date/datetime rendering ────────────────────────────────────────

    /// `AnyValue::Date` must render as "yyyy-mm-dd", not as a raw epoch-day integer.
    #[test]
    fn excel_date_column_renders_as_string() {
        // 19723 epoch days = 2024-01-15
        let days = Series::new("d".into(), &[19723i32]);
        let date_series = days.cast(&DataType::Date).unwrap();
        let val = date_series.get(0).unwrap();
        let text = format!("{val}");
        assert!(
            !text.chars().all(|c| c.is_ascii_digit()),
            "Date should not render as a raw integer, got: '{text}'"
        );
        assert!(
            text.contains('-'),
            "Date should render in yyyy-mm-dd form, got: '{text}'"
        );
    }

    /// `AnyValue::Datetime` must render as a human-readable string, not raw microseconds.
    #[test]
    fn excel_datetime_column_renders_as_string() {
        // 1_705_276_800_000_000 µs = 2024-01-15 00:00:00 UTC
        let ts = Series::new("t".into(), &[1_705_276_800_000_000i64]);
        let dt_series = ts
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap();
        let val = dt_series.get(0).unwrap();
        let text = format!("{val}");
        assert!(
            !text.chars().all(|c| c.is_ascii_digit()),
            "Datetime should not render as a raw integer, got: '{text}'"
        );
        assert!(
            text.contains('-') || text.contains(':'),
            "Datetime should render as ISO 8601, got: '{text}'"
        );
    }
}
