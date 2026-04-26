//! Built-in CSV source and sink factories.
//!
//! ## Config for `CsvSource`
//!
//! ```toml
//! [[sources]]
//! name = "input"
//! type = "CsvSource"
//! target_component = "orders"
//!
//! [sources.config]
//! path = "/data/orders.csv"
//! has_headers = true          # optional, default true
//!
//! [[sources.config.schema_fields]]
//! name = "id"
//! type = "Int64"
//! nullable = false
//!
//! [[sources.config.schema_fields]]
//! name = "amount"
//! type = "Float64"
//! nullable = true
//! ```
//!
//! ## Config for `CsvSink`
//!
//! ```toml
//! [[sinks]]
//! name = "output"
//! type = "CsvSink"
//! source_component = "orders"
//!
//! [sinks.config]
//! path = "/data/output.csv"
//! has_headers = true
//!
//! [[sinks.config.schema_fields]]
//! name = "id"
//! type = "Int64"
//! nullable = false
//! ```

use std::path::Path;

use crate::error::PcsError;
use crate::io::csv_sink::CsvSink;
use crate::io::csv_source::CsvSource;
use crate::io::sink::Sink;
use crate::io::source::Source;
use crate::service::registry::{SinkFactory, SourceFactory};

use super::parquet::parse_schema_fields;

// ---------------------------------------------------------------------------
// CsvSourceFactory
// ---------------------------------------------------------------------------

/// Factory for [`CsvSource`].
///
/// Config fields:
/// - `path` (string, required) — path to the CSV file.
/// - `has_headers` (bool, optional, default `true`) — whether the first row
///   contains column names.
/// - `schema_fields` (list, required) — Arrow schema definition.
pub struct CsvSourceFactory;

impl SourceFactory for CsvSourceFactory {
    fn type_name(&self) -> &'static str {
        "CsvSource"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Source>, PcsError> {
        let path_str = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("CsvSource config requires a 'path' string field")
        })?;
        let has_headers = config
            .get("has_headers")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let schema = parse_schema_fields(config, "CsvSource")?;
        let source = CsvSource::from_path(Path::new(path_str), schema, has_headers)?;
        Ok(Box::new(source))
    }
}

// ---------------------------------------------------------------------------
// CsvSinkFactory
// ---------------------------------------------------------------------------

/// Factory for [`CsvSink`].
///
/// Config fields:
/// - `path` (string, required) — path of the CSV file to write.
/// - `has_headers` (bool, optional, default `true`) — write header row.
/// - `schema_fields` (list, required) — Arrow schema for the output file.
pub struct CsvSinkFactory;

impl SinkFactory for CsvSinkFactory {
    fn type_name(&self) -> &'static str {
        "CsvSink"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Sink>, PcsError> {
        let path_str = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("CsvSink config requires a 'path' string field")
        })?;
        let has_headers = config
            .get("has_headers")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let schema = parse_schema_fields(config, "CsvSink")?;
        let sink = CsvSink::from_path(Path::new(path_str), schema, has_headers)?;
        Ok(Box::new(sink))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    #[test]
    fn test_csv_source_factory_type_name() {
        assert_eq!(CsvSourceFactory.type_name(), "CsvSource");
    }

    #[test]
    fn test_csv_sink_factory_type_name() {
        assert_eq!(CsvSinkFactory.type_name(), "CsvSink");
    }

    #[test]
    fn test_csv_source_missing_path_returns_error() {
        let err = CsvSourceFactory
            .build(&toml::Value::Table(toml::Table::new()))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("path"));
    }

    #[test]
    fn test_csv_sink_missing_path_returns_error() {
        let err = CsvSinkFactory
            .build(&toml::Value::Table(toml::Table::new()))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("path"));
    }
}
