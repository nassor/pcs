//! Built-in JSON source and sink factories.
//!
//! `JsonSource` reads a newline-delimited JSON file and `JsonSink` writes one.
//! Both take `path` and `schema_fields` in their `config` table.

use std::path::Path;

use crate::error::PcsError;
use crate::io::json_sink::JsonSink;
use crate::io::json_source::JsonSource;
use crate::io::sink::Sink;
use crate::io::source::Source;
use crate::service::registry::{SinkFactory, SourceFactory};

use super::parquet::parse_schema_fields;

/// Factory for [`JsonSource`].
///
/// Config fields:
/// - `path` (string, required): path to the newline-delimited JSON file.
/// - `schema_fields` (list, required): Arrow schema definition.
pub struct JsonSourceFactory;

impl SourceFactory for JsonSourceFactory {
    fn type_name(&self) -> &'static str {
        "JsonSource"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Source>, PcsError> {
        let path_str = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("JsonSource config requires a 'path' string field")
        })?;
        let schema = parse_schema_fields(config, "JsonSource")?;
        let source = JsonSource::from_path(Path::new(path_str), schema)?;
        Ok(Box::new(source))
    }
}

/// Factory for [`JsonSink`].
///
/// Config fields:
/// - `path` (string, required): path of the JSON file to write.
pub struct JsonSinkFactory;

impl SinkFactory for JsonSinkFactory {
    fn type_name(&self) -> &'static str {
        "JsonSink"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Sink>, PcsError> {
        let path_str = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("JsonSink config requires a 'path' string field")
        })?;
        let schema = parse_schema_fields(config, "JsonSink")?;
        let sink = JsonSink::from_path(Path::new(path_str), schema)?;
        Ok(Box::new(sink))
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    #[test]
    fn test_json_source_factory_type_name() {
        assert_eq!(JsonSourceFactory.type_name(), "JsonSource");
    }

    #[test]
    fn test_json_sink_factory_type_name() {
        assert_eq!(JsonSinkFactory.type_name(), "JsonSink");
    }

    #[test]
    fn test_json_source_missing_path_returns_error() {
        let err = JsonSourceFactory
            .build(&toml::Value::Table(toml::Table::new()))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("path"));
    }

    #[test]
    fn test_json_sink_missing_path_returns_error() {
        let err = JsonSinkFactory
            .build(&toml::Value::Table(toml::Table::new()))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("path"));
    }
}
