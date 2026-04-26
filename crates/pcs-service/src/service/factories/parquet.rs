//! Built-in Parquet source and sink factories.
//!
//! ## Config for `ParquetSource`
//!
//! ```toml
//! [[sources]]
//! name = "input"
//! type = "ParquetSource"
//! target_component = "orders"
//!
//! [sources.config]
//! path = "/data/orders.parquet"
//! ```
//!
//! ## Config for `ParquetSink`
//!
//! ```toml
//! [[sinks]]
//! name = "output"
//! type = "ParquetSink"
//! source_component = "orders"
//!
//! [sinks.config]
//! path = "/data/output.parquet"
//! # Required fields: schema_fields (array of {name, type, nullable})
//!
//! [[sinks.config.schema_fields]]
//! name = "id"
//! type = "Int64"
//! nullable = false
//!
//! [[sinks.config.schema_fields]]
//! name = "total"
//! type = "Float64"
//! nullable = false
//! ```

use std::path::Path;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use crate::error::PcsError;
use crate::io::parquet_sink::ParquetSink;
use crate::io::parquet_source::ParquetSource;
use crate::io::sink::Sink;
use crate::io::source::Source;
use crate::service::registry::{SinkFactory, SourceFactory};

// ---------------------------------------------------------------------------
// ParquetSourceFactory
// ---------------------------------------------------------------------------

/// Factory for [`ParquetSource`].
///
/// Config fields:
/// - `path` (string, required) — path to the Parquet file to read.
pub struct ParquetSourceFactory;

impl SourceFactory for ParquetSourceFactory {
    fn type_name(&self) -> &'static str {
        "ParquetSource"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Source>, PcsError> {
        let path_str = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("ParquetSource config requires a 'path' string field")
        })?;
        let source = ParquetSource::from_path(Path::new(path_str))?;
        Ok(Box::new(source))
    }
}

// ---------------------------------------------------------------------------
// ParquetSinkFactory
// ---------------------------------------------------------------------------

/// Factory for [`ParquetSink`].
///
/// Config fields:
/// - `path` (string, required) — path of the Parquet file to write.
/// - `schema_fields` (list, required) — Arrow schema for the output file.
///   Each entry: `{name: string, type: string, nullable: bool?}`.
pub struct ParquetSinkFactory;

impl SinkFactory for ParquetSinkFactory {
    fn type_name(&self) -> &'static str {
        "ParquetSink"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Sink>, PcsError> {
        let path_str = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("ParquetSink config requires a 'path' string field")
        })?;

        let schema = parse_schema_fields(config, "ParquetSink")?;
        let sink = ParquetSink::from_path(Path::new(path_str), schema)?;
        Ok(Box::new(sink))
    }
}

// ---------------------------------------------------------------------------
// Shared helper: parse schema_fields from config
// ---------------------------------------------------------------------------

pub(super) fn parse_schema_fields(
    config: &toml::Value,
    factory_name: &str,
) -> Result<Arc<Schema>, PcsError> {
    let fields_val = config.get("schema_fields").ok_or_else(|| {
        PcsError::configuration(format!(
            "{factory_name} config requires a 'schema_fields' list"
        ))
    })?;

    let fields_seq = fields_val.as_array().ok_or_else(|| {
        PcsError::configuration(format!(
            "{factory_name} config.schema_fields must be a TOML array"
        ))
    })?;

    let mut fields = Vec::with_capacity(fields_seq.len());
    for (i, entry) in fields_seq.iter().enumerate() {
        let name = entry.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration(format!(
                "{factory_name} schema_fields[{i}] missing required 'name'"
            ))
        })?;

        let type_str = entry.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration(format!(
                "{factory_name} schema_fields[{i}] ('{name}') missing required 'type'"
            ))
        })?;

        let nullable = entry
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let data_type = parse_data_type(type_str).ok_or_else(|| {
            PcsError::configuration(format!(
                "{factory_name} schema_fields '{name}' has unknown Arrow type '{type_str}'"
            ))
        })?;

        fields.push(Field::new(name, data_type, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn parse_data_type(s: &str) -> Option<DataType> {
    match s.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => Some(DataType::Boolean),
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" => Some(DataType::Int32),
        "int64" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float32" | "float" => Some(DataType::Float32),
        "float64" | "double" => Some(DataType::Float64),
        "utf8" | "string" | "varchar" => Some(DataType::Utf8),
        "largeutf8" | "largestring" => Some(DataType::LargeUtf8),
        "binary" => Some(DataType::Binary),
        "date32" => Some(DataType::Date32),
        "date64" => Some(DataType::Date64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use tempfile::NamedTempFile;

    fn write_parquet(schema: Arc<Schema>, rows: Vec<i64>) -> NamedTempFile {
        use parquet::arrow::ArrowWriter;
        let tmp = NamedTempFile::new().unwrap();
        let arr = Arc::new(Int64Array::from(rows));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        let mut writer =
            ArrowWriter::try_new(std::io::BufWriter::new(tmp.reopen().unwrap()), schema, None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        tmp
    }

    #[test]
    fn test_parquet_source_factory_type_name() {
        assert_eq!(ParquetSourceFactory.type_name(), "ParquetSource");
    }

    #[test]
    fn test_parquet_source_factory_missing_path_returns_error() {
        let err = ParquetSourceFactory
            .build(&toml::Value::Table(toml::Table::new()))
            .err()
            .expect("should return an error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("path"));
    }

    #[tokio::test]
    async fn test_parquet_source_factory_builds_and_reads() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let tmp = write_parquet(schema, vec![1, 2, 3]);

        let config_str = format!("path = \"{}\"", tmp.path().display());
        let config: toml::Value = toml::from_str(&config_str).unwrap();
        let mut src = ParquetSourceFactory.build(&config).unwrap();

        let batch = src.next_batch().await.unwrap();
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_parquet_sink_factory_type_name() {
        assert_eq!(ParquetSinkFactory.type_name(), "ParquetSink");
    }

    #[test]
    fn test_parquet_sink_factory_missing_path_returns_error() {
        let err = ParquetSinkFactory
            .build(&toml::Value::Table(toml::Table::new()))
            .err()
            .expect("should return an error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("path"));
    }

    #[tokio::test]
    async fn test_parquet_sink_factory_builds_and_writes() {
        let tmp = NamedTempFile::new().unwrap();
        let config_str = format!(
            "path = \"{}\"\n[[schema_fields]]\nname = \"id\"\ntype = \"Int64\"\nnullable = false\n",
            tmp.path().display()
        );
        let config: toml::Value = toml::from_str(&config_str).unwrap();
        let mut sink = ParquetSinkFactory.build(&config).unwrap();

        let schema = sink.schema();
        let arr = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
    }
}
