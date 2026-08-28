//! The file source and sink factories.
//!
//! Both resolve their byte format through the transformer the host bound to
//! this connector instance via a declared `transformer` key. The schema is
//! where they differ: a source hands the format whatever `schema_fields`
//! says, including nothing, and lets the format decide, while a sink always
//! needs one because it is the schema the rows are written with.

use std::path::Path;

use pcs_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_optional_schema_fields,
    parse_schema_fields,
};
use pcs_core::error::PcsError;
use pcs_core::io::{sink::Sink, source::Source};

use crate::{FileSink, FileSource};

/// Factory for [`FileSource`].
///
/// Config fields:
/// - `path` (string, required): the file to read.
/// - `schema_fields` (list, optional): the declared Arrow schema. Required by
///   `csv`, rejected by `parquet`, inferred by `ndjson` when absent.
///
/// The byte format is whatever transformer the `source` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
pub struct FileSourceFactory;

impl SourceFactory for FileSourceFactory {
    fn type_name(&self) -> &'static str {
        "FileSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, PcsError> {
        let path = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("FileSource config requires a 'path' string field")
        })?;
        let transformer = ctx.transformer("FileSource")?;
        let declared = parse_optional_schema_fields(config, "FileSource")?;
        Ok(Box::new(FileSource::open(
            Path::new(path),
            transformer,
            declared,
        )?))
    }
}

/// Factory for [`FileSink`].
///
/// Config fields:
/// - `path` (string, required): the file to write.
/// - `schema_fields` (list, required): the Arrow schema for the output file.
///
/// The byte format is whatever transformer the `sink` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
pub struct FileSinkFactory;

impl SinkFactory for FileSinkFactory {
    fn type_name(&self) -> &'static str {
        "FileSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, PcsError> {
        let path = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            PcsError::configuration("FileSink config requires a 'path' string field")
        })?;
        let transformer = ctx.transformer("FileSink")?;
        let schema = parse_schema_fields(config, "FileSink")?;
        Ok(Box::new(FileSink::create(
            Path::new(path),
            transformer,
            schema,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pcs_connector::{ConfigMap, from_kdl_str};
    use pcs_transformer::{Transformer, TransformerFactory};
    use pcs_transformer_csv::CsvTransformerFactory;
    use pcs_transformer_parquet::ParquetTransformerFactory;
    use tempfile::TempDir;

    use super::*;

    fn csv_transformer(options: &ConfigValue) -> Arc<dyn Transformer> {
        CsvTransformerFactory
            .build(options)
            .expect("csv transformer builds")
    }

    fn parquet_transformer() -> Arc<dyn Transformer> {
        ParquetTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("parquet transformer builds")
    }

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    /// A path is written into a config string, and a Windows path has
    /// backslashes a KDL quoted string reads as escapes.
    fn config_path(dir: &TempDir, name: &str) -> String {
        dir.path().join(name).to_string_lossy().replace('\\', "/")
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    const CSV_SCHEMA: &str = r#"
schema_fields "id" type="Int64" nullable=#false
"#;

    #[test]
    fn the_type_names_match_the_config_type_key() {
        assert_eq!(FileSourceFactory.type_name(), "FileSource");
        assert_eq!(FileSinkFactory.type_name(), "FileSink");
    }

    #[test]
    fn a_missing_path_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);

        let Err(err) = FileSourceFactory.build(&empty_config(), &ctx) else {
            panic!("path is required");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "FileSource config requires a 'path' string field"
        );

        let Err(err) = FileSinkFactory.build(&empty_config(), &ctx) else {
            panic!("path is required");
        };
        assert_eq!(
            err.message(),
            "FileSink config requires a 'path' string field"
        );
    }

    #[test]
    fn a_source_with_no_bound_transformer_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let dir = TempDir::new().expect("temp dir");
        let raw = format!("path \"{}\"\n{CSV_SCHEMA}", config_path(&dir, "in.csv"));

        let Err(err) = FileSourceFactory.build(&config(&raw), &ctx) else {
            panic!("a source that moves bytes needs a bound transformer");
        };
        assert_eq!(
            err.message(),
            "FileSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn parquet_with_declared_schema_fields_is_rejected_by_the_format() {
        let ctx = ConnectorContext::new(Some(parquet_transformer()));
        let dir = TempDir::new().expect("temp dir");

        // The file exists so the failure is the schema rule rather than the
        // open. The rule is checked before any byte of it is read, so its
        // contents do not matter.
        let path = dir.path().join("in.parquet");
        std::fs::write(&path, b"").expect("fixture");

        let raw = format!("path \"{}\"\n{CSV_SCHEMA}", config_path(&dir, "in.parquet"));
        let Err(err) = FileSourceFactory.build(&config(&raw), &ctx) else {
            panic!("parquet must reject a declared schema");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_sink_without_schema_fields_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_transformer(&empty_config())));
        let dir = TempDir::new().expect("temp dir");
        let raw = format!("path \"{}\"\n", config_path(&dir, "out.csv"));

        let Err(err) = FileSinkFactory.build(&config(&raw), &ctx) else {
            panic!("a sink needs the schema it writes");
        };
        assert!(err.message().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_csv_source_builds_and_reports_the_declared_schema() {
        let mut options = ConfigMap::new();
        options.insert("has_headers".to_string(), ConfigValue::Bool(true));
        let ctx = ConnectorContext::new(Some(csv_transformer(&ConfigValue::Object(options))));
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("in.csv");
        std::fs::write(&path, "id\n1\n2\n").expect("fixture");

        let raw = format!("path \"{}\"\n{CSV_SCHEMA}", config_path(&dir, "in.csv"));
        let source = FileSourceFactory
            .build(&config(&raw), &ctx)
            .expect("source builds");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");
    }
}
