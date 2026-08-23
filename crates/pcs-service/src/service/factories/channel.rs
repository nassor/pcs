//! Built-in channel source and sink factories.
//!
//! `ChannelSource` and `ChannelSink` are in-memory, mpsc-backed IO endpoints,
//! useful for testing pipelines without file IO and for in-process fan-out.
//!
//! A channel is created as a `(tx, rx)` pair, so a factory that returns only
//! one half must drop the other: the source starts at EOF and the sink writes
//! into a closed channel. Build the channel by hand for real use.
//!
//! Both take `buffer` (mpsc capacity, default 8) and `schema_fields` in their
//! `config` table.

use crate::error::PcsError;
use crate::io::channel_sink::ChannelSink;
use crate::io::channel_source::ChannelSource;
use crate::io::sink::Sink;
use crate::io::source::Source;
use crate::service::registry::{SinkFactory, SourceFactory};

use super::parquet::parse_schema_fields;

/// Factory for [`ChannelSource`].
///
/// Produces a source backed by a closed channel, so it reports EOF at once.
/// Build the channel manually for live data.
///
/// Config fields:
/// - `buffer` (usize, optional, default `8`): mpsc channel capacity.
/// - `schema_fields` (list, required): Arrow schema definition.
pub struct ChannelSourceFactory;

impl SourceFactory for ChannelSourceFactory {
    fn type_name(&self) -> &'static str {
        "ChannelSource"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Source>, PcsError> {
        let buffer = config
            .get("buffer")
            .and_then(|v| v.as_integer())
            .map(|v| v.max(0) as usize)
            .unwrap_or(8);
        let schema = parse_schema_fields(config, "ChannelSource")?;
        // Dropping the sender leaves the channel at EOF.
        let (_tx, src) = ChannelSource::new(schema, buffer);
        Ok(Box::new(src))
    }
}

/// Factory for [`ChannelSink`].
///
/// Produces a sink whose receiver is dropped at construction. Build the channel
/// manually to consume the data.
///
/// Config fields:
/// - `buffer` (usize, optional, default `8`): mpsc channel capacity.
/// - `schema_fields` (list, required): Arrow schema definition.
pub struct ChannelSinkFactory;

impl SinkFactory for ChannelSinkFactory {
    fn type_name(&self) -> &'static str {
        "ChannelSink"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Sink>, PcsError> {
        let buffer = config
            .get("buffer")
            .and_then(|v| v.as_integer())
            .map(|v| v.max(0) as usize)
            .unwrap_or(8);
        let schema = parse_schema_fields(config, "ChannelSink")?;
        // Dropping the receiver leaves the sink writing to a closed channel.
        let (sink, _rx) = ChannelSink::new(schema, buffer);
        Ok(Box::new(sink))
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn schema_config() -> toml::Value {
        toml::from_str(
            r#"
buffer = 4

[[schema_fields]]
name = "id"
type = "Int64"
nullable = false
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_channel_source_factory_type_name() {
        assert_eq!(ChannelSourceFactory.type_name(), "ChannelSource");
    }

    #[test]
    fn test_channel_sink_factory_type_name() {
        assert_eq!(ChannelSinkFactory.type_name(), "ChannelSink");
    }

    #[test]
    fn test_channel_source_factory_builds_source() {
        let src = ChannelSourceFactory.build(&schema_config()).unwrap();
        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(src.schema().field(0).name(), "id");
        assert_eq!(src.schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_channel_sink_factory_builds_sink() {
        let sink = ChannelSinkFactory.build(&schema_config()).unwrap();
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn test_channel_source_missing_schema_fields_returns_error() {
        let err = ChannelSourceFactory
            .build(&toml::Value::Table(toml::Table::new()))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"));
    }

    #[tokio::test]
    async fn test_channel_source_yields_eof_immediately() {
        let src = ChannelSourceFactory.build(&schema_config()).unwrap();
        // The tx was dropped, so the closed channel must report EOF on the
        // first poll.
        let mut src = src;
        let batch = src.next_batch().await.unwrap();
        assert!(batch.is_none(), "expected EOF (None), got a batch");
    }
}
