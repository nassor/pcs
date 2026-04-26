//! Built-in channel source and sink factories.
//!
//! `ChannelSource` and `ChannelSink` are in-memory, mpsc-backed IO endpoints.
//! They are useful for testing pipelines without file I/O and for in-process
//! fan-out scenarios where another part of the application produces or consumes
//! the data.
//!
//! Because channels are constructed with an `(tx, rx)` pair, the built-in
//! factories produce "empty" sources and sinks that communicate over a
//! zero-capacity channel — the sender is immediately dropped, signalling EOF.
//! For real use-cases, build the channel manually and bypass the factory.
//!
//! ## Config for `ChannelSource`
//!
//! ```toml
//! [[sources]]
//! name = "input"
//! type = "ChannelSource"
//! target_component = "events"
//!
//! [sources.config]
//! buffer = 8        # mpsc buffer capacity, optional (default 8)
//!
//! [[sources.config.schema_fields]]
//! name = "id"
//! type = "Int64"
//! nullable = false
//! ```
//!
//! ## Config for `ChannelSink`
//!
//! ```toml
//! [[sinks]]
//! name = "output"
//! type = "ChannelSink"
//! source_component = "events"
//!
//! [sinks.config]
//! buffer = 8
//!
//! [[sinks.config.schema_fields]]
//! name = "id"
//! type = "Int64"
//! nullable = false
//! ```

use crate::error::PcsError;
use crate::io::channel_sink::ChannelSink;
use crate::io::channel_source::ChannelSource;
use crate::io::sink::Sink;
use crate::io::source::Source;
use crate::service::registry::{SinkFactory, SourceFactory};

use super::parquet::parse_schema_fields;

// ---------------------------------------------------------------------------
// ChannelSourceFactory
// ---------------------------------------------------------------------------

/// Factory for [`ChannelSource`].
///
/// Produces a source backed by a closed channel (EOF immediately). The sender
/// is dropped after construction. For live data, build the channel manually.
///
/// Config fields:
/// - `buffer` (usize, optional, default `8`) — mpsc channel capacity.
/// - `schema_fields` (list, required) — Arrow schema definition.
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
        // Sender is immediately dropped — channel signals EOF.
        let (_tx, src) = ChannelSource::new(schema, buffer);
        Ok(Box::new(src))
    }
}

// ---------------------------------------------------------------------------
// ChannelSinkFactory
// ---------------------------------------------------------------------------

/// Factory for [`ChannelSink`].
///
/// Produces a sink backed by a channel. The receiver is dropped after
/// construction. For live data, build the sink manually.
///
/// Config fields:
/// - `buffer` (usize, optional, default `8`) — mpsc channel capacity.
/// - `schema_fields` (list, required) — Arrow schema definition.
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
        // Receiver is dropped — sink writes to a closed channel.
        let (sink, _rx) = ChannelSink::new(schema, buffer);
        Ok(Box::new(sink))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        // The tx was immediately dropped — channel is closed (EOF).
        // The source must return None on first poll.
        let mut src = src;
        let batch = src.next_batch().await.unwrap();
        assert!(batch.is_none(), "expected EOF (None), got a batch");
    }
}
