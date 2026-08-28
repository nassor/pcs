//! The channel source and sink factories.
//!
//! `ChannelSource` and `ChannelSink` are in-memory, mpsc-backed IO endpoints.
//! Both resolve their half of a named channel through the host's registered
//! [`pcs_connector::ChannelBridge`]: a `ChannelSink` in one workflow and a
//! `ChannelSource` in another meet on one shared `mpsc` pair, so the
//! producer's sink dropping is what signals the consumer's source EOF.
//!
//! Both take `name` (required, the channel to resolve through the bridge),
//! `buffer` (mpsc capacity, default 8) and `schema_fields` in their `config`
//! table.

use pcs_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use pcs_core::error::PcsError;
use pcs_core::io::{sink::Sink, source::Source};

fn buffer_of(config: &ConfigValue) -> usize {
    config
        .get("buffer")
        .and_then(|v| v.as_i64())
        .map(|v| v.max(0) as usize)
        .unwrap_or(8)
}

fn name_of<'a>(config: &'a ConfigValue, what: &str) -> Result<&'a str, PcsError> {
    config.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
        PcsError::configuration(format!(
            "{what} config requires a 'name' key naming the shared channel"
        ))
    })
}

/// Factory for [`ChannelSource`](crate::ChannelSource).
///
/// Resolves the source half of the named channel through the host's
/// registered [`pcs_connector::ChannelBridge`].
///
/// Config fields:
/// - `name` (string, required): the channel name, shared with the paired
///   `ChannelSink`.
/// - `buffer` (usize, optional, default `8`): mpsc channel capacity.
/// - `schema_fields` (list, required): Arrow schema definition.
pub struct ChannelSourceFactory;

impl SourceFactory for ChannelSourceFactory {
    fn type_name(&self) -> &'static str {
        "ChannelSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, PcsError> {
        let bridge = ctx.channel_bridge().ok_or_else(|| {
            PcsError::configuration(
                "ChannelSource requires a channel bridge; register one via \
                 ServiceBuilder::with_channel_bridge",
            )
        })?;
        let name = name_of(config, "ChannelSource")?;
        let buffer = buffer_of(config);
        let schema = parse_schema_fields(config, "ChannelSource")?;
        bridge.source(name, schema, buffer)
    }
}

/// Factory for [`ChannelSink`](crate::ChannelSink).
///
/// Resolves the sink half of the named channel through the host's registered
/// [`pcs_connector::ChannelBridge`].
///
/// Config fields:
/// - `name` (string, required): the channel name, shared with the paired
///   `ChannelSource`.
/// - `buffer` (usize, optional, default `8`): mpsc channel capacity.
/// - `schema_fields` (list, required): Arrow schema definition.
pub struct ChannelSinkFactory;

impl SinkFactory for ChannelSinkFactory {
    fn type_name(&self) -> &'static str {
        "ChannelSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, PcsError> {
        let bridge = ctx.channel_bridge().ok_or_else(|| {
            PcsError::configuration(
                "ChannelSink requires a channel bridge; register one via \
                 ServiceBuilder::with_channel_bridge",
            )
        })?;
        let name = name_of(config, "ChannelSink")?;
        let buffer = buffer_of(config);
        let schema = parse_schema_fields(config, "ChannelSink")?;
        bridge.sink(name, schema, buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pcs_connector::{ConfigMap, from_kdl_str};
    use std::sync::Arc;

    use crate::ChannelRegistry;

    fn schema_config() -> ConfigValue {
        from_kdl_str(
            r#"
name "bridge"
buffer 4

schema_fields "id" type="Int64" nullable=#false
"#,
        )
        .unwrap()
    }

    fn ctx_with_bridge() -> ConnectorContext {
        ConnectorContext::new(None).with_channels(Arc::new(ChannelRegistry::default()))
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
    fn test_channel_source_and_sink_pair_through_the_bridge() {
        let ctx = ctx_with_bridge();
        let sink = ChannelSinkFactory.build(&schema_config(), &ctx).unwrap();
        let src = ChannelSourceFactory.build(&schema_config(), &ctx).unwrap();
        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn test_channel_source_missing_schema_fields_returns_error() {
        let ctx = ctx_with_bridge();
        let mut config = ConfigMap::new();
        config.insert(
            "name".to_string(),
            ConfigValue::String("bridge".to_string()),
        );
        let err = ChannelSourceFactory
            .build(&ConfigValue::Object(config), &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"));
    }

    #[test]
    fn test_channel_source_missing_name_returns_error() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str("schema_fields \"id\" type=\"Int64\" nullable=#false\n").unwrap();
        let err = ChannelSourceFactory
            .build(&raw, &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'name'"));
    }

    #[test]
    fn test_channel_sink_missing_name_returns_error() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str("schema_fields \"id\" type=\"Int64\" nullable=#false\n").unwrap();
        let err = ChannelSinkFactory
            .build(&raw, &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'name'"));
    }

    #[test]
    fn test_channel_source_without_a_registered_bridge_returns_error() {
        let err = ChannelSourceFactory
            .build(&schema_config(), &ConnectorContext::new(None))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("channel bridge"));
    }

    #[test]
    fn test_channel_sink_without_a_registered_bridge_returns_error() {
        let err = ChannelSinkFactory
            .build(&schema_config(), &ConnectorContext::new(None))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("channel bridge"));
    }
}
