//! The TCP ingest source factory.
//!
//! [`TcpIngestSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run_mode kind="stream"`).
//! The service config validator rejects `type="tcp"` in any other mode.
//!
//! Producers write a `u32` big-endian length plus one payload per frame. What
//! the payload bytes are is the transformer's business: the source's
//! `transformer` key names a declared `transformer` node, and that node's
//! format decides how each frame decodes. There is no default.

use pcs_connector::{ConfigValue, ConnectorContext, SourceFactory, parse_schema_fields};
use pcs_core::error::PcsError;
use pcs_core::io::source::Source;

use crate::TcpIngestSource;

/// Default number of decoded batches queued before backpressure.
const DEFAULT_BUFFER: usize = 64;
/// Default per-frame size cap (8 MiB).
const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Factory for [`TcpIngestSource`].
///
/// Config fields:
/// - `bind` (string, required): listen address, e.g. `"0.0.0.0:9500"`.
/// - `buffer` (usize, optional, default `64`): queued batch capacity.
/// - `max_frame_bytes` (usize, optional, default `8388608`): per-frame cap.
/// - `schema_fields` (list, required): Arrow schema definition.
///
/// Each frame's payload is decoded by the transformer the host bound to this
/// node from its `transformer` key, reached through the [`ConnectorContext`]
/// rather than read out of `config`.
pub struct TcpSourceFactory;

impl SourceFactory for TcpSourceFactory {
    fn type_name(&self) -> &'static str {
        "tcp"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, PcsError> {
        let bind = config
            .get("bind")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PcsError::configuration("tcp source config requires a 'bind' string"))?;

        let buffer = config
            .get("buffer")
            .and_then(|v| v.as_i64())
            .map(|v| v.max(1) as usize)
            .unwrap_or(DEFAULT_BUFFER);

        let max_frame_bytes = config
            .get("max_frame_bytes")
            .and_then(|v| v.as_i64())
            .map(|v| v.max(1) as usize)
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES);

        let schema = parse_schema_fields(config, "tcp")?;
        let transformer = ctx.transformer("tcp")?;

        Ok(Box::new(TcpIngestSource::new(
            bind,
            schema,
            buffer,
            max_frame_bytes,
            transformer,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pcs_connector::{ConfigMap, from_kdl_str};
    use pcs_transformer::{Transformer, TransformerFactory};
    use pcs_transformer_arrow_ipc::ArrowIpcTransformerFactory;
    use std::sync::Arc;

    /// A built transformer, the way the host hands one to a factory.
    fn transformer() -> Arc<dyn Transformer> {
        ArrowIpcTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("build arrow-ipc transformer")
    }

    fn config(extra: &str) -> ConfigValue {
        let raw = format!(
            r#"
bind "127.0.0.1:0"
{extra}

schema_fields "v" type="Int64" nullable=#false
"#
        );
        from_kdl_str(&raw).expect("parse test config")
    }

    #[test]
    fn builds_with_defaults() {
        let source = TcpSourceFactory
            .build(&config(""), &ConnectorContext::new(Some(transformer())))
            .expect("build");
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn missing_bind_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
schema_fields "v" type="Int64"
"#,
        )
        .unwrap();
        let err = TcpSourceFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'bind'"), "got: {err}");
    }

    #[test]
    fn a_missing_transformer_is_a_configuration_error() {
        let err = TcpSourceFactory
            .build(&config(""), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'transformer'"), "got: {err}");
    }

    #[test]
    fn missing_schema_fields_is_a_configuration_error() {
        let cfg = from_kdl_str("bind \"127.0.0.1:0\"\n").unwrap();
        let err = TcpSourceFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("schema_fields"), "got: {err}");
    }
}
