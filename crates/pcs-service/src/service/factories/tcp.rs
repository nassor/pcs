//! Built-in TCP ingest source factory.
//!
//! [`TcpIngestSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run_mode` `kind = "stream"`).
//! [`ServiceConfig::validate`](crate::service::config::ServiceConfig::validate)
//! rejects `type = "tcp"` in any other mode.
//!
//! Producers write a `u32` big-endian length plus one Arrow IPC stream payload
//! (schema header and exactly one `RecordBatch`) per frame.

use crate::error::PcsError;
use crate::io::source::Source;
use crate::io::tcp_source::TcpIngestSource;
use crate::service::registry::SourceFactory;

use super::parquet::parse_schema_fields;

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
pub struct TcpSourceFactory;

impl SourceFactory for TcpSourceFactory {
    fn type_name(&self) -> &'static str {
        "tcp"
    }

    fn build(&self, config: &toml::Value) -> Result<Box<dyn Source>, PcsError> {
        let bind = config
            .get("bind")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PcsError::configuration("tcp source config requires a 'bind' string"))?;

        let buffer = config
            .get("buffer")
            .and_then(|v| v.as_integer())
            .map(|v| v.max(1) as usize)
            .unwrap_or(DEFAULT_BUFFER);

        let max_frame_bytes = config
            .get("max_frame_bytes")
            .and_then(|v| v.as_integer())
            .map(|v| v.max(1) as usize)
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES);

        let schema = parse_schema_fields(config, "tcp")?;

        Ok(Box::new(TcpIngestSource::new(
            bind,
            schema,
            buffer,
            max_frame_bytes,
        )?))
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    fn config(extra: &str) -> toml::Value {
        let raw = format!(
            r#"
bind = "127.0.0.1:0"
{extra}

[[schema_fields]]
name = "v"
type = "Int64"
nullable = false
"#
        );
        toml::from_str(&raw).expect("parse test config")
    }

    #[test]
    fn builds_with_defaults() {
        let source = TcpSourceFactory.build(&config("")).expect("build");
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn missing_bind_is_a_configuration_error() {
        let cfg: toml::Value = toml::from_str(
            r#"
[[schema_fields]]
name = "v"
type = "Int64"
"#,
        )
        .unwrap();
        let err = TcpSourceFactory.build(&cfg).err().expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'bind'"), "got: {err}");
    }

    #[test]
    fn missing_schema_fields_is_a_configuration_error() {
        let cfg: toml::Value = toml::from_str("bind = \"127.0.0.1:0\"\n").unwrap();
        let err = TcpSourceFactory.build(&cfg).err().expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("schema_fields"), "got: {err}");
    }
}
