//! Built-in factory re-exports and registration.
//!
//! Two kinds of factory reach the registry here. A connector factory builds a
//! [`Source`](pcs_core::io::source::Source) or a
//! [`Sink`](pcs_core::io::sink::Sink) that moves bytes; a transformer factory
//! builds the byte format a connector's `format` key names. One feature per
//! crate decides whether it is compiled in and registered:
//!
//! - `connector-channel`: [`ChannelSourceFactory`], [`ChannelSinkFactory`]
//! - `connector-file`: [`FileSourceFactory`], [`FileSinkFactory`]
//! - `connector-http`: [`HttpSourceFactory`], [`HttpSinkFactory`]
//! - `connector-kafka`: [`KafkaSourceFactory`], [`KafkaSinkFactory`]
//! - `connector-nats`: [`NatsSourceFactory`], [`NatsSinkFactory`]
//! - `connector-s3`: [`S3SourceFactory`], [`S3SinkFactory`]
//! - `connector-tcp`: [`TcpSourceFactory`], [`TcpSinkFactory`]
//! - `transformer-arrow-ipc`: [`ArrowIpcTransformerFactory`]
//! - `transformer-avro`: [`AvroTransformerFactory`]
//! - `transformer-csv`: [`CsvTransformerFactory`]
//! - `transformer-ndjson`: [`NdjsonTransformerFactory`]
//! - `transformer-parquet`: [`ParquetTransformerFactory`]
//!
//! [`register_builtin_factories`] adds every enabled factory to a
//! [`ServiceBuilder`] in one call.

#[cfg(feature = "connector-channel")]
pub use pcs_connector_channel::{ChannelSinkFactory, ChannelSourceFactory};
#[cfg(feature = "connector-file")]
pub use pcs_connector_file::{FileSinkFactory, FileSourceFactory};
#[cfg(feature = "connector-http")]
pub use pcs_connector_http::{HttpSinkFactory, HttpSourceFactory};
#[cfg(feature = "connector-kafka")]
pub use pcs_connector_kafka::{KafkaSinkFactory, KafkaSourceFactory};
#[cfg(feature = "connector-nats")]
pub use pcs_connector_nats::{NatsSinkFactory, NatsSourceFactory};
#[cfg(feature = "connector-postgresql")]
pub use pcs_connector_postgresql::{PostgresSinkFactory, PostgresSourceFactory};
#[cfg(feature = "connector-s3")]
pub use pcs_connector_s3::{S3SinkFactory, S3SourceFactory};
#[cfg(feature = "connector-tcp")]
pub use pcs_connector_tcp::{TcpSinkFactory, TcpSourceFactory};
#[cfg(feature = "transformer-arrow-ipc")]
pub use pcs_transformer_arrow_ipc::ArrowIpcTransformerFactory;
#[cfg(feature = "transformer-avro")]
pub use pcs_transformer_avro::AvroTransformerFactory;
#[cfg(feature = "transformer-csv")]
pub use pcs_transformer_csv::CsvTransformerFactory;
#[cfg(feature = "transformer-ndjson")]
pub use pcs_transformer_ndjson::NdjsonTransformerFactory;
#[cfg(feature = "transformer-parquet")]
pub use pcs_transformer_parquet::ParquetTransformerFactory;

use super::builder::ServiceBuilder;

/// Register every enabled connector's and transformer's factories into
/// `builder`.
///
/// A crate reaches the registry only when its feature is on. Supply the runtime
/// separately through [`ServiceBuilder::with_runtime`] or `pipeline.wasm` in the
/// config.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use pcs_service::service::builder::ServiceBuilder;
/// use pcs_service::service::factories::register_builtin_factories;
///
/// let builder = register_builtin_factories(ServiceBuilder::new());
/// # }
/// ```
pub fn register_builtin_factories(builder: ServiceBuilder) -> ServiceBuilder {
    // Transformers first: a connector factory resolves its `format` key against
    // the registry, so the format has to be in it by the time config is built.
    #[cfg(feature = "transformer-arrow-ipc")]
    let builder = builder.register_transformer(ArrowIpcTransformerFactory);

    #[cfg(feature = "transformer-avro")]
    let builder = builder.register_transformer(AvroTransformerFactory);

    #[cfg(feature = "transformer-csv")]
    let builder = builder.register_transformer(CsvTransformerFactory);

    #[cfg(feature = "transformer-ndjson")]
    let builder = builder.register_transformer(NdjsonTransformerFactory);

    #[cfg(feature = "transformer-parquet")]
    let builder = builder.register_transformer(ParquetTransformerFactory);

    #[cfg(feature = "connector-channel")]
    let builder = builder
        .register_source(ChannelSourceFactory)
        .register_sink(ChannelSinkFactory)
        .with_channel_bridge(std::sync::Arc::new(
            pcs_connector_channel::ChannelRegistry::default(),
        ));

    #[cfg(feature = "connector-file")]
    let builder = builder
        .register_source(FileSourceFactory)
        .register_sink(FileSinkFactory);

    #[cfg(feature = "connector-http")]
    let builder = builder
        .register_source(HttpSourceFactory)
        .register_sink(HttpSinkFactory);

    #[cfg(feature = "connector-kafka")]
    let builder = builder
        .register_source(KafkaSourceFactory)
        .register_sink(KafkaSinkFactory);

    #[cfg(feature = "connector-nats")]
    let builder = builder
        .register_source(NatsSourceFactory)
        .register_sink(NatsSinkFactory);

    #[cfg(feature = "connector-postgresql")]
    let builder = builder
        .register_source(PostgresSourceFactory)
        .register_sink(PostgresSinkFactory);

    #[cfg(feature = "connector-s3")]
    let builder = builder
        .register_source(S3SourceFactory)
        .register_sink(S3SinkFactory);

    #[cfg(feature = "connector-tcp")]
    let builder = builder
        .register_source(TcpSourceFactory)
        .register_sink(TcpSinkFactory);

    builder
}
