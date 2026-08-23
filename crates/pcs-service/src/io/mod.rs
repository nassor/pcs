//! Ingestion and egress layer for the Arrow pipeline.
//!
//! [`Source`] and [`Sink`] read data into and write it out of a
//! [`Dataset`](crate::dataset::Dataset), with implementations for Parquet, JSON
//! Lines, CSV, and an in-memory channel transport.

// Trait definitions and channel impls live in pcs-core; re-exported here so
// paths like `pcs_service::io::source::Source` resolve.
pub use pcs_core::io::cast;
pub use pcs_core::io::channel_sink;
pub use pcs_core::io::channel_source;
pub use pcs_core::io::sink;
pub use pcs_core::io::source;

pub mod csv_sink;
pub mod csv_source;
pub mod json_sink;
pub mod json_source;
pub mod parquet_sink;
pub mod parquet_source;
/// Live TCP ingest. Requires the `service` feature (tokio net, io-util).
#[cfg(feature = "service")]
pub mod tcp_source;

pub use cast::{CastingSource, build_target_schema, cast_batch};
pub use channel_sink::ChannelSink;
pub use channel_source::ChannelSource;
pub use csv_sink::CsvSink;
pub use csv_source::CsvSource;
pub use json_sink::JsonSink;
pub use json_source::JsonSource;
pub use parquet_sink::ParquetSink;
pub use parquet_source::ParquetSource;
pub use sink::{Sink, drain_dataset};
pub use source::{Source, drain_into_dataset};
#[cfg(feature = "service")]
pub use tcp_source::TcpIngestSource;

#[cfg(feature = "datafusion")]
pub mod datafusion_source;
#[cfg(feature = "datafusion")]
pub use datafusion_source::DataFusionSource;
