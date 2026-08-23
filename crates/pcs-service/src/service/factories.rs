//! Built-in factory implementations shipped with PCS.
//!
//! IO sources: [`ParquetSourceFactory`], [`JsonSourceFactory`],
//! [`CsvSourceFactory`], [`ChannelSourceFactory`], [`TcpSourceFactory`].
//! IO sinks: [`ParquetSinkFactory`], [`JsonSinkFactory`], [`CsvSinkFactory`],
//! [`ChannelSinkFactory`]. All are gated on `feature = "io"`.
//!
//! [`register_builtin_factories`] adds every built-in factory to a
//! [`ServiceBuilder`] in one call.

#[cfg(feature = "io")]
pub mod channel;
#[cfg(feature = "io")]
pub mod csv;
#[cfg(feature = "io")]
pub mod json;
#[cfg(feature = "io")]
pub mod parquet;
#[cfg(feature = "io")]
pub mod tcp;

#[cfg(feature = "io")]
pub use channel::{ChannelSinkFactory, ChannelSourceFactory};
#[cfg(feature = "io")]
pub use csv::{CsvSinkFactory, CsvSourceFactory};
#[cfg(feature = "io")]
pub use json::{JsonSinkFactory, JsonSourceFactory};
#[cfg(feature = "io")]
pub use parquet::{ParquetSinkFactory, ParquetSourceFactory};
#[cfg(feature = "io")]
pub use tcp::TcpSourceFactory;

use super::builder::ServiceBuilder;

/// Register all built-in IO factories into `builder`.
///
/// Covers the Parquet, JSON, CSV, Channel, and TCP source and sink factories,
/// all gated on `feature = "io"`. Supply the runtime separately through
/// [`ServiceBuilder::with_runtime`] or `pipeline.wasm` in the config.
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
    #[cfg(not(feature = "io"))]
    let builder = builder;

    #[cfg(feature = "io")]
    let builder = builder
        .register_source(ParquetSourceFactory)
        .register_source(JsonSourceFactory)
        .register_source(CsvSourceFactory)
        .register_source(ChannelSourceFactory)
        .register_source(TcpSourceFactory)
        .register_sink(ParquetSinkFactory)
        .register_sink(JsonSinkFactory)
        .register_sink(CsvSinkFactory)
        .register_sink(ChannelSinkFactory);

    builder
}
