//! Built-in factory implementations shipped with PCS.
//!
//! These factories cover the IO source and sink types PCS ships natively.
//! All IO factories are gated on `feature = "io"`.
//!
//! ## Included factories
//!
//! - **IO sources** (feature = "io"): [`ParquetSourceFactory`], [`JsonSourceFactory`],
//!   [`CsvSourceFactory`], [`ChannelSourceFactory`]
//! - **IO sinks** (feature = "io"): [`ParquetSinkFactory`], [`JsonSinkFactory`],
//!   [`CsvSinkFactory`], [`ChannelSinkFactory`]
//!
//! ## Convenience registration
//!
//! Use [`register_builtin_factories`] to add all built-in factories to a
//! [`ServiceBuilder`] in one call.
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use pcs_service::service::builder::ServiceBuilder;
//! use pcs_service::service::factories::register_builtin_factories;
//!
//! let builder = register_builtin_factories(ServiceBuilder::new());
//! # }
//! ```

#[cfg(feature = "io")]
pub mod channel;
#[cfg(feature = "io")]
pub mod csv;
#[cfg(feature = "io")]
pub mod json;
#[cfg(feature = "io")]
pub mod parquet;

#[cfg(feature = "io")]
pub use channel::{ChannelSinkFactory, ChannelSourceFactory};
#[cfg(feature = "io")]
pub use csv::{CsvSinkFactory, CsvSourceFactory};
#[cfg(feature = "io")]
pub use json::{JsonSinkFactory, JsonSourceFactory};
#[cfg(feature = "io")]
pub use parquet::{ParquetSinkFactory, ParquetSourceFactory};

use super::builder::ServiceBuilder;

/// Register all built-in IO factories into `builder`.
///
/// This includes IO source/sink factories (Parquet, JSON, CSV, Channel) —
/// all gated on `feature = "io"`. The runtime must be provided separately
/// via [`ServiceBuilder::with_runtime`] or via `pipeline.wasm` in the config.
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
        .register_sink(ParquetSinkFactory)
        .register_sink(JsonSinkFactory)
        .register_sink(CsvSinkFactory)
        .register_sink(ChannelSinkFactory);

    builder
}
