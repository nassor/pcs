//! `pcs-transformer-arrow-ipc`: the `arrow-ipc` byte format for PCS.
//!
//! One payload is one Arrow IPC stream: a schema header followed by one or more
//! `RecordBatch`es. This is the format a TCP frame and an Arrow-native Kafka
//! topic carry, and it is the only one that needs no schema declaration on the
//! wire while still checking the one it was given.
//!
//! Message surface only. A `.arrows` file source and sink do not exist in PCS,
//! so [`ArrowIpcTransformer`] leaves the stream surface at the transformer
//! contract's `unsupported` error rather than inventing one.
//!
//! ```kdl
//! source "wire" type="tcp" {
//!     config bind="0.0.0.0:9500" format="arrow-ipc"
//! }
//! ```

#![deny(missing_docs)]

pub mod transformer;

pub use transformer::{ArrowIpcTransformer, ArrowIpcTransformerFactory};
