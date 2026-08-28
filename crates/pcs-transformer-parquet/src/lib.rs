//! `pcs-transformer-parquet`: the `parquet` byte format for PCS.
//!
//! Parquet is self-describing, so [`ParquetTransformer`] reads its schema from
//! the file footer and rejects a declared one. It is also the only format that
//! reports `estimated_rows`, summed from row-group metadata without reading any
//! data. Writes use Snappy compression.
//!
//! Stream surface only: a Parquet file's footer is what makes it readable, so
//! there is no per-message form of it.
//!
//! ```kdl
//! source "trades" type="FileSource" {
//!     config path="/data/trades.parquet" format="parquet"
//! }
//! ```

#![deny(missing_docs)]

pub mod transformer;

pub use transformer::{ParquetTransformer, ParquetTransformerFactory};
