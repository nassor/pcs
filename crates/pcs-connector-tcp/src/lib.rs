//! `pcs-connector-tcp`: a live TCP [`Source`] for PCS stream mode.
//!
//! [`Source`]: pcs_core::io::source::Source
//!
//! [`TcpIngestSource`] listens on `bind` and yields one `RecordBatch` per
//! received frame. A frame is a `u32` big-endian length prefix followed by that
//! many payload bytes; the [`Transformer`](pcs_transformer::Transformer) the
//! host resolved from the source's `transformer` key decodes them.
//!
//! Framing is transport and belongs here; decoding is format and does not. That
//! split is why the same listener carries Arrow IPC or newline-delimited JSON
//! without a line of code in this crate knowing either.
//!
//! The source never reaches EOF, so only the stream runner can drive it. Its
//! `config` table takes `bind`, an optional `buffer` and `max_frame_bytes`, and
//! a `schema_fields` array declaring the Arrow schema.
//!
//! ```kdl
//! transformer "frames-ipc" name="Frames Arrow IPC" format="arrow-ipc"
//!
//! source "ingest" type="tcp" transformer="frames-ipc" component="Reading" {
//!     config bind="0.0.0.0:9500" max_frame_bytes=8388608 {
//!         schema_fields "v" type="Int64" nullable=#false
//!     }
//! }
//! ```

pub mod factory;
pub mod source;

pub use factory::TcpSourceFactory;
pub use source::TcpIngestSource;
