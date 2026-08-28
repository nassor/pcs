pub mod cast;
pub mod sink;
pub mod source;

pub use cast::{CastingSource, build_target_schema, cast_batch};
pub use sink::{Sink, drain_dataset};
pub use source::{Source, drain_into_dataset};
