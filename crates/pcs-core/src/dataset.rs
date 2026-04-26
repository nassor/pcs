//! [`Dataset`] — the Arrow-backed columnar data container.
//!
//! `Dataset` stores data in Apache Arrow [`RecordBatch`]es, one per registered
//! component type. All batches share the same row count so that any row index is
//! valid across all components simultaneously.
//!
//! ## Design goals
//!
//! - **Column-first access**: there is intentionally no per-row `get::<C>(row)`.
//!   Callers should read a whole column (or a projection) and operate over it.
//! - **Batch-only ingestion**: data enters via [`append`](Dataset::append) only,
//!   which adds an aligned slice across all components in one shot.
//! - **Lazy deletes**: [`mark_dead`](Dataset::mark_dead) flips a validity bit.
//!   [`compact`](Dataset::compact) filters all batches at once when the dead
//!   fraction is large enough.
//! - **IPC round-trip**: [`write_ipc`](Dataset::write_ipc) /
//!   [`read_ipc`](Dataset::read_ipc) serialise and reconstruct the whole dataset
//!   as an Arrow IPC stream with no intermediate copying.

use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, OnceLock},
};

use arrow_array::RecordBatch;
use arrow_buffer::builder::BooleanBufferBuilder;
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};

use crate::{resource::ResourceMap, schema::SchemaRegistry};

pub(crate) const COMPONENT_NAME_KEY: &str = "__pcs_component";
pub(crate) const SCHEMA_VERSION_KEY: &str = "__pcs_schema_version";

const ALIVE_BATCH_NAME: &str = "__alive";

const MERGE_THRESHOLD: usize = 16;

// ---------------------------------------------------------------------------
// Component-name interner
// ---------------------------------------------------------------------------

static NAME_INTERNER: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();

pub(crate) fn intern_component_name(name: &str) -> &'static str {
    let set = NAME_INTERNER.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = set.lock().unwrap();
    if let Some(existing) = guard.get(name) {
        return existing;
    }
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    guard.insert(leaked);
    leaked
}

/// Columnar data container backed by Apache Arrow [`RecordBatch`]es.
///
/// All component data lives in columnar form; resources remain as boxed Rust
/// values. Row indices (`Row`) are stable until [`compact`](Self::compact) is
/// called.
///
/// ## Invariant
///
/// Every registered component's accumulated chunks have exactly `row_count`
/// rows in total. The alive bitmap also has exactly `row_count` bits.
pub struct Dataset {
    pub(crate) components: HashMap<&'static str, Vec<RecordBatch>>,
    merged_cache: Mutex<HashMap<&'static str, Box<RecordBatch>>>,
    schemas: SchemaRegistry,
    row_count: usize,
    alive: BooleanBufferBuilder,
    live_count: usize,
    dead_count: usize,
    resources: ResourceMap,
}

// SAFETY: `BooleanBufferBuilder` contains a raw pointer internally, but it is
// only accessed through `&mut self` methods, and `Dataset` is otherwise composed
// of `Send + Sync` types.  The merged_cache `Mutex` ensures interior-mutability
// accesses from `&self` are data-race-free.
unsafe impl Send for Dataset {}
unsafe impl Sync for Dataset {}

impl Dataset {
    /// Create an empty dataset with no components and no resources.
    ///
    /// # Example
    ///
    /// ```rust
    /// #
    /// # {
    /// use pcs_core::pipeline::Dataset;
    /// let dataset = Dataset::new();
    /// assert_eq!(dataset.rows(), 0);
    /// # }
    /// ```
    pub fn new() -> Self {
        Self {
            components: HashMap::new(),
            merged_cache: Mutex::new(HashMap::new()),
            schemas: SchemaRegistry::new(),
            row_count: 0,
            alive: BooleanBufferBuilder::new(0),
            live_count: 0,
            dead_count: 0,
            resources: ResourceMap::new(),
        }
    }
}

impl Default for Dataset {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// DatasetBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for [`Dataset`].
///
/// # Example
///
/// ```rust
/// # {
/// # use std::sync::Arc;
/// # use arrow_schema::{DataType, Field, Schema};
/// # use pcs_core::component::Component;
/// # use pcs_core::pipeline::{Dataset, DatasetBuilder};
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct Order { id: u64 }
/// # impl Component for Order {
/// #     fn name() -> &'static str { "Order" }
/// #     fn schema() -> Arc<Schema> {
/// #         Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
/// #     }
/// # }
/// struct Config { max: u32 }
/// let dataset = Dataset::builder()
///     .with::<Order>()
///     .with_resource(Config { max: 100 })
///     .build();
/// # }
/// ```
pub struct DatasetBuilder(pub(crate) Dataset);

// ---------------------------------------------------------------------------
// IPC helpers (used by Dataset and Pipeline)
// ---------------------------------------------------------------------------

pub(crate) fn batch_to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>, crate::error::PcsError> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::default();
    {
        let mut sw = StreamWriter::try_new_with_options(&mut buf, batch.schema_ref(), options)
            .map_err(|e| crate::error::PcsError::generic(format!("IPC StreamWriter init: {e}")))?;
        sw.write(batch)
            .map_err(|e| crate::error::PcsError::generic(format!("IPC StreamWriter write: {e}")))?;
        sw.finish().map_err(|e| {
            crate::error::PcsError::generic(format!("IPC StreamWriter finish: {e}"))
        })?;
    }
    Ok(buf)
}

pub(crate) fn ipc_bytes_to_batch(bytes: &[u8]) -> Result<RecordBatch, crate::error::PcsError> {
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| crate::error::PcsError::generic(format!("IPC StreamReader init: {e}")))?;
    let batch = reader
        .next()
        .ok_or_else(|| crate::error::PcsError::generic("IPC stream contained no batches"))?
        .map_err(|e| crate::error::PcsError::generic(format!("IPC StreamReader read: {e}")))?;
    Ok(batch)
}

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

mod append;
mod builder;
mod chunks;
mod ipc;
mod lifecycle;
mod reads;
mod register;
mod resources;
mod write;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_component_name_pointer_stable() {
        let p1 = intern_component_name("__test_intern_dataset_foo__");
        let p2 = intern_component_name("__test_intern_dataset_foo__");
        assert!(std::ptr::eq(p1, p2));
    }

    #[test]
    fn test_intern_component_name_distinct_names() {
        let p1 = intern_component_name("__test_intern_dataset_alpha__");
        let p2 = intern_component_name("__test_intern_dataset_beta__");
        assert!(!std::ptr::eq(p1, p2));
    }
}
