use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;

use crate::error::PcsError;

use super::Dataset;

impl Dataset {
    /// Merge `chunks` into a single batch in-place.
    ///
    /// No-op if `chunks.len() <= 1`.
    pub(super) fn compact_chunks(
        chunks: &mut Vec<RecordBatch>,
        schema: &Arc<Schema>,
    ) -> Result<(), PcsError> {
        if chunks.len() <= 1 {
            return Ok(());
        }
        let merged = concat_batches(schema, chunks.iter())
            .map_err(|e| PcsError::generic(format!("arrow concat_batches error: {e}")))?;
        *chunks = vec![merged];
        Ok(())
    }

    /// Merge all components that have more than one pending chunk and warm the
    /// merged cache for each.
    pub(super) fn flush_all_pending(&mut self) -> Result<(), PcsError> {
        let cache = self.merged_cache.get_mut().unwrap();
        for (name, chunks) in &mut self.components {
            if chunks.len() > 1 {
                let schema = self
                    .schemas
                    .get(name)
                    .expect("schema registered but get() failed — internal inconsistency");
                let merged = concat_batches(&schema, chunks.iter())
                    .map_err(|e| PcsError::generic(format!("arrow concat_batches error: {e}")))?;
                *chunks = vec![merged];
                cache.insert(name, Box::new(chunks[0].clone()));
            } else if !cache.contains_key(name)
                && let Some(batch) = chunks.first()
            {
                cache.insert(name, Box::new(batch.clone()));
            }
        }
        Ok(())
    }

    /// Return a reference to the merged `RecordBatch` for `name`.
    ///
    /// # Safety (internal)
    ///
    /// We extend the lifetime of the raw pointer obtained from inside the
    /// `Mutex` guard to `'self`. This is sound because:
    ///
    /// 1. The `RecordBatch` lives on the heap behind a `Box` owned by
    ///    `self.merged_cache`. Its heap address is stable regardless of
    ///    `HashMap` reallocations (the `Box` pointer moves; the heap
    ///    allocation it points to does not).
    /// 2. We hold `&self`, so no `&mut Dataset` can coexist.
    /// 3. The `Mutex` serialises concurrent `&self` reads that both trigger
    ///    cache population, preventing data races on the `HashMap` itself.
    pub(super) fn get_or_build_merged(&self, name: &'static str) -> &RecordBatch {
        let mut cache = self.merged_cache.lock().unwrap();

        if let Some(boxed) = cache.get(name) {
            // SAFETY: see method-level doc comment.
            let ptr: *const RecordBatch = boxed.as_ref();
            drop(cache);
            return unsafe { &*ptr };
        }

        let chunks = self
            .components
            .get(name)
            .expect("get_or_build_merged called for unregistered component");

        let merged: RecordBatch = if chunks.len() == 1 {
            chunks[0].clone()
        } else {
            let schema = self
                .schemas
                .get(name)
                .expect("schema registered but get() failed — internal inconsistency");
            concat_batches(&schema, chunks.iter())
                .expect("concat_batches failed during merged-cache build")
        };

        cache.insert(name, Box::new(merged));
        // SAFETY: see method-level doc comment.
        let ptr: *const RecordBatch = cache[name].as_ref();
        drop(cache);
        unsafe { &*ptr }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::UInt64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;

    #[derive(Serialize, Deserialize)]
    struct CkOrder {
        id: u64,
    }

    impl Component for CkOrder {
        fn name() -> &'static str {
            "CkOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
        }
    }

    #[test]
    fn test_many_appends_merged_correctly() {
        let mut ds = Dataset::new();
        ds.register_component::<CkOrder>().unwrap();
        for i in 0..100usize {
            ds.append::<CkOrder>(&[CkOrder { id: i as u64 }]).unwrap();
        }
        let batch = ds.columns::<CkOrder>().unwrap();
        assert_eq!(batch.num_rows(), 100);
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 0u64);
        assert_eq!(arr.value(99), 99u64);
    }
}
