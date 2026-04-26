use std::io::{Read, Write};
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_buffer::builder::BooleanBufferBuilder;
use arrow_schema::{DataType, Field, Schema};

use crate::component::Component;
use crate::error::PcsError;

use super::{
    ALIVE_BATCH_NAME, COMPONENT_NAME_KEY, Dataset, SCHEMA_VERSION_KEY, batch_to_ipc_bytes,
    intern_component_name, ipc_bytes_to_batch,
};

impl Dataset {
    fn annotate_batch(
        batch: &RecordBatch,
        name: &str,
        version: u32,
    ) -> Result<RecordBatch, PcsError> {
        let mut meta = batch.schema().metadata().clone();
        meta.insert(COMPONENT_NAME_KEY.to_string(), name.to_string());
        meta.insert(SCHEMA_VERSION_KEY.to_string(), version.to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            batch.schema().fields().iter().cloned().collect::<Vec<_>>(),
            meta,
        ));
        RecordBatch::try_new(schema, batch.columns().to_vec())
            .map_err(|e| PcsError::generic(format!("IPC rebuild batch error: {e}")))
    }

    fn alive_ipc_bytes(&self) -> Result<Vec<u8>, PcsError> {
        let alive_array: ArrayRef = Arc::new(BooleanArray::new(self.alive.finish_cloned(), None));
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("alive", DataType::Boolean, false)],
            [(COMPONENT_NAME_KEY.to_string(), ALIVE_BATCH_NAME.to_string())]
                .into_iter()
                .collect(),
        ));
        let batch = RecordBatch::try_new(schema, vec![alive_array])
            .map_err(|e| PcsError::generic(format!("IPC alive batch error: {e}")))?;
        let seg = batch_to_ipc_bytes(&batch)?;
        let mut out = Vec::with_capacity(4 + seg.len());
        out.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        out.extend_from_slice(&seg);
        Ok(out)
    }

    /// Serialise the whole dataset to an Arrow IPC stream.
    ///
    /// Each component is written as one `RecordBatch` with a metadata entry
    /// `__pcs_component = <name>`. The alive bitmap is written last as a
    /// pseudo-batch under the `__alive` key.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` on any Arrow IPC writer error.
    pub fn write_ipc<W: Write>(&self, writer: &mut W) -> Result<(), PcsError> {
        let mut names: Vec<&&str> = self.components.keys().collect();
        names.sort();

        for name in names {
            let batch = self.get_or_build_merged(name);
            let version = self.schemas.get_version(name).unwrap_or(1);
            let annotated = Self::annotate_batch(batch, name, version)?;
            let segment = batch_to_ipc_bytes(&annotated)?;
            let len = segment.len() as u32;
            writer
                .write_all(&len.to_le_bytes())
                .map_err(|e| PcsError::generic(format!("IPC write len: {e}")))?;
            writer
                .write_all(&segment)
                .map_err(|e| PcsError::generic(format!("IPC write segment: {e}")))?;
        }

        let alive_seg = self.alive_ipc_bytes()?;
        writer
            .write_all(&alive_seg)
            .map_err(|e| PcsError::generic(format!("IPC write alive: {e}")))?;

        writer
            .write_all(&0u32.to_le_bytes())
            .map_err(|e| PcsError::generic(format!("IPC write sentinel: {e}")))?;

        Ok(())
    }

    /// Serialise a single component's `RecordBatch` to an Arrow IPC stream.
    ///
    /// Produces the same length-prefixed framing as [`write_ipc`](Self::write_ipc)
    /// but includes **only** the named component plus a sentinel — no alive
    /// bitmap, no other components.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the component is not registered or if
    /// any Arrow IPC write fails.
    pub fn write_component_ipc<C: Component>(&self, writer: &mut Vec<u8>) -> Result<(), PcsError> {
        let name = C::name();
        if !self.schemas.contains(name) {
            return Err(PcsError::generic(format!(
                "Dataset::write_component_ipc: component '{name}' is not registered"
            )));
        }

        let batch = self.get_or_build_merged(name);
        let version = self.schemas.get_version(name).unwrap_or(1);
        let annotated = Self::annotate_batch(batch, name, version)?;
        let segment = batch_to_ipc_bytes(&annotated)?;
        let len = segment.len() as u32;
        writer.extend_from_slice(&len.to_le_bytes());
        writer.extend_from_slice(&segment);

        writer.extend_from_slice(&self.alive_ipc_bytes()?);
        writer.extend_from_slice(&0u32.to_le_bytes());

        Ok(())
    }

    /// Reconstruct a `Dataset` from an IPC stream produced by [`write_ipc`](Self::write_ipc).
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the stream is malformed, truncated, or
    /// contains batches without the `__pcs_component` metadata key.
    pub fn read_ipc<R: Read>(reader: &mut R) -> Result<Self, PcsError> {
        let mut dataset = Self::new();
        let mut alive_count: u32 = 0;

        loop {
            let mut len_buf = [0u8; 4];
            reader
                .read_exact(&mut len_buf)
                .map_err(|e| PcsError::generic(format!("IPC read len: {e}")))?;
            let len = u32::from_le_bytes(len_buf);

            if len == 0 {
                break;
            }

            let mut segment = vec![0u8; len as usize];
            reader
                .read_exact(&mut segment)
                .map_err(|e| PcsError::generic(format!("IPC read segment: {e}")))?;

            let batch = ipc_bytes_to_batch(&segment)?;
            let name_str = batch
                .schema()
                .metadata()
                .get(COMPONENT_NAME_KEY)
                .ok_or_else(|| {
                    PcsError::generic(format!(
                        "IPC batch missing '{}' metadata key",
                        COMPONENT_NAME_KEY
                    ))
                })?
                .clone();

            if name_str == ALIVE_BATCH_NAME {
                alive_count += 1;
                if alive_count > 1 {
                    return Err(PcsError::generic(
                        "IPC stream contains more than one '__alive' batch; stream is corrupted",
                    ));
                }

                let alive_col = batch.column(0);
                let bool_arr = alive_col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| PcsError::generic("IPC alive column is not BooleanArray"))?;
                let n = bool_arr.len();
                let mut builder = BooleanBufferBuilder::new(n);
                let mut dead = 0usize;
                for i in 0..n {
                    let v = bool_arr.value(i);
                    builder.append(v);
                    if !v {
                        dead += 1;
                    }
                }
                dataset.row_count = n;
                dataset.live_count = n - dead;
                dataset.alive = builder;
                dataset.dead_count = dead;
            } else {
                // Parse the on-disk schema version (defaults to 1 if absent).
                let on_disk_version: u32 = batch
                    .schema()
                    .metadata()
                    .get(SCHEMA_VERSION_KEY)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);

                let mut meta = batch.schema().metadata().clone();
                meta.remove(COMPONENT_NAME_KEY);
                meta.remove(SCHEMA_VERSION_KEY);
                let clean_schema = Arc::new(Schema::new_with_metadata(
                    batch.schema().fields().iter().cloned().collect::<Vec<_>>(),
                    meta,
                ));
                let clean_batch =
                    RecordBatch::try_new(clean_schema.clone(), batch.columns().to_vec())
                        .map_err(|e| PcsError::generic(format!("IPC clean batch error: {e}")))?;

                let static_name = intern_component_name(&name_str);

                // Register with on-disk version; migration is dispatched below
                // once the registry has a typed entry (if one exists).
                dataset
                    .schemas
                    .register_raw(static_name, clean_schema, on_disk_version);
                dataset.components.insert(static_name, vec![clean_batch]);
            }
        }

        if alive_count == 0 {
            return Err(PcsError::generic(
                "IPC stream contains no '__alive' batch; stream is corrupted or incomplete",
            ));
        }

        for (name, chunks) in &dataset.components {
            let component_rows: usize = chunks.iter().map(|b| b.num_rows()).sum();
            if component_rows != dataset.row_count {
                return Err(PcsError::generic(format!(
                    "IPC component '{name}' has {component_rows} rows but alive bitmap has {}; \
                     stream is corrupted",
                    dataset.row_count
                )));
            }
        }

        Ok(dataset)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::UInt64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::{ALIVE_BATCH_NAME, COMPONENT_NAME_KEY, Dataset, batch_to_ipc_bytes};
    use crate::row::Row;

    #[derive(Serialize, Deserialize)]
    struct IpcOrder {
        id: u64,
        total: f64,
    }

    impl Component for IpcOrder {
        fn name() -> &'static str {
            "IpcOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_ipc_round_trip() {
        let mut ds = Dataset::new();
        ds.register_component::<IpcOrder>().unwrap();
        for i in 0..500usize {
            ds.append::<IpcOrder>(&[IpcOrder {
                id: i as u64,
                total: i as f64,
            }])
            .unwrap();
        }
        ds.mark_dead(Row::new(1));
        ds.mark_dead(Row::new(3));

        let mut buf: Vec<u8> = Vec::new();
        ds.write_ipc(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(&buf);
        let restored = Dataset::read_ipc(&mut cursor).unwrap();

        assert_eq!(restored.rows(), 500);
        assert_eq!(restored.live_rows(), 498);
    }

    fn build_ipc_buffer(
        component_rows: usize,
        alive_rows: Option<usize>,
        duplicate_alive: bool,
    ) -> Vec<u8> {
        use arrow_array::{ArrayRef, BooleanArray, Float64Array, RecordBatch};
        use arrow_schema::Schema;

        let mut buf: Vec<u8> = Vec::new();
        let ids: Vec<u64> = (0..component_rows as u64).collect();
        let totals: Vec<f64> = (0..component_rows).map(|i| i as f64).collect();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ],
            [(COMPONENT_NAME_KEY.to_string(), "IpcOrder".to_string())]
                .into_iter()
                .collect(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(Float64Array::from(totals)) as ArrayRef,
            ],
        )
        .unwrap();
        let seg = batch_to_ipc_bytes(&batch).unwrap();
        buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        buf.extend_from_slice(&seg);

        let write_alive = |out: &mut Vec<u8>, n: usize| {
            let alive_array: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
            let alive_schema = Arc::new(Schema::new_with_metadata(
                vec![Field::new("alive", DataType::Boolean, false)],
                [(COMPONENT_NAME_KEY.to_string(), ALIVE_BATCH_NAME.to_string())]
                    .into_iter()
                    .collect(),
            ));
            let alive_batch = RecordBatch::try_new(alive_schema, vec![alive_array]).unwrap();
            let seg = batch_to_ipc_bytes(&alive_batch).unwrap();
            out.extend_from_slice(&(seg.len() as u32).to_le_bytes());
            out.extend_from_slice(&seg);
        };

        if let Some(n) = alive_rows {
            write_alive(&mut buf, n);
            if duplicate_alive {
                write_alive(&mut buf, n);
            }
        }

        buf.extend_from_slice(&0u32.to_le_bytes());
        buf
    }

    #[test]
    fn test_read_ipc_missing_alive_returns_error() {
        let buf = build_ipc_buffer(10, None, false);
        let mut cursor = std::io::Cursor::new(&buf);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("__alive"));
    }

    #[test]
    fn test_read_ipc_duplicate_alive_returns_error() {
        let buf = build_ipc_buffer(10, Some(10), true);
        let mut cursor = std::io::Cursor::new(&buf);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("__alive"));
    }

    #[test]
    fn test_read_ipc_row_count_mismatch_returns_error() {
        let buf = build_ipc_buffer(10, Some(5), false);
        let mut cursor = std::io::Cursor::new(&buf);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("IpcOrder"));
    }

    #[test]
    fn test_ipc_intern_on_repeated_reload() {
        let mut ds = Dataset::new();
        ds.register_component::<IpcOrder>().unwrap();
        ds.append::<IpcOrder>(&[IpcOrder { id: 0, total: 0.0 }])
            .unwrap();

        let mut buf: Vec<u8> = Vec::new();
        ds.write_ipc(&mut buf).unwrap();

        let mut cursor1 = std::io::Cursor::new(&buf);
        let restored1 = Dataset::read_ipc(&mut cursor1).unwrap();
        let name1: &'static str = restored1.components.keys().next().unwrap();

        let mut cursor2 = std::io::Cursor::new(&buf);
        let restored2 = Dataset::read_ipc(&mut cursor2).unwrap();
        let name2: &'static str = restored2.components.keys().next().unwrap();

        assert!(std::ptr::eq(name1, name2));
    }
}
