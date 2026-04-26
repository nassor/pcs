use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::component::Component;
use crate::error::PcsError;

use super::Dataset;

impl Dataset {
    /// Register a component type, creating an empty chunk list for it.
    ///
    /// Safe to call multiple times for the same `C`; subsequent calls are
    /// no-ops and return `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] if `row_count > 0` when
    /// registering a new component, because that would create a mismatched
    /// column count. Register all components before appending data, or ensure
    /// the dataset is empty (call [`clear`](Dataset::clear) first).
    ///
    /// # Example
    ///
    /// ```rust
    /// #
    /// # {
    /// use std::sync::Arc;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use pcs_core::pipeline::Dataset;
    /// use pcs_core::component::Component;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct Price { value: f64 }
    /// impl Component for Price {
    ///     fn name() -> &'static str { "Price" }
    ///     fn schema() -> Arc<Schema> {
    ///         Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]))
    ///     }
    /// }
    ///
    /// let mut dataset = Dataset::new();
    /// dataset.register_component::<Price>().unwrap();
    /// # }
    /// ```
    pub fn register_component<C: Component>(&mut self) -> Result<(), PcsError> {
        if self.schemas.contains(C::name()) {
            return Ok(());
        }
        if self.row_count != 0 {
            return Err(PcsError::configuration(format!(
                "Dataset: cannot register new component '{}' after rows have been appended \
                 (row_count={}). Register all components before appending data, or call \
                 clear() first.",
                C::name(),
                self.row_count
            )));
        }
        self.schemas.register::<C>();
        let schema = C::schema();
        let empty = RecordBatch::new_empty(schema);
        self.components.insert(C::name(), vec![empty]);
        Ok(())
    }

    /// Register a component using an explicit Arrow [`Schema`], without a typed
    /// [`Component`] implementation.
    ///
    /// Useful for Source implementations where the schema is determined at
    /// runtime (e.g. from Parquet file metadata). `version` defaults to `1`
    /// when not known; pass the actual schema version for migration support.
    ///
    /// # Panics
    ///
    /// Panics if `row_count > 0` when registering a new component.
    pub fn register_raw_component(&mut self, name: &'static str, schema: Arc<Schema>) {
        self.register_raw_component_versioned(name, schema, 1);
    }

    /// Like [`register_raw_component`](Self::register_raw_component) but with
    /// an explicit schema `version`.
    ///
    /// # Panics
    ///
    /// Panics if `row_count > 0` when registering a new component.
    pub fn register_raw_component_versioned(
        &mut self,
        name: &'static str,
        schema: Arc<Schema>,
        version: u32,
    ) {
        if self.schemas.contains(name) {
            return;
        }
        assert!(
            self.row_count == 0,
            "Dataset: cannot register raw component '{name}' after rows have been appended \
             (row_count={}). Register all components before appending data.",
            self.row_count
        );
        self.schemas.register_raw(name, schema.clone(), version);
        let empty = RecordBatch::new_empty(schema);
        self.components.insert(name, vec![empty]);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;

    #[derive(Serialize, Deserialize)]
    struct Reg {
        value: f64,
    }

    impl Component for Reg {
        fn name() -> &'static str {
            "Reg"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                false,
            )]))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct RegB {
        val: u64,
    }

    impl Component for RegB {
        fn name() -> &'static str {
            "RegB"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "val",
                DataType::UInt64,
                false,
            )]))
        }
    }

    #[test]
    fn test_register_ok() {
        let mut ds = Dataset::new();
        assert!(ds.register_component::<Reg>().is_ok());
    }

    #[test]
    fn test_register_idempotent() {
        let mut ds = Dataset::new();
        ds.register_component::<Reg>().unwrap();
        assert!(ds.register_component::<Reg>().is_ok());
    }

    #[test]
    fn test_register_after_rows_different_type_errors() {
        let mut ds = Dataset::new();
        ds.register_component::<Reg>().unwrap();
        ds.append::<Reg>(&[Reg { value: 1.0 }]).unwrap();
        let result = ds.register_component::<RegB>();
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("RegB"));
    }

    #[test]
    fn test_register_raw_component() {
        let mut ds = Dataset::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        ds.register_raw_component("raw_test", schema);
        assert!(ds.schemas().contains("raw_test"));
    }
}
