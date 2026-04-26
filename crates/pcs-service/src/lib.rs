pub use pcs_core::column;
pub use pcs_core::component;
pub use pcs_core::dataset;
pub use pcs_core::error;
pub use pcs_core::partition;
pub use pcs_core::pipeline;
pub use pcs_core::resource;
pub use pcs_core::retry;
pub use pcs_core::row;
pub use pcs_core::scheduler;
pub use pcs_core::schema;
pub use pcs_core::system;

#[cfg(feature = "windows")]
pub use pcs_core::windows;

pub use pcs_core::Component;
pub use pcs_core::Row;
pub use pcs_core::SchemaRegistry;
pub use pcs_core::{BackpressureSpec, DependencyKind, PipelineConfig, Scheduler};
pub use pcs_core::{Dataset, Pipeline, PipelineBuilder, RunStats};
pub use pcs_core::{
    FieldAccess, FieldRef, ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta,
    WriteSet, system_fn,
};
pub use pcs_core::{PcsError, PcsResult};
pub use pcs_core::{RetryMode, SystemConfig};

#[cfg(feature = "io")]
pub mod io;

#[cfg(feature = "distributed")]
pub mod distributed;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "service")]
pub mod service;

/// Convenience re-exports for common usage patterns.
///
/// Use `use pcs_service::prelude::*;` to import the most commonly used types and traits.
pub mod prelude {
    pub use crate::{
        BackpressureSpec, Component, Dataset, DependencyKind, FieldAccess, ParallelSystem,
        PcsError, PcsResult, Pipeline, PipelineBuilder, PipelineConfig, ResourceUpdate, RetryMode,
        Row, RunStats, Scheduler, SchemaRegistry, SliceWriteSet, System, SystemConfig, SystemMeta,
        WriteSet, system_fn,
    };

    pub use crate::column::ComponentView;
    pub use crate::pipeline::DatasetBuilder;
    pub use crate::system::FieldRef;

    pub use async_trait::async_trait;

    #[cfg(feature = "windows")]
    pub use crate::windows::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator};

    pub use crate::partition::KeyPartition;
}
