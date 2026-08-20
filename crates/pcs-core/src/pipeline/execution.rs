use std::time::Instant;

use crate::error::{PcsError, PcsResult};
use crate::pipeline::RunStats;
use crate::retry::SystemConfig;

#[cfg(feature = "runtime")]
use std::sync::Arc;

#[cfg(feature = "runtime")]
use crate::system::{ParallelSystem, SLICE_PARALLEL_THRESHOLD, SliceWriteSet, WriteSet};

#[cfg(feature = "io")]
use crate::io::{drain_dataset, drain_into_dataset};

use super::Pipeline;
use super::dag::SystemEntry;
use crate::dataset::Dataset;

/// Returns number of retries before success (0 = first attempt succeeded).
async fn run_arrow_system_with_retries(
    sys: &dyn crate::system::System,
    config: &SystemConfig,
    data: &mut Dataset,
) -> PcsResult<u32> {
    let (_, retries) = crate::retry::run_with_retries(config, async || match sys.run_sync(data) {
        Some(sync_result) => sync_result,
        None => sys.run(data).await,
    })
    .await?;
    Ok(retries)
}

/// Returns `(write_set, retries)` where retries = 0 means first attempt succeeded.
#[cfg(feature = "runtime")]
async fn run_parallel_system_with_retries(
    sys: Arc<dyn ParallelSystem>,
    config: &SystemConfig,
    data: &Dataset,
) -> PcsResult<(WriteSet, u32)> {
    let use_slices =
        data.rows() as u32 >= SLICE_PARALLEL_THRESHOLD && sys.run_slice(data, 0..0).is_some();
    crate::retry::run_with_retries(config, async || {
        if use_slices {
            run_parallel_system_sliced(Arc::clone(&sys), data).await
        } else {
            sys.run(data).await
        }
    })
    .await
}

/// Split `total_rows` into CPU-count equal-ish ranges for parallel dispatch.
#[cfg(feature = "runtime")]
fn compute_row_ranges(total_rows: u32) -> Vec<std::ops::Range<u32>> {
    let num_chunks = num_cpus::get().max(1) as u32;
    let chunk_size = total_rows.div_ceil(num_chunks);
    (0..num_chunks)
        .map(|i| {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(total_rows);
            start..end
        })
        .filter(|r| !r.is_empty())
        .collect()
}

#[cfg(feature = "runtime")]
async fn run_parallel_system_sliced(
    sys: Arc<dyn ParallelSystem>,
    data: &Dataset,
) -> Result<WriteSet, PcsError> {
    use rayon::prelude::*;

    let ranges = compute_row_ranges(data.rows() as u32);

    // SAFETY: spawn_blocking is awaited before return; data outlives the task.
    let world_ptr = data as *const Dataset as usize;

    tokio::task::spawn_blocking(move || {
        let data = unsafe { &*(world_ptr as *const Dataset) };

        let slice_results: Vec<Result<SliceWriteSet, PcsError>> = ranges
            .into_par_iter()
            .map(|range| {
                sys.run_slice(data, range.clone()).unwrap_or_else(|| {
                    Err(PcsError::generic(
                        "ParallelSystem::run_slice returned None during slice execution",
                    ))
                })
            })
            .collect();

        let mut slices = Vec::with_capacity(slice_results.len());
        for r in slice_results {
            slices.push(r?);
        }

        sys.merge_slices(slices)
    })
    .await
    .map_err(|e| PcsError::generic(format!("spawn_blocking join error: {e}")))
    .and_then(|r| r)
}

/// Returns total retry count across all systems in the stage.
#[cfg(feature = "runtime")]
async fn run_parallel_stage(
    systems: &[SystemEntry],
    configs: &[SystemConfig],
    stage: &[usize],
    data: &mut Dataset,
) -> PcsResult<u32> {
    struct TaskDesc {
        sys: Arc<dyn ParallelSystem>,
        config: SystemConfig,
        use_slices: bool,
    }

    let world_rows = data.rows() as u32;

    let mut task_descs: Vec<TaskDesc> = Vec::with_capacity(stage.len());
    for &sys_idx in stage {
        if let SystemEntry::Parallel(ref sys) = systems[sys_idx] {
            let config = &configs[sys_idx];
            let use_slices =
                world_rows >= SLICE_PARALLEL_THRESHOLD && sys.run_slice(data, 0..0).is_some();
            task_descs.push(TaskDesc {
                sys: Arc::clone(sys),
                config: *config,
                use_slices,
            });
        }
    }

    // SAFETY: all JoinHandles are awaited before `data` is next accessed.
    let world_ptr = data as *const Dataset as usize;

    let handle = tokio::runtime::Handle::current();
    let num_tasks = task_descs.len();

    let handles: Vec<tokio::task::JoinHandle<Result<(WriteSet, u32), PcsError>>> = task_descs
        .into_iter()
        .map(|desc| {
            let handle = handle.clone();
            tokio::task::spawn_blocking(move || {
                // SAFETY: world_ptr is valid; all handles are awaited before return.
                let data = unsafe { &*(world_ptr as *const Dataset) };

                crate::retry::run_with_retries_blocking(&desc.config, || {
                    if desc.use_slices {
                        use rayon::prelude::*;
                        let ranges = compute_row_ranges(world_rows);

                        let slice_results: Vec<Result<SliceWriteSet, PcsError>> = ranges
                            .into_par_iter()
                            .map(|range| {
                                desc.sys.run_slice(data, range).unwrap_or_else(|| {
                                    Err(PcsError::generic(
                                        "run_slice returned None during parallel stage slice",
                                    ))
                                })
                            })
                            .collect();

                        let mut slices = Vec::with_capacity(slice_results.len());
                        for r in slice_results {
                            slices.push(r?);
                        }
                        desc.sys.merge_slices(slices)
                    } else {
                        handle.block_on(desc.sys.run(data))
                    }
                })
            })
        })
        .collect();

    type StageTaskResult = Result<Result<(WriteSet, u32), PcsError>, tokio::task::JoinError>;

    // Await ALL handles before inspecting results (SAFETY: raw ptr must not alias).
    let mut join_results: Vec<StageTaskResult> = Vec::with_capacity(num_tasks);
    for handle in handles {
        join_results.push(handle.await);
    }

    let mut write_sets: Vec<WriteSet> = Vec::with_capacity(num_tasks);
    let mut total_retries: u32 = 0;
    for result in join_results {
        let (ws, retries) =
            result.map_err(|e| PcsError::generic(format!("parallel stage task join: {e}")))??;
        write_sets.push(ws);
        total_retries += retries;
    }

    #[cfg(debug_assertions)]
    {
        let mut seen_keys = std::collections::HashSet::new();
        for ws in &write_sets {
            for key in ws.fields.keys() {
                debug_assert!(
                    seen_keys.insert(*key),
                    "Two ParallelSystems in the same stage both write field '{}.{}'.",
                    key.0,
                    key.1
                );
            }
        }
    }

    let mut merged = WriteSet::new();
    for ws in write_sets {
        for (key, array) in ws.fields {
            merged.fields.insert(key, array);
        }
        merged.resource_updates.extend(ws.resource_updates);
    }

    data.apply_write_set(merged)?;
    Ok(total_retries)
}

/// Execute all stages against `data`, returning `(systems_run, retries_this_batch)`.
#[cfg(feature = "runtime")]
async fn run_stages(
    systems: &[SystemEntry],
    configs: &[SystemConfig],
    stages: &[Vec<usize>],
    data: &mut Dataset,
) -> PcsResult<(usize, u32)> {
    let mut systems_run = 0usize;
    let mut retries_this_batch: u32 = 0;

    for stage in stages.iter() {
        let all_parallel = stage.iter().all(|&idx| systems[idx].is_parallel());

        if all_parallel && stage.len() > 1 {
            systems_run += stage.len();
            retries_this_batch += run_parallel_stage(systems, configs, stage, data).await?;
        } else if all_parallel && stage.len() == 1 {
            let sys_idx = stage[0];
            let config = &configs[sys_idx];
            if let SystemEntry::Parallel(sys) = &systems[sys_idx] {
                let (write_set, retries) =
                    run_parallel_system_with_retries(Arc::clone(sys), config, data).await?;
                data.apply_write_set(write_set)?;
                systems_run += 1;
                retries_this_batch += retries;
            }
        } else {
            for &sys_idx in stage {
                let config = &configs[sys_idx];

                match &systems[sys_idx] {
                    SystemEntry::Sequential(sys) => {
                        // `max_attempts == 1` needs no special case: the retry
                        // driver wraps the first failure in `RetryExhausted`
                        // with attempt 1 and never sleeps.
                        retries_this_batch +=
                            run_arrow_system_with_retries(sys.as_ref(), config, data).await?;
                        systems_run += 1;
                    }
                    SystemEntry::Parallel(sys) => {
                        let (write_set, retries) =
                            run_parallel_system_with_retries(Arc::clone(sys), config, data).await?;
                        data.apply_write_set(write_set)?;
                        systems_run += 1;
                        retries_this_batch += retries;
                    }
                }
            }
        }
    }

    Ok((systems_run, retries_this_batch))
}

/// Execute all stages sequentially (no-runtime/guest path).
/// `ParallelSystem` entries are rejected — register only `System` impls.
#[cfg(not(feature = "runtime"))]
async fn run_stages_sequential(
    systems: &[SystemEntry],
    configs: &[SystemConfig],
    stages: &[Vec<usize>],
    data: &mut Dataset,
) -> PcsResult<(usize, u32)> {
    let mut systems_run = 0usize;
    let mut retries_this_batch: u32 = 0;

    for stage in stages {
        for &sys_idx in stage {
            let config = &configs[sys_idx];

            match &systems[sys_idx] {
                SystemEntry::Sequential(sys) => {
                    retries_this_batch +=
                        run_arrow_system_with_retries(sys.as_ref(), config, data).await?;
                    systems_run += 1;
                }
                SystemEntry::Parallel(_) => {
                    return Err(PcsError::generic(
                        "ParallelSystem is not supported on the guest (no-runtime) path",
                    ));
                }
            }
        }
    }

    Ok((systems_run, retries_this_batch))
}

impl Pipeline {
    /// Run all systems against `self.data`.
    #[cfg(feature = "runtime")]
    pub async fn run(&mut self) -> PcsResult<()> {
        self.ensure_plan(self.data.schemas())?;

        let start = Instant::now();
        let rows_before = self.data.live_rows() as isize;

        let Self {
            data,
            systems,
            stages,
            configs,
            ..
        } = self;

        let stages_val = stages.get().unwrap().as_ref().unwrap();
        let configs_val = configs.get().unwrap();

        if stages_val.is_empty() {
            self.last_stats.set(RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            });
            return Ok(());
        }

        let (systems_run, retries_this_batch) =
            run_stages(systems, configs_val, stages_val, data).await?;

        self.last_stats.set(RunStats {
            rows_produced: data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        });
        Ok(())
    }

    /// Run all systems against `self.data` (guest/sequential path).
    ///
    /// `ParallelSystem` entries are not supported on this path — register only
    /// `System` impls when targeting wasm32-wasip2.
    #[cfg(not(feature = "runtime"))]
    pub async fn run(&mut self) -> PcsResult<()> {
        self.ensure_plan(self.data.schemas())?;

        let start = Instant::now();
        let rows_before = self.data.live_rows() as isize;

        // Clone stages/configs to break the borrow conflict with &mut self.data.
        let stages = self.stages.get().unwrap().as_ref().unwrap().clone();
        let configs: Vec<SystemConfig> = self.configs.get().unwrap().to_vec();

        if stages.is_empty() {
            self.last_stats.set(RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            });
            return Ok(());
        }

        let (systems_run, retries_this_batch) =
            run_stages_sequential(&self.systems, &configs, &stages, &mut self.data).await?;

        self.last_stats.set(RunStats {
            rows_produced: self.data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        });
        Ok(())
    }

    /// Run all systems against a separately-provided `Dataset`, returning the
    /// stats for **this call**.
    ///
    /// This is an escape hatch for distributed runners and WASM guests that
    /// manage their own per-partition datasets. Sources and sinks on `self` are
    /// ignored. The returned [`RunStats`] is also recorded in
    /// [`last_stats`](Pipeline::last_stats); callers that need a per-call value
    /// (e.g. the guest SDK filling WIT `run-metrics`) should use the return
    /// value rather than reading the shared cell back.
    #[cfg(feature = "runtime")]
    pub async fn run_on_with_stats(&self, data: &mut Dataset) -> PcsResult<RunStats> {
        self.ensure_plan(data.schemas())?;

        let start = Instant::now();
        let rows_before = data.live_rows() as isize;

        let stages_val = self.stages.get().unwrap().as_ref().unwrap();
        let configs_val = self.configs.get().unwrap();

        if stages_val.is_empty() {
            let stats = RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            };
            self.last_stats.set(stats);
            return Ok(stats);
        }

        let (systems_run, retries_this_batch) =
            run_stages(&self.systems, configs_val, stages_val, data).await?;

        let stats = RunStats {
            rows_produced: data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        };
        self.last_stats.set(stats);
        Ok(stats)
    }

    /// Run all systems against a separately-provided `Dataset` (guest/sequential
    /// path), returning the stats for **this call**.
    #[cfg(not(feature = "runtime"))]
    pub async fn run_on_with_stats(&self, data: &mut Dataset) -> PcsResult<RunStats> {
        self.ensure_plan(data.schemas())?;

        let start = Instant::now();
        let rows_before = data.live_rows() as isize;

        let stages_val = self.stages.get().unwrap().as_ref().unwrap();
        let configs_val = self.configs.get().unwrap();

        if stages_val.is_empty() {
            let stats = RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            };
            self.last_stats.set(stats);
            return Ok(stats);
        }

        let (systems_run, retries_this_batch) =
            run_stages_sequential(&self.systems, configs_val, stages_val, data).await?;

        let stats = RunStats {
            rows_produced: data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        };
        self.last_stats.set(stats);
        Ok(stats)
    }

    /// Run all systems against a separately-provided `Dataset`.
    ///
    /// Thin wrapper over [`run_on_with_stats`](Self::run_on_with_stats) that
    /// discards the per-call stats; they remain readable via
    /// [`last_stats`](Pipeline::last_stats).
    pub async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
        self.run_on_with_stats(data).await.map(|_| ())
    }

    /// Drain sources → run → drain sinks.
    #[cfg(feature = "io")]
    pub async fn run_with_io(&mut self) -> PcsResult<()> {
        let start = Instant::now();
        let rows_before = self.data.live_rows() as isize;

        {
            let Self { sources, data, .. } = &mut *self;
            for (comp, src) in sources.iter_mut() {
                drain_into_dataset(src.as_mut(), data, comp).await?;
            }
        }
        self.run().await?;
        let systems_run = self.last_stats.get().systems_run;
        let retries_this_batch = self.last_stats.get().retries_this_batch;
        {
            let Self { sinks, data, .. } = &mut *self;
            for (comp, sink) in sinks.iter_mut() {
                drain_dataset(data, comp, sink.as_mut()).await?;
                sink.finish().await?;
            }
        }

        self.last_stats.set(RunStats {
            rows_produced: self.data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        });
        Ok(())
    }
}

// Every test here drives `Pipeline::run` / `run_on` through the tokio+rayon
// stage executor, so the module is gated on `runtime`. The guest path is
// covered end-to-end by `pcs-guest-smoketest` against a real component.
#[cfg(all(test, feature = "runtime"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::pipeline::Pipeline;
    use crate::row::Row;
    use crate::system::{System, SystemMeta, WriteSet};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct ExecOrder {
        id: u64,
        total: f64,
    }

    impl Component for ExecOrder {
        fn name() -> &'static str {
            "ExecOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    fn make_orders(n: usize) -> Vec<ExecOrder> {
        (0..n)
            .map(|i| ExecOrder {
                id: i as u64,
                total: i as f64 * 10.0,
            })
            .collect()
    }

    struct BumpSystem {
        field: &'static str,
    }

    #[async_trait::async_trait]
    impl System for BumpSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("bump")
                .read("ExecOrder", "id")
                .write("ExecOrder", self.field)
        }

        async fn run(&self, data: &mut Dataset) -> crate::PcsResult<()> {
            let batch = data.columns::<ExecOrder>().unwrap().clone();
            let total_col = batch
                .column(batch.schema().index_of(self.field).unwrap())
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let bumped: Float64Array = total_col.iter().map(|v| v.map(|x| x + 1.0)).collect();
            let ws = WriteSet::new().put("ExecOrder", self.field, Arc::new(bumped));
            data.apply_write_set(ws)
        }
    }

    #[tokio::test]
    async fn test_pipeline_run_single_system() {
        let mut p = Pipeline::new("test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(5)).unwrap();
        p.add_system(BumpSystem { field: "total" });

        p.run().await.unwrap();

        let col = p.data.column::<ExecOrder>("total").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 1.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_run_on_uses_external_dataset() {
        let p = Pipeline::new("template");

        let mut ext = Dataset::new();
        ext.register_component::<ExecOrder>().unwrap();
        ext.append::<ExecOrder>(&make_orders(3)).unwrap();

        p.run_on(&mut ext).await.unwrap();
        assert_eq!(ext.rows(), 3);
    }

    #[tokio::test]
    async fn test_retries_this_batch_counts_failures_before_success() {
        use crate::error::PcsError;
        use crate::retry::{RetryMode, SystemConfig};
        use crate::system::SystemMeta;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct FlakySystem {
            fail_times: usize,
        }

        #[async_trait::async_trait]
        impl System for FlakySystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("flaky").write("ExecOrder", "total")
            }

            fn config(&self) -> SystemConfig {
                SystemConfig {
                    retry_mode: RetryMode::Fixed {
                        retries: 5,
                        delay: std::time::Duration::ZERO,
                    },
                }
            }

            async fn run(&self, _data: &mut Dataset) -> crate::PcsResult<()> {
                let n = CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                if n < self.fail_times {
                    Err(PcsError::generic("transient"))
                } else {
                    Ok(())
                }
            }
        }

        CALL_COUNT.store(0, Ordering::SeqCst);
        let mut p = Pipeline::new("retry-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(2)).unwrap();
        p.add_system(FlakySystem { fail_times: 3 });

        p.run().await.unwrap();
        assert_eq!(p.last_stats().retries_this_batch, 3);
    }

    #[tokio::test]
    async fn test_retries_this_batch_zero_on_clean_run() {
        let mut p = Pipeline::new("no-retry-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(2)).unwrap();
        p.add_system(BumpSystem { field: "total" });

        p.run().await.unwrap();
        assert_eq!(p.last_stats().retries_this_batch, 0);
    }

    #[tokio::test]
    async fn test_pipeline_marks_rows_dead_via_system() {
        struct KillFirstSystem;
        #[async_trait::async_trait]
        impl System for KillFirstSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("kill").write("ExecOrder", "id")
            }
            async fn run(&self, data: &mut Dataset) -> crate::PcsResult<()> {
                data.mark_dead(Row::new(0));
                Ok(())
            }
        }

        let mut p = Pipeline::new("kill-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(5)).unwrap();
        p.add_system(KillFirstSystem);
        p.run().await.unwrap();
        assert_eq!(p.data.live_rows(), 4);
    }

    /// `run_on` populates `last_stats` with systems_run and retries_this_batch.
    #[tokio::test]
    async fn test_run_on_populates_last_stats() {
        use crate::error::PcsError;
        use crate::retry::{RetryMode, SystemConfig};
        use crate::system::SystemMeta;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct FlakySystem2 {
            fail_times: usize,
        }

        #[async_trait::async_trait]
        impl System for FlakySystem2 {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("flaky2").write("ExecOrder", "total")
            }

            fn config(&self) -> SystemConfig {
                SystemConfig {
                    retry_mode: RetryMode::Fixed {
                        retries: 5,
                        delay: std::time::Duration::ZERO,
                    },
                }
            }

            async fn run(&self, _data: &mut Dataset) -> crate::PcsResult<()> {
                let n = CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                if n < self.fail_times {
                    Err(PcsError::generic("transient"))
                } else {
                    Ok(())
                }
            }
        }

        CALL_COUNT.store(0, Ordering::SeqCst);
        let mut p = Pipeline::new("run-on-stats-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.add_system(BumpSystem { field: "total" });
        p.add_system(FlakySystem2 { fail_times: 2 });

        let mut ext = Dataset::new();
        ext.register_component::<ExecOrder>().unwrap();
        ext.append::<ExecOrder>(&make_orders(3)).unwrap();

        p.run_on(&mut ext).await.unwrap();

        let stats = p.last_stats();
        assert_eq!(stats.systems_run, 2, "two systems should have run");
        assert_eq!(
            stats.retries_this_batch, 2,
            "FlakySystem2 failed twice before succeeding"
        );
        assert!(
            stats.duration_millis < 5_000,
            "duration should be populated"
        );
    }

    /// `run_on` with no systems sets last_stats to zero counts (not stale from prior run).
    #[tokio::test]
    async fn test_run_on_empty_pipeline_stats_are_zero() {
        let p = Pipeline::new("empty-run-on");
        let mut ext = Dataset::new();

        // Use a separately-registered component so ensure_plan sees a schema.
        struct Dummy;
        impl Component for Dummy {
            fn name() -> &'static str {
                "Dummy"
            }
            fn schema() -> std::sync::Arc<arrow_schema::Schema> {
                std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "x",
                    arrow_schema::DataType::UInt8,
                    false,
                )]))
            }
        }
        ext.register_component::<Dummy>().unwrap();

        p.run_on(&mut ext).await.unwrap();

        let stats = p.last_stats();
        assert_eq!(stats.systems_run, 0);
        assert_eq!(stats.retries_this_batch, 0);
    }
}
