# Scheduler

The cross-pipeline features this file used to ask for have shipped. `Scheduler`
(`crates/pcs-core/src/scheduler.rs`, exported from `pcs_core` and its prelude)
holds many `Pipeline`s registered via `add_pipeline` / `add_pipeline_with_config`
and ticks them in dependency-stage order: `PipelineConfig` carries
`dependencies: Vec<(String, DependencyKind)>` — `DependencyKind::Order` for pure
ordering, `DependencyKind::Data` to also skip a dependent whose predecessor
produced zero rows — plus an `i32` `priority` (lower runs first, stable within a
stage) and an optional `BackpressureSpec` that skips a pipeline for the current
tick. Stages are computed by Kahn wave-peeling over the dependency names.

Not wired up: `pcs-service` cannot drive a `Scheduler`. `ServiceConfig.pipeline`
is a single `PipelineSpec`, so the service hosts exactly one pipeline and the
orchestrator is reachable only from library code.
