---
name: Windows FSM Design (Apr 2026)
description: Design for Apache Flink-style windows in Canudo — FSM, triggers, pipeline integration, watermarks, distributed concerns
type: project
---

Design proposed 2026-04-12 for adding Flink-style windows to Canudo.

**Why:** Team lead asked for FSM + pipeline integration design. No impl yet. Distributed durability delegated to rust-ds-db-expert; API surface delegated to rust-dev.

**How to apply:** When users ask about windows, watermarks, late data, triggers, or stateful streaming in Canudo, this is the reference design. Update if any decision changes.

## Phase 1 batch-first simplification (2026-04-12 update)

Team lead confirmed plan is batch-first: every row present before emit, no streaming triggers/timers in Phase 1.

- **FSM collapses to a 2-state flag** on each WindowTable entry: `Open | Emitted`. No Trigger trait, no timers in Phase 1. Document constraint: one window owned by exactly one batch run on exactly one instance.
- **Phase 2 session windows**: only 3 materialized states (`Sealed | Emitted | Discarded`). Replace merge FSM with a sort-and-walk system (`A_seal_sessions`) injected between `A_assign` and `A_emit`. Sessions must reach `Sealed` before `Emitted` to avoid double-count from late merges.
- **Phase 3 future**: when streaming or iterative batches arrive, the flag widens into the full 7-state FSM without breaking the user API. Original FSM design below kept as forward reference.
- **Critical workflow concern with monolithic `WindowedSystem::run`**: collapses stage parallelism (one DAG node for all phases), denies user insertion between assign/emit, makes retry granularity too coarse, and forces SystemMeta to declare a superset of all phases (scheduler poison pill — silently serializes every other system touching those fields). Even in batch-only Phase 1, internally split into per-phase injected systems with precise SystemMeta. User API stays single-object (`pipeline.add_window(assigner)`).

## Core decisions (full streaming design — Phase 3 reference)

- **Inject systems, do not wrap**: per-assigner the pipeline gets 4 injected systems (`assign`, `advance_watermark`, `trigger_eval`, `emit`, `cleanup`) plus resources (`WindowTable<A>`, `Watermark<A>`, `PendingPanes<A>`). Stage ordering falls out of existing field/resource SystemMeta DAG.
- **No `WindowedSystem` trait**: introduce a `WindowAssigner` trait (with associated `Source`/`Result`/`AggState`) and a `Trigger` trait. The `System` trait stays stateless; per-window state lives in `WindowTable<A>` resource. Aggregation functions are data, not systems.
- **`A_emit` is a `ParallelSystem`**: per-window aggregation fans out across windows.
- **Window membership is a component**: `WindowAssignment_A { window_id }` (or `ListArray<u64>` for sliding) so it participates in the DAG. Sliding windows return multiple WindowKeys from `assign()`.

## FSM (7 states, 1 terminal)

`Pending → Accumulating → (Triggering ↔ Accumulating | Purged) → Closing → Closed → Discarded`

- `Pending` exists so empty windows can be pre-created on watermark advance.
- `Triggering`/`Closing` are transient.
- `Closing` is single-shot: once entered, late data goes to side-output, never re-enters window.
- Window state must key on stable `window_id` (assigner-generated), NOT `Row` index (compact invalidates Row).

## Trigger model

`TriggerVerdict ::= Continue | Fire | FireAndPurge | Purge | FireAndClose`

Three event sources, all pipeline-driven (no internal threads): `OnElement`, `OnProcessingTime`, `OnEventTime`. Composition via `AfterFirst`/`AfterAll`/`Repeatedly`/`Until`. Trigger state lives inside `WindowTable<A>` entries (so checkpointing covers it).

## Watermarks

**Per-assigner** watermark resource (not global). Strategies: `MaxObserved`, `Punctuated`, `BoundedOutOfOrderness(d)`. Monotonic — lowering attempts dropped + metric.

**Why per-assigner:** Canudo has no source-operator concept; per-assigner avoids two assigners with different event-time fields stalling each other.

## Distributed gaps for rust-ds-db-expert

1. **Resource checkpointing gap**: existing `CheckpointStore` snapshots `World` via Arrow IPC, but resources are NOT in IPC today. Recommend: require windowing resources to expose Arrow encoding so they snapshot as synthetic components (keeps replay through `read_ipc` clean).
2. **Watermark merging**: global watermark = `min(local_watermarks)` propagated via `ConsensusCommand::AdvanceWatermark { assigner, value }`. No instance may transition window into `Closing` until global watermark advances.
3. **Window ownership**: partition `(assigner_id, window_id)` across instances via `PartitionSource` lease semantics — window itself is a partitioned entity.

## Cleanup

Three mechanisms in `A_cleanup`:
1. Time retention (default `allowed_lateness + 5min`)
2. Cardinality cap (evict oldest `Closed` → `Closing` → refuse new `Pending`) — safety net for misconfigured session windows
3. Manual `assigner.purge(window_id)`

Row cleanup uses `World::mark_dead`; does NOT force `World::compact`.

## Open questions still to resolve

- Late data side-output: configurable, default = metric only
- Empty window emission: `emit_empty: bool` flag, default off for tumbling
- Multi-assignment for sliding: yes, via `ListArray<u64>` in `WindowAssignment_A`
