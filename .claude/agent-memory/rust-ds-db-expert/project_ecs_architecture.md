---
name: Canudo ECS Architecture (ecs-rewrite branch)
description: Core ECS architecture of Canudo on ecs-rewrite branch — World, System, Pipeline, Scheduler patterns and constraints relevant to distributed design
type: project
---

Canudo on ecs-rewrite branch has replaced the FSM workflow model with an ECS processing model.

**Key architectural facts for distributed systems design:**

- World is NOT Send or Sync — it uses raw pointers for performance (unsafe downcasts in TypedColumn). Sharing requires Arc<Mutex<World>> or Arc<RwLock<World>>.
- Entity is Copy: { id: u32, generation: u32 }. The generation guards against use-after-despawn. Entity.id is the dense index into Vec<Option<T>> columns.
- Components are stored as TypedColumn<T> (Vec<Option<T>>) indexed by entity.id. No serialization support (no serde derives). Any Send+Sync+'static type is a component — no registration needed.
- Resources are global singletons per TypeId, stored as Box<dyn Any + Send + Sync>.
- Pipeline takes &mut World — single owner, sequential stages. Systems within a stage run sequentially today (future parallel planned). The execution plan is cached in OnceLock.
- Scheduler uses WorldFactory pattern: each pipeline run gets a fresh World from a closure. The existing Scheduler does NOT carry state between runs.
- System::run takes &mut World — exclusive access to the entire world per system call. No component-level locking.
- RetryMode exists per-system (exponential backoff, fixed, none). Default: 3 retries, 100ms base, 2x, 30s cap.

**Why:** These constraints mean distributed work distribution cannot split a World across instances (no component-level serialization, &mut World required). Distribution must happen at the entity-batch or pipeline-run level.

**How to apply:** Any distributed design must partition WORK (entity batches or pipeline runs), not the World itself. Serialization of entity data requires a user-provided serde layer or a new World snapshot mechanism.
