---
name: Pipeline Soundness Fixes
description: Four pipeline correctness fixes: fat-pointer transmute removal, backoff sleep in blocking pool, O(n·k) indexed DAG, structured RetryExhausted
type: project
---

Four changes applied coherently to src/pipeline.rs, src/retry.rs, src/error.rs.

**Task #11 — Arc<dyn ParallelSystem> replaces fat-pointer transmutes**

`SystemEntry::Parallel` now stores `Arc<dyn ParallelSystem>` instead of `Box<dyn ParallelSystem>`. `add_parallel_system_boxed` converts via `Arc::from(box)`. In `run_parallel_stage` and `run_parallel_system_sliced`, we clone the Arc into `spawn_blocking` closures — no fat-pointer transmutes needed. World is still passed as `*const World as usize` (thin pointer, single-word, well-defined). The SAFETY comment explains: `spawn_blocking` tasks are joined before the function returns.

**Task #12 — std::thread::sleep for retry backoff in blocking pool**

The `else` branch in `run_parallel_stage`'s spawn_blocking closure now calls `std::thread::sleep(delay)` between retry attempts using `desc.retry_mode.delay_for_attempt(attempt - 1)`. Blocking sleep is correct on a blocking thread.

**Task #25 — O(n·k) indexed DAG in build_stages_inner**

`must_precede` function deleted. `build_stages_inner` now builds:
- `writers_by_field: HashMap<(&str, &str), Vec<usize>>`
- `readers_by_field: HashMap<(&str, &str), Vec<usize>>`
- `resource_writers: HashMap<TypeId, Vec<usize>>`
- `resource_readers: HashMap<TypeId, Vec<usize>>`

Then iterates each system j once, looking up predecessors via index. Edge deduplication via `HashSet<(usize, usize)>`. The `i < j` guard is enforced in `add_edge` closure.

Key: 20-system test `test_indexed_dag_20_systems_stage_assignments` verifies topological ordering. Note that multiple systems writing the same field get serialized (write-write conflict), so 5 id-writers in sequence land in 5 different stages — the test asserts ordering invariants, not specific stage numbers.

**Task #27 — Structured RetryExhausted**

`CanudoError::RetryExhausted` changed from `String` to `{ source: Box<CanudoError>, attempts: usize }`. `retry_exhausted()` constructor now takes `(source: CanudoError, attempts: usize)`. All call sites pass the original error + attempt count directly. `Display` shows "Retry exhausted after N attempt(s): <source>". `PartialEq` compares both fields.

**Why:** pre-existing caller code in `canudo-service` and distributed benches have unrelated pre-existing errors; those are out of scope.

**Test count:** 129 lib tests pass, 23 doc tests pass.
