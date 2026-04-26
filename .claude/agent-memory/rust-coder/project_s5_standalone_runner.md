---
name: Service S5 Standalone Runner
description: run_standalone implementation, StandaloneStats, ChannelSource EOF semantics in tests, World::clear behavior
type: project
---

`src/service/standalone.rs` (1044 lines) implements `run_standalone(BuiltService, &ServiceConfig, CancellationToken) -> Result<StandaloneStats, CanudoError>`.

**Why:** Phase S5 standalone runner bridges BuiltService to the actual execution loop.

**How to apply:** Coder 5 (HTTP) integrates StandaloneStats via Arc<RwLock<StandaloneStats>>.

## Key design decisions

- `Box::leak` converts `String` component names to `&'static str` once at startup (required by World API)
- `World::clear()` exists and resets rows while keeping component schemas — call it between iterations
- `drain_into_world` / `drain_world` both require `&'static str` component names (not `&str`)
- `ChannelSource` blocks on open channel — tests must use either EOF-signaling sources or cancellation to exit
- `BurstSource` test helper: uses `Arc<Mutex<Vec<RecordBatch>>>` and returns `None` when empty — safe for multi-iteration tests

## ChannelSource EOF gotcha

`drain_into_world` loops until `next_batch()` returns `None`. `ChannelSource::next_batch()` blocks via `rx.recv()` until sender is dropped. If the tx is still alive, the drain blocks forever — the `tokio::select!` with `cancel.cancelled()` is the only rescue. Tests using ChannelSource in interval/continuous mode will appear to stall unless:
1. The tx is dropped (EOF signaled), or
2. Cancellation fires to exit the `select!`

Use `BurstSource` (defined in test module) for multi-iteration tests where you want finite data per iteration.

## StandaloneStats for Coder 5

Recommendation: use `Arc<RwLock<StandaloneStats>>` shared between the runner and the HTTP handler. The runner writes periodically (after each iteration), HTTP reads. This avoids mpsc channel overhead for a polling use case. The runner can accept an `Option<Arc<RwLock<StandaloneStats>>>` parameter to write updates.

## tokio-util already in Cargo.toml

`tokio-util = { version = "0.7", features = ["rt"], optional = true }` was already added by Coder 5 and included in the `service` feature. No additional Cargo.toml changes needed.

## Test categories (all pass)
1. OneShot runs one iteration
2. Continuous processes multiple iterations
3. Interval honors the sleep (use no-source variant to isolate sleep timing)
4. Cancellation exits cleanly (Ok return)
5. Source error → iteration_errors++, loop continues
6. Pipeline error → still drains sink
7. No sources → pipeline still runs
8. Sink finish called on exit
9. World clear between iterations (verified via BurstSource + RecordingSystem)
