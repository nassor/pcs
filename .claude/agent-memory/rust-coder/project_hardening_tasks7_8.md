---
name: Distributed Hardening Tasks #7 and #8
description: Checkpoint atomicity, lease math, background renewal, graceful shutdown — key gotchas for future work
type: project
---

Tasks #7 (checkpoint/recovery) and #8 (runner/lease chaos) completed on 2026-04-15.

## Key changes

**partition.rs — BatchClaim now carries `lease_ttl_millis: u64` and `claimed_at: Instant`**
- `Instant` is not serialized; stamped at claim-grant time locally.
- `should_renew` fixed (P0-5): old formula used `lease_expires_at / 10 * 3` (30% of unix timestamp ~5e11ms) making the 30% branch dead. New: `(lease_ttl_millis * 3 / 10).min(10_000)`.
- `BatchClaim` no longer derives `PartialEq`/`Eq` (Instant doesn't implement those stably in all contexts); changed test to use field-level asserts.

**store.rs — claim_next_batch stamps new fields, exponential backoff**
- `claimed_at: Instant::now()` and `lease_ttl_millis` stamped on every `BatchClaim` construction.
- `yield_now()` replaced with exponential backoff: `10ms * 2^attempt + jitter`.
- `>=` → `>` for MAX_LOG_ENTRY_BYTES in register_master_batch and save_checkpoint.

**runner.rs — background mid-execution renewal (P0-3), graceful shutdown (P1-1), release observability (P1-2)**
- `run()` delegates to `run_with_shutdown(world_factory, CancellationToken::new())`.
- Background renewal pattern: two tokens (`renewal_failed`, `run_abort`); renewal loop selects between sleep and `run_abort.cancelled()`; if renewal fails it cancels `renewal_failed`; `tokio::select!` between `pipeline.run_on` and `renewal_failed.cancelled()`.
- CRITICAL: the renewal future must be `await`ed after the `select!` completes (it exits quickly because run_abort is cancelled). Do NOT spawn it — avoids 'static lifetime requirement.
- Accumulator load failure (P1-3): now uses release+continue (same as save failure), not return Err.
- `release_with_log` helper: logs error to tracing (or eprintln without tracing) instead of `let _ = ...`.
- `now_millis()` now panics on pre-epoch clock instead of silently returning 0.

**parquet_checkpoint.rs — atomic write (P1-4)**
- `write_checkpoint_internal` writes to `.parquet.tmp`, fsync, rename to final, fsync parent dir.
- Same pattern for JSON sidecar (`.meta.json.tmp`).
- Truncated Parquet: `ParquetRecordBatchReaderBuilder::try_new` already returns Err; load returns Err not None.
- Uses `std::io::Write as _` for `file.write_all`.

**Why:** P0-5 math bug meant short TTLs always-renewed; background renewal is needed for long-running DAGs to avoid P0-3 split-brain; atomic write prevents torn checkpoints after crash.

**How to apply:** When touching distributed processing, always check `should_renew`, background renewal loop structure, and Parquet write paths.
