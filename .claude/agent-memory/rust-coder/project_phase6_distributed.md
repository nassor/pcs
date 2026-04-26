---
name: Phase 6 Arrow Distributed Layer
description: Arrow-IPC distributed execution layer under src/arrow/distributed/; feature flags, key design decisions, completed deliverables
type: project
---

Phase 6 delivered the Arrow-IPC distributed execution layer under `src/arrow/distributed/`, parallel to the existing `src/distributed/` (not modified).

**Feature flags added:**
- `arrow-distributed` — core traits + redb state machine + store + runner + transport
- `arrow-distributed-raft` — openraft consensus layer (log store, state machine, snapshot, driver)

**Key design decisions (from advisors):**
- Separate redb files for log store and state machine (not shared)
- serde_json for all Raft log entry payloads (not postcard)
- 1 MiB hard cap (`MAX_LOG_ENTRY_BYTES`) enforced at the propose boundary in `ArrowRedbSharedStore`, not only inside state machine
- `DEFAULT_LEASE_TTL_MILLIS = 90_000` (90s >> election_timeout × 3)
- Runners STOP immediately on lease renewal failure (at-most-once semantics for stage execution)
- No content-hashed side-channel blob store; snapshot handled via openraft's native `build_snapshot` / `install_snapshot`

**Critical redb pattern:** `table.get()` returns `AccessGuard<'_, &[u8]>` that borrows the table. Must extract to owned bytes (`guard.value().to_vec()`) before any subsequent insert on the same table in the same transaction.

**Module status:** All files pass `cargo clippy --all-targets --all-features -- -D warnings` and `cargo fmt`. 289 tests pass under `--features arrow-distributed-raft`.

**Deliverables completed:**
- `src/arrow/distributed/{partition,checkpoint,strategy,runner}.rs`
- `src/arrow/distributed/consensus/{types,state_machine,snapshot,storage,store,driver,transport,mod}.rs`
- `examples/arrow_distributed_pipeline.rs` (requires `arrow-distributed`)
- `benches/arrow_distributed.rs` (requires `arrow-distributed`; snapshot bench behind `arrow-distributed-raft`)

**Why:** How to apply:** When touching distributed layer files, always check the redb borrow guard pattern and the 1 MiB cap enforcement point.
