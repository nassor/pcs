---
name: Task #24 design — claim-level retry cap
description: Full design for MasterBatchRecord.release_attempts + PoisonBatch cap enforcement. Draft for circulation.
type: project
---

# Task #24: Claim-level retry cap — design doc

**Author**: dist-expert
**Status**: DRAFT — circulating for concurrence before code
**Date**: 2026-04-15
**Scope**: ~1 week. Escalate before day 5 if drift.

---

## Problem

`DistributedRunner` has no upper bound on how many times a batch can cycle `Pending → Claimed → Pending` via `release_claim` / `ReclaimExpired`. A permanently-failing batch (poison batch, guest trap on bad input, deterministic `unreachable!`) will loop forever, wasting cluster resources and flooding logs. This hazard is the motivation for #24 and is also why the WIT `Generic → permanent` mapping was locked in #12.

## Solution overview

Persist a **per-row-range release counter** on `MasterBatchRecord`, increment on release and on `ReclaimExpired`, reset on successful ack, and enforce a **runner-local cap** that trips a new idempotent `ConsensusCommand::PoisonBatch { batch_id }` when the counter exceeds `RunnerConfig::max_claim_releases` (default 5, `0` = unlimited).

## Design decisions

### 1. Counter lives on `MasterBatchRecord`, not `BatchClaim`

`BatchClaim.claim_id: Uuid` is fresh per grant (`ConsensusCommand::ClaimRowRange` takes a new Uuid from the caller; state machine at `crates/pcs-service/src/distributed/consensus/state_machine.rs:272-289` verified). A counter on `BatchClaim` reads 0 every time. The counter must persist across `claim_id` boundaries, which means it lives on the row range.

**Field**: `release_attempts: u32` on `MasterBatchRecord` (state_machine.rs:99-109), next to the existing `checkpoint_seq: u64`. Both are monotonic counters updated inside the serialized Raft state machine write path — same precedent pattern.

### 2. Reset-on-ack semantics (consecutive, not lifetime)

`apply_ack_claim` zeros `release_attempts` in the same redb write txn that marks the claim acked.

**Why**: Monotonic counters accumulate over batch lifetime and eventually poison healthy batches for no reason. Reset-on-ack counts *consecutive* failures since the last success, matching how every retry budget / circuit breaker works (HTTP retry policies, tokio retry, etc.).

**Failure mode avoided**: a pipeline that successfully processes a batch 999 times and then fails once would hit a monotonic `release_attempts == 1000` and poison — wrong. Reset-on-ack gives you `release_attempts == 1`, correct.

### 3. `apply_reclaim_expired` must also increment

A crashed runner (lease expired, claim reclaimed by the sweeper) is indistinguishable from an explicit release — the batch was attempted and not completed. If we only count explicit releases, a runner that crashes 1000 times in a row on a poison batch never trips the cap.

One-line addition to `apply_reclaim_expired` at state_machine.rs (current path writes `BatchRecord::Pending` back; needs to also read the master batch, bump `release_attempts`, write it back).

### 4. Schema migration: versioned `StoredMasterBatch` enum

`MasterBatchRecord` is postcard-encoded. Adding a `u32` field is a breaking encoding change. Use an explicit version enum rather than `#[serde(default)]`:

```rust
#[derive(Serialize, Deserialize)]
pub enum StoredMasterBatch {
    V1(MasterBatchRecordV1),  // old layout, no release_attempts
    V2(MasterBatchRecordV2),  // new layout, has release_attempts: u32
}
```

On read: try `StoredMasterBatch::V2` decode first; if it fails (old on-disk format), fall back to `V1` decode and upgrade in-place on next write.

**Why explicit enum, not serde default**: respects the halt-on-decode-failure determinism invariant (see `project_consensus_determinism_invariants.md`). We don't silently eat decode errors. A versioned enum makes the upgrade path explicit and testable.

**Migration risk**: this is ~half the work of #24. Budget accordingly. The biggest risk is getting the V1→V2 upgrade write path correct without breaking Raft log replay (new nodes replaying a log with V1 writes must decode cleanly).

### 5. Poison decision: runner-local enforcement via `PoisonBatch`

Three options I considered:
- **A** (reject): runner embeds `max_attempts` in `ReleaseClaim` command — racy, last-writer-wins.
- **B**: cluster-wide config table, set at bootstrap — correct-by-design, but inflexible for testing.
- **C** (chosen): runner-local enforcement. When the runner observes `release_attempts >= max_claim_releases`, it proposes `ConsensusCommand::PoisonBatch { batch_id }`. State machine atomically marks the batch poisoned. Idempotent — two runners racing to poison the same batch both succeed, first writer wins on timestamp.

**Why C**: lets individual runners override the cap (e.g. tests use `max_claim_releases = 0` for unlimited, prod uses 5). No new cluster-wide config table needed. The determinism of the cap trip is preserved because the state machine owns the counter; the runner only decides *when* to propose `PoisonBatch`, not *what* the counter says.

**Idempotency requirement**: `apply_poison_batch` MUST early-return if the batch is already `Poisoned`. Do not overwrite `poisoned_at` on second writer. This was flagged explicitly by wasm-lead and is captured in #24 acceptance test 8.

```rust
fn apply_poison_batch(db: &Database, batch_id: u64, now_at_propose: u64) -> PcsResult<ConsensusResponse> {
    let txn = db.begin_write()?;
    {
        let mut table = txn.open_table(MASTER_BATCHES)?;
        let Some(g) = table.get(&batch_id)? else {
            return Ok(ConsensusResponse::Error {
                message: format!("PoisonBatch: {batch_id} not found")
            });
        };
        let stored = dec::<StoredMasterBatch>(g.value())?;
        let mut record = stored.into_latest();
        if record.status == BatchStatus::Poisoned {
            return Ok(ConsensusResponse::Ok); // idempotent no-op
        }
        record.status = BatchStatus::Poisoned;
        record.poisoned_at = Some(now_at_propose);
        table.insert(&batch_id, enc(&StoredMasterBatch::V2(record))?.as_slice())?;
    }
    txn.commit()?;
    Ok(ConsensusResponse::Ok)
}
```

### 6. Config: `RunnerConfig::max_claim_releases: u32`

Default `5`. `0` = unlimited (preserves current behavior, explicit opt-out for tests). Documented in `RunnerConfig` rustdoc.

### 7. `BatchClaim.release_attempts` as read-only cache

When `claim_next_batch` reads the `MasterBatchRecord`, it copies `release_attempts` into the returned `BatchClaim` for tracing spans ("processing claim X, release_attempts=3/5"). Read-only from the runner's perspective; source of truth remains the master batch.

### 8. `BatchStatus::Poisoned` is a terminal state

Once poisoned, `claim_next_batch` will not return the batch. Operators can observe poisoned batches via `/status` endpoint. Manual intervention (or a future `UnpoisonBatch` command) is required to resume processing.

**Not adding UnpoisonBatch in #24**: operators who want to retry a poisoned batch can delete and re-register it as a new `batch_id`. Keeping the surface small.

## Determinism analysis

All state mutations happen inside Raft state machine write txns:
- `apply_release_claim`: read master batch, `release_attempts += 1`, write back.
- `apply_reclaim_expired`: read master batch, `release_attempts += 1`, write back (for each reclaimed claim).
- `apply_ack_claim`: read master batch, `release_attempts = 0`, write back.
- `apply_poison_batch`: read master batch, early-return if already Poisoned, else set status + `poisoned_at`, write back.

No `SystemTime::now()` reads in any apply handler. All timestamps come from `now_at_propose` in the command, deterministic on replay. Satisfies halt-on-decode-failure + no-wall-clock-reads invariants already documented.

## Race scenarios

### Scenario 1: concurrent release + claim
- Runner A holds claim_id=X, decides to release. Proposes `ReleaseClaim { claim_id: X }`.
- Raft applies → counter becomes N+1, claim transitions to Pending.
- Runner B immediately calls `claim_next_batch`, gets a new claim_id=Y with `release_attempts=N+1` cached on the returned `BatchClaim`.
- Runner A's release completed, returns `Ok`.
- No divergence — the state machine serialized both.

### Scenario 2: cross-node poison race
- Runner A on node 1 observes `release_attempts=5` (cached from claim), decides to propose `PoisonBatch`.
- Runner B on node 2 observes `release_attempts=6` (its claim came after another release), also proposes `PoisonBatch`.
- Raft orders both. First applied → batch → Poisoned, `poisoned_at=now_A`. Second applied → no-op (already Poisoned).
- `/status` may briefly show different `release_attempts` on each node until replication converges. Transient, cosmetic, non-issue.

### Scenario 3: runner crash during release
- Runner A proposes `ReleaseClaim`, state machine applies `release_attempts += 1`, runner crashes before observing response.
- On next claim, runner B (or restarted A) sees `release_attempts=N+1` — correct.
- No double-counting because the commit is atomic in the state machine, not split across runner + state machine.

### Scenario 4: lease expiry during release
- Runner A holds claim, lease expires while A is still processing. `apply_reclaim_expired` fires (sweeper), increments `release_attempts`.
- Runner A's delayed `ReleaseClaim` command arrives at the state machine. What happens?
- **Option a**: `ReleaseClaim` checks claim status → finds it already Released (by reclaim) → no-op, no increment. Correct.
- **Option b**: `ReleaseClaim` is idempotent on already-released claims → same behavior. Need to verify current `apply_release_claim` handles this case.

**Action item**: audit `apply_release_claim` for idempotency on already-released claims. If not idempotent today, fix as part of #24 to avoid double-counting with `ReclaimExpired`.

## Test matrix (10 tests)

1. **apply_release_claim increments**: seed a batch, apply `ReleaseClaim`, assert `release_attempts == 1` readable from `MasterBatchRecord`.
2. **apply_ack_claim resets**: seed, release (N=3), ack, assert `release_attempts == 0`.
3. **apply_reclaim_expired increments**: seed, claim, let lease expire, trigger `ReclaimExpired`, assert `release_attempts == 1`.
4. **N consecutive releases**: apply N releases in sequence, assert `release_attempts == N`.
5. **N releases then ack**: apply N releases, then ack, assert `release_attempts == 0`.
6. **Cap trip via PoisonBatch**: runner with `max_claim_releases=3`, force 3 releases, assert 3rd release triggers `PoisonBatch`, batch status becomes `Poisoned`, runner returns Err with poison log.
7. **cap=0 never poisons**: runner with `max_claim_releases=0`, force 100 releases, assert no `PoisonBatch` ever issued.
8. **apply_poison_batch idempotent + first-writer `poisoned_at`**: propose `PoisonBatch { now_at_propose: 100 }`, then `PoisonBatch { now_at_propose: 200 }`, assert final `poisoned_at == Some(100)`.
9. **StoredMasterBatch V1→V2 upgrade**: write a `V1` to redb, open with new binary, assert read path produces an upgraded `V2` with `release_attempts=0` default.
10. **3-node Raft cluster convergence**: spin up 3-node cluster, apply a sequence of claim/release/ack commands across nodes, assert all 3 nodes agree on `release_attempts` after replication.

Additional determinism replay test: two nodes processing identical command log reach identical state (verifies no `SystemTime::now()` or other non-deterministic reads slipped in).

## Scope estimate

| Phase | Days |
|-------|------|
| State machine + storage changes (apply handlers, `PoisonBatch` command, `BatchStatus::Poisoned`) | 1-2 |
| Versioned schema migration infrastructure (`StoredMasterBatch` enum, read-path upgrade) | 1-2 |
| Runner changes (propose `PoisonBatch` on cap trip, `release_attempts` cache on `BatchClaim`, `max_claim_releases` config) | 0.5 |
| Unit tests (1-9) | 1 |
| 3-node Raft cluster convergence test (10) + determinism replay | 1 |
| **Total** | **~1 week** |

Escalate to team-lead before day 5 if drift.

## Open questions — RESOLVED

Team-lead concurrence received on all 5 questions, and the race-4 audit is complete.

1. **`UnpoisonBatch`**: deferred. Operators re-register. File Phase-4 follow-up if ops asks.
2. **`/status` exposure**: default show poisoned batches. Paginate if noisy; don't hide.
3. **Metric emission**: `pcs.distributed.poisoned_claims` counter behind `#[cfg(feature = "tracing")]`.
4. **Race 4 double-count audit — NO HAZARD.** Code-read confirms:
   - `apply_reclaim_expired` at state_machine.rs:1091 filters `record.status == ClaimStatus::Claimed` then transitions to `Pending`.
   - `apply_release_claim` at state_machine.rs:849 guards `if record.status != ClaimStatus::Claimed` and returns `Error` without mutating.
   - A late `ReleaseClaim` after a reclaim hits the guard; no counter touch.
   - **Safe to put `release_attempts += 1` inside the `ClaimStatus::Claimed` branch** of `apply_release_claim`, and inside the reclaim loop of `apply_reclaim_expired`.
5. **Poisoned filter in `claim_next_batch` — Option α (remove from PENDING_BATCHES secondary index).**
   - `find_first_pending_batch` (state_machine.rs:1306) iterates `PENDING_BATCHES` redb table — already the authoritative eligibility mechanism.
   - `apply_ack_claim` removes batch_ids when all claims Completed; `apply_poison_batch` does the same on poison.
   - `find_first_pending_batch` needs zero changes. Zero hot-path overhead.
   - `/status` reads `MasterBatchRecord.status` directly to list poisoned.

## Audit findings — additional implementation details

**No existing `BatchStatus` enum.** `MasterBatchRecord` (state_machine.rs:99-109) has NO status field today — only `batch_id`, `component`, `schema_id`, `ipc_bytes`, `total_rows`, `created_at`, `checkpoint_seq`. `BatchStatus { Active, Poisoned }` must be introduced fresh in #24 as part of the V1→V2 migration.

**Schema migration shape** (locked after audit):

```rust
// V1 (legacy on-disk)
pub struct MasterBatchRecordV1 {
    pub batch_id: u64,
    pub component: String,
    pub schema_id: u32,
    pub ipc_bytes: Vec<u8>,
    pub total_rows: u32,
    pub created_at: u64,
    pub checkpoint_seq: u64,
}

// V2 (current, with cap support)
pub struct MasterBatchRecordV2 {
    pub batch_id: u64,
    pub component: String,
    pub schema_id: u32,
    pub ipc_bytes: Vec<u8>,
    pub total_rows: u32,
    pub created_at: u64,
    pub checkpoint_seq: u64,
    // V2 additions:
    pub release_attempts: u32,
    pub status: BatchStatus,
    pub poisoned_at: Option<u64>,  // unix millis, Some iff status == Poisoned
}

pub enum BatchStatus { Active, Poisoned }

pub enum StoredMasterBatch {
    V1(MasterBatchRecordV1),
    V2(MasterBatchRecordV2),
}
```

Read path tries `V2` decode first; on failure falls back to `V1` and upgrades to `V2` in-place with `release_attempts: 0, status: Active, poisoned_at: None`. First subsequent write persists as V2.

**Edits needed in existing apply handlers**:

- `apply_release_claim` (state_machine.rs:823-889): inside the successful branch where `record.status == ClaimStatus::Claimed`, after writing the claim record, call `increment_release_attempts(&txn, batch_id)`.
- `apply_ack_claim` (state_machine.rs:700-820): after writing the Completed claim record, call `reset_release_attempts(&txn, batch_id)` (zero the counter on successful completion).
- `apply_reclaim_expired` (state_machine.rs:1066-1139): inside the loop at line 1114, after the claim insert, call `increment_release_attempts(&txn, record.batch_id)` for each reclaimed claim.

**New `apply_poison_batch` handler**: idempotent early-return if `record.status == BatchStatus::Poisoned`; else set `status = Poisoned`, `poisoned_at = Some(now_at_propose)`, write back, AND remove `batch_id` from `PENDING_BATCHES`. No counter reset (leaves `release_attempts` as observed for audit).

**Helper functions** (new, private in state_machine.rs):
```rust
fn increment_release_attempts(txn: &WriteTransaction, batch_id: u64) -> PcsResult<()>;
fn reset_release_attempts(txn: &WriteTransaction, batch_id: u64) -> PcsResult<()>;
fn load_master_batch(txn: &impl ReadableTable, batch_id: u64) -> PcsResult<Option<MasterBatchRecordV2>>;
```

Each opens `MASTER_BATCHES`, reads via `StoredMasterBatch::V2` / V1 fallback, mutates, writes back. All inside the already-open write txn.

**No changes needed to**:
- `find_first_pending_batch` (state_machine.rs:1306) — poisoned batches are simply absent from `PENDING_BATCHES` after `apply_poison_batch` removes them.
- `claim_next_batch` on the store side (store.rs:432) — unchanged.
- `apply_claim_row_range` — unchanged.

**Test 11 addition** (beyond the 10-test matrix): poisoned batch removal from `PENDING_BATCHES` — apply `PoisonBatch`, then call `find_first_pending_batch` and assert the poisoned batch is NOT returned.

## Circulation plan

Once this doc is saved:
1. Ping team-lead + wasm-lead (as previous #23 reviewer) + architect (as #10 reviewer and general consensus-layer watcher) for concurrence.
2. Address comments.
3. Claim #24 code work (already in_progress via TaskUpdate — this doc itself is the in_progress activity).
4. Execute implementation in the order listed in "Scope estimate".
5. All 10 tests green → ping architect for code review → mark #24 completed.

**Not touching code until concurrence is in.**
