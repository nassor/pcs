---
name: Task #28 Raft State Machine Secondary Index
description: CLAIMS_BY_BATCH secondary index added to apply_claim_row_range for O(k) overlap checks; two-phase claim with read precheck
type: project
---

Task #28 added a `CLAIMS_BY_BATCH` persistent secondary index to `src/distributed/consensus/state_machine.rs` to replace the O(total_claims) full-table scan in `apply_claim_row_range`.

**Why:** Under load, every claim attempt opened a `WriteTransaction` and scanned all claims in the database. This serialised all writes and made claim latency linear in the number of historical claims.

**Index design:**
- Key: `batch_id_be8 ++ claim_id_16` (24 bytes) — big-endian batch ID prefix enables O(k) range scan for a single batch via `table.range(batch_lo..batch_hi)`
- Value: `start_row_be4 ++ end_row_be4 ++ status_byte` (9 bytes) — encodes all data needed for overlap check without touching the primary `arrow_claims` table
- Status byte: 0=Pending, 1=Claimed, 2=Completed

**Two-phase claim protocol:**
1. Phase 1: ReadTransaction precheck — O(1) batch existence + O(k) overlap scan. Returns early on reject without write lock.
2. Phase 2: WriteTransaction final check — re-runs O(k) overlap scan under write lock (TOCTOU safety), then inserts into both `arrow_claims` and `CLAIMS_BY_BATCH`.

**Secondary index maintenance:**
- `apply_claim_row_range`: inserts secondary entry with `Claimed` status on success
- `apply_ack_claim`: updates secondary entry status to `Completed`
- `apply_release_claim`: updates secondary entry status to `Pending`
- `restore_state`: rebuilds the entire secondary index from claims list (used after snapshot install)
- `find_first_pending_claim`: updated to use secondary index range scan instead of full claims table scan

**Transport.rs fix:** Prior agent changes to transport.rs introduced broken openraft API usage (`openraft::SnapshotResponse` root import, `openraft::impls::CommittedLeaderId` not found). Fixed by removing the redundant test imports and fixing 3 clippy warnings (redundant_closure, unnecessary_lazy_evaluations, clone_on_copy). These were pre-existing latent errors exposed when my state_machine.rs changes forced recompilation.

**Test count:** 172 → 198 (26 new tests: 4 state_machine secondary index tests + 21 previously uncached transport tests + 1 runner test).

**How to apply:** When adding any claim-related apply handler, always update CLAIMS_BY_BATCH in the same transaction as the primary CLAIMS table.
