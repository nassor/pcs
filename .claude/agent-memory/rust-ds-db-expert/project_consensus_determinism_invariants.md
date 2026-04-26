---
name: Consensus apply-determinism invariants
description: Deterministic-apply rules for the openraft state machine — no SystemTime reads in apply, halt on decode failure, respect Completed claim status
type: project
---

Core invariants the `src/distributed/consensus/` state machine must preserve:

1. **No wall-clock reads inside `apply()`**. Every mutating `ConsensusCommand` variant carries a `now_at_propose: u64` field stamped by the proposer (`propose_now_millis()` in `store.rs`). `apply_register_master_batch`, `apply_claim_row_range`, `apply_renew_claim`, and `apply_checkpoint` all use the stamped value for `created_at` / `lease_expires_at`. The private helper `propose_now_millis()` is `pub(super)` in `state_machine.rs` and MUST NOT be called from any `apply_*` handler — doing so would break Raft replay determinism.

**Why:** A follower replaying the committed log on a different wall clock would compute different timestamps than the leader, so snapshot parity and replay determinism would break. Regression test `test_state_machine_apply_is_deterministic_across_replicas` applies a fixed command sequence to two independent DBs and asserts byte-equal `dump_state` output.

**How to apply:** When adding a new mutating variant to `ConsensusCommand`, add a `now_at_propose: u64` field (marked `#[serde(default)]` for backwards JSON compat), stamp it at every proposer site in `store.rs`, and use the stamped value — never `SystemTime::now()` — inside the apply handler.

2. **Halt on committed-log decode failure**. `ArrowRedbStateMachine::apply` in `storage.rs` panics if a `Normal` entry fails to decode: decode failures indicate programmer/build bugs (schema mismatch, corrupt log), and the only safe response is halt — advancing `last_applied` past an un-applied entry would silently diverge the state machine from the committed log. Order is strictly: decode → apply → advance. `last_applied` is never advanced before success.

**How to apply:** Don't add fallbacks for decode failures in the apply path. If you ever want to tolerate forward-compat issues, gate it at the propose boundary (reject the command before it's appended to the log), not at apply time.

3. **`Completed` claims are NOT pending**. In `state_machine.rs`, both `find_first_pending_claim` and `apply_claim_row_range`'s overlap check treat `ClaimStatus::Claimed` AND `ClaimStatus::Completed` as occupied. Only `Pending` (released) and absent ranges are eligible for a new claim. Skipping `Completed` would re-hand-out already-processed rows, producing at-least-twice delivery beyond the intended semantics. Regression test: `test_find_first_pending_claim_skips_completed_ranges`.

4. **Transient-overlap retry loop in `claim_next_batch`**. `store.rs` wraps the scan+propose in a bounded retry loop (`MAX_CONFLICT_RETRIES = 8`). A `ConsensusResponse::Error` whose message contains `"overlaps an existing active claim"` is treated as a race (another runner claimed the range between our scan and our propose) and triggers a rescan rather than returning `Ok(None)`. A true empty scan returns `Ok(None)`. The classification is currently string-matching (`is_transient_overlap_conflict`) — a dedicated `ConsensusResponse::Conflict` variant would be cleaner; TODO left in the code.
