---
name: RedbSharedStore Design (redb + Raft consensus)
description: Architecture for self-contained distributed deployment using embedded redb + Raft consensus — no external database needed
type: project
---

Designed a RedbSharedStore as an alternative to PostgresSharedStore for Canudo's distributed ECS engine.

**Key decisions:**
- redb is the embedded storage backend (pure Rust, ACID, crash-safe). Used both for Raft log persistence (implementing raft::Storage trait) and application state (work batches, checkpoints).
- raft-rs (tikv/raft-rs v0.7) provides the consensus algorithm. Transport is TCP + bincode with 4-byte length-prefix framing.
- All mutations (ClaimBatch, AckBatch, SaveCheckpoint, etc.) flow through Raft as StoreCommand enum entries. Reads are local redb reads (leader) or stale reads (follower).
- Single-node mode: Raft with quorum=1, no network. Transitions to multi-node via ConfChange.
- Feature-gated under `consensus` flag to avoid pulling raft/redb/serde/bincode for users who don't need it.
- Snapshots are logical (serialize all application table rows), not redb file copies. Log compaction after 10K entries.
- Lease expiry uses leader-proposed ExpireLeases commands with leader's wall clock timestamp for determinism.
- Critical invariant: ClaimBatch goes through Raft total ordering to prevent double-claiming across instances.

**Why:** Enables zero-dependency deployment for small clusters (3-5 nodes), edge, embedded, dev/testing. PostgresSharedStore remains the choice for large fleets with existing DB infra.

**How to apply:** When implementing, start with Phase 1 (redb Storage trait) and Phase 3 (single-node integration) — these are independently testable. Module structure lives under src/distributed/redb_store/.
