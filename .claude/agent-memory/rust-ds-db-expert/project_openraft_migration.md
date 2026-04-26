---
name: openraft migration
description: Canudo switched from tikv/raft to openraft 0.10.0-alpha.17 for Raft consensus
type: project
---

Canudo switched from tikv/raft (v0.7) to openraft (v0.10.0-alpha.17) because tikv/raft depends on protobuf-build which rejects protobuf >= 30.

**Why:** tikv/raft was a build-time dependency blocker. openraft is pure Rust, no protobuf, actively maintained, and has a cleaner API.

**How to apply:**
- Feature flag is `distributed-consensus-raft` (not just `raft`)
- `CanudoTypeConfig` is defined via `declare_raft_types!` in `types.rs`, re-exported from `transport.rs`
- `RedbLogStore` implements `RaftLogStorage` + `RaftLogReader` (storage.rs)
- `RedbStateMachine` implements `RaftStateMachine` (also in storage.rs)
- Driver is thin — openraft's `Raft::new()` handles tick loop, Ready processing, etc.
- Entries serialized as JSON via serde_json (not protobuf or bincode)
- openraft uses `entry.log_id` field directly (not `.get_log_id()`)
- `Unreachable::new()` requires `'static` on the error type
