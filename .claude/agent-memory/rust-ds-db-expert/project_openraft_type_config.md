---
name: CanudoTypeConfig D/R bounds and encoding choice
description: openraft 0.10.0-alpha.17 AppData requires Debug+Display; Canudo uses D=ConsensusCommand, R=ConsensusResponse with postcard log encoding
type: project
---

openraft 0.10.0-alpha.17's `AppData` trait bound is `OptionalFeatures + fmt::Debug + fmt::Display + 'static`. `AppDataResponse` is just `OptionalFeatures + 'static`. With `serde` feature on, `OptionalFeatures` requires Serialize+Deserialize+Send+Sync.

Canudo `CanudoTypeConfig`:
- `D = ConsensusCommand`, `R = ConsensusResponse` (was `D = R = String` with serde_json round-trip through driver + state machine; that path is gone)
- `ConsensusCommand` has a hand-written `Display` impl that deliberately omits `ipc_bytes` (can be up to 1 MiB) — only tag + identifying keys. Needed because D requires Display.

**Why:** carrying types directly lets openraft give the state machine the command as-is. No chance of encode/decode divergence splitting the cluster's view of a committed entry, and no UTF-8 round-trip cost on the hot apply path.

**How to apply:** if you ever widen `ConsensusCommand`, update both the Display impl and any new field must be stable-ordered (no HashMap) for postcard canonical-ness. If you need to change `D` to a type that lacks Display, you'll have to newtype it.

**Log entry encoding:** postcard (not serde_json). Canonical by construction — matters because any future content-hashing of log entries needs byte stability. redb log storage in `ArrowRedbLogStore` wraps all redb I/O in `tokio::task::spawn_blocking` because `txn.commit()` fsyncs; also uses `Arc<Database>` (no Mutex) since redb's `Database::begin_write`/`begin_read` both take `&self` and coordinate internally. The wider `ArrowRedbStateMachine` still uses `Arc<Mutex<Database>>` for the app_db because that type is shared with snapshot.rs/driver.rs/store.rs.
