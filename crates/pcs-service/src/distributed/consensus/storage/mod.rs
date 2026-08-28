//! Persistent Raft log storage and Arrow-IPC state machine backed by separate redb files.
//!
//! ## Separate redb files
//!
//! Arrow blob storage in the state machine competes with log storage for write I/O, so
//! the two are kept strictly separate:
//!
//! - **Log store** ([`RaftRedbLogStore`], in `log_store`): Raft metadata (hard state,
//!   conf state), log entries and the latest snapshot. No Arrow data lives here.
//!
//! - **State machine** ([`AppStateMachine`], in `state_machine_store`): drives the
//!   Arrow-IPC application state through `sm_apply`. Arrow IPC bytes are written in the
//!   same transaction as the log apply, so the fsync is fate-shared.
//!
//! Both halves share the redb table definitions, the `postcard` encode/decode helpers,
//! and the `validate_store_consistency` startup guard, all defined in this file.
//!
//! ## Blocking-I/O discipline
//!
//! All redb read/write transactions are wrapped in
//! [`tokio::task::spawn_blocking`]. redb's commit path issues `fsync`, which must not
//! run on a tokio worker thread: blocking a worker for fsync latency stalls every
//! other task on that runtime. The log store holds an `Arc<Database>` and no
//! [`std::sync::Mutex`], because redb's `Database::begin_write` / `begin_read` both
//! take `&self` and coordinate internally, so external serialization would only add
//! lock contention.
//!
//! ## Entry encoding
//!
//! Log entries are prost-encoded `eraftpb::Entry` (the raft `prost-codec` wire
//! format); metadata rows (hard state, conf state) are `postcard`. Postcard is
//! canonical by construction: the same input always produces the same bytes, and
//! it has no JSON map-ordering ambiguity if a nested type grows a map field.

#[cfg(feature = "distributed-raft")]
#[path = "."]
pub(crate) mod raft_impl {
    mod log_store;
    mod state_machine_store;

    use std::io;

    use redb::TableDefinition;

    use crate::error::{PcsError, PcsResult};

    pub use log_store::RaftRedbLogStore;
    pub use state_machine_store::AppStateMachine;

    /// State-machine metadata table (lives in the *app* redb file).
    ///
    /// Stores `last_applied` so it survives restarts. Using a separate table keeps
    /// SM metadata writes fate-shared with the application data in the same redb
    /// file, so one fsync covers both.
    const SM_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("arrow_sm_meta");

    fn enc<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
        postcard::to_allocvec(v).map_err(|e| io::Error::other(format!("postcard encode: {e}")))
    }

    fn dec<T: for<'de> serde::Deserialize<'de>>(b: &[u8]) -> io::Result<T> {
        postcard::from_bytes(b).map_err(|e| io::Error::other(format!("postcard decode: {e}")))
    }

    fn to_io(e: impl std::error::Error) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Validate that the state machine is not behind what the log store still
    /// holds.
    ///
    /// Call this once from [`ArrowRaftDriver::start`] after opening both
    /// halves of a node directory. A mismatch indicates the files were
    /// restored from mismatched backups or the log store was wiped while the
    /// state-machine file was retained.
    ///
    /// `first_index` is the log store's first retained index; `applied` is the
    /// state machine's last applied index (`None` when nothing has applied).
    /// The state machine is behind when `applied + 1 < first_index`.
    ///
    /// # Safety
    ///
    /// This is a diagnostic check only and modifies no state.
    pub fn validate_store_consistency(first_index: u64, applied: Option<u64>) -> PcsResult<()> {
        if applied.is_some_and(|a| a + 1 < first_index) {
            return Err(PcsError::store(format!(
                "store consistency violation: log store's first retained index is {} \
                 but state machine last_applied is index {} — state machine is behind. \
                 Do not mix log and state-machine redb files from different backups.",
                first_index,
                applied.expect("checked is_some_and")
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::distributed::consensus::state_machine::KEY_SM_LAST_APPLIED;
        use raft::eraftpb::Entry;
        use redb::{Database, ReadableDatabase};
        use tempfile::TempDir;

        pub(super) fn make_store(dir: &TempDir) -> RaftRedbLogStore {
            RaftRedbLogStore::open(dir.path().join("arrow_raft.db")).unwrap()
        }

        /// A plain `(term, index)` tuple; the raft-rs world has no LogId type.
        pub(super) fn log_id(term: u64, index: u64) -> (u64, u64) {
            (term, index)
        }

        pub(super) fn blank_entry(index: u64) -> Entry {
            Entry {
                term: 1,
                index,
                data: Vec::new(),
                ..Default::default()
            }
        }

        /// Verify postcard enc/dec round-trip for Option<(term, index)>.
        #[test]
        fn test_postcard_log_id_round_trip() {
            let opt: Option<(u64, u64)> = Some(log_id(2, 10));
            let bytes = enc(&opt).unwrap();
            let decoded: Option<(u64, u64)> = dec(&bytes).unwrap();
            assert_eq!(decoded, Some((2, 10)));
        }

        /// An empty begin_write + open_table + commit must not destroy data already in
        /// the table. `AppStateMachine::open()` relies on this to be restart-safe.
        #[test]
        fn test_redb_open_table_preserves_existing_data() {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("preserve_test.redb");

            {
                let db = Database::create(&path).unwrap();
                let txn = db.begin_write().unwrap();
                {
                    let mut t = txn.open_table(SM_META_TABLE).unwrap();
                    t.insert(KEY_SM_LAST_APPLIED, b"test_data".as_slice())
                        .unwrap();
                }
                txn.commit().unwrap();
            }

            // Re-open: do the same write-txn that open() does (no inserts), then read.
            {
                let db = Database::create(&path).unwrap();
                {
                    let txn = db.begin_write().unwrap();
                    txn.open_table(SM_META_TABLE).unwrap();
                    txn.commit().unwrap();
                }
                let txn = db.begin_read().unwrap();
                let t = txn.open_table(SM_META_TABLE).unwrap();
                let val = t.get(KEY_SM_LAST_APPLIED).unwrap();
                assert!(
                    val.is_some(),
                    "data must survive reopen with empty write txn"
                );
                assert_eq!(
                    val.unwrap().value(),
                    b"test_data".as_slice(),
                    "data value must be intact"
                );
            }
        }

        #[test]
        fn test_validate_store_consistency_fresh_node() {
            // A fresh log store's first retained index is 1 with nothing applied.
            validate_store_consistency(1, None).expect("fresh node is consistent");
        }

        #[test]
        fn test_validate_store_consistency_no_purge() {
            validate_store_consistency(1, Some(5)).expect("applied set: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_ahead() {
            validate_store_consistency(4, Some(10)).expect("applied > first_index: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_equals_first_minus_one() {
            // applied == first_index - 1 is the exact boundary.
            validate_store_consistency(6, Some(5)).expect("applied == first_index - 1: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_behind_first() {
            let err = validate_store_consistency(11, Some(3))
                .expect_err("applied + 1 < first_index: must be an error");
            assert!(
                err.to_string().contains("state machine is behind"),
                "error message must explain the skew: {err}"
            );
        }

        #[test]
        fn test_validate_store_consistency_applied_none_is_consistent() {
            // Nothing applied yet: raft will apply from the first retained index.
            validate_store_consistency(6, None).expect("applied None is consistent");
        }
    }
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::{AppStateMachine, RaftRedbLogStore, validate_store_consistency};
