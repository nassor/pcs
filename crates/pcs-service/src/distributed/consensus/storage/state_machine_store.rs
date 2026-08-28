//! Arrow-IPC Raft state machine over the application redb file.
//!
//! Holds [`AppStateMachine`]: committed commands are applied through
//! `sm_apply`, and `last_applied` is persisted into the same redb file as the
//! application data so a single fsync covers both.
//!
//! Named `state_machine_store` rather than `state_machine` so it does not
//! collide with the sibling [`crate::distributed::consensus::state_machine`]
//! module whose `apply` it drives.

use std::io;
use std::sync::{Arc, Mutex};

use raft::eraftpb::Entry;
use redb::{Database, ReadableDatabase};

use super::{SM_META_TABLE, dec, enc, to_io};
use crate::distributed::consensus::state_machine::{KEY_SM_LAST_APPLIED, apply as sm_apply};
use crate::distributed::consensus::types::ConsensusResponse;
use crate::error::{PcsError, PcsResult};

/// State machine applying committed Arrow-IPC
/// [`ConsensusCommand`](crate::distributed::consensus::ConsensusCommand)
/// entries to the redb application database.
///
/// Membership is static (seeded into the log store on first boot), so this
/// half tracks only `last_applied`; it never interprets conf-change entries.
///
/// ## Persistence
///
/// `last_applied` is persisted in the same redb database as the application
/// data, under the `arrow_sm_meta` table. This makes the state machine
/// restart-safe: `open()` restores the watermark so the driver can seed
/// `Config.applied` and skip re-applying already-committed log entries.
pub struct AppStateMachine {
    pub(crate) db: Arc<Mutex<Database>>,
    /// `(term, index)` of the last applied entry; `None` before the first
    /// apply. Interior mutability: the driver calls [`apply_batch`](Self::apply_batch)
    /// concurrently with reads of the watermark.
    last_applied: Mutex<Option<(u64, u64)>>,
}

impl Clone for AppStateMachine {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            last_applied: Mutex::new(self.last_applied()),
        }
    }
}

impl AppStateMachine {
    /// Open (or create) a state machine wrapping the given redb application
    /// database.
    ///
    /// Reads `last_applied` from the persisted `arrow_sm_meta` table so
    /// restarts recover the correct watermark. Also ensures the
    /// `arrow_sm_meta` table exists (creates it on first open).
    pub fn open(db: Arc<Mutex<Database>>) -> io::Result<Self> {
        let last_applied = {
            let guard = db
                .lock()
                .map_err(|_| io::Error::other("db mutex poisoned"))?;

            // Ensure the SM metadata table exists. redb requires a write txn to
            // create a table for the first time; the txn is a no-op once it exists.
            {
                let txn = guard.begin_write().map_err(to_io)?;
                txn.open_table(SM_META_TABLE).map_err(to_io)?;
                txn.commit().map_err(to_io)?;
            }

            let txn = guard.begin_read().map_err(to_io)?;
            let table = txn.open_table(SM_META_TABLE).map_err(to_io)?;
            match table.get(KEY_SM_LAST_APPLIED).map_err(to_io)? {
                Some(v) => dec(v.value())?,
                None => None,
            }
        };

        Ok(Self {
            db,
            last_applied: Mutex::new(last_applied),
        })
    }

    /// The `(term, index)` of the last applied entry, if any.
    pub fn last_applied(&self) -> Option<(u64, u64)> {
        *self.last_applied.lock().unwrap()
    }

    /// Override the watermark after installing a snapshot.
    pub fn set_last_applied(&self, term: u64, index: u64) {
        *self.last_applied.lock().unwrap() = Some((term, index));
    }

    /// Apply a batch of committed entries and return one response per
    /// data-carrying entry, in apply order.
    ///
    /// Each `Entry.data` is a postcard-encoded [`ConsensusCommand`]
    /// (the payload the leader proposed). Blank entries (leader elections)
    /// carry no data and produce no response; they only advance the
    /// watermark. `last_applied` advances only after a successful apply, so a
    /// crash mid-batch cannot leave a false watermark.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] when `sm_apply` fails or the watermark
    /// cannot be updated in memory; the caller must then halt the node rather
    /// than skip the entry.
    pub fn apply_batch(&self, entries: &[Entry]) -> PcsResult<Vec<(u64, ConsensusResponse)>> {
        let mut responses = Vec::new();
        let mut applied: Option<(u64, u64)> = self.last_applied();
        let db = Arc::clone(&self.db);
        let db = db
            .lock()
            .map_err(|_| PcsError::store("db mutex poisoned"))?;
        for entry in entries {
            if entry.data.is_empty() {
                // Blank entry (e.g. a newly elected leader's no-op): no state
                // change, no client response, watermark advances.
                applied = Some((entry.term, entry.index));
                continue;
            }
            let cmd = postcard::from_bytes(&entry.data)
                .map_err(|e| PcsError::store(format!("decode ConsensusCommand from entry: {e}")))?;
            let response = sm_apply(&db, cmd).map_err(|e| {
                PcsError::store(format!("state machine apply at index {}: {e}", entry.index))
            })?;
            applied = Some((entry.term, entry.index));
            responses.push((entry.index, response));
        }
        *self.last_applied.lock().unwrap() = applied;
        Ok(responses)
    }

    /// Persist the current `last_applied` watermark to the app redb file.
    ///
    /// One transaction, one fsync, fate-shared with nothing else (the apply
    /// itself already committed its own transaction).
    pub fn persist_applied(&self) -> PcsResult<()> {
        let applied = self.last_applied();
        let applied_bytes =
            enc(&applied).map_err(|e| PcsError::store(format!("encode last_applied: {e}")))?;
        let guard = self
            .db
            .lock()
            .map_err(|_| PcsError::store("db mutex poisoned"))?;
        let txn = guard
            .begin_write()
            .map_err(|e| PcsError::store(e.to_string()))?;
        {
            let mut table = txn
                .open_table(SM_META_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            table
                .insert(KEY_SM_LAST_APPLIED, applied_bytes.as_slice())
                .map_err(|e| PcsError::store(e.to_string()))?;
        }
        txn.commit().map_err(|e| PcsError::store(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::types::ConsensusCommand;
    use tempfile::TempDir;

    fn make_db(path: &std::path::Path) -> Arc<Mutex<Database>> {
        Arc::new(Mutex::new(Database::create(path).unwrap()))
    }

    fn data_entry(term: u64, index: u64, cmd: &ConsensusCommand) -> Entry {
        Entry {
            term,
            index,
            data: postcard::to_allocvec(cmd).unwrap(),
            ..Default::default()
        }
    }

    fn blank_entry(term: u64, index: u64) -> Entry {
        Entry {
            term,
            index,
            data: Vec::new(),
            ..Default::default()
        }
    }

    /// The `last_applied` watermark written by `persist_applied` survives a
    /// `Database` close and reopen.
    #[test]
    fn test_persist_applied_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("meta_persist_test.redb");

        {
            let sm = AppStateMachine::open(make_db(&db_path)).unwrap();
            let cmd = ConsensusCommand::AckClaim {
                claim_id: uuid::Uuid::nil(),
                instance_id: uuid::Uuid::nil(),
            };
            sm.apply_batch(&[data_entry(2, 10, &cmd)]).unwrap();
            sm.persist_applied().unwrap();
        }

        let sm2 = AppStateMachine::open(make_db(&db_path)).unwrap();
        assert_eq!(
            sm2.last_applied(),
            Some((2, 10)),
            "last_applied must persist across reopen"
        );
    }

    /// Applying a data entry advances the watermark and produces the row in
    /// the application redb file.
    #[test]
    fn test_apply_batch_advances_last_applied_and_returns_response() {
        use crate::distributed::consensus::state_machine::read_master_batch;

        let dir = TempDir::new().unwrap();
        let app_db = make_db(&dir.path().join("sm_apply_app.redb"));
        let sm = AppStateMachine::open(Arc::clone(&app_db)).unwrap();
        assert_eq!(sm.last_applied(), None, "fresh SM: no watermark");

        let cmd = ConsensusCommand::RegisterMasterBatch {
            batch_id: 42,
            component: "task3".to_string(),
            schema_id: 1,
            ipc_bytes: vec![0u8; 32],
            total_rows: 10,
            now_at_propose: 0,
        };
        let responses = sm.apply_batch(&[data_entry(1, 5, &cmd)]).unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].0, 5);
        assert_eq!(sm.last_applied(), Some((1, 5)));

        let db = app_db.lock().unwrap();
        let record = read_master_batch(&db, 42)
            .unwrap()
            .expect("master batch 42 must exist after apply");
        assert_eq!(record.component, "task3");
        assert_eq!(record.total_rows, 10);
    }

    /// Blank entries advance the watermark but produce no response, and a
    /// data entry after them still applies with its own response.
    #[test]
    fn test_apply_batch_skips_blank_entries() {
        let dir = TempDir::new().unwrap();
        let sm = AppStateMachine::open(make_db(&dir.path().join("blank_test.redb"))).unwrap();

        // Heartbeat is a no-op that succeeds for any instance.
        let cmd = ConsensusCommand::Heartbeat {
            instance_id: uuid::Uuid::nil(),
            at: 0,
        };
        let responses = sm
            .apply_batch(&[blank_entry(1, 1), data_entry(1, 2, &cmd)])
            .unwrap();
        assert_eq!(
            responses.len(),
            1,
            "blank entries yield no response, data entries one each"
        );
        assert_eq!(responses[0].0, 2);
        assert_eq!(sm.last_applied(), Some((1, 2)));
    }

    /// An undecodable entry (corrupt `data`) propagates out of `apply_batch`
    /// and leaves the watermark at its pre-apply value.
    #[test]
    fn test_apply_batch_err_halts_batch() {
        let dir = TempDir::new().unwrap();
        let sm = AppStateMachine::open(make_db(&dir.path().join("sm_test.redb"))).unwrap();
        assert_eq!(sm.last_applied(), None);

        let mut bad = blank_entry(1, 5);
        bad.data = vec![0xDE, 0xAD, 0xBE, 0xEF]; // not a postcard ConsensusCommand
        let result = sm.apply_batch(&[bad]);
        assert!(result.is_err(), "apply must propagate decode errors");
        assert_eq!(
            sm.last_applied(),
            None,
            "watermark must not advance on a failed apply"
        );
    }

    /// A restart restores the watermark from disk.
    #[test]
    fn test_restart_restores_watermark() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("restart_test_app.redb");

        let index = {
            let sm = AppStateMachine::open(make_db(&db_path)).unwrap();
            let cmd = ConsensusCommand::RegisterMasterBatch {
                batch_id: 7,
                component: "restart_comp".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 32],
                total_rows: 5,
                now_at_propose: 0,
            };
            sm.apply_batch(&[data_entry(2, 10, &cmd)]).unwrap();
            sm.persist_applied().unwrap();
            sm.last_applied().unwrap().1
        };

        let sm2 = AppStateMachine::open(make_db(&db_path)).unwrap();
        assert_eq!(sm2.last_applied().map(|(_, i)| i), Some(index));
    }
}
