//! Raft log storage in a dedicated redb file.
//!
//! Holds [`RaftRedbLogStore`], the sync [`raft::Storage`] implementation that
//! backs [`RawNode`](raft::RawNode). Raft metadata (hard state, conf state),
//! log entries and the latest snapshot only, no Arrow data. The driver calls
//! every method through [`tokio::task::spawn_blocking`]; the parent module doc
//! explains why.

use std::path::Path;
use std::sync::Arc;

use prost::Message as _;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::storage::{GetEntriesContext, RaftState};
use raft::{Error, Result as RaftResult, Storage, StorageError};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::{dec, enc};
use crate::error::{PcsError, PcsResult};

const HARD_STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_hard_state");
const CONF_STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_conf_state");
const ENTRIES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_entries");
const SNAPSHOT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_snapshot");

const KEY_HARD_STATE: &str = "hard_state";
const KEY_CONF_STATE: &str = "conf_state";
const KEY_SNAPSHOT: &str = "snapshot";

/// One stored snapshot: the PCS snapshot bytes plus the raft bookkeeping
/// (index, term, conf state) the log needs to serve [`Storage::snapshot`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotRecord {
    index: u64,
    term: u64,
    voters: Vec<u64>,
    learners: Vec<u64>,
    data: Vec<u8>,
}

impl From<&Snapshot> for SnapshotRecord {
    fn from(s: &Snapshot) -> Self {
        let meta = s.metadata.as_ref();
        let conf = meta.and_then(|m| m.conf_state.as_ref());
        SnapshotRecord {
            index: meta.map_or(0, |m| m.index),
            term: meta.map_or(0, |m| m.term),
            voters: conf.map(|c| c.voters.clone()).unwrap_or_default(),
            learners: conf.map(|c| c.learners.clone()).unwrap_or_default(),
            data: s.data.clone(),
        }
    }
}

impl From<SnapshotRecord> for Snapshot {
    fn from(r: SnapshotRecord) -> Self {
        Snapshot {
            data: r.data,
            metadata: Some(SnapshotMetadata {
                index: r.index,
                term: r.term,
                conf_state: Some(ConfState {
                    voters: r.voters,
                    learners: r.learners,
                    ..Default::default()
                }),
            }),
        }
    }
}

/// Persistent Raft log storage in a dedicated redb file.
///
/// Contains only Raft metadata and log entries; no Arrow data.
///
/// Cloning is cheap (an `Arc` bump). All redb I/O runs synchronously; the
/// driver wraps every call in [`tokio::task::spawn_blocking`].
#[derive(Clone)]
pub struct RaftRedbLogStore {
    db: Arc<Database>,
}

fn to_raft_err(e: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Store(StorageError::Other(Box::new(e)))
}

impl RaftRedbLogStore {
    /// Open (or create) the log store at `path`.
    ///
    /// Initial table creation runs synchronously on the caller thread
    /// because this is a one-shot open path, not a hot trait method. The
    /// `fsync` cost at open time is acceptable.
    pub fn open(path: impl AsRef<Path>) -> PcsResult<Self> {
        let db = Database::create(path.as_ref())
            .map_err(|e| PcsError::store(format!("open arrow log redb: {e}")))?;
        {
            let txn = db
                .begin_write()
                .map_err(|e| PcsError::store(e.to_string()))?;
            txn.open_table(HARD_STATE_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            txn.open_table(CONF_STATE_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            txn.open_table(ENTRIES_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            txn.open_table(SNAPSHOT_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            txn.commit().map_err(|e| PcsError::store(e.to_string()))?;
        }
        Ok(Self { db: Arc::new(db) })
    }

    /// Persist the hard state (term / vote / commit). One row, overwritten.
    pub fn persist_hard_state(&self, hs: &HardState) -> PcsResult<()> {
        let bytes = hs.encode_to_vec();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| PcsError::store(e.to_string()))?;
        {
            let mut table = txn
                .open_table(HARD_STATE_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            table
                .insert(KEY_HARD_STATE, bytes.as_slice())
                .map_err(|e| PcsError::store(e.to_string()))?;
        }
        txn.commit().map_err(|e| PcsError::store(e.to_string()))?;
        Ok(())
    }

    /// Persist the conf state (static membership, written once on first boot).
    pub fn persist_conf_state(&self, cs: &ConfState) -> PcsResult<()> {
        let bytes = cs.encode_to_vec();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| PcsError::store(e.to_string()))?;
        {
            let mut table = txn
                .open_table(CONF_STATE_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            table
                .insert(KEY_CONF_STATE, bytes.as_slice())
                .map_err(|e| PcsError::store(e.to_string()))?;
        }
        txn.commit().map_err(|e| PcsError::store(e.to_string()))?;
        Ok(())
    }

    /// Persist log entries; returns the number stored.
    ///
    /// Entries are prost-encoded `eraftpb::Entry` under big-endian u64 index
    /// keys, so a later snapshot compaction can delete a prefix range.
    pub fn append_entries(&self, entries: &[Entry]) -> PcsResult<usize> {
        let encoded: Vec<(u64, Vec<u8>)> = entries
            .iter()
            .map(|e| (e.index, e.encode_to_vec()))
            .collect();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| PcsError::store(e.to_string()))?;
        {
            let mut table = txn
                .open_table(ENTRIES_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            for (idx, bytes) in &encoded {
                table
                    .insert(*idx, bytes.as_slice())
                    .map_err(|e| PcsError::store(e.to_string()))?;
            }
        }
        txn.commit().map_err(|e| PcsError::store(e.to_string()))?;
        Ok(encoded.len())
    }

    /// Read the persisted hard state, or an empty one when absent.
    pub fn read_hard_state(&self) -> PcsResult<HardState> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| PcsError::store(e.to_string()))?;
        let table = txn
            .open_table(HARD_STATE_TABLE)
            .map_err(|e| PcsError::store(e.to_string()))?;
        match table
            .get(KEY_HARD_STATE)
            .map_err(|e| PcsError::store(e.to_string()))?
        {
            Some(v) => HardState::decode(v.value()).map_err(|e| PcsError::store(e.to_string())),
            None => Ok(HardState::default()),
        }
    }

    /// Read the persisted conf state, or an empty one when absent.
    pub fn read_conf_state(&self) -> PcsResult<ConfState> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| PcsError::store(e.to_string()))?;
        let table = txn
            .open_table(CONF_STATE_TABLE)
            .map_err(|e| PcsError::store(e.to_string()))?;
        match table
            .get(KEY_CONF_STATE)
            .map_err(|e| PcsError::store(e.to_string()))?
        {
            Some(v) => ConfState::decode(v.value()).map_err(|e| PcsError::store(e.to_string())),
            None => Ok(ConfState::default()),
        }
    }

    /// Compact the log: delete entries up to `index` and persist `snapshot` as
    /// the new base. One write transaction, one fsync.
    pub fn compact_to(&self, index: u64, snapshot: &Snapshot) -> PcsResult<()> {
        let record = SnapshotRecord::from(snapshot);
        let snap_bytes =
            enc(&record).map_err(|e| PcsError::store(format!("encode snapshot: {e}")))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| PcsError::store(e.to_string()))?;
        {
            let mut snap_table = txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            snap_table
                .insert(KEY_SNAPSHOT, snap_bytes.as_slice())
                .map_err(|e| PcsError::store(e.to_string()))?;
            let mut entries = txn
                .open_table(ENTRIES_TABLE)
                .map_err(|e| PcsError::store(e.to_string()))?;
            let to_remove: Vec<u64> = entries
                .range(..=index)
                .map_err(|e| PcsError::store(e.to_string()))?
                .map(|r| {
                    r.map(|(k, _)| k.value())
                        .map_err(|e| PcsError::store(e.to_string()))
                })
                .collect::<PcsResult<_>>()?;
            for idx in to_remove {
                entries
                    .remove(idx)
                    .map_err(|e| PcsError::store(e.to_string()))?;
            }
        }
        txn.commit().map_err(|e| PcsError::store(e.to_string()))?;
        Ok(())
    }

    /// Read the latest snapshot, if any.
    pub fn read_snapshot(&self) -> PcsResult<Option<Snapshot>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| PcsError::store(e.to_string()))?;
        let table = txn
            .open_table(SNAPSHOT_TABLE)
            .map_err(|e| PcsError::store(e.to_string()))?;
        match table
            .get(KEY_SNAPSHOT)
            .map_err(|e| PcsError::store(e.to_string()))?
        {
            Some(v) => {
                let record: SnapshotRecord =
                    dec(v.value()).map_err(|e| PcsError::store(e.to_string()))?;
                Ok(Some(record.into()))
            }
            None => Ok(None),
        }
    }

    /// The index of the first retained log entry: the snapshot index plus one,
    /// or 1 when no snapshot exists.
    pub fn first_index(&self) -> PcsResult<u64> {
        Ok(match self.read_snapshot()? {
            Some(s) => s.metadata.as_ref().map_or(1, |m| m.index + 1),
            None => 1,
        })
    }

    /// The index of the last retained log entry, or `first_index - 1` when the
    /// log is empty.
    pub fn last_index(&self) -> PcsResult<u64> {
        let first = self.first_index()?;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| PcsError::store(e.to_string()))?;
        let table = txn
            .open_table(ENTRIES_TABLE)
            .map_err(|e| PcsError::store(e.to_string()))?;
        Ok(
            match table.last().map_err(|e| PcsError::store(e.to_string()))? {
                Some((k, _)) => k.value(),
                None => first.saturating_sub(1),
            },
        )
    }
}

impl Storage for RaftRedbLogStore {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let hard_state = self.read_hard_state().map_err(to_raft_err)?;
        let conf_state = self.read_conf_state().map_err(to_raft_err)?;
        Ok(RaftState {
            hard_state,
            conf_state,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _ctx: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        if low < self.first_index().map_err(to_raft_err)? {
            return Err(Error::Store(StorageError::Compacted));
        }
        if high > self.last_index().map_err(to_raft_err)? + 1 {
            return Err(Error::Store(StorageError::Unavailable));
        }
        let max_size = max_size.into();
        let txn = self.db.begin_read().map_err(to_raft_err)?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(to_raft_err)?;
        let mut out = Vec::new();
        let mut size: u64 = 0;
        for item in table.range(low..high).map_err(to_raft_err)? {
            let (_k, v) = item.map_err(to_raft_err)?;
            let entry = Entry::decode(v.value()).map_err(to_raft_err)?;
            // Match raft's own limit_size semantics: always include the first
            // entry, then stop once the cumulative encoded size passes max.
            let entry_size = entry.encoded_len() as u64;
            if !out.is_empty() && size + entry_size > max_size.unwrap_or(u64::MAX) {
                break;
            }
            size += entry_size;
            out.push(entry);
        }
        Ok(out)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let first = self.first_index().map_err(to_raft_err)?;
        if idx == first.saturating_sub(1) {
            // The term of the entry before first_index is retained for matching.
            return Ok(self
                .read_snapshot()
                .map_err(to_raft_err)?
                .and_then(|s| s.metadata)
                .map_or(0, |m| m.term));
        }
        let txn = self.db.begin_read().map_err(to_raft_err)?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(to_raft_err)?;
        match table.get(idx).map_err(to_raft_err)? {
            Some(v) => Ok(Entry::decode(v.value()).map_err(to_raft_err)?.term),
            None => Err(Error::Store(StorageError::Unavailable)),
        }
    }

    fn first_index(&self) -> RaftResult<u64> {
        Self::first_index(self).map_err(to_raft_err)
    }

    fn last_index(&self) -> RaftResult<u64> {
        Self::last_index(self).map_err(to_raft_err)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        match self.read_snapshot().map_err(to_raft_err)? {
            Some(s) => {
                let idx = s.metadata.as_ref().map_or(0, |m| m.index);
                if idx >= request_index {
                    Ok(s)
                } else {
                    Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable))
                }
            }
            None => Ok(Snapshot::default()),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::super::tests::{blank_entry, make_store};
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_empty_state() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        assert_eq!(store.first_index().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 0);
        let state = store.initial_state().unwrap();
        assert_eq!(state.hard_state, HardState::default());
        assert_eq!(state.conf_state, ConfState::default());
    }

    #[test]
    fn test_append_and_read_entries() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let n = store
            .append_entries(&[blank_entry(1), blank_entry(2), blank_entry(3)])
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(store.last_index().unwrap(), 3);
        let ents = store
            .entries(1, 4, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(ents.len(), 3);
        assert_eq!(ents[0].index, 1);
        assert_eq!(store.term(1).unwrap(), 1);
        assert!(
            store.term(99).is_err(),
            "term outside the log is unavailable"
        );
    }

    #[test]
    fn test_hard_state_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let hs = HardState {
            term: 2,
            vote: 3,
            commit: 5,
        };
        store.persist_hard_state(&hs).unwrap();
        assert_eq!(store.read_hard_state().unwrap(), hs);
    }

    #[test]
    fn test_conf_state_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let cs = ConfState {
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        store.persist_conf_state(&cs).unwrap();
        assert_eq!(store.read_conf_state().unwrap(), cs);
        assert_eq!(store.initial_state().unwrap().conf_state, cs);
    }

    #[test]
    fn test_compact_and_snapshot() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        store
            .append_entries(&[blank_entry(1), blank_entry(2)])
            .unwrap();
        let snap = Snapshot {
            data: b"pcs-snapshot".to_vec(),
            metadata: Some(SnapshotMetadata {
                index: 2,
                term: 1,
                conf_state: Some(ConfState {
                    voters: vec![1],
                    ..Default::default()
                }),
            }),
        };
        store.compact_to(2, &snap).unwrap();
        assert_eq!(store.first_index().unwrap(), 3);
        assert_eq!(store.last_index().unwrap(), 2);
        // Entries at or below the snapshot index are gone.
        assert!(
            store
                .entries(1, 2, None, GetEntriesContext::empty(false))
                .is_err(),
            "compacted entries must be unavailable"
        );
        // The term of first_index - 1 comes from the snapshot.
        assert_eq!(store.term(2).unwrap(), 1);
        let served = store.snapshot(0, 0).unwrap();
        assert_eq!(served.data, b"pcs-snapshot");
    }
}
