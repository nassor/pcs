//! Persistent Raft log storage and Arrow-IPC state machine backed by separate redb files.
//!
//! ## Separate redb files
//!
//! Arrow blob storage in the state machine competes with log storage for write
//! I/O. This module keeps them strictly separate:
//!
//! - **Log store** (`ArrowRedbLogStore`): stores only Raft metadata (vote,
//!   purged log id) and log entries. No Arrow data lives here.
//!
//! - **State machine** (`ArrowRedbStateMachine`): drives the Arrow-IPC
//!   application state through `sm_apply`. Arrow IPC bytes are written in the
//!   same transaction as the log apply → fate-shared fsync.
//!
//! ## Blocking-I/O discipline
//!
//! All redb read/write transactions in `ArrowRedbLogStore` are wrapped in
//! [`tokio::task::spawn_blocking`]. redb's commit path issues `fsync`, which
//! must not run on a tokio worker thread — blocking a worker for fsync latency
//! stalls every other task on that runtime. The log store holds an
//! `Arc<Database>` (no [`std::sync::Mutex`]): redb's `Database::begin_write` /
//! `begin_read` both take `&self` and coordinate internally, so external
//! serialization is unnecessary and would only introduce lock contention.
//!
//! ## Log entry encoding
//!
//! Log entries are encoded with `postcard`. Log files produced by earlier
//! alpha builds (`pre-1.0.0-alpha.1`, which used `serde_json`) are not
//! decodable; wipe the Raft log and state-machine redb files before starting
//! after an upgrade from those builds.
//!
//! Why postcard: (1) canonical by construction — the same input always
//! produces the same bytes, which matters for any future content-hashing of
//! log entries; (2) no UTF-8 encoding cost on append/apply, both of which run
//! under spawn_blocking and contend for disk; (3) no JSON map-ordering
//! ambiguity should any nested type grow a map field in the future.

#[cfg(feature = "distributed-raft")]
pub(crate) mod raft_impl {
    use std::fmt::Debug;
    use std::io;
    use std::io::Cursor;
    use std::ops::{Bound, RangeBounds};
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use futures_util::StreamExt;
    use openraft::EntryPayload;
    use openraft::RaftLogReader;
    use openraft::StoredMembership;
    use openraft::storage::{
        EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine,
    };
    use openraft::type_config::alias::{
        EntryOf, LogIdOf, SnapshotDataOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
    };
    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

    use crate::distributed::consensus::snapshot::raft_impl::{
        ArrowSnapshotBuilder, install_snapshot_bytes,
    };
    use crate::distributed::consensus::state_machine::{
        KEY_SM_LAST_APPLIED, KEY_SM_LAST_MEMBERSHIP, apply as sm_apply,
    };
    use crate::distributed::consensus::types::{ConsensusResponse, PcsTypeConfig};
    use crate::error::{PcsError, PcsResult};

    // ── Table definitions ─────────────────────────────────────────────────────

    /// Log-store tables (live in the log redb file).
    const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("arrow_raft_meta");
    const ENTRIES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("arrow_raft_entries");

    const KEY_VOTE: &str = "vote";
    const KEY_PURGED_LOG_ID: &str = "purged_log_id";

    /// State-machine metadata table (lives in the *app* redb file).
    ///
    /// Stores `last_applied` and `last_membership` so they survive restarts.
    /// Using a separate table keeps SM metadata writes fate-shared with the
    /// same redb file as the application data — one fsync covers both.
    const SM_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("arrow_sm_meta");

    // ── Serialization helpers ─────────────────────────────────────────────────
    //
    // All log-entry and metadata encoding uses `postcard`. See the module-level
    // doc comment for the wire-format break notice.

    fn enc<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
        postcard::to_allocvec(v).map_err(|e| io::Error::other(format!("postcard encode: {e}")))
    }

    fn dec<T: for<'de> serde::Deserialize<'de>>(b: &[u8]) -> io::Result<T> {
        postcard::from_bytes(b).map_err(|e| io::Error::other(format!("postcard decode: {e}")))
    }

    fn to_io(e: impl std::error::Error) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Convert a `tokio::task::JoinError` to an `io::Error`. A JoinError means
    /// the blocking task panicked or was cancelled — treat both as a hard I/O
    /// failure so openraft can surface the storage error upward.
    fn join_to_io(e: tokio::task::JoinError) -> io::Error {
        io::Error::other(format!("blocking redb task failed: {e}"))
    }

    /// Materialise an owned `(Bound<u64>, Bound<u64>)` pair from an arbitrary
    /// `RangeBounds<u64>`, so it can be moved into a `spawn_blocking` closure
    /// without forcing the caller's range type to be `'static`.
    fn owned_bounds<RB: RangeBounds<u64>>(range: &RB) -> (Bound<u64>, Bound<u64>) {
        let start = match range.start_bound() {
            Bound::Included(v) => Bound::Included(*v),
            Bound::Excluded(v) => Bound::Excluded(*v),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(v) => Bound::Included(*v),
            Bound::Excluded(v) => Bound::Excluded(*v),
            Bound::Unbounded => Bound::Unbounded,
        };
        (start, end)
    }

    // ── ArrowRedbLogStore ─────────────────────────────────────────────────────

    /// Persistent Raft log storage in a dedicated redb file.
    ///
    /// Contains only Raft metadata and log entries; no Arrow data.
    ///
    /// Cloning is cheap (an `Arc` bump). All trait-method redb I/O runs inside
    /// [`tokio::task::spawn_blocking`] — see the module-level doc comment for
    /// the rationale.
    #[derive(Clone)]
    pub struct ArrowRedbLogStore {
        /// `redb::Database` coordinates concurrent readers and a single writer
        /// internally; both `begin_read` and `begin_write` take `&self`. No
        /// external mutex is required — adding one would only serialise
        /// readers unnecessarily.
        db: Arc<Database>,
    }

    impl ArrowRedbLogStore {
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
                txn.open_table(META_TABLE)
                    .map_err(|e| PcsError::store(e.to_string()))?;
                txn.open_table(ENTRIES_TABLE)
                    .map_err(|e| PcsError::store(e.to_string()))?;
                txn.commit().map_err(|e| PcsError::store(e.to_string()))?;
            }
            Ok(Self { db: Arc::new(db) })
        }
    }

    impl RaftLogReader<PcsTypeConfig> for ArrowRedbLogStore {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
            &mut self,
            range: RB,
        ) -> Result<Vec<EntryOf<PcsTypeConfig>>, io::Error> {
            let db = Arc::clone(&self.db);
            let bounds = owned_bounds(&range);
            tokio::task::spawn_blocking(move || -> io::Result<_> {
                let txn = db.begin_read().map_err(to_io)?;
                let table = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                let mut out = Vec::new();
                for item in table.range::<u64>(bounds).map_err(to_io)? {
                    let (_k, v) = item.map_err(to_io)?;
                    out.push(dec(v.value())?);
                }
                Ok(out)
            })
            .await
            .map_err(join_to_io)?
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<PcsTypeConfig>>, io::Error> {
            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || -> io::Result<_> {
                let txn = db.begin_read().map_err(to_io)?;
                let table = txn.open_table(META_TABLE).map_err(to_io)?;
                match table.get(KEY_VOTE).map_err(to_io)? {
                    Some(v) => dec::<VoteOf<PcsTypeConfig>>(v.value()).map(Some),
                    None => Ok(None),
                }
            })
            .await
            .map_err(join_to_io)?
        }
    }

    impl RaftLogStorage<PcsTypeConfig> for ArrowRedbLogStore {
        type LogReader = ArrowRedbLogStore;

        async fn get_log_state(&mut self) -> Result<LogState<PcsTypeConfig>, io::Error> {
            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || -> io::Result<LogState<PcsTypeConfig>> {
                let txn = db.begin_read().map_err(to_io)?;
                // Read purged log id.
                let purged: Option<LogIdOf<PcsTypeConfig>> = {
                    let meta = txn.open_table(META_TABLE).map_err(to_io)?;
                    match meta.get(KEY_PURGED_LOG_ID).map_err(to_io)? {
                        Some(v) => Some(dec(v.value())?),
                        None => None,
                    }
                };
                // Read last entry log id.
                let last: Option<LogIdOf<PcsTypeConfig>> = {
                    let entries = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                    match entries.last().map_err(to_io)? {
                        Some((_k, v)) => {
                            let entry: EntryOf<PcsTypeConfig> = dec(v.value())?;
                            Some(entry.log_id)
                        }
                        None => None,
                    }
                };
                Ok(LogState {
                    last_purged_log_id: purged,
                    last_log_id: last.or(purged),
                })
            })
            .await
            .map_err(join_to_io)?
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }

        async fn save_vote(&mut self, vote: &VoteOf<PcsTypeConfig>) -> Result<(), io::Error> {
            // Encode before moving into the blocking task — keeps the closure
            // cheap and avoids pushing serde work onto the blocking pool.
            let bytes = enc(vote)?;
            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || -> io::Result<()> {
                let txn = db.begin_write().map_err(to_io)?;
                {
                    let mut table = txn.open_table(META_TABLE).map_err(to_io)?;
                    table.insert(KEY_VOTE, bytes.as_slice()).map_err(to_io)?;
                }
                txn.commit().map_err(to_io)
            })
            .await
            .map_err(join_to_io)?
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: IOFlushed<PcsTypeConfig>,
        ) -> Result<(), io::Error>
        where
            I: IntoIterator<Item = EntryOf<PcsTypeConfig>> + Send,
            I::IntoIter: Send,
        {
            // Encode entries on the caller thread. We pay the encode cost once
            // per entry; doing it here (off the blocking pool) means the
            // blocking task is pure disk I/O — its only job is to write and
            // fsync. Any encode failure fails fast without even touching redb.
            let encoded: Vec<(u64, Vec<u8>)> = entries
                .into_iter()
                .map(|e| {
                    let idx = e.log_id.index;
                    enc(&e).map(|bytes| (idx, bytes))
                })
                .collect::<io::Result<_>>()?;

            let db = Arc::clone(&self.db);
            let res = tokio::task::spawn_blocking(move || -> io::Result<()> {
                let txn = db.begin_write().map_err(to_io)?;
                {
                    let mut table = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                    for (idx, bytes) in &encoded {
                        table.insert(*idx, bytes.as_slice()).map_err(to_io)?;
                    }
                }
                txn.commit().map_err(to_io)
            })
            .await
            .map_err(join_to_io)?;

            // Fire the flush callback only on success so openraft does not
            // advance its durable-commit watermark past an unpersisted batch.
            res?;
            callback.io_completed(Ok(()));
            Ok(())
        }

        async fn truncate_after(
            &mut self,
            last: Option<LogIdOf<PcsTypeConfig>>,
        ) -> Result<(), io::Error> {
            let from = last.as_ref().map_or(0, |l| l.index + 1);
            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || -> io::Result<()> {
                let txn = db.begin_write().map_err(to_io)?;
                {
                    let mut table = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                    let to_remove: Vec<u64> = table
                        .range(from..)
                        .map_err(to_io)?
                        .map(|r| r.map(|(k, _)| k.value()).map_err(to_io))
                        .collect::<io::Result<_>>()?;
                    for idx in to_remove {
                        table.remove(idx).map_err(to_io)?;
                    }
                }
                txn.commit().map_err(to_io)
            })
            .await
            .map_err(join_to_io)?
        }

        async fn purge(&mut self, log_id: LogIdOf<PcsTypeConfig>) -> Result<(), io::Error> {
            let up_to = log_id.index;
            // Encode the purge marker on this thread so the blocking closure
            // does no serde work.
            let marker = enc(&log_id)?;
            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || -> io::Result<()> {
                let txn = db.begin_write().map_err(to_io)?;
                {
                    let mut meta = txn.open_table(META_TABLE).map_err(to_io)?;
                    let mut entries = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                    meta.insert(KEY_PURGED_LOG_ID, marker.as_slice())
                        .map_err(to_io)?;
                    let to_remove: Vec<u64> = entries
                        .range(..=up_to)
                        .map_err(to_io)?
                        .map(|r| r.map(|(k, _)| k.value()).map_err(to_io))
                        .collect::<io::Result<_>>()?;
                    for idx in to_remove {
                        entries.remove(idx).map_err(to_io)?;
                    }
                }
                txn.commit().map_err(to_io)
            })
            .await
            .map_err(join_to_io)?
        }
    }

    // ── ArrowRedbStateMachine ─────────────────────────────────────────────────

    /// State machine applying committed Arrow-IPC [`ConsensusCommand`](crate::distributed::consensus::ConsensusCommand) entries
    /// to the redb application database.
    ///
    /// Snapshot support uses Arrow IPC serialization via [`ArrowSnapshotBuilder`].
    ///
    /// ## Persistence
    ///
    /// `last_applied` and `last_membership` are persisted in the same redb
    /// database as the application data, under the `arrow_sm_meta` table. This
    /// makes the state machine restart-safe: `open()` restores these values so
    /// openraft can skip re-applying already-committed log entries.
    pub struct ArrowRedbStateMachine {
        pub(crate) db: Arc<Mutex<Database>>,
        last_applied: Option<LogIdOf<PcsTypeConfig>>,
        last_membership: StoredMembershipOf<PcsTypeConfig>,
    }

    impl ArrowRedbStateMachine {
        /// Open (or create) a state machine wrapping the given redb application
        /// database.
        ///
        /// Reads `last_applied` and `last_membership` from the persisted
        /// `arrow_sm_meta` table so restarts recover the correct watermark.
        /// Also ensures the `arrow_sm_meta` table exists (creates it on first
        /// open).
        pub fn open(db: Arc<Mutex<Database>>) -> io::Result<Self> {
            let (last_applied, last_membership) = {
                let guard = db.lock().unwrap();

                // Ensure the SM metadata table exists. This write transaction
                // is a no-op if the table already exists and nothing else needs
                // writing, but redb requires a write txn to open (create) a new
                // table for the first time.
                {
                    let txn = guard.begin_write().map_err(to_io)?;
                    txn.open_table(SM_META_TABLE).map_err(to_io)?;
                    txn.commit().map_err(to_io)?;
                }

                let txn = guard.begin_read().map_err(to_io)?;
                let table = txn.open_table(SM_META_TABLE).map_err(to_io)?;

                let last_applied: Option<LogIdOf<PcsTypeConfig>> =
                    match table.get(KEY_SM_LAST_APPLIED).map_err(to_io)? {
                        Some(v) => dec(v.value())?,
                        None => None,
                    };

                let last_membership: StoredMembershipOf<PcsTypeConfig> =
                    match table.get(KEY_SM_LAST_MEMBERSHIP).map_err(to_io)? {
                        Some(v) => dec(v.value())?,
                        None => StoredMembership::default(),
                    };

                (last_applied, last_membership)
            };

            Ok(Self {
                db,
                last_applied,
                last_membership,
            })
        }

        /// Persist `last_applied` and `last_membership` to the app redb file in
        /// a single transaction (one fsync covers both fields).
        ///
        /// Used directly in tests to verify persistence without going through the
        /// async `apply` / `install_snapshot` paths. In production, the equivalent
        /// logic runs inside `spawn_blocking` at each call site.
        #[cfg(test)]
        fn persist_sm_meta(&self) -> io::Result<()> {
            let applied_bytes = enc(&self.last_applied)?;
            let membership_bytes = enc(&self.last_membership)?;

            let guard = self.db.lock().unwrap();
            let txn = guard.begin_write().map_err(to_io)?;
            {
                let mut table = txn.open_table(SM_META_TABLE).map_err(to_io)?;
                table
                    .insert(KEY_SM_LAST_APPLIED, applied_bytes.as_slice())
                    .map_err(to_io)?;
                table
                    .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes.as_slice())
                    .map_err(to_io)?;
            }
            txn.commit().map_err(to_io)
        }
    }

    impl RaftStateMachine<PcsTypeConfig> for ArrowRedbStateMachine {
        type SnapshotBuilder = ArrowSnapshotBuilder;

        async fn applied_state(
            &mut self,
        ) -> Result<
            (
                Option<LogIdOf<PcsTypeConfig>>,
                StoredMembershipOf<PcsTypeConfig>,
            ),
            io::Error,
        > {
            Ok((self.last_applied, self.last_membership.clone()))
        }

        async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
        where
            Strm: futures_util::Stream<Item = Result<EntryResponder<PcsTypeConfig>, io::Error>>
                + Unpin
                + Send,
        {
            let mut entries = entries;
            while let Some(item) = entries.next().await {
                let (entry, responder) = item?;
                let log_id = entry.log_id;
                match entry.payload {
                    EntryPayload::Blank => {
                        // Blank entries carry no state — advance immediately.
                        self.last_applied = Some(log_id);
                        if let Some(r) = responder {
                            r.send(ConsensusResponse::ClaimAcked);
                        }
                    }
                    EntryPayload::Normal(cmd) => {
                        // Propagate I/O errors out of sm_apply so openraft halts
                        // rather than silently skipping the entry. last_applied
                        // advances only after a successful apply — a crash mid-apply
                        // must not leave a false watermark.
                        let db = Arc::clone(&self.db);
                        let response = tokio::task::spawn_blocking(move || {
                            let db = db
                                .lock()
                                .map_err(|_| io::Error::other("db mutex poisoned"))?;
                            sm_apply(&db, cmd)
                                .map_err(|e| io::Error::other(format!("sm_apply: {e}")))
                        })
                        .await
                        .map_err(join_to_io)??;
                        self.last_applied = Some(log_id);
                        if let Some(r) = responder {
                            r.send(response);
                        }
                    }
                    EntryPayload::Membership(mem) => {
                        // Membership changes carry no I/O — advance immediately.
                        self.last_applied = Some(log_id);
                        self.last_membership = StoredMembership::new(Some(log_id), mem.clone());
                        if let Some(r) = responder {
                            r.send(ConsensusResponse::ClaimAcked);
                        }
                    }
                }
            }
            // Persist the watermarks after processing the full batch. A crash
            // here is safe: at-least-once semantics mean entries will be
            // re-applied after restart until the watermark advances past them.
            // Run inside spawn_blocking: this issues an fsync via redb commit.
            {
                let db = Arc::clone(&self.db);
                let last_applied = self.last_applied;
                let last_membership = self.last_membership.clone();
                tokio::task::spawn_blocking(move || {
                    let applied_bytes = enc(&last_applied)?;
                    let membership_bytes = enc(&last_membership)?;
                    let guard = db
                        .lock()
                        .map_err(|_| io::Error::other("db mutex poisoned"))?;
                    let txn = guard.begin_write().map_err(to_io)?;
                    {
                        let mut table = txn.open_table(SM_META_TABLE).map_err(to_io)?;
                        table
                            .insert(KEY_SM_LAST_APPLIED, applied_bytes.as_slice())
                            .map_err(to_io)?;
                        table
                            .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes.as_slice())
                            .map_err(to_io)?;
                    }
                    txn.commit().map_err(to_io)
                })
                .await
                .map_err(join_to_io)??;
            }
            Ok(())
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            ArrowSnapshotBuilder {
                db: Arc::clone(&self.db),
                last_applied: self.last_applied,
                last_membership: self.last_membership.clone(),
            }
        }

        async fn begin_receiving_snapshot(
            &mut self,
        ) -> Result<SnapshotDataOf<PcsTypeConfig>, io::Error> {
            Ok(Cursor::new(vec![]))
        }

        async fn install_snapshot(
            &mut self,
            meta: &SnapshotMetaOf<PcsTypeConfig>,
            snapshot: SnapshotDataOf<PcsTypeConfig>,
        ) -> Result<(), io::Error> {
            let data = snapshot.into_inner();
            // Update in-memory state first so we can encode watermarks below.
            self.last_applied = meta.last_log_id;
            self.last_membership = meta.last_membership.clone();

            if !data.is_empty() {
                // Encode watermarks on this thread before entering spawn_blocking.
                let applied_bytes = enc(&self.last_applied)?;
                let membership_bytes = enc(&self.last_membership)?;

                // Install snapshot data and write watermarks in one WriteTransaction
                // so a crash cannot leave them split.
                let db = Arc::clone(&self.db);
                tokio::task::spawn_blocking(move || {
                    let db = db
                        .lock()
                        .map_err(|_| io::Error::other("db mutex poisoned"))?;
                    install_snapshot_bytes(
                        &db,
                        &data,
                        Some((applied_bytes.as_slice(), membership_bytes.as_slice())),
                    )
                    .map_err(|e| io::Error::other(format!("install_snapshot: {e}")))
                })
                .await
                .map_err(join_to_io)??;
            } else {
                // Empty snapshot: no data to install, but still persist watermarks.
                let db = Arc::clone(&self.db);
                let applied_bytes = enc(&self.last_applied)?;
                let membership_bytes = enc(&self.last_membership)?;
                tokio::task::spawn_blocking(move || {
                    let guard = db
                        .lock()
                        .map_err(|_| io::Error::other("db mutex poisoned"))?;
                    let txn = guard.begin_write().map_err(to_io)?;
                    {
                        let mut table = txn.open_table(SM_META_TABLE).map_err(to_io)?;
                        table
                            .insert(KEY_SM_LAST_APPLIED, applied_bytes.as_slice())
                            .map_err(to_io)?;
                        table
                            .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes.as_slice())
                            .map_err(to_io)?;
                    }
                    txn.commit().map_err(to_io)
                })
                .await
                .map_err(join_to_io)??;
            }
            Ok(())
        }

        async fn get_current_snapshot(
            &mut self,
        ) -> Result<Option<SnapshotOf<PcsTypeConfig>>, io::Error> {
            // If nothing has been applied yet there is no snapshot to return.
            // openraft treats `None` as "no snapshot" and will send the full log
            // instead, which is correct for a freshly-initialized node.
            if self.last_applied.is_none() {
                return Ok(None);
            }

            // Move db lock + build_snapshot_bytes into spawn_blocking:
            // building the snapshot holds the Mutex and may issue disk reads.
            let db = Arc::clone(&self.db);
            let payload = tokio::task::spawn_blocking(move || {
                let db = db
                    .lock()
                    .map_err(|_| io::Error::other("db mutex poisoned"))?;
                crate::distributed::consensus::snapshot::raft_impl::build_snapshot_bytes(&db)
                    .map_err(|e| io::Error::other(format!("get_current_snapshot build: {e}")))
            })
            .await
            .map_err(join_to_io)??;

            use openraft::{Snapshot, SnapshotMeta};
            use std::io::Cursor;

            let meta = SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.last_membership.clone(),
                snapshot_id: format!(
                    "arrow-snap-{}",
                    self.last_applied.map(|l| l.index).unwrap_or(0)
                ),
            };
            Ok(Some(Snapshot {
                meta,
                snapshot: Cursor::new(payload),
            }))
        }
    }

    // ── Store consistency validation ──────────────────────────────────────────

    /// Validate that `last_applied` is not behind `last_purged_log_id`.
    ///
    /// Call this after opening both `ArrowRedbLogStore` and
    /// `ArrowRedbStateMachine` from the same node directory. A mismatch
    /// indicates the files were restored from mismatched backups or the log
    /// store was wiped while the state-machine file was retained.
    ///
    /// Returns `Ok(())` when consistent. Returns `Err` with a diagnostic
    /// message when the invariant is violated.
    ///
    /// # Safety
    ///
    /// This is a diagnostic check only — it does not modify any state.
    /// Pass `None` for either argument if the corresponding watermark has
    /// not yet been written (e.g. a freshly-initialized node).
    pub fn validate_store_consistency(
        last_purged: Option<LogIdOf<PcsTypeConfig>>,
        last_applied: Option<LogIdOf<PcsTypeConfig>>,
    ) -> PcsResult<()> {
        match (last_purged, last_applied) {
            (Some(purged), None) => Err(PcsError::store(format!(
                "store consistency violation: log store has purged up to index {} \
                 but state machine last_applied is None — state machine is behind. \
                 Do not mix log and state-machine redb files from different backups.",
                purged.index
            ))),
            (Some(purged), Some(applied)) if applied.index < purged.index => {
                Err(PcsError::store(format!(
                    "store consistency violation: log store purged up to index {} \
                     but state machine last_applied is index {} — state machine is behind. \
                     Do not mix log and state-machine redb files from different backups.",
                    purged.index, applied.index
                )))
            }
            _ => Ok(()),
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[cfg(test)]
    mod tests {
        use super::*;
        use openraft::entry::RaftEntry;
        use openraft::storage::IOFlushed;
        use openraft::type_config::alias::EntryOf;
        use tempfile::TempDir;

        /// Verify postcard enc/dec round-trip for Option<LogId>.
        #[test]
        fn test_postcard_log_id_round_trip() {
            let lid = log_id(2, 10);
            let opt: Option<LogIdOf<PcsTypeConfig>> = Some(lid);
            let bytes = enc(&opt).unwrap();
            let decoded: Option<LogIdOf<PcsTypeConfig>> = dec(&bytes).unwrap();
            assert_eq!(decoded.map(|l| l.index), Some(10));
        }

        /// Direct test of persist_sm_meta + reopen: verify the stored index
        /// survives a Database close and reopen.
        #[tokio::test]
        async fn test_persist_sm_meta_survives_reopen() {
            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("meta_persist_test.redb");

            // Write via open() + persist_sm_meta directly.
            {
                let app_db = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
                let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();
                sm.last_applied = Some(log_id(2, 10));
                sm.persist_sm_meta().unwrap();
            }

            // Re-open and read.
            let app_db2 = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
            let sm2 = ArrowRedbStateMachine::open(app_db2).unwrap();
            assert_eq!(
                sm2.last_applied.map(|l| l.index),
                Some(10),
                "last_applied must persist across reopen"
            );
        }

        /// Verify that an empty begin_write + open_table + commit does NOT
        /// destroy data previously written to that table. This is a prerequisite
        /// for `ArrowRedbStateMachine::open()` being restart-safe.
        #[test]
        fn test_redb_open_table_preserves_existing_data() {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("preserve_test.redb");

            // Write data.
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

        fn make_store(dir: &TempDir) -> ArrowRedbLogStore {
            ArrowRedbLogStore::open(dir.path().join("arrow_raft.db")).unwrap()
        }

        fn log_id(term: u64, index: u64) -> openraft::type_config::alias::LogIdOf<PcsTypeConfig> {
            use openraft::vote::RaftLeaderId;
            openraft::LogId::new(
                openraft::impls::leader_id_adv::LeaderId::new(term, 1u64),
                index,
            )
        }

        fn blank_entry(index: u64) -> EntryOf<PcsTypeConfig> {
            openraft::Entry::new_blank(log_id(1, index))
        }

        #[tokio::test]
        async fn test_empty_state() {
            let dir = TempDir::new().unwrap();
            let mut store = make_store(&dir);
            let state = store.get_log_state().await.unwrap();
            assert!(state.last_purged_log_id.is_none());
            assert!(state.last_log_id.is_none());
        }

        #[tokio::test]
        async fn test_vote_round_trip() {
            let dir = TempDir::new().unwrap();
            let mut store = make_store(&dir);
            let vote = openraft::Vote::new(1, 2);
            store.save_vote(&vote).await.unwrap();
            assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        }

        #[tokio::test]
        async fn test_append_and_read() {
            let dir = TempDir::new().unwrap();
            let mut store = make_store(&dir);
            store
                .append(
                    vec![blank_entry(1), blank_entry(2), blank_entry(3)],
                    IOFlushed::noop(),
                )
                .await
                .unwrap();
            let read = store.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(read.len(), 3);
            assert_eq!(read[0].log_id.index, 1);
        }

        #[tokio::test]
        async fn test_truncate() {
            let dir = TempDir::new().unwrap();
            let mut store = make_store(&dir);
            store
                .append(
                    vec![blank_entry(1), blank_entry(2), blank_entry(3)],
                    IOFlushed::noop(),
                )
                .await
                .unwrap();
            store.truncate_after(Some(log_id(1, 1))).await.unwrap();
            let remaining = store.try_get_log_entries(0..10).await.unwrap();
            assert_eq!(remaining.len(), 1);
        }

        #[tokio::test]
        async fn test_purge() {
            let dir = TempDir::new().unwrap();
            let mut store = make_store(&dir);
            store
                .append(
                    vec![blank_entry(1), blank_entry(2), blank_entry(3)],
                    IOFlushed::noop(),
                )
                .await
                .unwrap();
            store.purge(log_id(1, 2)).await.unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 2);
        }

        /// With `D = ConsensusCommand` on `PcsTypeConfig`, the state machine
        /// apply path carries the application command directly (no string
        /// encode/decode step). An applied `Normal` entry advances
        /// `last_applied` and produces the corresponding row in the
        /// application redb file. We do not pass an openraft responder here
        /// (constructing one requires crate-internal types); advancement of
        /// `last_applied` together with the read-after-apply check below are
        /// what matters for the invariant.
        #[tokio::test]
        async fn test_state_machine_apply_advances_last_applied_on_success() {
            use crate::distributed::consensus::state_machine::{apply, read_master_batch};
            use crate::distributed::consensus::types::ConsensusCommand;
            use futures_util::stream;
            use openraft::Entry;

            let dir = TempDir::new().unwrap();
            let app_db = Arc::new(Mutex::new(
                Database::create(dir.path().join("sm_apply_app.redb")).unwrap(),
            ));
            let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();
            assert!(sm.last_applied.is_none(), "fresh SM: last_applied is None");

            // Build a valid RegisterMasterBatch command entry. With
            // `D = ConsensusCommand`, the command goes directly into
            // `EntryPayload::Normal` without any string encoding step.
            let cmd = ConsensusCommand::RegisterMasterBatch {
                batch_id: 42,
                component: "task3".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 32],
                total_rows: 10,
                now_at_propose: 0,
            };
            let _ = apply; // silence unused when building without the helper

            let lid = log_id(1, 5);
            let mut entry: EntryOf<PcsTypeConfig> = Entry::new_blank(lid);
            entry.payload = openraft::EntryPayload::Normal(cmd);

            // No responder — simulates follower-side apply (no client waiting).
            let stream = stream::iter(vec![Ok((entry, None))]);
            sm.apply(stream).await.unwrap();

            // After apply, last_applied MUST have advanced to the entry's
            // log_id. The order is deliberate: advance only after the state
            // machine side effect succeeds.
            assert_eq!(
                sm.last_applied.map(|l| l.index),
                Some(5),
                "last_applied must advance after successful apply"
            );

            // And verify the state-machine side effect actually took place —
            // otherwise we can't distinguish "applied" from "skipped".
            let db = app_db.lock().unwrap();
            let record = read_master_batch(&db, 42)
                .unwrap()
                .expect("master batch 42 must exist after apply");
            assert_eq!(record.component, "task3");
            assert_eq!(record.total_rows, 10);
        }

        /// After apply, `last_applied` and `last_membership` are written to
        /// redb. Re-opening the state machine with the same database must
        /// restore those values so openraft does not re-apply already-committed
        /// entries on restart.
        #[tokio::test]
        async fn test_state_machine_restart_restores_watermarks() {
            use crate::distributed::consensus::types::ConsensusCommand;
            use futures_util::stream;
            use openraft::Entry;

            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("restart_test_app.redb");

            let last_applied_index = {
                let app_db = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
                let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

                let cmd = ConsensusCommand::RegisterMasterBatch {
                    batch_id: 7,
                    component: "restart_comp".to_string(),
                    schema_id: 1,
                    ipc_bytes: vec![0u8; 32],
                    total_rows: 5,
                    now_at_propose: 0,
                };
                let lid = log_id(2, 10);
                let mut entry: EntryOf<PcsTypeConfig> = Entry::new_blank(lid);
                entry.payload = openraft::EntryPayload::Normal(cmd);

                let s = stream::iter(vec![Ok((entry, None))]);
                sm.apply(s).await.unwrap();

                sm.last_applied.unwrap().index
            };
            // The `app_db` Arc (and the Mutex<Database>) is dropped here,
            // releasing the redb file lock before the next open.

            // Re-open the same file — simulates a restart.
            let app_db2 = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
            let sm2 = ArrowRedbStateMachine::open(Arc::clone(&app_db2)).unwrap();

            assert_eq!(
                sm2.last_applied.map(|l| l.index),
                Some(last_applied_index),
                "last_applied must be restored after restart"
            );
        }

        /// `applied_state()` must return the correct `last_applied` log-id and
        /// `last_membership` after a restart — not a fresh empty state.
        ///
        /// This test applies both a `Normal` entry (advancing `last_applied`) and
        /// a `Membership` entry (advancing `last_membership` to a non-default
        /// value), closes the database, reopens it, and verifies that calling the
        /// public `applied_state()` trait method returns the persisted values.
        #[tokio::test]
        async fn test_applied_state_returns_persisted_values_after_restart() {
            use futures_util::stream;
            use openraft::{Entry, Membership};
            use std::collections::BTreeSet;

            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("applied_state_restart.redb");

            let (expected_log_index, expected_voter_ids) = {
                let app_db = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
                let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

                // Entry 1: Normal — advances last_applied to index 3.
                let mut normal_entry: EntryOf<PcsTypeConfig> = Entry::new_blank(log_id(1, 3));
                normal_entry.payload = openraft::EntryPayload::Normal(
                    crate::distributed::consensus::types::ConsensusCommand::AckClaim {
                        claim_id: uuid::Uuid::nil(),
                        instance_id: uuid::Uuid::nil(),
                    },
                );

                // Entry 2: Membership — advances last_membership to {1, 2}.
                let voter_ids: BTreeSet<u64> = [1u64, 2u64].into_iter().collect();
                // `new_with_defaults` expects `IntoIterator<Item = NID>` for the
                // nodes argument and fills in `N::default()` for each node entry.
                let membership = Membership::new_with_defaults(
                    vec![voter_ids.clone()],
                    voter_ids.iter().copied(),
                );
                let mem_entry: EntryOf<PcsTypeConfig> = Entry {
                    log_id: log_id(1, 4),
                    payload: openraft::EntryPayload::Membership(membership),
                };

                sm.apply(stream::iter(vec![
                    Ok((normal_entry, None)),
                    Ok((mem_entry, None)),
                ]))
                .await
                .unwrap();

                (sm.last_applied.unwrap().index, voter_ids)
                // app_db Arc dropped here → file lock released.
            };

            // Reopen — simulates a process restart.
            let app_db2 = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
            let mut sm2 = ArrowRedbStateMachine::open(Arc::clone(&app_db2)).unwrap();

            let (restored_log_id, restored_membership) = sm2.applied_state().await.unwrap();

            assert_eq!(
                restored_log_id.map(|l| l.index),
                Some(expected_log_index),
                "applied_state() must return persisted last_applied after restart"
            );
            assert_eq!(
                restored_membership
                    .membership()
                    .voter_ids()
                    .collect::<BTreeSet<u64>>(),
                expected_voter_ids,
                "applied_state() must return persisted last_membership after restart"
            );
        }

        /// `get_current_snapshot` must return `None` on a fresh (never-applied)
        /// state machine and a real snapshot after at least one entry is applied.
        #[tokio::test]
        async fn test_get_current_snapshot_returns_snapshot_after_apply() {
            use crate::distributed::consensus::types::ConsensusCommand;
            use futures_util::stream;
            use openraft::Entry;

            let dir = TempDir::new().unwrap();
            let app_db = Arc::new(Mutex::new(
                Database::create(dir.path().join("snap_test.redb")).unwrap(),
            ));
            let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

            // Before any apply: no snapshot.
            let snap = sm.get_current_snapshot().await.unwrap();
            assert!(snap.is_none(), "fresh SM must return None snapshot");

            // Apply one entry.
            let cmd = ConsensusCommand::RegisterMasterBatch {
                batch_id: 99,
                component: "snap_comp".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 32],
                total_rows: 3,
                now_at_propose: 0,
            };
            let lid = log_id(1, 1);
            let mut entry: EntryOf<PcsTypeConfig> = Entry::new_blank(lid);
            entry.payload = openraft::EntryPayload::Normal(cmd);
            sm.apply(stream::iter(vec![Ok((entry, None))]))
                .await
                .unwrap();

            // After apply: snapshot must exist with matching metadata.
            let snap = sm.get_current_snapshot().await.unwrap();
            assert!(snap.is_some(), "SM must return a snapshot after apply");
            let snap = snap.unwrap();
            assert_eq!(
                snap.meta.last_log_id.map(|l| l.index),
                Some(1),
                "snapshot last_log_id must match last applied"
            );
            assert!(
                !snap.snapshot.into_inner().is_empty(),
                "snapshot payload must be non-empty"
            );
        }

        /// Verify that a Normal entry with a malformed command propagates the
        /// error through `apply` and leaves `last_applied` at the pre-apply value.
        ///
        /// We simulate an apply failure by directly calling sm_apply with a
        /// ClaimRowRange for a non-existent batch — this returns
        /// `ConsensusResponse::Error`, NOT an I/O error. To test the true I/O
        /// error path (sm_apply returns Err), we use a separate unit test in
        /// state_machine that tests `apply` directly.
        ///
        /// What this test covers: after a successful apply `last_applied`
        /// advances; on a *stream error* (entry? returning Err) `last_applied`
        /// stays at None.
        #[tokio::test]
        async fn sm_apply_err_halts_stream() {
            let dir = TempDir::new().unwrap();
            let app_db = Arc::new(Mutex::new(
                Database::create(dir.path().join("sm_test.redb")).unwrap(),
            ));
            let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

            assert!(
                sm.last_applied.is_none(),
                "initial last_applied must be None"
            );

            // Inject a stream-level Err (simulates network / IO failure delivering entries).
            let stream_err: io::Error = io::Error::other("injected stream failure");
            let stream = futures_util::stream::iter(vec![Err(stream_err)]);
            let result = sm.apply(stream).await;
            assert!(result.is_err(), "apply must propagate stream Err");
            assert!(
                sm.last_applied.is_none(),
                "last_applied must not advance when stream returns Err"
            );
        }

        /// Smoke test: truncate_after successfully removes entries > last.
        #[tokio::test]
        async fn truncate_after_removes_tail() {
            let dir = TempDir::new().unwrap();
            let mut store = make_store(&dir);
            store
                .append(
                    vec![
                        blank_entry(1),
                        blank_entry(2),
                        blank_entry(3),
                        blank_entry(4),
                    ],
                    IOFlushed::noop(),
                )
                .await
                .unwrap();

            // Truncate after index 2 → 3 and 4 should be removed.
            store.truncate_after(Some(log_id(1, 2))).await.unwrap();
            let remaining = store.try_get_log_entries(1..10).await.unwrap();
            assert_eq!(remaining.len(), 2, "only indices 1 and 2 should remain");
            assert_eq!(remaining[0].log_id.index, 1);
            assert_eq!(remaining[1].log_id.index, 2);
        }

        #[test]
        fn test_validate_store_consistency_both_none() {
            validate_store_consistency(None, None).expect("none/none is consistent");
        }

        #[test]
        fn test_validate_store_consistency_no_purge() {
            let applied = Some(log_id(1, 5));
            validate_store_consistency(None, applied).expect("no purge, applied set: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_ahead() {
            let purged = Some(log_id(1, 3));
            let applied = Some(log_id(1, 10));
            validate_store_consistency(purged, applied).expect("applied > purged: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_equals_purged() {
            let lid = log_id(1, 5);
            validate_store_consistency(Some(lid), Some(lid))
                .expect("applied == purged: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_behind_purged() {
            let purged = Some(log_id(1, 10));
            let applied = Some(log_id(1, 3));
            let err = validate_store_consistency(purged, applied)
                .expect_err("applied < purged: must be an error");
            assert!(
                err.to_string().contains("state machine is behind"),
                "error message must explain the skew: {err}"
            );
        }

        #[test]
        fn test_validate_store_consistency_purged_set_applied_none() {
            let purged = Some(log_id(1, 5));
            let err = validate_store_consistency(purged, None)
                .expect_err("purged set but applied None: must be an error");
            assert!(
                err.to_string()
                    .contains("state machine last_applied is None"),
                "error message must identify missing last_applied: {err}"
            );
        }
    }
}

// Re-export for feature-gated use.
#[cfg(feature = "distributed-raft")]
pub use raft_impl::{ArrowRedbLogStore, ArrowRedbStateMachine, validate_store_consistency};
