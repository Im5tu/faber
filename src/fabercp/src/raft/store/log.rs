//! Raft log storage in the `logs` and `log_meta` column families.

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Weak};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{AnyError, OptionalSend, RaftLogReader, StorageIOError};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{CF_LOG_META, CF_LOGS, cf, decode_index, encode_index, write_synced};
use crate::raft::types::{Entry, LogId, StorageError, TypeConfig, Vote};

const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_PURGED: &[u8] = b"last_purged";

#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<DB>,
}

impl LogStore {
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    pub(crate) fn weak_db(&self) -> Weak<DB> {
        Arc::downgrade(&self.db)
    }

    fn get_meta<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, StorageError> {
        let bytes = self
            .db
            .get_cf(cf(&self.db, CF_LOG_META), key)
            .map_err(|e| StorageIOError::read(&e))?;
        match bytes {
            None => Ok(None),
            Some(b) => Ok(Some(
                serde_json::from_slice(&b).map_err(|e| StorageIOError::read(&e))?,
            )),
        }
    }

    fn put_meta_synced<T: Serialize>(
        &self,
        key: &[u8],
        value: &T,
        err: fn(AnyError) -> StorageIOError<u64>,
    ) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(value).map_err(|e| err(AnyError::new(&e)))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf(&self.db, CF_LOG_META), key, bytes);
        write_synced(&self.db, batch).map_err(|e| err(AnyError::new(&e)))?;
        Ok(())
    }
}

fn write_err(e: AnyError) -> StorageIOError<u64> {
    StorageIOError::write(e)
}

fn write_vote_err(e: AnyError) -> StorageIOError<u64> {
    StorageIOError::write_vote(e)
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let start = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => match x.checked_add(1) {
                Some(v) => v,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => 0,
        };
        let start_key = encode_index(start);
        let iter = self.db.iterator_cf(
            cf(&self.db, CF_LOGS),
            IteratorMode::From(&start_key, Direction::Forward),
        );

        let mut entries = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|e| StorageIOError::read_logs(&e))?;
            let index = decode_index(&key)
                .ok_or_else(|| StorageIOError::read_logs(&std::io::Error::other("bad log key")))?;
            if !range.contains(&index) {
                break;
            }
            let entry: Entry =
                serde_json::from_slice(&value).map_err(|e| StorageIOError::read_logs(&e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last = self
            .db
            .iterator_cf(cf(&self.db, CF_LOGS), IteratorMode::End)
            .next()
            .transpose()
            .map_err(|e| StorageIOError::read_logs(&e))?;
        let last_log_id = match last {
            Some((_, value)) => Some(
                serde_json::from_slice::<Entry>(&value)
                    .map_err(|e| StorageIOError::read_logs(&e))?
                    .log_id,
            ),
            None => None,
        };
        let last_purged_log_id: Option<LogId> = self.get_meta(KEY_LAST_PURGED)?;
        Ok(LogState {
            last_purged_log_id,
            last_log_id: last_log_id.or(last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        self.put_meta_synced(KEY_VOTE, vote, write_vote_err)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        self.get_meta(KEY_VOTE)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        self.put_meta_synced(KEY_COMMITTED, &committed, write_err)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(self.get_meta::<Option<LogId>>(KEY_COMMITTED)?.flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let logs = cf(&self.db, CF_LOGS);
        let mut batch = WriteBatch::default();
        for entry in entries {
            let bytes = serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?;
            batch.put_cf(logs, encode_index(entry.log_id.index), bytes);
        }
        let result = write_synced(&self.db, batch);
        // Only report the entries as flushed once the synced write returned.
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.log_io_completed(Err(std::io::Error::other(e.to_string())));
                Err(StorageIOError::write_logs(&e).into())
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        tracing::debug!(%log_id, "truncate logs from");
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(
            cf(&self.db, CF_LOGS),
            encode_index(log_id.index),
            encode_index(u64::MAX),
        );
        // `delete_range` excludes its end key.
        batch.delete_cf(cf(&self.db, CF_LOGS), encode_index(u64::MAX));
        write_synced(&self.db, batch).map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        tracing::debug!(%log_id, "purge logs up to");
        let bytes = serde_json::to_vec(&log_id).map_err(|e| StorageIOError::write_logs(&e))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf(&self.db, CF_LOG_META), KEY_LAST_PURGED, bytes);
        batch.delete_range_cf(
            cf(&self.db, CF_LOGS),
            encode_index(0),
            encode_index(log_id.index.saturating_add(1)),
        );
        write_synced(&self.db, batch).map_err(|e| StorageIOError::write_logs(&e).into())
    }
}
