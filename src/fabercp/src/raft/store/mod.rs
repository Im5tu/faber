//! RocksDB-backed Raft storage.
//!
//! One database per node with four column families:
//! - `logs`: log entries keyed by big-endian index, JSON values.
//! - `log_meta`: vote, committed log id and last purged log id.
//! - `sm_data`: the key/value state machine (UTF-8 key, JSON value).
//! - `sm_meta`: last applied log id, last membership, current snapshot and the
//!   snapshot id counter.
//!
//! Every write that Raft depends on for safety goes through a synced
//! `WriteBatch`, so a crash never loses an acknowledged write.

pub mod keys;
mod log;
pub mod place;
pub mod services;
mod sm;
pub mod status;

use std::path::Path;
use std::sync::{Arc, Weak};

use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, RaftLogReader};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Options, WriteBatch, WriteOptions};

pub use log::LogStore;
pub use sm::{PlacementView, ReadError, SnapshotBuilder, StateMachineStore, StateReader};

use crate::raft::types::{NodeId, StorageError};

pub(crate) const CF_LOGS: &str = "logs";
pub(crate) const CF_LOG_META: &str = "log_meta";
pub(crate) const CF_SM_DATA: &str = "sm_data";
pub(crate) const CF_SM_META: &str = "sm_meta";

const ALL_CFS: [&str; 4] = [CF_LOGS, CF_LOG_META, CF_SM_DATA, CF_SM_META];

/// Handles produced by [`open`].
pub struct Storage {
    pub log_store: LogStore,
    pub state_machine: StateMachineStore,
    /// Read-only view of `sm_data` that does not keep the database open.
    pub reader: StateReader,
}

/// Opens (or creates) the node database under `dir`.
pub fn open(dir: impl AsRef<Path>) -> Result<Storage, StorageError> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let cfs = ALL_CFS
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()));
    let db = DB::open_cf_descriptors(&opts, dir.as_ref(), cfs)
        .map_err(|e| openraft::StorageIOError::<u64>::read(&e))?;
    let db = Arc::new(db);

    let state_machine = StateMachineStore::new(db.clone())?;
    let reader = state_machine.reader();
    Ok(Storage {
        log_store: LogStore::new(db),
        state_machine,
        reader,
    })
}

/// Returns a weak handle, used by callers that must observe when the database
/// has been closed (all strong handles dropped).
pub fn weak_db(storage: &Storage) -> Weak<DB> {
    storage.log_store.weak_db()
}

/// Whether `node_id` is in a membership this storage holds: the one the
/// state machine last applied, or one in a log entry that is still stored.
/// These are the places raft reads its membership from when it starts, so a
/// node that is `false` here has never been made a learner or a voter.
pub async fn knows_member(storage: &mut Storage, node_id: NodeId) -> Result<bool, StorageError> {
    let (_, applied) = storage.state_machine.applied_state().await?;
    if applied.membership().get_node(&node_id).is_some() {
        return Ok(true);
    }
    let entries = storage.log_store.try_get_log_entries(..).await?;
    Ok(entries.iter().any(|entry| match &entry.payload {
        EntryPayload::Membership(m) => m.get_node(&node_id).is_some(),
        _ => false,
    }))
}

pub(crate) fn cf<'a>(db: &'a DB, name: &str) -> &'a ColumnFamily {
    // The column families are created in `open`; a missing one is a bug.
    db.cf_handle(name)
        .unwrap_or_else(|| panic!("column family {name} missing"))
}

/// Writes `batch` and fsyncs the WAL before returning.
pub(crate) fn write_synced(db: &DB, batch: WriteBatch) -> Result<(), rocksdb::Error> {
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    db.write_opt(batch, &wo)
}

/// Big-endian so that byte order matches numeric order in RocksDB iteration.
pub(crate) fn encode_index(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

pub(crate) fn decode_index(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_encoding_sorts_numerically() {
        let values = [0u64, 1, 9, 10, 255, 256, 65_535, 65_536, u64::MAX - 1];
        for pair in values.windows(2) {
            assert!(encode_index(pair[0]) < encode_index(pair[1]), "{pair:?}");
        }
    }

    fn membership_entry(index: u64, ids: &[u64]) -> crate::raft::types::Entry {
        use std::collections::BTreeMap;

        use crate::raft::types::Node;

        let nodes: BTreeMap<u64, Node> = ids
            .iter()
            .map(|id| (*id, Node::new(format!("10.99.0.{id}:7401"))))
            .collect();
        crate::raft::types::Entry {
            log_id: openraft::testing::log_id(1, 1, index),
            payload: EntryPayload::Membership(openraft::Membership::new(
                vec![[1u64].into_iter().collect()],
                nodes,
            )),
        }
    }

    #[tokio::test]
    async fn empty_storage_knows_no_member() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        assert!(!knows_member(&mut s, 1).await.unwrap());
        assert!(!knows_member(&mut s, 2).await.unwrap());
    }

    #[tokio::test]
    async fn an_applied_membership_is_known() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        // Node 2 is a learner here: in the nodes, not among the voters.
        s.state_machine
            .apply([membership_entry(1, &[1, 2])])
            .await
            .unwrap();
        assert!(knows_member(&mut s, 1).await.unwrap());
        assert!(knows_member(&mut s, 2).await.unwrap());
        assert!(!knows_member(&mut s, 3).await.unwrap());
    }

    #[tokio::test]
    async fn a_membership_still_in_the_log_is_known() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        // Appended but not applied yet.
        let entry = membership_entry(4, &[1, 2]);
        let db = weak_db(&s).upgrade().unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(
            cf(&db, CF_LOGS),
            encode_index(entry.log_id.index),
            serde_json::to_vec(&entry).unwrap(),
        );
        write_synced(&db, batch).unwrap();
        drop(db);
        assert!(knows_member(&mut s, 2).await.unwrap());
        assert!(!knows_member(&mut s, 3).await.unwrap());
    }

    #[test]
    fn index_encoding_round_trips() {
        for v in [0u64, 42, u64::MAX] {
            assert_eq!(decode_index(&encode_index(v)), Some(v));
        }
        assert_eq!(decode_index(b"short"), None);
    }
}
