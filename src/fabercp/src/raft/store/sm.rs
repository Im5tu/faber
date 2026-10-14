//! The key/value state machine in the `sm_data` and `sm_meta` column families.
//!
//! State lives on disk; nothing is replayed at startup. Each `apply` call
//! commits its data changes together with `last_applied` and
//! `last_membership` in one synced batch, so the two can never disagree.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::sync::{Arc, Weak};

use faber_container_runtime::ContainerRuntimeSpec;
use openraft::storage::RaftStateMachine;
use openraft::{AnyError, EntryPayload, OptionalSend, RaftSnapshotBuilder, StorageIOError};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;

use super::{CF_SM_DATA, CF_SM_META, cf, keys, place, services, status, write_synced};
use crate::apps::state::{ENV_PREFIX, KEY_PROJECTS, env_key, is_reserved_key};
use crate::apps::{EnvironmentRecord, ProjectIndex};
use crate::mesh::state::{KEY_CONTAINERD, KEY_MESH, RESERVED_KEYS};
use crate::mesh::{MeshState, Rejection};
use crate::raft::types::{
    Entry, LogId, Request, Response, Snapshot, SnapshotMeta, StorageError, StoredMembership,
    TypeConfig,
};

const KEY_LAST_APPLIED: &[u8] = b"last_applied";
const KEY_LAST_MEMBERSHIP: &[u8] = b"last_membership";
const KEY_SNAPSHOT: &[u8] = b"snapshot";
const KEY_SNAPSHOT_COUNTER: &[u8] = b"snapshot_counter";

// One `append` RPC carries at most `max_payload_entries` log entries, which
// is 300 by openraft's default (`Config::max_payload_entries`), and fabercp
// keeps the default. Should every one of them be a deploy of the largest
// size, the batch must still fit the body limit of the raft routes.
const _: () = assert!(
    300 * faber_api_types::workload::limits::MAX_DEPLOY_REQUEST_BYTES
        < crate::raft::rpc::MAX_RPC_BODY
);

/// A snapshot as persisted in `sm_meta`.
#[derive(Debug, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta,
    /// JSON object of every `sm_data` entry.
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    db: Arc<DB>,
    /// Ticked after every synced apply batch and snapshot install. Dropping
    /// the last store closes the channel.
    changes: Arc<watch::Sender<u64>>,
}

/// Data writes of the current apply batch: queued in the RocksDB batch and
/// visible to later entries of the same batch.
struct Pending<'a> {
    data: &'a rocksdb::ColumnFamily,
    batch: WriteBatch,
    /// The value each key written in this batch holds now. `None` for a
    /// key the batch deleted.
    values: HashMap<String, Option<Value>>,
    /// The membership entry applied latest in this batch, if there was one.
    /// Written to `sm_meta` with the batch.
    membership: Option<StoredMembership>,
}

impl Pending<'_> {
    fn put<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), StorageError> {
        let value = serde_json::to_value(value)
            .map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?;
        self.batch
            .put_cf(self.data, key.as_bytes(), encode(&value)?);
        self.values.insert(key.to_string(), Some(value));
        Ok(())
    }

    fn delete(&mut self, key: &str) {
        self.batch.delete_cf(self.data, key.as_bytes());
        self.values.insert(key.to_string(), None);
    }
}

/// Whether only typed commands may write `key`: `Request::Set` is refused.
fn is_reserved(key: &str) -> bool {
    RESERVED_KEYS.contains(&key) || is_reserved_key(key)
}

fn reject(code: &str, message: impl Into<String>) -> Response {
    Rejection::new(code, message).into()
}

/// Builds snapshots from a point-in-time RocksDB snapshot, so it can run while
/// the state machine keeps applying entries.
#[derive(Debug, Clone)]
pub struct SnapshotBuilder {
    db: Arc<DB>,
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    serde_json::from_slice(bytes).map_err(|e| StorageIOError::read_state_machine(&e).into())
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(value).map_err(|e| StorageIOError::write_state_machine(&e).into())
}

fn read_err(e: rocksdb::Error) -> StorageError {
    StorageIOError::read_state_machine(&e).into()
}

impl StateMachineStore {
    pub(crate) fn new(db: Arc<DB>) -> Result<Self, StorageError> {
        let (changes, _) = watch::channel(0);
        Ok(Self {
            db,
            changes: Arc::new(changes),
        })
    }

    /// A reader over this state machine's data that does not keep it open.
    pub fn reader(&self) -> StateReader {
        StateReader::new(Arc::downgrade(&self.db), self.changes.subscribe())
    }

    /// Tells subscribers that `sm_data` may have changed. `send_modify`
    /// updates the value even when nobody is subscribed yet.
    fn notify(&self) {
        self.changes.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// The `mesh` record as seen by entries earlier in the current batch.
    fn mesh(&self, pending: &Pending<'_>) -> Result<Option<MeshState>, StorageError> {
        self.typed(pending, KEY_MESH)
    }

    /// The record under `key` as seen by entries earlier in the current
    /// batch.
    fn typed<T: DeserializeOwned>(
        &self,
        pending: &Pending<'_>,
        key: &str,
    ) -> Result<Option<T>, StorageError> {
        self.current(&pending.values, key)?
            .map(|v| {
                serde_json::from_value(v)
                    .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))
            })
            .transpose()
    }

    /// Node ids, voters and learners, of the last applied raft membership as
    /// seen by the entry being applied: the membership entry applied latest
    /// in the current batch, or else the one stored by an earlier batch or
    /// a snapshot install. Empty before any membership was applied.
    ///
    /// Every replica applies the same entries in the same order, so each
    /// computes the same set for the same entry, however the entries are
    /// split into batches.
    fn member_ids(&self, pending: &Pending<'_>) -> Result<BTreeSet<u64>, StorageError> {
        let stored = match &pending.membership {
            Some(m) => m.clone(),
            None => self.meta(KEY_LAST_MEMBERSHIP)?.unwrap_or_default(),
        };
        Ok(stored
            .voter_ids()
            .chain(stored.nodes().map(|(id, _)| *id))
            .collect())
    }

    /// Applies one log entry into `pending`. A membership entry becomes the
    /// membership that later entries of the batch see.
    fn apply_entry(
        &self,
        pending: &mut Pending<'_>,
        entry: Entry,
    ) -> Result<Response, StorageError> {
        Ok(match entry.payload {
            EntryPayload::Blank => Response::None,
            EntryPayload::Normal(Request::Set { key, .. }) if is_reserved(&key) => reject(
                faber_api_types::error_code::BAD_REQUEST,
                format!("key {key} is reserved"),
            ),
            EntryPayload::Normal(Request::Set { key, value }) => {
                let previous = self.current(&pending.values, &key)?;
                pending.put(&key, &value)?;
                Response::Previous(previous)
            }
            // Project and workload commands go ahead of `apply_mesh`: none
            // needs a mesh.
            EntryPayload::Normal(
                req @ (Request::CreateProject { .. }
                | Request::DeleteProject { .. }
                | Request::AddEnvironment { .. }
                | Request::RemoveEnvironment { .. }
                | Request::ApplyWorkloads { .. }
                | Request::RemoveWorkloads { .. }),
            ) => self.apply_apps(pending, req, entry.log_id.index)?,
            EntryPayload::Normal(Request::CreateDataKey(cmd)) => keys::apply_create_data_key(cmd),
            EntryPayload::Normal(Request::SetClusterKeyId(cmd)) => {
                keys::apply_set_cluster_key_id(cmd)
            }
            EntryPayload::Normal(Request::PlaceReplicas(cmd)) => place::apply_place_replicas(cmd),
            EntryPayload::Normal(Request::ReportNode(cmd)) => status::apply_report_node(cmd),
            EntryPayload::Normal(Request::SetStatusInterval(cmd)) => {
                status::apply_set_status_interval(cmd)
            }
            EntryPayload::Normal(Request::AllocateServiceIp(cmd)) => {
                services::apply_allocate_service_ip(cmd)
            }
            EntryPayload::Normal(Request::ReleaseServiceIp(cmd)) => {
                services::apply_release_service_ip(cmd)
            }
            EntryPayload::Normal(req) => self.apply_mesh(pending, req)?,
            EntryPayload::Membership(m) => {
                pending.membership = Some(StoredMembership::new(Some(entry.log_id), m));
                Response::None
            }
        })
    }

    /// Applies one project or workload command, the entry at log index
    /// `index`. A refused command becomes `Response::Rejected` and writes
    /// nothing; only storage failures are errors.
    ///
    /// The index and the environment records a command changes go into the
    /// same batch, so the two never disagree.
    fn apply_apps(
        &self,
        pending: &mut Pending<'_>,
        req: Request,
        index: u64,
    ) -> Result<Response, StorageError> {
        let stored: ProjectIndex = self.typed(pending, KEY_PROJECTS)?.unwrap_or_default();
        let mut projects = stored.clone();
        // Per environment key the record to write, or `None` to delete the
        // key.
        let mut writes: Vec<(String, Option<EnvironmentRecord>)> = Vec::new();
        let result = match req {
            Request::CreateProject {
                name,
                environments,
                at,
            } => projects
                .create_project(&name, &environments, index, at)
                .map(|records| {
                    for (environment, record) in records {
                        writes.push((env_key(&name, &environment), Some(record)));
                    }
                    Response::None
                }),
            Request::DeleteProject { name } => projects.delete_project(&name).map(|environments| {
                for environment in &environments {
                    writes.push((env_key(&name, environment), None));
                }
                Response::None
            }),
            Request::AddEnvironment { project, name, at } => projects
                .add_environment(&project, &name, index, at)
                .map(|record| {
                    writes.push((env_key(&project, &name), Some(record)));
                    Response::None
                }),
            Request::RemoveEnvironment { project, name } => {
                projects.remove_environment(&project, &name).map(|()| {
                    writes.push((env_key(&project, &name), None));
                    Response::None
                })
            }
            Request::ApplyWorkloads {
                project,
                environment,
                workloads,
                prune,
                at,
            } => {
                let record = self.environment(pending, &stored, &project, &environment)?;
                projects
                    .apply_workloads(record, &project, &environment, workloads, prune, index, at)
                    .map(|done| {
                        // No record: nothing changed, nothing to write.
                        if let Some(record) = done.record {
                            writes.push((env_key(&project, &environment), Some(record)));
                        }
                        Response::from(done.applied)
                    })
            }
            Request::RemoveWorkloads {
                project,
                environment,
                names,
                at,
            } => {
                let record = self.environment(pending, &stored, &project, &environment)?;
                projects
                    .remove_workloads(record, &project, &environment, &names, index, at)
                    .map(|record| {
                        writes.push((env_key(&project, &environment), Some(record)));
                        Response::None
                    })
            }
            // Routed to `apply_mesh` by the caller; kept total rather than
            // panicking inside apply.
            Request::Set { .. }
            | Request::InitCluster { .. }
            | Request::CreateInvite { .. }
            | Request::RegisterNode { .. }
            | Request::SetEndpoint { .. }
            | Request::AdvanceClock { .. }
            | Request::RenewLease { .. }
            | Request::RemoveNode { .. }
            | Request::SetLease { .. }
            | Request::CreateDataKey(_)
            | Request::SetClusterKeyId(_)
            | Request::PlaceReplicas(_)
            | Request::ReportNode(_)
            | Request::SetStatusInterval(_)
            | Request::AllocateServiceIp(_)
            | Request::ReleaseServiceIp(_) => Err(Rejection::new(
                faber_api_types::error_code::INTERNAL,
                "not a project or workload command",
            )),
        };
        Ok(match result {
            Ok(resp) => {
                for (key, record) in &writes {
                    match record {
                        Some(record) => pending.put(key, record)?,
                        None => pending.delete(key),
                    }
                }
                if projects != stored {
                    pending.put(KEY_PROJECTS, &projects)?;
                }
                resp
            }
            // Discard `projects`: a rejection leaves what is stored untouched.
            Err(r) => r.into(),
        })
    }

    /// The record of an environment as seen by entries earlier in the
    /// current batch. `None` without a read when `projects` does not have
    /// the environment: the command is refused then, whatever the names
    /// hold.
    fn environment(
        &self,
        pending: &Pending<'_>,
        projects: &ProjectIndex,
        project: &str,
        environment: &str,
    ) -> Result<Option<EnvironmentRecord>, StorageError> {
        let known = projects
            .projects
            .get(project)
            .is_some_and(|p| p.environments.contains_key(environment));
        if !known {
            return Ok(None);
        }
        self.typed(pending, &env_key(project, environment))
    }

    /// Applies one mesh command. A refused command becomes
    /// `Response::Rejected` and writes nothing; only storage failures are
    /// errors.
    ///
    /// `RegisterNode` and `RemoveNode` are checked against the raft
    /// membership the entry sees (`member_ids`): a node's mesh record is
    /// only deleted after a membership entry without the node was applied.
    fn apply_mesh(
        &self,
        pending: &mut Pending<'_>,
        req: Request,
    ) -> Result<Response, StorageError> {
        if let Request::InitCluster {
            mesh,
            founder,
            containerd,
        } = req
        {
            if self.current(&pending.values, KEY_MESH)?.is_some() {
                return Ok(reject(
                    faber_api_types::error_code::ALREADY_INITIALIZED,
                    "the mesh is already initialized",
                ));
            }
            return Ok(match MeshState::init(mesh, founder) {
                Ok(state) => {
                    pending.put(KEY_MESH, &state)?;
                    pending.put(KEY_CONTAINERD, &containerd)?;
                    Response::None
                }
                Err(r) => r.into(),
            });
        }

        let Some(mut state) = self.mesh(pending)? else {
            return Ok(reject(
                faber_api_types::error_code::MESH_NOT_CONFIGURED,
                "the cluster has no mesh",
            ));
        };
        let result = match req {
            Request::CreateInvite {
                invite_id,
                public_key,
                endpoint,
                name,
                ttl_secs,
                at,
            } => state
                .create_invite(invite_id, public_key, endpoint, name, ttl_secs, at)
                .map(Response::from),
            Request::RegisterNode {
                invite_id,
                node_id,
                endpoint,
                instance_id,
                at,
            } => {
                let members = self.member_ids(pending)?;
                state
                    .register_node(&invite_id, node_id, &endpoint, &instance_id, at, &members)
                    .map(|()| Response::None)
            }
            Request::SetEndpoint { node_id, endpoint } => state
                .set_endpoint(node_id, endpoint)
                .map(|()| Response::None),
            Request::AdvanceClock { elapsed_secs } => {
                state.advance_clock(elapsed_secs).map(|()| Response::None)
            }
            Request::RenewLease {
                node_id,
                instance_id,
            } => state
                .renew_lease(node_id, &instance_id)
                .map(|()| Response::None),
            Request::RemoveNode { node_id } => {
                let members = self.member_ids(pending)?;
                state
                    .remove_node(node_id, &members)
                    .map(|()| Response::None)
            }
            Request::SetLease { secs } => state.set_lease(secs).map(|()| Response::None),
            // Handled by the caller and above; kept total rather than panicking
            // inside apply.
            Request::Set { .. }
            | Request::InitCluster { .. }
            | Request::CreateProject { .. }
            | Request::DeleteProject { .. }
            | Request::AddEnvironment { .. }
            | Request::RemoveEnvironment { .. }
            | Request::ApplyWorkloads { .. }
            | Request::RemoveWorkloads { .. }
            | Request::CreateDataKey(_)
            | Request::SetClusterKeyId(_)
            | Request::PlaceReplicas(_)
            | Request::ReportNode(_)
            | Request::SetStatusInterval(_)
            | Request::AllocateServiceIp(_)
            | Request::ReleaseServiceIp(_) => Err(Rejection::new(
                faber_api_types::error_code::INTERNAL,
                "not a mesh command",
            )),
        };
        Ok(match result {
            Ok(resp) => {
                pending.put(KEY_MESH, &state)?;
                resp
            }
            // Discard `state`: a rejection leaves the stored record untouched.
            Err(r) => r.into(),
        })
    }

    fn meta<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, StorageError> {
        match self
            .db
            .get_cf(cf(&self.db, CF_SM_META), key)
            .map_err(read_err)?
        {
            Some(b) => Ok(Some(decode(&b)?)),
            None => Ok(None),
        }
    }

    /// Value of `key` as seen by entries earlier in the current batch.
    fn current(
        &self,
        pending: &HashMap<String, Option<Value>>,
        key: &str,
    ) -> Result<Option<Value>, StorageError> {
        // A key the batch deleted is in `pending` too, as `None`.
        if let Some(v) = pending.get(key) {
            return Ok(v.clone());
        }
        match self
            .db
            .get_cf(cf(&self.db, CF_SM_DATA), key.as_bytes())
            .map_err(read_err)?
        {
            Some(b) => Ok(Some(decode(&b)?)),
            None => Ok(None),
        }
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let last_applied: Option<LogId> = self.meta::<Option<LogId>>(KEY_LAST_APPLIED)?.flatten();
        let membership: StoredMembership = self.meta(KEY_LAST_MEMBERSHIP)?.unwrap_or_default();
        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let meta = cf(&self.db, CF_SM_META);
        let mut pending = Pending {
            data: cf(&self.db, CF_SM_DATA),
            batch: WriteBatch::default(),
            values: HashMap::new(),
            membership: None,
        };
        let mut last_applied = None;
        let mut replies = Vec::new();

        for entry in entries {
            last_applied = Some(entry.log_id);
            replies.push(self.apply_entry(&mut pending, entry)?);
        }

        let Some(last_applied) = last_applied else {
            return Ok(replies);
        };
        let mut batch = pending.batch;
        batch.put_cf(meta, KEY_LAST_APPLIED, encode(&Some(last_applied))?);
        if let Some(m) = pending.membership {
            batch.put_cf(meta, KEY_LAST_MEMBERSHIP, encode(&m)?);
        }
        write_synced(&self.db, batch).map_err(|e| {
            StorageError::from(StorageIOError::apply(last_applied, AnyError::new(&e)))
        })?;
        self.notify();
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            db: self.db.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let bytes = snapshot.into_inner();
        let kvs: BTreeMap<String, Value> = serde_json::from_slice(&bytes)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let data = cf(&self.db, CF_SM_DATA);
        let sm_meta = cf(&self.db, CF_SM_META);
        let mut batch = WriteBatch::default();

        // Replace sm_data wholesale: delete every existing key, then write the
        // snapshot contents, all in the same batch.
        for item in self.db.iterator_cf(data, IteratorMode::Start) {
            let (key, _) = item.map_err(read_err)?;
            batch.delete_cf(data, key);
        }
        for (key, value) in &kvs {
            batch.put_cf(data, key.as_bytes(), encode(value)?);
        }
        batch.put_cf(sm_meta, KEY_LAST_APPLIED, encode(&meta.last_log_id)?);
        batch.put_cf(sm_meta, KEY_LAST_MEMBERSHIP, encode(&meta.last_membership)?);
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        };
        batch.put_cf(sm_meta, KEY_SNAPSHOT, encode(&stored)?);
        write_synced(&self.db, batch)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        self.notify();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let stored: Option<StoredSnapshot> = self.meta(KEY_SNAPSHOT)?;
        Ok(stored.map(|s| Snapshot {
            meta: s.meta,
            snapshot: Box::new(Cursor::new(s.data)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let db = &self.db;
        let data_cf = cf(db, CF_SM_DATA);
        let meta_cf = cf(db, CF_SM_META);

        // Read data and applied state from one consistent view.
        let (kvs, last_applied, last_membership) = {
            let view = db.snapshot();
            let last_applied: Option<LogId> =
                match view.get_cf(meta_cf, KEY_LAST_APPLIED).map_err(read_err)? {
                    Some(b) => decode::<Option<LogId>>(&b)?,
                    None => None,
                };
            let last_membership: StoredMembership = match view
                .get_cf(meta_cf, KEY_LAST_MEMBERSHIP)
                .map_err(read_err)?
            {
                Some(b) => decode(&b)?,
                None => StoredMembership::default(),
            };
            let mut kvs = serde_json::Map::new();
            for item in view.iterator_cf(data_cf, IteratorMode::Start) {
                let (key, value) = item.map_err(read_err)?;
                let key = String::from_utf8(key.to_vec())
                    .map_err(|e| StorageIOError::read_state_machine(&e))?;
                kvs.insert(key, decode(&value)?);
            }
            (kvs, last_applied, last_membership)
        };

        let counter: u64 = match db.get_cf(meta_cf, KEY_SNAPSHOT_COUNTER).map_err(read_err)? {
            Some(b) => decode(&b)?,
            None => 0,
        } + 1;

        let snapshot_id = match &last_applied {
            Some(id) => format!("{}-{}-{}", id.leader_id, id.index, counter),
            None => format!("--{counter}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        let bytes = encode(&kvs)?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };

        let mut batch = WriteBatch::default();
        batch.put_cf(meta_cf, KEY_SNAPSHOT_COUNTER, encode(&counter)?);
        batch.put_cf(meta_cf, KEY_SNAPSHOT, encode(&stored)?);
        write_synced(db, batch)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

/// Errors from [`StateReader`].
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("storage is closed")]
    Closed,
    #[error("storage read failed: {0}")]
    Storage(String),
}

/// Direct reads from `sm_data`. Holds only a weak handle so that shutting
/// Raft down closes the database.
#[derive(Debug, Clone)]
pub struct StateReader {
    db: Weak<DB>,
    /// Receiving end of the state machine's change channel. It does not keep
    /// the database or the sender alive.
    changes: watch::Receiver<u64>,
}

impl StateReader {
    pub(crate) fn new(db: Weak<DB>, changes: watch::Receiver<u64>) -> Self {
        Self { db, changes }
    }

    /// Whether the database is closed: raft has dropped the storage, so
    /// every read from here on fails with [`ReadError::Closed`].
    pub fn is_closed(&self) -> bool {
        self.db.strong_count() == 0
    }

    /// A receiver that changes after every synced apply batch and every
    /// snapshot install. The value is a counter with no meaning beyond
    /// "something may have changed"; re-read state after each tick.
    /// `changed()` returns `Err` once the storage is closed.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        let mut rx = self.changes.clone();
        rx.borrow_and_update();
        rx
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, ReadError> {
        let db = self.db.upgrade().ok_or(ReadError::Closed)?;
        let bytes = db
            .get_cf(cf(&db, CF_SM_DATA), key.as_bytes())
            .map_err(|e| ReadError::Storage(e.to_string()))?;
        bytes
            .map(|b| serde_json::from_slice(&b).map_err(|e| ReadError::Storage(e.to_string())))
            .transpose()
    }

    /// The replicated mesh, or `None` before `InitCluster`.
    pub fn mesh(&self) -> Result<Option<MeshState>, ReadError> {
        self.typed(KEY_MESH)
    }

    /// The replicated container runtime spec (`config/containerd`), or `None`
    /// before `InitCluster`.
    pub fn containerd(&self) -> Result<Option<ContainerRuntimeSpec>, ReadError> {
        self.typed(KEY_CONTAINERD)
    }

    /// Every project and, per environment, the figures of its record.
    /// Empty before the first `CreateProject`.
    pub fn projects(&self) -> Result<ProjectIndex, ReadError> {
        Ok(self.typed(KEY_PROJECTS)?.unwrap_or_default())
    }

    /// The record of one environment with its workloads, or `None` when
    /// the project does not have the environment.
    pub fn environment(
        &self,
        project: &str,
        environment: &str,
    ) -> Result<Option<EnvironmentRecord>, ReadError> {
        self.typed(&env_key(project, environment))
    }

    /// Every environment record as `(project, environment, record)`, sorted
    /// by the two names. All of them are read from one view of the
    /// database, so they are the records of one applied batch.
    pub fn environments(&self) -> Result<Vec<(String, String, EnvironmentRecord)>, ReadError> {
        let storage = |e: &dyn std::fmt::Display| ReadError::Storage(e.to_string());
        let db = self.db.upgrade().ok_or(ReadError::Closed)?;
        let view = db.snapshot();
        let from = IteratorMode::From(ENV_PREFIX.as_bytes(), Direction::Forward);
        let mut found = Vec::new();
        for item in view.iterator_cf(cf(&db, CF_SM_DATA), from) {
            let (key, value) = item.map_err(|e| storage(&e))?;
            let Some(names) = key.strip_prefix(ENV_PREFIX.as_bytes()) else {
                break;
            };
            let (project, environment) = std::str::from_utf8(names)
                .ok()
                .and_then(|names| names.split_once('/'))
                .ok_or_else(|| storage(&"an environment key is not env/<project>/<environment>"))?;
            let record = serde_json::from_slice(&value).map_err(|e| storage(&e))?;
            found.push((project.to_string(), environment.to_string(), record));
        }
        // Key order is not name order: `-` sorts before `/`.
        found.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        Ok(found)
    }

    fn typed<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, ReadError> {
        self.get(key)?
            .map(|v| serde_json::from_value(v).map_err(|e| ReadError::Storage(e.to_string())))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use openraft::storage::{RaftLogStorage, RaftStateMachine};
    use openraft::testing::log_id;
    use openraft::{EntryPayload, Membership};
    use serde_json::json;

    use super::*;
    use crate::raft::store::open;
    use crate::raft::types::{Node, Vote};

    fn set(term: u64, index: u64, key: &str, value: Value) -> Entry {
        Entry {
            log_id: log_id(term, 1, index),
            payload: EntryPayload::Normal(Request::Set {
                key: key.into(),
                value,
            }),
        }
    }

    #[tokio::test]
    async fn vote_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = open(dir.path()).unwrap();
            s.log_store.save_vote(&Vote::new(7, 3)).await.unwrap();
        }
        let mut s = open(dir.path()).unwrap();
        assert_eq!(
            s.log_store.read_vote().await.unwrap(),
            Some(Vote::new(7, 3))
        );
        drop(s);
    }

    #[tokio::test]
    async fn apply_returns_previous_values_within_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                set(1, 1, "a", json!(1)),
                set(1, 2, "a", json!(2)),
                set(1, 3, "a", json!(3)),
            ])
            .await
            .unwrap();
        assert_eq!(
            replies,
            vec![
                Response::Previous(None),
                Response::Previous(Some(json!(1))),
                Response::Previous(Some(json!(2))),
            ]
        );
        drop(s);
    }

    #[tokio::test]
    async fn apply_persists_data_and_applied_state_together() {
        let dir = tempfile::tempdir().unwrap();
        let membership = Membership::new(
            vec![[1u64].into_iter().collect()],
            [(1u64, Node::new("n1:1"))]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
        );
        {
            let mut s = open(dir.path()).unwrap();
            s.state_machine
                .apply([
                    Entry {
                        log_id: log_id(1, 1, 1),
                        payload: EntryPayload::Membership(membership.clone()),
                    },
                    set(1, 2, "a/b", json!({"x": 1})),
                    set(1, 3, "a/c", json!(true)),
                    set(1, 4, "a/c", json!(false)),
                ])
                .await
                .unwrap();
        }
        let mut s = open(dir.path()).unwrap();
        let (applied, stored) = s.state_machine.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 1, 4)));
        assert_eq!(stored.log_id(), &Some(log_id(1, 1, 1)));
        assert_eq!(stored.membership(), &membership);
        assert_eq!(s.reader.get("a/b").unwrap(), Some(json!({"x": 1})));
        assert_eq!(s.reader.get("a/c").unwrap(), Some(json!(false)));
        assert_eq!(s.reader.get("missing").unwrap(), None);
        drop(s);
    }

    #[tokio::test]
    async fn snapshot_install_replaces_all_data_and_ids_are_unique() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();

        a.state_machine
            .apply([set(1, 1, "keep", json!(1))])
            .await
            .unwrap();
        b.state_machine
            .apply([set(1, 1, "stale", json!(0))])
            .await
            .unwrap();

        let mut builder = a.state_machine.get_snapshot_builder().await;
        let s1 = builder.build_snapshot().await.unwrap();
        let s2 = builder.build_snapshot().await.unwrap();
        assert_ne!(s1.meta.snapshot_id, s2.meta.snapshot_id);

        b.state_machine
            .install_snapshot(&s2.meta, s2.snapshot)
            .await
            .unwrap();
        assert_eq!(b.reader.get("keep").unwrap(), Some(json!(1)));
        assert_eq!(b.reader.get("stale").unwrap(), None);
        assert_eq!(
            b.state_machine.applied_state().await.unwrap().0,
            Some(log_id(1, 1, 1))
        );
        drop((a, b));
    }

    #[tokio::test]
    async fn snapshot_counter_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let first = {
            let mut s = open(dir.path()).unwrap();
            let mut builder = s.state_machine.get_snapshot_builder().await;
            builder.build_snapshot().await.unwrap().meta.snapshot_id
        };
        let mut s = open(dir.path()).unwrap();
        let mut builder = s.state_machine.get_snapshot_builder().await;
        let second = builder.build_snapshot().await.unwrap().meta.snapshot_id;
        assert_ne!(first, second);
        drop((builder, s));
    }

    // --- mesh commands -----------------------------------------------------

    use crate::mesh::state::{KEY_CONTAINERD, KEY_MESH};
    use crate::mesh::{MeshConfig, MeshState, NodeRecord};
    use faber_api_types::error_code as ec;

    fn normal(index: u64, req: Request) -> Entry {
        Entry {
            log_id: log_id(1, 1, index),
            payload: EntryPayload::Normal(req),
        }
    }

    fn founder() -> NodeRecord {
        NodeRecord {
            node_id: 1,
            public_key: "pk1".into(),
            mesh_ip: "10.99.0.1".parse().unwrap(),
            endpoint: Some("203.0.113.1:51820".into()),
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: String::new(),
            lease_renewed_at: 0,
        }
    }

    /// The founder as `InitCluster` stores it: `founder()` sends no name, so
    /// it gets the default one.
    fn stored_founder() -> NodeRecord {
        NodeRecord {
            name: "node-1".into(),
            ..founder()
        }
    }

    fn init_cluster() -> Request {
        Request::InitCluster {
            mesh: MeshConfig::default(),
            founder: founder(),
            containerd: ContainerRuntimeSpec::default(),
        }
    }

    fn create_invite(n: u8) -> Request {
        Request::CreateInvite {
            invite_id: format!("{n:02x}").repeat(16),
            public_key: format!("pk{n}"),
            endpoint: format!("198.51.100.{n}:51820"),
            name: format!("node-{n}"),
            ttl_secs: 60,
            at: 1_000,
        }
    }

    fn rejected(code: &str) -> impl Fn(&Response) -> bool + '_ {
        move |r| matches!(r, Response::Rejected { code: c, .. } if c == code)
    }

    async fn last_index(s: &mut crate::raft::store::Storage) -> Option<u64> {
        s.state_machine
            .applied_state()
            .await
            .unwrap()
            .0
            .map(|l| l.index)
    }

    #[tokio::test]
    async fn init_cluster_seeds_mesh_and_containerd() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        assert_eq!(s.reader.mesh().unwrap(), None);
        assert_eq!(s.reader.containerd().unwrap(), None);

        let replies = s
            .state_machine
            .apply([normal(1, init_cluster())])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None]);
        let want = MeshState::init(MeshConfig::default(), founder()).unwrap();
        assert_eq!(s.reader.mesh().unwrap(), Some(want.clone()));
        assert_eq!(
            s.reader.containerd().unwrap(),
            Some(ContainerRuntimeSpec::default())
        );
        assert_eq!(
            s.reader.get(KEY_MESH).unwrap(),
            Some(serde_json::to_value(&want).unwrap())
        );
        drop(s);
    }

    #[tokio::test]
    async fn second_init_cluster_is_rejected_and_still_applied() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let mut other = init_cluster();
        if let Request::InitCluster { founder, .. } = &mut other {
            founder.public_key = "different".into();
        }
        let replies = s
            .state_machine
            .apply([normal(1, init_cluster()), normal(2, other)])
            .await
            .unwrap();
        assert!(
            rejected(ec::ALREADY_INITIALIZED)(&replies[1]),
            "{replies:?}"
        );
        assert_eq!(last_index(&mut s).await, Some(2));
        assert_eq!(
            s.reader.mesh().unwrap().unwrap().nodes[&1],
            stored_founder()
        );

        // Also rejected in a later batch.
        let replies = s
            .state_machine
            .apply([normal(3, init_cluster())])
            .await
            .unwrap();
        assert!(rejected(ec::ALREADY_INITIALIZED)(&replies[0]));
        assert_eq!(last_index(&mut s).await, Some(3));
        drop(s);
    }

    #[tokio::test]
    async fn init_cluster_with_bad_founder_is_rejected_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let mut bad = init_cluster();
        if let Request::InitCluster { founder, .. } = &mut bad {
            founder.mesh_ip = "192.0.2.1".parse().unwrap();
        }
        let replies = s.state_machine.apply([normal(1, bad)]).await.unwrap();
        assert!(rejected(ec::BAD_REQUEST)(&replies[0]));
        assert_eq!(s.reader.mesh().unwrap(), None);
        assert_eq!(s.reader.containerd().unwrap(), None);
        assert_eq!(last_index(&mut s).await, Some(1));
        drop(s);
    }

    #[tokio::test]
    async fn set_rejects_reserved_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                set(1, 1, KEY_MESH, json!({"forged": true})),
                set(1, 2, KEY_CONTAINERD, json!({})),
            ])
            .await
            .unwrap();
        assert!(replies.iter().all(rejected(ec::BAD_REQUEST)), "{replies:?}");
        assert_eq!(s.reader.get(KEY_MESH).unwrap(), None);
        assert_eq!(s.reader.get(KEY_CONTAINERD).unwrap(), None);
        assert_eq!(last_index(&mut s).await, Some(2));
        drop(s);
    }

    #[tokio::test]
    async fn mesh_commands_without_mesh_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                normal(1, create_invite(2)),
                normal(
                    2,
                    Request::RegisterNode {
                        invite_id: "ab".repeat(16),
                        node_id: 2,
                        endpoint: "198.51.100.2:51820".into(),
                        instance_id: "cd".repeat(16),
                        at: 1_000,
                    },
                ),
                normal(
                    3,
                    Request::SetEndpoint {
                        node_id: 1,
                        endpoint: "192.0.2.1:51820".into(),
                    },
                ),
            ])
            .await
            .unwrap();
        assert!(
            replies.iter().all(rejected(ec::MESH_NOT_CONFIGURED)),
            "{replies:?}"
        );
        assert_eq!(s.reader.mesh().unwrap(), None);
        assert_eq!(last_index(&mut s).await, Some(3));
        drop(s);
    }

    #[tokio::test]
    async fn one_batch_sees_earlier_mesh_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                normal(1, init_cluster()),
                normal(2, create_invite(2)),
                normal(3, create_invite(3)),
                normal(
                    4,
                    Request::RegisterNode {
                        invite_id: "02".repeat(16),
                        node_id: 2,
                        endpoint: "198.51.100.2:51820".into(),
                        instance_id: "cd".repeat(16),
                        at: 1_001,
                    },
                ),
                normal(
                    5,
                    Request::SetEndpoint {
                        node_id: 2,
                        endpoint: "198.51.100.22:51820".into(),
                    },
                ),
            ])
            .await
            .unwrap();
        assert_eq!(
            replies,
            vec![
                Response::None,
                Response::Invited {
                    node_id: 2,
                    mesh_ip: "10.99.0.2".parse().unwrap(),
                    expires_at: 1_060,
                    name: "node-2".into(),
                    replaces: None,
                },
                Response::Invited {
                    node_id: 3,
                    mesh_ip: "10.99.0.3".parse().unwrap(),
                    expires_at: 1_060,
                    name: "node-3".into(),
                    replaces: None,
                },
                Response::None,
                Response::None,
            ]
        );
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh.next_node_id, 4);
        assert_eq!(
            mesh.nodes[&2].endpoint.as_deref(),
            Some("198.51.100.22:51820")
        );
        assert_eq!(mesh.invites.len(), 1);
        assert_eq!(mesh.nodes[&2].name, "node-2");
        assert_eq!(mesh.invites[&"03".repeat(16)].name, "node-3");
        drop(s);
    }

    #[tokio::test]
    async fn rejected_invite_in_batch_keeps_earlier_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                normal(1, init_cluster()),
                normal(2, create_invite(2)),
                // Same invite id and key again.
                normal(3, create_invite(2)),
                normal(4, create_invite(3)),
            ])
            .await
            .unwrap();
        assert!(rejected(ec::BAD_REQUEST)(&replies[2]), "{replies:?}");
        assert!(matches!(replies[3], Response::Invited { node_id: 3, .. }));
        assert_eq!(last_index(&mut s).await, Some(4));
        drop(s);
    }

    #[tokio::test]
    async fn watch_ticks_after_apply_and_snapshot_install() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();
        let mut rx = a.reader.subscribe();
        let start = *rx.borrow_and_update();

        // An empty batch does not tick.
        a.state_machine.apply(Vec::<Entry>::new()).await.unwrap();
        assert!(!rx.has_changed().unwrap());

        a.state_machine
            .apply([normal(1, init_cluster())])
            .await
            .unwrap();
        assert!(rx.has_changed().unwrap());
        let after_apply = *rx.borrow_and_update();
        assert!(after_apply > start);

        // A rejected command is still an applied batch.
        a.state_machine
            .apply([normal(2, init_cluster())])
            .await
            .unwrap();
        assert!(rx.has_changed().unwrap());
        rx.borrow_and_update();

        // Readers made from the state machine share the channel.
        let mut rx_sm = a.state_machine.reader().subscribe();
        rx_sm.borrow_and_update();

        let mut rx_b = b.reader.subscribe();
        rx_b.borrow_and_update();
        let snap = a
            .state_machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        b.state_machine
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();
        assert!(rx_b.has_changed().unwrap());
        assert!(!rx_sm.has_changed().unwrap(), "a is unaffected by b");

        a.state_machine
            .apply([normal(3, create_invite(2))])
            .await
            .unwrap();
        assert!(rx_sm.has_changed().unwrap());

        rx.borrow_and_update();
        drop((a, b));
        // Closing the storage closes the channel.
        assert!(rx.changed().await.is_err());
    }

    #[tokio::test]
    async fn snapshot_carries_mesh_and_containerd() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();
        a.state_machine
            .apply([normal(1, init_cluster()), normal(2, create_invite(2))])
            .await
            .unwrap();
        let snap = a
            .state_machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        b.state_machine
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();
        assert_eq!(b.reader.mesh().unwrap(), a.reader.mesh().unwrap());
        assert!(b.reader.mesh().unwrap().is_some());
        assert_eq!(
            b.reader.containerd().unwrap(),
            Some(ContainerRuntimeSpec::default())
        );
        drop((a, b));
    }

    // --- membership seen by mesh commands ----------------------------------

    /// A membership entry with `voters` voting and `learners` not.
    fn membership(index: u64, voters: &[u64], learners: &[u64]) -> Entry {
        let nodes = voters
            .iter()
            .chain(learners)
            .map(|id| (*id, Node::new(format!("n{id}:1"))))
            .collect::<BTreeMap<_, _>>();
        Entry {
            log_id: log_id(1, 1, index),
            payload: EntryPayload::Membership(Membership::new(
                vec![voters.iter().copied().collect()],
                nodes,
            )),
        }
    }

    fn pending(sm: &StateMachineStore) -> Pending<'_> {
        Pending {
            data: cf(&sm.db, CF_SM_DATA),
            batch: WriteBatch::default(),
            values: HashMap::new(),
            membership: None,
        }
    }

    fn ids(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    #[tokio::test]
    async fn member_ids_are_empty_before_any_membership() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let sm = &s.state_machine;
        assert_eq!(sm.member_ids(&pending(sm)).unwrap(), ids(&[]));
        s.state_machine
            .apply([set(1, 1, "a", json!(1))])
            .await
            .unwrap();
        let sm = &s.state_machine;
        assert_eq!(sm.member_ids(&pending(sm)).unwrap(), ids(&[]));
        drop(s);
    }

    #[tokio::test]
    async fn member_ids_come_from_the_membership_an_earlier_batch_stored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = open(dir.path()).unwrap();
            s.state_machine
                .apply([membership(1, &[1, 2], &[3]), set(1, 2, "a", json!(1))])
                .await
                .unwrap();
            // Voters and learners both count.
            let sm = &s.state_machine;
            assert_eq!(sm.member_ids(&pending(sm)).unwrap(), ids(&[1, 2, 3]));

            // A batch without a membership entry leaves it as it was.
            s.state_machine
                .apply([set(1, 3, "a", json!(2))])
                .await
                .unwrap();
            let sm = &s.state_machine;
            assert_eq!(sm.member_ids(&pending(sm)).unwrap(), ids(&[1, 2, 3]));
        }
        // And it is read from disk after a restart.
        let s = open(dir.path()).unwrap();
        let sm = &s.state_machine;
        assert_eq!(sm.member_ids(&pending(sm)).unwrap(), ids(&[1, 2, 3]));
        drop(s);
    }

    /// A command sees the membership entry applied before it in the same
    /// batch, not the one on disk, and not one that follows it.
    #[tokio::test]
    async fn member_ids_follow_membership_entries_within_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([membership(1, &[1, 2, 3], &[])])
            .await
            .unwrap();

        let sm = &s.state_machine;
        let mut p = pending(sm);
        assert_eq!(sm.member_ids(&p).unwrap(), ids(&[1, 2, 3]));
        // Entries that are not membership changes do not disturb it.
        sm.apply_entry(&mut p, set(1, 2, "a", json!(1))).unwrap();
        sm.apply_entry(&mut p, normal(3, init_cluster())).unwrap();
        assert_eq!(sm.member_ids(&p).unwrap(), ids(&[1, 2, 3]));
        // Node 3 leaves: visible to the entries after it, before any write.
        sm.apply_entry(&mut p, membership(4, &[1, 2], &[])).unwrap();
        assert_eq!(sm.member_ids(&p).unwrap(), ids(&[1, 2]));
        sm.apply_entry(&mut p, normal(5, create_invite(2))).unwrap();
        assert_eq!(sm.member_ids(&p).unwrap(), ids(&[1, 2]));
        // The latest membership entry of the batch wins.
        sm.apply_entry(&mut p, membership(6, &[1, 2], &[4]))
            .unwrap();
        assert_eq!(sm.member_ids(&p).unwrap(), ids(&[1, 2, 4]));
        // Nothing was written: another batch still sees what is on disk.
        assert_eq!(sm.member_ids(&pending(sm)).unwrap(), ids(&[1, 2, 3]));
        drop(p);
        drop(s);
    }

    /// The same entries give the same membership whether they arrive as one
    /// batch or several.
    #[tokio::test]
    async fn member_ids_do_not_depend_on_how_entries_are_batched() {
        let entries = || {
            vec![
                membership(1, &[1], &[]),
                normal(2, init_cluster()),
                membership(3, &[1], &[2]),
                normal(4, create_invite(2)),
                membership(5, &[1, 2], &[]),
            ]
        };
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();

        a.state_machine.apply(entries()).await.unwrap();
        for entry in entries() {
            b.state_machine.apply([entry]).await.unwrap();
        }
        let (sm_a, sm_b) = (&a.state_machine, &b.state_machine);
        assert_eq!(sm_a.member_ids(&pending(sm_a)).unwrap(), ids(&[1, 2]));
        assert_eq!(sm_b.member_ids(&pending(sm_b)).unwrap(), ids(&[1, 2]));
        assert_eq!(
            a.state_machine.applied_state().await.unwrap(),
            b.state_machine.applied_state().await.unwrap()
        );
        drop((a, b));
    }

    #[tokio::test]
    async fn member_ids_come_from_an_installed_snapshot() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();
        a.state_machine
            .apply([membership(1, &[1, 2], &[5]), normal(2, init_cluster())])
            .await
            .unwrap();
        b.state_machine
            .apply([membership(1, &[9], &[])])
            .await
            .unwrap();
        let snap = a
            .state_machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        b.state_machine
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();
        let sm = &b.state_machine;
        assert_eq!(sm.member_ids(&pending(sm)).unwrap(), ids(&[1, 2, 5]));
        drop((a, b));
    }

    /// The lease commands reach the mesh state: without a mesh they are
    /// refused like every other mesh command.
    #[tokio::test]
    async fn lease_commands_without_mesh_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                normal(1, Request::AdvanceClock { elapsed_secs: 30 }),
                normal(
                    2,
                    Request::RenewLease {
                        node_id: 1,
                        instance_id: "cd".repeat(16),
                    },
                ),
                normal(3, Request::RemoveNode { node_id: 2 }),
                normal(4, Request::SetLease { secs: 600 }),
            ])
            .await
            .unwrap();
        assert!(
            replies.iter().all(rejected(ec::MESH_NOT_CONFIGURED)),
            "{replies:?}"
        );
        assert_eq!(last_index(&mut s).await, Some(4));
        drop(s);
    }

    fn init_cluster_with_range(range: &str, prefix: u8) -> Request {
        Request::InitCluster {
            mesh: MeshConfig {
                container_cidr: Some(range.parse().unwrap()),
                container_subnet_prefix: prefix,
                ..MeshConfig::default()
            },
            founder: founder(),
            containerd: ContainerRuntimeSpec::default(),
        }
    }

    fn register(n: u8) -> Request {
        Request::RegisterNode {
            invite_id: format!("{n:02x}").repeat(16),
            node_id: u64::from(n),
            endpoint: format!("198.51.100.{n}:51820"),
            instance_id: "cd".repeat(16),
            at: 1_001,
        }
    }

    // --- names and leases --------------------------------------------------

    use crate::mesh::state::{MAX_LEASE_SECS, MIN_LEASE_SECS};

    /// `create_invite(n)` for the name `name`.
    fn invite_named(n: u8, name: &str) -> Request {
        let mut req = create_invite(n);
        if let Request::CreateInvite { name: slot, .. } = &mut req {
            *slot = name.into();
        }
        req
    }

    /// A renewal from the machine that `register` registers.
    fn renew(node_id: u64) -> Request {
        Request::RenewLease {
            node_id,
            instance_id: "cd".repeat(16),
        }
    }

    /// Twelve entries, from index `from`, that set the shortest lease and
    /// run the clock one whole lease on. The founder renews just before the
    /// end, so every lease but its own expires.
    fn expire_all_but_founder(from: u64) -> Vec<Entry> {
        let advance = Request::AdvanceClock { elapsed_secs: 30 };
        let mut reqs = vec![Request::SetLease {
            secs: MIN_LEASE_SECS,
        }];
        reqs.extend(std::iter::repeat_n(advance.clone(), 9));
        reqs.push(renew(1));
        reqs.push(advance);
        (from..).zip(reqs).map(|(i, r)| normal(i, r)).collect()
    }

    /// The stored `mesh` value, to show that a command wrote nothing.
    fn raw_mesh(s: &crate::raft::store::Storage) -> Value {
        s.reader.get(KEY_MESH).unwrap().unwrap()
    }

    #[tokio::test]
    async fn lease_commands_apply_and_persist() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                normal(1, init_cluster()),
                // Clamped to a tenth of the default lease.
                normal(
                    2,
                    Request::AdvanceClock {
                        elapsed_secs: 10_000,
                    },
                ),
                normal(3, Request::SetLease { secs: 600 }),
                normal(4, Request::AdvanceClock { elapsed_secs: 45 }),
            ])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None; 4]);
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!((mesh.clock, mesh.lease_secs), (2_925, 600));
        // `SetLease` restarted the founder's lease at the clock.
        assert_eq!(mesh.nodes[&1].lease_renewed_at, 2_880);
        assert_eq!(mesh.nodes[&1].instance_id, None);

        // A later batch starts from the stored clock.
        let replies = s
            .state_machine
            .apply([
                normal(5, renew(1)),
                // An unknown node: the retry of a removal already applied.
                normal(6, Request::RemoveNode { node_id: 7 }),
            ])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None; 2]);
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh.nodes[&1].lease_renewed_at, 2_925);
        // The founder's record adopted the machine that renewed.
        assert_eq!(mesh.nodes[&1].instance_id, Some("cd".repeat(16)));
        assert_eq!(mesh.nodes.len(), 1);
        assert_eq!(last_index(&mut s).await, Some(6));
        drop(s);
    }

    #[tokio::test]
    async fn rejected_lease_commands_are_applied_and_write_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([
                membership(1, &[1], &[]),
                normal(2, init_cluster()),
                normal(3, create_invite(2)),
                normal(4, register(2)),
            ])
            .await
            .unwrap();
        let before = raw_mesh(&s);

        let replies = s
            .state_machine
            .apply([
                normal(
                    5,
                    Request::SetLease {
                        secs: MIN_LEASE_SECS - 1,
                    },
                ),
                normal(
                    6,
                    Request::SetLease {
                        secs: MAX_LEASE_SECS + 1,
                    },
                ),
                normal(7, renew(9)),
                normal(
                    8,
                    Request::RenewLease {
                        node_id: 2,
                        instance_id: "ef".repeat(16),
                    },
                ),
                // Out of the membership, but its lease is live.
                normal(9, Request::RemoveNode { node_id: 2 }),
                normal(10, invite_named(3, "")),
                normal(11, invite_named(3, "Not-A-Label")),
            ])
            .await
            .unwrap();
        let codes: Vec<&str> = replies
            .iter()
            .map(|r| match r {
                Response::Rejected { code, .. } => code.as_str(),
                other => panic!("not rejected: {other:?}"),
            })
            .collect();
        assert_eq!(
            codes,
            vec![
                ec::BAD_REQUEST,
                ec::BAD_REQUEST,
                ec::MESH_NODE_REMOVED,
                ec::FORBIDDEN,
                ec::NOT_READY,
                ec::BAD_REQUEST,
                ec::BAD_REQUEST,
            ]
        );
        assert_eq!(raw_mesh(&s), before);
        assert_eq!(last_index(&mut s).await, Some(11));
        drop(s);
    }

    /// A two node cluster, both raft members, where node 2's lease has
    /// expired and the founder's has not. Returns the next free index.
    async fn with_expired_node_2(s: &mut crate::raft::store::Storage) -> u64 {
        let mut entries = vec![
            membership(1, &[1, 2], &[]),
            normal(2, init_cluster_with_range("10.200.0.0/16", 24)),
            normal(3, create_invite(2)),
            normal(4, register(2)),
        ];
        entries.extend(expire_all_but_founder(5));
        let replies = s.state_machine.apply(entries).await.unwrap();
        assert!(
            replies
                .iter()
                .all(|r| matches!(r, Response::None | Response::Invited { .. })),
            "{replies:?}"
        );
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!((mesh.clock, mesh.lease_secs), (300, MIN_LEASE_SECS));
        assert!(mesh.lease_expired(&mesh.nodes[&2]));
        assert!(!mesh.lease_expired(&mesh.nodes[&1]));
        17
    }

    #[tokio::test]
    async fn remove_node_waits_for_the_membership_across_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let next = with_expired_node_2(&mut s).await;
        let before = raw_mesh(&s);

        // Expiry is sticky, and the node is still a raft member.
        let replies = s
            .state_machine
            .apply([
                normal(next, renew(2)),
                normal(next + 1, Request::RemoveNode { node_id: 2 }),
            ])
            .await
            .unwrap();
        assert!(rejected(ec::MESH_LEASE_EXPIRED)(&replies[0]), "{replies:?}");
        assert!(rejected(ec::NOT_READY)(&replies[1]), "{replies:?}");
        assert_eq!(raw_mesh(&s), before);
        assert_eq!(last_index(&mut s).await, Some(next + 1));

        s.state_machine
            .apply([membership(next + 2, &[1], &[])])
            .await
            .unwrap();
        let replies = s
            .state_machine
            .apply([normal(next + 3, Request::RemoveNode { node_id: 2 })])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None]);
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), vec![1]);

        // The retry of a new leader changes nothing.
        let before = raw_mesh(&s);
        let replies = s
            .state_machine
            .apply([normal(next + 4, Request::RemoveNode { node_id: 2 })])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None]);
        assert_eq!(raw_mesh(&s), before);
        drop(s);
    }

    #[tokio::test]
    async fn remove_node_waits_for_the_membership_within_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let next = with_expired_node_2(&mut s).await;

        let remove = || Request::RemoveNode { node_id: 2 };
        let replies = s
            .state_machine
            .apply([
                normal(next, remove()),
                // A learner is a member too.
                membership(next + 1, &[1], &[2]),
                normal(next + 2, remove()),
                membership(next + 3, &[1], &[]),
                normal(next + 4, remove()),
                normal(next + 5, remove()),
            ])
            .await
            .unwrap();
        assert!(rejected(ec::NOT_READY)(&replies[0]), "{replies:?}");
        assert!(rejected(ec::NOT_READY)(&replies[2]), "{replies:?}");
        assert_eq!(replies[4..], vec![Response::None; 2]);
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), vec![1]);
        assert_eq!(last_index(&mut s).await, Some(next + 5));

        // The name and subnet are free: a new node gets both, replacing
        // nobody.
        let replies = s
            .state_machine
            .apply([
                normal(next + 6, invite_named(3, "node-2")),
                normal(next + 7, register(3)),
            ])
            .await
            .unwrap();
        assert!(
            matches!(replies[0], Response::Invited { replaces: None, .. }),
            "{replies:?}"
        );
        assert_eq!(replies[1], Response::None);
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh.nodes[&3].name, "node-2");
        assert_eq!(
            mesh.nodes[&3].container_subnet,
            Some("10.200.1.0/24".parse().unwrap())
        );
        assert_eq!(mesh.nodes[&3].lease_renewed_at, 300);
        drop(s);
    }

    /// A two node cluster, both raft members, with a live invite for the
    /// name node 2 holds. Returns the next free index.
    async fn with_takeover_invite(s: &mut crate::raft::store::Storage) -> u64 {
        let replies = s
            .state_machine
            .apply([
                membership(1, &[1, 2], &[]),
                normal(2, init_cluster_with_range("10.200.0.0/16", 24)),
                normal(3, create_invite(2)),
                normal(4, register(2)),
                normal(5, invite_named(3, "node-2")),
            ])
            .await
            .unwrap();
        assert_eq!(
            replies[4],
            Response::Invited {
                node_id: 3,
                mesh_ip: "10.99.0.3".parse().unwrap(),
                expires_at: 1_060,
                name: "node-2".into(),
                replaces: Some(2),
            }
        );
        6
    }

    /// Node 3 holds what node 2 held, and node 2 is gone.
    fn assert_taken_over(mesh: &MeshState) {
        assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), vec![1, 3]);
        assert!(mesh.invites.is_empty());
        let n = &mesh.nodes[&3];
        assert_eq!(n.name, "node-2");
        assert_eq!(n.container_subnet, Some("10.200.1.0/24".parse().unwrap()));
        assert_eq!(n.mesh_ip, std::net::Ipv4Addr::new(10, 99, 0, 3));
    }

    #[tokio::test]
    async fn takeover_waits_for_the_membership_across_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let next = with_takeover_invite(&mut s).await;
        let before = raw_mesh(&s);

        let replies = s
            .state_machine
            .apply([normal(next, register(3))])
            .await
            .unwrap();
        assert!(rejected(ec::NOT_READY)(&replies[0]), "{replies:?}");
        assert_eq!(raw_mesh(&s), before, "the invite and node 2 both stay");
        assert_eq!(last_index(&mut s).await, Some(next));

        s.state_machine
            .apply([membership(next + 1, &[1], &[])])
            .await
            .unwrap();
        let replies = s
            .state_machine
            .apply([normal(next + 2, register(3))])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None]);
        assert_taken_over(&s.reader.mesh().unwrap().unwrap());

        // A retry is answered before the takeover rule and changes nothing,
        // also once node 3 is a member itself.
        let before = raw_mesh(&s);
        let replies = s
            .state_machine
            .apply([
                normal(next + 3, register(3)),
                membership(next + 4, &[1, 3], &[]),
                normal(next + 5, register(3)),
            ])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None; 3]);
        assert_eq!(raw_mesh(&s), before);
        drop(s);
    }

    #[tokio::test]
    async fn takeover_waits_for_the_membership_within_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let next = with_takeover_invite(&mut s).await;

        let replies = s
            .state_machine
            .apply([
                normal(next, register(3)),
                membership(next + 1, &[1], &[]),
                normal(next + 2, register(3)),
                normal(next + 3, register(3)),
            ])
            .await
            .unwrap();
        assert!(rejected(ec::NOT_READY)(&replies[0]), "{replies:?}");
        assert_eq!(replies[1..], vec![Response::None; 3]);
        assert_taken_over(&s.reader.mesh().unwrap().unwrap());
        assert_eq!(last_index(&mut s).await, Some(next + 3));
        drop(s);
    }

    #[tokio::test]
    async fn snapshot_carries_names_and_leases() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();
        let mut init = init_cluster();
        if let Request::InitCluster { founder, .. } = &mut init {
            founder.name = "alpha".into();
        }
        let advance = || Request::AdvanceClock { elapsed_secs: 60 };
        let replies = a
            .state_machine
            .apply([
                normal(1, init),
                normal(2, Request::SetLease { secs: 600 }),
                normal(3, advance()),
                normal(4, advance()),
                normal(5, invite_named(2, "beta")),
                normal(6, register(2)),
                normal(7, advance()),
                normal(8, renew(1)),
                normal(9, invite_named(3, "gamma")),
            ])
            .await
            .unwrap();
        assert!(
            replies
                .iter()
                .all(|r| matches!(r, Response::None | Response::Invited { .. })),
            "{replies:?}"
        );
        let snap = a
            .state_machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        b.state_machine
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        let mesh = b.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh, a.reader.mesh().unwrap().unwrap());
        assert_eq!((mesh.clock, mesh.lease_secs), (180, 600));
        assert_eq!(mesh.nodes[&1].name, "alpha");
        assert_eq!(mesh.nodes[&1].lease_renewed_at, 180);
        assert_eq!(mesh.nodes[&2].name, "beta");
        assert_eq!(mesh.nodes[&2].lease_renewed_at, 120);
        assert_eq!(mesh.invites[&"03".repeat(16)].name, "gamma");

        // The replica carries on from the clock it was given.
        b.state_machine
            .apply([normal(10, advance())])
            .await
            .unwrap();
        assert_eq!(b.reader.mesh().unwrap().unwrap().clock, 240);
        drop((a, b));
    }

    // --- container subnets -------------------------------------------------

    /// An `InitCluster` logged before the container fields existed applies
    /// as a mesh with no container range, and nobody gets a subnet.
    #[tokio::test]
    async fn init_cluster_logged_before_container_fields_applies_without_a_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let old: Request = serde_json::from_value(json!({"InitCluster": {
            "mesh": {"cidr": "10.99.0.0/16", "peer_port": 7401, "wg_port": 51820},
            "founder": {
                "node_id": 1, "public_key": "pk1", "mesh_ip": "10.99.0.1",
                "endpoint": "203.0.113.1:51820", "invite_id": null
            },
            "containerd": ContainerRuntimeSpec::default(),
        }}))
        .unwrap();
        let replies = s
            .state_machine
            .apply([
                normal(1, old),
                normal(2, create_invite(2)),
                normal(3, register(2)),
            ])
            .await
            .unwrap();
        assert_eq!(replies[0], Response::None);
        assert_eq!(replies[2], Response::None);
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh.config.container_cidr, None);
        assert_eq!(mesh.nodes[&1], stored_founder());
        assert_eq!(mesh.nodes[&1].container_subnet, None);
        assert_eq!(mesh.nodes[&2].container_subnet, None);
        // The stored record has no subnet key at all.
        let raw = s.reader.get(KEY_MESH).unwrap().unwrap();
        assert!(raw["nodes"]["1"].get("container_subnet").is_none(), "{raw}");
        assert!(raw["nodes"]["2"].get("container_subnet").is_none(), "{raw}");
        drop(s);
    }

    #[tokio::test]
    async fn init_cluster_with_bad_container_range_is_rejected_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                // Overlaps the mesh CIDR.
                normal(1, init_cluster_with_range("10.99.128.0/17", 24)),
                // Host bits set.
                normal(2, init_cluster_with_range("10.200.1.0/16", 24)),
            ])
            .await
            .unwrap();
        assert!(replies.iter().all(rejected(ec::BAD_REQUEST)), "{replies:?}");
        assert_eq!(s.reader.get(KEY_MESH).unwrap(), None);
        assert_eq!(s.reader.get(KEY_CONTAINERD).unwrap(), None);
        assert_eq!(last_index(&mut s).await, Some(2));

        // The rejection left nothing behind: a good range still initialises.
        let replies = s
            .state_machine
            .apply([normal(3, init_cluster_with_range("10.200.0.0/16", 24))])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::None]);
        assert_eq!(
            s.reader.mesh().unwrap().unwrap().nodes[&1].container_subnet,
            Some("10.200.0.0/24".parse().unwrap())
        );
        drop(s);
    }

    #[tokio::test]
    async fn invite_at_container_capacity_is_rejected_and_still_applied() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        // Two subnets: the founder's and one more.
        let replies = s
            .state_machine
            .apply([
                normal(1, init_cluster_with_range("10.200.0.0/29", 30)),
                normal(2, create_invite(2)),
                normal(3, create_invite(3)),
            ])
            .await
            .unwrap();
        assert!(matches!(replies[1], Response::Invited { node_id: 2, .. }));
        assert!(rejected(ec::MESH_EXHAUSTED)(&replies[2]), "{replies:?}");
        assert_eq!(last_index(&mut s).await, Some(3));
        let mesh = s.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh.next_node_id, 3);
        assert_eq!(mesh.invites.len(), 1);
        drop(s);
    }

    #[tokio::test]
    async fn snapshot_carries_container_subnets() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();
        a.state_machine
            .apply([
                normal(1, init_cluster_with_range("10.200.0.0/16", 24)),
                normal(2, create_invite(2)),
                normal(3, register(2)),
            ])
            .await
            .unwrap();
        let snap = a
            .state_machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        b.state_machine
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();
        let mesh = b.reader.mesh().unwrap().unwrap();
        assert_eq!(mesh, a.reader.mesh().unwrap().unwrap());
        assert_eq!(
            mesh.config.container_cidr,
            Some("10.200.0.0/16".parse().unwrap())
        );
        assert_eq!(
            mesh.nodes[&1].container_subnet,
            Some("10.200.0.0/24".parse().unwrap())
        );
        assert_eq!(
            mesh.nodes[&2].container_subnet,
            Some("10.200.1.0/24".parse().unwrap())
        );
        drop((a, b));
    }

    #[tokio::test]
    async fn typed_readers_report_closed_and_bad_data() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(dir.path()).unwrap();
        // Corrupt value written behind the state machine's back.
        let db = s.state_machine.db.clone();
        db.put_cf(cf(&db, CF_SM_DATA), KEY_MESH.as_bytes(), b"{\"x\":1}")
            .unwrap();
        assert!(matches!(s.reader.mesh(), Err(ReadError::Storage(_))));
        drop(db);
        let reader = s.reader.clone();
        drop(s);
        assert!(matches!(reader.mesh(), Err(ReadError::Closed)));
        assert!(matches!(reader.containerd(), Err(ReadError::Closed)));
    }

    #[tokio::test]
    async fn reader_reports_closed_after_db_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(dir.path()).unwrap();
        let reader = s.reader.clone();
        drop(s);
        assert!(matches!(reader.get("x"), Err(ReadError::Closed)));
    }

    // --- projects and workloads --------------------------------------------

    use crate::apps::WorkloadRecord;
    use crate::raft::store::Storage;
    use faber_api_types::workload::{WorkloadSpec, limits};

    fn workload(image: &str) -> WorkloadSpec {
        serde_json::from_value(json!({"containers": [{"name": "main", "image": image}]})).unwrap()
    }

    fn strings(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// A project with `staging` and `production`.
    fn create_project(name: &str) -> Request {
        Request::CreateProject {
            name: name.into(),
            environments: strings(&["staging", "production"]),
            at: 1_000,
        }
    }

    fn delete_project(name: &str) -> Request {
        Request::DeleteProject { name: name.into() }
    }

    fn add_environment(project: &str, name: &str) -> Request {
        Request::AddEnvironment {
            project: project.into(),
            name: name.into(),
            at: 1_003,
        }
    }

    fn remove_environment(project: &str, name: &str) -> Request {
        Request::RemoveEnvironment {
            project: project.into(),
            name: name.into(),
        }
    }

    /// A deploy of `workloads`, each a name and an image, to `staging` of
    /// `project`.
    fn deploy(project: &str, workloads: &[(&str, &str)], prune: bool) -> Request {
        Request::ApplyWorkloads {
            project: project.into(),
            environment: "staging".into(),
            workloads: workloads
                .iter()
                .map(|(name, image)| (name.to_string(), workload(image)))
                .collect(),
            prune,
            at: 1_001,
        }
    }

    /// Removes `names` from `staging` of `project`.
    fn remove_workloads(project: &str, names: &[&str]) -> Request {
        Request::RemoveWorkloads {
            project: project.into(),
            environment: "staging".into(),
            names: strings(names),
            at: 1_002,
        }
    }

    fn applied(
        generation: u64,
        created: &[&str],
        updated: &[&str],
        unchanged: &[&str],
        removed: &[&str],
    ) -> Response {
        Response::Applied {
            generation,
            created: strings(created),
            updated: strings(updated),
            unchanged: strings(unchanged),
            removed: strings(removed),
        }
    }

    /// Every `sm_data` entry as it is stored, to compare two states and to
    /// show that a command wrote nothing.
    fn dump(s: &Storage) -> BTreeMap<String, Vec<u8>> {
        let db = &s.state_machine.db;
        db.iterator_cf(cf(db, CF_SM_DATA), IteratorMode::Start)
            .map(|item| {
                let (key, value) = item.unwrap();
                (String::from_utf8(key.to_vec()).unwrap(), value.to_vec())
            })
            .collect()
    }

    /// The index and the stored records name the same environments, and
    /// the figures in the index are those of each record.
    fn assert_apps_agree(s: &Storage) {
        let stored = dump(s);
        let mut indexed = Vec::new();
        for (project, p) in &s.reader.projects().unwrap().projects {
            for (environment, entry) in &p.environments {
                let at = env_key(project, environment);
                let record = s
                    .reader
                    .environment(project, environment)
                    .unwrap()
                    .unwrap_or_else(|| panic!("no record under {at}"));
                assert_eq!(entry.generation, record.generation, "{at}");
                assert_eq!(entry.updated_at, record.updated_at, "{at}");
                assert_eq!(entry.workloads as usize, record.workloads.len(), "{at}");
                assert_eq!(entry.bytes as usize, stored[&at].len(), "{at}");
                indexed.push((project.clone(), environment.clone(), record));
            }
        }
        assert_eq!(s.reader.environments().unwrap(), indexed);
        // And no environment key is left without an index entry.
        let keys = stored.keys().filter(|k| k.starts_with(ENV_PREFIX)).count();
        assert_eq!(keys, indexed.len());
    }

    /// Applies `req` as a batch of its own and returns its reply.
    async fn apply_one(s: &mut Storage, index: u64, req: Request) -> Response {
        let replies = s.state_machine.apply([normal(index, req)]).await.unwrap();
        assert_apps_agree(s);
        replies.into_iter().next().unwrap()
    }

    /// Every command, one batch each, on a cluster that has no mesh.
    #[tokio::test]
    async fn app_commands_apply_without_a_mesh() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        assert_eq!(s.reader.projects().unwrap(), ProjectIndex::default());
        assert_eq!(s.reader.environments().unwrap(), vec![]);
        assert_eq!(s.reader.environment("shop", "staging").unwrap(), None);

        assert_eq!(
            apply_one(&mut s, 1, create_project("shop")).await,
            Response::None
        );
        let projects = s.reader.projects().unwrap();
        assert_eq!(projects.projects["shop"].created_at, 1_000);
        assert_eq!(
            projects.projects["shop"]
                .environments
                .keys()
                .collect::<Vec<_>>(),
            vec!["production", "staging"]
        );
        assert_eq!(
            s.reader.environment("shop", "staging").unwrap(),
            Some(EnvironmentRecord {
                generation: 1,
                updated_at: 1_000,
                workloads: BTreeMap::new(),
            })
        );

        assert_eq!(
            apply_one(&mut s, 2, add_environment("shop", "dev")).await,
            Response::None
        );
        assert_eq!(
            s.reader.environment("shop", "dev").unwrap(),
            Some(EnvironmentRecord {
                generation: 2,
                updated_at: 1_003,
                workloads: BTreeMap::new(),
            })
        );

        let two = [("web", "nginx"), ("api", "api:1")];
        assert_eq!(
            apply_one(&mut s, 3, deploy("shop", &two, false)).await,
            applied(3, &["api", "web"], &[], &[], &[])
        );
        let record = s.reader.environment("shop", "staging").unwrap().unwrap();
        assert_eq!((record.generation, record.updated_at), (3, 1_001));
        assert_eq!(
            record.workloads["web"],
            WorkloadRecord {
                revision: 3,
                spec: workload("nginx"),
            }
        );

        // The same deploy again writes nothing and keeps the generation.
        let before = dump(&s);
        assert_eq!(
            apply_one(&mut s, 4, deploy("shop", &two, false)).await,
            applied(3, &[], &[], &["api", "web"], &[])
        );
        assert_eq!(dump(&s), before);
        assert_eq!(last_index(&mut s).await, Some(4));

        assert_eq!(
            apply_one(&mut s, 5, deploy("shop", &[("web", "nginx:2")], true)).await,
            applied(5, &[], &["web"], &[], &["api"])
        );
        let record = s.reader.environment("shop", "staging").unwrap().unwrap();
        assert_eq!(record.generation, 5);
        assert_eq!(record.workloads.keys().collect::<Vec<_>>(), vec!["web"]);
        assert_eq!(record.workloads["web"].revision, 5);

        assert_eq!(
            apply_one(&mut s, 6, remove_workloads("shop", &["web"])).await,
            Response::None
        );
        assert_eq!(
            s.reader.environment("shop", "staging").unwrap(),
            Some(EnvironmentRecord {
                generation: 6,
                updated_at: 1_002,
                workloads: BTreeMap::new(),
            })
        );

        assert_eq!(
            apply_one(&mut s, 7, remove_environment("shop", "dev")).await,
            Response::None
        );
        assert_eq!(s.reader.environment("shop", "dev").unwrap(), None);
        assert_eq!(s.reader.get(&env_key("shop", "dev")).unwrap(), None);
        assert_eq!(s.reader.environments().unwrap().len(), 2);

        assert_eq!(
            apply_one(&mut s, 8, delete_project("shop")).await,
            Response::None
        );
        // Only the empty index is left.
        assert_eq!(
            dump(&s),
            BTreeMap::from([(KEY_PROJECTS.to_string(), br#"{"projects":{}}"#.to_vec())])
        );
        assert_eq!(s.reader.mesh().unwrap(), None);
        assert_eq!(last_index(&mut s).await, Some(8));
        drop(s);
    }

    /// Later entries of a batch see the projects, records and deletions of
    /// the earlier ones.
    #[tokio::test]
    async fn one_batch_sees_earlier_app_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        let replies = s
            .state_machine
            .apply([
                normal(1, create_project("shop")),
                normal(2, deploy("shop", &[("web", "nginx"), ("db", "pg")], false)),
                normal(3, remove_workloads("shop", &["web"])),
                normal(4, deploy("shop", &[("web", "nginx")], false)),
                normal(5, delete_project("shop")),
                normal(6, remove_workloads("shop", &["db", "web"])),
                normal(7, delete_project("shop")),
                normal(8, deploy("shop", &[("web", "nginx")], false)),
                normal(9, create_project("shop")),
                normal(10, deploy("shop", &[("web", "nginx")], false)),
                normal(11, deploy("shop", &[("web", "nginx")], true)),
            ])
            .await
            .unwrap();
        assert_eq!(replies[0], Response::None);
        assert_eq!(replies[1], applied(2, &["db", "web"], &[], &[], &[]));
        assert_eq!(replies[2], Response::None);
        // Removed and deployed again: a new workload, not an unchanged one.
        assert_eq!(replies[3], applied(4, &["web"], &[], &[], &[]));
        assert!(rejected(ec::NOT_EMPTY)(&replies[4]), "{replies:?}");
        assert_eq!(replies[5], Response::None);
        assert_eq!(replies[6], Response::None);
        assert!(rejected(ec::NOT_FOUND)(&replies[7]), "{replies:?}");
        assert_eq!(replies[8], Response::None);
        // The project made again starts empty: nothing of the deleted one
        // is seen, and its generations carry on from the log index.
        assert_eq!(replies[9], applied(10, &["web"], &[], &[], &[]));
        assert_eq!(replies[10], applied(10, &[], &[], &["web"], &[]));

        let staging = s.reader.environment("shop", "staging").unwrap().unwrap();
        assert_eq!(staging.generation, 10);
        assert_eq!(staging.workloads["web"].revision, 10);
        assert_eq!(
            s.reader
                .environment("shop", "production")
                .unwrap()
                .unwrap()
                .generation,
            9
        );
        assert_apps_agree(&s);
        assert_eq!(last_index(&mut s).await, Some(11));
        drop(s);
    }

    #[tokio::test]
    async fn a_delete_is_visible_to_later_entries_of_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([set(1, 1, "a", json!(1)), set(1, 2, "b", json!(1))])
            .await
            .unwrap();

        let sm = &s.state_machine;
        let mut p = pending(sm);
        assert_eq!(sm.current(&p.values, "a").unwrap(), Some(json!(1)));
        p.delete("a");
        assert_eq!(sm.current(&p.values, "a").unwrap(), None);
        // A write after the delete wins, and a delete after that again.
        p.put("a", &json!(2)).unwrap();
        assert_eq!(sm.current(&p.values, "a").unwrap(), Some(json!(2)));
        p.put("b", &json!(2)).unwrap();
        p.delete("b");
        assert_eq!(sm.current(&p.values, "b").unwrap(), None);
        // A key that never existed.
        p.delete("c");
        assert_eq!(sm.current(&p.values, "c").unwrap(), None);
        // Nothing is on disk before the batch is written.
        assert_eq!(s.reader.get("a").unwrap(), Some(json!(1)));

        write_synced(&sm.db, p.batch).unwrap();
        assert_eq!(s.reader.get("a").unwrap(), Some(json!(2)));
        assert_eq!(s.reader.get("b").unwrap(), None);
        assert_eq!(s.reader.get("c").unwrap(), None);
        drop(s);
    }

    #[tokio::test]
    async fn scaffold_commands_are_rejected_and_write_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([normal(1, create_project("shop"))])
            .await
            .unwrap();
        let before = dump(&s);

        let commands = [
            Request::CreateDataKey(keys::CreateDataKey {
                key_id: "k1".into(),
                sealed: vec![1],
                at: 1,
            }),
            Request::SetClusterKeyId(keys::SetClusterKeyId {
                key_id: "c1".into(),
            }),
            Request::PlaceReplicas(place::PlaceReplicas {
                project: "shop".into(),
                environment: "staging".into(),
                workload: "web".into(),
                nodes: vec![1],
                at: 1,
            }),
            Request::ReportNode(status::ReportNode {
                node_id: 1,
                report: json!({}),
                at: 1,
            }),
            Request::SetStatusInterval(status::SetStatusInterval { secs: 10 }),
            Request::AllocateServiceIp(services::AllocateServiceIp {
                project: "shop".into(),
                environment: "staging".into(),
                workload: "web".into(),
            }),
            Request::ReleaseServiceIp(services::ReleaseServiceIp {
                project: "shop".into(),
                environment: "staging".into(),
                workload: "web".into(),
            }),
        ];
        let entries: Vec<Entry> = commands
            .into_iter()
            .enumerate()
            .map(|(n, req)| normal(2 + n as u64, req))
            .collect();
        let replies = s.state_machine.apply(entries).await.unwrap();
        assert_eq!(replies.len(), 7);
        assert!(replies.iter().all(rejected("internal")), "{replies:?}");
        assert_eq!(dump(&s), before);
        drop(s);
    }

    #[tokio::test]
    async fn rejected_app_commands_are_applied_and_write_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([
                normal(1, create_project("shop")),
                normal(2, deploy("shop", &[("web", "nginx")], false)),
            ])
            .await
            .unwrap();
        let before = dump(&s);

        let too_many: Vec<String> = (0..=limits::MAX_WORKLOADS_PER_ENVIRONMENT)
            .map(|n| format!("w{n}"))
            .collect();
        let too_many: Vec<(&str, &str)> = too_many.iter().map(|n| (n.as_str(), "img")).collect();
        let no_environments = Request::CreateProject {
            name: "blog".into(),
            environments: vec![],
            at: 1,
        };
        let commands = [
            (create_project("shop"), ec::PROJECT_EXISTS),
            (create_project("Bad_Name"), ec::BAD_REQUEST),
            (no_environments, ec::BAD_REQUEST),
            (delete_project("blog"), ec::NOT_FOUND),
            (delete_project("shop"), ec::NOT_EMPTY),
            (add_environment("blog", "dev"), ec::NOT_FOUND),
            (add_environment("shop", "Dev"), ec::BAD_REQUEST),
            (add_environment("shop", "staging"), ec::PROJECT_EXISTS),
            (remove_environment("shop", "dev"), ec::NOT_FOUND),
            (remove_environment("shop", "staging"), ec::NOT_EMPTY),
            (deploy("blog", &[("web", "nginx")], false), ec::NOT_FOUND),
            // Not a name, so not a key either.
            (
                deploy("shop/staging", &[("web", "nginx")], false),
                ec::NOT_FOUND,
            ),
            (
                deploy("shop", &[("db_backup", "pg")], true),
                ec::BAD_REQUEST,
            ),
            (deploy("shop", &too_many, false), ec::WORKLOADS_EXHAUSTED),
            (remove_workloads("blog", &["web"]), ec::NOT_FOUND),
            (remove_workloads("shop", &["web", "db"]), ec::NOT_FOUND),
        ];
        let want: Vec<&str> = commands.iter().map(|(_, code)| *code).collect();
        let last = 2 + commands.len() as u64;

        // In one batch, where a write of one would be seen by the next.
        let entries: Vec<Entry> = (3..)
            .zip(commands.iter())
            .map(|(index, (req, _))| normal(index, req.clone()))
            .collect();
        let replies = s.state_machine.apply(entries).await.unwrap();
        let codes: Vec<&str> = replies
            .iter()
            .map(|r| match r {
                Response::Rejected { code, .. } => code.as_str(),
                other => panic!("not rejected: {other:?}"),
            })
            .collect();
        assert_eq!(codes, want);
        assert_eq!(dump(&s), before);
        assert_eq!(last_index(&mut s).await, Some(last));

        // And each as a batch of its own.
        for (index, (req, code)) in (last + 1..).zip(commands) {
            let replies = s.state_machine.apply([normal(index, req)]).await.unwrap();
            assert!(rejected(code)(&replies[0]), "{replies:?}");
            assert_eq!(last_index(&mut s).await, Some(index));
        }
        assert_eq!(dump(&s), before);
        assert_apps_agree(&s);
        drop(s);
    }

    #[tokio::test]
    async fn set_rejects_the_project_index_and_environment_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([
                normal(1, create_project("shop")),
                normal(2, deploy("shop", &[("web", "nginx")], false)),
            ])
            .await
            .unwrap();
        let before = dump(&s);

        let replies = s
            .state_machine
            .apply([
                set(1, 3, KEY_PROJECTS, json!({"projects": {}})),
                set(1, 4, "env/x/y", json!({"generation": 1, "updated_at": 1})),
                set(1, 5, "env/shop/staging", json!({"forged": true})),
                set(1, 6, "env/", json!(1)),
            ])
            .await
            .unwrap();
        assert!(replies.iter().all(rejected(ec::BAD_REQUEST)), "{replies:?}");
        assert_eq!(dump(&s), before);
        assert_eq!(last_index(&mut s).await, Some(6));

        // Keys that only look alike are free.
        let replies = s
            .state_machine
            .apply([
                set(1, 7, "projects/x", json!(1)),
                set(1, 8, "environment", json!(1)),
            ])
            .await
            .unwrap();
        assert_eq!(replies, vec![Response::Previous(None); 2]);
        assert_apps_agree(&s);
        drop(s);
    }

    #[tokio::test]
    async fn snapshot_carries_projects_and_replaces_environments() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();
        a.state_machine
            .apply([
                normal(1, create_project("shop")),
                normal(2, deploy("shop", &[("web", "nginx"), ("db", "pg")], false)),
                normal(3, add_environment("shop", "dev")),
            ])
            .await
            .unwrap();
        // What `b` has, the snapshot does not: another project, and in
        // `shop` another environment and other workloads.
        b.state_machine
            .apply([
                normal(1, create_project("old")),
                normal(2, deploy("old", &[("legacy", "cobol")], false)),
                normal(3, create_project("shop")),
                normal(4, add_environment("shop", "qa")),
                normal(5, deploy("shop", &[("stale", "img")], false)),
            ])
            .await
            .unwrap();

        let snap = a
            .state_machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        b.state_machine
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        assert_eq!(dump(&b), dump(&a));
        assert_eq!(b.reader.projects().unwrap(), a.reader.projects().unwrap());
        assert_eq!(
            b.reader.environments().unwrap(),
            a.reader.environments().unwrap()
        );
        assert_eq!(b.reader.environments().unwrap().len(), 3);
        assert_eq!(b.reader.environment("old", "staging").unwrap(), None);
        assert_eq!(b.reader.environment("shop", "qa").unwrap(), None);
        let staging = b.reader.environment("shop", "staging").unwrap().unwrap();
        assert_eq!(
            staging.workloads.keys().collect::<Vec<_>>(),
            vec!["db", "web"]
        );
        assert_apps_agree(&b);

        // The replica carries on from what it was given.
        let replies = b
            .state_machine
            .apply([normal(4, deploy("shop", &[("web", "nginx:2")], false))])
            .await
            .unwrap();
        assert_eq!(replies, vec![applied(4, &[], &["web"], &[], &[])]);
        assert_apps_agree(&b);
        drop((a, b));
    }

    /// The same entries give the same state and the same replies whether
    /// they arrive as one batch or one by one.
    #[tokio::test]
    async fn app_state_does_not_depend_on_how_entries_are_batched() {
        let entries = || {
            let requests = vec![
                create_project("shop"),
                Request::Set {
                    key: "k".into(),
                    value: json!(1),
                },
                deploy("shop", &[("web", "nginx"), ("db", "pg")], false),
                deploy("shop", &[("web", "nginx"), ("db", "pg")], false),
                create_project("shop"),
                remove_workloads("shop", &["web"]),
                init_cluster(),
                deploy("shop", &[("web", "nginx"), ("api", "api:1")], false),
                delete_project("shop"),
                add_environment("shop", "dev"),
                remove_environment("shop", "production"),
                create_project("blog"),
                deploy("blog", &[("web", "ghost")], false),
                remove_workloads("blog", &["web"]),
                delete_project("blog"),
                create_project("blog"),
                deploy("shop", &[("api", "api:2")], true),
                remove_workloads("shop", &["nope"]),
            ];
            let mut entries: Vec<Entry> = (1..).zip(requests).map(|(i, r)| normal(i, r)).collect();
            entries.push(Entry {
                log_id: log_id(1, 1, 19),
                payload: EntryPayload::Blank,
            });
            entries.push(normal(20, deploy("blog", &[("web", "ghost")], false)));
            entries
        };
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut a = open(dir_a.path()).unwrap();
        let mut b = open(dir_b.path()).unwrap();

        let replies_a = a.state_machine.apply(entries()).await.unwrap();
        let mut replies_b = Vec::new();
        for entry in entries() {
            replies_b.extend(b.state_machine.apply([entry]).await.unwrap());
            assert_apps_agree(&b);
        }
        assert_eq!(replies_a, replies_b);
        assert_eq!(dump(&a), dump(&b));
        assert_eq!(
            a.state_machine.applied_state().await.unwrap(),
            b.state_machine.applied_state().await.unwrap()
        );
        assert_apps_agree(&a);

        // Not an empty comparison.
        assert_eq!(
            replies_a[16],
            applied(17, &[], &["api"], &[], &["db", "web"])
        );
        assert_eq!(replies_a[19], applied(20, &["web"], &[], &[], &[]));
        let projects = a.reader.projects().unwrap();
        assert_eq!(
            projects.projects.keys().collect::<Vec<_>>(),
            vec!["blog", "shop"]
        );
        let blog = &projects.projects["blog"].environments;
        assert_eq!(blog["staging"].generation, 20);
        assert_eq!(blog["production"].generation, 16);
        drop((a, b));
    }

    /// The mesh and the projects are separate records: commands on one
    /// leave the other as it was.
    #[tokio::test]
    async fn app_commands_leave_the_mesh_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([normal(1, init_cluster()), normal(2, create_invite(2))])
            .await
            .unwrap();
        let mesh = raw_mesh(&s);

        let replies = s
            .state_machine
            .apply([
                normal(3, create_project("shop")),
                normal(4, deploy("shop", &[("web", "nginx")], false)),
                normal(5, create_invite(3)),
                normal(6, deploy("shop", &[("web", "nginx:2")], false)),
            ])
            .await
            .unwrap();
        assert_eq!(replies[1], applied(4, &["web"], &[], &[], &[]));
        assert!(matches!(replies[2], Response::Invited { node_id: 3, .. }));
        assert_eq!(replies[3], applied(6, &[], &["web"], &[], &[]));
        let projects = s.reader.projects().unwrap();
        assert_ne!(raw_mesh(&s), mesh, "the invite was stored");

        let mesh = raw_mesh(&s);
        s.state_machine
            .apply([
                normal(7, deploy("shop", &[("api", "api:1")], false)),
                normal(8, delete_project("shop")),
            ])
            .await
            .unwrap();
        assert_eq!(raw_mesh(&s), mesh);
        s.state_machine
            .apply([normal(9, Request::AdvanceClock { elapsed_secs: 30 })])
            .await
            .unwrap();
        assert_ne!(raw_mesh(&s), mesh);
        assert_ne!(s.reader.projects().unwrap(), projects);
        assert_apps_agree(&s);
        drop(s);
    }

    /// `environments()` is sorted by the names, which is not the order of
    /// the keys: `-` sorts before `/`.
    #[tokio::test]
    async fn environments_are_sorted_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path()).unwrap();
        s.state_machine
            .apply([
                normal(1, create_project("a")),
                normal(2, create_project("a-b")),
                normal(3, set_key("env0")),
                normal(4, set_key("enw")),
                normal(5, deploy("a-b", &[("web", "nginx")], false)),
            ])
            .await
            .unwrap();
        let keys: Vec<String> = dump(&s).into_keys().collect();
        assert_eq!(
            keys,
            vec![
                "env/a-b/production",
                "env/a-b/staging",
                "env/a/production",
                "env/a/staging",
                "env0",
                "enw",
                "projects",
            ]
        );
        let names: Vec<(String, String, usize)> = s
            .reader
            .environments()
            .unwrap()
            .into_iter()
            .map(|(project, environment, record)| (project, environment, record.workloads.len()))
            .collect();
        assert_eq!(
            names,
            vec![
                ("a".to_string(), "production".to_string(), 0),
                ("a".to_string(), "staging".to_string(), 0),
                ("a-b".to_string(), "production".to_string(), 0),
                ("a-b".to_string(), "staging".to_string(), 1),
            ]
        );
        assert_apps_agree(&s);
        drop(s);
    }

    fn set_key(key: &str) -> Request {
        Request::Set {
            key: key.into(),
            value: json!(1),
        }
    }

    #[tokio::test]
    async fn app_readers_report_closed_and_bad_data() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(dir.path()).unwrap();
        // Corrupt values written behind the state machine's back.
        let db = &s.state_machine.db;
        let data = cf(db, CF_SM_DATA);
        db.put_cf(data, KEY_PROJECTS.as_bytes(), b"{\"projects\":1}")
            .unwrap();
        assert!(matches!(s.reader.projects(), Err(ReadError::Storage(_))));

        db.put_cf(data, b"env/shop/staging", b"{\"x\":1}").unwrap();
        assert!(matches!(
            s.reader.environment("shop", "staging"),
            Err(ReadError::Storage(_))
        ));
        assert!(matches!(
            s.reader.environments(),
            Err(ReadError::Storage(_))
        ));
        // A key under the prefix that names no environment.
        db.delete_cf(data, b"env/shop/staging").unwrap();
        db.put_cf(data, b"env/odd", b"{\"generation\":1,\"updated_at\":1}")
            .unwrap();
        assert!(matches!(
            s.reader.environments(),
            Err(ReadError::Storage(_))
        ));

        let reader = s.reader.clone();
        drop(s);
        assert!(matches!(reader.projects(), Err(ReadError::Closed)));
        assert!(matches!(
            reader.environment("shop", "staging"),
            Err(ReadError::Closed)
        ));
        assert!(matches!(reader.environments(), Err(ReadError::Closed)));
    }
}
