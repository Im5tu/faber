//! What the containers step and the supervisor share.
//!
//! The step decides which replicas should run and starts them; the
//! supervisor keeps them running and says how they are. They meet here: the
//! step registers replicas, and asks for stops; the supervisor publishes what
//! it saw, which the status reporter reads through [`ReplicaSource`].
//!
//! Nothing here holds an env value. A replica is registered with its spec
//! only; the env is fetched when a container is made (see
//! [`super::env::EnvSource`]) and lives in the request.
//!
//! The record lock is held for a moment, never across an await. The
//! per-replica locks ([`RunnerState::replica_lock`]) are held across the
//! calls that start, restart or remove one replica, so that those never
//! overlap.

use std::collections::{BTreeSet, HashMap};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use faber_api_types::workload::ReplicaState;
use faber_node_protocol::ReplicaRef;
use ipnet::Ipv4Net;

use super::desired::{DesiredReplica, parse_revision};
use crate::raft::store::status::{MAX_DETAIL_BYTES, MAX_REPLICAS_PER_REPORT, ReplicaReport};
use crate::status::ReplicaSource;

/// A replica without its revision: what a volume belongs to. Volumes are
/// not scoped by revision, so the pods of two revisions of one workload at
/// one index share them.
pub type Slot = (String, String, String, u32);

/// A workload: project, environment, name.
pub type WorkloadKey = (String, String, String);

pub fn slot_of(replica: &ReplicaRef) -> Slot {
    (
        replica.project.clone(),
        replica.env.clone(),
        replica.workload.clone(),
        replica.replica_index,
    )
}

pub fn workload_of(replica: &ReplicaRef) -> WorkloadKey {
    (
        replica.project.clone(),
        replica.env.clone(),
        replica.workload.clone(),
    )
}

/// A total order on replicas: `ReplicaRef` has none of its own.
pub fn order(replica: &ReplicaRef) -> (&str, &str, &str, &str, u32) {
    (
        &replica.project,
        &replica.env,
        &replica.workload,
        &replica.revision,
        replica.replica_index,
    )
}

/// Why a replica is being stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Placement no longer has it here.
    Removed,
    /// A newer revision is available here. Taken back if that stops being so.
    Superseded,
    /// A newer revision needs its block volume.
    Replaced,
}

/// One replica the runner knows.
#[derive(Debug, Clone)]
pub struct Entry {
    /// What to run. `None` for a pod found on the node that an earlier run
    /// of fabercp started: it is stopped when its time comes, but it cannot
    /// be restarted or probed.
    pub desired: Option<Arc<DesiredReplica>>,
    /// The step has started it. Until then it is `deploying`.
    pub launched: bool,
    /// A newer revision of the workload is placed here. The supervisor asks
    /// for this one's stop once the newer revision is available.
    pub superseded: bool,
    /// Set once a stop is wanted. The replica reads `unavailable` from then
    /// on, so that the virtual IP drops it; the supervisor signals the stop
    /// after the drain period.
    pub stop: Option<StopReason>,
    /// Counts the stops asked for: it changes each time a stop is asked for
    /// after being withdrawn, so a drain period never carries over from an
    /// earlier ask.
    pub stop_gen: u64,
    /// Not started yet because an older revision still holds the block
    /// volume. The supervisor starts it when that one is gone.
    pub deferred: bool,
    /// Why it has not started, or failed to start again, for people.
    pub detail: Option<String>,
}

/// What the supervisor last saw of a replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    pub state: ReplicaState,
    pub pod_ip: Option<Ipv4Addr>,
    pub detail: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<ReplicaRef, Entry>,
    observed: HashMap<ReplicaRef, Observed>,
    /// Workloads whose env the step could not open: nothing of theirs is
    /// started, stopped or garbage collected.
    held: BTreeSet<WorkloadKey>,
    /// This node's container subnet: the only addresses probed or reported.
    subnet: Option<Ipv4Net>,
    locks: HashMap<ReplicaRef, Arc<tokio::sync::Mutex<()>>>,
    slot_locks: HashMap<Slot, Arc<tokio::sync::Mutex<()>>>,
}

#[derive(Debug, Default)]
pub struct RunnerState {
    inner: Mutex<Inner>,
}

impl RunnerState {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Every method leaves the record whole.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The lock that one replica's starts, restarts and removals hold. Take
    /// it, then look at the entry again: it may have changed while waiting.
    pub fn replica_lock(&self, replica: &ReplicaRef) -> Arc<tokio::sync::Mutex<()>> {
        let mut inner = self.lock();
        // A lock that only the map holds is idle: forget it.
        inner.locks.retain(|_, l| Arc::strong_count(l) > 1);
        inner.locks.entry(replica.clone()).or_default().clone()
    }

    /// The lock that makes and removes the volumes of one slot. A start
    /// holds it while it ensures its volumes; the ramdisk collector holds it
    /// from the check that nobody is in the slot to the removal. Always
    /// taken after a replica lock, never before.
    pub fn slot_lock(&self, slot: &Slot) -> Arc<tokio::sync::Mutex<()>> {
        let mut inner = self.lock();
        inner.slot_locks.retain(|_, l| Arc::strong_count(l) > 1);
        inner.slot_locks.entry(slot.clone()).or_default().clone()
    }

    /// Adds `replica`, or refreshes it. A replica not seen before is not
    /// launched. A stop that was asked for is withdrawn: the replica is
    /// wanted again.
    pub fn register(&self, replica: &DesiredReplica) {
        let mut inner = self.lock();
        let desired = Arc::new(replica.clone());
        match inner.entries.get_mut(&replica.replica) {
            Some(entry) => {
                if entry.desired.as_deref() != Some(&*desired) {
                    entry.launched = false;
                }
                entry.desired = Some(desired);
                entry.superseded = false;
                entry.stop = None;
            }
            None => {
                inner.entries.insert(
                    replica.replica.clone(),
                    Entry {
                        desired: Some(desired),
                        launched: false,
                        superseded: false,
                        stop: None,
                        stop_gen: 0,
                        deferred: false,
                        detail: None,
                    },
                );
            }
        }
    }

    fn entry_or_adopted<'a>(inner: &'a mut Inner, replica: &ReplicaRef) -> &'a mut Entry {
        inner.entries.entry(replica.clone()).or_insert(Entry {
            desired: None,
            launched: true,
            superseded: false,
            stop: None,
            stop_gen: 0,
            deferred: false,
            detail: None,
        })
    }

    /// Marks `replica` as superseded, adding it if it is unknown.
    pub fn mark_superseded(&self, replica: &ReplicaRef) {
        let mut inner = self.lock();
        Self::entry_or_adopted(&mut inner, replica).superseded = true;
    }

    /// Asks for `replica` to be stopped, adding it if it is unknown. The
    /// first reason stays.
    pub fn request_stop(&self, replica: &ReplicaRef, reason: StopReason) {
        let mut inner = self.lock();
        let entry = Self::entry_or_adopted(&mut inner, replica);
        if entry.stop.is_none() {
            entry.stop = Some(reason);
            entry.stop_gen += 1;
        }
    }

    /// Takes back a stop asked for because of a newer revision.
    pub fn cancel_superseded_stop(&self, replica: &ReplicaRef) {
        if let Some(e) = self.lock().entries.get_mut(replica)
            && e.stop == Some(StopReason::Superseded)
        {
            e.stop = None;
        }
    }

    pub fn mark_launched(&self, replica: &ReplicaRef) {
        if let Some(e) = self.lock().entries.get_mut(replica) {
            e.launched = true;
            e.deferred = false;
            e.detail = None;
        }
    }

    pub fn mark_failed(&self, replica: &ReplicaRef, detail: String) {
        if let Some(e) = self.lock().entries.get_mut(replica) {
            e.detail = Some(detail);
        }
    }

    pub fn set_deferred(&self, replica: &ReplicaRef, detail: Option<String>) {
        if let Some(e) = self.lock().entries.get_mut(replica) {
            e.deferred = detail.is_some();
            e.detail = detail;
        }
    }

    /// Drops `replica`.
    pub fn forget(&self, replica: &ReplicaRef) -> Option<Entry> {
        let mut inner = self.lock();
        inner.observed.remove(replica);
        inner.entries.remove(replica)
    }

    /// Every entry, in the order of [`order`].
    pub fn entries(&self) -> Vec<(ReplicaRef, Entry)> {
        let mut all: Vec<_> = self
            .lock()
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        all.sort_by(|a, b| order(&a.0).cmp(&order(&b.0)));
        all
    }

    pub fn entry(&self, replica: &ReplicaRef) -> Option<Entry> {
        self.lock().entries.get(replica).cloned()
    }

    /// The replicas of `slot` other than `except`.
    pub fn others_in_slot(&self, slot: &Slot, except: &ReplicaRef) -> Vec<ReplicaRef> {
        self.lock()
            .entries
            .keys()
            .filter(|k| *k != except && slot_of(k) == *slot)
            .cloned()
            .collect()
    }

    /// Replaces what the supervisor saw.
    pub fn set_observed(&self, observed: HashMap<ReplicaRef, Observed>) {
        self.lock().observed = observed;
    }

    pub fn set_held(&self, held: BTreeSet<WorkloadKey>) {
        self.lock().held = held;
    }

    pub fn held(&self) -> BTreeSet<WorkloadKey> {
        self.lock().held.clone()
    }

    pub fn set_subnet(&self, subnet: Option<Ipv4Net>) {
        self.lock().subnet = subnet;
    }

    pub fn subnet(&self) -> Option<Ipv4Net> {
        self.lock().subnet
    }

    /// The slots that have an entry.
    pub fn used_slots(&self) -> BTreeSet<Slot> {
        self.lock().entries.keys().map(slot_of).collect()
    }
}

/// `text` cut to what a report accepts, on a character boundary.
fn clip(mut text: String) -> String {
    if text.len() > MAX_DETAIL_BYTES {
        let mut end = MAX_DETAIL_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

impl ReplicaSource for RunnerState {
    fn replicas(&self) -> Vec<ReplicaReport> {
        let inner = self.lock();
        let mut entries: Vec<_> = inner.entries.iter().collect();
        entries.sort_by(|a, b| order(a.0).cmp(&order(b.0)));
        entries
            .into_iter()
            .filter(|(_, e)| e.desired.is_some())
            .filter_map(|(key, entry)| {
                let revision = parse_revision(&key.revision)?;
                let seen = inner.observed.get(key);
                let pod_ip = seen.and_then(|o| o.pod_ip);
                let (state, detail) = if entry.stop.is_some() {
                    // Out of the virtual IP from now on.
                    (ReplicaState::Unavailable, Some("stopping".to_owned()))
                } else if !entry.launched {
                    (ReplicaState::Deploying, entry.detail.clone())
                } else if entry.detail.is_some() {
                    (ReplicaState::Unavailable, entry.detail.clone())
                } else {
                    match seen {
                        Some(o) => (o.state, o.detail.clone()),
                        None => (ReplicaState::Deploying, None),
                    }
                };
                Some(ReplicaReport {
                    project: key.project.clone(),
                    environment: key.env.clone(),
                    workload: key.workload.clone(),
                    revision,
                    replica_index: key.replica_index,
                    state,
                    pod_ip,
                    detail: detail.map(clip),
                })
            })
            .take(MAX_REPLICAS_PER_REPORT)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use faber_node_protocol::VolumeKind;

    use super::*;
    use crate::runner::testing::{container, desired, volume};

    fn d(revision: u64) -> DesiredReplica {
        desired(
            "web",
            revision,
            0,
            faber_api_types::workload::RestartPolicy::Always,
            vec![container("app", false)],
            vec![volume("data", VolumeKind::Block)],
        )
    }

    #[test]
    fn a_stop_request_reads_unavailable_at_once() {
        let state = RunnerState::default();
        state.register(&d(1));
        state.mark_launched(&d(1).replica);
        state.set_observed(HashMap::from([(
            d(1).replica,
            Observed {
                state: ReplicaState::Available,
                pod_ip: None,
                detail: None,
            },
        )]));
        assert_eq!(state.replicas()[0].state, ReplicaState::Available);
        state.request_stop(&d(1).replica, StopReason::Removed);
        let report = state.replicas().remove(0);
        assert_eq!(report.state, ReplicaState::Unavailable);
        assert_eq!(report.detail.as_deref(), Some("stopping"));
        // Wanted again before the drain ends: the stop is withdrawn.
        state.register(&d(1));
        assert_eq!(state.replicas()[0].state, ReplicaState::Available);
    }

    #[test]
    fn only_a_superseded_stop_can_be_taken_back() {
        let state = RunnerState::default();
        state.register(&d(1));
        state.request_stop(&d(1).replica, StopReason::Superseded);
        state.cancel_superseded_stop(&d(1).replica);
        assert_eq!(state.entry(&d(1).replica).unwrap().stop, None);
        state.request_stop(&d(1).replica, StopReason::Replaced);
        state.cancel_superseded_stop(&d(1).replica);
        assert_eq!(
            state.entry(&d(1).replica).unwrap().stop,
            Some(StopReason::Replaced)
        );
    }

    #[test]
    fn a_failed_start_shows_in_the_report_before_and_after_launch() {
        let state = RunnerState::default();
        let r = d(1).replica;
        state.register(&d(1));
        state.mark_failed(&r, "pull failed".into());
        let report = state.replicas().remove(0);
        assert_eq!(report.state, ReplicaState::Deploying);
        assert_eq!(report.detail.as_deref(), Some("pull failed"));
        // The same failure on a replica that has been running.
        state.mark_launched(&r);
        state.mark_failed(&r, "could not start again".into());
        let report = state.replicas().remove(0);
        assert_eq!(report.state, ReplicaState::Unavailable);
        assert_eq!(report.detail.as_deref(), Some("could not start again"));
        state.mark_launched(&r);
        assert_eq!(state.replicas()[0].detail, None);
    }

    #[test]
    fn nothing_in_the_state_holds_an_env_value() {
        let state = RunnerState::default();
        state.register(&d(1));
        let text = format!("{:?}", state.entries());
        assert!(!text.contains(crate::runner::testing::SECRET), "{text}");
    }
}
