//! Replica placement: chooses the nodes that run a workload.
//!
//! [`plan`] is a pure function. It takes the workloads of every environment,
//! the nodes with the state of their leases and their reported capacity, and
//! the assignments stored so far, and returns the assignments that should be
//! stored. The same inputs always give the same plan, and feeding a plan back
//! in as the stored assignments gives the same plan again, so the leader
//! proposes only when something changed ([`proposals`]).
//!
//! Rules:
//!
//! - Candidates are nodes with a live lease and a capacity report, narrowed
//!   to `placement.nodes` when the workload lists any.
//! - An assignment is kept while its node is alive, still a candidate, and
//!   the capacity still fits. A replica moves only when its node's lease has
//!   expired or the node is gone, never on stale status.
//! - A replica of a workload with a block volume never moves once placed. If
//!   its node is removed it stays unplaced with `volume_lost`.
//! - `daemon` is one replica per candidate node.
//! - `spread` prefers nodes that run the workload least, then nodes that
//!   already host the environment, then the least loaded. `pack` prefers
//!   nodes that run the workload, then nodes that host the environment, then
//!   the most loaded that still fits.
//! - Each replica reserves the sum of its containers' `cpu_millis` and
//!   `memory_bytes` against the capacity a node reported. Disk is not
//!   reserved.
//!
//! The leader runs the planner from [`spawn_placement_task`] on every change
//! of the replicated state, which includes node reports and the lease
//! clock's ticks.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use faber_api_types::workload::limits::MAX_REPLICAS;
use faber_api_types::workload::{Scale, Strategy, VolumeSource};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::apps::EnvironmentRecord;
use crate::mesh::MeshState;
use crate::raft::store::place::{
    MAX_REASON_BYTES, PlaceReplicas, PlacementRecord, ReplicaPlacement, reason,
};
use crate::raft::store::status::{Capacity, NodeStatus};
use crate::raft::types::Request;
use crate::reconcile::{ReconcileTiming, debounce};
use crate::service::{FaberService, ServiceError, ServiceResult};

/// An environment: `(project, environment)`.
pub type EnvKey = (String, String);

/// The replicas of the workloads of each environment.
pub type Plan = BTreeMap<EnvKey, BTreeMap<String, Vec<ReplicaPlacement>>>;

/// What a replica reserves on its node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cost {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    /// The sizes of the block volumes of the pod. Ramdisks count as memory.
    pub disk_bytes: u64,
}

/// What the planner needs to know about a workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadInput {
    pub scale: Scale,
    /// `placement.nodes`: node names. Any node when empty.
    pub pins: Vec<String>,
    /// Of one replica.
    pub cost: Cost,
    pub block_volume: bool,
}

/// What the planner needs to know about a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInput {
    pub name: String,
    /// The lease has not expired.
    pub live: bool,
    /// What the node last reported, if it ever did.
    pub capacity: Option<Capacity>,
}

/// Everything [`plan`] reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inputs {
    pub workloads: BTreeMap<EnvKey, BTreeMap<String, WorkloadInput>>,
    /// Every node in the mesh.
    pub nodes: BTreeMap<u64, NodeInput>,
    /// The assignments stored now.
    pub current: Plan,
}

/// Chooses the node of every replica. See the module documentation.
pub fn plan(inputs: &Inputs) -> Plan {
    let mut planner = Planner {
        inputs,
        by_name: inputs
            .nodes
            .iter()
            .map(|(id, n)| (n.name.as_str(), *id))
            .collect(),
        reserved: BTreeMap::new(),
        hosts: BTreeMap::new(),
        kept: BTreeMap::new(),
    };
    // Replicas with a block volume first: they cannot move, so the others
    // are placed around them.
    planner.keep(true);
    planner.keep(false);
    planner.place_missing()
}

type WorkloadKey<'a> = (&'a EnvKey, &'a str);

/// Every workload of `inputs` with its environment and name, in order.
fn workloads(inputs: &Inputs) -> Vec<(&EnvKey, &str, &WorkloadInput)> {
    inputs
        .workloads
        .iter()
        .flat_map(|(env, w)| w.iter().map(move |(name, wl)| (env, name.as_str(), wl)))
        .collect()
}

struct Planner<'a> {
    inputs: &'a Inputs,
    by_name: BTreeMap<&'a str, u64>,
    /// Reserved on each node by the replicas decided so far.
    reserved: BTreeMap<u64, Cost>,
    /// The nodes that host a replica of each environment so far.
    hosts: BTreeMap<&'a EnvKey, BTreeSet<u64>>,
    /// The replicas that keep their place or their `volume_lost`.
    kept: BTreeMap<WorkloadKey<'a>, Vec<ReplicaPlacement>>,
}

impl<'a> Planner<'a> {
    fn existing(&self, env: &EnvKey, name: &str) -> Vec<&'a ReplicaPlacement> {
        let mut found: Vec<&ReplicaPlacement> = self
            .inputs
            .current
            .get(env)
            .and_then(|w| w.get(name))
            .map_or_else(Vec::new, |r| r.iter().collect());
        found.sort_by_key(|r| r.index);
        found
    }

    /// Whether `wl` may run on `node`: alive, has reported capacity, and is
    /// not excluded by the workload's pins.
    fn is_candidate(&self, wl: &WorkloadInput, node: u64) -> bool {
        let Some(n) = self.inputs.nodes.get(&node) else {
            return false;
        };
        n.live && n.capacity.is_some() && (wl.pins.is_empty() || wl.pins.contains(&n.name))
    }

    fn fits(&self, node: u64, cost: Cost) -> bool {
        let Some(capacity) = self.inputs.nodes.get(&node).and_then(|n| n.capacity) else {
            return false;
        };
        let used = self.reserved.get(&node).copied().unwrap_or_default();
        used.cpu_millis.saturating_add(cost.cpu_millis) <= capacity.cpu_millis
            && used.memory_bytes.saturating_add(cost.memory_bytes) <= capacity.memory_bytes
            && used.disk_bytes.saturating_add(cost.disk_bytes) <= capacity.disk_bytes
    }

    /// Whether a replica already on `node` may stay: the node is alive and
    /// the workload's pins allow it. Capacity is not asked again: a smaller
    /// report or none at all moves nothing.
    fn may_stay(&self, wl: &WorkloadInput, node: u64) -> bool {
        self.inputs
            .nodes
            .get(&node)
            .is_some_and(|n| n.live && (wl.pins.is_empty() || wl.pins.contains(&n.name)))
    }

    /// The pinned names no node has, sorted.
    fn unknown_pins<'w>(&self, wl: &'w WorkloadInput) -> Vec<&'w str> {
        let mut names: Vec<&str> = wl
            .pins
            .iter()
            .map(String::as_str)
            .filter(|p| !self.by_name.contains_key(*p))
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    /// How full `node` is, in thousandths of its tighter resource.
    fn load(&self, node: u64) -> u64 {
        let capacity = self
            .inputs
            .nodes
            .get(&node)
            .and_then(|n| n.capacity)
            .unwrap_or_default();
        let used = self.reserved.get(&node).copied().unwrap_or_default();
        let ratio = |used: u64, capacity: u64| match capacity {
            0 if used == 0 => 0,
            0 => u64::MAX,
            c => u64::try_from(u128::from(used) * 1000 / u128::from(c)).unwrap_or(u64::MAX),
        };
        ratio(used.cpu_millis, capacity.cpu_millis)
            .max(ratio(used.memory_bytes, capacity.memory_bytes))
    }

    fn reserve(&mut self, env: &'a EnvKey, node: u64, cost: Cost) {
        let used = self.reserved.entry(node).or_default();
        used.cpu_millis = used.cpu_millis.saturating_add(cost.cpu_millis);
        used.memory_bytes = used.memory_bytes.saturating_add(cost.memory_bytes);
        used.disk_bytes = used.disk_bytes.saturating_add(cost.disk_bytes);
        self.hosts.entry(env).or_default().insert(node);
    }

    /// Decides which existing replicas stay, for the workloads with a block
    /// volume (`sticky`) or for the others.
    fn keep(&mut self, sticky: bool) {
        for (env, name, wl) in workloads(self.inputs) {
            if wl.block_volume != sticky {
                continue;
            }
            let daemon = matches!(wl.scale, Scale::Daemon);
            let mut kept: Vec<ReplicaPlacement> = Vec::new();
            // A daemon runs once per node; others may share one.
            let mut nodes = BTreeSet::new();
            for r in self.existing(env, name) {
                if let Scale::Replicas { count, .. } = wl.scale
                    && r.index >= count
                {
                    continue;
                }
                match r.node {
                    // The volume is on the node: stay while the node is in
                    // the mesh, whatever its lease or capacity say.
                    Some(node) if sticky => {
                        if !self.inputs.nodes.contains_key(&node) {
                            kept.push(ReplicaPlacement::unplaced(r.index, reason::VOLUME_LOST));
                        } else if !daemon || nodes.insert(node) {
                            self.reserve(env, node, wl.cost);
                            kept.push(r.clone());
                        }
                    }
                    Some(node) if self.may_stay(wl, node) && !nodes.contains(&node) => {
                        if daemon {
                            nodes.insert(node);
                        }
                        self.reserve(env, node, wl.cost);
                        kept.push(r.clone());
                    }
                    None if sticky && r.reason.as_deref() == Some(reason::VOLUME_LOST) => {
                        kept.push(r.clone());
                    }
                    _ => {}
                }
            }
            self.kept.insert((env, name), kept);
        }
    }

    fn place_missing(mut self) -> Plan {
        let mut out: Plan = BTreeMap::new();
        for (env, name, wl) in workloads(self.inputs) {
            let kept = self.kept.remove(&(env, name)).unwrap_or_default();
            let replicas = match wl.scale {
                Scale::Replicas { count, strategy } => {
                    self.place_replicated(env, wl, count, strategy, kept)
                }
                Scale::Daemon => self.place_daemon(env, wl, kept),
            };
            out.entry(env.clone())
                .or_default()
                .insert(name.to_string(), replicas);
        }
        out
    }

    fn place_replicated(
        &mut self,
        env: &'a EnvKey,
        wl: &WorkloadInput,
        count: u32,
        strategy: Strategy,
        mut replicas: Vec<ReplicaPlacement>,
    ) -> Vec<ReplicaPlacement> {
        let count = count.min(MAX_REPLICAS);
        let have: BTreeSet<u32> = replicas.iter().map(|r| r.index).collect();
        for index in (0..count).filter(|i| !have.contains(i)) {
            let placement = match self.choose(env, wl, strategy, &replicas) {
                Ok(node) => {
                    self.reserve(env, node, wl.cost);
                    ReplicaPlacement::on(index, node)
                }
                Err(why) => ReplicaPlacement::unplaced(index, &why),
            };
            replicas.push(placement);
        }
        replicas.sort_by_key(|r| r.index);
        replicas
    }

    /// The node for one more replica of `wl`, or why there is none.
    /// `placed` are the replicas of the workload decided so far.
    fn choose(
        &self,
        env: &'a EnvKey,
        wl: &WorkloadInput,
        strategy: Strategy,
        placed: &[ReplicaPlacement],
    ) -> Result<u64, String> {
        // A pinned name no node has explains every replica left unplaced.
        let unknown = self.unknown_pins(wl);
        let unplaced = |why: &str| {
            if unknown.is_empty() {
                why.to_string()
            } else {
                pinned_unknown_reason(&unknown)
            }
        };
        let allowed: Vec<u64> = self
            .inputs
            .nodes
            .iter()
            .filter(|(_, n)| wl.pins.is_empty() || wl.pins.contains(&n.name))
            .map(|(id, _)| *id)
            .collect();
        if allowed.is_empty() {
            return Err(unplaced(reason::NO_NODE_AVAILABLE));
        }
        let candidates: Vec<u64> = allowed
            .into_iter()
            .filter(|id| self.is_candidate(wl, *id))
            .collect();
        if candidates.is_empty() {
            return Err(unplaced(reason::NO_NODE_AVAILABLE));
        }
        let hosts = self.hosts.get(env);
        let running = |node: u64| placed.iter().filter(|r| r.node == Some(node)).count();
        let hosts_env = |node: u64| hosts.is_some_and(|h| h.contains(&node));
        let fitting = candidates.into_iter().filter(|id| self.fits(*id, wl.cost));
        let chosen = match strategy {
            Strategy::Spread => {
                fitting.min_by_key(|id| (running(*id), !hosts_env(*id), self.load(*id), *id))
            }
            Strategy::Pack => fitting.min_by_key(|id| {
                (
                    running(*id) == 0,
                    !hosts_env(*id),
                    Reverse(self.load(*id)),
                    *id,
                )
            }),
        };
        chosen.ok_or_else(|| unplaced(reason::INSUFFICIENT_CAPACITY))
    }

    fn place_daemon(
        &mut self,
        env: &'a EnvKey,
        wl: &WorkloadInput,
        mut replicas: Vec<ReplicaPlacement>,
    ) -> Vec<ReplicaPlacement> {
        let on: BTreeSet<u64> = replicas.iter().filter_map(|r| r.node).collect();
        let mut used: BTreeSet<u32> = replicas.iter().map(|r| r.index).collect();
        let mut next_index = move || {
            let index = (0..).find(|i| !used.contains(i)).unwrap_or(u32::MAX);
            used.insert(index);
            index
        };
        let mut added = Vec::new();
        let candidates: Vec<u64> = self
            .inputs
            .nodes
            .keys()
            .copied()
            .filter(|id| !on.contains(id) && self.is_candidate(wl, *id))
            .collect();
        for node in candidates {
            if replicas.len() + added.len() >= MAX_REPLICAS as usize {
                break;
            }
            added.push(if self.fits(node, wl.cost) {
                self.reserve(env, node, wl.cost);
                ReplicaPlacement::on(next_index(), node)
            } else {
                ReplicaPlacement::unplaced(next_index(), reason::INSUFFICIENT_CAPACITY)
            });
        }
        // A listed node the cluster does not have.
        let mut pins: Vec<&String> = wl.pins.iter().collect();
        pins.sort();
        for pin in pins {
            if !self.by_name.contains_key(pin.as_str())
                && replicas.len() + added.len() < MAX_REPLICAS as usize
            {
                added.push(ReplicaPlacement::unplaced(
                    next_index(),
                    &pinned_unknown_reason(&[pin.as_str()]),
                ));
            }
        }
        replicas.extend(added);
        replicas.sort_by_key(|r| r.index);
        replicas
    }
}

/// `pinned_node_unknown: a, b`, cut to [`MAX_REASON_BYTES`].
fn pinned_unknown_reason(names: &[&str]) -> String {
    let mut text = format!("{}: {}", reason::PINNED_NODE_UNKNOWN, names.join(", "));
    if text.len() > MAX_REASON_BYTES {
        let mut end = MAX_REASON_BYTES - 3;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("...");
    }
    text
}

/// The inputs for [`plan`] from the replicated state: the environments, the
/// mesh with its lease clock, the nodes' reports, and the stored plans.
pub fn gather(
    environments: &[(String, String, EnvironmentRecord)],
    mesh: &MeshState,
    statuses: &BTreeMap<u64, NodeStatus>,
    records: &BTreeMap<EnvKey, PlacementRecord>,
) -> Inputs {
    let nodes = mesh
        .nodes
        .iter()
        .map(|(id, n)| {
            let input = NodeInput {
                name: n.name.clone(),
                live: !mesh.lease_expired(n),
                capacity: statuses.get(id).map(|s| s.capacity),
            };
            (*id, input)
        })
        .collect();
    let workloads = environments
        .iter()
        .map(|(project, environment, record)| {
            let workloads = record
                .workloads
                .iter()
                .map(|(name, w)| {
                    let cost = w
                        .spec
                        .containers
                        .iter()
                        .fold(Cost::default(), |sum, c| Cost {
                            cpu_millis: sum.cpu_millis.saturating_add(c.resources.cpu_millis),
                            memory_bytes: sum.memory_bytes.saturating_add(c.resources.memory_bytes),
                            disk_bytes: sum.disk_bytes,
                        });
                    let cost = Cost {
                        disk_bytes: w
                            .spec
                            .volumes
                            .values()
                            .map(|v| match v {
                                VolumeSource::Block { size_bytes } => *size_bytes,
                                VolumeSource::Ramdisk { .. } => 0,
                            })
                            .fold(0, u64::saturating_add),
                        ..cost
                    };
                    let input = WorkloadInput {
                        scale: w.spec.scale,
                        pins: w.spec.placement.nodes.clone(),
                        cost,
                        block_volume: w
                            .spec
                            .volumes
                            .values()
                            .any(|v| matches!(v, VolumeSource::Block { .. })),
                    };
                    (name.clone(), input)
                })
                .collect();
            ((project.clone(), environment.clone()), workloads)
        })
        .collect();
    let current = records
        .iter()
        .map(|(env, record)| {
            let workloads = record
                .workloads
                .iter()
                .map(|(name, w)| (name.clone(), w.replicas.clone()))
                .collect();
            (env.clone(), workloads)
        })
        .collect();
    Inputs {
        workloads,
        nodes,
        current,
    }
}

/// The commands that bring the stored plans to `plan`, which was computed
/// from the state as of log entry `basis`: one per environment
/// whose workloads or replicas differ from the stored record, and an empty
/// one for every record whose environment is gone. Nothing for a plan that
/// is already stored, whatever generation it was made at.
pub fn proposals(
    environments: &[(String, String, EnvironmentRecord)],
    records: &BTreeMap<EnvKey, PlacementRecord>,
    plan: &Plan,
    basis: u64,
) -> Vec<PlaceReplicas> {
    let mut commands = Vec::new();
    for (project, environment, record) in environments {
        let env = (project.clone(), environment.clone());
        let wanted = plan.get(&env);
        let stored = records.get(&env);
        let same = match (wanted, stored) {
            (None, None) => true,
            (Some(wanted), None) => wanted.is_empty(),
            (None, Some(stored)) => stored.workloads.is_empty(),
            (Some(wanted), Some(stored)) => {
                wanted.len() == stored.workloads.len()
                    && wanted.iter().all(|(name, replicas)| {
                        stored.replicas(name) == replicas.as_slice()
                            && stored.workloads.contains_key(name)
                    })
            }
        };
        if !same {
            commands.push(PlaceReplicas {
                project: project.clone(),
                environment: environment.clone(),
                generation: record.generation,
                basis,
                workloads: wanted.cloned().unwrap_or_default(),
            });
        }
    }
    for (project, environment) in records.keys() {
        if !environments
            .iter()
            .any(|(p, e, _)| p == project && e == environment)
        {
            commands.push(PlaceReplicas {
                project: project.clone(),
                environment: environment.clone(),
                generation: 0,
                basis,
                workloads: BTreeMap::new(),
            });
        }
    }
    commands
}

/// Longest wait for one proposal.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// The planner runs at least this often while the node leads, in case no
/// change reaches it: a new leader sees no change until the lease clock
/// ticks.
const RESYNC: Duration = Duration::from_secs(30);

impl FaberService {
    /// Computes the plan from this node's copy of the state and proposes
    /// what differs from the stored plans. Returns how many commands were
    /// accepted. A command that apply refuses is normal when the state moved
    /// since it was computed: the next change plans again.
    pub(crate) async fn place_once(&self) -> ServiceResult<usize> {
        // Plan from what the cluster has agreed on, not from what this
        // node has applied so far, and from one view of it.
        self.settle().await?;
        let view = self
            .state()
            .placement_view()
            .map_err(|e| ServiceError::Internal(e.to_string()))?;
        let Some(mesh) = view.mesh.as_ref() else {
            return Ok(0);
        };
        let inputs = gather(&view.environments, mesh, &view.statuses, &view.records);
        let commands = proposals(
            &view.environments,
            &view.records,
            &plan(&inputs),
            view.applied,
        );
        let mut accepted = 0;
        for command in commands {
            let (project, environment) = (command.project.clone(), command.environment.clone());
            let write = self.propose(Request::PlaceReplicas(command));
            match tokio::time::timeout(WRITE_TIMEOUT, write).await {
                Ok(Ok(_)) => accepted += 1,
                Ok(Err(e)) => {
                    tracing::debug!(%project, %environment, error = %e, "placement was not accepted");
                }
                Err(_) => {
                    tracing::debug!(%project, %environment, "placement timed out");
                    break;
                }
            }
        }
        Ok(accepted)
    }
}

/// Runs [`FaberService::place_once`] on every change of the replicated state
/// (debounced) while this node leads, until `cancel` fires.
pub fn spawn_placement_task(
    service: FaberService,
    timing: ReconcileTiming,
    resync: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut changes = service.state().subscribe();
        let mut last = Instant::now();
        loop {
            let wake = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                changed = changes.changed() => changed.is_ok(),
                () = tokio::time::sleep_until(last + resync) => true,
            };
            if !wake || !debounce(&mut changes, &timing, &cancel).await {
                return;
            }
            last = Instant::now();
            if !service.is_leader() {
                continue;
            }
            match service.place_once().await {
                Ok(0) => {}
                Ok(n) => tracing::info!(commands = n, "placed replicas"),
                Err(ServiceError::NotReady(why)) => {
                    tracing::debug!(%why, "placement waits for the next change");
                }
                Err(e) => tracing::warn!(error = %e, "placement failed; trying on the next change"),
            }
        }
    })
}

/// [`spawn_placement_task`] with the timing a node runs with.
pub fn spawn_default(service: FaberService, cancel: CancellationToken) -> JoinHandle<()> {
    spawn_placement_task(service, ReconcileTiming::default(), RESYNC, cancel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::WorkloadRecord;
    use crate::mesh::state::{MeshConfig, NodeRecord};
    use crate::raft::store::place::{UID_RANGE_START, WorkloadPlacement};

    const CPU: u64 = 100;
    const MEM: u64 = 100;

    fn env(name: &str) -> EnvKey {
        ("shop".to_string(), name.to_string())
    }

    fn cost() -> Cost {
        Cost {
            cpu_millis: CPU,
            memory_bytes: MEM,
            disk_bytes: 0,
        }
    }

    fn replicas(count: u32, strategy: Strategy) -> WorkloadInput {
        WorkloadInput {
            scale: Scale::Replicas { count, strategy },
            pins: Vec::new(),
            cost: cost(),
            block_volume: false,
        }
    }

    fn daemon() -> WorkloadInput {
        WorkloadInput {
            scale: Scale::Daemon,
            ..replicas(1, Strategy::Spread)
        }
    }

    fn capacity(slots: u64) -> Option<Capacity> {
        Some(Capacity {
            cpu_millis: CPU * slots,
            memory_bytes: MEM * slots,
            disk_bytes: 0,
        })
    }

    fn node(id: u64, slots: u64) -> NodeInput {
        NodeInput {
            name: format!("n{id}"),
            live: true,
            capacity: capacity(slots),
        }
    }

    /// Nodes 1..=n named `n1`..: alive, room for `slots` replicas.
    fn cluster(n: u64, slots: u64) -> Inputs {
        let mut inputs = Inputs::default();
        for id in 1..=n {
            inputs.nodes.insert(id, node(id, slots));
        }
        inputs
    }

    fn add(inputs: &mut Inputs, environment: &str, name: &str, wl: WorkloadInput) {
        inputs
            .workloads
            .entry(env(environment))
            .or_default()
            .insert(name.to_string(), wl);
    }

    fn set_current(inputs: &mut Inputs, environment: &str, name: &str, r: Vec<ReplicaPlacement>) {
        inputs
            .current
            .entry(env(environment))
            .or_default()
            .insert(name.to_string(), r);
    }

    fn workload_mut<'a>(inputs: &'a mut Inputs, name: &str) -> &'a mut WorkloadInput {
        inputs
            .workloads
            .get_mut(&env("staging"))
            .unwrap()
            .get_mut(name)
            .unwrap()
    }

    fn nodes_of(plan: &Plan, environment: &str, name: &str) -> Vec<Option<u64>> {
        plan[&env(environment)][name]
            .iter()
            .map(|r| r.node)
            .collect()
    }

    fn reasons_of<'a>(plan: &'a Plan, environment: &str, name: &str) -> Vec<Option<&'a str>> {
        plan[&env(environment)][name]
            .iter()
            .map(|r| r.reason.as_deref())
            .collect()
    }

    /// The plan is stable: planned again from itself it is the same.
    fn assert_stable(inputs: &Inputs, planned: &Plan) {
        let mut again = inputs.clone();
        again.current = planned.clone();
        assert_eq!(&plan(&again), planned);
    }

    #[test]
    fn spread_puts_replicas_on_different_nodes() {
        let mut inputs = cluster(3, 10);
        add(&mut inputs, "staging", "web", replicas(3, Strategy::Spread));
        let planned = plan(&inputs);
        let mut got = nodes_of(&planned, "staging", "web");
        got.sort();
        assert_eq!(got, [Some(1), Some(2), Some(3)]);
        assert_stable(&inputs, &planned);
    }

    #[test]
    fn spread_shares_nodes_when_there_are_more_replicas_than_nodes() {
        let mut inputs = cluster(2, 10);
        add(&mut inputs, "staging", "web", replicas(5, Strategy::Spread));
        let planned = plan(&inputs);
        let nodes = nodes_of(&planned, "staging", "web");
        let on = |id| nodes.iter().filter(|n| **n == Some(id)).count();
        assert_eq!((on(1), on(2)), (3, 2));
    }

    #[test]
    fn pack_fills_one_node_before_the_next() {
        let mut inputs = cluster(3, 3);
        add(&mut inputs, "staging", "web", replicas(5, Strategy::Pack));
        let planned = plan(&inputs);
        let nodes = nodes_of(&planned, "staging", "web");
        // Three fit on the first node; the other two go to one other node.
        let first = nodes[0].unwrap();
        assert_eq!(nodes.iter().filter(|n| **n == Some(first)).count(), 3);
        let second = nodes.iter().find(|n| **n != Some(first)).unwrap();
        assert_eq!(nodes.iter().filter(|n| *n == second).count(), 2);
        assert_stable(&inputs, &planned);
    }

    #[test]
    fn pack_prefers_the_node_that_already_runs_the_workload() {
        let mut inputs = cluster(3, 10);
        add(&mut inputs, "staging", "web", replicas(2, Strategy::Pack));
        set_current(
            &mut inputs,
            "staging",
            "web",
            vec![ReplicaPlacement::on(0, 3)],
        );
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "web"), [Some(3), Some(3)]);
    }

    #[test]
    fn an_environment_stays_together_while_capacity_allows() {
        let mut inputs = cluster(3, 10);
        add(&mut inputs, "staging", "db", replicas(1, Strategy::Spread));
        add(&mut inputs, "staging", "web", replicas(1, Strategy::Spread));
        add(
            &mut inputs,
            "production",
            "db",
            replicas(1, Strategy::Spread),
        );
        // The production db runs on node 2, the staging db on node 3.
        set_current(
            &mut inputs,
            "production",
            "db",
            vec![ReplicaPlacement::on(0, 2)],
        );
        set_current(
            &mut inputs,
            "staging",
            "db",
            vec![ReplicaPlacement::on(0, 3)],
        );
        let planned = plan(&inputs);
        // web joins the db of its own environment, not the least loaded node.
        assert_eq!(nodes_of(&planned, "staging", "web"), [Some(3)]);
        assert_eq!(nodes_of(&planned, "staging", "db"), [Some(3)]);
        assert_eq!(nodes_of(&planned, "production", "db"), [Some(2)]);
    }

    #[test]
    fn an_environment_spills_over_when_its_node_is_full() {
        let mut inputs = cluster(2, 2);
        add(&mut inputs, "staging", "db", replicas(1, Strategy::Spread));
        add(&mut inputs, "staging", "web", replicas(2, Strategy::Spread));
        let planned = plan(&inputs);
        // Three replicas of one environment, two slots a node.
        let mut all = nodes_of(&planned, "staging", "db");
        all.extend(nodes_of(&planned, "staging", "web"));
        assert!(all.iter().all(Option::is_some), "{all:?}");
        assert!(all.contains(&Some(1)) && all.contains(&Some(2)), "{all:?}");
    }

    #[test]
    fn spread_prefers_a_node_with_the_environment_among_those_without_the_workload() {
        let mut inputs = cluster(3, 10);
        add(&mut inputs, "staging", "db", replicas(1, Strategy::Spread));
        add(&mut inputs, "staging", "web", replicas(2, Strategy::Spread));
        set_current(
            &mut inputs,
            "staging",
            "db",
            vec![ReplicaPlacement::on(0, 2)],
        );
        // Node 1 is the least loaded, but node 2 hosts the environment.
        let planned = plan(&inputs);
        let web = nodes_of(&planned, "staging", "web");
        assert_eq!(web[0], Some(2));
        // The second replica does not share a node that runs web.
        assert_ne!(web[1], Some(2));
    }

    #[test]
    fn a_daemon_runs_once_on_every_candidate_node() {
        let mut inputs = cluster(4, 10);
        inputs.nodes.get_mut(&2).unwrap().live = false;
        inputs.nodes.get_mut(&3).unwrap().capacity = None;
        add(&mut inputs, "staging", "agent", daemon());
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "agent"), [Some(1), Some(4)]);
        assert_stable(&inputs, &planned);

        // A new node gets a replica without moving the others.
        inputs.current = planned;
        inputs.nodes.insert(5, node(5, 10));
        let planned = plan(&inputs);
        assert_eq!(
            planned[&env("staging")]["agent"],
            [
                ReplicaPlacement::on(0, 1),
                ReplicaPlacement::on(1, 4),
                ReplicaPlacement::on(2, 5)
            ]
        );
        // A node that dies loses its replica.
        inputs.nodes.get_mut(&4).unwrap().live = false;
        inputs.current = planned;
        let planned = plan(&inputs);
        assert_eq!(
            planned[&env("staging")]["agent"],
            [ReplicaPlacement::on(0, 1), ReplicaPlacement::on(2, 5)]
        );
    }

    #[test]
    fn a_daemon_node_without_room_shows_as_unplaced() {
        let mut inputs = cluster(2, 10);
        inputs.nodes.get_mut(&2).unwrap().capacity = capacity(0);
        add(&mut inputs, "staging", "agent", daemon());
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "agent"), [Some(1), None]);
        assert_eq!(
            reasons_of(&planned, "staging", "agent"),
            [None, Some(reason::INSUFFICIENT_CAPACITY)]
        );
        assert_stable(&inputs, &planned);
    }

    #[test]
    fn a_daemon_with_pins_runs_on_the_listed_nodes_only() {
        let mut inputs = cluster(3, 10);
        let mut wl = daemon();
        wl.pins = vec!["n3".into(), "n1".into(), "ghost".into()];
        add(&mut inputs, "staging", "agent", wl);
        let planned = plan(&inputs);
        let replicas = &planned[&env("staging")]["agent"];
        assert_eq!(
            replicas.iter().map(|r| r.node).collect::<Vec<_>>(),
            [Some(1), Some(3), None]
        );
        assert_eq!(
            replicas[2].reason.as_deref(),
            Some("pinned_node_unknown: ghost")
        );
        assert_stable(&inputs, &planned);
    }

    #[test]
    fn a_pin_narrows_the_candidates() {
        let mut inputs = cluster(3, 10);
        let mut wl = replicas(2, Strategy::Spread);
        wl.pins = vec!["n2".into(), "n3".into()];
        add(&mut inputs, "staging", "web", wl);
        let planned = plan(&inputs);
        let mut got = nodes_of(&planned, "staging", "web");
        got.sort();
        assert_eq!(got, [Some(2), Some(3)]);
    }

    #[test]
    fn a_pin_to_an_unknown_node_leaves_the_replica_unplaced() {
        let mut inputs = cluster(2, 10);
        let mut wl = replicas(2, Strategy::Spread);
        wl.pins = vec!["ghost".into()];
        add(&mut inputs, "staging", "web", wl);
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "web"), [None, None]);
        assert_eq!(
            reasons_of(&planned, "staging", "web"),
            [
                Some("pinned_node_unknown: ghost"),
                Some("pinned_node_unknown: ghost")
            ]
        );
        assert_stable(&inputs, &planned);
        // One known pin and one unknown: the known one is used.
        workload_mut(&mut inputs, "web").pins = vec!["ghost".into(), "n2".into()];
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "web"), [Some(2), Some(2)]);
        // When the known node is full, the unknown name is the reason.
        inputs.nodes.get_mut(&2).unwrap().capacity = capacity(1);
        inputs.current.clear();
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "web"), [Some(2), None]);
        assert_eq!(
            reasons_of(&planned, "staging", "web")[1],
            Some("pinned_node_unknown: ghost")
        );
        // Many long names are cut to the bound.
        let names: Vec<String> = (0..20).map(|i| format!("missing-node-{i}")).collect();
        workload_mut(&mut inputs, "web").pins = names;
        let planned = plan(&inputs);
        let why = planned[&env("staging")]["web"][0].reason.clone().unwrap();
        assert!(why.len() <= MAX_REASON_BYTES, "{why}");
        assert!(why.starts_with("pinned_node_unknown: missing-node-0"));
    }

    #[test]
    fn a_pinned_node_that_is_dead_leaves_the_replica_unplaced() {
        let mut inputs = cluster(2, 10);
        inputs.nodes.get_mut(&2).unwrap().live = false;
        let mut wl = replicas(1, Strategy::Spread);
        wl.pins = vec!["n2".into()];
        add(&mut inputs, "staging", "web", wl);
        let planned = plan(&inputs);
        assert_eq!(
            reasons_of(&planned, "staging", "web"),
            [Some(reason::NO_NODE_AVAILABLE)]
        );
    }

    #[test]
    fn replicas_that_fit_nowhere_stay_unplaced() {
        let mut inputs = cluster(2, 2);
        add(&mut inputs, "staging", "web", replicas(5, Strategy::Spread));
        let planned = plan(&inputs);
        let replicas = &planned[&env("staging")]["web"];
        assert_eq!(replicas.iter().filter(|r| r.node.is_some()).count(), 4);
        assert_eq!(replicas[4].node, None);
        assert_eq!(
            replicas[4].reason.as_deref(),
            Some(reason::INSUFFICIENT_CAPACITY)
        );
        assert_stable(&inputs, &planned);

        // Room appears: the unplaced replica is placed, the others stay.
        inputs.current = planned.clone();
        inputs.nodes.get_mut(&1).unwrap().capacity = capacity(3);
        let after = plan(&inputs);
        assert_eq!(
            after[&env("staging")]["web"][..4],
            planned[&env("staging")]["web"][..4]
        );
        assert_eq!(after[&env("staging")]["web"][4].node, Some(1));
    }

    #[test]
    fn memory_and_cpu_are_both_reserved() {
        let mut inputs = cluster(1, 10);
        inputs.nodes.get_mut(&1).unwrap().capacity = Some(Capacity {
            cpu_millis: 10_000,
            memory_bytes: 250,
            disk_bytes: 0,
        });
        add(&mut inputs, "staging", "web", replicas(3, Strategy::Pack));
        let planned = plan(&inputs);
        // Memory allows two of 100.
        assert_eq!(
            nodes_of(&planned, "staging", "web"),
            [Some(1), Some(1), None]
        );
    }

    #[test]
    fn a_replica_stays_when_its_node_reports_less_or_nothing() {
        let mut inputs = cluster(2, 10);
        add(&mut inputs, "staging", "web", replicas(2, Strategy::Spread));
        let both = vec![ReplicaPlacement::on(0, 1), ReplicaPlacement::on(1, 1)];
        set_current(&mut inputs, "staging", "web", both.clone());
        // The node now reports less than the replicas need...
        inputs.nodes.get_mut(&1).unwrap().capacity = Some(Capacity {
            cpu_millis: 50,
            memory_bytes: 50,
            disk_bytes: 0,
        });
        assert_eq!(plan(&inputs)[&env("staging")]["web"], both);
        // ...or no report at all.
        inputs.nodes.get_mut(&1).unwrap().capacity = None;
        assert_eq!(plan(&inputs)[&env("staging")]["web"], both);
        // A daemon replica stays too.
        add(&mut inputs, "staging", "agent", daemon());
        set_current(
            &mut inputs,
            "staging",
            "agent",
            vec![ReplicaPlacement::on(0, 1)],
        );
        assert_eq!(nodes_of(&plan(&inputs), "staging", "agent")[0], Some(1));
    }

    #[test]
    fn only_new_replicas_must_fit_beside_the_kept_ones() {
        let mut inputs = cluster(1, 2);
        add(&mut inputs, "staging", "web", replicas(3, Strategy::Spread));
        set_current(
            &mut inputs,
            "staging",
            "web",
            vec![ReplicaPlacement::on(0, 1)],
        );
        // One is kept, one more fits, the third does not.
        let planned = plan(&inputs);
        assert_eq!(
            nodes_of(&planned, "staging", "web"),
            [Some(1), Some(1), None]
        );
        // A pin that now excludes the node does move the replica.
        let mut inputs = cluster(2, 10);
        let mut wl = replicas(1, Strategy::Spread);
        wl.pins = vec!["n2".into()];
        add(&mut inputs, "staging", "web", wl);
        set_current(
            &mut inputs,
            "staging",
            "web",
            vec![ReplicaPlacement::on(0, 1)],
        );
        assert_eq!(nodes_of(&plan(&inputs), "staging", "web"), [Some(2)]);
    }

    #[test]
    fn block_volumes_are_reserved_against_the_disk_a_node_reported() {
        let mut inputs = cluster(2, 10);
        inputs.nodes.get_mut(&1).unwrap().capacity = Some(Capacity {
            cpu_millis: 10_000,
            memory_bytes: 10_000,
            disk_bytes: 250,
        });
        let mut wl = replicas(3, Strategy::Pack);
        wl.cost.disk_bytes = 100;
        add(&mut inputs, "staging", "db", wl);
        // Node 2 reports no disk at all: nothing with a volume fits there.
        let planned = plan(&inputs);
        assert_eq!(
            nodes_of(&planned, "staging", "db"),
            [Some(1), Some(1), None]
        );
        assert_eq!(
            reasons_of(&planned, "staging", "db")[2],
            Some(reason::INSUFFICIENT_CAPACITY)
        );
        assert_stable(&inputs, &planned);
    }

    #[test]
    fn existing_assignments_do_not_churn() {
        let mut inputs = cluster(3, 10);
        add(&mut inputs, "staging", "web", replicas(2, Strategy::Spread));
        // Both on node 3, which spread would not choose today.
        let both = vec![ReplicaPlacement::on(0, 3), ReplicaPlacement::on(1, 3)];
        set_current(&mut inputs, "staging", "web", both.clone());
        let planned = plan(&inputs);
        assert_eq!(planned[&env("staging")]["web"], both);
        // Scaling up places only the new replica; scaling down drops the end.
        add(&mut inputs, "staging", "web", replicas(3, Strategy::Spread));
        let up = plan(&inputs);
        assert_eq!(up[&env("staging")]["web"][..2], both[..]);
        assert_ne!(up[&env("staging")]["web"][2].node, Some(3));
        add(&mut inputs, "staging", "web", replicas(1, Strategy::Spread));
        assert_eq!(nodes_of(&plan(&inputs), "staging", "web"), [Some(3)]);
    }

    #[test]
    fn a_replica_moves_only_when_its_lease_has_expired_or_its_node_is_gone() {
        let mut inputs = cluster(3, 10);
        add(&mut inputs, "staging", "web", replicas(1, Strategy::Spread));
        set_current(
            &mut inputs,
            "staging",
            "web",
            vec![ReplicaPlacement::on(0, 3)],
        );
        assert_eq!(nodes_of(&plan(&inputs), "staging", "web"), [Some(3)]);
        // Lease expired: moved to a live node.
        inputs.nodes.get_mut(&3).unwrap().live = false;
        let moved = nodes_of(&plan(&inputs), "staging", "web");
        assert!(matches!(moved[0], Some(1) | Some(2)), "{moved:?}");
        // Node removed: also moved.
        inputs.nodes.remove(&3);
        let moved = nodes_of(&plan(&inputs), "staging", "web");
        assert!(matches!(moved[0], Some(1) | Some(2)), "{moved:?}");
    }

    #[test]
    fn a_block_volume_replica_never_moves() {
        let mut inputs = cluster(3, 10);
        let mut wl = replicas(1, Strategy::Spread);
        wl.block_volume = true;
        add(&mut inputs, "staging", "db", wl);
        set_current(
            &mut inputs,
            "staging",
            "db",
            vec![ReplicaPlacement::on(0, 3)],
        );
        // Neither an expired lease, nor no room, nor a pin elsewhere moves it.
        inputs.nodes.get_mut(&3).unwrap().live = false;
        inputs.nodes.get_mut(&3).unwrap().capacity = Some(Capacity::default());
        workload_mut(&mut inputs, "db").pins = vec!["n1".into()];
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "db"), [Some(3)]);
        assert_stable(&inputs, &planned);
    }

    #[test]
    fn a_block_volume_replica_whose_node_is_removed_is_lost() {
        let mut inputs = cluster(3, 10);
        let mut wl = replicas(2, Strategy::Spread);
        wl.block_volume = true;
        add(&mut inputs, "staging", "db", wl);
        let both = vec![ReplicaPlacement::on(0, 3), ReplicaPlacement::on(1, 2)];
        set_current(&mut inputs, "staging", "db", both);
        inputs.nodes.remove(&3);
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "db"), [None, Some(2)]);
        assert_eq!(
            reasons_of(&planned, "staging", "db"),
            [Some(reason::VOLUME_LOST), None]
        );
        // It stays lost, also when other nodes have room.
        assert_stable(&inputs, &planned);
        // Without the block volume the replica is placed again.
        inputs.current = planned;
        workload_mut(&mut inputs, "db").block_volume = false;
        let planned = plan(&inputs);
        assert!(planned[&env("staging")]["db"][0].node.is_some());
    }

    #[test]
    fn block_volume_replicas_and_kept_replicas_hold_their_place_before_new_ones() {
        let mut inputs = cluster(1, 1);
        let mut db = replicas(1, Strategy::Spread);
        db.block_volume = true;
        // `api` sorts before `db` and also has a replica on node 1.
        add(&mut inputs, "staging", "api", replicas(1, Strategy::Spread));
        add(&mut inputs, "staging", "db", db);
        set_current(
            &mut inputs,
            "staging",
            "api",
            vec![ReplicaPlacement::on(0, 1)],
        );
        set_current(
            &mut inputs,
            "staging",
            "db",
            vec![ReplicaPlacement::on(0, 1)],
        );
        let planned = plan(&inputs);
        assert_eq!(nodes_of(&planned, "staging", "db"), [Some(1)]);
        assert_eq!(nodes_of(&planned, "staging", "api"), [Some(1)]);
        // A new replica finds the node full: kept replicas hold their place.
        add(&mut inputs, "staging", "new", replicas(1, Strategy::Spread));
        assert_eq!(nodes_of(&plan(&inputs), "staging", "new"), [None]);
    }

    #[test]
    fn a_removed_workload_is_not_in_the_plan_and_holds_no_capacity() {
        let mut inputs = cluster(1, 1);
        add(&mut inputs, "staging", "web", replicas(1, Strategy::Spread));
        set_current(
            &mut inputs,
            "staging",
            "gone",
            vec![ReplicaPlacement::on(0, 1)],
        );
        let planned = plan(&inputs);
        assert!(!planned[&env("staging")].contains_key("gone"));
        assert_eq!(nodes_of(&planned, "staging", "web"), [Some(1)]);
    }

    #[test]
    fn zero_replicas_is_an_empty_list() {
        let mut inputs = cluster(2, 10);
        add(&mut inputs, "staging", "web", replicas(0, Strategy::Spread));
        assert!(plan(&inputs)[&env("staging")]["web"].is_empty());
    }

    #[test]
    fn no_nodes_means_nothing_is_placed() {
        let mut inputs = Inputs::default();
        add(&mut inputs, "staging", "web", replicas(2, Strategy::Spread));
        let planned = plan(&inputs);
        assert_eq!(
            reasons_of(&planned, "staging", "web"),
            [
                Some(reason::NO_NODE_AVAILABLE),
                Some(reason::NO_NODE_AVAILABLE)
            ]
        );
        assert_stable(&inputs, &planned);
    }

    // ---- state to inputs, and inputs to commands ----

    fn environment(generation: u64, workloads: &[(&str, serde_json::Value)]) -> EnvironmentRecord {
        EnvironmentRecord {
            generation,
            updated_at: 0,
            workloads: workloads
                .iter()
                .map(|(name, json)| {
                    let record = WorkloadRecord {
                        revision: 1,
                        spec: serde_json::from_value(json.clone()).unwrap(),
                        env: BTreeMap::new(),
                    };
                    (name.to_string(), record)
                })
                .collect(),
        }
    }

    fn mesh_of(nodes: &[(u64, &str, u64)]) -> MeshState {
        let node = |id: u64, name: &str, renewed: u64| NodeRecord {
            node_id: id,
            public_key: "k".into(),
            mesh_ip: format!("10.99.0.{id}").parse().unwrap(),
            endpoint: None,
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: name.into(),
            lease_renewed_at: renewed,
        };
        let mut mesh = MeshState::init(MeshConfig::default(), node(1, "n1", 0)).unwrap();
        mesh.nodes = nodes
            .iter()
            .map(|(id, name, renewed)| (*id, node(*id, name, *renewed)))
            .collect();
        mesh
    }

    fn status(cpu: u64, mem: u64) -> NodeStatus {
        NodeStatus {
            reported_at: 0,
            capacity: Capacity {
                cpu_millis: cpu,
                memory_bytes: mem,
                disk_bytes: 0,
            },
            replicas: Vec::new(),
        }
    }

    #[test]
    fn inputs_come_from_the_specs_the_mesh_and_the_reports() {
        let db = serde_json::json!({
            "scale": "daemon",
            "placement": {"nodes": ["n2"]},
            "volumes": {"data": {"type": "block", "size_bytes": 1048576}},
            "containers": [
                {"name": "a", "image": "pg", "resources": {"cpu_millis": 300, "memory_bytes": 1000}},
                {"name": "b", "image": "x", "resources": {"cpu_millis": 200, "memory_bytes": 24}},
            ],
        });
        let environments = vec![(
            "shop".to_string(),
            "staging".to_string(),
            environment(5, &[("db", db)]),
        )];
        // Node 1's lease has run out, node 2's has one second left.
        let mut mesh = mesh_of(&[(1, "n1", 0), (2, "n2", 1)]);
        mesh.clock = mesh.lease_secs;
        let statuses = BTreeMap::from([(2, status(4000, 8000))]);
        let inputs = gather(&environments, &mesh, &statuses, &BTreeMap::new());
        let wl = &inputs.workloads[&env("staging")]["db"];
        assert_eq!(wl.scale, Scale::Daemon);
        assert_eq!(wl.pins, ["n2"]);
        assert_eq!(
            wl.cost,
            Cost {
                cpu_millis: 500,
                memory_bytes: 1024,
                disk_bytes: 1_048_576
            }
        );
        assert!(wl.block_volume);
        assert!(!inputs.nodes[&1].live);
        assert!(inputs.nodes[&2].live);
        assert_eq!(inputs.nodes[&1].capacity, None);
        assert_eq!(inputs.nodes[&2].capacity.unwrap().cpu_millis, 4000);
    }

    fn record(generation: u64, workloads: &[(&str, Vec<ReplicaPlacement>)]) -> PlacementRecord {
        PlacementRecord {
            generation,
            basis: 0,
            workloads: workloads
                .iter()
                .map(|(name, replicas)| {
                    let placement = WorkloadPlacement {
                        uid_base: UID_RANGE_START,
                        replicas: replicas.clone(),
                    };
                    (name.to_string(), placement)
                })
                .collect(),
        }
    }

    fn plan_of(workloads: &[(&str, Vec<ReplicaPlacement>)]) -> Plan {
        let workloads = workloads
            .iter()
            .map(|(name, r)| (name.to_string(), r.clone()))
            .collect();
        BTreeMap::from([(env("staging"), workloads)])
    }

    #[test]
    fn commands_are_proposed_only_for_what_differs() {
        let web = serde_json::json!({"containers": [{"name": "app", "image": "nginx"}]});
        let environments = vec![(
            "shop".to_string(),
            "staging".to_string(),
            environment(5, &[("web", web)]),
        )];
        let on_2 = vec![ReplicaPlacement::on(0, 2)];
        let wanted = plan_of(&[("web", on_2.clone())]);
        // Nothing stored yet: one command, at the environment's generation.
        let got = proposals(&environments, &BTreeMap::new(), &wanted, 11);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].generation, 5);
        assert_eq!(got[0].basis, 11);
        assert_eq!(got[0].workloads["web"], on_2);
        // The same plan stored, at an older generation: nothing.
        let stored = BTreeMap::from([(env("staging"), record(3, &[("web", on_2.clone())]))]);
        assert!(proposals(&environments, &stored, &wanted, 11).is_empty());
        // A different node: a command.
        let other = vec![ReplicaPlacement::on(0, 1)];
        let moved = BTreeMap::from([(env("staging"), record(5, &[("web", other)]))]);
        assert_eq!(proposals(&environments, &moved, &wanted, 11).len(), 1);
        // A workload in the record that the plan no longer has: a command.
        let extra = record(5, &[("web", on_2.clone()), ("old", on_2)]);
        let extra = BTreeMap::from([(env("staging"), extra)]);
        assert_eq!(proposals(&environments, &extra, &wanted, 11).len(), 1);
        // A new workload with no replicas still needs its record.
        let none = plan_of(&[("web", Vec::new())]);
        assert_eq!(
            proposals(&environments, &BTreeMap::new(), &none, 11).len(),
            1
        );
        // An environment with no workloads and no record: nothing.
        let empty = vec![(
            "shop".to_string(),
            "staging".to_string(),
            environment(5, &[]),
        )];
        assert!(proposals(&empty, &BTreeMap::new(), &Plan::new(), 11).is_empty());
    }

    #[test]
    fn a_record_whose_environment_is_gone_is_cleaned_up() {
        let stored = BTreeMap::from([(
            env("gone"),
            record(3, &[("web", vec![ReplicaPlacement::on(0, 2)])]),
        )]);
        let got = proposals(&[], &stored, &Plan::new(), 11);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].environment, "gone");
        assert!(got[0].workloads.is_empty());
    }
}
