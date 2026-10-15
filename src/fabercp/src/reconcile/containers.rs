//! Container reconcile step: starts and stops the containers placed on this node.
//!
//! [`ContainersStep::plan`] takes the replicas that placement assigned to
//! this node from the replicated state. The result, [`ContainersDesired`],
//! is what the step applies. It holds no env: the env is fetched when a
//! container is made ([`crate::runner::EnvSource`]).
//!
//! [`ContainersStep::apply`] compares that with what `faber-node` lists, not
//! with what it did last time, so a pod left by an earlier run of fabercp is
//! found and handled:
//!
//! - a wanted replica is registered with the [`RunnerState`] and, the first
//!   time, launched: its volumes, images, sandbox and containers. After
//!   that it is the supervisor's: a container that exits or vanishes is
//!   restarted under the restart policy and the backoff, not by this step;
//! - a pod nobody wants is asked to stop. It reads `unavailable` from then
//!   on, and the supervisor signals the stop after the drain period (see
//!   [`crate::runner::supervisor`]);
//! - a pod of an older revision of a workload that still has replicas here
//!   is left to the supervisor, which asks for its stop once the newer
//!   revision is available. If the workload has a block volume the two
//!   revisions would share it, so the older one is asked to stop at once,
//!   and the newer one starts when it is gone;
//! - ramdisks of replicas that are not on this node are removed, found
//!   through `ListVolumes`. Block volumes are kept: they live as long as the
//!   workload.
//!
//! A workload whose env cannot be opened (no cluster key, the wrong key) is
//! *held*: its pods are neither started nor stopped. A missing key must not
//! look like "nothing is wanted".

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use faber_api_types::workload::{DNS_SUFFIX, VolumeSource, WorkloadSpec};
use faber_node_protocol::{DnsConfig, ReplicaInfo, ReplicaRef, VolumeKind};
use ipnet::Ipv4Net;

use super::step::{Planned, ReconcileStep, ReplicatedStateView, StepFuture};
use crate::raft::store::place::ReplicaPlacement;
use crate::runner::desired::{parse_revision, revision_string};
use crate::runner::env::data_key_for;
use crate::runner::launch::{launch, remove_unused_ramdisks};
use crate::runner::node::{NodeClient, list_replicas};
use crate::runner::state::{RunnerState, StopReason, WorkloadKey, order, slot_of, workload_of};
use crate::runner::{
    DEFAULT_STOP_GRACE_SECS, DesiredContainer, DesiredReplica, DesiredVolume, EnvSource,
};
use crate::secrets::{ClusterKeyProvider, env_scope};

/// What the host needs to run containers.
#[derive(Clone)]
pub struct ContainersWiring {
    /// The socket of `faber-node`.
    pub socket: PathBuf,
    /// Shared by the step, the supervisor and the status reporter.
    pub state: Arc<RunnerState>,
    pub cluster_key: Arc<dyn ClusterKeyProvider>,
    /// Where containers get their env.
    pub env: Arc<dyn EnvSource>,
}

/// The replicas to run on this node.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContainersDesired {
    /// Sorted by project, environment, workload, revision and index.
    pub replicas: Vec<DesiredReplica>,
    /// Workloads placed here whose env could not be opened. Their pods are
    /// left as they are.
    pub held: BTreeSet<WorkloadKey>,
    /// This node's container subnet, the only pod addresses probed.
    pub subnet: Option<Ipv4Net>,
}

/// The step that runs the replicas placed on this node.
pub struct ContainersStep<C> {
    client: C,
    state: Arc<RunnerState>,
    node_id: u64,
    cluster_key: Arc<dyn ClusterKeyProvider>,
    env: Arc<dyn EnvSource>,
}

impl<C: NodeClient> ContainersStep<C> {
    pub fn new(
        client: C,
        state: Arc<RunnerState>,
        node_id: u64,
        cluster_key: Arc<dyn ClusterKeyProvider>,
        env: Arc<dyn EnvSource>,
    ) -> Self {
        Self {
            client,
            state,
            node_id,
            cluster_key,
            env,
        }
    }

    async fn reconcile(&self, desired: &ContainersDesired) -> anyhow::Result<()> {
        self.state.set_held(desired.held.clone());
        self.state.set_subnet(desired.subnet);
        let listed = list_replicas(&self.client).await?;
        let pods: HashMap<&ReplicaRef, &ReplicaInfo> =
            listed.iter().map(|p| (&p.replica, p)).collect();
        let wanted: HashSet<&ReplicaRef> = desired.replicas.iter().map(|d| &d.replica).collect();
        let mut newest: BTreeMap<WorkloadKey, u64> = BTreeMap::new();
        let mut with_block: BTreeSet<WorkloadKey> = BTreeSet::new();
        for d in &desired.replicas {
            if let Some(revision) = parse_revision(&d.replica.revision) {
                let entry = newest.entry(workload_of(&d.replica)).or_insert(revision);
                *entry = (*entry).max(revision);
            }
            if d.has_block_volume() {
                with_block.insert(workload_of(&d.replica));
            }
        }

        let mut known: Vec<ReplicaRef> = listed.iter().map(|p| p.replica.clone()).collect();
        known.extend(self.state.entries().into_iter().map(|(k, _)| k));
        known.sort_by(|a, b| order(a).cmp(&order(b)));
        known.dedup();
        for key in &known {
            let workload = workload_of(key);
            if wanted.contains(key) || desired.held.contains(&workload) {
                continue;
            }
            let older = matches!(
                (newest.get(&workload), parse_revision(&key.revision)),
                (Some(newest), Some(this)) if this < *newest
            );
            match (older, with_block.contains(&workload)) {
                // The newer revision needs the volume the older one has.
                (true, true) => self.state.request_stop(key, StopReason::Replaced),
                // The supervisor asks for the stop once the newer revision
                // is available.
                (true, false) => self.state.mark_superseded(key),
                (false, _) => self.state.request_stop(key, StopReason::Removed),
            }
        }

        for d in &desired.replicas {
            self.state.register(d);
        }
        let mut errors: Vec<String> = Vec::new();
        for d in &desired.replicas {
            // Launched once; from then on the supervisor's.
            if self.state.entry(&d.replica).is_some_and(|e| e.launched) {
                continue;
            }
            if d.has_block_volume() && self.slot_is_taken(d, &listed) {
                for other in self.others_in_slot(d, &listed) {
                    self.state.request_stop(&other, StopReason::Replaced);
                }
                self.state.set_deferred(
                    &d.replica,
                    Some("waiting for the older revision to release its block volume".to_owned()),
                );
                continue;
            }
            let existing = pods.get(&d.replica).copied();
            if let Err(e) = launch(&self.client, &self.state, self.env.as_ref(), d, existing).await
            {
                self.state.mark_failed(&d.replica, e.to_string());
                errors.push(format!("starting {}: {e}", describe(&d.replica)));
            }
        }

        if let Err(e) = remove_unused_ramdisks(&self.client, &self.state).await {
            errors.push(format!("removing a ramdisk: {e}"));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("{}", errors.join("; "))
        }
    }

    /// The other replicas in `d`'s slot, known to the runner or listed by
    /// faber-node.
    fn others_in_slot(&self, d: &DesiredReplica, listed: &[ReplicaInfo]) -> Vec<ReplicaRef> {
        let slot = slot_of(&d.replica);
        let mut others = self.state.others_in_slot(&slot, &d.replica);
        others.extend(
            listed
                .iter()
                .map(|p| &p.replica)
                .filter(|r| **r != d.replica && slot_of(r) == slot)
                .cloned(),
        );
        others.sort_by(|a, b| order(a).cmp(&order(b)));
        others.dedup();
        others
    }

    fn slot_is_taken(&self, d: &DesiredReplica, listed: &[ReplicaInfo]) -> bool {
        !self.others_in_slot(d, listed).is_empty()
    }

    /// Whether the env of `workload` can be opened, or why not. Opens
    /// nothing: only the data key is unwrapped.
    fn check_env(
        &self,
        view: &ReplicatedStateView,
        project: &str,
        environment: &str,
        record: &crate::apps::WorkloadRecord,
    ) -> Result<(), String> {
        if record.env.values().all(|e| e.values.is_empty()) {
            return Ok(());
        }
        let scope = env_scope(project, environment);
        data_key_for(
            self.cluster_key.as_ref(),
            view.data_keys.get(&scope),
            &scope,
        )
        .map(|_| ())
    }

    fn desired_replicas(
        subnet: Option<Ipv4Net>,
        key: &WorkloadKey,
        revision: u64,
        uid_base: u32,
        spec: &WorkloadSpec,
        mine: &[&ReplicaPlacement],
    ) -> Vec<DesiredReplica> {
        let (project, environment, workload) = key;
        let volumes: Vec<DesiredVolume> = spec
            .volumes
            .iter()
            .map(|(name, source)| match source {
                VolumeSource::Block { size_bytes } => DesiredVolume {
                    name: name.clone(),
                    kind: VolumeKind::Block,
                    size_bytes: *size_bytes,
                },
                VolumeSource::Ramdisk { size_bytes } => DesiredVolume {
                    name: name.clone(),
                    kind: VolumeKind::Ramdisk,
                    size_bytes: *size_bytes,
                },
            })
            .collect();
        let containers: Vec<DesiredContainer> = spec
            .containers
            .iter()
            .map(|c| {
                // The env is fetched when the container is made.
                let mut spec = c.clone();
                spec.env.clear();
                DesiredContainer { spec }
            })
            .collect();
        mine.iter()
            .map(|p| DesiredReplica {
                replica: ReplicaRef {
                    project: project.clone(),
                    env: environment.clone(),
                    workload: workload.clone(),
                    revision: revision_string(revision),
                    replica_index: p.index,
                },
                uid_base,
                restart: spec.restart,
                stop_grace_secs: spec.stop_grace_secs.unwrap_or(DEFAULT_STOP_GRACE_SECS),
                dns: pod_dns(subnet, project, environment),
                volumes: volumes.clone(),
                containers: containers.clone(),
            })
            .collect()
    }
}

fn describe(replica: &ReplicaRef) -> String {
    format!(
        "{}/{}/{} revision {} replica {}",
        replica.project, replica.env, replica.workload, replica.revision, replica.replica_index
    )
}

/// The pod's resolver: the node's bridge address, which is the first host
/// address of the node's container subnet, and the environment's domain as
/// the search name, so that a service answers to its short name.
pub fn pod_dns(subnet: Option<Ipv4Net>, project: &str, environment: &str) -> DnsConfig {
    let gateway: Option<Ipv4Addr> = subnet.and_then(|net| net.hosts().next());
    DnsConfig {
        nameservers: gateway.into_iter().collect(),
        searches: vec![format!("{environment}.{project}.{DNS_SUFFIX}")],
    }
}

impl<C: NodeClient> ReconcileStep for ContainersStep<C> {
    type Desired = ContainersDesired;

    fn name(&self) -> &'static str {
        "containers"
    }

    /// `faber-node` may start after fabercp, and a first start can pull
    /// images for minutes. The reconcile loop applies this step, not startup.
    fn required_at_startup(&self) -> bool {
        false
    }

    fn plan(&self, view: &ReplicatedStateView) -> Planned<ContainersDesired> {
        let subnet = view
            .mesh
            .as_ref()
            .and_then(|m| m.nodes.get(&self.node_id))
            .and_then(|n| n.container_subnet);
        let mut desired = ContainersDesired {
            subnet,
            ..ContainersDesired::default()
        };
        let mut warnings = Vec::new();
        let Some(placed) = view.placement.as_ref() else {
            return Planned { desired, warnings };
        };
        for (project, environment, record) in &placed.environments {
            let Some(plan) = placed.records.get(&(project.clone(), environment.clone())) else {
                continue;
            };
            for (name, placement) in &plan.workloads {
                let mine: Vec<&ReplicaPlacement> = placement
                    .replicas
                    .iter()
                    .filter(|r| r.node == Some(self.node_id))
                    .collect();
                let Some(workload) = record.workloads.get(name).filter(|_| !mine.is_empty()) else {
                    continue;
                };
                let key = (project.clone(), environment.clone(), name.clone());
                match self.check_env(view, project, environment, workload) {
                    Ok(()) => desired.replicas.extend(Self::desired_replicas(
                        subnet,
                        &key,
                        workload.revision,
                        placement.uid_base,
                        &workload.spec,
                        &mine,
                    )),
                    Err(why) => {
                        warnings.push(format!(
                            "{project}/{environment}/{name}: {why}; its containers are left as they are"
                        ));
                        desired.held.insert(key);
                    }
                }
            }
        }
        desired
            .replicas
            .sort_by(|a, b| order(&a.replica).cmp(&order(&b.replica)));
        Planned { desired, warnings }
    }

    fn apply<'a>(&'a self, desired: &'a ContainersDesired) -> StepFuture<'a> {
        Box::pin(self.reconcile(desired))
    }
}

#[cfg(test)]
mod tests;
