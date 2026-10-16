use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use faber_api_types::workload::{ReplicaState, RestartPolicy, WorkloadSpec};
use faber_crypto::{ClusterKey, DataKey, key_id, wrap};
use faber_node_protocol::VolumeKind;

use super::*;
use crate::apps::{EnvironmentRecord, WorkloadRecord};
use crate::raft::store::PlacementView;
use crate::raft::store::keys::DataKeyRecord;
use crate::raft::store::place::{PlacementRecord, WorkloadPlacement};
use crate::runner::clock::FakeClock;
use crate::runner::env::StaticEnv;
use crate::runner::node::fake::FakeNode;
use crate::runner::testing::{FakeProber, SECRET};
use crate::runner::{DRAIN_PERIOD, Supervisor, SupervisorConfig};
use crate::secrets::{NoClusterKey, StaticClusterKey, seal_workload};
use crate::status::ReplicaSource;

const NODE: u64 = 1;

fn spec(block: bool, restart: RestartPolicy) -> WorkloadSpec {
    let mut volumes = serde_json::json!({
        "scratch": {"type": "ramdisk", "size_bytes": 1048576},
    });
    if block {
        volumes["data"] = serde_json::json!({"type": "block", "size_bytes": 1048576});
    }
    serde_json::from_value(serde_json::json!({
        "containers": [{
            "name": "app",
            "image": "img:1",
            "env": {"PASSWORD": SECRET},
        }],
        "volumes": volumes,
        "restart": restart,
    }))
    .unwrap()
}

struct Cluster {
    cluster: ClusterKey,
    data: DataKey,
    block: bool,
    restart: RestartPolicy,
}

impl Cluster {
    fn new() -> Self {
        Self {
            cluster: ClusterKey::generate().unwrap(),
            data: DataKey::generate().unwrap(),
            block: true,
            restart: RestartPolicy::Never,
        }
    }

    fn stateless() -> Self {
        Self {
            block: false,
            ..Self::new()
        }
    }

    fn restarting(self, restart: RestartPolicy) -> Self {
        Self { restart, ..self }
    }

    fn record(&self, revision: u64) -> WorkloadRecord {
        let stored = seal_workload(
            &self.data,
            "shop",
            "prod",
            "web",
            &spec(self.block, self.restart),
            None,
        )
        .unwrap();
        WorkloadRecord {
            revision,
            spec: stored.spec,
            env: stored.env,
        }
    }

    /// `web` at `revision`, replica `i` on node `nodes[i]`.
    fn view(&self, revision: u64, nodes: &[u64]) -> ReplicatedStateView {
        let replicas = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| ReplicaPlacement::on(i as u32, *n))
            .collect();
        let environments = vec![(
            "shop".to_owned(),
            "prod".to_owned(),
            EnvironmentRecord {
                generation: 1,
                updated_at: 0,
                workloads: BTreeMap::from([("web".into(), self.record(revision))]),
            },
        )];
        let records = BTreeMap::from([(
            ("shop".to_owned(), "prod".to_owned()),
            PlacementRecord {
                generation: 1,
                basis: 0,
                workloads: BTreeMap::from([(
                    "web".into(),
                    WorkloadPlacement {
                        uid_base: 1_048_576,
                        replicas,
                    },
                )]),
            },
        )]);
        ReplicatedStateView {
            mesh: None,
            placement: Some(PlacementView {
                applied: 0,
                mesh: None,
                environments,
                statuses: BTreeMap::new(),
                records,
                service_ips: Default::default(),
            }),
            status_interval_secs: 5,
            data_keys: BTreeMap::from([(
                "shop/prod".into(),
                DataKeyRecord {
                    wrapped: wrap(&self.cluster, &self.data, "shop/prod").unwrap(),
                    key_id: key_id(&self.cluster),
                },
            )]),
            now: 0,
        }
    }

    fn step(&self, node: &FakeNode, state: &Arc<RunnerState>) -> ContainersStep<FakeNode> {
        ContainersStep::new(
            node.clone(),
            state.clone(),
            NODE,
            Arc::new(StaticClusterKey(self.cluster.clone())),
            StaticEnv::secret(),
        )
    }
}

fn kinds(log: &[String]) -> Vec<&str> {
    log.iter()
        .map(|l| l.split(' ').next().unwrap_or_default())
        .collect()
}

fn web(revision: u64, index: u32) -> ReplicaRef {
    crate::runner::testing::replica_ref("web", revision, index)
}

/// The supervisor of a node, on a clock the test moves.
struct Super {
    clock: Arc<FakeClock>,
    sup: Supervisor<FakeNode, Arc<FakeClock>, FakeProber>,
}

impl Super {
    fn new(node: &FakeNode, state: &Arc<RunnerState>) -> Self {
        let clock = Arc::new(FakeClock::default());
        state.set_subnet(Some("10.1.0.0/24".parse().unwrap()));
        let sup = Supervisor::new(
            node.clone(),
            clock.clone(),
            FakeProber::default(),
            StaticEnv::secret(),
            state.clone(),
            SupervisorConfig::default(),
        );
        Self { clock, sup }
    }

    async fn step(&mut self, by: Duration) {
        self.clock.advance(by);
        self.sup.tick().await;
        self.sup.settle().await;
    }
}

async fn run(step: &ContainersStep<FakeNode>, view: &ReplicatedStateView) {
    step.apply(&step.plan(view).desired).await.unwrap();
}

#[test]
fn plan_takes_the_replicas_placed_here_and_holds_no_env() {
    let c = Cluster::new();
    let step = c.step(&FakeNode::default(), &Arc::default());
    let planned = step.plan(&c.view(7, &[NODE, 2]));
    assert!(planned.warnings.is_empty());
    let [replica] = planned.desired.replicas.as_slice() else {
        panic!("one replica is placed here");
    };
    assert_eq!(replica.replica, web(7, 0));
    assert_eq!(replica.uid_base, 1_048_576);
    assert_eq!(replica.volumes.len(), 2);
    assert!(replica.containers[0].spec.env.is_empty());
    for text in [
        format!("{:?}", planned.desired),
        format!("{:#?}", planned.desired),
    ] {
        assert!(!text.contains(SECRET), "{text}");
    }
}

#[test]
fn plan_holds_a_workload_whose_env_cannot_be_opened() {
    let c = Cluster::new();
    let step = ContainersStep::new(
        FakeNode::default(),
        Arc::default(),
        NODE,
        Arc::new(NoClusterKey),
        StaticEnv::secret(),
    );
    let planned = step.plan(&c.view(7, &[NODE]));
    assert!(planned.desired.replicas.is_empty());
    assert_eq!(
        planned.desired.held,
        BTreeSet::from([("shop".into(), "prod".into(), "web".into())])
    );
    assert_eq!(planned.warnings.len(), 1);
}

#[test]
fn plan_names_the_pods_resolver_from_the_node_subnet() {
    let dns = pod_dns(Some("10.200.3.0/24".parse().unwrap()), "shop", "prod");
    assert_eq!(dns.nameservers, ["10.200.3.1".parse::<Ipv4Addr>().unwrap()]);
    assert_eq!(dns.searches, ["prod.shop.faber.local"]);
    assert!(pod_dns(None, "shop", "prod").nameservers.is_empty());
}

#[tokio::test]
async fn apply_starts_the_replica_and_hands_over_the_env() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let step = c.step(&node, &state);
    run(&step, &c.view(7, &[NODE])).await;

    assert_eq!(
        kinds(&node.log()),
        [
            "ensure_volume",
            "ensure_volume",
            "pull_image",
            "ensure_sandbox",
            "create_container",
            "start_container"
        ]
    );
    let env = node.env_of(&web(7, 0), "app").unwrap();
    assert_eq!(env["PASSWORD"], SECRET);
    assert!(state.entry(&web(7, 0)).unwrap().launched);
}

#[tokio::test]
async fn applying_again_changes_nothing_and_a_recreate_is_the_supervisors() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let step = c.step(&node, &state);
    let view = c.view(7, &[NODE]);
    run(&step, &view).await;
    // The container exits, then the whole pod vanishes. The workload says
    // `restart: never`, so the step must not make them again.
    node.exit(&web(7, 0), "app", 1);
    node.take_log();
    run(&step, &view).await;
    node.vanish(&web(7, 0));
    node.take_log();
    run(&step, &view).await;
    let log = node.log();
    assert!(
        !log.iter()
            .any(|l| l.contains("create") || l.contains("sandbox") || l.contains("start")),
        "{log:?}"
    );
    assert_eq!(node.sandbox_count(), 0);
}

#[tokio::test]
async fn a_replica_that_leaves_reads_unavailable_then_stops_and_loses_its_ramdisk() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let step = c.step(&node, &state);
    run(&step, &c.view(7, &[NODE])).await;
    let mut sup = Super::new(&node, &state);
    sup.step(Duration::ZERO).await;
    assert_eq!(state.replicas()[0].state, ReplicaState::Available);
    assert_eq!(node.volumes().len(), 2);
    node.take_log();

    // Placement moves the replica to node 2.
    run(&step, &c.view(7, &[2])).await;
    assert_eq!(state.replicas()[0].state, ReplicaState::Unavailable);
    assert_eq!(
        state.entry(&web(7, 0)).unwrap().stop,
        Some(StopReason::Removed)
    );
    assert!(kinds(&node.log()).is_empty(), "the step signals nothing");

    sup.step(Duration::ZERO).await;
    sup.step(DRAIN_PERIOD - Duration::from_millis(1)).await;
    assert_eq!(node.sandbox_count(), 1, "still draining");
    sup.step(Duration::from_millis(1)).await;
    assert_eq!(node.sandbox_count(), 0);
    assert!(state.entries().is_empty());
    // Stop, then the sandbox, then the ramdisk.
    assert_eq!(
        kinds(&node.log()),
        ["stop_container", "remove_sandbox", "remove_volume"]
    );
    assert!(node.log()[2].ends_with("scratch"));
    // The block volume is kept.
    let left = node.volumes();
    assert_eq!(left.len(), 1);
    assert_eq!((left[0].1.as_str(), left[0].2), ("data", VolumeKind::Block));
}

#[tokio::test]
async fn a_pod_left_by_an_earlier_run_is_stopped_and_its_ramdisk_found() {
    let (c, node) = (Cluster::new(), FakeNode::default());
    let first = c.step(&node, &Arc::default());
    run(&first, &c.view(7, &[NODE])).await;
    assert_eq!(node.sandbox_count(), 1);

    // fabercp starts again: no memory of the pod or of its volumes.
    let state = Arc::<RunnerState>::default();
    let second = c.step(&node, &state);
    second.apply(&ContainersDesired::default()).await.unwrap();
    assert_eq!(state.replicas().len(), 0, "an adopted pod is not reported");
    let mut sup = Super::new(&node, &state);
    sup.step(Duration::ZERO).await;
    sup.step(DRAIN_PERIOD).await;
    assert_eq!(node.sandbox_count(), 0);
    let left = node.volumes();
    assert_eq!(left.len(), 1, "{left:?}");
    assert_eq!(left[0].1, "data");
}

#[tokio::test]
async fn ramdisks_with_no_replica_here_are_collected_and_block_volumes_kept() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    node.leave_volume(&web(3, 0), "scratch", VolumeKind::Ramdisk);
    node.leave_volume(&web(3, 0), "data", VolumeKind::Block);
    let step = c.step(&node, &state);
    step.apply(&ContainersDesired::default()).await.unwrap();
    assert_eq!(kinds(&node.log()), ["remove_volume"], "{:?}", node.log());
    let left = node.volumes();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].1, "data");
}

#[tokio::test]
async fn a_held_workload_keeps_its_pods_and_its_ramdisks() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let step = c.step(&node, &state);
    run(&step, &c.view(7, &[NODE])).await;
    node.take_log();

    let held = ContainersDesired {
        held: BTreeSet::from([("shop".into(), "prod".into(), "web".into())]),
        ..ContainersDesired::default()
    };
    // As after a restart: the entries are gone, the pods and volumes not.
    let fresh = Arc::<RunnerState>::default();
    let step = c.step(&node, &fresh);
    step.apply(&held).await.unwrap();
    assert!(node.log().is_empty(), "{:?}", node.log());
    assert!(fresh.entries().is_empty());
    assert_eq!(node.sandbox_count(), 1);
}

#[tokio::test]
async fn an_older_revision_of_a_stateless_workload_overlaps_and_drains() {
    let (c, node, state) = (
        Cluster::stateless(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let step = c.step(&node, &state);
    run(&step, &c.view(7, &[NODE])).await;
    let mut sup = Super::new(&node, &state);
    sup.step(Duration::ZERO).await;
    node.take_log();

    run(&step, &c.view(9, &[NODE])).await;
    assert_eq!(node.sandbox_count(), 2, "both run");
    assert!(state.entry(&web(7, 0)).unwrap().superseded);
    assert!(state.entry(&web(9, 0)).unwrap().launched);
    assert!(!kinds(&node.log()).iter().any(|k| k.starts_with("stop")));

    // The new revision is available: the old one drains, then goes.
    sup.step(Duration::ZERO).await;
    assert_eq!(state.replicas()[0].state, ReplicaState::Unavailable);
    sup.step(DRAIN_PERIOD).await;
    assert_eq!(node.sandbox_count(), 1);
    // The ramdisk is shared by slot and stays for the new revision.
    assert_eq!(node.volumes().len(), 1);
}

#[tokio::test]
async fn a_block_volume_workload_stops_the_old_revision_before_the_new_one_starts() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let step = c.step(&node, &state);
    run(&step, &c.view(7, &[NODE])).await;
    let mut sup = Super::new(&node, &state);
    sup.step(Duration::ZERO).await;
    node.take_log();

    run(&step, &c.view(9, &[NODE])).await;
    // The old one is told to go; the new one waits.
    assert_eq!(
        state.entry(&web(7, 0)).unwrap().stop,
        Some(StopReason::Replaced)
    );
    let waiting = state.entry(&web(9, 0)).unwrap();
    assert!(!waiting.launched && waiting.deferred);
    assert_eq!(node.sandbox_count(), 1);
    assert!(node.log().is_empty(), "{:?}", node.log());

    sup.step(Duration::ZERO).await;
    sup.step(DRAIN_PERIOD).await;
    assert_eq!(node.sandbox_count(), 0, "the old revision stopped fully");
    sup.step(Duration::ZERO).await;
    assert_eq!(node.sandbox_count(), 1);
    assert!(state.entry(&web(9, 0)).unwrap().launched);
    let log = node.log();
    let removed = log
        .iter()
        .position(|l| l.starts_with("remove_sandbox"))
        .unwrap();
    let created = log
        .iter()
        .position(|l| l.starts_with("create_container"))
        .unwrap();
    assert!(removed < created, "{log:?}");
    // The block volume was never removed.
    assert!(node.volumes().iter().any(|v| v.1 == "data"));
    sup.step(Duration::ZERO).await;
    assert_eq!(state.replicas()[0].state, ReplicaState::Available);
}

#[tokio::test]
async fn a_failed_start_is_reported_and_the_other_replicas_still_start() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let step = c.step(&node, &state);
    node.fail_next("pull_image");
    let planned = step.plan(&c.view(7, &[NODE, NODE]));
    let err = step.apply(&planned.desired).await.unwrap_err();
    assert!(format!("{err:#}").contains("replica 0"), "{err:#}");
    assert!(state.entry(&web(7, 1)).unwrap().launched);
    let first = state.entry(&web(7, 0)).unwrap();
    assert!(!first.launched);
    assert!(first.detail.is_some());
    // The retry starts it.
    step.apply(&planned.desired).await.unwrap();
    assert!(state.entry(&web(7, 0)).unwrap().launched);
}

// The roll of a block-volume workload, one replica index at a time.

fn mesh_node(id: u64) -> crate::mesh::state::NodeRecord {
    crate::mesh::state::NodeRecord {
        node_id: id,
        public_key: "k".into(),
        mesh_ip: format!("10.99.0.{id}").parse().unwrap(),
        endpoint: None,
        invite_id: None,
        instance_id: None,
        container_subnet: None,
        name: format!("n{id}"),
        lease_renewed_at: 0,
    }
}

fn report(
    revision: u64,
    index: u32,
    state: ReplicaState,
) -> crate::raft::store::status::ReplicaReport {
    crate::raft::store::status::ReplicaReport {
        project: "shop".into(),
        environment: "prod".into(),
        workload: "web".into(),
        revision,
        replica_index: index,
        state,
        pod_ip: None,
        detail: None,
    }
}

type Reports = Option<(u64, Vec<crate::raft::store::status::ReplicaReport>)>;

/// `web` at `revision` with `placed[i]` the node of replica `i` (`None`:
/// unplaced), the lease clock at `clock`, and what node 2 reported at the
/// given time.
fn rolling(
    c: &Cluster,
    revision: u64,
    placed: &[Option<u64>],
    clock: u64,
    node_two: Reports,
) -> ReplicatedStateView {
    let mut view = c.view(revision, &[]);
    let mut mesh =
        crate::mesh::MeshState::init(crate::mesh::state::MeshConfig::default(), mesh_node(1))
            .unwrap();
    mesh.nodes = (1..=3).map(|id| (id, mesh_node(id))).collect();
    mesh.clock = clock;
    let placement = view.placement.as_mut().unwrap();
    placement.mesh = Some(mesh);
    let record = placement
        .records
        .get_mut(&("shop".to_owned(), "prod".to_owned()))
        .unwrap();
    record.workloads.get_mut("web").unwrap().replicas = placed
        .iter()
        .enumerate()
        .map(|(i, n)| match n {
            Some(n) => ReplicaPlacement::on(i as u32, *n),
            None => ReplicaPlacement::unplaced(i as u32, "volume_lost"),
        })
        .collect();
    if let Some((reported_at, replicas)) = node_two {
        placement.statuses.insert(
            2,
            crate::raft::store::status::NodeStatus {
                reported_at,
                capacity: Default::default(),
                replicas,
            },
        );
    }
    view
}

/// Replica 1 of `web` runs at revision 7 on this node and is available.
/// Replica 0 is on node 2.
async fn serving_old_replica(
    c: &Cluster,
    node: &FakeNode,
    state: &Arc<RunnerState>,
) -> (ContainersStep<FakeNode>, Super) {
    let step = c.step(node, state);
    run(&step, &rolling(c, 7, &[Some(2), Some(NODE)], 100, None)).await;
    let mut sup = Super::new(node, state);
    sup.step(Duration::ZERO).await;
    assert_eq!(state.replicas()[0].state, ReplicaState::Available);
    node.take_log();
    (step, sup)
}

#[tokio::test]
async fn a_replica_waits_for_the_lower_index_and_the_old_one_keeps_serving() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let (step, mut sup) = serving_old_replica(&c, &node, &state).await;

    // Replica 0 of revision 9 is still deploying on node 2.
    let stuck = rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        100,
        Some((100, vec![report(9, 0, ReplicaState::Deploying)])),
    );
    let planned = step.plan(&stuck);
    assert_eq!(planned.desired.waiting, HashSet::from([web(9, 1)]));
    step.apply(&planned.desired).await.unwrap();
    sup.step(DRAIN_PERIOD * 2).await;
    // The old replica is untouched and still available: the roll stopped
    // at index 0 and the service keeps a backend.
    assert_eq!(state.entry(&web(7, 1)).unwrap().stop, None);
    assert_eq!(state.replicas()[0].state, ReplicaState::Available);
    assert!(node.log().is_empty(), "{:?}", node.log());
    assert!(!state.entry(&web(9, 1)).unwrap().launched);

    // Replica 0 is available: this one's turn. The old one stops first.
    let open = rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        100,
        Some((100, vec![report(9, 0, ReplicaState::Available)])),
    );
    let planned = step.plan(&open);
    assert!(planned.desired.waiting.is_empty());
    step.apply(&planned.desired).await.unwrap();
    assert_eq!(
        state.entry(&web(7, 1)).unwrap().stop,
        Some(StopReason::Replaced)
    );
    sup.step(Duration::ZERO).await;
    sup.step(DRAIN_PERIOD).await;
    sup.step(Duration::ZERO).await;
    assert!(state.entry(&web(9, 1)).unwrap().launched);
    let log = node.log();
    let removed = log
        .iter()
        .position(|l| l.starts_with("remove_sandbox"))
        .unwrap();
    let created = log
        .iter()
        .position(|l| l.starts_with("create_container"))
        .unwrap();
    assert!(removed < created, "{log:?}");
}

#[test]
fn a_status_report_that_moves_no_gate_leaves_the_plan_as_it_was() {
    let c = Cluster::new();
    let step = c.step(&FakeNode::default(), &Arc::default());
    let before = step.plan(&rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        100,
        Some((100, vec![report(9, 0, ReplicaState::Deploying)])),
    ));
    // A later report: newer time and a detail. Still deploying.
    let mut again = report(9, 0, ReplicaState::Deploying);
    again.detail = Some("pulling".into());
    let after = step.plan(&rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        105,
        Some((105, vec![again])),
    ));
    assert_eq!(before.desired, after.desired);
    // Available does move the gate.
    let moved = step.plan(&rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        105,
        Some((105, vec![report(9, 0, ReplicaState::Available)])),
    ));
    assert_ne!(before.desired, moved.desired);
}

#[test]
fn the_gate_is_judged_from_placement_and_staleness() {
    let c = Cluster::new();
    let step = c.step(&FakeNode::default(), &Arc::default());
    let waiting = |view: &ReplicatedStateView| step.plan(view).desired.waiting;
    let mine = HashSet::from([web(9, 1)]);
    let available = vec![report(9, 0, ReplicaState::Available)];

    // Available and fresh: open.
    let fresh = rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        100,
        Some((100, available.clone())),
    );
    assert!(waiting(&fresh).is_empty());
    // The node of replica 0 has not reported for a long time: wait.
    let stale = rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        10_000,
        Some((100, available)),
    );
    assert_eq!(waiting(&stale), mine);
    // Its node never reported: wait.
    let silent = rolling(&c, 9, &[Some(2), Some(NODE)], 100, None);
    assert_eq!(waiting(&silent), mine);
    // Its volume is lost and it is unplaced: the roll does not deadlock.
    let lost = rolling(&c, 9, &[None, Some(NODE)], 100, None);
    assert!(waiting(&lost).is_empty());
    // Index 0 is never gated, whatever the others do.
    let first = rolling(&c, 9, &[Some(NODE), Some(2)], 100, None);
    assert!(!waiting(&first).contains(&web(9, 0)));
}

#[test]
fn a_stateless_workload_is_never_gated() {
    let c = Cluster::stateless();
    let step = c.step(&FakeNode::default(), &Arc::default());
    let view = rolling(
        &c,
        9,
        &[Some(2), Some(NODE)],
        100,
        Some((100, vec![report(9, 0, ReplicaState::Deploying)])),
    );
    assert!(step.plan(&view).desired.waiting.is_empty());
}

#[tokio::test]
async fn a_newer_revision_mid_roll_takes_the_roll_over() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let (step, mut sup) = serving_old_replica(&c, &node, &state).await;
    let deploying = |rev| {
        rolling(
            &c,
            rev,
            &[Some(2), Some(NODE)],
            100,
            Some((100, vec![report(rev, 0, ReplicaState::Deploying)])),
        )
    };
    run(&step, &deploying(9)).await;
    assert!(state.entry(&web(9, 1)).unwrap().deferred);

    // Revision 11 arrives before revision 9 got its turn. Revision 9 never
    // started, so it is dropped; revision 7 still serves.
    run(&step, &deploying(11)).await;
    sup.step(Duration::ZERO).await;
    assert_eq!(
        state.entry(&web(9, 1)).and_then(|e| e.stop),
        Some(StopReason::Replaced)
    );
    assert_eq!(state.entry(&web(7, 1)).unwrap().stop, None);
    assert!(state.entry(&web(11, 1)).unwrap().deferred);
    assert_eq!(node.sandbox_count(), 1);
    assert!(node.log().is_empty(), "{:?}", node.log());
}

#[tokio::test]
async fn a_gate_that_closes_after_the_old_replica_was_stopped_does_not_hold_the_new_one() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let (step, mut sup) = serving_old_replica(&c, &node, &state).await;
    let ok = vec![report(9, 0, ReplicaState::Available)];
    let open = rolling(&c, 9, &[Some(2), Some(NODE)], 100, Some((100, ok.clone())));
    run(&step, &open).await;
    assert_eq!(
        state.entry(&web(7, 1)).unwrap().stop,
        Some(StopReason::Replaced)
    );
    // Node 2 goes quiet before the old replica is gone: the gate closes.
    let quiet = rolling(&c, 9, &[Some(2), Some(NODE)], 10_000, Some((100, ok)));
    assert_eq!(
        step.plan(&quiet).desired.waiting,
        HashSet::from([web(9, 1)])
    );
    run(&step, &quiet).await;
    sup.step(Duration::ZERO).await;
    sup.step(DRAIN_PERIOD).await;
    sup.step(Duration::ZERO).await;
    assert!(state.entry(&web(9, 1)).unwrap().launched);
}

#[tokio::test]
async fn an_index_moved_to_another_node_keeps_the_roll_gate() {
    let (c, node, state) = (
        Cluster::new(),
        FakeNode::default(),
        Arc::<RunnerState>::default(),
    );
    let (step, mut sup) = serving_old_replica(&c, &node, &state).await;

    // Replica 1 moved to node 3 and nothing of revision 9 is placed here.
    // Replica 0 is still deploying: the old pod here keeps serving.
    let stuck = rolling(
        &c,
        9,
        &[Some(2), Some(3)],
        100,
        Some((100, vec![report(9, 0, ReplicaState::Deploying)])),
    );
    let planned = step.plan(&stuck);
    assert!(planned.desired.replicas.is_empty());
    assert_eq!(planned.desired.waiting, HashSet::from([web(9, 1)]));
    step.apply(&planned.desired).await.unwrap();
    sup.step(DRAIN_PERIOD * 2).await;
    assert_eq!(state.entry(&web(7, 1)).unwrap().stop, None);
    assert_eq!(state.replicas()[0].state, ReplicaState::Available);

    // Replica 0 is available: the old pod is replaced.
    let open = rolling(
        &c,
        9,
        &[Some(2), Some(3)],
        100,
        Some((100, vec![report(9, 0, ReplicaState::Available)])),
    );
    run(&step, &open).await;
    assert_eq!(
        state.entry(&web(7, 1)).unwrap().stop,
        Some(StopReason::Replaced)
    );
}

#[test]
fn without_a_mesh_the_roll_gate_is_skipped() {
    let c = Cluster::new();
    let step = c.step(&FakeNode::default(), &Arc::default());
    // No lease clock to judge staleness by, and replica 0 has no status.
    let view = c.view(9, &[2, NODE]);
    assert!(view.placement.as_ref().unwrap().mesh.is_none());
    assert!(step.plan(&view).desired.waiting.is_empty());
}

#[tokio::test]
async fn a_dead_old_pod_that_will_not_restart_is_removed_from_a_closed_slot() {
    // (policy, exit code, removed at once)
    let table = [
        (RestartPolicy::Never, 0, true),
        (RestartPolicy::Never, 1, true),
        (RestartPolicy::OnFailure, 0, true),
        (RestartPolicy::OnFailure, 1, false),
        (RestartPolicy::Always, 0, false),
    ];
    for (policy, code, removed) in table {
        let (c, node, state) = (
            Cluster::new().restarting(policy),
            FakeNode::default(),
            Arc::<RunnerState>::default(),
        );
        let (step, _sup) = serving_old_replica(&c, &node, &state).await;
        node.exit(&web(7, 1), "app", code);
        let stuck = rolling(
            &c,
            9,
            &[Some(2), Some(NODE)],
            100,
            Some((100, vec![report(9, 0, ReplicaState::Deploying)])),
        );
        let planned = step.plan(&stuck);
        assert_eq!(planned.desired.waiting, HashSet::from([web(9, 1)]));
        step.apply(&planned.desired).await.unwrap();
        let want = removed.then_some(StopReason::Replaced);
        assert_eq!(
            state.entry(&web(7, 1)).unwrap().stop,
            want,
            "{policy:?} exit {code}"
        );
    }
}
