use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use faber_api_types::workload::{ReplicaState, RestartPolicy};
use faber_node_protocol::{ContainerState, VolumeKind};
use tokio_util::sync::CancellationToken;

use super::{Supervisor, SupervisorConfig};
use crate::runner::clock::FakeClock;
use crate::runner::env::StaticEnv;
use crate::runner::launch::{remove_unused_ramdisks, restart_container, retire};
use crate::runner::node::fake::FakeNode;
use crate::runner::node::list_replicas;
use crate::runner::state::RunnerState;
use crate::runner::state::{StopReason, slot_of};
use crate::runner::testing::{FakeProber, GatedNode, container, desired, start, volume};
use crate::runner::{DRAIN_PERIOD, DesiredReplica, MonotonicClock, POLL_INTERVAL};
use crate::status::ReplicaSource;

/// The container subnet of the fake node: the fake pods get addresses in it.
const SUBNET: &str = "10.1.0.0/24";

const fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

const fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

struct Rig {
    node: FakeNode,
    clock: Arc<FakeClock>,
    prober: FakeProber,
    state: Arc<RunnerState>,
    sup: Supervisor<FakeNode, Arc<FakeClock>, FakeProber>,
}

impl Rig {
    fn new() -> Self {
        let node = FakeNode::default();
        let clock = Arc::new(FakeClock::default());
        let prober = FakeProber::default();
        let state = Arc::new(RunnerState::default());
        state.set_subnet(Some(SUBNET.parse().unwrap()));
        let sup = Supervisor::new(
            node.clone(),
            clock.clone(),
            prober.clone(),
            StaticEnv::secret(),
            state.clone(),
            SupervisorConfig::default(),
        );
        Self {
            node,
            clock,
            prober,
            state,
            sup,
        }
    }

    /// Moves the clock, runs a tick and lets what it started finish.
    async fn step(&mut self, by: Duration) {
        self.clock.advance(by);
        self.sup.tick().await;
        self.sup.settle().await;
    }

    fn creates(&self) -> usize {
        self.node
            .log()
            .iter()
            .filter(|l| l.starts_with("create_container"))
            .count()
    }

    fn state_of(&self, workload: &str, revision: u64) -> ReplicaState {
        self.state
            .replicas()
            .into_iter()
            .find(|r| r.workload == workload && r.revision == revision)
            .map(|r| r.state)
            .expect("the replica is reported")
    }
}

fn plain(revision: u64, policy: RestartPolicy, probed: bool) -> DesiredReplica {
    desired(
        "web",
        revision,
        0,
        policy,
        vec![container("app", probed)],
        vec![],
    )
}

#[tokio::test]
async fn a_running_replica_is_reported_available_with_its_pod_ip() {
    let mut rig = Rig::new();
    let d = plain(7, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    let report = rig.state.replicas().remove(0);
    assert_eq!(report.state, ReplicaState::Available);
    assert_eq!(report.revision, 7);
    assert_eq!(report.pod_ip, Some("10.1.0.1".parse().unwrap()));
}

#[tokio::test]
async fn a_replica_is_deploying_until_the_step_has_started_it() {
    let rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    rig.state.register(&d);
    assert_eq!(rig.state_of("web", 1), ReplicaState::Deploying);
}

#[tokio::test]
async fn restart_delays_double_from_100ms_and_reset_after_ten_seconds_up() {
    let mut rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.creates(), 1);

    // Crash at once, over and over: 100 ms, 200 ms, 400 ms.
    for (n, delay) in [100u64, 200, 400].into_iter().enumerate() {
        rig.node.exit(&d.replica, "app", 1);
        rig.step(Duration::ZERO).await;
        assert_eq!(rig.state_of("web", 1), ReplicaState::Unavailable);
        rig.step(ms(delay - 1)).await;
        assert_eq!(rig.creates(), 1 + n, "{delay} ms: not yet");
        rig.step(ms(1)).await;
        assert_eq!(rig.creates(), 2 + n, "{delay} ms: restarted");
    }

    // Up for ten seconds: the next restart waits 100 ms again.
    rig.step(secs(10)).await;
    rig.node.exit(&d.replica, "app", 1);
    rig.step(Duration::ZERO).await;
    rig.step(ms(99)).await;
    assert_eq!(rig.creates(), 4);
    rig.step(ms(1)).await;
    assert_eq!(rig.creates(), 5);
    assert_eq!(rig.state_of("web", 1), ReplicaState::Available);
}

#[tokio::test]
async fn restart_policies_decide_which_exits_come_back() {
    let table = [
        (RestartPolicy::Always, 0, true),
        (RestartPolicy::Always, 1, true),
        (RestartPolicy::OnFailure, 0, false),
        (RestartPolicy::OnFailure, 1, true),
        (RestartPolicy::Never, 0, false),
        (RestartPolicy::Never, 1, false),
    ];
    for (policy, code, comes_back) in table {
        let mut rig = Rig::new();
        let d = plain(1, policy, false);
        start(&rig.node, &rig.state, &d).await;
        rig.step(Duration::ZERO).await;
        rig.node.exit(&d.replica, "app", code);
        rig.step(Duration::ZERO).await;
        rig.step(secs(3600)).await;
        rig.step(Duration::ZERO).await;
        let want = if comes_back { 2 } else { 1 };
        assert_eq!(rig.creates(), want, "{policy:?} exit {code}");
        let state = if comes_back {
            ReplicaState::Available
        } else {
            ReplicaState::Unavailable
        };
        assert_eq!(rig.state_of("web", 1), state, "{policy:?} exit {code}");
    }
}

#[tokio::test]
async fn a_container_that_vanished_is_restarted_like_one_that_exited() {
    let mut rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    rig.node.vanish(&d.replica);
    rig.step(Duration::ZERO).await;
    rig.step(ms(100)).await;
    assert_eq!(rig.creates(), 2);
    assert_eq!(rig.node.sandbox_count(), 1);
    assert_eq!(rig.state_of("web", 1), ReplicaState::Available);
}

#[tokio::test]
async fn the_first_probe_waits_one_interval_and_goes_to_the_pod_ip() {
    let mut rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, true);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.state_of("web", 1), ReplicaState::Deploying);
    rig.step(ms(4999)).await;
    assert!(rig.prober.asked().is_empty());
    rig.step(ms(1)).await;
    let asked = rig.prober.asked();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].ip, "10.1.0.1".parse::<Ipv4Addr>().unwrap());
    assert_eq!(rig.state_of("web", 1), ReplicaState::Available);
    // Then one per interval.
    rig.step(secs(5)).await;
    assert_eq!(rig.prober.asked().len(), 2);
}

#[tokio::test]
async fn one_failed_probe_is_unavailable_and_three_in_a_row_restart() {
    let mut rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, true);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    rig.step(secs(5)).await;
    assert_eq!(rig.state_of("web", 1), ReplicaState::Available);

    // One failure: out of rotation, not restarted.
    rig.prober.set_failing(true);
    rig.step(secs(5)).await;
    assert_eq!(rig.state_of("web", 1), ReplicaState::Unavailable);
    assert_eq!(rig.creates(), 1);
    // A pass resets the count: two more failures still do not restart.
    rig.prober.set_failing(false);
    rig.step(secs(5)).await;
    assert_eq!(rig.state_of("web", 1), ReplicaState::Available);
    rig.prober.set_failing(true);
    rig.step(secs(5)).await;
    rig.step(secs(5)).await;
    assert_eq!(rig.creates(), 1);
    assert!(
        !rig.node
            .log()
            .iter()
            .any(|l| l.starts_with("stop_container"))
    );

    // The third in a row reads unavailable, waits out the drain period, and
    // only then stops the container; it comes back after that.
    rig.step(secs(5)).await;
    assert_eq!(rig.state_of("web", 1), ReplicaState::Unavailable);
    let stopped = |rig: &Rig| {
        rig.node
            .log()
            .iter()
            .any(|l| l.starts_with("stop_container"))
    };
    assert!(!stopped(&rig));
    rig.prober.set_failing(false);
    rig.step(DRAIN_PERIOD - ms(1)).await;
    assert!(!stopped(&rig));
    assert_eq!(rig.creates(), 1, "not restarted while the stop is pending");
    rig.step(ms(1)).await;
    assert!(stopped(&rig));
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.creates(), 2);
    assert_eq!(rig.state_of("web", 1), ReplicaState::Deploying);
    rig.step(secs(5)).await;
    assert_eq!(rig.state_of("web", 1), ReplicaState::Available);
}

#[tokio::test]
async fn a_probe_stop_follows_the_restart_policy() {
    let mut rig = Rig::new();
    let d = plain(1, RestartPolicy::Never, true);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    rig.prober.set_failing(true);
    for _ in 0..3 {
        rig.step(secs(5)).await;
    }
    rig.step(secs(3600)).await;
    assert_eq!(rig.creates(), 1);
    assert_eq!(rig.state_of("web", 1), ReplicaState::Unavailable);
}

#[tokio::test(start_paused = true)]
async fn the_task_sees_an_exit_without_waiting_for_a_resync() {
    let node = FakeNode::default();
    let state = Arc::new(RunnerState::default());
    state.set_subnet(Some(SUBNET.parse().unwrap()));
    let d = plain(1, RestartPolicy::Always, false);
    start(&node, &state, &d).await;
    let cancel = CancellationToken::new();
    let task = Supervisor::new(
        node.clone(),
        MonotonicClock::new(),
        FakeProber::default(),
        StaticEnv::secret(),
        state.clone(),
        SupervisorConfig::default(),
    )
    .spawn(cancel.clone());
    let settle = || async {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    };
    settle().await;
    node.exit(&d.replica, "app", 1);
    // Two polls: one to notice, one past the 100 ms backoff.
    for _ in 0..3 {
        tokio::time::advance(POLL_INTERVAL).await;
        settle().await;
    }
    assert_eq!(
        node.container_states(&d.replica),
        [("app".to_owned(), ContainerState::Running)]
    );
    let creates = node
        .log()
        .iter()
        .filter(|l| l.starts_with("create_container"))
        .count();
    assert_eq!(creates, 2);
    cancel.cancel();
    task.await.unwrap();
}

fn rollout(probed: bool) -> (DesiredReplica, DesiredReplica) {
    let volumes = || {
        vec![
            volume("scratch", VolumeKind::Ramdisk),
            volume("data", VolumeKind::Block),
        ]
    };
    let old = desired(
        "web",
        1,
        0,
        RestartPolicy::Always,
        vec![container("app", false)],
        volumes(),
    );
    let new = desired(
        "web",
        2,
        0,
        RestartPolicy::Always,
        vec![container("app", probed)],
        volumes(),
    );
    (old, new)
}

#[tokio::test]
async fn the_old_revision_stops_after_the_drain_once_the_new_one_is_available() {
    let mut rig = Rig::new();
    let (old, new) = rollout(false);
    start(&rig.node, &rig.state, &old).await;
    start(&rig.node, &rig.state, &new).await;
    rig.state.mark_superseded(&old.replica);
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.node.sandbox_count(), 2);
    // The new one is available: the old one is out of rotation from now on,
    // though it still runs.
    assert_eq!(rig.state_of("web", 1), ReplicaState::Unavailable);
    assert_eq!(rig.state_of("web", 2), ReplicaState::Available);
    assert!(
        !rig.node
            .log()
            .iter()
            .any(|l| l.starts_with("stop_container"))
    );

    rig.step(DRAIN_PERIOD - ms(1)).await;
    assert_eq!(rig.node.sandbox_count(), 2, "still draining");
    rig.step(ms(1)).await;
    assert_eq!(rig.node.sandbox_count(), 1, "the old revision is gone");
    assert!(rig.state.entry(&old.replica).is_none());
    assert!(rig.state.entry(&new.replica).is_some());
    // The volumes are the new revision's too (they are not scoped by
    // revision), so neither is removed.
    assert_eq!(rig.node.volumes().len(), 2);
    assert!(
        !rig.node
            .log()
            .iter()
            .any(|l| l.starts_with("remove_volume"))
    );
    assert_eq!(rig.state_of("web", 2), ReplicaState::Available);
}

#[tokio::test]
async fn the_old_revision_keeps_running_while_the_new_one_is_not_available() {
    let mut rig = Rig::new();
    let (old, new) = rollout(true);
    start(&rig.node, &rig.state, &old).await;
    start(&rig.node, &rig.state, &new).await;
    rig.state.mark_superseded(&old.replica);
    rig.prober.set_failing(true);
    rig.step(Duration::ZERO).await;
    rig.step(secs(60)).await;
    assert_eq!(rig.node.sandbox_count(), 2);

    // It becomes available; the drain starts then, not before.
    rig.prober.set_failing(false);
    rig.step(secs(5)).await;
    assert_eq!(rig.state_of("web", 2), ReplicaState::Available);
    rig.step(DRAIN_PERIOD - ms(1)).await;
    assert_eq!(rig.node.sandbox_count(), 2);
    rig.step(ms(1)).await;
    assert_eq!(rig.node.sandbox_count(), 1);
}

fn stop_calls(rig: &Rig) -> Vec<String> {
    rig.node
        .log()
        .into_iter()
        .filter(|l| l.starts_with("stop_container") || l.starts_with("remove_"))
        .map(|l| l.split(' ').take(1).collect())
        .collect()
}

#[tokio::test]
async fn every_stop_reports_unavailable_then_waits_the_drain_then_signals() {
    let mut rig = Rig::new();
    let d = desired(
        "web",
        1,
        0,
        RestartPolicy::Always,
        vec![container("app", false)],
        vec![
            volume("scratch", VolumeKind::Ramdisk),
            volume("data", VolumeKind::Block),
        ],
    );
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.state_of("web", 1), ReplicaState::Available);

    rig.state.request_stop(&d.replica, StopReason::Removed);
    // Visible at once, before any tick.
    assert_eq!(rig.state_of("web", 1), ReplicaState::Unavailable);
    rig.step(Duration::ZERO).await;
    rig.step(DRAIN_PERIOD - ms(1)).await;
    assert!(
        stop_calls(&rig).is_empty(),
        "nothing is signalled in the drain"
    );
    assert_eq!(rig.node.sandbox_count(), 1);

    rig.step(ms(1)).await;
    // Stop, then the sandbox, then the ramdisk. The block volume stays.
    assert_eq!(
        stop_calls(&rig),
        ["stop_container", "remove_sandbox", "remove_volume"]
    );
    assert_eq!(rig.node.sandbox_count(), 0);
    assert!(rig.state.entry(&d.replica).is_none());
    let left = rig.node.volumes();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].1, "data");
}

#[tokio::test]
async fn a_stopping_replica_is_not_restarted() {
    let mut rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    rig.state.request_stop(&d.replica, StopReason::Removed);
    rig.node.exit(&d.replica, "app", 1);
    rig.step(ms(50)).await;
    rig.step(secs(1)).await;
    assert_eq!(rig.creates(), 1);
}

#[tokio::test]
async fn a_restart_that_waited_for_the_lock_does_not_bring_back_a_removed_replica() {
    let rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    let env = StaticEnv::secret();
    let container = &d.containers[0];

    rig.state.request_stop(&d.replica, StopReason::Removed);
    rig.node.take_log();
    let did = restart_container(&rig.node, &rig.state, env.as_ref(), &d, container, None)
        .await
        .unwrap();
    assert!(!did);
    rig.state.forget(&d.replica);
    let did = restart_container(&rig.node, &rig.state, env.as_ref(), &d, container, None)
        .await
        .unwrap();
    assert!(!did);
    assert!(rig.node.log().is_empty(), "{:?}", rig.node.log());
}

#[tokio::test]
async fn a_restart_does_not_wait_for_a_start_that_holds_the_replica_lock() {
    let rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    // A start (a long pull, say) holds the replica's lock.
    let lock = rig.state.replica_lock(&d.replica);
    let held = lock.lock().await;
    rig.node.take_log();
    let env = StaticEnv::secret();
    let did = tokio::time::timeout(
        secs(5),
        restart_container(
            &rig.node,
            &rig.state,
            env.as_ref(),
            &d,
            &d.containers[0],
            None,
        ),
    )
    .await
    .expect("the restart does not wait")
    .unwrap();
    assert!(!did);
    assert!(rig.node.log().is_empty());
    drop(held);
    // Next tick it can go.
    assert!(
        restart_container(
            &rig.node,
            &rig.state,
            env.as_ref(),
            &d,
            &d.containers[0],
            None
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn stops_and_removals_run_off_the_tick() {
    let node = GatedNode::new();
    let clock = Arc::new(FakeClock::default());
    let state = Arc::new(RunnerState::default());
    state.set_subnet(Some(SUBNET.parse().unwrap()));
    let mut sup = Supervisor::new(
        node.clone(),
        clock.clone(),
        FakeProber::default(),
        StaticEnv::secret(),
        state.clone(),
        SupervisorConfig::default(),
    );
    let gone = plain(1, RestartPolicy::Always, false);
    let other = desired(
        "api",
        1,
        0,
        RestartPolicy::Always,
        vec![container("app", false)],
        vec![],
    );
    start(&node.inner, &state, &gone).await;
    start(&node.inner, &state, &other).await;
    sup.tick().await;
    state.request_stop(&gone.replica, StopReason::Removed);
    sup.tick().await;

    // The drain is over; the stop blocks inside faber-node.
    clock.advance(DRAIN_PERIOD);
    node.inner.exit(&other.replica, "app", 1);
    tokio::time::timeout(secs(5), sup.tick())
        .await
        .expect("the tick does not wait for the stop");
    // The tick noticed the exit of the other replica meanwhile.
    clock.advance(ms(100));
    tokio::time::timeout(secs(5), sup.tick())
        .await
        .expect("the tick does not wait for the stop");
    assert_eq!(
        node.inner.container_states(&other.replica),
        [("app".to_owned(), ContainerState::Running)]
    );
    assert_eq!(node.inner.sandbox_count(), 2, "the stop is still blocked");

    node.gate.add_permits(100);
    sup.settle().await;
    assert_eq!(node.inner.sandbox_count(), 1);
}

#[tokio::test]
async fn a_probe_stop_runs_off_the_tick() {
    let node = GatedNode::new();
    let clock = Arc::new(FakeClock::default());
    let prober = FakeProber::default();
    let state = Arc::new(RunnerState::default());
    state.set_subnet(Some(SUBNET.parse().unwrap()));
    let mut sup = Supervisor::new(
        node.clone(),
        clock.clone(),
        prober.clone(),
        StaticEnv::secret(),
        state.clone(),
        SupervisorConfig::default(),
    );
    let d = plain(1, RestartPolicy::Always, true);
    start(&node.inner, &state, &d).await;
    sup.tick().await;
    prober.set_failing(true);
    for _ in 0..3 {
        clock.advance(secs(5));
        tokio::time::timeout(secs(5), sup.tick())
            .await
            .expect("the tick does not wait for the stop");
    }
    clock.advance(DRAIN_PERIOD);
    tokio::time::timeout(secs(5), sup.tick())
        .await
        .expect("the tick does not wait for the stop");
    assert!(
        !node
            .inner
            .log()
            .iter()
            .any(|l| l.starts_with("stop_container")),
        "the stop is blocked in faber-node"
    );
    node.gate.add_permits(100);
    sup.settle().await;
    assert!(
        node.inner
            .log()
            .iter()
            .any(|l| l.starts_with("stop_container"))
    );
}

#[tokio::test]
async fn only_addresses_in_the_node_subnet_are_probed_or_reported() {
    let mut rig = Rig::new();
    rig.state.set_subnet(Some("10.9.0.0/24".parse().unwrap()));
    let d = plain(1, RestartPolicy::Always, true);
    start(&rig.node, &rig.state, &d).await;
    rig.step(Duration::ZERO).await;
    rig.step(secs(5)).await;
    assert!(rig.prober.asked().is_empty());
    assert_eq!(rig.state.replicas()[0].pod_ip, None);

    // Without a subnet nothing can be checked either.
    rig.state.set_subnet(None);
    rig.step(secs(5)).await;
    assert!(rig.prober.asked().is_empty());

    // The network and broadcast addresses are not hosts.
    for edge in ["10.1.0.0/31", "10.1.0.1/32"] {
        rig.state.set_subnet(Some(edge.parse().unwrap()));
        rig.step(secs(5)).await;
    }
    rig.state.set_subnet(Some(SUBNET.parse().unwrap()));
    rig.step(secs(5)).await;
    assert_eq!(
        rig.state.replicas()[0].pod_ip,
        Some("10.1.0.1".parse().unwrap())
    );
}

#[tokio::test]
async fn a_replica_waiting_for_a_block_volume_starts_when_the_old_one_is_gone() {
    let mut rig = Rig::new();
    let block = |revision| {
        desired(
            "db",
            revision,
            0,
            RestartPolicy::Always,
            vec![container("app", false)],
            vec![volume("data", VolumeKind::Block)],
        )
    };
    let (old, new) = (block(1), block(2));
    start(&rig.node, &rig.state, &old).await;
    rig.state.request_stop(&old.replica, StopReason::Replaced);
    rig.state.register(&new);
    rig.state.set_deferred(&new.replica, Some("waiting".into()));
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.state_of("db", 2), ReplicaState::Deploying);
    assert_eq!(rig.node.sandbox_count(), 1);

    // The old one stops fully first; only then does the new one start.
    rig.step(DRAIN_PERIOD).await;
    assert_eq!(rig.node.sandbox_count(), 0, "the old revision is gone");
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.node.sandbox_count(), 1);
    let log = rig.node.log();
    let removed = log
        .iter()
        .position(|l| l.starts_with("remove_sandbox"))
        .unwrap();
    let created = log
        .iter()
        .rposition(|l| l.starts_with("create_container"))
        .unwrap();
    assert!(removed < created, "{log:?}");
    rig.step(Duration::ZERO).await;
    assert_eq!(rig.state_of("db", 2), ReplicaState::Available);
    // The block volume was kept throughout.
    assert_eq!(rig.node.volumes().len(), 1);
}

#[tokio::test]
async fn a_failed_deferred_start_is_tried_again_after_a_growing_pause() {
    let mut rig = Rig::new();
    let block = |revision| {
        desired(
            "db",
            revision,
            0,
            RestartPolicy::Always,
            vec![container("app", false)],
            vec![volume("data", VolumeKind::Block)],
        )
    };
    let (old, new) = (block(1), block(2));
    start(&rig.node, &rig.state, &old).await;
    rig.state.request_stop(&old.replica, StopReason::Replaced);
    rig.state.register(&new);
    rig.state.set_deferred(&new.replica, Some("waiting".into()));
    rig.step(Duration::ZERO).await;
    rig.step(DRAIN_PERIOD).await;
    assert_eq!(rig.node.sandbox_count(), 0);

    let pulls = |rig: &Rig| {
        rig.node
            .log()
            .iter()
            .filter(|l| l.starts_with("pull_image"))
            .count()
    };
    let before = pulls(&rig);
    rig.node.fail_next("pull_image");
    rig.node.fail_next("pull_image");
    rig.step(Duration::ZERO).await;
    assert_eq!(pulls(&rig) - before, 1, "first try");
    rig.step(ms(99)).await;
    assert_eq!(pulls(&rig) - before, 1, "100 ms pause");
    rig.step(ms(1)).await;
    assert_eq!(pulls(&rig) - before, 2, "second try");
    rig.step(ms(199)).await;
    assert_eq!(pulls(&rig) - before, 2, "200 ms pause");
    rig.step(ms(1)).await;
    assert_eq!(pulls(&rig) - before, 3, "third try works");
    assert!(rig.state.entry(&new.replica).unwrap().launched);
    rig.step(secs(60)).await;
    assert_eq!(pulls(&rig) - before, 3, "no more tries once it runs");
}

#[tokio::test]
async fn a_stop_that_was_withdrawn_and_asked_again_starts_its_drain_over() {
    let mut rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    rig.state.request_stop(&d.replica, StopReason::Removed);
    rig.step(Duration::ZERO).await;
    rig.step(DRAIN_PERIOD - ms(1)).await;
    // Wanted again, then not wanted again, between two ticks.
    rig.state.register(&d);
    rig.state.request_stop(&d.replica, StopReason::Removed);
    rig.step(ms(1)).await;
    assert_eq!(rig.node.sandbox_count(), 1, "the old wait does not count");
    rig.step(DRAIN_PERIOD - ms(1)).await;
    assert_eq!(rig.node.sandbox_count(), 1);
    rig.step(ms(1)).await;
    assert_eq!(rig.node.sandbox_count(), 0);
}

#[tokio::test]
async fn a_removal_that_waited_for_the_lock_finds_the_stop_withdrawn() {
    let rig = Rig::new();
    let d = plain(1, RestartPolicy::Always, false);
    start(&rig.node, &rig.state, &d).await;
    rig.state.request_stop(&d.replica, StopReason::Removed);
    let pod = list_replicas(&rig.node).await.unwrap().remove(0);
    rig.node.take_log();

    let lock = rig.state.replica_lock(&d.replica);
    let held = lock.lock().await;
    let (node, state, replica) = (rig.node.clone(), rig.state.clone(), d.replica.clone());
    let removal = tokio::spawn(async move { retire(&node, &state, &replica, Some(&pod), 5).await });
    tokio::task::yield_now().await;
    // Wanted again while the removal waits.
    rig.state.register(&d);
    drop(held);
    removal.await.unwrap().unwrap();
    assert!(rig.node.log().is_empty(), "{:?}", rig.node.log());
    assert!(rig.state.entry(&d.replica).is_some());
}

#[tokio::test]
async fn the_ramdisk_collector_does_not_race_a_start_of_the_same_slot() {
    let rig = Rig::new();
    let old = desired(
        "web",
        1,
        0,
        RestartPolicy::Always,
        vec![container("app", false)],
        vec![volume("scratch", VolumeKind::Ramdisk)],
    );
    rig.node
        .leave_volume(&old.replica, "scratch", VolumeKind::Ramdisk);
    // A start of the slot holds its lock while it makes the volumes.
    let lock = rig.state.slot_lock(&slot_of(&old.replica));
    let held = lock.lock().await;
    let (node, state) = (rig.node.clone(), rig.state.clone());
    let collector = tokio::spawn(async move { remove_unused_ramdisks(&node, &state).await });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    // The start registers its replica, then lets go of the lock.
    rig.state.register(&desired(
        "web",
        2,
        0,
        RestartPolicy::Always,
        vec![container("app", false)],
        vec![volume("scratch", VolumeKind::Ramdisk)],
    ));
    drop(held);
    collector.await.unwrap().unwrap();
    assert!(
        !rig.node
            .log()
            .iter()
            .any(|l| l.starts_with("remove_volume"))
    );
    assert_eq!(rig.node.volumes().len(), 1);
}
