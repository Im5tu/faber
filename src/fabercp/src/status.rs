//! Node status: every node reports its replicas and its capacity through the
//! leader on a cluster-wide interval, and readers work out `unavailable`
//! from how old a report is.
//!
//! The write side is [`Reporter`]: once per interval it sends one
//! `ReportNode` with everything this node knows. The node sends no time;
//! apply stamps the record with the lease clock (see
//! [`crate::raft::store::status`]). The read side is [`view`]: a replica
//! whose node has not reported for three intervals on that same clock reads
//! `unavailable`, whatever it last said. No write marks a node as quiet.
//!
//! The lease clock moves in steps of the leader's tick (5 to 60 seconds in
//! real time), so a report is up to one tick older on the clock than it is
//! in real time. The limit is three intervals plus one tick, so a node that
//! reports on time never reads `unavailable`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use faber_api_types::workload::{ReplicaState, ReplicaStatus};
use faber_node_protocol::{Request, Response};
use tokio_util::sync::CancellationToken;

use crate::mesh::MeshState;
use crate::raft::store::place::ReplicaPlacement;
use crate::raft::store::status::{
    Capacity, NodeStatus, ReplicaReport, ReportNode, STALE_AFTER_INTERVALS,
};
use crate::raft::store::{ReadError, StateReader};
use crate::runner::NodeClient;
use crate::service::{FaberService, ServiceError, ServiceResult};

/// Longest wait for one report to be accepted. A slower one is dropped; the
/// next tick sends fresh state anyway.
const REPORT_TIMEOUT: Duration = Duration::from_secs(10);

/// Least real time between two reports. The wait is the interval in lease
/// seconds, which a test runs a thousand times faster than real time; every
/// report is a raft write, so it has a floor. The shortest interval an admin
/// can set is one second, so production never reaches it.
const MIN_REPORT_WAIT: Duration = Duration::from_secs(1);

/// What this node's runner knows about its replicas.
pub trait ReplicaSource: Send + Sync {
    /// Every replica on this node, in any order.
    fn replicas(&self) -> Vec<ReplicaReport>;
}

/// The source of a node that runs nothing: reports no replicas.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoReplicas;

impl ReplicaSource for NoReplicas {
    fn replicas(&self) -> Vec<ReplicaReport> {
        Vec::new()
    }
}

/// Measures what this node can offer. May block briefly (it reads `/proc`),
/// so the reporter calls it off the async threads. Its `disk_bytes` is
/// replaced by the [`DiskProbe`] of the reporter, when there is one.
pub trait CapacityProbe: Send + Sync {
    fn capacity(&self) -> Capacity;
}

/// Measures the free disk space that block volumes can use. Those live under
/// the volume root of faber-node, which fabercp does not know and may not
/// read, so only faber-node can say.
pub trait DiskProbe: Send + Sync {
    /// The free bytes, or `None` when they cannot be measured.
    fn free_bytes(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>>;
}

/// How long the reporter waits for a [`DiskProbe`]. A report that waited
/// longer would be dropped as a whole ([`REPORT_TIMEOUT`]) and the node
/// would go quiet.
const DISK_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The [`DiskProbe`] that asks faber-node ([`Request::VolumeSpace`]).
pub struct NodeDisk<C> {
    client: C,
    /// Whether the last measurement failed, so that a faber-node that is
    /// down is logged once and not at every report.
    failing: AtomicBool,
}

impl<C> NodeDisk<C> {
    pub fn new(client: C) -> Self {
        Self {
            client,
            failing: AtomicBool::new(false),
        }
    }
}

impl<C: NodeClient> DiskProbe for NodeDisk<C> {
    fn free_bytes(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>> {
        Box::pin(async move {
            let answer = match self.client.call(Request::VolumeSpace).await {
                Ok(Response::VolumeSpace { free_bytes }) => Ok(free_bytes),
                Ok(_) => Err("it answered with a response that is not for this request".into()),
                Err(e) => Err(e.to_string()),
            };
            match answer {
                Ok(free_bytes) => {
                    if self.failing.swap(false, Ordering::Relaxed) {
                        tracing::info!("faber-node reports the free disk space again");
                    }
                    Some(free_bytes)
                }
                Err(why) => {
                    if !self.failing.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            error = %why,
                            "cannot get the free disk space from faber-node; this node reports \
                             no disk, so no replica with a block volume is placed on it"
                        );
                    }
                    None
                }
            }
        })
    }
}

/// A probe that answers a fixed figure: for tests, and for hosts that have
/// no real probe.
#[derive(Debug, Clone, Copy)]
pub struct FixedCapacity(pub Capacity);

impl CapacityProbe for FixedCapacity {
    fn capacity(&self) -> Capacity {
        self.0
    }
}

/// CPU capacity in millicores: one thousand per available CPU.
fn cpu_millis() -> u64 {
    std::thread::available_parallelism().map_or(1_000, |n| (n.get() as u64).saturating_mul(1_000))
}

/// `MemTotal` of the text of `/proc/meminfo`, in bytes.
pub fn parse_meminfo_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let mut parts = line.split_whitespace().skip(1);
    let value: u64 = parts.next()?.parse().ok()?;
    match parts.next() {
        Some("kB") => value.checked_mul(1024),
        None => Some(value),
        Some(_) => None,
    }
}

/// The probe for this host: on Linux the CPUs and `MemTotal`; elsewhere only
/// the CPUs. The disk is not measured here ([`DiskProbe`]).
pub fn system_probe() -> Arc<dyn CapacityProbe> {
    #[cfg(target_os = "linux")]
    {
        Arc::new(linux::ProcProbe)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Arc::new(FixedCapacity(Capacity {
            cpu_millis: cpu_millis(),
            ..Capacity::default()
        }))
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Capacity, CapacityProbe, cpu_millis, parse_meminfo_total};

    pub struct ProcProbe;

    impl CapacityProbe for ProcProbe {
        fn capacity(&self) -> Capacity {
            let memory_bytes = std::fs::read_to_string("/proc/meminfo")
                .ok()
                .and_then(|text| parse_meminfo_total(&text))
                .unwrap_or(0);
            Capacity {
                cpu_millis: cpu_millis(),
                memory_bytes,
                disk_bytes: 0,
            }
        }
    }
}

/// Where a report goes: through the leader, in production. A trait so that
/// tests can count and inspect the writes.
pub trait ReportSink: Send + Sync {
    fn send(&self, report: ReportNode) -> impl Future<Output = ServiceResult<()>> + Send;
}

impl ReportSink for FaberService {
    async fn send(&self, report: ReportNode) -> ServiceResult<()> {
        self.report_node(report, false).await
    }
}

/// How long a capacity measurement is reused. Capacity moves slowly, and a
/// probe reads `/proc` and asks the file system.
const CAPACITY_REFRESH: Duration = Duration::from_secs(60);
/// The first wait after a failed report; it doubles up to the interval.
const RETRY_MIN: Duration = Duration::from_secs(1);

/// Who this node reports as: its id and instance id, or `None` while it has
/// no mesh identity.
type Identity = Box<dyn Fn() -> Option<(u64, String)> + Send + Sync>;

/// Sends this node's status once per interval.
pub struct Reporter<S> {
    identity: Identity,
    sink: S,
    replicas: Arc<dyn ReplicaSource>,
    capacity: Arc<dyn CapacityProbe>,
    /// Where the disk figure comes from. Without one, the probe's own.
    disk: Option<Arc<dyn DiskProbe>>,
    /// The last measurement and when it was made.
    measured: Mutex<Option<(tokio::time::Instant, Capacity)>>,
}

impl<S: ReportSink> Reporter<S> {
    pub fn new(
        identity: impl Fn() -> Option<(u64, String)> + Send + Sync + 'static,
        sink: S,
        replicas: Arc<dyn ReplicaSource>,
        capacity: Arc<dyn CapacityProbe>,
    ) -> Self {
        Self {
            identity: Box::new(identity),
            sink,
            replicas,
            capacity,
            disk: None,
            measured: Mutex::new(None),
        }
    }

    /// Takes the disk capacity from `disk`. When it cannot say, the node
    /// reports no disk at all, which keeps every replica with a block volume
    /// off it, rather than the free space of some other file system. That
    /// measurement is not kept, so the next report asks again.
    #[must_use]
    pub fn with_disk(mut self, disk: Arc<dyn DiskProbe>) -> Self {
        self.disk = Some(disk);
        self
    }

    /// The capacity, measured at most once per [`CAPACITY_REFRESH`].
    async fn capacity(&self) -> ServiceResult<Capacity> {
        let now = tokio::time::Instant::now();
        let fresh = |m: &Option<(tokio::time::Instant, Capacity)>| {
            m.filter(|(at, _)| now.duration_since(*at) < CAPACITY_REFRESH)
                .map(|(_, c)| c)
        };
        if let Some(c) = fresh(&self.measured.lock().unwrap_or_else(|e| e.into_inner())) {
            return Ok(c);
        }
        let probe = self.capacity.clone();
        let mut c = tokio::task::spawn_blocking(move || probe.capacity())
            .await
            .map_err(|e| ServiceError::Internal(format!("the capacity probe failed: {e}")))?;
        if let Some(disk) = &self.disk {
            let free = tokio::time::timeout(DISK_PROBE_TIMEOUT, disk.free_bytes())
                .await
                .ok()
                .flatten();
            let Some(free) = free else {
                c.disk_bytes = 0;
                return Ok(c);
            };
            c.disk_bytes = free;
        }
        *self.measured.lock().unwrap_or_else(|e| e.into_inner()) = Some((now, c));
        Ok(c)
    }

    /// One report with all replicas in one write. An error while the node
    /// has no identity: the next tick asks again.
    pub async fn report_once(&self) -> ServiceResult<()> {
        let (node_id, instance_id) = (self.identity)().ok_or_else(|| {
            ServiceError::BadRequest("this node has no instance id to report with".into())
        })?;
        let capacity = self.capacity().await?;
        self.sink
            .send(ReportNode {
                node_id,
                instance_id,
                capacity,
                replicas: self.replicas.replicas(),
            })
            .await
    }

    /// Reports at once and then every `interval()` seconds, each lasting
    /// `second` of real time, until `cancel` fires. `interval` is read before
    /// every wait, so a changed setting is followed from the next tick. A
    /// failed report is logged and tried again after a wait that starts at
    /// a second and doubles up to the interval; the first failure after a
    /// success is a warning, the rest are debug.
    pub async fn run(
        &self,
        interval: impl Fn() -> u64 + Send,
        second: Duration,
        cancel: &CancellationToken,
    ) {
        let mut failures: u32 = 0;
        loop {
            let attempt = tokio::select! {
                () = cancel.cancelled() => return,
                r = tokio::time::timeout(REPORT_TIMEOUT, self.report_once()) => r,
            };
            match attempt {
                Ok(Ok(())) => {
                    failures = 0;
                    tracing::debug!("reported this node's status");
                }
                failed => {
                    let why = match failed {
                        Ok(Err(e)) => e.to_string(),
                        _ => format!("no answer within {REPORT_TIMEOUT:?}"),
                    };
                    if failures > 0 {
                        tracing::debug!(error = %why, "could not report this node's status");
                    } else {
                        tracing::warn!(error = %why, "could not report this node's status");
                    }
                    failures = failures.saturating_add(1);
                }
            }
            let every = second
                .saturating_mul(u32::try_from(interval()).unwrap_or(u32::MAX))
                .max(MIN_REPORT_WAIT);
            let wait = match failures {
                0 => every,
                n => RETRY_MIN
                    .saturating_mul(1u32.checked_shl(n - 1).unwrap_or(u32::MAX))
                    .min(every),
            };
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(wait) => {}
            }
        }
    }
}

/// What the nodes reported, with staleness applied.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StatusView {
    /// Every node that ever reported.
    pub nodes: BTreeMap<u64, NodeView>,
    replicas: Vec<(u64, ReplicaReport)>,
}

/// One node in a [`StatusView`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    pub capacity: Capacity,
    /// The lease clock at its last report.
    pub reported_at: u64,
    /// No report for [`STALE_AFTER_INTERVALS`] intervals.
    pub stale: bool,
}

impl StatusView {
    /// Builds the view of `statuses` at `now` on the lease clock, for reports
    /// every `interval_secs`. Every replica of a stale node is `unavailable`.
    pub fn new(
        statuses: BTreeMap<u64, NodeStatus>,
        interval_secs: u64,
        now: u64,
        tick_secs: u64,
    ) -> Self {
        // The clock runs in steps of `tick_secs`, so a node that reported on
        // time can be a whole tick behind it.
        let limit = interval_secs
            .saturating_mul(STALE_AFTER_INTERVALS)
            .saturating_add(tick_secs);
        let mut view = StatusView::default();
        for (id, status) in statuses {
            let stale = now.saturating_sub(status.reported_at) > limit;
            view.nodes.insert(
                id,
                NodeView {
                    capacity: status.capacity,
                    reported_at: status.reported_at,
                    stale,
                },
            );
            for mut replica in status.replicas {
                if stale {
                    replica.state = ReplicaState::Unavailable;
                }
                view.replicas.push((id, replica));
            }
        }
        view
    }

    /// The revisions of one workload that some node reported a replica of,
    /// whether or not the replica is placed on that node.
    pub fn revisions(&self, project: &str, environment: &str, workload: &str) -> BTreeSet<u64> {
        self.replicas
            .iter()
            .map(|(_, r)| r)
            .filter(|r| {
                r.project == project && r.environment == environment && r.workload == workload
            })
            .map(|r| r.revision)
            .collect()
    }

    /// The replicas of one workload at `revision`, one per replica in
    /// `placed` and in its order (by replica index).
    ///
    /// A replica shows the report of the node it is placed on, and only that
    /// one: what another node claims about a replica that is not placed on it
    /// is ignored, as are reports of another revision and of a workload that
    /// is gone. A replica that its node has not reported yet is `deploying`,
    /// or `unavailable` when the node is stale. An unplaced replica is
    /// `unavailable` with the reason it has no node. `revision` is the stored
    /// revision of a workload that exists.
    pub fn workload(
        &self,
        project: &str,
        environment: &str,
        workload: &str,
        revision: u64,
        placed: &[ReplicaPlacement],
    ) -> Vec<ReplicaStatus> {
        placed
            .iter()
            .map(|p| {
                let Some(node) = p.node else {
                    return ReplicaStatus {
                        node: None,
                        replica_index: p.index,
                        revision,
                        state: ReplicaState::Unavailable,
                        pod_ip: None,
                        reason: p.reason.clone(),
                    };
                };
                let report = self.replicas.iter().find(|(n, r)| {
                    *n == node
                        && r.project == project
                        && r.environment == environment
                        && r.workload == workload
                        && r.revision == revision
                        && r.replica_index == p.index
                });
                let (state, pod_ip) = match report {
                    Some((_, r)) => (r.state, r.pod_ip),
                    None if self.nodes.get(&node).is_some_and(|n| n.stale) => {
                        (ReplicaState::Unavailable, None)
                    }
                    None => (ReplicaState::Deploying, None),
                };
                ReplicaStatus {
                    node: Some(node),
                    replica_index: p.index,
                    revision,
                    state,
                    pod_ip,
                    reason: None,
                }
            })
            .collect()
    }
}

/// The reports in `state` as of the lease clock of `mesh`, for nodes that
/// are in the mesh: a removed node's last report is not shown. A cluster
/// with no reports gives an empty view. The interval is the one in force
/// ([`crate::raft::store::status::IntervalSetting::effective`]).
pub fn view(
    state: &StateReader,
    mesh: &MeshState,
    tick_secs: u64,
) -> Result<StatusView, ReadError> {
    let mut statuses = state.statuses()?;
    statuses.retain(|id, _| mesh.nodes.contains_key(id));
    if statuses.is_empty() {
        return Ok(StatusView::default());
    }
    let interval = state.status_setting()?.effective(mesh.clock);
    Ok(StatusView::new(statuses, interval, mesh.clock, tick_secs))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn replica(workload: &str, index: u32, state: ReplicaState) -> ReplicaReport {
        ReplicaReport {
            project: "shop".into(),
            environment: "staging".into(),
            workload: workload.into(),
            revision: 2,
            replica_index: index,
            state,
            pod_ip: Some("10.200.1.2".parse().unwrap()),
            detail: None,
        }
    }

    /// Replica `index` placed on `node`, for each pair.
    fn placed(pairs: &[(u32, u64)]) -> Vec<ReplicaPlacement> {
        pairs
            .iter()
            .map(|(index, node)| ReplicaPlacement::on(*index, *node))
            .collect()
    }

    fn record(reported_at: u64, replicas: Vec<ReplicaReport>) -> NodeStatus {
        NodeStatus {
            reported_at,
            capacity: Capacity::default(),
            replicas,
        }
    }

    #[test]
    fn a_report_older_than_three_intervals_reads_unavailable() {
        let statuses = BTreeMap::from([
            (
                1,
                record(100, vec![replica("web", 0, ReplicaState::Available)]),
            ),
            (
                2,
                record(100, vec![replica("web", 1, ReplicaState::Deploying)]),
            ),
        ]);
        // Interval 5: fresh up to and including 15 seconds of age.
        for (now, stale) in [(100, false), (115, false), (116, true), (10_000, true)] {
            let view = StatusView::new(statuses.clone(), 5, now, 0);
            let got = view.workload("shop", "staging", "web", 2, &placed(&[(0, 1), (1, 2)]));
            assert_eq!(got.len(), 2);
            let want = if stale {
                [ReplicaState::Unavailable, ReplicaState::Unavailable]
            } else {
                [ReplicaState::Available, ReplicaState::Deploying]
            };
            assert_eq!([got[0].state, got[1].state], want, "now {now}");
            assert_eq!(view.nodes[&1].stale, stale);
        }
        // The interval scales the limit.
        let view = StatusView::new(statuses, 30, 190, 0);
        assert!(!view.nodes[&1].stale);
        // A node that is quiet does not hide a node that reports.
        let mixed = BTreeMap::from([
            (
                1,
                record(0, vec![replica("web", 0, ReplicaState::Available)]),
            ),
            (
                2,
                record(100, vec![replica("web", 1, ReplicaState::Available)]),
            ),
        ]);
        let got = StatusView::new(mixed, 5, 100, 0).workload(
            "shop",
            "staging",
            "web",
            2,
            &placed(&[(0, 1), (1, 2)]),
        );
        assert_eq!(
            got.iter().map(|r| (r.node, r.state)).collect::<Vec<_>>(),
            vec![
                (Some(1), ReplicaState::Unavailable),
                (Some(2), ReplicaState::Available)
            ]
        );
    }

    /// The clock moves 60 seconds at a time; reports come every 5.
    #[test]
    fn a_coarse_clock_tick_does_not_make_a_node_that_reports_look_stale() {
        let tick = 60;
        let reported = |at| {
            BTreeMap::from([(
                1,
                record(at, vec![replica("web", 0, ReplicaState::Available)]),
            )])
        };
        let state = |at, now| {
            StatusView::new(reported(at), 5, now, tick).workload(
                "shop",
                "staging",
                "web",
                2,
                &placed(&[(0, 1)]),
            )[0]
            .state
        };
        // Reported just before the clock ticked, and not again yet: a whole
        // tick old on the clock.
        assert_eq!(state(0, 60), ReplicaState::Available);
        // It stops: unavailable once older than three intervals and a tick.
        assert_eq!(state(0, 75), ReplicaState::Available);
        assert_eq!(state(0, 76), ReplicaState::Unavailable);
        // Without the tick the same age would already be stale.
        let bare = StatusView::new(reported(0), 5, 60, 0);
        assert!(bare.nodes[&1].stale);
    }

    #[test]
    fn a_workload_reads_only_its_own_replicas_in_index_order() {
        let statuses = BTreeMap::from([
            (
                3,
                record(
                    0,
                    vec![
                        replica("web", 2, ReplicaState::Available),
                        replica("db", 0, ReplicaState::Available),
                    ],
                ),
            ),
            (
                1,
                record(0, vec![replica("web", 0, ReplicaState::Available)]),
            ),
        ]);
        let view = StatusView::new(statuses, 5, 0, 0);
        let web = view.workload("shop", "staging", "web", 2, &placed(&[(0, 1), (2, 3)]));
        assert_eq!(
            web.iter()
                .map(|r| (r.replica_index, r.node))
                .collect::<Vec<_>>(),
            vec![(0, Some(1)), (2, Some(3))]
        );
        assert!(
            view.workload("shop", "production", "web", 2, &[])
                .is_empty()
        );
        assert!(
            StatusView::default()
                .workload("a", "b", "c", 2, &[])
                .is_empty()
        );
    }

    #[test]
    fn only_the_report_of_the_node_a_replica_is_placed_on_is_shown() {
        // Node 2 claims replica 0, which is placed on node 1, and replica 1
        // twice over, which is placed on node 2 only.
        let statuses = BTreeMap::from([
            (
                1,
                record(0, vec![replica("web", 0, ReplicaState::Available)]),
            ),
            (
                2,
                record(
                    0,
                    vec![
                        replica("web", 0, ReplicaState::Deploying),
                        replica("web", 1, ReplicaState::Available),
                    ],
                ),
            ),
            (
                3,
                record(0, vec![replica("web", 1, ReplicaState::Deploying)]),
            ),
        ]);
        let view = StatusView::new(statuses, 5, 0, 0);
        let got = view.workload("shop", "staging", "web", 2, &placed(&[(0, 1), (1, 2)]));
        assert_eq!(
            got.iter()
                .map(|r| (r.node, r.replica_index, r.state))
                .collect::<Vec<_>>(),
            vec![
                (Some(1), 0, ReplicaState::Available),
                (Some(2), 1, ReplicaState::Available)
            ]
        );
        // Nothing is placed: nothing is shown, whatever the nodes claim.
        assert!(view.workload("shop", "staging", "web", 2, &[]).is_empty());
        // A node that is not the one it is placed on has no say.
        let got = view.workload("shop", "staging", "web", 2, &placed(&[(0, 3)]));
        assert_eq!(got[0].state, ReplicaState::Deploying);
        assert_eq!(got[0].pod_ip, None);
    }

    #[test]
    fn a_placed_replica_nobody_reported_is_deploying_and_an_unplaced_one_says_why() {
        let statuses = BTreeMap::from([(
            1,
            record(0, vec![replica("web", 0, ReplicaState::Available)]),
        )]);
        let mut unplaced = placed(&[(0, 1), (1, 1)]);
        unplaced.push(ReplicaPlacement::unplaced(2, "insufficient_capacity"));
        let fresh = StatusView::new(statuses.clone(), 5, 0, 0);
        let got = fresh.workload("shop", "staging", "web", 2, &unplaced);
        assert_eq!(
            got.iter().map(|r| r.state).collect::<Vec<_>>(),
            [
                ReplicaState::Available,
                ReplicaState::Deploying,
                ReplicaState::Unavailable
            ]
        );
        assert_eq!((got[2].node, got[2].revision), (None, 2));
        assert_eq!(got[2].reason.as_deref(), Some("insufficient_capacity"));
        assert_eq!(got[1].reason, None);
        // The node went quiet: what it never reported is unavailable too.
        let stale = StatusView::new(statuses, 5, 1000, 0);
        let got = stale.workload("shop", "staging", "web", 2, &unplaced);
        assert!(got.iter().all(|r| r.state == ReplicaState::Unavailable));
        // A report of another revision does not count.
        let got = fresh.workload("shop", "staging", "web", 3, &placed(&[(0, 1)]));
        assert_eq!(got[0].state, ReplicaState::Deploying);
        assert_eq!(got[0].revision, 3);
    }

    #[test]
    fn meminfo_total_is_read_in_bytes() {
        let text = "MemTotal:       16318412 kB\nMemFree:         1 kB\n";
        assert_eq!(parse_meminfo_total(text), Some(16_318_412 * 1024));
        assert_eq!(parse_meminfo_total("MemFree: 1 kB\n"), None);
        assert_eq!(parse_meminfo_total("MemTotal: lots kB\n"), None);
        assert_eq!(parse_meminfo_total("MemTotal: 5 MB\n"), None);
        assert_eq!(parse_meminfo_total(""), None);
    }

    struct Replicas(Vec<ReplicaReport>);

    impl ReplicaSource for Replicas {
        fn replicas(&self) -> Vec<ReplicaReport> {
            self.0.clone()
        }
    }

    /// Records every report with the paused clock's time of arrival.
    #[derive(Clone, Default)]
    struct Recorder {
        sent: Arc<Mutex<Vec<(tokio::time::Instant, ReportNode)>>>,
    }

    impl ReportSink for Recorder {
        async fn send(&self, report: ReportNode) -> ServiceResult<()> {
            self.sent
                .lock()
                .unwrap()
                .push((tokio::time::Instant::now(), report));
            Ok(())
        }
    }

    fn fake_capacity() -> Capacity {
        Capacity {
            cpu_millis: 2_000,
            memory_bytes: 1 << 30,
            disk_bytes: 1 << 34,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_reporter_batches_every_replica_in_one_write_per_tick() {
        let recorder = Recorder::default();
        let reporter = Reporter::new(
            || Some((4, "ab".repeat(16))),
            recorder.clone(),
            Arc::new(Replicas(vec![
                replica("web", 0, ReplicaState::Available),
                replica("web", 1, ReplicaState::Deploying),
                replica("db", 0, ReplicaState::Available),
            ])),
            Arc::new(FixedCapacity(fake_capacity())),
        );
        let interval = Arc::new(AtomicU64::new(5));
        let cancel = CancellationToken::new();
        let task = {
            let (interval, cancel) = (interval.clone(), cancel.clone());
            tokio::spawn(async move {
                reporter
                    .run(
                        move || interval.load(Ordering::Relaxed),
                        Duration::from_secs(1),
                        &cancel,
                    )
                    .await;
            })
        };

        // Let the first tick run: one write at once, with all three replicas.
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(recorder.sent.lock().unwrap().len(), 1);

        // Two more ticks, five seconds apart.
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(recorder.sent.lock().unwrap().len(), 3);

        // A new interval is followed from the next wait on.
        interval.store(20, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(recorder.sent.lock().unwrap().len(), 4);
        tokio::time::sleep(Duration::from_secs(19)).await;
        assert_eq!(recorder.sent.lock().unwrap().len(), 4);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(recorder.sent.lock().unwrap().len(), 5);

        cancel.cancel();
        task.await.unwrap();
        let sent = recorder.sent.lock().unwrap();
        for (_, report) in sent.iter() {
            assert_eq!(report.node_id, 4);
            assert_eq!(report.instance_id, "ab".repeat(16));
            assert_eq!(report.capacity, fake_capacity());
            assert_eq!(report.replicas.len(), 3);
        }
        let gaps: Vec<u64> = sent
            .windows(2)
            .map(|w| (w[1].0 - w[0].0).as_secs())
            .collect();
        assert_eq!(gaps, vec![5, 5, 5, 20]);
    }

    /// Tests run the lease clock a thousand times faster; reports must not
    /// follow it down to milliseconds and flood raft.
    #[tokio::test(start_paused = true)]
    async fn reports_are_never_closer_together_than_the_floor() {
        let recorder = Recorder::default();
        let reporter = Reporter::new(
            || Some((1, String::new())),
            recorder.clone(),
            Arc::new(NoReplicas),
            Arc::new(FixedCapacity(Capacity::default())),
        );
        let cancel = CancellationToken::new();
        let task = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                reporter.run(|| 5, Duration::from_millis(1), &cancel).await;
            })
        };
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel.cancel();
        task.await.unwrap();
        let sent = recorder.sent.lock().unwrap();
        assert!(sent.len() <= 11, "{} reports in ten seconds", sent.len());
        assert!(
            sent.windows(2).all(|w| w[1].0 - w[0].0 >= MIN_REPORT_WAIT),
            "reports closer than {MIN_REPORT_WAIT:?}"
        );
    }

    /// Fails twice, then works.
    struct Flaky(Arc<AtomicU64>);

    impl ReportSink for Flaky {
        async fn send(&self, _report: ReportNode) -> ServiceResult<()> {
            if self.0.fetch_add(1, Ordering::Relaxed) < 2 {
                Err(ServiceError::NoLeader)
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_report_is_tried_again_after_a_short_growing_wait() {
        let calls = Arc::new(AtomicU64::new(0));
        let reporter = Reporter::new(
            || Some((1, String::new())),
            Flaky(calls.clone()),
            Arc::new(NoReplicas),
            Arc::new(FixedCapacity(Capacity::default())),
        );
        let cancel = CancellationToken::new();
        let task = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                reporter.run(|| 5, Duration::from_secs(1), &cancel).await;
            })
        };
        // Fails at 0 s and 1 s, waits 1 s then 2 s, works at 3 s, and then
        // waits the interval: 8 s, and 13 s is still to come.
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert_eq!(calls.load(Ordering::Relaxed), 4);
        cancel.cancel();
        task.await.unwrap();
    }

    struct CountingProbe(Arc<AtomicU64>);

    impl CapacityProbe for CountingProbe {
        fn capacity(&self) -> Capacity {
            self.0.fetch_add(1, Ordering::Relaxed);
            fake_capacity()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_is_measured_once_a_minute_and_a_missing_identity_is_retried() {
        let probes = Arc::new(AtomicU64::new(0));
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let recorder = Recorder::default();
        let reporter = {
            let ready = ready.clone();
            Reporter::new(
                move || ready.load(Ordering::Relaxed).then(|| (1, String::new())),
                recorder.clone(),
                Arc::new(NoReplicas),
                Arc::new(CountingProbe(probes.clone())),
            )
        };
        let cancel = CancellationToken::new();
        let task = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                reporter.run(|| 5, Duration::from_secs(1), &cancel).await;
            })
        };
        // No identity: nothing is sent or measured, but the loop goes on.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(recorder.sent.lock().unwrap().len(), 0);
        assert_eq!(probes.load(Ordering::Relaxed), 0);
        ready.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(!recorder.sent.lock().unwrap().is_empty());
        assert_eq!(probes.load(Ordering::Relaxed), 1);
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(probes.load(Ordering::Relaxed), 2);
        assert!(recorder.sent.lock().unwrap().len() >= 15);
        cancel.cancel();
        task.await.unwrap();
    }

    /// A disk probe with a script of answers, where `None` is a measurement
    /// that failed. With `hang` it never answers.
    struct ScriptedDisk {
        answers: Mutex<std::collections::VecDeque<Option<u64>>>,
        asked: AtomicU64,
        hang: bool,
    }

    impl ScriptedDisk {
        fn new(answers: &[Option<u64>]) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(answers.iter().copied().collect()),
                asked: AtomicU64::new(0),
                hang: false,
            })
        }
    }

    impl DiskProbe for ScriptedDisk {
        fn free_bytes(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>> {
            Box::pin(async move {
                self.asked.fetch_add(1, Ordering::Relaxed);
                if self.hang {
                    std::future::pending::<()>().await;
                }
                self.answers.lock().unwrap().pop_front().flatten()
            })
        }
    }

    fn reporter_with(disk: Arc<dyn DiskProbe>) -> Reporter<Recorder> {
        Reporter::new(
            || Some((1, String::new())),
            Recorder::default(),
            Arc::new(NoReplicas),
            Arc::new(FixedCapacity(fake_capacity())),
        )
        .with_disk(disk)
    }

    #[tokio::test(start_paused = true)]
    async fn the_disk_comes_from_the_disk_probe_not_the_capacity_probe() {
        let reporter = reporter_with(ScriptedDisk::new(&[Some(777)]));
        let c = reporter.capacity().await.unwrap();
        assert_eq!(c.disk_bytes, 777);
        // The rest is the capacity probe's.
        assert_eq!(c.memory_bytes, fake_capacity().memory_bytes);
        assert_eq!(c.cpu_millis, fake_capacity().cpu_millis);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unknown_disk_is_reported_as_zero_and_asked_again() {
        let disk = ScriptedDisk::new(&[None, Some(500)]);
        let reporter = reporter_with(disk.clone());
        assert_eq!(reporter.capacity().await.unwrap().disk_bytes, 0);
        // The failure was not kept for a minute.
        assert_eq!(reporter.capacity().await.unwrap().disk_bytes, 500);
        assert_eq!(disk.asked.load(Ordering::Relaxed), 2);
        // The success is.
        assert_eq!(reporter.capacity().await.unwrap().disk_bytes, 500);
        assert_eq!(disk.asked.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_disk_probe_that_hangs_does_not_hold_up_the_report() {
        let disk = Arc::new(ScriptedDisk {
            answers: Mutex::default(),
            asked: AtomicU64::new(0),
            hang: true,
        });
        let reporter = reporter_with(disk);
        let started = tokio::time::Instant::now();
        assert_eq!(reporter.capacity().await.unwrap().disk_bytes, 0);
        assert!(started.elapsed() < REPORT_TIMEOUT);
    }

    /// Answers every request with the next of a list.
    struct Scripted(Mutex<Vec<Result<Response, crate::runner::NodeCallError>>>);

    impl NodeClient for Scripted {
        async fn call(&self, request: Request) -> Result<Response, crate::runner::NodeCallError> {
            assert_eq!(request, Request::VolumeSpace);
            self.0.lock().unwrap().remove(0)
        }
    }

    #[tokio::test]
    async fn faber_node_is_asked_for_the_space_and_anything_else_is_unknown() {
        use crate::runner::NodeCallError;
        use faber_node_protocol::ErrorCode;

        let disk = NodeDisk::new(Scripted(Mutex::new(vec![
            Ok(Response::VolumeSpace { free_bytes: 42 }),
            Err(NodeCallError::Node {
                code: ErrorCode::Runtime,
                message: "no".into(),
            }),
            Err(NodeCallError::Transport("down".into())),
            Ok(Response::Done),
            Ok(Response::VolumeSpace { free_bytes: 7 }),
        ])));
        assert_eq!(disk.free_bytes().await, Some(42));
        assert_eq!(disk.free_bytes().await, None);
        assert_eq!(disk.free_bytes().await, None);
        assert_eq!(disk.free_bytes().await, None);
        assert_eq!(disk.free_bytes().await, Some(7));
    }

    #[tokio::test]
    async fn the_fake_node_answers_the_space_request() {
        let disk = NodeDisk::new(crate::runner::node::fake::FakeNode::default());
        assert!(disk.free_bytes().await.is_some());
    }
}
