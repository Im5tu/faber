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

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use faber_api_types::workload::{ReplicaState, ReplicaStatus};
use tokio_util::sync::CancellationToken;

use crate::raft::store::status::{
    Capacity, NodeStatus, ReplicaReport, ReportNode, STALE_AFTER_INTERVALS,
};
use crate::raft::store::{ReadError, StateReader};
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

/// Measures what this node can offer. May block briefly (it reads `/proc`
/// and asks the file system), so the reporter calls it off the async
/// threads.
pub trait CapacityProbe: Send + Sync {
    fn capacity(&self) -> Capacity;
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

/// The probe for this host: on Linux the CPUs, `MemTotal` and the free space
/// of the file system under `data_dir`; elsewhere only the CPUs.
pub fn system_probe(data_dir: PathBuf) -> Arc<dyn CapacityProbe> {
    #[cfg(target_os = "linux")]
    {
        Arc::new(linux::ProcProbe { data_dir })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = data_dir;
        Arc::new(FixedCapacity(Capacity {
            cpu_millis: cpu_millis(),
            ..Capacity::default()
        }))
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::path::PathBuf;

    use super::{Capacity, CapacityProbe, cpu_millis, parse_meminfo_total};

    pub struct ProcProbe {
        pub data_dir: PathBuf,
    }

    impl CapacityProbe for ProcProbe {
        fn capacity(&self) -> Capacity {
            let memory_bytes = std::fs::read_to_string("/proc/meminfo")
                .ok()
                .and_then(|text| parse_meminfo_total(&text))
                .unwrap_or(0);
            // Free space for unprivileged use: fragment size times free blocks.
            let disk_bytes = nix::sys::statvfs::statvfs(&self.data_dir).map_or(0, |s| {
                let bytes = s.fragment_size() as u128 * s.blocks_available() as u128;
                u64::try_from(bytes).unwrap_or(u64::MAX)
            });
            Capacity {
                cpu_millis: cpu_millis(),
                memory_bytes,
                disk_bytes,
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

/// Sends this node's status once per interval.
pub struct Reporter<S> {
    node_id: u64,
    instance_id: String,
    sink: S,
    replicas: Arc<dyn ReplicaSource>,
    capacity: Arc<dyn CapacityProbe>,
}

impl<S: ReportSink> Reporter<S> {
    pub fn new(
        node_id: u64,
        instance_id: String,
        sink: S,
        replicas: Arc<dyn ReplicaSource>,
        capacity: Arc<dyn CapacityProbe>,
    ) -> Self {
        Self {
            node_id,
            instance_id,
            sink,
            replicas,
            capacity,
        }
    }

    /// One report with all replicas in one write.
    pub async fn report_once(&self) -> ServiceResult<()> {
        let probe = self.capacity.clone();
        let capacity = tokio::task::spawn_blocking(move || probe.capacity())
            .await
            .map_err(|e| ServiceError::Internal(format!("the capacity probe failed: {e}")))?;
        self.sink
            .send(ReportNode {
                node_id: self.node_id,
                instance_id: self.instance_id.clone(),
                capacity,
                replicas: self.replicas.replicas(),
            })
            .await
    }

    /// Reports at once and then every `interval()` seconds, each lasting
    /// `second` of real time, until `cancel` fires. `interval` is read before
    /// every wait, so a changed setting is followed from the next tick. A
    /// failed report is logged and the next tick tries again; the first
    /// failure after a success is a warning, the rest are debug.
    pub async fn run(
        &self,
        interval: impl Fn() -> u64 + Send,
        second: Duration,
        cancel: &CancellationToken,
    ) {
        let mut failing = false;
        loop {
            let attempt = tokio::select! {
                () = cancel.cancelled() => return,
                r = tokio::time::timeout(REPORT_TIMEOUT, self.report_once()) => r,
            };
            match attempt {
                Ok(Ok(())) => {
                    failing = false;
                    tracing::debug!("reported this node's status");
                }
                failed => {
                    let why = match failed {
                        Ok(Err(e)) => e.to_string(),
                        _ => format!("no answer within {REPORT_TIMEOUT:?}"),
                    };
                    if failing {
                        tracing::debug!(error = %why, "could not report this node's status");
                    } else {
                        failing = true;
                        tracing::warn!(error = %why, "could not report this node's status");
                    }
                }
            }
            let wait = second
                .saturating_mul(u32::try_from(interval()).unwrap_or(u32::MAX))
                .max(MIN_REPORT_WAIT);
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

    /// The replicas of one workload, by replica index and then node.
    pub fn workload(&self, project: &str, environment: &str, workload: &str) -> Vec<ReplicaStatus> {
        let mut found: Vec<ReplicaStatus> = self
            .replicas
            .iter()
            .filter(|(_, r)| {
                r.project == project && r.environment == environment && r.workload == workload
            })
            .map(|(node, r)| ReplicaStatus {
                node: *node,
                replica_index: r.replica_index,
                revision: r.revision,
                state: r.state,
                pod_ip: r.pod_ip,
            })
            .collect();
        found.sort_by_key(|r| (r.replica_index, r.node));
        found
    }
}

/// The reports in `state` at `now` on the lease clock. A cluster with no
/// reports gives an empty view.
pub fn view(state: &StateReader, now: u64, tick_secs: u64) -> Result<StatusView, ReadError> {
    let statuses = state.statuses()?;
    if statuses.is_empty() {
        return Ok(StatusView::default());
    }
    Ok(StatusView::new(
        statuses,
        state.status_interval()?,
        now,
        tick_secs,
    ))
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
            let got = view.workload("shop", "staging", "web");
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
        let got = StatusView::new(mixed, 5, 100, 0).workload("shop", "staging", "web");
        assert_eq!(
            got.iter().map(|r| (r.node, r.state)).collect::<Vec<_>>(),
            vec![(1, ReplicaState::Unavailable), (2, ReplicaState::Available)]
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
            StatusView::new(reported(at), 5, now, tick).workload("shop", "staging", "web")[0].state
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
        let web = view.workload("shop", "staging", "web");
        assert_eq!(
            web.iter()
                .map(|r| (r.replica_index, r.node))
                .collect::<Vec<_>>(),
            vec![(0, 1), (2, 3)]
        );
        assert!(view.workload("shop", "production", "web").is_empty());
        assert!(StatusView::default().workload("a", "b", "c").is_empty());
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
            4,
            "ab".repeat(16),
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
            1,
            String::new(),
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
    async fn a_failed_report_is_tried_again_on_the_next_tick() {
        let calls = Arc::new(AtomicU64::new(0));
        let reporter = Reporter::new(
            1,
            String::new(),
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
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        cancel.cancel();
        task.await.unwrap();
    }
}
