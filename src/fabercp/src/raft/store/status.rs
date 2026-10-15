//! Node status commands: what each node reports about its replicas and its
//! capacity, and how often it does.
//!
//! A report replaces the whole record of its node under `status/<node_id>`.
//! Apply stamps it with the lease clock (`MeshState::clock`), never with a
//! time the node sent. A report writes no other key, except that a node's first
//! report records its instance id in the mesh as a lease renewal does. The
//! environment records stay as they are, so no generation moves and the
//! reconcile steps that compare their own desired values see no change.
//!
//! Nothing writes when a node goes quiet. A reader decides a record is stale
//! by comparing `reported_at` with the clock ([`crate::status::view`]).

use std::net::Ipv4Addr;

use faber_api_types::error_code;
use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::ReplicaState;
use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

use crate::mesh::{MeshState, Rejection};

/// Every status record is under this prefix. `Request::Set` refuses it.
pub const KEY_PREFIX: &str = "status/";
/// The report interval. Outside [`KEY_PREFIX`] so that a scan of the records
/// never sees it. `Request::Set` refuses it.
pub const KEY_INTERVAL: &str = "config/status_interval";

/// Report interval until an admin sets one, in seconds.
pub const DEFAULT_INTERVAL_SECS: u64 = 5;
/// Shortest report interval, in seconds.
pub const MIN_INTERVAL_SECS: u64 = 1;
/// Longest report interval, in seconds.
pub const MAX_INTERVAL_SECS: u64 = 300;
/// A record older than this many intervals reads as `unavailable`.
pub const STALE_AFTER_INTERVALS: u64 = 3;

/// Most replicas one report may carry. With the name and `detail` limits this
/// keeps a report below 64 KiB, the body limit of the route.
pub const MAX_REPLICAS_PER_REPORT: usize = 100;
/// Largest CPU capacity a report may claim, in millicores (4096 CPUs).
pub const MAX_CPU_MILLIS: u64 = 4_096_000;
/// Largest memory or disk capacity a report may claim: 1 PiB.
pub const MAX_BYTES: u64 = 1 << 50;
/// Longest `detail` of a replica, in bytes.
pub const MAX_DETAIL_BYTES: usize = 200;
/// Longest name of a project, environment or workload, in bytes.
const MAX_NAME_BYTES: usize = 63;

/// The key of the record of `node_id`.
pub fn node_key(node_id: u64) -> String {
    format!("{KEY_PREFIX}{node_id}")
}

/// What a node can offer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

/// One replica as its node sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaReport {
    pub project: String,
    pub environment: String,
    pub workload: String,
    pub revision: u64,
    pub replica_index: u32,
    pub state: ReplicaState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<Ipv4Addr>,
    /// Why the replica is in `state`, for people.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// A node's status report. The node sends no time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportNode {
    pub node_id: u64,
    /// The machine the node is registered to, as for `RenewLease`.
    pub instance_id: String,
    pub capacity: Capacity,
    pub replicas: Vec<ReplicaReport>,
}

/// Changes how often nodes report their status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetStatusInterval {
    pub secs: u64,
}

/// The record of one node under [`node_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    /// The lease clock when the report was applied.
    pub reported_at: u64,
    pub capacity: Capacity,
    pub replicas: Vec<ReplicaReport>,
}

/// Applies `ReportNode` against the mesh as the entry sees it, and returns
/// the record to store under [`node_key`]. Checked in this order:
///
/// - No mesh: `mesh_not_configured`.
/// - The lease of the node has expired: `mesh_lease_expired`, as for a
///   renewal.
/// - The instance id, by the rule `RenewLease` uses
///   ([`MeshState::claim_instance`]): another machine `forbidden`. A node whose record has no instance id
///   yet takes the one given, so `mesh` changes then and the caller must
///   store it. It does not otherwise.
/// - Too many replicas, or a name or `detail` over its limit: `bad_request`.
/// - Capacity over [`MAX_CPU_MILLIS`] or [`MAX_BYTES`], a name that is no DNS
///   label, or a `pod_ip` outside the container subnet the mesh recorded
///   for the node: `bad_request`. One bad entry refuses the whole report, so
///   a node cannot point a workload's address at anything but its own pods.
///
/// What a node reports is not trusted beyond that: readers ignore replicas
/// of workloads that do not exist, and placement treats capacity as
/// reported, never as measured.
///
/// On a rejection the caller discards `mesh`.
pub fn apply_report_node(
    mesh: Option<&mut MeshState>,
    cmd: ReportNode,
) -> Result<(NodeStatus, bool), Rejection> {
    let mesh = mesh.ok_or_else(|| {
        Rejection::new(error_code::MESH_NOT_CONFIGURED, "the cluster has no mesh")
    })?;
    // Unknown node and expired lease first, as `renew_lease` does.
    let node = mesh.nodes.get(&cmd.node_id).ok_or_else(|| {
        Rejection::new(
            error_code::MESH_NODE_REMOVED,
            format!("node {} is not in the mesh", cmd.node_id),
        )
    })?;
    if mesh.lease_expired(node) {
        return Err(Rejection::new(
            error_code::MESH_LEASE_EXPIRED,
            format!("the lease of node {} has expired", cmd.node_id),
        ));
    }
    let mesh_changed = mesh.claim_instance(cmd.node_id, &cmd.instance_id)?;
    check_sizes(&cmd.replicas)?;
    check_capacity(&cmd.capacity)?;
    check_replicas(&cmd.replicas)?;
    let subnet = mesh
        .nodes
        .get(&cmd.node_id)
        .and_then(|n| n.container_subnet);
    check_pod_ips(&cmd.replicas, subnet)?;
    let record = NodeStatus {
        reported_at: mesh.clock,
        capacity: cmd.capacity,
        replicas: cmd.replicas,
    };
    Ok((record, mesh_changed))
}

fn bad(message: String) -> Rejection {
    Rejection::new(error_code::BAD_REQUEST, message)
}

fn check_capacity(c: &Capacity) -> Result<(), Rejection> {
    if c.cpu_millis > MAX_CPU_MILLIS || c.memory_bytes > MAX_BYTES || c.disk_bytes > MAX_BYTES {
        return Err(bad("the capacity is over what a node can have".into()));
    }
    Ok(())
}

fn check_replicas(replicas: &[ReplicaReport]) -> Result<(), Rejection> {
    for r in replicas {
        for name in [&r.project, &r.environment, &r.workload] {
            validate_dns_label(name).map_err(|e| bad(format!("{name:?}: {e}")))?;
        }
    }
    Ok(())
}

fn check_pod_ips(replicas: &[ReplicaReport], subnet: Option<Ipv4Net>) -> Result<(), Rejection> {
    for ip in replicas.iter().filter_map(|r| r.pod_ip) {
        if !subnet.is_some_and(|net| net.contains(&ip)) {
            return Err(bad(format!(
                "pod address {ip} is outside the container subnet of the node"
            )));
        }
    }
    Ok(())
}

fn check_sizes(replicas: &[ReplicaReport]) -> Result<(), Rejection> {
    if replicas.len() > MAX_REPLICAS_PER_REPORT {
        return Err(bad(format!(
            "a report holds at most {MAX_REPLICAS_PER_REPORT} replicas"
        )));
    }
    for r in replicas {
        let names = [&r.project, &r.environment, &r.workload];
        if names.iter().any(|n| n.len() > MAX_NAME_BYTES) {
            return Err(bad(format!("a name is over {MAX_NAME_BYTES} bytes")));
        }
        if r.detail
            .as_ref()
            .is_some_and(|d| d.len() > MAX_DETAIL_BYTES)
        {
            return Err(bad(format!("detail is over {MAX_DETAIL_BYTES} bytes")));
        }
    }
    Ok(())
}

/// The report interval as stored under [`KEY_INTERVAL`], with the one it
/// replaced: reports sent under the old interval stay fresh for as long as
/// that interval says, so a reader cannot judge them by the new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalSetting {
    pub secs: u64,
    /// The interval before the last change; 0 when it never changed.
    #[serde(default)]
    pub previous_secs: u64,
    /// The lease clock at the last change.
    #[serde(default)]
    pub changed_at: u64,
}

impl Default for IntervalSetting {
    fn default() -> Self {
        Self {
            secs: DEFAULT_INTERVAL_SECS,
            previous_secs: 0,
            changed_at: 0,
        }
    }
}

impl IntervalSetting {
    /// The interval a reader judges reports by at `now` on the lease clock:
    /// the larger of the current and the previous one, until the reports of
    /// the previous one (up to [`STALE_AFTER_INTERVALS`] of its intervals
    /// old) have gone stale anyway.
    pub fn effective(&self, now: u64) -> u64 {
        let window = self.previous_secs.saturating_mul(STALE_AFTER_INTERVALS);
        if now.saturating_sub(self.changed_at) <= window {
            self.secs.max(self.previous_secs)
        } else {
            self.secs
        }
    }
}

/// Applies `SetStatusInterval` against the setting stored now and the lease
/// clock (0 without a mesh): the setting to store, or `bad_request` outside
/// [`MIN_INTERVAL_SECS`] to [`MAX_INTERVAL_SECS`]. Setting the interval it
/// already has changes nothing.
pub fn apply_set_status_interval(
    cmd: &SetStatusInterval,
    current: Option<IntervalSetting>,
    clock: u64,
) -> Result<IntervalSetting, Rejection> {
    if !(MIN_INTERVAL_SECS..=MAX_INTERVAL_SECS).contains(&cmd.secs) {
        return Err(Rejection::new(
            error_code::BAD_REQUEST,
            format!(
                "the status interval must be {MIN_INTERVAL_SECS} to {MAX_INTERVAL_SECS} seconds"
            ),
        ));
    }
    let current = current.unwrap_or_default();
    if current.secs == cmd.secs {
        return Ok(current);
    }
    Ok(IntervalSetting {
        secs: cmd.secs,
        previous_secs: current.secs,
        changed_at: clock,
    })
}

/// Whether only typed commands may write `key`.
pub fn is_reserved_key(key: &str) -> bool {
    key == KEY_INTERVAL || key.starts_with(KEY_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::{MeshConfig, NodeRecord};

    fn founder() -> NodeRecord {
        NodeRecord {
            node_id: 1,
            public_key: "pk1".into(),
            mesh_ip: "10.99.0.1".parse().unwrap(),
            endpoint: None,
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: "node-1".into(),
            lease_renewed_at: 0,
        }
    }

    /// Node 1 has the first container subnet, node 2 the next.
    fn mesh() -> MeshState {
        let config = MeshConfig {
            container_cidr: Some("10.200.0.0/16".parse().unwrap()),
            ..MeshConfig::default()
        };
        let mut mesh = MeshState::init(config, founder()).unwrap();
        let mut second = mesh.nodes[&1].clone();
        second.node_id = 2;
        second.container_subnet = Some("10.200.1.0/24".parse().unwrap());
        mesh.nodes.insert(2, second);
        mesh
    }

    fn report(ip: Option<&str>) -> ReportNode {
        ReportNode {
            node_id: 1,
            instance_id: "ab".repeat(16),
            capacity: Capacity::default(),
            replicas: vec![ReplicaReport {
                project: "shop".into(),
                environment: "staging".into(),
                workload: "web".into(),
                revision: 1,
                replica_index: 0,
                state: ReplicaState::Available,
                pod_ip: ip.map(|ip| ip.parse().unwrap()),
                detail: None,
            }],
        }
    }

    fn code(r: Result<(NodeStatus, bool), Rejection>) -> Option<String> {
        r.err().map(|r| r.code)
    }

    #[test]
    fn a_pod_address_must_be_in_the_container_subnet_of_the_reporting_node() {
        let mut mesh = mesh();
        let own = mesh.nodes[&1].container_subnet.unwrap();
        assert_eq!(own.to_string(), "10.200.0.0/24");
        for ok in [None, Some("10.200.0.2"), Some("10.200.0.254")] {
            assert_eq!(
                code(apply_report_node(Some(&mut mesh), report(ok))),
                None,
                "{ok:?}"
            );
        }
        for bad in [
            "127.0.0.1",
            "169.254.169.254",
            "10.200.1.2",
            "10.99.0.2",
            "192.168.1.1",
        ] {
            let got = code(apply_report_node(Some(&mut mesh), report(Some(bad))));
            assert_eq!(got.as_deref(), Some(error_code::BAD_REQUEST), "{bad}");
        }
        // One bad address refuses the whole report.
        let mut two = report(Some("10.200.0.2"));
        two.replicas
            .push(report(Some("127.0.0.1")).replicas.remove(0));
        assert!(apply_report_node(Some(&mut mesh), two).is_err());
        // A node without a subnet has no pod address.
        mesh.nodes.get_mut(&1).unwrap().container_subnet = None;
        assert!(apply_report_node(Some(&mut mesh), report(Some("10.200.0.2"))).is_err());
        assert!(apply_report_node(Some(&mut mesh), report(None)).is_ok());
    }

    #[test]
    fn names_are_dns_labels_and_capacity_is_bounded() {
        let mut mesh = mesh();
        for field in 0..3 {
            let mut r = report(None);
            let rep = &mut r.replicas[0];
            let [a, b, c] = [&mut rep.project, &mut rep.environment, &mut rep.workload];
            let slot = [a, b, c].into_iter().nth(field).unwrap();
            *slot = "Web_1".into();
            assert!(apply_report_node(Some(&mut mesh), r).is_err(), "{field}");
        }
        let at_max = Capacity {
            cpu_millis: MAX_CPU_MILLIS,
            memory_bytes: MAX_BYTES,
            disk_bytes: MAX_BYTES,
        };
        let mut r = report(None);
        r.capacity = at_max;
        assert!(apply_report_node(Some(&mut mesh), r).is_ok());
        for over in [
            Capacity {
                cpu_millis: MAX_CPU_MILLIS + 1,
                ..at_max
            },
            Capacity {
                memory_bytes: MAX_BYTES + 1,
                ..at_max
            },
            Capacity {
                disk_bytes: u64::MAX,
                ..at_max
            },
        ] {
            let mut r = report(None);
            r.capacity = over;
            assert!(apply_report_node(Some(&mut mesh), r).is_err(), "{over:?}");
        }
    }

    #[test]
    fn interval_bounds() {
        for secs in [MIN_INTERVAL_SECS, 5, MAX_INTERVAL_SECS] {
            let got = apply_set_status_interval(&SetStatusInterval { secs }, None, 0);
            assert_eq!(got.ok().map(|s| s.secs), Some(secs));
        }
        for secs in [0, MAX_INTERVAL_SECS + 1, u64::MAX] {
            let r = apply_set_status_interval(&SetStatusInterval { secs }, None, 0).unwrap_err();
            assert_eq!(r.code, error_code::BAD_REQUEST);
        }
    }

    #[test]
    fn a_changed_interval_keeps_the_old_one_for_readers_until_its_reports_are_stale() {
        let set = |secs, current, clock| {
            apply_set_status_interval(&SetStatusInterval { secs }, current, clock).unwrap()
        };
        let first = set(30, None, 100);
        assert_eq!(
            (first.secs, first.previous_secs, first.changed_at),
            (30, 5, 100)
        );
        // The same value again is no change.
        assert_eq!(set(30, Some(first), 500), first);
        // 30 to 5: reports sent under 30 stay good for three of those.
        let down = set(5, Some(first), 1_000);
        assert_eq!(down.effective(1_000), 30);
        assert_eq!(down.effective(1_090), 30);
        assert_eq!(down.effective(1_091), 5);
        // 5 to 30: the larger one applies at once anyway.
        assert_eq!(first.effective(100), 30);
        assert_eq!(first.effective(100 + 15), 30);
        assert_eq!(first.effective(100 + 16), 30);
        assert_eq!(IntervalSetting::default().effective(0), 5);
    }

    #[test]
    fn keys_are_reserved() {
        assert_eq!(node_key(12), "status/12");
        assert!(is_reserved_key("status/1"));
        assert!(is_reserved_key(KEY_INTERVAL));
        assert!(!is_reserved_key("statuses"));
        assert!(!is_reserved_key("config/containerd"));
    }

    #[test]
    fn oversize_reports_are_refused() {
        let replica = |detail: Option<String>, workload: &str| ReplicaReport {
            project: "p".into(),
            environment: "e".into(),
            workload: workload.into(),
            revision: 1,
            replica_index: 0,
            state: ReplicaState::Available,
            pod_ip: None,
            detail,
        };
        assert!(check_sizes(&[replica(None, "web")]).is_ok());
        let many = vec![replica(None, "web"); MAX_REPLICAS_PER_REPORT + 1];
        assert!(check_sizes(&many).is_err());
        assert!(check_sizes(&[replica(Some("x".repeat(MAX_DETAIL_BYTES + 1)), "web")]).is_err());
        assert!(check_sizes(&[replica(None, &"w".repeat(64))]).is_err());
    }
}
