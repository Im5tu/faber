//! Node status commands: what each node reports about its replicas and its
//! capacity, and how often it does.
//!
//! A report replaces the whole record of its node under `status/<node_id>`.
//! Apply stamps it with the lease clock (`MeshState::clock`), never with a
//! time the node sent. A report writes no other key: the mesh record and the
//! environment records stay as they are, so no generation moves and the
//! reconcile steps that compare their own desired values see no change.
//!
//! Nothing writes when a node goes quiet. A reader decides a record is stale
//! by comparing `reported_at` with the clock ([`crate::status::view`]).

use std::net::Ipv4Addr;

use faber_api_types::error_code;
use faber_api_types::workload::ReplicaState;
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
/// keeps a report below `MAX_DEPLOY_REQUEST_BYTES`.
pub const MAX_REPLICAS_PER_REPORT: usize = 200;
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
/// - Unknown node: `mesh_node_removed`.
/// - The record has an `instance_id` and it is not the one given:
///   `forbidden`. A record without one (the founder's) accepts any: unlike
///   `RenewLease`, a report does not adopt it.
/// - Too many replicas, or a name or `detail` over its limit: `bad_request`.
pub fn apply_report_node(
    mesh: Option<&MeshState>,
    cmd: ReportNode,
) -> Result<NodeStatus, Rejection> {
    let mesh = mesh.ok_or_else(|| {
        Rejection::new(error_code::MESH_NOT_CONFIGURED, "the cluster has no mesh")
    })?;
    let node = mesh.nodes.get(&cmd.node_id).ok_or_else(|| {
        Rejection::new(
            error_code::MESH_NODE_REMOVED,
            format!("node {} is not in the mesh", cmd.node_id),
        )
    })?;
    if node
        .instance_id
        .as_deref()
        .is_some_and(|held| held != cmd.instance_id)
    {
        return Err(Rejection::new(
            error_code::FORBIDDEN,
            format!("node {} is registered to another machine", cmd.node_id),
        ));
    }
    check_sizes(&cmd.replicas)?;
    Ok(NodeStatus {
        reported_at: mesh.clock,
        capacity: cmd.capacity,
        replicas: cmd.replicas,
    })
}

fn check_sizes(replicas: &[ReplicaReport]) -> Result<(), Rejection> {
    let bad = |m: String| Rejection::new(error_code::BAD_REQUEST, m);
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

/// Applies `SetStatusInterval`: the seconds to store, or `bad_request`
/// outside [`MIN_INTERVAL_SECS`] to [`MAX_INTERVAL_SECS`].
pub fn apply_set_status_interval(cmd: &SetStatusInterval) -> Result<u64, Rejection> {
    if (MIN_INTERVAL_SECS..=MAX_INTERVAL_SECS).contains(&cmd.secs) {
        Ok(cmd.secs)
    } else {
        Err(Rejection::new(
            error_code::BAD_REQUEST,
            format!(
                "the status interval must be {MIN_INTERVAL_SECS} to {MAX_INTERVAL_SECS} seconds"
            ),
        ))
    }
}

/// Whether only typed commands may write `key`.
pub fn is_reserved_key(key: &str) -> bool {
    key == KEY_INTERVAL || key.starts_with(KEY_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_bounds() {
        for secs in [MIN_INTERVAL_SECS, 5, MAX_INTERVAL_SECS] {
            let got = apply_set_status_interval(&SetStatusInterval { secs });
            assert_eq!(got.ok(), Some(secs));
        }
        for secs in [0, MAX_INTERVAL_SECS + 1, u64::MAX] {
            let r = apply_set_status_interval(&SetStatusInterval { secs }).unwrap_err();
            assert_eq!(r.code, error_code::BAD_REQUEST);
        }
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
