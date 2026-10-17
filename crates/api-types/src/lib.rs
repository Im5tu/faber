//! Public API contract between the `fabercp` control plane and its clients.
//!
//! Every type here is serialised as JSON on `/api/v1`. Changing a field name
//! or shape is a breaking API change. Node-to-node (`/raft`) types are not
//! part of this crate.
//!
//! [`workload`] holds the workload model and the types of the project,
//! deploy and workload routes. [`name`] holds the DNS label rule that every
//! name follows.

use std::fmt;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

pub mod name;
pub mod workload;

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
pub use client::{ClientError, ClientTimeouts, FaberClient};

/// Port of the local (loopback) API listener when none is configured.
pub const DEFAULT_PORT: u16 = 7400;

/// Port of the peer (WireGuard) listener when none is configured.
pub const DEFAULT_PEER_PORT: u16 = 7401;

/// Header marking a request that one node has forwarded to another, so a
/// forwarded request is never forwarded again.
pub const FORWARDED_HEADER: &str = "x-faber-forwarded";

/// Machine-readable values for [`ErrorBody::error`].
pub mod error_code {
    pub const NOT_FOUND: &str = "not_found";
    pub const BAD_REQUEST: &str = "bad_request";
    pub const ALREADY_INITIALIZED: &str = "already_initialized";
    /// `POST /api/v1/cluster/join` on a node that has already joined (409).
    pub const ALREADY_JOINED: &str = "already_joined";
    pub const NOT_LEADER: &str = "not_leader";
    pub const NO_LEADER: &str = "no_leader";
    /// The leader cannot do this yet, for example promote a joining node
    /// that has not caught up with the log. Retry later (503).
    pub const NOT_READY: &str = "not_ready";
    /// A peer or the leader could not be reached (502).
    pub const PEER_UNREACHABLE: &str = "peer_unreachable";
    pub const INTERNAL: &str = "internal";
    /// The invite does not exist, or the joiner's details do not match it.
    pub const INVITE_INVALID: &str = "invite_invalid";
    /// The invite was already used by another node.
    pub const INVITE_USED: &str = "invite_used";
    pub const INVITE_EXPIRED: &str = "invite_expired";
    /// The cluster has no WireGuard mesh (`mesh` key absent).
    pub const MESH_NOT_CONFIGURED: &str = "mesh_not_configured";
    /// No existing node has a WireGuard endpoint, so a joiner could not reach any.
    pub const MESH_NO_ENDPOINTS: &str = "mesh_no_endpoints";
    /// Every host address in the mesh CIDR is taken.
    pub const MESH_EXHAUSTED: &str = "mesh_exhausted";
    /// The mesh has no such node: it was removed after its lease ran out,
    /// or another machine took its name (409).
    pub const MESH_NODE_REMOVED: &str = "mesh_node_removed";
    /// A lease renewal for a node the mesh still has, whose lease has run
    /// out. The node may not have been removed yet (409).
    pub const MESH_LEASE_EXPIRED: &str = "mesh_lease_expired";
    /// Rejected by the Host/Origin guard (403).
    pub const FORBIDDEN: &str = "forbidden";
    /// A project or an environment of that name already exists (409).
    pub const PROJECT_EXISTS: &str = "project_exists";
    /// The project or environment still holds workloads (409).
    pub const NOT_EMPTY: &str = "not_empty";
    /// A count or size limit of the workload model is reached: projects,
    /// environments, workloads, or stored bytes (409).
    pub const WORKLOADS_EXHAUSTED: &str = "workloads_exhausted";
    /// The cluster key is missing or wrong on the node that has to seal or
    /// open env values, or a stored data key cannot be opened with it (503).
    pub const KEY_UNAVAILABLE: &str = "key_unavailable";
    /// The node is up but cannot work: its raft core has stopped or its
    /// storage is closed (503 from `GET /api/v1/health`).
    pub const UNAVAILABLE: &str = "unavailable";
}

/// `GET /api/v1/health`
///
/// A healthy node answers 200 with `status` `"ok"`. A node whose raft core
/// has stopped, or whose storage is closed, answers 503 with `status`
/// `"unavailable"`; that body also carries the [`ErrorBody`] fields `error`
/// (`unavailable`) and `message`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: u64,
}

/// Raft role of a node, serialised in lowercase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Leader,
    Follower,
    Candidate,
    Learner,
    Shutdown,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Leader => "leader",
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Learner => "learner",
            Role::Shutdown => "shutdown",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<str> for Role {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Role {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// `GET /api/v1/cluster/status`, and the body of a successful `init` or `join`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_id: u64,
    /// This node is a voter in the membership config. A learner has not
    /// joined yet; it shows in `members` with `voter: false`.
    pub joined: bool,
    pub role: Role,
    pub term: u64,
    pub leader_id: Option<u64>,
    /// Peer listener address of the leader.
    pub leader_addr: Option<String>,
    pub members: Vec<MemberStatus>,
}

/// One entry in [`ClusterStatus::members`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberStatus {
    pub node_id: u64,
    /// Peer listener address.
    pub addr: String,
    /// `false` means the member is a learner.
    pub voter: bool,
    /// Highest log index the leader knows this member holds. `null` when the
    /// leader is unknown or unreachable.
    pub matched_index: Option<u64>,
    /// Leader's last log index minus `matched_index`. `null` like above.
    pub lag: Option<u64>,
    /// The member's name in the mesh. Absent outside a mesh and for a member
    /// with no mesh record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// `POST /api/v1/cluster/join`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Peer listener `ip:port` of any node already in the cluster.
    pub peer: String,
}

/// `POST /api/v1/mesh/invites` (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateInviteRequest {
    /// The name the new node will hold: a DNS label. A node that already
    /// holds it is replaced when the new node registers.
    pub name: String,
    /// The new node's public WireGuard endpoint, `ip` or `ip:port`, as the
    /// existing nodes will see it.
    pub endpoint: String,
    /// Invite lifetime; the server default applies when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

/// 201 body of `POST /api/v1/mesh/invites`.
///
/// `token` carries the new node's private key, so `Debug` redacts it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteResponse {
    pub token: String,
    pub node_id: u64,
    pub mesh_ip: Ipv4Addr,
    /// Unix seconds, by the leader's clock.
    pub expires_at: u64,
    /// The name the new node will hold.
    pub name: String,
    /// The node that holds `name` now and will be replaced when the new node
    /// registers. Absent when nobody holds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces_node_id: Option<u64>,
}

impl fmt::Debug for InviteResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InviteResponse")
            .field("token", &"<redacted>")
            .field("node_id", &self.node_id)
            .field("mesh_ip", &self.mesh_ip)
            .field("expires_at", &self.expires_at)
            .field("name", &self.name)
            .field("replaces_node_id", &self.replaces_node_id)
            .finish()
    }
}

/// `GET /api/v1/mesh/lease`, and the 200 body of `PUT /api/v1/mesh/lease`
/// (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseResponse {
    /// How long a node keeps its name and subnet without renewing, in seconds.
    pub lease_secs: u64,
}

/// `PUT /api/v1/mesh/lease` (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetLeaseRequest {
    pub lease_secs: u64,
}

/// `GET /api/v1/mesh/status-interval`, and the 200 body of `PUT` on it
/// (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusIntervalResponse {
    /// Seconds between two status reports of a node.
    pub interval_secs: u64,
}

/// `PUT /api/v1/mesh/status-interval` (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetStatusIntervalRequest {
    pub interval_secs: u64,
}

/// JSON body of every non-2xx response. `leader_id`/`leader_addr` are set on
/// `not_leader` errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub leader_id: Option<u64>,
    #[serde(default)]
    pub leader_addr: Option<String>,
}

impl ErrorBody {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: None,
            leader_id: None,
            leader_addr: None,
        }
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}
