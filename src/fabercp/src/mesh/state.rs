//! Replicated mesh records, stored as JSON in `sm_data`.
//!
//! Everything here is pure: `apply` calls these functions with the time the
//! leader stamped on the command (`at`, Unix seconds) and never reads the
//! clock itself, so every replica computes the same result.
//!
//! Invites expire by `at`. Leases do not: they run on [`MeshState::clock`],
//! a logical clock that only `AdvanceClock` moves, so time the cluster
//! spent without a leader counts against no lease.
//!
//! A node holds its name, mesh IP and container subnet from registration
//! until its record is deleted, by `RemoveNode` after its lease expired or
//! by a new node registering under its name. Either way the node has to be
//! out of the raft membership first. Node names are unique.
//!

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use faber_api_types::{DEFAULT_PEER_PORT, error_code};
use faber_node_protocol::{MAX_CONTAINER_SUBNET_PREFIX, check_container_subnet};
use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

/// `sm_data` key holding the JSON [`MeshState`].
pub const KEY_MESH: &str = "mesh";
/// Keys that only the typed mesh commands may write; `Request::Set` rejects them.
pub const RESERVED_KEYS: &[&str] = &[KEY_MESH];

/// Mesh CIDR used when `--mesh-cidr` is not given: `10.99.0.0/16`.
pub const DEFAULT_MESH_CIDR: Ipv4Net = match Ipv4Net::new(Ipv4Addr::new(10, 99, 0, 0), 16) {
    Ok(n) => n,
    Err(_) => panic!("valid default mesh cidr"),
};
/// Container CIDR used when `--container-cidr` is not given: `10.200.0.0/16`.
pub const DEFAULT_CONTAINER_CIDR: Ipv4Net = match Ipv4Net::new(Ipv4Addr::new(10, 200, 0, 0), 16) {
    Ok(n) => n,
    Err(_) => panic!("valid default container cidr"),
};
/// Service (virtual IP) range used when no other is given for a new mesh.
pub const DEFAULT_SERVICE_CIDR: Ipv4Net = match Ipv4Net::new(Ipv4Addr::new(10, 201, 0, 0), 16) {
    Ok(n) => n,
    Err(_) => panic!("valid default service cidr"),
};
/// Longest prefix a service range may have: two host addresses.
pub const MAX_SERVICE_CIDR_PREFIX: u8 = 30;
/// Prefix length of the container subnet each node gets.
pub const DEFAULT_CONTAINER_SUBNET_PREFIX: u8 = 24;
/// WireGuard UDP listen port used when `--wg-port` is not given.
pub const DEFAULT_WG_PORT: u16 = 51820;
/// Invite lifetime when the request gives none.
pub const DEFAULT_INVITE_TTL_SECS: u64 = 60 * 60;
/// Longest invite lifetime accepted.
pub const MAX_INVITE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
/// `PersistentKeepalive` for every wg peer.
pub const WG_KEEPALIVE_SECS: u16 = 25;
/// Lease duration a new mesh starts with: 8 hours.
pub const DEFAULT_LEASE_SECS: u64 = 28_800;
/// Shortest lease duration `SetLease` accepts.
pub const MIN_LEASE_SECS: u64 = 300;
/// Longest lease duration `SetLease` accepts: 30 days.
pub const MAX_LEASE_SECS: u64 = 2_592_000;
/// The founder's name when `--node-name` is not given.
pub const FOUNDER_DEFAULT_NAME: &str = "node-1";

/// Cluster-wide mesh settings, fixed at `InitCluster`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshConfig {
    /// Every node's mesh IP is a host address in this network.
    pub cidr: Ipv4Net,
    /// TCP port of the peer listener on each mesh IP.
    pub peer_port: u16,
    /// WireGuard UDP listen port.
    pub wg_port: u16,
    /// Every node's container subnet is inside this network. `None` for a
    /// mesh created without a container range, or before containers were
    /// networked: then no node gets a subnet.
    #[serde(default)]
    pub container_cidr: Option<Ipv4Net>,
    /// Prefix length of each node's container subnet. Stored here, not read
    /// from a constant, so replaying the log always allocates the same way.
    #[serde(default = "default_container_subnet_prefix")]
    pub container_subnet_prefix: u8,
    /// Every service's virtual IP is a host address in this network. `None`
    /// for a mesh created without one: then no service gets an address.
    /// Fixed once the mesh exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_cidr: Option<Ipv4Net>,
}

fn default_container_subnet_prefix() -> u8 {
    DEFAULT_CONTAINER_SUBNET_PREFIX
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            cidr: DEFAULT_MESH_CIDR,
            peer_port: DEFAULT_PEER_PORT,
            wg_port: DEFAULT_WG_PORT,
            container_cidr: None,
            container_subnet_prefix: DEFAULT_CONTAINER_SUBNET_PREFIX,
            service_cidr: None,
        }
    }
}

/// A mesh member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: u64,
    /// WireGuard public key, standard base64 (as `wg pubkey` prints it).
    pub public_key: String,
    pub mesh_ip: Ipv4Addr,
    /// Public WireGuard endpoint, `ip:port`. `None` means peers wait for this
    /// node to connect to them.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// The invite this node joined with; `None` for the founder.
    #[serde(default)]
    pub invite_id: Option<String>,
    /// The machine that registered with the invite: the random id it made
    /// when it created its identity from the token. `None` for the founder
    /// and for nodes registered before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    /// The container subnet raft allocated to this node: the founder's in
    /// `InitCluster`, every other node's in `RegisterNode`. `None` when the
    /// mesh has no container range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_subnet: Option<Ipv4Net>,
    /// The name this node holds a lease on. Empty for nodes registered
    /// before names existed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// [`MeshState::clock`] when the lease was last renewed.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub lease_renewed_at: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// An outstanding invite. Its key, IP and name are reserved until it is
/// used, expires or is replaced by a newer invite for the same name. It
/// counts against the container range's capacity unless it will take over
/// the subnet of the node that holds its name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteRecord {
    /// 128-bit random, lowercase hex.
    pub invite_id: String,
    pub node_id: u64,
    pub public_key: String,
    pub mesh_ip: Ipv4Addr,
    /// The new node's public WireGuard endpoint, from `mesh invite --wg-endpoint`.
    pub endpoint: String,
    /// Unix seconds, by the leader's clock. Expired once `at >= expires_at`.
    pub expires_at: u64,
    /// The name the new node will hold, from `mesh invite --name`. Empty for
    /// invites created before names existed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

/// The whole replicated mesh, stored under [`KEY_MESH`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshState {
    pub config: MeshConfig,
    pub nodes: BTreeMap<u64, NodeRecord>,
    /// Keyed by `invite_id`.
    pub invites: BTreeMap<String, InviteRecord>,
    /// Node ID the next invite gets.
    pub next_node_id: u64,
    /// Logical lease clock, in seconds. Only `AdvanceClock` moves it; it
    /// never follows the wall clock.
    #[serde(default)]
    pub clock: u64,
    /// How long a lease lasts without a renewal, in seconds of `clock`.
    #[serde(default = "default_lease_secs")]
    pub lease_secs: u64,
}

fn default_lease_secs() -> u64 {
    DEFAULT_LEASE_SECS
}

/// What `CreateInvite` reserved; becomes `Response::Invited`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Allocation {
    pub node_id: u64,
    pub mesh_ip: Ipv4Addr,
    pub expires_at: u64,
    /// The name the invite carries.
    pub name: String,
    /// The node that holds `name` now and will be replaced when the invite
    /// registers, if any.
    pub replaces: Option<u64>,
}

/// A mesh command that `apply` refused. It still advances `last_applied`; it
/// is returned as `Response::Rejected`, never as a `StorageError`.
///
/// `code` is one of `faber_api_types::error_code`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Rejection {
    pub code: String,
    pub message: String,
}

impl Rejection {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

impl MeshState {
    /// The state `InitCluster` writes: `founder` as the only node, no invites,
    /// `next_node_id = founder.node_id + 1`.
    ///
    /// Rejects with `bad_request` when `founder.mesh_ip` is not a host
    /// address of `config.cidr` (the network and broadcast addresses are not
    /// host addresses). `apply` itself rejects `InitCluster` with
    /// `already_initialized` when [`KEY_MESH`] already exists.
    ///
    /// When `config.container_cidr` is set, also rejects with `bad_request`
    /// when `config.container_subnet_prefix` is above 30 or shorter than the
    /// range's own prefix, when the range has host bits set, or when it
    /// overlaps `config.cidr`.
    ///
    /// When `config.service_cidr` is set, also rejects with `bad_request`
    /// when the range has host bits set, is longer than
    /// [`MAX_SERVICE_CIDR_PREFIX`], or overlaps `config.cidr` or
    /// `config.container_cidr`.
    ///
    /// The founder's container subnet is allocated here: the first subnet of
    /// the range, or `None` when there is no range. Whatever
    /// `founder.container_subnet` the proposer sent is overwritten.
    ///
    /// An empty `founder.name` becomes [`FOUNDER_DEFAULT_NAME`]; any other
    /// name must pass [`MeshState::validate_name`] or the command is
    /// rejected with `bad_request`. The founder's lease starts at clock 0,
    /// whatever `founder.lease_renewed_at` the proposer sent.
    pub fn init(config: MeshConfig, mut founder: NodeRecord) -> Result<Self, Rejection> {
        if !is_host_addr(config.cidr, founder.mesh_ip) {
            return Err(Rejection::new(
                error_code::BAD_REQUEST,
                format!(
                    "founder mesh IP {} is not a host address of {}",
                    founder.mesh_ip, config.cidr
                ),
            ));
        }
        let next_node_id = founder.node_id.checked_add(1).ok_or_else(|| {
            Rejection::new(error_code::BAD_REQUEST, "founder node ID is too large")
        })?;
        if founder.name.is_empty() {
            founder.name = FOUNDER_DEFAULT_NAME.to_string();
        } else {
            Self::validate_name(&founder.name)?;
        }
        founder.container_subnet = match config.container_cidr {
            Some(range) => Some(first_container_subnet(&config, range)?),
            None => None,
        };
        if let Some(range) = config.service_cidr {
            check_service_range(&config, range)?;
        }
        founder.lease_renewed_at = 0;
        Ok(Self {
            config,
            nodes: BTreeMap::from([(founder.node_id, founder)]),
            invites: BTreeMap::new(),
            next_node_id,
            clock: 0,
            lease_secs: DEFAULT_LEASE_SECS,
        })
    }

    /// The node that holds `name`, whether or not its lease has expired: a
    /// name is held until `remove_node` or a takeover deletes the record.
    /// The empty name of a record stored before names existed is held by
    /// nobody.
    fn name_holder(&self, name: &str) -> Option<&NodeRecord> {
        if name.is_empty() {
            return None;
        }
        self.nodes.values().find(|n| n.name == name)
    }

    /// Lowest host address of `config.cidr` held by no node and no invite.
    /// A node whose lease has expired holds its address until it is
    /// removed. Call after pruning expired invites.
    fn free_ip(&self) -> Option<Ipv4Addr> {
        let taken: BTreeSet<Ipv4Addr> = self
            .nodes
            .values()
            .map(|n| n.mesh_ip)
            .chain(self.invites.values().map(|i| i.mesh_ip))
            .collect();
        self.config.cidr.hosts().find(|ip| !taken.contains(ip))
    }

    /// How many node subnets `config.container_cidr` holds, or `None` when
    /// there is no range. A /8 range cut into /30s holds over four million,
    /// hence `u64`.
    fn container_subnet_count(&self) -> Option<u64> {
        let range = self.config.container_cidr?;
        let prefix = self.config.container_subnet_prefix;
        Some(match prefix.checked_sub(range.prefix_len()) {
            Some(bits) if bits <= 32 => 1u64 << bits,
            _ => 0,
        })
    }

    /// The subnet a newly registered node gets: the lowest subnet of
    /// `config.container_cidr` (by network address) that no node holds, or
    /// `None` when there is no range. Every subnet taken: `mesh_exhausted`.
    /// A node whose lease has expired holds its subnet until it is removed.
    fn free_container_subnet(&self) -> Result<Option<Ipv4Net>, Rejection> {
        let Some(range) = self.config.container_cidr else {
            return Ok(None);
        };
        let prefix = self.config.container_subnet_prefix;
        let taken: BTreeSet<Ipv4Net> = self
            .nodes
            .values()
            .filter_map(|n| n.container_subnet)
            .collect();
        range
            .subnets(prefix)
            .ok()
            .and_then(|mut subnets| subnets.find(|s| !taken.contains(s)))
            .map(Some)
            .ok_or_else(|| {
                Rejection::new(
                    error_code::MESH_EXHAUSTED,
                    format!("no free /{prefix} subnet in container range {range}"),
                )
            })
    }

    /// Applies `CreateInvite`, in this order:
    /// 1. Drops every invite with `at >= expires_at`.
    /// 2. Rejects with `bad_request` when `ttl_secs` is 0 or above
    ///    [`MAX_INVITE_TTL_SECS`], when `name` fails
    ///    [`MeshState::validate_name`] (names are required, so an empty one
    ///    fails), or when `invite_id` is already an invite or was used by a
    ///    node.
    /// 3. Drops the live invite that carries the same `name`, if there is
    ///    one: a newer invite for a name replaces the older one, so a lost
    ///    token cannot block a name. What the older invite reserved is free
    ///    for the steps below.
    /// 4. Rejects with `bad_request` when `public_key` belongs to a node or
    ///    a live invite.
    /// 5. With a container range configured, rejects with `mesh_exhausted`
    ///    when the nodes holding a subnet, plus the live invites that will
    ///    not take over a subnet, plus this invite if it will not, number
    ///    more than the range has subnets. An invite takes over a subnet
    ///    when a node holds its name and a subnet.
    /// 6. Takes `node_id = next_node_id` and increments `next_node_id`.
    /// 7. Takes the lowest host address of `config.cidr` (skipping network
    ///    and broadcast) not held by a node or a live invite. Addresses of
    ///    expired invites are reused. None left: `mesh_exhausted`.
    /// 8. Stores the invite with `expires_at = at + ttl_secs`, `endpoint`
    ///    and `name`.
    ///
    /// The answer's `replaces` is the node that holds `name` now, expired
    /// or not: `RegisterNode` will delete it and hand its subnet to the new
    /// node. Creating the invite changes no node.
    ///
    /// A rejection changes nothing beyond the prune in step 1: it never
    /// consumes a node ID and it keeps the older invite of step 3.
    ///
    /// The invite holds no subnet of its own; step 5 only keeps one free for
    /// it, so `RegisterNode` cannot run out. The check has to be here: a
    /// joiner retries `mesh_exhausted` forever. If the node an invite would
    /// replace is removed first, its subnet becomes free in the same step
    /// as the invite starts to count, so the total never passes the limit.
    ///
    /// Whether any node has an endpoint (`mesh_no_endpoints`) is checked by
    /// the service before proposing, not here.
    pub fn create_invite(
        &mut self,
        invite_id: String,
        public_key: String,
        endpoint: String,
        name: String,
        ttl_secs: u64,
        at: u64,
    ) -> Result<Allocation, Rejection> {
        self.invites.retain(|_, i| at < i.expires_at);

        let bad = |msg: &str| Err(Rejection::new(error_code::BAD_REQUEST, msg));
        if ttl_secs == 0 || ttl_secs > MAX_INVITE_TTL_SECS {
            return bad(&format!(
                "ttl_secs must be between 1 and {MAX_INVITE_TTL_SECS}"
            ));
        }
        let Some(expires_at) = at.checked_add(ttl_secs) else {
            return bad("invite expiry overflows");
        };
        Self::validate_name(&name)?;
        if self.invites.contains_key(&invite_id)
            || self
                .nodes
                .values()
                .any(|n| n.invite_id.as_deref() == Some(invite_id.as_str()))
        {
            return bad("invite_id is already in use");
        }

        // Take the older invites for this name out before allocating, and
        // put them back if the new invite is rejected.
        let superseded: Vec<String> = self
            .invites
            .values()
            .filter(|i| i.name == name)
            .map(|i| i.invite_id.clone())
            .collect();
        let superseded: Vec<InviteRecord> = superseded
            .iter()
            .filter_map(|id| self.invites.remove(id))
            .collect();
        let result = self.reserve_invite(invite_id, public_key, endpoint, name, expires_at);
        if result.is_err() {
            self.invites
                .extend(superseded.into_iter().map(|i| (i.invite_id.clone(), i)));
        }
        result
    }

    /// Steps 4 to 8 of [`MeshState::create_invite`]. A rejection changes
    /// nothing.
    fn reserve_invite(
        &mut self,
        invite_id: String,
        public_key: String,
        endpoint: String,
        name: String,
        expires_at: u64,
    ) -> Result<Allocation, Rejection> {
        let bad = |msg: &str| Err(Rejection::new(error_code::BAD_REQUEST, msg));
        if self.nodes.values().any(|n| n.public_key == public_key)
            || self.invites.values().any(|i| i.public_key == public_key)
        {
            return bad("public_key is already in use");
        }
        let Some(next_node_id) = self.next_node_id.checked_add(1) else {
            return bad("node IDs exhausted");
        };
        let replaces = self.name_holder(&name).map(|n| n.node_id);
        if let (Some(range), Some(total)) =
            (self.config.container_cidr, self.container_subnet_count())
        {
            // Names whose invite will take over a subnet, not need a new one.
            let reusable: BTreeSet<&str> = self
                .nodes
                .values()
                .filter(|n| n.container_subnet.is_some() && !n.name.is_empty())
                .map(|n| n.name.as_str())
                .collect();
            let held = self
                .nodes
                .values()
                .filter(|n| n.container_subnet.is_some())
                .count();
            let waiting = self
                .invites
                .values()
                .filter(|i| !reusable.contains(i.name.as_str()))
                .count();
            let spoken_for = (held as u64)
                .saturating_add(waiting as u64)
                .saturating_add(u64::from(!reusable.contains(name.as_str())));
            if spoken_for > total {
                return Err(Rejection::new(
                    error_code::MESH_EXHAUSTED,
                    format!(
                        "container range {range} is full: every /{} subnet ({total}) \
                         is held by a node or reserved by a live invite",
                        self.config.container_subnet_prefix
                    ),
                ));
            }
        }
        let Some(mesh_ip) = self.free_ip() else {
            return Err(Rejection::new(
                error_code::MESH_EXHAUSTED,
                format!("no free host address in {}", self.config.cidr),
            ));
        };

        let node_id = self.next_node_id;
        self.next_node_id = next_node_id;
        self.invites.insert(
            invite_id.clone(),
            InviteRecord {
                invite_id,
                node_id,
                public_key,
                mesh_ip,
                endpoint,
                expires_at,
                name: name.clone(),
            },
        );
        Ok(Allocation {
            node_id,
            mesh_ip,
            expires_at,
            name,
            replaces,
        })
    }

    /// Applies `RegisterNode`: moves the invite into `nodes` as
    /// `NodeRecord{endpoint: Some(invite.endpoint), invite_id: Some(..),
    /// instance_id: Some(..)}`. An invite is used once, by one machine:
    /// `instance_id` names the machine that registers.
    ///
    /// The node takes the invite's name, node id and mesh IP, and its lease
    /// starts at the current `clock`.
    ///
    /// When no node holds the name, the node gets the lowest subnet of
    /// `config.container_cidr` that no node holds, or none when there is no
    /// range. When another node holds the name, this is a takeover: that
    /// node's record is deleted and its subnet goes to the new node in the
    /// same change, so no two nodes ever hold one subnet. An old record
    /// without a subnet is deleted and the new node allocates as usual.
    /// Whether the old node's lease has expired makes no difference.
    ///
    /// A retry returns before anything is allocated or taken over, so the
    /// node keeps the subnet it was given and a retry never removes a
    /// second node.
    ///
    /// - A node already registered from this invite by the same machine
    ///   (same `instance_id`, `node_id` and `endpoint`): `Ok` (the retry of a
    ///   registration whose answer was lost).
    /// - A node registered from this invite by another machine, by a machine
    ///   that gave no `instance_id`, or with other details: `invite_used`.
    /// - No such invite: `invite_invalid`.
    /// - `at >= expires_at`: `invite_expired`.
    /// - `node_id` or `endpoint` differs from the invite: `invite_invalid`.
    /// - The node holding the name is in `members`: `not_ready`. Raft
    ///   membership goes first; the joiner retries until the leader has
    ///   taken the old node out.
    /// - A container range is configured, there is no subnet to take over
    ///   and every subnet is held: `mesh_exhausted`. The invite stays.
    ///   `create_invite` keeps a subnet free for each live invite that
    ///   needs one, so this does not happen in practice.
    ///
    /// An empty `instance_id` (a command logged before the field existed)
    /// registers the node without one, and never counts as a retry. An
    /// invite stored before names existed has an empty name, which no node
    /// holds, so it takes over nothing.
    ///
    /// `members` is the set of node ids, voters and learners, in the last
    /// applied raft membership.
    pub fn register_node(
        &mut self,
        invite_id: &str,
        node_id: u64,
        endpoint: &str,
        instance_id: &str,
        at: u64,
        members: &BTreeSet<u64>,
    ) -> Result<(), Rejection> {
        if let Some(n) = self
            .nodes
            .values()
            .find(|n| n.invite_id.as_deref() == Some(invite_id))
        {
            let same_machine = !instance_id.is_empty()
                && n.instance_id.as_deref() == Some(instance_id)
                && n.node_id == node_id
                && n.endpoint.as_deref() == Some(endpoint);
            if same_machine {
                return Ok(());
            }
            return Err(Rejection::new(
                error_code::INVITE_USED,
                "invite already used by another node",
            ));
        }
        let invalid = || Rejection::new(error_code::INVITE_INVALID, "invite is not valid");
        let invite = self.invites.get(invite_id).ok_or_else(invalid)?;
        if at >= invite.expires_at {
            return Err(Rejection::new(
                error_code::INVITE_EXPIRED,
                "invite has expired",
            ));
        }
        if invite.node_id != node_id
            || invite.endpoint != endpoint
            || self.nodes.contains_key(&node_id)
        {
            return Err(invalid());
        }
        // The retry check above has already returned for this node, so the
        // holder is always another one.
        let holder = self
            .name_holder(&invite.name)
            .map(|n| (n.node_id, n.container_subnet));
        // Decide everything before the invite is removed, so a rejection
        // changes nothing.
        let container_subnet = match holder {
            Some((old, _)) if members.contains(&old) => {
                return Err(Rejection::new(
                    error_code::NOT_READY,
                    format!(
                        "node {old} holds the name {} and is still in the raft membership",
                        invite.name
                    ),
                ));
            }
            Some((_, Some(subnet))) => Some(subnet),
            _ => self.free_container_subnet()?,
        };
        let invite = self.invites.remove(invite_id).ok_or_else(invalid)?;
        // The old record goes in the same change that gives its subnet to
        // the new one: no state exists where two nodes hold it.
        if let Some((old, _)) = holder {
            self.nodes.remove(&old);
        }
        self.nodes.insert(
            node_id,
            NodeRecord {
                node_id,
                public_key: invite.public_key,
                mesh_ip: invite.mesh_ip,
                endpoint: Some(invite.endpoint),
                invite_id: Some(invite.invite_id),
                instance_id: Some(instance_id.to_string()).filter(|i| !i.is_empty()),
                container_subnet,
                name: invite.name,
                lease_renewed_at: self.clock,
            },
        );
        Ok(())
    }

    /// Applies `SetEndpoint`: replaces `nodes[node_id].endpoint`. Unknown
    /// node: `not_found`.
    pub fn set_endpoint(&mut self, node_id: u64, endpoint: String) -> Result<(), Rejection> {
        let node = self.nodes.get_mut(&node_id).ok_or_else(|| {
            Rejection::new(error_code::NOT_FOUND, format!("node {node_id} not found"))
        })?;
        node.endpoint = Some(endpoint);
        Ok(())
    }

    /// Applies `AdvanceClock`: moves the lease clock forward by
    /// `elapsed_secs`, but by no more than a tenth of `lease_secs` (and at
    /// least one second of room). The clamp bounds how far one command can
    /// move the clock, so a leader that measured a huge gap cannot expire
    /// every lease at once. The clock saturates at `u64::MAX`.
    ///
    /// Never rejects, and changes nothing but `clock`.
    pub fn advance_clock(&mut self, elapsed_secs: u64) -> Result<(), Rejection> {
        let step = elapsed_secs.min((self.lease_secs / 10).max(1));
        self.clock = self.clock.saturating_add(step);
        Ok(())
    }

    /// Applies `RenewLease`: sets `lease_renewed_at` of `node_id` to the
    /// current `clock`, for the machine `instance_id`. Checked in this
    /// order:
    ///
    /// - Unknown node: `mesh_node_removed`. A record goes only after its
    ///   node has left the raft membership ([`Self::remove_node`],
    ///   [`Self::register_node`]), so the node may take this answer as
    ///   proof that it was removed.
    /// - The lease has already expired: `mesh_lease_expired`. Expiry is
    ///   sticky: the leader may be removing the node, and a late renewal
    ///   that revived it would leave it in the mesh but out of raft. The
    ///   node is still in the mesh and may still be a voter, so this is no
    ///   proof of removal.
    /// - The record has an `instance_id` and it is not the one given:
    ///   `forbidden`.
    ///
    /// A record without an `instance_id` (the founder's is created without
    /// one) adopts the one given, unless that is empty.
    pub fn renew_lease(&mut self, node_id: u64, instance_id: &str) -> Result<(), Rejection> {
        let removed = |msg: String| Rejection::new(error_code::MESH_NODE_REMOVED, msg);
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| removed(format!("node {node_id} is not in the mesh")))?;
        if self.lease_expired(node) {
            return Err(Rejection::new(
                error_code::MESH_LEASE_EXPIRED,
                format!("the lease of node {node_id} has expired"),
            ));
        }
        let _ = self.claim_instance(node_id, instance_id)?;
        let clock = self.clock;
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.lease_renewed_at = clock;
        }
        Ok(())
    }

    /// The instance id rule of every command a node sends for itself
    /// (`RenewLease`, `ReportNode`): a record that holds an `instance_id`
    /// accepts only that one (`forbidden` otherwise). A record without one
    /// (the founder's is created without one) adopts the one given, unless
    /// that is empty. Nothing changes when it fails. A node that is not in
    /// the mesh is `mesh_node_removed`. Answers whether it recorded an
    /// instance id, that is whether the record changed.
    pub fn claim_instance(&mut self, node_id: u64, instance_id: &str) -> Result<bool, Rejection> {
        let node = self.nodes.get_mut(&node_id).ok_or_else(|| {
            Rejection::new(
                error_code::MESH_NODE_REMOVED,
                format!("node {node_id} is not in the mesh"),
            )
        })?;
        match node.instance_id.as_deref() {
            Some(held) if held != instance_id => Err(Rejection::new(
                error_code::FORBIDDEN,
                format!("node {node_id} is registered to another machine"),
            )),
            Some(_) => Ok(false),
            None if instance_id.is_empty() => Ok(false),
            None => {
                node.instance_id = Some(instance_id.to_string());
                Ok(true)
            }
        }
    }

    /// Applies `RemoveNode`: deletes the record of `node_id`, which frees
    /// its mesh IP, container subnet and name.
    ///
    /// - Unknown node: `Ok`, nothing changes (the retry of a new leader).
    /// - The lease has not expired: `not_ready`.
    /// - `node_id` is in `members`: `not_ready`. Raft membership goes first:
    ///   dropping the mesh record first would cut WireGuard to a node whose
    ///   ack may be needed to commit its own removal.
    ///
    /// `members` is the set of node ids, voters and learners, in the last
    /// applied raft membership.
    pub fn remove_node(&mut self, node_id: u64, members: &BTreeSet<u64>) -> Result<(), Rejection> {
        let Some(node) = self.nodes.get(&node_id) else {
            return Ok(());
        };
        let not_ready = |msg: String| Err(Rejection::new(error_code::NOT_READY, msg));
        if !self.lease_expired(node) {
            return not_ready(format!("the lease of node {node_id} has not expired"));
        }
        if members.contains(&node_id) {
            return not_ready(format!("node {node_id} is still in the raft membership"));
        }
        self.nodes.remove(&node_id);
        Ok(())
    }

    /// Applies `SetLease`: sets `lease_secs` and restarts the lease of every
    /// node that had not expired under the old duration, so that shortening
    /// the lease cannot expire the whole cluster in one step.
    ///
    /// A node that had already expired keeps its `lease_renewed_at`: expiry
    /// is sticky, and the leader may be removing the node. Lengthening the
    /// lease can still un-expire such a node by arithmetic (`clock -
    /// lease_renewed_at` falls below the new duration). That only delays
    /// its removal until the longer lease runs out.
    ///
    /// `secs` outside [`MIN_LEASE_SECS`]`..=`[`MAX_LEASE_SECS`]:
    /// `bad_request`.
    pub fn set_lease(&mut self, secs: u64) -> Result<(), Rejection> {
        if !(MIN_LEASE_SECS..=MAX_LEASE_SECS).contains(&secs) {
            return Err(Rejection::new(
                error_code::BAD_REQUEST,
                format!("lease must be between {MIN_LEASE_SECS} and {MAX_LEASE_SECS} seconds"),
            ));
        }
        // Judge expiry by the old duration, before it changes.
        let (clock, old_secs) = (self.clock, self.lease_secs);
        for node in self.nodes.values_mut() {
            if clock.saturating_sub(node.lease_renewed_at) < old_secs {
                node.lease_renewed_at = clock;
            }
        }
        self.lease_secs = secs;
        Ok(())
    }

    /// Whether the lease of `node` has run out: `clock - lease_renewed_at >=
    /// lease_secs`. A renewal stamped after the clock counts as just made.
    pub fn lease_expired(&self, node: &NodeRecord) -> bool {
        self.clock.saturating_sub(node.lease_renewed_at) >= self.lease_secs
    }

    /// Checks a node name: a DNS label, 1 to 63 characters of `a-z`, `0-9`
    /// and `-`, with no hyphen at either end. Anything else: `bad_request`.
    pub fn validate_name(name: &str) -> Result<(), Rejection> {
        faber_api_types::name::validate_dns_label(name)
            .map_err(|message| Rejection::new(error_code::BAD_REQUEST, message))
    }
}

/// Whether `ip` is a host address of `net`: inside it, and neither the
/// network nor the broadcast address (both count as hosts in /31 and /32,
/// matching [`Ipv4Net::hosts`]).
pub(crate) fn is_host_addr(net: Ipv4Net, ip: Ipv4Addr) -> bool {
    net.contains(&ip) && (net.prefix_len() >= 31 || (ip != net.network() && ip != net.broadcast()))
}

/// Checks the container range `range` of `config` and returns its first
/// subnet, the founder's. Every failure is a `bad_request`; see
/// [`MeshState::init`] for the rules.
fn first_container_subnet(config: &MeshConfig, range: Ipv4Net) -> Result<Ipv4Net, Rejection> {
    let bad = |msg: String| Rejection::new(error_code::BAD_REQUEST, msg);
    let prefix = config.container_subnet_prefix;
    if prefix > MAX_CONTAINER_SUBNET_PREFIX {
        return Err(bad(format!(
            "container subnet prefix /{prefix} is longer than /{MAX_CONTAINER_SUBNET_PREFIX}"
        )));
    }
    if range.prefix_len() > prefix {
        return Err(bad(format!(
            "container range {range} is smaller than one /{prefix} subnet"
        )));
    }
    // The rule faber-node applies to every subnet it writes, so a subnet
    // handed out here is never refused there. A range that passes holds only
    // subnets that pass.
    check_container_subnet(range).map_err(|e| bad(format!("container range {e}")))?;
    if range.contains(&config.cidr.network()) || config.cidr.contains(&range.network()) {
        return Err(bad(format!(
            "container range {range} overlaps the mesh CIDR {}",
            config.cidr
        )));
    }
    range
        .subnets(prefix)
        .ok()
        .and_then(|mut subnets| subnets.next())
        .ok_or_else(|| bad(format!("container range {range} has no /{prefix} subnet")))
}

/// Whether two networks share an address.
fn overlap(a: Ipv4Net, b: Ipv4Net) -> bool {
    a.contains(&b.network()) || b.contains(&a.network())
}

/// Why `range` cannot be the service range of a mesh with `config`, if it
/// cannot.
fn check_service_range(config: &MeshConfig, range: Ipv4Net) -> Result<(), Rejection> {
    let bad = |msg: String| Rejection::new(error_code::BAD_REQUEST, msg);
    if range.prefix_len() > MAX_SERVICE_CIDR_PREFIX {
        return Err(bad(format!(
            "service range {range} is longer than /{MAX_SERVICE_CIDR_PREFIX}"
        )));
    }
    if range != range.trunc() {
        return Err(bad(format!(
            "service range {range} has host bits set; did you mean {}?",
            range.trunc()
        )));
    }
    if overlap(range, config.cidr) {
        return Err(bad(format!(
            "service range {range} overlaps the mesh CIDR {}",
            config.cidr
        )));
    }
    if let Some(containers) = config.container_cidr.filter(|c| overlap(range, *c)) {
        return Err(bad(format!(
            "service range {range} overlaps the container range {containers}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn node(id: u64, ip: &str) -> NodeRecord {
        NodeRecord {
            node_id: id,
            public_key: format!("pk{id}"),
            mesh_ip: ip.parse().unwrap(),
            endpoint: Some(format!("203.0.113.{id}:51820")),
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: String::new(),
            lease_renewed_at: 0,
        }
    }

    #[test]
    fn constants() {
        assert_eq!(KEY_MESH, "mesh");
        assert_eq!(RESERVED_KEYS, &["mesh"]);
        assert_eq!(DEFAULT_MESH_CIDR.to_string(), "10.99.0.0/16");
        assert_eq!(DEFAULT_CONTAINER_CIDR.to_string(), "10.200.0.0/16");
        assert_eq!(DEFAULT_CONTAINER_SUBNET_PREFIX, 24);
        assert_eq!(DEFAULT_WG_PORT, 51820);
        assert_eq!(DEFAULT_INVITE_TTL_SECS, 3600);
        assert_eq!(MAX_INVITE_TTL_SECS, 604_800);
        assert_eq!(WG_KEEPALIVE_SECS, 25);
        assert_eq!(DEFAULT_LEASE_SECS, 8 * 60 * 60);
        assert_eq!(MIN_LEASE_SECS, 5 * 60);
        assert_eq!(MAX_LEASE_SECS, 30 * 24 * 60 * 60);
        assert_eq!(FOUNDER_DEFAULT_NAME, "node-1");

        let c = MeshConfig::default();
        assert_eq!(c.cidr, DEFAULT_MESH_CIDR);
        assert_eq!(c.peer_port, 7401);
        assert_eq!(c.wg_port, 51820);
        assert_eq!(c.container_cidr, None);
        assert_eq!(c.container_subnet_prefix, 24);
    }

    /// A config stored before the container fields existed still decodes.
    #[test]
    fn mesh_config_without_container_fields_decodes() {
        let c: MeshConfig = serde_json::from_value(json!({
            "cidr": "10.99.0.0/16", "peer_port": 7401, "wg_port": 51820
        }))
        .unwrap();
        assert_eq!(c, MeshConfig::default());
        assert_eq!(c.container_cidr, None);
        assert_eq!(c.container_subnet_prefix, 24);

        let with = MeshConfig {
            container_cidr: Some(DEFAULT_CONTAINER_CIDR),
            container_subnet_prefix: 26,
            ..c
        };
        let v = serde_json::to_value(with).unwrap();
        assert_eq!(
            v,
            json!({
                "cidr": "10.99.0.0/16", "peer_port": 7401, "wg_port": 51820,
                "container_cidr": "10.200.0.0/16", "container_subnet_prefix": 26
            })
        );
        assert_eq!(serde_json::from_value::<MeshConfig>(v).unwrap(), with);
    }

    #[test]
    fn mesh_state_round_trips_with_integer_node_keys() {
        let mut nodes = BTreeMap::new();
        nodes.insert(1, node(1, "10.99.0.1"));
        let mut n2 = node(2, "10.99.0.2");
        n2.endpoint = None;
        n2.invite_id = Some("ab".repeat(16));
        nodes.insert(2, n2);
        let invite = InviteRecord {
            invite_id: "cd".repeat(16),
            node_id: 3,
            public_key: "pk3".into(),
            mesh_ip: "10.99.0.3".parse().unwrap(),
            endpoint: "198.51.100.3:51820".into(),
            expires_at: 1_000,
            name: "node-3".into(),
        };
        let state = MeshState {
            config: MeshConfig::default(),
            nodes,
            invites: BTreeMap::from([(invite.invite_id.clone(), invite)]),
            next_node_id: 4,
            clock: 7,
            lease_secs: 600,
        };
        let v = serde_json::to_value(&state).unwrap();
        assert_eq!(v["config"]["cidr"], "10.99.0.0/16");
        assert_eq!(v["nodes"]["1"]["mesh_ip"], "10.99.0.1");
        assert_eq!(v["nodes"]["2"]["endpoint"], serde_json::Value::Null);
        assert_eq!(v["clock"], 7);
        assert_eq!(v["lease_secs"], 600);
        assert_eq!(serde_json::from_value::<MeshState>(v).unwrap(), state);
    }

    /// A mesh stored before leases existed decodes with the clock at zero,
    /// the default lease and unnamed nodes and invites.
    #[test]
    fn mesh_state_without_lease_fields_decodes() {
        let s: MeshState = serde_json::from_value(json!({
            "config": {"cidr": "10.99.0.0/16", "peer_port": 7401, "wg_port": 51820},
            "nodes": {"1": {"node_id": 1, "public_key": "pk1", "mesh_ip": "10.99.0.1"}},
            "invites": {"ab": {
                "invite_id": "ab", "node_id": 2, "public_key": "pk2",
                "mesh_ip": "10.99.0.2", "endpoint": "198.51.100.2:51820",
                "expires_at": 1_000
            }},
            "next_node_id": 3
        }))
        .unwrap();
        assert_eq!(s.clock, 0);
        assert_eq!(s.lease_secs, DEFAULT_LEASE_SECS);
        assert_eq!(s.nodes[&1].name, "");
        assert_eq!(s.nodes[&1].lease_renewed_at, 0);
        assert_eq!(s.invites["ab"].name, "");
    }

    /// An invite stored before names existed decodes, and an unnamed invite
    /// serialises as it did then.
    #[test]
    fn invite_record_without_a_name_decodes() {
        let old = json!({
            "invite_id": "ab", "node_id": 2, "public_key": "pk2",
            "mesh_ip": "10.99.0.2", "endpoint": "198.51.100.2:51820",
            "expires_at": 1_000
        });
        let i: InviteRecord = serde_json::from_value(old.clone()).unwrap();
        assert_eq!(i.name, "");
        assert_eq!(serde_json::to_value(&i).unwrap(), old);

        let named = InviteRecord {
            name: "web-1".into(),
            ..i
        };
        let v = serde_json::to_value(&named).unwrap();
        assert_eq!(v["name"], "web-1");
        assert_eq!(serde_json::from_value::<InviteRecord>(v).unwrap(), named);
    }

    /// A record that predates the optional, container and lease fields
    /// decodes with them unset, and serialises without them.
    #[test]
    fn node_record_optional_fields_default_and_stay_off_the_wire() {
        let n: NodeRecord = serde_json::from_value(json!({
            "node_id": 1, "public_key": "pk", "mesh_ip": "10.99.0.1"
        }))
        .unwrap();
        assert_eq!(n.endpoint, None);
        assert_eq!(n.invite_id, None);
        assert_eq!(n.instance_id, None);
        assert_eq!(n.container_subnet, None);
        assert_eq!(n.name, "");
        assert_eq!(n.lease_renewed_at, 0);
        let v = serde_json::to_value(&n).unwrap();
        for absent in [
            "instance_id",
            "container_subnet",
            "name",
            "lease_renewed_at",
        ] {
            assert!(v.get(absent).is_none(), "{absent}: {v}");
        }

        let with = NodeRecord {
            container_subnet: Some("10.200.3.0/24".parse().unwrap()),
            name: "web-1".into(),
            lease_renewed_at: 42,
            ..n
        };
        let v = serde_json::to_value(&with).unwrap();
        assert_eq!(v["container_subnet"], "10.200.3.0/24");
        assert_eq!(v["name"], "web-1");
        assert_eq!(v["lease_renewed_at"], 42);
        assert_eq!(serde_json::from_value::<NodeRecord>(v).unwrap(), with);
    }

    // --- MeshState behaviour -------------------------------------------------

    use faber_api_types::error_code as ec;

    const T0: u64 = 1_000;

    fn cfg(cidr: &str) -> MeshConfig {
        MeshConfig {
            cidr: cidr.parse().unwrap(),
            ..MeshConfig::default()
        }
    }

    fn first_host(cidr: &str) -> String {
        let net: Ipv4Net = cidr.parse().unwrap();
        net.hosts().next().unwrap().to_string()
    }

    fn founded(cidr: &str) -> MeshState {
        MeshState::init(cfg(cidr), node(1, &first_host(cidr))).unwrap()
    }

    fn id(n: u8) -> String {
        format!("{n:02x}").repeat(16)
    }

    fn ep(n: u8) -> String {
        format!("198.51.100.{n}:51820")
    }

    fn name(n: u8) -> String {
        format!("node-{n}")
    }

    fn invite(s: &mut MeshState, n: u8, ttl: u64, at: u64) -> Result<Allocation, Rejection> {
        s.create_invite(id(n), format!("inv-pk{n}"), ep(n), name(n), ttl, at)
    }

    /// No raft membership, for the tests that do not depend on one.
    fn no_members() -> BTreeSet<u64> {
        BTreeSet::new()
    }

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    /// The machine the tests register from.
    const MACHINE: &str = "11111111111111111111111111111111";
    /// Another machine holding a copy of the same token.
    const OTHER_MACHINE: &str = "22222222222222222222222222222222";

    #[test]
    fn init_seeds_founder_and_next_id() {
        let s = MeshState::init(cfg("10.99.0.0/16"), node(1, "10.99.0.1")).unwrap();
        assert_eq!(s.config, cfg("10.99.0.0/16"));
        assert_eq!(s.nodes.len(), 1);
        assert_eq!(
            s.nodes[&1],
            NodeRecord {
                name: "node-1".into(),
                ..node(1, "10.99.0.1")
            }
        );
        assert!(s.invites.is_empty());
        assert_eq!(s.next_node_id, 2);
        assert_eq!(s.clock, 0);
        assert_eq!(s.lease_secs, DEFAULT_LEASE_SECS);

        let s = MeshState::init(cfg("10.99.0.0/16"), node(5, "10.99.3.4")).unwrap();
        assert_eq!(s.next_node_id, 6);
    }

    #[test]
    fn init_rejects_founder_ip_outside_hosts() {
        for bad in ["10.98.0.1", "10.99.0.0", "10.99.255.255"] {
            let err = MeshState::init(cfg("10.99.0.0/16"), node(1, bad)).unwrap_err();
            assert_eq!(err.code, ec::BAD_REQUEST, "{bad}");
        }
    }

    #[test]
    fn init_rejects_node_id_overflow() {
        let err = MeshState::init(cfg("10.99.0.0/16"), node(u64::MAX, "10.99.0.1")).unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);
    }

    #[test]
    fn invites_allocate_monotonic_ids_and_lowest_free_ips() {
        let mut s = founded("10.99.0.0/16");
        let a = invite(&mut s, 2, 60, T0).unwrap();
        let b = invite(&mut s, 3, 60, T0).unwrap();
        assert_eq!(
            a,
            Allocation {
                node_id: 2,
                mesh_ip: ip("10.99.0.2"),
                expires_at: T0 + 60,
                name: "node-2".into(),
                replaces: None,
            }
        );
        assert_eq!((b.node_id, b.mesh_ip), (3, ip("10.99.0.3")));
        assert_eq!(s.next_node_id, 4);
        assert_eq!(
            s.invites[&id(2)],
            InviteRecord {
                invite_id: id(2),
                node_id: 2,
                public_key: "inv-pk2".into(),
                mesh_ip: ip("10.99.0.2"),
                endpoint: ep(2),
                expires_at: T0 + 60,
                name: "node-2".into(),
            }
        );
    }

    #[test]
    fn allocation_fills_gaps_below_the_founder() {
        let mut s = MeshState::init(cfg("10.99.0.0/16"), node(1, "10.99.0.3")).unwrap();
        assert_eq!(invite(&mut s, 2, 60, T0).unwrap().mesh_ip, ip("10.99.0.1"));
        assert_eq!(invite(&mut s, 3, 60, T0).unwrap().mesh_ip, ip("10.99.0.2"));
        assert_eq!(invite(&mut s, 4, 60, T0).unwrap().mesh_ip, ip("10.99.0.4"));
    }

    #[test]
    fn allocation_skips_network_and_broadcast_then_exhausts() {
        // /29: network .0, hosts .1-.6, broadcast .7.
        let mut s = founded("10.0.0.0/29");
        let mut ips = Vec::new();
        for n in 2..=6 {
            ips.push(invite(&mut s, n, 60, T0).unwrap().mesh_ip);
        }
        let want: Vec<Ipv4Addr> = (2..=6).map(|h| Ipv4Addr::new(10, 0, 0, h)).collect();
        assert_eq!(ips, want);
        assert_eq!(
            invite(&mut s, 7, 60, T0).unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
    }

    #[test]
    fn tiny_cidr_exhausts_and_leaves_state_unchanged() {
        // /30: hosts .1 and .2 only.
        let mut s = founded("10.0.0.0/30");
        assert_eq!(invite(&mut s, 2, 60, T0).unwrap().mesh_ip, ip("10.0.0.2"));
        let before = s.clone();
        assert_eq!(
            invite(&mut s, 3, 60, T0).unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_eq!(s, before, "a rejected invite must not consume a node id");
    }

    #[test]
    fn expired_invites_are_pruned_and_their_ip_reused() {
        let mut s = founded("10.0.0.0/30");
        invite(&mut s, 2, 60, T0).unwrap();
        // Still live one second before expiry.
        assert_eq!(
            invite(&mut s, 3, 60, T0 + 59).unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        // Expired at exactly expires_at.
        let c = invite(&mut s, 3, 60, T0 + 60).unwrap();
        assert_eq!(c.mesh_ip, ip("10.0.0.2"));
        assert_eq!(c.node_id, 3, "node ids are never reused");
        assert!(!s.invites.contains_key(&id(2)));
        assert!(s.invites.contains_key(&id(3)));
    }

    #[test]
    fn ttl_bounds() {
        let mut s = founded("10.99.0.0/16");
        assert_eq!(invite(&mut s, 2, 0, T0).unwrap_err().code, ec::BAD_REQUEST);
        assert_eq!(
            invite(&mut s, 2, MAX_INVITE_TTL_SECS + 1, T0)
                .unwrap_err()
                .code,
            ec::BAD_REQUEST
        );
        let a = invite(&mut s, 2, MAX_INVITE_TTL_SECS, T0).unwrap();
        assert_eq!(a.expires_at, T0 + MAX_INVITE_TTL_SECS);
        assert_eq!(s.next_node_id, 3, "rejections do not consume ids");
    }

    #[test]
    fn expiry_overflow_is_rejected() {
        let mut s = founded("10.99.0.0/16");
        let err = invite(&mut s, 2, 60, u64::MAX - 10).unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);
    }

    #[test]
    fn duplicate_invite_id_or_public_key_is_rejected() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();

        // Same invite id, new key.
        let err = s
            .create_invite(id(2), "other".into(), ep(9), name(9), 60, T0)
            .unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);
        // Key of a live invite.
        let err = s
            .create_invite(id(9), "inv-pk2".into(), ep(9), name(9), 60, T0)
            .unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);
        // Key of an existing node.
        let err = s
            .create_invite(id(9), "pk1".into(), ep(9), name(9), 60, T0)
            .unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);

        // Invite id already used by a node.
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        let err = s
            .create_invite(id(2), "fresh".into(), ep(9), name(9), 60, T0)
            .unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);
        assert_eq!(s.next_node_id, 3);
    }

    #[test]
    fn key_of_expired_invite_can_be_invited_again() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        let a = s
            .create_invite(id(3), "inv-pk2".into(), ep(3), name(3), 60, T0 + 60)
            .unwrap();
        assert_eq!(a.node_id, 3);
    }

    #[test]
    fn register_moves_invite_into_nodes() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0 + 10, &no_members())
            .unwrap();
        assert!(s.invites.is_empty());
        assert_eq!(
            s.nodes[&2],
            NodeRecord {
                node_id: 2,
                public_key: "inv-pk2".into(),
                mesh_ip: ip("10.99.0.2"),
                endpoint: Some(ep(2)),
                invite_id: Some(id(2)),
                instance_id: Some(MACHINE.into()),
                container_subnet: None,
                name: "node-2".into(),
                lease_renewed_at: 0,
            }
        );
        // The IP stays taken by the node.
        assert_eq!(invite(&mut s, 3, 60, T0).unwrap().mesh_ip, ip("10.99.0.3"));
    }

    #[test]
    fn register_is_idempotent_on_retry() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        let after = s.clone();
        // A retry succeeds even after the invite would have expired.
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0 + 1_000, &no_members())
            .unwrap();
        assert_eq!(s, after);
    }

    #[test]
    fn register_with_used_invite_but_other_details_is_invite_used() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        let after = s.clone();
        assert_eq!(
            s.register_node(&id(2), 7, &ep(2), MACHINE, T0, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_USED
        );
        assert_eq!(
            s.register_node(&id(2), 2, &ep(9), MACHINE, T0, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_USED
        );
        assert_eq!(s, after);
    }

    /// A second machine that holds a copy of the token is not the node.
    #[test]
    fn register_from_another_machine_is_invite_used() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        let after = s.clone();
        assert_eq!(
            s.register_node(&id(2), 2, &ep(2), OTHER_MACHINE, T0, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_USED
        );
        assert_eq!(s, after);
        // The machine that registered can still repeat its registration.
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0 + 5, &no_members())
            .unwrap();
        assert_eq!(s, after);
    }

    /// A record without an instance id (registered before the field existed)
    /// matches no machine, and a request without one is never a retry.
    #[test]
    fn register_without_an_instance_id_is_never_a_retry() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), "", T0, &no_members())
            .unwrap();
        assert_eq!(s.nodes[&2].instance_id, None);
        for instance in ["", MACHINE] {
            assert_eq!(
                s.register_node(&id(2), 2, &ep(2), instance, T0, &no_members())
                    .unwrap_err()
                    .code,
                ec::INVITE_USED,
                "{instance:?}"
            );
        }

        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        assert_eq!(
            s.register_node(&id(2), 2, &ep(2), "", T0, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_USED
        );
    }

    #[test]
    fn register_unknown_invite_is_invalid() {
        let mut s = founded("10.99.0.0/16");
        assert_eq!(
            s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_INVALID
        );
    }

    #[test]
    fn register_mismatch_is_invalid() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        let before = s.clone();
        assert_eq!(
            s.register_node(&id(2), 3, &ep(2), MACHINE, T0, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_INVALID
        );
        assert_eq!(
            s.register_node(&id(2), 2, &ep(9), MACHINE, T0, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_INVALID
        );
        assert_eq!(s, before);
    }

    #[test]
    fn register_expiry_boundary() {
        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0 + 59, &no_members())
            .unwrap();

        let mut s = founded("10.99.0.0/16");
        invite(&mut s, 2, 60, T0).unwrap();
        let before = s.clone();
        assert_eq!(
            s.register_node(&id(2), 2, &ep(2), MACHINE, T0 + 60, &no_members())
                .unwrap_err()
                .code,
            ec::INVITE_EXPIRED
        );
        assert_eq!(s, before);
    }

    // --- container subnets ---------------------------------------------------

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    /// The default mesh CIDR with a container range of `range`, cut into
    /// subnets of `/prefix`.
    fn cfg_with_range(range: &str, prefix: u8) -> MeshConfig {
        MeshConfig {
            container_cidr: Some(net(range)),
            container_subnet_prefix: prefix,
            ..MeshConfig::default()
        }
    }

    fn founded_with_range(range: &str, prefix: u8) -> MeshState {
        MeshState::init(cfg_with_range(range, prefix), node(1, "10.99.0.1")).unwrap()
    }

    fn subnets(s: &MeshState) -> Vec<Option<Ipv4Net>> {
        s.nodes.values().map(|n| n.container_subnet).collect()
    }

    #[test]
    fn init_gives_the_founder_the_first_subnet() {
        let s = founded_with_range("10.200.0.0/16", 24);
        assert_eq!(s.nodes[&1].container_subnet, Some(net("10.200.0.0/24")));
        assert_eq!(s.config, cfg_with_range("10.200.0.0/16", 24));

        let s = founded_with_range("172.20.8.0/22", 26);
        assert_eq!(s.nodes[&1].container_subnet, Some(net("172.20.8.0/26")));
    }

    #[test]
    fn init_ignores_the_subnet_the_proposer_sent() {
        let mut founder = node(1, "10.99.0.1");
        founder.container_subnet = Some(net("10.200.7.0/24"));
        let s = MeshState::init(cfg_with_range("10.200.0.0/16", 24), founder.clone()).unwrap();
        assert_eq!(s.nodes[&1].container_subnet, Some(net("10.200.0.0/24")));

        // With no range the founder gets none, whatever was sent.
        let s = MeshState::init(MeshConfig::default(), founder).unwrap();
        assert_eq!(s.nodes[&1].container_subnet, None);
    }

    #[test]
    fn init_rejects_bad_container_ranges() {
        for (range, prefix, why) in [
            ("10.200.0.0/25", 24, "range smaller than one subnet"),
            ("10.200.0.0/16", 31, "subnet prefix above 30"),
            ("10.200.0.0/32", 32, "subnet prefix above 30"),
            ("10.200.1.0/16", 24, "host bits set"),
            ("10.99.0.0/16", 24, "same as the mesh"),
            ("10.99.4.0/22", 24, "inside the mesh"),
            ("10.0.0.0/8", 24, "contains the mesh"),
            ("127.0.0.0/16", 24, "loopback"),
            ("169.254.0.0/16", 24, "link-local"),
            ("224.0.0.0/16", 24, "multicast"),
            ("0.0.0.0/8", 24, "this network"),
            ("240.0.0.0/8", 24, "reserved, holds the broadcast address"),
            ("128.0.0.0/2", 24, "holds link-local"),
        ] {
            let err =
                MeshState::init(cfg_with_range(range, prefix), node(1, "10.99.0.1")).unwrap_err();
            assert_eq!(err.code, ec::BAD_REQUEST, "{range} /{prefix}: {why}");
        }
    }

    #[test]
    fn init_accepts_the_edges_of_a_valid_container_range() {
        // One subnet as large as the range.
        let s = founded_with_range("10.200.0.0/24", 24);
        assert_eq!(s.nodes[&1].container_subnet, Some(net("10.200.0.0/24")));
        // The longest subnet prefix allowed.
        let s = founded_with_range("10.200.0.0/16", 30);
        assert_eq!(s.nodes[&1].container_subnet, Some(net("10.200.0.0/30")));
        // Next to the mesh, not overlapping it.
        let s = founded_with_range("10.100.0.0/16", 24);
        assert_eq!(s.nodes[&1].container_subnet, Some(net("10.100.0.0/24")));
    }

    fn cfg_with_service_range(service: &str) -> MeshConfig {
        MeshConfig {
            service_cidr: Some(net(service)),
            ..cfg_with_range("10.200.0.0/16", 24)
        }
    }

    #[test]
    fn init_rejects_bad_service_ranges() {
        for (range, why) in [
            ("10.201.0.0/31", "fewer than two hosts"),
            ("10.201.0.1/16", "host bits set"),
            ("10.99.0.0/16", "same as the mesh"),
            ("10.99.8.0/24", "inside the mesh"),
            ("10.0.0.0/8", "contains the mesh and the containers"),
            ("10.200.0.0/16", "same as the containers"),
            ("10.200.5.0/24", "inside the containers"),
            ("10.192.0.0/10", "contains the containers"),
        ] {
            let err =
                MeshState::init(cfg_with_service_range(range), node(1, "10.99.0.1")).unwrap_err();
            assert_eq!(err.code, ec::BAD_REQUEST, "{range}: {why}");
        }
        // Without a container range only the mesh counts.
        let config = MeshConfig {
            service_cidr: Some(net("10.200.0.0/16")),
            ..MeshConfig::default()
        };
        assert!(MeshState::init(config, node(1, "10.99.0.1")).is_ok());
    }

    #[test]
    fn init_accepts_a_service_range_apart_from_the_others() {
        for range in ["10.201.0.0/16", "10.201.0.0/30", "192.168.0.0/16"] {
            let s = MeshState::init(cfg_with_service_range(range), node(1, "10.99.0.1")).unwrap();
            assert_eq!(s.config.service_cidr, Some(net(range)));
        }
        assert_eq!(DEFAULT_SERVICE_CIDR.to_string(), "10.201.0.0/16");
        MeshState::init(
            cfg_with_service_range(&DEFAULT_SERVICE_CIDR.to_string()),
            node(1, "10.99.0.1"),
        )
        .unwrap();
    }

    #[test]
    fn a_mesh_config_with_a_service_range_round_trips() {
        let config = cfg_with_service_range("10.201.0.0/16");
        let v = serde_json::to_value(config).unwrap();
        assert_eq!(v["service_cidr"], "10.201.0.0/16");
        assert_eq!(serde_json::from_value::<MeshConfig>(v).unwrap(), config);
    }

    #[test]
    fn no_range_means_no_subnets_anywhere() {
        let mut s = founded("10.99.0.0/16");
        assert_eq!(s.config.container_cidr, None);
        for n in 2..=4 {
            invite(&mut s, n, 60, T0).unwrap();
            s.register_node(&id(n), u64::from(n), &ep(n), MACHINE, T0, &no_members())
                .unwrap();
        }
        assert_eq!(subnets(&s), vec![None; 4]);
        let v = serde_json::to_value(&s).unwrap();
        assert!(v["nodes"]["2"].get("container_subnet").is_none(), "{v}");
    }

    #[test]
    fn register_gives_each_node_the_next_subnet() {
        let mut s = founded_with_range("10.200.0.0/16", 24);
        for n in 2..=3 {
            invite(&mut s, n, 60, T0).unwrap();
            s.register_node(&id(n), u64::from(n), &ep(n), MACHINE, T0, &no_members())
                .unwrap();
        }
        assert_eq!(
            subnets(&s),
            vec![
                Some(net("10.200.0.0/24")),
                Some(net("10.200.1.0/24")),
                Some(net("10.200.2.0/24")),
            ]
        );
    }

    #[test]
    fn register_fills_the_lowest_gap() {
        let mut s = founded_with_range("10.200.0.0/16", 24);
        // Make the gap by hand: the founder holds the third subnet and the
        // first two are free.
        s.nodes.get_mut(&1).unwrap().container_subnet = Some(net("10.200.2.0/24"));
        for n in 2..=4 {
            invite(&mut s, n, 60, T0).unwrap();
            s.register_node(&id(n), u64::from(n), &ep(n), MACHINE, T0, &no_members())
                .unwrap();
        }
        assert_eq!(
            subnets(&s),
            vec![
                Some(net("10.200.2.0/24")),
                Some(net("10.200.0.0/24")),
                Some(net("10.200.1.0/24")),
                Some(net("10.200.3.0/24")),
            ]
        );
    }

    #[test]
    fn register_retry_keeps_the_subnet() {
        let mut s = founded_with_range("10.200.0.0/16", 24);
        invite(&mut s, 2, 60, T0).unwrap();
        invite(&mut s, 3, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        s.register_node(&id(3), 3, &ep(3), OTHER_MACHINE, T0, &no_members())
            .unwrap();
        let after = s.clone();
        assert_eq!(s.nodes[&2].container_subnet, Some(net("10.200.1.0/24")));

        s.register_node(&id(2), 2, &ep(2), MACHINE, T0 + 1_000, &no_members())
            .unwrap();
        assert_eq!(s, after);
        assert_eq!(s.nodes[&2].container_subnet, Some(net("10.200.1.0/24")));
    }

    #[test]
    fn invite_is_rejected_when_every_subnet_is_spoken_for() {
        // A /30 range cut into /30s is one subnet, and the founder holds it.
        let mut s = founded_with_range("10.200.0.0/30", 30);
        let before = s.clone();
        let err = invite(&mut s, 2, 60, T0).unwrap_err();
        assert_eq!(err.code, ec::MESH_EXHAUSTED);
        assert_eq!(
            err.message,
            "container range 10.200.0.0/30 is full: every /30 subnet (1) \
             is held by a node or reserved by a live invite"
        );
        assert_eq!(s, before, "a rejected invite must not consume a node id");
    }

    #[test]
    fn live_invites_count_towards_container_capacity() {
        // Two subnets: the founder's and one more.
        let mut s = founded_with_range("10.200.0.0/29", 30);
        invite(&mut s, 2, 60, T0).unwrap();
        let before = s.clone();
        assert_eq!(
            invite(&mut s, 3, 60, T0).unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_eq!(s, before);

        // Registering moves the reservation to the node; it frees nothing.
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        assert_eq!(s.nodes[&2].container_subnet, Some(net("10.200.0.4/30")));
        assert_eq!(
            invite(&mut s, 3, 60, T0).unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_eq!(s.next_node_id, 3);
    }

    #[test]
    fn expired_invite_frees_container_capacity() {
        let mut s = founded_with_range("10.200.0.0/29", 30);
        invite(&mut s, 2, 60, T0).unwrap();
        // Still live one second before expiry.
        assert_eq!(
            invite(&mut s, 3, 60, T0 + 59).unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        // Expired at exactly expires_at.
        let a = invite(&mut s, 3, 60, T0 + 60).unwrap();
        assert_eq!(a.node_id, 3);
        s.register_node(&id(3), 3, &ep(3), MACHINE, T0 + 61, &no_members())
            .unwrap();
        assert_eq!(s.nodes[&3].container_subnet, Some(net("10.200.0.4/30")));
    }

    #[test]
    fn register_rejects_and_changes_nothing_when_no_subnet_is_free() {
        let mut s = founded_with_range("10.200.0.0/30", 30);
        // `create_invite` would refuse this invite, so store it by hand.
        s.invites.insert(
            id(2),
            InviteRecord {
                invite_id: id(2),
                node_id: 2,
                public_key: "inv-pk2".into(),
                mesh_ip: ip("10.99.0.2"),
                endpoint: ep(2),
                expires_at: T0 + 60,
                name: "node-2".into(),
            },
        );
        s.next_node_id = 3;
        let before = s.clone();
        let err = s
            .register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap_err();
        assert_eq!(err.code, ec::MESH_EXHAUSTED);
        assert_eq!(
            err.message,
            "no free /30 subnet in container range 10.200.0.0/30"
        );
        assert_eq!(s, before);
        assert!(s.invites.contains_key(&id(2)), "the invite must survive");
    }

    #[test]
    fn subnet_count_does_not_overflow() {
        let count = |range: &str, prefix: u8| {
            let mut s = founded("10.99.0.0/16");
            s.config = cfg_with_range(range, prefix);
            s.container_subnet_count()
        };
        assert_eq!(count("10.200.0.0/16", 24), Some(256));
        assert_eq!(count("10.200.0.0/30", 30), Some(1));
        assert_eq!(count("11.0.0.0/8", 30), Some(4_194_304));
        // Not reachable through `init`, but must not wrap or panic.
        assert_eq!(count("0.0.0.0/0", 32), Some(1 << 32));
        assert_eq!(count("10.200.0.0/24", 16), Some(0));
        assert_eq!(founded("10.99.0.0/16").container_subnet_count(), None);

        // A large range still hands out invites and subnets.
        let mut s = founded_with_range("11.0.0.0/8", 30);
        invite(&mut s, 2, 60, T0).unwrap();
        s.register_node(&id(2), 2, &ep(2), MACHINE, T0, &no_members())
            .unwrap();
        assert_eq!(s.nodes[&2].container_subnet, Some(net("11.0.0.4/30")));
    }

    // --- names and leases ---------------------------------------------------

    #[test]
    fn validate_name_accepts_dns_labels() {
        let longest = "a".repeat(63);
        for good in ["a", "0", "node-1", "web-eu-2", "9lives", "a--b", &longest] {
            assert_eq!(MeshState::validate_name(good), Ok(()), "{good}");
        }
    }

    #[test]
    fn validate_name_rejects_everything_else() {
        let rejected = |bad: &str| {
            let err = MeshState::validate_name(bad).unwrap_err();
            assert_eq!(err.code, ec::BAD_REQUEST, "{bad:?}");
        };
        // Empty, too long, or a hyphen at either end.
        let too_long = "a".repeat(64);
        for bad in ["", &too_long, "-a", "a-", "-"] {
            rejected(bad);
        }
        // Characters outside a-z, 0-9 and the hyphen.
        for bad in ["Node-1", "node_1", "node.1", "node 1", " node", "node\n"] {
            rejected(bad);
        }
        rejected("n\u{f6}de");
    }

    #[test]
    fn lease_expires_once_a_whole_lease_has_passed_without_renewal() {
        let mut s = founded("10.99.0.0/16");
        s.lease_secs = 600;
        let mut n = node(1, "10.99.0.1");
        n.lease_renewed_at = 100;

        s.clock = 100;
        assert!(!s.lease_expired(&n));
        s.clock = 699;
        assert!(!s.lease_expired(&n));
        s.clock = 700;
        assert!(s.lease_expired(&n));
        s.clock = u64::MAX;
        assert!(s.lease_expired(&n));

        // A renewal stamped after the clock does not underflow.
        s.clock = 50;
        assert!(!s.lease_expired(&n));
    }

    fn members(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    /// An invite for `name`, made at `T0` with a 60 second lifetime.
    fn invite_named(s: &mut MeshState, n: u8, name: &str) -> Result<Allocation, Rejection> {
        s.create_invite(id(n), format!("inv-pk{n}"), ep(n), name.into(), 60, T0)
    }

    fn register(s: &mut MeshState, n: u8, members: &BTreeSet<u64>) -> Result<(), Rejection> {
        s.register_node(&id(n), u64::from(n), &ep(n), MACHINE, T0, members)
    }

    /// Invites and registers node `n` under the name `node-<n>`.
    fn join(s: &mut MeshState, n: u8) {
        invite(s, n, 60, T0).unwrap();
        register(s, n, &no_members()).unwrap();
    }

    /// Moves the lease clock forward by `secs`, in as many clamped steps as
    /// that takes.
    fn pass(s: &mut MeshState, secs: u64) {
        let target = s.clock + secs;
        while s.clock < target {
            let before = s.clock;
            s.advance_clock(target - s.clock).unwrap();
            assert!(s.clock > before, "the clock must move");
        }
    }

    /// What must hold after every command: node names are unique, and no two
    /// nodes share a mesh IP or a container subnet. Invites reserve a name
    /// and a mesh IP of their own.
    fn assert_invariants(s: &MeshState) {
        let mut names = BTreeSet::new();
        let mut ips = BTreeSet::new();
        let mut subnets = BTreeSet::new();
        for (key, n) in &s.nodes {
            assert_eq!(*key, n.node_id);
            assert_eq!(MeshState::validate_name(&n.name), Ok(()), "{n:?}");
            assert!(names.insert(n.name.as_str()), "name held twice: {n:?}");
            assert!(ips.insert(n.mesh_ip), "mesh IP held twice: {n:?}");
            if let Some(subnet) = n.container_subnet {
                assert!(subnets.insert(subnet), "subnet held twice: {n:?}");
            }
            assert!(n.lease_renewed_at <= s.clock, "{n:?}");
        }
        let mut invited = BTreeSet::new();
        for i in s.invites.values() {
            assert!(invited.insert(i.name.as_str()), "name invited twice: {i:?}");
            assert!(ips.insert(i.mesh_ip), "mesh IP held twice: {i:?}");
            assert!(!s.nodes.contains_key(&i.node_id), "{i:?}");
            assert!(i.node_id < s.next_node_id, "{i:?}");
        }
    }

    #[test]
    fn init_names_the_founder() {
        // No name given: the default.
        let s = founded("10.99.0.0/16");
        assert_eq!(s.nodes[&1].name, FOUNDER_DEFAULT_NAME);

        let mut founder = node(1, "10.99.0.1");
        founder.name = "web-1".into();
        let s = MeshState::init(cfg("10.99.0.0/16"), founder).unwrap();
        assert_eq!(s.nodes[&1].name, "web-1");
        assert_invariants(&s);

        let too_long = "a".repeat(64);
        for bad in ["Web-1", "-web", "web_1", &too_long] {
            let mut founder = node(1, "10.99.0.1");
            founder.name = bad.into();
            let err = MeshState::init(cfg("10.99.0.0/16"), founder).unwrap_err();
            assert_eq!(err.code, ec::BAD_REQUEST, "{bad}");
        }
    }

    #[test]
    fn init_starts_the_founder_lease_at_zero() {
        let mut founder = node(1, "10.99.0.1");
        founder.lease_renewed_at = 77;
        let s = MeshState::init(cfg("10.99.0.0/16"), founder).unwrap();
        assert_eq!(s.nodes[&1].lease_renewed_at, 0);
        assert!(!s.lease_expired(&s.nodes[&1]));
    }

    #[test]
    fn advance_clock_is_clamped_to_a_tenth_of_the_lease() {
        let mut s = founded("10.99.0.0/16");
        let start = s.clone();
        // Default lease, 28800 seconds: one command moves at most 2880.
        s.advance_clock(10).unwrap();
        assert_eq!(s.clock, 10);
        s.advance_clock(2_880).unwrap();
        assert_eq!(s.clock, 2_890);
        s.advance_clock(2_881).unwrap();
        assert_eq!(s.clock, 5_770);
        s.advance_clock(u64::MAX).unwrap();
        assert_eq!(s.clock, 8_650);
        // Nothing passed: nothing moves.
        s.advance_clock(0).unwrap();
        assert_eq!(s.clock, 8_650);
        // Only the clock changed.
        assert_eq!(MeshState { clock: 0, ..s }, start);

        // The shortest lease: at most 30.
        let mut s = founded("10.99.0.0/16");
        s.set_lease(MIN_LEASE_SECS).unwrap();
        s.advance_clock(29).unwrap();
        assert_eq!(s.clock, 29);
        s.advance_clock(1_000).unwrap();
        assert_eq!(s.clock, 59);

        // The longest lease: at most 259200.
        let mut s = founded("10.99.0.0/16");
        s.set_lease(MAX_LEASE_SECS).unwrap();
        s.advance_clock(u64::MAX).unwrap();
        assert_eq!(s.clock, 259_200);

        // A lease below ten seconds (not reachable through `set_lease`)
        // still lets the clock move by one.
        for tiny in [0, 5, 9] {
            let mut s = founded("10.99.0.0/16");
            s.lease_secs = tiny;
            s.advance_clock(1_000).unwrap();
            assert_eq!(s.clock, 1, "lease_secs {tiny}");
        }
    }

    #[test]
    fn advance_clock_saturates() {
        let mut s = founded("10.99.0.0/16");
        s.clock = u64::MAX - 1;
        s.advance_clock(100).unwrap();
        assert_eq!(s.clock, u64::MAX);
        s.advance_clock(100).unwrap();
        assert_eq!(s.clock, u64::MAX);
    }

    #[test]
    fn renew_restarts_the_lease() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        pass(&mut s, 1_000);
        s.renew_lease(2, MACHINE).unwrap();
        assert_eq!(s.nodes[&2].lease_renewed_at, 1_000);
        assert_eq!(
            s.nodes[&1].lease_renewed_at, 0,
            "other leases are untouched"
        );

        // One second before the lease runs out is still in time.
        pass(&mut s, DEFAULT_LEASE_SECS - 1);
        s.renew_lease(2, MACHINE).unwrap();
        assert_eq!(s.nodes[&2].lease_renewed_at, 1_000 + DEFAULT_LEASE_SECS - 1);
        assert_invariants(&s);
    }

    /// Expiry is sticky: once the lease has run out, a renewal is refused.
    #[test]
    fn renew_of_an_expired_lease_is_refused() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        pass(&mut s, DEFAULT_LEASE_SECS);
        assert!(s.lease_expired(&s.nodes[&2]));
        let before = s.clone();
        let err = s.renew_lease(2, MACHINE).unwrap_err();
        assert_eq!(err.code, ec::MESH_LEASE_EXPIRED);
        assert_eq!(err.message, "the lease of node 2 has expired");
        assert_eq!(s, before);

        // Still refused later, and for a machine that would be forbidden.
        pass(&mut s, 5_000);
        let before = s.clone();
        for instance in [MACHINE, OTHER_MACHINE] {
            assert_eq!(
                s.renew_lease(2, instance).unwrap_err().code,
                ec::MESH_LEASE_EXPIRED
            );
        }
        assert_eq!(s, before);

        // Once the record is gone the answer changes: the node has left
        // raft by then, and may take that as proof.
        s.remove_node(2, &BTreeSet::new()).unwrap();
        assert_eq!(
            s.renew_lease(2, MACHINE).unwrap_err().code,
            ec::MESH_NODE_REMOVED
        );
    }

    #[test]
    fn renew_of_an_unknown_node_is_refused() {
        let mut s = founded("10.99.0.0/16");
        let before = s.clone();
        let err = s.renew_lease(9, MACHINE).unwrap_err();
        assert_eq!(err.code, ec::MESH_NODE_REMOVED);
        assert_eq!(err.message, "node 9 is not in the mesh");
        assert_eq!(s, before);
    }

    #[test]
    fn renew_from_another_machine_is_forbidden() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        pass(&mut s, 100);
        let before = s.clone();
        // Another machine, and a request that names no machine.
        for instance in [OTHER_MACHINE, ""] {
            let err = s.renew_lease(2, instance).unwrap_err();
            assert_eq!(err.code, ec::FORBIDDEN, "{instance:?}");
            assert_eq!(err.message, "node 2 is registered to another machine");
        }
        assert_eq!(s, before);
    }

    /// The founder's record is created without an instance id: the first
    /// renewal that names one sets it.
    #[test]
    fn renew_adopts_the_instance_of_a_record_without_one() {
        let mut s = founded("10.99.0.0/16");
        assert_eq!(s.nodes[&1].instance_id, None);
        pass(&mut s, 50);

        // Nothing to adopt from a request that names no machine.
        s.renew_lease(1, "").unwrap();
        assert_eq!(s.nodes[&1].instance_id, None);
        assert_eq!(s.nodes[&1].lease_renewed_at, 50);

        pass(&mut s, 50);
        s.renew_lease(1, MACHINE).unwrap();
        assert_eq!(s.nodes[&1].instance_id.as_deref(), Some(MACHINE));
        assert_eq!(s.nodes[&1].lease_renewed_at, 100);

        // From here on only that machine renews.
        let before = s.clone();
        assert_eq!(
            s.renew_lease(1, OTHER_MACHINE).unwrap_err().code,
            ec::FORBIDDEN
        );
        assert_eq!(s, before);
        s.renew_lease(1, MACHINE).unwrap();
    }

    #[test]
    fn remove_of_an_unknown_node_changes_nothing() {
        let mut s = founded("10.99.0.0/16");
        let before = s.clone();
        s.remove_node(9, &no_members()).unwrap();
        // Also when the id is somehow still listed as a member.
        s.remove_node(9, &members(&[1, 9])).unwrap();
        assert_eq!(s, before);
    }

    #[test]
    fn remove_is_refused_while_the_lease_is_live() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        pass(&mut s, DEFAULT_LEASE_SECS - 1);
        let before = s.clone();
        let err = s.remove_node(2, &members(&[1])).unwrap_err();
        assert_eq!(err.code, ec::NOT_READY);
        assert_eq!(err.message, "the lease of node 2 has not expired");
        assert_eq!(s, before);

        pass(&mut s, 1);
        s.remove_node(2, &members(&[1])).unwrap();
        assert!(!s.nodes.contains_key(&2));
    }

    /// Raft membership goes first: the mesh record of a member stays.
    #[test]
    fn remove_is_refused_while_the_node_is_a_member() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        pass(&mut s, DEFAULT_LEASE_SECS);
        let before = s.clone();
        let err = s.remove_node(2, &members(&[1, 2])).unwrap_err();
        assert_eq!(err.code, ec::NOT_READY);
        assert_eq!(err.message, "node 2 is still in the raft membership");
        assert_eq!(s, before);

        s.remove_node(2, &members(&[1])).unwrap();
        assert_eq!(s.nodes.keys().copied().collect::<Vec<_>>(), vec![1]);
        // A retry by a new leader finds nothing to do.
        let after = s.clone();
        s.remove_node(2, &members(&[1])).unwrap();
        assert_eq!(s, after);
    }

    /// Until `remove_node` runs, an expired node holds its mesh IP, subnet
    /// and name. Removing it frees all three.
    #[test]
    fn only_removal_frees_what_an_expired_node_holds() {
        // Two subnets, held by the founder and node 2.
        let mut s = founded_with_range("10.200.0.0/29", 30);
        join(&mut s, 2);
        pass(&mut s, DEFAULT_LEASE_SECS - 1);
        s.renew_lease(1, MACHINE).unwrap();
        pass(&mut s, 1);
        assert!(s.lease_expired(&s.nodes[&2]));
        assert!(!s.lease_expired(&s.nodes[&1]));

        // Expired, not removed: no subnet for a new name, and the name
        // still counts as held.
        let before = s.clone();
        assert_eq!(
            invite(&mut s, 3, 60, T0).unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_eq!(s, before);
        assert_eq!(s.name_holder("node-2").map(|n| n.node_id), Some(2));
        assert_eq!(s.free_ip(), Some(ip("10.99.0.3")));

        s.remove_node(2, &no_members()).unwrap();
        assert_eq!(s.free_ip(), Some(ip("10.99.0.2")));
        let a = invite_named(&mut s, 3, "node-2").unwrap();
        assert_eq!((a.mesh_ip, a.replaces), (ip("10.99.0.2"), None));
        register(&mut s, 3, &no_members()).unwrap();
        assert_eq!(s.nodes[&3].container_subnet, Some(net("10.200.0.4/30")));
        assert_invariants(&s);
    }

    #[test]
    fn set_lease_bounds() {
        let mut s = founded("10.99.0.0/16");
        let before = s.clone();
        for bad in [0, MIN_LEASE_SECS - 1, MAX_LEASE_SECS + 1, u64::MAX] {
            let err = s.set_lease(bad).unwrap_err();
            assert_eq!(err.code, ec::BAD_REQUEST, "{bad}");
            assert_eq!(err.message, "lease must be between 300 and 2592000 seconds");
        }
        assert_eq!(s, before);
        for good in [MIN_LEASE_SECS, 3_600, MAX_LEASE_SECS] {
            s.set_lease(good).unwrap();
            assert_eq!(s.lease_secs, good);
        }
    }

    /// Shortening the lease must not expire nodes that were in time under
    /// the old one: every lease starts again.
    #[test]
    fn set_lease_restarts_every_lease() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        join(&mut s, 3);
        pass(&mut s, 10_000);
        s.renew_lease(3, MACHINE).unwrap();
        pass(&mut s, 10_000);

        s.set_lease(MIN_LEASE_SECS).unwrap();
        assert_eq!(s.lease_secs, MIN_LEASE_SECS);
        assert_eq!(s.clock, 20_000, "the clock does not move");
        for n in s.nodes.values() {
            assert_eq!(n.lease_renewed_at, 20_000, "{n:?}");
            assert!(!s.lease_expired(n), "{n:?}");
        }

        // The new duration counts from here.
        pass(&mut s, MIN_LEASE_SECS - 1);
        assert!(s.nodes.values().all(|n| !s.lease_expired(n)));
        pass(&mut s, 1);
        assert!(s.nodes.values().all(|n| s.lease_expired(n)));
        assert_invariants(&s);
    }

    /// Expiry is sticky across a lease change: a node that had expired
    /// under the old duration is not restarted, and can still be removed.
    #[test]
    fn set_lease_shorter_leaves_an_expired_node_expired_and_removable() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        join(&mut s, 3);
        pass(&mut s, DEFAULT_LEASE_SECS - 1);
        s.renew_lease(1, MACHINE).unwrap();
        s.renew_lease(3, MACHINE).unwrap();
        pass(&mut s, 1);
        assert!(s.lease_expired(&s.nodes[&2]));

        s.set_lease(MIN_LEASE_SECS).unwrap();
        assert_eq!(s.nodes[&2].lease_renewed_at, 0);
        assert!(s.lease_expired(&s.nodes[&2]));
        assert_eq!(
            s.renew_lease(2, MACHINE).unwrap_err().code,
            ec::MESH_LEASE_EXPIRED
        );
        // The live nodes were restarted.
        for n in [1, 3] {
            assert_eq!(s.nodes[&n].lease_renewed_at, DEFAULT_LEASE_SECS);
            assert!(!s.lease_expired(&s.nodes[&n]));
        }

        s.remove_node(2, &members(&[1, 3])).unwrap();
        assert!(!s.nodes.contains_key(&2));
        assert_invariants(&s);
    }

    #[test]
    fn set_lease_restarts_a_live_node() {
        let mut s = founded("10.99.0.0/16");
        s.set_lease(MIN_LEASE_SECS).unwrap();
        // One second before the lease runs out.
        pass(&mut s, MIN_LEASE_SECS - 1);
        s.set_lease(MIN_LEASE_SECS).unwrap();
        assert_eq!(s.nodes[&1].lease_renewed_at, MIN_LEASE_SECS - 1);
        pass(&mut s, MIN_LEASE_SECS - 1);
        assert!(!s.lease_expired(&s.nodes[&1]));
        pass(&mut s, 1);
        assert!(s.lease_expired(&s.nodes[&1]));
    }

    /// A longer lease can un-expire a node by arithmetic. Its stamp is not
    /// touched, so it expires again once the longer lease has run out.
    #[test]
    fn set_lease_longer_only_delays_an_expired_node() {
        let mut s = founded("10.99.0.0/16");
        s.set_lease(MIN_LEASE_SECS).unwrap();
        pass(&mut s, MIN_LEASE_SECS);
        assert!(s.lease_expired(&s.nodes[&1]));

        s.set_lease(2 * MIN_LEASE_SECS).unwrap();
        assert_eq!(s.nodes[&1].lease_renewed_at, 0, "not restarted");
        assert!(!s.lease_expired(&s.nodes[&1]));
        pass(&mut s, MIN_LEASE_SECS);
        assert!(s.lease_expired(&s.nodes[&1]));
    }

    #[test]
    fn invite_name_is_validated() {
        let mut s = founded("10.99.0.0/16");
        let before = s.clone();
        // Names are required: an empty one is as bad as a malformed one.
        let too_long = "a".repeat(64);
        for bad in ["", "Web-1", "web_1", "-web", "web-", &too_long] {
            let err = invite_named(&mut s, 2, bad).unwrap_err();
            assert_eq!(err.code, ec::BAD_REQUEST, "{bad:?}");
        }
        assert_eq!(s, before, "a rejected invite must not consume a node id");
        let a = invite_named(&mut s, 2, "web-1").unwrap();
        assert_eq!((a.node_id, a.name.as_str()), (2, "web-1"));
    }

    /// A lost token must not block its name: the next invite for the name
    /// drops the older one and frees what it reserved.
    #[test]
    fn newer_invite_for_a_name_replaces_the_older_one() {
        // /30 mesh: one address besides the founder's. Two container
        // subnets: one besides the founder's.
        let mut s = MeshState::init(
            MeshConfig {
                cidr: net("10.0.0.0/30"),
                ..cfg_with_range("10.200.0.0/29", 30)
            },
            node(1, "10.0.0.1"),
        )
        .unwrap();
        let a = invite_named(&mut s, 2, "web").unwrap();
        assert_eq!((a.node_id, a.mesh_ip), (2, ip("10.0.0.2")));
        // Another name finds neither an address nor a subnet.
        assert_eq!(
            invite_named(&mut s, 3, "db").unwrap_err().code,
            ec::MESH_EXHAUSTED
        );

        let b = invite_named(&mut s, 3, "web").unwrap();
        assert_eq!(
            b,
            Allocation {
                node_id: 3,
                mesh_ip: ip("10.0.0.2"),
                expires_at: T0 + 60,
                name: "web".into(),
                replaces: None,
            }
        );
        assert_eq!(s.invites.keys().collect::<Vec<_>>(), vec![&id(3)]);
        assert_invariants(&s);

        // The older token is dead; the newer one registers.
        assert_eq!(
            register(&mut s, 2, &no_members()).unwrap_err().code,
            ec::INVITE_INVALID
        );
        register(&mut s, 3, &no_members()).unwrap();
        assert_eq!(s.nodes[&3].name, "web");
        assert_invariants(&s);
    }

    #[test]
    fn rejected_invite_keeps_the_older_invite_for_its_name() {
        let mut s = founded("10.99.0.0/16");
        invite_named(&mut s, 2, "web").unwrap();
        let before = s.clone();
        // The founder's key.
        let err = s
            .create_invite(id(3), "pk1".into(), ep(3), "web".into(), 60, T0)
            .unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);
        // The older invite's own id.
        let err = s
            .create_invite(id(2), "fresh".into(), ep(3), "web".into(), 60, T0)
            .unwrap_err();
        assert_eq!(err.code, ec::BAD_REQUEST);
        assert_eq!(s, before);
    }

    #[test]
    fn invite_reports_the_node_it_replaces() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        assert_eq!(invite_named(&mut s, 3, "web").unwrap().replaces, None);
        let a = invite_named(&mut s, 4, "node-2").unwrap();
        assert_eq!(a.replaces, Some(2));
        assert_eq!(a.name, "node-2");
        assert_eq!(invite_named(&mut s, 5, "node-1").unwrap().replaces, Some(1));
        // Creating the invite changes no node.
        assert_eq!(s.nodes.len(), 2);

        // An expired node still holds its name.
        pass(&mut s, DEFAULT_LEASE_SECS);
        assert_eq!(invite_named(&mut s, 6, "node-2").unwrap().replaces, Some(2));
        assert_invariants(&s);
    }

    /// An invite that replaces a node will reuse that node's subnet, so it
    /// needs none kept free.
    #[test]
    fn replacing_invite_does_not_count_towards_container_capacity() {
        // Two subnets, both held.
        let mut s = founded_with_range("10.200.0.0/29", 30);
        join(&mut s, 2);
        let before = s.clone();
        assert_eq!(
            invite_named(&mut s, 3, "web").unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_eq!(s, before);

        // Full, and still room to replace either node.
        assert_eq!(invite_named(&mut s, 3, "node-2").unwrap().replaces, Some(2));
        assert_eq!(invite_named(&mut s, 4, "node-1").unwrap().replaces, Some(1));
        assert_eq!(
            invite_named(&mut s, 5, "web").unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_invariants(&s);

        // Four subnets, three held: a live replacing invite leaves the free
        // one to the next new name, which then takes it.
        let mut s = founded_with_range("10.200.0.0/28", 30);
        join(&mut s, 2);
        join(&mut s, 3);
        invite_named(&mut s, 4, "node-2").unwrap();
        invite_named(&mut s, 5, "web").unwrap();
        assert_eq!(
            invite_named(&mut s, 6, "db").unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_invariants(&s);
    }

    /// The node an invite would replace can be removed by expiry before the
    /// invite registers. The invite then counts like any other and the
    /// registration allocates like any other.
    #[test]
    fn replacing_invite_survives_the_removal_of_its_node() {
        // Two subnets, both held.
        let mut s = founded_with_range("10.200.0.0/29", 30);
        join(&mut s, 2);
        assert_eq!(invite_named(&mut s, 3, "node-2").unwrap().replaces, Some(2));

        pass(&mut s, DEFAULT_LEASE_SECS - 1);
        s.renew_lease(1, MACHINE).unwrap();
        pass(&mut s, 1);
        s.remove_node(2, &members(&[1])).unwrap();

        // The freed subnet is spoken for by the invite.
        let before = s.clone();
        assert_eq!(
            invite_named(&mut s, 4, "web").unwrap_err().code,
            ec::MESH_EXHAUSTED
        );
        assert_eq!(s, before);

        register(&mut s, 3, &members(&[1])).unwrap();
        assert_eq!(s.nodes[&3].name, "node-2");
        assert_eq!(s.nodes[&3].container_subnet, Some(net("10.200.0.4/30")));
        assert_invariants(&s);
    }

    /// Not reachable through the commands, but registration must stay
    /// total: with the named node gone and nothing free it rejects.
    #[test]
    fn replacing_invite_without_its_node_rejects_when_nothing_is_free() {
        let mut s = founded_with_range("10.200.0.0/29", 30);
        join(&mut s, 2);
        invite_named(&mut s, 3, "node-2").unwrap();
        // Node 2 loses its name and keeps its subnet.
        s.nodes.get_mut(&2).unwrap().name = "other".into();
        let before = s.clone();
        let err = register(&mut s, 3, &members(&[1])).unwrap_err();
        assert_eq!(err.code, ec::MESH_EXHAUSTED);
        assert_eq!(s, before);
    }

    #[test]
    fn register_starts_the_lease_at_the_clock() {
        let mut s = founded("10.99.0.0/16");
        pass(&mut s, 500);
        join(&mut s, 2);
        assert_eq!(s.nodes[&2].lease_renewed_at, 500);
        assert!(!s.lease_expired(&s.nodes[&2]));
    }

    /// The new machine keeps the node id and mesh IP of its invite; the
    /// name and the subnet move to it, and the old record goes.
    #[test]
    fn takeover_keeps_the_subnet_and_removes_the_old_record() {
        let mut s = founded_with_range("10.200.0.0/16", 24);
        join(&mut s, 2);
        join(&mut s, 3);
        pass(&mut s, 700);
        assert_eq!(invite_named(&mut s, 4, "node-2").unwrap().replaces, Some(2));

        register(&mut s, 4, &members(&[1, 3])).unwrap();
        assert!(!s.nodes.contains_key(&2));
        assert!(s.invites.is_empty());
        assert_eq!(
            s.nodes[&4],
            NodeRecord {
                node_id: 4,
                public_key: "inv-pk4".into(),
                mesh_ip: ip("10.99.0.4"),
                endpoint: Some(ep(4)),
                invite_id: Some(id(4)),
                instance_id: Some(MACHINE.into()),
                container_subnet: Some(net("10.200.1.0/24")),
                name: "node-2".into(),
                lease_renewed_at: 700,
            }
        );
        assert_eq!(s.nodes[&3].container_subnet, Some(net("10.200.2.0/24")));
        assert_invariants(&s);

        // The old node's mesh IP is free again; its subnet is not.
        assert_eq!(s.free_ip(), Some(ip("10.99.0.2")));
        assert_eq!(
            s.free_container_subnet().unwrap(),
            Some(net("10.200.3.0/24"))
        );
    }

    #[test]
    fn takeover_is_refused_while_the_old_node_is_a_member() {
        let mut s = founded_with_range("10.200.0.0/16", 24);
        join(&mut s, 2);
        invite_named(&mut s, 3, "node-2").unwrap();
        let before = s.clone();
        let err = register(&mut s, 3, &members(&[1, 2])).unwrap_err();
        assert_eq!(err.code, ec::NOT_READY);
        assert_eq!(
            err.message,
            "node 2 holds the name node-2 and is still in the raft membership"
        );
        assert_eq!(s, before, "the invite and the old node both stay");

        // The joiner retries once node 2 has left the membership.
        register(&mut s, 3, &members(&[1])).unwrap();
        assert!(!s.nodes.contains_key(&2));
        assert_eq!(s.nodes[&3].container_subnet, Some(net("10.200.1.0/24")));
        assert_invariants(&s);
    }

    /// Expiry does not shortcut a takeover: an expired node that is still a
    /// raft member has to leave the membership first, like a live one.
    #[test]
    fn takeover_of_an_expired_node_follows_the_same_rule() {
        let mut s = founded_with_range("10.200.0.0/16", 24);
        join(&mut s, 2);
        invite_named(&mut s, 3, "node-2").unwrap();
        pass(&mut s, DEFAULT_LEASE_SECS);
        assert!(s.lease_expired(&s.nodes[&2]));
        let before = s.clone();
        assert_eq!(
            register(&mut s, 3, &members(&[1, 2])).unwrap_err().code,
            ec::NOT_READY
        );
        assert_eq!(s, before);

        register(&mut s, 3, &members(&[1])).unwrap();
        assert!(!s.nodes.contains_key(&2));
        assert_eq!(s.nodes[&3].container_subnet, Some(net("10.200.1.0/24")));
        assert_eq!(s.nodes[&3].lease_renewed_at, DEFAULT_LEASE_SECS);
        assert!(!s.lease_expired(&s.nodes[&3]));
        assert_invariants(&s);
    }

    /// The retry check runs before the takeover, so a repeated registration
    /// removes nobody, whatever the membership is by then.
    #[test]
    fn retry_after_takeover_is_idempotent() {
        let mut s = founded_with_range("10.200.0.0/16", 24);
        join(&mut s, 2);
        invite_named(&mut s, 3, "node-2").unwrap();
        register(&mut s, 3, &members(&[1])).unwrap();
        let after = s.clone();

        pass(&mut s, 10);
        let after = MeshState { clock: 10, ..after };
        for m in [members(&[1]), members(&[1, 3]), members(&[1, 2, 3])] {
            register(&mut s, 3, &m).unwrap();
            assert_eq!(s, after);
        }
        // Another machine with the same token is still turned away.
        assert_eq!(
            s.register_node(&id(3), 3, &ep(3), OTHER_MACHINE, T0, &members(&[1]))
                .unwrap_err()
                .code,
            ec::INVITE_USED
        );
        assert_eq!(s, after);
        assert_invariants(&s);
    }

    #[test]
    fn takeover_of_a_node_without_a_subnet_allocates_one() {
        // No container range: nobody has a subnet before or after.
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        invite_named(&mut s, 3, "node-2").unwrap();
        register(&mut s, 3, &members(&[1])).unwrap();
        assert!(!s.nodes.contains_key(&2));
        assert_eq!(s.nodes[&3].container_subnet, None);
        assert_invariants(&s);

        // A range, and an old record that holds no subnet (made by hand):
        // the lowest free one.
        let mut s = founded_with_range("10.200.0.0/16", 24);
        join(&mut s, 2);
        join(&mut s, 3);
        s.nodes.get_mut(&2).unwrap().container_subnet = None;
        invite_named(&mut s, 4, "node-2").unwrap();
        register(&mut s, 4, &members(&[1, 3])).unwrap();
        assert!(!s.nodes.contains_key(&2));
        assert_eq!(s.nodes[&4].container_subnet, Some(net("10.200.1.0/24")));
        assert_invariants(&s);
    }

    /// Records stored before names existed have an empty name. An empty
    /// name is held by nobody, so such an invite replaces nothing.
    #[test]
    fn invite_without_a_name_takes_over_nothing() {
        let mut s = founded("10.99.0.0/16");
        join(&mut s, 2);
        s.nodes.get_mut(&2).unwrap().name = String::new();
        s.invites.insert(
            id(3),
            InviteRecord {
                invite_id: id(3),
                node_id: 3,
                public_key: "inv-pk3".into(),
                mesh_ip: ip("10.99.0.3"),
                endpoint: ep(3),
                expires_at: T0 + 60,
                name: String::new(),
            },
        );
        s.next_node_id = 4;
        assert_eq!(s.name_holder("").map(|n| n.node_id), None);
        register(&mut s, 3, &members(&[1, 2])).unwrap();
        assert_eq!(s.nodes.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    /// A cluster through joins, a lease change, an expiry, a removal, a
    /// takeover and a rejoin, checking the invariants after every step.
    #[test]
    fn invariants_hold_through_a_cluster_lifetime() {
        let mut s = founded_with_range("10.200.0.0/28", 30);
        let mut raft = members(&[1]);
        assert_invariants(&s);

        for n in 2..=4 {
            invite(&mut s, n, 60, T0).unwrap();
            assert_invariants(&s);
            register(&mut s, n, &raft).unwrap();
            raft.insert(u64::from(n));
            assert_invariants(&s);
        }
        // Four subnets, four nodes.
        assert_eq!(
            invite_named(&mut s, 5, "web").unwrap_err().code,
            ec::MESH_EXHAUSTED
        );

        s.set_lease(600).unwrap();
        assert_invariants(&s);

        // Node 4 stops renewing; the others keep their leases.
        for _ in 0..4 {
            pass(&mut s, 150);
            for n in 1..=3 {
                s.renew_lease(n, MACHINE).unwrap();
            }
            assert_invariants(&s);
        }
        assert!(s.lease_expired(&s.nodes[&4]));
        assert_eq!(
            s.renew_lease(4, MACHINE).unwrap_err().code,
            ec::MESH_LEASE_EXPIRED
        );
        assert_eq!(s.remove_node(4, &raft).unwrap_err().code, ec::NOT_READY);
        raft.remove(&4);
        s.remove_node(4, &raft).unwrap();
        assert_invariants(&s);

        // Node 3 is replaced by a new machine under its name.
        assert_eq!(invite_named(&mut s, 5, "node-3").unwrap().replaces, Some(3));
        assert_invariants(&s);
        assert_eq!(register(&mut s, 5, &raft).unwrap_err().code, ec::NOT_READY);
        assert_invariants(&s);
        raft.remove(&3);
        register(&mut s, 5, &raft).unwrap();
        raft.insert(5);
        assert_invariants(&s);
        register(&mut s, 5, &raft).unwrap();
        assert_invariants(&s);

        // The name and addresses node 4 held go to a new node.
        let a = invite_named(&mut s, 6, "node-4").unwrap();
        assert_eq!((a.replaces, a.mesh_ip), (None, ip("10.99.0.3")));
        register(&mut s, 6, &raft).unwrap();
        raft.insert(6);
        assert_invariants(&s);

        let held: Vec<(u64, &str, Option<Ipv4Net>)> = s
            .nodes
            .values()
            .map(|n| (n.node_id, n.name.as_str(), n.container_subnet))
            .collect();
        assert_eq!(
            held,
            vec![
                (1, "node-1", Some(net("10.200.0.0/30"))),
                (2, "node-2", Some(net("10.200.0.4/30"))),
                (5, "node-3", Some(net("10.200.0.8/30"))),
                (6, "node-4", Some(net("10.200.0.12/30"))),
            ]
        );
        assert!(s.invites.is_empty());
    }

    #[test]
    fn set_endpoint_updates_known_node_only() {
        let mut s = founded("10.99.0.0/16");
        s.set_endpoint(1, "192.0.2.1:51820".into()).unwrap();
        assert_eq!(s.nodes[&1].endpoint.as_deref(), Some("192.0.2.1:51820"));
        let before = s.clone();
        assert_eq!(
            s.set_endpoint(9, "192.0.2.9:51820".into())
                .unwrap_err()
                .code,
            ec::NOT_FOUND
        );
        assert_eq!(s, before);
    }
}
