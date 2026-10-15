//! Transport-neutral facade over Raft for cluster operations.
//!
//! Operations that need the leader are forwarded server-side to the leader's
//! peer listener (`/raft/cluster/*`). A forwarded request carries
//! [`FORWARDED_HEADER`]; a node that receives one and is not the leader
//! answers 503 `not_leader` rather than forwarding again, so there are no loops.
//!
//! The lease operations live here too: reading and setting the lease
//! duration, renewing a node's lease, and taking a node out of the raft
//! membership (for an expired lease, and for a takeover of its name). The
//! timer that drives expiry is in [`crate::lease`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use faber_api_types::{
    ClusterStatus, CreateInviteRequest, ErrorBody, FORWARDED_HEADER, HealthResponse,
    InviteResponse, JoinRequest, LeaseResponse, MemberStatus, Role, StatusIntervalResponse,
    error_code,
};
use faber_container_runtime::ContainerRuntimeSpec;
use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::{ChangeMembers, RaftMetrics, ServerState};
use serde::Serialize;

use crate::dto::{
    PeerEndpointRequest, PeerInviteRequest, PeerInviteResponse, PeerJoinRequest, PeerLeaseRequest,
    PeerRegisterRequest, PeerRegisterResponse, PeerRenewRequest, PeerStatusIntervalRequest,
    ReplicationStatus,
};
use crate::lease;
use crate::mesh::identity::FOUNDER_NODE_ID;
use crate::mesh::state::{DEFAULT_INVITE_TTL_SECS, MAX_INVITE_TTL_SECS};
use crate::mesh::token::TOKEN_VERSION;
use crate::mesh::{
    InviteToken, JoinPending, MeshConfig, MeshState, NodeRecord, TokenPeer, generate_keypair,
    parse_endpoint, parse_full_endpoint, validate_public_key,
};
use crate::raft::store::StateReader;
use crate::raft::store::status::{ReportNode, SetStatusInterval};
use crate::raft::types::{Node, NodeId, Raft, Request, Response};
use crate::secrets::{ClusterKeyProvider, NoClusterKey};

/// How long `init` and `join` wait for the local node to see the result.
const SETTLE_WAIT: Duration = Duration::from_secs(10);
/// A join can include a full snapshot transfer to the new learner.
const JOIN_TIMEOUT: Duration = Duration::from_secs(120);
/// How long a repeated join waits for a node that is already a learner to
/// catch up before it answers `not_ready`. The joiner retries.
const CATCH_UP_WAIT: Duration = Duration::from_secs(5);
/// A forwarded invite, registration, endpoint change, renewal or lease change
/// is one raft write on the leader. A registration that takes over a name
/// changes the raft membership first.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(30);
/// Replication info is best effort; don't hold up `status` for long.
const STATUS_FETCH_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ServiceError {
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("cluster is already initialized")]
    AlreadyInitialized,
    #[error("this node has already joined a cluster")]
    AlreadyJoined,
    /// Only returned for requests that were already forwarded once.
    #[error("not the leader; leader is node {leader_id} at {leader_addr}")]
    NotLeader {
        leader_id: NodeId,
        leader_addr: String,
    },
    #[error("no leader is known")]
    NoLeader,
    /// The leader cannot do this yet; the caller should retry.
    #[error("not ready: {0}")]
    NotReady(String),
    #[error("peer unreachable: {0}")]
    PeerUnreachable(String),
    /// A mesh command or request refused with a specific `error_code`
    /// (`invite_*`, `mesh_*`). The HTTP status depends on the code.
    #[error("{code}: {message}")]
    Rejected { code: String, message: String },
    /// Refused by the Host/Origin guard, or a peer request from an address
    /// that may not make it.
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// A non-2xx answer from a peer, passed through unchanged.
    #[error("peer returned {status}: {}", body.error)]
    Upstream { status: u16, body: ErrorBody },
    #[error("internal error: {0}")]
    Internal(String),
}

impl ServiceError {
    /// The `error_code` a client sees for this error, when it is one of the
    /// mesh codes (`invite_*`, `mesh_*`), including one passed through from
    /// a peer.
    pub fn mesh_code(&self) -> Option<&str> {
        match self {
            ServiceError::Rejected { code, .. } => Some(code.as_str()),
            ServiceError::Upstream { body, .. } => Some(body.error.as_str()),
            _ => None,
        }
        .filter(|c| c.starts_with("invite_") || c.starts_with("mesh_"))
    }
}

pub type ServiceResult<T> = Result<T, ServiceError>;

/// Current Unix time in seconds, stamped as `at` on mesh commands.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// This node's place in the WireGuard mesh, from `identity.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshLocal {
    /// Cluster-wide settings the founder's `init` seeds, including the
    /// container range (`--container-cidr`).
    pub config: MeshConfig,
    /// This node as `InitCluster` records it. Its `instance_id` is the one
    /// this node renews its lease with ([`FaberService::renew_own_lease`]).
    pub record: NodeRecord,
    /// Mesh IPs of the peers in this node's join token: the nodes a joiner
    /// takes raft RPCs from while `join_pending` says so
    /// ([`FaberService::raft_source_allowed`]). Empty for a founder.
    pub bootstrap_peers: BTreeSet<Ipv4Addr>,
    /// Whether this node is still joining.
    pub join_pending: JoinPending,
}

#[derive(Clone)]
pub struct FaberService {
    inner: Arc<Inner>,
    /// Where this node gets the cluster key that seals and opens env values.
    cluster_keys: Arc<dyn ClusterKeyProvider>,
}

struct Inner {
    node_id: NodeId,
    /// This node's peer listener address, as stored in the membership config.
    peer_addr: String,
    raft: Raft,
    http: reqwest::Client,
    state: StateReader,
    /// Set in mesh mode.
    mesh: Option<MeshLocal>,
    /// Check where peer requests come from: a joiner's address against its
    /// invite's mesh IP, the source of an endpoint change or a lease
    /// renewal against the node's mesh IP, and the source of a raft RPC
    /// against the registered nodes. Off when this node's own peer listener
    /// is loopback (single-host tests and dev), where every node has the
    /// same address.
    strict_peer_source: bool,
    /// The clocks of the lease task, set once at startup. Readers of node
    /// status need the length of a tick of the lease clock.
    lease_timing: OnceLock<lease::LeaseTiming>,
    /// Set when the log line about a cluster key that is not the cluster's
    /// has been written, so that it is written once.
    key_mismatch_logged: AtomicBool,
}

impl FaberService {
    pub fn new(
        node_id: NodeId,
        peer_addr: String,
        raft: Raft,
        state: StateReader,
        mesh: Option<MeshLocal>,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;
        let strict_peer_source = !peer_addr
            .parse::<SocketAddr>()
            .is_ok_and(|a| a.ip().is_loopback());
        Ok(Self {
            inner: Arc::new(Inner {
                node_id,
                peer_addr,
                raft,
                http,
                state,
                mesh,
                strict_peer_source,
                lease_timing: OnceLock::new(),
                key_mismatch_logged: AtomicBool::new(false),
            }),
            cluster_keys: Arc::new(NoClusterKey),
        })
    }

    /// Takes the cluster key from `provider`. Without one every deploy and
    /// every revealing read fails with `key_unavailable`.
    pub fn with_cluster_keys(mut self, provider: Arc<dyn ClusterKeyProvider>) -> Self {
        self.cluster_keys = provider;
        self
    }

    /// True the first time it is called: the caller logs a cluster key
    /// mismatch then and not again.
    pub(crate) fn first_key_mismatch(&self) -> bool {
        !self.inner.key_mismatch_logged.swap(true, Ordering::Relaxed)
    }

    pub(crate) fn cluster_keys(&self) -> &Arc<dyn ClusterKeyProvider> {
        &self.cluster_keys
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub fn raft(&self) -> &Raft {
        &self.inner.raft
    }

    fn metrics(&self) -> RaftMetrics<NodeId, Node> {
        self.inner.raft.metrics().borrow().clone()
    }

    /// `status` is [`HEALTH_OK`], or [`HEALTH_UNAVAILABLE`] when
    /// [`Self::health_problem`] finds one.
    pub fn health(&self) -> HealthResponse {
        let status = match self.health_problem() {
            None => HEALTH_OK,
            Some(_) => HEALTH_UNAVAILABLE,
        };
        HealthResponse {
            status: status.to_string(),
            node_id: self.inner.node_id,
        }
    }

    /// Why this node cannot do its work, if it cannot: the raft core has
    /// stopped (a fatal storage error, a panic, a shutdown) or the storage is
    /// closed. A node in this state answers requests but never makes
    /// progress again; only a restart helps.
    pub fn health_problem(&self) -> Option<String> {
        if let Some(reason) = raft_stopped_reason(&self.inner.raft.metrics()) {
            return Some(format!("the raft core is not running: {reason}"));
        }
        if self.inner.state.is_closed() {
            return Some("the storage is closed".into());
        }
        None
    }

    /// Local Raft view plus, when a leader is known, its replication progress.
    pub async fn status(&self) -> ClusterStatus {
        let m = self.metrics();
        let membership = m.membership_config.membership();
        let id = self.inner.node_id;
        let member = membership.get_node(&id).is_some();
        // A learner is in the membership but has not joined yet: it does not
        // vote, and its join is still being retried.
        let joined = membership.voter_ids().any(|v| v == id);
        let leader_id = m.current_leader;
        let leader_addr = leader_id
            .and_then(|l| membership.get_node(&l))
            .map(|n| n.addr.clone());

        let replication = match (leader_id, &leader_addr) {
            (Some(l), _) if l == id => self.replication().ok(),
            (Some(_), Some(addr)) if member => self.fetch_replication(addr).await,
            _ => None,
        };

        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        // Names come from the mesh; a cluster without one has none.
        let mesh = self.read_mesh().ok().flatten();
        let name_of = |nid: &NodeId| {
            let node = mesh.as_ref()?.nodes.get(nid)?;
            Some(node.name.clone()).filter(|n| !n.is_empty())
        };
        let members = membership
            .nodes()
            .map(|(nid, node)| {
                let matched = replication
                    .as_ref()
                    .and_then(|r| r.matched.get(nid).copied().flatten());
                let lag = replication
                    .as_ref()
                    .and_then(|r| Some(r.last_log_index?.saturating_sub(matched?)));
                MemberStatus {
                    node_id: *nid,
                    addr: node.addr.clone(),
                    voter: voters.contains(nid),
                    matched_index: matched,
                    lag,
                    name: name_of(nid),
                }
            })
            .collect();

        ClusterStatus {
            node_id: id,
            joined,
            role: role(&m.state),
            term: m.current_term,
            leader_id,
            leader_addr,
            members,
        }
    }

    /// Replication progress; only the leader has it.
    pub fn replication(&self) -> ServiceResult<ReplicationStatus> {
        let m = self.metrics();
        let id = self.inner.node_id;
        let Some(repl) = m
            .replication
            .as_ref()
            .filter(|_| m.current_leader == Some(id))
        else {
            return Err(self.not_leader(&m));
        };
        let mut matched: BTreeMap<NodeId, Option<u64>> = repl
            .iter()
            .map(|(nid, log_id)| (*nid, log_id.map(|l| l.index)))
            .collect();
        // The leader's own log is by definition fully matched.
        matched.entry(id).or_insert(m.last_log_index);
        Ok(ReplicationStatus {
            leader_id: id,
            last_log_index: m.last_log_index,
            matched,
        })
    }

    async fn fetch_replication(&self, leader_addr: &str) -> Option<ReplicationStatus> {
        let resp = self
            .inner
            .http
            .get(format!("http://{leader_addr}/raft/cluster/status"))
            .timeout(STATUS_FETCH_TIMEOUT)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json().await.ok()
    }

    /// Makes this node a single-node cluster.
    ///
    /// In mesh mode the founder also proposes `InitCluster`, seeding `mesh`
    /// and `config/containerd`. If an earlier init stopped between the two
    /// steps, a repeated init on the leader completes the seed. A node that
    /// joined (or is joining) from an invite cannot found a cluster.
    ///
    /// The seed carries the container range, and apply gives the founder
    /// its first subnet. A range apply rejects is a `BadRequest`, with raft
    /// already initialised; `Config::validate` refuses such a range at
    /// startup so that it does not get this far.
    pub async fn init(&self) -> ServiceResult<ClusterStatus> {
        let id = self.inner.node_id;
        if let Some(mesh) = &self.inner.mesh
            && mesh.record.node_id != FOUNDER_NODE_ID
        {
            let member = self
                .metrics()
                .membership_config
                .membership()
                .get_node(&id)
                .is_some();
            return Err(if member {
                ServiceError::AlreadyInitialized
            } else {
                ServiceError::BadRequest(format!(
                    "node {id} joins an existing mesh with its invite; it cannot create a cluster"
                ))
            });
        }

        // The key file is there before the cluster is, so that a restart
        // between the two finds it and keeps it.
        let fresh = self
            .metrics()
            .membership_config
            .membership()
            .nodes()
            .next()
            .is_none();
        if fresh {
            self.create_cluster_key_file().await?;
        }
        let nodes = BTreeMap::from([(id, Node::new(&self.inner.peer_addr))]);
        match self.inner.raft.initialize(nodes).await {
            Ok(()) => {}
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                // Raft is initialised. In mesh mode with no `mesh` yet, an
                // earlier init stopped before the seed: finish it below
                // (which needs this node to be the leader) instead of
                // reporting a half-made cluster as initialised.
                let unseeded = self.inner.mesh.is_some() && self.read_mesh()?.is_none();
                if !unseeded {
                    return Err(ServiceError::AlreadyInitialized);
                }
            }
            Err(RaftError::APIError(InitializeError::NotInMembers(e))) => {
                return Err(ServiceError::Internal(e.to_string()));
            }
            Err(RaftError::Fatal(e)) => return Err(ServiceError::Internal(e.to_string())),
        }

        // Give the first election a chance so a call straight after init works.
        let _ = self
            .inner
            .raft
            .wait(Some(SETTLE_WAIT))
            .metrics(|m| m.current_leader.is_some(), "leader after init")
            .await;

        if let Some(mesh) = &self.inner.mesh {
            let seed = Request::InitCluster {
                mesh: mesh.config,
                founder: mesh.record.clone(),
                containerd: ContainerRuntimeSpec::default(),
            };
            match self.propose(seed).await {
                Ok(_) | Err(ServiceError::AlreadyInitialized) => {}
                Err(e) => return Err(e),
            }
        }
        self.create_and_record_cluster_key().await?;
        Ok(self.status().await)
    }

    /// `POST /api/v1/mesh/invites`: reserves a node ID and mesh IP for a new
    /// node named `req.name` and returns its invite token.
    ///
    /// The keypair is generated here; only the public key goes to the leader
    /// (and into the log). The private key exists only in the returned token.
    pub async fn invite(&self, req: CreateInviteRequest) -> ServiceResult<InviteResponse> {
        let ttl_secs = req.ttl_secs.unwrap_or(DEFAULT_INVITE_TTL_SECS);
        if ttl_secs == 0 || ttl_secs > MAX_INVITE_TTL_SECS {
            return Err(ServiceError::BadRequest(format!(
                "ttl_secs must be between 1 and {MAX_INVITE_TTL_SECS}"
            )));
        }
        let mesh = self.read_mesh()?.ok_or_else(mesh_not_configured)?;
        let endpoint = parse_endpoint(&req.endpoint, mesh.config.wg_port)
            .map_err(|e| ServiceError::BadRequest(e.to_string()))?
            .to_string();

        let (private_key, public_key) =
            generate_keypair().map_err(|e| ServiceError::Internal(e.to_string()))?;
        let invite_id = random_invite_id()?;
        let peer_req = PeerInviteRequest {
            invite_id: invite_id.clone(),
            public_key,
            endpoint: endpoint.clone(),
            name: req.name,
            ttl_secs,
        };
        let r = self.submit_invite(peer_req, false).await?;

        let token = InviteToken {
            v: TOKEN_VERSION,
            invite_id,
            expires_at: r.expires_at,
            node_id: r.node_id,
            mesh_ip: r.mesh_ip,
            mesh_cidr: r.mesh_cidr,
            endpoint,
            private_key: private_key.expose().to_string(),
            peers: r.peers,
            join_addrs: r.join_addrs,
        };
        tracing::info!(
            node_id = r.node_id,
            mesh_ip = %r.mesh_ip,
            expires_at = r.expires_at,
            "invite created"
        );
        Ok(InviteResponse {
            token: token.encode(),
            node_id: r.node_id,
            mesh_ip: r.mesh_ip,
            expires_at: r.expires_at,
            name: r.name,
            replaces_node_id: r.replaces,
        })
    }

    /// Handles `POST /raft/cluster/invites`: the leader proposes
    /// `CreateInvite`; a follower forwards once.
    ///
    /// The only node that has a reason to send it is a follower forwarding
    /// its own loopback request, so `source` must be the mesh IP of a
    /// registered node, forwarded or not. Anything else is `Forbidden`. The
    /// peer firewall also lets in machines that only hold an invite. One of
    /// those could otherwise invite itself under the name of a live node
    /// and take that node over.
    ///
    /// Not checked when this node's own peer listener is loopback (see
    /// `strict_peer_source`). An invite this node makes itself
    /// ([`Self::invite`]) does not come through here.
    pub async fn peer_invite(
        &self,
        req: PeerInviteRequest,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<PeerInviteResponse> {
        if self.inner.strict_peer_source {
            let mesh = self.read_mesh()?;
            if !source_is_registered(mesh.as_ref(), source.ip()) {
                tracing::warn!(
                    source = %source.ip(),
                    forwarded,
                    "refused an invite request from an address that is no registered node"
                );
                return Err(ServiceError::Forbidden(format!(
                    "{} may not create invites",
                    source.ip()
                )));
            }
        }
        self.submit_invite(req, forwarded).await
    }

    /// Proposes `CreateInvite` on the leader, or forwards the request to it.
    async fn submit_invite(
        &self,
        mut req: PeerInviteRequest,
        forwarded: bool,
    ) -> ServiceResult<PeerInviteResponse> {
        check_invite_id(&req.invite_id)?;
        validate_public_key(&req.public_key)
            .map_err(|e| ServiceError::BadRequest(format!("public_key: {e}")))?;
        req.endpoint = canonical_endpoint(&req.endpoint)?;
        match self.forward_target(forwarded)? {
            None => self.invite_as_leader(req).await,
            Some(leader_addr) => {
                let bytes = self
                    .post_peer(&leader_addr, "/raft/cluster/invites", &req, true)
                    .await?;
                serde_json::from_slice(&bytes).map_err(|e| {
                    ServiceError::Internal(format!("bad invite response from {leader_addr}: {e}"))
                })
            }
        }
    }

    async fn invite_as_leader(&self, req: PeerInviteRequest) -> ServiceResult<PeerInviteResponse> {
        let mesh = self.read_mesh()?.ok_or_else(mesh_not_configured)?;
        if !mesh.nodes.values().any(|n| n.endpoint.is_some()) {
            return Err(ServiceError::Rejected {
                code: error_code::MESH_NO_ENDPOINTS.into(),
                message: "no mesh node has a WireGuard endpoint a new node could reach".into(),
            });
        }
        let resp = self
            .propose(Request::CreateInvite {
                invite_id: req.invite_id,
                public_key: req.public_key,
                endpoint: req.endpoint,
                name: req.name,
                ttl_secs: req.ttl_secs,
                at: now_secs(),
            })
            .await?;
        let Response::Invited {
            node_id,
            mesh_ip,
            expires_at,
            name,
            replaces,
        } = resp
        else {
            return Err(ServiceError::Internal(format!(
                "unexpected CreateInvite response {resp:?}"
            )));
        };
        let mesh = self.read_mesh()?.ok_or_else(mesh_not_configured)?;
        let peers = mesh
            .nodes
            .values()
            .map(|n| TokenPeer {
                node_id: n.node_id,
                public_key: n.public_key.clone(),
                endpoint: n.endpoint.clone(),
                mesh_ip: n.mesh_ip,
            })
            .collect();
        Ok(PeerInviteResponse {
            node_id,
            mesh_ip,
            expires_at,
            mesh_cidr: mesh.config.cidr,
            peers,
            join_addrs: self.join_addrs(),
            name,
            replaces,
        })
    }

    /// `GET /api/v1/mesh/lease`: the lease duration in this node's copy of
    /// the replicated state. `mesh_not_configured` without a mesh.
    pub async fn lease(&self) -> ServiceResult<LeaseResponse> {
        let mesh = self.read_mesh()?.ok_or_else(mesh_not_configured)?;
        Ok(LeaseResponse {
            lease_secs: mesh.lease_secs,
        })
    }

    /// `PUT /api/v1/mesh/lease` and `POST /raft/cluster/lease`: the leader
    /// proposes `SetLease`; a follower forwards once. Apply checks the
    /// bounds, so a duration outside them is a `bad_request` from whichever
    /// node proposed it.
    pub async fn set_lease(&self, secs: u64, forwarded: bool) -> ServiceResult<LeaseResponse> {
        match self.forward_target(forwarded)? {
            None => {
                self.read_mesh()?.ok_or_else(mesh_not_configured)?;
                self.propose(Request::SetLease { secs }).await?;
                Ok(LeaseResponse { lease_secs: secs })
            }
            Some(leader_addr) => {
                let req = PeerLeaseRequest { lease_secs: secs };
                let bytes = self
                    .post_peer(&leader_addr, "/raft/cluster/lease", &req, true)
                    .await?;
                serde_json::from_slice(&bytes).map_err(|e| {
                    ServiceError::Internal(format!("bad lease response from {leader_addr}: {e}"))
                })
            }
        }
    }

    /// Handles `POST /raft/cluster/lease`: [`Self::set_lease`] for a request
    /// from another node.
    ///
    /// The only node that has a reason to send it is a follower forwarding
    /// its own loopback request, so `source` must be the mesh IP of a
    /// registered node, forwarded or not. Anything else is `Forbidden`: the
    /// peer firewall also lets in machines that only hold an invite, and
    /// those may not change the lease of the cluster.
    ///
    /// Not checked when this node's own peer listener is loopback (see
    /// `strict_peer_source`).
    pub async fn peer_set_lease(
        &self,
        req: PeerLeaseRequest,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<LeaseResponse> {
        if self.inner.strict_peer_source {
            let mesh = self.read_mesh()?;
            if !source_is_registered(mesh.as_ref(), source.ip()) {
                tracing::warn!(
                    source = %source.ip(),
                    forwarded,
                    "refused a lease change from an address that is no registered node"
                );
                return Err(ServiceError::Forbidden(format!(
                    "{} may not change the lease",
                    source.ip()
                )));
            }
        }
        self.set_lease(req.lease_secs, forwarded).await
    }

    /// Renews this node's own lease (`RenewLease`), through the leader.
    ///
    /// When the mesh no longer has this node (it was removed after its
    /// lease ran out, or another machine took its name) the error's
    /// [`ServiceError::mesh_code`] is `mesh_node_removed`, whether this node
    /// is the leader or the answer came from it. `mesh_lease_expired` says
    /// that the lease has run out but the node is still in the mesh, and
    /// maybe still a voter: no renewal will be accepted, and the node has
    /// not been removed. Every other error is worth a retry.
    pub async fn renew_own_lease(&self) -> ServiceResult<()> {
        let mesh = self.inner.mesh.as_ref().ok_or_else(mesh_not_configured)?;
        let instance_id = mesh.record.instance_id.clone().ok_or_else(|| {
            ServiceError::BadRequest("this node has no instance id to renew its lease with".into())
        })?;
        let req = PeerRenewRequest {
            node_id: self.inner.node_id,
            instance_id,
        };
        self.submit_renew(req, false).await
    }

    /// Handles `POST /raft/cluster/renew`: the leader proposes `RenewLease`;
    /// a follower forwards once.
    ///
    /// `source` is checked as for [`Self::peer_set_endpoint`]: a node may
    /// only renew its own lease, so the request must come from the mesh IP
    /// of `req.node_id`, or be a forward from the mesh IP of a registered
    /// node. Anything else is `Forbidden`. Apply also refuses a renewal by
    /// another machine than the one the node is registered to.
    pub async fn peer_renew(
        &self,
        req: PeerRenewRequest,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<()> {
        check_hex_id("instance_id", &req.instance_id)?;
        if self.inner.strict_peer_source {
            let mesh = self.read_mesh()?;
            if !source_may_act_for(mesh.as_ref(), req.node_id, source.ip(), forwarded) {
                tracing::warn!(
                    node_id = req.node_id,
                    source = %source.ip(),
                    forwarded,
                    "refused a lease renewal from an address that may not make it"
                );
                return Err(ServiceError::Forbidden(format!(
                    "{} may not renew the lease of node {}",
                    source.ip(),
                    req.node_id
                )));
            }
        }
        self.submit_renew(req, forwarded).await
    }

    /// Proposes `RenewLease` on the leader, or forwards the request to it.
    async fn submit_renew(&self, req: PeerRenewRequest, forwarded: bool) -> ServiceResult<()> {
        match self.forward_target(forwarded)? {
            None => {
                let renew = Request::RenewLease {
                    node_id: req.node_id,
                    instance_id: req.instance_id,
                };
                self.propose(renew).await.map(drop)
            }
            Some(leader_addr) => self
                .post_peer(&leader_addr, "/raft/cluster/renew", &req, true)
                .await
                .map(drop),
        }
    }

    /// `GET /api/v1/mesh/status-interval`: seconds between two status
    /// reports of a node, in this node's copy of the replicated state.
    pub async fn status_interval(&self) -> ServiceResult<StatusIntervalResponse> {
        let interval_secs = self
            .inner
            .state
            .status_interval()
            .map_err(|e| ServiceError::Internal(e.to_string()))?;
        Ok(StatusIntervalResponse { interval_secs })
    }

    /// `PUT /api/v1/mesh/status-interval` and `POST
    /// /raft/cluster/status-interval`: the leader proposes
    /// `SetStatusInterval`; a follower forwards once. Apply checks the
    /// bounds, so a value outside them is a `bad_request` from whichever
    /// node proposed it.
    pub async fn set_status_interval(
        &self,
        secs: u64,
        forwarded: bool,
    ) -> ServiceResult<StatusIntervalResponse> {
        match self.forward_target(forwarded)? {
            None => {
                let cmd = SetStatusInterval { secs };
                self.propose(Request::SetStatusInterval(cmd)).await?;
                Ok(StatusIntervalResponse {
                    interval_secs: secs,
                })
            }
            Some(leader_addr) => {
                let req = PeerStatusIntervalRequest {
                    interval_secs: secs,
                };
                let bytes = self
                    .post_peer(&leader_addr, "/raft/cluster/status-interval", &req, true)
                    .await?;
                serde_json::from_slice(&bytes).map_err(|e| {
                    ServiceError::Internal(format!(
                        "bad status interval response from {leader_addr}: {e}"
                    ))
                })
            }
        }
    }

    /// Handles `POST /raft/cluster/status-interval`:
    /// [`Self::set_status_interval`] for a request from another node. As for
    /// [`Self::peer_set_lease`], the source must be the mesh IP of a
    /// registered node, forwarded or not.
    pub async fn peer_set_status_interval(
        &self,
        req: PeerStatusIntervalRequest,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<StatusIntervalResponse> {
        if self.inner.strict_peer_source {
            let mesh = self.read_mesh()?;
            if !source_is_registered(mesh.as_ref(), source.ip()) {
                tracing::warn!(
                    source = %source.ip(),
                    forwarded,
                    "refused a status interval change from an address that is no registered node"
                );
                return Err(ServiceError::Forbidden(format!(
                    "{} may not change the status interval",
                    source.ip()
                )));
            }
        }
        self.set_status_interval(req.interval_secs, forwarded).await
    }

    /// Proposes `ReportNode` on the leader, or forwards the report to it
    /// once. Apply refuses a node the mesh does not have (`mesh_node_removed`)
    /// and another machine than the one the node is registered to
    /// (`forbidden`).
    pub async fn report_node(&self, report: ReportNode, forwarded: bool) -> ServiceResult<()> {
        match self.forward_target(forwarded)? {
            None => self.propose(Request::ReportNode(report)).await.map(drop),
            Some(leader_addr) => self
                .post_peer(&leader_addr, "/raft/cluster/status", &report, true)
                .await
                .map(drop),
        }
    }

    /// Handles `POST /raft/cluster/status`. `source` is checked as for
    /// [`Self::peer_renew`]: a node may only report for itself, so the report
    /// must come from the mesh IP of `report.node_id`, or be a forward from
    /// the mesh IP of a registered node. Anything else is `Forbidden`. Apply
    /// holds the size limits.
    pub async fn peer_report(
        &self,
        report: ReportNode,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<()> {
        check_hex_id("instance_id", &report.instance_id)?;
        if self.inner.strict_peer_source {
            let mesh = self.read_mesh()?;
            if !source_may_act_for(mesh.as_ref(), report.node_id, source.ip(), forwarded) {
                tracing::warn!(
                    node_id = report.node_id,
                    source = %source.ip(),
                    forwarded,
                    "refused a status report from an address that may not make it"
                );
                return Err(ServiceError::Forbidden(format!(
                    "{} may not report for node {}",
                    source.ip(),
                    report.node_id
                )));
            }
        }
        // A node's first report records its instance id, which then guards
        // its status and its lease. Only the node itself may claim it, never
        // a registered node forwarding for it.
        if self.inner.strict_peer_source && !report.instance_id.is_empty() {
            let mesh = self.read_mesh()?;
            let unclaimed = mesh
                .as_ref()
                .and_then(|m| m.nodes.get(&report.node_id))
                .filter(|n| n.instance_id.is_none());
            if unclaimed.is_some_and(|n| IpAddr::V4(n.mesh_ip) != source.ip().to_canonical()) {
                tracing::warn!(
                    node_id = report.node_id,
                    source = %source.ip(),
                    "refused to record an instance id from an address that is not the node's"
                );
                return Err(ServiceError::Forbidden(format!(
                    "only node {} may record its instance id",
                    report.node_id
                )));
            }
        }
        self.report_node(report, forwarded).await
    }

    /// Tells the service the clocks of the lease task, which set how far the
    /// lease clock can run ahead of a report. Once; later calls are ignored.
    pub fn set_lease_timing(&self, timing: lease::LeaseTiming) {
        let _ = self.inner.lease_timing.set(timing);
    }

    /// What the nodes reported, as of the lease clock of this node's copy of
    /// the mesh ([`crate::status::view`]). Empty without a mesh.
    pub(crate) fn status_view(&self) -> ServiceResult<crate::status::StatusView> {
        let Some(mesh) = self.read_mesh()? else {
            return Ok(Default::default());
        };
        let timing = self.inner.lease_timing.get().copied().unwrap_or_default();
        let tick = timing.tick_clock_secs(mesh.lease_secs);
        crate::status::view(&self.inner.state, &mesh, tick)
            .map_err(|e| ServiceError::Internal(format!("status: {e}")))
    }

    /// The id and instance id this node reports with, or `None` when it has
    /// no mesh identity.
    pub(crate) fn report_identity(&self) -> Option<(NodeId, String)> {
        let instance_id = self.inner.mesh.as_ref()?.record.instance_id.clone()?;
        Some((self.inner.node_id, instance_id))
    }

    /// Whether a raft RPC (`/raft/append`, `/raft/vote`, `/raft/snapshot`)
    /// may come from `source`.
    ///
    /// The peer firewall also admits machines that only hold an invite. One
    /// of those could otherwise post a forged snapshot and replace all
    /// replicated state, or a membership without some node, which that node
    /// takes as proof that it was removed. So `source` must be one of:
    ///
    /// - the mesh IP of a registered node in this node's copy of the mesh;
    /// - while this node's join is pending: the mesh IP of a peer in its
    ///   join token. Those were the registered nodes when the invite was
    ///   made. They count next to the mesh and not only before there is
    ///   one: a joiner applies the log a batch at a time, the mesh of the
    ///   first batch need not have the node that leads now, and that
    ///   leader's appends are the only way forward;
    /// - the address of a member of the raft membership in this node's log.
    ///   A node becomes a member only after it has registered, and the log
    ///   can be ahead of what this node has applied.
    ///
    /// This is never stricter than the WireGuard peer set without its
    /// invites, which is built by the same rules
    /// ([`crate::reconcile::desired_network`]). A member that knows a node
    /// only as an invite refuses it, even if it has registered since; it
    /// hears of the registration from any other member.
    ///
    /// This node's own mesh IP is always refused. Raft never sends to
    /// itself, so such a request comes from some other process on this
    /// machine.
    ///
    /// Not checked when this node's own peer listener is loopback (see
    /// `strict_peer_source`), and not outside mesh mode, where there are no
    /// invites and nothing to check against.
    pub fn raft_source_allowed(&self, source: IpAddr) -> bool {
        if !self.inner.strict_peer_source {
            return true;
        }
        let Some(local) = &self.inner.mesh else {
            return true;
        };
        let source = source.to_canonical();
        if source == IpAddr::V4(local.record.mesh_ip) {
            return false;
        }
        let Ok(mesh) = self.read_mesh() else {
            return false; // the storage is closed
        };
        let from_token = || {
            local
                .bootstrap_peers
                .iter()
                .any(|ip| IpAddr::V4(*ip) == source)
        };
        source_is_registered(mesh.as_ref(), source)
            || (local.join_pending.is_pending() && from_token())
            || member_addr_ips(&self.metrics()).contains(&source)
    }

    /// The raft term this node is in.
    pub(crate) fn term(&self) -> u64 {
        self.metrics().current_term
    }

    /// Whether this node is the raft leader right now.
    pub(crate) fn is_leader(&self) -> bool {
        self.metrics().state == ServerState::Leader
    }

    /// Node ids, voters and learners, of the raft membership this node
    /// knows about.
    pub(crate) fn member_ids(&self) -> BTreeSet<NodeId> {
        let m = self.metrics();
        let membership = m.membership_config.membership();
        membership.nodes().map(|(id, _)| *id).collect()
    }

    /// Takes `ids` out of the raft membership, voters and learners alike,
    /// if the quorum guard allows it. This node must be the leader.
    ///
    /// `mark` is the index of a log entry this leader wrote a while ago; a
    /// voter counts as caught up when it has replicated that entry (see
    /// [`lease::raft_removal_allowed`]). `NotReady` when the guard refuses.
    pub(crate) async fn remove_from_raft(
        &self,
        ids: &BTreeSet<NodeId>,
        mark: u64,
    ) -> ServiceResult<()> {
        if !lease::raft_removal_allowed(&self.metrics(), ids, self.inner.node_id, mark) {
            return Err(ServiceError::NotReady(format!(
                "too few of the voters that would remain have caught up to remove {ids:?}"
            )));
        }
        self.drop_members(ids).await
    }

    /// Milliseconds since a quorum last acknowledged this leader, from the
    /// raft metrics. `None` on a node that is not the leader, and on a
    /// leader no quorum has acknowledged yet.
    pub(crate) fn millis_since_quorum_ack(&self) -> Option<u64> {
        self.metrics().millis_since_quorum_ack
    }

    /// The log index of the membership this node knows about, if that is a
    /// joint config: a voter change that has made its first step and not
    /// its second.
    pub(crate) fn joint_config_index(&self) -> Option<u64> {
        let m = self.metrics();
        let joint = m.membership_config.membership().get_joint_config().len() > 1;
        joint.then(|| m.membership_config.log_id().map_or(0, |l| l.index))
    }

    /// Makes the second step of a voter change that stopped at its joint
    /// config, if the quorum guard passes for every voter set of that
    /// config. `mark` is as for [`Self::remove_from_raft`]. `NotReady` when
    /// the guard refuses.
    ///
    /// Removing no voter from a joint config asks raft for the config's own
    /// target, which is the uniform config the first step was heading for.
    /// With `retain` off, the voters that the change dropped are removed as
    /// nodes too, as [`Self::drop_members`] would have done.
    pub(crate) async fn finish_joint_config(&self, mark: u64) -> ServiceResult<()> {
        let nobody = BTreeSet::new();
        if !lease::raft_removal_allowed(&self.metrics(), &nobody, self.inner.node_id, mark) {
            return Err(ServiceError::NotReady(
                "too few voters of the joint config have caught up to finish it".into(),
            ));
        }
        self.inner
            .raft
            .change_membership(ChangeMembers::RemoveVoters(nobody), false)
            .await
            .map_err(|e| self.write_error(e))?;
        Ok(())
    }

    /// Removes `ids` from the raft membership without asking the guard.
    ///
    /// Voters go through `RemoveVoters` with `retain` off, which removes
    /// them as nodes too. Raft does that in two steps, a joint config and
    /// then the final one. If an earlier call got only as far as the joint
    /// config (the leader changed), the same call again finishes it.
    /// Learners go through `RemoveNodes`.
    async fn drop_members(&self, ids: &BTreeSet<NodeId>) -> ServiceResult<()> {
        let raft = &self.inner.raft;
        let voters: BTreeSet<NodeId> = {
            let m = self.metrics();
            let membership = m.membership_config.membership();
            membership.voter_ids().filter(|v| ids.contains(v)).collect()
        };
        if !voters.is_empty() {
            raft.change_membership(ChangeMembers::RemoveVoters(voters), false)
                .await
                .map_err(|e| self.write_error(e))?;
        }
        let learners: BTreeSet<NodeId> = {
            let m = self.metrics();
            let membership = m.membership_config.membership();
            membership
                .learner_ids()
                .filter(|l| ids.contains(l))
                .collect()
        };
        if !learners.is_empty() {
            raft.change_membership(ChangeMembers::RemoveNodes(learners), false)
                .await
                .map_err(|e| self.write_error(e))?;
        }
        Ok(())
    }

    /// Before a registration that takes over a name: removes the node that
    /// holds the name from the raft membership. `RegisterNode` is rejected
    /// while that node is still a member, so that the old node can never
    /// vote with a mesh record that now belongs to another machine.
    ///
    /// Does nothing unless `req` matches a live invite whose name another
    /// node holds and that node is a raft member; apply judges everything
    /// else.
    ///
    /// The guard is the one the lease task uses. Its mark is an
    /// `AdvanceClock` of zero seconds written here, so only voters that
    /// answer now count. `NotReady` when too few do within
    /// [`CATCH_UP_WAIT`], or when the membership change fails; the joiner
    /// retries.
    ///
    /// When the holder is this node, the leader removes itself. Raft keeps
    /// it leading until that change is committed and applied, then it steps
    /// down. The answer is `NotReady`: the joiner's retry reaches the next
    /// leader, where the old node is no longer a member.
    async fn make_way_for(&self, mesh: &MeshState, req: &PeerRegisterRequest) -> ServiceResult<()> {
        let Some(invite) = mesh.invites.get(&req.invite_id) else {
            return Ok(());
        };
        let usable = !invite.name.is_empty()
            && invite.node_id == req.node_id
            && invite.endpoint == req.endpoint
            && now_secs() < invite.expires_at;
        if !usable {
            return Ok(());
        }
        let holder = mesh
            .nodes
            .values()
            .find(|n| n.name == invite.name && n.node_id != req.node_id)
            .map(|n| n.node_id);
        let Some(old) = holder.filter(|old| self.member_ids().contains(old)) else {
            return Ok(());
        };
        let me = self.inner.node_id;
        let name = &invite.name;
        let not_ready = |why: String| {
            ServiceError::NotReady(format!(
                "node {old} holds the name {name} and cannot leave the raft membership yet: {why}"
            ))
        };

        let (mark, _) = self
            .write(Request::AdvanceClock { elapsed_secs: 0 })
            .await?;
        let remove = BTreeSet::from([old]);
        let allowed = {
            let remove = remove.clone();
            move |m: &RaftMetrics<NodeId, Node>| lease::raft_removal_allowed(m, &remove, me, mark)
        };
        self.inner
            .raft
            .wait(Some(CATCH_UP_WAIT))
            .metrics(allowed, "enough voters caught up for a takeover")
            .await
            .map_err(|_| not_ready("too few of the other voters have caught up".into()))?;
        self.drop_members(&remove)
            .await
            .map_err(|e| not_ready(e.to_string()))?;
        tracing::info!(
            node_id = old,
            name = %name,
            new_node_id = req.node_id,
            "removed a node from the raft membership; another machine takes over its name"
        );
        if old == me {
            return Err(ServiceError::NotReady(format!(
                "node {old} held the name {name} and has left the raft membership; \
                 retry the registration with the next leader"
            )));
        }
        Ok(())
    }

    /// Peer listener addresses of all members, this node (the leader) first.
    fn join_addrs(&self) -> Vec<String> {
        let m = self.metrics();
        let id = self.inner.node_id;
        let mut addrs = vec![self.inner.peer_addr.clone()];
        addrs.extend(
            m.membership_config
                .membership()
                .nodes()
                .filter(|(nid, _)| **nid != id)
                .map(|(_, n)| n.addr.clone()),
        );
        addrs
    }

    /// Replaces this node's replicated WireGuard endpoint (`SetEndpoint`),
    /// through the leader.
    pub async fn set_endpoint(&self, endpoint: SocketAddr) -> ServiceResult<()> {
        let req = PeerEndpointRequest {
            node_id: self.inner.node_id,
            endpoint: endpoint.to_string(),
        };
        self.submit_endpoint(req, false).await
    }

    /// Handles `POST /raft/cluster/endpoint`: the leader proposes
    /// `SetEndpoint`; a follower forwards once.
    ///
    /// `source` is the address the request came from. A node may only change
    /// its own endpoint, so the request must come from the mesh IP of
    /// `req.node_id`. A forwarded request comes from the node that forwarded
    /// it, which made that check itself; it must come from the mesh IP of a
    /// registered node. Anything else is `Forbidden`. The peer firewall also
    /// lets nodes that only hold an invite in, and those are not registered.
    ///
    /// Not checked when this node's own peer listener is loopback (see
    /// `strict_peer_source`).
    pub async fn peer_set_endpoint(
        &self,
        req: PeerEndpointRequest,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<()> {
        if self.inner.strict_peer_source {
            let mesh = self.read_mesh()?;
            if !source_may_act_for(mesh.as_ref(), req.node_id, source.ip(), forwarded) {
                tracing::warn!(
                    node_id = req.node_id,
                    source = %source.ip(),
                    forwarded,
                    "refused an endpoint change from an address that may not make it"
                );
                return Err(ServiceError::Forbidden(format!(
                    "{} may not change the endpoint of node {}",
                    source.ip(),
                    req.node_id
                )));
            }
        }
        self.submit_endpoint(req, forwarded).await
    }

    /// Proposes `SetEndpoint` on the leader, or forwards the request to it.
    async fn submit_endpoint(
        &self,
        mut req: PeerEndpointRequest,
        forwarded: bool,
    ) -> ServiceResult<()> {
        req.endpoint = canonical_endpoint(&req.endpoint)?;
        match self.forward_target(forwarded)? {
            None => {
                let set = Request::SetEndpoint {
                    node_id: req.node_id,
                    endpoint: req.endpoint,
                };
                self.propose(set).await.map(drop)
            }
            Some(leader_addr) => self
                .post_peer(&leader_addr, "/raft/cluster/endpoint", &req, true)
                .await
                .map(drop),
        }
    }

    /// Joins the cluster that `req.peer` belongs to, as a voter.
    ///
    /// TODO(security): this makes the node send a request to any address the
    /// local caller names. Fine while the API is loopback/WireGuard only.
    pub async fn join(&self, req: JoinRequest) -> ServiceResult<ClusterStatus> {
        let peer = req.peer.trim();
        if peer.is_empty() || peer.contains('/') || peer.contains('@') {
            return Err(ServiceError::BadRequest("peer must be host:port".into()));
        }
        let id = self.inner.node_id;
        if self.is_voter(id) {
            return Err(ServiceError::AlreadyJoined);
        }

        let body = PeerJoinRequest {
            node_id: id,
            addr: self.inner.peer_addr.clone(),
            invite_id: None,
            instance_id: None,
        };
        self.post_peer(peer, "/raft/cluster/join", &body, false)
            .await?;

        let _ = self
            .inner
            .raft
            .wait(Some(SETTLE_WAIT))
            .metrics(
                |m| {
                    m.membership_config
                        .membership()
                        .voter_ids()
                        .any(|v| v == id)
                },
                "self is a voter after join",
            )
            .await;
        Ok(self.status().await)
    }

    /// First step of a join with an invite: asks `peer` (a join address from
    /// the invite token) to register `invite_id` to this machine,
    /// `instance_id`. `endpoint` must be the invite's. Safe to repeat: the
    /// same machine registering again is accepted, any other is refused with
    /// `invite_used`. The answer may carry the cluster key
    /// ([`Self::accept_cluster_key`]).
    pub async fn register_invited(
        &self,
        peer: &str,
        invite_id: &str,
        endpoint: SocketAddr,
        instance_id: &str,
    ) -> ServiceResult<PeerRegisterResponse> {
        let body = PeerRegisterRequest {
            invite_id: invite_id.to_string(),
            node_id: self.inner.node_id,
            endpoint: endpoint.to_string(),
            instance_id: instance_id.to_string(),
        };
        let bytes = self
            .post_peer(peer, "/raft/cluster/register", &body, false)
            .await?;
        // A leader that does not send a body has no key to give.
        if bytes.is_empty() {
            return Ok(PeerRegisterResponse::default());
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| ServiceError::Internal(format!("bad register response from {peer}: {e}")))
    }

    /// Second step: asks `peer` to add this node, which has registered
    /// `invite_id` as `instance_id`, as a voter.
    pub async fn join_invited(
        &self,
        peer: &str,
        invite_id: &str,
        instance_id: &str,
    ) -> ServiceResult<()> {
        let body = PeerJoinRequest {
            node_id: self.inner.node_id,
            addr: self.inner.peer_addr.clone(),
            invite_id: Some(invite_id.to_string()),
            instance_id: Some(instance_id.to_string()),
        };
        self.post_peer(peer, "/raft/cluster/join", &body, false)
            .await
            .map(drop)
    }

    /// Waits up to `timeout` for this node's own raft to show it as a voter.
    /// The leader answers a join once the membership change is committed;
    /// this node sees it a moment later.
    pub async fn wait_voter_self(&self, timeout: Duration) -> bool {
        let id = self.inner.node_id;
        self.inner
            .raft
            .wait(Some(timeout))
            .metrics(
                |m| {
                    m.membership_config
                        .membership()
                        .voter_ids()
                        .any(|v| v == id)
                },
                "self is a voter",
            )
            .await
            .is_ok()
    }

    /// Whether this node is a voter in the membership it knows about.
    pub fn is_voter_self(&self) -> bool {
        self.is_voter(self.inner.node_id)
    }

    /// Whether this node is in the membership it knows about, as a voter or
    /// a learner.
    pub fn is_member_self(&self) -> bool {
        self.metrics()
            .membership_config
            .membership()
            .get_node(&self.inner.node_id)
            .is_some()
    }

    /// Handles `POST /raft/cluster/join` from another node. The leader adds
    /// the node as a learner, waits for it to catch up, then promotes it to
    /// voter. Idempotent for a node that is already a voter at the same address.
    ///
    /// A node that is already a learner (an earlier join stopped half way) is
    /// promoted only once it has caught up; until then the answer is
    /// `not_ready` and the joiner retries. Promoting a learner that cannot be
    /// reached would add a voter that cannot vote and could cost the cluster
    /// its quorum.
    ///
    /// In a mesh the node must have registered its invite first
    /// ([`Self::peer_register`]).
    pub async fn peer_join(&self, req: PeerJoinRequest, forwarded: bool) -> ServiceResult<()> {
        validate_addr(req.node_id, &req.addr)?;
        match self.forward_target(forwarded)? {
            None => self.join_as_leader(&req).await,
            Some(leader_addr) => self
                .post_peer(&leader_addr, "/raft/cluster/join", &req, true)
                .await
                .map(drop),
        }
    }

    /// Handles `POST /raft/cluster/register`: the leader proposes
    /// `RegisterNode`, which turns the invite into a node that belongs to the
    /// machine `req.instance_id`; a follower forwards once. Repeating it from
    /// the same machine succeeds. Another machine gets `invite_used`.
    ///
    /// When another node holds the invite's name, the leader first removes
    /// that node from the raft membership ([`Self::make_way_for`]), and
    /// `RegisterNode` then replaces its record. `not_ready` until that
    /// worked; the joiner retries.
    ///
    /// An `invite_id` that is neither a live invite nor the invite of a
    /// registered node is refused without a raft write
    /// ([`Self::registrable`]).
    pub async fn peer_register(
        &self,
        mut req: PeerRegisterRequest,
        forwarded: bool,
    ) -> ServiceResult<()> {
        check_hex_id("invite_id", &req.invite_id)?;
        check_hex_id("instance_id", &req.instance_id)?;
        // The invite stores the canonical spelling; compare like with like.
        req.endpoint = parse_full_endpoint(&req.endpoint)
            .map_err(|e| invite_invalid(e.to_string()))?
            .to_string();
        match self.forward_target(forwarded)? {
            None => {
                let mesh = self.registrable(&req.invite_id).await?;
                self.make_way_for(&mesh, &req).await?;
                self.propose(Request::RegisterNode {
                    invite_id: req.invite_id,
                    node_id: req.node_id,
                    endpoint: req.endpoint,
                    instance_id: req.instance_id,
                    at: now_secs(),
                })
                .await
                .map(drop)
            }
            Some(leader_addr) => self
                .post_peer(&leader_addr, "/raft/cluster/register", &req, true)
                .await
                .map(drop),
        }
    }

    /// The mesh, if a registration with `invite_id` is worth a raft write:
    /// the invite is live, or a node was registered with it. Anything else
    /// is refused here with the code apply would give ([`invite_refusal`]).
    ///
    /// `RegisterNode` is a synced write on every node, and the route is
    /// open to every machine the peer firewall admits, so an id the mesh
    /// does not know must not reach the log.
    ///
    /// The refusal makes a joiner give up its identity, so it is given only
    /// by a leader that has applied everything committed before now
    /// ([`Self::settle`]): one that was just elected may not have applied
    /// the invite yet.
    async fn registrable(&self, invite_id: &str) -> ServiceResult<MeshState> {
        let mesh = self.read_mesh()?.ok_or_else(mesh_not_configured)?;
        if invite_refusal(&mesh, invite_id, now_secs()).is_none() {
            return Ok(mesh);
        }
        self.settle().await?;
        let mesh = self.read_mesh()?.ok_or_else(mesh_not_configured)?;
        match invite_refusal(&mesh, invite_id, now_secs()) {
            Some(refusal) => Err(refusal),
            None => Ok(mesh),
        }
    }

    /// Waits until this node, as the leader, has applied every entry that
    /// was committed before the call, so that what it reads from its state
    /// is not behind what the cluster has agreed on. Costs a round of
    /// heartbeats and no raft write. `NotReady` when it cannot confirm
    /// within [`CATCH_UP_WAIT`] that it still leads; the caller retries.
    pub(crate) async fn settle(&self) -> ServiceResult<()> {
        let settled = self.inner.raft.ensure_linearizable();
        match tokio::time::timeout(CATCH_UP_WAIT, settled).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(ServiceError::NotReady(format!(
                "this node could not confirm that it leads: {e}"
            ))),
            Err(_) => Err(ServiceError::NotReady(format!(
                "this node could not confirm that it leads within {CATCH_UP_WAIT:?}"
            ))),
        }
    }

    /// In a mesh only a registered node may join: `req` must name the invite
    /// and the machine that the node record of `req.node_id` was registered
    /// with, and the address of that node.
    ///
    /// A node with no record gets `not_ready` while its invite is still in
    /// the state: it has not registered yet. It gets `not_ready` too unless
    /// `settled` says that this leader has applied everything committed
    /// ([`Self::settle`]): a leader that was just elected may not have
    /// applied the registration yet. With neither a record nor an invite on
    /// a settled leader, the mesh has removed the node (its lease ran out
    /// before it finished joining, or another machine took its name), and
    /// the answer is `mesh_node_removed`. That makes the joiner give up its
    /// identity, so it is never given on a guess.
    fn check_registered(&self, req: &PeerJoinRequest, settled: bool) -> ServiceResult<()> {
        let Some(mesh) = self.read_mesh()? else {
            return match req.invite_id {
                Some(_) => Err(mesh_not_configured()),
                None => Ok(()),
            };
        };
        let (Some(invite_id), Some(instance_id)) = (&req.invite_id, &req.instance_id) else {
            return Err(invite_invalid(
                "a join to this mesh must name the invite and the instance it registered with",
            ));
        };
        let Some(node) = mesh.nodes.get(&req.node_id) else {
            if settled && !mesh.invites.contains_key(invite_id) {
                return Err(ServiceError::Rejected {
                    code: error_code::MESH_NODE_REMOVED.into(),
                    message: format!(
                        "node {} is not in the mesh and no invite is waiting for it",
                        req.node_id
                    ),
                });
            }
            return Err(ServiceError::NotReady(format!(
                "node {} is not registered; register the invite first, then retry the join",
                req.node_id
            )));
        };
        if node.invite_id.as_ref() != Some(invite_id) {
            return Err(invite_invalid(format!(
                "node {} was not registered with this invite",
                req.node_id
            )));
        }
        if node.instance_id.as_ref() != Some(instance_id) {
            return Err(ServiceError::Rejected {
                code: error_code::INVITE_USED.into(),
                message: "invite already used by another node".into(),
            });
        }
        if self.inner.strict_peer_source
            && !join_addr_matches(&req.addr, node.mesh_ip, mesh.config.peer_port)
        {
            return Err(invite_invalid(format!(
                "address {} does not match the invite ({}:{})",
                req.addr, node.mesh_ip, mesh.config.peer_port
            )));
        }
        Ok(())
    }

    async fn join_as_leader(&self, req: &PeerJoinRequest) -> ServiceResult<()> {
        if let Err(first) = self.check_registered(req, false) {
            if !matches!(first, ServiceError::NotReady(_)) {
                return Err(first);
            }
            // No record of the node. Look again once this leader has
            // applied everything, which tells a removed node from one whose
            // registration is still on its way.
            self.settle().await?;
            self.check_registered(req, true)?;
        }
        let raft = &self.inner.raft;
        let m = self.metrics();
        let membership = m.membership_config.membership();
        match membership.get_node(&req.node_id) {
            Some(existing) if existing.addr != req.addr => {
                return Err(ServiceError::BadRequest(format!(
                    "node {} is already a member at {}",
                    req.node_id, existing.addr
                )));
            }
            Some(_) if membership.voter_ids().any(|v| v == req.node_id) => return Ok(()),
            Some(_) => self.wait_caught_up(req.node_id).await?,
            None => {
                raft.add_learner(req.node_id, Node::new(&req.addr), true)
                    .await
                    .map_err(|e| self.write_error(e))?;
            }
        }
        raft.change_membership(
            ChangeMembers::AddVoterIds(BTreeSet::from([req.node_id])),
            true,
        )
        .await
        .map_err(|e| self.write_error(e))?;
        Ok(())
    }

    /// Waits up to [`CATCH_UP_WAIT`] for learner `id` to have replicated the
    /// leader's log as it is now. `NotReady` if it has not: the learner is
    /// unreachable or still far behind, and must not become a voter yet.
    async fn wait_caught_up(&self, id: NodeId) -> ServiceResult<()> {
        let target = self.metrics().last_log_index;
        let caught_up = move |m: &RaftMetrics<NodeId, Node>| {
            let matched = m
                .replication
                .as_ref()
                .and_then(|r| r.get(&id))
                .and_then(|log_id| log_id.map(|l| l.index));
            matched.is_some() && matched >= target
        };
        self.inner
            .raft
            .wait(Some(CATCH_UP_WAIT))
            .metrics(caught_up, "learner caught up")
            .await
            .map(drop)
            .map_err(|_| {
                ServiceError::NotReady(format!(
                    "node {id} is a learner that has not caught up with the log; retry the join"
                ))
            })
    }

    /// POSTs `body` as JSON to `http://{peer}{path}` and returns the body of
    /// a 2xx answer. Any other answer is [`ServiceError::Upstream`].
    pub(crate) async fn post_peer<B: Serialize>(
        &self,
        peer: &str,
        path: &str,
        body: &B,
        forwarded: bool,
    ) -> ServiceResult<Vec<u8>> {
        let timeout = if path == "/raft/cluster/join" {
            JOIN_TIMEOUT
        } else {
            FORWARD_TIMEOUT
        };
        let mut rb = self
            .inner
            .http
            .post(format!("http://{peer}{path}"))
            .timeout(timeout)
            .json(body);
        if forwarded {
            rb = rb.header(FORWARDED_HEADER, "1");
        }
        let resp = rb
            .send()
            .await
            .map_err(|e| ServiceError::PeerUnreachable(format!("{peer}: {e}")))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ServiceError::PeerUnreachable(format!("{peer}: {e}")))?;
        if status.is_success() {
            return Ok(bytes.to_vec());
        }
        let body = serde_json::from_slice::<ErrorBody>(&bytes).unwrap_or_else(|_| {
            ErrorBody::new(error_code::INTERNAL)
                .with_message(String::from_utf8_lossy(&bytes).into_owned())
        });
        Err(ServiceError::Upstream {
            status: status.as_u16(),
            body,
        })
    }

    /// Proposes `req` and turns `Response::Rejected` into an error.
    pub(crate) async fn propose(&self, req: Request) -> ServiceResult<Response> {
        self.write(req).await.map(|(_, resp)| resp)
    }

    /// [`Self::propose`], also returning the index of the log entry that
    /// carried `req`.
    pub(crate) async fn write(&self, req: Request) -> ServiceResult<(u64, Response)> {
        let resp = self
            .inner
            .raft
            .client_write(req)
            .await
            .map_err(|e| self.write_error(e))?;
        match resp.data {
            Response::Rejected { code, message } => Err(rejection(code, message)),
            other => Ok((resp.log_id.index, other)),
        }
    }

    pub(crate) fn read_mesh(&self) -> ServiceResult<Option<MeshState>> {
        self.inner
            .state
            .mesh()
            .map_err(|e| ServiceError::Internal(e.to_string()))
    }

    /// This node's copy of the replicated state.
    pub(crate) fn state(&self) -> &StateReader {
        &self.inner.state
    }

    /// Whether the source of a peer request is checked: not when this
    /// node's own peer listener is loopback (see `strict_peer_source`).
    pub(crate) fn checks_peer_source(&self) -> bool {
        self.inner.strict_peer_source
    }

    pub(crate) fn is_voter(&self, id: NodeId) -> bool {
        self.metrics()
            .membership_config
            .membership()
            .voter_ids()
            .any(|v| v == id)
    }

    fn write_error(&self, e: RaftError<NodeId, ClientWriteError<NodeId, Node>>) -> ServiceError {
        match e {
            RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => {
                // Leadership moved mid-operation; the caller may retry.
                self.not_leader(&self.metrics())
            }
            RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
                ServiceError::BadRequest(e.to_string())
            }
            RaftError::Fatal(e) => ServiceError::Internal(e.to_string()),
        }
    }

    /// Where a leader-only request is handled: `None` when this node is the
    /// leader, otherwise the leader's peer address to forward it to. A
    /// request that was already `forwarded` is never forwarded again
    /// (`NotLeader`); with no known leader it is `NoLeader`.
    pub(crate) fn forward_target(&self, forwarded: bool) -> ServiceResult<Option<String>> {
        let m = self.metrics();
        if m.current_leader == Some(self.inner.node_id) {
            return Ok(None);
        }
        match self.not_leader(&m) {
            ServiceError::NotLeader { leader_addr, .. } if !forwarded => Ok(Some(leader_addr)),
            other => Err(other),
        }
    }

    /// `NotLeader` with the current leader's peer address, or `NoLeader`.
    fn not_leader(&self, m: &RaftMetrics<NodeId, Node>) -> ServiceError {
        let leader = m
            .current_leader
            .filter(|l| *l != self.inner.node_id)
            .and_then(|l| {
                m.membership_config
                    .membership()
                    .get_node(&l)
                    .map(|n| (l, n.addr.clone()))
            });
        match leader {
            Some((leader_id, leader_addr)) => ServiceError::NotLeader {
                leader_id,
                leader_addr,
            },
            None => ServiceError::NoLeader,
        }
    }
}

/// `status` of a healthy node in [`HealthResponse`].
pub const HEALTH_OK: &str = "ok";
/// `status` of a node whose raft core has stopped or whose storage is
/// closed. Served with HTTP 503.
pub const HEALTH_UNAVAILABLE: &str = "unavailable";

/// Why the raft core behind `metrics` is not running, or `None` while it is.
/// The core records the error it stopped with. A core that panicked records
/// nothing, but its end closes the metrics channel.
pub(crate) fn raft_stopped_reason(
    metrics: &tokio::sync::watch::Receiver<RaftMetrics<NodeId, Node>>,
) -> Option<String> {
    if let Err(fatal) = &metrics.borrow().running_state {
        return Some(fatal.to_string());
    }
    metrics
        .has_changed()
        .is_err()
        .then(|| "the raft task ended without reporting an error".to_string())
}

fn role(state: &ServerState) -> Role {
    match state {
        ServerState::Leader => Role::Leader,
        ServerState::Follower => Role::Follower,
        ServerState::Candidate => Role::Candidate,
        ServerState::Learner => Role::Learner,
        ServerState::Shutdown => Role::Shutdown,
    }
}

fn mesh_not_configured() -> ServiceError {
    ServiceError::Rejected {
        code: error_code::MESH_NOT_CONFIGURED.into(),
        message: "this cluster has no WireGuard mesh".into(),
    }
}

fn invite_invalid(message: impl Into<String>) -> ServiceError {
    ServiceError::Rejected {
        code: error_code::INVITE_INVALID.into(),
        message: message.into(),
    }
}

/// A `Response::Rejected` as a [`ServiceError`], keeping the generic codes on
/// their usual variants. A `not_found` that says what is missing stays
/// `Rejected`, which is the same 404 with its message.
fn rejection(code: String, message: String) -> ServiceError {
    match code.as_str() {
        error_code::BAD_REQUEST => ServiceError::BadRequest(message),
        error_code::ALREADY_INITIALIZED => ServiceError::AlreadyInitialized,
        error_code::NOT_FOUND if message.is_empty() => ServiceError::NotFound,
        error_code::NOT_READY => ServiceError::NotReady(message),
        _ => ServiceError::Rejected { code, message },
    }
}

/// 128 random bits as 32 lowercase hex characters.
fn random_invite_id() -> ServiceResult<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| ServiceError::Internal(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn check_invite_id(id: &str) -> ServiceResult<()> {
    check_hex_id("invite_id", id)
}

/// `id` (the request field `field`) must be 128 bits as lowercase hex.
fn check_hex_id(field: &str, id: &str) -> ServiceResult<()> {
    let ok = id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if ok {
        Ok(())
    } else {
        Err(ServiceError::BadRequest(format!(
            "{field} must be 32 lowercase hex characters"
        )))
    }
}

/// An internal request's endpoint must already be `ip:port`. Returns its
/// canonical spelling, which is what goes into the log.
fn canonical_endpoint(endpoint: &str) -> ServiceResult<String> {
    parse_full_endpoint(endpoint)
        .map(|addr| addr.to_string())
        .map_err(|_| ServiceError::BadRequest("endpoint must be ip:port".into()))
}

/// Whether a request that a node may only make for itself (an endpoint
/// change, a lease renewal), naming `node_id`, may come from `source`: the
/// mesh IP of that node, or for a `forwarded` request the mesh IP of any
/// registered node. Nodes that only hold an invite do not count.
fn source_may_act_for(
    mesh: Option<&MeshState>,
    node_id: NodeId,
    source: IpAddr,
    forwarded: bool,
) -> bool {
    let Some(mesh) = mesh else {
        return false;
    };
    let source = source.to_canonical();
    let is = |node: &NodeRecord| IpAddr::V4(node.mesh_ip) == source;
    if forwarded {
        mesh.nodes.values().any(is)
    } else {
        mesh.nodes.get(&node_id).is_some_and(is)
    }
}

/// Whether `source` is the mesh IP of a registered node: what
/// [`source_may_act_for`] asks of a forwarded request, for requests that
/// name no node.
pub(crate) fn source_is_registered(mesh: Option<&MeshState>, source: IpAddr) -> bool {
    // With `forwarded` set the node id is not looked at.
    source_may_act_for(mesh, 0, source, true)
}

/// The IP addresses of the peer listeners of every member, voters and
/// learners, of the raft membership in `m`. An address that is not `ip:port`
/// is left out.
fn member_addr_ips(m: &RaftMetrics<NodeId, Node>) -> BTreeSet<IpAddr> {
    m.membership_config
        .membership()
        .nodes()
        .filter_map(|(_, node)| node.addr.parse::<SocketAddr>().ok())
        .map(|addr| addr.ip().to_canonical())
        .collect()
}

/// Why a registration with `invite_id` cannot work in `mesh` at `now`, or
/// `None` when apply has to judge it: the invite is live, or a node was
/// registered with it (a retry by the same machine succeeds, another
/// machine gets `invite_used`). The codes are the ones apply gives.
fn invite_refusal(mesh: &MeshState, invite_id: &str, now: u64) -> Option<ServiceError> {
    let used = |n: &NodeRecord| n.invite_id.as_deref() == Some(invite_id);
    if mesh.nodes.values().any(used) {
        return None;
    }
    match mesh.invites.get(invite_id) {
        Some(invite) if now >= invite.expires_at => Some(ServiceError::Rejected {
            code: error_code::INVITE_EXPIRED.into(),
            message: "invite has expired".into(),
        }),
        Some(_) => None,
        None => Some(invite_invalid("invite is not valid")),
    }
}

/// Whether a joiner's peer address is exactly its invite's mesh IP on the
/// mesh peer port.
fn join_addr_matches(addr: &str, mesh_ip: Ipv4Addr, peer_port: u16) -> bool {
    addr.parse::<SocketAddr>().ok() == Some(SocketAddr::new(IpAddr::V4(mesh_ip), peer_port))
}

fn validate_addr(id: NodeId, addr: &str) -> ServiceResult<()> {
    if addr.trim().is_empty() || addr.contains('/') || addr.contains('@') {
        return Err(ServiceError::BadRequest(format!(
            "node {id}: address must be host:port"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TestNode;

    #[test]
    fn internal_endpoints_are_canonicalised() {
        assert_eq!(
            canonical_endpoint("[2001:DB8::2]:51820").as_deref(),
            Ok("[2001:db8::2]:51820")
        );
        assert_eq!(
            canonical_endpoint("198.51.100.2:51820").as_deref(),
            Ok("198.51.100.2:51820")
        );
        for bad in ["198.51.100.2", "node2.example:51820", ""] {
            assert!(
                matches!(canonical_endpoint(bad), Err(ServiceError::BadRequest(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn join_address_must_be_the_invite_mesh_ip_and_peer_port() {
        let ip: Ipv4Addr = "10.99.0.2".parse().unwrap();
        assert!(join_addr_matches("10.99.0.2:7401", ip, 7401));
        for bad in [
            "10.99.0.3:7401",
            "10.99.0.2:7402",
            "127.0.0.1:7401",
            "node2:7401",
            "10.99.0.2",
            "",
        ] {
            assert!(!join_addr_matches(bad, ip, 7401), "{bad}");
        }
    }

    /// Node 1 (the founder) and node 2 are registered; node 3 only holds a
    /// live invite, so its mesh IP passes the firewall but is not a node yet.
    fn mesh_of_two() -> MeshState {
        let node = |node_id: u64, ip: &str| NodeRecord {
            node_id,
            public_key: "HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw=".into(),
            mesh_ip: ip.parse().unwrap(),
            endpoint: Some(format!("203.0.113.{node_id}:51820")),
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: format!("node-{node_id}"),
            lease_renewed_at: 0,
        };
        MeshState {
            config: MeshConfig::default(),
            nodes: BTreeMap::from([(1, node(1, "10.99.0.1")), (2, node(2, "10.99.0.2"))]),
            invites: BTreeMap::from([(
                "ab".repeat(16),
                crate::mesh::InviteRecord {
                    invite_id: "ab".repeat(16),
                    node_id: 3,
                    public_key: "xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg=".into(),
                    mesh_ip: "10.99.0.3".parse().unwrap(),
                    endpoint: "203.0.113.3:51820".into(),
                    expires_at: u64::MAX,
                    name: "node-3".into(),
                },
            )]),
            next_node_id: 4,
            clock: 0,
            lease_secs: crate::mesh::state::DEFAULT_LEASE_SECS,
        }
    }

    #[test]
    fn a_node_may_only_change_its_own_endpoint() {
        let mesh = mesh_of_two();
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let allowed = |node_id, source: &str, forwarded| {
            source_may_act_for(Some(&mesh), node_id, ip(source), forwarded)
        };
        // Its own, straight from its mesh IP.
        assert!(allowed(1, "10.99.0.1", false));
        assert!(allowed(2, "10.99.0.2", false));
        assert!(allowed(2, "::ffff:10.99.0.2", false));
        // Somebody else's.
        assert!(!allowed(1, "10.99.0.2", false));
        assert!(!allowed(2, "10.99.0.1", false));
        // An invited node that has not registered is no node yet.
        assert!(!allowed(3, "10.99.0.3", false));
        assert!(!allowed(1, "10.99.0.3", false));
        // Unknown node ids and addresses outside the mesh.
        assert!(!allowed(9, "10.99.0.1", false));
        assert!(!allowed(1, "198.51.100.7", false));
        assert!(!allowed(1, "127.0.0.1", false));
    }

    #[test]
    fn a_forwarded_endpoint_change_must_come_from_a_registered_node() {
        let mesh = mesh_of_two();
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let allowed =
            |node_id, source: &str| source_may_act_for(Some(&mesh), node_id, ip(source), true);
        // Node 2 forwarding for itself or for node 1, whose address it
        // checked.
        assert!(allowed(2, "10.99.0.2"));
        assert!(allowed(1, "10.99.0.2"));
        // The header does not help an address that is no registered node.
        assert!(!allowed(1, "10.99.0.3"));
        assert!(!allowed(1, "198.51.100.7"));
        assert!(!allowed(1, "127.0.0.1"));
    }

    #[test]
    fn without_a_mesh_nobody_may_change_an_endpoint() {
        for forwarded in [false, true] {
            assert!(!source_may_act_for(
                None,
                1,
                "10.99.0.1".parse().unwrap(),
                forwarded
            ));
        }
    }

    /// A service on the raft of `node` whose peer listener counts as a mesh
    /// address, so the source of an endpoint change is checked.
    fn strict_service(node: &TestNode) -> FaberService {
        FaberService::new(
            1,
            "10.99.0.1:7401".into(),
            node.service.raft().clone(),
            node.service.inner.state.clone(),
            None,
        )
        .unwrap()
    }

    async fn seed_mesh(node: &TestNode) {
        let (_, public_key) = generate_keypair().unwrap();
        node.service
            .propose(Request::InitCluster {
                mesh: MeshConfig::default(),
                founder: NodeRecord {
                    node_id: 1,
                    public_key,
                    mesh_ip: "10.99.0.1".parse().unwrap(),
                    endpoint: Some("203.0.113.1:51820".into()),
                    invite_id: None,
                    instance_id: None,
                    container_subnet: None,
                    name: "node-1".into(),
                    lease_renewed_at: 0,
                },
                containerd: ContainerRuntimeSpec::default(),
            })
            .await
            .unwrap();
    }

    fn endpoint_of(node: &TestNode, id: NodeId) -> Option<String> {
        node.service.read_mesh().unwrap().unwrap().nodes[&id]
            .endpoint
            .clone()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_endpoint_change_from_another_address_is_forbidden() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        let strict = strict_service(&node);
        let req = |endpoint: &str| PeerEndpointRequest {
            node_id: 1,
            endpoint: endpoint.into(),
        };

        // Another mesh peer names node 1.
        for forwarded in [false, true] {
            let spoofed = "10.99.0.2:40000".parse().unwrap();
            let err = strict
                .peer_set_endpoint(req("198.51.100.66:51820"), forwarded, spoofed)
                .await
                .unwrap_err();
            assert!(matches!(err, ServiceError::Forbidden(_)), "{err:?}");
        }
        assert_eq!(endpoint_of(&node, 1).as_deref(), Some("203.0.113.1:51820"));

        // Node 1 itself.
        let own = "10.99.0.1:40000".parse().unwrap();
        strict
            .peer_set_endpoint(req("203.0.113.9:51820"), false, own)
            .await
            .unwrap();
        assert_eq!(endpoint_of(&node, 1).as_deref(), Some("203.0.113.9:51820"));

        // A malformed endpoint is still a bad request.
        let err = strict
            .peer_set_endpoint(req("node1.example:51820"), false, own)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::BadRequest(_)), "{err:?}");

        drop(strict);
        node.shutdown().await;
    }

    /// With the peer listener on loopback (development and single-host
    /// tests) every node shares one address, so the source is not checked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_loopback_peer_listener_does_not_check_the_source() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        let req = PeerEndpointRequest {
            node_id: 1,
            endpoint: "203.0.113.9:51820".into(),
        };
        let source = "127.0.0.1:40000".parse().unwrap();
        node.service
            .peer_set_endpoint(req, false, source)
            .await
            .unwrap();
        assert_eq!(endpoint_of(&node, 1).as_deref(), Some("203.0.113.9:51820"));
        // The change a node makes for itself has no source to check.
        node.service
            .set_endpoint("203.0.113.10:51820".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(endpoint_of(&node, 1).as_deref(), Some("203.0.113.10:51820"));
        node.shutdown().await;
    }

    #[test]
    fn invite_ids() {
        let id = random_invite_id().unwrap();
        check_invite_id(&id).unwrap();
        assert_ne!(id, random_invite_id().unwrap());
        for bad in [
            "",
            "AB".repeat(16).as_str(),
            "ab".repeat(15).as_str(),
            "zz".repeat(16).as_str(),
        ] {
            assert!(check_invite_id(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn rejections_keep_generic_codes() {
        assert_eq!(
            rejection("bad_request".into(), "m".into()),
            ServiceError::BadRequest("m".into())
        );
        assert_eq!(
            rejection("already_initialized".into(), "m".into()),
            ServiceError::AlreadyInitialized
        );
        // Apply says `not_ready` for a removal or a takeover that has to
        // wait; it is the same 503 as when the service says it.
        assert_eq!(
            rejection("not_ready".into(), "m".into()),
            ServiceError::NotReady("m".into())
        );
        // `not_found` keeps the message that says what is missing.
        assert_eq!(
            rejection("not_found".into(), String::new()),
            ServiceError::NotFound
        );
        assert_eq!(
            rejection("not_found".into(), "m".into()),
            ServiceError::Rejected {
                code: "not_found".into(),
                message: "m".into()
            }
        );
        let used = rejection("invite_used".into(), "m".into());
        assert_eq!(used.mesh_code(), Some("invite_used"));
        assert_eq!(ServiceError::NoLeader.mesh_code(), None);
        let upstream = ServiceError::Upstream {
            status: 410,
            body: ErrorBody::new("invite_expired"),
        };
        assert_eq!(upstream.mesh_code(), Some("invite_expired"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invite_outside_a_mesh_is_mesh_not_configured() {
        let node = TestNode::start_initialized().await;
        let err = node
            .service
            .invite(CreateInviteRequest {
                name: "node-2".into(),
                endpoint: "198.51.100.2".into(),
                ttl_secs: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NOT_CONFIGURED));
        let err = node
            .service
            .invite(CreateInviteRequest {
                name: "node-2".into(),
                endpoint: "198.51.100.2".into(),
                ttl_secs: Some(MAX_INVITE_TTL_SECS + 1),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::BadRequest(_)), "{err:?}");
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_invite_validates_input() {
        let node = TestNode::start_initialized().await;
        let (_, pk) = generate_keypair().unwrap();
        let good = PeerInviteRequest {
            invite_id: "ab".repeat(16),
            public_key: pk,
            endpoint: "198.51.100.2:51820".into(),
            name: "node-2".into(),
            ttl_secs: 60,
        };
        let source: SocketAddr = "127.0.0.1:40000".parse().unwrap();
        for bad in [
            PeerInviteRequest {
                invite_id: "x".into(),
                ..good.clone()
            },
            PeerInviteRequest {
                public_key: "not a key".into(),
                ..good.clone()
            },
            PeerInviteRequest {
                endpoint: "node.example:51820".into(),
                ..good.clone()
            },
        ] {
            let err = node
                .service
                .peer_invite(bad, false, source)
                .await
                .unwrap_err();
            assert!(matches!(err, ServiceError::BadRequest(_)), "{err:?}");
        }
        let err = node
            .service
            .peer_invite(good, false, source)
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NOT_CONFIGURED));
        node.shutdown().await;
    }

    fn peer_invite_req(name: &str) -> PeerInviteRequest {
        PeerInviteRequest {
            invite_id: random_invite_id().unwrap(),
            public_key: generate_keypair().unwrap().1,
            endpoint: "198.51.100.9:51820".into(),
            name: name.into(),
            ttl_secs: 60,
        }
    }

    /// Node 1 is registered; node 2 only holds an invite. Its mesh IP
    /// passes the peer firewall, and with its own key it could otherwise
    /// make itself an invite for the name of a live node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_invite_must_come_from_a_registered_node() {
        let node = TestNode::start_initialized().await;
        invited(&node).await;
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invitee = mesh.invites.values().next().unwrap().mesh_ip;
        let strict = strict_service(&node);
        let invites = || node.service.read_mesh().unwrap().unwrap().invites.len();

        let holder = format!("{invitee}:40000");
        for source in [holder.as_str(), "198.51.100.7:40000", "127.0.0.1:40000"] {
            for forwarded in [false, true] {
                let err = strict
                    .peer_invite(
                        peer_invite_req("node-1"),
                        forwarded,
                        source.parse().unwrap(),
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, ServiceError::Forbidden(_)),
                    "{source}: {err:?}"
                );
            }
        }
        assert_eq!(invites(), 1);

        // A registered node, which is what a forwarding follower is.
        let registered = "10.99.0.1:40000".parse().unwrap();
        let got = strict
            .peer_invite(peer_invite_req("node-3"), true, registered)
            .await
            .unwrap();
        assert_eq!(got.name, "node-3");
        assert_eq!(invites(), 2);

        // The leader's own invite does not come through the route.
        let own = strict
            .invite(CreateInviteRequest {
                name: "node-4".into(),
                endpoint: "198.51.100.4".into(),
                ttl_secs: None,
            })
            .await
            .unwrap();
        assert_eq!(own.name, "node-4");
        assert_eq!(invites(), 3);

        // A loopback peer listener does not check the source.
        let anyone = "127.0.0.1:40000".parse().unwrap();
        node.service
            .peer_invite(peer_invite_req("node-5"), false, anyone)
            .await
            .unwrap();
        assert_eq!(invites(), 4);

        drop(strict);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_validates_peer_and_refuses_when_already_joined() {
        let node = TestNode::start().await;
        for bad in ["", " ", "a/b", "user@host:1"] {
            let err = node
                .service
                .join(JoinRequest { peer: bad.into() })
                .await
                .unwrap_err();
            assert!(matches!(err, ServiceError::BadRequest(_)), "{bad}: {err:?}");
        }
        node.service.init().await.unwrap();
        let err = node
            .service
            .join(JoinRequest {
                peer: "127.0.0.1:1".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(err, ServiceError::AlreadyJoined);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_join_without_leader_is_no_leader() {
        let node = TestNode::start().await;
        let req = PeerJoinRequest {
            node_id: 2,
            addr: "127.0.0.1:1".into(),
            invite_id: None,
            instance_id: None,
        };
        assert_eq!(
            node.service.peer_join(req.clone(), false).await,
            Err(ServiceError::NoLeader)
        );
        assert_eq!(node.service.replication(), Err(ServiceError::NoLeader));
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leader_join_is_idempotent_and_checks_addresses() {
        let node = TestNode::start_initialized().await;
        let me = PeerJoinRequest {
            node_id: 1,
            addr: node.peer_addr.to_string(),
            invite_id: None,
            instance_id: None,
        };
        node.service.peer_join(me, false).await.unwrap();

        let clash = PeerJoinRequest {
            node_id: 1,
            addr: "127.0.0.1:1".into(),
            invite_id: None,
            instance_id: None,
        };
        let err = node.service.peer_join(clash, false).await.unwrap_err();
        assert!(matches!(err, ServiceError::BadRequest(_)), "{err:?}");

        let bad = PeerJoinRequest {
            node_id: 5,
            addr: "".into(),
            invite_id: None,
            instance_id: None,
        };
        let err = node.service.peer_join(bad, false).await.unwrap_err();
        assert!(matches!(err, ServiceError::BadRequest(_)), "{err:?}");
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_learner_is_not_promoted() {
        let node = TestNode::start_initialized().await;
        // Node 2 was added as a learner by an earlier join that then stopped;
        // nothing listens at its address.
        node.service
            .raft()
            .add_learner(2, Node::new("127.0.0.1:1"), false)
            .await
            .unwrap();
        let req = PeerJoinRequest {
            node_id: 2,
            addr: "127.0.0.1:1".into(),
            invite_id: None,
            instance_id: None,
        };
        let err = node.service.peer_join(req, false).await.unwrap_err();
        assert!(matches!(err, ServiceError::NotReady(_)), "{err:?}");
        assert!(!node.service.is_voter(2));
        assert!(node.service.is_voter(1));
        // The single voter still commits writes.
        node.service
            .propose(Request::Set {
                key: "k".into(),
                value: serde_json::json!(1),
            })
            .await
            .unwrap();
        node.shutdown().await;
    }

    const MACHINE: &str = "11111111111111111111111111111111";
    const OTHER_MACHINE: &str = "22222222222222222222222222222222";

    /// A mesh with an invite for node 2. Returns the invite's id.
    async fn invited(node: &TestNode) -> String {
        seed_mesh(node).await;
        let invite = node
            .service
            .invite(CreateInviteRequest {
                name: "node-2".into(),
                endpoint: "198.51.100.2".into(),
                ttl_secs: None,
            })
            .await
            .unwrap();
        InviteToken::decode(&invite.token).unwrap().invite_id
    }

    fn register_req(invite_id: &str, instance_id: &str) -> PeerRegisterRequest {
        PeerRegisterRequest {
            invite_id: invite_id.into(),
            node_id: 2,
            endpoint: "198.51.100.2:51820".into(),
            instance_id: instance_id.into(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_invite_registers_one_machine() {
        let node = TestNode::start_initialized().await;
        let invite_id = invited(&node).await;
        let svc = &node.service;

        svc.peer_register(register_req(&invite_id, MACHINE), false)
            .await
            .unwrap();
        let mesh = svc.read_mesh().unwrap().unwrap();
        assert_eq!(mesh.nodes[&2].instance_id.as_deref(), Some(MACHINE));
        assert!(mesh.invites.is_empty());

        // The same machine again: its first answer may have been lost.
        svc.peer_register(register_req(&invite_id, MACHINE), false)
            .await
            .unwrap();
        // A second machine with a copy of the token.
        let err = svc
            .peer_register(register_req(&invite_id, OTHER_MACHINE), false)
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::INVITE_USED));
        assert_eq!(
            svc.read_mesh().unwrap().unwrap().nodes[&2]
                .instance_id
                .as_deref(),
            Some(MACHINE)
        );
        node.shutdown().await;
    }

    fn source(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), 40_000)
    }

    fn with_test_key(svc: FaberService) -> FaberService {
        svc.with_cluster_keys(Arc::new(crate::secrets::StaticClusterKey(
            crate::testing::test_cluster_key(),
        )))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_cluster_key_goes_only_to_the_machine_the_invite_is_bound_to() {
        let node = TestNode::start_initialized().await;
        let invite_id = invited(&node).await;
        let invitee = node.service.read_mesh().unwrap().unwrap().invites[&invite_id].mesh_ip;
        let strict = with_test_key(strict_mesh_service_at(
            &node,
            "10.99.0.1",
            &[],
            &JoinPending::new(false),
        ));
        let key = crate::testing::test_cluster_key();
        let expected = PeerRegisterResponse {
            cluster_key: Some(crate::dto::WireKey::new(key.to_base64())),
            key_id: Some(faber_crypto::key_id(&key)),
        };
        let register = |forwarded, from: &str, machine| {
            strict.peer_register_for(register_req(&invite_id, machine), forwarded, source(from))
        };

        // Another address, with the right instance id: registered, no key.
        let got = register(false, "10.99.0.9", MACHINE).await.unwrap();
        assert_eq!(got, PeerRegisterResponse::default());
        // A forwarded request shows the forwarder's address: no key.
        let got = register(true, &invitee.to_string(), MACHINE).await.unwrap();
        assert_eq!(got, PeerRegisterResponse::default());
        // Another machine holding a copy of the token: refused.
        let err = register(false, &invitee.to_string(), OTHER_MACHINE)
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::INVITE_USED));
        // The invite's own address, and again after a lost answer.
        for _ in 0..2 {
            let got = register(false, &invitee.to_string(), MACHINE)
                .await
                .unwrap();
            assert_eq!(got, expected);
        }
        let mapped = format!("::ffff:{invitee}");
        assert_eq!(register(false, &mapped, MACHINE).await.unwrap(), expected);

        // A loopback listener cannot tell nodes apart and releases nothing.
        let loopback = with_test_key(node.service.clone());
        let got = loopback
            .peer_register_for(
                register_req(&invite_id, MACHINE),
                false,
                source(&invitee.to_string()),
            )
            .await
            .unwrap();
        assert_eq!(got, PeerRegisterResponse::default());
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_without_a_key_file_releases_no_key() {
        let node = TestNode::start_initialized().await;
        let invite_id = invited(&node).await;
        let invitee = node.service.read_mesh().unwrap().unwrap().invites[&invite_id].mesh_ip;
        let strict = strict_mesh_service_at(&node, "10.99.0.1", &[], &JoinPending::new(false));
        let got = strict
            .peer_register_for(
                register_req(&invite_id, MACHINE),
                false,
                source(&invitee.to_string()),
            )
            .await
            .unwrap();
        assert_eq!(got, PeerRegisterResponse::default());
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_join_token_never_holds_the_cluster_key() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        let invite = node
            .service
            .invite(CreateInviteRequest {
                name: "node-2".into(),
                endpoint: "198.51.100.2".into(),
                ttl_secs: None,
            })
            .await
            .unwrap();
        let key = crate::testing::test_cluster_key();
        let text = key.to_base64();
        let token = InviteToken::decode(&invite.token).unwrap();
        let decoded = serde_json::to_vec(&token).unwrap();
        let has =
            |haystack: &[u8], needle: &[u8]| haystack.windows(needle.len()).any(|w| w == needle);
        for needle in [text.as_bytes(), &[7u8; 32][..]] {
            assert!(!has(invite.token.as_bytes(), needle));
            assert!(!has(&decoded, needle));
        }
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_joiner_stores_the_key_and_refuses_one_that_is_not_the_clusters() {
        let node = TestNode::start_initialized().await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.key");
        let joiner = node
            .service
            .clone()
            .with_cluster_keys(Arc::new(crate::secrets::FileClusterKey::new(&path)));
        let key = crate::testing::test_cluster_key();
        let id = faber_crypto::key_id(&key);
        let sent = |key: &faber_crypto::ClusterKey, key_id: Option<&str>| PeerRegisterResponse {
            cluster_key: Some(crate::dto::WireKey::new(key.to_base64())),
            key_id: key_id.map(String::from),
        };

        // A key of another id than the leader names is not stored.
        let other = faber_crypto::ClusterKey::from_bytes([1; faber_crypto::KEY_LEN]);
        let err = joiner
            .accept_cluster_key(&sent(&other, Some(&id)))
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::secrets::KeyDeliveryError::Mismatch { .. }),
            "{err:?}"
        );
        // Nor one that disagrees with the id in this node's own raft.
        let err = joiner
            .accept_cluster_key(&sent(&other, None))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::secrets::KeyDeliveryError::Mismatch { .. }
        ));
        assert!(!path.exists());
        let err = joiner
            .accept_cluster_key(&PeerRegisterResponse {
                cluster_key: Some(crate::dto::WireKey::new("not a key".to_string().into())),
                key_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, crate::secrets::KeyDeliveryError::Malformed));

        // The right one is written, private, before anything else happens.
        joiner
            .accept_cluster_key(&sent(&key, Some(&id)))
            .await
            .unwrap();
        use crate::secrets::ClusterKeyProvider as _;
        assert_eq!(
            crate::secrets::FileClusterKey::new(&path)
                .cluster_key()
                .unwrap(),
            key
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // A cluster with no key to give is no error.
        joiner
            .accept_cluster_key(&PeerRegisterResponse::default())
            .await
            .unwrap();
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn register_validates_its_input() {
        let node = TestNode::start_initialized().await;
        let invite_id = invited(&node).await;
        let svc = &node.service;
        for bad in [
            register_req("x", MACHINE),
            register_req(&invite_id, ""),
            register_req(&invite_id, &"AB".repeat(16)),
        ] {
            let err = svc.peer_register(bad, false).await.unwrap_err();
            assert!(matches!(err, ServiceError::BadRequest(_)), "{err:?}");
        }
        let mut hostname = register_req(&invite_id, MACHINE);
        hostname.endpoint = "node2.example:51820".into();
        let err = svc.peer_register(hostname, false).await.unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::INVITE_INVALID));
        // An invite the mesh has never had is refused without a raft write:
        // anyone the peer firewall admits can send these.
        let before = svc.metrics().last_log_index;
        for _ in 0..3 {
            let err = svc
                .peer_register(register_req(&"00".repeat(16), MACHINE), false)
                .await
                .unwrap_err();
            assert_eq!(err.mesh_code(), Some(error_code::INVITE_INVALID));
        }
        assert_eq!(svc.metrics().last_log_index, before);
        // Nothing was registered.
        assert_eq!(svc.read_mesh().unwrap().unwrap().nodes.len(), 1);
        node.shutdown().await;
    }

    /// An invite that has run out is still in the state until the next
    /// invite is made. Registering with it gets the answer apply would
    /// give, without a raft write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_expired_invite_is_refused_without_a_raft_write() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        let svc = &node.service;
        let invite = svc
            .invite(CreateInviteRequest {
                name: "node-2".into(),
                endpoint: "198.51.100.2".into(),
                ttl_secs: Some(1),
            })
            .await
            .unwrap();
        let invite_id = InviteToken::decode(&invite.token).unwrap().invite_id;
        while now_secs() < invite.expires_at {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let before = svc.metrics().last_log_index;
        let err = svc
            .peer_register(register_req(&invite_id, MACHINE), false)
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::INVITE_EXPIRED));
        assert_eq!(svc.metrics().last_log_index, before);
        node.shutdown().await;
    }

    /// A joiner registered, went away for longer than its lease, and was
    /// removed. Its join can never work again, and the answer says so. A
    /// node that has not registered yet is told to retry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_join_for_a_node_the_mesh_removed_is_mesh_node_removed() {
        let node = TestNode::start_initialized().await;
        let invite_id = invited(&node).await;
        let svc = &node.service;
        let join = PeerJoinRequest {
            node_id: 2,
            addr: "127.0.0.1:1".into(),
            invite_id: Some(invite_id.clone()),
            instance_id: Some(MACHINE.into()),
        };
        let err = svc.peer_join(join.clone(), false).await.unwrap_err();
        assert!(matches!(err, ServiceError::NotReady(_)), "{err:?}");

        svc.peer_register(register_req(&invite_id, MACHINE), false)
            .await
            .unwrap();
        svc.set_lease(300, false).await.unwrap();
        for _ in 0..11 {
            advance(&node, 30).await;
        }
        svc.propose(Request::RemoveNode { node_id: 2 })
            .await
            .unwrap();
        assert!(!svc.read_mesh().unwrap().unwrap().nodes.contains_key(&2));

        let err = svc.peer_join(join, false).await.unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NODE_REMOVED));
        let answer = axum::response::IntoResponse::into_response(err);
        assert_eq!(answer.status(), axum::http::StatusCode::CONFLICT);
        // The membership was not touched.
        assert!(!svc.member_ids().contains(&2));
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn register_outside_a_mesh_is_mesh_not_configured() {
        let node = TestNode::start_initialized().await;
        let err = node
            .service
            .peer_register(register_req(&"ab".repeat(16), MACHINE), false)
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NOT_CONFIGURED));
        node.shutdown().await;
    }

    /// The join no longer registers: it only adds a node whose record says
    /// it was registered by the machine that asks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mesh_join_needs_the_registered_instance() {
        let node = TestNode::start_initialized().await;
        let invite_id = invited(&node).await;
        let svc = &node.service;
        let join = |invite: Option<&str>, instance: Option<&str>| PeerJoinRequest {
            node_id: 2,
            addr: "127.0.0.1:1".into(),
            invite_id: invite.map(str::to_string),
            instance_id: instance.map(str::to_string),
        };

        // Not registered yet: retry later, and the invite is still unused.
        let err = svc
            .peer_join(join(Some(&invite_id), Some(MACHINE)), false)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::NotReady(_)), "{err:?}");
        assert_eq!(svc.read_mesh().unwrap().unwrap().invites.len(), 1);

        svc.peer_register(register_req(&invite_id, MACHINE), false)
            .await
            .unwrap();
        // Without the invite or the instance, and with another invite.
        for req in [
            join(None, None),
            join(Some(&invite_id), None),
            join(None, Some(MACHINE)),
            join(Some(&"00".repeat(16)), Some(MACHINE)),
        ] {
            let err = svc.peer_join(req.clone(), false).await.unwrap_err();
            assert_eq!(
                err.mesh_code(),
                Some(error_code::INVITE_INVALID),
                "{req:?}: {err:?}"
            );
        }
        // Another machine with the same token.
        let err = svc
            .peer_join(join(Some(&invite_id), Some(OTHER_MACHINE)), false)
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::INVITE_USED));
        // None of them touched the membership.
        assert!(
            !svc.metrics()
                .membership_config
                .membership()
                .nodes()
                .any(|(id, _)| *id == 2)
        );
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn health_reports_a_stopped_raft_core() {
        let node = TestNode::start_initialized().await;
        assert_eq!(node.service.health_problem(), None);
        let health = node.service.health();
        assert_eq!((health.status.as_str(), health.node_id), (HEALTH_OK, 1));

        node.service.raft().shutdown().await.unwrap();
        let problem = node.service.health_problem().expect("raft is stopped");
        assert!(problem.contains("raft core is not running"), "{problem}");
        let health = node.service.health();
        assert_eq!(
            (health.status.as_str(), health.node_id),
            (HEALTH_UNAVAILABLE, 1)
        );
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_of_single_node_leader() {
        let node = TestNode::start_initialized().await;
        let s = node.service.status().await;
        assert!(s.joined);
        assert_eq!(s.role, "leader");
        assert_eq!(s.leader_id, Some(1));
        assert_eq!(s.leader_addr, Some(node.peer_addr.to_string()));
        assert_eq!(s.members.len(), 1);
        let me = &s.members[0];
        assert!(me.voter);
        assert_eq!(me.addr, node.peer_addr.to_string());
        assert!(me.matched_index.is_some());
        assert_eq!(me.lag, Some(0));
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_names_members_from_the_mesh() {
        let node = TestNode::start_initialized().await;
        assert_eq!(node.service.status().await.members[0].name, None);
        seed_mesh(&node).await;
        let s = node.service.status().await;
        assert_eq!(s.members[0].name.as_deref(), Some("node-1"));
        // A raft member the mesh does not know has no name.
        node.service
            .raft()
            .add_learner(2, Node::new("127.0.0.1:1"), false)
            .await
            .unwrap();
        let s = node.service.status().await;
        let names: Vec<_> = s.members.iter().map(|m| m.name.as_deref()).collect();
        assert_eq!(names, [Some("node-1"), None]);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_lease_is_read_from_state_and_set_through_raft() {
        let node = TestNode::start_initialized().await;
        let svc = &node.service;
        let err = svc.lease().await.unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NOT_CONFIGURED));
        let err = svc.set_lease(600, false).await.unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NOT_CONFIGURED));

        seed_mesh(&node).await;
        let default = crate::mesh::state::DEFAULT_LEASE_SECS;
        assert_eq!(svc.lease().await.unwrap().lease_secs, default);
        assert_eq!(svc.set_lease(600, false).await.unwrap().lease_secs, 600);
        assert_eq!(svc.lease().await.unwrap().lease_secs, 600);
        // The leader handles a forwarded request the same way.
        assert_eq!(svc.set_lease(900, true).await.unwrap().lease_secs, 900);

        // Apply owns the bounds.
        for bad in [0, 299, 2_592_001] {
            let err = svc.set_lease(bad, false).await.unwrap_err();
            assert!(matches!(err, ServiceError::BadRequest(_)), "{bad}: {err:?}");
        }
        assert_eq!(svc.lease().await.unwrap().lease_secs, 900);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn set_lease_without_a_leader_is_no_leader() {
        let node = TestNode::start().await;
        for forwarded in [false, true] {
            assert_eq!(
                node.service.set_lease(600, forwarded).await,
                Err(ServiceError::NoLeader)
            );
        }
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_status_interval_is_set_through_raft_within_bounds() {
        let node = TestNode::start_initialized().await;
        let svc = &node.service;
        assert_eq!(svc.status_interval().await.unwrap().interval_secs, 5);
        assert_eq!(
            svc.set_status_interval(30, false)
                .await
                .unwrap()
                .interval_secs,
            30
        );
        // The leader handles a forwarded request the same way.
        assert_eq!(
            svc.set_status_interval(60, true)
                .await
                .unwrap()
                .interval_secs,
            60
        );
        for bad in [0, 301] {
            let err = svc.set_status_interval(bad, false).await.unwrap_err();
            assert!(matches!(err, ServiceError::BadRequest(_)), "{bad}: {err:?}");
        }
        assert_eq!(svc.status_interval().await.unwrap().interval_secs, 60);
        node.shutdown().await;
    }

    /// A follower forwards once; a request that was forwarded already is not
    /// forwarded again. With no leader there is nowhere to send either.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_writes_without_a_leader_are_no_leader() {
        let node = TestNode::start().await;
        let report = || ReportNode {
            node_id: 1,
            instance_id: "ab".repeat(16),
            capacity: Default::default(),
            replicas: Vec::new(),
        };
        for forwarded in [false, true] {
            assert_eq!(
                node.service.set_status_interval(10, forwarded).await,
                Err(ServiceError::NoLeader)
            );
            assert_eq!(
                node.service.report_node(report(), forwarded).await,
                Err(ServiceError::NoLeader)
            );
        }
        node.shutdown().await;
    }

    /// Node 1, the founder, has no instance id yet, and the first report
    /// records one. Node 2 is registered and may forward for node 1, but it
    /// may not claim node 1's instance id.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_a_node_itself_records_its_instance_id_through_a_report() {
        let node = TestNode::start_initialized().await;
        let invite = invited(&node).await;
        node.service
            .peer_register(register_req(&invite, &"ef".repeat(16)), false)
            .await
            .unwrap();
        let strict = strict_service(&node);
        let report = |instance_id: &str| ReportNode {
            node_id: 1,
            instance_id: instance_id.into(),
            capacity: Default::default(),
            replicas: Vec::new(),
        };
        let claimed = || {
            strict.read_mesh().unwrap().unwrap().nodes[&1]
                .instance_id
                .clone()
        };
        let other = "10.99.0.2:40000".parse().unwrap();
        let own = "10.99.0.1:40000".parse().unwrap();
        let thief = "ab".repeat(16);

        let err = strict
            .peer_report(report(&thief), true, other)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::Forbidden(_)), "{err:?}");
        assert_eq!(claimed(), None);

        strict
            .peer_report(report(&thief), false, own)
            .await
            .unwrap();
        assert_eq!(claimed(), Some(thief));
        drop(strict);
        node.shutdown().await;
    }

    /// Node 1 is registered; node 2 only holds an invite, so its mesh IP
    /// passes the peer firewall without being a node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_may_only_report_for_itself_and_only_a_node_sets_the_interval() {
        let node = TestNode::start_initialized().await;
        invited(&node).await;
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invitee = mesh.invites.values().next().unwrap().mesh_ip;
        let strict = strict_service(&node);
        let report = |node_id| ReportNode {
            node_id,
            instance_id: "ab".repeat(16),
            capacity: Default::default(),
            replicas: Vec::new(),
        };
        let interval = |secs| PeerStatusIntervalRequest {
            interval_secs: secs,
        };

        let holder = format!("{invitee}:40000");
        for source in [holder.as_str(), "198.51.100.7:40000", "127.0.0.1:40000"] {
            for forwarded in [false, true] {
                let source = source.parse().unwrap();
                let err = strict
                    .peer_report(report(1), forwarded, source)
                    .await
                    .unwrap_err();
                assert!(matches!(err, ServiceError::Forbidden(_)), "{err:?}");
                let err = strict
                    .peer_set_status_interval(interval(10), forwarded, source)
                    .await
                    .unwrap_err();
                assert!(matches!(err, ServiceError::Forbidden(_)), "{err:?}");
            }
        }
        // Node 1 for itself, and a registered node forwarding for another.
        let registered = "10.99.0.1:40000".parse().unwrap();
        strict
            .peer_report(report(1), false, registered)
            .await
            .unwrap();
        strict
            .peer_report(report(1), true, registered)
            .await
            .unwrap();
        strict
            .peer_set_status_interval(interval(10), false, registered)
            .await
            .unwrap();
        assert_eq!(strict.status_interval().await.unwrap().interval_secs, 10);
        // Not for another node when it is not forwarded.
        let err = strict
            .peer_report(report(2), false, registered)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::Forbidden(_)), "{err:?}");
        // The instance id has the shape of one.
        let mut bad = report(1);
        bad.instance_id = "nope".into();
        let err = strict
            .peer_report(bad, false, registered)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::BadRequest(_)), "{err:?}");

        drop(strict);
        node.shutdown().await;
    }

    /// Node 1 is registered; node 2 only holds an invite, so its mesh IP
    /// passes the peer firewall without being a node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_lease_change_must_come_from_a_registered_node() {
        let node = TestNode::start_initialized().await;
        invited(&node).await;
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invitee = mesh.invites.values().next().unwrap().mesh_ip;
        let strict = strict_service(&node);
        let req = |secs| PeerLeaseRequest { lease_secs: secs };

        let holder = format!("{invitee}:40000");
        for source in [holder.as_str(), "198.51.100.7:40000", "127.0.0.1:40000"] {
            for forwarded in [false, true] {
                let err = strict
                    .peer_set_lease(req(600), forwarded, source.parse().unwrap())
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, ServiceError::Forbidden(_)),
                    "{source}: {err:?}"
                );
            }
        }
        assert_eq!(mesh.lease_secs, strict.lease().await.unwrap().lease_secs);

        // A registered node, which is what a forwarding follower is.
        let registered = "10.99.0.1:40000".parse().unwrap();
        for (secs, forwarded) in [(600, false), (900, true)] {
            let got = strict
                .peer_set_lease(req(secs), forwarded, registered)
                .await
                .unwrap();
            assert_eq!(got.lease_secs, secs);
        }

        // A loopback peer listener does not check the source.
        let anyone = "127.0.0.1:40000".parse().unwrap();
        let got = node
            .service
            .peer_set_lease(req(1_200), false, anyone)
            .await
            .unwrap();
        assert_eq!(got.lease_secs, 1_200);

        drop(strict);
        node.shutdown().await;
    }

    /// A service on the raft of `node` as a mesh node on a mesh address
    /// has it: the source of a raft RPC is checked, and it knows the peers
    /// of its join token, `bootstrap`.
    ///
    /// Its own mesh address is 10.99.0.7, so that 10.99.0.1, the founder
    /// in the state of `node`, is another node to it. `pending` is the
    /// flag that says whether it is still joining.
    fn strict_mesh_service_with(
        node: &TestNode,
        bootstrap: &[&str],
        pending: &JoinPending,
    ) -> FaberService {
        strict_mesh_service_at(node, "10.99.0.7", bootstrap, pending)
    }

    /// [`strict_mesh_service_with`] for a node whose mesh address is `own`.
    fn strict_mesh_service_at(
        node: &TestNode,
        own: &str,
        bootstrap: &[&str],
        pending: &JoinPending,
    ) -> FaberService {
        let (_, public_key) = generate_keypair().unwrap();
        let local = MeshLocal {
            config: MeshConfig::default(),
            record: NodeRecord {
                node_id: 1,
                public_key,
                mesh_ip: own.parse().unwrap(),
                endpoint: None,
                invite_id: None,
                instance_id: None,
                container_subnet: None,
                name: "node-1".into(),
                lease_renewed_at: 0,
            },
            bootstrap_peers: bootstrap.iter().map(|ip| ip.parse().unwrap()).collect(),
            join_pending: pending.clone(),
        };
        FaberService::new(
            1,
            format!("{own}:7401"),
            node.service.raft().clone(),
            node.service.inner.state.clone(),
            Some(local),
        )
        .unwrap()
    }

    /// [`strict_mesh_service_with`] for a node that is still joining when
    /// it has `bootstrap` peers, and a member when it has none.
    fn strict_mesh_service(node: &TestNode, bootstrap: &[&str]) -> FaberService {
        let pending = JoinPending::new(!bootstrap.is_empty());
        strict_mesh_service_with(node, bootstrap, &pending)
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// Node 1 is registered; node 2 only holds an invite, so the peer
    /// firewall lets its mesh IP in. A follower takes raft RPCs from the
    /// leader, which is a registered node, and not from the invite holder
    /// until that has registered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raft_rpcs_are_taken_from_registered_nodes_only() {
        let node = TestNode::start_initialized().await;
        let invite_id = invited(&node).await;
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invitee = IpAddr::V4(mesh.invites.values().next().unwrap().mesh_ip);
        let strict = strict_mesh_service(&node, &[]);

        assert!(strict.raft_source_allowed(ip("10.99.0.1")));
        assert!(strict.raft_source_allowed(ip("::ffff:10.99.0.1")));
        for other in [
            invitee,
            ip("10.99.0.9"),
            ip("198.51.100.7"),
            ip("127.0.0.2"),
        ] {
            assert!(!strict.raft_source_allowed(other), "{other}");
        }
        // The peers of a join token count only while the join is pending.
        let joined = strict_mesh_service_with(&node, &["10.99.0.9"], &JoinPending::new(false));
        assert!(!joined.raft_source_allowed(ip("10.99.0.9")));
        // Raft never sends to itself. A request from this node's own mesh
        // address is a local process, though the state has that address as
        // a registered node and the membership has it as a member.
        node.service
            .raft()
            .add_learner(7, Node::new("10.99.0.1:7401"), false)
            .await
            .unwrap();
        let founder = strict_mesh_service_at(&node, "10.99.0.1", &[], &JoinPending::new(false));
        for own in [ip("10.99.0.1"), ip("::ffff:10.99.0.1")] {
            assert!(strict.raft_source_allowed(own), "{own}");
            assert!(!founder.raft_source_allowed(own), "{own}");
        }
        drop(founder);

        // Once it has registered it is a node like any other.
        node.service
            .peer_register(register_req(&invite_id, MACHINE), false)
            .await
            .unwrap();
        assert!(strict.raft_source_allowed(invitee));

        // A loopback peer listener (development and single-host tests)
        // checks nothing, with or without a mesh identity.
        let loopback = mesh_service(&node, MACHINE);
        for anyone in [ip("10.99.0.9"), ip("198.51.100.7"), ip("127.0.0.1")] {
            assert!(node.service.raft_source_allowed(anyone), "{anyone}");
            assert!(loopback.raft_source_allowed(anyone), "{anyone}");
        }
        // Outside mesh mode there are no invites and no mesh to check
        // against: a cluster on a WireGuard interface of its own keeps
        // working.
        let no_mesh = strict_service(&node);
        assert!(no_mesh.raft_source_allowed(ip("10.99.0.9")));

        drop((strict, joined, loopback, no_mesh));
        node.shutdown().await;
    }

    /// A joiner has no copy of the mesh until its first append or snapshot
    /// arrives, and that comes from the leader. Until then it takes raft
    /// RPCs from the peers in its token, which were the registered nodes
    /// when the invite was made, and from nobody else.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_joiner_takes_raft_rpcs_from_the_peers_in_its_token() {
        let node = TestNode::start().await;
        assert_eq!(node.service.read_mesh().unwrap(), None);
        let joiner = strict_mesh_service(&node, &["10.99.0.1", "10.99.0.3"]);
        assert!(joiner.raft_source_allowed(ip("10.99.0.1")));
        assert!(joiner.raft_source_allowed(ip("::ffff:10.99.0.3")));
        // Another invite holder, and an address outside the mesh.
        for other in [ip("10.99.0.4"), ip("198.51.100.7"), ip("127.0.0.1")] {
            assert!(!joiner.raft_source_allowed(other), "{other}");
        }
        // A founder before `cluster init` has no peer at all.
        let founder = strict_mesh_service(&node, &[]);
        assert!(!founder.raft_source_allowed(ip("10.99.0.1")));

        drop((joiner, founder));
        node.shutdown().await;
    }

    /// A joiner applies the log in batches. The first one gives it a mesh
    /// with the nodes of that prefix. A leader that registered later is, in
    /// that mesh, an invite at best, and is not in any membership the
    /// joiner has. Its appends are the only way forward, so the peers of
    /// the token count for as long as the join is pending, next to the
    /// mesh. Here node 2 is that leader: registered for real, an invite in
    /// what the joiner has applied.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_joiner_takes_appends_from_a_leader_its_prefix_does_not_have() {
        let node = TestNode::start_initialized().await;
        invited(&node).await;
        // Another invite, made after the joiner's token: not in it.
        node.service
            .invite(CreateInviteRequest {
                name: "node-3".into(),
                endpoint: "198.51.100.3".into(),
                ttl_secs: None,
            })
            .await
            .unwrap();
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invited_ip = |id: NodeId| {
            let invite = mesh.invites.values().find(|i| i.node_id == id).unwrap();
            IpAddr::V4(invite.mesh_ip)
        };
        let (leader, stranger) = (invited_ip(2), invited_ip(3));
        assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), [1]);

        let pending = JoinPending::new(true);
        let token_peers = ["10.99.0.1", "10.99.0.2"];
        assert_eq!(leader, ip(token_peers[1]));
        let joiner = strict_mesh_service_with(&node, &token_peers, &pending);
        // The leader, from the token, and the founder, from both.
        assert!(joiner.raft_source_allowed(leader));
        assert!(joiner.raft_source_allowed(ip("10.99.0.1")));
        // An address that only holds an invite is still refused.
        assert!(!joiner.raft_source_allowed(stranger));
        assert!(!joiner.raft_source_allowed(ip("198.51.100.7")));

        // The join is done: from here on only replicated state counts.
        pending.clear();
        assert!(!joiner.raft_source_allowed(leader));
        assert!(joiner.raft_source_allowed(ip("10.99.0.1")));

        drop(joiner);
        node.shutdown().await;
    }

    /// The log can be ahead of the state: a node that restarts between
    /// appending a registration and applying it knows the new node only as
    /// an invite, but has the membership that names it. If that node now
    /// leads, refusing it would leave this one behind for good. A node
    /// becomes a raft member only after it has registered, so a member's
    /// address is as good as a mesh record.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raft_rpcs_are_taken_from_members_the_state_does_not_have_yet() {
        let node = TestNode::start_initialized().await;
        invited(&node).await;
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invitee = mesh.invites.values().next().unwrap().mesh_ip;
        let strict = strict_mesh_service(&node, &[]);
        assert!(!strict.raft_source_allowed(IpAddr::V4(invitee)));

        node.service
            .raft()
            .add_learner(2, Node::new(format!("{invitee}:7401")), false)
            .await
            .unwrap();
        assert!(strict.raft_source_allowed(IpAddr::V4(invitee)));
        // The state still has it as an invite only.
        let mesh = node.service.read_mesh().unwrap().unwrap();
        assert!(!mesh.nodes.contains_key(&2));
        // A node that is in neither is still refused.
        assert!(!strict.raft_source_allowed(ip("10.99.0.9")));

        drop(strict);
        node.shutdown().await;
    }

    /// The attack the check is for. A machine that holds an invite and has
    /// not registered posts a snapshot with a higher committed vote. Taken,
    /// it replaces everything this node has replicated.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_forged_snapshot_from_an_invite_holder_is_refused() {
        use axum::body::Body;
        use axum::extract::ConnectInfo;
        use axum::http::{Request as HttpRequest, StatusCode};
        use openraft::raft::InstallSnapshotRequest;
        use openraft::storage::RaftStateMachine;
        use openraft::{
            CommittedLeaderId, LogId, Membership, RaftSnapshotBuilder, SnapshotMeta,
            StoredMembership, Vote,
        };
        use tower::ServiceExt;

        use crate::raft::rpc::{self, RaftGate, SourceCheck};
        use crate::raft::types::TypeConfig;

        let node = TestNode::start_initialized().await;
        invited(&node).await;
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invitee = mesh.invites.values().next().unwrap().mesh_ip;
        let strict = strict_mesh_service(&node, &[]);
        let sources = {
            let strict = strict.clone();
            SourceCheck::new(move |source| strict.raft_source_allowed(source))
        };
        let router = rpc::router(node.service.raft().clone(), RaftGate::new(true), sources);

        // A well-formed snapshot of an empty state machine, said to come
        // from a leader of term 99 that is the only member.
        let scratch = tempfile::tempdir().unwrap();
        let mut empty = crate::raft::store::open(scratch.path()).unwrap();
        let mut builder = empty.state_machine.get_snapshot_builder().await;
        let data = builder
            .build_snapshot()
            .await
            .unwrap()
            .snapshot
            .into_inner();
        let last = LogId::new(CommittedLeaderId::new(99, 2), 1_000);
        let attacker = Node::new(format!("{invitee}:7401"));
        let nodes = BTreeMap::from([(2, attacker)]);
        let membership = Membership::new(vec![BTreeSet::from([2])], nodes);
        let forged = InstallSnapshotRequest::<TypeConfig> {
            vote: Vote::new_committed(99, 2),
            meta: SnapshotMeta {
                last_log_id: Some(last),
                last_membership: StoredMembership::new(Some(last), membership),
                snapshot_id: "forged".into(),
            },
            offset: 0,
            data,
            done: true,
        };
        let post = |source: String| {
            let mut req = HttpRequest::post("/raft/snapshot")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&forged).unwrap()))
                .unwrap();
            let addr: SocketAddr = source.parse().unwrap();
            req.extensions_mut().insert(ConnectInfo(addr));
            router.clone().oneshot(req)
        };
        let raft_view = || {
            let m = node.service.metrics();
            (m.vote, m.last_applied, m.membership_config.clone())
        };

        let before = (raft_view(), node.service.read_mesh().unwrap());
        let resp = post(format!("{invitee}:40000")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let body: ErrorBody = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.error, error_code::FORBIDDEN);
        assert_eq!((raft_view(), node.service.read_mesh().unwrap()), before);
        // Still the leader, and still able to commit.
        advance(&node, 1).await;

        // What it would have done: the same request from a registered
        // node's address is taken, and the mesh is gone.
        let resp = post("10.99.0.1:40000".to_string()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(node.service.read_mesh().unwrap(), None);
        assert_eq!(node.service.metrics().last_applied, Some(last));

        drop((router, strict, empty));
        node.shutdown().await;
    }

    fn renew_req(node_id: NodeId, instance_id: &str) -> PeerRenewRequest {
        PeerRenewRequest {
            node_id,
            instance_id: instance_id.into(),
        }
    }

    fn renewed_at(node: &TestNode, id: NodeId) -> u64 {
        node.service.read_mesh().unwrap().unwrap().nodes[&id].lease_renewed_at
    }

    async fn advance(node: &TestNode, elapsed_secs: u64) {
        node.service
            .propose(Request::AdvanceClock { elapsed_secs })
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_may_only_renew_its_own_lease() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        advance(&node, 10).await;
        let strict = strict_service(&node);

        // Another mesh address names node 1, with and without the header.
        for forwarded in [false, true] {
            let other = "10.99.0.2:40000".parse().unwrap();
            let err = strict
                .peer_renew(renew_req(1, MACHINE), forwarded, other)
                .await
                .unwrap_err();
            assert!(matches!(err, ServiceError::Forbidden(_)), "{err:?}");
        }
        assert_eq!(renewed_at(&node, 1), 0);

        // Node 1 itself. The founder's record takes the machine that
        // renews first.
        let own = "10.99.0.1:40000".parse().unwrap();
        strict
            .peer_renew(renew_req(1, MACHINE), false, own)
            .await
            .unwrap();
        assert_eq!(renewed_at(&node, 1), 10);

        // Another machine at the right address is refused by apply.
        advance(&node, 10).await;
        let err = strict
            .peer_renew(renew_req(1, OTHER_MACHINE), false, own)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, ServiceError::Rejected { code, .. } if code == error_code::FORBIDDEN),
            "{err:?}"
        );
        assert_eq!(renewed_at(&node, 1), 10);

        // The instance id is checked before anything is proposed.
        for bad in ["", "x", &"AB".repeat(16)] {
            let err = strict
                .peer_renew(renew_req(1, bad), false, own)
                .await
                .unwrap_err();
            assert!(matches!(err, ServiceError::BadRequest(_)), "{bad}: {err:?}");
        }

        drop(strict);
        node.shutdown().await;
    }

    /// A service on the raft of `node` that knows its machine, as a mesh
    /// node's does.
    fn mesh_service(node: &TestNode, instance_id: &str) -> FaberService {
        let (_, public_key) = generate_keypair().unwrap();
        let local = MeshLocal {
            config: MeshConfig::default(),
            record: NodeRecord {
                node_id: 1,
                public_key,
                mesh_ip: "10.99.0.1".parse().unwrap(),
                endpoint: None,
                invite_id: None,
                instance_id: Some(instance_id.into()),
                container_subnet: None,
                name: "node-1".into(),
                lease_renewed_at: 0,
            },
            bootstrap_peers: BTreeSet::new(),
            join_pending: JoinPending::new(false),
        };
        FaberService::new(
            1,
            node.peer_addr.to_string(),
            node.service.raft().clone(),
            node.service.inner.state.clone(),
            Some(local),
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_renewal_after_the_lease_ran_out_is_mesh_lease_expired() {
        let node = TestNode::start_initialized().await;
        // Without an identity there is no lease to renew.
        let err = node.service.renew_own_lease().await.unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NOT_CONFIGURED));

        seed_mesh(&node).await;
        node.service.set_lease(300, false).await.unwrap();
        let own = mesh_service(&node, MACHINE);
        advance(&node, 30).await;
        own.renew_own_lease().await.unwrap();
        assert_eq!(renewed_at(&node, 1), 30);

        // One command moves the clock by a tenth of the lease at most.
        for _ in 0..10 {
            advance(&node, 30).await;
        }
        let err = own.renew_own_lease().await.unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_LEASE_EXPIRED));
        assert_eq!(renewed_at(&node, 1), 30);

        // A node the mesh does not have gets another answer: only that one
        // says the node is gone.
        let source = "127.0.0.1:40000".parse().unwrap();
        let err = node
            .service
            .peer_renew(renew_req(9, MACHINE), false, source)
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_NODE_REMOVED));

        drop(own);
        node.shutdown().await;
    }
}
