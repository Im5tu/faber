//! Faber node agent: Raft consensus over RocksDB storage, with a REST API and
//! an MCP endpoint.
//!
//! Two listeners, never bound anywhere but loopback or WireGuard:
//! - local (loopback): `/api/v1` (including the loopback-only
//!   `/api/v1/mesh/*`) and `/mcp`
//! - peer (WireGuard or loopback): `/api/v1` and `/raft`
//!
//! In mesh mode ([`Config::mesh_mode`]) the node also owns the host's
//! WireGuard interface, the `inet faber` nftables table, the containerd
//! drop-in and the CNI network config, all driven from replicated state (see
//! [`start_with`]). It holds a lease on its name and container subnet, which
//! it renews through raft ([`hold_lease`]); a node the mesh has removed
//! takes its container network config and its interface down, discards its
//! identity and stays idle, also across restarts.
#![allow(clippy::result_large_err)] // openraft's error types are large by design.

pub mod api;
pub mod apps;
pub mod bind;
pub mod config;
pub mod dns;
pub mod dto;
pub mod error;
pub mod guard;
pub mod lease;
pub mod mcp;
pub mod mesh;
pub mod peer;
pub mod placement;
pub mod raft;
pub mod reconcile;
pub mod runner;
pub mod secrets;
pub mod service;
pub mod status;

#[cfg(test)]
pub(crate) mod testing;

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::middleware::{from_fn, from_fn_with_state};
use faber_api_types::error_code;
use faber_container_runtime::ContainerRuntime;
use faber_network_management::{BoxFuture, NetworkError, NetworkManager};
use openraft::RaftMetrics;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

pub use config::Config;
pub use reconcile::HostBackends;
pub use service::{FaberService, ServiceError};

use crate::bind::SystemInterfaces;
use crate::guard::HostGuard;
use crate::mesh::identity::{self, Identity, IdentityError};
use crate::mesh::state::{DEFAULT_CONTAINER_SUBNET_PREFIX, FOUNDER_DEFAULT_NAME};
use crate::mesh::{
    BootstrapError, JoinPending, KeyError, MeshConfig, NodeRecord, parse_full_endpoint,
};
use crate::raft::network::HttpNetwork;
use crate::raft::rpc::{RaftGate, SourceCheck};
use crate::raft::store::StateReader;
use crate::raft::types::{Node, NodeId, Raft};
use crate::raft::{rpc, store};
use crate::reconcile::containers::ContainersStep;
use crate::reconcile::services::ServicesStep;
use crate::reconcile::step::{StepList, StepRunner, StepValue};
use crate::reconcile::{HostReconciler, NetworkStep, ReconcileTiming, RuntimeStep};
use crate::secrets::KeyDeliveryError;
use crate::secrets::delivery::{KeyStep, key_step};
use crate::service::{MeshLocal, now_secs};

/// How long shutdown waits for in-flight HTTP requests and for storage to close.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// Retry delays of the pending join, the endpoint update and the lease
/// renewal.
const RETRY_MIN: Duration = Duration::from_millis(200);
const RETRY_MAX: Duration = Duration::from_secs(10);
/// A node renews its lease this many times per lease period.
const RENEWALS_PER_LEASE: u64 = 4;
/// Longest wait for one lease renewal. A renewal that takes longer is tried
/// again.
const RENEW_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a follower waits to apply an endpoint change the leader has
/// committed before it proposes the change again.
const ENDPOINT_APPLY_WAIT: Duration = Duration::from_secs(5);
/// How long a joiner waits to see itself as a voter after the leader
/// accepted its join, before it asks again.
const VOTER_WAIT: Duration = Duration::from_secs(10);

type ServerTask = JoinHandle<std::io::Result<()>>;

/// Binds the peer listener once the host is set up. Gets the address the
/// node expects to listen on: `mesh_ip:peer_port` in mesh mode.
pub type DeferredBind =
    Box<dyn FnOnce(SocketAddr) -> BoxFuture<'static, anyhow::Result<TcpListener>> + Send>;

/// The peer listener handed to [`start_with`].
pub enum PeerListener {
    /// Already bound (dev mode and tests).
    Bound(TcpListener),
    /// Bound after WireGuard and nftables are up, so that in production the
    /// mesh IP exists and the peer-port rule is loaded before anything
    /// listens.
    Deferred(DeferredBind),
}

impl PeerListener {
    pub fn deferred<F>(bind: F) -> Self
    where
        F: FnOnce(SocketAddr) -> BoxFuture<'static, anyhow::Result<TcpListener>> + Send + 'static,
    {
        PeerListener::Deferred(Box::new(bind))
    }
}

/// A running node. Call [`NodeHandle::shutdown`] to stop it and close its
/// database.
pub struct NodeHandle {
    api_addr: SocketAddr,
    peer_addr: SocketAddr,
    service: FaberService,
    cancel: CancellationToken,
    servers: Vec<ServerTask>,
    /// The leader's lease task, then the task that finishes the join,
    /// replicates the endpoint, renews the lease and waits for the host
    /// reconcile loop.
    tasks: Vec<JoinHandle<()>>,
    state: StateReader,
    db: Weak<rocksdb::DB>,
    /// Set when this node takes itself out of the mesh.
    left_mesh: Arc<OnceLock<&'static str>>,
}

impl NodeHandle {
    /// The bound local (loopback) listener address.
    pub fn api_addr(&self) -> SocketAddr {
        self.api_addr
    }

    /// The bound peer listener address; also this node's Raft address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn service(&self) -> &FaberService {
        &self.service
    }

    pub fn raft(&self) -> &Raft {
        self.service.raft()
    }

    /// Read access to the local state machine.
    pub fn state(&self) -> &StateReader {
        &self.state
    }

    /// What made this node take itself out of the mesh, once it has; `None`
    /// while it is a member or still joining. One of:
    /// - `invite_rejected`: the mesh refused the invite during the join;
    /// - `key_unavailable`: the cluster never sent its cluster key;
    /// - `key_mismatch`: the cluster key the cluster sent is not the one
    ///   raft records;
    /// - `join_refused`: the mesh had removed the node before it finished
    ///   joining;
    /// - `renewal_refused`: the leader answered a lease renewal with
    ///   `mesh_node_removed`;
    /// - `left_membership`: the raft membership this node applied no longer
    ///   has it as a voter.
    ///
    /// The `bootstrap-error` marker holds only `invite_rejected` or
    /// `node_removed`; this says which evidence the node acted on.
    pub fn left_mesh_because(&self) -> Option<&'static str> {
        self.left_mesh.get().copied()
    }

    /// Completes when the raft core stops, with the reason: a fatal storage
    /// error, a panic inside raft, or a shutdown. A node whose core has
    /// stopped still answers HTTP but never makes progress again, so the
    /// process must end ([`run`] exits with an error and systemd restarts
    /// it). Does not borrow the handle.
    pub fn raft_stopped(&self) -> impl Future<Output = String> + Send + 'static {
        let mut metrics = self.service.raft().metrics();
        async move {
            loop {
                if let Some(reason) = service::raft_stopped_reason(&metrics) {
                    return reason;
                }
                // An error here is the closed channel, which the next
                // round reports.
                let _ = metrics.changed().await;
            }
        }
    }

    /// Stops the background tasks and both HTTP servers, then Raft, then
    /// closes RocksDB so the data directory can be reopened as soon as this
    /// returns. Every step runs even if an earlier one failed; all failures
    /// are reported together.
    ///
    /// The background tasks go first because they write through raft (the
    /// leader's lease task, the lease renewal, the endpoint update) and hold
    /// handles on it: raft cannot shut down, and the database cannot close,
    /// while one of them is still running.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let NodeHandle {
            service,
            cancel,
            servers,
            tasks,
            db,
            ..
        } = self;
        let mut errors: Vec<String> = Vec::new();
        // Held to the end, so that the database is closed here and not by
        // whichever raft task drops the last handle. See below.
        let db = db.upgrade();

        cancel.cancel();
        for mut task in tasks {
            match tokio::time::timeout(SHUTDOWN_GRACE, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if e.is_cancelled() => {}
                Ok(Err(e)) => errors.push(format!("background task failed: {e}")),
                Err(_) => {
                    tracing::warn!("background task did not stop in time; aborting");
                    task.abort();
                }
            }
        }
        for mut server in servers {
            match tokio::time::timeout(SHUTDOWN_GRACE, &mut server).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => errors.push(format!("HTTP server failed: {e}")),
                Ok(Err(e)) => errors.push(format!("server task panicked: {e}")),
                Err(_) => {
                    tracing::warn!("HTTP server did not stop in time; aborting");
                    server.abort();
                }
            }
        }

        if let Err(e) = service.raft().shutdown().await {
            errors.push(format!("raft shutdown failed: {e}"));
        }
        drop(service);

        // openraft's tasks drop the log store and state machine as they wind
        // down; wait until this handle is the only one left. Then drop it and
        // wait for that: the drop is what closes RocksDB and releases its
        // LOCK file. Watching a weak handle's count reach zero is not
        // enough, because the count is zero before the close has run, and a
        // caller that reopened the directory at once found it locked.
        if let Some(db) = db {
            let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
            while Arc::strong_count(&db) > 1 {
                if tokio::time::Instant::now() >= deadline {
                    errors.push("storage still open after shutdown".into());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if let Err(e) = tokio::task::spawn_blocking(move || drop(db)).await {
                errors.push(format!("closing storage failed: {e}"));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(errors.join("; ")))
        }
    }
}

/// Opens storage, starts Raft and serves both listeners, without touching
/// the host. Refuses listeners that are not loopback (API) or
/// loopback/WireGuard (peer).
pub async fn start(
    config: Config,
    api_listener: TcpListener,
    peer_listener: TcpListener,
) -> anyhow::Result<NodeHandle> {
    start_with(
        config,
        api_listener,
        PeerListener::Bound(peer_listener),
        None,
    )
    .await
}

/// Starts a node. In mesh mode ([`Config::mesh_mode`]), in this order:
/// 1. Creates `--state-dir` if it is missing. A node that has left the mesh
///    and has nothing to start as stops here ([`LeftMesh`]), before it
///    touches the host. With `backends`: creates `--wg-interface` unless
///    this node already created it. An interface someone else made stops
///    startup here, before anything else is written.
/// 2. If an earlier run's invite was rejected (the `join-failed` marker),
///    removes what is left of that identity and its raft data. Does the same
///    to an identity whose join is still pending when `--join-token-file`
///    now holds a different, usable invite and this node is in no raft
///    membership ([`replace_superseded_join`]). Then loads `identity.json`,
///    or creates it from `--join-token-file` or `--new-mesh`, and removes
///    the `bootstrap-error` marker (unless this start wrote it, about a
///    replacement token that cannot be used). Opens storage and reads the
///    replicated `mesh` (a fresh joiner has none and uses its token's peers).
/// 3. With `backends`: reconciles the network (WireGuard up, the container
///    range routed into it, `inet faber` loaded), then the container runtime
///    if its spec is replicated. A failure stops startup.
/// 4. Binds the peer listener (for [`PeerListener::Deferred`]), starts Raft
///    and serves. A joiner whose registration the mesh has not accepted yet
///    answers every raft RPC with 503 ([`RaftGate`]).
/// 5. With `backends`: spawns the host reconcile loop (network, then
///    container runtime), seeded with what step 3 applied.
/// 6. Spawns the leader's lease task ([`lease::spawn_leader_task_with`]),
///    which acts only while this node leads.
/// 7. Finishes a pending join in two steps, each retried: register the
///    invite to this machine, then join as a voter. `pending_join` is
///    cleared once this node sees itself as a voter. If the invite is
///    rejected, the host reconcile loop is stopped, the node's CNI network
///    config and the WireGuard interface are removed, the identity is
///    discarded, the `bootstrap-error` marker says `invite_rejected`, and
///    the node stays up but idle. The same happens, with `node_removed` in
///    the marker, when the mesh removed the node before it finished joining.
/// 8. As a member: replicates `--wg-endpoint` if it changed, and renews this
///    node's lease ([`hold_lease`]). When the mesh has removed the node, it
///    does what step 7 does for a rejected invite, with `node_removed` in
///    the marker.
///
/// Outside mesh mode only step 4 runs, and `backends` is not used. `None`
/// never runs commands or writes host files.
pub async fn start_with(
    config: Config,
    api_listener: TcpListener,
    peer: PeerListener,
    backends: Option<HostBackends>,
) -> anyhow::Result<NodeHandle> {
    config.validate()?;
    let api_addr = api_listener.local_addr()?;
    bind::check_api_addr(api_addr)?;
    if let PeerListener::Bound(l) = &peer {
        bind::check_peer_ip(l.local_addr()?.ip(), &SystemInterfaces)?;
    }

    let wg_endpoint = config.resolved_wg_endpoint()?;
    let identity = if config.mesh_mode() {
        ensure_state_dir(&config.state_dir)?;
        if let Some(reason) = left_for_good(&config)? {
            // What is left of the old identity goes, as at any other start.
            identity::discard_failed_join(&config.state_dir, &config.data_dir)?;
            secrets::remove_cluster_key_file(&config.cluster_key_path());
            return Err(anyhow::Error::new(LeftMesh { reason }));
        }
        if let Some(backends) = &backends {
            claim_interface(&backends.network, &config.state_dir, config.wg_iface()).await?;
        }
        if identity::discard_failed_join(&config.state_dir, &config.data_dir)? {
            secrets::remove_cluster_key_file(&config.cluster_key_path());
            tracing::warn!(
                "removed the identity and raft data of an earlier join whose invite was rejected"
            );
        }
        let superseded = replace_superseded_join(&config).await?;
        let mut identity = load_identity(&config)?;
        // A joiner registers with it, and every node renews its lease with
        // it.
        identity::ensure_instance_id(&config.state_dir, &mut identity)
            .context("giving this node an instance id")?;
        match superseded {
            // The marker says what is wrong with the token the operator
            // just supplied; it stays for install.sh to read.
            Superseded::Refused(reason) => note_bootstrap_error(&config.state_dir, reason),
            // Whatever stopped an earlier bootstrap no longer applies.
            Superseded::Replaced | Superseded::Kept => clear_bootstrap_error(&config.state_dir),
        }
        Some(identity)
    } else {
        None
    };
    let node_id = config.resolve_node_id(identity.as_ref())?;

    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating {}", config.data_dir.display()))?;
    let mut storage = store::open(&config.data_dir)
        .with_context(|| format!("opening storage in {}", config.data_dir.display()))?;
    let db = store::weak_db(&storage);
    let reader = storage.reader.clone();

    // A joiner takes no raft RPC before the mesh has accepted its
    // registration. One whose own log already has it as a member got that
    // log from the cluster, so it is the registered node.
    let unregistered = identity
        .as_ref()
        .and_then(|i| i.pending_join.as_ref())
        .is_some_and(|p| !p.registered);
    let raft_gate = RaftGate::new(
        !unregistered
            || store::knows_member(&mut storage, node_id)
                .await
                .context("reading the stored membership")?,
    );

    // Shared by everything that treats a joiner differently, and cleared
    // when this node sees itself as a voter.
    let joining = identity.as_ref().is_some_and(|i| i.pending_join.is_some());
    let join_pending = JoinPending::new(joining);
    let cluster_key: Arc<dyn secrets::ClusterKeyProvider> =
        Arc::new(secrets::FileClusterKey::new(config.cluster_key_path()));
    let host = backends
        .zip(identity.clone())
        .map(|(backends, identity)| HostReconciler {
            backends,
            iface: config.wg_iface().to_string(),
            identity,
            join_pending: join_pending.clone(),
            peer_port: config.peer_port,
            lease_timing: config.lease_timing,
            reader: reader.clone(),
            containers: reconcile::containers::ContainersWiring {
                socket: config.node_socket.clone(),
                state: Arc::default(),
                cluster_key: cluster_key.clone(),
                env: Arc::new(runner::StateEnvSource::new(
                    reader.clone(),
                    cluster_key.clone(),
                )),
            },
        });
    let started = match &host {
        Some(host) => Some(bring_up_host(host).await?),
        None => None,
    };

    let peer_listener = match peer {
        PeerListener::Bound(l) => l,
        PeerListener::Deferred(bind_peer) => {
            let intended = match &identity {
                Some(i) => SocketAddr::new(IpAddr::V4(i.mesh_ip), config.peer_port),
                None => SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), config.peer_port),
            };
            let l = bind_peer(intended).await?;
            bind::check_peer_ip(l.local_addr()?.ip(), &SystemInterfaces)?;
            l
        }
    };
    let peer_addr = peer_listener.local_addr()?;

    let raft = Raft::new(
        node_id,
        Arc::new(config.raft_config()?),
        HttpNetwork::new()?,
        storage.log_store,
        storage.state_machine,
    )
    .await
    .map_err(|e| anyhow::anyhow!("starting raft: {e}"))?;

    let mesh_local = match &identity {
        Some(i) => Some(MeshLocal {
            config: MeshConfig {
                cidr: i.mesh_cidr,
                peer_port: peer_addr.port(),
                wg_port: i.wg_port,
                // Only the founder's `InitCluster` reads this. The range is
                // fixed there, and raft hands out the subnets.
                container_cidr: Some(config.container_cidr),
                container_subnet_prefix: DEFAULT_CONTAINER_SUBNET_PREFIX,
                service_cidr: Some(config.service_cidr),
            },
            record: NodeRecord {
                node_id,
                public_key: i.public_key().context("deriving the public key")?,
                mesh_ip: i.mesh_ip,
                endpoint: Some(i.endpoint.to_string()),
                invite_id: None,
                // The founder's `InitCluster` records it, and every node
                // renews its lease as this machine.
                instance_id: i.instance_id.clone(),
                container_subnet: None,
                // Only the founder's `InitCluster` reads this. Every other
                // node gets its name from its invite.
                name: config
                    .node_name
                    .clone()
                    .unwrap_or_else(|| FOUNDER_DEFAULT_NAME.to_string()),
                lease_renewed_at: 0,
            },
            bootstrap_peers: i
                .pending_join
                .iter()
                .flat_map(|p| &p.bootstrap_peers)
                .map(|peer| peer.mesh_ip)
                .collect(),
            join_pending: join_pending.clone(),
        }),
        None => None,
    };
    let service = FaberService::new(
        node_id,
        peer_addr.to_string(),
        raft.clone(),
        reader.clone(),
        mesh_local,
    )?
    .with_cluster_keys(Arc::new(secrets::FileClusterKey::new(
        config.cluster_key_path(),
    )));
    service.set_lease_timing(config.lease_timing);
    // A key that is not the cluster's is said once at start, not at the
    // first deploy.
    service.check_cluster_key().await;
    let cancel = CancellationToken::new();

    // Loopback only: the peer listener also admits machines that hold just an
    // invite, and the tools can deploy and create invites.
    let mcp = mcp::service(
        service.clone(),
        config.allowed_mcp_hosts(),
        cancel.child_token(),
    );
    let mcp_app = Router::new()
        .nest_service("/mcp", mcp)
        .layer(RequestBodyLimitLayer::new(
            faber_api_types::workload::limits::MAX_HTTP_BODY_BYTES,
        ));
    let local_app = api::local_router(service.clone())
        .layer(from_fn_with_state(HostGuard::local(), guard::host_guard))
        .merge(mcp_app)
        .layer(from_fn(guard::origin_guard))
        .layer(TraceLayer::new_for_http());
    let raft_sources = {
        let service = service.clone();
        SourceCheck::new(move |source| service.raft_source_allowed(source))
    };
    let peer_app = api::router(service.clone())
        .merge(rpc::router(raft, raft_gate.clone(), raft_sources))
        .merge(peer::router(service.clone()))
        .layer(from_fn_with_state(
            HostGuard::peer(peer_addr.ip()),
            guard::host_guard,
        ))
        .layer(from_fn(guard::origin_guard))
        .layer(TraceLayer::new_for_http());

    let servers = vec![
        serve(api_listener, local_app, cancel.clone()),
        serve(peer_listener, peer_app, cancel.clone()),
    ];

    let mut tasks = Vec::new();
    // What the status reporter reports: the replicas the runner runs, or none
    // on a node that does not manage its host.
    let replicas: Arc<dyn status::ReplicaSource> = match &host {
        Some(host) => host.containers.state.clone(),
        None => Arc::new(status::NoReplicas),
    };
    let prober = runner::NetProber::new().context("building the probe client")?;
    // The host loop has a stop of its own: a rejected invite ends it while
    // the rest of the node stays up. `host` implies `identity`. The
    // supervisor stops with it.
    let host = host.zip(started).map(|(host, (runner, applied))| {
        let stop = cancel.child_token();
        tasks.push(
            runner::Supervisor::new(
                runner::SocketNodeClient::new(host.containers.socket.clone()),
                runner::MonotonicClock::new(),
                prober,
                host.containers.env.clone(),
                host.containers.state.clone(),
                runner::SupervisorConfig::default(),
            )
            .spawn(stop.clone()),
        );
        HostControl {
            network: host.backends.network.clone(),
            runtime: host.backends.runtime.clone(),
            reader: host.reader.clone(),
            iface: host.iface.clone(),
            stop: stop.clone(),
            task: host.spawn(
                ReconcileTiming::with_resync(Duration::from_secs(config.reconcile_interval_secs)),
                runner,
                applied,
                stop,
            ),
        }
    });
    tasks.push(service.spawn_key_id_healer(cancel.child_token()));
    let left_mesh = Arc::new(OnceLock::new());
    if let Some(identity) = identity {
        // Every mesh node runs the leader's side of the leases; it acts only
        // while the node leads.
        tasks.push(lease::spawn_leader_task_with(
            service.clone(),
            reader.clone(),
            config.lease_timing,
            cancel.child_token(),
        ));
        // Every mesh node runs the leader's side of placement; it acts only
        // while the node leads.
        tasks.push(placement::spawn_default(
            service.clone(),
            cancel.child_token(),
        ));
        tasks.push(dns::spawn_node(
            reader.clone(),
            node_id,
            cancel.child_token(),
        ));
        tasks.push(tokio::spawn(mesh_membership(
            MeshMembership {
                service: service.clone(),
                state_dir: config.state_dir.clone(),
                cluster_key_file: config.cluster_key_path(),
                node_socket: host.is_some().then(|| config.node_socket.clone()),
                wg_endpoint,
                reader: reader.clone(),
                replicas,
                gate: raft_gate,
                lease_second: config.lease_timing.second,
                left_mesh: left_mesh.clone(),
                join_pending,
                cancel: cancel.child_token(),
            },
            identity,
            host,
        )));
    }

    tracing::info!(node_id, %api_addr, %peer_addr, "fabercp node started");
    Ok(NodeHandle {
        api_addr,
        peer_addr,
        service,
        cancel,
        servers,
        tasks,
        state: reader,
        db,
        left_mesh,
    })
}

/// A join token that can never work: it has expired, or it does not hold a
/// valid identity. Startup with one is not a failure worth retrying, see
/// [`start_or_idle`].
#[derive(Debug, thiserror::Error)]
#[error("the join token cannot be used")]
struct UnusableToken {
    reason: BootstrapError,
    #[source]
    source: IdentityError,
}

/// Why an identity could not be made from a join token, when the cause is
/// the token itself. `None` for everything a restart or a changed flag can
/// fix (I/O, an `--wg-endpoint` that differs from the invite).
fn unusable_token(e: &IdentityError) -> Option<BootstrapError> {
    match e {
        IdentityError::TokenExpired { .. } => Some(BootstrapError::TokenExpired),
        IdentityError::Token(_)
        | IdentityError::InvalidEndpoint(_)
        | IdentityError::IpOutsideCidr { .. }
        | IdentityError::Key(KeyError::Base64 | KeyError::Length(_)) => {
            Some(BootstrapError::TokenInvalid)
        }
        _ => None,
    }
}

/// This node took itself out of the mesh in an earlier run, and nothing in
/// its configuration makes a new identity. Startup with that is not a
/// failure worth retrying, see [`start_or_idle`].
#[derive(Debug, thiserror::Error)]
#[error("this node has left the mesh ({}) and has no join token to join again with", reason.code())]
struct LeftMesh {
    reason: BootstrapError,
}

/// Whether this node left the mesh for good in an earlier run, and why. All
/// of these must hold:
/// - the `bootstrap-error` marker says `node_removed`, `invite_rejected` or
///   `key_mismatch`, which no start can put right: a wrong key file does not
///   fix itself. (`key_unavailable` is not among them: the key may be put on
///   a mesh node, so that start tries again.)
/// - neither `--join-token-file` nor `--new-mesh` is set, so nothing makes a
///   new identity. install.sh takes both out of the environment once the
///   node has created or joined its cluster, and leaves them in place after
///   a join that failed. So this is the state of a node that was a member
///   and was removed. A node whose join failed still has its token setting
///   and does not come through here: it makes the same identity again,
///   tries the invite once more, is refused, and ends idle that way
///   ([`leave_mesh`]), or its token has expired by then ([`UnusableToken`]);
/// - there is no identity, or the `join-failed` marker says it is to be
///   discarded. A marker next to a whole identity is a teardown that was
///   cut short: the node starts as itself and finds out again.
///
/// Without this check such a start would create the WireGuard interface and
/// then fail for want of an identity, every few seconds under systemd.
fn left_for_good(config: &Config) -> anyhow::Result<Option<BootstrapError>> {
    if config.join_token_file.is_some() || config.new_mesh {
        return Ok(None);
    }
    let state_dir = &config.state_dir;
    let marker = mesh::bootstrap::path(state_dir);
    let reason = mesh::bootstrap::read(state_dir)
        .with_context(|| format!("reading {}", marker.display()))?
        .filter(|r| {
            matches!(
                r,
                BootstrapError::NodeRemoved
                    | BootstrapError::InviteRejected
                    | BootstrapError::KeyMismatch
            )
        });
    let discarded =
        identity::join_failed_path(state_dir).exists() || !identity::path(state_dir).exists();
    Ok(reason.filter(|_| discarded))
}

/// Loads the identity, or creates it from the join token or `--new-mesh`.
/// The token file is not read when there is an identity: install.sh removes
/// it once the node is configured. ([`replace_superseded_join`] has by now
/// removed an identity that the token replaces.) A token that can never work
/// is an [`UnusableToken`] error.
fn load_identity(config: &Config) -> anyhow::Result<Identity> {
    if let Some(existing) = identity::load(&config.state_dir)? {
        return Ok(existing);
    }
    let token = match &config.join_token_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("reading join token {}", path.display()))?,
        ),
        None => None,
    };
    let from_token = token.is_some();
    let identity = identity::resolve(
        &config.state_dir,
        config.bootstrap_input(token)?,
        now_secs(),
    )
    .map_err(|e| match unusable_token(&e).filter(|_| from_token) {
        Some(reason) => anyhow::Error::new(UnusableToken { reason, source: e }),
        None => e.into(),
    })?;
    tracing::info!(
        node_id = identity.node_id,
        mesh_ip = %identity.mesh_ip,
        "created node identity"
    );
    Ok(identity)
}

/// Gives up a pending join that a new token has replaced, so that
/// [`load_identity`] bootstraps from that token. Returns whether it did.
///
/// A join can get stuck for good without the mesh ever saying so: once the
/// invite has been used by another node or has expired, the firewall on the
/// other nodes drops this node, and no `invite_*` answer arrives. The
/// operator's way out is a new invite. The old identity and raft data are
/// removed (as for a rejected invite) only if all of these hold:
/// - `identity.json` exists and still has `pending_join`;
/// - `--join-token-file` is set and readable, and its token is usable (it
///   decodes, has not expired, makes a valid identity) and is for another
///   invite than the pending one;
/// - this node is in no raft membership, as a voter or a learner
///   ([`store::knows_member`]). A member has raft state worth keeping and
///   never gives up its identity.
///
/// With the same token or none, the pending join carries on as before. With
/// a token that cannot be used it carries on too, and the caller leaves the
/// reason in the `bootstrap-error` marker: the operator supplied that token
/// to get out of a stuck join and needs to know why nothing changed. Must
/// run before storage is opened.
async fn replace_superseded_join(config: &Config) -> anyhow::Result<Superseded> {
    let Some(token_file) = &config.join_token_file else {
        return Ok(Superseded::Kept);
    };
    let Some(existing) = identity::load(&config.state_dir)? else {
        return Ok(Superseded::Kept);
    };
    if existing.pending_join.is_none() {
        return Ok(Superseded::Kept);
    }
    // install.sh removes the file once the node has joined; a missing or
    // unreadable file is no new token.
    let Ok(token) = std::fs::read_to_string(token_file) else {
        return Ok(Superseded::Kept);
    };
    match identity::supersedes_pending_join(
        &existing,
        &token,
        config.resolved_wg_endpoint()?,
        config.wg_port,
        now_secs(),
    ) {
        Ok(true) => {}
        Ok(false) => return Ok(Superseded::Kept),
        Err(e) => {
            let reason = refused_replacement(&e);
            tracing::warn!(
                token_file = %token_file.display(),
                error = %e,
                code = reason.code(),
                "the join token cannot replace the pending join; still trying the pending one"
            );
            return Ok(Superseded::Refused(reason));
        }
    }
    if stored_member(&config.data_dir, existing.node_id).await? {
        tracing::info!(
            node_id = existing.node_id,
            "ignoring the new join token: this node is already in the raft membership"
        );
        return Ok(Superseded::Kept);
    }
    identity::mark_join_failed(&config.state_dir)?;
    identity::discard_failed_join(&config.state_dir, &config.data_dir)?;
    secrets::remove_cluster_key_file(&config.cluster_key_path());
    tracing::warn!(
        node_id = existing.node_id,
        "the join token is for another invite than the pending join, which never completed: \
         removed the old identity and its raft data"
    );
    Ok(Superseded::Replaced)
}

/// What [`replace_superseded_join`] did about a pending join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Superseded {
    /// The old identity and its raft data are gone; the token file makes the
    /// new identity.
    Replaced,
    /// Nothing replaces the pending join, which carries on.
    Kept,
    /// The token file holds another invite that cannot be used. The pending
    /// join carries on, and the marker gets this code.
    Refused(BootstrapError),
}

/// The marker code for a replacement token that cannot be used. An expired
/// token is `token_expired`. Everything else is `token_invalid`, including a
/// token whose endpoint differs from `--wg-endpoint`: unlike at a first
/// start, the process stays up, so the marker is the only place to say it.
fn refused_replacement(e: &IdentityError) -> BootstrapError {
    unusable_token(e).unwrap_or(BootstrapError::TokenInvalid)
}

/// Whether the raft storage in `data_dir` has `node_id` in a membership. The
/// database is closed again before this returns.
async fn stored_member(data_dir: &Path, node_id: u64) -> anyhow::Result<bool> {
    if !data_dir.exists() {
        return Ok(false);
    }
    let mut storage = store::open(data_dir)
        .with_context(|| format!("opening storage in {}", data_dir.display()))?;
    let member = store::knows_member(&mut storage, node_id)
        .await
        .with_context(|| format!("reading the membership in {}", data_dir.display()))?;
    Ok(member)
}

/// The host reconcile steps, in the order they run: network, runtime
/// configuration, services, then containers. Later steps are added here.
pub(crate) fn host_steps(host: &HostReconciler) -> StepRunner {
    StepRunner::new(
        StepList::default()
            .with(NetworkStep::new(
                host.backends.network.clone(),
                host.iface.clone(),
                host.identity.clone(),
                host.join_pending.clone(),
                host.peer_port,
            ))
            .with(RuntimeStep::new(
                host.backends.runtime.clone(),
                host.identity.node_id,
            ))
            .with(
                ServicesStep::new(
                    host.backends.network.clone(),
                    host.identity.node_id,
                    host.lease_timing,
                )
                .with_live_replicas(host.containers.state.clone()),
            )
            .with(
                ContainersStep::new(
                    runner::SocketNodeClient::new(host.containers.socket.clone()),
                    host.containers.state.clone(),
                    host.identity.node_id,
                    host.containers.cluster_key.clone(),
                    host.containers.env.clone(),
                )
                .with_lease_timing(host.lease_timing),
            ),
    )
}

/// Startup host setup, in the contract order: `network.reconcile` (wg up,
/// then `nft -f -`), then `runtime.reconcile`. The same steps and the same
/// calls as the reconcile loop, except that the first failure stops startup.
/// Returns the steps and what they applied, to seed the loop. The interface
/// has been claimed ([`claim_interface`]) by now.
async fn bring_up_host(host: &HostReconciler) -> anyhow::Result<(StepRunner, Vec<StepValue>)> {
    let runner = host_steps(host);
    let view = host.view().context("reading replicated state")?;
    let plan = runner.plan(&view);
    for w in &plan.warnings {
        tracing::warn!("{w}");
    }
    // A step that startup may leave undone (the containers step, while
    // faber-node is not up yet) is seeded with a value that differs from its
    // plan, so that the loop's first pass applies it and only it.
    let failed = runner.bring_up(&plan.desired).await?;
    let mut seed = plan.desired;
    for index in failed {
        seed[index] = StepValue::new(());
    }
    Ok((runner, seed))
}

/// Creates `--state-dir` if it is missing (mode 0700 on unix). Under systemd
/// `StateDirectory=` has already made it.
fn ensure_state_dir(state_dir: &Path) -> anyhow::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(state_dir)
        .with_context(|| format!("creating {}", state_dir.display()))
}

/// Makes sure `iface` is an interface this node created, so that fabercp
/// never reconfigures somebody else's WireGuard link.
///
/// With the ownership record ([`mesh::iface`]) there is nothing to do: the
/// link is ours, and if it is missing `network.reconcile` creates it. Without
/// the record, an interface that already exists is an error and nothing else
/// is run against it. Otherwise the record is written first and the link
/// created second. A kill between the two leaves a record without a link,
/// which the next start handles like a reboot. The other order would leave a
/// link without a record, which every later start would refuse.
///
/// If the link cannot be created, the record is removed again.
async fn claim_interface(
    network: &NetworkManager,
    state_dir: &Path,
    iface: &str,
) -> anyhow::Result<()> {
    let record = mesh::iface::path(state_dir);
    if mesh::iface::is_owned(state_dir, iface)
        .with_context(|| format!("reading {}", record.display()))?
    {
        return Ok(());
    }
    let hint = || {
        format!(
            "cannot create the WireGuard interface {iface}; if another interface has that \
             name, remove it (`ip link del {iface}`) or pick another name with --wg-interface"
        )
    };
    if network.interface_exists(iface).await.with_context(hint)? {
        return Err(NetworkError::InterfaceExists(iface.to_string())).with_context(hint);
    }
    mesh::iface::record(state_dir, iface)
        .with_context(|| format!("writing {}", record.display()))?;
    if let Err(e) = network.create_interface(iface).await {
        if let Err(undo) = mesh::iface::forget(state_dir) {
            tracing::error!(
                record = %record.display(),
                error = %undo,
                "could not remove the ownership record of an interface that was not created; \
                 remove the file by hand"
            );
        }
        return Err(e).with_context(hint);
    }
    tracing::info!(interface = iface, "created the WireGuard interface");
    Ok(())
}

/// What [`mesh_membership`] works with.
struct MeshMembership {
    service: FaberService,
    state_dir: PathBuf,
    /// The cluster key file, removed with the identity.
    cluster_key_file: PathBuf,
    /// Where faber-node listens, which is asked for the free space of the
    /// volume root that this node reports as its disk capacity. `None` when
    /// this node runs no containers: it then reports no disk.
    node_socket: Option<PathBuf>,
    wg_endpoint: Option<SocketAddr>,
    reader: StateReader,
    /// What this node's runner runs, for the status reports.
    replicas: Arc<dyn status::ReplicaSource>,
    /// Opened once the mesh has accepted this node's registration, closed
    /// again when the invite is rejected or the mesh removes the node.
    gate: RaftGate,
    /// Real time that one second of the lease clock takes
    /// ([`lease::LeaseTiming::second`]).
    lease_second: Duration,
    /// [`NodeHandle::left_mesh_because`].
    left_mesh: Arc<OnceLock<&'static str>>,
    /// Cleared once this node sees itself as a voter.
    join_pending: JoinPending,
    cancel: CancellationToken,
}

/// The running host reconcile loop and what removes its work again.
struct HostControl {
    network: NetworkManager,
    runtime: Arc<dyn ContainerRuntime>,
    /// For the replicated runtime settings, see [`Self::teardown`].
    reader: StateReader,
    iface: String,
    /// Stops the loop and nothing else.
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl HostControl {
    /// Waits for the loop to end, which it does when `stop` or the node's
    /// own shutdown fires.
    async fn finished(self) {
        if let Err(e) = self.task.await
            && !e.is_cancelled()
        {
            tracing::error!(error = %e, "the host reconcile loop failed");
        }
    }

    /// Stops the loop, removes this node's CNI network config and the
    /// WireGuard interface, then forgets that this node created the
    /// interface. The loop has ended before either goes, so it cannot bring
    /// them back.
    ///
    /// For a rejected invite: the interface carries a key that belongs to
    /// another node, and traffic meant for that node could reach it. For a
    /// node the mesh has removed: the interface routes a container subnet
    /// that now belongs to nobody, or to the machine that took the name.
    ///
    /// The CNI config names that same subnet, and while the file is there
    /// the container runtime keeps giving new containers addresses from it.
    /// It goes first, best effort: a failure is logged and the interface is
    /// removed all the same.
    ///
    /// The `inet faber` nftables table stays. Its rules drop what the
    /// containers that are still running send beyond the container range.
    async fn teardown(self, state_dir: &Path) {
        let HostControl {
            network,
            runtime,
            reader,
            iface,
            stop,
            task,
        } = self;
        stop.cancel();
        if let Err(e) = task.await
            && !e.is_cancelled()
        {
            tracing::error!(error = %e, "the host reconcile loop failed");
        }
        remove_pod_network(runtime.as_ref(), &reader).await;
        if let Err(e) = network.teardown(&iface).await {
            tracing::error!(
                interface = %iface,
                error = %e,
                "could not remove the WireGuard interface this node no longer uses; \
                 remove it by hand (`ip link del`)"
            );
            return;
        }
        // The link is gone, so the record goes too: the next start creates
        // the interface again and checks first that nobody else has.
        match mesh::iface::forget(state_dir) {
            Ok(()) => tracing::warn!(
                interface = %iface,
            "removed the WireGuard interface this node no longer uses"
            ),
            Err(e) => tracing::error!(
                record = %mesh::iface::path(state_dir).display(),
                error = %e,
                "could not remove the interface ownership record"
            ),
        }
    }
}

/// Removes this node's CNI network config and nothing else of the container
/// runtime's. Needs no replicated settings, so `_reader` is not read: a
/// config left by an earlier life of this node goes too. Failures are logged.
async fn remove_pod_network(runtime: &dyn ContainerRuntime, _reader: &StateReader) {
    match runtime.remove_pod_network().await {
        Ok(true) => tracing::warn!("removed the CNI network config this node no longer uses"),
        Ok(false) => {}
        Err(e) => tracing::error!(
            error = %format!("{:#}", anyhow::Error::from(e)),
            "could not remove this node's CNI network config; remove it by hand, or new \
             containers get addresses from a subnet that is no longer this node's"
        ),
    }
}

/// Background work of a mesh node: finish a pending join, then, as a member,
/// make the replicated endpoint match `--wg-endpoint` and keep this node's
/// lease renewed ([`hold_lease`]). Ends when the host reconcile loop (`host`)
/// has ended, so shutdown waits for both.
///
/// A rejected invite ends the work early, and so does the mesh removing this
/// node ([`leave_mesh`]). Neither the endpoint nor the lease is touched
/// while the join is pending: the node is not in the mesh yet.
async fn mesh_membership(ctx: MeshMembership, mut identity: Identity, host: Option<HostControl>) {
    if identity.pending_join.is_some() {
        match finish_join(&ctx, &mut identity).await {
            JoinOutcome::Voter => {
                // From here on only replicated state says who is a peer.
                ctx.join_pending.clear();
                if let Err(e) = clear_pending_join(&ctx.state_dir, &mut identity).await {
                    tracing::error!(error = %e, "could not clear pending_join in identity.json");
                }
                // A marker about a replacement token this node did not need.
                clear_bootstrap_error(&ctx.state_dir);
            }
            JoinOutcome::Rejected(reason) => {
                let because = match reason {
                    BootstrapError::NodeRemoved => "join_refused",
                    BootstrapError::KeyMismatch => "key_mismatch",
                    BootstrapError::KeyUnavailable => "key_unavailable",
                    _ => "invite_rejected",
                };
                leave_mesh(ctx, host, reason, because).await;
                return;
            }
            JoinOutcome::Cancelled => {}
        }
    }
    if !ctx.cancel.is_cancelled() {
        // The endpoint update ends when the lease loop does.
        let stop = ctx.cancel.child_token();
        let endpoint = async {
            if let Some(endpoint) = ctx.wg_endpoint {
                sync_endpoint(&ctx.service, endpoint, &ctx.reader, &stop).await;
            }
        };
        let lease = async {
            let end = hold_lease(&ctx.service, &ctx.reader, ctx.lease_second, &ctx.cancel).await;
            stop.cancel();
            end
        };
        // Reports this node's status every interval, which an admin sets for
        // the cluster, in lease seconds.
        let status = async {
            let service = ctx.service.clone();
            let mut reporter = status::Reporter::new(
                move || service.report_identity(),
                ctx.service.clone(),
                ctx.replicas.clone(),
                status::system_probe(),
            );
            if let Some(socket) = &ctx.node_socket {
                reporter = reporter.with_disk(Arc::new(status::NodeDisk::new(
                    runner::SocketNodeClient::new(socket.clone()),
                )));
            }
            let interval = || {
                ctx.reader
                    .status_interval()
                    .unwrap_or(raft::store::status::DEFAULT_INTERVAL_SECS)
            };
            reporter.run(interval, ctx.lease_second, &stop).await;
        };
        if let ((), LeaseEnd::Removed(evidence), ()) = tokio::join!(endpoint, lease, status) {
            tracing::error!(
                evidence = evidence.as_str(),
                "the mesh has removed this node: its lease ran out, or another machine took \
                 over its name. Its identity is being discarded and its WireGuard interface \
                 removed. To join again, create a new invite on a mesh node (`faber mesh \
                 invite`) and run `install.sh --join <token>` here. Until then fabercp stays \
                 up but idle."
            );
            leave_mesh(ctx, host, BootstrapError::NodeRemoved, evidence.code()).await;
            return;
        }
    }
    if let Some(host) = host {
        host.finished().await;
    }
}

/// Takes this node out of the mesh on its own side, after a rejected invite
/// (`invite_rejected`) or after the mesh removed it (`node_removed`). In this
/// order: it takes no more raft RPCs, the host loop stops, the CNI network
/// config and the WireGuard interface are removed, and the identity is
/// discarded with `reason` in the `bootstrap-error` marker. `because` is
/// what [`NodeHandle::left_mesh_because`] reports.
///
/// The process stays up, idle. Exiting would make systemd start it again
/// every few seconds. A later start finds the marker and no identity, and
/// stays idle as well ([`left_for_good`]).
async fn leave_mesh(
    ctx: MeshMembership,
    host: Option<HostControl>,
    reason: BootstrapError,
    because: &'static str,
) {
    // Only ever set here, once.
    let _ = ctx.left_mesh.set(because);
    ctx.gate.close();
    if let Some(host) = host {
        host.teardown(&ctx.state_dir).await;
    }
    discard_identity(ctx.state_dir, ctx.cluster_key_file, reason).await;
}

/// Why [`hold_lease`] stopped renewing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseEnd {
    /// The mesh has removed this node, and this is how the node knows.
    Removed(RemovalEvidence),
    /// Shutdown, or the storage closed.
    Stopped,
}

/// What told a node that the mesh has removed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemovalEvidence {
    /// The leader answered a renewal with `mesh_node_removed`.
    RenewalRefused,
    /// The raft membership this node has applied no longer has it as a
    /// voter.
    LeftMembership,
}

impl RemovalEvidence {
    /// [`NodeHandle::left_mesh_because`].
    fn code(self) -> &'static str {
        match self {
            RemovalEvidence::RenewalRefused => "renewal_refused",
            RemovalEvidence::LeftMembership => "left_membership",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            RemovalEvidence::RenewalRefused => "the leader says the mesh no longer has this node",
            RemovalEvidence::LeftMembership => {
                "the raft membership no longer has this node as a voter"
            }
        }
    }
}

/// Whether the raft membership in `m` says that node `id`, which has been a
/// voter, is being removed. The membership must have members and be applied
/// here, and one of these must hold:
/// - `id` is not in it at all, neither as a voter nor as a learner;
/// - it is a joint config, and `id` is a voter of an older voter set but not
///   of the newest. Raft removes a voter in two steps, and this is the
///   first. From here raft only moves on to the newest set, and no command
///   puts an existing node back.
///
/// The second case is the one a removed follower sees. The leader stops
/// replicating to a node as soon as the membership without it is in the
/// leader's log, so the joint config is the last membership the node ever
/// gets. A leader that removed itself (another machine took over its name)
/// sees the first case.
///
/// Read from raft's metrics, with two guards:
/// - An empty membership is no evidence. Metrics start out empty and are
///   filled from storage a moment after raft starts, and a founder has none
///   until `cluster init`.
/// - The membership in the metrics is the newest one in the log, committed
///   or not. One that is not committed can still be dropped by the next
///   leader, so it counts only once `last_applied` has reached it.
///
/// A learner in a config that is not joint is no evidence either: nothing
/// here turns a voter into a learner.
fn left_membership(m: &RaftMetrics<NodeId, Node>, id: NodeId) -> bool {
    let membership = m.membership_config.membership();
    if membership.nodes().next().is_none() {
        return false;
    }
    let voter_sets = membership.get_joint_config();
    let leaving = voter_sets.iter().any(|voters| voters.contains(&id))
        && voter_sets
            .last()
            .is_some_and(|newest| !newest.contains(&id));
    if membership.get_node(&id).is_some() && !leaving {
        return false;
    }
    match (m.membership_config.log_id(), &m.last_applied) {
        (Some(changed), Some(applied)) => changed.index <= applied.index,
        _ => false,
    }
}

/// Real time between two renewals of a lease of `lease_secs`: a quarter of
/// it, where `second` is the real time of one second of the lease clock.
fn renew_every(lease_secs: u64, second: Duration) -> Duration {
    lease_period(lease_secs / RENEWALS_PER_LEASE, second)
}

/// Real time of `secs` seconds of the lease clock.
fn lease_period(secs: u64, second: Duration) -> Duration {
    second.saturating_mul(u32::try_from(secs).unwrap_or(u32::MAX))
}

/// Tells when to warn that the lease has gone unrenewed: once half a lease
/// has passed without a renewal, which is two renewals missed and still
/// time to act, and then once per whole lease.
#[derive(Debug)]
struct Unrenewed {
    /// The last renewal, or the last warning if that is later.
    since: tokio::time::Instant,
    /// Whether a warning was given since the last renewal.
    warned: bool,
}

impl Unrenewed {
    fn new(now: tokio::time::Instant) -> Self {
        Self {
            since: now,
            warned: false,
        }
    }

    fn renewed(&mut self, now: tokio::time::Instant) {
        *self = Self::new(now);
    }

    /// Whether it is time to warn: half of `lease` has passed since the
    /// last renewal, or a whole `lease` since the last warning. Counts this
    /// call as the warning.
    fn warn_due(&mut self, now: tokio::time::Instant, lease: Duration) -> bool {
        let wait = if self.warned { lease } else { lease / 2 };
        let due = now.saturating_duration_since(self.since) >= wait;
        if due {
            self.since = now;
            self.warned = true;
        }
        due
    }
}

/// The code of a refused renewal that no retry can change, if `e` is one:
/// `forbidden` (the record is registered to another machine, or the request
/// did not come from this node's mesh address) or `bad_request` (this node
/// has no usable instance id). From this node as the leader or passed
/// through from it.
fn lasting_refusal(e: &ServiceError) -> Option<&'static str> {
    let code = match e {
        ServiceError::Forbidden(_) => error_code::FORBIDDEN,
        ServiceError::BadRequest(_) => error_code::BAD_REQUEST,
        ServiceError::Rejected { code, .. } => code.as_str(),
        ServiceError::Upstream { body, .. } => body.error.as_str(),
        _ => return None,
    };
    [error_code::FORBIDDEN, error_code::BAD_REQUEST]
        .into_iter()
        .find(|lasting| *lasting == code)
}

/// Keeps this node's lease renewed until the mesh has removed the node or
/// `cancel` fires. Waits for the mesh to replicate first.
///
/// Renews at once, then every quarter of the lease, measured on the
/// monotonic clock. The lease duration is read from the replicated state
/// every time that changes: when an admin changes it (`SetLease`), the node
/// renews at once and follows the new duration from then on.
///
/// A renewal that fails is tried again with a growing delay, or at once when
/// raft reports a new leader, whatever the reason (no leader, a peer that
/// cannot be reached, a timeout, `not_ready`), and nothing is torn down.
/// Only two things count as evidence that the mesh has removed this node:
/// - the leader answers a renewal with `mesh_node_removed`: the mesh has no
///   record of the node, and a record goes only after its node left raft;
/// - the raft membership this node has applied no longer has it, or has it
///   on the way out ([`left_membership`]).
///
/// `mesh_lease_expired` is not evidence. The lease has run out, but the
/// node is still in the mesh and may still be a voter that the others need
/// for their quorum: the leader does not remove an expired voter while that
/// would stall raft ([`lease::removal_allowed`]). A node that tore itself
/// down on this answer would take away the very vote the leader kept it
/// for. So it warns once, keeps its raft gate open and its interface up,
/// and waits for one of the two things above. It asks again once per
/// renewal period, without a growing delay: the answer becomes
/// `mesh_node_removed` once the record is gone, and a renewal succeeds
/// again if an admin lengthens the lease far enough.
///
/// Silence is not evidence either. A node that cannot renew may have been
/// removed, or the network may be down for every node at once, and wiping
/// on silence would then destroy the cluster. It warns once half a lease
/// has passed without a renewal, then once per lease period, and keeps
/// trying. A refusal that no retry can change (`forbidden`, `bad_request`)
/// is warned about at once.
async fn hold_lease(
    service: &FaberService,
    reader: &StateReader,
    second: Duration,
    cancel: &CancellationToken,
) -> LeaseEnd {
    use tokio::time::Instant;

    let id = service.node_id();
    let read_lease = || reader.mesh().map(|mesh| mesh.map(|m| m.lease_secs));
    let mut changes = reader.subscribe();
    let mut metrics = service.raft().metrics();
    // `None` until the mesh has replicated to this node.
    let Ok(mut lease_secs) = read_lease() else {
        return LeaseEnd::Stopped;
    };
    // When the lease was last renewed, and the duration it had then.
    let mut renewed: Option<(Instant, u64)> = None;
    let mut delay = RETRY_MIN;
    let mut not_before = Instant::now();
    let mut unrenewed = Unrenewed::new(Instant::now());
    // The refusal last warned about, and whether the leader has said that
    // the lease ran out. Both start again with a renewal.
    let mut refused: Option<&'static str> = None;
    let mut expired = false;
    let mut leader = None;
    loop {
        let now = Instant::now();
        {
            let m = metrics.borrow_and_update();
            if left_membership(&m, id) {
                return LeaseEnd::Removed(RemovalEvidence::LeftMembership);
            }
            // A new leader may take what the last attempt could not deliver:
            // do not sit out the rest of the retry delay.
            if m.current_leader != leader {
                leader = m.current_leader;
                if leader.is_some() {
                    delay = RETRY_MIN;
                    not_before = now;
                }
            }
        }
        let due = lease_secs.map(|secs| match renewed {
            Some((at, held)) if held == secs => at + renew_every(secs, second),
            // Never renewed, or the duration changed: renew now.
            _ => now,
        });
        let due = due.map(|due| due.max(not_before));
        if let (Some(secs), Some(due)) = (lease_secs, due)
            && due <= now
        {
            let attempt = tokio::select! {
                _ = cancel.cancelled() => return LeaseEnd::Stopped,
                r = tokio::time::timeout(RENEW_TIMEOUT, service.renew_own_lease()) => r,
            };
            let now = Instant::now();
            match attempt {
                Ok(Ok(())) => {
                    tracing::debug!(lease_secs = secs, "renewed this node's lease");
                    renewed = Some((now, secs));
                    delay = RETRY_MIN;
                    not_before = now;
                    unrenewed.renewed(now);
                    refused = None;
                    expired = false;
                }
                Ok(Err(e)) if e.mesh_code() == Some(error_code::MESH_NODE_REMOVED) => {
                    return LeaseEnd::Removed(RemovalEvidence::RenewalRefused);
                }
                Ok(Err(e)) if e.mesh_code() == Some(error_code::MESH_LEASE_EXPIRED) => {
                    if !expired {
                        expired = true;
                        tracing::warn!(
                            lease_secs = secs,
                            "this node's lease has run out, and the leader accepts no more \
                             renewals for it. The mesh has not removed the node: it stays in \
                             raft with its interface up, because the others may need its vote, \
                             until it sees that it was removed. To bring it back sooner, get a \
                             new invite for its name (`faber mesh invite`) and run `install.sh \
                             --join <token> --reset` here."
                        );
                    }
                    // Nothing to retry in a hurry: ask again in a renewal
                    // period, for the answer that says the node is gone.
                    renewed = None;
                    delay = RETRY_MIN;
                    not_before = now + renew_every(secs, second);
                }
                failed => {
                    let (why, lasting) = match &failed {
                        Ok(Err(e)) => (e.to_string(), lasting_refusal(e)),
                        _ => (format!("no answer within {RENEW_TIMEOUT:?}"), None),
                    };
                    tracing::debug!(error = %why, "could not renew this node's lease; retrying");
                    if let Some(code) = lasting.filter(|code| refused != Some(*code)) {
                        refused = Some(code);
                        tracing::warn!(
                            error = %why,
                            code,
                            lease_secs = secs,
                            "the mesh refuses to renew this node's lease, and trying again \
                             will not change that. Unless this is put right the lease runs \
                             out and the mesh removes this node. `forbidden` means that the \
                             node's record belongs to another machine or that the request did \
                             not come from this node's mesh address."
                        );
                    }
                    if unrenewed.warn_due(now, lease_period(secs, second)) {
                        tracing::warn!(
                            error = %why,
                            lease_secs = secs,
                            "this node has not renewed its lease for half a lease period or \
                             longer. Once a whole period has passed, the rest of the mesh, if \
                             it is running, removes this node; to join again then, get a new \
                             invite (`faber mesh invite`) and run `install.sh --join <token> \
                             --reset` here. Nothing is torn down meanwhile, because a network \
                             outage looks the same from here."
                        );
                    }
                    not_before = now + delay;
                    delay = (delay * 2).min(RETRY_MAX);
                }
            }
            // Whatever changed meanwhile.
            match read_lease() {
                Ok(secs) => lease_secs = secs,
                Err(_) => return LeaseEnd::Stopped,
            }
            continue;
        }
        tokio::select! {
            _ = cancel.cancelled() => return LeaseEnd::Stopped,
            changed = changes.changed() => {
                if changed.is_err() {
                    return LeaseEnd::Stopped; // storage closed
                }
                match read_lease() {
                    Ok(secs) => lease_secs = secs,
                    Err(_) => return LeaseEnd::Stopped,
                }
            }
            changed = metrics.changed() => {
                if changed.is_err() {
                    return LeaseEnd::Stopped; // raft stopped
                }
            }
            // Without a mesh there is nothing to renew: wait for a change.
            _ = tokio::time::sleep_until(due.unwrap_or(now + RETRY_MAX)) => {}
        }
    }
}

/// How a pending join ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinOutcome {
    /// This node sees itself as a voter.
    Voter,
    /// Retrying cannot help. The mesh refused the invite (`invite_invalid`,
    /// `invite_used`, `invite_expired`): `InviteRejected`. Or it had
    /// removed the node before the join finished (`mesh_node_removed`):
    /// `NodeRemoved`.
    Rejected(BootstrapError),
    /// Shutdown came first.
    Cancelled,
}

/// Whether `e` is the mesh refusing the invite itself, as opposed to an
/// error worth retrying (`not_ready`, `no_leader`, an unreachable peer...).
fn is_invite_rejection(e: &ServiceError) -> bool {
    e.mesh_code().is_some_and(|c| c.starts_with("invite_"))
}

/// Whether `e` is the leader saying that the mesh has no record of this
/// node and no invite for it (`mesh_node_removed`). To a joiner that has
/// registered this means that it was removed again before it became a
/// voter: its machine was off for longer than the lease, or another machine
/// took its name. Nothing renews a lease while a join is pending.
fn is_removal(e: &ServiceError) -> bool {
    e.mesh_code() == Some(error_code::MESH_NODE_REMOVED)
}

/// Finishes a pending join against each join address in turn, until this
/// node sees itself as a voter, the invite is rejected or `cancel` fires.
/// Two steps, both safe to repeat:
///
/// 1. `POST /raft/cluster/register` gives the invite to this machine
///    (`instance_id`). Once the mesh has accepted it, `registered` is saved
///    in `identity.json` and the raft gate opens. A restart after that goes
///    straight to step 2. A restart before the save registers again, which
///    the mesh accepts from the same machine.
/// 2. `POST /raft/cluster/join` adds the node as a voter.
///
/// An HTTP 200 from the join is not the end: the outcome is `Voter` only
/// when this node's own raft has the membership that says so.
///
/// Every error other than a rejected invite and `mesh_node_removed` is
/// retried with a growing delay.
async fn finish_join(ctx: &MeshMembership, identity: &mut Identity) -> JoinOutcome {
    let Some(pending) = identity.pending_join.clone() else {
        return JoinOutcome::Voter;
    };
    let (service, cancel) = (&ctx.service, &ctx.cancel);
    let instance_id = identity.instance_id.clone().unwrap_or_default();
    let mut registered = pending.registered;
    let mut keyless = 0u32;
    // A node that is already in the membership has raft state worth keeping,
    // so its join is not given up on for an `invite_*` answer. When the
    // mesh has removed the node, whatever membership its own log still
    // shows is out of date: its record went only after it had left raft.
    let rejected = |step: &str, via: &str, e: &ServiceError| {
        let reason = if is_removal(e) {
            Some(BootstrapError::NodeRemoved)
        } else if is_invite_rejection(e) && !service.is_member_self() {
            Some(BootstrapError::InviteRejected)
        } else {
            None
        };
        match reason {
            Some(BootstrapError::NodeRemoved) => tracing::error!(
                %via,
                step,
                error = %e,
                "the mesh removed this node before it finished joining: its lease ran out, \
                 or another machine took over its name. Its identity is being discarded and \
                 its WireGuard interface removed. To join again, create a new invite on a \
                 mesh node (`faber mesh invite`) and run `install.sh --join <token>` here. \
                 Until then fabercp stays up but idle."
            ),
            Some(_) => tracing::error!(
                %via,
                step,
                error = %e,
                "the mesh rejected this node's invite, so it has not joined. \
                 Its identity is being discarded and its WireGuard interface removed. To \
                 recover, create a new invite on a mesh node (`faber mesh invite`) and run \
                 `install.sh --join <token>` here, or put the new token in \
                 FABER_JOIN_TOKEN_FILE and restart fabercp. Until then fabercp stays up but idle."
            ),
            None => tracing::warn!(%via, step, error = %e, "join attempt failed; retrying"),
        }
        reason
    };
    let mut delay = RETRY_MIN;
    loop {
        if service.is_voter_self() {
            tracing::info!("joined the mesh as a voter");
            return JoinOutcome::Voter;
        }
        for addr in &pending.join_addrs {
            if !registered {
                let attempt = tokio::select! {
                    _ = cancel.cancelled() => return JoinOutcome::Cancelled,
                    r = service.register_invited(
                        addr, &pending.invite_id, identity.endpoint, &instance_id,
                    ) => r,
                };
                match attempt {
                    Ok(delivered) => {
                        // A cluster that checks sources must send the key. A
                        // registration without one is not recorded: it is
                        // repeated, and given up on after a few tries.
                        match key_step(service.checks_peer_source(), &delivered, &mut keyless) {
                            KeyStep::Proceed => {}
                            KeyStep::Retry => {
                                tracing::warn!(
                                    via = %addr,
                                    "the cluster sent no cluster key; registering again"
                                );
                                continue;
                            }
                            KeyStep::GiveUp => {
                                tracing::error!(
                                    via = %addr,
                                    "the cluster never sent its cluster key, so this node will \
                                     not join. Its identity is being discarded and its WireGuard \
                                     interface removed. Put the cluster key file on a mesh node, \
                                     create a new invite (`faber mesh invite`) and run \
                                     `install.sh --join <token>` here."
                                );
                                return JoinOutcome::Rejected(BootstrapError::KeyUnavailable);
                            }
                        }
                        // The key is on disk before this node can become a
                        // voter, and before the registration is recorded.
                        match service.accept_cluster_key(&delivered).await {
                            Ok(()) => {}
                            Err(e @ KeyDeliveryError::Mismatch { .. }) => {
                                tracing::error!(
                                    via = %addr,
                                    error = %e,
                                    "the cluster key this node was sent is not the cluster's, so \
                                     it will not join. Its identity is being discarded and its \
                                     WireGuard interface removed. Create a new invite on a mesh \
                                     node (`faber mesh invite`) once the cluster key is sound, \
                                     and run `install.sh --join <token>` here."
                                );
                                return JoinOutcome::Rejected(BootstrapError::KeyMismatch);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    via = %addr,
                                    error = %e,
                                    "could not store the cluster key; retrying"
                                );
                                continue;
                            }
                        }
                        if let Err(e) = mark_registered(&ctx.state_dir, identity).await {
                            tracing::error!(
                                error = %e,
                                "could not record the registration in identity.json; retrying"
                            );
                            continue;
                        }
                        ctx.gate.open();
                        registered = true;
                        tracing::info!(via = %addr, "registered with the mesh");
                    }
                    Err(e) => match rejected("register", addr, &e) {
                        Some(reason) => return JoinOutcome::Rejected(reason),
                        None => continue,
                    },
                }
            }
            let attempt = tokio::select! {
                _ = cancel.cancelled() => return JoinOutcome::Cancelled,
                r = service.join_invited(addr, &pending.invite_id, &instance_id) => r,
            };
            match attempt {
                Ok(()) => {
                    let seen = tokio::select! {
                        _ = cancel.cancelled() => return JoinOutcome::Cancelled,
                        seen = service.wait_voter_self(VOTER_WAIT) => seen,
                    };
                    if seen {
                        tracing::info!(via = %addr, "joined the mesh as a voter");
                        return JoinOutcome::Voter;
                    }
                    tracing::warn!(
                        via = %addr,
                        "the join was accepted but this node does not see itself as a voter yet; \
                         retrying"
                    );
                }
                Err(e) => {
                    if let Some(reason) = rejected("join", addr, &e) {
                        return JoinOutcome::Rejected(reason);
                    }
                }
            }
            if cancel.is_cancelled() {
                return JoinOutcome::Cancelled;
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return JoinOutcome::Cancelled,
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(RETRY_MAX);
    }
}

/// After a rejected invite or a removal from the mesh: writes the
/// `bootstrap-error` marker with `reason`, then removes `identity.json` and
/// leaves the `join-failed` marker, so the node is unconfigured again. The
/// reason is on disk before the identity goes, so whoever sees a node without
/// an identity can read why. The raft data directory is cleared at the next
/// start ([`identity::discard_failed_join`]), because the database is open
/// until this process stops. The cluster key file is removed too.
async fn discard_identity(state_dir: PathBuf, key_file: PathBuf, reason: BootstrapError) {
    let file = identity::path(&state_dir);
    let result = tokio::task::spawn_blocking(move || {
        note_bootstrap_error(&state_dir, reason);
        let discarded = identity::mark_join_failed(&state_dir);
        // The cluster key goes with the identity, so it does not linger on a
        // node that is no longer in the cluster.
        secrets::remove_cluster_key_file(&key_file);
        discarded
    })
    .await;
    match result {
        Ok(Ok(())) => tracing::warn!(
            identity = %file.display(),
            code = reason.code(),
            "discarded this node's identity; its raft data is removed at the next start"
        ),
        Ok(Err(e)) => tracing::error!(
            identity = %file.display(),
            code = reason.code(),
            error = %e,
            "could not discard this node's identity; remove the file by hand"
        ),
        Err(e) => tracing::error!(error = %e, "discarding the identity panicked"),
    }
}

/// Writes the `bootstrap-error` marker. A failure is logged: the marker only
/// explains what the log already says.
fn note_bootstrap_error(state_dir: &Path, reason: BootstrapError) {
    if let Err(e) = mesh::bootstrap::write(state_dir, reason) {
        tracing::error!(
            marker = %mesh::bootstrap::path(state_dir).display(),
            code = reason.code(),
            error = %e,
            "could not write the bootstrap-error marker"
        );
    }
}

/// Removes the `bootstrap-error` marker. A failure is logged.
fn clear_bootstrap_error(state_dir: &Path) {
    if let Err(e) = mesh::bootstrap::clear(state_dir) {
        tracing::warn!(
            marker = %mesh::bootstrap::path(state_dir).display(),
            error = %e,
            "could not remove the bootstrap-error marker"
        );
    }
}

/// Saves a changed copy of `identity` off the async threads, and updates
/// `identity` only when the save worked.
async fn save_identity_change(
    state_dir: &Path,
    identity: &mut Identity,
    change: fn(&Path, &mut Identity) -> Result<(), IdentityError>,
) -> anyhow::Result<()> {
    let dir = state_dir.to_path_buf();
    let mut copy = identity.clone();
    let copy =
        tokio::task::spawn_blocking(move || change(&dir, &mut copy).map(|()| copy)).await??;
    *identity = copy;
    Ok(())
}

async fn clear_pending_join(state_dir: &Path, identity: &mut Identity) -> anyhow::Result<()> {
    save_identity_change(state_dir, identity, identity::clear_pending_join).await
}

async fn mark_registered(state_dir: &Path, identity: &mut Identity) -> anyhow::Result<()> {
    save_identity_change(state_dir, identity, identity::mark_registered).await
}

/// This node's endpoint in the replicated mesh as this node has applied it:
/// `None` while the node is not in the mesh, `Some(None)` for a node without
/// an endpoint. `Err` once the storage is closed.
fn replicated_endpoint(
    reader: &StateReader,
    id: u64,
) -> Result<Option<Option<String>>, store::ReadError> {
    Ok(reader
        .mesh()?
        .and_then(|mesh| mesh.nodes.get(&id).map(|n| n.endpoint.clone())))
}

/// Compared as addresses, so spelling does not cause a write.
fn is_endpoint(replicated: &Option<Option<String>>, endpoint: SocketAddr) -> bool {
    matches!(replicated, Some(Some(ep)) if parse_full_endpoint(ep).ok() == Some(endpoint))
}

/// Proposes `SetEndpoint` until this node's replicated endpoint is
/// `endpoint`. Waits for the mesh to replicate first.
///
/// A successful proposal is committed on the leader, but a follower applies
/// it a moment later. So after one it waits for the change to arrive here
/// ([`wait_endpoint_applied`]) rather than reading the old value again and
/// proposing the same change a second time.
async fn sync_endpoint(
    service: &FaberService,
    endpoint: SocketAddr,
    reader: &StateReader,
    cancel: &CancellationToken,
) {
    let id = service.node_id();
    let mut changes = reader.subscribe();
    let mut delay = RETRY_MIN;
    loop {
        let Ok(current) = replicated_endpoint(reader, id) else {
            return; // storage closed
        };
        if is_endpoint(&current, endpoint) {
            return;
        }
        let wait = match current {
            Some(_) => match tokio::select! {
                _ = cancel.cancelled() => return,
                r = service.set_endpoint(endpoint) => r,
            } {
                Ok(()) => {
                    tracing::info!(%endpoint, "replicated the new WireGuard endpoint");
                    delay = RETRY_MIN;
                    if !wait_endpoint_applied(reader, &mut changes, id, endpoint, cancel).await {
                        return;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(%endpoint, error = %e, "could not update the endpoint; retrying");
                    let d = delay;
                    delay = (delay * 2).min(RETRY_MAX);
                    d
                }
            },
            // Not in the replicated mesh yet.
            None => RETRY_MAX,
        };
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = changes.changed() => {}
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// Waits until this node has applied `endpoint` as its own, following the
/// change feed, for at most [`ENDPOINT_APPLY_WAIT`]. `false` when `cancel`
/// fired or the storage closed, and the caller should stop. `true`
/// otherwise, including after the wait ran out: the caller then looks again
/// and proposes once more if the endpoint is still not there.
async fn wait_endpoint_applied(
    reader: &StateReader,
    changes: &mut tokio::sync::watch::Receiver<u64>,
    id: u64,
    endpoint: SocketAddr,
    cancel: &CancellationToken,
) -> bool {
    let deadline = tokio::time::Instant::now() + ENDPOINT_APPLY_WAIT;
    loop {
        match replicated_endpoint(reader, id) {
            Ok(current) if is_endpoint(&current, endpoint) => return true,
            Ok(_) => {}
            Err(_) => return false,
        }
        tokio::select! {
            _ = cancel.cancelled() => return false,
            changed = changes.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            _ = tokio::time::sleep_until(deadline) => return true,
        }
    }
}

/// Serves `app` with the address of the client attached to every request
/// (`ConnectInfo<SocketAddr>`), which the peer routes check.
fn serve(listener: TcpListener, app: Router, cancel: CancellationToken) -> ServerTask {
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
    })
}

/// [`start_with`], except that a join token that can never work (expired,
/// or not a valid token) is not an error: the node is not started and the
/// result is `None`.
///
/// Under systemd (`Restart=always`) an error exit is retried every few
/// seconds for ever, and such a token never becomes valid. This happens to a
/// node that was installed too late, and to one restarted after its invite
/// was rejected with the old token still in place. The caller stays up,
/// doing nothing, until the operator supplies a new token and restarts.
/// Nothing listens meanwhile, so the reason is left in the `bootstrap-error`
/// marker (`token_expired` or `token_invalid`) for install.sh to read.
///
/// The same goes for a node that took itself out of the mesh in an earlier
/// run and has no token to join again with ([`LeftMesh`]): it stays idle,
/// without creating its WireGuard interface, and the marker it wrote then
/// (`node_removed` or `invite_rejected`) stays as it is.
///
/// Every other startup failure is still an error.
pub async fn start_or_idle(
    config: Config,
    api_listener: TcpListener,
    peer: PeerListener,
    backends: Option<HostBackends>,
) -> anyhow::Result<Option<NodeHandle>> {
    let state_dir = config.state_dir.clone();
    let e = match start_with(config, api_listener, peer, backends).await {
        Ok(node) => return Ok(Some(node)),
        Err(e) => e,
    };
    if let Some(left) = e.downcast_ref::<LeftMesh>() {
        tracing::error!(
            code = left.reason.code(),
            "this node has left the mesh and has no join token to join again with. To join, \
             create a new invite on a mesh node (`faber mesh invite`) and run `install.sh \
             --join <token>` here. Until then fabercp stays up but idle, with no listeners \
             and no WireGuard interface."
        );
        return Ok(None);
    }
    let Some(reason) = e.downcast_ref::<UnusableToken>().map(|u| u.reason) else {
        return Err(e);
    };
    note_bootstrap_error(&state_dir, reason);
    tracing::error!(
        error = %format!("{e:#}"),
        code = reason.code(),
        "this node cannot join the mesh with its join token. To recover, create a new invite \
         on a mesh node (`faber mesh invite`) and run `install.sh --join <token>` here, or put \
         the new token in FABER_JOIN_TOKEN_FILE and restart fabercp. Until then fabercp stays \
         up but idle, with no listeners."
    );
    Ok(None)
}

/// Where the deferred peer listener binds in mesh mode: the address
/// `start_with` asks for, which is the identity's `mesh_ip:peer_port`. The
/// listener never takes "whatever address the interface has", so a stale
/// address on the interface cannot become the node's Raft address. An
/// explicit `--peer-listen` (development on loopback) still wins.
fn mesh_peer_bind_addr(peer_listen: Option<SocketAddr>, intended: SocketAddr) -> SocketAddr {
    peer_listen.unwrap_or(intended)
}

/// Completes with the signal's name on Ctrl-C (SIGINT) or, on unix, SIGTERM,
/// which is what `systemctl stop` sends. The handlers are installed by this
/// call, not when the future is first polled, so a signal that arrives
/// during startup is still seen.
#[cfg(unix)]
fn shutdown_signal() -> std::io::Result<impl Future<Output = std::io::Result<&'static str>>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => Ok("SIGTERM"),
            _ = interrupt.recv() => Ok("SIGINT"),
        }
    })
}

/// Completes with the signal's name on Ctrl-C. There is no SIGTERM off unix.
#[cfg(not(unix))]
fn shutdown_signal() -> std::io::Result<impl Future<Output = std::io::Result<&'static str>>> {
    Ok(async {
        tokio::signal::ctrl_c().await?;
        Ok("Ctrl-C")
    })
}

/// Validates the config, binds both listeners, runs until Ctrl-C or SIGTERM,
/// then shuts down. Returns an error, so the process exits non-zero, if the
/// raft core stops before a signal arrives ([`run_until_stopped`]). In mesh mode the peer listener is bound only after
/// WireGuard is up, on the identity's `mesh_ip:peer_port`. With a join token
/// that can never work nothing is started and the process only waits for the
/// signal ([`start_or_idle`]).
pub async fn run(config: Config) -> anyhow::Result<()> {
    config.validate()?;
    bind::check_api_addr(config.api_listen)?;
    let stop = shutdown_signal().context("installing signal handlers")?;
    let api_listener = TcpListener::bind(config.api_listen)
        .await
        .with_context(|| format!("binding {}", config.api_listen))?;
    let backends = config.manage_host.then(|| HostBackends {
        network: NetworkManager::system(),
        runtime: Arc::new(runner::NodeRuntime::new(runner::SocketNodeClient::new(
            config.node_socket.clone(),
        ))),
    });

    let peer = if config.mesh_mode() {
        let peer_listen = config.peer_listen;
        PeerListener::deferred(move |intended| {
            Box::pin(async move {
                let addr = mesh_peer_bind_addr(peer_listen, intended);
                TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("binding {addr}"))
            })
        })
    } else {
        let peer_addr = bind::peer_addr(
            config.wg_interface.as_deref(),
            config.peer_listen,
            config.peer_port,
            &SystemInterfaces,
        )?;
        PeerListener::Bound(
            TcpListener::bind(peer_addr)
                .await
                .with_context(|| format!("binding {peer_addr}"))?,
        )
    };

    let node = start_or_idle(config, api_listener, peer, backends).await?;
    run_until_stopped(node, stop).await
}

/// Waits for the shutdown signal `stop`, then shuts `node` down. If the raft
/// core stops first, that is an error: the node is shut down as far as that
/// still works and the error is returned, so the process exits non-zero and
/// the service manager starts it again. An idle process (`None`) only waits
/// for the signal.
async fn run_until_stopped(
    node: Option<NodeHandle>,
    stop: impl Future<Output = std::io::Result<&'static str>>,
) -> anyhow::Result<()> {
    let Some(node) = node else {
        let signal = stop.await?;
        tracing::info!(signal, "shutting down");
        return Ok(());
    };
    let raft_stopped = node.raft_stopped();
    tokio::select! {
        signal = stop => {
            let signal = signal?;
            tracing::info!(signal, "shutting down");
            node.shutdown().await
        }
        reason = raft_stopped => {
            tracing::error!(
                %reason,
                "the raft core stopped; fabercp exits so that it is started again"
            );
            if let Err(e) = node.shutdown().await {
                tracing::warn!(error = %format!("{e:#}"), "shutdown after the raft core stopped");
            }
            Err(anyhow::anyhow!("the raft core stopped: {reason}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use faber_api_types::{ErrorBody, error_code};
    use faber_container_runtime::{NodeRuntimeSpec, PodNetworkSpec};
    use faber_network_management::fake::RecordingRunner;
    use faber_network_management::{CommandOutput, CommandRunner};

    use super::*;

    /// A node can leave the mesh before the mesh reached it, and with a CNI
    /// config from an earlier life still on faber-node's disk. The removal
    /// needs no replicated state and asks faber-node for the removal only.
    #[tokio::test]
    async fn the_pod_network_is_removed_without_replicated_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = raft::store::open(dir.path().join("data")).unwrap();
        assert!(storage.reader.mesh().unwrap().is_none());
        let node = runner::node::fake::FakeNode::default();
        let runtime = runner::NodeRuntime::new(node.clone());
        let spec = NodeRuntimeSpec {
            pod_network: Some(PodNetworkSpec {
                subnet: "10.200.3.0/24".parse().unwrap(),
                mtu: 1420,
            }),
        };
        runtime.reconcile(&spec).await.unwrap();
        node.take_log();

        remove_pod_network(&runtime, &storage.reader).await;
        assert_eq!(node.log(), ["remove_cni_config"]);
        assert_eq!(node.runtime_files().unwrap().pod_network, None);

        // Nothing left to remove: still not a failure.
        remove_pod_network(&runtime, &storage.reader).await;
        assert_eq!(node.runtime_files().unwrap().pod_network, None);
    }

    #[test]
    fn the_mesh_peer_listener_binds_the_identity_address() {
        let intended: SocketAddr = "10.99.0.2:7401".parse().unwrap();
        assert_eq!(mesh_peer_bind_addr(None, intended), intended);
        let explicit: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        assert_eq!(mesh_peer_bind_addr(Some(explicit), intended), explicit);
    }

    #[test]
    fn only_invite_codes_end_a_pending_join() {
        let upstream = |status: u16, code: &str| ServiceError::Upstream {
            status,
            body: ErrorBody::new(code),
        };
        for code in [
            error_code::INVITE_INVALID,
            error_code::INVITE_USED,
            error_code::INVITE_EXPIRED,
        ] {
            assert!(is_invite_rejection(&upstream(409, code)), "{code}");
        }
        // Retryable: the leader is not ready to promote, or is unknown.
        for retry in [
            upstream(503, error_code::NOT_READY),
            upstream(503, error_code::NO_LEADER),
            upstream(503, error_code::NOT_LEADER),
            upstream(409, error_code::MESH_NOT_CONFIGURED),
            upstream(409, error_code::MESH_LEASE_EXPIRED),
            ServiceError::NotReady("x".into()),
            ServiceError::PeerUnreachable("x".into()),
        ] {
            assert!(!is_invite_rejection(&retry), "{retry:?}");
            assert!(!is_removal(&retry), "{retry:?}");
        }
        // The mesh has removed the node: the other answer that ends a join.
        let removed = upstream(409, error_code::MESH_NODE_REMOVED);
        assert!(is_removal(&removed));
        assert!(!is_invite_rejection(&removed));
        assert!(!is_removal(&upstream(409, error_code::INVITE_USED)));
    }

    /// Metrics of a node whose newest membership, in the log entry at
    /// `changed`, has `voters` and `learners`, and that has applied its log
    /// up to `applied`.
    fn metrics_with(
        voters: &[u64],
        learners: &[u64],
        changed: Option<u64>,
        applied: Option<u64>,
    ) -> RaftMetrics<NodeId, Node> {
        joint_metrics(&[voters], learners, changed, applied)
    }

    /// [`metrics_with`] for a membership with the voter sets `voter_sets`,
    /// oldest first: a joint config when there are two.
    fn joint_metrics(
        voter_sets: &[&[u64]],
        learners: &[u64],
        changed: Option<u64>,
        applied: Option<u64>,
    ) -> RaftMetrics<NodeId, Node> {
        use openraft::testing::log_id;
        use openraft::{Membership, StoredMembership};

        let nodes: std::collections::BTreeMap<u64, Node> = voter_sets
            .iter()
            .flat_map(|voters| voters.iter())
            .chain(learners)
            .map(|id| (*id, Node::new(format!("10.99.0.{id}:7401"))))
            .collect();
        let voter_sets = voter_sets
            .iter()
            .map(|voters| voters.iter().copied().collect())
            .collect();
        let membership = Membership::new(voter_sets, nodes);
        let mut m = RaftMetrics::new_initial(2);
        m.membership_config = Arc::new(StoredMembership::new(
            changed.map(|i| log_id(1, 1, i)),
            membership,
        ));
        m.last_applied = applied.map(|i| log_id(1, 1, i));
        m
    }

    #[test]
    fn only_an_applied_membership_without_the_node_says_it_was_removed() {
        // Removed: others are members, this node is not, and it is applied.
        assert!(left_membership(
            &metrics_with(&[1, 3], &[], Some(7), Some(7)),
            2
        ));
        assert!(left_membership(
            &metrics_with(&[1, 3], &[4], Some(7), Some(9)),
            2
        ));

        // Still a member, as a voter or as a learner.
        assert!(!left_membership(
            &metrics_with(&[1, 2, 3], &[], Some(7), Some(9)),
            2
        ));
        assert!(!left_membership(
            &metrics_with(&[1, 3], &[2], Some(7), Some(9)),
            2
        ));
        // Not loaded yet, or a node that was never in a cluster.
        assert!(!left_membership(&RaftMetrics::new_initial(2), 2));
        assert!(!left_membership(&metrics_with(&[], &[], None, None), 2));
        assert!(!left_membership(
            &metrics_with(&[], &[], Some(7), Some(9)),
            2
        ));
        // In the log but not applied: the next leader may still drop it.
        assert!(!left_membership(
            &metrics_with(&[1, 3], &[], Some(7), Some(6)),
            2
        ));
        assert!(!left_membership(
            &metrics_with(&[1, 3], &[], Some(7), None),
            2
        ));
        assert!(!left_membership(
            &metrics_with(&[1, 3], &[], None, Some(9)),
            2
        ));
    }

    /// Raft removes a voter through a joint config, and a removed follower
    /// never gets the membership after it.
    #[test]
    fn an_applied_joint_membership_that_drops_the_node_says_it_was_removed() {
        let leaving: &[&[u64]] = &[&[1, 2, 3], &[1, 3]];
        assert!(left_membership(
            &joint_metrics(leaving, &[], Some(7), Some(7)),
            2
        ));
        // The others stay.
        assert!(!left_membership(
            &joint_metrics(leaving, &[], Some(7), Some(7)),
            1
        ));
        assert!(!left_membership(
            &joint_metrics(leaving, &[], Some(7), Some(7)),
            3
        ));
        // Not applied yet.
        assert!(!left_membership(
            &joint_metrics(leaving, &[], Some(7), Some(6)),
            2
        ));

        // Another node joins or leaves: this one is in the newest set.
        let joining: &[&[u64]] = &[&[1, 2], &[1, 2, 3]];
        assert!(!left_membership(
            &joint_metrics(joining, &[], Some(7), Some(7)),
            2
        ));
        assert!(!left_membership(
            &joint_metrics(joining, &[], Some(7), Some(7)),
            3
        ));
        let other: &[&[u64]] = &[&[1, 2, 3], &[1, 2]];
        assert!(!left_membership(
            &joint_metrics(other, &[], Some(7), Some(7)),
            2
        ));
        // A learner while voters change was never a voter.
        assert!(!left_membership(
            &joint_metrics(joining, &[4], Some(7), Some(7)),
            4
        ));
    }

    #[test]
    fn a_lease_is_renewed_four_times_per_period() {
        let second = Duration::from_secs(1);
        assert_eq!(renew_every(28_800, second), Duration::from_secs(7_200));
        assert_eq!(renew_every(300, second), Duration::from_secs(75));
        assert_eq!(lease_period(300, second), Duration::from_secs(300));
        // Tests run the lease clock faster.
        let fast = Duration::from_millis(1);
        assert_eq!(renew_every(2_000, fast), Duration::from_millis(500));
        // Nothing overflows.
        assert!(renew_every(u64::MAX, second) >= Duration::from_secs(u32::MAX.into()));
    }

    #[tokio::test(start_paused = true)]
    async fn an_unrenewed_lease_is_warned_about_after_half_a_period() {
        use tokio::time::{Instant, advance};

        let lease = Duration::from_secs(300);
        let mut unrenewed = Unrenewed::new(Instant::now());
        advance(Duration::from_secs(149)).await;
        assert!(!unrenewed.warn_due(Instant::now(), lease));
        advance(Duration::from_secs(1)).await;
        assert!(unrenewed.warn_due(Instant::now(), lease));
        // Not again until a whole period has passed since.
        assert!(!unrenewed.warn_due(Instant::now(), lease));
        advance(Duration::from_secs(299)).await;
        assert!(!unrenewed.warn_due(Instant::now(), lease));
        advance(Duration::from_secs(1)).await;
        assert!(unrenewed.warn_due(Instant::now(), lease));
        // A renewal starts it all again.
        advance(Duration::from_secs(200)).await;
        unrenewed.renewed(Instant::now());
        advance(Duration::from_secs(149)).await;
        assert!(!unrenewed.warn_due(Instant::now(), lease));
        advance(Duration::from_secs(1)).await;
        assert!(unrenewed.warn_due(Instant::now(), lease));
    }

    #[test]
    fn only_forbidden_and_bad_request_are_lasting_refusals() {
        let upstream = |status: u16, code: &str| ServiceError::Upstream {
            status,
            body: ErrorBody::new(code),
        };
        let rejected = |code: &str| ServiceError::Rejected {
            code: code.into(),
            message: "m".into(),
        };
        for (e, code) in [
            (ServiceError::Forbidden("x".into()), "forbidden"),
            (rejected(error_code::FORBIDDEN), "forbidden"),
            (upstream(403, error_code::FORBIDDEN), "forbidden"),
            (ServiceError::BadRequest("x".into()), "bad_request"),
            (upstream(400, error_code::BAD_REQUEST), "bad_request"),
        ] {
            assert_eq!(lasting_refusal(&e), Some(code), "{e:?}");
        }
        for e in [
            ServiceError::NoLeader,
            ServiceError::NotReady("x".into()),
            ServiceError::PeerUnreachable("x".into()),
            upstream(503, error_code::NOT_READY),
            upstream(409, error_code::MESH_LEASE_EXPIRED),
            rejected(error_code::MESH_NODE_REMOVED),
        ] {
            assert_eq!(lasting_refusal(&e), None, "{e:?}");
        }
    }

    /// A config that joins from `token`, with everything under `dir`.
    fn joiner_config(dir: &Path, token: &mesh::InviteToken) -> Config {
        let state = dir.join("state");
        std::fs::create_dir_all(&state).unwrap();
        let file = dir.join("join.token");
        std::fs::write(&file, token.encode()).unwrap();
        let mut config = Config::new(1, dir.join("data"));
        config.node_id = None;
        config.state_dir = state;
        config.join_token_file = Some(file);
        config
    }

    async fn loopback() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").await.unwrap()
    }

    /// A token for node 2 that is valid until `expires_at`.
    fn invite_token(invite_id: &str, expires_at: u64) -> mesh::InviteToken {
        let (key, _) = mesh::generate_keypair().unwrap();
        mesh::InviteToken {
            v: mesh::token::TOKEN_VERSION,
            invite_id: invite_id.into(),
            expires_at,
            node_id: 2,
            mesh_ip: "10.99.0.2".parse().unwrap(),
            mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            endpoint: "198.51.100.2:51820".into(),
            private_key: key.expose().to_string(),
            peers: vec![],
            join_addrs: vec![],
        }
    }

    fn bootstrap_error(state_dir: &Path) -> Option<String> {
        std::fs::read_to_string(mesh::bootstrap::path(state_dir)).ok()
    }

    async fn idle(config: Config) -> anyhow::Result<Option<NodeHandle>> {
        start_or_idle(
            config,
            loopback().await,
            PeerListener::Bound(loopback().await),
            None,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_expired_join_token_leaves_the_process_idle() {
        let dir = tempfile::tempdir().unwrap();
        let config = joiner_config(dir.path(), &invite_token(&"ab".repeat(16), 1));
        let state_dir = config.state_dir.clone();

        let node = idle(config)
            .await
            .expect("an expired token is not a startup error");
        assert!(node.is_none());
        assert!(!identity::path(&state_dir).exists());
        // Nothing listens, so the reason is left on disk.
        assert_eq!(
            bootstrap_error(&state_dir).as_deref(),
            Some("token_expired\n")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::symlink_metadata(mesh::bootstrap::path(&state_dir)).unwrap();
            assert!(meta.file_type().is_file());
            assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_invalid_join_token_leaves_the_process_idle() {
        let mut outside = invite_token(&"ab".repeat(16), u64::MAX);
        outside.mesh_ip = "10.100.0.2".parse().unwrap();
        for token in ["not a token".to_string(), String::new(), outside.encode()] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = joiner_config(dir.path(), &outside);
            std::fs::write(config.join_token_file.as_ref().unwrap(), &token).unwrap();
            config.manage_host = true;
            let state_dir = config.state_dir.clone();

            let node = idle(config)
                .await
                .expect("an invalid token is not a startup error");
            assert!(node.is_none(), "{token:?}");
            assert!(!identity::path(&state_dir).exists());
            assert_eq!(
                bootstrap_error(&state_dir).as_deref(),
                Some("token_invalid\n"),
                "{token:?}"
            );
        }
    }

    #[test]
    fn only_the_token_itself_makes_a_token_unusable() {
        use crate::mesh::TokenError;

        assert_eq!(
            unusable_token(&IdentityError::TokenExpired {
                expires_at: 1,
                now: 2
            }),
            Some(BootstrapError::TokenExpired)
        );
        for invalid in [
            IdentityError::Token(TokenError::BadPrefix),
            IdentityError::InvalidEndpoint("node2.example:51820".into()),
            IdentityError::IpOutsideCidr {
                mesh_ip: "10.100.0.2".parse().unwrap(),
                mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            },
            IdentityError::Key(KeyError::Length(3)),
        ] {
            assert_eq!(
                unusable_token(&invalid),
                Some(BootstrapError::TokenInvalid),
                "{invalid}"
            );
        }
        // A flag or the disk is at fault, and a restart can help.
        for other in [
            IdentityError::NotBootstrapped,
            IdentityError::EndpointMismatch {
                given: "192.0.2.9:51820".parse().unwrap(),
                invite: "198.51.100.2:51820".parse().unwrap(),
            },
            IdentityError::Key(KeyError::Rng("x".into())),
            IdentityError::Io {
                path: PathBuf::from("identity.json"),
                source: std::io::Error::other("disk"),
            },
        ] {
            assert_eq!(unusable_token(&other), None, "{other}");
        }
    }

    /// A broken identity.json is not the token's fault, even though the
    /// error is of the same kind as for a broken token.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_corrupt_identity_is_an_error_and_leaves_no_marker() {
        let dir = tempfile::tempdir().unwrap();
        let config = joiner_config(dir.path(), &invite_token(&"ab".repeat(16), 1));
        let state_dir = config.state_dir.clone();
        let mut broken = identity_of(&invite_token(&"ab".repeat(16), u64::MAX));
        broken.mesh_ip = "10.100.0.2".parse().unwrap();
        std::fs::write(
            identity::path(&state_dir),
            serde_json::to_vec(&broken).unwrap(),
        )
        .unwrap();

        let err = idle(config).await.err().map(|e| format!("{e:#}"));
        assert!(
            err.as_deref().is_some_and(|e| e.contains("is not in")),
            "{err:?}"
        );
        assert_eq!(bootstrap_error(&state_dir), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_start_that_resolves_an_identity_removes_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let config = joiner_config(dir.path(), &invite_token(&"ab".repeat(16), u64::MAX));
        let state_dir = config.state_dir.clone();
        mesh::bootstrap::write(&state_dir, BootstrapError::TokenExpired).unwrap();

        let node = idle(config).await.unwrap().expect("the node starts");
        assert!(identity::path(&state_dir).is_file());
        assert_eq!(bootstrap_error(&state_dir), None);
        node.shutdown().await.unwrap();
    }

    /// The identity a node has while it joins with `token`.
    fn identity_of(token: &mesh::InviteToken) -> Identity {
        Identity {
            v: identity::IDENTITY_VERSION,
            node_id: token.node_id,
            private_key: mesh::PrivateKey::new(token.private_key.clone()),
            mesh_ip: token.mesh_ip,
            mesh_cidr: token.mesh_cidr,
            wg_port: 51820,
            endpoint: token.endpoint.parse().unwrap(),
            instance_id: Some("1d".repeat(16)),
            pending_join: Some(mesh::PendingJoin {
                invite_id: token.invite_id.clone(),
                join_addrs: token.join_addrs.clone(),
                bootstrap_peers: token.peers.clone(),
                registered: false,
            }),
        }
    }

    /// A node that is joining with invite `ab..`, and whose token file now
    /// holds `token`. Returns its config and the pending identity.
    fn pending_joiner(dir: &Path, token: &mesh::InviteToken) -> (Config, Identity) {
        let config = joiner_config(dir, token);
        let pending = identity_of(&invite_token(&"ab".repeat(16), u64::MAX));
        identity::save(&config.state_dir, &pending).unwrap();
        std::fs::create_dir_all(&config.data_dir).unwrap();
        (config, pending)
    }

    #[tokio::test]
    async fn another_usable_token_replaces_a_pending_join() {
        let dir = tempfile::tempdir().unwrap();
        let token = invite_token(&"cd".repeat(16), u64::MAX);
        let (config, _) = pending_joiner(dir.path(), &token);
        // Raft storage the pending join left behind, with no membership.
        drop(store::open(&config.data_dir).unwrap());
        assert!(std::fs::read_dir(&config.data_dir).unwrap().count() > 0);

        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Replaced
        );
        assert!(identity::load(&config.state_dir).unwrap().is_none());
        assert_eq!(std::fs::read_dir(&config.data_dir).unwrap().count(), 0);
        assert!(!identity::join_failed_path(&config.state_dir).exists());
        // Nothing left to replace.
        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Kept
        );
        // The new token then makes the identity.
        let created = load_identity(&config).unwrap();
        assert_eq!(created.pending_join.unwrap().invite_id, "cd".repeat(16));
    }

    #[tokio::test]
    async fn a_pending_join_is_kept_without_another_usable_token() {
        use BootstrapError::{TokenExpired, TokenInvalid};

        let same = invite_token(&"ab".repeat(16), u64::MAX);
        let expired = invite_token(&"cd".repeat(16), 1);
        let mut invalid = invite_token(&"cd".repeat(16), u64::MAX);
        invalid.mesh_ip = "10.100.0.2".parse().unwrap();
        for (token, want) in [
            (same, Superseded::Kept),
            (expired, Superseded::Refused(TokenExpired)),
            (invalid, Superseded::Refused(TokenInvalid)),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (config, pending) = pending_joiner(dir.path(), &token);
            assert_eq!(replace_superseded_join(&config).await.unwrap(), want);
            assert_eq!(identity::load(&config.state_dir).unwrap(), Some(pending));
        }

        // A usable token for an endpoint other than --wg-endpoint.
        let dir = tempfile::tempdir().unwrap();
        let other = invite_token(&"cd".repeat(16), u64::MAX);
        let (mut config, pending) = pending_joiner(dir.path(), &other);
        config.wg_endpoint = Some("192.0.2.9:51820".into());
        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Refused(TokenInvalid)
        );
        config.wg_endpoint = None;

        // Garbage in the token file, no token file, and no setting at all.
        let file = config.join_token_file.clone().unwrap();
        std::fs::write(&file, "not a token").unwrap();
        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Refused(TokenInvalid)
        );
        std::fs::remove_file(&file).unwrap();
        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Kept
        );
        config.join_token_file = None;
        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Kept
        );
        assert_eq!(identity::load(&config.state_dir).unwrap(), Some(pending));
    }

    /// The operator replaced the token of a stuck join with one that cannot
    /// be used. The node keeps its pending join, and the marker says what is
    /// wrong with the new token, so install.sh can tell the operator. The
    /// start that finds the token unusable must not remove the marker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_replacement_token_leaves_the_reason_in_the_marker() {
        let mut invalid = invite_token(&"cd".repeat(16), u64::MAX);
        invalid.mesh_ip = "10.100.0.2".parse().unwrap();
        for (token, code) in [
            (invite_token(&"cd".repeat(16), 1), "token_expired\n"),
            (invalid, "token_invalid\n"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (config, pending) = pending_joiner(dir.path(), &token);
            let state_dir = config.state_dir.clone();

            let node = idle(config).await.unwrap().expect("the node starts");
            assert_eq!(bootstrap_error(&state_dir).as_deref(), Some(code));
            let kept = identity::load(&state_dir).unwrap().unwrap();
            assert_eq!(kept.pending_join, pending.pending_join);
            assert_eq!(kept.node_id, pending.node_id);
            node.shutdown().await.unwrap();
        }

        // The same token as the pending join is no replacement: an old
        // marker goes, as at any start that has an identity.
        let dir = tempfile::tempdir().unwrap();
        let same = invite_token(&"ab".repeat(16), u64::MAX);
        let (config, _) = pending_joiner(dir.path(), &same);
        let state_dir = config.state_dir.clone();
        mesh::bootstrap::write(&state_dir, BootstrapError::TokenExpired).unwrap();
        let node = idle(config).await.unwrap().expect("the node starts");
        assert_eq!(bootstrap_error(&state_dir), None);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_joined_node_keeps_its_identity_whatever_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let other = invite_token(&"cd".repeat(16), u64::MAX);
        let (config, pending) = pending_joiner(dir.path(), &other);
        let mut joined = pending;
        joined.pending_join = None;
        identity::save(&config.state_dir, &joined).unwrap();
        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Kept
        );
        assert_eq!(identity::load(&config.state_dir).unwrap(), Some(joined));
    }

    /// Node 2 is a learner in the stored membership: its join got that far,
    /// so its identity and raft data stay.
    #[tokio::test]
    async fn a_member_keeps_its_pending_identity_whatever_the_token() {
        use std::collections::BTreeMap;

        use openraft::storage::RaftStateMachine;

        use crate::raft::types::{Entry, Node};

        let dir = tempfile::tempdir().unwrap();
        let other = invite_token(&"cd".repeat(16), u64::MAX);
        let (config, pending) = pending_joiner(dir.path(), &other);
        {
            let mut storage = store::open(&config.data_dir).unwrap();
            let nodes: BTreeMap<u64, Node> = [1u64, 2]
                .into_iter()
                .map(|id| (id, Node::new(format!("10.99.0.{id}:7401"))))
                .collect();
            let entry = Entry {
                log_id: openraft::testing::log_id(1, 1, 1),
                payload: openraft::EntryPayload::Membership(openraft::Membership::new(
                    vec![[1u64].into_iter().collect()],
                    nodes,
                )),
            };
            storage.state_machine.apply([entry]).await.unwrap();
        }

        assert_eq!(
            replace_superseded_join(&config).await.unwrap(),
            Superseded::Kept
        );
        assert_eq!(identity::load(&config.state_dir).unwrap(), Some(pending));
        // The storage was closed again and is intact.
        let mut storage = store::open(&config.data_dir).unwrap();
        assert!(store::knows_member(&mut storage, 2).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn other_bootstrap_failures_are_still_errors() {
        let dir = tempfile::tempdir().unwrap();
        // Mesh mode with no identity, no token and no --new-mesh.
        let mut config = Config::new(1, dir.path().join("data"));
        config.node_id = None;
        config.state_dir = dir.path().to_path_buf();
        config.manage_host = true;
        let err = start_or_idle(
            config,
            loopback().await,
            PeerListener::Bound(loopback().await),
            None,
        )
        .await
        .err()
        .map(|e| e.to_string());
        assert!(
            err.as_deref()
                .is_some_and(|e| e.contains("not bootstrapped")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn discarding_the_identity_removes_the_cluster_key() {
        use crate::secrets::ClusterKeyProvider as _;

        for reason in [
            BootstrapError::NodeRemoved,
            BootstrapError::InviteRejected,
            BootstrapError::KeyMismatch,
            BootstrapError::KeyUnavailable,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state_dir = dir.path().to_path_buf();
            let key_file = state_dir.join("cluster.key");
            secrets::FileClusterKey::new(&key_file)
                .create_if_missing()
                .unwrap();
            assert!(key_file.exists());

            discard_identity(state_dir.clone(), key_file.clone(), reason).await;
            assert!(!key_file.exists(), "{reason:?}");
            assert_eq!(mesh::bootstrap::read(&state_dir).unwrap(), Some(reason));
        }
    }

    /// What `leave_mesh` leaves behind: no identity, the `join-failed`
    /// marker and the reason. The bootstrap settings are gone from the
    /// environment, as install.sh leaves it once a node has joined (or as
    /// an operator leaves it by hand). A restart must not create the
    /// interface again and then exit because there is nothing to start as:
    /// systemd would repeat that every few seconds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_that_left_the_mesh_stays_idle_after_a_restart() {
        for reason in [
            BootstrapError::NodeRemoved,
            BootstrapError::InviteRejected,
            BootstrapError::KeyMismatch,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state_dir = dir.path().join("state");
            let data_dir = dir.path().join("data");
            std::fs::create_dir_all(&state_dir).unwrap();
            std::fs::create_dir_all(&data_dir).unwrap();
            std::fs::write(data_dir.join("CURRENT"), "raft data of the old identity").unwrap();
            mesh::bootstrap::write(&state_dir, reason).unwrap();
            identity::mark_join_failed(&state_dir).unwrap();
            let mut config = Config::new(1, &data_dir);
            config.node_id = None;
            config.state_dir = state_dir.clone();
            config.manage_host = true;
            let (host, network) = link_host(&state_dir);
            let backends = HostBackends {
                network,
                runtime: Arc::new(runner::NodeRuntime::new(
                    runner::node::fake::FakeNode::default(),
                )),
            };

            let node = start_or_idle(
                config,
                loopback().await,
                PeerListener::Bound(loopback().await),
                Some(backends),
            )
            .await
            .expect("a node that left the mesh is not a startup error");
            assert!(node.is_none(), "{reason:?}");
            // The host was not touched.
            assert_eq!(host.rec.cmdlines(), Vec::<String>::new(), "{reason:?}");
            assert!(!mesh::iface::path(&state_dir).exists());
            // The reason stays for install.sh to read.
            assert_eq!(
                bootstrap_error(&state_dir),
                Some(format!("{}\n", reason.code()))
            );
            // The raft data of the old identity is gone, as at any start.
            assert!(!identity::join_failed_path(&state_dir).exists());
            assert_eq!(std::fs::read_dir(&data_dir).unwrap().count(), 0);
        }
    }

    /// The marker alone does not make a node idle: with a way to make an
    /// identity, or with one still in place, it starts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_removed_node_with_a_new_token_or_an_identity_starts() {
        // A new token (expired here, so that nothing is started).
        let dir = tempfile::tempdir().unwrap();
        let config = joiner_config(dir.path(), &invite_token(&"ab".repeat(16), 1));
        let state_dir = config.state_dir.clone();
        mesh::bootstrap::write(&state_dir, BootstrapError::NodeRemoved).unwrap();
        assert!(idle(config).await.unwrap().is_none());
        assert_eq!(
            bootstrap_error(&state_dir).as_deref(),
            Some("token_expired\n")
        );

        // A marker from a teardown that was cut short, before the identity
        // went: the node starts as itself and finds out again.
        let dir = tempfile::tempdir().unwrap();
        let token = invite_token(&"ab".repeat(16), u64::MAX);
        let (mut config, _) = pending_joiner(dir.path(), &token);
        config.join_token_file = None;
        config.manage_host = true;
        let state_dir = config.state_dir.clone();
        mesh::bootstrap::write(&state_dir, BootstrapError::NodeRemoved).unwrap();
        let node = idle(config).await.unwrap().expect("the node starts");
        assert_eq!(bootstrap_error(&state_dir), None);
        node.shutdown().await.unwrap();

        // Any other marker without an identity is still a startup error. A
        // missing cluster key may be put right on the mesh, so
        // `key_unavailable` keeps retrying where `key_mismatch` stays idle.
        for reason in [BootstrapError::TokenExpired, BootstrapError::KeyUnavailable] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = Config::new(1, dir.path().join("data"));
            config.node_id = None;
            config.state_dir = dir.path().to_path_buf();
            config.manage_host = true;
            mesh::bootstrap::write(&config.state_dir, reason).unwrap();
            let err = idle(config).await.err().map(|e| e.to_string());
            assert!(
                err.as_deref()
                    .is_some_and(|e| e.contains("not bootstrapped")),
                "{reason:?}: {err:?}"
            );
        }
    }

    /// A host whose link exists only once `ip link add` has run (or `link`
    /// was set by the test). Notes whether the ownership record was on disk
    /// when `ip link add` ran.
    #[derive(Default)]
    struct LinkHost {
        rec: RecordingRunner,
        link: AtomicBool,
        fail_add: AtomicBool,
        record: std::sync::Mutex<Option<PathBuf>>,
        record_seen_at_add: AtomicBool,
    }

    impl CommandRunner for LinkHost {
        fn run<'a>(
            &'a self,
            prog: &'a str,
            args: &'a [&'a str],
            stdin: Option<Vec<u8>>,
        ) -> BoxFuture<'a, std::io::Result<CommandOutput>> {
            Box::pin(async move {
                let out = self.rec.run(prog, args, stdin).await?;
                if prog == "ip" && args.starts_with(&["link", "show"]) {
                    return Ok(if self.link.load(Ordering::SeqCst) {
                        out
                    } else {
                        CommandOutput::failure(1, "Device does not exist.")
                    });
                }
                if prog == "ip" && args.starts_with(&["link", "add"]) {
                    if let Some(record) = self.record.lock().unwrap().as_deref() {
                        self.record_seen_at_add
                            .store(record.is_file(), Ordering::SeqCst);
                    }
                    if self.fail_add.load(Ordering::SeqCst) {
                        return Ok(CommandOutput::failure(2, "RTNETLINK answers: boom"));
                    }
                    self.link.store(true, Ordering::SeqCst);
                }
                Ok(out)
            })
        }
    }

    fn link_host(state_dir: &Path) -> (Arc<LinkHost>, NetworkManager) {
        let host = Arc::new(LinkHost::default());
        *host.record.lock().unwrap() = Some(mesh::iface::path(state_dir));
        (host.clone(), NetworkManager::new(host))
    }

    fn added(host: &LinkHost) -> bool {
        host.rec
            .cmdlines()
            .iter()
            .any(|l| l.starts_with("ip link add"))
    }

    /// A kill between `ip link add` and the record would leave a link that
    /// every later start refuses, so the record goes first.
    #[tokio::test]
    async fn the_ownership_record_is_on_disk_before_the_link_is_created() {
        let dir = tempfile::tempdir().unwrap();
        let (host, net) = link_host(dir.path());
        claim_interface(&net, dir.path(), "faber0").await.unwrap();
        assert!(added(&host));
        assert!(host.record_seen_at_add.load(Ordering::SeqCst));
        assert!(mesh::iface::is_owned(dir.path(), "faber0").unwrap());
        // No temp file is left next to the record.
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec![mesh::iface::IFACE_FILE.to_string()]);
    }

    #[tokio::test]
    async fn a_record_that_cannot_be_written_creates_no_link() {
        let dir = tempfile::tempdir().unwrap();
        // The state dir does not exist, so the record cannot be written.
        let state = dir.path().join("missing");
        let (host, net) = link_host(&state);
        let err = claim_interface(&net, &state, "faber0").await.unwrap_err();
        assert!(format!("{err:#}").contains("iface"), "{err:#}");
        assert!(!added(&host), "{:?}", host.rec.cmdlines());
    }

    #[tokio::test]
    async fn a_link_that_cannot_be_created_leaves_no_record() {
        let dir = tempfile::tempdir().unwrap();
        let (host, net) = link_host(dir.path());
        host.fail_add.store(true, Ordering::SeqCst);
        let err = claim_interface(&net, dir.path(), "faber0")
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("boom"), "{err:#}");
        assert!(!mesh::iface::path(dir.path()).exists());
        // The next start tries again from the beginning.
        host.fail_add.store(false, Ordering::SeqCst);
        claim_interface(&net, dir.path(), "faber0").await.unwrap();
        assert!(mesh::iface::is_owned(dir.path(), "faber0").unwrap());
    }

    /// The state a kill between the record and `ip link add` leaves behind,
    /// and the state after a reboot: `network.reconcile` creates the link.
    #[tokio::test]
    async fn a_recorded_interface_is_owned_with_or_without_its_link() {
        let dir = tempfile::tempdir().unwrap();
        mesh::iface::record(dir.path(), "faber0").unwrap();
        for link in [false, true] {
            let (host, net) = link_host(dir.path());
            host.link.store(link, Ordering::SeqCst);
            claim_interface(&net, dir.path(), "faber0").await.unwrap();
            assert!(host.rec.cmdlines().is_empty(), "{:?}", host.rec.cmdlines());
        }
    }

    #[tokio::test]
    async fn an_unrecorded_existing_link_is_refused_and_not_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let (host, net) = link_host(dir.path());
        host.link.store(true, Ordering::SeqCst);
        let err = claim_interface(&net, dir.path(), "faber0")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("exists and was not created by faber"), "{msg}");
        // The hint reads as one sentence, not a line broken in the source.
        assert!(
            msg.contains("if another interface has that name, remove it"),
            "{msg}"
        );
        assert_eq!(host.rec.cmdlines(), vec!["ip link show faber0"]);
        assert!(!mesh::iface::path(dir.path()).exists());
    }

    /// Without `StateDirectory=` from systemd the state dir may not exist.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_creates_a_missing_state_dir_before_claiming_the_interface() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("lib").join("faber");
        let (host, net) = link_host(&state);
        let mut config = crate::testing::fast_config(1, &dir.path().join("data"));
        config.node_id = None;
        config.state_dir = state.clone();
        config.manage_host = true;
        config.new_mesh = true;
        config.wg_endpoint = Some("203.0.113.1:51820".into());
        let backends = HostBackends {
            network: net,
            runtime: Arc::new(runner::NodeRuntime::new(
                runner::node::fake::FakeNode::default(),
            )),
        };
        let node = start_with(
            config,
            loopback().await,
            PeerListener::Bound(loopback().await),
            Some(backends),
        )
        .await
        .unwrap();
        assert!(host.record_seen_at_add.load(Ordering::SeqCst));
        assert!(mesh::iface::is_owned(&state, "faber0").unwrap());
        assert!(identity::path(&state).is_file());
        node.shutdown().await.unwrap();
    }

    async fn single_node() -> (tempfile::TempDir, NodeHandle) {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::testing::fast_config(1, dir.path());
        let node = start(config, loopback().await, loopback().await)
            .await
            .unwrap();
        node.service().init().await.unwrap();
        (dir, node)
    }

    /// A dead raft core must not leave the process running: nothing would
    /// restart it, and it would never make progress again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stopped_raft_core_ends_the_run_with_an_error() {
        let (_dir, node) = single_node().await;
        let raft = node.raft().clone();
        let stopped = node.raft_stopped();
        // No signal ever arrives.
        let run = tokio::spawn(run_until_stopped(
            Some(node),
            std::future::pending::<std::io::Result<&'static str>>(),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!run.is_finished(), "a healthy node keeps running");

        raft.shutdown().await.unwrap();
        let reason = tokio::time::timeout(Duration::from_secs(10), stopped)
            .await
            .expect("the stop was not seen");
        assert!(reason.contains("stopped"), "{reason}");
        let err = tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("run did not end")
            .unwrap()
            .expect_err("a stopped raft core is an error");
        assert!(
            format!("{err:#}").contains("the raft core stopped"),
            "{err:#}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_signal_ends_the_run_cleanly() {
        let (_dir, node) = single_node().await;
        run_until_stopped(Some(node), async { Ok("SIGTERM") })
            .await
            .unwrap();
        // An idle process has no node and only waits for the signal.
        run_until_stopped(None, async { Ok("SIGTERM") })
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sigterm_ends_the_wait_for_shutdown() {
        let stop = shutdown_signal().unwrap();
        let pid = std::process::id().to_string();
        let status = tokio::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .await
            .unwrap();
        assert!(status.success());
        let signal = tokio::time::timeout(Duration::from_secs(5), stop)
            .await
            .expect("SIGTERM was not seen")
            .unwrap();
        assert_eq!(signal, "SIGTERM");
    }

    #[cfg(not(unix))]
    #[tokio::test]
    async fn the_shutdown_wait_is_pending_without_a_signal() {
        let stop = shutdown_signal().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stop)
                .await
                .is_err()
        );
    }
}
