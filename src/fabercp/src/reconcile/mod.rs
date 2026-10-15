//! Host reconciliation.
//!
//! This module maps replicated state to the shapes the integration crates
//! take ([`desired_network`] to a [`NetworkSpec`], [`desired_runtime`] to a
//! [`NodeRuntimeSpec`]) and runs the one loop that applies them. How
//! WireGuard, nftables and the container runtime are driven is not known
//! here; see `faber-network-management` and `faber-container-runtime`.
//!
//! Container networking is part of both shapes. Raft gives every node a
//! subnet of the mesh's container range, and each node routes the range into
//! the tunnel, lists every other node's subnet in that peer's AllowedIPs,
//! and hands its own subnet to the runtime for the CNI config. The subnets
//! are read from the replicated `mesh` on every plan, never from
//! `identity.json`.
//!
//! The loop ([`HostReconciler::spawn`]) is spawned by `start_with` when it is
//! given [`HostBackends`], seeded with the state startup already applied. It
//! wakes on a state-machine watch tick (debounced), on the resync interval,
//! and when the earliest live invite expires (expiry changes the peer set
//! without any write). When the desired state differs from the last applied
//! one (or on the resync tick) it runs the [`step`]s in order: network, then
//! runtime configuration. Each step applies only when its own desired value
//! changed, or on the resync tick. A failing step does not skip the others;
//! any failure is logged and the loop backs off.

pub mod containers;
pub mod services;
pub mod step;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::Ipv4Addr;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use faber_container_runtime::{
    ContainerRuntime, ContainerRuntimeSpec, NodeRuntimeSpec, PodNetworkSpec,
};
use faber_network_management::{
    ContainerFirewall, FirewallSpec, NetworkManager, NetworkSpec, WG_MTU, WireGuardPeer,
    WireGuardSpec, endpoint_ip,
};
use ipnet::Ipv4Net;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use self::step::{
    Planned, ReconcileStep, ReplicatedStateView, StepFuture, StepRunner, StepValue, spawn_steps,
};
use crate::config::DEFAULT_RECONCILE_INTERVAL_SECS;
use crate::lease::LeaseTiming;
use crate::mesh::state::{MeshConfig, MeshState, WG_KEEPALIVE_SECS};
use crate::mesh::token::TokenPeer;
use crate::mesh::{Identity, JoinPending, validate_public_key};
use crate::raft::store::{ReadError, StateReader};
use crate::service::now_secs;

/// State changes are collapsed until this long passes without another one.
pub const DEBOUNCE_QUIET: Duration = Duration::from_secs(1);
/// ... but the loop never waits longer than this after the first change, so
/// a steady stream of writes cannot hold it off.
pub const DEBOUNCE_MAX: Duration = Duration::from_secs(5);
/// First retry delay after a failed apply.
pub const BACKOFF_MIN: Duration = Duration::from_secs(1);
/// Longest retry delay.
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Name of the bridge containers attach to, the same on every node.
pub const POD_BRIDGE: &str = "faberbr0";

/// The network state this node should have on interface `iface`, plus a
/// warning for every peer, endpoint or container subnet that was skipped
/// because it is malformed.
///
/// Peers: with `mesh`, every node except this one plus every invite with
/// `expires_at > now` (a node wins over an invite with the same mesh IP).
/// While `identity` has a pending join, the bootstrap peers from its join
/// token as well, with or without `mesh`: a joiner applies the log a batch
/// at a time, and the mesh of the first batch need not have the node that
/// leads now. Without its token peers the joiner would drop the leader's
/// WireGuard peer half way through catching up. A token peer gives way to a
/// node or invite with the same mesh IP or the same public key. Public
/// keys must be base64 of 32 bytes, or the peer is skipped. Endpoints must be
/// `ip:port` / `[ip6]:port`, or the peer is kept without an endpoint and its
/// IP stays out of the firewall's endpoint set. Sorted by mesh IP.
///
/// The WireGuard spec and the firewall spec are built from the same peer
/// set, so the firewall admits exactly the peers WireGuard is configured
/// with.
///
/// Container subnets, when `mesh` has a container range
/// (`config.container_cidr`). They are read from `mesh` on every call;
/// `identity` holds none.
/// - A registered node's AllowedIPs are its mesh `/32` and its
///   `container_subnet`. Invites and bootstrap peers keep the `/32` alone.
/// - A peer's subnet is left out, with a warning, when it does not fit the
///   range ([`subnet_problem`]), is this node's own, or is already in the
///   AllowedIPs of an earlier peer: WireGuard gives an AllowedIP that two
///   peers list to the last of them, without an error.
/// - The range is routed into the interface (`routes`).
/// - `firewall.containers` holds the range, this node's own subnet from
///   `mesh.nodes` (when it fits the range) and the mesh IPs of the other
///   registered nodes. Invites are not in that set, so a machine holding an
///   unused invite has no way to the containers.
///
/// Without a range, and without `mesh`, there is no route and no container
/// rule.
pub fn desired_network(
    identity: &Identity,
    iface: &str,
    peer_port: u16,
    mesh: Option<&MeshState>,
    now: u64,
) -> (NetworkSpec, Vec<String>) {
    let bootstrap = identity
        .pending_join
        .as_ref()
        .map_or(&[][..], |p| p.bootstrap_peers.as_slice());
    let (sources, mut warnings) = peer_set(identity.node_id, mesh, bootstrap, now);

    let range = mesh.and_then(|m| m.config.container_cidr);
    let local_subnet = mesh.and_then(|m| match own_subnet(m, identity.node_id) {
        Ok(subnet) => subnet,
        Err(problem) => {
            warnings.push(format!(
                "node {} (this node): {problem}; no container network here",
                identity.node_id
            ));
            None
        }
    });

    let mut peers = Vec::with_capacity(sources.len());
    let mut peer_endpoint_ips = BTreeSet::new();
    let mut peer_mesh_ips = BTreeSet::new();
    let mut node_mesh_ips = BTreeSet::new();
    // Subnets already in a peer's AllowedIPs, with that peer's label.
    let mut claimed: BTreeMap<Ipv4Net, String> = BTreeMap::new();
    for p in sources {
        let endpoint = p.endpoint.and_then(|ep| match endpoint_ip(ep) {
            Some(ip) => {
                peer_endpoint_ips.insert(ip);
                Some(ep.to_string())
            }
            None => {
                warnings.push(format!(
                    "{}: endpoint {ep:?} is not ip:port; peer added without an endpoint",
                    p.label
                ));
                None
            }
        });
        peer_mesh_ips.insert(p.mesh_ip);
        if p.registered {
            node_mesh_ips.insert(p.mesh_ip);
        }
        let mut allowed_ips = vec![Ipv4Net::from(p.mesh_ip)];
        if let (Some(subnet), Some(m)) = (p.container_subnet, mesh) {
            let problem = subnet_problem(&m.config, subnet).or_else(|| {
                if Some(subnet) == local_subnet {
                    Some(format!(
                        "container subnet {subnet} is this node's own subnet"
                    ))
                } else {
                    claimed.get(&subnet).map(|holder| {
                        format!("container subnet {subnet} is already routed to {holder}")
                    })
                }
            });
            match problem {
                Some(problem) => {
                    warnings.push(format!("{}: {problem}; subnet not routed", p.label));
                }
                None => {
                    claimed.insert(subnet, p.label.clone());
                    allowed_ips.push(subnet);
                }
            }
        }
        peers.push(WireGuardPeer {
            public_key: p.public_key.to_string(),
            endpoint,
            allowed_ips,
            keepalive: WG_KEEPALIVE_SECS,
        });
    }

    let address = Ipv4Net::new(identity.mesh_ip, identity.mesh_cidr.prefix_len())
        .expect("prefix length taken from an Ipv4Net is valid");
    let spec = NetworkSpec {
        interface: iface.to_string(),
        wireguard: WireGuardSpec {
            private_key: identity.private_key.expose().to_string(),
            listen_port: identity.wg_port,
            address,
            peers,
            routes: range.into_iter().collect(),
        },
        firewall: FirewallSpec {
            wg_port: identity.wg_port,
            peer_port,
            peer_endpoint_ips,
            peer_mesh_ips,
            containers: range.map(|cidr| ContainerFirewall {
                cidr,
                local_subnet,
                node_mesh_ips,
                bridge: POD_BRIDGE.to_string(),
            }),
        },
    };
    (spec, warnings)
}

/// Why `subnet` cannot be a node's container subnet in a mesh with `config`,
/// if it cannot: the mesh has no container range, the prefix is not
/// `container_subnet_prefix`, host bits are set, or it lies outside the
/// range. Raft allocates subnets that pass; this guards the host against a
/// record that does not.
fn subnet_problem(config: &MeshConfig, subnet: Ipv4Net) -> Option<String> {
    let Some(range) = config.container_cidr else {
        return Some(format!(
            "container subnet {subnet} in a mesh without a container range"
        ));
    };
    let prefix = config.container_subnet_prefix;
    if subnet.prefix_len() != prefix {
        return Some(format!("container subnet {subnet} is not a /{prefix}"));
    }
    if subnet != subnet.trunc() {
        return Some(format!("container subnet {subnet} has host bits set"));
    }
    if !range.contains(&subnet) {
        return Some(format!(
            "container subnet {subnet} is outside the container range {range}"
        ));
    }
    None
}

/// The container subnet of node `node_id` in `mesh.nodes`. `Ok(None)` when
/// the node is not registered or its record has no subnet; `Err` with the
/// reason when the record's subnet fails [`subnet_problem`].
fn own_subnet(mesh: &MeshState, node_id: u64) -> Result<Option<Ipv4Net>, String> {
    let Some(subnet) = mesh.nodes.get(&node_id).and_then(|n| n.container_subnet) else {
        return Ok(None);
    };
    match subnet_problem(&mesh.config, subnet) {
        Some(problem) => Err(problem),
        None => Ok(Some(subnet)),
    }
}

/// How long until the earliest invite that is still live at `now` expires.
/// `None` when there is no such invite. Expiry removes the invite from the
/// peer set without a state change, so the loop sleeps until then.
pub fn next_invite_expiry(mesh: Option<&MeshState>, now: u64) -> Option<Duration> {
    mesh?
        .invites
        .values()
        .map(|i| i.expires_at)
        .filter(|expires_at| *expires_at > now)
        .min()
        .map(|expires_at| Duration::from_secs(expires_at - now))
}

/// The runtime spec for node `node_id`: `settings` as replicated under
/// `config/containerd`, plus the node's container network. `None` when
/// `settings` is `None` (`InitCluster` has not replicated): nothing to apply
/// yet.
///
/// The network is the bridge [`POD_BRIDGE`] on the subnet `mesh.nodes` holds
/// for this node, with the tunnel's MTU. It is `None` while the node has no
/// subnet there, or one that does not fit the range ([`subnet_problem`];
/// [`desired_network`] warns about it), and the runtime then removes its
/// network config.
pub fn desired_runtime(
    settings: Option<ContainerRuntimeSpec>,
    mesh: Option<&MeshState>,
    node_id: u64,
) -> Option<NodeRuntimeSpec> {
    let subnet = mesh.and_then(|m| own_subnet(m, node_id).ok().flatten());
    settings.map(|settings| NodeRuntimeSpec {
        settings,
        pod_network: subnet.map(|subnet| PodNetworkSpec {
            bridge: POD_BRIDGE.to_string(),
            subnet,
            mtu: WG_MTU,
        }),
    })
}

/// A warning when the directory containerd is told to read CNI configs from
/// (the replicated `settings.cni_conf_dir`) is not the one this node writes
/// its config to (`--cni-conf-dir`): containerd would never see the file.
/// Compared as paths, so a trailing slash makes no difference.
pub fn cni_dir_warning(settings: &ContainerRuntimeSpec, cni_conf_dir: &Path) -> Option<String> {
    if Path::new(&settings.cni_conf_dir) == cni_conf_dir {
        return None;
    }
    Some(format!(
        "containerd reads CNI configs from {} but this node writes its config to {} \
         (--cni-conf-dir); containers get no network until the two match",
        settings.cni_conf_dir,
        cni_conf_dir.display()
    ))
}

/// The two host integrations, each behind its own `reconcile`.
#[derive(Clone)]
pub struct HostBackends {
    /// WireGuard and nftables.
    pub network: NetworkManager,
    /// The container runtime.
    pub runtime: Arc<dyn ContainerRuntime>,
}

/// The network step: `network.reconcile` for the WireGuard and nftables state
/// [`desired_network`] derives.
pub struct NetworkStep {
    network: NetworkManager,
    iface: String,
    identity: Identity,
    join_pending: JoinPending,
    peer_port: u16,
}

impl NetworkStep {
    pub fn new(
        network: NetworkManager,
        iface: String,
        identity: Identity,
        join_pending: JoinPending,
        peer_port: u16,
    ) -> Self {
        Self {
            network,
            iface,
            identity,
            join_pending,
            peer_port,
        }
    }
}

impl ReconcileStep for NetworkStep {
    type Desired = NetworkSpec;

    fn name(&self) -> &'static str {
        "network"
    }

    fn plan(&self, view: &ReplicatedStateView) -> Planned<NetworkSpec> {
        // The copy from startup still has the pending join after the node
        // became a voter. The token's peers must go then.
        let joined;
        let identity = if self.join_pending.is_pending() {
            &self.identity
        } else {
            joined = Identity {
                pending_join: None,
                ..self.identity.clone()
            };
            &joined
        };
        let (desired, warnings) = desired_network(
            identity,
            &self.iface,
            self.peer_port,
            view.mesh.as_ref(),
            view.now,
        );
        Planned { desired, warnings }
    }

    fn apply<'a>(&'a self, desired: &'a NetworkSpec) -> StepFuture<'a> {
        Box::pin(async move { Ok(self.network.reconcile(desired).await?) })
    }
}

/// The runtime configuration step: `runtime.reconcile` for what
/// [`desired_runtime`] derives. Nothing is applied while `InitCluster` has
/// not replicated the settings.
pub struct RuntimeStep {
    runtime: Arc<dyn ContainerRuntime>,
    node_id: u64,
    cni_conf_dir: PathBuf,
}

impl RuntimeStep {
    pub fn new(runtime: Arc<dyn ContainerRuntime>, node_id: u64, cni_conf_dir: PathBuf) -> Self {
        Self {
            runtime,
            node_id,
            cni_conf_dir,
        }
    }
}

impl ReconcileStep for RuntimeStep {
    type Desired = Option<NodeRuntimeSpec>;

    fn name(&self) -> &'static str {
        "runtime"
    }

    fn plan(&self, view: &ReplicatedStateView) -> Planned<Option<NodeRuntimeSpec>> {
        let warnings = view
            .containerd
            .as_ref()
            .and_then(|settings| cni_dir_warning(settings, &self.cni_conf_dir))
            .into_iter()
            .collect();
        Planned {
            desired: desired_runtime(view.containerd.clone(), view.mesh.as_ref(), self.node_id),
            warnings,
        }
    }

    fn apply<'a>(&'a self, desired: &'a Option<NodeRuntimeSpec>) -> StepFuture<'a> {
        Box::pin(async move {
            if let Some(spec) = desired
                && self.runtime.reconcile(spec).await?
            {
                tracing::info!("container runtime configuration updated");
            }
            Ok(())
        })
    }
}

/// One node's host reconciliation: where the desired state comes from and
/// what applies it.
#[derive(Clone)]
pub struct HostReconciler {
    pub backends: HostBackends,
    /// WireGuard interface name.
    pub iface: String,
    /// This node's own WireGuard settings and, for a joiner, its token's
    /// bootstrap peers. A copy made at startup: `join_pending` says whether
    /// its `pending_join` still counts.
    pub identity: Identity,
    /// Whether this node is still joining. Read on every plan, so the
    /// token's peers go with the first plan after the join completes.
    pub join_pending: JoinPending,
    /// TCP port of the peer listener, for the firewall.
    pub peer_port: u16,
    /// Where this host's runtime backend writes the CNI config
    /// (`--cni-conf-dir`). Only compared with the replicated setting, see
    /// [`cni_dir_warning`].
    pub cni_conf_dir: PathBuf,
    /// The clocks of the lease task: how stale a node's report can look
    /// though the node is on time ([`LeaseTiming::tick_clock_secs`]).
    pub lease_timing: LeaseTiming,
    pub reader: StateReader,
}

impl HostReconciler {
    /// The replicated state as it is now. Every step plans from the same
    /// view, so the route, the firewall and the CNI config agree on this
    /// node's container subnet.
    pub fn view(&self) -> Result<ReplicatedStateView, ReadError> {
        ReplicatedStateView::read(&self.reader, now_secs())
    }

    /// Spawns the one host reconcile loop over `runner`'s steps (see
    /// [`crate::host_steps`]). `applied`
    /// is the desired state startup already brought up through `runner`; the
    /// loop does not apply it a second time.
    pub fn spawn(
        self,
        timing: ReconcileTiming,
        runner: StepRunner,
        applied: Vec<StepValue>,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let changes = self.reader.subscribe();
        spawn_steps(
            "host",
            changes,
            timing,
            runner,
            Some(applied),
            move || self.view(),
            shutdown,
        )
    }
}

/// One wg peer candidate, borrowed from `mesh` or the bootstrap list.
struct PeerSource<'a> {
    label: String,
    public_key: &'a str,
    endpoint: Option<&'a str>,
    mesh_ip: Ipv4Addr,
    /// A node in `mesh.nodes`, as opposed to an invite or a bootstrap peer.
    registered: bool,
    /// The subnet in the node's record, not validated. Always `None` for
    /// invites and bootstrap peers.
    container_subnet: Option<Ipv4Net>,
}

/// Every peer except `self_id`, one per mesh IP and per public key (nodes
/// before invites before `bootstrap` peers, lowest node ID first within
/// each), sorted by mesh IP. Peers with a malformed public key are dropped
/// with a warning: the key is written into the wg config text.
///
/// `bootstrap` holds the peers of this node's join token while its join is
/// pending, and is empty otherwise. They are never `registered` and carry
/// no subnet, whatever the nodes behind them hold by now.
fn peer_set<'a>(
    self_id: u64,
    mesh: Option<&'a MeshState>,
    bootstrap: &'a [TokenPeer],
    now: u64,
) -> (Vec<PeerSource<'a>>, Vec<String>) {
    let mut all: Vec<PeerSource<'a>> = Vec::new();
    if let Some(m) = mesh {
        all.extend(
            m.nodes
                .values()
                .filter(|n| n.node_id != self_id)
                .map(|n| PeerSource {
                    label: format!("node {}", n.node_id),
                    public_key: &n.public_key,
                    endpoint: n.endpoint.as_deref(),
                    mesh_ip: n.mesh_ip,
                    registered: true,
                    container_subnet: n.container_subnet,
                }),
        );
        let mut invites: Vec<_> = m
            .invites
            .values()
            .filter(|i| {
                i.node_id != self_id && i.expires_at > now && !m.nodes.contains_key(&i.node_id)
            })
            .collect();
        invites.sort_by_key(|i| i.node_id);
        all.extend(invites.into_iter().map(|i| PeerSource {
            label: format!("invite for node {}", i.node_id),
            public_key: &i.public_key,
            endpoint: Some(i.endpoint.as_str()),
            mesh_ip: i.mesh_ip,
            registered: false,
            container_subnet: None,
        }));
    }
    let replicated = all.len();
    let mut peers: Vec<_> = bootstrap.iter().filter(|p| p.node_id != self_id).collect();
    peers.sort_by_key(|p| p.node_id);
    all.extend(peers.into_iter().map(|p| PeerSource {
        label: format!("bootstrap peer {}", p.node_id),
        public_key: &p.public_key,
        endpoint: p.endpoint.as_deref(),
        mesh_ip: p.mesh_ip,
        registered: false,
        container_subnet: None,
    }));

    let mut warnings = Vec::new();
    let mut by_ip: BTreeMap<Ipv4Addr, PeerSource<'a>> = BTreeMap::new();
    let mut keys: BTreeSet<&str> = BTreeSet::new();
    for (i, p) in all.into_iter().enumerate() {
        if validate_public_key(p.public_key).is_err() {
            warnings.push(format!("{}: malformed public key; peer skipped", p.label));
            continue;
        }
        // WireGuard keeps one peer per key. A token peer whose key the
        // replicated state already has is that node under an older address.
        let from_token = i >= replicated;
        if from_token && keys.contains(p.public_key) {
            continue;
        }
        if let std::collections::btree_map::Entry::Vacant(slot) = by_ip.entry(p.mesh_ip) {
            keys.insert(p.public_key);
            slot.insert(p);
        }
    }
    (by_ip.into_values().collect(), warnings)
}

/// Loop timing. [`Default`] is a 300s resync, 1s..30s backoff and the
/// [`DEBOUNCE_QUIET`]/[`DEBOUNCE_MAX`] debounce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileTiming {
    pub resync: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Quiet period that ends a burst of state changes.
    pub debounce_quiet: Duration,
    /// Longest wait from the first change of a burst.
    pub debounce_max: Duration,
}

impl Default for ReconcileTiming {
    fn default() -> Self {
        Self {
            resync: Duration::from_secs(DEFAULT_RECONCILE_INTERVAL_SECS),
            backoff_min: BACKOFF_MIN,
            backoff_max: BACKOFF_MAX,
            debounce_quiet: DEBOUNCE_QUIET,
            debounce_max: DEBOUNCE_MAX,
        }
    }
}

impl ReconcileTiming {
    /// [`Default`] with the resync every `resync`.
    pub fn with_resync(resync: Duration) -> Self {
        Self {
            resync,
            ..Self::default()
        }
    }

    /// Delay before retry number `failures` (1-based): `backoff_min` doubled
    /// per extra failure, capped at `backoff_max`.
    pub fn backoff(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(31);
        self.backoff_min
            .saturating_mul(1u32 << doublings)
            .min(self.backoff_max)
    }
}

/// [`run_loop`] for loops whose `apply` ignores the resync flag.
#[cfg(test)]
async fn run_reconciler<D, C, A, F>(
    name: &'static str,
    changes: watch::Receiver<u64>,
    timing: ReconcileTiming,
    applied: Option<D>,
    compute: C,
    apply: A,
    shutdown: CancellationToken,
) where
    D: Clone + PartialEq + Send + 'static,
    C: Fn() -> (Option<D>, Option<Duration>) + Send + 'static,
    A: Fn(D) -> F + Send + 'static,
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    run_loop(
        name,
        changes,
        timing,
        applied,
        compute,
        move |desired, _resync| apply(desired),
        shutdown,
    )
    .await;
}

/// A reconcile loop named `name` (used in logs). It is generic over the
/// desired state so that its timing and failure handling are tested without a
/// host; [`HostReconciler::spawn`] is the one production caller.
///
/// It runs `compute` at start, after each burst of `changes` ticks (debounced:
/// once `debounce_quiet` passes without a tick, or `debounce_max` after the
/// first), on every resync tick, after a failure's backoff, and when the
/// time `compute` last asked for has passed. `compute` returns the desired
/// state (`None`: nothing to apply yet) and, if that state will change with
/// no `changes` tick, how long until it does; a zero wait would spin, so it
/// must be positive.
///
/// `apply` runs only when the computed value differs from the last
/// successfully applied one, or on the resync tick. `applied` seeds that
/// last applied value: with it, the first `apply` waits for a difference or
/// a resync; without it, the first computed state is applied.
///
/// A failed (or panicking) `apply` or a panicking `compute` is logged and
/// retried after [`ReconcileTiming::backoff`]; ticks during the backoff do
/// not cut it short. The loop ends when `shutdown` is cancelled or the
/// `changes` sender is dropped (storage is closed, so there is nothing left
/// to read); an `apply` already running is allowed to finish first.
///
/// `apply` also learns whether the pass was forced by a resync (or is the
/// first one), so that it can re-apply what has not changed on those passes
/// only.
async fn run_loop<D, C, A, F>(
    name: &'static str,
    mut changes: watch::Receiver<u64>,
    timing: ReconcileTiming,
    applied: Option<D>,
    compute: C,
    apply: A,
    shutdown: CancellationToken,
) where
    D: Clone + PartialEq + Send + 'static,
    C: Fn() -> (Option<D>, Option<Duration>) + Send + 'static,
    A: Fn(D, bool) -> F + Send + 'static,
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let mut force = applied.is_none();
    let mut last_applied: Option<D> = applied;
    let mut failures: u32 = 0;
    let mut resync = tokio::time::interval_at(Instant::now() + timing.resync, timing.resync);
    resync.set_missed_tick_behavior(MissedTickBehavior::Delay);
    changes.borrow_and_update();

    while !shutdown.is_cancelled() {
        let (desired, next_change) = match std::panic::catch_unwind(AssertUnwindSafe(&compute)) {
            Ok((desired, next_change)) => (Ok(desired), next_change),
            Err(_) => (Err(()), None),
        };
        let wake_at = next_change.map(|wait| Instant::now() + wait);
        match desired {
            Err(()) => {
                failures += 1;
                tracing::error!(
                    reconciler = name,
                    failures,
                    "computing desired state panicked"
                );
            }
            Ok(None) => {
                failures = 0;
                tracing::debug!(reconciler = name, "nothing to apply yet");
            }
            Ok(Some(desired)) if force || last_applied.as_ref() != Some(&desired) => {
                // Run on its own task so a panic in `apply` is contained.
                match tokio::spawn(apply(desired.clone(), force)).await {
                    Ok(Ok(())) => {
                        if failures > 0 {
                            tracing::info!(reconciler = name, "applied after {failures} failures");
                        } else {
                            tracing::debug!(reconciler = name, "applied");
                        }
                        failures = 0;
                        last_applied = Some(desired);
                    }
                    Ok(Err(e)) => {
                        failures += 1;
                        last_applied = None;
                        tracing::warn!(reconciler = name, failures, error = %format!("{e:#}"), "apply failed");
                    }
                    Err(e) => {
                        failures += 1;
                        last_applied = None;
                        tracing::error!(reconciler = name, failures, error = %e, "apply panicked");
                    }
                }
            }
            // Unchanged. A compute panic before this one is over, so the
            // loop must leave its backoff and wait for triggers again.
            Ok(Some(_)) => failures = 0,
        }
        force = false;

        if failures > 0 {
            let delay = timing.backoff(failures);
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(delay) => continue,
            }
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            changed = changes.changed() => {
                if changed.is_err() || !debounce(&mut changes, &timing, &shutdown).await {
                    break;
                }
            }
            _ = resync.tick() => force = true,
            _ = sleep_until_due(wake_at) => {}
        }
    }
    tracing::debug!(reconciler = name, "stopped");
}

/// Sleeps until `at`, or forever when there is nothing due.
async fn sleep_until_due(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Swallows further change ticks until `debounce_quiet` passes without one,
/// or `debounce_max` after the call. Returns `false` if `shutdown` fired or
/// the change feed closed.
pub(crate) async fn debounce(
    changes: &mut watch::Receiver<u64>,
    timing: &ReconcileTiming,
    shutdown: &CancellationToken,
) -> bool {
    let cap = Instant::now() + timing.debounce_max;
    loop {
        let quiet_until = (Instant::now() + timing.debounce_quiet).min(cap);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return false,
            _ = tokio::time::sleep_until(quiet_until) => return true,
            changed = changes.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::IpAddr;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use faber_container_runtime::Containerd;
    use faber_container_runtime::containerd::DROP_IN_FILE;
    use faber_network_management::CommandOutput;
    use faber_network_management::fake::RecordingRunner;
    use tokio::sync::mpsc;
    use tokio::time::{Instant, timeout};

    use super::*;
    use crate::mesh::state::{
        DEFAULT_LEASE_SECS, InviteRecord, MeshConfig, NodeRecord, WG_KEEPALIVE_SECS,
    };
    use crate::mesh::{PendingJoin, PrivateKey};
    use crate::reconcile::step::StepList;

    // ---- derivation ----

    fn key(id: u64) -> String {
        STANDARD.encode([id as u8; 32])
    }

    fn node(id: u64, endpoint: Option<&str>) -> NodeRecord {
        NodeRecord {
            node_id: id,
            public_key: key(id),
            mesh_ip: Ipv4Addr::new(10, 99, 0, id as u8),
            endpoint: endpoint.map(str::to_string),
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: format!("node-{id}"),
            lease_renewed_at: 0,
        }
    }

    fn invite(id: u64, endpoint: &str, expires_at: u64) -> InviteRecord {
        InviteRecord {
            invite_id: format!("{id:032x}"),
            node_id: id,
            public_key: key(id),
            mesh_ip: Ipv4Addr::new(10, 99, 0, id as u8),
            endpoint: endpoint.into(),
            expires_at,
            name: format!("node-{id}"),
        }
    }

    fn mesh(nodes: Vec<NodeRecord>, invites: Vec<InviteRecord>) -> MeshState {
        MeshState {
            config: MeshConfig::default(),
            next_node_id: 100,
            nodes: nodes.into_iter().map(|n| (n.node_id, n)).collect(),
            invites: invites
                .into_iter()
                .map(|i| (i.invite_id.clone(), i))
                .collect::<BTreeMap<_, _>>(),
            clock: 0,
            lease_secs: DEFAULT_LEASE_SECS,
        }
    }

    fn local(id: u64) -> Identity {
        Identity {
            v: 1,
            node_id: id,
            private_key: PrivateKey::new(STANDARD.encode([0xAAu8; 32])),
            mesh_ip: Ipv4Addr::new(10, 99, 0, id as u8),
            mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            wg_port: 51820,
            endpoint: format!("203.0.113.{id}:51820").parse().unwrap(),
            instance_id: None,
            pending_join: None,
        }
    }

    /// [`local`] still joining, with `bootstrap_peers` from its token.
    fn joiner(id: u64, bootstrap_peers: Vec<TokenPeer>) -> Identity {
        Identity {
            pending_join: Some(PendingJoin {
                invite_id: "ab".repeat(16),
                join_addrs: vec![],
                bootstrap_peers,
                registered: false,
            }),
            ..local(id)
        }
    }

    /// The WireGuard half of [`desired_network`].
    fn desired_wg(
        identity: &Identity,
        mesh: Option<&MeshState>,
        now: u64,
    ) -> (WireGuardSpec, Vec<String>) {
        let (network, warnings) = desired_network(identity, "faber0", 7401, mesh, now);
        assert_eq!(network.interface, "faber0");
        (network.wireguard, warnings)
    }

    /// The firewall half of [`desired_network`].
    fn desired_firewall(
        identity: &Identity,
        mesh: Option<&MeshState>,
        now: u64,
    ) -> (FirewallSpec, Vec<String>) {
        let (network, warnings) = desired_network(identity, "faber0", 7401, mesh, now);
        (network.firewall, warnings)
    }

    /// Every peer here has one `/32` in its AllowedIPs; the summary gives its
    /// address.
    fn peer_summary(d: &WireGuardSpec) -> Vec<(String, Option<String>, Ipv4Addr, u16)> {
        d.peers
            .iter()
            .map(|p| {
                assert_eq!(p.allowed_ips.len(), 1, "{p:?}");
                assert_eq!(p.allowed_ips[0].prefix_len(), 32, "{p:?}");
                (
                    p.public_key.clone(),
                    p.endpoint.clone(),
                    p.allowed_ips[0].addr(),
                    p.keepalive,
                )
            })
            .collect()
    }

    #[test]
    fn desired_wg_from_mesh_excludes_self_and_expired_invites() {
        let m = mesh(
            vec![
                node(3, Some("203.0.113.3:51820")),
                node(1, Some("203.0.113.1:51820")),
                node(2, Some("203.0.113.2:51820")),
            ],
            vec![
                invite(5, "198.51.100.5:51820", 1001),
                invite(4, "198.51.100.4:51820", 1000),
            ],
        );
        let (d, warnings) = desired_wg(&local(2), Some(&m), 1000);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(d.private_key, local(2).private_key.expose());
        assert_eq!(d.listen_port, 51820);
        assert_eq!(d.address, "10.99.0.2/16".parse::<Ipv4Net>().unwrap());
        assert_eq!(
            peer_summary(&d),
            vec![
                (
                    key(1),
                    Some("203.0.113.1:51820".into()),
                    "10.99.0.1".parse().unwrap(),
                    WG_KEEPALIVE_SECS
                ),
                (
                    key(3),
                    Some("203.0.113.3:51820".into()),
                    "10.99.0.3".parse().unwrap(),
                    WG_KEEPALIVE_SECS
                ),
                (
                    key(5),
                    Some("198.51.100.5:51820".into()),
                    "10.99.0.5".parse().unwrap(),
                    WG_KEEPALIVE_SECS
                ),
            ]
        );
    }

    #[test]
    fn desired_wg_node_without_endpoint_is_still_a_peer() {
        let m = mesh(vec![node(1, None), node(2, None)], vec![]);
        let (d, _) = desired_wg(&local(2), Some(&m), 0);
        assert_eq!(
            peer_summary(&d),
            vec![(key(1), None, "10.99.0.1".parse().unwrap(), 25)]
        );
    }

    #[test]
    fn desired_wg_excludes_own_pending_invite() {
        // A joiner that has replicated `mesh` before registering sees its own
        // invite.
        let m = mesh(
            vec![node(1, Some("203.0.113.1:51820"))],
            vec![invite(2, "198.51.100.2:51820", 5000)],
        );
        let (d, _) = desired_wg(&local(2), Some(&m), 10);
        assert_eq!(d.peers.len(), 1);
        assert_eq!(d.peers[0].public_key, key(1));
    }

    #[test]
    fn desired_wg_node_wins_over_invite_with_same_ip() {
        let mut inv = invite(3, "198.51.100.9:51820", 5000);
        inv.public_key = key(9);
        let m = mesh(vec![node(3, Some("203.0.113.3:51820"))], vec![inv]);
        let (d, _) = desired_wg(&local(1), Some(&m), 10);
        assert_eq!(d.peers.len(), 1);
        assert_eq!(d.peers[0].public_key, key(3));
    }

    #[test]
    fn desired_wg_uses_bootstrap_without_mesh() {
        let bootstrap = vec![
            TokenPeer {
                node_id: 3,
                public_key: key(3),
                endpoint: None,
                mesh_ip: "10.99.0.3".parse().unwrap(),
            },
            TokenPeer {
                node_id: 1,
                public_key: key(1),
                endpoint: Some("203.0.113.1:51820".into()),
                mesh_ip: "10.99.0.1".parse().unwrap(),
            },
            TokenPeer {
                node_id: 2,
                public_key: key(2),
                endpoint: None,
                mesh_ip: "10.99.0.2".parse().unwrap(),
            },
        ];
        let (d, warnings) = desired_wg(&joiner(2, bootstrap.clone()), None, 0);
        assert!(warnings.is_empty());
        assert_eq!(
            peer_summary(&d),
            vec![
                (
                    key(1),
                    Some("203.0.113.1:51820".into()),
                    "10.99.0.1".parse().unwrap(),
                    25
                ),
                (key(3), None, "10.99.0.3".parse().unwrap(), 25),
            ]
        );
        // With a mesh they stay while the join is pending: the mesh a
        // joiner has applied may be a prefix without the current leader.
        let m = mesh(vec![node(2, None)], vec![]);
        let (d, _) = desired_wg(&joiner(2, bootstrap.clone()), Some(&m), 0);
        assert_eq!(peer_summary(&d).len(), 2);
        // Once the join is done, only the mesh counts.
        let joined = Identity {
            pending_join: None,
            ..joiner(2, bootstrap)
        };
        let (d, _) = desired_wg(&joined, Some(&m), 0);
        assert!(d.peers.is_empty());
    }

    fn token_peer(id: u64, public_key: String, endpoint: &str) -> TokenPeer {
        TokenPeer {
            node_id: id,
            public_key,
            endpoint: Some(endpoint.into()),
            mesh_ip: Ipv4Addr::new(10, 99, 0, id as u8),
        }
    }

    /// A joiner (node 9) has applied a prefix of the log: node 1 with a
    /// newer endpoint than the token has and a container subnet, and an
    /// invite for node 5. Node 3, which leads by now, registered after
    /// that prefix and is known from the token only.
    #[test]
    fn a_pending_joiner_keeps_its_token_peers_next_to_the_mesh() {
        let mut replicated = node_with_subnet(1, "10.200.0.0/24");
        replicated.endpoint = Some("203.0.113.101:51820".into());
        let m = container_mesh(vec![replicated], vec![invite(5, "203.0.113.5:51820", 100)]);
        let bootstrap = vec![
            token_peer(1, key(1), "203.0.113.1:51820"),
            token_peer(3, key(3), "203.0.113.3:51820"),
            // The key of node 1 under another address, and the address of
            // the invite under another key: the replicated records win.
            token_peer(6, key(1), "203.0.113.6:51820"),
            token_peer(5, key(7), "203.0.113.7:51820"),
            // The joiner itself is never its own peer.
            token_peer(9, key(9), "203.0.113.9:51820"),
        ];
        let (network, warnings) =
            desired_network(&joiner(9, bootstrap), "faber0", 7401, Some(&m), 10);
        assert!(warnings.is_empty(), "{warnings:?}");

        // One peer per key and per address. The token peer has its mesh
        // address only, whatever subnet it holds by now.
        assert_eq!(
            allowed_ips(&network.wireguard),
            vec![
                (key(1), nets(&["10.99.0.1/32", "10.200.0.0/24"])),
                (key(3), nets(&["10.99.0.3/32"])),
                (key(5), nets(&["10.99.0.5/32"])),
            ]
        );
        let endpoints: Vec<_> = network
            .wireguard
            .peers
            .iter()
            .map(|p| p.endpoint.clone().unwrap())
            .collect();
        assert_eq!(
            endpoints,
            [
                "203.0.113.101:51820",
                "203.0.113.3:51820",
                "203.0.113.5:51820"
            ]
        );

        // The firewall admits the same three, and only the registered node
        // has a way to the containers.
        let firewall = network.firewall;
        let ips = |last: &[u8]| -> BTreeSet<Ipv4Addr> {
            last.iter().map(|b| Ipv4Addr::new(10, 99, 0, *b)).collect()
        };
        assert_eq!(firewall.peer_mesh_ips, ips(&[1, 3, 5]));
        assert_eq!(
            firewall.peer_endpoint_ips,
            ["203.0.113.101", "203.0.113.3", "203.0.113.5"]
                .iter()
                .map(|ip| ip.parse::<IpAddr>().unwrap())
                .collect()
        );
        assert_eq!(firewall.containers.unwrap().node_mesh_ips, ips(&[1]));
    }

    /// The reconciler's identity is a copy made at startup and still has
    /// the pending join after the node became a voter. The shared flag is
    /// what ends the token peers.
    #[tokio::test]
    async fn the_plan_drops_the_token_peers_when_the_join_completes() {
        use openraft::EntryPayload;
        use openraft::storage::RaftStateMachine;

        use crate::raft::store;
        use crate::raft::types::{Entry, Request};

        let dir = tempfile::tempdir().unwrap();
        let mut storage = store::open(dir.path().join("data")).unwrap();
        let runner = Arc::new(RecordingRunner::new());
        let bootstrap = vec![
            token_peer(1, key(1), "203.0.113.1:51820"),
            token_peer(3, key(3), "203.0.113.3:51820"),
        ];
        let host = HostReconciler {
            backends: backends(&runner, dir.path()),
            iface: "faber0".into(),
            identity: joiner(9, bootstrap),
            join_pending: JoinPending::new(true),
            peer_port: 7401,
            cni_conf_dir: PathBuf::from("/etc/cni/net.d"),
            lease_timing: LeaseTiming::default(),
            reader: storage.reader.clone(),
        };
        let keys = |host: &HostReconciler| -> Vec<String> {
            let plan = plan_of(host);
            let peers = plan.desired.network.wireguard.peers;
            peers.into_iter().map(|p| p.public_key).collect()
        };
        assert_eq!(keys(&host), [key(1), key(3)]);

        // The prefix the joiner applies first has node 1 and nobody else.
        let seed = Entry {
            log_id: openraft::testing::log_id(1, 1, 1),
            payload: EntryPayload::Normal(Request::InitCluster {
                mesh: MeshConfig::default(),
                founder: node(1, Some("203.0.113.1:51820")),
                containerd: ContainerRuntimeSpec::default(),
            }),
        };
        storage.state_machine.apply([seed]).await.unwrap();
        assert_eq!(keys(&host), [key(1), key(3)]);

        host.join_pending.clear();
        assert_eq!(keys(&host), [key(1)]);
    }

    #[test]
    fn desired_wg_skips_bad_keys_and_endpoints_with_warnings() {
        let mut bad_key = node(3, Some("203.0.113.3:51820"));
        bad_key.public_key = "not a key\nPrivateKey = x".into();
        let mut short_key = node(4, None);
        short_key.public_key = STANDARD.encode([1u8; 16]);
        let m = mesh(
            vec![
                node(1, Some("example.com:51820")),
                bad_key,
                short_key,
                node(5, Some("203.0.113.5:51820\n[Peer]")),
            ],
            vec![],
        );
        let (d, warnings) = desired_wg(&local(2), Some(&m), 0);
        assert_eq!(
            peer_summary(&d),
            vec![
                (key(1), None, "10.99.0.1".parse().unwrap(), 25),
                (key(5), None, "10.99.0.5".parse().unwrap(), 25),
            ]
        );
        assert_eq!(warnings.len(), 4, "{warnings:?}");
        assert!(warnings.iter().all(|w| !w.contains('\n')), "{warnings:?}");
    }

    #[test]
    fn desired_wg_is_deterministic() {
        let m = mesh(
            vec![node(4, None), node(1, None), node(9, None)],
            vec![
                invite(7, "198.51.100.7:1", 99),
                invite(6, "198.51.100.6:1", 99),
            ],
        );
        let a = desired_wg(&local(1), Some(&m), 0).0;
        let b = desired_wg(&local(1), Some(&m.clone()), 0).0;
        assert_eq!(a, b);
        let ips: Vec<_> = a
            .peers
            .iter()
            .map(|p| p.allowed_ips[0].addr().octets()[3])
            .collect();
        assert_eq!(ips, vec![4, 6, 7, 9]);
    }

    #[test]
    fn desired_firewall_from_mesh() {
        let m = mesh(
            vec![
                node(1, Some("203.0.113.1:51820")),
                node(2, Some("203.0.113.2:51820")),
                node(3, None),
                node(4, Some("[2001:db8::4]:51820")),
            ],
            vec![
                invite(5, "198.51.100.5:51820", 1001),
                invite(6, "198.51.100.6:51820", 1000),
            ],
        );
        let (spec, warnings) = desired_firewall(&local(2), Some(&m), 1000);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(spec.wg_port, 51820);
        assert_eq!(spec.peer_port, 7401);
        let ips: Vec<IpAddr> = ["198.51.100.5", "203.0.113.1", "2001:db8::4"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        assert_eq!(spec.peer_endpoint_ips.into_iter().collect::<Vec<_>>(), ips);
        let mesh_ips: Vec<Ipv4Addr> = ["10.99.0.1", "10.99.0.3", "10.99.0.4", "10.99.0.5"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        assert_eq!(spec.peer_mesh_ips.into_iter().collect::<Vec<_>>(), mesh_ips);
    }

    #[test]
    fn desired_firewall_from_bootstrap_with_warnings() {
        let bootstrap = vec![
            TokenPeer {
                node_id: 1,
                public_key: key(1),
                endpoint: Some("gw.example:51820".into()),
                mesh_ip: "10.99.0.1".parse().unwrap(),
            },
            TokenPeer {
                node_id: 3,
                public_key: key(3),
                endpoint: Some("203.0.113.3:51820".into()),
                mesh_ip: "10.99.0.3".parse().unwrap(),
            },
        ];
        let (spec, warnings) = desired_firewall(&joiner(2, bootstrap), None, 0);
        assert_eq!(
            spec.peer_endpoint_ips.into_iter().collect::<Vec<_>>(),
            vec!["203.0.113.3".parse::<IpAddr>().unwrap()]
        );
        assert_eq!(spec.peer_mesh_ips.len(), 2);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("gw.example:51820"), "{warnings:?}");
    }

    // ---- container subnets ----

    const RANGE: &str = "10.200.0.0/16";

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    /// [`node`] holding the container subnet `subnet`.
    fn node_with_subnet(id: u64, subnet: &str) -> NodeRecord {
        NodeRecord {
            container_subnet: Some(net(subnet)),
            ..node(id, Some(&format!("203.0.113.{id}:51820")))
        }
    }

    /// [`mesh`] with the container range [`RANGE`], cut into /24s.
    fn container_mesh(nodes: Vec<NodeRecord>, invites: Vec<InviteRecord>) -> MeshState {
        let mut m = mesh(nodes, invites);
        m.config.container_cidr = Some(net(RANGE));
        m
    }

    /// Every peer's public key with its AllowedIPs, as text.
    fn allowed_ips(d: &WireGuardSpec) -> Vec<(String, Vec<String>)> {
        d.peers
            .iter()
            .map(|p| {
                let nets = p.allowed_ips.iter().map(Ipv4Net::to_string).collect();
                (p.public_key.clone(), nets)
            })
            .collect()
    }

    fn nets(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_node_peer_carries_its_container_subnet() {
        let m = container_mesh(
            vec![
                node_with_subnet(1, "10.200.0.0/24"),
                node_with_subnet(2, "10.200.1.0/24"),
                node_with_subnet(3, "10.200.2.0/24"),
            ],
            vec![],
        );
        let (d, warnings) = desired_wg(&local(2), Some(&m), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            allowed_ips(&d),
            vec![
                (key(1), nets(&["10.99.0.1/32", "10.200.0.0/24"])),
                (key(3), nets(&["10.99.0.3/32", "10.200.2.0/24"])),
            ]
        );
        // The whole range goes into the tunnel; AllowedIPs picks the peer.
        assert_eq!(d.routes, vec![net(RANGE)]);
    }

    #[test]
    fn an_invite_peer_gets_only_its_mesh_address() {
        let m = container_mesh(
            vec![
                node_with_subnet(1, "10.200.0.0/24"),
                node_with_subnet(2, "10.200.1.0/24"),
            ],
            vec![invite(3, "198.51.100.3:51820", 5000)],
        );
        let (d, warnings) = desired_wg(&local(1), Some(&m), 10);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            allowed_ips(&d),
            vec![
                (key(2), nets(&["10.99.0.2/32", "10.200.1.0/24"])),
                (key(3), nets(&["10.99.0.3/32"])),
            ]
        );
    }

    #[test]
    fn a_bootstrap_peer_gets_only_its_mesh_address() {
        let bootstrap = vec![TokenPeer {
            node_id: 1,
            public_key: key(1),
            endpoint: Some("203.0.113.1:51820".into()),
            mesh_ip: "10.99.0.1".parse().unwrap(),
        }];
        let (network, warnings) = desired_network(&joiner(2, bootstrap), "faber0", 7401, None, 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            allowed_ips(&network.wireguard),
            vec![(key(1), nets(&["10.99.0.1/32"]))]
        );
        // No replicated state yet: no route and no container rules.
        assert!(network.wireguard.routes.is_empty());
        assert_eq!(network.firewall.containers, None);
    }

    #[test]
    fn a_peer_subnet_that_cannot_be_routed_is_skipped_with_a_warning() {
        for (subnet, why) in [
            ("10.201.0.0/24", "outside the container range"),
            ("10.200.4.0/25", "is not a /24"),
            ("10.200.0.0/16", "is not a /24"),
            ("10.200.4.7/24", "host bits"),
            ("10.200.1.0/24", "this node's own subnet"),
        ] {
            let m = container_mesh(
                vec![
                    node_with_subnet(1, "10.200.0.0/24"),
                    node_with_subnet(2, "10.200.1.0/24"),
                    node_with_subnet(3, subnet),
                ],
                vec![],
            );
            let (network, warnings) = desired_network(&local(2), "faber0", 7401, Some(&m), 0);
            assert_eq!(
                allowed_ips(&network.wireguard),
                vec![
                    (key(1), nets(&["10.99.0.1/32", "10.200.0.0/24"])),
                    (key(3), nets(&["10.99.0.3/32"])),
                ],
                "{subnet}"
            );
            assert_eq!(warnings.len(), 1, "{subnet}: {warnings:?}");
            assert!(warnings[0].contains("node 3"), "{subnet}: {warnings:?}");
            assert!(warnings[0].contains(subnet), "{subnet}: {warnings:?}");
            assert!(warnings[0].contains(why), "{subnet}: {warnings:?}");
            // Still a node: it keeps its mesh address and its way in.
            let containers = network.firewall.containers.unwrap();
            assert!(
                containers
                    .node_mesh_ips
                    .contains(&Ipv4Addr::new(10, 99, 0, 3))
            );
            assert_eq!(containers.local_subnet, Some(net("10.200.1.0/24")));
        }
    }

    /// WireGuard moves an AllowedIP that two peers list to the last of them,
    /// without an error. The first peer in the (mesh IP) order keeps it.
    #[test]
    fn a_subnet_two_peers_claim_goes_to_the_first_of_them() {
        let m = container_mesh(
            vec![
                node_with_subnet(4, "10.200.7.0/24"),
                node_with_subnet(1, "10.200.0.0/24"),
                node_with_subnet(3, "10.200.7.0/24"),
            ],
            vec![],
        );
        let (d, warnings) = desired_wg(&local(1), Some(&m), 0);
        assert_eq!(
            allowed_ips(&d),
            vec![
                (key(3), nets(&["10.99.0.3/32", "10.200.7.0/24"])),
                (key(4), nets(&["10.99.0.4/32"])),
            ]
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("node 4"), "{warnings:?}");
        assert!(warnings[0].contains("node 3"), "{warnings:?}");
        assert!(warnings[0].contains("10.200.7.0/24"), "{warnings:?}");
    }

    #[test]
    fn container_firewall_admits_registered_nodes_only() {
        let m = container_mesh(
            vec![
                node_with_subnet(1, "10.200.0.0/24"),
                node_with_subnet(2, "10.200.1.0/24"),
                node_with_subnet(3, "10.200.2.0/24"),
            ],
            vec![
                invite(5, "198.51.100.5:51820", 1001),
                invite(6, "198.51.100.6:51820", 1000),
            ],
        );
        let (spec, warnings) = desired_firewall(&local(2), Some(&m), 1000);
        assert!(warnings.is_empty(), "{warnings:?}");
        let ips = |list: &[&str]| -> BTreeSet<Ipv4Addr> {
            list.iter().map(|s| s.parse().unwrap()).collect()
        };
        // The live invite is a WireGuard peer ...
        assert_eq!(
            spec.peer_mesh_ips,
            ips(&["10.99.0.1", "10.99.0.3", "10.99.0.5"])
        );
        // ... but has no way to the containers, and neither has this node's
        // own address.
        assert_eq!(
            spec.containers,
            Some(ContainerFirewall {
                cidr: net(RANGE),
                local_subnet: Some(net("10.200.1.0/24")),
                node_mesh_ips: ips(&["10.99.0.1", "10.99.0.3"]),
                bridge: "faberbr0".into(),
            })
        );
    }

    /// A lease that has run out changes nothing on the host. The node stays
    /// a WireGuard peer with its subnet until `RemoveNode` deletes its
    /// record: its ack may be needed to commit its own removal from raft.
    #[test]
    fn an_expired_node_stays_a_peer_until_it_is_removed() {
        let mut m = container_mesh(
            vec![
                node_with_subnet(1, "10.200.0.0/24"),
                node_with_subnet(2, "10.200.1.0/24"),
                node_with_subnet(3, "10.200.2.0/24"),
            ],
            vec![],
        );
        // Nodes 1 and 2 renewed; node 3 did not.
        m.clock = DEFAULT_LEASE_SECS + 1;
        for id in [1, 2] {
            m.nodes.get_mut(&id).unwrap().lease_renewed_at = m.clock;
        }
        assert!(m.lease_expired(&m.nodes[&3]));
        assert!(!m.lease_expired(&m.nodes[&2]));

        let both = vec![
            (key(2), nets(&["10.99.0.2/32", "10.200.1.0/24"])),
            (key(3), nets(&["10.99.0.3/32", "10.200.2.0/24"])),
        ];
        let (d, warnings) = desired_wg(&local(1), Some(&m), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(allowed_ips(&d), both);
        let (spec, _) = desired_firewall(&local(1), Some(&m), 0);
        let node_ips = spec.containers.unwrap().node_mesh_ips;
        assert!(
            node_ips.contains(&Ipv4Addr::new(10, 99, 0, 3)),
            "{node_ips:?}"
        );

        // Still a raft member: `RemoveNode` is refused and nothing changes.
        let members: BTreeSet<u64> = [1, 2, 3].into();
        assert!(m.remove_node(3, &members).is_err());
        assert_eq!(allowed_ips(&desired_wg(&local(1), Some(&m), 0).0), both);

        // Out of raft, then out of the mesh: the peer and its subnet go.
        let members: BTreeSet<u64> = [1, 2].into();
        m.remove_node(3, &members).unwrap();
        let (d, warnings) = desired_wg(&local(1), Some(&m), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            allowed_ips(&d),
            vec![(key(2), nets(&["10.99.0.2/32", "10.200.1.0/24"]))]
        );
        let (spec, _) = desired_firewall(&local(1), Some(&m), 0);
        assert_eq!(
            spec.peer_mesh_ips,
            BTreeSet::from([Ipv4Addr::new(10, 99, 0, 2)])
        );
        let node_ips = spec.containers.unwrap().node_mesh_ips;
        assert_eq!(node_ips, BTreeSet::from([Ipv4Addr::new(10, 99, 0, 2)]));
    }

    /// A takeover moves the old node's subnet to the new node in one change
    /// of state, so on every plan exactly one peer carries it.
    #[test]
    fn after_a_takeover_only_the_new_node_carries_the_subnet() {
        let taken = "10.200.2.0/24";
        let mut m = container_mesh(
            vec![
                node_with_subnet(1, "10.200.0.0/24"),
                node_with_subnet(2, "10.200.1.0/24"),
                node_with_subnet(3, taken),
            ],
            // An invite for a new machine (node 4) that names node 3.
            vec![InviteRecord {
                name: "node-3".into(),
                ..invite(4, "198.51.100.4:51820", 5000)
            }],
        );
        let invite_id = m.invites.keys().next().unwrap().clone();

        // Before it registers: node 3 has the subnet, the invite has none.
        let (d, warnings) = desired_wg(&local(1), Some(&m), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            allowed_ips(&d),
            vec![
                (key(2), nets(&["10.99.0.2/32", "10.200.1.0/24"])),
                (key(3), nets(&["10.99.0.3/32", taken])),
                (key(4), nets(&["10.99.0.4/32"])),
            ]
        );

        // Node 3 has left raft, and the new machine registers.
        let members: BTreeSet<u64> = [1, 2].into();
        m.register_node(
            &invite_id,
            4,
            "198.51.100.4:51820",
            &"ab".repeat(16),
            0,
            &members,
        )
        .unwrap();
        assert!(!m.nodes.contains_key(&3));

        // Seen from an old node and from the new one.
        let (d, warnings) = desired_wg(&local(1), Some(&m), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            allowed_ips(&d),
            vec![
                (key(2), nets(&["10.99.0.2/32", "10.200.1.0/24"])),
                (key(4), nets(&["10.99.0.4/32", taken])),
            ]
        );
        let carriers = |d: &WireGuardSpec| {
            d.peers
                .iter()
                .filter(|p| p.allowed_ips.contains(&net(taken)))
                .count()
        };
        assert_eq!(carriers(&d), 1);
        let (d, warnings) = desired_wg(&local(4), Some(&m), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(carriers(&d), 0);
        let (spec, _) = desired_firewall(&local(4), Some(&m), 0);
        assert_eq!(spec.containers.unwrap().local_subnet, Some(net(taken)));
    }

    /// The subnet is read from the replicated record of this node on every
    /// plan, so it follows the state and needs nothing in `identity.json`.
    #[test]
    fn this_nodes_subnet_comes_from_its_record() {
        let m = |own: NodeRecord| {
            container_mesh(vec![node_with_subnet(1, "10.200.0.0/24"), own], vec![])
        };
        let local_subnet = |m: &MeshState| {
            let (spec, warnings) = desired_firewall(&local(2), Some(m), 0);
            (spec.containers.unwrap().local_subnet, warnings)
        };

        let (subnet, warnings) = local_subnet(&m(node_with_subnet(2, "10.200.9.0/24")));
        assert_eq!(subnet, Some(net("10.200.9.0/24")));
        assert!(warnings.is_empty(), "{warnings:?}");

        // A record without a subnet (a node registered before containers
        // were networked): the range rules without the local ones.
        let (subnet, warnings) = local_subnet(&m(node(2, None)));
        assert_eq!(subnet, None);
        assert!(warnings.is_empty(), "{warnings:?}");

        // Not registered yet: the same.
        let only_founder = container_mesh(vec![node_with_subnet(1, "10.200.0.0/24")], vec![]);
        let (subnet, warnings) = local_subnet(&only_founder);
        assert_eq!(subnet, None);
        assert!(warnings.is_empty(), "{warnings:?}");

        // A subnet that does not fit the range is not used.
        for bad in ["10.201.0.0/24", "10.200.9.0/25", "10.200.9.3/24"] {
            let (subnet, warnings) = local_subnet(&m(node_with_subnet(2, bad)));
            assert_eq!(subnet, None, "{bad}");
            assert_eq!(warnings.len(), 1, "{bad}: {warnings:?}");
            assert!(warnings[0].contains("node 2"), "{bad}: {warnings:?}");
            assert!(warnings[0].contains(bad), "{bad}: {warnings:?}");
        }
    }

    #[test]
    fn a_mesh_without_a_container_range_gets_no_container_network() {
        let m = mesh(
            vec![
                node(1, Some("203.0.113.1:51820")),
                node(2, Some("203.0.113.2:51820")),
            ],
            vec![invite(3, "198.51.100.3:51820", 5000)],
        );
        let (network, warnings) = desired_network(&local(2), "faber0", 7401, Some(&m), 10);
        assert!(warnings.is_empty(), "{warnings:?}");
        let id = local(2);
        assert_eq!(
            network,
            NetworkSpec {
                interface: "faber0".into(),
                wireguard: WireGuardSpec {
                    private_key: id.private_key.expose().to_string(),
                    listen_port: 51820,
                    address: net("10.99.0.2/16"),
                    peers: vec![
                        WireGuardPeer {
                            public_key: key(1),
                            endpoint: Some("203.0.113.1:51820".into()),
                            allowed_ips: vec![net("10.99.0.1/32")],
                            keepalive: WG_KEEPALIVE_SECS,
                        },
                        WireGuardPeer {
                            public_key: key(3),
                            endpoint: Some("198.51.100.3:51820".into()),
                            allowed_ips: vec![net("10.99.0.3/32")],
                            keepalive: WG_KEEPALIVE_SECS,
                        },
                    ],
                    routes: vec![],
                },
                firewall: FirewallSpec {
                    wg_port: 51820,
                    peer_port: 7401,
                    peer_endpoint_ips: ["203.0.113.1", "198.51.100.3"]
                        .iter()
                        .map(|s| s.parse().unwrap())
                        .collect(),
                    peer_mesh_ips: ["10.99.0.1", "10.99.0.3"]
                        .iter()
                        .map(|s| s.parse().unwrap())
                        .collect(),
                    containers: None,
                },
            }
        );

        // A record that holds a subnet all the same is not routed.
        let stray = mesh(
            vec![node_with_subnet(1, "10.200.0.0/24"), node(2, None)],
            vec![],
        );
        let (network, warnings) = desired_network(&local(2), "faber0", 7401, Some(&stray), 0);
        assert_eq!(
            allowed_ips(&network.wireguard),
            vec![(key(1), nets(&["10.99.0.1/32"]))]
        );
        assert!(network.wireguard.routes.is_empty());
        assert_eq!(network.firewall.containers, None);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("node 1"), "{warnings:?}");
    }

    // ---- runtime ----

    #[test]
    fn desired_runtime_gives_this_node_its_pod_network() {
        let m = container_mesh(
            vec![
                node_with_subnet(1, "10.200.0.0/24"),
                node_with_subnet(2, "10.200.1.0/24"),
            ],
            vec![],
        );
        let settings = ContainerRuntimeSpec::default();
        assert_eq!(POD_BRIDGE, "faberbr0");
        assert_eq!(
            desired_runtime(Some(settings.clone()), Some(&m), 2),
            Some(NodeRuntimeSpec {
                settings,
                pod_network: Some(PodNetworkSpec {
                    bridge: "faberbr0".into(),
                    subnet: net("10.200.1.0/24"),
                    mtu: 1420,
                }),
            })
        );
        // No settings replicated yet: nothing to apply, subnet or not.
        assert_eq!(desired_runtime(None, Some(&m), 2), None);
    }

    #[test]
    fn desired_runtime_without_a_valid_subnet_has_no_pod_network() {
        let mut meshes = vec![
            // This node's record has no subnet.
            container_mesh(
                vec![node_with_subnet(1, "10.200.0.0/24"), node(2, None)],
                vec![],
            ),
            // This node is not registered: only its invite is there.
            container_mesh(
                vec![node_with_subnet(1, "10.200.0.0/24")],
                vec![invite(2, "198.51.100.2:51820", u64::MAX)],
            ),
            // A subnet in a mesh without a range.
            mesh(vec![node_with_subnet(2, "10.200.1.0/24")], vec![]),
        ];
        // Subnets that do not fit the range.
        for bad in ["10.201.0.0/24", "10.200.1.0/25", "10.200.1.9/24"] {
            meshes.push(container_mesh(vec![node_with_subnet(2, bad)], vec![]));
        }
        let none = NodeRuntimeSpec {
            settings: ContainerRuntimeSpec::default(),
            pod_network: None,
        };
        for m in &meshes {
            assert_eq!(
                desired_runtime(Some(ContainerRuntimeSpec::default()), Some(m), 2),
                Some(none.clone()),
                "{m:?}"
            );
        }
        assert_eq!(
            desired_runtime(Some(ContainerRuntimeSpec::default()), None, 2),
            Some(none)
        );
    }

    #[test]
    fn a_cni_directory_other_than_the_replicated_one_is_a_warning() {
        let settings = ContainerRuntimeSpec::default();
        assert_eq!(settings.cni_conf_dir, "/etc/cni/net.d");
        assert_eq!(
            cni_dir_warning(&settings, Path::new("/etc/cni/net.d")),
            None
        );
        // Spelling does not matter.
        assert_eq!(
            cni_dir_warning(&settings, Path::new("/etc/cni/net.d/")),
            None
        );
        let warning = cni_dir_warning(&settings, Path::new("/tmp/cni")).expect("a warning");
        assert!(warning.contains("/etc/cni/net.d"), "{warning}");
        assert!(warning.contains("/tmp/cni"), "{warning}");
        assert!(warning.contains("--cni-conf-dir"), "{warning}");
        // One sentence, not a line broken in the source.
        assert!(
            warning.contains("to /tmp/cni (--cni-conf-dir)"),
            "{warning}"
        );
    }

    /// [`crate::host_steps`] over real storage: nothing before
    /// `InitCluster`, and after it the founder's subnet in every part of the
    /// plan.
    #[tokio::test]
    async fn the_plan_follows_the_replicated_state() {
        use openraft::EntryPayload;
        use openraft::storage::RaftStateMachine;

        use crate::raft::store;
        use crate::raft::types::{Entry, Request};

        let dir = tempfile::tempdir().unwrap();
        let mut storage = store::open(dir.path().join("data")).unwrap();
        let runner = Arc::new(RecordingRunner::new());
        let host = |cni_conf_dir: &str| HostReconciler {
            backends: backends(&runner, dir.path()),
            iface: "faber0".into(),
            identity: local(1),
            join_pending: JoinPending::new(false),
            peer_port: 7401,
            cni_conf_dir: PathBuf::from(cni_conf_dir),
            lease_timing: LeaseTiming::default(),
            reader: storage.reader.clone(),
        };
        let (replicated, elsewhere) = (host("/etc/cni/net.d"), host("/var/tmp/cni"));

        // Nothing replicated: no runtime spec, so no directory to compare.
        for host in [&replicated, &elsewhere] {
            let plan = plan_of(host);
            assert_eq!(plan.desired.runtime, None);
            assert!(plan.desired.network.wireguard.routes.is_empty());
            assert_eq!(plan.desired.network.firewall.containers, None);
            assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);
        }

        let seed = Entry {
            log_id: openraft::testing::log_id(1, 1, 1),
            payload: EntryPayload::Normal(Request::InitCluster {
                mesh: MeshConfig {
                    container_cidr: Some(net(RANGE)),
                    ..MeshConfig::default()
                },
                founder: node(1, Some("203.0.113.1:51820")),
                containerd: ContainerRuntimeSpec::default(),
            }),
        };
        storage.state_machine.apply([seed]).await.unwrap();

        let plan = plan_of(&replicated);
        assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);
        let own = net("10.200.0.0/24");
        assert_eq!(plan.desired.network.wireguard.routes, vec![net(RANGE)]);
        assert_eq!(
            plan.desired.network.firewall.containers,
            Some(ContainerFirewall {
                cidr: net(RANGE),
                local_subnet: Some(own),
                node_mesh_ips: BTreeSet::new(),
                bridge: "faberbr0".into(),
            })
        );
        let runtime = plan.desired.runtime.clone().expect("settings replicated");
        assert_eq!(runtime.pod_network.map(|p| p.subnet), Some(own));

        // The same state with another local directory: one warning, on every
        // plan, and the same desired state.
        for _ in 0..2 {
            let other = plan_of(&elsewhere);
            assert_eq!(other.desired, plan.desired);
            assert_eq!(other.warnings.len(), 1, "{:?}", other.warnings);
            assert!(other.warnings[0].contains("/var/tmp/cni"));
        }
    }

    #[test]
    fn next_change_is_the_earliest_live_invite_expiry() {
        let m = mesh(
            vec![node(1, None)],
            vec![
                invite(5, "198.51.100.5:51820", 1300),
                invite(4, "198.51.100.4:51820", 1000),
                invite(6, "198.51.100.6:51820", 1060),
            ],
        );
        // The invite expiring at 1000 is already dead at 1000.
        assert_eq!(
            next_invite_expiry(Some(&m), 1000),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            next_invite_expiry(Some(&m), 999),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            next_invite_expiry(Some(&m), 1060),
            Some(Duration::from_secs(240))
        );
        assert_eq!(next_invite_expiry(Some(&m), 1300), None);
        assert_eq!(next_invite_expiry(None, 0), None);
        let empty = mesh(vec![node(1, None)], vec![]);
        assert_eq!(next_invite_expiry(Some(&empty), 0), None);
    }

    // ---- loop ----

    #[test]
    fn backoff_doubles_and_caps() {
        let t = ReconcileTiming::default();
        assert_eq!(t.backoff(1), Duration::from_secs(1));
        assert_eq!(t.backoff(2), Duration::from_secs(2));
        assert_eq!(t.backoff(5), Duration::from_secs(16));
        assert_eq!(t.backoff(6), Duration::from_secs(30));
        assert_eq!(t.backoff(1000), Duration::from_secs(30));
        assert_eq!(t.backoff(0), Duration::from_secs(1));
    }

    const WAIT: Duration = Duration::from_secs(5);
    const QUIET: Duration = Duration::from_millis(150);

    fn fast() -> ReconcileTiming {
        ReconcileTiming {
            resync: Duration::from_secs(3600),
            backoff_min: Duration::from_millis(40),
            backoff_max: Duration::from_millis(80),
            debounce_quiet: Duration::ZERO,
            debounce_max: Duration::ZERO,
        }
    }

    struct Harness {
        desired: Arc<Mutex<Option<u32>>>,
        /// Number of upcoming applies that fail.
        fail_next: Arc<AtomicU32>,
        /// Number of upcoming applies that panic.
        panic_next: Arc<AtomicU32>,
        applied: mpsc::UnboundedReceiver<(u32, Instant)>,
        tx: watch::Sender<u64>,
        shutdown: CancellationToken,
        handle: JoinHandle<()>,
    }

    impl Harness {
        fn start(initial: Option<u32>, timing: ReconcileTiming) -> Self {
            Self::start_seeded(initial, None, timing)
        }

        /// `seed`: what startup already applied, handed to the loop.
        fn start_seeded(initial: Option<u32>, seed: Option<u32>, timing: ReconcileTiming) -> Self {
            let desired = Arc::new(Mutex::new(initial));
            let fail_next = Arc::new(AtomicU32::new(0));
            let panic_next = Arc::new(AtomicU32::new(0));
            let (applied_tx, applied) = mpsc::unbounded_channel();
            let (tx, rx) = watch::channel(0u64);
            let shutdown = CancellationToken::new();
            let d = desired.clone();
            let (f, p) = (fail_next.clone(), panic_next.clone());
            let handle = tokio::spawn(run_reconciler(
                "test",
                rx,
                timing,
                seed,
                move || (*d.lock().unwrap(), None),
                move |v: u32| {
                    let (f, p, applied_tx) = (f.clone(), p.clone(), applied_tx.clone());
                    async move {
                        applied_tx.send((v, Instant::now())).unwrap();
                        if p.load(Ordering::SeqCst) > 0 {
                            p.fetch_sub(1, Ordering::SeqCst);
                            panic!("apply exploded");
                        }
                        if f.load(Ordering::SeqCst) > 0 {
                            f.fetch_sub(1, Ordering::SeqCst);
                            anyhow::bail!("apply failed");
                        }
                        Ok(())
                    }
                },
                shutdown.clone(),
            ));
            Self {
                desired,
                fail_next,
                panic_next,
                applied,
                tx,
                shutdown,
                handle,
            }
        }

        fn set(&self, v: Option<u32>) {
            *self.desired.lock().unwrap() = v;
            self.tx.send_modify(|n| *n += 1);
        }

        async fn next(&mut self) -> (u32, Instant) {
            timeout(WAIT, self.applied.recv())
                .await
                .expect("apply expected")
                .unwrap()
        }

        async fn quiet(&mut self) {
            if let Ok(Some(v)) = timeout(QUIET, self.applied.recv()).await {
                panic!("unexpected apply of {v:?}");
            }
        }

        async fn stop(self) {
            self.shutdown.cancel();
            timeout(WAIT, self.handle)
                .await
                .expect("loop did not stop")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn applies_at_start_and_on_change() {
        let mut h = Harness::start(Some(1), fast());
        assert_eq!(h.next().await.0, 1);
        h.set(Some(2));
        assert_eq!(h.next().await.0, 2);
        h.quiet().await;
        h.stop().await;
    }

    #[tokio::test]
    async fn a_seeded_loop_does_not_apply_the_same_state_again() {
        let mut h = Harness::start_seeded(Some(1), Some(1), fast());
        h.quiet().await;
        // A change tick with the same state is still nothing to do.
        h.set(Some(1));
        h.quiet().await;
        h.set(Some(2));
        assert_eq!(h.next().await.0, 2);
        h.stop().await;
    }

    #[tokio::test]
    async fn a_seeded_loop_applies_a_state_that_differs_from_the_seed() {
        let mut h = Harness::start_seeded(Some(2), Some(1), fast());
        assert_eq!(h.next().await.0, 2);
        h.quiet().await;
        h.stop().await;
    }

    #[tokio::test]
    async fn skips_identical_desired_state() {
        let mut h = Harness::start(Some(7), fast());
        assert_eq!(h.next().await.0, 7);
        h.set(Some(7));
        h.set(Some(7));
        h.quiet().await;
        h.stop().await;
    }

    #[tokio::test]
    async fn none_applies_nothing_until_some() {
        let mut h = Harness::start(None, fast());
        h.quiet().await;
        h.set(Some(3));
        assert_eq!(h.next().await.0, 3);
        h.stop().await;
    }

    #[tokio::test]
    async fn retries_after_failure_with_backoff() {
        let mut h = Harness::start(None, fast());
        h.fail_next.store(2, Ordering::SeqCst);
        h.set(Some(4));
        let (v1, t1) = h.next().await;
        // A tick during the backoff must not cut it short.
        h.tx.send_modify(|n| *n += 1);
        let (v2, t2) = h.next().await;
        let (v3, t3) = h.next().await;
        assert_eq!((v1, v2, v3), (4, 4, 4));
        assert!(t2 - t1 >= Duration::from_millis(40), "{:?}", t2 - t1);
        assert!(t3 - t2 >= Duration::from_millis(80), "{:?}", t3 - t2);
        // Success clears the retry; identical state is not re-applied.
        h.quiet().await;
        h.stop().await;
    }

    #[tokio::test]
    async fn panicking_apply_is_contained_and_retried() {
        let mut h = Harness::start(None, fast());
        h.panic_next.store(1, Ordering::SeqCst);
        h.set(Some(8));
        assert_eq!(h.next().await.0, 8);
        assert_eq!(h.next().await.0, 8);
        h.quiet().await;
        assert!(!h.handle.is_finished());
        h.stop().await;
    }

    #[tokio::test]
    async fn panicking_compute_is_contained() {
        let (tx, rx) = watch::channel(0u64);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let (applied_tx, mut applied) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(run_reconciler(
            "test",
            rx,
            fast(),
            None,
            move || {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("compute exploded");
                }
                (Some(1u32), None)
            },
            move |v| {
                let applied_tx = applied_tx.clone();
                async move {
                    applied_tx.send(v).unwrap();
                    Ok(())
                }
            },
            shutdown.clone(),
        ));
        assert_eq!(timeout(WAIT, applied.recv()).await.unwrap(), Some(1));
        drop(tx);
        shutdown.cancel();
        timeout(WAIT, handle).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stops_on_shutdown() {
        let mut h = Harness::start(Some(1), fast());
        assert_eq!(h.next().await.0, 1);
        let tx = h.tx.clone();
        let desired = h.desired.clone();
        h.shutdown.cancel();
        timeout(WAIT, &mut h.handle).await.unwrap().unwrap();
        *desired.lock().unwrap() = Some(2);
        let _ = tx.send(1);
        h.quiet().await;
    }

    #[tokio::test]
    async fn stops_when_the_change_feed_closes() {
        let mut h = Harness::start(Some(1), fast());
        assert_eq!(h.next().await.0, 1);
        let Harness { tx, handle, .. } = h;
        drop(tx);
        timeout(WAIT, handle)
            .await
            .expect("loop did not stop")
            .unwrap();
    }

    #[tokio::test]
    async fn stops_during_backoff() {
        let mut h = Harness::start(
            None,
            ReconcileTiming {
                backoff_min: Duration::from_secs(3600),
                backoff_max: Duration::from_secs(3600),
                ..fast()
            },
        );
        h.fail_next.store(1, Ordering::SeqCst);
        h.set(Some(1));
        assert_eq!(h.next().await.0, 1);
        h.stop().await;
    }

    // ---- host apply ----

    /// The default settings with no container network.
    fn runtime_spec() -> NodeRuntimeSpec {
        NodeRuntimeSpec {
            settings: ContainerRuntimeSpec::default(),
            pod_network: None,
        }
    }

    /// What the host steps want, as typed values.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct HostDesired {
        network: NetworkSpec,
        runtime: Option<NodeRuntimeSpec>,
    }

    struct HostPlan {
        desired: HostDesired,
        warnings: Vec<String>,
    }

    /// The host steps planned against `host`'s replicated state.
    fn plan_of(host: &HostReconciler) -> HostPlan {
        let plan = crate::host_steps(host).plan(&host.view().unwrap());
        let network = plan.desired[0].downcast_ref::<NetworkSpec>().unwrap();
        let runtime = plan.desired[1]
            .downcast_ref::<Option<NodeRuntimeSpec>>()
            .unwrap();
        HostPlan {
            desired: HostDesired {
                network: network.clone(),
                runtime: runtime.clone(),
            },
            warnings: plan.warnings,
        }
    }

    /// The host steps applied once, network then runtime.
    async fn apply_host(backends: &HostBackends, desired: &HostDesired) -> anyhow::Result<()> {
        let runner = StepRunner::new(
            StepList::default()
                .with(NetworkStep {
                    network: backends.network.clone(),
                    iface: "faber0".into(),
                    identity: local(2),
                    join_pending: JoinPending::new(false),
                    peer_port: 7401,
                })
                .with(RuntimeStep {
                    runtime: backends.runtime.clone(),
                    node_id: 2,
                    cni_conf_dir: PathBuf::from("/etc/cni/net.d"),
                }),
        );
        let values = [
            StepValue::new(desired.network.clone()),
            StepValue::new(desired.runtime.clone()),
        ];
        runner.apply(&values, false).await
    }

    fn host_desired(runtime: Option<NodeRuntimeSpec>) -> HostDesired {
        let m = mesh(
            vec![node(1, Some("203.0.113.1:51820")), node(2, None)],
            vec![],
        );
        let (network, warnings) = desired_network(&local(2), "faber0", 7401, Some(&m), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
        HostDesired { network, runtime }
    }

    /// The fake network runner with containerd's drop-in written to `dir`.
    fn backends(runner: &Arc<RecordingRunner>, dir: &std::path::Path) -> HostBackends {
        HostBackends {
            network: NetworkManager::new(runner.clone()),
            runtime: Arc::new(Containerd {
                conf_dir: dir.to_path_buf(),
                cni_conf_dir: dir.join("cni"),
            }),
        }
    }

    #[tokio::test]
    async fn apply_host_runs_network_then_runtime() {
        let runner = Arc::new(RecordingRunner::new());
        let dir = tempfile::tempdir().unwrap();
        let d = host_desired(Some(runtime_spec()));
        apply_host(&backends(&runner, dir.path()), &d)
            .await
            .unwrap();
        let lines = runner.cmdlines();
        let sync = lines
            .iter()
            .position(|l| l.starts_with("wg syncconf"))
            .unwrap();
        let nft = lines.iter().position(|l| l == "nft -f -").unwrap();
        assert!(sync < nft, "{lines:?}");
        assert!(dir.path().join(DROP_IN_FILE).is_file());
        // No spec yet: nothing written, still Ok.
        let empty = tempfile::tempdir().unwrap();
        apply_host(&backends(&runner, empty.path()), &host_desired(None))
            .await
            .unwrap();
        assert!(!empty.path().join(DROP_IN_FILE).exists());
    }

    #[tokio::test]
    async fn a_failing_network_step_does_not_skip_the_runtime_step() {
        let runner = Arc::new(RecordingRunner::new());
        runner.script("wg", None, CommandOutput::failure(1, "boom"));
        let dir = tempfile::tempdir().unwrap();
        let d = host_desired(Some(runtime_spec()));
        let err = apply_host(&backends(&runner, dir.path()), &d)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("faber0"), "{err:#}");
        assert!(dir.path().join(DROP_IN_FILE).is_file());

        // And the other way round: a bad drop-in dir does not stop the network.
        let runner = Arc::new(RecordingRunner::new());
        let missing = backends(&runner, &dir.path().join("missing"));
        let err = apply_host(&missing, &d).await.unwrap_err();
        assert!(format!("{err:#}").contains("drop-in"), "{err:#}");
        assert!(runner.cmdlines().iter().any(|l| l == "nft -f -"));
    }

    // ---- triggers: debounce and resync (paused time) ----

    type TimedLoop = (
        watch::Sender<u64>,
        mpsc::UnboundedReceiver<Instant>,
        CancellationToken,
        JoinHandle<()>,
    );

    /// A loop over a counter the test bumps through the sender; every apply
    /// sends the instant it ran.
    fn timed_loop(timing: ReconcileTiming) -> TimedLoop {
        let (tx, rx) = watch::channel(0u64);
        let (applied_tx, applied) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let seen = rx.clone();
        let handle = tokio::spawn(run_reconciler(
            "timed",
            rx,
            timing,
            None,
            move || (Some(*seen.borrow()), None),
            move |_| {
                let applied_tx = applied_tx.clone();
                async move {
                    applied_tx.send(Instant::now()).unwrap();
                    Ok(())
                }
            },
            shutdown.clone(),
        ));
        (tx, applied, shutdown, handle)
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_of_changes_is_applied_once_after_the_quiet_period() {
        let (tx, mut applied, shutdown, handle) =
            timed_loop(ReconcileTiming::with_resync(Duration::from_secs(3600)));
        applied.recv().await.unwrap(); // initial apply
        let start = Instant::now();
        for i in 1..=10u64 {
            tx.send(i).unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let at = applied.recv().await.unwrap();
        // Last change at 0.9s, then 1s of quiet.
        let waited = at - start;
        assert!(
            waited >= Duration::from_millis(1900) && waited < Duration::from_millis(2100),
            "{waited:?}"
        );
        // Exactly one apply for the burst.
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(applied.try_recv().is_err());
        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_continuous_stream_is_applied_at_the_cap() {
        let (tx, mut applied, shutdown, handle) =
            timed_loop(ReconcileTiming::with_resync(Duration::from_secs(3600)));
        applied.recv().await.unwrap();
        let start = Instant::now();
        let feeder = tokio::spawn(async move {
            for i in 1..=200u64 {
                if tx.send(i).is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        let at = applied.recv().await.unwrap();
        let waited = at - start;
        assert!(
            waited >= DEBOUNCE_MAX && waited < DEBOUNCE_MAX + Duration::from_millis(300),
            "{waited:?}"
        );
        shutdown.cancel();
        handle.await.unwrap();
        feeder.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn resync_fires_at_the_configured_interval() {
        let interval = Duration::from_secs(7);
        let (_tx, mut applied, shutdown, handle) =
            timed_loop(ReconcileTiming::with_resync(interval));
        let first = applied.recv().await.unwrap();
        let second = applied.recv().await.unwrap();
        let third = applied.recv().await.unwrap();
        assert_eq!(second - first, interval);
        assert_eq!(third - second, interval);
        shutdown.cancel();
        handle.await.unwrap();
    }

    /// The desired state changes at `due` with no change tick (an invite
    /// expiring). The loop must wake then, not at the next resync.
    #[tokio::test(start_paused = true)]
    async fn wakes_when_the_desired_state_is_due_to_change() {
        let (_tx, rx) = watch::channel(0u64);
        let (applied_tx, mut applied) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let start = Instant::now();
        let due = Duration::from_secs(30);
        let handle = tokio::spawn(run_reconciler(
            "expiry",
            rx,
            ReconcileTiming::with_resync(Duration::from_secs(3600)),
            None,
            move || {
                let elapsed = start.elapsed();
                if elapsed < due {
                    (Some("with invite"), Some(due - elapsed))
                } else {
                    (Some("without invite"), None)
                }
            },
            move |v| {
                let applied_tx = applied_tx.clone();
                async move {
                    applied_tx.send((v, Instant::now())).unwrap();
                    Ok(())
                }
            },
            shutdown.clone(),
        ));
        assert_eq!(applied.recv().await.unwrap(), ("with invite", start));
        let second = timeout(Duration::from_secs(600), applied.recv())
            .await
            .expect("no wake at the expiry")
            .unwrap();
        assert_eq!(second, ("without invite", start + due));
        // Nothing further is due.
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert!(applied.try_recv().is_err());
        shutdown.cancel();
        handle.await.unwrap();
    }

    /// A compute panic followed by an unchanged state must end the backoff;
    /// otherwise the loop retries every `backoff_min` forever and never
    /// reaches the resync tick.
    #[tokio::test(start_paused = true)]
    async fn a_compute_panic_does_not_starve_the_resync() {
        let (tx, rx) = watch::channel(0u64);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let (applied_tx, mut applied) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let resync = Duration::from_secs(60);
        let handle = tokio::spawn(run_reconciler(
            "panic",
            rx,
            ReconcileTiming::with_resync(resync),
            None,
            move || {
                // Only the second computation panics.
                if c.fetch_add(1, Ordering::SeqCst) == 1 {
                    panic!("compute exploded");
                }
                (Some(1u32), None)
            },
            move |_| {
                let applied_tx = applied_tx.clone();
                async move {
                    applied_tx.send(Instant::now()).unwrap();
                    Ok(())
                }
            },
            shutdown.clone(),
        ));
        let first = applied.recv().await.unwrap();
        tx.send(1).unwrap(); // triggers the panicking computation
        let second = timeout(resync * 3, applied.recv())
            .await
            .expect("the resync never ran")
            .unwrap();
        assert_eq!(second - first, resync);
        // The loop is idle between resyncs, not recomputing every second.
        let before = calls.load(Ordering::SeqCst);
        tokio::time::sleep(resync / 2).await;
        assert_eq!(calls.load(Ordering::SeqCst), before);
        shutdown.cancel();
        handle.await.unwrap();
    }

    #[test]
    fn default_timing() {
        let t = ReconcileTiming::default();
        assert_eq!(t.resync, Duration::from_secs(300));
        assert_eq!(t.debounce_quiet, Duration::from_secs(1));
        assert_eq!(t.debounce_max, Duration::from_secs(5));
        assert_eq!(
            ReconcileTiming::with_resync(Duration::from_secs(60)).resync,
            Duration::from_secs(60)
        );
    }
}
