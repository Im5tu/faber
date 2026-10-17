//! WireGuard mesh bootstrap on loopback with a fake host: founder init, the
//! invite endpoint and its Host/Origin guard, joining from a token in two
//! steps (register, then join), invite reuse and expiry, a second machine
//! with a used token, invites through a follower, restarts, interface
//! ownership, endpoint changes, a rejected invite, a pending join that a
//! new token replaces, and the container subnets raft hands out (the route,
//! the peers' AllowedIPs, the firewall's container rules and the CNI config).
//!
//! Then node leases, with the lease clock run a thousand times faster: a
//! stopped node is removed when its lease runs out, a removed node that
//! comes back gives up its identity, a new machine takes over a name, and
//! nodes that keep renewing are never removed.

mod common;

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use faber_api_types::{
    ClientError, CreateInviteRequest, ErrorBody, FaberClient, InviteResponse, error_code,
};
use faber_network_management::fake::{Call, RecordingRunner};
use faber_network_management::{BoxFuture, CommandOutput, CommandRunner, NetworkManager};
use fabercp::dto::{PeerJoinRequest, PeerRegisterRequest};
use fabercp::lease::LeaseTiming;
use fabercp::mesh::{BootstrapInput, InviteToken, PendingJoin, identity, public_key_of};
use fabercp::runner::NodeRuntime;
use fabercp::runner::node::fake::FakeNode;
use fabercp::{Config, HostBackends, NodeHandle, PeerListener, start_or_idle, start_with};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

const WAIT: Duration = Duration::from_secs(20);
const FOUNDER_ENDPOINT: &str = "203.0.113.1:51820";
const NODE2_ENDPOINT: &str = "198.51.100.2:51820";

/// Records every command like [`RecordingRunner`] and behaves like a host
/// whose faber0 does not exist until `ip link add` creates it, and is gone
/// again after `ip link del`.
#[derive(Default)]
struct FakeHost {
    rec: RecordingRunner,
    link_up: AtomicBool,
}

impl CommandRunner for FakeHost {
    fn run<'a>(
        &'a self,
        prog: &'a str,
        args: &'a [&'a str],
        stdin: Option<Vec<u8>>,
    ) -> BoxFuture<'a, io::Result<CommandOutput>> {
        Box::pin(async move {
            let out = self.rec.run(prog, args, stdin).await?;
            if prog == "ip"
                && args.starts_with(&["link", "show"])
                && !self.link_up.load(Ordering::SeqCst)
            {
                return Ok(CommandOutput::failure(
                    1,
                    "Device \"faber0\" does not exist.",
                ));
            }
            if prog == "ip" && args.starts_with(&["link", "add"]) {
                self.link_up.store(true, Ordering::SeqCst);
            }
            if prog == "ip" && args.starts_with(&["link", "del"]) {
                self.link_up.store(false, Ordering::SeqCst);
            }
            Ok(out)
        })
    }
}

impl FakeHost {
    fn calls(&self) -> Vec<Call> {
        self.rec.calls()
    }

    /// stdin of the most recent call to `prog` with first argument `first`.
    fn last_stdin(&self, prog: &str, first: &str) -> Option<String> {
        self.calls()
            .into_iter()
            .rev()
            .find(|c| c.prog == prog && c.args.first().map(String::as_str) == Some(first))
            .and_then(|c| c.stdin)
            .map(|b| String::from_utf8(b).unwrap())
    }

    fn last_syncconf(&self) -> String {
        self.last_stdin("wg", "syncconf").unwrap_or_default()
    }

    fn last_nft(&self) -> String {
        self.last_stdin("nft", "-f").unwrap_or_default()
    }
}

enum Bootstrap {
    NewMesh,
    Token(String),
    /// `--join-token-file` still set, but install.sh has removed the file.
    MissingTokenFile,
    Existing,
}

struct MeshNode {
    dir: TempDir,
    host: Arc<FakeHost>,
    /// faber-node, which writes the runtime config. It outlives a launch,
    /// as the files on a real host do.
    node: FakeNode,
    handle: Option<NodeHandle>,
    /// Commands the fake host had seen when the peer listener was bound.
    before_bind: Arc<Mutex<Option<Vec<Call>>>>,
    /// The address `start_with` asked the deferred peer listener to bind.
    intended: Arc<Mutex<Option<SocketAddr>>>,
    /// `--wg-endpoint` for the next launch, when the test changes it.
    wg_endpoint: Option<String>,
    /// Lease clocks for the next launch; production clocks when `None`.
    lease_timing: Option<LeaseTiming>,
    /// `--election-min-ms` and `--election-max-ms` for the next launch;
    /// [`common::steady`] when `None`.
    election_ms: Option<(u64, u64)>,
    /// `--node-name` for the next launch.
    node_name: Option<String>,
}

impl MeshNode {
    fn state_dir(&self) -> PathBuf {
        self.dir.path().join("state")
    }

    /// The subnet of the CNI network config faber-node keeps for this node,
    /// or `None` while it has none.
    fn cni_subnet(&self) -> Option<String> {
        let network = self.node.runtime_files()?.pod_network?;
        Some(network.subnet.to_string())
    }

    /// Whether faber-node has been asked to configure the runtime at all.
    fn runtime_configured(&self) -> bool {
        self.node.runtime_files().is_some()
    }

    /// The fake host for the network, and the fake faber-node for the
    /// runtime config.
    fn backends(&self) -> Option<HostBackends> {
        Some(HostBackends {
            network: NetworkManager::new(self.host.clone()),
            runtime: Arc::new(NodeRuntime::new(self.node.clone())),
        })
    }

    fn config(&self, boot: &Bootstrap) -> Config {
        let mut c = common::fast_config(1, &self.dir.path().join("data"));
        common::steady(&mut c);
        c.node_id = None;
        c.state_dir = self.state_dir();
        c.manage_host = true;
        c.reconcile_interval_secs = 2;
        match boot {
            Bootstrap::NewMesh => {
                c.new_mesh = true;
                c.wg_endpoint = Some(FOUNDER_ENDPOINT.into());
            }
            Bootstrap::Token(token) => {
                let file = self.dir.path().join("join.token");
                std::fs::write(&file, token).unwrap();
                c.join_token_file = Some(file);
            }
            Bootstrap::MissingTokenFile => {
                c.join_token_file = Some(self.dir.path().join("removed.token"));
            }
            Bootstrap::Existing => {}
        }
        if let Some(endpoint) = &self.wg_endpoint {
            c.wg_endpoint = Some(endpoint.clone());
        }
        if let Some(timing) = self.lease_timing {
            c.lease_timing = timing;
        }
        if let Some((min, max)) = self.election_ms {
            c.election_min_ms = min;
            c.election_max_ms = max;
        }
        c.node_name = self.node_name.clone();
        c
    }

    /// A node that has not been started: empty state and a fake host with
    /// no interface.
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        MeshNode {
            dir,
            host: Arc::new(FakeHost::default()),
            node: FakeNode::default(),
            handle: None,
            before_bind: Arc::new(Mutex::new(None)),
            intended: Arc::new(Mutex::new(None)),
            wg_endpoint: None,
            lease_timing: None,
            election_ms: None,
            node_name: None,
        }
    }

    /// [`MeshNode::new`] with the lease clocks of [`fast_lease`].
    fn fast() -> Self {
        let mut node = Self::new();
        node.lease_timing = Some(fast_lease());
        node
    }

    async fn start(boot: Bootstrap) -> Self {
        let mut node = Self::new();
        node.launch(boot, None).await;
        node
    }

    async fn launch(&mut self, boot: Bootstrap, reuse_port: Option<u16>) {
        self.try_launch(boot, reuse_port).await.unwrap();
    }

    /// `reuse_port`: bind the peer listener on this loopback port, so the
    /// address in the membership config stays valid across a restart.
    async fn try_launch(&mut self, boot: Bootstrap, reuse_port: Option<u16>) -> anyhow::Result<()> {
        let config = self.config(&boot);
        let backends = self.backends();
        let peer = self.peer_listener(reuse_port);
        let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.handle = Some(start_with(config, api, peer, backends).await?);
        Ok(())
    }

    /// Starts the node the way the binary does. Returns whether it runs:
    /// `false` when it has nothing to start as and stays idle.
    async fn launch_or_idle(&mut self, boot: Bootstrap) -> bool {
        let config = self.config(&boot);
        let backends = self.backends();
        let peer = self.peer_listener(None);
        let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.handle = start_or_idle(config, api, peer, backends).await.unwrap();
        self.handle.is_some()
    }

    /// The deferred peer listener: notes the address it was asked to bind
    /// and what the fake host had seen by then, and binds loopback.
    fn peer_listener(&self, reuse_port: Option<u16>) -> PeerListener {
        let host = self.host.clone();
        let snapshot = self.before_bind.clone();
        let asked = self.intended.clone();
        PeerListener::deferred(move |intended: SocketAddr| {
            Box::pin(async move {
                *asked.lock().unwrap() = Some(intended);
                *snapshot.lock().unwrap() = Some(host.calls());
                let addr = SocketAddr::from(([127, 0, 0, 1], reuse_port.unwrap_or(0)));
                let mut tries = 0;
                loop {
                    match TcpListener::bind(addr).await {
                        Ok(l) => return Ok(l),
                        Err(e) if tries < 100 => {
                            tries += 1;
                            let _ = e;
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            })
        })
    }

    fn cmdlines(&self) -> Vec<String> {
        self.host.calls().iter().map(Call::cmdline).collect()
    }

    async fn restart(&mut self) {
        let port = self.peer_addr().port();
        self.stop().await;
        self.host.rec.clear();
        *self.before_bind.lock().unwrap() = None;
        self.launch(Bootstrap::Existing, Some(port)).await;
    }

    async fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown().await.unwrap();
        }
    }

    fn handle(&self) -> &NodeHandle {
        self.handle.as_ref().expect("running")
    }

    fn api_addr(&self) -> SocketAddr {
        self.handle().api_addr()
    }

    fn peer_addr(&self) -> SocketAddr {
        self.handle().peer_addr()
    }

    fn client(&self) -> FaberClient {
        FaberClient::new(&format!("http://{}", self.api_addr())).unwrap()
    }

    fn identity(&self) -> identity::Identity {
        identity::load(&self.state_dir())
            .unwrap()
            .expect("identity")
    }

    /// This node's view of the replicated endpoint of node `id`.
    fn replicated_endpoint(&self, id: u64) -> Option<String> {
        let mesh = self.handle().state().mesh().unwrap()?;
        mesh.nodes.get(&id)?.endpoint.clone()
    }

    /// `(term, last log index)` as this node's raft sees them.
    fn log_position(&self) -> (u64, Option<u64>) {
        let m = self.handle().raft().metrics().borrow().clone();
        (m.current_term, m.last_log_index)
    }

    fn voters(&self) -> Vec<u64> {
        self.handle()
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    /// Voters as this node's raft sees them, in order.
    fn sorted_voters(&self) -> Vec<u64> {
        let mut v = self.voters();
        v.sort();
        v
    }

    /// The replicated mesh as this node has applied it.
    fn mesh(&self) -> fabercp::mesh::MeshState {
        self.handle().state().mesh().unwrap().expect("mesh")
    }

    /// The `bootstrap-error` marker, if there is one.
    fn bootstrap_error(&self) -> Option<String> {
        std::fs::read_to_string(self.state_dir().join("bootstrap-error")).ok()
    }

    fn before_bind(&self) -> Vec<Call> {
        self.before_bind
            .lock()
            .unwrap()
            .clone()
            .expect("peer listener bound")
    }

    /// An invite for a node that will hold `name`.
    async fn invite(&self, name: &str, endpoint: &str, ttl_secs: Option<u64>) -> InviteResponse {
        self.client()
            .mesh_invite(&CreateInviteRequest {
                name: name.into(),
                endpoint: endpoint.into(),
                ttl_secs,
            })
            .await
            .unwrap_or_else(|e| panic!("invite: {e}"))
    }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Lets `secs` seconds of the lease clock go by on `node`, which must run
/// [`fast_lease`] clocks. The clock moves only through committed entries and
/// never faster than the wall clock, so a load that stalls the leader makes
/// this wait longer, never shorter. For checks that something does not
/// happen: a host resync (2 s in these tests) takes 2000 of them.
async fn lease_clock_runs(node: &MeshNode, secs: u64) {
    let until = node.mesh().clock + secs;
    eventually("the lease clock runs on", || node.mesh().clock >= until).await;
}

/// Lease clock seconds that cover one host resync (2 s) with room to spare.
const A_RESYNC: u64 = 2_500;

/// Sets the lease through whichever of `alive` leads, so that the change is
/// not forwarded. Followers name a node that stopped as leader until their
/// election timers run out, and a change forwarded to it fails with
/// `peer_unreachable`. A leader that changes during the call answers
/// `not_leader` or `no_leader`; the same lease set again is harmless, so
/// that is retried. Any other answer fails the test.
async fn set_lease(alive: &[&MeshNode], secs: u64) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let leader = alive
            .iter()
            .find(|n| n.handle().raft().metrics().borrow().state.is_leader());
        if let Some(leader) = leader {
            match leader.client().mesh_set_lease(secs).await {
                Ok(_) => return,
                Err(ClientError::Api { status: 503, .. }) => {}
                Err(e) => panic!("the lease was not set: {e}"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "none of the running nodes took the lead"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Lease clocks for tests: a second of the lease clock takes a millisecond,
/// so the shortest lease an admin can set (300 seconds) runs out in 300 ms.
fn fast_lease() -> LeaseTiming {
    LeaseTiming {
        tick_min: Duration::from_millis(20),
        tick_max: Duration::from_millis(100),
        second: Duration::from_millis(1),
        write_timeout: Duration::from_secs(5),
    }
}

/// A lease the fast clocks run out in two seconds. Every node renews it
/// every half second.
const SHORT_LEASE_SECS: u64 = 2000;
const SHORT_LEASE: Duration = Duration::from_secs(2);

/// Starts a founder and runs `cluster init` through the local API.
async fn founder() -> MeshNode {
    found(MeshNode::new()).await
}

/// Starts `node` as a founder and runs `cluster init` through the local API.
async fn found(mut node: MeshNode) -> MeshNode {
    node.launch(Bootstrap::NewMesh, None).await;
    node.client().cluster_init().await.unwrap();
    node.handle()
        .raft()
        .wait(Some(WAIT))
        .current_leader(1, "founder leads")
        .await
        .unwrap();
    eventually("mesh seeded", || {
        node.handle().state().mesh().unwrap().is_some()
    })
    .await;
    node
}

/// Founder plus node 2 joined from a token.
async fn two_nodes() -> (MeshNode, MeshNode, InviteResponse) {
    let n1 = founder().await;
    let invite = n1.invite("node-2", "198.51.100.2", None).await;
    let n2 = MeshNode::start(Bootstrap::Token(invite.token.clone())).await;
    eventually("node 2 is a voter", || {
        let mut v = n1.voters();
        v.sort();
        v == [1, 2]
    })
    .await;
    eventually("pending join cleared", || {
        n2.identity().pending_join.is_none()
    })
    .await;
    // The pending join goes only once the node itself sees it is a voter.
    assert!(n2.voters().contains(&2), "{:?}", n2.voters());
    (n1, n2, invite)
}

/// Invites a node named `name` through `via` and starts `node` with the
/// token. Returns once `via` sees the new node as a voter and the node has
/// cleared its pending join.
async fn join(via: &MeshNode, mut node: MeshNode, name: &str, endpoint: &str) -> MeshNode {
    let invite = via.invite(name, endpoint, None).await;
    node.launch(Bootstrap::Token(invite.token.clone()), None)
        .await;
    eventually("the new node is a voter", || {
        via.voters().contains(&invite.node_id)
    })
    .await;
    eventually("pending join cleared", || {
        node.identity().pending_join.is_none()
    })
    .await;
    node
}

/// Founder (`node-1`) plus `node-2` and `node-3`, which get the subnets
/// 10.200.1.0/24 and 10.200.2.0/24. With `fast`, every node runs the lease
/// clocks of [`fast_lease`].
async fn three_nodes(fast: bool) -> (MeshNode, MeshNode, MeshNode) {
    let new = || {
        if fast {
            MeshNode::fast()
        } else {
            MeshNode::new()
        }
    };
    let n1 = found(new()).await;
    let n2 = join(&n1, new(), "node-2", "198.51.100.2").await;
    let n3 = join(&n1, new(), "node-3", "198.51.100.3").await;
    for node in [&n1, &n2, &n3] {
        eventually("every node sees three voters", || {
            node.sorted_voters() == [1, 2, 3]
        })
        .await;
    }
    (n1, n2, n3)
}

/// Whether the node with `public_key` and `mesh_ip` is gone from `node`'s
/// host: no WireGuard peer, and in neither firewall set.
fn host_dropped(node: &MeshNode, public_key: &str, mesh_ip: &str) -> bool {
    let nft = node.host.last_nft();
    !node.host.last_syncconf().contains(public_key)
        && !nft_set(&nft, "peer_mesh_ips")
            .iter()
            .any(|ip| ip == mesh_ip)
        && !nft_set(&nft, "node_mesh_ips")
            .iter()
            .any(|ip| ip == mesh_ip)
}

/// An instance id no node in these tests has.
fn other_machine() -> String {
    "ee".repeat(16)
}

fn register_body(
    invite_id: &str,
    node_id: u64,
    endpoint: &str,
    instance_id: &str,
) -> serde_json::Value {
    serde_json::to_value(PeerRegisterRequest {
        invite_id: invite_id.into(),
        node_id,
        endpoint: endpoint.into(),
        instance_id: instance_id.into(),
    })
    .unwrap()
}

fn join_body(
    node_id: u64,
    addr: SocketAddr,
    invite_id: &str,
    instance_id: &str,
) -> serde_json::Value {
    serde_json::to_value(PeerJoinRequest {
        node_id,
        addr: addr.to_string(),
        invite_id: Some(invite_id.into()),
        instance_id: Some(instance_id.into()),
    })
    .unwrap()
}

/// The status of a POST and, unless it succeeded, its error code.
async fn post_code(url: &str, body: &serde_json::Value) -> (StatusCode, Option<String>) {
    let resp = common::http().post(url).json(body).send().await.unwrap();
    let status = resp.status();
    if status.is_success() {
        return (status, None);
    }
    let body: ErrorBody = resp.json().await.unwrap();
    (status, Some(body.error))
}

/// The identity a machine makes from `token` on its first start, written
/// into `node`'s state dir without starting the node.
fn make_identity(node: &MeshNode, token: &str) -> identity::Identity {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let input = BootstrapInput {
        new_mesh: false,
        mesh_cidr: "10.99.0.0/16".parse().unwrap(),
        join_token: Some(token.to_string()),
        wg_port: 51820,
        endpoint: None,
    };
    identity::resolve(&node.state_dir(), input, now).unwrap()
}

fn position(calls: &[Call], cmdline: &str) -> usize {
    calls
        .iter()
        .position(|c| c.cmdline() == cmdline)
        .unwrap_or_else(|| {
            panic!(
                "{cmdline:?} not in {:#?}",
                calls.iter().map(Call::cmdline).collect::<Vec<_>>()
            )
        })
}

/// The elements of the set `name` in an `nft -f -` script. Empty when the
/// set has no elements or is not in the script.
fn nft_set(script: &str, name: &str) -> Vec<String> {
    let header = format!("set {name} {{");
    script
        .lines()
        .map(str::trim)
        .skip_while(|l| *l != header)
        .take_while(|l| *l != "}")
        .find_map(|l| l.strip_prefix("elements = { ")?.strip_suffix(" }"))
        .map(|list| list.split(", ").map(str::to_string).collect())
        .unwrap_or_default()
}

/// The `AllowedIPs` of the peer with `public_key` in a syncconf text. `None`
/// when there is no such peer.
fn peer_allowed_ips(conf: &str, public_key: &str) -> Option<String> {
    conf.split("[Peer]")
        .find(|section| section.contains(public_key))?
        .lines()
        .find_map(|l| l.strip_prefix("AllowedIPs = "))
        .map(str::to_string)
}

/// The forward rules that name the node's own subnet `local`, in the default
/// container range.
fn local_subnet_rules(local: &str) -> [String; 4] {
    let range = "10.200.0.0/16";
    [
        format!("ip saddr {local} ip daddr != {range} drop"),
        format!(
            "iifname \"faber0\" oifname \"faberbr0\" ip saddr @node_mesh_ips ip daddr {local} accept"
        ),
        format!("iifname \"faber0\" oifname \"faberbr0\" ip saddr {range} ip daddr {local} accept"),
        format!("iifname \"faberbr0\" oifname \"faber0\" ip saddr {local} ip daddr {range} accept"),
    ]
}

async fn post_status(url: &str, body: &serde_json::Value) -> (StatusCode, ErrorBody) {
    let resp = common::http().post(url).json(body).send().await.unwrap();
    let status = resp.status();
    let bytes = resp.bytes().await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("{status} {e}: {}", String::from_utf8_lossy(&bytes)));
    (status, body)
}

// (1)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn founder_brings_host_up_before_binding_and_seeds_mesh() {
    let mut n1 = founder().await;
    let id = n1.identity();
    assert_eq!(id.node_id, 1);
    assert_eq!(id.mesh_ip.to_string(), "10.99.0.1");

    // The peer listener is asked to bind the identity's mesh IP, whatever
    // other addresses the interface may carry.
    assert_eq!(
        *n1.intended.lock().unwrap(),
        Some("10.99.0.1:7401".parse().unwrap())
    );
    // The node recorded that it created the interface.
    assert_eq!(
        std::fs::read_to_string(n1.state_dir().join("iface")).unwrap(),
        "faber0\n"
    );

    let calls = n1.before_bind();
    let add = position(&calls, "ip link add faber0 type wireguard");
    let addr = position(&calls, "ip address replace 10.99.0.1/16 dev faber0");
    let sync = position(&calls, "wg syncconf faber0 /dev/stdin");
    let up = position(&calls, "ip link set faber0 mtu 1420 up");
    let nft = position(&calls, "nft -f -");
    assert!(
        add < addr && addr < sync && sync < up && up < nft,
        "order: {calls:#?}"
    );

    // The private key goes only to `wg` on stdin.
    let key = id.private_key.expose().to_string();
    for c in n1.host.calls() {
        assert!(!c.cmdline().contains(&key), "key in argv: {}", c.cmdline());
        let stdin = c
            .stdin
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_default();
        if c.prog == "wg" {
            assert!(stdin.contains(&key));
        } else {
            assert!(!stdin.contains(&key), "key on stdin of {}", c.cmdline());
        }
    }
    let nft_script = calls[nft]
        .stdin
        .clone()
        .map(String::from_utf8)
        .unwrap()
        .unwrap();
    assert!(nft_script.contains("table inet faber"), "{nft_script}");
    assert!(nft_script.contains("chain forward"), "{nft_script}");

    // InitCluster seeded the mesh.
    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    let me = &mesh.nodes[&1];
    assert_eq!(me.public_key, id.public_key().unwrap());
    assert_eq!(me.endpoint.as_deref(), Some(FOUNDER_ENDPOINT));
    assert_eq!(mesh.config.cidr.to_string(), "10.99.0.0/16");
    assert_eq!(mesh.config.wg_port, 51820);

    // The runtime step has faber-node write the containerd drop-in.
    eventually("runtime configured", || n1.runtime_configured()).await;

    // A second init is a conflict.
    let err = n1.client().cluster_init().await.unwrap_err().to_string();
    assert!(err.contains("409"), "{err}");

    n1.stop().await;
}

/// `cluster init` fixes the container range and gives the founder its first
/// subnet. The founder's host follows: the range is routed into the tunnel,
/// the firewall gets the container rules, and the CNI config appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_founder_gets_the_first_container_subnet() {
    let mut n1 = founder().await;
    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    assert_eq!(
        mesh.config.container_cidr,
        Some("10.200.0.0/16".parse().unwrap())
    );
    assert_eq!(
        mesh.nodes[&1].container_subnet,
        Some("10.200.0.0/24".parse().unwrap())
    );

    // The runtime step follows the network step of the same apply, so the
    // route and the table are there once the CNI config is.
    eventually("CNI config written", || n1.cni_subnet().is_some()).await;
    assert_eq!(n1.cni_subnet().as_deref(), Some("10.200.0.0/24"));

    let calls = n1.host.calls();
    let route = position(&calls, "ip route replace 10.200.0.0/16 dev faber0");
    let up = calls[..route]
        .iter()
        .rposition(|c| c.cmdline() == "ip link set faber0 mtu 1420 up")
        .expect("link up before the route");
    let nft = calls
        .iter()
        .rposition(|c| c.cmdline() == "nft -f -")
        .expect("nft ran");
    assert!(up < route && route < nft, "order: {calls:#?}");
    // Nothing else runs between link up and the route.
    assert_eq!(up + 1, route, "order: {calls:#?}");

    let script = String::from_utf8(calls[nft].stdin.clone().unwrap()).unwrap();
    assert!(script.contains("set node_mesh_ips {"), "{script}");
    // The only node so far.
    assert!(nft_set(&script, "node_mesh_ips").is_empty(), "{script}");
    for rule in local_subnet_rules("10.200.0.0/24") {
        assert!(script.contains(&rule), "{rule:?} not in {script}");
    }

    n1.stop().await;
}

/// A node that joins gets the next subnet at registration. Each side then
/// routes the other's subnet to it and lets the other node reach its
/// containers, and the joiner's CNI config appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_node_gets_the_next_subnet_and_both_sides_route_it() {
    let (mut n1, mut n2, invite) = two_nodes().await;
    let token = InviteToken::decode(&invite.token).unwrap();
    let pk1 = n1.identity().public_key().unwrap();
    let pk2 = public_key_of(&token.private_key).unwrap();

    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    assert_eq!(
        mesh.nodes[&2].container_subnet,
        Some("10.200.1.0/24".parse().unwrap())
    );

    eventually("node 1 routes node 2's subnet to it", || {
        peer_allowed_ips(&n1.host.last_syncconf(), &pk2).as_deref()
            == Some("10.99.0.2/32, 10.200.1.0/24")
    })
    .await;
    eventually("node 2 routes node 1's subnet to it", || {
        peer_allowed_ips(&n2.host.last_syncconf(), &pk1).as_deref()
            == Some("10.99.0.1/32, 10.200.0.0/24")
    })
    .await;

    // Each node keeps the CNI config of its own subnet.
    for (node, subnet, other) in [
        (&n1, "10.200.0.0/24", "10.200.1.0/24"),
        (&n2, "10.200.1.0/24", "10.200.0.0/24"),
    ] {
        eventually("CNI config has the node's subnet", || {
            node.cni_subnet().as_deref() == Some(subnet)
        })
        .await;
        assert_ne!(node.cni_subnet().as_deref(), Some(other));
    }

    // Each firewall lets the other node, and no one else, reach the local
    // containers.
    for (node, local, other) in [
        (&n1, "10.200.0.0/24", "10.99.0.2"),
        (&n2, "10.200.1.0/24", "10.99.0.1"),
    ] {
        eventually("firewall admits the other node", || {
            nft_set(&node.host.last_nft(), "node_mesh_ips") == [other]
        })
        .await;
        let script = node.host.last_nft();
        for rule in local_subnet_rules(local) {
            assert!(script.contains(&rule), "{rule:?} not in {script}");
        }
    }
    // The joiner routes the range too.
    position(
        &n2.host.calls(),
        "ip route replace 10.200.0.0/16 dev faber0",
    );

    n2.stop().await;
    n1.stop().await;
}

/// A machine that holds an invite but has not registered is a WireGuard
/// peer with its mesh address only: it holds no subnet, and the forward
/// chain gives it no way to the containers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_invite_gets_no_way_to_the_containers() {
    let mut n1 = founder().await;
    let invite = n1.invite("node-2", "198.51.100.2", None).await;
    let token = InviteToken::decode(&invite.token).unwrap();
    let pk2 = public_key_of(&token.private_key).unwrap();
    assert_eq!(invite.mesh_ip.to_string(), "10.99.0.2");

    eventually("invite peer added", || {
        n1.host.last_syncconf().contains(&pk2)
    })
    .await;
    assert_eq!(
        peer_allowed_ips(&n1.host.last_syncconf(), &pk2).as_deref(),
        Some("10.99.0.2/32")
    );
    eventually("firewall admits the invite into the tunnel", || {
        nft_set(&n1.host.last_nft(), "peer_mesh_ips") == ["10.99.0.2"]
    })
    .await;
    let script = n1.host.last_nft();
    assert!(script.contains("set node_mesh_ips {"), "{script}");
    assert!(nft_set(&script, "node_mesh_ips").is_empty(), "{script}");
    // Nothing in state gives it a subnet before it registers.
    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    assert!(!mesh.nodes.contains_key(&invite.node_id));

    n1.stop().await;
}

// (2)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invite_is_local_only_and_guarded() {
    let mut n1 = founder().await;
    let http = common::http();
    let body = serde_json::json!({"name": "node-2", "endpoint": "198.51.100.2"});

    // Works on the loopback listener; a bare IP gets the mesh wg_port.
    let invite = n1.invite("node-2", "198.51.100.2", None).await;
    let token = InviteToken::decode(&invite.token).unwrap();
    assert_eq!(invite.node_id, 2);
    assert_eq!(invite.mesh_ip.to_string(), "10.99.0.2");
    assert_eq!(token.endpoint, NODE2_ENDPOINT);
    assert_eq!(token.node_id, 2);
    assert_eq!(token.invite_id.len(), 32);
    assert_eq!(token.join_addrs, vec![n1.peer_addr().to_string()]);
    assert_eq!(token.peers.len(), 1);
    assert_eq!(token.peers[0].endpoint.as_deref(), Some(FOUNDER_ENDPOINT));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(invite.expires_at > now + 3500 && invite.expires_at <= now + 3601);

    // Not served on the peer listener.
    let resp = http
        .post(format!("http://{}/api/v1/mesh/invites", n1.peer_addr()))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let url = format!("http://{}/api/v1/mesh/invites", n1.api_addr());
    let forbidden = |resp: reqwest::Response| async move {
        let status = resp.status();
        let body: ErrorBody = resp.json().await.unwrap();
        assert_eq!(
            (status, body.error.as_str()),
            (StatusCode::FORBIDDEN, error_code::FORBIDDEN)
        );
    };
    // Bad Host.
    let port = n1.api_addr().port();
    for host in [
        format!("evil.example:{port}"),
        "evil.example".into(),
        format!("10.99.0.1:{port}"),
    ] {
        let resp = http
            .post(&url)
            .header(reqwest::header::HOST, host)
            .json(&body)
            .send()
            .await
            .unwrap();
        forbidden(resp).await;
    }
    // Any Origin, even a loopback one.
    let resp = http
        .post(&url)
        .header(reqwest::header::ORIGIN, format!("http://127.0.0.1:{port}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    forbidden(resp).await;
    // Sec-Fetch-Site other than none.
    let resp = http
        .post(&url)
        .header("sec-fetch-site", "same-origin")
        .json(&body)
        .send()
        .await
        .unwrap();
    forbidden(resp).await;
    // The guard covers the rest of the API too.
    let resp = http
        .get(format!("http://{}/api/v1/health", n1.api_addr()))
        .header(reqwest::header::HOST, "evil.example")
        .send()
        .await
        .unwrap();
    forbidden(resp).await;

    // Allowed Host forms and Sec-Fetch-Site: none.
    for host in [
        "localhost".to_string(),
        format!("localhost:{port}"),
        format!("127.0.0.1:{port}"),
        "[::1]".into(),
        format!("[::1]:{port}"),
    ] {
        let resp = http
            .get(format!("http://{}/api/v1/health", n1.api_addr()))
            .header(reqwest::header::HOST, &host)
            .header("sec-fetch-site", "none")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "host {host}");
    }

    // Peer listener: its own IP is fine, others are not.
    let peer = n1.peer_addr();
    let resp = http
        .get(format!("http://{peer}/raft/cluster/status"))
        .header(reqwest::header::HOST, "evil.example")
        .send()
        .await
        .unwrap();
    forbidden(resp).await;
    let resp = http
        .get(format!("http://{peer}/raft/cluster/status"))
        .header(reqwest::header::HOST, peer.ip().to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Hostname endpoints are refused.
    let (s, b) = post_status(
        &url,
        &serde_json::json!({"name": "node-2", "endpoint": "node2.example:51820"}),
    )
    .await;
    assert_eq!(
        (s, b.error.as_str()),
        (StatusCode::BAD_REQUEST, error_code::BAD_REQUEST)
    );
    // So are silly TTLs.
    for ttl in [0u64, 604_801] {
        let (s, b) = post_status(
            &url,
            &serde_json::json!({"name": "node-3", "endpoint": "198.51.100.3", "ttl_secs": ttl}),
        )
        .await;
        assert_eq!(
            (s, b.error.as_str()),
            (StatusCode::BAD_REQUEST, error_code::BAD_REQUEST),
            "ttl {ttl}"
        );
    }

    n1.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invite_without_mesh_is_mesh_not_configured() {
    let mut node = MeshNode::start(Bootstrap::NewMesh).await;
    let url = format!("http://{}/api/v1/mesh/invites", node.api_addr());
    // Not initialised: no leader yet, no mesh.
    let (s, b) = post_status(
        &url,
        &serde_json::json!({"name": "node-2", "endpoint": "198.51.100.2"}),
    )
    .await;
    assert_eq!(
        (s, b.error.as_str()),
        (StatusCode::CONFLICT, error_code::MESH_NOT_CONFIGURED)
    );
    node.stop().await;
}

// (3) + (4)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_joins_from_token_and_invites_are_single_use_and_expire() {
    let (mut n1, mut n2, invite) = two_nodes().await;
    let token = InviteToken::decode(&invite.token).unwrap();
    let pk1 = n1.identity().public_key().unwrap();
    let pk2 = public_key_of(&token.private_key).unwrap();
    assert_eq!(n2.identity().node_id, 2);
    assert_eq!(n2.identity().public_key().unwrap(), pk2);

    eventually("node 1 peers with node 2", || {
        let conf = n1.host.last_syncconf();
        conf.contains(&pk2) && conf.contains(&format!("Endpoint = {NODE2_ENDPOINT}"))
    })
    .await;
    eventually("node 2 peers with node 1", || {
        let conf = n2.host.last_syncconf();
        conf.contains(&pk1) && conf.contains(&format!("Endpoint = {FOUNDER_ENDPOINT}"))
    })
    .await;
    eventually("node 1 firewall allows node 2", || {
        let nft = n1.host.last_nft();
        nft.lines()
            .any(|l| l.contains("elements") && l.contains("198.51.100.2"))
            && nft
                .lines()
                .any(|l| l.contains("elements") && l.contains("10.99.0.2"))
    })
    .await;
    // Node 2 has the replicated mesh and the drop-in.
    eventually("node 2 has mesh", || {
        n2.handle()
            .state()
            .mesh()
            .unwrap()
            .is_some_and(|m| m.nodes.len() == 2)
    })
    .await;
    eventually("node 2 runtime configured", || n2.runtime_configured()).await;
    let n2_addr = n2.peer_addr();

    // The node record names the machine that registered.
    let instance2 = n2.identity().instance_id.expect("a joiner has one");
    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    assert_eq!(
        mesh.nodes[&2].instance_id.as_deref(),
        Some(instance2.as_str())
    );

    // (4) The invite is used. Another machine with the same token can
    // neither register with it nor join as the node it made.
    let register_url = format!("http://{}/raft/cluster/register", n1.peer_addr());
    let join_url = format!("http://{}/raft/cluster/join", n1.peer_addr());
    let used = (
        StatusCode::CONFLICT,
        Some(error_code::INVITE_USED.to_string()),
    );
    let invalid = (
        StatusCode::BAD_REQUEST,
        Some(error_code::INVITE_INVALID.to_string()),
    );
    let stolen = register_body(&token.invite_id, 2, NODE2_ENDPOINT, &other_machine());
    assert_eq!(post_code(&register_url, &stolen).await, used);
    let stolen = join_body(2, n2_addr, &token.invite_id, &other_machine());
    assert_eq!(post_code(&join_url, &stolen).await, used);
    // The machine that registered may repeat both: an answer can get lost.
    let again = register_body(&token.invite_id, 2, NODE2_ENDPOINT, &instance2);
    assert_eq!(
        post_code(&register_url, &again).await,
        (StatusCode::OK, None)
    );
    let again = join_body(2, n2_addr, &token.invite_id, &instance2);
    assert_eq!(
        post_code(&join_url, &again).await,
        (StatusCode::NO_CONTENT, None)
    );
    // Not with other details, though.
    let moved = register_body(&token.invite_id, 2, "198.51.100.9:51820", &instance2);
    assert_eq!(post_code(&register_url, &moved).await, used);

    // The register route is internal: the local API does not serve it.
    let local = format!("http://{}/raft/cluster/register", n1.api_addr());
    let resp = common::http()
        .post(&local)
        .json(&again)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // A join without an invite is refused in a mesh.
    let bare = serde_json::json!({"node_id": 9, "addr": "127.0.0.1:1"});
    assert_eq!(post_code(&join_url, &bare).await, invalid);

    // An unknown invite cannot be registered. A join for a node the mesh
    // has neither a record of nor an invite for is told so: that is what a
    // joiner looks like after the mesh removed it.
    let unknown = register_body(&"00".repeat(16), 9, "198.51.100.9:51820", &other_machine());
    assert_eq!(post_code(&register_url, &unknown).await, invalid);
    let unregistered = join_body(
        9,
        "127.0.0.1:1".parse().unwrap(),
        &"00".repeat(16),
        &other_machine(),
    );
    assert_eq!(
        post_code(&join_url, &unregistered).await,
        (
            StatusCode::CONFLICT,
            Some(error_code::MESH_NODE_REMOVED.to_string())
        )
    );

    // A short-lived invite is a wg peer until it expires. Under load the
    // peer may come and go before this test looks, so it does not wait for
    // the peer to appear (`a_live_invite_gets_no_way_to_the_containers`
    // does): it waits for the invite's time to pass, then for the leader to
    // have dropped the peer.
    let short = n1.invite("node-3", "198.51.100.3:51000", Some(3)).await;
    let short_token = InviteToken::decode(&short.token).unwrap();
    let pk3 = public_key_of(&short_token.private_key).unwrap();
    eventually("the invite's time has passed", || {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now >= short.expires_at
    })
    .await;
    eventually("expired invite dropped from wg", || {
        let conf = n1.host.last_syncconf();
        !conf.contains(&pk3) && conf.contains(&pk2)
    })
    .await;
    let late = register_body(
        &short_token.invite_id,
        short.node_id,
        "198.51.100.3:51000",
        &other_machine(),
    );
    assert_eq!(
        post_code(&register_url, &late).await,
        (
            StatusCode::GONE,
            Some(error_code::INVITE_EXPIRED.to_string())
        )
    );

    n2.stop().await;
    n1.stop().await;
}

// (5) + (6)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_invites_are_forwarded_and_restart_does_not_rejoin() {
    let (mut n1, mut n2, _) = two_nodes().await;

    // A joined node is part of an initialised cluster.
    let err = n2.client().cluster_init().await.unwrap_err().to_string();
    assert!(err.contains("409"), "{err}");

    // (5) Invite through node 2, a follower.
    n2.handle()
        .raft()
        .wait(Some(WAIT))
        .current_leader(1, "node 2 sees leader")
        .await
        .unwrap();
    let invite = n2.invite("node-4", "198.51.100.4", Some(600)).await;
    let token = InviteToken::decode(&invite.token).unwrap();
    assert_eq!(invite.node_id, 3);
    assert_eq!(token.join_addrs.first(), Some(&n1.peer_addr().to_string()));
    assert_eq!(token.join_addrs.len(), 2);
    assert_eq!(token.peers.len(), 2);
    let pk3 = public_key_of(&token.private_key).unwrap();
    // The private key never reached the leader's log or state.
    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    assert_eq!(mesh.invites.values().next().unwrap().public_key, pk3);
    assert!(
        !serde_json::to_string(&mesh)
            .unwrap()
            .contains(&token.private_key)
    );

    // Wait for node 2 to have the invite replicated, then restart it.
    eventually("node 2 sees invite", || {
        n2.handle()
            .state()
            .mesh()
            .unwrap()
            .is_some_and(|m| !m.invites.is_empty())
    })
    .await;
    n2.restart().await;

    // (6) wg comes up from the replicated state (the invite is not in the
    // token's bootstrap peers) before the peer listener binds.
    let calls = n2.before_bind();
    let sync = position(&calls, "wg syncconf faber0 /dev/stdin");
    let conf = String::from_utf8(calls[sync].stdin.clone().unwrap()).unwrap();
    assert!(conf.contains(&pk3), "{conf}");
    assert!(
        conf.contains(&n1.identity().public_key().unwrap()),
        "{conf}"
    );
    // faber0 already existed, so no `ip link add`.
    assert!(!calls.iter().any(|c| c.cmdline().starts_with("ip link add")));
    position(&calls, "nft -f -");
    assert!(n2.identity().pending_join.is_none());

    // Still a voter; no re-join needed.
    n2.handle()
        .raft()
        .wait(Some(WAIT))
        .current_leader(1, "node 2 back")
        .await
        .unwrap();
    let mut v = n2.voters();
    v.sort();
    assert_eq!(v, [1, 2]);

    // A registration that reaches a follower is forwarded to the leader.
    let register_url = format!("http://{}/raft/cluster/register", n2.peer_addr());
    let body = register_body(&token.invite_id, 3, &token.endpoint, &other_machine());
    assert_eq!(
        post_code(&register_url, &body).await,
        (StatusCode::OK, None)
    );
    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    assert_eq!(
        mesh.nodes[&3].instance_id.as_deref(),
        Some(other_machine().as_str())
    );
    assert!(mesh.invites.is_empty());

    n2.stop().await;
    n1.stop().await;
}

/// A second machine started from a token that another node already joined
/// with holds that node's key and mesh address, so the leader's raft RPCs
/// for the node can reach it. It must never act as the node: the mesh
/// refuses its registration, it takes no raft RPC, and it gives up the
/// identity and the interface that carries the other node's key. The real
/// node and the leader carry on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_machine_with_a_used_token_never_acts_as_the_node() {
    let (mut n1, mut n2, invite) = two_nodes().await;
    let id2 = n2.identity();

    let mut twin = MeshNode::start(Bootstrap::Token(invite.token.clone())).await;
    let identity_file = twin.state_dir().join("identity.json");
    eventually("the twin's identity is discarded", || {
        !identity_file.exists()
    })
    .await;
    assert_eq!(
        std::fs::read_to_string(twin.state_dir().join("bootstrap-error")).unwrap(),
        "invite_rejected\n"
    );
    assert!(twin.state_dir().join("join-failed").is_file());

    // The interface is removed before the identity goes, and it is no
    // longer recorded as the twin's.
    let lines = twin.cmdlines();
    assert_eq!(
        lines.last().map(String::as_str),
        Some("ip link del faber0"),
        "{lines:#?}"
    );
    assert!(!twin.host.link_up.load(Ordering::SeqCst));
    assert!(!twin.state_dir().join("iface").exists());
    // The host loop has stopped: teardown joins it before it removes the
    // interface, and `ip link del` is the last command, so nothing is left
    // that could bring the interface back, the periodic resync included.
    assert_eq!(twin.cmdlines(), lines);

    // It takes no raft RPC.
    for rpc in ["append", "vote", "snapshot"] {
        let url = format!("http://{}/raft/{rpc}", twin.peer_addr());
        let (s, b) = post_status(&url, &serde_json::json!({})).await;
        assert_eq!(
            (s, b.error.as_str()),
            (StatusCode::SERVICE_UNAVAILABLE, error_code::NOT_READY),
            "{rpc}"
        );
    }
    // Its own raft never heard of a cluster.
    let status = twin.client().cluster_status().await.unwrap();
    assert!(!status.joined);
    assert!(status.members.is_empty(), "{status:?}");
    assert_eq!(twin.log_position(), (0, None));

    // Node 2 and the leader are unaffected.
    assert_eq!(n2.identity(), id2);
    let mesh = n1.handle().state().mesh().unwrap().unwrap();
    assert_eq!(mesh.nodes[&2].instance_id, id2.instance_id);
    for node in [&n1, &n2] {
        let mut v = node.voters();
        v.sort();
        assert_eq!(v, [1, 2]);
        assert_eq!(node.client().health().await.unwrap().status, "ok");
    }
    // The cluster keeps committing: a new invite needs both voters.
    let later = n1.invite("node-5", "198.51.100.5", None).await;
    eventually("node 2 applies the new invite", || {
        n2.handle()
            .state()
            .mesh()
            .unwrap()
            .is_some_and(|m| m.invites.values().any(|i| i.node_id == later.node_id))
    })
    .await;

    twin.stop().await;
    n2.stop().await;
    n1.stop().await;
}

/// The leader commits a registration and the joiner stops before it joins.
/// Either the answer never reached it, so it has not recorded the
/// registration, or it has. Started again with the token, it registers once
/// more if it has to (the same machine, so the mesh accepts it), and the
/// join completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_that_registered_before_it_stopped_goes_on_to_join() {
    for recorded in [false, true] {
        let mut n1 = founder().await;
        let invite = n1.invite("node-2", "198.51.100.2", None).await;
        let token = InviteToken::decode(&invite.token).unwrap();

        // The first step, as the node made it before it stopped.
        let mut n2 = MeshNode::new();
        let mut made = make_identity(&n2, &invite.token);
        let instance = made.instance_id.clone().expect("a joiner has one");
        assert!(!made.pending_join.as_ref().unwrap().registered);
        let register_url = format!("http://{}/raft/cluster/register", n1.peer_addr());
        let body = register_body(&token.invite_id, 2, NODE2_ENDPOINT, &instance);
        assert_eq!(
            post_code(&register_url, &body).await,
            (StatusCode::OK, None)
        );
        if recorded {
            identity::mark_registered(&n2.state_dir(), &mut made).unwrap();
        }
        assert_eq!(n1.voters(), [1], "recorded: {recorded}");

        // The token file is still in place, as install.sh leaves it until
        // the node has joined.
        n2.launch(Bootstrap::Token(invite.token.clone()), None)
            .await;
        eventually("node 2 is a voter", || n1.sorted_voters() == [1, 2]).await;
        eventually("pending join cleared", || {
            n2.identity().pending_join.is_none()
        })
        .await;
        assert!(n2.voters().contains(&2));
        // Still the same machine, with the same key.
        assert_eq!(
            n2.identity().instance_id.as_deref(),
            Some(instance.as_str())
        );
        assert_eq!(n2.identity().private_key, made.private_key);
        assert!(n2.state_dir().join("identity.json").is_file());

        n2.stop().await;
        n1.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_endpoint_is_replicated_on_restart() {
    let mut n1 = founder().await;
    n1.stop().await;
    // Same identity, new --wg-endpoint.
    let config_endpoint = "203.0.113.50:51820";
    let dir_state: &Path = &n1.state_dir();
    assert!(dir_state.join("identity.json").is_file());
    let mut config = n1.config(&Bootstrap::Existing);
    config.wg_endpoint = Some(config_endpoint.into());
    let backends = n1.backends();
    let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    n1.handle = Some(
        start_with(config, api, PeerListener::Bound(peer), backends)
            .await
            .unwrap(),
    );
    eventually("endpoint updated", || {
        n1.handle()
            .state()
            .mesh()
            .unwrap()
            .is_some_and(|m| m.nodes[&1].endpoint.as_deref() == Some(config_endpoint))
    })
    .await;
    n1.stop().await;
}

/// `POST /raft/cluster/endpoint` is served on the peer listener only. On
/// loopback, where every node has the same address, the source of the
/// request is not checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_peer_endpoint_route_replicates_an_endpoint() {
    let mut n1 = founder().await;
    let body = serde_json::json!({"node_id": 1, "endpoint": "203.0.113.77:51820"});

    let resp = common::http()
        .post(format!("http://{}/raft/cluster/endpoint", n1.api_addr()))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(n1.replicated_endpoint(1).as_deref(), Some(FOUNDER_ENDPOINT));

    let resp = common::http()
        .post(format!("http://{}/raft/cluster/endpoint", n1.peer_addr()))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        n1.replicated_endpoint(1).as_deref(),
        Some("203.0.113.77:51820")
    );
    n1.stop().await;
}

/// A follower gets its new endpoint replicated through the leader, and
/// writes it once: it waits to see the write applied locally before it looks
/// again, instead of proposing the same change until it arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_replicates_its_new_endpoint_with_one_write() {
    let (mut n1, mut n2, _) = two_nodes().await;
    let new_endpoint = "198.51.100.77:51820";
    eventually("node 2 has itself in the mesh", || {
        n2.replicated_endpoint(2).as_deref() == Some(NODE2_ENDPOINT)
    })
    .await;

    let port = n2.peer_addr().port();
    n2.stop().await;
    n2.wg_endpoint = Some(new_endpoint.into());
    n2.launch(Bootstrap::Existing, Some(port)).await;

    eventually("node 2 sees its new endpoint", || {
        n2.replicated_endpoint(2).as_deref() == Some(new_endpoint)
    })
    .await;
    assert_eq!(n1.replicated_endpoint(2).as_deref(), Some(new_endpoint));
    // Let anything node 2 proposed again reach the log: an entry of our own
    // is committed behind it, and node 2 has applied that too.
    let barrier = n1.invite("node-3", "198.51.100.3", None).await;
    eventually("node 2 applies the barrier", || {
        n2.mesh()
            .invites
            .values()
            .any(|i| i.node_id == barrier.node_id)
    })
    .await;

    // Count what node 2 proposed in the logs, once both are shut down and
    // the databases are free. A leader change while node 2 was down puts the
    // entry in either log, so the most either holds is the cluster's count.
    let data = [n1.dir.path().join("data"), n2.dir.path().join("data")];
    n2.stop().await;
    n1.stop().await;
    let mut written = 0;
    for dir in &data {
        written = written.max(set_endpoint_entries(dir, 2, new_endpoint).await);
    }
    assert_eq!(written, 1);
}

/// A follower that still names the leader it can no longer reach answers a
/// forwarded write 503 `no_leader`, which a client retries, and not 502.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_forwarded_to_a_stopped_leader_is_no_leader() {
    let (mut n1, mut n2, _) = two_nodes().await;
    n1.stop().await;
    match n2.client().mesh_set_lease(600).await {
        Err(ClientError::Api { status, body }) => {
            assert_eq!((status, body.error.as_str()), (503, "no_leader"));
        }
        other => panic!("expected 503 no_leader, got {other:?}"),
    }
    n2.stop().await;
}

/// A workload write goes the same way: a deploy forwarded to a stopped
/// leader is 503 `no_leader`, not 502.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workload_write_forwarded_to_a_stopped_leader_is_no_leader() {
    use faber_api_types::workload::{CreateProjectRequest, DeployRequest, WorkloadSpec};

    let (mut n1, mut n2, _) = two_nodes().await;
    let project = CreateProjectRequest {
        name: "shop".into(),
        environments: None,
    };
    n2.client().project_create(&project).await.unwrap();
    n1.stop().await;
    let web: WorkloadSpec = serde_json::from_value(
        serde_json::json!({"containers": [{"name": "main", "image": "nginx:1.27"}]}),
    )
    .unwrap();
    let req = DeployRequest {
        workloads: [("web".to_string(), web)].into(),
        prune: false,
    };
    match n2.client().deploy("shop", "staging", &req).await {
        Err(ClientError::Api { status, body }) => {
            assert_eq!((status, body.error.as_str()), (503, "no_leader"));
        }
        other => panic!("expected 503 no_leader, got {other:?}"),
    }
    n2.stop().await;
}

/// How many `SetEndpoint` entries for node `id` with `endpoint` the log in
/// the data directory `data` holds. The node must not be running.
async fn set_endpoint_entries(data: &Path, id: u64, endpoint: &str) -> usize {
    use fabercp::raft::store;
    use fabercp::raft::types::Request;
    use openraft::EntryPayload;
    use openraft::storage::RaftLogReader;

    let mut log = store::open(data).unwrap().log_store;
    let entries = log.try_get_log_entries(..).await.unwrap();
    entries
        .iter()
        .filter(|e| {
            matches!(
                &e.payload,
                EntryPayload::Normal(Request::SetEndpoint { node_id, endpoint: ep })
                    if *node_id == id && ep == endpoint
            )
        })
        .count()
}

/// Raft was initialised but the process stopped before `InitCluster`: a
/// repeated init finishes the seed instead of answering already_initialized.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_completes_a_half_done_seed() {
    use std::collections::BTreeMap;

    use fabercp::raft::types::Node;

    let mut n1 = MeshNode::start(Bootstrap::NewMesh).await;
    let members = BTreeMap::from([(1, Node::new(n1.peer_addr().to_string()))]);
    n1.handle().raft().initialize(members).await.unwrap();
    assert!(n1.handle().state().mesh().unwrap().is_none());

    // Straight away, before the election has necessarily finished.
    n1.client().cluster_init().await.unwrap();
    let mesh = n1.handle().state().mesh().unwrap().expect("mesh seeded");
    assert_eq!(mesh.nodes[&1].endpoint.as_deref(), Some(FOUNDER_ENDPOINT));

    // Now it really is initialised.
    let err = n1.client().cluster_init().await.unwrap_err().to_string();
    assert!(err.contains("409"), "{err}");
    n1.stop().await;
}

/// A node started from a token cannot found its own cluster, and shuts down
/// cleanly while its join is still being retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joiner_cannot_init_and_stops_while_join_is_pending() {
    let mut n1 = founder().await;
    let invite = n1.invite("node-2", "198.51.100.2", None).await;
    n1.stop().await;

    let mut n2 = MeshNode::start(Bootstrap::Token(invite.token)).await;
    let url = format!("http://{}/api/v1/cluster/init", n2.api_addr());
    let (s, b) = post_status(&url, &serde_json::json!({})).await;
    assert_eq!(
        (s, b.error.as_str()),
        (StatusCode::BAD_REQUEST, error_code::BAD_REQUEST)
    );
    assert!(n2.identity().pending_join.is_some());
    // The mesh has not accepted its registration, so it takes no raft RPC.
    assert!(!n2.identity().pending_join.unwrap().registered);
    let (s, b) = post_status(
        &format!("http://{}/raft/vote", n2.peer_addr()),
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(
        (s, b.error.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, error_code::NOT_READY)
    );
    // faber0 came up from the token's bootstrap peers.
    let conf = n2.host.last_syncconf();
    assert!(
        conf.contains(&format!("Endpoint = {FOUNDER_ENDPOINT}")),
        "{conf}"
    );

    let started = std::time::Instant::now();
    n2.stop().await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
}

/// Startup applies the host state once; the reconcile loop starts from that
/// and has nothing to do until something changes. The loop makes its first
/// pass as soon as it starts, so a second apply of the startup state would
/// show within the second of lease clock this waits for, which is inside the
/// 2s resync of these tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_applies_the_host_state_once() {
    let mut node = found(MeshNode::fast()).await;
    eventually("runtime configured", || node.runtime_configured()).await;
    node.restart().await;
    lease_clock_runs(&node, 1_000).await;
    let lines = node.cmdlines();
    let count = |prefix: &str| lines.iter().filter(|l| l.starts_with(prefix)).count();
    assert_eq!(count("wg syncconf"), 1, "{lines:#?}");
    assert_eq!(count("nft -f"), 1, "{lines:#?}");
    node.stop().await;
}

/// An interface with our name that fabercp did not create is left alone:
/// startup fails before any address, WireGuard or nftables command.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interface_fabercp_did_not_create_is_refused() {
    let mut node = MeshNode::new();
    node.host.link_up.store(true, Ordering::SeqCst);
    let err = node
        .try_launch(Bootstrap::NewMesh, None)
        .await
        .expect_err("startup must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("interface faber0 exists and was not created by faber"),
        "{msg}"
    );
    assert_eq!(node.cmdlines(), vec!["ip link show faber0"]);
    // Nothing was written: no ownership record and no identity.
    assert!(!node.state_dir().join("iface").exists());
    assert!(!node.state_dir().join("identity.json").exists());
    assert!(node.before_bind.lock().unwrap().is_none());

    // Once the foreign interface is gone, the same node starts and takes
    // ownership.
    node.host.link_up.store(false, Ordering::SeqCst);
    node.launch(Bootstrap::NewMesh, None).await;
    position(&node.before_bind(), "ip link add faber0 type wireguard");
    assert!(node.state_dir().join("iface").is_file());
    node.stop().await;
}

/// After a reboot the link is gone but the ownership record is not: the
/// interface is created again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_interface_is_recreated_after_a_reboot() {
    let mut n1 = founder().await;
    let port = n1.peer_addr().port();
    n1.stop().await;
    n1.host.link_up.store(false, Ordering::SeqCst);
    n1.host.rec.clear();
    *n1.before_bind.lock().unwrap() = None;

    n1.launch(Bootstrap::Existing, Some(port)).await;
    let calls = n1.before_bind();
    let add = position(&calls, "ip link add faber0 type wireguard");
    let sync = position(&calls, "wg syncconf faber0 /dev/stdin");
    assert!(add < sync, "{calls:#?}");
    n1.stop().await;
}

/// A joiner whose invite the mesh rejects gives up its identity and its
/// WireGuard interface and stays idle. Restarted with a new token it starts
/// from nothing: a new identity, a new interface and empty raft storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_invite_discards_the_identity_and_a_new_token_joins() {
    let mut n1 = found(MeshNode::fast()).await;
    // A token for an invite the mesh does not have.
    let invite = n1.invite("node-2", "198.51.100.2", None).await;
    let mut forged = InviteToken::decode(&invite.token).unwrap();
    forged.invite_id = "0f".repeat(16);

    let mut n2 = MeshNode::fast();
    n2.launch(Bootstrap::Token(forged.encode()), None).await;
    let identity_file = n2.state_dir().join("identity.json");
    let marker = n2.state_dir().join("join-failed");
    let bootstrap_error = n2.state_dir().join("bootstrap-error");
    eventually("identity discarded", || !identity_file.exists()).await;
    assert!(marker.is_file());
    // The reason is there by the time the identity is gone.
    assert_eq!(
        std::fs::read_to_string(&bootstrap_error).unwrap(),
        "invite_rejected\n"
    );
    // The interface held the key of an invite that is not this node's.
    assert!(
        n2.cmdlines().iter().any(|l| l == "ip link del faber0"),
        "{:#?}",
        n2.cmdlines()
    );
    assert!(!n2.state_dir().join("iface").exists());
    // Idle, not dead: the API answers and the node is in no cluster.
    assert_eq!(n2.client().health().await.unwrap().node_id, 2);
    assert!(!n2.client().cluster_status().await.unwrap().joined);
    assert_eq!(n1.voters(), [1]);
    // It stays that way; nothing recreates the identity, however many
    // retries and resyncs would have had the time.
    lease_clock_runs(&n1, A_RESYNC).await;
    assert!(!identity_file.exists());

    n2.stop().await;
    // Something the old raft storage left behind.
    let leftover = n2.dir.path().join("data").join("leftover");
    std::fs::write(&leftover, "old").unwrap();

    // The operator gets a new invite and restarts with its token.
    let fresh = n1.invite("node-3", "198.51.100.3", None).await;
    n2.host.rec.clear();
    n2.launch(Bootstrap::Token(fresh.token.clone()), None).await;
    assert!(!marker.exists());
    assert!(!bootstrap_error.exists());
    assert!(!leftover.exists(), "raft data was not cleared");
    assert_eq!(n2.identity().node_id, fresh.node_id);
    assert_eq!(n2.identity().mesh_ip, fresh.mesh_ip);
    eventually("the new identity joins", || {
        let mut v = n1.voters();
        v.sort();
        v == [1, fresh.node_id]
    })
    .await;
    eventually("pending join cleared", || {
        n2.identity().pending_join.is_none()
    })
    .await;
    // It removed its interface when the invite was rejected, so it makes
    // one again and records it.
    position(&n2.before_bind(), "ip link add faber0 type wireguard");
    assert_eq!(
        std::fs::read_to_string(n2.state_dir().join("iface")).unwrap(),
        "faber0\n"
    );

    n2.stop().await;
    n1.stop().await;
}

/// A join that can never finish: the token's invite went to another node or
/// expired, and the leader's firewall drops this one, so no `invite_*`
/// answer ever comes. Restarting with the same token keeps trying; a
/// different token replaces the old identity and its raft data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_token_replaces_a_pending_join_that_never_completes() {
    let mut n1 = found(MeshNode::fast()).await;
    let invite = n1.invite("node-2", "198.51.100.2", None).await;
    // Nothing answers at the only join address.
    let mut stuck = InviteToken::decode(&invite.token).unwrap();
    stuck.join_addrs = vec!["127.0.0.1:1".into()];
    let stuck = stuck.encode();

    let mut n2 = MeshNode::fast();
    n2.launch(Bootstrap::Token(stuck.clone()), None).await;
    // Join attempts keep failing for as long as it takes the lease clock to
    // pass a resync.
    lease_clock_runs(&n1, A_RESYNC).await;
    let old = n2.identity();
    assert_eq!(old.node_id, invite.node_id);
    assert!(old.pending_join.is_some());
    assert_eq!(n1.voters(), [1]);

    // The same token again: the same identity, still pending.
    n2.stop().await;
    n2.launch(Bootstrap::Token(stuck), None).await;
    assert_eq!(n2.identity(), old);
    n2.stop().await;
    // Something the old raft storage left behind.
    let leftover = n2.dir.path().join("data").join("leftover");
    std::fs::write(&leftover, "old").unwrap();

    // The operator gets a new invite and restarts with its token.
    let fresh = n1.invite("node-3", "198.51.100.3", None).await;
    assert_ne!(fresh.node_id, old.node_id);
    n2.launch(Bootstrap::Token(fresh.token.clone()), None).await;
    assert!(!leftover.exists(), "raft data was not cleared");
    assert!(!n2.state_dir().join("join-failed").exists());
    assert_eq!(n2.identity().node_id, fresh.node_id);
    assert_eq!(n2.identity().mesh_ip, fresh.mesh_ip);
    eventually("the new identity joins", || {
        let mut v = n1.voters();
        v.sort();
        v == [1, fresh.node_id]
    })
    .await;
    eventually("pending join cleared", || {
        n2.identity().pending_join.is_none()
    })
    .await;

    n2.stop().await;
    n1.stop().await;
}

/// A node that is in the raft membership keeps its identity and its raft
/// data whatever token it is restarted with, even while `pending_join` is
/// still set (the process stopped before it could clear it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_token_never_replaces_the_identity_of_a_member() {
    let (mut n1, mut n2, invite) = two_nodes().await;
    let joined = n2.identity();
    let port = n2.peer_addr().port();
    // While both voters are up: one of two cannot commit an invite.
    let other = n1.invite("node-3", "198.51.100.3", None).await;
    n2.stop().await;

    // As if it had stopped just before clearing pending_join.
    let token = InviteToken::decode(&invite.token).unwrap();
    let mut pending = joined.clone();
    pending.pending_join = Some(PendingJoin {
        invite_id: token.invite_id,
        join_addrs: token.join_addrs,
        bootstrap_peers: token.peers,
        registered: false,
    });
    identity::save(&n2.state_dir(), &pending).unwrap();

    n2.launch(Bootstrap::Token(other.token), Some(port)).await;
    assert_eq!(n2.identity().node_id, joined.node_id);
    assert_eq!(n2.identity().private_key, joined.private_key);
    eventually("pending join cleared", || n2.identity() == joined).await;
    n2.handle()
        .raft()
        .wait(Some(WAIT))
        .metrics(|m| m.current_leader.is_some(), "a leader after the restart")
        .await
        .unwrap();
    let mut v = n2.voters();
    v.sort();
    assert_eq!(v, [1, 2]);

    n2.stop().await;
    n1.stop().await;
}

/// install.sh strips the bootstrap settings from fabercp.env only once the
/// cluster is created or joined, so a restart in between still carries them.
/// With an identity they are ignored: no new identity, no second join.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_settings_are_ignored_once_an_identity_exists() {
    let (mut n1, mut n2, invite) = two_nodes().await;
    let id1 = n1.identity();
    let id2 = n2.identity();

    // The founder restarts with --new-mesh still set.
    let port1 = n1.peer_addr().port();
    n1.stop().await;
    n1.launch(Bootstrap::NewMesh, Some(port1)).await;
    assert_eq!(n1.identity(), id1);

    // The joiner restarts with its (used) token still in place, and then
    // with the setting pointing at a file that is gone.
    for boot in [
        Bootstrap::Token(invite.token.clone()),
        Bootstrap::MissingTokenFile,
    ] {
        let port2 = n2.peer_addr().port();
        n2.stop().await;
        n2.launch(boot, Some(port2)).await;
        assert_eq!(n2.identity(), id2);
        assert!(!n2.state_dir().join("join-failed").exists());
    }

    n1.handle()
        .raft()
        .wait(Some(WAIT))
        .metrics(
            |m| m.current_leader.is_some(),
            "a leader after the restarts",
        )
        .await
        .unwrap();
    let mut v = n1.voters();
    v.sort();
    assert_eq!(v, [1, 2]);
    assert!(n1.handle().state().mesh().unwrap().unwrap().nodes.len() == 2);

    n2.stop().await;
    n1.stop().await;
}

/// A node that stops is removed once its lease has run out: from the raft
/// voters, from the mesh, and from the WireGuard config and firewall sets of
/// the nodes that are left. Its subnet goes to the next node that registers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_is_removed_when_its_lease_runs_out() {
    let (mut n1, mut n2, mut n3) = three_nodes(true).await;
    let pk3 = n3.identity().public_key().unwrap();
    assert_eq!(
        n1.mesh().nodes[&3].container_subnet,
        Some("10.200.2.0/24".parse().unwrap())
    );
    for node in [&n1, &n2] {
        eventually("node 3 is a peer", || {
            peer_allowed_ips(&node.host.last_syncconf(), &pk3).as_deref()
                == Some("10.99.0.3/32, 10.200.2.0/24")
        })
        .await;
    }

    n3.stop().await;
    set_lease(&[&n1, &n2], SHORT_LEASE_SECS).await;

    for node in [&n1, &n2] {
        eventually("node 3 left the raft membership", || {
            node.sorted_voters() == [1, 2]
        })
        .await;
        eventually("node 3 left the mesh", || {
            !node.mesh().nodes.contains_key(&3)
        })
        .await;
        eventually("the host dropped node 3", || {
            host_dropped(node, &pk3, "10.99.0.3")
        })
        .await;
    }
    // The nodes that kept renewing are all still there.
    let mesh = n1.mesh();
    assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), [1, 2]);

    // The subnet is free again: the next node to register gets it.
    let invite = n1.invite("node-4", "198.51.100.4", None).await;
    assert_eq!(invite.replaces_node_id, None);
    let token = InviteToken::decode(&invite.token).unwrap();
    let register_url = format!("http://{}/raft/cluster/register", n1.peer_addr());
    let body = register_body(
        &token.invite_id,
        invite.node_id,
        &token.endpoint,
        &other_machine(),
    );
    assert_eq!(
        post_code(&register_url, &body).await,
        (StatusCode::OK, None)
    );
    assert_eq!(
        n1.mesh().nodes[&invite.node_id].container_subnet,
        Some("10.200.2.0/24".parse().unwrap())
    );

    n2.stop().await;
    n1.stop().await;
}

/// A node whose lease ran out while it was stopped comes back. Its own raft
/// state still has it as a voter, so it learns that it was removed only from
/// the answer to its first renewal: `mesh_node_removed`. It then gives up
/// its interface and its identity and stays idle, and a new invite makes it
/// a new node.
///
/// The renewal gets through here because the fake hosts do not drop the
/// peer. On real hosts the other nodes have removed its WireGuard peer by
/// then, the renewal reaches nobody, and the node only warns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_removed_while_stopped_gives_up_its_identity_when_it_returns() {
    let (mut n1, mut n2, mut n3) = three_nodes(true).await;
    eventually("node 3 has the CNI config of its subnet", || {
        n3.cni_subnet().as_deref() == Some("10.200.2.0/24")
    })
    .await;
    let port = n3.peer_addr().port();
    n3.stop().await;
    set_lease(&[&n1, &n2], SHORT_LEASE_SECS).await;
    eventually("node 3 left the mesh", || {
        n1.sorted_voters() == [1, 2] && !n1.mesh().nodes.contains_key(&3)
    })
    .await;

    // The node still names node 1 as its leader until its election timer
    // runs out, and sends the renewal there. Give it time to do so.
    n3.election_ms = Some((5_000, 6_000));
    n3.host.rec.clear();
    assert!(n3.cni_subnet().is_some());
    n3.launch(Bootstrap::Existing, Some(port)).await;
    let identity_file = n3.state_dir().join("identity.json");
    eventually("the identity is discarded", || !identity_file.exists()).await;
    assert_eq!(n3.bootstrap_error().as_deref(), Some("node_removed\n"));
    assert!(n3.state_dir().join("join-failed").is_file());
    // The answer to its renewal is what it acted on.
    assert_eq!(n3.handle().left_mesh_because(), Some("renewal_refused"));
    // The CNI config for the subnet it used to hold was still there when
    // it started. The teardown removed it: the subnet may be another
    // node's by now.
    assert!(n3.cni_subnet().is_none());
    // The firewall table stays: only ever loaded, never deleted.
    assert!(
        n3.cmdlines()
            .iter()
            .all(|l| !l.starts_with("nft") || l == "nft -f -"),
        "{:#?}",
        n3.cmdlines()
    );
    // The interface went before the identity did.
    let lines = n3.cmdlines();
    assert_eq!(
        lines.last().map(String::as_str),
        Some("ip link del faber0"),
        "{lines:#?}"
    );
    assert!(!n3.host.link_up.load(Ordering::SeqCst));
    assert!(!n3.state_dir().join("iface").exists());
    // Idle, not dead: the API answers, and raft takes no more RPCs.
    assert_eq!(n3.client().health().await.unwrap().node_id, 3);
    let (s, b) = post_status(
        &format!("http://{}/raft/vote", n3.peer_addr()),
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(
        (s, b.error.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, error_code::NOT_READY)
    );
    // The host loop has stopped: teardown joins it before it removes the
    // interface, and `ip link del` is the last command, so nothing is left
    // that could bring the interface back, the periodic resync included.
    assert_eq!(n3.cmdlines(), lines);
    assert_eq!(n1.sorted_voters(), [1, 2]);

    // A restart without a new token. This node had joined, so install.sh
    // took the bootstrap settings out of its environment long ago: the
    // node stays idle and leaves the host alone.
    n3.stop().await;
    n3.host.rec.clear();
    assert!(!n3.launch_or_idle(Bootstrap::Existing).await);
    assert_eq!(n3.cmdlines(), Vec::<String>::new());
    assert!(!n3.host.link_up.load(Ordering::SeqCst));
    assert_eq!(n3.bootstrap_error().as_deref(), Some("node_removed\n"));

    // A restart with a new invite starts from nothing.
    let fresh = n1.invite("node-3", "198.51.100.3", None).await;
    n3.election_ms = None;
    n3.launch(Bootstrap::Token(fresh.token.clone()), None).await;
    assert_eq!(n3.bootstrap_error(), None);
    assert_eq!(n3.identity().node_id, fresh.node_id);
    eventually("the new identity joins", || {
        n1.sorted_voters() == [1, 2, fresh.node_id]
    })
    .await;
    eventually("pending join cleared", || {
        n3.identity().pending_join.is_none()
    })
    .await;
    assert_eq!(
        n1.mesh().nodes[&fresh.node_id].container_subnet,
        Some("10.200.2.0/24".parse().unwrap())
    );

    n3.stop().await;
    n2.stop().await;
    n1.stop().await;
}

/// A new machine registers with an invite for a name that a stopped node
/// holds. It gets the name and the subnet of that node, and a node id, key
/// and mesh address of its own. The old node is gone from raft and from the
/// mesh, and the others route the subnet to the new machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_machine_takes_over_the_name_and_subnet_of_a_stopped_node() {
    let (mut n1, mut n2, mut n3) = three_nodes(false).await;
    let pk3 = n3.identity().public_key().unwrap();
    n3.stop().await;

    let invite = n1.invite("node-3", "198.51.100.33", None).await;
    assert_eq!(invite.name, "node-3");
    assert_eq!(invite.replaces_node_id, Some(3));
    assert_eq!(invite.node_id, 4);
    let token = InviteToken::decode(&invite.token).unwrap();
    let pk4 = public_key_of(&token.private_key).unwrap();

    let mut n4 = MeshNode::start(Bootstrap::Token(invite.token.clone())).await;
    for node in [&n1, &n2] {
        eventually("the new machine replaced node 3 as a voter", || {
            node.sorted_voters() == [1, 2, 4]
        })
        .await;
    }
    eventually("pending join cleared", || {
        n4.identity().pending_join.is_none()
    })
    .await;

    let mesh = n1.mesh();
    assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), [1, 2, 4]);
    let new = &mesh.nodes[&4];
    assert_eq!(new.name, "node-3");
    assert_eq!(new.container_subnet, Some("10.200.2.0/24".parse().unwrap()));
    assert_eq!(new.mesh_ip.to_string(), "10.99.0.4");
    assert_eq!(new.public_key, pk4);
    assert_ne!(pk4, pk3);

    for node in [&n1, &n2] {
        eventually("the subnet is routed to the new machine", || {
            peer_allowed_ips(&node.host.last_syncconf(), &pk4).as_deref()
                == Some("10.99.0.4/32, 10.200.2.0/24")
        })
        .await;
        eventually("the host dropped node 3", || {
            host_dropped(node, &pk3, "10.99.0.3")
        })
        .await;
    }
    // The new machine has the CNI config of the subnet it took over.
    eventually("CNI config has the subnet", || {
        n4.cni_subnet().as_deref() == Some("10.200.2.0/24")
    })
    .await;

    n4.stop().await;
    n2.stop().await;
    n1.stop().await;
}

/// Three nodes that keep renewing lose nobody, the leader included, however
/// many lease periods pass. The lease is shortened on the running cluster:
/// every renew loop picks the new duration up, or the nodes that kept the
/// old interval (seven seconds here) would be removed after two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_that_renew_are_never_removed_and_follow_a_shorter_lease() {
    let (mut n1, mut n2, mut n3) = three_nodes(true).await;
    assert_eq!(n1.client().mesh_lease().await.unwrap().lease_secs, 28_800);
    n2.client().mesh_set_lease(SHORT_LEASE_SECS).await.unwrap();
    assert_eq!(
        n1.client().mesh_lease().await.unwrap().lease_secs,
        SHORT_LEASE_SECS
    );

    lease_clock_runs(&n1, SHORT_LEASE_SECS * 7 / 2).await;

    let mesh = n1.mesh();
    assert!(mesh.clock >= 3 * SHORT_LEASE_SECS, "clock {}", mesh.clock);
    assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), [1, 2, 3]);
    for record in mesh.nodes.values() {
        assert!(!mesh.lease_expired(record), "{record:?} at {}", mesh.clock);
        // Renewed within the last lease period, long after the change.
        assert!(
            record.lease_renewed_at > mesh.clock - SHORT_LEASE_SECS,
            "{record:?} at {}",
            mesh.clock
        );
    }
    for node in [&n1, &n2, &n3] {
        assert_eq!(node.sorted_voters(), [1, 2, 3]);
        assert_eq!(node.bootstrap_error(), None);
        assert!(node.state_dir().join("identity.json").is_file());
        assert!(node.host.link_up.load(Ordering::SeqCst));
    }

    n3.stop().await;
    n2.stop().await;
    n1.stop().await;
}

/// A node that cannot reach a leader for longer than its lease has no
/// evidence that it was removed, and tears nothing down. The lease clock
/// stands still while there is no leader, so when the others come back
/// every node renews and nobody is removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_without_a_leader_keeps_its_identity_and_the_cluster_recovers() {
    let (mut n1, mut n2, mut n3) = three_nodes(true).await;
    n1.client().mesh_set_lease(SHORT_LEASE_SECS).await.unwrap();
    eventually("node 3 follows the shorter lease", || {
        n3.mesh().lease_secs == SHORT_LEASE_SECS
    })
    .await;
    // Every lease runs while the first node back leads and the second is
    // still opening its store, so stop with all three just renewed: each
    // lease then has most of its period left for that gap.
    eventually("every lease was just renewed", || {
        let mesh = n3.mesh();
        mesh.nodes.len() == 3
            && mesh
                .nodes
                .values()
                .all(|r| mesh.clock - r.lease_renewed_at <= SHORT_LEASE_SECS / 8)
    })
    .await;
    let (port1, port2) = (n1.peer_addr().port(), n2.peer_addr().port());
    n1.stop().await;
    n2.stop().await;
    let commands = n3.cmdlines().len();

    // The sleep is the test: with no leader there is no lease clock to
    // count by, so this is the one wait that has to be wall time. Node 3
    // goes without an answer for longer than its lease.
    tokio::time::sleep(SHORT_LEASE * 5 / 2).await;
    assert_eq!(n3.bootstrap_error(), None);
    assert!(n3.state_dir().join("identity.json").is_file());
    assert!(n3.host.link_up.load(Ordering::SeqCst));
    assert!(
        !n3.cmdlines()[commands..]
            .iter()
            .any(|l| l.starts_with("ip link del")),
        "{:#?}",
        n3.cmdlines()
    );

    // Together, so that the node that opens first leads for as short a
    // time as possible before the other renews.
    tokio::join!(
        n1.launch(Bootstrap::Existing, Some(port1)),
        n2.launch(Bootstrap::Existing, Some(port2))
    );
    eventually("a leader again", || {
        n3.handle()
            .raft()
            .metrics()
            .borrow()
            .current_leader
            .is_some()
    })
    .await;
    let clock = n3.mesh().clock;
    eventually("another lease period passed", || {
        n3.mesh().clock >= clock + SHORT_LEASE_SECS + SHORT_LEASE_SECS / 2
    })
    .await;
    // Every node has applied what the leader has written, so a view that
    // differs is a removal and not a node that is behind.
    let written = [&n1, &n2, &n3]
        .iter()
        .filter_map(|n| n.handle().raft().metrics().borrow().last_log_index)
        .max()
        .unwrap();
    for (name, node) in [("n1", &n1), ("n2", &n2), ("n3", &n3)] {
        eventually(&format!("{name} applied index {written}"), || {
            let m = node.handle().raft().metrics().borrow().clone();
            m.last_applied.is_some_and(|id| id.index >= written)
        })
        .await;
    }
    for (name, node) in [("n1", &n1), ("n2", &n2), ("n3", &n3)] {
        let m = node.handle().raft().metrics().borrow().clone();
        let mesh = node.mesh();
        let renewed: Vec<_> = mesh
            .nodes
            .values()
            .map(|r| (r.node_id, r.lease_renewed_at))
            .collect();
        assert_eq!(
            node.sorted_voters(),
            [1, 2, 3],
            "{name}: leader {:?}, last applied {:?}, mesh nodes {renewed:?}, clock {}",
            m.current_leader,
            m.last_applied,
            mesh.clock
        );
        assert_eq!(
            mesh.nodes.len(),
            3,
            "{name}: leader {:?}, mesh nodes {renewed:?}, clock {}",
            m.current_leader,
            mesh.clock
        );
        assert_eq!(node.bootstrap_error(), None, "{name}");
    }

    n3.stop().await;
    n2.stop().await;
    n1.stop().await;
}

/// The founder's name comes from `--node-name`, every other node's from its
/// invite. `cluster status` shows both, on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn names_come_from_the_founder_flag_and_from_the_invite() {
    let mut first = MeshNode::new();
    first.node_name = Some("alpha".into());
    let mut n1 = found(first).await;
    // The record carries the machine's instance id, which the founder
    // renews its lease with.
    let record = n1.mesh().nodes[&1].clone();
    assert_eq!(record.name, "alpha");
    assert_eq!(record.instance_id, n1.identity().instance_id);
    assert!(record.instance_id.is_some());

    let mut n2 = join(&n1, MeshNode::new(), "beta", "198.51.100.2").await;
    assert_eq!(n1.mesh().nodes[&2].name, "beta");
    for node in [&n1, &n2] {
        eventually("status names both members", || node.mesh().nodes.len() == 2).await;
        let status = node.client().cluster_status().await.unwrap();
        let names: Vec<_> = status
            .members
            .iter()
            .map(|m| (m.node_id, m.name.clone()))
            .collect();
        assert_eq!(
            names,
            [
                (1, Some("alpha".to_string())),
                (2, Some("beta".to_string()))
            ]
        );
    }

    // A name that is not a DNS label stops the founder before it starts.
    let mut bad = MeshNode::new();
    bad.node_name = Some("Not_A_Name".into());
    let err = bad
        .try_launch(Bootstrap::NewMesh, None)
        .await
        .expect_err("startup must fail");
    assert!(format!("{err:#}").contains("--node-name"), "{err:#}");
    assert!(!bad.state_dir().join("identity.json").exists());

    n2.stop().await;
    n1.stop().await;
}

/// What a node the mesh has removed looks like: no identity, the reason in
/// the marker, no interface, and raft takes no more RPCs. The process is
/// still up.
async fn assert_removed_and_idle(node: &MeshNode) {
    assert!(!node.state_dir().join("identity.json").exists());
    assert_eq!(node.bootstrap_error().as_deref(), Some("node_removed\n"));
    assert!(node.state_dir().join("join-failed").is_file());
    let lines = node.cmdlines();
    assert_eq!(
        lines.last().map(String::as_str),
        Some("ip link del faber0"),
        "{lines:#?}"
    );
    assert!(!node.host.link_up.load(Ordering::SeqCst));
    assert!(!node.state_dir().join("iface").exists());
    // Its CNI config went with the subnet, and the firewall table stayed.
    assert!(node.cni_subnet().is_none());
    assert!(
        lines
            .iter()
            .all(|l| !l.starts_with("nft") || l == "nft -f -"),
        "{lines:#?}"
    );
    let (s, b) = post_status(
        &format!("http://{}/raft/vote", node.peer_addr()),
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(
        (s, b.error.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, error_code::NOT_READY)
    );
    node.client().health().await.unwrap();
}

/// A new machine takes over the name of a node that is still running. The
/// old node is a follower: the last membership it gets from raft is the
/// joint config that drops it. Once that is applied it knows, gives up its
/// interface and its identity, and stays idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_node_whose_name_is_taken_over_tears_itself_down() {
    let (mut n1, mut n2, mut n3) = three_nodes(false).await;
    let pk3 = n3.identity().public_key().unwrap();
    eventually("node 3 has the CNI config of its subnet", || {
        n3.cni_subnet().as_deref() == Some("10.200.2.0/24")
    })
    .await;

    let invite = n1.invite("node-3", "198.51.100.33", None).await;
    assert_eq!(invite.replaces_node_id, Some(3));
    let mut n4 = MeshNode::start(Bootstrap::Token(invite.token.clone())).await;
    for node in [&n1, &n2] {
        eventually("the new machine replaced node 3 as a voter", || {
            node.sorted_voters() == [1, 2, invite.node_id]
        })
        .await;
    }
    eventually("pending join cleared", || {
        n4.identity().pending_join.is_none()
    })
    .await;
    let mesh = n1.mesh();
    assert!(!mesh.nodes.contains_key(&3));
    assert_eq!(mesh.nodes[&invite.node_id].name, "node-3");
    assert_eq!(
        mesh.nodes[&invite.node_id].container_subnet,
        Some("10.200.2.0/24".parse().unwrap())
    );

    let identity_file = n3.state_dir().join("identity.json");
    eventually("the old node discards its identity", || {
        !identity_file.exists()
    })
    .await;
    assert_removed_and_idle(&n3).await;
    // The old node acted on the membership change, not on a refused
    // renewal. The leader stopped replicating to it before the record
    // went, so it still has itself in its copy of the mesh.
    assert_eq!(n3.handle().left_mesh_because(), Some("left_membership"));
    assert!(n3.mesh().nodes.contains_key(&3));
    for node in [&n1, &n2] {
        eventually("the host dropped the old node", || {
            host_dropped(node, &pk3, "10.99.0.3")
        })
        .await;
        assert_eq!(node.sorted_voters(), [1, 2, invite.node_id]);
        assert_eq!(node.bootstrap_error(), None);
    }

    n4.stop().await;
    n3.stop().await;
    n2.stop().await;
    n1.stop().await;
}

/// A new machine takes over the name of the leader. The leader removes
/// itself from raft, answers `not_ready` and steps down; the joiner goes on
/// to the next join address and completes against the leader the other two
/// elect. The old leader applies the membership without itself and tears
/// itself down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn taking_over_the_leaders_name_completes_against_the_next_leader() {
    let (mut n1, mut n2, mut n3) = three_nodes(false).await;
    assert_eq!(
        n2.handle().raft().metrics().borrow().current_leader,
        Some(1)
    );
    eventually("node 1 has the CNI config of its subnet", || {
        n1.cni_subnet().as_deref() == Some("10.200.0.0/24")
    })
    .await;

    // Asked of a follower; the token still lists the leader first.
    let invite = n2.invite("node-1", "198.51.100.11", None).await;
    assert_eq!(invite.replaces_node_id, Some(1));
    let token = InviteToken::decode(&invite.token).unwrap();
    assert_eq!(token.join_addrs.first(), Some(&n1.peer_addr().to_string()));
    assert_eq!(token.join_addrs.len(), 3);

    let mut n4 = MeshNode::start(Bootstrap::Token(invite.token.clone())).await;
    for node in [&n2, &n3] {
        eventually("the new machine replaced node 1 as a voter", || {
            node.sorted_voters() == [2, 3, invite.node_id]
        })
        .await;
    }
    eventually("pending join cleared", || {
        n4.identity().pending_join.is_none()
    })
    .await;
    let leader = n2.handle().raft().metrics().borrow().current_leader;
    assert!(matches!(leader, Some(2 | 3)), "{leader:?}");

    let mesh = n2.mesh();
    assert_eq!(
        mesh.nodes.keys().copied().collect::<Vec<_>>(),
        [2, 3, invite.node_id]
    );
    let new = &mesh.nodes[&invite.node_id];
    assert_eq!(new.name, "node-1");
    assert_eq!(new.container_subnet, Some("10.200.0.0/24".parse().unwrap()));
    assert_eq!(new.mesh_ip, invite.mesh_ip);

    let identity_file = n1.state_dir().join("identity.json");
    eventually("the old leader discards its identity", || {
        !identity_file.exists()
    })
    .await;
    assert_removed_and_idle(&n1).await;
    assert_eq!(n1.handle().left_mesh_because(), Some("left_membership"));
    // The cluster keeps committing without it.
    n3.invite("node-5", "198.51.100.5", None).await;

    n4.stop().await;
    n3.stop().await;
    n2.stop().await;
    n1.stop().await;
}

/// A joiner registered and then stopped before it joined. Nothing renews a
/// lease while a join is pending, so the lease from the registration runs
/// out and the mesh removes the node. When the machine comes back its join
/// is refused for good, and it gives up as a removed node does, not as one
/// that retries for ever. A restart after that ends idle as well, with the
/// token still configured and without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_removed_before_it_joined_gives_up_its_identity() {
    let mut n1 = found(MeshNode::fast()).await;
    let invite = n1.invite("node-2", "198.51.100.2", None).await;
    let token = InviteToken::decode(&invite.token).unwrap();

    // The first step, as the node made it before it stopped.
    let mut n2 = MeshNode::fast();
    let mut made = make_identity(&n2, &invite.token);
    let instance = made.instance_id.clone().expect("a joiner has one");
    let register_url = format!("http://{}/raft/cluster/register", n1.peer_addr());
    let body = register_body(&token.invite_id, 2, NODE2_ENDPOINT, &instance);
    assert_eq!(
        post_code(&register_url, &body).await,
        (StatusCode::OK, None)
    );
    identity::mark_registered(&n2.state_dir(), &mut made).unwrap();
    assert!(n1.mesh().nodes.contains_key(&2));

    n1.client().mesh_set_lease(SHORT_LEASE_SECS).await.unwrap();
    eventually("node 2 left the mesh", || !n1.mesh().nodes.contains_key(&2)).await;

    n2.launch(Bootstrap::Token(invite.token.clone()), None)
        .await;
    let identity_file = n2.state_dir().join("identity.json");
    eventually("the identity is discarded", || !identity_file.exists()).await;
    assert_eq!(n2.bootstrap_error().as_deref(), Some("node_removed\n"));
    assert_eq!(n2.handle().left_mesh_because(), Some("join_refused"));
    assert!(n2.state_dir().join("join-failed").is_file());
    let lines = n2.cmdlines();
    assert_eq!(
        lines.last().map(String::as_str),
        Some("ip link del faber0"),
        "{lines:#?}"
    );
    assert!(!n2.state_dir().join("iface").exists());
    // Idle, not dead, and in no cluster.
    assert_eq!(n2.client().health().await.unwrap().node_id, 2);
    assert_eq!(n1.voters(), [1]);
    assert!(!n1.mesh().nodes.contains_key(&2));

    // install.sh leaves the token setting and the token in place after a
    // join that failed. A restart makes the same identity again, tries the
    // invite, which the mesh no longer has, and ends idle once more. It
    // does not exit and get started again for ever.
    n2.stop().await;
    assert!(
        n2.launch_or_idle(Bootstrap::Token(invite.token.clone()))
            .await
    );
    eventually("the identity is discarded again", || {
        n2.handle().left_mesh_because().is_some() && !identity_file.exists()
    })
    .await;
    assert_eq!(n2.handle().left_mesh_because(), Some("invite_rejected"));
    assert_eq!(n2.bootstrap_error().as_deref(), Some("invite_rejected\n"));
    assert!(!n2.host.link_up.load(Ordering::SeqCst));
    assert_eq!(n2.client().health().await.unwrap().node_id, 2);

    // With the token setting gone as well there is nothing to start as,
    // and the host is left alone.
    n2.stop().await;
    n2.host.rec.clear();
    assert!(!n2.launch_or_idle(Bootstrap::Existing).await);
    assert_eq!(n2.cmdlines(), Vec::<String>::new());
    assert_eq!(n2.bootstrap_error().as_deref(), Some("invite_rejected\n"));

    n1.stop().await;
}

/// The MCP tools reach the mesh service: an invite maps its arguments, and
/// the lease reads and sets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_tools_invite_and_set_the_lease_in_a_mesh() {
    use rmcp::ServiceExt;
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::StreamableHttpClientTransport;

    let mut n1 = founder().await;
    let transport =
        StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", n1.api_addr()));
    let mcp = ().serve(transport).await.unwrap();
    let call = |tool: &'static str, args: serde_json::Value| {
        let mcp = &mcp;
        async move {
            let params =
                CallToolRequestParams::new(tool).with_arguments(args.as_object().unwrap().clone());
            let result = mcp.call_tool(params).await.unwrap();
            assert_eq!(result.is_error, Some(false), "{tool}: {result:?}");
            let text = result.content[0].as_text().unwrap().text.clone();
            serde_json::from_str::<serde_json::Value>(&text).unwrap()
        }
    };

    let invite = call(
        "mesh_invite",
        serde_json::json!({"name": "node-2", "wg_endpoint": NODE2_ENDPOINT, "ttl_secs": 600}),
    )
    .await;
    assert_eq!(invite["name"], "node-2");
    assert!(invite["mesh_ip"].is_string());
    let token = InviteToken::decode(invite["token"].as_str().unwrap()).unwrap();
    assert_eq!(token.endpoint, NODE2_ENDPOINT);

    let lease = call("mesh_lease", serde_json::json!({})).await;
    assert!(lease["lease_secs"].is_u64());
    let set = call("mesh_lease", serde_json::json!({"set_secs": 4000})).await;
    assert_eq!(set["lease_secs"], 4000);
    let got = call("mesh_lease", serde_json::json!({})).await;
    assert_eq!(got["lease_secs"], 4000);
    assert_eq!(n1.client().mesh_lease().await.unwrap().lease_secs, 4000);

    mcp.cancel().await.unwrap();
    n1.stop().await;
}
