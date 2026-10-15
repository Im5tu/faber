//! Command-line and environment configuration (`FABER_*`).

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Component, Path, PathBuf};

use clap::builder::BoolishValueParser;
use clap::{ArgAction, Parser};
use faber_network_management::DEFAULT_WG_INTERFACE;
use ipnet::Ipv4Net;

use crate::lease::LeaseTiming;
use crate::mesh::identity::{BootstrapInput, Identity};
use crate::mesh::state::{DEFAULT_CONTAINER_SUBNET_PREFIX, MAX_SERVICE_CIDR_PREFIX};
use crate::mesh::{MeshState, parse_endpoint};
use crate::raft::types::NodeId;

pub use crate::mesh::state::{
    DEFAULT_CONTAINER_CIDR, DEFAULT_MESH_CIDR, DEFAULT_SERVICE_CIDR, DEFAULT_WG_PORT,
};
pub use faber_api_types::DEFAULT_PEER_PORT;

/// Hosts the MCP endpoint always accepts in the `Host` header. Guards against
/// DNS rebinding from a browser on the same machine.
pub const DEFAULT_MCP_ALLOWED_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "::1"];

/// Default `--api-listen`.
pub const DEFAULT_API_LISTEN: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::LOCALHOST),
    faber_api_types::DEFAULT_PORT,
);
/// Default `--data-dir`.
pub const DEFAULT_DATA_DIR: &str = "./data";
/// Default `--state-dir`.
pub const DEFAULT_STATE_DIR: &str = "/var/lib/faber";
/// Name of the cluster key file in `--state-dir`.
pub const CLUSTER_KEY_FILE: &str = "cluster.key";
/// Default `--containerd-conf-dir`.
pub const DEFAULT_CONTAINERD_CONF_DIR: &str = "/etc/containerd/conf.d";
/// Default `--cni-conf-dir`.
pub const DEFAULT_CNI_CONF_DIR: &str = "/etc/cni/net.d";
/// Default `--heartbeat-ms`.
pub const DEFAULT_HEARTBEAT_MS: u64 = 250;
/// Default `--election-min-ms`.
pub const DEFAULT_ELECTION_MIN_MS: u64 = 1000;
/// Default `--election-max-ms`.
pub const DEFAULT_ELECTION_MAX_MS: u64 = 2000;
/// Default `--snapshot-logs`.
pub const DEFAULT_SNAPSHOT_LOGS: u64 = 5000;
/// Default `--keep-logs`.
pub const DEFAULT_KEEP_LOGS: u64 = 1000;
/// Default `--reconcile-interval-secs`.
pub const DEFAULT_RECONCILE_INTERVAL_SECS: u64 = 300;

#[derive(Debug, Clone, Parser)]
#[command(name = "fabercp", version, about = "Faber control plane")]
pub struct Config {
    /// Unique Raft node id of this node. Read from identity.json when there
    /// is one; required otherwise (dev mode).
    #[arg(long, env = "FABER_NODE_ID")]
    pub node_id: Option<NodeId>,

    /// Local listener for /api/v1 and /mcp. Must be a loopback address.
    #[arg(long, env = "FABER_API_LISTEN", default_value_t = DEFAULT_API_LISTEN)]
    pub api_listen: SocketAddr,

    /// WireGuard interface. In mesh mode fabercp creates it (default
    /// `faber0`), refuses to use one it did not create, and binds the peer
    /// listener to the identity's mesh IP. Outside mesh mode the peer
    /// listener binds to this interface's address.
    #[arg(long, env = "FABER_WG_INTERFACE", conflicts_with = "peer_listen")]
    pub wg_interface: Option<String>,

    /// Port of the peer listener when using --wg-interface (or the loopback default).
    #[arg(long, env = "FABER_PEER_PORT", default_value_t = DEFAULT_PEER_PORT)]
    pub peer_port: u16,

    /// Explicit peer listener address (/raft, /api/v1, /mcp). Must be loopback
    /// or an address on a WireGuard interface. Without this or
    /// --wg-interface, the peer listener binds 127.0.0.1:<peer-port>.
    #[arg(long, env = "FABER_PEER_LISTEN")]
    pub peer_listen: Option<SocketAddr>,

    /// Directory holding the RocksDB database.
    #[arg(long, env = "FABER_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,

    /// Raft leader heartbeat interval.
    #[arg(long, env = "FABER_HEARTBEAT_MS", default_value_t = DEFAULT_HEARTBEAT_MS)]
    pub heartbeat_ms: u64,

    /// Lower bound of the randomised election timeout.
    #[arg(long, env = "FABER_ELECTION_MIN_MS", default_value_t = DEFAULT_ELECTION_MIN_MS)]
    pub election_min_ms: u64,

    /// Upper bound of the randomised election timeout.
    #[arg(long, env = "FABER_ELECTION_MAX_MS", default_value_t = DEFAULT_ELECTION_MAX_MS)]
    pub election_max_ms: u64,

    /// Build a snapshot after this many new log entries.
    #[arg(long, env = "FABER_SNAPSHOT_LOGS", default_value_t = DEFAULT_SNAPSHOT_LOGS)]
    pub snapshot_logs: u64,

    /// Log entries to keep after a snapshot, so slightly lagging followers can
    /// catch up without a full snapshot transfer.
    #[arg(long, env = "FABER_KEEP_LOGS", default_value_t = DEFAULT_KEEP_LOGS)]
    pub keep_logs: u64,

    /// Extra `Host` header value accepted by /mcp (repeatable). Loopback
    /// names and the peer listener address are always accepted.
    #[arg(
        long = "mcp-allowed-host",
        env = "FABER_MCP_ALLOWED_HOSTS",
        value_delimiter = ','
    )]
    pub mcp_allowed_hosts: Vec<String>,

    /// Directory holding identity.json.
    #[arg(long, env = "FABER_STATE_DIR", default_value = DEFAULT_STATE_DIR)]
    pub state_dir: PathBuf,

    /// File holding the cluster key, 32 bytes as base64. It seals and opens
    /// the data keys of the environments, and is read when a deploy or a
    /// revealing read needs it. Defaults to `cluster.key` in --state-dir.
    #[arg(long, env = "FABER_CLUSTER_KEY_FILE")]
    pub cluster_key_file: Option<PathBuf>,

    /// Create a new mesh with this node as the founder. Only used while the
    /// node has no identity. Cannot be combined with --join-token-file (see
    /// [`Config::validate`]).
    #[arg(
        long,
        env = "FABER_NEW_MESH",
        action = ArgAction::SetTrue,
        value_parser = BoolishValueParser::new()
    )]
    pub new_mesh: bool,

    /// Mesh address range for --new-mesh.
    #[arg(long, env = "FABER_MESH_CIDR", default_value_t = DEFAULT_MESH_CIDR)]
    pub mesh_cidr: Ipv4Net,

    /// Container address range for --new-mesh: every node gets one /24 of
    /// it. Must be a /24 or larger, without host bits, and must not overlap
    /// --mesh-cidr (see [`Config::validate`]). Fixed once the mesh exists.
    #[arg(long, env = "FABER_CONTAINER_CIDR", default_value_t = DEFAULT_CONTAINER_CIDR)]
    pub container_cidr: Ipv4Net,

    /// Service address range for --new-mesh: every service gets one virtual
    /// IP from it. Must have two hosts or more, no host bits set, and must
    /// not overlap --mesh-cidr or --container-cidr (see [`Config::validate`]).
    /// Fixed once the mesh exists.
    #[arg(long, env = "FABER_SERVICE_CIDR", default_value_t = DEFAULT_SERVICE_CIDR)]
    pub service_cidr: Ipv4Net,

    /// Name of this node for --new-mesh: a DNS label (a-z, 0-9 and
    /// hyphens, at most 63 characters, no hyphen at either end). Defaults to
    /// node-1. Read when the cluster is created (`cluster init`), not when
    /// the identity is.
    #[arg(long, env = "FABER_NODE_NAME")]
    pub node_name: Option<String>,

    /// File holding an invite token to join an existing mesh with. Only used
    /// while the node has no identity.
    #[arg(long, env = "FABER_JOIN_TOKEN_FILE")]
    pub join_token_file: Option<PathBuf>,

    /// This node's public WireGuard endpoint: an IP address with an optional
    /// port, which defaults to --wg-port. Hostnames are refused. Read it
    /// through [`Config::resolved_wg_endpoint`].
    #[arg(long, env = "FABER_WG_ENDPOINT")]
    pub wg_endpoint: Option<String>,

    /// WireGuard listen port.
    #[arg(long, env = "FABER_WG_PORT", default_value_t = DEFAULT_WG_PORT)]
    pub wg_port: u16,

    /// Bring up faber0, nftables and the containerd drop-in on this host.
    #[arg(
        long,
        env = "FABER_MANAGE_HOST",
        action = ArgAction::SetTrue,
        value_parser = BoolishValueParser::new()
    )]
    pub manage_host: bool,

    /// Directory for the containerd drop-in (faber.toml).
    #[arg(
        long,
        env = "FABER_CONTAINERD_CONF_DIR",
        default_value = DEFAULT_CONTAINERD_CONF_DIR
    )]
    pub containerd_conf_dir: PathBuf,

    /// Directory this node writes its CNI network config to
    /// (10-faber.conflist). Must be the directory containerd reads, which is
    /// a replicated setting; fabercp warns when the two differ.
    #[arg(long, env = "FABER_CNI_CONF_DIR", default_value = DEFAULT_CNI_CONF_DIR)]
    pub cni_conf_dir: PathBuf,

    /// Seconds between full host re-applies (WireGuard, nftables,
    /// containerd), on top of the applies triggered by state changes. At
    /// least 1.
    #[arg(
        long,
        env = "FABER_RECONCILE_INTERVAL_SECS",
        default_value_t = DEFAULT_RECONCILE_INTERVAL_SECS
    )]
    pub reconcile_interval_secs: u64,

    /// Clocks of the lease tasks. Not a flag: production runs the default,
    /// and tests shrink it.
    #[arg(skip)]
    pub lease_timing: LeaseTiming,
}

impl Config {
    /// Default settings for `node_id` storing data in `data_dir`. Reads
    /// neither the command line nor `FABER_*` variables.
    pub fn new(node_id: NodeId, data_dir: impl Into<PathBuf>) -> Self {
        Config {
            node_id: Some(node_id),
            api_listen: DEFAULT_API_LISTEN,
            wg_interface: None,
            peer_port: DEFAULT_PEER_PORT,
            peer_listen: None,
            data_dir: data_dir.into(),
            heartbeat_ms: DEFAULT_HEARTBEAT_MS,
            election_min_ms: DEFAULT_ELECTION_MIN_MS,
            election_max_ms: DEFAULT_ELECTION_MAX_MS,
            snapshot_logs: DEFAULT_SNAPSHOT_LOGS,
            keep_logs: DEFAULT_KEEP_LOGS,
            mcp_allowed_hosts: Vec::new(),
            state_dir: PathBuf::from(DEFAULT_STATE_DIR),
            cluster_key_file: None,
            new_mesh: false,
            mesh_cidr: DEFAULT_MESH_CIDR,
            container_cidr: DEFAULT_CONTAINER_CIDR,
            service_cidr: DEFAULT_SERVICE_CIDR,
            node_name: None,
            join_token_file: None,
            wg_endpoint: None,
            wg_port: DEFAULT_WG_PORT,
            manage_host: false,
            containerd_conf_dir: PathBuf::from(DEFAULT_CONTAINERD_CONF_DIR),
            cni_conf_dir: PathBuf::from(DEFAULT_CNI_CONF_DIR),
            reconcile_interval_secs: DEFAULT_RECONCILE_INTERVAL_SECS,
            lease_timing: LeaseTiming::default(),
        }
    }

    /// Checks what clap cannot: `--new-mesh` with `--join-token-file` (done
    /// here rather than with `conflicts_with` because clap treats
    /// `FABER_NEW_MESH=0` as present), with `--new-mesh` a usable
    /// `--container-cidr` ([`Config::check_container_cidr`]) and
    /// `--node-name` ([`Config::check_node_name`]),
    /// `--reconcile-interval-secs` of at least 1, in mesh mode a
    /// `--state-dir` that is not `--data-dir` or inside it, and a
    /// `--wg-endpoint` that parses (its default port is another flag).
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.new_mesh && self.join_token_file.is_some() {
            anyhow::bail!("--new-mesh cannot be used with --join-token-file");
        }
        if self.new_mesh {
            self.check_container_cidr()?;
            self.check_service_cidr()?;
            self.check_node_name()?;
        }
        if self.reconcile_interval_secs < 1 {
            anyhow::bail!("--reconcile-interval-secs must be at least 1");
        }
        if self.mesh_mode() {
            let (state, data) = (resolved_dir(&self.state_dir), resolved_dir(&self.data_dir));
            if state.starts_with(&data) {
                anyhow::bail!(
                    "--state-dir {} must not be --data-dir {} or inside it: discarding a \
                     failed join deletes everything in the data dir",
                    self.state_dir.display(),
                    self.data_dir.display()
                );
            }
        }
        self.resolved_wg_endpoint()?;
        Ok(())
    }

    /// `--container-cidr` as a new mesh needs it: large enough for one node
    /// subnet, without host bits, and apart from `--mesh-cidr`. The founder's
    /// `InitCluster` is rejected for the same reasons, but by then raft is
    /// initialised and the cluster cannot be seeded, so the check is made
    /// before anything starts.
    fn check_container_cidr(&self) -> anyhow::Result<()> {
        let (range, mesh) = (self.container_cidr, self.mesh_cidr);
        if range.prefix_len() > DEFAULT_CONTAINER_SUBNET_PREFIX {
            anyhow::bail!(
                "--container-cidr {range} is too small: every node gets a \
                 /{DEFAULT_CONTAINER_SUBNET_PREFIX} of it"
            );
        }
        if range != range.trunc() {
            anyhow::bail!(
                "--container-cidr {range} has host bits set; did you mean {}?",
                range.trunc()
            );
        }
        if range.contains(&mesh.network()) || mesh.contains(&range.network()) {
            anyhow::bail!("--container-cidr {range} overlaps --mesh-cidr {mesh}");
        }
        Ok(())
    }

    /// `--service-cidr` as a new mesh needs it: at least two hosts, without
    /// host bits, and apart from `--mesh-cidr` and `--container-cidr`. The
    /// founder's `InitCluster` is rejected for the same reasons, so the check
    /// is made before anything starts, as for `--container-cidr`. The default
    /// is checked too: a mesh or container range that contains it needs
    /// `--service-cidr` as well.
    fn check_service_cidr(&self) -> anyhow::Result<()> {
        let range = self.service_cidr;
        if range.prefix_len() > MAX_SERVICE_CIDR_PREFIX {
            anyhow::bail!(
                "--service-cidr {range} is too small: it must be a /{MAX_SERVICE_CIDR_PREFIX} or larger"
            );
        }
        if range != range.trunc() {
            anyhow::bail!(
                "--service-cidr {range} has host bits set; did you mean {}?",
                range.trunc()
            );
        }
        let overlaps =
            |other: Ipv4Net| range.contains(&other.network()) || other.contains(&range.network());
        if overlaps(self.mesh_cidr) {
            anyhow::bail!(
                "--service-cidr {range} overlaps --mesh-cidr {}",
                self.mesh_cidr
            );
        }
        if overlaps(self.container_cidr) {
            anyhow::bail!(
                "--service-cidr {range} overlaps --container-cidr {}",
                self.container_cidr
            );
        }
        Ok(())
    }

    /// `--node-name` as a new mesh needs it: a DNS label, or not given. The
    /// founder's `InitCluster` is rejected for a bad name too, but by then
    /// raft is initialised and the cluster cannot be seeded, so the check is
    /// made before anything starts.
    ///
    /// The name is not stored with the identity. `cluster init` reads it
    /// from this process's settings, so it must still be set when that runs.
    fn check_node_name(&self) -> anyhow::Result<()> {
        let Some(name) = &self.node_name else {
            return Ok(());
        };
        MeshState::validate_name(name)
            .map_err(|e| anyhow::anyhow!("--node-name {name:?} is not usable: {}", e.message))
    }

    /// `--wg-endpoint` as an address, with `--wg-port` filled in when it has
    /// no port. An error for anything that is not an IP endpoint, so a
    /// mistyped value stops startup instead of being retried forever.
    pub fn resolved_wg_endpoint(&self) -> anyhow::Result<Option<SocketAddr>> {
        self.wg_endpoint
            .as_deref()
            .map(|raw| parse_endpoint(raw, self.wg_port))
            .transpose()
            .map_err(|e| anyhow::anyhow!("--wg-endpoint: {e}"))
    }

    /// True when the node id must come from identity.json: the node manages
    /// the host, or is being bootstrapped from a token or `--new-mesh`.
    pub fn mesh_mode(&self) -> bool {
        self.manage_host || self.new_mesh || self.join_token_file.is_some()
    }

    /// The node id to run as. The identity's id wins, and `--node-id`, if
    /// also given, must match it. Without an identity, `--node-id` is
    /// required.
    pub fn resolve_node_id(&self, identity: Option<&Identity>) -> anyhow::Result<NodeId> {
        match (identity, self.node_id) {
            (Some(id), Some(flag)) if flag != id.node_id => anyhow::bail!(
                "--node-id {flag} does not match node_id {} in identity.json",
                id.node_id
            ),
            (Some(id), _) => Ok(id.node_id),
            (None, Some(flag)) => Ok(flag),
            (None, None) => anyhow::bail!(
                "--node-id is required when the node has no identity \
                 (use --new-mesh or --join-token-file to create one)"
            ),
        }
    }

    /// The cluster key file: `--cluster-key-file`, or `cluster.key` in
    /// `--state-dir`.
    pub fn cluster_key_path(&self) -> PathBuf {
        self.cluster_key_file
            .clone()
            .unwrap_or_else(|| self.state_dir.join(CLUSTER_KEY_FILE))
    }

    /// The WireGuard interface the node manages: `--wg-interface`, or
    /// [`DEFAULT_WG_INTERFACE`].
    pub fn wg_iface(&self) -> &str {
        self.wg_interface.as_deref().unwrap_or(DEFAULT_WG_INTERFACE)
    }

    /// Bootstrap settings for [`crate::mesh::identity::resolve`].
    /// `join_token` is the contents of `--join-token-file`, read by the
    /// caller. Fails on a `--wg-endpoint` that does not parse.
    pub fn bootstrap_input(&self, join_token: Option<String>) -> anyhow::Result<BootstrapInput> {
        Ok(BootstrapInput {
            new_mesh: self.new_mesh,
            mesh_cidr: self.mesh_cidr,
            join_token,
            wg_port: self.wg_port,
            endpoint: self.resolved_wg_endpoint()?,
        })
    }

    /// Loopback names, the peer listener's IP and `ip:port`, then any
    /// configured extras, without duplicates.
    pub fn allowed_mcp_hosts(&self, peer_addr: SocketAddr) -> Vec<String> {
        let mut hosts: Vec<String> = DEFAULT_MCP_ALLOWED_HOSTS
            .iter()
            .map(|h| h.to_string())
            .collect();
        hosts.push(peer_addr.ip().to_string());
        hosts.push(peer_addr.to_string());
        hosts.extend(self.mcp_allowed_hosts.iter().cloned());
        let mut seen = BTreeSet::new();
        hosts.retain(|h| seen.insert(h.clone()));
        hosts
    }

    pub fn raft_config(&self) -> anyhow::Result<openraft::Config> {
        let c = openraft::Config {
            cluster_name: "faber".to_string(),
            heartbeat_interval: self.heartbeat_ms,
            election_timeout_min: self.election_min_ms,
            election_timeout_max: self.election_max_ms,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(self.snapshot_logs),
            max_in_snapshot_log_to_keep: self.keep_logs,
            ..Default::default()
        };
        Ok(c.validate()?)
    }
}

/// `path` in a form two spellings of one directory share: absolute, without
/// `.` and `..`, and with symlinks resolved as far as the path exists.
fn resolved_dir(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut lexical = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                lexical.pop();
            }
            other => lexical.push(other),
        }
    }
    // Canonicalise the longest prefix that exists, then put the rest back.
    let mut missing = Vec::new();
    let mut existing = lexical.as_path();
    loop {
        if let Ok(real) = std::fs::canonicalize(existing) {
            return missing.iter().rev().fold(real, |p, name| p.join(name));
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name.to_os_string());
                existing = parent;
            }
            _ => return lexical.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use super::*;
    use crate::lease::LeaseTiming;
    use crate::mesh::keys::PrivateKey;

    /// Parsing reads `FABER_*`, which is process-wide. Every test that parses
    /// or sets variables holds this lock.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const MESH_VARS: [&str; 14] = [
        "FABER_STATE_DIR",
        "FABER_NEW_MESH",
        "FABER_MESH_CIDR",
        "FABER_CONTAINER_CIDR",
        "FABER_NODE_NAME",
        "FABER_JOIN_TOKEN_FILE",
        "FABER_WG_ENDPOINT",
        "FABER_WG_PORT",
        "FABER_MANAGE_HOST",
        "FABER_CONTAINERD_CONF_DIR",
        "FABER_CNI_CONF_DIR",
        "FABER_NODE_ID",
        "FABER_RECONCILE_INTERVAL_SECS",
        "FABER_WG_INTERFACE",
    ];

    fn lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Sets `vars` for the duration of `f`, then removes them.
    fn with_env<T>(vars: &[(&str, &str)], f: impl FnOnce() -> T) -> T {
        let _g = lock();
        for (k, v) in vars {
            // SAFETY: every test that reads or writes the environment holds
            // ENV_LOCK, so no other thread touches it concurrently.
            unsafe { std::env::set_var(k, v) };
        }
        let out = f();
        for (k, _) in vars {
            // SAFETY: as above.
            unsafe { std::env::remove_var(k) };
        }
        out
    }

    fn parse(args: &[&str]) -> Config {
        with_env(&[], || {
            Config::parse_from(std::iter::once("fabercp").chain(args.iter().copied()))
        })
    }

    fn try_parse(args: &[&str]) -> Result<Config, clap::Error> {
        with_env(&[], || {
            Config::try_parse_from(std::iter::once("fabercp").chain(args.iter().copied()))
        })
    }

    fn identity(node_id: u64) -> Identity {
        Identity {
            v: 1,
            node_id,
            private_key: PrivateKey::new("yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk="),
            mesh_ip: "10.99.0.2".parse().unwrap(),
            mesh_cidr: DEFAULT_MESH_CIDR,
            wg_port: 51820,
            endpoint: "198.51.100.2:51820".parse().unwrap(),
            instance_id: None,
            pending_join: None,
        }
    }

    #[test]
    fn defaults_are_loopback() {
        let c = Config::new(3, "/tmp/x");
        assert_eq!(c.node_id, Some(3));
        assert_eq!(c.api_listen, "127.0.0.1:7400".parse().unwrap());
        assert_eq!(c.peer_listen, None);
        assert_eq!(c.wg_interface, None);
        assert_eq!(c.peer_port, DEFAULT_PEER_PORT);
        c.raft_config().unwrap();
    }

    #[test]
    fn new_matches_parsed_defaults() {
        let parsed = parse(&["--node-id", "3", "--data-dir", "/tmp/x"]);
        assert_eq!(
            format!("{parsed:?}"),
            format!("{:?}", Config::new(3, "/tmp/x"))
        );
    }

    #[test]
    fn new_ignores_environment() {
        let c = with_env(
            &[
                ("FABER_NEW_MESH", "1"),
                ("FABER_WG_PORT", "1234"),
                ("FABER_API_LISTEN", "127.0.0.1:1"),
            ],
            || Config::new(1, "d"),
        );
        assert!(!c.new_mesh);
        assert_eq!(c.wg_port, DEFAULT_WG_PORT);
        assert_eq!(c.api_listen, DEFAULT_API_LISTEN);
    }

    #[test]
    fn mesh_defaults() {
        let c = parse(&[]);
        assert_eq!(c.node_id, None);
        assert_eq!(c.state_dir, PathBuf::from("/var/lib/faber"));
        assert!(!c.new_mesh);
        assert_eq!(c.mesh_cidr, "10.99.0.0/16".parse::<Ipv4Net>().unwrap());
        assert_eq!(
            c.container_cidr,
            "10.200.0.0/16".parse::<Ipv4Net>().unwrap()
        );
        assert_eq!(c.node_name, None);
        assert_eq!(c.join_token_file, None);
        assert_eq!(c.wg_endpoint, None);
        assert_eq!(c.wg_port, 51820);
        assert!(!c.manage_host);
        assert_eq!(
            c.containerd_conf_dir,
            PathBuf::from("/etc/containerd/conf.d")
        );
        assert_eq!(c.cni_conf_dir, PathBuf::from("/etc/cni/net.d"));
        assert!(!c.mesh_mode());
    }

    #[test]
    fn mesh_flags_parse() {
        let c = parse(&[
            "--state-dir",
            "/s",
            "--new-mesh",
            "--mesh-cidr",
            "172.20.0.0/24",
            "--container-cidr",
            "172.21.0.0/16",
            "--node-name",
            "web-1",
            "--wg-endpoint",
            "203.0.113.1:51820",
            "--wg-port",
            "51000",
            "--manage-host",
            "--containerd-conf-dir",
            "/c",
            "--cni-conf-dir",
            "/n",
        ]);
        assert_eq!(c.state_dir, PathBuf::from("/s"));
        assert!(c.new_mesh);
        assert_eq!(c.mesh_cidr, "172.20.0.0/24".parse::<Ipv4Net>().unwrap());
        assert_eq!(
            c.container_cidr,
            "172.21.0.0/16".parse::<Ipv4Net>().unwrap()
        );
        assert_eq!(c.node_name.as_deref(), Some("web-1"));
        assert_eq!(c.wg_endpoint.as_deref(), Some("203.0.113.1:51820"));
        assert_eq!(c.wg_port, 51000);
        assert!(c.manage_host);
        assert_eq!(c.containerd_conf_dir, PathBuf::from("/c"));
        assert_eq!(c.cni_conf_dir, PathBuf::from("/n"));
        assert!(c.mesh_mode());

        let c = parse(&["--join-token-file", "/etc/faber/join.token"]);
        assert_eq!(
            c.join_token_file,
            Some(PathBuf::from("/etc/faber/join.token"))
        );
        assert!(c.mesh_mode());
    }

    #[test]
    fn mesh_env_parse() {
        let c = with_env(
            &[
                ("FABER_STATE_DIR", "/s"),
                ("FABER_NEW_MESH", "1"),
                ("FABER_MESH_CIDR", "172.20.0.0/24"),
                ("FABER_CONTAINER_CIDR", "172.21.0.0/16"),
                ("FABER_NODE_NAME", "web-1"),
                ("FABER_WG_ENDPOINT", "203.0.113.1:51820"),
                ("FABER_WG_PORT", "51000"),
                ("FABER_MANAGE_HOST", "1"),
                ("FABER_CONTAINERD_CONF_DIR", "/c"),
                ("FABER_CNI_CONF_DIR", "/n"),
                ("FABER_NODE_ID", "7"),
            ],
            || Config::parse_from(["fabercp"]),
        );
        assert_eq!(c.node_id, Some(7));
        assert_eq!(c.state_dir, PathBuf::from("/s"));
        assert!(c.new_mesh);
        assert_eq!(c.mesh_cidr, "172.20.0.0/24".parse::<Ipv4Net>().unwrap());
        assert_eq!(
            c.container_cidr,
            "172.21.0.0/16".parse::<Ipv4Net>().unwrap()
        );
        assert_eq!(c.node_name.as_deref(), Some("web-1"));
        assert_eq!(c.wg_endpoint.as_deref(), Some("203.0.113.1:51820"));
        assert_eq!(c.wg_port, 51000);
        assert!(c.manage_host);
        assert_eq!(c.containerd_conf_dir, PathBuf::from("/c"));
        assert_eq!(c.cni_conf_dir, PathBuf::from("/n"));

        let c = with_env(&[("FABER_JOIN_TOKEN_FILE", "/t")], || {
            Config::parse_from(["fabercp"])
        });
        assert_eq!(c.join_token_file, Some(PathBuf::from("/t")));
    }

    #[test]
    fn bool_env_values() {
        for (v, want) in [
            ("1", true),
            ("true", true),
            ("yes", true),
            ("0", false),
            ("false", false),
            ("no", false),
        ] {
            let c = with_env(&[("FABER_NEW_MESH", v), ("FABER_MANAGE_HOST", v)], || {
                Config::parse_from(["fabercp"])
            });
            assert_eq!(c.new_mesh, want, "FABER_NEW_MESH={v}");
            assert_eq!(c.manage_host, want, "FABER_MANAGE_HOST={v}");
        }
        let r = with_env(&[("FABER_NEW_MESH", "maybe")], || {
            Config::try_parse_from(["fabercp"])
        });
        assert!(r.is_err());
    }

    #[test]
    fn new_mesh_and_join_token_conflict() {
        let c = parse(&["--new-mesh", "--join-token-file", "/t"]);
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("--new-mesh"), "{err}");
        let c = with_env(
            &[("FABER_NEW_MESH", "1"), ("FABER_JOIN_TOKEN_FILE", "/t")],
            || Config::parse_from(["fabercp"]),
        );
        assert!(c.validate().is_err());
        // A false env value does not count as --new-mesh.
        let c = with_env(
            &[("FABER_NEW_MESH", "0"), ("FABER_JOIN_TOKEN_FILE", "/t")],
            || Config::parse_from(["fabercp"]),
        );
        c.validate().unwrap();
        parse(&["--new-mesh"]).validate().unwrap();
        parse(&["--join-token-file", "/t"]).validate().unwrap();
        Config::new(1, "d").validate().unwrap();
    }

    /// A rejected join clears everything inside the data dir, which must
    /// not take identity.json, the ownership record or the markers with it.
    #[test]
    fn the_state_dir_must_not_be_the_data_dir_or_inside_it() {
        let dir = tempfile::tempdir().unwrap();
        let mesh = |data: PathBuf, state: PathBuf| {
            let mut c = Config::new(1, data);
            c.node_id = None;
            c.state_dir = state;
            c.manage_host = true;
            c
        };
        let root = dir.path().to_path_buf();
        for (data, state) in [
            (root.clone(), root.clone()),
            // Spelled differently.
            (root.join("x").join(".."), root.clone()),
            (root.clone(), root.join(".")),
            // The state dir inside the data dir.
            (root.clone(), root.join("state")),
            // Neither exists yet.
            (root.join("new"), root.join("new")),
            (root.join("new"), root.join("new").join("state")),
        ] {
            let err = mesh(data.clone(), state.clone())
                .validate()
                .expect_err(&format!("{data:?} {state:?}"))
                .to_string();
            assert!(err.contains("--state-dir"), "{err}");
            assert!(err.contains("--data-dir"), "{err}");
            // One sentence, not a line broken in the source.
            assert!(
                err.ends_with("discarding a failed join deletes everything in the data dir"),
                "{err}"
            );
        }
        // The layout install.sh writes, and siblings.
        mesh(root.join("data"), root.clone()).validate().unwrap();
        mesh(root.join("data"), root.join("state"))
            .validate()
            .unwrap();
        mesh(root.join("data"), root.join("database"))
            .validate()
            .unwrap();
        // Outside mesh mode the state dir is not used.
        let mut dev = Config::new(1, root.clone());
        dev.state_dir = root;
        dev.validate().unwrap();
    }

    /// `InitCluster` refuses the same ranges, but only once raft has been
    /// initialised, which leaves a cluster that can never be seeded. A bad
    /// range has to stop the founder before it starts.
    #[test]
    fn a_new_mesh_needs_a_usable_container_range() {
        let founder = |mesh: &str, containers: &str| {
            let mut c = Config::new(1, "d");
            c.node_id = None;
            c.new_mesh = true;
            c.mesh_cidr = mesh.parse().unwrap();
            c.container_cidr = containers.parse().unwrap();
            c
        };
        let error = |mesh: &str, containers: &str| {
            founder(mesh, containers)
                .validate()
                .expect_err(&format!("{mesh} {containers}"))
                .to_string()
        };

        // Too small to give one node its /24.
        for small in ["10.200.0.0/25", "10.200.0.0/30", "10.200.0.1/32"] {
            let err = error("10.99.0.0/16", small);
            assert!(err.contains("--container-cidr"), "{err}");
            assert!(err.contains(small), "{err}");
            assert!(err.contains("/24"), "{err}");
            // One sentence, not a line broken in the source.
            assert!(err.ends_with("every node gets a /24 of it"), "{err}");
        }
        // Host bits.
        let err = error("10.99.0.0/16", "10.200.1.0/16");
        assert!(err.contains("--container-cidr 10.200.1.0/16"), "{err}");
        assert!(err.contains("host bits"), "{err}");
        assert!(err.contains("10.200.0.0/16"), "{err}");
        // Overlap, whichever range holds the other.
        for (mesh, containers) in [
            ("10.99.0.0/16", "10.99.0.0/16"),
            ("10.99.0.0/16", "10.0.0.0/8"),
            ("10.99.0.0/16", "10.99.4.0/24"),
            ("10.0.0.0/8", "10.200.0.0/16"),
        ] {
            let err = error(mesh, containers);
            assert!(err.contains("--container-cidr"), "{err}");
            assert!(err.contains("overlaps"), "{err}");
            assert!(err.contains(&format!("--mesh-cidr {mesh}")), "{err}");
        }

        // The defaults, the smallest range, and a neighbour of the mesh.
        for (mesh, containers) in [
            ("10.99.0.0/16", "10.200.0.0/16"),
            ("10.99.0.0/16", "10.200.7.0/24"),
            ("10.99.0.0/16", "10.100.0.0/16"),
            ("10.99.0.0/16", "172.16.0.0/12"),
        ] {
            founder(mesh, containers).validate().unwrap();
        }
        // As the flags arrive from the command line.
        let c = parse(&["--new-mesh", "--container-cidr", "10.200.0.0/25"]);
        assert!(c.validate().is_err());

        // Only a new mesh takes the range from here: a joiner and a
        // configured node get it from the replicated state.
        let mut joiner = founder("10.99.0.0/16", "10.99.0.0/30");
        joiner.new_mesh = false;
        joiner.validate().unwrap();
        joiner.join_token_file = Some(PathBuf::from("/t"));
        joiner.validate().unwrap();
    }

    /// Like the container range: `InitCluster` refuses a bad service range
    /// only after raft is initialised, so the founder checks it first. That
    /// includes the default against the other ranges.
    #[test]
    fn a_new_mesh_needs_a_usable_service_range() {
        assert_eq!(parse(&[]).service_cidr.to_string(), "10.201.0.0/16");
        let founder = |args: &[&str]| {
            let mut all = vec!["--new-mesh"];
            all.extend_from_slice(args);
            parse(&all)
        };
        founder(&[]).validate().unwrap();
        founder(&["--service-cidr", "10.201.0.0/30"])
            .validate()
            .unwrap();
        let from_env = with_env(&[("FABER_SERVICE_CIDR", "192.168.0.0/24")], || {
            Config::parse_from(["fabercp", "--new-mesh"])
        });
        assert_eq!(from_env.service_cidr.to_string(), "192.168.0.0/24");

        for (args, expected) in [
            (vec!["--service-cidr", "10.201.0.0/31"], "too small"),
            (vec!["--service-cidr", "10.201.1.0/16"], "host bits"),
            (
                vec!["--service-cidr", "10.99.0.0/24"],
                "overlaps --mesh-cidr 10.99.0.0/16",
            ),
            (
                vec!["--service-cidr", "10.200.4.0/24"],
                "overlaps --container-cidr 10.200.0.0/16",
            ),
            // The default service range inside a bigger container range.
            (
                vec!["--container-cidr", "10.128.0.0/9"],
                "overlaps --container-cidr 10.128.0.0/9",
            ),
        ] {
            let err = founder(&args).validate().unwrap_err().to_string();
            assert!(err.contains("--service-cidr"), "{args:?}: {err}");
            assert!(err.contains(expected), "{args:?}: {err}");
        }

        // Only a new mesh takes the range from here.
        let mut joiner = founder(&["--service-cidr", "10.99.0.0/24"]);
        joiner.new_mesh = false;
        joiner.validate().unwrap();
    }

    #[test]
    fn reconcile_interval() {
        assert_eq!(parse(&[]).reconcile_interval_secs, 300);
        assert_eq!(Config::new(1, "d").reconcile_interval_secs, 300);
        let c = parse(&["--reconcile-interval-secs", "60"]);
        assert_eq!(c.reconcile_interval_secs, 60);
        c.validate().unwrap();
        let c = with_env(&[("FABER_RECONCILE_INTERVAL_SECS", "5")], || {
            Config::parse_from(["fabercp"])
        });
        assert_eq!(c.reconcile_interval_secs, 5);
        let err = parse(&["--reconcile-interval-secs", "0"])
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("--reconcile-interval-secs"), "{err}");
        assert!(try_parse(&["--reconcile-interval-secs", "-1"]).is_err());
        assert!(try_parse(&["--reconcile-interval-secs", "soon"]).is_err());
    }

    /// The env files install.sh writes (docs/contract.md), parsed as systemd
    /// hands them to fabercp.
    #[test]
    fn install_env_files_parse() {
        let common = [
            ("FABER_WG_INTERFACE", "faber0"),
            ("FABER_STATE_DIR", "/var/lib/faber"),
            ("FABER_DATA_DIR", "/var/lib/faber/data"),
            ("FABER_WG_PORT", "51820"),
            ("FABER_MANAGE_HOST", "1"),
            ("FABER_CONTAINERD_CONF_DIR", "/etc/containerd/conf.d"),
        ];

        let mut founder = common.to_vec();
        founder.extend([
            ("FABER_WG_ENDPOINT", "203.0.113.1:51820"),
            ("FABER_NEW_MESH", "1"),
            ("FABER_MESH_CIDR", "10.99.0.0/16"),
        ]);
        let c = with_env(&founder, || Config::try_parse_from(["fabercp"])).unwrap();
        c.validate().unwrap();
        assert!(c.manage_host && c.new_mesh && c.mesh_mode());
        assert_eq!(c.wg_interface.as_deref(), Some("faber0"));
        assert_eq!(c.data_dir, PathBuf::from("/var/lib/faber/data"));
        assert_eq!(c.wg_endpoint.as_deref(), Some("203.0.113.1:51820"));
        assert_eq!(c.node_id, None);
        assert_eq!(c.reconcile_interval_secs, 300);

        // A joiner without --endpoint: no FABER_WG_ENDPOINT at all.
        let mut joiner = common.to_vec();
        joiner.push(("FABER_JOIN_TOKEN_FILE", "/etc/faber/join.token"));
        let c = with_env(&joiner, || Config::try_parse_from(["fabercp"])).unwrap();
        c.validate().unwrap();
        assert!(c.mesh_mode() && !c.new_mesh);
        assert_eq!(c.wg_endpoint, None);
        assert_eq!(
            c.join_token_file,
            Some(PathBuf::from("/etc/faber/join.token"))
        );

        // Configured: neither bootstrap variable.
        let mut configured = common.to_vec();
        configured.push(("FABER_WG_ENDPOINT", "203.0.113.1:51820"));
        let c = with_env(&configured, || Config::try_parse_from(["fabercp"])).unwrap();
        c.validate().unwrap();
        assert!(c.mesh_mode());
    }

    #[test]
    fn bad_mesh_values_are_rejected() {
        assert!(try_parse(&["--mesh-cidr", "10.99.0.0"]).is_err());
        assert!(try_parse(&["--mesh-cidr", "fd00::/64"]).is_err());
        assert!(try_parse(&["--container-cidr", "10.200.0.0"]).is_err());
        assert!(try_parse(&["--container-cidr", "fd00::/64"]).is_err());
        assert!(try_parse(&["--wg-port", "70000"]).is_err());
    }

    #[test]
    fn test_env_vars_are_cleaned_up() {
        let _g = lock();
        for k in MESH_VARS {
            assert!(std::env::var_os(k).is_none(), "{k} left set");
        }
    }

    #[test]
    fn resolve_node_id_prefers_identity() {
        let mut c = Config::new(2, "d");
        assert_eq!(c.resolve_node_id(Some(&identity(2))).unwrap(), 2);
        assert!(c.resolve_node_id(Some(&identity(5))).is_err());
        assert_eq!(c.resolve_node_id(None).unwrap(), 2);
        c.node_id = None;
        assert_eq!(c.resolve_node_id(Some(&identity(5))).unwrap(), 5);
        let err = c.resolve_node_id(None).unwrap_err().to_string();
        assert!(err.contains("--node-id is required"), "{err}");
    }

    #[test]
    fn bootstrap_input_from_flags() {
        let mut c = Config::new(1, "d");
        c.new_mesh = true;
        c.wg_port = 51000;
        c.wg_endpoint = Some("203.0.113.1:51000".into());
        let i = c.bootstrap_input(Some("faber1.x".into())).unwrap();
        assert_eq!(
            i,
            BootstrapInput {
                new_mesh: true,
                mesh_cidr: DEFAULT_MESH_CIDR,
                join_token: Some("faber1.x".into()),
                wg_port: 51000,
                endpoint: Some("203.0.113.1:51000".parse().unwrap()),
            }
        );
        c.wg_endpoint = Some("gateway.example:51000".into());
        assert!(c.bootstrap_input(None).is_err());
    }

    #[test]
    fn wg_endpoint_resolves_to_an_address() {
        let mut c = Config::new(1, "d");
        assert_eq!(c.resolved_wg_endpoint().unwrap(), None);
        c.validate().unwrap();

        // A bare IP gets --wg-port.
        c.wg_port = 51000;
        c.wg_endpoint = Some("203.0.113.1".into());
        assert_eq!(
            c.resolved_wg_endpoint().unwrap(),
            Some("203.0.113.1:51000".parse().unwrap())
        );
        c.wg_endpoint = Some("203.0.113.1:51820".into());
        assert_eq!(
            c.resolved_wg_endpoint().unwrap(),
            Some("203.0.113.1:51820".parse().unwrap())
        );
        c.validate().unwrap();

        // IPv6 spelling does not matter.
        c.wg_endpoint = Some("[2001:DB8::2]:51820".into());
        let upper = c.resolved_wg_endpoint().unwrap().unwrap();
        c.wg_endpoint = Some("[2001:db8::2]:51820".into());
        let lower = c.resolved_wg_endpoint().unwrap().unwrap();
        assert_eq!(upper, lower);
        assert_eq!(upper.to_string(), "[2001:db8::2]:51820");
    }

    #[test]
    fn malformed_wg_endpoint_fails_validation() {
        for bad in ["gateway.example:51820", "203.0.113.1:port", "0.0.0.0", ""] {
            let mut c = Config::new(1, "d");
            c.wg_endpoint = Some(bad.into());
            let err = c.validate().unwrap_err().to_string();
            assert!(err.contains("--wg-endpoint"), "{bad}: {err}");
        }
        // The same through the environment, as systemd passes it.
        let c = with_env(&[("FABER_WG_ENDPOINT", "gateway.example:51820")], || {
            Config::try_parse_from(["fabercp"])
        })
        .unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn a_founder_name_must_be_a_dns_label() {
        let founder = |name: Option<&str>| {
            let mut c = Config::new(1, "d");
            c.new_mesh = true;
            c.node_name = name.map(str::to_string);
            c
        };
        for good in [None, Some("web-1"), Some("a"), Some("node-1")] {
            founder(good).validate().unwrap();
        }
        for bad in ["", "Web-1", "web_1", "-web", "web-", "web.example", "a b"] {
            let err = founder(Some(bad)).validate().unwrap_err().to_string();
            assert!(err.contains("--node-name"), "{bad:?}: {err}");
            assert!(err.contains(&format!("{bad:?}")), "{bad:?}: {err}");
        }
        let long = "a".repeat(64);
        assert!(founder(Some(&long)).validate().is_err());
        founder(Some(&"a".repeat(63))).validate().unwrap();

        // Only a new mesh reads the name: every other node gets its name
        // from its invite, so a stray value does not stop it.
        let mut joiner = founder(Some("Not A Name"));
        joiner.new_mesh = false;
        joiner.validate().unwrap();
        // The same through the environment, as install.sh passes it.
        let c = with_env(
            &[("FABER_NEW_MESH", "1"), ("FABER_NODE_NAME", "Web_1")],
            || Config::try_parse_from(["fabercp"]),
        )
        .unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn lease_timing_is_not_a_flag_and_defaults_to_production() {
        assert_eq!(parse(&[]).lease_timing, LeaseTiming::default());
        assert_eq!(Config::new(1, "d").lease_timing, LeaseTiming::default());
        assert!(try_parse(&["--lease-timing", "x"]).is_err());
    }

    #[test]
    fn mcp_hosts_include_loopback_peer_and_extras() {
        let mut c = Config::new(1, "d");
        c.mcp_allowed_hosts = vec!["node1.wg".into()];
        assert_eq!(
            c.allowed_mcp_hosts("10.8.0.1:7401".parse().unwrap()),
            vec![
                "localhost",
                "127.0.0.1",
                "::1",
                "10.8.0.1",
                "10.8.0.1:7401",
                "node1.wg"
            ]
        );
        let v6 = c.allowed_mcp_hosts("[fd00::1]:7401".parse().unwrap());
        assert!(v6.contains(&"fd00::1".to_string()));
        assert!(v6.contains(&"[fd00::1]:7401".to_string()));
    }

    #[test]
    fn mcp_hosts_have_no_duplicates() {
        let mut c = Config::new(1, "d");
        c.mcp_allowed_hosts = vec!["localhost".into(), "a.example".into(), "a.example".into()];
        assert_eq!(
            c.allowed_mcp_hosts("127.0.0.1:7401".parse().unwrap()),
            vec![
                "localhost",
                "127.0.0.1",
                "::1",
                "127.0.0.1:7401",
                "a.example"
            ]
        );
    }

    #[test]
    fn flags_parse() {
        let c = parse(&[
            "--node-id",
            "2",
            "--api-listen",
            "[::1]:9000",
            "--wg-interface",
            "faber0",
            "--peer-port",
            "9001",
            "--data-dir",
            "d",
            "--heartbeat-ms",
            "50",
            "--election-min-ms",
            "150",
            "--election-max-ms",
            "300",
            "--mcp-allowed-host",
            "a.example",
            "--mcp-allowed-host",
            "b.example:9000",
        ]);
        assert_eq!(c.node_id, Some(2));
        assert_eq!(c.wg_interface.as_deref(), Some("faber0"));
        assert_eq!(c.peer_port, 9001);
        assert_eq!(c.mcp_allowed_hosts, vec!["a.example", "b.example:9000"]);
        let r = c.raft_config().unwrap();
        assert_eq!(r.heartbeat_interval, 50);
        assert_eq!(r.election_timeout_min, 150);
    }

    /// Status reports add a raft entry per node and interval. A snapshot
    /// every few thousand entries, with the log purged up to it (less the
    /// entries kept for slow followers), keeps the log from growing without
    /// bound.
    #[test]
    fn raft_snapshots_and_purges_the_log_by_default() {
        let r = Config::new(1, "d").raft_config().unwrap();
        assert!(
            matches!(
                r.snapshot_policy,
                openraft::SnapshotPolicy::LogsSinceLast(5000)
            ),
            "{:?}",
            r.snapshot_policy
        );
        assert_eq!(r.max_in_snapshot_log_to_keep, 1000);
    }

    #[test]
    fn wg_interface_and_peer_listen_conflict() {
        let r = try_parse(&[
            "--node-id",
            "1",
            "--wg-interface",
            "faber0",
            "--peer-listen",
            "127.0.0.1:7401",
        ]);
        assert!(r.is_err());
    }

    #[test]
    fn invalid_raft_timings_are_rejected() {
        let mut c = Config::new(1, "d");
        c.election_min_ms = 100;
        c.heartbeat_ms = 200;
        assert!(c.raft_config().is_err());
    }
}
