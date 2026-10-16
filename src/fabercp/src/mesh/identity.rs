//! This node's `identity.json`: its node id, WireGuard key and mesh address.
//!
//! The file is written once, before the first start completes, and holds the
//! node's permanent private key. [`save`] writes it atomically with mode 0600
//! (unix). [`resolve`] picks where the identity comes from: an existing file
//! always wins, then an invite token, then `--new-mesh`.
//!
//! A joiner whose invite is rejected never became part of the mesh, so its
//! identity is thrown away again: [`mark_join_failed`] removes the file and
//! leaves a `join-failed` marker, and [`discard_failed_join`] finishes the
//! cleanup (the raft data directory) at the next start, before any storage is
//! open. The same two steps give up a pending join that a newer token has
//! replaced ([`supersedes_pending_join`]), and the identity of a node that
//! the mesh has removed (its lease ran out, or another machine took its
//! name).

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

use super::endpoint::parse_full_endpoint;
use super::keys::{self, KeyError, PrivateKey};
use super::token::{InviteToken, TokenError, TokenPeer};

/// File name inside the state dir.
pub const IDENTITY_FILE: &str = "identity.json";
/// Marker inside the state dir: this node's invite was rejected or the mesh
/// removed the node, and its identity and raft data are to be removed before
/// the next start.
pub const JOIN_FAILED_FILE: &str = "join-failed";
/// Value of [`Identity::v`] this build writes and accepts.
pub const IDENTITY_VERSION: u32 = 1;
/// Node id of the node that creates a mesh.
pub const FOUNDER_NODE_ID: u64 = 1;

/// Contents of `identity.json`. `Debug` redacts the private key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// Format version, [`IDENTITY_VERSION`].
    pub v: u32,
    pub node_id: u64,
    /// Standard base64 WireGuard private key.
    pub private_key: PrivateKey,
    pub mesh_ip: Ipv4Addr,
    pub mesh_cidr: Ipv4Net,
    pub wg_port: u16,
    /// This node's public WireGuard endpoint, stored as `ip:port`.
    pub endpoint: SocketAddr,
    /// Names this machine: 128 random bits as lowercase hex, made when the
    /// identity is created. A joiner registers its invite with it. An invite
    /// belongs to the first machine that registers with it, so a second
    /// machine holding a copy of the token is refused. Every node renews its
    /// lease with it. `None` only in a file written before the field
    /// existed, until [`ensure_instance_id`] fills it in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    /// Set until this node sees itself as a voter. A rejected invite removes
    /// the whole identity instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_join: Option<PendingJoin>,
}

/// What a joiner still needs until it is a voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingJoin {
    pub invite_id: String,
    /// Peer listener addresses, leader first.
    pub join_addrs: Vec<String>,
    /// wg peers to use until `mesh` has been replicated to this node.
    pub bootstrap_peers: Vec<TokenPeer>,
    /// The mesh has accepted this machine's registration for the invite.
    /// Until then the node takes no raft RPCs.
    #[serde(default, skip_serializing_if = "is_false")]
    pub registered: bool,
}

/// Whether this node's join is still pending: it has an identity from an
/// invite and does not yet see itself as a voter. Set from
/// [`Identity::pending_join`] at startup and cleared, in every clone at
/// once, when the join completes.
///
/// While it is set, the peers of the join token count as peers next to
/// whatever part of the mesh has replicated: the node may have applied only
/// a prefix of the log, in which the current leader is not registered yet.
#[derive(Debug, Clone, Default)]
pub struct JoinPending(Arc<AtomicBool>);

impl JoinPending {
    pub fn new(pending: bool) -> Self {
        Self(Arc::new(AtomicBool::new(pending)))
    }

    pub fn is_pending(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    /// The join has completed. There is no way back.
    pub fn clear(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl PartialEq for JoinPending {
    fn eq(&self, other: &Self) -> bool {
        self.is_pending() == other.is_pending()
    }
}

impl Eq for JoinPending {}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Bootstrap settings from the command line, used only when there is no
/// identity yet. `Debug` redacts the join token, which holds a private key.
#[derive(Clone, PartialEq, Eq)]
pub struct BootstrapInput {
    /// `--new-mesh`: create a new mesh with this node as the founder.
    pub new_mesh: bool,
    /// `--mesh-cidr`, used with `new_mesh` only.
    pub mesh_cidr: Ipv4Net,
    /// Contents of `--join-token-file`.
    pub join_token: Option<String>,
    /// `--wg-port`.
    pub wg_port: u16,
    /// `--wg-endpoint`.
    pub endpoint: Option<SocketAddr>,
}

impl fmt::Debug for BootstrapInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootstrapInput")
            .field("new_mesh", &self.new_mesh)
            .field("mesh_cidr", &self.mesh_cidr)
            .field(
                "join_token",
                &self.join_token.as_ref().map(|_| "<redacted>"),
            )
            .field("wg_port", &self.wg_port)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// Why the identity could not be loaded, saved or created. Never contains
/// key material.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("node not bootstrapped: no identity, join token or --new-mesh")]
    NotBootstrapped,
    #[error("--new-mesh and a join token cannot be used together")]
    ConflictingBootstrap,
    #[error("--wg-endpoint is required to create a new mesh")]
    EndpointRequired,
    #[error("endpoint {0:?} is not an ip:port")]
    InvalidEndpoint(String),
    #[error("--wg-endpoint {given} does not match the invite endpoint {invite}")]
    EndpointMismatch {
        given: SocketAddr,
        invite: SocketAddr,
    },
    #[error("mesh cidr {0} has no host addresses")]
    NoHostAddress(Ipv4Net),
    #[error("invite token expired at {expires_at} (now {now})")]
    TokenExpired { expires_at: u64, now: u64 },
    #[error("invite token: {0}")]
    Token(#[from] TokenError),
    #[error("mesh ip {mesh_ip} is not in {mesh_cidr}")]
    IpOutsideCidr {
        mesh_ip: Ipv4Addr,
        mesh_cidr: Ipv4Net,
    },
    #[error("wireguard key: {0}")]
    Key(#[from] KeyError),
    #[error("unsupported identity version {0}")]
    UnsupportedVersion(u32),
    #[error("{} is not a regular file", .0.display())]
    NotRegularFile(PathBuf),
    #[error("{} is not valid: {source}", path.display())]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("{}: {source}", path.display())]
    Io { path: PathBuf, source: io::Error },
}

fn io_err(path: &Path) -> impl FnOnce(io::Error) -> IdentityError + '_ {
    move |source| IdentityError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// `<state_dir>/identity.json`.
pub fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(IDENTITY_FILE)
}

/// Reads `identity.json`, or `None` if there is none. Refuses a symlink or
/// anything else that is not a regular file.
pub fn load(state_dir: &Path) -> Result<Option<Identity>, IdentityError> {
    let path = path(state_dir);
    let meta = match fs::symlink_metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(&path)(e)),
    };
    if !meta.file_type().is_file() {
        return Err(IdentityError::NotRegularFile(path));
    }
    let bytes = fs::read(&path).map_err(io_err(&path))?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|source| IdentityError::Json {
            path: path.clone(),
            source,
        })?;
    match value.get("v").and_then(serde_json::Value::as_u64) {
        Some(v) if v == u64::from(IDENTITY_VERSION) => {}
        Some(v) => {
            return Err(IdentityError::UnsupportedVersion(
                v.min(u32::MAX.into()) as u32
            ));
        }
        None => return Err(IdentityError::UnsupportedVersion(0)),
    }
    let identity: Identity =
        serde_json::from_value(value).map_err(|source| IdentityError::Json {
            path: path.clone(),
            source,
        })?;
    identity.check()?;
    Ok(Some(identity))
}

/// Writes `identity.json` atomically: a temp file in the same dir (mode 0600
/// on unix), fsync, rename over the old file, then fsync the dir (unix).
pub fn save(state_dir: &Path, identity: &Identity) -> Result<(), IdentityError> {
    let path = path(state_dir);
    let mut json = serde_json::to_vec_pretty(identity).expect("Identity serialises");
    json.push(b'\n');

    let mut suffix = [0u8; 8];
    getrandom::fill(&mut suffix).map_err(|e| KeyError::Rng(e.to_string()))?;
    let suffix: String = suffix.iter().map(|b| format!("{b:02x}")).collect();
    let tmp = state_dir.join(format!(".{IDENTITY_FILE}.{suffix}.tmp"));

    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let write = || -> io::Result<()> {
        let mut f = opts.open(&tmp)?;
        f.write_all(&json)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, &path)
    };
    if let Err(e) = write() {
        let _ = fs::remove_file(&tmp);
        return Err(io_err(&path)(e));
    }
    sync_dir(state_dir)
}

/// `<state_dir>/join-failed`.
pub fn join_failed_path(state_dir: &Path) -> PathBuf {
    state_dir.join(JOIN_FAILED_FILE)
}

/// Gives up the identity of a joiner whose invite was rejected, or of a node
/// the mesh has removed: writes the `join-failed` marker, then removes
/// `identity.json`, so the node counts as unconfigured again. The raft data
/// directory is left alone because the database is still open;
/// [`discard_failed_join`] clears it at the next start.
pub fn mark_join_failed(state_dir: &Path) -> Result<(), IdentityError> {
    let marker = join_failed_path(state_dir);
    let write = || -> io::Result<()> {
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&marker)?;
        f.sync_all()
    };
    write().map_err(io_err(&marker))?;
    sync_dir(state_dir)?;
    remove_identity(state_dir)?;
    sync_dir(state_dir)
}

/// Finishes what [`mark_join_failed`] started. If the marker is there:
/// removes `identity.json` (in case the earlier removal did not happen),
/// everything inside `data_dir`, and then the marker. Returns whether there
/// was anything to do. Must run before storage is opened. Safe to repeat
/// after a crash part way through: the marker goes last.
pub fn discard_failed_join(state_dir: &Path, data_dir: &Path) -> Result<bool, IdentityError> {
    let marker = join_failed_path(state_dir);
    match fs::symlink_metadata(&marker) {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(io_err(&marker)(e)),
    }
    remove_identity(state_dir)?;
    clear_dir(data_dir)?;
    fs::remove_file(&marker).map_err(io_err(&marker))?;
    sync_dir(state_dir)?;
    Ok(true)
}

fn remove_identity(state_dir: &Path) -> Result<(), IdentityError> {
    let path = path(state_dir);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err(&path)(e)),
    }
}

/// Removes everything inside `dir` but not `dir` itself, which may be a
/// mount point or have its own ownership. A missing `dir` is fine.
fn clear_dir(dir: &Path) -> Result<(), IdentityError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err(dir)(e)),
    };
    for entry in entries {
        let entry = entry.map_err(io_err(dir))?;
        let path = entry.path();
        // `file_type` does not follow symlinks, so a link is unlinked, not
        // followed.
        let is_dir = entry.file_type().map_err(io_err(&path))?.is_dir();
        let removed = if is_dir {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        removed.map_err(io_err(&path))?;
    }
    Ok(())
}

/// fsyncs `dir` so a rename, create or unlink inside it is durable (unix;
/// a no-op elsewhere).
fn sync_dir(dir: &Path) -> Result<(), IdentityError> {
    #[cfg(unix)]
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(io_err(dir))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// 128 random bits as 32 lowercase hex characters.
pub fn new_instance_id() -> Result<String, KeyError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| KeyError::Rng(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// Gives an identity that has no `instance_id` (one written before the field
/// existed, or a founder's from before founders had one) a new one and saves
/// it. Returns the id. Nothing is written when there already is one.
pub fn ensure_instance_id(
    state_dir: &Path,
    identity: &mut Identity,
) -> Result<String, IdentityError> {
    if let Some(id) = &identity.instance_id {
        return Ok(id.clone());
    }
    let id = new_instance_id()?;
    identity.instance_id = Some(id.clone());
    save(state_dir, identity)?;
    Ok(id)
}

/// Records in `pending_join` that the mesh accepted this machine's
/// registration, and saves it. Does nothing without a pending join or when
/// it is already recorded.
pub fn mark_registered(state_dir: &Path, identity: &mut Identity) -> Result<(), IdentityError> {
    match &mut identity.pending_join {
        Some(pending) if !pending.registered => pending.registered = true,
        _ => return Ok(()),
    }
    save(state_dir, identity)
}

/// Removes `pending_join` from `identity` and saves it. Does nothing if it
/// was already clear.
pub fn clear_pending_join(state_dir: &Path, identity: &mut Identity) -> Result<(), IdentityError> {
    if identity.pending_join.take().is_some() {
        save(state_dir, identity)?;
    }
    Ok(())
}

/// Loads the identity, or creates and saves it once:
/// 1. An existing `identity.json` wins; `input` is ignored. The caller
///    compares [`Identity::endpoint`] and the replicated endpoint with
///    `--wg-endpoint` to decide on `SetEndpoint`.
/// 2. A join token: the identity comes from the token, with `pending_join`
///    set. An explicit `input.endpoint` must equal the token's.
/// 3. `new_mesh`: this node founds the mesh as node 1 on the first host
///    address of `mesh_cidr`, with a new key. `input.endpoint` is required.
///
/// `now` is unix seconds, checked against the token's `expires_at`.
pub fn resolve(
    state_dir: &Path,
    input: BootstrapInput,
    now: u64,
) -> Result<Identity, IdentityError> {
    if let Some(existing) = load(state_dir)? {
        return Ok(existing);
    }
    let identity = match (input.join_token, input.new_mesh) {
        (Some(_), true) => return Err(IdentityError::ConflictingBootstrap),
        (Some(token), false) => from_token(
            &InviteToken::decode(&token)?,
            input.endpoint,
            input.wg_port,
            now,
        )?,
        (None, true) => founder(input.mesh_cidr, input.endpoint, input.wg_port)?,
        (None, false) => return Err(IdentityError::NotBootstrapped),
    };
    save(state_dir, &identity)?;
    Ok(identity)
}

/// Whether `token` should replace the join `identity` is still waiting on:
/// `identity` has a `pending_join`, and `token` is a usable invite (it
/// decodes, has not expired at `now`, and makes a valid identity with
/// `endpoint` and `wg_port`) other than the pending one.
///
/// `Ok(false)` without a pending join, or when `token` is the invite that is
/// already pending. `Err` when `token` cannot be used, in which case the
/// pending join is all this node has and must be kept.
pub fn supersedes_pending_join(
    identity: &Identity,
    token: &str,
    endpoint: Option<SocketAddr>,
    wg_port: u16,
    now: u64,
) -> Result<bool, IdentityError> {
    let Some(pending) = &identity.pending_join else {
        return Ok(false);
    };
    let token = InviteToken::decode(token)?;
    if token.invite_id == pending.invite_id {
        return Ok(false);
    }
    from_token(&token, endpoint, wg_port, now)?;
    Ok(true)
}

fn from_token(
    token: &InviteToken,
    endpoint: Option<SocketAddr>,
    wg_port: u16,
    now: u64,
) -> Result<Identity, IdentityError> {
    if now >= token.expires_at {
        return Err(IdentityError::TokenExpired {
            expires_at: token.expires_at,
            now,
        });
    }
    let invite = parse_full_endpoint(&token.endpoint)
        .map_err(|_| IdentityError::InvalidEndpoint(token.endpoint.clone()))?;
    if let Some(given) = endpoint
        && given != invite
    {
        return Err(IdentityError::EndpointMismatch { given, invite });
    }
    let identity = Identity {
        v: IDENTITY_VERSION,
        node_id: token.node_id,
        private_key: PrivateKey::new(token.private_key.clone()),
        mesh_ip: token.mesh_ip,
        mesh_cidr: token.mesh_cidr,
        wg_port,
        endpoint: invite,
        instance_id: Some(new_instance_id()?),
        pending_join: Some(PendingJoin {
            invite_id: token.invite_id.clone(),
            join_addrs: token.join_addrs.clone(),
            bootstrap_peers: token.peers.clone(),
            registered: false,
        }),
    };
    identity.check()?;
    Ok(identity)
}

fn founder(
    mesh_cidr: Ipv4Net,
    endpoint: Option<SocketAddr>,
    wg_port: u16,
) -> Result<Identity, IdentityError> {
    let endpoint = endpoint.ok_or(IdentityError::EndpointRequired)?;
    let mesh_cidr = mesh_cidr.trunc();
    let mesh_ip = mesh_cidr
        .hosts()
        .next()
        .ok_or(IdentityError::NoHostAddress(mesh_cidr))?;
    let (private_key, _) = keys::generate_keypair()?;
    let identity = Identity {
        v: IDENTITY_VERSION,
        node_id: FOUNDER_NODE_ID,
        private_key,
        mesh_ip,
        mesh_cidr,
        wg_port,
        endpoint,
        instance_id: Some(new_instance_id()?),
        pending_join: None,
    };
    identity.check()?;
    Ok(identity)
}

impl Identity {
    /// This node's public key.
    pub fn public_key(&self) -> Result<String, KeyError> {
        self.private_key.public_key()
    }

    fn check(&self) -> Result<(), IdentityError> {
        if self.v != IDENTITY_VERSION {
            return Err(IdentityError::UnsupportedVersion(self.v));
        }
        keys::public_key_of(self.private_key.expose())?;
        if !self.mesh_cidr.contains(&self.mesh_ip) {
            return Err(IdentityError::IpOutsideCidr {
                mesh_ip: self.mesh_ip,
                mesh_cidr: self.mesh_cidr,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::mesh::token::TOKEN_VERSION;

    const KEY: &str = "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=";
    const NOW: u64 = 1_700_000_000;
    const INSTANCE: &str = "00112233445566778899aabbccddeeff";

    /// `id` with the fixed test instance id, after checking that it has a
    /// well-formed random one.
    fn fixed_instance(mut id: Identity) -> Identity {
        let instance = id.instance_id.as_deref().expect("a joiner has one");
        assert_eq!(instance.len(), 32, "{instance}");
        assert!(
            instance
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "{instance}"
        );
        id.instance_id = Some(INSTANCE.into());
        id
    }

    fn token() -> InviteToken {
        InviteToken {
            v: TOKEN_VERSION,
            invite_id: "0123456789abcdef0123456789abcdef".into(),
            expires_at: NOW + 3600,
            node_id: 2,
            mesh_ip: "10.99.0.2".parse().unwrap(),
            mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            endpoint: "198.51.100.2:51820".into(),
            private_key: KEY.into(),
            peers: vec![TokenPeer {
                node_id: 1,
                public_key: "HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw=".into(),
                endpoint: Some("203.0.113.1:51820".into()),
                mesh_ip: "10.99.0.1".parse().unwrap(),
            }],
            join_addrs: vec!["10.99.0.1:7401".into()],
        }
    }

    fn input() -> BootstrapInput {
        BootstrapInput {
            new_mesh: false,
            mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            join_token: None,
            wg_port: 51820,
            endpoint: None,
        }
    }

    fn founder_input() -> BootstrapInput {
        BootstrapInput {
            new_mesh: true,
            endpoint: Some("203.0.113.1:51820".parse().unwrap()),
            ..input()
        }
    }

    fn joiner_input() -> BootstrapInput {
        BootstrapInput {
            join_token: Some(token().encode()),
            ..input()
        }
    }

    fn sample() -> Identity {
        Identity {
            v: 1,
            node_id: 2,
            private_key: PrivateKey::new(KEY),
            mesh_ip: "10.99.0.2".parse().unwrap(),
            mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            wg_port: 51820,
            endpoint: "198.51.100.2:51820".parse().unwrap(),
            instance_id: Some(INSTANCE.into()),
            pending_join: Some(PendingJoin {
                invite_id: "0123456789abcdef0123456789abcdef".into(),
                join_addrs: vec!["10.99.0.1:7401".into()],
                bootstrap_peers: token().peers,
                registered: false,
            }),
        }
    }

    #[test]
    fn json_shape_matches_contract() {
        let v = serde_json::to_value(sample()).unwrap();
        assert_eq!(
            v,
            json!({
                "v": 1,
                "node_id": 2,
                "private_key": KEY,
                "mesh_ip": "10.99.0.2",
                "mesh_cidr": "10.99.0.0/16",
                "wg_port": 51820,
                "endpoint": "198.51.100.2:51820",
                "instance_id": INSTANCE,
                "pending_join": {
                    "invite_id": "0123456789abcdef0123456789abcdef",
                    "join_addrs": ["10.99.0.1:7401"],
                    "bootstrap_peers": [{
                        "node_id": 1,
                        "public_key": "HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw=",
                        "endpoint": "203.0.113.1:51820",
                        "mesh_ip": "10.99.0.1"
                    }]
                }
            })
        );
        let mut founder = sample();
        founder.pending_join = None;
        founder.instance_id = None;
        let v = serde_json::to_value(founder).unwrap();
        assert!(v.get("pending_join").is_none(), "{v}");
        assert!(v.get("instance_id").is_none(), "{v}");

        let mut registered = sample();
        registered.pending_join.as_mut().unwrap().registered = true;
        let v = serde_json::to_value(registered).unwrap();
        assert_eq!(v["pending_join"]["registered"], json!(true));
    }

    /// An identity written before `instance_id` and `registered` existed.
    #[test]
    fn an_older_identity_loads_as_unregistered_without_an_instance() {
        let dir = TempDir::new().unwrap();
        let mut v = serde_json::to_value(sample()).unwrap();
        v.as_object_mut().unwrap().remove("instance_id");
        fs::write(path(dir.path()), v.to_string()).unwrap();
        let mut loaded = load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.instance_id, None);
        assert!(!loaded.pending_join.as_ref().unwrap().registered);

        // A joiner gets an instance id before it registers, and keeps it.
        let made = ensure_instance_id(dir.path(), &mut loaded).unwrap();
        assert_eq!(loaded.instance_id.as_deref(), Some(made.as_str()));
        assert_eq!(load(dir.path()).unwrap(), Some(loaded.clone()));
        assert_eq!(ensure_instance_id(dir.path(), &mut loaded).unwrap(), made);

        // So does a node with no pending join: it renews its lease with it.
        let mut joined = sample();
        joined.pending_join = None;
        joined.instance_id = None;
        let made = ensure_instance_id(dir.path(), &mut joined).unwrap();
        assert_eq!(made.len(), 32, "{made}");
        assert_eq!(joined.instance_id.as_deref(), Some(made.as_str()));
        assert_eq!(load(dir.path()).unwrap(), Some(joined.clone()));
        // Nothing is written for an identity that has one.
        assert_eq!(
            ensure_instance_id(&dir.path().join("nope"), &mut joined).unwrap(),
            made
        );
    }

    #[test]
    fn mark_registered_saves_once() {
        let dir = TempDir::new().unwrap();
        let mut id = sample();
        save(dir.path(), &id).unwrap();
        mark_registered(dir.path(), &mut id).unwrap();
        assert!(id.pending_join.as_ref().unwrap().registered);
        assert_eq!(load(dir.path()).unwrap(), Some(id.clone()));
        // Already recorded, and nothing pending: no write, so a missing dir
        // is fine.
        mark_registered(&dir.path().join("nope"), &mut id).unwrap();
        id.pending_join = None;
        mark_registered(&dir.path().join("nope"), &mut id).unwrap();
    }

    #[test]
    fn instance_ids_are_random_hex() {
        let a = new_instance_id().unwrap();
        assert_eq!(a.len(), 32);
        assert!(
            a.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        assert_ne!(a, new_instance_id().unwrap());
    }

    #[test]
    fn debug_redacts_private_key() {
        let s = format!("{:?}", sample());
        assert!(!s.contains(KEY), "{s}");
        assert!(s.contains("10.99.0.2"), "{s}");
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = TempDir::new().unwrap();
        assert!(load(dir.path()).unwrap().is_none());
        save(dir.path(), &sample()).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(sample()));
        // Overwrite.
        let mut changed = sample();
        changed.pending_join = None;
        save(dir.path(), &changed).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(changed));
        // No temp files left behind.
        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec![IDENTITY_FILE.to_string()]);
    }

    #[cfg(unix)]
    #[test]
    fn save_uses_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        save(dir.path(), &sample()).unwrap();
        let mode = fs::metadata(path(dir.path())).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn load_refuses_symlink() {
        let dir = TempDir::new().unwrap();
        let real = dir.path().join("real.json");
        fs::write(&real, serde_json::to_vec(&sample()).unwrap()).unwrap();
        std::os::unix::fs::symlink(&real, path(dir.path())).unwrap();
        assert!(matches!(
            load(dir.path()),
            Err(IdentityError::NotRegularFile(_))
        ));
    }

    #[test]
    fn load_refuses_directory() {
        let dir = TempDir::new().unwrap();
        fs::create_dir(path(dir.path())).unwrap();
        assert!(matches!(
            load(dir.path()),
            Err(IdentityError::NotRegularFile(_))
        ));
    }

    #[test]
    fn load_rejects_other_versions_and_bad_json() {
        let dir = TempDir::new().unwrap();
        let mut v = serde_json::to_value(sample()).unwrap();
        v["v"] = json!(2);
        fs::write(path(dir.path()), v.to_string()).unwrap();
        assert!(matches!(
            load(dir.path()),
            Err(IdentityError::UnsupportedVersion(2))
        ));
        fs::write(path(dir.path()), "{not json").unwrap();
        assert!(matches!(load(dir.path()), Err(IdentityError::Json { .. })));
    }

    #[test]
    fn load_rejects_bad_key() {
        let dir = TempDir::new().unwrap();
        let mut v = serde_json::to_value(sample()).unwrap();
        v["private_key"] = json!("AAAA");
        fs::write(path(dir.path()), v.to_string()).unwrap();
        let err = load(dir.path()).unwrap_err();
        assert!(matches!(err, IdentityError::Key(_)), "{err}");
    }

    #[test]
    fn clear_pending_join_saves() {
        let dir = TempDir::new().unwrap();
        let mut id = sample();
        save(dir.path(), &id).unwrap();
        clear_pending_join(dir.path(), &mut id).unwrap();
        assert_eq!(id.pending_join, None);
        assert_eq!(load(dir.path()).unwrap().unwrap().pending_join, None);
        // Already clear: no write, so a missing dir is fine.
        clear_pending_join(&dir.path().join("nope"), &mut id).unwrap();
    }

    #[test]
    fn existing_identity_wins() {
        let dir = TempDir::new().unwrap();
        save(dir.path(), &sample()).unwrap();
        for i in [
            input(),
            founder_input(),
            joiner_input(),
            BootstrapInput {
                endpoint: Some("192.0.2.9:51820".parse().unwrap()),
                ..joiner_input()
            },
            BootstrapInput {
                new_mesh: true,
                ..joiner_input()
            },
        ] {
            assert_eq!(resolve(dir.path(), i, NOW).unwrap(), sample());
        }
    }

    #[test]
    fn nothing_is_not_bootstrapped() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(
            resolve(dir.path(), input(), NOW),
            Err(IdentityError::NotBootstrapped)
        ));
        assert!(load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn token_and_new_mesh_conflict() {
        let dir = TempDir::new().unwrap();
        let i = BootstrapInput {
            new_mesh: true,
            ..joiner_input()
        };
        assert!(matches!(
            resolve(dir.path(), i, NOW),
            Err(IdentityError::ConflictingBootstrap)
        ));
    }

    #[test]
    fn founder_identity() {
        let dir = TempDir::new().unwrap();
        let id = resolve(dir.path(), founder_input(), NOW).unwrap();
        assert_eq!(id.v, 1);
        assert_eq!(id.node_id, 1);
        assert_eq!(id.mesh_ip, "10.99.0.1".parse::<Ipv4Addr>().unwrap());
        assert_eq!(id.mesh_cidr, "10.99.0.0/16".parse::<Ipv4Net>().unwrap());
        assert_eq!(id.wg_port, 51820);
        assert_eq!(id.endpoint.to_string(), "203.0.113.1:51820");
        assert_eq!(id.pending_join, None);
        // The founder renews its lease as this machine.
        let instance = id.instance_id.as_deref().expect("the founder has one");
        assert_eq!(instance.len(), 32, "{instance}");
        keys::validate_public_key(&id.public_key().unwrap()).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(id.clone()));
        // A second resolve keeps the same key.
        assert_eq!(resolve(dir.path(), founder_input(), NOW).unwrap(), id);
    }

    #[test]
    fn founder_uses_first_host_of_custom_cidr() {
        let dir = TempDir::new().unwrap();
        let i = BootstrapInput {
            mesh_cidr: "172.20.5.9/24".parse().unwrap(),
            wg_port: 51000,
            ..founder_input()
        };
        let id = resolve(dir.path(), i, NOW).unwrap();
        assert_eq!(id.mesh_ip, "172.20.5.1".parse::<Ipv4Addr>().unwrap());
        assert_eq!(id.mesh_cidr, "172.20.5.0/24".parse::<Ipv4Net>().unwrap());
        assert_eq!(id.wg_port, 51000);
    }

    #[test]
    fn founder_requires_endpoint() {
        let dir = TempDir::new().unwrap();
        let i = BootstrapInput {
            endpoint: None,
            ..founder_input()
        };
        assert!(matches!(
            resolve(dir.path(), i, NOW),
            Err(IdentityError::EndpointRequired)
        ));
        assert!(load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn load_rejects_an_endpoint_that_is_not_ip_port() {
        let dir = TempDir::new().unwrap();
        let mut v = serde_json::to_value(sample()).unwrap();
        v["endpoint"] = json!("example.com:51820");
        fs::write(path(dir.path()), v.to_string()).unwrap();
        assert!(matches!(load(dir.path()), Err(IdentityError::Json { .. })));
    }

    #[test]
    fn bootstrap_input_debug_redacts_the_join_token() {
        let i = joiner_input();
        let raw = i.join_token.clone().unwrap();
        let s = format!("{i:?}");
        assert!(!s.contains(&raw), "{s}");
        assert!(!s.contains(KEY), "{s}");
        assert!(s.contains("<redacted>"), "{s}");
        assert!(s.contains("51820"), "{s}");
        // Nothing to redact without a token.
        assert!(format!("{:?}", input()).contains("join_token: None"));
    }

    #[test]
    fn mark_join_failed_removes_the_identity_and_leaves_the_marker() {
        let dir = TempDir::new().unwrap();
        save(dir.path(), &sample()).unwrap();
        mark_join_failed(dir.path()).unwrap();
        assert!(load(dir.path()).unwrap().is_none());
        assert!(!path(dir.path()).exists());
        assert!(join_failed_path(dir.path()).is_file());
        // Repeating it is fine.
        mark_join_failed(dir.path()).unwrap();
    }

    #[test]
    fn discard_failed_join_clears_identity_data_and_marker() {
        let dir = TempDir::new().unwrap();
        let state = dir.path().join("state");
        let data = dir.path().join("data");
        fs::create_dir_all(data.join("sub")).unwrap();
        fs::create_dir_all(&state).unwrap();
        fs::write(data.join("CURRENT"), "x").unwrap();
        fs::write(data.join("sub").join("000001.log"), "y").unwrap();
        // The marker without the identity removal having happened.
        save(&state, &sample()).unwrap();
        fs::write(join_failed_path(&state), "").unwrap();

        assert!(discard_failed_join(&state, &data).unwrap());
        assert!(load(&state).unwrap().is_none());
        assert!(!join_failed_path(&state).exists());
        assert!(data.is_dir(), "the data dir itself stays");
        assert_eq!(fs::read_dir(&data).unwrap().count(), 0);
        // Nothing left to do, and a missing data dir is fine.
        assert!(!discard_failed_join(&state, &data).unwrap());
        fs::write(join_failed_path(&state), "").unwrap();
        assert!(discard_failed_join(&state, &dir.path().join("nope")).unwrap());
    }

    #[test]
    fn discard_failed_join_without_a_marker_touches_nothing() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        fs::write(data.join("CURRENT"), "x").unwrap();
        save(dir.path(), &sample()).unwrap();
        assert!(!discard_failed_join(dir.path(), &data).unwrap());
        assert_eq!(load(dir.path()).unwrap(), Some(sample()));
        assert!(data.join("CURRENT").is_file());
    }

    #[test]
    fn joiner_identity_from_token() {
        let dir = TempDir::new().unwrap();
        let id = resolve(dir.path(), joiner_input(), NOW).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(id.clone()));
        assert_eq!(fixed_instance(id.clone()), sample());
        // The instance id is made once: a second resolve loads the same one.
        assert_eq!(resolve(dir.path(), joiner_input(), NOW).unwrap(), id);
        // Another machine with the same token gets another id.
        let other = TempDir::new().unwrap();
        let twin = resolve(other.path(), joiner_input(), NOW).unwrap();
        assert_ne!(twin.instance_id, id.instance_id);
        assert_eq!(fixed_instance(twin), sample());
    }

    #[test]
    fn joiner_accepts_matching_endpoint() {
        let dir = TempDir::new().unwrap();
        let i = BootstrapInput {
            endpoint: Some("198.51.100.2:51820".parse().unwrap()),
            ..joiner_input()
        };
        assert_eq!(
            fixed_instance(resolve(dir.path(), i, NOW).unwrap()),
            sample()
        );
    }

    #[test]
    fn joiner_endpoint_comparison_ignores_spelling() {
        let dir = TempDir::new().unwrap();
        let mut t = token();
        t.endpoint = "[2001:DB8::2]:51820".into();
        let i = BootstrapInput {
            join_token: Some(t.encode()),
            endpoint: Some("[2001:db8::2]:51820".parse().unwrap()),
            ..input()
        };
        let id = resolve(dir.path(), i, NOW).unwrap();
        assert_eq!(id.endpoint.to_string(), "[2001:db8::2]:51820");
        // The file holds the canonical spelling.
        let raw = fs::read_to_string(path(dir.path())).unwrap();
        assert!(raw.contains("\"[2001:db8::2]:51820\""), "{raw}");
    }

    #[test]
    fn joiner_rejects_a_token_endpoint_that_is_not_ip_port() {
        let dir = TempDir::new().unwrap();
        let mut t = token();
        t.endpoint = "node2.example:51820".into();
        let i = BootstrapInput {
            join_token: Some(t.encode()),
            ..input()
        };
        assert!(matches!(
            resolve(dir.path(), i, NOW),
            Err(IdentityError::InvalidEndpoint(_))
        ));
        assert!(load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn joiner_rejects_endpoint_mismatch() {
        let dir = TempDir::new().unwrap();
        let i = BootstrapInput {
            endpoint: Some("192.0.2.9:51820".parse().unwrap()),
            ..joiner_input()
        };
        let err = resolve(dir.path(), i, NOW).unwrap_err();
        assert!(
            matches!(err, IdentityError::EndpointMismatch { .. }),
            "{err}"
        );
        assert!(load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn joiner_rejects_expired_token() {
        let dir = TempDir::new().unwrap();
        let expires_at = token().expires_at;
        for now in [expires_at, expires_at + 1] {
            assert!(matches!(
                resolve(dir.path(), joiner_input(), now),
                Err(IdentityError::TokenExpired { .. })
            ));
        }
        resolve(dir.path(), joiner_input(), expires_at - 1).unwrap();
    }

    fn other_token() -> InviteToken {
        InviteToken {
            invite_id: "fedcba9876543210fedcba9876543210".into(),
            node_id: 3,
            mesh_ip: "10.99.0.3".parse().unwrap(),
            ..token()
        }
    }

    #[test]
    fn a_different_usable_token_supersedes_a_pending_join() {
        let supersedes = |id: &Identity, t: &InviteToken, now| {
            supersedes_pending_join(id, &t.encode(), None, 51820, now)
        };
        assert!(supersedes(&sample(), &other_token(), NOW).unwrap());
        // The invite that is already pending, even with other details.
        assert!(!supersedes(&sample(), &token(), NOW).unwrap());
        let same_invite = InviteToken {
            invite_id: token().invite_id,
            ..other_token()
        };
        assert!(!supersedes(&sample(), &same_invite, NOW).unwrap());
        // A node that has joined has nothing pending.
        let mut joined = sample();
        joined.pending_join = None;
        assert!(!supersedes(&joined, &other_token(), NOW).unwrap());
    }

    #[test]
    fn an_unusable_token_does_not_supersede_a_pending_join() {
        let id = sample();
        let expires_at = other_token().expires_at;
        assert!(matches!(
            supersedes_pending_join(&id, &other_token().encode(), None, 51820, expires_at),
            Err(IdentityError::TokenExpired { .. })
        ));
        assert!(matches!(
            supersedes_pending_join(&id, "faber2.abc", None, 51820, NOW),
            Err(IdentityError::Token(TokenError::BadPrefix))
        ));
        let mut outside = other_token();
        outside.mesh_ip = "10.100.0.3".parse().unwrap();
        assert!(matches!(
            supersedes_pending_join(&id, &outside.encode(), None, 51820, NOW),
            Err(IdentityError::IpOutsideCidr { .. })
        ));
        let given = Some("192.0.2.9:51820".parse().unwrap());
        assert!(matches!(
            supersedes_pending_join(&id, &other_token().encode(), given, 51820, NOW),
            Err(IdentityError::EndpointMismatch { .. })
        ));
    }

    #[test]
    fn joiner_rejects_bad_token() {
        let dir = TempDir::new().unwrap();
        let i = BootstrapInput {
            join_token: Some("faber2.abc".into()),
            ..input()
        };
        assert!(matches!(
            resolve(dir.path(), i, NOW),
            Err(IdentityError::Token(TokenError::BadPrefix))
        ));
    }

    #[test]
    fn joiner_rejects_inconsistent_token() {
        let dir = TempDir::new().unwrap();
        let mut t = token();
        t.mesh_ip = "10.100.0.2".parse().unwrap();
        let i = BootstrapInput {
            join_token: Some(t.encode()),
            ..input()
        };
        assert!(matches!(
            resolve(dir.path(), i, NOW),
            Err(IdentityError::IpOutsideCidr { .. })
        ));
        let mut t = token();
        t.private_key = "AAAA".into();
        let i = BootstrapInput {
            join_token: Some(t.encode()),
            ..input()
        };
        assert!(matches!(
            resolve(dir.path(), i, NOW),
            Err(IdentityError::Key(_))
        ));
    }

    #[test]
    fn errors_do_not_leak_key() {
        let dir = TempDir::new().unwrap();
        let mut v = serde_json::to_value(sample()).unwrap();
        v["mesh_ip"] = json!("not an ip");
        fs::write(path(dir.path()), v.to_string()).unwrap();
        let err = load(dir.path()).unwrap_err();
        let s = format!("{err} {err:?}");
        assert!(!s.contains(KEY), "{s}");
    }
}
