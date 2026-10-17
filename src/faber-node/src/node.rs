//! The request handler: every trust check, then the CRI and volume calls.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use faber_api_types::workload::limits;
use faber_container_runtime::{
    ContainerRuntime, Containerd, NodeRuntimeSpec, PodNetworkSpec, RuntimeError,
};
use faber_cri::{
    ContainerFilter, ContainerState as CriContainerState, CriClient, CriError, PodSandboxFilter,
    PodSandboxState,
};
use faber_image::ImagePuller;
use faber_node_protocol::{
    ContainerInfo, ContainerState, ErrorCode, LABEL_MANAGED, NodeError, PROTOCOL_VERSION,
    ReplicaInfo, ReplicaRef, Request, Response, SandboxState,
};

use crate::config::UidRange;
use crate::cri_config::{self, ANNOTATION_SPEC_HASH, ANNOTATION_UID_BASE, ResolvedMount};
use crate::dirs::DirPolicy;
use crate::host_net::{HostNetworks, check_subnet_free};
use crate::trust;
use crate::volume::{EnsureVolume, Owner, VolumeError, VolumeManager, VolumeRef};

/// A failed request, ready to be sent as an error response. A local type so
/// the runtime and volume errors convert with `?`.
pub(crate) struct Fail(NodeError);

impl Fail {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self(NodeError::new(code, message))
    }
}

impl From<NodeError> for Fail {
    fn from(e: NodeError) -> Self {
        Self(e)
    }
}

impl From<CriError> for Fail {
    fn from(e: CriError) -> Self {
        let code = if e.is_not_found() {
            ErrorCode::NotFound
        } else {
            ErrorCode::Runtime
        };
        Self::new(code, e.to_string())
    }
}

impl From<VolumeError> for Fail {
    fn from(e: VolumeError) -> Self {
        let code = match e {
            VolumeError::Invalid(_) => ErrorCode::InvalidRequest,
            VolumeError::NotMounted(_) => ErrorCode::NotFound,
            _ => ErrorCode::Runtime,
        };
        Self::new(code, e.to_string())
    }
}

impl From<RuntimeError> for Fail {
    fn from(e: RuntimeError) -> Self {
        // The message names the step; the cause (a path, an errno) stays in
        // faber-node's log and is not sent to fabercp.
        tracing::error!(error = ?e, "writing the runtime config failed");
        Self::new(ErrorCode::Runtime, e.to_string())
    }
}

fn managed_selector() -> HashMap<String, String> {
    HashMap::from([(LABEL_MANAGED.to_owned(), "true".to_owned())])
}

fn is_managed(labels: &HashMap<String, String>) -> bool {
    labels.get(LABEL_MANAGED).map(String::as_str) == Some("true")
}

/// One lock per replica, so concurrent requests for a replica's sandbox and
/// containers run one after the other and find each other's work.
#[derive(Default)]
struct ReplicaLocks(std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>);

impl ReplicaLocks {
    async fn lock(&self, replica: &ReplicaRef) -> tokio::sync::OwnedMutexGuard<()> {
        let key = format!(
            "{}/{}/{}/{}/{}",
            replica.project, replica.env, replica.workload, replica.revision, replica.replica_index
        );
        let lock = {
            let mut map = self.0.lock().unwrap_or_else(|e| e.into_inner());
            // A lock that only the map holds is idle: forget it.
            map.retain(|_, l| Arc::strong_count(l) > 1);
            Arc::clone(map.entry(key).or_default())
        };
        lock.lock_owned().await
    }
}

type UidReservations = std::sync::Mutex<HashMap<u32, (Owner, usize)>>;

/// Runs `work` in its own task and waits for it. If the caller is dropped the
/// task still runs to the end.
async fn detached(
    work: impl std::future::Future<Output = Result<Response, Fail>> + Send + 'static,
) -> Result<Response, Fail> {
    tokio::spawn(work).await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "a request task failed");
        Err(Fail::new(ErrorCode::Runtime, "the request did not finish"))
    })
}

/// A `uid_base` reserved for its owner while a create runs. Dropped once the
/// claim is visible (annotation or marker) or the create failed.
struct UidReservation {
    map: Arc<UidReservations>,
    uid_base: u32,
}

impl Drop for UidReservation {
    fn drop(&mut self) {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, count)) = map.get_mut(&self.uid_base) {
            *count -= 1;
            if *count == 0 {
                map.remove(&self.uid_base);
            }
        }
    }
}

/// What the runtime config needs of the host: where it goes, who may own
/// the directories, and what the host's network looks like.
pub struct RuntimeHost {
    pub containerd: Containerd,
    pub dirs: DirPolicy,
    pub networks: Arc<dyn HostNetworks>,
    /// The WireGuard interface a container subnet must be routed into.
    pub mesh_interface: String,
}

pub struct Node<C> {
    sandbox_locks: ReplicaLocks,
    /// Held while a `uid_base` is checked and reserved, so two requests
    /// cannot both pass the check. Not held across the create.
    uid_lock: tokio::sync::Mutex<()>,
    /// Bases whose create is running, with their owner and how many of that
    /// owner's requests run: the claim is not yet visible to the check.
    reservations: Arc<UidReservations>,
    uid_range: UidRange,
    runtime_handler: String,
    cri: C,
    images: ImagePuller<C>,
    volumes: Arc<dyn VolumeManager>,
    runtime: RuntimeHost,
}

impl<C: CriClient + Clone + 'static> Node<C> {
    pub fn new(
        uid_range: UidRange,
        runtime_handler: String,
        cri: C,
        volumes: Arc<dyn VolumeManager>,
        runtime: RuntimeHost,
    ) -> Self {
        Self {
            runtime,
            sandbox_locks: ReplicaLocks::default(),
            uid_lock: tokio::sync::Mutex::new(()),
            reservations: Arc::default(),
            uid_range,
            runtime_handler,
            images: ImagePuller::new(cri.clone()),
            cri,
            volumes,
        }
    }

    /// Answers one request. Never panics on input; every failure is a typed
    /// error response.
    pub async fn handle(&self, request: Request) -> Response {
        match self.dispatch(request).await {
            Ok(response) => response,
            Err(e) => e.0.into(),
        }
    }

    async fn dispatch(&self, request: Request) -> Result<Response, Fail> {
        match request {
            Request::Version { client_version } => {
                if client_version != PROTOCOL_VERSION {
                    return Err(Fail::new(
                        ErrorCode::VersionMismatch,
                        format!("this node speaks protocol {PROTOCOL_VERSION}"),
                    ));
                }
                Ok(Response::Version {
                    protocol_version: PROTOCOL_VERSION,
                    node_version: env!("CARGO_PKG_VERSION").to_owned(),
                })
            }
            Request::PullImage { image } => {
                trust::check_image(&image)?;
                let image_id = self
                    .images
                    .ensure(&image)
                    .await
                    .map_err(|e| Fail::new(ErrorCode::Runtime, e.to_string()))?;
                Ok(Response::Image { image_id })
            }
            Request::EnsureSandbox {
                replica,
                uid_base,
                dns,
                ports,
            } => {
                trust::check_replica(&replica)?;
                trust::check_uid_base(&self.uid_range, uid_base)?;
                trust::check_dns(&dns)?;
                trust::check_ports(&ports)?;
                let guard = self.sandbox_locks.lock(&replica).await;
                let reservation = self.reserve_uid_base(&replica, uid_base).await?;
                let cri = self.cri.clone();
                let handler = self.runtime_handler.clone();
                // The task owns the locks, so a dropped request does not free
                // the base while the sandbox is still being started.
                detached(async move {
                    let _held = (guard, reservation);
                    Self::ensure_sandbox(cri, handler, replica, uid_base, dns, ports).await
                })
                .await
            }
            Request::CreateContainer {
                replica,
                sandbox_id,
                container,
                env,
                uid_base,
            } => {
                trust::check_replica(&replica)?;
                trust::check_uid_base(&self.uid_range, uid_base)?;
                trust::check_runtime_id("sandbox_id", &sandbox_id)?;
                trust::check_container(&container, &env)?;
                let _guard = self.sandbox_locks.lock(&replica).await;
                let sandbox = self.cri.pod_sandbox_status(&sandbox_id).await?;
                let want = replica.labels();
                let have: BTreeMap<_, _> = sandbox.labels.clone().into_iter().collect();
                let ready = sandbox.state == PodSandboxState::SandboxReady as i32;
                let owner = sandbox.annotations.get(ANNOTATION_UID_BASE);
                if have != want || owner != Some(&uid_base.to_string()) {
                    return Err(Fail::new(
                        ErrorCode::Forbidden,
                        "the sandbox does not belong to this replica and uid_base",
                    ));
                }
                if !ready {
                    return Err(Fail::new(ErrorCode::Runtime, "the sandbox is not ready"));
                }
                let mut mounts = Vec::new();
                for m in &container.mounts {
                    let volume = VolumeRef::new(&replica, &m.volume)?;
                    mounts.push(ResolvedMount {
                        container_path: m.path.clone(),
                        host_path: self.volumes.mount_path(&volume).await?,
                        read_only: m.read_only,
                    });
                }
                let name = cri_config::container_name(&replica, &container.name);
                let existing = self
                    .cri
                    .list_containers(Some(ContainerFilter {
                        pod_sandbox_id: sandbox_id.clone(),
                        ..Default::default()
                    }))
                    .await?;
                let mut config =
                    cri_config::container_config(&replica, &container, &env, uid_base, 0, &mounts)
                        .map_err(|e| Fail::new(ErrorCode::InvalidRequest, e))?;
                let spec_hash = cri_config::spec_hash(&container, &env, uid_base, &mounts);
                // A retried request finds the container it already made, if
                // it was made from the same spec. One made from another spec
                // is stale: it is removed and replaced.
                let same_name =
                    |c: &&faber_cri::Container| c.metadata.as_ref().is_some_and(|m| m.name == name);
                let mut remaining = Vec::new();
                for c in existing.iter().filter(same_name) {
                    if c.state == CriContainerState::ContainerCreated as i32 {
                        if c.annotations.get(ANNOTATION_SPEC_HASH) == Some(&spec_hash) {
                            return Ok(Response::Container {
                                container_id: c.id.clone(),
                            });
                        }
                        self.cri.remove_container(&c.id).await?;
                    } else {
                        remaining.push(c);
                    }
                }
                let attempt = remaining
                    .iter()
                    .filter_map(|c| c.metadata.as_ref())
                    .map(|m| m.attempt + 1)
                    .max()
                    .unwrap_or(0);
                if let Some(metadata) = config.metadata.as_mut() {
                    metadata.attempt = attempt;
                }
                config
                    .annotations
                    .insert(ANNOTATION_SPEC_HASH.to_owned(), spec_hash);
                let sandbox_config =
                    cri_config::container_sandbox_config(&replica, uid_base, sandbox.metadata);
                let container_id = self
                    .cri
                    .create_container(&sandbox_id, config, sandbox_config)
                    .await?;
                Ok(Response::Container { container_id })
            }
            Request::StartContainer { container_id } => {
                self.check_managed_container(&container_id).await?;
                self.cri.start_container(&container_id).await?;
                Ok(Response::Done)
            }
            Request::StopContainer {
                container_id,
                grace_secs,
            } => {
                if grace_secs > limits::MAX_STOP_GRACE_SECS {
                    return Err(Fail::new(
                        ErrorCode::InvalidRequest,
                        format!("grace_secs must be at most {}", limits::MAX_STOP_GRACE_SECS),
                    ));
                }
                self.check_managed_container(&container_id).await?;
                self.cri
                    .stop_container(&container_id, i64::from(grace_secs))
                    .await?;
                Ok(Response::Done)
            }
            Request::RemoveContainer { container_id } => {
                match self.check_managed_container(&container_id).await {
                    Err(e) if e.0.code == ErrorCode::NotFound => return Ok(Response::Done),
                    other => other?,
                }
                self.cri.remove_container(&container_id).await?;
                Ok(Response::Done)
            }
            Request::RemoveSandbox { sandbox_id } => {
                trust::check_runtime_id("sandbox_id", &sandbox_id)?;
                let status = match self.cri.pod_sandbox_status(&sandbox_id).await {
                    Ok(status) => status,
                    Err(e) if e.is_not_found() => return Ok(Response::Done),
                    Err(e) => return Err(e.into()),
                };
                if !is_managed(&status.labels) {
                    return Err(Fail::new(
                        ErrorCode::Forbidden,
                        "the sandbox is not managed by faber",
                    ));
                }
                match self.cri.stop_pod_sandbox(&sandbox_id).await {
                    Ok(()) => {}
                    Err(e) if e.is_not_found() => return Ok(Response::Done),
                    Err(e) => return Err(e.into()),
                }
                match self.cri.remove_pod_sandbox(&sandbox_id).await {
                    Ok(()) => {}
                    Err(e) if e.is_not_found() => {}
                    Err(e) => return Err(e.into()),
                }
                Ok(Response::Done)
            }
            Request::EnsureVolume {
                replica,
                volume_name,
                kind,
                size_bytes,
                uid_base,
            } => {
                trust::check_replica(&replica)?;
                trust::check_volume_name(&volume_name)?;
                trust::check_uid_base(&self.uid_range, uid_base)?;
                trust::check_volume_size(kind, size_bytes)?;
                let volume = VolumeRef::new(&replica, &volume_name)?;
                let reservation = self.reserve_uid_base(&replica, uid_base).await?;
                let volumes = Arc::clone(&self.volumes);
                // As for the sandbox: the mkfs and mount finish, and the
                // reservation is held, even if the request is dropped.
                detached(async move {
                    let _held = reservation;
                    volumes
                        .ensure(&EnsureVolume {
                            volume,
                            kind,
                            size_bytes,
                            uid_base,
                        })
                        .await?;
                    Ok(Response::Done)
                })
                .await
            }
            Request::RemoveVolume {
                replica,
                volume_name,
            } => {
                trust::check_replica(&replica)?;
                trust::check_volume_name(&volume_name)?;
                self.volumes
                    .remove(&VolumeRef::new(&replica, &volume_name)?)
                    .await?;
                Ok(Response::Done)
            }
            Request::ListReplicas => self.list_replicas().await,
            Request::ListVolumes => Ok(Response::Volumes {
                volumes: self.volumes.list().await?,
            }),
            Request::VolumeSpace => Ok(Response::VolumeSpace {
                free_bytes: self.volumes.free_bytes().await?,
            }),
            Request::ConfigureRuntime { pod_network } => {
                if let Some(network) = &pod_network {
                    trust::check_pod_network(network)?;
                    self.check_subnet_free(network.subnet).await?;
                }
                self.check_runtime_dirs()?;
                let spec = NodeRuntimeSpec {
                    pod_network: pod_network.map(|n| PodNetworkSpec {
                        subnet: n.subnet,
                        mtu: n.mtu,
                    }),
                };
                let changed = self.runtime.containerd.reconcile(&spec).await?;
                Ok(Response::Changed { changed })
            }
            Request::RemoveCniConfig => {
                self.check_runtime_dirs()?;
                Ok(Response::Changed {
                    changed: self.runtime.containerd.remove_pod_network().await?,
                })
            }
        }
    }

    /// The directories root loads config from are checked again before every
    /// write, not only at startup. The detail (a path) is logged, not sent.
    fn check_runtime_dirs(&self) -> Result<(), Fail> {
        self.runtime
            .dirs
            .check_runtime_dirs(&self.runtime.containerd)
            .map_err(|e| {
                tracing::error!(role = e.role, detail = %e.detail, "a runtime directory cannot be trusted");
                Fail::new(
                    ErrorCode::Forbidden,
                    format!("the {} cannot be trusted; nothing was written", e.role),
                )
            })
    }

    /// A subnet that overlaps what the host uses, or that is not routed
    /// into the mesh interface or the bridge, is refused. The response says
    /// nothing of the host; the log says which network and interface.
    async fn check_subnet_free(&self, subnet: ipnet::Ipv4Net) -> Result<(), Fail> {
        let claims = self.runtime.networks.claims().await.map_err(|e| {
            tracing::error!(error = %e, "reading the host's addresses and routes failed");
            Fail::new(ErrorCode::Runtime, "the host's addresses cannot be read")
        })?;
        check_subnet_free(
            &claims,
            subnet,
            &self.runtime.containerd.bridge,
            &self.runtime.mesh_interface,
        )
        .map_err(|why| {
            tracing::error!(%subnet, %why, "a container subnet was refused");
            Fail::new(
                ErrorCode::Forbidden,
                "the subnet is not allowed on this host",
            )
        })
    }

    /// A `uid_base` belongs to one workload. It is refused when a ready
    /// sandbox faber made, or a volume marker, holds it for another. Call
    /// On success the base is reserved for the owner until the returned
    /// guard drops: hold it across the create, which runs outside the lock.
    async fn reserve_uid_base(
        &self,
        replica: &ReplicaRef,
        uid_base: u32,
    ) -> Result<UidReservation, Fail> {
        let owner = Owner::of(replica);
        let taken = || {
            Fail::new(
                ErrorCode::Forbidden,
                "the uid_base is already held by another workload",
            )
        };
        // The lock is held for the check and the reservation only. The
        // reservations are read before the holders: a request that reserved
        // earlier and has finished is then seen as a holder, and one still
        // running is seen as a reservation.
        let _lock = self.uid_lock.lock().await;
        if self
            .reservations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&uid_base)
            .is_some_and(|(held_by, _)| *held_by != owner)
        {
            return Err(taken());
        }
        let sandboxes = self
            .cri
            .list_pod_sandboxes(Some(PodSandboxFilter {
                label_selector: managed_selector(),
                ..Default::default()
            }))
            .await?;
        let base = uid_base.to_string();
        for sandbox in sandboxes {
            if sandbox.state != PodSandboxState::SandboxReady as i32
                || sandbox.annotations.get(ANNOTATION_UID_BASE) != Some(&base)
            {
                continue;
            }
            // A managed sandbox whose labels do not name a replica holds the
            // base for nobody we can match: refuse.
            let holder = ReplicaRef::from_labels(&sandbox.labels).map(|r| Owner::of(&r));
            if holder.as_ref() != Some(&owner) {
                return Err(taken());
            }
        }
        let holders = self.volumes.uid_holders().await?;
        if holders
            .iter()
            .any(|h| h.uid_base == uid_base && h.owner != owner)
        {
            return Err(taken());
        }
        let mut map = self.reservations.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(uid_base).or_insert_with(|| (owner, 0)).1 += 1;
        Ok(UidReservation {
            map: Arc::clone(&self.reservations),
            uid_base,
        })
    }

    /// Owns what it needs, so it can run in a task that outlives the request.
    async fn ensure_sandbox(
        cri: C,
        runtime_handler: String,
        replica: ReplicaRef,
        uid_base: u32,
        dns: faber_node_protocol::DnsConfig,
        ports: Vec<faber_api_types::workload::PortSpec>,
    ) -> Result<Response, Fail> {
        let replica = &replica;
        let existing = cri
            .list_pod_sandboxes(Some(PodSandboxFilter {
                label_selector: replica.labels().into_iter().collect(),
                ..Default::default()
            }))
            .await?;
        for sandbox in existing {
            if sandbox.state == PodSandboxState::SandboxReady as i32 {
                if sandbox.annotations.get(ANNOTATION_UID_BASE) != Some(&uid_base.to_string()) {
                    return Err(Fail::new(
                        ErrorCode::Forbidden,
                        "the replica's sandbox runs with a different uid_base",
                    ));
                }
                return Ok(Response::Sandbox {
                    sandbox_id: sandbox.id,
                });
            }
            // A sandbox that is no longer ready cannot run containers.
            cri.stop_pod_sandbox(&sandbox.id).await?;
            cri.remove_pod_sandbox(&sandbox.id).await?;
        }
        let config = cri_config::sandbox_config(replica, uid_base, &dns, &ports);
        let sandbox_id = cri.run_pod_sandbox(config, &runtime_handler).await?;
        Ok(Response::Sandbox { sandbox_id })
    }

    /// Only containers faber created may be acted on.
    async fn check_managed_container(&self, id: &str) -> Result<(), Fail> {
        trust::check_runtime_id("container_id", id)?;
        let status = self.cri.container_status(id).await?;
        if is_managed(&status.labels) {
            Ok(())
        } else {
            Err(Fail::new(
                ErrorCode::Forbidden,
                "the container is not managed by faber",
            ))
        }
    }

    async fn list_replicas(&self) -> Result<Response, Fail> {
        let sandboxes = self
            .cri
            .list_pod_sandboxes(Some(PodSandboxFilter {
                label_selector: managed_selector(),
                ..Default::default()
            }))
            .await?;
        let containers = self
            .cri
            .list_containers(Some(ContainerFilter {
                label_selector: managed_selector(),
                ..Default::default()
            }))
            .await?;
        let mut replicas = Vec::new();
        for sandbox in sandboxes {
            let Some(replica) = ReplicaRef::from_labels(&sandbox.labels) else {
                continue;
            };
            let pod_ip = match self.cri.pod_sandbox_status(&sandbox.id).await {
                Ok(status) => status.network.map(|n| n.ip).filter(|ip| !ip.is_empty()),
                Err(e) if e.is_not_found() => continue,
                Err(e) => return Err(e.into()),
            };
            let mut infos = Vec::new();
            for c in containers.iter().filter(|c| c.pod_sandbox_id == sandbox.id) {
                let state = match CriContainerState::try_from(c.state) {
                    Ok(CriContainerState::ContainerCreated) => ContainerState::Created,
                    Ok(CriContainerState::ContainerRunning) => ContainerState::Running,
                    Ok(CriContainerState::ContainerExited) => ContainerState::Exited,
                    _ => ContainerState::Unknown,
                };
                let exit_code = if state == ContainerState::Exited {
                    match self.cri.container_status(&c.id).await {
                        Ok(status) => Some(status.exit_code),
                        Err(e) if e.is_not_found() => continue,
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    None
                };
                let (name, attempt) = c
                    .metadata
                    .as_ref()
                    .map(|m| (m.name.clone(), m.attempt))
                    .unwrap_or_default();
                infos.push(ContainerInfo {
                    id: c.id.clone(),
                    name,
                    attempt,
                    state,
                    exit_code,
                    labels: c.labels.clone().into_iter().collect(),
                });
            }
            replicas.push(ReplicaInfo {
                replica,
                sandbox_id: sandbox.id.clone(),
                state: if sandbox.state == PodSandboxState::SandboxReady as i32 {
                    SandboxState::Ready
                } else {
                    SandboxState::NotReady
                },
                pod_ip,
                labels: sandbox.labels.into_iter().collect(),
                containers: infos,
            });
        }
        Ok(Response::Replicas { replicas })
    }
}
