//! The request handler: every trust check, then the CRI and volume calls.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use faber_api_types::workload::limits;
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
use crate::cri_config::{self, ANNOTATION_UID_BASE, ResolvedMount};
use crate::trust;
use crate::volume::{EnsureVolume, VolumeError, VolumeManager, VolumeRef};

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

fn managed_selector() -> HashMap<String, String> {
    HashMap::from([(LABEL_MANAGED.to_owned(), "true".to_owned())])
}

fn is_managed(labels: &HashMap<String, String>) -> bool {
    labels.get(LABEL_MANAGED).map(String::as_str) == Some("true")
}

pub struct Node<C> {
    uid_range: UidRange,
    runtime_handler: String,
    cri: C,
    images: ImagePuller<C>,
    volumes: Arc<dyn VolumeManager>,
}

impl<C: CriClient + Clone> Node<C> {
    pub fn new(
        uid_range: UidRange,
        runtime_handler: String,
        cri: C,
        volumes: Arc<dyn VolumeManager>,
    ) -> Self {
        Self {
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
                self.ensure_sandbox(&replica, uid_base, &dns, &ports).await
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
                if container.mounts.iter().any(|m| m.sub_path.is_some()) {
                    // A container can plant a symlink in its volume, and the
                    // runtime would follow it as root on the host.
                    return Err(Fail::new(
                        ErrorCode::Forbidden,
                        "mount sub_path is not supported by faber-node",
                    ));
                }
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
                let attempt = existing
                    .iter()
                    .filter_map(|c| c.metadata.as_ref())
                    .filter(|m| m.name == name)
                    .map(|m| m.attempt + 1)
                    .max()
                    .unwrap_or(0);
                let config = cri_config::container_config(
                    &replica, &container, &env, uid_base, attempt, &mounts,
                );
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
                self.volumes
                    .ensure(&EnsureVolume {
                        volume,
                        kind,
                        size_bytes,
                        uid_base,
                    })
                    .await?;
                Ok(Response::Done)
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
        }
    }

    async fn ensure_sandbox(
        &self,
        replica: &ReplicaRef,
        uid_base: u32,
        dns: &faber_node_protocol::DnsConfig,
        ports: &[faber_api_types::workload::PortSpec],
    ) -> Result<Response, Fail> {
        let existing = self
            .cri
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
            self.cri.stop_pod_sandbox(&sandbox.id).await?;
            self.cri.remove_pod_sandbox(&sandbox.id).await?;
        }
        let config = cri_config::sandbox_config(replica, uid_base, dns, ports);
        let sandbox_id = self
            .cri
            .run_pod_sandbox(config, &self.runtime_handler)
            .await?;
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
