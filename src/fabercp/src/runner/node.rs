//! The client of `faber-node`, the root helper that runs the containers.
//!
//! [`NodeClient`] is one request in, one response out. The real client,
//! [`SocketNodeClient`], opens the Unix socket for each request and speaks
//! the frames of `faber_node_protocol`, so no connection is shared and no
//! lock is held across an await. Tests use [`fake::FakeNode`].

use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use faber_node_protocol::{ErrorCode, ReplicaInfo, Request, Response, VolumeInfo};

/// Where `faber-node` listens unless told otherwise: the default of its
/// `FABER_NODE_SOCKET`.
pub const DEFAULT_NODE_SOCKET: &str = "/run/faber/node.sock";

/// How long one request may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);
/// A pull downloads an image.
const PULL_TIMEOUT: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, thiserror::Error)]
pub enum NodeCallError {
    /// The socket, the frames or the handshake failed.
    #[error("faber-node: {0}")]
    Transport(String),
    /// `faber-node` refused the request or the runtime failed.
    #[error("faber-node refused the request ({code:?}): {message}")]
    Node { code: ErrorCode, message: String },
    /// The env of a container could not be had.
    #[error("the env of a container cannot be had: {0}")]
    Env(String),
    /// A response that does not belong to the request.
    #[error("faber-node answered a {request} request with {response}")]
    Unexpected {
        request: &'static str,
        response: &'static str,
    },
}

impl NodeCallError {
    /// The runtime had nothing by that id.
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::Node {
                code: ErrorCode::NotFound,
                ..
            }
        )
    }
}

/// One exchange with `faber-node`. A [`Response::Error`] is returned as
/// [`NodeCallError::Node`], so a returned response is never an error.
pub trait NodeClient: Send + Sync + 'static {
    fn call(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Response, NodeCallError>> + Send;
}

fn name_of(response: &Response) -> &'static str {
    match response {
        Response::Version { .. } => "version",
        Response::Image { .. } => "image",
        Response::Sandbox { .. } => "sandbox",
        Response::Container { .. } => "container",
        Response::Done => "done",
        Response::Changed { .. } => "changed",
        Response::Replicas { .. } => "replicas",
        Response::Volumes { .. } => "volumes",
        Response::VolumeSpace { .. } => "volume_space",
        Response::Error { .. } => "error",
    }
}

/// A request that is answered with [`Response::Done`].
pub async fn call_done<C: NodeClient>(
    client: &C,
    request: Request,
    name: &'static str,
) -> Result<(), NodeCallError> {
    match client.call(request).await? {
        Response::Done => Ok(()),
        other => Err(NodeCallError::Unexpected {
            request: name,
            response: name_of(&other),
        }),
    }
}

/// A request answered with the id of a sandbox.
pub async fn call_sandbox<C: NodeClient>(
    client: &C,
    request: Request,
) -> Result<String, NodeCallError> {
    match client.call(request).await? {
        Response::Sandbox { sandbox_id } => Ok(sandbox_id),
        other => Err(NodeCallError::Unexpected {
            request: "ensure_sandbox",
            response: name_of(&other),
        }),
    }
}

/// A request answered with the id of a container.
pub async fn call_container<C: NodeClient>(
    client: &C,
    request: Request,
) -> Result<String, NodeCallError> {
    match client.call(request).await? {
        Response::Container { container_id } => Ok(container_id),
        other => Err(NodeCallError::Unexpected {
            request: "create_container",
            response: name_of(&other),
        }),
    }
}

/// A request answered with whether a file on the node changed.
pub async fn call_changed<C: NodeClient>(
    client: &C,
    request: Request,
    name: &'static str,
) -> Result<bool, NodeCallError> {
    match client.call(request).await? {
        Response::Changed { changed } => Ok(changed),
        other => Err(NodeCallError::Unexpected {
            request: name,
            response: name_of(&other),
        }),
    }
}

/// `PullImage`: the image is on the node when this returns.
pub async fn pull_image<C: NodeClient>(client: &C, image: &str) -> Result<(), NodeCallError> {
    let request = Request::PullImage {
        image: image.to_owned(),
    };
    match client.call(request).await? {
        Response::Image { .. } => Ok(()),
        other => Err(NodeCallError::Unexpected {
            request: "pull_image",
            response: name_of(&other),
        }),
    }
}

/// `ListVolumes`: every volume on the node.
pub async fn list_volumes<C: NodeClient>(client: &C) -> Result<Vec<VolumeInfo>, NodeCallError> {
    match client.call(Request::ListVolumes).await? {
        Response::Volumes { volumes } => Ok(volumes),
        other => Err(NodeCallError::Unexpected {
            request: "list_volumes",
            response: name_of(&other),
        }),
    }
}

/// `ListReplicas`: every replica faber manages on this node.
pub async fn list_replicas<C: NodeClient>(client: &C) -> Result<Vec<ReplicaInfo>, NodeCallError> {
    match client.call(Request::ListReplicas).await? {
        Response::Replicas { replicas } => Ok(replicas),
        other => Err(NodeCallError::Unexpected {
            request: "list_replicas",
            response: name_of(&other),
        }),
    }
}

/// The real client: the Unix socket of `faber-node`.
#[derive(Debug, Clone)]
#[cfg_attr(
    not(unix),
    allow(
        dead_code,
        reason = "only the Unix client reads the path and the owner"
    )
)]
pub struct SocketNodeClient {
    path: PathBuf,
    /// The user that must own the process at the other end of the socket.
    owner_uid: u32,
}

impl SocketNodeClient {
    /// A client of the socket at `path`, which only root may answer on.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            owner_uid: 0,
        }
    }

    /// Accepts an answerer owned by `uid` instead of root. For tests, where
    /// the server is not root.
    #[cfg(test)]
    pub fn expecting_owner(mut self, uid: u32) -> Self {
        self.owner_uid = uid;
        self
    }

    /// How long `request` may take. A stop waits out its grace period before
    /// the container is killed, so the call gets that on top.
    pub fn timeout_for(request: &Request) -> Duration {
        match request {
            Request::PullImage { .. } => PULL_TIMEOUT,
            Request::StopContainer { grace_secs, .. } => {
                CALL_TIMEOUT + Duration::from_secs(u64::from(*grace_secs))
            }
            _ => CALL_TIMEOUT,
        }
    }
}

#[cfg(unix)]
impl NodeClient for SocketNodeClient {
    async fn call(&self, request: Request) -> Result<Response, NodeCallError> {
        use faber_node_protocol::{MAX_FRAME_BYTES, PROTOCOL_VERSION, read_frame, write_frame};

        let transport = |what: &str, e: &dyn std::fmt::Display| {
            NodeCallError::Transport(format!("{what}: {e}"))
        };
        let exchange = async {
            let mut stream = tokio::net::UnixStream::connect(&self.path)
                .await
                .map_err(|e| {
                    transport(&format!("cannot connect to {}", self.path.display()), &e)
                })?;
            // The request may carry secrets: send it only to root's helper,
            // not to whatever listens on a path someone else could write.
            let peer = stream
                .peer_cred()
                .map_err(|e| transport("reading the socket's owner", &e))?;
            if peer.uid() != self.owner_uid {
                return Err(NodeCallError::Transport(format!(
                    "the process on {} is owned by uid {}, not {}; nothing was sent",
                    self.path.display(),
                    peer.uid(),
                    self.owner_uid
                )));
            }
            // The server wants the handshake before anything else.
            write_frame(
                &mut stream,
                &Request::Version {
                    client_version: PROTOCOL_VERSION,
                },
            )
            .await
            .map_err(|e| transport("sending the handshake", &e))?;
            match read_frame::<_, Response>(&mut stream, MAX_FRAME_BYTES)
                .await
                .map_err(|e| transport("reading the handshake", &e))?
            {
                Some(Response::Version { .. }) => {}
                Some(Response::Error { code, message }) => {
                    return Err(NodeCallError::Node { code, message });
                }
                _ => return Err(NodeCallError::Transport("no handshake answer".into())),
            }
            write_frame(&mut stream, &request)
                .await
                .map_err(|e| transport("sending the request", &e))?;
            read_frame::<_, Response>(&mut stream, MAX_FRAME_BYTES)
                .await
                .map_err(|e| transport("reading the answer", &e))?
                .ok_or_else(|| NodeCallError::Transport("the connection closed".into()))
        };
        let response = tokio::time::timeout(Self::timeout_for(&request), exchange)
            .await
            .map_err(|_| NodeCallError::Transport("no answer in time".into()))??;
        match response {
            Response::Error { code, message } => Err(NodeCallError::Node { code, message }),
            other => Ok(other),
        }
    }
}

#[cfg(not(unix))]
impl NodeClient for SocketNodeClient {
    async fn call(&self, _request: Request) -> Result<Response, NodeCallError> {
        Err(NodeCallError::Transport(
            "faber-node needs a Unix socket, which this platform lacks".into(),
        ))
    }
}

// Public, but hidden, so that integration tests under `tests/` can use it,
// as `faber_network_management::fake` is.
#[doc(hidden)]
pub mod fake {
    //! An in-memory `faber-node`: it keeps sandboxes, containers, volumes and
    //! the runtime config, and logs every request as a line.

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

    use faber_node_protocol::{
        ContainerInfo, ContainerState, LABEL_CONTAINER, PodNetwork, ReplicaInfo, ReplicaRef,
        SandboxState, VolumeKind,
    };

    use super::*;

    #[derive(Debug, Clone)]
    struct Container {
        id: String,
        sandbox_id: String,
        name: String,
        state: ContainerState,
        exit_code: Option<i32>,
        env: BTreeMap<String, String>,
    }

    /// What the node's runtime config holds: the CNI network, if there is a
    /// CNI config. The drop-in has nothing a request can change.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RuntimeFiles {
        pub pod_network: Option<PodNetwork>,
    }

    #[derive(Debug, Default)]
    struct Inner {
        runtime: Option<RuntimeFiles>,
        next_id: u32,
        sandboxes: Vec<(String, ReplicaRef, SandboxState, String)>,
        containers: Vec<Container>,
        volumes: Vec<(ReplicaRef, String, VolumeKind)>,
        log: Vec<String>,
        /// Requests of this kind (the first word of the log line) fail once.
        fail: Vec<&'static str>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct FakeNode(Arc<Mutex<Inner>>);

    fn key(r: &ReplicaRef) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            r.project, r.env, r.workload, r.revision, r.replica_index
        )
    }

    impl FakeNode {
        fn inner(&self) -> MutexGuard<'_, Inner> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// Every request so far, one line each.
        pub fn log(&self) -> Vec<String> {
            self.inner().log.clone()
        }

        pub fn take_log(&self) -> Vec<String> {
            std::mem::take(&mut self.inner().log)
        }

        /// The next request of this kind fails with a runtime error.
        pub fn fail_next(&self, kind: &'static str) {
            self.inner().fail.push(kind);
        }

        /// The container `name` of any replica exits with `code`.
        pub fn exit(&self, replica: &ReplicaRef, name: &str, code: i32) {
            let mut inner = self.inner();
            let sandbox = inner
                .sandboxes
                .iter()
                .find(|s| &s.1 == replica)
                .map(|s| s.0.clone());
            for c in inner.containers.iter_mut() {
                if Some(&c.sandbox_id) == sandbox.as_ref() && c.name == name {
                    c.state = ContainerState::Exited;
                    c.exit_code = Some(code);
                }
            }
        }

        /// Removes a container behind faber's back.
        pub fn vanish(&self, replica: &ReplicaRef) {
            let mut inner = self.inner();
            let ids: Vec<_> = inner
                .sandboxes
                .iter()
                .filter(|s| &s.1 == replica)
                .map(|s| s.0.clone())
                .collect();
            inner.containers.retain(|c| !ids.contains(&c.sandbox_id));
            inner.sandboxes.retain(|s| &s.1 != replica);
        }

        /// A volume appears on the node without any request: it was left by
        /// an earlier run, or by a pod that is gone.
        pub fn leave_volume(&self, replica: &ReplicaRef, name: &str, kind: VolumeKind) {
            self.inner()
                .volumes
                .push((replica.clone(), name.to_owned(), kind));
        }

        pub fn volumes(&self) -> Vec<(String, String, VolumeKind)> {
            self.inner()
                .volumes
                .iter()
                .map(|(r, n, k)| (key(r), n.clone(), *k))
                .collect()
        }

        /// The runtime config files as the node holds them, once configured.
        pub fn runtime_files(&self) -> Option<RuntimeFiles> {
            self.inner().runtime.clone()
        }

        pub fn sandbox_count(&self) -> usize {
            self.inner().sandboxes.len()
        }

        /// The env of the running or created container `name` of `replica`.
        pub fn env_of(&self, replica: &ReplicaRef, name: &str) -> Option<BTreeMap<String, String>> {
            let inner = self.inner();
            let sandbox = inner.sandboxes.iter().find(|s| &s.1 == replica)?.0.clone();
            inner
                .containers
                .iter()
                .find(|c| c.sandbox_id == sandbox && c.name == name)
                .map(|c| c.env.clone())
        }

        pub fn container_states(&self, replica: &ReplicaRef) -> Vec<(String, ContainerState)> {
            let inner = self.inner();
            let Some(sandbox) = inner.sandboxes.iter().find(|s| &s.1 == replica) else {
                return Vec::new();
            };
            inner
                .containers
                .iter()
                .filter(|c| c.sandbox_id == sandbox.0)
                .map(|c| (c.name.clone(), c.state))
                .collect()
        }
    }

    impl NodeClient for FakeNode {
        async fn call(&self, request: Request) -> Result<Response, NodeCallError> {
            let mut inner = self.inner();
            let (line, kind): (String, &'static str) = match &request {
                Request::Version { .. } => ("version".into(), "version"),
                Request::PullImage { image } => (format!("pull_image {image}"), "pull_image"),
                Request::EnsureSandbox { replica, .. } => {
                    (format!("ensure_sandbox {}", key(replica)), "ensure_sandbox")
                }
                Request::CreateContainer {
                    replica, container, ..
                } => (
                    format!("create_container {} {}", key(replica), container.name),
                    "create_container",
                ),
                Request::StartContainer { container_id } => {
                    (format!("start_container {container_id}"), "start_container")
                }
                Request::StopContainer { container_id, .. } => {
                    (format!("stop_container {container_id}"), "stop_container")
                }
                Request::RemoveContainer { container_id } => (
                    format!("remove_container {container_id}"),
                    "remove_container",
                ),
                Request::RemoveSandbox { sandbox_id } => {
                    (format!("remove_sandbox {sandbox_id}"), "remove_sandbox")
                }
                Request::EnsureVolume {
                    replica,
                    volume_name,
                    kind,
                    ..
                } => (
                    format!("ensure_volume {} {volume_name} {kind:?}", key(replica)),
                    "ensure_volume",
                ),
                Request::RemoveVolume {
                    replica,
                    volume_name,
                } => (
                    format!("remove_volume {} {volume_name}", key(replica)),
                    "remove_volume",
                ),
                Request::ListReplicas => ("list_replicas".into(), "list_replicas"),
                Request::ListVolumes => ("list_volumes".into(), "list_volumes"),
                Request::VolumeSpace => ("volume_space".into(), "volume_space"),
                Request::ConfigureRuntime { pod_network } => (
                    format!(
                        "configure_runtime network={}",
                        pod_network.as_ref().map_or_else(
                            || "none".to_owned(),
                            |n| format!("{} {}", n.subnet, n.mtu)
                        )
                    ),
                    "configure_runtime",
                ),
                Request::RemoveCniConfig => ("remove_cni_config".into(), "remove_cni_config"),
            };
            if kind != "list_replicas" && kind != "list_volumes" {
                inner.log.push(line);
            }
            if let Some(at) = inner.fail.iter().position(|k| *k == kind) {
                inner.fail.remove(at);
                return Err(NodeCallError::Node {
                    code: ErrorCode::Runtime,
                    message: format!("{kind} failed on purpose"),
                });
            }
            match request {
                Request::Version { .. } => Ok(Response::Version {
                    protocol_version: 1,
                    node_version: "fake".into(),
                }),
                Request::PullImage { image } => Ok(Response::Image { image_id: image }),
                Request::EnsureSandbox { replica, .. } => {
                    if let Some(s) = inner
                        .sandboxes
                        .iter()
                        .find(|s| s.1 == replica && s.2 == SandboxState::Ready)
                    {
                        return Ok(Response::Sandbox {
                            sandbox_id: s.0.clone(),
                        });
                    }
                    inner.next_id += 1;
                    let n = inner.next_id;
                    let id = format!("sb-{n}");
                    // A sandbox that is not ready is replaced, with its containers.
                    let old: Vec<_> = inner
                        .sandboxes
                        .iter()
                        .filter(|s| s.1 == replica)
                        .map(|s| s.0.clone())
                        .collect();
                    inner.containers.retain(|c| !old.contains(&c.sandbox_id));
                    inner.sandboxes.retain(|s| s.1 != replica);
                    inner.sandboxes.push((
                        id.clone(),
                        replica,
                        SandboxState::Ready,
                        format!("10.1.0.{n}"),
                    ));
                    Ok(Response::Sandbox { sandbox_id: id })
                }
                Request::CreateContainer {
                    sandbox_id,
                    container,
                    env,
                    ..
                } => {
                    if let Some(c) = inner.containers.iter().find(|c| {
                        c.sandbox_id == sandbox_id
                            && c.name == container.name
                            && c.state == ContainerState::Created
                    }) {
                        return Ok(Response::Container {
                            container_id: c.id.clone(),
                        });
                    }
                    inner.next_id += 1;
                    let id = format!("c-{}", inner.next_id);
                    inner.containers.push(Container {
                        id: id.clone(),
                        sandbox_id,
                        name: container.name,
                        state: ContainerState::Created,
                        exit_code: None,
                        env: env.0.clone(),
                    });
                    Ok(Response::Container { container_id: id })
                }
                Request::StartContainer { container_id } => {
                    match inner.containers.iter_mut().find(|c| c.id == container_id) {
                        Some(c) => {
                            c.state = ContainerState::Running;
                            Ok(Response::Done)
                        }
                        None => Err(not_found()),
                    }
                }
                Request::StopContainer { container_id, .. } => {
                    match inner.containers.iter_mut().find(|c| c.id == container_id) {
                        Some(c) => {
                            c.state = ContainerState::Exited;
                            c.exit_code = Some(137);
                            Ok(Response::Done)
                        }
                        None => Err(not_found()),
                    }
                }
                Request::RemoveContainer { container_id } => {
                    inner.containers.retain(|c| c.id != container_id);
                    Ok(Response::Done)
                }
                Request::RemoveSandbox { sandbox_id } => {
                    inner.containers.retain(|c| c.sandbox_id != sandbox_id);
                    inner.sandboxes.retain(|s| s.0 != sandbox_id);
                    Ok(Response::Done)
                }
                Request::EnsureVolume {
                    replica,
                    volume_name,
                    kind,
                    ..
                } => {
                    if !inner
                        .volumes
                        .iter()
                        .any(|v| same_slot(&v.0, &replica) && v.1 == volume_name)
                    {
                        // Volumes are not scoped by revision, as in faber-node.
                        inner.volumes.push((replica, volume_name, kind));
                    }
                    Ok(Response::Done)
                }
                Request::RemoveVolume {
                    replica,
                    volume_name,
                } => {
                    inner.volumes.retain(|v| {
                        !(v.0.project == replica.project
                            && v.0.env == replica.env
                            && v.0.workload == replica.workload
                            && v.0.replica_index == replica.replica_index
                            && v.1 == volume_name)
                    });
                    Ok(Response::Done)
                }
                Request::ListVolumes => {
                    let mut volumes: Vec<_> = inner
                        .volumes
                        .iter()
                        .map(|(r, name, kind)| VolumeInfo {
                            project: r.project.clone(),
                            env: r.env.clone(),
                            workload: r.workload.clone(),
                            replica_index: r.replica_index,
                            volume: name.clone(),
                            kind: *kind,
                        })
                        .collect();
                    volumes.sort_by(|a, b| {
                        (&a.project, &a.env, &a.workload, a.replica_index, &a.volume).cmp(&(
                            &b.project,
                            &b.env,
                            &b.workload,
                            b.replica_index,
                            &b.volume,
                        ))
                    });
                    Ok(Response::Volumes { volumes })
                }
                Request::VolumeSpace => Ok(Response::VolumeSpace {
                    free_bytes: 1 << 40,
                }),
                Request::ConfigureRuntime { pod_network } => {
                    let files = RuntimeFiles { pod_network };
                    let changed = inner.runtime.as_ref() != Some(&files);
                    inner.runtime = Some(files);
                    Ok(Response::Changed { changed })
                }
                Request::RemoveCniConfig => {
                    let changed = inner
                        .runtime
                        .as_mut()
                        .is_some_and(|files| files.pod_network.take().is_some());
                    Ok(Response::Changed { changed })
                }
                Request::ListReplicas => {
                    let replicas = inner
                        .sandboxes
                        .iter()
                        .map(|(id, replica, state, ip)| ReplicaInfo {
                            replica: replica.clone(),
                            sandbox_id: id.clone(),
                            state: *state,
                            pod_ip: Some(ip.clone()),
                            labels: replica.labels(),
                            containers: inner
                                .containers
                                .iter()
                                .filter(|c| &c.sandbox_id == id)
                                .map(|c| ContainerInfo {
                                    id: c.id.clone(),
                                    name: format!("{}-{}", replica.workload, c.name),
                                    attempt: 0,
                                    state: c.state,
                                    exit_code: c.exit_code,
                                    labels: BTreeMap::from([(
                                        LABEL_CONTAINER.to_owned(),
                                        c.name.clone(),
                                    )]),
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(Response::Replicas { replicas })
                }
            }
        }
    }

    fn same_slot(a: &ReplicaRef, b: &ReplicaRef) -> bool {
        (&a.project, &a.env, &a.workload, a.replica_index)
            == (&b.project, &b.env, &b.workload, b.replica_index)
    }

    fn not_found() -> NodeCallError {
        NodeCallError::Node {
            code: ErrorCode::NotFound,
            message: "no such container".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stop_gets_its_grace_period_on_top_of_the_call_timeout() {
        let stop = |grace_secs| Request::StopContainer {
            container_id: "c".into(),
            grace_secs,
        };
        assert_eq!(SocketNodeClient::timeout_for(&stop(0)), CALL_TIMEOUT);
        assert_eq!(
            SocketNodeClient::timeout_for(&stop(3600)),
            CALL_TIMEOUT + Duration::from_secs(3600)
        );
        assert_eq!(
            SocketNodeClient::timeout_for(&Request::PullImage { image: "i".into() }),
            PULL_TIMEOUT
        );
        assert_eq!(
            SocketNodeClient::timeout_for(&Request::ListReplicas),
            CALL_TIMEOUT
        );
    }

    #[cfg(unix)]
    mod socket {
        use faber_node_protocol::{PROTOCOL_VERSION, read_frame, write_frame};
        use tokio::net::UnixListener;

        use super::*;

        /// A server that answers the handshake and then every request with
        /// `Done`. Counts the requests it read.
        fn serve(listener: UnixListener) -> std::sync::Arc<std::sync::atomic::AtomicUsize> {
            let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter = seen.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let counter = counter.clone();
                    tokio::spawn(async move {
                        while let Ok(Some(request)) = read_frame::<_, Request>(
                            &mut stream,
                            faber_node_protocol::MAX_FRAME_BYTES,
                        )
                        .await
                        {
                            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let reply = match request {
                                Request::Version { .. } => Response::Version {
                                    protocol_version: PROTOCOL_VERSION,
                                    node_version: "test".into(),
                                },
                                _ => Response::Done,
                            };
                            if write_frame(&mut stream, &reply).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });
            seen
        }

        fn my_uid() -> u32 {
            nix::unistd::getuid().as_raw()
        }

        #[tokio::test]
        async fn the_client_speaks_the_handshake_then_the_request() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("node.sock");
            let seen = serve(UnixListener::bind(&path).unwrap());
            let client = SocketNodeClient::new(&path).expecting_owner(my_uid());
            let reply = client.call(Request::ListReplicas).await.unwrap();
            assert_eq!(reply, Response::Done);
            // The handshake and the request.
            assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn nothing_is_sent_to_a_process_that_root_does_not_own() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("node.sock");
            let seen = serve(UnixListener::bind(&path).unwrap());
            // The test's server runs as this user. A client that wants root
            // (the default) must refuse it, unless the tests run as root.
            let wanted = if my_uid() == 0 { 12345 } else { 0 };
            let mut client = SocketNodeClient::new(&path);
            client.owner_uid = wanted;
            let err = client.call(Request::ListReplicas).await.unwrap_err();
            assert!(
                matches!(&err, NodeCallError::Transport(m) if m.contains("nothing was sent")),
                "{err}"
            );
            tokio::task::yield_now().await;
            assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn a_missing_socket_is_a_transport_error() {
            let dir = tempfile::tempdir().unwrap();
            let client = SocketNodeClient::new(dir.path().join("absent.sock"));
            assert!(matches!(
                client.call(Request::ListReplicas).await,
                Err(NodeCallError::Transport(_))
            ));
        }
    }
}
