//! In-memory [`CriClient`] for tests: tracks images, sandboxes and
//! containers, records calls and can inject errors.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    BoxFuture, Container, ContainerConfig, ContainerFilter, ContainerState, ContainerStatus,
    CriClient, CriError, Image, PodSandbox, PodSandboxConfig, PodSandboxFilter, PodSandboxState,
    PodSandboxStatus, RegistryCredentials, Result, VersionResponse,
};

/// One call made on a [`FakeCri`], in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Call {
    Version,
    ImageStatus(String),
    PullImage {
        image: String,
        auth: Option<RegistryCredentials>,
    },
    RunPodSandbox {
        runtime_handler: String,
    },
    StopPodSandbox(String),
    RemovePodSandbox(String),
    ListPodSandboxes,
    PodSandboxStatus(String),
    CreateContainer {
        sandbox_id: String,
    },
    StartContainer(String),
    StopContainer {
        id: String,
        timeout_secs: i64,
    },
    RemoveContainer(String),
    ListContainers,
    ContainerStatus(String),
}

impl Call {
    /// The [`CriClient`] method name, as used by [`FakeCri::fail_next`].
    pub fn method(&self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::ImageStatus(_) => "image_status",
            Self::PullImage { .. } => "pull_image",
            Self::RunPodSandbox { .. } => "run_pod_sandbox",
            Self::StopPodSandbox(_) => "stop_pod_sandbox",
            Self::RemovePodSandbox(_) => "remove_pod_sandbox",
            Self::ListPodSandboxes => "list_pod_sandboxes",
            Self::PodSandboxStatus(_) => "pod_sandbox_status",
            Self::CreateContainer { .. } => "create_container",
            Self::StartContainer(_) => "start_container",
            Self::StopContainer { .. } => "stop_container",
            Self::RemoveContainer(_) => "remove_container",
            Self::ListContainers => "list_containers",
            Self::ContainerStatus(_) => "container_status",
        }
    }
}

#[derive(Default)]
struct State {
    next_id: u64,
    /// Fake clock in nanoseconds; advances by one per timestamp taken.
    clock: i64,
    calls: Vec<Call>,
    failures: HashMap<&'static str, VecDeque<CriError>>,
    images: BTreeMap<String, Image>,
    sandboxes: BTreeMap<String, PodSandboxStatus>,
    containers: BTreeMap<String, (Container, ContainerStatus)>,
}

impl State {
    fn tick(&mut self) -> i64 {
        self.clock += 1;
        self.clock
    }

    fn new_id(&mut self, prefix: &str) -> String {
        self.next_id += 1;
        format!("{prefix}-{}", self.next_id)
    }

    /// Records the call and returns an injected error for it, if any.
    fn begin(&mut self, call: Call) -> Result<()> {
        let method = call.method();
        self.calls.push(call);
        match self.failures.get_mut(method).and_then(VecDeque::pop_front) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// Cheap to clone; clones share state, so a test can keep a handle while the
/// code under test owns another.
#[derive(Clone, Default)]
pub struct FakeCri {
    state: Arc<Mutex<State>>,
}

impl FakeCri {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A poisoned lock only means another test thread panicked.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Every call so far, oldest first.
    pub fn calls(&self) -> Vec<Call> {
        self.lock().calls.clone()
    }

    /// Makes the next call to `method` (a [`Call::method`] name) fail with
    /// `err`. Several injections for one method are consumed in order.
    pub fn fail_next(&self, method: &'static str, err: CriError) {
        self.lock()
            .failures
            .entry(method)
            .or_default()
            .push_back(err);
    }

    /// Adds an image as if it had been pulled earlier, and returns its id.
    pub fn add_image(&self, reference: &str) -> String {
        let mut s = self.lock();
        add_image(&mut s, reference)
    }

    /// Simulates a container exiting on its own with `exit_code`.
    pub fn exit_container(&self, id: &str, exit_code: i32) {
        let mut s = self.lock();
        let now = s.tick();
        if let Some((container, status)) = s.containers.get_mut(id) {
            finish(container, status, exit_code, now);
        }
    }
}

/// Exit code of a container killed after its stop grace period.
const KILLED_EXIT_CODE: i32 = 137;

fn finish(container: &mut Container, status: &mut ContainerStatus, exit_code: i32, now: i64) {
    container.state = ContainerState::ContainerExited as i32;
    status.state = ContainerState::ContainerExited as i32;
    status.exit_code = exit_code;
    status.finished_at = now;
}

fn add_image(s: &mut State, reference: &str) -> String {
    if let Some(image) = s.images.get(reference) {
        return image.id.clone();
    }
    let id = format!("sha256:{:064x}", s.images.len() + 1);
    s.images.insert(
        reference.to_owned(),
        Image {
            id: id.clone(),
            repo_tags: vec![reference.to_owned()],
            ..Default::default()
        },
    );
    id
}

fn not_found(kind: &str, id: &str) -> CriError {
    CriError::NotFound(format!("{kind} {id}"))
}

fn labels_match(labels: &HashMap<String, String>, selector: &HashMap<String, String>) -> bool {
    selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

impl CriClient for FakeCri {
    fn version(&self) -> BoxFuture<'_, Result<VersionResponse>> {
        Box::pin(async move {
            self.lock().begin(Call::Version)?;
            Ok(VersionResponse {
                version: "0.1.0".to_owned(),
                runtime_name: "fake".to_owned(),
                runtime_version: "0.0.0".to_owned(),
                runtime_api_version: "v1".to_owned(),
            })
        })
    }

    fn image_status<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<Option<Image>>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::ImageStatus(image.to_owned()))?;
            Ok(s.images
                .get(image)
                .or_else(|| s.images.values().find(|i| i.id == image))
                .cloned())
        })
    }

    fn pull_image<'a>(
        &'a self,
        image: &'a str,
        auth: Option<RegistryCredentials>,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            self.lock().begin(Call::PullImage {
                image: image.to_owned(),
                auth,
            })?;
            // A real pull takes time; yielding lets concurrent callers interleave.
            tokio::task::yield_now().await;
            Ok(add_image(&mut self.lock(), image))
        })
    }

    fn run_pod_sandbox<'a>(
        &'a self,
        config: PodSandboxConfig,
        runtime_handler: &'a str,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::RunPodSandbox {
                runtime_handler: runtime_handler.to_owned(),
            })?;
            let id = s.new_id("sandbox");
            let created_at = s.tick();
            s.sandboxes.insert(
                id.clone(),
                PodSandboxStatus {
                    id: id.clone(),
                    metadata: config.metadata,
                    state: PodSandboxState::SandboxReady as i32,
                    created_at,
                    labels: config.labels,
                    annotations: config.annotations,
                    runtime_handler: runtime_handler.to_owned(),
                    ..Default::default()
                },
            );
            Ok(id)
        })
    }

    fn stop_pod_sandbox<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::StopPodSandbox(id.to_owned()))?;
            let sandbox = s
                .sandboxes
                .get_mut(id)
                .ok_or_else(|| not_found("sandbox", id))?;
            sandbox.state = PodSandboxState::SandboxNotready as i32;
            let now = s.tick();
            for (container, status) in s.containers.values_mut() {
                if container.pod_sandbox_id == id
                    && container.state == ContainerState::ContainerRunning as i32
                {
                    finish(container, status, KILLED_EXIT_CODE, now);
                }
            }
            Ok(())
        })
    }

    fn remove_pod_sandbox<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::RemovePodSandbox(id.to_owned()))?;
            s.sandboxes
                .remove(id)
                .ok_or_else(|| not_found("sandbox", id))?;
            s.containers.retain(|_, (c, _)| c.pod_sandbox_id != id);
            Ok(())
        })
    }

    fn list_pod_sandboxes(
        &self,
        filter: Option<PodSandboxFilter>,
    ) -> BoxFuture<'_, Result<Vec<PodSandbox>>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::ListPodSandboxes)?;
            let filter = filter.unwrap_or_default();
            Ok(s.sandboxes
                .values()
                .filter(|sb| filter.id.is_empty() || sb.id == filter.id)
                .filter(|sb| filter.state.as_ref().is_none_or(|v| v.state == sb.state))
                .filter(|sb| labels_match(&sb.labels, &filter.label_selector))
                .map(|sb| PodSandbox {
                    id: sb.id.clone(),
                    metadata: sb.metadata.clone(),
                    state: sb.state,
                    created_at: sb.created_at,
                    labels: sb.labels.clone(),
                    annotations: sb.annotations.clone(),
                    runtime_handler: sb.runtime_handler.clone(),
                })
                .collect())
        })
    }

    fn pod_sandbox_status<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<PodSandboxStatus>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::PodSandboxStatus(id.to_owned()))?;
            s.sandboxes
                .get(id)
                .cloned()
                .ok_or_else(|| not_found("sandbox", id))
        })
    }

    fn create_container<'a>(
        &'a self,
        sandbox_id: &'a str,
        config: ContainerConfig,
        _sandbox_config: PodSandboxConfig,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::CreateContainer {
                sandbox_id: sandbox_id.to_owned(),
            })?;
            let sandbox = s
                .sandboxes
                .get(sandbox_id)
                .ok_or_else(|| not_found("sandbox", sandbox_id))?;
            if sandbox.state != PodSandboxState::SandboxReady as i32 {
                return Err(CriError::FailedPrecondition(format!(
                    "sandbox {sandbox_id} is not ready"
                )));
            }
            let wanted = config.metadata.as_ref();
            let duplicate = s.containers.values().any(|(c, _)| {
                c.pod_sandbox_id == sandbox_id
                    && c.metadata.as_ref().map(|m| (&m.name, m.attempt))
                        == wanted.map(|m| (&m.name, m.attempt))
            });
            if duplicate {
                return Err(CriError::AlreadyExists(format!(
                    "container name already reserved in sandbox {sandbox_id}"
                )));
            }
            let image = config.image.clone().unwrap_or_default();
            let image_ref = match s
                .images
                .get(&image.image)
                .or_else(|| s.images.values().find(|i| i.id == image.image))
            {
                Some(found) => found.id.clone(),
                None => return Err(not_found("image", &image.image)),
            };
            let id = s.new_id("container");
            let created_at = s.tick();
            let state = ContainerState::ContainerCreated as i32;
            let container = Container {
                id: id.clone(),
                pod_sandbox_id: sandbox_id.to_owned(),
                metadata: config.metadata.clone(),
                image: Some(image.clone()),
                image_ref: image_ref.clone(),
                image_id: image_ref.clone(),
                state,
                created_at,
                labels: config.labels.clone(),
                annotations: config.annotations.clone(),
            };
            let status = ContainerStatus {
                id: id.clone(),
                metadata: config.metadata,
                state,
                created_at,
                image: Some(image),
                image_ref,
                labels: config.labels,
                annotations: config.annotations,
                ..Default::default()
            };
            s.containers.insert(id.clone(), (container, status));
            Ok(id)
        })
    }

    fn start_container<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::StartContainer(id.to_owned()))?;
            let now = s.tick();
            let (container, status) = s
                .containers
                .get_mut(id)
                .ok_or_else(|| not_found("container", id))?;
            if container.state != ContainerState::ContainerCreated as i32 {
                return Err(CriError::FailedPrecondition(format!(
                    "container {id} is not in the created state"
                )));
            }
            container.state = ContainerState::ContainerRunning as i32;
            status.state = ContainerState::ContainerRunning as i32;
            status.started_at = now;
            Ok(())
        })
    }

    fn stop_container<'a>(&'a self, id: &'a str, timeout_secs: i64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::StopContainer {
                id: id.to_owned(),
                timeout_secs,
            })?;
            let now = s.tick();
            let (container, status) = s
                .containers
                .get_mut(id)
                .ok_or_else(|| not_found("container", id))?;
            // Stopping an already stopped container succeeds, as in containerd.
            if container.state == ContainerState::ContainerRunning as i32 {
                finish(container, status, KILLED_EXIT_CODE, now);
            }
            Ok(())
        })
    }

    fn remove_container<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::RemoveContainer(id.to_owned()))?;
            s.containers
                .remove(id)
                .ok_or_else(|| not_found("container", id))?;
            Ok(())
        })
    }

    fn list_containers(
        &self,
        filter: Option<ContainerFilter>,
    ) -> BoxFuture<'_, Result<Vec<Container>>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::ListContainers)?;
            let filter = filter.unwrap_or_default();
            Ok(s.containers
                .values()
                .map(|(c, _)| c)
                .filter(|c| filter.id.is_empty() || c.id == filter.id)
                .filter(|c| {
                    filter.pod_sandbox_id.is_empty() || c.pod_sandbox_id == filter.pod_sandbox_id
                })
                .filter(|c| filter.state.as_ref().is_none_or(|v| v.state == c.state))
                .filter(|c| labels_match(&c.labels, &filter.label_selector))
                .cloned()
                .collect())
        })
    }

    fn container_status<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<ContainerStatus>> {
        Box::pin(async move {
            let mut s = self.lock();
            s.begin(Call::ContainerStatus(id.to_owned()))?;
            s.containers
                .get(id)
                .map(|(_, status)| status.clone())
                .ok_or_else(|| not_found("container", id))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::{ContainerMetadata, ImageSpec, PodSandboxMetadata, PodSandboxStateValue};

    fn sandbox_config(name: &str) -> PodSandboxConfig {
        PodSandboxConfig {
            metadata: Some(PodSandboxMetadata {
                name: name.to_owned(),
                ..Default::default()
            }),
            labels: HashMap::from([("app".to_owned(), name.to_owned())]),
            ..Default::default()
        }
    }

    fn container_config(image: &str) -> ContainerConfig {
        ContainerConfig {
            metadata: Some(ContainerMetadata {
                name: "main".to_owned(),
                attempt: 0,
            }),
            image: Some(ImageSpec {
                image: image.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn image_status_is_none_until_pulled() {
        let cri = FakeCri::new();
        assert!(cri.image_status("nginx:1").await.unwrap().is_none());
        let id = cri.pull_image("nginx:1", None).await.unwrap();
        let image = cri.image_status("nginx:1").await.unwrap().unwrap();
        assert_eq!(image.id, id);
        // Lookup by id works too.
        assert!(cri.image_status(&id).await.unwrap().is_some());
        assert_eq!(cri.calls()[0], Call::ImageStatus("nginx:1".to_owned()));
    }

    #[tokio::test]
    async fn call_debug_redacts_credentials() {
        let cri = FakeCri::new();
        let auth = RegistryCredentials {
            username: "user-secret".to_owned(),
            password: "pw-secret".to_owned(),
            ..Default::default()
        };
        cri.pull_image("a/b", Some(auth)).await.unwrap();
        assert!(!format!("{:?}", cri.calls()).contains("secret"));
    }

    #[tokio::test]
    async fn injected_errors_are_one_shot_and_ordered() {
        let cri = FakeCri::new();
        cri.fail_next("version", CriError::Unavailable("down".to_owned()));
        cri.fail_next("version", CriError::NotFound("gone".to_owned()));
        assert!(matches!(cri.version().await, Err(CriError::Unavailable(_))));
        assert!(matches!(cri.version().await, Err(CriError::NotFound(_))));
        assert_eq!(cri.version().await.unwrap().runtime_name, "fake");
        assert_eq!(cri.calls().len(), 3);
    }

    #[tokio::test]
    async fn sandbox_and_container_lifecycle() {
        let cri = FakeCri::new();
        cri.add_image("busybox");
        let sb = cri
            .run_pod_sandbox(sandbox_config("web"), "runc")
            .await
            .unwrap();
        assert_eq!(
            cri.pod_sandbox_status(&sb).await.unwrap().state,
            PodSandboxState::SandboxReady as i32
        );

        let c = cri
            .create_container(&sb, container_config("busybox"), sandbox_config("web"))
            .await
            .unwrap();
        assert_eq!(
            cri.container_status(&c).await.unwrap().state,
            ContainerState::ContainerCreated as i32
        );
        cri.start_container(&c).await.unwrap();
        assert!(matches!(
            cri.start_container(&c).await,
            Err(CriError::FailedPrecondition(_))
        ));
        let running = ContainerFilter {
            state: Some(crate::v1::ContainerStateValue {
                state: ContainerState::ContainerRunning as i32,
            }),
            ..Default::default()
        };
        assert_eq!(cri.list_containers(Some(running)).await.unwrap().len(), 1);

        cri.stop_container(&c, 10).await.unwrap();
        cri.stop_container(&c, 10).await.unwrap();
        assert_eq!(
            cri.container_status(&c).await.unwrap().state,
            ContainerState::ContainerExited as i32
        );
        cri.remove_container(&c).await.unwrap();
        assert!(cri.container_status(&c).await.unwrap_err().is_not_found());

        cri.stop_pod_sandbox(&sb).await.unwrap();
        let ready = PodSandboxFilter {
            state: Some(PodSandboxStateValue {
                state: PodSandboxState::SandboxReady as i32,
            }),
            ..Default::default()
        };
        assert!(
            cri.list_pod_sandboxes(Some(ready))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(cri.list_pod_sandboxes(None).await.unwrap().len(), 1);
        cri.remove_pod_sandbox(&sb).await.unwrap();
        assert!(
            cri.pod_sandbox_status(&sb)
                .await
                .unwrap_err()
                .is_not_found()
        );
    }

    #[tokio::test]
    async fn create_container_needs_sandbox_and_image() {
        let cri = FakeCri::new();
        let sb = cri
            .run_pod_sandbox(sandbox_config("web"), "")
            .await
            .unwrap();
        let err = cri
            .create_container(&sb, container_config("missing"), sandbox_config("web"))
            .await
            .unwrap_err();
        assert!(err.is_not_found());
        cri.add_image("missing");
        let err = cri
            .create_container("nope", container_config("missing"), sandbox_config("web"))
            .await
            .unwrap_err();
        assert!(err.is_not_found());
    }

    #[tokio::test]
    async fn create_container_rejects_stopped_sandbox_and_duplicates() {
        let cri = FakeCri::new();
        cri.add_image("busybox");
        let sb = cri
            .run_pod_sandbox(sandbox_config("web"), "")
            .await
            .unwrap();
        cri.create_container(&sb, container_config("busybox"), sandbox_config("web"))
            .await
            .unwrap();
        let dup = cri
            .create_container(&sb, container_config("busybox"), sandbox_config("web"))
            .await
            .unwrap_err();
        assert!(matches!(dup, CriError::AlreadyExists(_)));
        let mut second = container_config("busybox");
        second.metadata.as_mut().unwrap().attempt = 1;
        cri.create_container(&sb, second, sandbox_config("web"))
            .await
            .unwrap();

        cri.stop_pod_sandbox(&sb).await.unwrap();
        let mut third = container_config("busybox");
        third.metadata.as_mut().unwrap().name = "other".to_owned();
        let err = cri
            .create_container(&sb, third, sandbox_config("web"))
            .await
            .unwrap_err();
        assert!(matches!(err, CriError::FailedPrecondition(_)));
    }

    #[tokio::test]
    async fn transitions_set_timestamps_and_exit_codes() {
        let cri = FakeCri::new();
        cri.add_image("busybox");
        let sb = cri
            .run_pod_sandbox(sandbox_config("web"), "")
            .await
            .unwrap();
        let c = cri
            .create_container(&sb, container_config("busybox"), sandbox_config("web"))
            .await
            .unwrap();
        let created = cri.container_status(&c).await.unwrap();
        assert!(created.created_at > 0);
        assert_eq!((created.started_at, created.finished_at), (0, 0));

        cri.start_container(&c).await.unwrap();
        let running = cri.container_status(&c).await.unwrap();
        assert!(running.started_at > running.created_at);

        cri.stop_container(&c, 5).await.unwrap();
        let stopped = cri.container_status(&c).await.unwrap();
        assert_eq!(stopped.exit_code, 137);
        assert!(stopped.finished_at > stopped.started_at);
        let listed = cri.list_containers(None).await.unwrap();
        assert_eq!(listed[0].created_at, created.created_at);

        let d = cri
            .create_container(
                &sb,
                {
                    let mut c = container_config("busybox");
                    c.metadata.as_mut().unwrap().name = "job".to_owned();
                    c
                },
                sandbox_config("web"),
            )
            .await
            .unwrap();
        cri.start_container(&d).await.unwrap();
        cri.exit_container(&d, 3);
        let exited = cri.container_status(&d).await.unwrap();
        assert_eq!(exited.exit_code, 3);
        assert!(exited.finished_at > exited.started_at);
        // A later stop does not overwrite the exit code.
        cri.stop_container(&d, 5).await.unwrap();
        assert_eq!(cri.container_status(&d).await.unwrap().exit_code, 3);
    }

    #[tokio::test]
    async fn removing_a_sandbox_removes_its_containers() {
        let cri = FakeCri::new();
        cri.add_image("busybox");
        let sb = cri
            .run_pod_sandbox(sandbox_config("web"), "")
            .await
            .unwrap();
        cri.create_container(&sb, container_config("busybox"), sandbox_config("web"))
            .await
            .unwrap();
        cri.remove_pod_sandbox(&sb).await.unwrap();
        assert!(cri.list_containers(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_sandboxes_filters_by_label() {
        let cri = FakeCri::new();
        cri.run_pod_sandbox(sandbox_config("a"), "").await.unwrap();
        cri.run_pod_sandbox(sandbox_config("b"), "").await.unwrap();
        let filter = PodSandboxFilter {
            label_selector: HashMap::from([("app".to_owned(), "b".to_owned())]),
            ..Default::default()
        };
        let found = cri.list_pod_sandboxes(Some(filter)).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].metadata.as_ref().unwrap().name, "b");
    }
}
