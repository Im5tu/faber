use std::future::Future;
use std::path::Path;
use std::time::Duration;

use tonic::transport::Channel;

use crate::v1::image_service_client::ImageServiceClient;
use crate::v1::runtime_service_client::RuntimeServiceClient;
use crate::v1::{
    Container, ContainerConfig, ContainerFilter, ContainerStatus, CreateContainerRequest, Image,
    ImageSpec, ImageStatusRequest, ListContainersRequest, ListPodSandboxRequest, PodSandbox,
    PodSandboxConfig, PodSandboxFilter, PodSandboxStatus, PodSandboxStatusRequest,
    PullImageRequest, RemoveContainerRequest, RemovePodSandboxRequest, RunPodSandboxRequest,
    StartContainerRequest, StopContainerRequest, StopPodSandboxRequest, VersionRequest,
    VersionResponse,
};
use crate::{BoxFuture, CriClient, CriError, RegistryCredentials, Result};

/// containerd's default CRI socket.
pub const DEFAULT_SOCKET: &str = "/run/containerd/containerd.sock";

/// The CRI version this client speaks, sent in `Version`.
const API_VERSION: &str = "v1";

/// Time limits for [`GrpcCri`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    /// Establishing the socket connection.
    pub connect: Duration,
    /// Each call except image pulls.
    pub call: Duration,
    /// Image pulls, which can legitimately take minutes.
    pub pull: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            call: Duration::from_secs(30),
            pull: Duration::from_secs(15 * 60),
        }
    }
}

/// Awaits a gRPC call under `limit`, turning expiry into [`CriError::Timeout`].
async fn call<T>(
    limit: Duration,
    fut: impl Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
) -> Result<T> {
    match tokio::time::timeout(limit, fut).await {
        Ok(response) => Ok(response?.into_inner()),
        Err(_) => Err(CriError::Timeout(limit)),
    }
}

/// CRI client over a Unix socket (containerd). Cheap to clone.
#[derive(Debug, Clone)]
pub struct GrpcCri {
    timeouts: Timeouts,
    runtime: RuntimeServiceClient<Channel>,
    image: ImageServiceClient<Channel>,
}

impl GrpcCri {
    /// Connects to the runtime socket at `path`.
    #[cfg(unix)]
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        Self::connect_with(path, Timeouts::default()).await
    }

    /// Connects to the runtime socket at `path` with explicit time limits.
    #[cfg(unix)]
    pub async fn connect_with(path: impl AsRef<Path>, timeouts: Timeouts) -> Result<Self> {
        use hyper_util::rt::TokioIo;
        use tonic::transport::{Endpoint, Uri};
        use tower::service_fn;

        let path = path.as_ref().to_path_buf();
        // The URI is required by tonic but ignored by the connector.
        let channel = Endpoint::from_static("http://localhost")
            .connect_timeout(timeouts.connect)
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    Ok::<_, std::io::Error>(TokioIo::new(
                        tokio::net::UnixStream::connect(path).await?,
                    ))
                }
            }))
            .await
            .map_err(|e| CriError::Connect(e.to_string()))?;
        Ok(Self {
            timeouts,
            runtime: RuntimeServiceClient::new(channel.clone()),
            image: ImageServiceClient::new(channel),
        })
    }

    /// Connects to the runtime socket at `path`.
    #[cfg(not(unix))]
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        Self::connect_with(path, Timeouts::default()).await
    }

    /// Connects to the runtime socket at `path` with explicit time limits.
    #[cfg(not(unix))]
    pub async fn connect_with(_path: impl AsRef<Path>, _timeouts: Timeouts) -> Result<Self> {
        Err(CriError::Unsupported)
    }

    /// Connects to [`DEFAULT_SOCKET`].
    pub async fn connect_default() -> Result<Self> {
        Self::connect(DEFAULT_SOCKET).await
    }
}

fn spec(image: &str) -> ImageSpec {
    ImageSpec {
        image: image.to_owned(),
        ..Default::default()
    }
}

impl CriClient for GrpcCri {
    fn version(&self) -> BoxFuture<'_, Result<VersionResponse>> {
        Box::pin(async move {
            let req = VersionRequest {
                version: API_VERSION.to_owned(),
            };
            Ok(call(self.timeouts.call, self.runtime.clone().version(req)).await?)
        })
    }

    fn image_status<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<Option<Image>>> {
        Box::pin(async move {
            let req = ImageStatusRequest {
                image: Some(spec(image)),
                verbose: false,
            };
            Ok(
                call(self.timeouts.call, self.image.clone().image_status(req))
                    .await?
                    .image,
            )
        })
    }

    fn pull_image<'a>(
        &'a self,
        image: &'a str,
        auth: Option<RegistryCredentials>,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let req = PullImageRequest {
                image: Some(spec(image)),
                auth: auth.map(Into::into),
                sandbox_config: None,
            };
            Ok(call(self.timeouts.pull, self.image.clone().pull_image(req))
                .await?
                .image_ref)
        })
    }

    fn run_pod_sandbox<'a>(
        &'a self,
        config: PodSandboxConfig,
        runtime_handler: &'a str,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let req = RunPodSandboxRequest {
                config: Some(config),
                runtime_handler: runtime_handler.to_owned(),
            };
            Ok(call(
                self.timeouts.call,
                self.runtime.clone().run_pod_sandbox(req),
            )
            .await?
            .pod_sandbox_id)
        })
    }

    fn stop_pod_sandbox<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let req = StopPodSandboxRequest {
                pod_sandbox_id: id.to_owned(),
            };
            call(
                self.timeouts.call,
                self.runtime.clone().stop_pod_sandbox(req),
            )
            .await?;
            Ok(())
        })
    }

    fn remove_pod_sandbox<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let req = RemovePodSandboxRequest {
                pod_sandbox_id: id.to_owned(),
            };
            call(
                self.timeouts.call,
                self.runtime.clone().remove_pod_sandbox(req),
            )
            .await?;
            Ok(())
        })
    }

    fn list_pod_sandboxes(
        &self,
        filter: Option<PodSandboxFilter>,
    ) -> BoxFuture<'_, Result<Vec<PodSandbox>>> {
        Box::pin(async move {
            let req = ListPodSandboxRequest { filter };
            Ok(call(
                self.timeouts.call,
                self.runtime.clone().list_pod_sandbox(req),
            )
            .await?
            .items)
        })
    }

    fn pod_sandbox_status<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<PodSandboxStatus>> {
        Box::pin(async move {
            let req = PodSandboxStatusRequest {
                pod_sandbox_id: id.to_owned(),
                verbose: false,
            };
            call(
                self.timeouts.call,
                self.runtime.clone().pod_sandbox_status(req),
            )
            .await?
            .status
            .ok_or(CriError::MalformedResponse("pod sandbox status"))
        })
    }

    fn create_container<'a>(
        &'a self,
        sandbox_id: &'a str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let req = CreateContainerRequest {
                pod_sandbox_id: sandbox_id.to_owned(),
                config: Some(config),
                sandbox_config: Some(sandbox_config),
            };
            Ok(call(
                self.timeouts.call,
                self.runtime.clone().create_container(req),
            )
            .await?
            .container_id)
        })
    }

    fn start_container<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let req = StartContainerRequest {
                container_id: id.to_owned(),
            };
            call(
                self.timeouts.call,
                self.runtime.clone().start_container(req),
            )
            .await?;
            Ok(())
        })
    }

    fn stop_container<'a>(&'a self, id: &'a str, timeout_secs: i64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let req = StopContainerRequest {
                container_id: id.to_owned(),
                timeout: timeout_secs,
            };
            call(self.timeouts.call, self.runtime.clone().stop_container(req)).await?;
            Ok(())
        })
    }

    fn remove_container<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let req = RemoveContainerRequest {
                container_id: id.to_owned(),
            };
            call(
                self.timeouts.call,
                self.runtime.clone().remove_container(req),
            )
            .await?;
            Ok(())
        })
    }

    fn list_containers(
        &self,
        filter: Option<ContainerFilter>,
    ) -> BoxFuture<'_, Result<Vec<Container>>> {
        Box::pin(async move {
            let req = ListContainersRequest { filter };
            Ok(call(
                self.timeouts.call,
                self.runtime.clone().list_containers(req),
            )
            .await?
            .containers)
        })
    }

    fn container_status<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<ContainerStatus>> {
        Box::pin(async move {
            let req = crate::v1::ContainerStatusRequest {
                container_id: id.to_owned(),
                verbose: false,
            };
            call(
                self.timeouts.call,
                self.runtime.clone().container_status(req),
            )
            .await?
            .status
            .ok_or(CriError::MalformedResponse("container status"))
        })
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;

    #[tokio::test]
    async fn call_times_out() {
        let err = call::<()>(Duration::from_millis(10), std::future::pending())
            .await
            .unwrap_err();
        assert!(matches!(err, CriError::Timeout(_)));
    }

    #[tokio::test]
    async fn call_maps_status_errors() {
        let err = call::<()>(Duration::from_secs(1), async {
            Err(tonic::Status::not_found("x"))
        })
        .await
        .unwrap_err();
        assert!(err.is_not_found());
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Needs a running containerd; run with `cargo test -p faber-cri -- --ignored`.
    #[tokio::test]
    #[ignore = "needs containerd on /run/containerd/containerd.sock"]
    async fn version_against_real_socket() {
        let socket = std::env::var("CRI_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET.to_owned());
        let cri = GrpcCri::connect(socket).await.unwrap();
        let version = cri.version().await.unwrap();
        assert!(!version.runtime_name.is_empty());
    }
}
