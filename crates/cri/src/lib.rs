//! CRI v1 gRPC client: the client trait, an in-memory fake and the real
//! client that talks to containerd over its Unix socket.
//!
//! The wire types are generated from the vendored `kubernetes/cri-api`
//! v0.34.0 `api.proto` (see the crate README) and live in [`v1`].

mod credentials;
mod error;
pub mod fake;
mod grpc;

use std::future::Future;
use std::pin::Pin;

pub use credentials::RegistryCredentials;
pub use error::CriError;
pub use grpc::{DEFAULT_SOCKET, GrpcCri, Timeouts};

/// Generated CRI v1 messages and clients.
#[allow(
    clippy::all,
    clippy::pedantic,
    missing_docs,
    rustdoc::invalid_html_tags,
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls,
    reason = "prost/tonic generated code, checked in as is"
)]
pub mod v1 {
    include!("generated/runtime.v1.rs");
}

pub use v1::{
    Container, ContainerConfig, ContainerFilter, ContainerState, ContainerStatus, Image,
    PodSandbox, PodSandboxConfig, PodSandboxFilter, PodSandboxState, PodSandboxStatus,
    VersionResponse,
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type Result<T> = std::result::Result<T, CriError>;

/// The slice of the CRI runtime and image services that faber uses.
pub trait CriClient: Send + Sync {
    fn version(&self) -> BoxFuture<'_, Result<VersionResponse>>;

    /// `None` when the image is not present on the node.
    fn image_status<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<Option<Image>>>;

    /// Pulls `image` and returns the resulting image reference (its id).
    fn pull_image<'a>(
        &'a self,
        image: &'a str,
        auth: Option<RegistryCredentials>,
    ) -> BoxFuture<'a, Result<String>>;

    /// Returns the new sandbox id.
    fn run_pod_sandbox<'a>(
        &'a self,
        config: PodSandboxConfig,
        runtime_handler: &'a str,
    ) -> BoxFuture<'a, Result<String>>;

    fn stop_pod_sandbox<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>>;

    fn remove_pod_sandbox<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>>;

    fn list_pod_sandboxes(
        &self,
        filter: Option<PodSandboxFilter>,
    ) -> BoxFuture<'_, Result<Vec<PodSandbox>>>;

    fn pod_sandbox_status<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<PodSandboxStatus>>;

    /// Returns the new container id.
    fn create_container<'a>(
        &'a self,
        sandbox_id: &'a str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> BoxFuture<'a, Result<String>>;

    fn start_container<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>>;

    /// `timeout_secs` is the grace period before the runtime kills the container.
    fn stop_container<'a>(&'a self, id: &'a str, timeout_secs: i64) -> BoxFuture<'a, Result<()>>;

    fn remove_container<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<()>>;

    fn list_containers(
        &self,
        filter: Option<ContainerFilter>,
    ) -> BoxFuture<'_, Result<Vec<Container>>>;

    fn container_status<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<ContainerStatus>>;
}
