//! Container runtime configuration for a faber node.
//!
//! The caller describes what it wants as a [`NodeRuntimeSpec`], which names
//! no particular runtime, and hands it to a [`ContainerRuntime`].
//! [`Containerd`] is the one implementation today; supporting another runtime
//! means adding another implementation of the trait.
//!
//! [`Containerd`] owns two files: the containerd drop-in, rendered from its
//! own fixed settings, and the CNI network config ([`cni`]), rendered from
//! this node's container network and removed while the node has none.

pub mod cni;
pub mod containerd;
mod file;

use std::future::Future;
use std::io;
use std::pin::Pin;

use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

pub use containerd::Containerd;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What `ContainerRuntime::reconcile` applies on one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRuntimeSpec {
    /// This node's container network. `None` until raft has allocated a subnet.
    pub pod_network: Option<PodNetworkSpec>,
}

/// The container network of one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodNetworkSpec {
    /// This node's container subnet.
    pub subnet: Ipv4Net,
    pub mtu: u16,
}

/// Why a runtime could not be brought to its spec. The message says what was
/// being done; the cause is the [`source`](std::error::Error::source).
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("{action}")]
    Io {
        action: &'static str,
        #[source]
        source: io::Error,
    },
    /// The blocking task doing the work panicked or was cancelled.
    #[error("{action} did not finish")]
    Task {
        action: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },
}

/// A container runtime on this host. Object safe, so it is shared as
/// `Arc<dyn ContainerRuntime>` and can be swapped.
pub trait ContainerRuntime: Send + Sync {
    /// Brings the runtime's configuration to `spec`. Idempotent. Returns
    /// whether anything on the host changed.
    fn reconcile<'a>(
        &'a self,
        spec: &'a NodeRuntimeSpec,
    ) -> BoxFuture<'a, Result<bool, RuntimeError>>;

    /// Removes this node's container network config and nothing else, for a
    /// node that is leaving the cluster. Idempotent.
    /// Returns whether a file was removed.
    fn remove_pod_network(&self) -> BoxFuture<'_, Result<bool, RuntimeError>>;
}
