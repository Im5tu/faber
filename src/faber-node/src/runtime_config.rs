//! The containerd drop-in and the CNI network config, written by root.
//!
//! Both files are loaded by root (containerd), so only faber-node writes
//! them. It renders them with `faber-container-runtime`, into directories
//! and with plugin paths that its own root-owned config names. A request
//! supplies only checked values, never a path or a file body.
//!
//! faber-node does not restart containerd: the installer's
//! `faber-containerd-reload.path` unit does that when the drop-in changes,
//! and containerd watches the CNI directory by itself.

use std::path::Path;
use std::sync::Arc;

use faber_container_runtime::{
    ContainerRuntime, ContainerRuntimeSpec, Containerd, NodeRuntimeSpec, PodNetworkSpec,
    RuntimeError,
};
use faber_node_protocol::PodNetwork;

use crate::config::NodeConfig;

/// Writes the runtime's config files and knows where they go.
#[derive(Clone)]
pub struct RuntimeConfig {
    runtime: Arc<dyn ContainerRuntime>,
    /// Written into the drop-in: where containerd finds its CNI plugins.
    cni_bin_dir: String,
    /// Written into the drop-in: where containerd reads the CNI config.
    cni_conf_dir: String,
}

impl RuntimeConfig {
    /// Files go to the directories of `config`.
    pub fn for_host(config: &NodeConfig) -> Self {
        Self::new(
            Arc::new(Containerd {
                conf_dir: config.containerd_conf_dir.clone(),
                cni_conf_dir: config.cni_conf_dir.clone(),
            }),
            &config.cni_bin_dir,
            &config.cni_conf_dir,
        )
    }

    /// `runtime` writes the files. The drop-in names `cni_bin_dir` and
    /// `cni_conf_dir`, whatever a request says.
    pub fn new(
        runtime: Arc<dyn ContainerRuntime>,
        cni_bin_dir: &Path,
        cni_conf_dir: &Path,
    ) -> Self {
        Self {
            runtime,
            cni_bin_dir: cni_bin_dir.to_string_lossy().into_owned(),
            cni_conf_dir: cni_conf_dir.to_string_lossy().into_owned(),
        }
    }

    /// Brings both files to the given, already checked, values. Returns
    /// whether one changed.
    pub async fn configure(
        &self,
        systemd_cgroup: bool,
        sandbox_image: &str,
        pod_network: Option<&PodNetwork>,
    ) -> Result<bool, RuntimeError> {
        let spec = NodeRuntimeSpec {
            settings: ContainerRuntimeSpec {
                systemd_cgroup,
                sandbox_image: sandbox_image.to_owned(),
                cni_bin_dir: self.cni_bin_dir.clone(),
                cni_conf_dir: self.cni_conf_dir.clone(),
            },
            pod_network: pod_network.map(|n| PodNetworkSpec {
                bridge: n.bridge.clone(),
                subnet: n.subnet,
                mtu: n.mtu,
            }),
        };
        self.runtime.reconcile(&spec).await
    }

    /// Removes the CNI network config. Returns whether a file was removed.
    pub async fn remove_cni_config(&self) -> Result<bool, RuntimeError> {
        self.runtime.remove_pod_network().await
    }
}
