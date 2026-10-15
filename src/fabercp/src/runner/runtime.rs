//! The container runtime's config, written by `faber-node`.
//!
//! containerd runs as root and loads the files this writes, so fabercp does
//! not write them. [`NodeRuntime`] sends the values to `faber-node`, which
//! checks them and writes the files to paths of its own. No path or file
//! body is ever sent, and the CNI directories in the replicated
//! `ContainerRuntimeSpec` are not read: `faber-node` owns both.

use std::io;

use faber_container_runtime::{
    BoxFuture, ContainerRuntime, NodeRuntimeSpec, PodNetworkSpec, RuntimeError,
};
use faber_node_protocol::{PodNetwork, Request};

use super::node::{NodeCallError, NodeClient, call_changed};

const CONFIGURE: &str = "configuring containerd through faber-node";
const REMOVE_CNI: &str = "removing the CNI network config through faber-node";

/// A [`ContainerRuntime`] that asks `faber-node` to do the work.
pub struct NodeRuntime<C> {
    client: C,
}

impl<C: NodeClient> NodeRuntime<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }
}

fn failed(action: &'static str, source: NodeCallError) -> RuntimeError {
    RuntimeError::Io {
        action,
        source: io::Error::other(source),
    }
}

fn pod_network(spec: &PodNetworkSpec) -> PodNetwork {
    PodNetwork {
        bridge: spec.bridge.clone(),
        subnet: spec.subnet,
        mtu: spec.mtu,
    }
}

impl<C: NodeClient> ContainerRuntime for NodeRuntime<C> {
    fn reconcile<'a>(
        &'a self,
        spec: &'a NodeRuntimeSpec,
    ) -> BoxFuture<'a, Result<bool, RuntimeError>> {
        Box::pin(async move {
            let request = Request::ConfigureRuntime {
                systemd_cgroup: spec.settings.systemd_cgroup,
                sandbox_image: spec.settings.sandbox_image.clone(),
                pod_network: spec.pod_network.as_ref().map(pod_network),
            };
            call_changed(&self.client, request, "configure_runtime")
                .await
                .map_err(|e| failed(CONFIGURE, e))
        })
    }

    fn remove_pod_network(&self) -> BoxFuture<'_, Result<bool, RuntimeError>> {
        Box::pin(async move {
            call_changed(&self.client, Request::RemoveCniConfig, "remove_cni_config")
                .await
                .map_err(|e| failed(REMOVE_CNI, e))
        })
    }
}

#[cfg(test)]
mod tests {
    use faber_container_runtime::ContainerRuntimeSpec;

    use super::*;
    use crate::runner::node::fake::{FakeNode, RuntimeFiles};

    fn network() -> PodNetworkSpec {
        PodNetworkSpec {
            bridge: "faberbr0".into(),
            subnet: "10.200.3.0/24".parse().unwrap(),
            mtu: 1420,
        }
    }

    fn spec(pod_network: Option<PodNetworkSpec>) -> NodeRuntimeSpec {
        NodeRuntimeSpec {
            settings: ContainerRuntimeSpec::default(),
            pod_network,
        }
    }

    #[tokio::test]
    async fn reconcile_sends_the_settings_and_the_network_and_no_paths() {
        let node = FakeNode::default();
        let runtime = NodeRuntime::new(node.clone());
        // Directories in the replicated settings stay on this side.
        let mut with_dirs = spec(Some(network()));
        with_dirs.settings.cni_bin_dir = "/tmp/evil-plugins".into();
        with_dirs.settings.cni_conf_dir = "/tmp/evil-conf".into();

        assert!(runtime.reconcile(&with_dirs).await.unwrap());
        assert_eq!(
            node.log(),
            [
                "configure_runtime cgroup=true image=registry.k8s.io/pause:3.10 \
              network=faberbr0 10.200.3.0/24 1420"
            ]
        );
        assert_eq!(
            node.runtime_files(),
            Some(RuntimeFiles {
                systemd_cgroup: true,
                sandbox_image: "registry.k8s.io/pause:3.10".into(),
                pod_network: Some(PodNetwork {
                    bridge: "faberbr0".into(),
                    subnet: "10.200.3.0/24".parse().unwrap(),
                    mtu: 1420,
                }),
            })
        );
    }

    #[tokio::test]
    async fn whether_the_host_changed_is_what_the_node_says() {
        let node = FakeNode::default();
        let runtime = NodeRuntime::new(node);
        let with_network = spec(Some(network()));
        assert!(runtime.reconcile(&with_network).await.unwrap());
        assert!(!runtime.reconcile(&with_network).await.unwrap());
    }

    #[tokio::test]
    async fn no_pod_network_is_sent_as_none() {
        let node = FakeNode::default();
        let runtime = NodeRuntime::new(node.clone());
        runtime.reconcile(&spec(Some(network()))).await.unwrap();
        assert!(runtime.reconcile(&spec(None)).await.unwrap());
        assert_eq!(node.runtime_files().unwrap().pod_network, None);
    }

    #[tokio::test]
    async fn removing_the_pod_network_asks_the_node_to_remove_the_cni_config() {
        let node = FakeNode::default();
        let runtime = NodeRuntime::new(node.clone());
        runtime.reconcile(&spec(Some(network()))).await.unwrap();
        node.take_log();

        assert!(runtime.remove_pod_network().await.unwrap());
        assert!(!runtime.remove_pod_network().await.unwrap());
        assert_eq!(node.log(), ["remove_cni_config", "remove_cni_config"]);
        assert_eq!(node.runtime_files().unwrap().pod_network, None);
    }

    #[tokio::test]
    async fn a_refusal_is_a_runtime_error_that_names_the_action() {
        let node = FakeNode::default();
        node.fail_next("configure_runtime");
        let runtime = NodeRuntime::new(node);
        let err = runtime.reconcile(&spec(None)).await.unwrap_err();
        assert!(matches!(err, RuntimeError::Io { action, .. } if action == CONFIGURE));
        let cause = std::error::Error::source(&err).unwrap().to_string();
        assert!(
            cause.contains("configure_runtime failed on purpose"),
            "{cause}"
        );
    }
}
