//! The DNS server of a node, run against the replicated state: service
//! addresses come from the store, and the server listens on the node's bridge
//! gateway once the mesh has given the node its container subnet.

use std::sync::Arc;

use ipnet::Ipv4Net;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{
    DnsConfig, DnsServer, LookupFailed, ServiceIps, UPSTREAM_TIMEOUT, listen_addr, system_upstreams,
};
use crate::mesh::state::MeshState;
use crate::raft::store::StateReader;
use crate::reconcile::own_subnet;

/// [`ServiceIps`] over the replicated service addresses.
#[derive(Debug, Clone)]
pub struct StateServiceIps {
    reader: StateReader,
}

impl StateServiceIps {
    pub fn new(reader: StateReader) -> Self {
        Self { reader }
    }
}

impl ServiceIps for StateServiceIps {
    fn lookup(
        &self,
        project: &str,
        env: &str,
        svc: &str,
    ) -> Result<Option<std::net::Ipv4Addr>, LookupFailed> {
        self.reader.service_ip(project, env, svc).map_err(|e| {
            tracing::warn!(error = %e, "cannot read the service address for DNS");
            LookupFailed
        })
    }
}

/// What the server is set up for: changing either part means a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    /// The node's container subnet; the server listens on its gateway.
    subnet: Ipv4Net,
    /// The container and service ranges, where no upstream may sit.
    excluded: Vec<Ipv4Net>,
}

/// The target of node `node_id` in `mesh`, `None` while the node has no valid
/// container subnet there.
fn target(mesh: &MeshState, node_id: u64) -> Option<Target> {
    let subnet = own_subnet(mesh, node_id).ok().flatten()?;
    let excluded = [mesh.config.container_cidr, mesh.config.service_cidr]
        .into_iter()
        .flatten()
        .collect();
    Some(Target { subnet, excluded })
}

/// A server running for one [`Target`].
struct Running {
    target: Target,
    stop: CancellationToken,
    server: DnsServer,
}

impl Running {
    fn start(
        target: Target,
        ips: &Arc<dyn ServiceIps>,
        parent: &CancellationToken,
    ) -> Option<Self> {
        let listen = listen_addr(target.subnet);
        let config = DnsConfig {
            listen,
            upstreams: system_upstreams(Some(listen.ip()), &target.excluded),
            upstream_timeout: UPSTREAM_TIMEOUT,
        };
        let stop = parent.child_token();
        match DnsServer::start(config, ips.clone(), stop.clone()) {
            Ok(server) => {
                tracing::info!(%listen, "DNS listening");
                Some(Self {
                    target,
                    stop,
                    server,
                })
            }
            Err(e) => {
                tracing::warn!(%listen, error = %e, "cannot start DNS");
                None
            }
        }
    }

    async fn stop(self) {
        self.stop.cancel();
        self.server.wait().await;
    }
}

/// Runs the DNS server of node `node_id` until `cancel` fires. The server
/// follows the replicated mesh: it starts once the node has a container
/// subnet, and restarts on the new gateway if the subnet or the ranges ever
/// change. A bind failure is logged, and tried again at the next change of
/// state; it leaves the node without DNS, not down.
pub fn spawn_node(reader: StateReader, node_id: u64, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let ips: Arc<dyn ServiceIps> = Arc::new(StateServiceIps::new(reader.clone()));
        let mut changes = reader.subscribe();
        let mut running: Option<Running> = None;
        loop {
            let want = match reader.mesh() {
                Ok(mesh) => mesh.and_then(|mesh| target(&mesh, node_id)),
                Err(e) => {
                    tracing::warn!(error = %e, "cannot read the mesh for DNS");
                    running.as_ref().map(|r| r.target.clone())
                }
            };
            if running.as_ref().map(|r| &r.target) != want.as_ref() {
                if let Some(old) = running.take() {
                    old.stop().await;
                }
                running = want.and_then(|t| Running::start(t, &ips, &cancel));
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                changed = changes.changed() => if changed.is_err() { break },
            }
        }
        if let Some(running) = running {
            running.stop().await;
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    use super::*;
    use crate::mesh::state::{DEFAULT_LEASE_SECS, MeshConfig, NodeRecord};

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    fn mesh(subnet: Option<Ipv4Net>) -> MeshState {
        let node = NodeRecord {
            node_id: 1,
            public_key: String::new(),
            mesh_ip: Ipv4Addr::new(10, 99, 0, 1),
            endpoint: None,
            invite_id: None,
            instance_id: None,
            container_subnet: subnet,
            name: "node-1".into(),
            lease_renewed_at: 0,
        };
        MeshState {
            config: MeshConfig {
                container_cidr: Some(net("10.200.0.0/16")),
                service_cidr: Some(net("10.201.0.0/16")),
                ..MeshConfig::default()
            },
            next_node_id: 100,
            nodes: BTreeMap::from([(1, node)]),
            invites: BTreeMap::new(),
            clock: 0,
            lease_secs: DEFAULT_LEASE_SECS,
        }
    }

    #[test]
    fn dns_target_is_the_own_subnet_and_both_ranges() {
        let got = target(&mesh(Some(net("10.200.3.0/24"))), 1).unwrap();
        assert_eq!(got.subnet, net("10.200.3.0/24"));
        assert_eq!(
            got.excluded,
            vec![net("10.200.0.0/16"), net("10.201.0.0/16")]
        );
    }

    #[test]
    fn dns_has_no_target_without_a_subnet_or_a_node() {
        assert_eq!(target(&mesh(None), 1), None);
        assert_eq!(target(&mesh(Some(net("10.200.3.0/24"))), 2), None);
        // A subnet outside the container range is not used.
        assert_eq!(target(&mesh(Some(net("192.168.0.0/24"))), 1), None);
    }
}
