//! The DNS server of a node, run against the replicated state: service
//! addresses come from the store, and the server listens on the node's bridge
//! gateway once the mesh has given the node its container subnet.

use std::net::Ipv4Addr;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use ipnet::Ipv4Net;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{
    DnsConfig, DnsServer, LookupFailed, ServiceIps, UPSTREAM_TIMEOUT, listen_addr, system_upstreams,
};
use crate::mesh::state::MeshState;
use crate::raft::store::StateReader;
use crate::raft::store::services::ServiceIps as StoredIps;
use crate::reconcile::own_subnet;

/// First wait before a failed bind is tried again.
const RETRY_MIN: Duration = Duration::from_secs(1);

/// Longest wait between tries of a failed bind.
const RETRY_MAX: Duration = Duration::from_secs(30);

/// [`ServiceIps`] over a copy of the replicated service addresses, held in
/// memory so a query reads no storage. [`spawn_node`] refreshes it on every
/// change of state.
#[derive(Debug, Default)]
pub struct StateServiceIps {
    /// `None` until the first successful read.
    loaded: RwLock<Option<Arc<StoredIps>>>,
}

impl StateServiceIps {
    /// Replaces the copy with `ips`.
    fn replace(&self, ips: StoredIps) {
        *self.loaded.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(ips));
    }
}

impl ServiceIps for StateServiceIps {
    fn lookup(
        &self,
        project: &str,
        env: &str,
        svc: &str,
    ) -> Result<Option<Ipv4Addr>, LookupFailed> {
        // Without a copy nothing is known, which is not "no such service".
        let loaded = self.loaded.read().unwrap_or_else(PoisonError::into_inner);
        let ips = loaded.as_ref().ok_or(LookupFailed)?;
        Ok(ips.get(project, env, svc))
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

/// The target of node `node_id` in `mesh`: `Ok(None)` while the node has no
/// container subnet there, `Err` with the reason when its subnet is invalid.
fn target(mesh: &MeshState, node_id: u64) -> Result<Option<Target>, String> {
    let Some(subnet) = own_subnet(mesh, node_id)? else {
        return Ok(None);
    };
    let excluded = [mesh.config.container_cidr, mesh.config.service_cidr]
        .into_iter()
        .flatten()
        .collect();
    Ok(Some(Target { subnet, excluded }))
}

/// The wait after a failed bind that followed a wait of `previous`: doubled,
/// from [`RETRY_MIN`] up to [`RETRY_MAX`].
fn next_delay(previous: Option<Duration>) -> Duration {
    previous.map_or(RETRY_MIN, |d| (d * 2).min(RETRY_MAX))
}

/// A target whose bind failed, and when to try it again.
struct Retry {
    target: Target,
    at: Instant,
    delay: Duration,
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
/// change. A bind failure is logged and tried again after 1 s, then after
/// twice as long each time up to 30 s; it leaves the node without DNS, not
/// down.
///
/// The service addresses are read again at each change of state, off the
/// async threads, and a failed read keeps the last good copy.
pub fn spawn_node(reader: StateReader, node_id: u64, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let cache = Arc::new(StateServiceIps::default());
        let ips: Arc<dyn ServiceIps> = cache.clone();
        let mut changes = reader.subscribe();
        let mut running: Option<Running> = None;
        let mut retry: Option<Retry> = None;
        loop {
            let read = {
                let reader = reader.clone();
                tokio::task::spawn_blocking(move || (reader.mesh(), reader.service_ips())).await
            };
            let want = match read {
                Ok((mesh, stored)) => {
                    match stored {
                        Ok(stored) => cache.replace(stored),
                        Err(e) => {
                            tracing::warn!(error = %e, "cannot read the service addresses for DNS")
                        }
                    }
                    match mesh.map(|m| m.map(|m| target(&m, node_id)).transpose()) {
                        Ok(Ok(want)) => Some(want.flatten()),
                        Ok(Err(problem)) => {
                            tracing::warn!(problem, "invalid container subnet, DNS stays as it is");
                            None
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "cannot read the mesh for DNS");
                            None
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reading the state for DNS failed");
                    None
                }
            };
            // `None`: not known, so leave things as they are.
            if let Some(want) = want {
                if running.as_ref().map(|r| &r.target) != want.as_ref()
                    && let Some(old) = running.take()
                {
                    old.stop().await;
                }
                if want.is_none() {
                    retry = None;
                }
                if running.is_none()
                    && let Some(target) = want
                {
                    let due = retry
                        .as_ref()
                        .is_none_or(|r| r.target != target || Instant::now() >= r.at);
                    if due {
                        let previous = retry
                            .as_ref()
                            .filter(|r| r.target == target)
                            .map(|r| r.delay);
                        running = Running::start(target.clone(), &ips, &cancel);
                        retry = if running.is_some() {
                            None
                        } else {
                            let delay = next_delay(previous);
                            Some(Retry {
                                target,
                                at: Instant::now() + delay,
                                delay,
                            })
                        };
                    }
                }
            }
            let retry_at = retry.as_ref().map(|r| r.at);
            tokio::select! {
                () = cancel.cancelled() => break,
                changed = changes.changed() => if changed.is_err() { break },
                () = async {
                    match retry_at {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {}
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
        let got = target(&mesh(Some(net("10.200.3.0/24"))), 1)
            .unwrap()
            .unwrap();
        assert_eq!(got.subnet, net("10.200.3.0/24"));
        assert_eq!(
            got.excluded,
            vec![net("10.200.0.0/16"), net("10.201.0.0/16")]
        );
    }

    #[test]
    fn dns_has_no_target_without_a_subnet_or_a_node() {
        assert_eq!(target(&mesh(None), 1), Ok(None));
        assert_eq!(target(&mesh(Some(net("10.200.3.0/24"))), 2), Ok(None));
    }

    #[test]
    fn dns_reports_a_subnet_outside_the_container_range() {
        assert!(target(&mesh(Some(net("192.168.0.0/24"))), 1).is_err());
    }

    #[test]
    fn dns_bind_retries_back_off_from_one_second_to_thirty() {
        let mut delay = next_delay(None);
        let mut seen = vec![delay.as_secs()];
        for _ in 0..6 {
            delay = next_delay(Some(delay));
            seen.push(delay.as_secs());
        }
        assert_eq!(seen, [1, 2, 4, 8, 16, 30, 30]);
    }

    #[test]
    fn dns_cache_fails_until_loaded_then_answers_from_memory() {
        let cache = StateServiceIps::default();
        assert_eq!(cache.lookup("shop", "prod", "web"), Err(LookupFailed));
        let mut stored = StoredIps::default();
        stored.ips.insert(
            crate::raft::store::services::service_key("shop", "prod", "web"),
            Ipv4Addr::new(10, 201, 0, 5),
        );
        cache.replace(stored);
        assert_eq!(
            cache.lookup("shop", "prod", "web"),
            Ok(Some(Ipv4Addr::new(10, 201, 0, 5)))
        );
        assert_eq!(cache.lookup("shop", "prod", "api"), Ok(None));
    }
}
