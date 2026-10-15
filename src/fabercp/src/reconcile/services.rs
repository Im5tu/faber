//! Service reconcile step: programs the virtual IPs of the services, and
//! the containers behind them, into this node's nftables.
//!
//! The desired value is [`desired_services`]: for every service with a
//! virtual IP, the IP and the sorted list of `(pod IP, port, protocol)` that
//! serve it. It holds nothing that changes when a node merely reports again
//! (no `reported_at`, no `detail`), so a status tick that moves no replica
//! gives an equal value and the step does not apply.
//!
//! Every node programs every service, so a container reaches a service from
//! any node without an extra hop. The rules are part of the one
//! `inet faber` table that the network step loads;
//! [`NetworkManager::set_services`] keeps them when that table is loaded
//! again.
//!
//! # Which revision serves
//!
//! During a deploy two revisions of a workload run side by side. The
//! **active revision** is the one whose replicas are the backends:
//!
//! 1. the newest revision (not newer than the stored one) that has every one
//!    of its desired replicas `available`, where the desired replicas are the
//!    ones placement lists for the workload; or, if there is none,
//! 2. the newest revision that has at least one `available` replica.
//!
//! A new revision therefore takes over only once all its replicas are
//! available (blue/green): while it is still coming up, the old revision keeps
//! the traffic. If no revision is complete, say because a replica of the
//! stored revision cannot be placed or is down, the newest revision that
//! serves at all is used: the service keeps answering from what is up, and a
//! failure of one replica of a running revision does not hand traffic back
//! to a revision that was already stopped. A revision with a replica that
//! cannot be placed never qualifies under the first rule; it takes over only
//! when no older revision is complete.
//!
//! Only replicas that are placed, reported `available` by their own node,
//! with a pod IP, count, and a node that has gone quiet for three report
//! intervals (plus one lease tick) counts for nothing
//! ([`crate::status::StatusView`]).
//!
//! The ports are the ports the stored spec declares, on the same number at
//! the virtual IP. They are read from the stored revision's spec for every
//! revision: the old revision of a deploy that changes the ports is served on
//! the new ports until the new revision takes over.

use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use faber_api_types::workload::{Protocol, ReplicaState};
use faber_network_management::{NatBackend, NatProtocol, NetworkManager, ServiceNat};

use super::step::{Planned, ReconcileStep, ReplicatedStateView, StepFuture};
use crate::lease::LeaseTiming;
use crate::raft::store::PlacementView;
use crate::status::StatusView;

/// The services step.
pub struct ServicesStep {
    network: NetworkManager,
    lease_timing: LeaseTiming,
}

impl ServicesStep {
    pub fn new(network: NetworkManager, lease_timing: LeaseTiming) -> Self {
        Self {
            network,
            lease_timing,
        }
    }
}

impl ReconcileStep for ServicesStep {
    type Desired = Vec<ServiceNat>;

    fn name(&self) -> &'static str {
        "services"
    }

    fn plan(&self, view: &ReplicatedStateView) -> Planned<Vec<ServiceNat>> {
        let desired = match &view.placement {
            Some(placement) => {
                let tick = placement
                    .mesh
                    .as_ref()
                    .map_or(0, |m| self.lease_timing.tick_clock_secs(m.lease_secs));
                desired_services(placement, view.status_interval_secs, tick)
            }
            None => Vec::new(),
        };
        Planned {
            desired,
            warnings: Vec::new(),
        }
    }

    fn apply<'a>(&'a self, desired: &'a Vec<ServiceNat>) -> StepFuture<'a> {
        Box::pin(async move { Ok(self.network.set_services(desired).await?) })
    }
}

/// The service rules this node should have, sorted by virtual IP. See the
/// module documentation for what a backend is and which revision serves.
///
/// `interval_secs` is the status report interval in force and `tick_secs` the
/// most the lease clock runs ahead of a report that is on time.
pub fn desired_services(
    view: &PlacementView,
    interval_secs: u64,
    tick_secs: u64,
) -> Vec<ServiceNat> {
    let Some(mesh) = view.mesh.as_ref() else {
        return Vec::new();
    };
    let mut statuses = view.statuses.clone();
    statuses.retain(|id, _| mesh.nodes.contains_key(id));
    let status = StatusView::new(statuses, interval_secs, mesh.clock, tick_secs);

    let mut services = Vec::new();
    for (project, environment, record) in &view.environments {
        let placement = view.records.get(&(project.clone(), environment.clone()));
        for (name, workload) in &record.workloads {
            let Some(vip) = view.service_ips.get(project, environment, name) else {
                continue;
            };
            let placed = placement.map_or(&[][..], |p| p.replicas(name));
            let ports: BTreeSet<(u16, NatProtocol)> = workload
                .spec
                .containers
                .iter()
                .flat_map(|c| &c.ports)
                .map(|p| (p.port, nat_protocol(p.protocol)))
                .collect();
            let pod_ips = active_pod_ips(
                &status,
                (project, environment, name),
                workload.revision,
                placed,
            );
            let backends = pod_ips
                .iter()
                .flat_map(|ip| {
                    ports.iter().map(|(port, protocol)| NatBackend {
                        ip: *ip,
                        port: *port,
                        protocol: *protocol,
                    })
                })
                .collect();
            services.push(ServiceNat { vip, backends });
        }
    }
    services.sort_by_key(|s| s.vip);
    services
}

fn nat_protocol(protocol: Protocol) -> NatProtocol {
    match protocol {
        Protocol::Tcp => NatProtocol::Tcp,
        Protocol::Udp => NatProtocol::Udp,
    }
}

/// The pod IPs of the available replicas of the active revision of a
/// workload, sorted and without repeats. `revision` is the stored one.
fn active_pod_ips(
    status: &StatusView,
    (project, environment, workload): (&str, &str, &str),
    revision: u64,
    placed: &[crate::raft::store::place::ReplicaPlacement],
) -> BTreeSet<Ipv4Addr> {
    let mut revisions = status.revisions(project, environment, workload);
    // A report of a revision the record does not have yet cannot be trusted
    // to belong to this workload.
    revisions.retain(|r| *r <= revision);
    let available = |r: u64| -> BTreeSet<Ipv4Addr> {
        status
            .workload(project, environment, workload, r, placed)
            .into_iter()
            .filter(|replica| replica.state == ReplicaState::Available)
            .filter_map(|replica| replica.pod_ip)
            .collect()
    };
    let complete = |ips: &BTreeSet<Ipv4Addr>| !placed.is_empty() && ips.len() == placed.len();

    let mut newest_serving = None;
    for r in revisions.iter().rev() {
        let ips = available(*r);
        if complete(&ips) {
            return ips;
        }
        if newest_serving.is_none() && !ips.is_empty() {
            newest_serving = Some(ips);
        }
    }
    newest_serving.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use faber_api_types::workload::ReplicaState::{Available, Deploying, Unavailable};
    use faber_network_management::fake::RecordingRunner;
    use faber_network_management::{ContainerFirewall, FirewallSpec};

    use super::*;
    use crate::apps::{EnvironmentRecord, WorkloadRecord};
    use crate::mesh::MeshState;
    use crate::mesh::state::{MeshConfig, NodeRecord};
    use crate::raft::store::place::{
        PlacementRecord, ReplicaPlacement, UID_RANGE_START, WorkloadPlacement,
    };
    use crate::raft::store::services::ServiceIps;
    use crate::raft::store::status::{Capacity, NodeStatus, ReplicaReport};

    const INTERVAL: u64 = 5;

    /// What a node reported about one replica.
    fn report(revision: u64, index: u32, state: ReplicaState, ip: &str) -> ReplicaReport {
        ReplicaReport {
            project: "shop".into(),
            environment: "staging".into(),
            workload: "web".into(),
            revision,
            replica_index: index,
            state,
            pod_ip: Some(ip.parse().unwrap()),
            detail: None,
        }
    }

    fn status(reported_at: u64, replicas: Vec<ReplicaReport>) -> NodeStatus {
        NodeStatus {
            reported_at,
            capacity: Capacity::default(),
            replicas,
        }
    }

    fn node(id: u64) -> NodeRecord {
        NodeRecord {
            node_id: id,
            public_key: "k".into(),
            mesh_ip: format!("10.99.0.{id}").parse().unwrap(),
            endpoint: None,
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: format!("n{id}"),
            lease_renewed_at: 0,
        }
    }

    /// A cluster of nodes 1..=3 at lease clock `clock` with the workload
    /// `web` at `revision`, exposing the given ports, placed on `placed`
    /// (the node of each replica, in order), and the reports of the nodes.
    struct Cluster {
        view: PlacementView,
    }

    impl Cluster {
        fn new(revision: u64, ports: &[(u16, &str)], placed: &[u64]) -> Self {
            let ports: Vec<_> = ports
                .iter()
                .map(|(p, proto)| serde_json::json!({"port": p, "protocol": proto}))
                .collect();
            let spec = serde_json::from_value(serde_json::json!({
                "containers": [{"name": "app", "image": "nginx", "ports": ports}]
            }))
            .unwrap();
            let mut mesh = MeshState::init(MeshConfig::default(), node(1)).unwrap();
            mesh.nodes = (1..=3).map(|id| (id, node(id))).collect();
            mesh.clock = 100;
            let environment = EnvironmentRecord {
                generation: 1,
                updated_at: 0,
                workloads: BTreeMap::from([(
                    "web".to_string(),
                    WorkloadRecord {
                        revision,
                        spec,
                        env: BTreeMap::new(),
                    },
                )]),
            };
            let replicas = placed
                .iter()
                .enumerate()
                .map(|(i, n)| ReplicaPlacement::on(i as u32, *n))
                .collect();
            let record = PlacementRecord {
                generation: 1,
                basis: 0,
                workloads: BTreeMap::from([(
                    "web".to_string(),
                    WorkloadPlacement {
                        uid_base: UID_RANGE_START,
                        replicas,
                    },
                )]),
            };
            let mut ips = ServiceIps::default();
            ips.allocate("10.201.0.0/16".parse().unwrap(), "shop/staging/web");
            Self {
                view: PlacementView {
                    applied: 10,
                    mesh: Some(mesh),
                    environments: vec![("shop".into(), "staging".into(), environment)],
                    statuses: BTreeMap::new(),
                    records: BTreeMap::from([(("shop".into(), "staging".into()), record)]),
                    service_ips: ips,
                },
            }
        }

        fn reported(mut self, node: u64, at: u64, replicas: Vec<ReplicaReport>) -> Self {
            self.view.statuses.insert(node, status(at, replicas));
            self
        }

        /// The pod IPs of the backends on the first port, if any service.
        fn desired(&self) -> Vec<ServiceNat> {
            desired_services(&self.view, INTERVAL, 0)
        }

        fn ips(&self) -> Vec<String> {
            let desired = self.desired();
            let mut ips: Vec<String> = desired
                .iter()
                .flat_map(|s| &s.backends)
                .map(|b| b.ip.to_string())
                .collect();
            ips.dedup();
            ips
        }
    }

    const HTTP: &[(u16, &str)] = &[(80, "tcp")];

    #[test]
    fn only_available_replicas_become_backends() {
        let c = Cluster::new(1, HTTP, &[1, 2, 3, 1]).reported(
            1,
            100,
            vec![
                report(1, 0, Available, "10.200.1.2"),
                report(1, 3, Deploying, "10.200.1.9"),
            ],
        );
        let c = c.reported(2, 100, vec![report(1, 1, Unavailable, "10.200.2.2")]);
        let c = c.reported(3, 100, vec![report(1, 2, Available, "10.200.3.2")]);
        assert_eq!(c.ips(), ["10.200.1.2", "10.200.3.2"]);
        assert_eq!(
            c.desired(),
            [ServiceNat {
                vip: "10.201.0.1".parse().unwrap(),
                backends: vec![
                    NatBackend {
                        ip: "10.200.1.2".parse().unwrap(),
                        port: 80,
                        protocol: NatProtocol::Tcp
                    },
                    NatBackend {
                        ip: "10.200.3.2".parse().unwrap(),
                        port: 80,
                        protocol: NatProtocol::Tcp
                    },
                ],
            }]
        );
    }

    #[test]
    fn a_replica_without_a_pod_ip_is_not_a_backend() {
        let mut no_ip = report(1, 0, Available, "10.200.1.2");
        no_ip.pod_ip = None;
        let c = Cluster::new(1, HTTP, &[1]).reported(1, 100, vec![no_ip]);
        assert!(c.desired()[0].backends.is_empty());
    }

    #[test]
    fn a_report_from_a_node_the_replica_is_not_placed_on_is_ignored() {
        // Replica 0 is placed on node 1; node 2 claims it too.
        let c = Cluster::new(1, HTTP, &[1])
            .reported(1, 100, vec![report(1, 0, Deploying, "10.200.1.2")])
            .reported(2, 100, vec![report(1, 0, Available, "10.200.2.2")]);
        assert!(c.desired()[0].backends.is_empty());
    }

    #[test]
    fn a_node_that_has_gone_quiet_serves_nothing() {
        let c = Cluster::new(1, HTTP, &[1, 2])
            .reported(1, 100, vec![report(1, 0, Available, "10.200.1.2")])
            .reported(
                2,
                100 - 3 * INTERVAL - 1,
                vec![report(1, 1, Available, "10.200.2.2")],
            );
        assert_eq!(c.ips(), ["10.200.1.2"]);
        // On the edge it still counts.
        let c = Cluster::new(1, HTTP, &[1, 2])
            .reported(1, 100, vec![report(1, 0, Available, "10.200.1.2")])
            .reported(
                2,
                100 - 3 * INTERVAL,
                vec![report(1, 1, Available, "10.200.2.2")],
            );
        assert_eq!(c.ips(), ["10.200.1.2", "10.200.2.2"]);
        // A removed node's last report is not shown.
        let mut c = c;
        c.view.mesh.as_mut().unwrap().nodes.remove(&2);
        assert_eq!(c.ips(), ["10.200.1.2"]);
    }

    #[test]
    fn a_service_without_an_address_or_workload_has_no_entry() {
        let mut c = Cluster::new(1, HTTP, &[1]).reported(
            1,
            100,
            vec![report(1, 0, Available, "10.200.1.2")],
        );
        c.view.service_ips = ServiceIps::default();
        assert!(c.desired().is_empty());

        // An address whose workload is gone is not programmed either.
        let mut c = Cluster::new(1, HTTP, &[1]);
        c.view
            .service_ips
            .allocate("10.201.0.0/16".parse().unwrap(), "shop/staging/old");
        let vips: Vec<_> = c.desired().iter().map(|s| s.vip.to_string()).collect();
        assert_eq!(vips, ["10.201.0.1"]);
    }

    #[test]
    fn a_service_with_no_available_replica_keeps_its_entry_with_no_backends() {
        let c = Cluster::new(1, HTTP, &[1]);
        let desired = c.desired();
        assert_eq!(desired.len(), 1);
        assert!(desired[0].backends.is_empty());
    }

    #[test]
    fn every_declared_port_and_protocol_is_served_by_every_backend() {
        let c = Cluster::new(
            1,
            &[(53, "udp"), (80, "tcp"), (53, "tcp"), (80, "tcp")],
            &[1],
        )
        .reported(1, 100, vec![report(1, 0, Available, "10.200.1.2")]);
        let got: Vec<_> = c.desired()[0]
            .backends
            .iter()
            .map(|b| (b.port, b.protocol))
            .collect();
        assert_eq!(
            got,
            [
                (53, NatProtocol::Tcp),
                (53, NatProtocol::Udp),
                (80, NatProtocol::Tcp)
            ]
        );
    }

    // ---- blue/green ----

    /// Revision 2 is stored; revision 1 ran on nodes 1 to 3 and is still up.
    fn deploy(new_available: usize) -> Cluster {
        let node_report = |node: u64, new_state: Option<ReplicaState>| {
            let ip = |rev: u64| format!("10.200.{node}.{rev}");
            let mut replicas = vec![report(1, node as u32 - 1, Available, &ip(1))];
            if let Some(state) = new_state {
                replicas.push(report(2, node as u32 - 1, state, &ip(2)));
            }
            replicas
        };
        let mut c = Cluster::new(2, HTTP, &[1, 2, 3]);
        for node in 1..=3u64 {
            let new_state = match node as usize <= new_available {
                true => Some(Available),
                false if node as usize == new_available + 1 => Some(Deploying),
                false => None,
            };
            c = c.reported(node, 100, node_report(node, new_state));
        }
        c
    }

    #[test]
    fn the_old_revision_serves_until_the_new_one_has_every_replica() {
        for new_available in 0..=2 {
            assert_eq!(
                deploy(new_available).ips(),
                ["10.200.1.1", "10.200.2.1", "10.200.3.1"],
                "{new_available} of 3 new replicas available"
            );
        }
    }

    #[test]
    fn the_map_switches_when_the_new_revision_qualifies() {
        assert_eq!(deploy(3).ips(), ["10.200.1.2", "10.200.2.2", "10.200.3.2"]);
    }

    #[test]
    fn the_switch_does_not_depend_on_the_old_revision_going_away() {
        // The old replicas stop after the switch; the new ones keep serving.
        let c = Cluster::new(2, HTTP, &[1, 2, 3])
            .reported(1, 100, vec![report(2, 0, Available, "10.200.1.2")])
            .reported(2, 100, vec![report(2, 1, Available, "10.200.2.2")])
            .reported(3, 100, vec![report(2, 2, Available, "10.200.3.2")]);
        assert_eq!(c.ips(), ["10.200.1.2", "10.200.2.2", "10.200.3.2"]);
    }

    #[test]
    fn a_running_revision_keeps_serving_when_one_replica_fails() {
        // The old revision is gone. The new one lost a replica: it serves
        // from the two that are up instead of leaving the service empty.
        let c = Cluster::new(2, HTTP, &[1, 2, 3])
            .reported(1, 100, vec![report(2, 0, Available, "10.200.1.2")])
            .reported(2, 100, vec![report(2, 1, Unavailable, "10.200.2.2")])
            .reported(3, 100, vec![report(2, 2, Available, "10.200.3.2")]);
        assert_eq!(c.ips(), ["10.200.1.2", "10.200.3.2"]);
    }

    #[test]
    fn with_no_complete_revision_the_newest_serving_one_is_used() {
        // Old: 2 of 3 up. New: 1 of 3 up. Neither is complete.
        let c = Cluster::new(2, HTTP, &[1, 2, 3])
            .reported(
                1,
                100,
                vec![
                    report(1, 0, Available, "10.200.1.1"),
                    report(2, 0, Available, "10.200.1.2"),
                ],
            )
            .reported(2, 100, vec![report(1, 1, Available, "10.200.2.1")])
            .reported(3, 100, vec![report(1, 2, Unavailable, "10.200.3.1")]);
        assert_eq!(c.ips(), ["10.200.1.2"]);
    }

    #[test]
    fn a_replica_that_cannot_be_placed_leaves_no_revision_complete() {
        let mut c = deploy(3);
        let record = c.view.records.values_mut().next().unwrap();
        record
            .workloads
            .get_mut("web")
            .unwrap()
            .replicas
            .push(ReplicaPlacement::unplaced(3, "insufficient_capacity"));
        // The three new replicas are not all four: the old revision, which
        // is not complete either, would be the only fallback if the new one
        // had no replica up. Here the new one has, and is newest.
        assert_eq!(c.ips(), ["10.200.1.2", "10.200.2.2", "10.200.3.2"]);
        // Take the new replicas down: the old revision serves.
        let mut c = Cluster::new(2, HTTP, &[1, 2, 3, 1]);
        c.view
            .records
            .values_mut()
            .next()
            .unwrap()
            .workloads
            .get_mut("web")
            .unwrap()
            .replicas[3] = ReplicaPlacement::unplaced(3, "insufficient_capacity");
        let c = c
            .reported(1, 100, vec![report(1, 0, Available, "10.200.1.1")])
            .reported(2, 100, vec![report(1, 1, Available, "10.200.2.1")]);
        assert_eq!(c.ips(), ["10.200.1.1", "10.200.2.1"]);
    }

    #[test]
    fn a_report_of_a_newer_revision_than_the_stored_one_is_ignored() {
        let c = Cluster::new(1, HTTP, &[1]).reported(
            1,
            100,
            vec![
                report(1, 0, Available, "10.200.1.1"),
                report(5, 0, Available, "10.200.1.5"),
            ],
        );
        assert_eq!(c.ips(), ["10.200.1.1"]);
    }

    // ---- stability ----

    #[test]
    fn a_status_tick_that_changes_nothing_gives_an_equal_desired() {
        let first = deploy(3);
        let mut again = deploy(3);
        // Every node reports again: later, with other details.
        for status in again.view.statuses.values_mut() {
            status.reported_at += 3;
            for r in &mut status.replicas {
                r.detail = Some("probe ok".into());
            }
        }
        again.view.applied += 7;
        again.view.mesh.as_mut().unwrap().clock += 3;
        assert_eq!(first.desired(), again.desired());
    }

    #[test]
    fn the_desired_value_changes_when_a_replica_does() {
        let before = deploy(3).desired();
        let mut c = deploy(3);
        c.view.statuses.get_mut(&2).unwrap().replicas[1].state = Unavailable;
        let after = c.desired();
        assert_ne!(before, after);
        // Only the one replica left: the old revision is complete again.
        assert_eq!(c.ips(), ["10.200.1.1", "10.200.2.1", "10.200.3.1"]);
    }

    #[test]
    fn nothing_is_desired_without_a_mesh_or_a_view() {
        let mut c = Cluster::new(1, HTTP, &[1]);
        c.view.mesh = None;
        assert!(c.desired().is_empty());
    }

    // ---- the step ----

    #[test]
    fn the_step_plans_from_the_view_and_names_itself() {
        let step = ServicesStep::new(
            NetworkManager::new(Arc::new(RecordingRunner::new())),
            LeaseTiming::default(),
        );
        assert_eq!(step.name(), "services");
        let view = ReplicatedStateView {
            mesh: None,
            containerd: None,
            placement: Some(
                Cluster::new(1, HTTP, &[1])
                    .reported(1, 100, vec![report(1, 0, Available, "10.200.1.2")])
                    .view,
            ),
            status_interval_secs: INTERVAL,
            now: 0,
        };
        let planned = step.plan(&view);
        assert_eq!(planned.desired.len(), 1);
        assert_eq!(planned.desired[0].backends.len(), 1);
        assert!(planned.warnings.is_empty());

        let empty = ReplicatedStateView {
            placement: None,
            ..view
        };
        assert!(step.plan(&empty).desired.is_empty());
    }

    #[tokio::test]
    async fn the_step_loads_the_rules_into_the_table_and_a_network_reload_keeps_them() {
        let runner = Arc::new(RecordingRunner::new());
        let network = NetworkManager::new(runner.clone());
        let step = ServicesStep::new(network.clone(), LeaseTiming::default());
        let spec = faber_network_management::NetworkSpec {
            interface: "faber0".into(),
            wireguard: faber_network_management::WireGuardSpec {
                private_key: "k".into(),
                listen_port: 51820,
                address: "10.99.0.1/16".parse().unwrap(),
                peers: vec![],
                routes: vec![],
            },
            firewall: FirewallSpec {
                wg_port: 51820,
                peer_port: 7401,
                peer_endpoint_ips: Default::default(),
                peer_mesh_ips: Default::default(),
                containers: Some(ContainerFirewall {
                    cidr: "10.200.0.0/16".parse().unwrap(),
                    local_subnet: Some("10.200.1.0/24".parse().unwrap()),
                    node_mesh_ips: Default::default(),
                    bridge: "faberbr0".into(),
                }),
            },
        };
        network.reconcile(&spec).await.unwrap();

        let desired = Cluster::new(1, HTTP, &[1])
            .reported(1, 100, vec![report(1, 0, Available, "10.200.1.2")])
            .desired();
        step.apply(&desired).await.unwrap();
        // The network step runs again, as after any peer change.
        network.reconcile(&spec).await.unwrap();

        let scripts: Vec<String> = runner
            .calls()
            .into_iter()
            .filter(|c| c.cmdline() == "nft -f -")
            .map(|c| String::from_utf8(c.stdin.unwrap()).unwrap())
            .collect();
        assert_eq!(scripts.len(), 3);
        let rule = "ip daddr 10.201.0.1 tcp dport 80 dnat ip to numgen random mod 1 map { 0 : 10.200.1.2 }";
        assert!(!scripts[0].contains(rule));
        assert!(scripts[1].contains(rule), "{}", scripts[1]);
        assert!(scripts[2].contains(rule), "{}", scripts[2]);
    }
}
