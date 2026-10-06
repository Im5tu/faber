//! Raft type configuration: the node descriptor and the log payloads.

use std::collections::BTreeMap;
use std::fmt;
// `declare_raft_types!` refers to `Cursor` unqualified for the snapshot type.
use std::io::Cursor;
use std::net::Ipv4Addr;

use faber_api_types::workload::WorkloadSpec;
use faber_container_runtime::ContainerRuntimeSpec;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::apps::Applied;
use crate::mesh::{Allocation, MeshConfig, NodeRecord, Rejection};

pub type NodeId = u64;

/// A cluster member as stored in the Raft membership config.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// `host:port` serving both `/raft/*` and `/api/v1`.
    pub addr: String,
}

impl Node {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.addr)
    }
}

/// A state machine command carried in a log entry.
///
/// Mesh commands carry `at` (Unix seconds) stamped by the leader when it
/// proposes them; `apply` never reads the clock. The lease commands carry no
/// time: leases run on the logical clock in `MeshState`, which only
/// `AdvanceClock` moves. All nodes must run the same fabercp version, since
/// an older build cannot decode newer variants.
///
/// The project and workload commands need no mesh. Those that create or
/// change an environment carry `at` too. `Debug` of a workload spec prints
/// env names only, so a logged command shows no env value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Request {
    /// Generic key/value write, not exposed over REST or MCP. Tests drive it
    /// through `Raft::client_write`. Rejected for `mesh::state::RESERVED_KEYS`
    /// and for the keys of `apps::state::is_reserved_key`.
    Set { key: String, value: Value },
    /// Seeds `mesh` and `config/containerd`. Rejected with
    /// `already_initialized` once `mesh` exists, and with `bad_request` for
    /// a bad founder, founder name or container range. `apply` allocates
    /// the founder's container subnet and ignores `founder.container_subnet`
    /// and `founder.lease_renewed_at`; an empty `founder.name` becomes
    /// `node-1`. See `MeshState::init`.
    InitCluster {
        mesh: MeshConfig,
        founder: NodeRecord,
        containerd: ContainerRuntimeSpec,
    },
    /// Reserves a node ID, mesh IP and name for a new node, and drops an
    /// older live invite for the same name. Answers [`Response::Invited`],
    /// which names the node the invite will replace when a node holds the
    /// name. Rejected with `bad_request` for a name that is not a DNS label,
    /// and with `mesh_exhausted` when the mesh CIDR or the container range
    /// has no room for another node. See `MeshState::create_invite`.
    CreateInvite {
        invite_id: String,
        /// The new node's WireGuard public key; the private key never enters
        /// the log.
        public_key: String,
        /// The new node's public WireGuard endpoint.
        endpoint: String,
        /// The name the new node will hold. Required: a command logged
        /// before the field existed decodes with an empty name and is
        /// rejected with `bad_request`.
        #[serde(default)]
        name: String,
        ttl_secs: u64,
        at: u64,
    },
    /// Turns an invite into a node, starts its lease and allocates its
    /// container subnet. When another node holds the invite's name, that
    /// node's record is deleted and its subnet goes to the new node;
    /// rejected with `not_ready` while that node is in the raft membership.
    /// See `MeshState::register_node`.
    RegisterNode {
        invite_id: String,
        node_id: u64,
        endpoint: String,
        /// The machine that registers: the random id in its `identity.json`.
        /// Empty in commands logged before the field existed.
        #[serde(default)]
        instance_id: String,
        at: u64,
    },
    /// Replaces a node's WireGuard endpoint.
    SetEndpoint { node_id: u64, endpoint: String },
    /// Moves the lease clock forward by the time the leader measured since
    /// its last tick, by at most a tenth of the lease duration. Never
    /// rejected. See `MeshState::advance_clock`.
    AdvanceClock { elapsed_secs: u64 },
    /// Restarts a node's lease, for the machine `instance_id`. Rejected
    /// with `mesh_node_removed` for an unknown node, with
    /// `mesh_lease_expired` for a lease that has already expired, and with
    /// `forbidden` for another machine. See `MeshState::renew_lease`.
    RenewLease { node_id: u64, instance_id: String },
    /// Deletes a node's mesh record, freeing its mesh IP, subnet and name.
    /// Rejected with `not_ready` while the node's lease is live or the node
    /// is in the raft membership; an unknown node is not an error. See
    /// `MeshState::remove_node`.
    RemoveNode { node_id: u64 },
    /// Changes the lease duration and restarts every node's lease. Rejected
    /// with `bad_request` outside 300 to 2592000 seconds. See
    /// `MeshState::set_lease`.
    SetLease { secs: u64 },
    /// Adds a project with these environments, all empty. Rejected with
    /// `bad_request` for a name that is not a DNS label, an environment
    /// named twice, no environment or more than a project may have, with
    /// `project_exists`, and with `workloads_exhausted` at the limit of
    /// projects. See `ProjectIndex::create_project`.
    CreateProject {
        name: String,
        environments: Vec<String>,
        at: u64,
    },
    /// Removes a project and its environments. Rejected with `not_found`,
    /// and with `not_empty` while an environment holds a workload. See
    /// `ProjectIndex::delete_project`.
    DeleteProject { name: String },
    /// Adds the empty environment `name` to `project`. Rejected with
    /// `not_found` for an unknown project, `bad_request` for a bad name,
    /// `project_exists` for an environment the project has, and
    /// `workloads_exhausted` at the limit. See
    /// `ProjectIndex::add_environment`.
    AddEnvironment {
        project: String,
        name: String,
        at: u64,
    },
    /// Removes the environment `name` of `project`. Rejected with
    /// `not_found`, and with `not_empty` while it holds a workload. See
    /// `ProjectIndex::remove_environment`.
    RemoveEnvironment { project: String, name: String },
    /// Creates or updates each of `workloads` in one environment, and with
    /// `prune` removes the others. Answers [`Response::Applied`]. Writes
    /// nothing when nothing changed. Rejected with `not_found` for an
    /// unknown project or environment, `bad_request` for a bad name or
    /// size, and `workloads_exhausted` at a limit. See
    /// `ProjectIndex::apply_workloads`.
    ApplyWorkloads {
        project: String,
        environment: String,
        /// The workloads by name.
        workloads: BTreeMap<String, WorkloadSpec>,
        prune: bool,
        at: u64,
    },
    /// Removes the workloads `names` from one environment. Rejected with
    /// `not_found`. See `ProjectIndex::remove_workloads`.
    RemoveWorkloads {
        project: String,
        environment: String,
        names: Vec<String>,
        at: u64,
    },
}

/// Result of applying one log entry.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// Blank and membership entries, and mesh commands with nothing to report.
    #[default]
    None,
    /// `Set`: the value the key held before this entry was applied.
    Previous(Option<Value>),
    /// `CreateInvite` succeeded.
    Invited {
        node_id: u64,
        mesh_ip: Ipv4Addr,
        expires_at: u64,
        /// The name the invite carries.
        #[serde(default)]
        name: String,
        /// The node that holds the name now and will be replaced, if any.
        #[serde(default)]
        replaces: Option<u64>,
    },
    /// The command was refused; `code` is one of
    /// `faber_api_types::error_code`. The entry still counts as applied.
    Rejected { code: String, message: String },
    /// `ApplyWorkloads` succeeded. Every workload of the command is in
    /// exactly one of `created`, `updated` and `unchanged`.
    Applied {
        /// The environment's generation after the command: the index of
        /// this entry when it changed anything, else what it was before.
        generation: u64,
        created: Vec<String>,
        updated: Vec<String>,
        unchanged: Vec<String>,
        /// Workloads removed by `prune`.
        removed: Vec<String>,
    },
}

impl From<Rejection> for Response {
    fn from(r: Rejection) -> Self {
        Response::Rejected {
            code: r.code,
            message: r.message,
        }
    }
}

impl From<Allocation> for Response {
    fn from(a: Allocation) -> Self {
        Response::Invited {
            node_id: a.node_id,
            mesh_ip: a.mesh_ip,
            expires_at: a.expires_at,
            name: a.name,
            replaces: a.replaces,
        }
    }
}

impl From<Applied> for Response {
    fn from(a: Applied) -> Self {
        Response::Applied {
            generation: a.generation,
            created: a.created,
            updated: a.updated,
            unchanged: a.unchanged,
            removed: a.removed,
        }
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = Node,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;
pub type StoredMembership = openraft::StoredMembership<NodeId, Node>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, Node>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type StorageError = openraft::StorageError<NodeId>;
pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn node_displays_its_address() {
        assert_eq!(Node::new("10.0.0.1:7400").to_string(), "10.0.0.1:7400");
    }

    #[test]
    fn request_round_trips_through_json() {
        let req = Request::Set {
            key: "a/b".into(),
            value: json!({"x": [1, 2]}),
        };
        let bytes = serde_json::to_vec(&req).unwrap();
        assert_eq!(serde_json::from_slice::<Request>(&bytes).unwrap(), req);
    }

    #[test]
    fn mesh_requests_round_trip_through_json() {
        let founder = NodeRecord {
            node_id: 1,
            public_key: "pk1".into(),
            mesh_ip: "10.99.0.1".parse().unwrap(),
            endpoint: Some("203.0.113.1:51820".into()),
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: "node-1".into(),
            lease_renewed_at: 3,
        };
        for req in [
            Request::InitCluster {
                mesh: MeshConfig::default(),
                founder,
                containerd: ContainerRuntimeSpec::default(),
            },
            Request::CreateInvite {
                invite_id: "ab".repeat(16),
                public_key: "pk2".into(),
                endpoint: "198.51.100.2:51820".into(),
                name: "node-2".into(),
                ttl_secs: 3600,
                at: 1_000,
            },
            Request::RegisterNode {
                invite_id: "ab".repeat(16),
                node_id: 2,
                endpoint: "198.51.100.2:51820".into(),
                instance_id: "cd".repeat(16),
                at: 1_001,
            },
            Request::SetEndpoint {
                node_id: 2,
                endpoint: "198.51.100.9:51820".into(),
            },
            Request::AdvanceClock { elapsed_secs: 30 },
            Request::RenewLease {
                node_id: 2,
                instance_id: "cd".repeat(16),
            },
            Request::RemoveNode { node_id: 2 },
            Request::SetLease { secs: 600 },
        ] {
            let bytes = serde_json::to_vec(&req).unwrap();
            assert_eq!(serde_json::from_slice::<Request>(&bytes).unwrap(), req);
        }
    }

    #[test]
    fn lease_requests_have_pinned_shapes() {
        for (req, want) in [
            (
                Request::AdvanceClock { elapsed_secs: 30 },
                json!({"AdvanceClock": {"elapsed_secs": 30}}),
            ),
            (
                Request::RenewLease {
                    node_id: 2,
                    instance_id: "cd".repeat(16),
                },
                json!({"RenewLease": {"node_id": 2, "instance_id": "cd".repeat(16)}}),
            ),
            (
                Request::RemoveNode { node_id: 2 },
                json!({"RemoveNode": {"node_id": 2}}),
            ),
            (
                Request::SetLease { secs: 600 },
                json!({"SetLease": {"secs": 600}}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&req).unwrap(), want);
        }
    }

    /// A `CreateInvite` logged before `name` existed still decodes.
    #[test]
    fn create_invite_without_a_name_decodes() {
        let old = json!({"CreateInvite": {
            "invite_id": "ab".repeat(16), "public_key": "pk2",
            "endpoint": "198.51.100.2:51820", "ttl_secs": 3600, "at": 1_000
        }});
        assert_eq!(
            serde_json::from_value::<Request>(old).unwrap(),
            Request::CreateInvite {
                invite_id: "ab".repeat(16),
                public_key: "pk2".into(),
                endpoint: "198.51.100.2:51820".into(),
                name: String::new(),
                ttl_secs: 3600,
                at: 1_000,
            }
        );
    }

    /// An `Invited` stored before `name` and `replaces` existed still decodes.
    #[test]
    fn invited_without_a_name_decodes() {
        let old = json!({"Invited": {
            "node_id": 2, "mesh_ip": "10.99.0.2", "expires_at": 99
        }});
        assert_eq!(
            serde_json::from_value::<Response>(old).unwrap(),
            Response::Invited {
                node_id: 2,
                mesh_ip: "10.99.0.2".parse().unwrap(),
                expires_at: 99,
                name: String::new(),
                replaces: None,
            }
        );
    }

    /// A `RegisterNode` logged before `instance_id` existed still decodes.
    #[test]
    fn register_node_without_an_instance_id_decodes() {
        let old = json!({"RegisterNode": {
            "invite_id": "ab".repeat(16), "node_id": 2,
            "endpoint": "198.51.100.2:51820", "at": 1_001
        }});
        assert_eq!(
            serde_json::from_value::<Request>(old).unwrap(),
            Request::RegisterNode {
                invite_id: "ab".repeat(16),
                node_id: 2,
                endpoint: "198.51.100.2:51820".into(),
                instance_id: String::new(),
                at: 1_001,
            }
        );
    }

    /// An `InitCluster` logged before the container fields existed decodes
    /// with no container range and no founder subnet.
    #[test]
    fn init_cluster_without_container_fields_decodes() {
        let old = json!({"InitCluster": {
            "mesh": {"cidr": "10.99.0.0/16", "peer_port": 7401, "wg_port": 51820},
            "founder": {
                "node_id": 1, "public_key": "pk1", "mesh_ip": "10.99.0.1",
                "endpoint": "203.0.113.1:51820", "invite_id": null
            },
            "containerd": ContainerRuntimeSpec::default(),
        }});
        let Request::InitCluster { mesh, founder, .. } =
            serde_json::from_value::<Request>(old).unwrap()
        else {
            panic!("not an InitCluster");
        };
        assert_eq!(mesh, MeshConfig::default());
        assert_eq!(mesh.container_cidr, None);
        assert_eq!(founder.container_subnet, None);
    }

    #[test]
    fn init_cluster_with_a_container_range_round_trips() {
        let req = Request::InitCluster {
            mesh: MeshConfig {
                container_cidr: Some("10.200.0.0/16".parse().unwrap()),
                container_subnet_prefix: 26,
                ..MeshConfig::default()
            },
            founder: NodeRecord {
                node_id: 1,
                public_key: "pk1".into(),
                mesh_ip: "10.99.0.1".parse().unwrap(),
                endpoint: None,
                invite_id: None,
                instance_id: None,
                container_subnet: Some("10.200.0.0/26".parse().unwrap()),
                name: String::new(),
                lease_renewed_at: 0,
            },
            containerd: ContainerRuntimeSpec::default(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["InitCluster"]["mesh"]["container_cidr"], "10.200.0.0/16");
        assert_eq!(v["InitCluster"]["mesh"]["container_subnet_prefix"], 26);
        assert_eq!(
            v["InitCluster"]["founder"]["container_subnet"],
            "10.200.0.0/26"
        );
        assert_eq!(serde_json::from_value::<Request>(v).unwrap(), req);
    }

    #[test]
    fn response_round_trips_through_json() {
        for resp in [
            Response::None,
            Response::Previous(None),
            Response::Previous(Some(json!({"a": 1}))),
            Response::Invited {
                node_id: 2,
                mesh_ip: "10.99.0.2".parse().unwrap(),
                expires_at: 99,
                name: "node-2".into(),
                replaces: Some(5),
            },
            Response::Rejected {
                code: "invite_used".into(),
                message: "used".into(),
            },
        ] {
            let bytes = serde_json::to_vec(&resp).unwrap();
            assert_eq!(serde_json::from_slice::<Response>(&bytes).unwrap(), resp);
        }
        assert_eq!(Response::default(), Response::None);
    }

    #[test]
    fn response_from_rejection_and_allocation() {
        assert_eq!(
            Response::from(Rejection::new("invite_expired", "expired")),
            Response::Rejected {
                code: "invite_expired".into(),
                message: "expired".into()
            }
        );
        let a = Allocation {
            node_id: 3,
            mesh_ip: "10.99.0.3".parse().unwrap(),
            expires_at: 7,
            name: "node-3".into(),
            replaces: Some(2),
        };
        assert_eq!(
            Response::from(a.clone()),
            Response::Invited {
                node_id: 3,
                mesh_ip: a.mesh_ip,
                expires_at: 7,
                name: "node-3".into(),
                replaces: Some(2),
            }
        );
    }

    fn web_spec() -> WorkloadSpec {
        serde_json::from_value(json!({"containers": [{"name": "web", "image": "nginx"}]})).unwrap()
    }

    /// The wire format of the project and workload commands.
    #[test]
    fn app_requests_have_pinned_shapes() {
        for (req, want) in [
            (
                Request::CreateProject {
                    name: "shop".into(),
                    environments: vec!["staging".into(), "production".into()],
                    at: 1_000,
                },
                json!({"CreateProject": {
                    "name": "shop", "environments": ["staging", "production"], "at": 1_000
                }}),
            ),
            (
                Request::DeleteProject {
                    name: "shop".into(),
                },
                json!({"DeleteProject": {"name": "shop"}}),
            ),
            (
                Request::AddEnvironment {
                    project: "shop".into(),
                    name: "dev".into(),
                    at: 1_001,
                },
                json!({"AddEnvironment": {"project": "shop", "name": "dev", "at": 1_001}}),
            ),
            (
                Request::RemoveEnvironment {
                    project: "shop".into(),
                    name: "dev".into(),
                },
                json!({"RemoveEnvironment": {"project": "shop", "name": "dev"}}),
            ),
            (
                Request::ApplyWorkloads {
                    project: "shop".into(),
                    environment: "staging".into(),
                    workloads: BTreeMap::from([("web".to_string(), web_spec())]),
                    prune: true,
                    at: 1_002,
                },
                json!({"ApplyWorkloads": {
                    "project": "shop", "environment": "staging",
                    "workloads": {"web": {
                        "scale": {"replicas": 1},
                        "restart": "always",
                        "expose": "environment",
                        "placement": {},
                        "volumes": {},
                        "containers": [{
                            "name": "web", "image": "nginx", "command": [], "args": [],
                            "env": {}, "ports": [], "mounts": [],
                            "resources": {"limits": {}, "requests": {}},
                            "capabilities": [], "read_only_root_fs": false
                        }]
                    }},
                    "prune": true, "at": 1_002
                }}),
            ),
            (
                Request::RemoveWorkloads {
                    project: "shop".into(),
                    environment: "staging".into(),
                    names: vec!["web".into(), "db".into()],
                    at: 1_003,
                },
                json!({"RemoveWorkloads": {
                    "project": "shop", "environment": "staging",
                    "names": ["web", "db"], "at": 1_003
                }}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&req).unwrap(), want);
            assert_eq!(serde_json::from_value::<Request>(want).unwrap(), req);
        }
    }

    /// Every field of the project and workload commands is required: none
    /// existed before, so there is no older shape to decode.
    #[test]
    fn app_requests_have_no_optional_fields() {
        for partial in [
            json!({"CreateProject": {"name": "shop", "environments": []}}),
            json!({"CreateProject": {"name": "shop", "at": 1}}),
            json!({"AddEnvironment": {"project": "shop", "name": "dev"}}),
            json!({"ApplyWorkloads": {
                "project": "shop", "environment": "staging", "workloads": {}, "at": 1
            }}),
            json!({"RemoveWorkloads": {
                "project": "shop", "environment": "staging", "names": []
            }}),
        ] {
            assert!(
                serde_json::from_value::<Request>(partial.clone()).is_err(),
                "{partial}"
            );
        }
    }

    #[test]
    fn applied_has_a_pinned_shape() {
        let resp = Response::Applied {
            generation: 9,
            created: vec!["web".into()],
            updated: vec!["api".into()],
            unchanged: vec!["db".into()],
            removed: vec!["old".into()],
        };
        let want = json!({"Applied": {
            "generation": 9, "created": ["web"], "updated": ["api"],
            "unchanged": ["db"], "removed": ["old"]
        }});
        assert_eq!(serde_json::to_value(&resp).unwrap(), want);
        assert_eq!(serde_json::from_value::<Response>(want).unwrap(), resp);
    }

    #[test]
    fn response_from_applied() {
        let a = Applied {
            generation: 9,
            created: vec!["web".into()],
            updated: vec![],
            unchanged: vec!["db".into()],
            removed: vec![],
        };
        assert_eq!(
            Response::from(a),
            Response::Applied {
                generation: 9,
                created: vec!["web".into()],
                updated: vec![],
                unchanged: vec!["db".into()],
                removed: vec![],
            }
        );
    }

    /// A logged command shows which env variables a workload has, never
    /// what they hold.
    #[test]
    fn debug_of_a_workload_command_hides_env_values() {
        let mut spec = web_spec();
        spec.containers[0]
            .env
            .insert("API_KEY".into(), "hunter2".into());
        let req = Request::ApplyWorkloads {
            project: "shop".into(),
            environment: "staging".into(),
            workloads: BTreeMap::from([("web".to_string(), spec)]),
            prune: false,
            at: 1,
        };
        let debug = format!("{req:?}");
        assert!(debug.contains("API_KEY"), "{debug}");
        assert!(!debug.contains("hunter2"), "{debug}");
    }

    #[test]
    fn entry_round_trips_through_json() {
        let entry = Entry {
            log_id: openraft::testing::log_id(3, 1, 7),
            payload: openraft::EntryPayload::Normal(Request::Set {
                key: "k".into(),
                value: json!(1),
            }),
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let back: Entry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.log_id, entry.log_id);
        assert!(matches!(
            back.payload,
            openraft::EntryPayload::Normal(Request::Set { .. })
        ));
    }
}
