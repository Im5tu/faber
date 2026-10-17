//! Internal node-to-node wire types, served on `/raft/cluster/*` (peer
//! listener only). They are not public API; the public `/api/v1` types live
//! in `faber_api_types`.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use faber_api_types::workload::{DeployResponse, ProjectResponse, WorkloadSpec};
use ipnet::Ipv4Net;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroizing;

use crate::mesh::TokenPeer;

/// `POST /raft/cluster/join` (internal): add `node_id` at `addr` as a voter.
///
/// In a mesh the node must have registered first
/// ([`PeerRegisterRequest`]); `invite_id` and `instance_id` must be the ones
/// it registered with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerJoinRequest {
    pub node_id: u64,
    /// The joining node's peer listener address.
    pub addr: String,
    /// Set by a node joining with an invite token. In wg mode the leader
    /// rejects a join without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invite_id: Option<String>,
    /// The joining machine's `instance_id` from its `identity.json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
}

/// `POST /raft/cluster/register` (internal, leader only; a follower forwards
/// it once): turn the invite into node `node_id`, for the machine
/// `instance_id`. The first step of a join with an invite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRegisterRequest {
    pub invite_id: String,
    pub node_id: u64,
    /// The joiner's public WireGuard endpoint; must equal the invite's.
    pub endpoint: String,
    /// 32 lowercase hex characters, made by the joining machine.
    pub instance_id: String,
}

/// A cluster key in transit: standard base64, wiped on drop. It prints as
/// `[redacted]`.
#[derive(Clone)]
pub struct WireKey(Zeroizing<String>);

impl WireKey {
    pub fn new(base64: Zeroizing<String>) -> Self {
        Self(base64)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl PartialEq for WireKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl std::fmt::Debug for WireKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WireKey([redacted])")
    }
}

impl Serialize for WireKey {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for WireKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(|s| Self(Zeroizing::new(s)))
    }
}

/// 200 body of `POST /raft/cluster/register`. The key goes only to the
/// machine the invite is bound to, over the mesh, and never into the join
/// token. Both fields are absent when this node does not release a key.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PeerRegisterResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_key: Option<WireKey>,
    /// The id of the cluster key recorded in raft, if one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
}

/// `POST /raft/cluster/invites` (internal, leader only; a follower forwards it
/// once, and only a registered node may send it). The receiving node
/// generated the keypair; only the public key is sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInviteRequest {
    pub invite_id: String,
    pub public_key: String,
    /// The new node's public WireGuard endpoint.
    pub endpoint: String,
    /// The name the new node will hold.
    pub name: String,
    pub ttl_secs: u64,
}

/// 201 body of `POST /raft/cluster/invites`: everything the token needs
/// except the private key, which never leaves the node that generated it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInviteResponse {
    pub node_id: u64,
    pub mesh_ip: Ipv4Addr,
    pub expires_at: u64,
    pub mesh_cidr: Ipv4Net,
    pub peers: Vec<TokenPeer>,
    /// Peer listener addresses, leader first.
    pub join_addrs: Vec<String>,
    /// The name the new node will hold.
    pub name: String,
    /// The node that holds `name` now and will be replaced, if any.
    #[serde(default)]
    pub replaces: Option<u64>,
}

/// `POST /raft/cluster/endpoint` (internal, leader only; a follower forwards
/// it once): replace `node_id`'s replicated WireGuard endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerEndpointRequest {
    pub node_id: u64,
    /// `ip:port`.
    pub endpoint: String,
}

/// `POST /raft/cluster/renew` (internal, leader only; a follower forwards it
/// once): restart the lease of `node_id`, for the machine `instance_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRenewRequest {
    pub node_id: u64,
    /// The renewing machine's `instance_id` from its `identity.json`.
    pub instance_id: String,
}

/// `POST /raft/cluster/lease` (internal, leader only; a follower forwards it
/// once, and only a registered node may send it): set the lease duration of
/// the whole mesh.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerLeaseRequest {
    pub lease_secs: u64,
}

/// `POST /raft/cluster/status-interval` (internal, leader only; a follower
/// forwards it once, and only a registered node may send it): set how often
/// nodes report their status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStatusIntervalRequest {
    pub interval_secs: u64,
}

/// `POST /raft/cluster/workloads` (internal, leader only; a follower forwards
/// it once, and only a registered node may send it): one of the six project
/// and workload writes, named by `op`. Each variant is its raft command
/// without `at`, which the leader stamps.
///
/// The names and the specs were validated by the node that forwards. The
/// leader validates them again, because it trusts no other node's check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PeerWorkloadRequest {
    CreateProject {
        name: String,
        /// The environments to create; the forwarding node has already
        /// filled in the defaults.
        environments: Vec<String>,
    },
    DeleteProject {
        name: String,
    },
    AddEnvironment {
        project: String,
        name: String,
    },
    RemoveEnvironment {
        project: String,
        name: String,
    },
    ApplyWorkloads {
        project: String,
        environment: String,
        workloads: BTreeMap<String, WorkloadSpec>,
        prune: bool,
    },
    RemoveWorkloads {
        project: String,
        environment: String,
        names: Vec<String>,
    },
}

/// 200 body of `POST /raft/cluster/workloads`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerWorkloadResponse {
    /// The raft log index of the entry that carried the command. The
    /// forwarding node waits until it has applied this index, so that a
    /// read on it sees the write. A deploy that would change nothing is
    /// not written: `index` is then the generation of the environment,
    /// which is the entry that last changed it.
    pub index: u64,
    /// What `ApplyWorkloads` did. Absent for the other five writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied: Option<DeployResponse>,
    /// The project after `CreateProject` and `AddEnvironment`, as the
    /// leader had it once it had applied the write. The forwarding node
    /// answers its client with it, whatever it has applied itself. Absent
    /// for the other four writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectResponse>,
}

/// `GET /raft/cluster/status` (internal, leader only): replication progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub leader_id: u64,
    pub last_log_index: Option<u64>,
    /// Matched log index per member.
    pub matched: BTreeMap<u64, Option<u64>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_join_request_invite_fields_are_optional() {
        let plain = PeerJoinRequest {
            node_id: 2,
            addr: "10.99.0.2:7401".into(),
            invite_id: None,
            instance_id: None,
        };
        assert_eq!(
            serde_json::to_value(&plain).unwrap(),
            serde_json::json!({"node_id": 2, "addr": "10.99.0.2:7401"})
        );
        let back: PeerJoinRequest =
            serde_json::from_value(serde_json::json!({"node_id": 2, "addr": "10.99.0.2:7401"}))
                .unwrap();
        assert_eq!(back, plain);

        let invited = PeerJoinRequest {
            invite_id: Some("ab".repeat(16)),
            instance_id: Some("cd".repeat(16)),
            ..plain
        };
        let json = serde_json::to_string(&invited).unwrap();
        assert_eq!(
            serde_json::from_str::<PeerJoinRequest>(&json).unwrap(),
            invited
        );
    }

    #[test]
    fn peer_register_request_shape() {
        let r = PeerRegisterRequest {
            invite_id: "ab".repeat(16),
            node_id: 2,
            endpoint: "198.51.100.2:51820".into(),
            instance_id: "cd".repeat(16),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"invite_id": "ab".repeat(16), "node_id": 2,
                               "endpoint": "198.51.100.2:51820",
                               "instance_id": "cd".repeat(16)})
        );
        assert_eq!(serde_json::from_value::<PeerRegisterRequest>(v).unwrap(), r);
        // Every field is required.
        let partial = serde_json::json!({"invite_id": "ab".repeat(16), "node_id": 2,
                                         "endpoint": "198.51.100.2:51820"});
        assert!(serde_json::from_value::<PeerRegisterRequest>(partial).is_err());
    }

    #[test]
    fn a_wire_key_never_prints() {
        let key = faber_crypto::ClusterKey::generate().unwrap();
        let text = key.to_base64();
        let resp = PeerRegisterResponse {
            cluster_key: Some(WireKey::new(key.to_base64())),
            key_id: Some("ab".into()),
        };
        assert!(!format!("{resp:?}").contains(text.as_str()));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(text.as_str()));
        assert_eq!(
            serde_json::from_str::<PeerRegisterResponse>(&json).unwrap(),
            resp
        );
        assert_eq!(
            serde_json::to_string(&PeerRegisterResponse::default()).unwrap(),
            "{}"
        );
    }

    #[test]
    fn peer_invite_request_shape() {
        let r = PeerInviteRequest {
            invite_id: "ab".repeat(16),
            public_key: "pk".into(),
            endpoint: "198.51.100.2:51820".into(),
            name: "node-2".into(),
            ttl_secs: 3600,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"invite_id": "ab".repeat(16), "public_key": "pk",
                               "endpoint": "198.51.100.2:51820", "name": "node-2",
                               "ttl_secs": 3600})
        );
        assert_eq!(serde_json::from_value::<PeerInviteRequest>(v).unwrap(), r);
        // The name is required.
        let partial = serde_json::json!({"invite_id": "ab".repeat(16), "public_key": "pk",
                                         "endpoint": "198.51.100.2:51820", "ttl_secs": 3600});
        assert!(serde_json::from_value::<PeerInviteRequest>(partial).is_err());
    }

    #[test]
    fn peer_invite_response_round_trips() {
        let r = PeerInviteResponse {
            node_id: 2,
            mesh_ip: "10.99.0.2".parse().unwrap(),
            expires_at: 99,
            mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            peers: vec![TokenPeer {
                node_id: 1,
                public_key: "pk1".into(),
                endpoint: Some("203.0.113.1:51820".into()),
                mesh_ip: "10.99.0.1".parse().unwrap(),
            }],
            join_addrs: vec!["10.99.0.1:7401".into()],
            name: "node-2".into(),
            replaces: Some(5),
        };
        let mut v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["mesh_cidr"], "10.99.0.0/16");
        assert_eq!(v["name"], "node-2");
        assert_eq!(v["replaces"], 5);
        assert_eq!(
            serde_json::from_value::<PeerInviteResponse>(v.clone()).unwrap(),
            r
        );
        // `replaces` may be absent.
        v.as_object_mut().unwrap().remove("replaces");
        let back: PeerInviteResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back.replaces, None);
    }

    #[test]
    fn peer_renew_request_shape() {
        let r = PeerRenewRequest {
            node_id: 2,
            instance_id: "cd".repeat(16),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"node_id": 2, "instance_id": "cd".repeat(16)})
        );
        assert_eq!(serde_json::from_value::<PeerRenewRequest>(v).unwrap(), r);
        // Every field is required.
        let partial = serde_json::json!({"node_id": 2});
        assert!(serde_json::from_value::<PeerRenewRequest>(partial).is_err());
    }

    #[test]
    fn peer_workload_request_shapes() {
        use serde_json::json;

        let spec: WorkloadSpec =
            serde_json::from_value(json!({"containers": [{"name": "web", "image": "nginx"}]}))
                .unwrap();
        let spec_json = serde_json::to_value(&spec).unwrap();
        for (req, want) in [
            (
                PeerWorkloadRequest::CreateProject {
                    name: "shop".into(),
                    environments: vec!["staging".into()],
                },
                json!({"op": "create_project", "name": "shop", "environments": ["staging"]}),
            ),
            (
                PeerWorkloadRequest::DeleteProject {
                    name: "shop".into(),
                },
                json!({"op": "delete_project", "name": "shop"}),
            ),
            (
                PeerWorkloadRequest::AddEnvironment {
                    project: "shop".into(),
                    name: "dev".into(),
                },
                json!({"op": "add_environment", "project": "shop", "name": "dev"}),
            ),
            (
                PeerWorkloadRequest::RemoveEnvironment {
                    project: "shop".into(),
                    name: "dev".into(),
                },
                json!({"op": "remove_environment", "project": "shop", "name": "dev"}),
            ),
            (
                PeerWorkloadRequest::ApplyWorkloads {
                    project: "shop".into(),
                    environment: "staging".into(),
                    workloads: BTreeMap::from([("web".to_string(), spec)]),
                    prune: true,
                },
                json!({"op": "apply_workloads", "project": "shop", "environment": "staging",
                       "workloads": {"web": spec_json}, "prune": true}),
            ),
            (
                PeerWorkloadRequest::RemoveWorkloads {
                    project: "shop".into(),
                    environment: "staging".into(),
                    names: vec!["web".into()],
                },
                json!({"op": "remove_workloads", "project": "shop",
                       "environment": "staging", "names": ["web"]}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&req).unwrap(), want);
            assert_eq!(
                serde_json::from_value::<PeerWorkloadRequest>(want).unwrap(),
                req
            );
        }
        // An unknown `op` and a missing field are both refused.
        for bad in [
            json!({"op": "set", "key": "projects"}),
            json!({"name": "shop"}),
            json!({"op": "apply_workloads", "project": "shop", "environment": "staging",
                   "workloads": {}}),
        ] {
            assert!(
                serde_json::from_value::<PeerWorkloadRequest>(bad.clone()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn peer_workload_response_shape() {
        let plain = PeerWorkloadResponse {
            index: 12,
            applied: None,
            project: None,
        };
        let v = serde_json::to_value(&plain).unwrap();
        assert_eq!(v, serde_json::json!({"index": 12}));
        assert_eq!(
            serde_json::from_value::<PeerWorkloadResponse>(v).unwrap(),
            plain
        );

        let deployed = PeerWorkloadResponse {
            index: 12,
            applied: Some(DeployResponse {
                generation: 12,
                created: vec!["web".into()],
                ..DeployResponse::default()
            }),
            project: None,
        };
        let v = serde_json::to_value(&deployed).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"index": 12, "applied": {
                "generation": 12, "created": ["web"], "updated": [],
                "unchanged": [], "removed": []
            }})
        );
        assert_eq!(
            serde_json::from_value::<PeerWorkloadResponse>(v).unwrap(),
            deployed
        );

        let created = PeerWorkloadResponse {
            index: 12,
            applied: None,
            project: Some(ProjectResponse {
                name: "shop".into(),
                created_at: 1_000,
                environments: vec![faber_api_types::workload::EnvironmentSummary {
                    name: "staging".into(),
                    generation: 12,
                    workloads: 0,
                    bytes: 53,
                    updated_at: 1_000,
                }],
            }),
        };
        let v = serde_json::to_value(&created).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"index": 12, "project": {
                "name": "shop", "created_at": 1_000, "environments": [{
                    "name": "staging", "generation": 12, "workloads": 0,
                    "bytes": 53, "updated_at": 1_000
                }]
            }})
        );
        assert_eq!(
            serde_json::from_value::<PeerWorkloadResponse>(v).unwrap(),
            created
        );
    }
}
