use std::collections::BTreeMap;
use std::fmt;
use std::net::Ipv4Addr;

use faber_api_types::workload::{ContainerSpec, PortSpec};
use serde::{Deserialize, Serialize};

use crate::replica::ReplicaRef;

/// Plaintext environment of a container. It may hold secrets, so `Debug`
/// prints the variable names only.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretEnv(pub BTreeMap<String, String>);

impl fmt::Debug for SecretEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.keys()).finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsConfig {
    pub nameservers: Vec<Ipv4Addr>,
    /// DNS names, without a trailing dot.
    pub searches: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeKind {
    Block,
    Ramdisk,
}

/// What fabercp may ask of `faber-node`. Anything else is not a frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// The handshake: answered with [`Response::Version`], or with a
    /// `version_mismatch` error when `client_version` is not understood.
    Version {
        client_version: u32,
    },
    PullImage {
        image: String,
    },
    /// Creates the replica's pod sandbox, or returns the ready one.
    EnsureSandbox {
        replica: ReplicaRef,
        /// First host id of the 65536 ids the pod's user namespace maps.
        uid_base: u32,
        dns: DnsConfig,
        ports: Vec<PortSpec>,
    },
    CreateContainer {
        replica: ReplicaRef,
        sandbox_id: String,
        container: ContainerSpec,
        /// Replaces `container.env`, which is ignored.
        env: SecretEnv,
        uid_base: u32,
    },
    StartContainer {
        container_id: String,
    },
    StopContainer {
        container_id: String,
        grace_secs: u32,
    },
    RemoveContainer {
        container_id: String,
    },
    RemoveSandbox {
        sandbox_id: String,
    },
    EnsureVolume {
        replica: ReplicaRef,
        volume_name: String,
        kind: VolumeKind,
        size_bytes: u64,
        uid_base: u32,
    },
    RemoveVolume {
        replica: ReplicaRef,
        volume_name: String,
    },
    ListReplicas,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The request is malformed or a name or number is out of range.
    InvalidRequest,
    /// The request is well formed but breaks a trust rule.
    Forbidden,
    NotFound,
    VersionMismatch,
    /// The runtime or a host command failed.
    Runtime,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeError {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxState {
    Ready,
    NotReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerState {
    Created,
    Running,
    Exited,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub attempt: u32,
    pub state: ContainerState,
    /// Set once the container has exited.
    pub exit_code: Option<i32>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub replica: ReplicaRef,
    pub sandbox_id: String,
    pub state: SandboxState,
    pub pod_ip: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub containers: Vec<ContainerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Version {
        protocol_version: u32,
        node_version: String,
    },
    Image {
        image_id: String,
    },
    Sandbox {
        sandbox_id: String,
    },
    Container {
        container_id: String,
    },
    /// The request had no result to return.
    Done,
    Replicas {
        replicas: Vec<ReplicaInfo>,
    },
    Error {
        code: ErrorCode,
        message: String,
    },
}

impl Response {
    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
        }
    }
}

impl From<NodeError> for Response {
    fn from(e: NodeError) -> Self {
        Self::Error {
            code: e.code,
            message: e.message,
        }
    }
}

impl NodeError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode_frame, read_frame};

    fn replica() -> ReplicaRef {
        ReplicaRef {
            project: "shop".into(),
            env: "prod".into(),
            workload: "redis".into(),
            revision: "a1b2c3d4".into(),
            replica_index: 0,
        }
    }

    fn requests() -> Vec<Request> {
        let mut container = ContainerSpec::new("redis", "redis:7");
        container.capabilities = vec!["CHOWN".into()];
        vec![
            Request::Version { client_version: 1 },
            Request::PullImage {
                image: "redis:7".into(),
            },
            Request::EnsureSandbox {
                replica: replica(),
                uid_base: 65536,
                dns: DnsConfig {
                    nameservers: vec![Ipv4Addr::new(10, 200, 0, 1)],
                    searches: vec!["prod.shop.faber.local".into()],
                },
                ports: vec![],
            },
            Request::CreateContainer {
                replica: replica(),
                sandbox_id: "sb-1".into(),
                container,
                env: SecretEnv(BTreeMap::from([("PASSWORD".into(), "hunter2".into())])),
                uid_base: 65536,
            },
            Request::StartContainer {
                container_id: "c1".into(),
            },
            Request::StopContainer {
                container_id: "c1".into(),
                grace_secs: 30,
            },
            Request::RemoveContainer {
                container_id: "c1".into(),
            },
            Request::RemoveSandbox {
                sandbox_id: "sb-1".into(),
            },
            Request::EnsureVolume {
                replica: replica(),
                volume_name: "data".into(),
                kind: VolumeKind::Block,
                size_bytes: 1 << 30,
                uid_base: 65536,
            },
            Request::RemoveVolume {
                replica: replica(),
                volume_name: "data".into(),
            },
            Request::ListReplicas,
        ]
    }

    #[tokio::test]
    async fn every_request_round_trips_through_a_frame() {
        for request in requests() {
            let wire = encode_frame(&request).unwrap();
            let back: Request = read_frame(&mut wire.as_slice(), crate::MAX_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(back, request);
        }
    }

    #[test]
    fn responses_round_trip() {
        let all = vec![
            Response::Version {
                protocol_version: 1,
                node_version: "0.1.0".into(),
            },
            Response::Image {
                image_id: "sha256:1".into(),
            },
            Response::Sandbox {
                sandbox_id: "s".into(),
            },
            Response::Container {
                container_id: "c".into(),
            },
            Response::Done,
            Response::Replicas {
                replicas: vec![ReplicaInfo {
                    replica: replica(),
                    sandbox_id: "s".into(),
                    state: SandboxState::Ready,
                    pod_ip: Some("10.1.0.2".into()),
                    labels: BTreeMap::new(),
                    containers: vec![ContainerInfo {
                        id: "c".into(),
                        name: "redis-a1b2c3d4-redis".into(),
                        attempt: 1,
                        state: ContainerState::Exited,
                        exit_code: Some(137),
                        labels: BTreeMap::new(),
                    }],
                }],
            },
            Response::error(ErrorCode::Forbidden, "no"),
        ];
        for response in all {
            let json = serde_json::to_string(&response).unwrap();
            assert_eq!(serde_json::from_str::<Response>(&json).unwrap(), response);
        }
    }

    #[test]
    fn unknown_request_types_and_fields_are_refused() {
        assert!(serde_json::from_str::<Request>(r#"{"type":"exec","cmd":"sh"}"#).is_err());
        assert!(
            serde_json::from_str::<Request>(r#"{"type":"pull_image","image":"a","path":"/"}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<Request>(r#"{"type":"list_replicas"}"#).is_ok());
    }

    #[test]
    fn env_values_never_reach_debug_output() {
        for request in requests() {
            for text in [format!("{request:?}"), format!("{request:#?}")] {
                assert!(!text.contains("hunter2"), "{text}");
            }
        }
        let text = format!("{:?}", requests()[3]);
        assert!(text.contains("PASSWORD"));
    }
}
