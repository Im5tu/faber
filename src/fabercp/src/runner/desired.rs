//! What the runner is asked to run: one replica with everything `faber-node`
//! needs for it, except the env.
//!
//! The env is fetched when a container is made (see [`super::env`]) and
//! lives in the request only, so a `DesiredReplica` held by the step, the
//! reconcile loop or the supervisor never holds a secret.

use faber_api_types::workload::{ContainerSpec, PortSpec, RestartPolicy};
use faber_node_protocol::{DnsConfig, ReplicaRef, VolumeKind};

/// A volume of the pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredVolume {
    pub name: String,
    pub kind: VolumeKind,
    pub size_bytes: u64,
}

/// One container. `spec.env` is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredContainer {
    pub spec: ContainerSpec,
}

/// One replica of one revision that placement put on this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredReplica {
    pub replica: ReplicaRef,
    /// First host uid of the workload's range.
    pub uid_base: u32,
    pub restart: RestartPolicy,
    pub stop_grace_secs: u32,
    pub dns: DnsConfig,
    pub volumes: Vec<DesiredVolume>,
    pub containers: Vec<DesiredContainer>,
}

impl DesiredReplica {
    /// Every port of every container: the ports of the pod.
    pub fn ports(&self) -> Vec<PortSpec> {
        self.containers
            .iter()
            .flat_map(|c| c.spec.ports.iter().cloned())
            .collect()
    }

    /// Whether any volume is a block volume. Such a replica cannot run next
    /// to another revision of itself: they would share the volume.
    pub fn has_block_volume(&self) -> bool {
        self.volumes.iter().any(|v| v.kind == VolumeKind::Block)
    }
}

/// The revision of a workload as `ReplicaRef` spells it: zero-padded hex,
/// which is lower-case hex of the length `faber-node` accepts.
pub fn revision_string(revision: u64) -> String {
    format!("{revision:016x}")
}

/// The inverse of [`revision_string`]. `None` for a revision faber did not
/// write.
pub fn parse_revision(revision: &str) -> Option<u64> {
    u64::from_str_radix(revision, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revisions_round_trip_and_are_valid_replica_revisions() {
        for revision in [0, 1, 4096, u64::MAX] {
            let text = revision_string(revision);
            assert_eq!(parse_revision(&text), Some(revision));
            let replica = ReplicaRef {
                project: "shop".into(),
                env: "prod".into(),
                workload: "web".into(),
                revision: text,
                replica_index: 0,
            };
            assert_eq!(replica.validate(), Ok(()));
        }
        assert_eq!(parse_revision("zz"), None);
    }
}
