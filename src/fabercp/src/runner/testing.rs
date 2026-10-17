//! Builders and fakes shared by the tests of the runner and the containers
//! step.

use std::sync::{Arc, Mutex, PoisonError};

use faber_api_types::workload::{ContainerSpec, Probe, ProbeAction, RestartPolicy};
use faber_node_protocol::{DnsConfig, ReplicaRef, VolumeKind};

use super::desired::{DesiredContainer, DesiredReplica, DesiredVolume, revision_string};
use super::env::StaticEnv;
use super::launch::launch;
use super::node::fake::FakeNode;
use super::probe::{ProbeOutcome, ProbeTarget, Prober};
use super::state::RunnerState;

/// The secret in every test env. No debug output may show it.
pub const SECRET: &str = "hunter2";

pub fn replica_ref(workload: &str, revision: u64, index: u32) -> ReplicaRef {
    ReplicaRef {
        project: "shop".into(),
        env: "prod".into(),
        workload: workload.into(),
        revision: revision_string(revision),
        replica_index: index,
    }
}

/// A container with a TCP probe every 5 seconds, or none.
pub fn container(name: &str, probed: bool) -> DesiredContainer {
    let mut spec = ContainerSpec::new(name, "img:1");
    if probed {
        spec.probe = Some(Probe::new(ProbeAction::Tcp { port: 80 }));
    }
    DesiredContainer { spec }
}

pub fn volume(name: &str, kind: VolumeKind) -> DesiredVolume {
    DesiredVolume {
        name: name.into(),
        kind,
        size_bytes: 1 << 20,
    }
}

pub fn desired(
    workload: &str,
    revision: u64,
    index: u32,
    restart: RestartPolicy,
    containers: Vec<DesiredContainer>,
    volumes: Vec<DesiredVolume>,
) -> DesiredReplica {
    DesiredReplica {
        replica: replica_ref(workload, revision, index),
        uid_base: 1_048_576,
        restart,
        stop_grace_secs: 5,
        dns: DnsConfig {
            nameservers: vec![],
            searches: vec![],
        },
        volumes,
        containers,
    }
}

/// What the containers step does for a new replica.
pub async fn start(node: &FakeNode, state: &RunnerState, d: &DesiredReplica) {
    state.register(d);
    launch(node, state, StaticEnv::secret().as_ref(), d, None)
        .await
        .unwrap();
}

/// A prober that answers what the test says and remembers what it was asked.
#[derive(Debug, Clone, Default)]
pub struct FakeProber {
    fail: Arc<Mutex<bool>>,
    asked: Arc<Mutex<Vec<ProbeTarget>>>,
}

impl FakeProber {
    pub fn set_failing(&self, failing: bool) {
        *self.fail.lock().unwrap_or_else(PoisonError::into_inner) = failing;
    }

    pub fn asked(&self) -> Vec<ProbeTarget> {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Prober for FakeProber {
    async fn probe(&self, target: ProbeTarget) -> ProbeOutcome {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(target);
        if *self.fail.lock().unwrap_or_else(PoisonError::into_inner) {
            ProbeOutcome::Fail("refused".into())
        } else {
            ProbeOutcome::Pass
        }
    }
}

/// A fake node whose `StopContainer` blocks until the test adds permits to
/// `gate`: a stop that waits out a long grace period.
#[derive(Clone)]
pub struct GatedNode {
    pub inner: FakeNode,
    pub gate: Arc<tokio::sync::Semaphore>,
}

impl GatedNode {
    pub fn new() -> Self {
        Self {
            inner: FakeNode::default(),
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

impl super::node::NodeClient for GatedNode {
    async fn call(
        &self,
        request: faber_node_protocol::Request,
    ) -> Result<faber_node_protocol::Response, super::node::NodeCallError> {
        if matches!(request, faber_node_protocol::Request::StopContainer { .. }) {
            // A permit is not given back: the gate stays open once opened.
            if let Ok(permit) = self.gate.acquire().await {
                permit.forget();
            }
        }
        self.inner.call(request).await
    }
}
