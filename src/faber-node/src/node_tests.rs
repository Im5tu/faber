//! Handler tests: the trust rules, against a fake runtime and fake volumes.

use std::net::Ipv4Addr;
use std::sync::Arc;

use faber_api_types::workload::{ContainerSpec, MountSpec};
use faber_cri::fake::{Call, FakeCri};
use faber_cri::{CriClient, PodSandboxConfig};
use faber_node_protocol::{
    ContainerState, DnsConfig, ErrorCode, ReplicaRef, Request, Response, SandboxState, SecretEnv,
    VolumeKind,
};

use crate::config::UidRange;
use crate::node::Node;
use crate::volume::fake::FakeVolumes;

const BASE: u32 = 65536 * 2;

struct Rig {
    node: Node<FakeCri>,
    cri: FakeCri,
    volumes: Arc<FakeVolumes>,
}

fn rig() -> Rig {
    let cri = FakeCri::new();
    let volumes = Arc::new(FakeVolumes::new("/vol"));
    Rig {
        node: Node::new(
            UidRange::new(65536 * 2, 65536 * 8).unwrap(),
            "runc".into(),
            cri.clone(),
            volumes.clone(),
        ),
        cri,
        volumes,
    }
}

fn replica() -> ReplicaRef {
    ReplicaRef {
        project: "shop".into(),
        env: "prod".into(),
        workload: "redis".into(),
        revision: "a1b2c3d4".into(),
        replica_index: 0,
    }
}

fn dns() -> DnsConfig {
    DnsConfig {
        nameservers: vec![Ipv4Addr::new(10, 200, 0, 1)],
        searches: vec![],
    }
}

fn sandbox_request(uid_base: u32) -> Request {
    Request::EnsureSandbox {
        replica: replica(),
        uid_base,
        dns: dns(),
        ports: vec![],
    }
}

fn container_request(sandbox_id: &str, container: ContainerSpec, uid_base: u32) -> Request {
    Request::CreateContainer {
        replica: replica(),
        sandbox_id: sandbox_id.into(),
        container,
        env: SecretEnv([("PASSWORD".to_owned(), "hunter2".to_owned())].into()),
        uid_base,
    }
}

fn data_mount(sub_path: Option<&str>) -> MountSpec {
    MountSpec {
        volume: "data".into(),
        path: "/data".into(),
        read_only: false,
        sub_path: sub_path.map(str::to_owned),
    }
}

fn error_code(r: &Response) -> Option<ErrorCode> {
    match r {
        Response::Error { code, .. } => Some(*code),
        _ => None,
    }
}

async fn sandbox(rig: &Rig) -> String {
    match rig.node.handle(sandbox_request(BASE)).await {
        Response::Sandbox { sandbox_id } => sandbox_id,
        other => panic!("{other:?}"),
    }
}

async fn ready_image(rig: &Rig) {
    let r = rig
        .node
        .handle(Request::PullImage {
            image: "redis:7".into(),
        })
        .await;
    assert!(matches!(r, Response::Image { .. }), "{r:?}");
}

/// A sandbox and container created behind faber's back: no faber labels.
async fn foreign_objects(rig: &Rig) -> (String, String) {
    rig.cri.add_image("x:1");
    let sandbox = rig
        .cri
        .run_pod_sandbox(
            PodSandboxConfig {
                metadata: Some(Default::default()),
                ..Default::default()
            },
            "",
        )
        .await
        .unwrap();
    let container = rig
        .cri
        .create_container(
            &sandbox,
            faber_cri::ContainerConfig {
                metadata: Some(faber_cri::v1::ContainerMetadata {
                    name: "c".into(),
                    attempt: 0,
                }),
                image: Some(faber_cri::v1::ImageSpec {
                    image: "x:1".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            PodSandboxConfig::default(),
        )
        .await
        .unwrap();
    (sandbox, container)
}

#[tokio::test]
async fn images_are_pulled_anonymously() {
    let rig = rig();
    ready_image(&rig).await;
    assert!(rig.cri.calls().contains(&Call::PullImage {
        image: "redis:7".into(),
        auth: None
    }));
    let bad = rig
        .node
        .handle(Request::PullImage {
            image: "a b".into(),
        })
        .await;
    assert_eq!(error_code(&bad), Some(ErrorCode::InvalidRequest));
}

#[tokio::test]
async fn a_bad_uid_base_is_forbidden_everywhere_and_touches_nothing() {
    let rig = rig();
    // 0, below the minimum, unaligned, past the maximum.
    for bad in [0, 65536, BASE + 1, 65536 * 8, u32::MAX] {
        let spec = ContainerSpec::new("redis", "redis:7");
        let volume = Request::EnsureVolume {
            replica: replica(),
            volume_name: "data".into(),
            kind: VolumeKind::Block,
            size_bytes: 8 << 20,
            uid_base: bad,
        };
        for request in [
            sandbox_request(bad),
            container_request("sandbox-1", spec, bad),
            volume,
        ] {
            let r = rig.node.handle(request).await;
            assert_eq!(error_code(&r), Some(ErrorCode::Forbidden), "{bad}");
        }
    }
    assert!(rig.cri.calls().is_empty());
    assert!(rig.volumes.ensured().is_empty());
}

#[tokio::test]
async fn a_capability_outside_the_allow_list_is_forbidden() {
    let rig = rig();
    ready_image(&rig).await;
    let sandbox_id = sandbox(&rig).await;
    let before = rig.cri.calls().len();
    for cap in ["SYS_ADMIN", "NET_ADMIN", "SYS_PTRACE", "ALL"] {
        let mut spec = ContainerSpec::new("redis", "redis:7");
        spec.capabilities = vec![cap.into()];
        let r = rig
            .node
            .handle(container_request(&sandbox_id, spec, BASE))
            .await;
        assert_eq!(error_code(&r), Some(ErrorCode::Forbidden), "{cap}");
    }
    assert_eq!(rig.cri.calls().len(), before);
}

#[tokio::test]
async fn path_like_names_are_refused() {
    let rig = rig();
    let mut r = replica();
    r.workload = "../../etc".into();
    let reply = rig
        .node
        .handle(Request::EnsureSandbox {
            replica: r,
            uid_base: BASE,
            dns: dns(),
            ports: vec![],
        })
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::InvalidRequest));
    for name in ["../x", "a/b", "", "a.b"] {
        let reply = rig
            .node
            .handle(Request::EnsureVolume {
                replica: replica(),
                volume_name: name.into(),
                kind: VolumeKind::Ramdisk,
                size_bytes: 8 << 20,
                uid_base: BASE,
            })
            .await;
        assert_eq!(
            error_code(&reply),
            Some(ErrorCode::InvalidRequest),
            "{name:?}"
        );
    }
    assert!(rig.cri.calls().is_empty());
    assert!(rig.volumes.ensured().is_empty());
}

#[tokio::test]
async fn the_container_lifecycle_with_a_volume() {
    let rig = rig();
    ready_image(&rig).await;
    let reply = rig
        .node
        .handle(Request::EnsureVolume {
            replica: replica(),
            volume_name: "data".into(),
            kind: VolumeKind::Block,
            size_bytes: 8 << 20,
            uid_base: BASE,
        })
        .await;
    assert_eq!(reply, Response::Done);
    let sandbox_id = sandbox(&rig).await;
    let mut spec = ContainerSpec::new("redis", "redis:7");
    spec.mounts = vec![data_mount(None)];
    let Response::Container { container_id } = rig
        .node
        .handle(container_request(&sandbox_id, spec, BASE))
        .await
    else {
        panic!("create failed")
    };
    assert_eq!(
        rig.node
            .handle(Request::StartContainer {
                container_id: container_id.clone()
            })
            .await,
        Response::Done
    );

    let Response::Replicas { replicas } = rig.node.handle(Request::ListReplicas).await else {
        panic!()
    };
    assert_eq!(replicas.len(), 1);
    let info = &replicas[0];
    assert_eq!(info.replica, replica());
    assert_eq!(info.state, SandboxState::Ready);
    assert_eq!(info.containers[0].state, ContainerState::Running);
    assert_eq!(info.containers[0].exit_code, None);
    assert_eq!(info.containers[0].name, "redis-a1b2c3d4-redis");
    assert_eq!(info.containers[0].labels["faber.container"], "redis");

    rig.cri.exit_container(&container_id, 3);
    let Response::Replicas { replicas } = rig.node.handle(Request::ListReplicas).await else {
        panic!()
    };
    assert_eq!(replicas[0].containers[0].state, ContainerState::Exited);
    assert_eq!(replicas[0].containers[0].exit_code, Some(3));

    for request in [
        Request::StopContainer {
            container_id: container_id.clone(),
            grace_secs: 10,
        },
        Request::RemoveContainer {
            container_id: container_id.clone(),
        },
        Request::RemoveSandbox {
            sandbox_id: sandbox_id.clone(),
        },
        Request::RemoveVolume {
            replica: replica(),
            volume_name: "data".into(),
        },
    ] {
        assert_eq!(rig.node.handle(request).await, Response::Done);
    }
    assert!(rig.volumes.ensured().is_empty());
    let Response::Replicas { replicas } = rig.node.handle(Request::ListReplicas).await else {
        panic!()
    };
    assert!(replicas.is_empty());
}

#[tokio::test]
async fn ensure_sandbox_is_idempotent_and_pins_the_uid_base() {
    let rig = rig();
    let first = sandbox(&rig).await;
    assert_eq!(sandbox(&rig).await, first);
    let runs: Vec<_> = rig
        .cri
        .calls()
        .into_iter()
        .filter(|c| matches!(c, Call::RunPodSandbox { .. }))
        .collect();
    assert_eq!(
        runs,
        vec![Call::RunPodSandbox {
            runtime_handler: "runc".into()
        }]
    );
    // The same replica under another range is refused, not remapped.
    let reply = rig.node.handle(sandbox_request(BASE + 65536)).await;
    assert_eq!(error_code(&reply), Some(ErrorCode::Forbidden));
    // A sandbox that is no longer ready is replaced.
    rig.cri.stop_pod_sandbox(&first).await.unwrap();
    let second = sandbox(&rig).await;
    assert_ne!(second, first);
}

#[tokio::test]
async fn containers_only_join_the_sandbox_of_their_replica_and_uid_base() {
    let rig = rig();
    ready_image(&rig).await;
    let sandbox_id = sandbox(&rig).await;
    let spec = || ContainerSpec::new("redis", "redis:7");

    let reply = rig
        .node
        .handle(container_request(&sandbox_id, spec(), BASE + 65536))
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::Forbidden));

    let mut other = replica();
    other.replica_index = 1;
    let reply = rig
        .node
        .handle(Request::CreateContainer {
            replica: other,
            sandbox_id: sandbox_id.clone(),
            container: spec(),
            env: SecretEnv::default(),
            uid_base: BASE,
        })
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::Forbidden));

    let (foreign, _) = foreign_objects(&rig).await;
    let reply = rig
        .node
        .handle(container_request(&foreign, spec(), BASE))
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::Forbidden));

    let reply = rig
        .node
        .handle(container_request("no-such", spec(), BASE))
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::NotFound));
}

#[tokio::test]
async fn runtime_objects_faber_did_not_create_are_untouchable() {
    let rig = rig();
    let (foreign, container) = foreign_objects(&rig).await;
    for request in [
        Request::StartContainer {
            container_id: container.clone(),
        },
        Request::StopContainer {
            container_id: container.clone(),
            grace_secs: 1,
        },
        Request::RemoveContainer {
            container_id: container.clone(),
        },
        Request::RemoveSandbox {
            sandbox_id: foreign.clone(),
        },
    ] {
        let reply = rig.node.handle(request).await;
        assert_eq!(error_code(&reply), Some(ErrorCode::Forbidden));
    }
    assert!(!rig.cri.calls().iter().any(|c| matches!(
        c,
        Call::StartContainer(_)
            | Call::StopContainer { .. }
            | Call::RemoveContainer(_)
            | Call::StopPodSandbox(_)
            | Call::RemovePodSandbox(_)
    )));
    // And they are not listed as replicas.
    let Response::Replicas { replicas } = rig.node.handle(Request::ListReplicas).await else {
        panic!()
    };
    assert!(replicas.is_empty());
}

#[tokio::test]
async fn removals_of_missing_objects_succeed_and_a_long_grace_is_refused() {
    let rig = rig();
    for request in [
        Request::RemoveSandbox {
            sandbox_id: "gone".into(),
        },
        Request::RemoveContainer {
            container_id: "gone".into(),
        },
        Request::RemoveVolume {
            replica: replica(),
            volume_name: "gone".into(),
        },
    ] {
        assert_eq!(rig.node.handle(request).await, Response::Done);
    }
    let reply = rig
        .node
        .handle(Request::StopContainer {
            container_id: "gone".into(),
            grace_secs: 99_999,
        })
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::InvalidRequest));
}

#[tokio::test]
async fn sub_path_mounts_are_refused() {
    let rig = rig();
    let sandbox_id = sandbox(&rig).await;
    let mut spec = ContainerSpec::new("redis", "redis:7");
    spec.mounts = vec![data_mount(Some("sub"))];
    let reply = rig
        .node
        .handle(container_request(&sandbox_id, spec, BASE))
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::Forbidden));
}

#[tokio::test]
async fn a_mount_needs_an_ensured_volume() {
    let rig = rig();
    let sandbox_id = sandbox(&rig).await;
    let mut spec = ContainerSpec::new("redis", "redis:7");
    spec.mounts = vec![data_mount(None)];
    let reply = rig
        .node
        .handle(container_request(&sandbox_id, spec, BASE))
        .await;
    assert_eq!(error_code(&reply), Some(ErrorCode::NotFound));
}

#[tokio::test]
async fn the_attempt_counter_follows_the_containers_in_the_sandbox() {
    let rig = rig();
    ready_image(&rig).await;
    let sandbox_id = sandbox(&rig).await;
    let mut attempts = Vec::new();
    for _ in 0..3 {
        let reply = rig
            .node
            .handle(container_request(
                &sandbox_id,
                ContainerSpec::new("redis", "redis:7"),
                BASE,
            ))
            .await;
        assert!(matches!(reply, Response::Container { .. }), "{reply:?}");
        let Response::Replicas { replicas } = rig.node.handle(Request::ListReplicas).await else {
            panic!()
        };
        attempts.push(
            replicas[0]
                .containers
                .iter()
                .map(|c| c.attempt)
                .max()
                .unwrap(),
        );
    }
    assert_eq!(attempts, vec![0, 1, 2]);
}

#[tokio::test]
async fn a_failure_never_echoes_the_env() {
    let rig = rig();
    let sandbox_id = sandbox(&rig).await;
    // The image was never pulled, so the runtime refuses.
    let reply = rig
        .node
        .handle(container_request(
            &sandbox_id,
            ContainerSpec::new("redis", "redis:7"),
            BASE,
        ))
        .await;
    assert!(error_code(&reply).is_some());
    assert!(!format!("{reply:?}").contains("hunter2"));
}
