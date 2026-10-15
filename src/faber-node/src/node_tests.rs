//! Handler tests: the trust rules, against a fake runtime and fake volumes.

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use faber_api_types::workload::{ContainerSpec, MountSpec};
use faber_container_runtime::Containerd;
use faber_container_runtime::cni::CONFLIST_FILE;
use faber_container_runtime::containerd::DROP_IN_FILE;
use faber_cri::fake::{Call, FakeCri};
use faber_cri::{CriClient, PodSandboxConfig};
use faber_node_protocol::{
    ContainerState, DnsConfig, ErrorCode, PodNetwork, ReplicaRef, Request, Response, SandboxState,
    SecretEnv, VolumeKind,
};

use crate::config::UidRange;
use crate::node::Node;
use crate::runtime_config::RuntimeConfig;
use crate::volume::fake::FakeVolumes;

const BASE: u32 = 65536 * 2;

struct Rig {
    node: Node<FakeCri>,
    cri: FakeCri,
    volumes: Arc<FakeVolumes>,
    /// Holds the directories faber-node was configured with.
    dirs: tempfile::TempDir,
}

impl Rig {
    fn containerd_dir(&self) -> PathBuf {
        self.dirs.path().join("conf.d")
    }

    fn cni_dir(&self) -> PathBuf {
        self.dirs.path().join("net.d")
    }

    /// Every file under the two directories, as `dir/name`.
    fn files(&self) -> Vec<String> {
        let mut out = Vec::new();
        for dir in ["conf.d", "net.d"] {
            for entry in std::fs::read_dir(self.dirs.path().join(dir)).unwrap() {
                out.push(format!(
                    "{dir}/{}",
                    entry.unwrap().file_name().to_string_lossy()
                ));
            }
        }
        out.sort();
        out
    }
}

fn rig() -> Rig {
    let cri = FakeCri::new();
    let volumes = Arc::new(FakeVolumes::new("/vol"));
    let dirs = tempfile::tempdir().unwrap();
    std::fs::create_dir(dirs.path().join("conf.d")).unwrap();
    std::fs::create_dir(dirs.path().join("net.d")).unwrap();
    let runtime = RuntimeConfig::new(
        Arc::new(Containerd {
            conf_dir: dirs.path().join("conf.d"),
            cni_conf_dir: dirs.path().join("net.d"),
        }),
        std::path::Path::new("/opt/cni/bin"),
        &dirs.path().join("net.d"),
    );
    Rig {
        node: Node::new(
            UidRange::new(65536 * 2, 65536 * 8).unwrap(),
            "runc".into(),
            cri.clone(),
            volumes.clone(),
            runtime,
        ),
        cri,
        volumes,
        dirs,
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

fn data_mount() -> MountSpec {
    MountSpec {
        volume: "data".into(),
        path: "/data".into(),
        read_only: false,
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
    spec.mounts = vec![data_mount()];
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
async fn a_mount_needs_an_ensured_volume() {
    let rig = rig();
    let sandbox_id = sandbox(&rig).await;
    let mut spec = ContainerSpec::new("redis", "redis:7");
    spec.mounts = vec![data_mount()];
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
        let Response::Container { container_id } = reply else {
            panic!("{reply:?}")
        };
        // The container ran and exited, so the next one is a new attempt.
        rig.cri.start_container(&container_id).await.unwrap();
        rig.cri.exit_container(&container_id, 1);
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
async fn a_retried_create_returns_the_container_already_made() {
    let rig = rig();
    ready_image(&rig).await;
    let sandbox_id = sandbox(&rig).await;
    let spec = || ContainerSpec::new("redis", "redis:7");
    let first = rig
        .node
        .handle(container_request(&sandbox_id, spec(), BASE))
        .await;
    let second = rig
        .node
        .handle(container_request(&sandbox_id, spec(), BASE))
        .await;
    assert!(matches!(first, Response::Container { .. }), "{first:?}");
    assert_eq!(first, second);
    let creates = rig
        .cri
        .calls()
        .into_iter()
        .filter(|c| matches!(c, Call::CreateContainer { .. }))
        .count();
    assert_eq!(creates, 1);
}

#[tokio::test]
async fn a_retried_create_with_a_changed_spec_replaces_the_stale_container() {
    let rig = rig();
    ready_image(&rig).await;
    let sandbox_id = sandbox(&rig).await;
    let spec = || ContainerSpec::new("redis", "redis:7");
    let Response::Container {
        container_id: first,
    } = rig
        .node
        .handle(container_request(&sandbox_id, spec(), BASE))
        .await
    else {
        panic!()
    };
    // Same spec, different env value.
    let changed = Request::CreateContainer {
        replica: replica(),
        sandbox_id: sandbox_id.clone(),
        container: spec(),
        env: SecretEnv([("PASSWORD".to_owned(), "other".to_owned())].into()),
        uid_base: BASE,
    };
    let Response::Container {
        container_id: second,
    } = rig.node.handle(changed).await
    else {
        panic!()
    };
    assert_ne!(first, second);
    assert!(
        rig.cri
            .calls()
            .contains(&Call::RemoveContainer(first.clone()))
    );
    let Response::Replicas { replicas } = rig.node.handle(Request::ListReplicas).await else {
        panic!()
    };
    let ids: Vec<_> = replicas[0].containers.iter().map(|c| &c.id).collect();
    assert_eq!(ids, vec![&second]);
}

#[tokio::test]
async fn concurrent_sandbox_requests_make_one_sandbox() {
    let rig = rig();
    let (a, b, c) = tokio::join!(
        rig.node.handle(sandbox_request(BASE)),
        rig.node.handle(sandbox_request(BASE)),
        rig.node.handle(sandbox_request(BASE)),
    );
    assert_eq!(a, b);
    assert_eq!(b, c);
    let runs = rig
        .cri
        .calls()
        .into_iter()
        .filter(|c| matches!(c, Call::RunPodSandbox { .. }))
        .count();
    assert_eq!(runs, 1);
}

#[tokio::test]
async fn a_failed_handshake_closes_the_connection() {
    use crate::connection::{Limits, Outcome, serve_connection};
    use crate::peer::fake::FakeStream;
    use crate::peer::{Authorizer, Peer};
    use faber_node_protocol::{MAX_FRAME_BYTES, read_frame, write_frame};

    let rig = rig();
    let (server, mut client) = FakeStream::pair(Peer {
        uid: 0,
        gid: 0,
        pid: None,
    });
    let task = tokio::spawn(async move {
        serve_connection(server, &rig.node, &Authorizer::new(990), Limits::default()).await
    });
    write_frame(&mut client, &Request::Version { client_version: 99 })
        .await
        .unwrap();
    let reply: Response = read_frame(&mut client, MAX_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(error_code(&reply), Some(ErrorCode::VersionMismatch));
    // The server hung up: a following request gets no answer.
    let _ = write_frame(&mut client, &Request::ListReplicas).await;
    let next: Option<Response> = read_frame(&mut client, MAX_FRAME_BYTES)
        .await
        .unwrap_or(None);
    assert_eq!(next, None);
    assert_eq!(task.await.unwrap(), Outcome::Finished);
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

#[tokio::test]
async fn list_volumes_answers_with_the_volumes_that_exist() {
    let rig = rig();
    for (name, kind) in [
        ("scratch", VolumeKind::Ramdisk),
        ("data", VolumeKind::Block),
    ] {
        let reply = rig
            .node
            .handle(Request::EnsureVolume {
                replica: replica(),
                volume_name: name.into(),
                kind,
                size_bytes: 1 << 20,
                uid_base: BASE,
            })
            .await;
        assert_eq!(reply, Response::Done);
    }
    let Response::Volumes { volumes } = rig.node.handle(Request::ListVolumes).await else {
        panic!("volumes expected");
    };
    let got: Vec<_> = volumes
        .iter()
        .map(|v| (v.volume.as_str(), v.kind))
        .collect();
    assert_eq!(
        got,
        [
            ("data", VolumeKind::Block),
            ("scratch", VolumeKind::Ramdisk)
        ]
    );

    let gone = rig
        .node
        .handle(Request::RemoveVolume {
            replica: replica(),
            volume_name: "scratch".into(),
        })
        .await;
    assert_eq!(gone, Response::Done);
    let Response::Volumes { volumes } = rig.node.handle(Request::ListVolumes).await else {
        panic!("volumes expected");
    };
    assert_eq!(volumes.len(), 1);
}

fn configure(sandbox_image: &str, pod_network: Option<PodNetwork>) -> Request {
    Request::ConfigureRuntime {
        systemd_cgroup: true,
        sandbox_image: sandbox_image.into(),
        pod_network,
    }
}

fn pod_network(bridge: &str, subnet: &str) -> PodNetwork {
    PodNetwork {
        bridge: bridge.into(),
        subnet: subnet.parse().unwrap(),
        mtu: 1420,
    }
}

#[tokio::test]
async fn the_runtime_config_is_written_to_the_nodes_own_paths() {
    let rig = rig();
    let reply = rig
        .node
        .handle(configure(
            "registry.k8s.io/pause:3.10",
            Some(pod_network("faberbr0", "10.200.3.0/24")),
        ))
        .await;
    assert_eq!(reply, Response::Changed { changed: true });
    assert_eq!(
        rig.files(),
        [
            format!("conf.d/{DROP_IN_FILE}"),
            format!("net.d/{CONFLIST_FILE}")
        ]
    );

    let drop_in = std::fs::read_to_string(rig.containerd_dir().join(DROP_IN_FILE)).unwrap();
    assert!(drop_in.contains(r#"sandbox_image = "registry.k8s.io/pause:3.10""#));
    assert!(drop_in.contains("SystemdCgroup = true"));
    // The plugin and config directories are the node's, not a request's.
    assert!(drop_in.contains(r#"bin_dir = "/opt/cni/bin""#));
    let cni = rig.cni_dir();
    assert!(drop_in.contains(&format!(r#"conf_dir = "{}""#, cni.display())));

    let conflist: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(rig.cni_dir().join(CONFLIST_FILE)).unwrap())
            .unwrap();
    assert_eq!(conflist["plugins"][0]["bridge"], "faberbr0");
    assert_eq!(conflist["plugins"][0]["mtu"], 1420);
    assert_eq!(
        conflist["plugins"][0]["ipam"]["ranges"][0][0]["subnet"],
        "10.200.3.0/24"
    );
    // Nothing is left behind by the atomic write.
    assert_eq!(rig.files().len(), 2);
}

#[tokio::test]
async fn configuring_again_changes_nothing_and_no_network_removes_the_conflist() {
    let rig = rig();
    let request = configure("pause:3", Some(pod_network("faberbr0", "10.200.3.0/24")));
    assert_eq!(
        rig.node.handle(request.clone()).await,
        Response::Changed { changed: true }
    );
    assert_eq!(
        rig.node.handle(request).await,
        Response::Changed { changed: false }
    );
    assert_eq!(
        rig.node.handle(configure("pause:3", None)).await,
        Response::Changed { changed: true }
    );
    assert_eq!(rig.files(), [format!("conf.d/{DROP_IN_FILE}")]);
}

#[tokio::test]
async fn remove_cni_config_removes_that_file_only() {
    let rig = rig();
    rig.node
        .handle(configure(
            "pause:3",
            Some(pod_network("faberbr0", "10.200.3.0/24")),
        ))
        .await;
    assert_eq!(
        rig.node.handle(Request::RemoveCniConfig).await,
        Response::Changed { changed: true }
    );
    assert_eq!(rig.files(), [format!("conf.d/{DROP_IN_FILE}")]);
    assert_eq!(
        rig.node.handle(Request::RemoveCniConfig).await,
        Response::Changed { changed: false }
    );
}

#[tokio::test]
async fn an_invalid_runtime_config_is_refused_and_writes_nothing() {
    let rig = rig();
    let good = pod_network("faberbr0", "10.200.3.0/24");
    let mut mtu = good.clone();
    mtu.mtu = 10;
    for request in [
        configure("a b", Some(good.clone())),
        configure("", None),
        configure(
            "x\"
[evil]",
            None,
        ),
        configure("pause:3", Some(pod_network("eth0", "10.200.3.0/24"))),
        configure("pause:3", Some(pod_network("faberbr0\"x", "10.200.3.0/24"))),
        configure("pause:3", Some(pod_network("faberbr0", "8.8.8.0/24"))),
        configure("pause:3", Some(pod_network("faberbr0", "10.200.3.1/24"))),
        configure("pause:3", Some(pod_network("faberbr0", "0.0.0.0/0"))),
        configure("pause:3", Some(mtu)),
    ] {
        let reply = rig.node.handle(request.clone()).await;
        assert_eq!(
            error_code(&reply),
            Some(ErrorCode::InvalidRequest),
            "{request:?}"
        );
    }
    assert!(rig.files().is_empty());
}

#[tokio::test]
async fn a_failed_write_is_a_runtime_error_without_the_path() {
    let rig = rig();
    std::fs::remove_dir(rig.containerd_dir()).unwrap();
    let reply = rig.node.handle(configure("pause:3", None)).await;
    let Response::Error { code, message } = reply else {
        panic!("{reply:?}")
    };
    assert_eq!(code, ErrorCode::Runtime);
    assert!(
        !message.contains(rig.dirs.path().to_str().unwrap()),
        "{message}"
    );
}
