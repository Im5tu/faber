use faber_api_types::workload::*;
use faber_api_types::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

/// Serialises `value`, checks it matches `expected` exactly, then checks it
/// deserialises back to an equal value.
fn assert_shape<T>(value: T, expected: Value)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let actual = serde_json::to_value(&value).expect("serialize");
    assert_eq!(actual, expected, "JSON shape mismatch");
    let back: T = serde_json::from_value(expected).expect("deserialize");
    assert_eq!(back, value, "round-trip mismatch");
}

#[test]
fn constants() {
    assert_eq!(DEFAULT_PORT, 7400);
    assert_eq!(DEFAULT_PEER_PORT, 7401);
    assert_eq!(FORWARDED_HEADER, "x-faber-forwarded");
    assert_eq!(error_code::NOT_FOUND, "not_found");
    assert_eq!(error_code::BAD_REQUEST, "bad_request");
    assert_eq!(error_code::ALREADY_INITIALIZED, "already_initialized");
    assert_eq!(error_code::ALREADY_JOINED, "already_joined");
    assert_eq!(error_code::NOT_LEADER, "not_leader");
    assert_eq!(error_code::NO_LEADER, "no_leader");
    assert_eq!(error_code::PEER_UNREACHABLE, "peer_unreachable");
    assert_eq!(error_code::INTERNAL, "internal");
    assert_eq!(error_code::INVITE_INVALID, "invite_invalid");
    assert_eq!(error_code::INVITE_USED, "invite_used");
    assert_eq!(error_code::INVITE_EXPIRED, "invite_expired");
    assert_eq!(error_code::MESH_NOT_CONFIGURED, "mesh_not_configured");
    assert_eq!(error_code::MESH_NO_ENDPOINTS, "mesh_no_endpoints");
    assert_eq!(error_code::MESH_EXHAUSTED, "mesh_exhausted");
    assert_eq!(error_code::MESH_NODE_REMOVED, "mesh_node_removed");
    assert_eq!(error_code::MESH_LEASE_EXPIRED, "mesh_lease_expired");
    assert_eq!(error_code::FORBIDDEN, "forbidden");
    assert_eq!(error_code::PROJECT_EXISTS, "project_exists");
    assert_eq!(error_code::NOT_EMPTY, "not_empty");
    assert_eq!(error_code::WORKLOADS_EXHAUSTED, "workloads_exhausted");
    assert_eq!(error_code::NOT_READY, "not_ready");
    assert_eq!(error_code::UNAVAILABLE, "unavailable");
}

/// The 503 body of an unhealthy node reads as either shape.
#[test]
fn unavailable_health_body_reads_as_health_and_as_error() {
    let body = json!({
        "status": "unavailable", "node_id": 3,
        "error": "unavailable", "message": "the raft core is not running: raft stopped"
    });
    let health: HealthResponse = serde_json::from_value(body.clone()).unwrap();
    assert_eq!(
        health,
        HealthResponse {
            status: "unavailable".into(),
            node_id: 3
        }
    );
    let error: ErrorBody = serde_json::from_value(body).unwrap();
    assert_eq!(error.error, error_code::UNAVAILABLE);
    assert!(error.message.unwrap().contains("raft"));
}

#[test]
fn health_response_shape() {
    assert_shape(
        HealthResponse {
            status: "ok".into(),
            node_id: 3,
        },
        json!({"status": "ok", "node_id": 3}),
    );
}

#[test]
fn role_is_lowercase_string() {
    for (role, s) in [
        (Role::Leader, "leader"),
        (Role::Follower, "follower"),
        (Role::Candidate, "candidate"),
        (Role::Learner, "learner"),
        (Role::Shutdown, "shutdown"),
    ] {
        assert_shape(role, json!(s));
        assert_eq!(role.as_str(), s);
        assert_eq!(role.to_string(), s);
        assert_eq!(role, s);
    }
}

#[test]
fn role_rejects_unknown_value() {
    assert!(serde_json::from_value::<Role>(json!("Leader")).is_err());
    assert!(serde_json::from_value::<Role>(json!("boss")).is_err());
}

#[test]
fn cluster_status_shape() {
    assert_shape(
        ClusterStatus {
            node_id: 2,
            joined: true,
            role: Role::Follower,
            term: 3,
            leader_id: Some(1),
            leader_addr: Some("10.8.0.1:7401".into()),
            members: vec![
                MemberStatus {
                    node_id: 1,
                    addr: "10.8.0.1:7401".into(),
                    voter: true,
                    matched_index: Some(5),
                    lag: Some(0),
                    name: Some("node-1".into()),
                },
                MemberStatus {
                    node_id: 2,
                    addr: "10.8.0.2:7401".into(),
                    voter: false,
                    matched_index: None,
                    lag: None,
                    name: None,
                },
            ],
        },
        json!({
            "node_id": 2, "joined": true, "role": "follower", "term": 3,
            "leader_id": 1, "leader_addr": "10.8.0.1:7401",
            "members": [
                {"node_id": 1, "addr": "10.8.0.1:7401", "voter": true,
                 "matched_index": 5, "lag": 0, "name": "node-1"},
                {"node_id": 2, "addr": "10.8.0.2:7401", "voter": false,
                 "matched_index": null, "lag": null}
            ]
        }),
    );
}

#[test]
fn cluster_status_of_unjoined_node() {
    assert_shape(
        ClusterStatus {
            node_id: 5,
            joined: false,
            role: Role::Learner,
            term: 0,
            leader_id: None,
            leader_addr: None,
            members: vec![],
        },
        json!({
            "node_id": 5, "joined": false, "role": "learner", "term": 0,
            "leader_id": null, "leader_addr": null, "members": []
        }),
    );
}

/// A status from a server that knows no names still decodes.
#[test]
fn member_status_without_a_name_decodes() {
    let m: MemberStatus = serde_json::from_value(json!({
        "node_id": 1, "addr": "10.8.0.1:7401", "voter": true,
        "matched_index": 5, "lag": 0
    }))
    .unwrap();
    assert_eq!(m.name, None);
}

#[test]
fn join_request_shape() {
    assert_shape(
        JoinRequest {
            peer: "10.8.0.1:7401".into(),
        },
        json!({"peer": "10.8.0.1:7401"}),
    );
}

#[test]
fn error_body_shape_full() {
    assert_shape(
        ErrorBody {
            error: error_code::NOT_LEADER.into(),
            message: Some("forward to leader".into()),
            leader_id: Some(2),
            leader_addr: Some("127.0.0.1:7401".into()),
        },
        json!({
            "error": "not_leader",
            "message": "forward to leader",
            "leader_id": 2,
            "leader_addr": "127.0.0.1:7401"
        }),
    );
}

#[test]
fn error_body_omits_missing_message() {
    assert_shape(
        ErrorBody {
            error: error_code::NOT_FOUND.into(),
            message: None,
            leader_id: None,
            leader_addr: None,
        },
        json!({"error": "not_found", "leader_id": null, "leader_addr": null}),
    );
}

#[test]
fn error_body_tolerates_missing_optional_fields() {
    let body: ErrorBody = serde_json::from_value(json!({"error": "internal"})).unwrap();
    assert_eq!(body, ErrorBody::new(error_code::INTERNAL));
}

#[test]
fn create_invite_request_shape() {
    assert_shape(
        CreateInviteRequest {
            name: "node-2".into(),
            endpoint: "203.0.113.7:51820".into(),
            ttl_secs: Some(600),
        },
        json!({"name": "node-2", "endpoint": "203.0.113.7:51820", "ttl_secs": 600}),
    );
    assert_shape(
        CreateInviteRequest {
            name: "node-2".into(),
            endpoint: "203.0.113.7".into(),
            ttl_secs: None,
        },
        json!({"name": "node-2", "endpoint": "203.0.113.7"}),
    );
}

#[test]
fn create_invite_request_requires_name_and_endpoint() {
    for partial in [
        json!({"name": "node-2", "ttl_secs": 60}),
        json!({"endpoint": "203.0.113.7", "ttl_secs": 60}),
    ] {
        assert!(
            serde_json::from_value::<CreateInviteRequest>(partial.clone()).is_err(),
            "{partial}"
        );
    }
}

#[test]
fn invite_response_shape() {
    assert_shape(
        InviteResponse {
            token: "faber1.abc".into(),
            node_id: 2,
            mesh_ip: "10.99.0.2".parse().unwrap(),
            expires_at: 1_700_000_000,
            name: "node-2".into(),
            replaces_node_id: None,
        },
        json!({"token": "faber1.abc", "node_id": 2, "mesh_ip": "10.99.0.2",
               "expires_at": 1_700_000_000u64, "name": "node-2"}),
    );
    assert_shape(
        InviteResponse {
            token: "faber1.abc".into(),
            node_id: 5,
            mesh_ip: "10.99.0.5".parse().unwrap(),
            expires_at: 1_700_000_000,
            name: "node-2".into(),
            replaces_node_id: Some(2),
        },
        json!({"token": "faber1.abc", "node_id": 5, "mesh_ip": "10.99.0.5",
               "expires_at": 1_700_000_000u64, "name": "node-2",
               "replaces_node_id": 2}),
    );
}

#[test]
fn lease_shapes() {
    assert_shape(
        LeaseResponse { lease_secs: 28_800 },
        json!({"lease_secs": 28_800}),
    );
    assert_shape(
        SetLeaseRequest { lease_secs: 600 },
        json!({"lease_secs": 600}),
    );
    assert!(serde_json::from_value::<SetLeaseRequest>(json!({})).is_err());
}

#[test]
fn invite_response_debug_redacts_token() {
    let r = InviteResponse {
        token: "faber1.secret-material".into(),
        node_id: 2,
        mesh_ip: "10.99.0.2".parse().unwrap(),
        expires_at: 1,
        name: "node-2".into(),
        replaces_node_id: None,
    };
    let dbg = format!("{r:?}");
    assert!(!dbg.contains("secret-material"), "{dbg}");
    assert!(dbg.contains("10.99.0.2"), "{dbg}");
}

#[test]
fn workload_constants() {
    assert_eq!(DNS_SUFFIX, "faber.local");
    assert_eq!(DEFAULT_ENVIRONMENTS, ["staging", "production"]);
    assert_eq!(
        fqdn("web", "staging", "shop"),
        "web.staging.shop.faber.local"
    );
    assert_eq!(faber_api_types::name::MAX_DNS_LABEL_LEN, 63);
}

/// The smallest container a client may send, as it is stored.
fn minimal_container_json(name: &str, image: &str) -> Value {
    json!({
        "name": name, "image": image, "command": [], "args": [], "env": {},
        "ports": [], "mounts": [],
        "resources": {"cpu_millis": 1000, "memory_bytes": 1_073_741_824},
        "capabilities": [], "read_only_root_fs": false
    })
}

fn minimal_spec() -> WorkloadSpec {
    WorkloadSpec {
        scale: Scale::Replicas {
            count: 1,
            strategy: Strategy::Spread,
        },
        restart: RestartPolicy::Always,
        expose: Exposure::Environment,
        placement: Placement { nodes: vec![] },
        stop_grace_secs: None,
        volumes: Default::default(),
        containers: vec![ContainerSpec::new("a", "b")],
    }
}

fn minimal_spec_json() -> Value {
    json!({
        "scale": {"replicas": {"count": 1, "strategy": "spread"}},
        "restart": "always", "expose": "environment",
        "placement": {"nodes": []}, "volumes": {},
        "containers": [minimal_container_json("a", "b")]
    })
}

#[test]
fn scale_shapes() {
    assert_shape(
        Scale::Replicas {
            count: 3,
            strategy: Strategy::Pack,
        },
        json!({"replicas": {"count": 3, "strategy": "pack"}}),
    );
    assert_shape(
        Scale::Replicas {
            count: 0,
            strategy: Strategy::Spread,
        },
        json!({"replicas": {"count": 0, "strategy": "spread"}}),
    );
    assert_shape(Scale::Daemon, json!("daemon"));
    assert_eq!(
        Scale::default(),
        Scale::Replicas {
            count: 1,
            strategy: Strategy::Spread
        }
    );
    // The strategy may be left out.
    assert_eq!(
        serde_json::from_value::<Scale>(json!({"replicas": {"count": 2}})).unwrap(),
        Scale::Replicas {
            count: 2,
            strategy: Strategy::Spread
        }
    );
    for bad in [
        json!(3),
        json!("replicas"),
        json!("per_node"),
        json!({"per_node": null}),
        json!({"replicas": 3}),
        json!({"replicas": {}}),
        json!({"replicas": {"count": -1}}),
        json!({"replicas": {"count": "3"}}),
        json!({"replicas": {"count": 1, "strategy": "random"}}),
        json!({"replicas": {"count": 1}, "daemon": null}),
        json!("Daemon"),
    ] {
        assert!(
            serde_json::from_value::<Scale>(bad.clone()).is_err(),
            "{bad}"
        );
    }
    assert_shape(Strategy::Spread, json!("spread"));
    assert_shape(Strategy::Pack, json!("pack"));
    assert_eq!(Strategy::default(), Strategy::Spread);
    assert!(serde_json::from_value::<Protocol>(json!("TCP")).is_err());

    assert_shape(HttpScheme::Http, json!("http"));
    assert_shape(HttpScheme::Https, json!("https"));
    assert_eq!(HttpScheme::default(), HttpScheme::Http);
    assert!(serde_json::from_value::<HttpScheme>(json!("HTTPS")).is_err());
}

#[test]
fn placement_shapes() {
    assert_shape(Placement { nodes: vec![] }, json!({"nodes": []}));
    assert_shape(
        Placement {
            nodes: vec!["node-1".into(), "node-2".into()],
        },
        json!({"nodes": ["node-1", "node-2"]}),
    );
    assert_eq!(
        serde_json::from_value::<Placement>(json!({})).unwrap(),
        Placement::default()
    );
}

#[test]
fn volume_source_shapes() {
    assert_shape(
        VolumeSource::Block {
            size_bytes: 1_073_741_824,
        },
        json!({"type": "block", "size_bytes": 1_073_741_824u64}),
    );
    assert_shape(
        VolumeSource::Ramdisk {
            size_bytes: 1_048_576,
        },
        json!({"type": "ramdisk", "size_bytes": 1_048_576}),
    );
}

#[test]
fn port_and_mount_shapes() {
    assert_shape(
        PortSpec {
            name: Some("dns".into()),
            port: 53,
            protocol: Protocol::Udp,
        },
        json!({"name": "dns", "port": 53, "protocol": "udp"}),
    );
    assert_shape(
        PortSpec {
            name: None,
            port: 80,
            protocol: Protocol::Tcp,
        },
        json!({"port": 80, "protocol": "tcp"}),
    );
    assert_shape(
        MountSpec {
            volume: "data".into(),
            path: "/data".into(),
            read_only: true,
        },
        json!({"volume": "data", "path": "/data", "read_only": true}),
    );
    assert_shape(
        MountSpec {
            volume: "data".into(),
            path: "/data".into(),
            read_only: false,
        },
        json!({"volume": "data", "path": "/data", "read_only": false}),
    );
}

#[test]
fn resources_shapes() {
    assert_shape(
        Resources::default(),
        json!({"cpu_millis": 1000, "memory_bytes": 1_073_741_824}),
    );
    assert_shape(
        Resources {
            cpu_millis: 500,
            memory_bytes: 268_435_456,
        },
        json!({"cpu_millis": 500, "memory_bytes": 268_435_456}),
    );
    // Each amount defaults on its own.
    assert_eq!(
        serde_json::from_value::<Resources>(json!({"cpu_millis": 250})).unwrap(),
        Resources {
            cpu_millis: 250,
            memory_bytes: 1_073_741_824,
        }
    );
}

#[test]
fn probe_shapes() {
    assert_shape(
        Probe::new(ProbeAction::Tcp { port: 5432 }),
        json!({
            "action": {"type": "tcp", "port": 5432},
            "interval_secs": 5, "timeout_secs": 1
        }),
    );
    assert_shape(
        Probe {
            action: ProbeAction::Tcp { port: 5432 },
            interval_secs: 30,
            timeout_secs: 3,
        },
        json!({
            "action": {"type": "tcp", "port": 5432},
            "interval_secs": 30, "timeout_secs": 3
        }),
    );
    assert_shape(
        ProbeAction::Http {
            port: 8080,
            path: "/ready".into(),
            scheme: HttpScheme::Https,
            host: Some("web.local".into()),
            headers: [("x-probe".to_string(), "1".to_string())].into(),
        },
        json!({
            "type": "http", "port": 8080, "path": "/ready", "scheme": "https",
            "host": "web.local", "headers": {"x-probe": "1"}
        }),
    );
    assert_shape(
        ProbeAction::Http {
            port: 80,
            path: "/".into(),
            scheme: HttpScheme::Http,
            host: None,
            headers: Default::default(),
        },
        json!({"type": "http", "port": 80, "path": "/", "scheme": "http", "headers": {}}),
    );
    assert_eq!(limits::PROBE_FAILURES_BEFORE_RESTART, 3);
}

#[test]
fn container_spec_shapes() {
    assert_shape(
        ContainerSpec::new("web", "nginx"),
        minimal_container_json("web", "nginx"),
    );
    let mut full = ContainerSpec::new("web", "nginx:1.27");
    full.command = vec!["nginx".into()];
    full.args = vec!["-g".into(), "daemon off;".into()];
    full.env = [("MODE".to_string(), "prod".to_string())].into();
    full.working_dir = Some("/srv".into());
    full.user = Some("101:101".into());
    full.ports = vec![PortSpec {
        name: Some("http".into()),
        port: 8080,
        protocol: Protocol::Tcp,
    }];
    full.mounts = vec![MountSpec {
        volume: "data".into(),
        path: "/data".into(),
        read_only: false,
    }];
    full.resources.cpu_millis = 500;
    full.capabilities = vec!["NET_BIND_SERVICE".into()];
    full.read_only_root_fs = true;
    full.probe = Some(Probe::new(ProbeAction::Tcp { port: 8080 }));
    assert_shape(
        full,
        json!({
            "name": "web", "image": "nginx:1.27",
            "command": ["nginx"], "args": ["-g", "daemon off;"],
            "env": {"MODE": "prod"},
            "working_dir": "/srv", "user": "101:101",
            "ports": [{"name": "http", "port": 8080, "protocol": "tcp"}],
            "mounts": [{"volume": "data", "path": "/data", "read_only": false}],
            "resources": {"cpu_millis": 500, "memory_bytes": 1_073_741_824},
            "capabilities": ["NET_BIND_SERVICE"],
            "read_only_root_fs": true,
            "probe": {
                "action": {"type": "tcp", "port": 8080},
                "interval_secs": 5, "timeout_secs": 1
            }
        }),
    );
}

#[test]
fn workload_spec_shapes() {
    assert_shape(minimal_spec(), minimal_spec_json());
    assert_shape(
        WorkloadSpec {
            scale: Scale::Daemon,
            restart: RestartPolicy::Never,
            expose: Exposure::Cluster,
            placement: Placement {
                nodes: vec!["node-1".into()],
            },
            stop_grace_secs: Some(30),
            volumes: [(
                "tmp".to_string(),
                VolumeSource::Ramdisk {
                    size_bytes: 1_048_576,
                },
            )]
            .into(),
            containers: vec![ContainerSpec::new("a", "b"), ContainerSpec::new("c", "d")],
        },
        json!({
            "scale": "daemon", "restart": "never", "expose": "cluster",
            "placement": {"nodes": ["node-1"]}, "stop_grace_secs": 30,
            "volumes": {"tmp": {"type": "ramdisk", "size_bytes": 1_048_576}},
            "containers": [minimal_container_json("a", "b"), minimal_container_json("c", "d")]
        }),
    );
}

/// Every field with a default may be left out, so a body written before
/// the field existed still decodes, and to the documented default.
#[test]
fn defaulted_fields_may_be_absent() {
    let spec: WorkloadSpec =
        serde_json::from_value(json!({"containers": [{"name": "a", "image": "b"}]})).unwrap();
    assert_eq!(spec, minimal_spec());
    assert_eq!(serde_json::to_value(&spec).unwrap(), minimal_spec_json());

    let decoded: PortSpec = serde_json::from_value(json!({"port": 53})).unwrap();
    assert_eq!(
        decoded,
        PortSpec {
            name: None,
            port: 53,
            protocol: Protocol::Tcp,
        }
    );
    let decoded: MountSpec =
        serde_json::from_value(json!({"volume": "data", "path": "/data"})).unwrap();
    assert_eq!(
        decoded,
        MountSpec {
            volume: "data".into(),
            path: "/data".into(),
            read_only: false,
        }
    );
    let decoded: Resources = serde_json::from_value(json!({})).unwrap();
    assert_eq!(decoded, Resources::default());

    let decoded: Probe =
        serde_json::from_value(json!({"action": {"type": "http", "port": 80}})).unwrap();
    assert_eq!(
        decoded,
        Probe {
            action: ProbeAction::Http {
                port: 80,
                path: "/".into(),
                scheme: HttpScheme::Http,
                host: None,
                headers: Default::default(),
            },
            interval_secs: 5,
            timeout_secs: 1,
        }
    );

    let decoded: CreateProjectRequest = serde_json::from_value(json!({"name": "shop"})).unwrap();
    assert_eq!(decoded.environments, None);
    let decoded: DeployRequest = serde_json::from_value(json!({"workloads": {}})).unwrap();
    assert!(!decoded.prune);
    let decoded: WorkloadQuery = serde_json::from_value(json!({})).unwrap();
    assert_eq!(decoded, WorkloadQuery::default());
}

#[test]
fn required_model_fields_are_required() {
    for bad in [
        json!({}),
        json!({"containers": [{"name": "a"}]}),
        json!({"containers": [{"image": "b"}]}),
        json!({"containers": [{"name": "a", "image": "b", "ports": [{}]}]}),
        json!({"containers": [{"name": "a", "image": "b", "mounts": [{"volume": "v"}]}]}),
        json!({"containers": [{"name": "a", "image": "b", "probe": {}}]}),
        json!({"containers": [{"name": "a", "image": "b"}], "volumes": {"v": {}}}),
        json!({"containers": [{"name": "a", "image": "b"}],
               "volumes": {"v": {"type": "block"}}}),
        json!({"containers": [{"name": "a", "image": "b"}],
               "volumes": {"v": {"type": "ramdisk"}}}),
    ] {
        assert!(
            serde_json::from_value::<WorkloadSpec>(bad.clone()).is_err(),
            "{bad}"
        );
    }
    for bad in [
        json!({"type": "exec", "command": ["true"]}),
        json!({"type": "http"}),
        json!({"type": "tcp"}),
        json!({"type": "grpc", "port": 1}),
        json!({"port": 1}),
    ] {
        assert!(
            serde_json::from_value::<ProbeAction>(bad.clone()).is_err(),
            "{bad}"
        );
    }
}

/// One refused body per model type, the tagged enums included.
#[test]
fn model_types_refuse_unknown_fields() {
    fn refused<T: DeserializeOwned>(body: Value) {
        assert!(serde_json::from_value::<T>(body.clone()).is_err(), "{body}");
    }
    refused::<WorkloadSpec>(json!({"containers": [{"name": "a", "image": "b"}], "replicas": 2}));
    refused::<Placement>(json!({"zone": "a"}));
    refused::<ContainerSpec>(json!({"name": "a", "image": "b", "privileged": true}));
    refused::<PortSpec>(json!({"port": 80, "host_port": 8080}));
    refused::<MountSpec>(json!({"volume": "v", "path": "/v", "propagation": "shared"}));
    refused::<Resources>(json!({"reservations": {}}));
    refused::<Resources>(json!({"limits": {}}));
    refused::<Probe>(json!({"action": {"type": "tcp", "port": 1}, "grpc": 1}));
    refused::<ProbeAction>(json!({"type": "tcp", "port": 1, "host": "x"}));
    refused::<ProbeAction>(json!({"type": "http", "port": 1, "method": "POST"}));
    refused::<VolumeSource>(json!({"type": "block", "size_bytes": 1048576, "path": "/srv"}));
    refused::<VolumeSource>(json!({"type": "ramdisk", "size_bytes": 1048576, "medium": "x"}));
    refused::<VolumeSource>(json!({"type": "host_path", "path": "/srv", "size_bytes": 1}));
    refused::<VolumeSource>(json!({"type": "ephemeral", "size_bytes": 1048576}));
    refused::<VolumeSource>(json!({"type": "nfs", "server": "x"}));
}

#[test]
fn project_shapes() {
    assert_shape(
        CreateProjectRequest {
            name: "shop".into(),
            environments: None,
        },
        json!({"name": "shop"}),
    );
    assert_shape(
        CreateProjectRequest {
            name: "shop".into(),
            environments: Some(vec!["dev".into()]),
        },
        json!({"name": "shop", "environments": ["dev"]}),
    );
    let project = ProjectResponse {
        name: "shop".into(),
        created_at: 1_700_000_000,
        environments: vec![EnvironmentSummary {
            name: "staging".into(),
            generation: 7,
            workloads: 2,
            bytes: 512,
            updated_at: 1_700_000_001,
        }],
    };
    let project_json = json!({
        "name": "shop", "created_at": 1_700_000_000u64,
        "environments": [{
            "name": "staging", "generation": 7, "workloads": 2, "bytes": 512,
            "updated_at": 1_700_000_001u64
        }]
    });
    assert_shape(project.clone(), project_json.clone());
    assert_shape(
        ProjectListResponse {
            projects: vec![project],
        },
        json!({"projects": [project_json]}),
    );
    assert_shape(
        ProjectListResponse { projects: vec![] },
        json!({"projects": []}),
    );
}

#[test]
fn deploy_shapes() {
    assert_shape(
        DeployRequest {
            workloads: [("web".to_string(), minimal_spec())].into(),
            prune: true,
        },
        json!({"workloads": {"web": minimal_spec_json()}, "prune": true}),
    );
    assert_shape(
        DeployRequest {
            workloads: Default::default(),
            prune: false,
        },
        json!({"workloads": {}, "prune": false}),
    );
    assert_shape(
        DeployResponse {
            generation: 9,
            created: vec!["web".into()],
            updated: vec!["api".into()],
            unchanged: vec!["db".into()],
            removed: vec!["old".into()],
        },
        json!({
            "generation": 9, "created": ["web"], "updated": ["api"],
            "unchanged": ["db"], "removed": ["old"]
        }),
    );
    assert_shape(
        DeployResponse::default(),
        json!({"generation": 0, "created": [], "updated": [], "unchanged": [], "removed": []}),
    );
}

#[test]
fn workload_route_shapes() {
    assert_shape(WorkloadQuery::default(), json!({}));
    assert_shape(
        WorkloadQuery {
            project: Some("shop".into()),
            environment: Some("staging".into()),
        },
        json!({"project": "shop", "environment": "staging"}),
    );
    let summary = WorkloadSummary {
        project: "shop".into(),
        environment: "staging".into(),
        name: "web".into(),
        fqdn: fqdn("web", "staging", "shop"),
        scale: Scale::Replicas {
            count: 3,
            strategy: Strategy::Spread,
        },
        expose: Exposure::Cluster,
        revision: 7,
        available: 2,
        desired: 3,
    };
    let summary_json = json!({
        "project": "shop", "environment": "staging", "name": "web",
        "fqdn": "web.staging.shop.faber.local",
        "scale": {"replicas": {"count": 3, "strategy": "spread"}},
        "expose": "cluster", "revision": 7, "available": 2, "desired": 3
    });
    assert_shape(summary.clone(), summary_json.clone());
    assert_shape(
        WorkloadListResponse {
            workloads: vec![summary],
        },
        json!({"workloads": [summary_json]}),
    );
    assert_shape(
        WorkloadResponse {
            project: "shop".into(),
            environment: "staging".into(),
            name: "a".into(),
            fqdn: fqdn("a", "staging", "shop"),
            revision: 7,
            generation: 9,
            spec: minimal_spec(),
            replicas: vec![
                ReplicaStatus {
                    node: Some(2),
                    replica_index: 0,
                    revision: 7,
                    state: ReplicaState::Available,
                    pod_ip: Some("10.200.2.5".parse().unwrap()),
                    reason: None,
                },
                ReplicaStatus {
                    node: Some(3),
                    replica_index: 1,
                    revision: 7,
                    state: ReplicaState::Unavailable,
                    pod_ip: None,
                    reason: None,
                },
                ReplicaStatus {
                    node: None,
                    replica_index: 2,
                    revision: 7,
                    state: ReplicaState::Unavailable,
                    pod_ip: None,
                    reason: Some("insufficient_capacity".into()),
                },
            ],
        },
        json!({
            "project": "shop", "environment": "staging", "name": "a",
            "fqdn": "a.staging.shop.faber.local", "revision": 7, "generation": 9,
            "spec": minimal_spec_json(),
            "replicas": [
                {"node": 2, "replica_index": 0, "revision": 7, "state": "available",
                 "pod_ip": "10.200.2.5"},
                {"node": 3, "replica_index": 1, "revision": 7, "state": "unavailable"},
                {"node": null, "replica_index": 2, "revision": 7, "state": "unavailable",
                 "reason": "insufficient_capacity"}
            ]
        }),
    );
    // A response from before replicas were reported still reads.
    let old: WorkloadResponse = serde_json::from_value(json!({
        "project": "shop", "environment": "staging", "name": "a",
        "fqdn": "a.staging.shop.faber.local", "revision": 7, "generation": 9,
        "spec": minimal_spec_json()
    }))
    .unwrap();
    assert!(old.replicas.is_empty());
    assert_shape(
        StatusIntervalResponse { interval_secs: 5 },
        json!({"interval_secs": 5}),
    );
    assert_shape(
        SetStatusIntervalRequest { interval_secs: 9 },
        json!({"interval_secs": 9}),
    );
}

#[test]
fn container_spec_debug_shows_env_names_and_no_value() {
    let mut container = ContainerSpec::new("web", "nginx");
    container.env = [
        ("API_KEY".to_string(), "hunter2".to_string()),
        ("MODE".to_string(), "prod-secret".to_string()),
    ]
    .into();
    let dbg = format!("{container:?}");
    assert!(dbg.contains("API_KEY") && dbg.contains("MODE"), "{dbg}");
    assert!(!dbg.contains("hunter2"), "{dbg}");
    assert!(!dbg.contains("prod-secret"), "{dbg}");

    // Also through the types that hold a container.
    let response = WorkloadResponse {
        project: "shop".into(),
        environment: "staging".into(),
        name: "web".into(),
        fqdn: fqdn("web", "staging", "shop"),
        revision: 1,
        generation: 1,
        spec: WorkloadSpec {
            containers: vec![container],
            ..minimal_spec()
        },
        replicas: Vec::new(),
    };
    let dbg = format!("{response:?}");
    assert!(dbg.contains("API_KEY"), "{dbg}");
    assert!(
        !dbg.contains("hunter2") && !dbg.contains("prod-secret"),
        "{dbg}"
    );
}
