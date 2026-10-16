use std::time::Duration;

use faber_api_types::workload::{
    CreateProjectRequest, DeployRequest, DeployResponse, Exposure, Scale, Strategy, WorkloadQuery,
};
use faber_api_types::*;
use serde_json::{Value, json};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> FaberClient {
    FaberClient::new(&server.uri()).expect("client")
}

fn status_body() -> Value {
    json!({
        "node_id": 1, "joined": true, "role": "leader", "term": 2,
        "leader_id": 1, "leader_addr": "10.8.0.1:7401",
        "members": [
            {"node_id": 1, "addr": "10.8.0.1:7401", "voter": true, "matched_index": 3, "lag": 0},
            {"node_id": 2, "addr": "10.8.0.2:7401", "voter": false, "matched_index": null, "lag": null}
        ]
    })
}

fn assert_api_error(err: ClientError, want_status: u16, want_code: &str) -> ErrorBody {
    match err {
        ClientError::Api { status, body } => {
            assert_eq!(status, want_status);
            assert_eq!(body.error, want_code);
            body
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn new_normalises_endpoint() {
    assert_eq!(
        FaberClient::new("http://127.0.0.1:7400/").unwrap().base(),
        "http://127.0.0.1:7400"
    );
    assert_eq!(
        FaberClient::new("127.0.0.1:7400").unwrap().base(),
        "http://127.0.0.1:7400"
    );
    assert_eq!(
        FaberClient::new("https://faber.local").unwrap().base(),
        "https://faber.local"
    );
}

#[tokio::test]
async fn health_ok() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"status": "ok", "node_id": 1})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).health().await.unwrap();
    assert_eq!(
        got,
        HealthResponse {
            status: "ok".into(),
            node_id: 1
        }
    );
}

#[tokio::test]
async fn cluster_status_ok() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/cluster/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(status_body()))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).cluster_status().await.unwrap();
    assert_eq!(got.role, Role::Leader);
    assert_eq!(got.leader_addr.as_deref(), Some("10.8.0.1:7401"));
    assert_eq!(got.members.len(), 2);
    assert!(!got.members[1].voter);
    assert_eq!(got.members[1].lag, None);
}

#[tokio::test]
async fn cluster_init_returns_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/init"))
        .respond_with(ResponseTemplate::new(200).set_body_json(status_body()))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).cluster_init().await.unwrap();
    assert!(got.joined);
    assert_eq!(got.leader_id, Some(1));
}

#[tokio::test]
async fn cluster_init_conflict_is_api_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/init"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": "already_initialized", "message": "cluster is already initialized",
            "leader_id": null, "leader_addr": null
        })))
        .mount(&server)
        .await;

    let err = client(&server).cluster_init().await.unwrap_err();
    let body = assert_api_error(err, 409, error_code::ALREADY_INITIALIZED);
    assert_eq!(
        body.message.as_deref(),
        Some("cluster is already initialized")
    );
}

#[tokio::test]
async fn cluster_join_posts_peer_and_returns_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/join"))
        .and(body_json(json!({"peer": "10.8.0.1:7401"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(status_body()))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).cluster_join("10.8.0.1:7401").await.unwrap();
    assert_eq!(got.node_id, 1);
    assert_eq!(got.members[0].matched_index, Some(3));
}

#[tokio::test]
async fn cluster_join_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/join"))
        .respond_with(ResponseTemplate::new(502).set_body_json(json!({
            "error": "peer_unreachable", "message": "cannot reach 10.8.0.9:7401",
            "leader_id": null, "leader_addr": null
        })))
        .mount(&server)
        .await;

    let err = client(&server)
        .cluster_join("10.8.0.9:7401")
        .await
        .unwrap_err();
    assert_api_error(err, 502, error_code::PEER_UNREACHABLE);
}

/// A node whose raft core has stopped answers health with 503; the client
/// reports it as an error with the node's own explanation.
#[tokio::test]
async fn unavailable_health_is_an_api_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "status": "unavailable", "node_id": 1,
            "error": "unavailable", "message": "the raft core is not running: raft stopped"
        })))
        .mount(&server)
        .await;

    let err = client(&server).health().await.unwrap_err();
    let body = assert_api_error(err, 503, error_code::UNAVAILABLE);
    assert_eq!(
        body.message.as_deref(),
        Some("the raft core is not running: raft stopped")
    );
}

/// A proxy or a limit layer may answer with plain text; the client keeps the
/// status and carries the text as the message, whichever call it was.
#[tokio::test]
async fn non_json_error_bodies_still_yield_api_errors() {
    let server = MockServer::start().await;
    for (verb, route, status, text) in [
        ("GET", "/api/v1/health", 500, "boom"),
        ("GET", "/api/v1/projects", 500, "boom"),
        ("POST", DEPLOY_PATH, 413, "length limit exceeded"),
        ("DELETE", WORKLOAD_PATH, 502, "bad gateway"),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(ResponseTemplate::new(status).set_body_string(text))
            .mount(&server)
            .await;
    }
    let c = client(&server);

    let errors = [
        (c.health().await.unwrap_err(), 500, "boom"),
        (c.project_list().await.unwrap_err(), 500, "boom"),
        (
            c.deploy("shop", "staging", &deploy_request())
                .await
                .unwrap_err(),
            413,
            "length limit exceeded",
        ),
        (
            c.workload_delete("shop", "staging", "web")
                .await
                .unwrap_err(),
            502,
            "bad gateway",
        ),
    ];
    for (err, status, text) in errors {
        let body = assert_api_error(err, status, error_code::INTERNAL);
        assert_eq!(body.message.as_deref(), Some(text));
    }
}

#[tokio::test]
async fn bad_success_body_is_decode_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let err = client(&server).health().await.unwrap_err();
    assert!(matches!(err, ClientError::Decode(_)), "got {err:?}");
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let server = MockServer::start().await;
    let elsewhere = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/cluster/status"))
        .respond_with(ResponseTemplate::new(307).insert_header(
            "location",
            format!("{}/api/v1/cluster/status", elsewhere.uri()),
        ))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(status_body()))
        .expect(0)
        .mount(&elsewhere)
        .await;

    let err = client(&server).cluster_status().await.unwrap_err();
    assert!(
        matches!(err, ClientError::Api { status: 307, .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn mesh_invite_posts_request_and_returns_invite() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .and(body_json(
            json!({"name": "node-2", "endpoint": "203.0.113.7:51820", "ttl_secs": 600}),
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "token": "faber1.abc", "node_id": 2, "mesh_ip": "10.99.0.2", "expires_at": 99,
            "name": "node-2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server)
        .mesh_invite(&CreateInviteRequest {
            name: "node-2".into(),
            endpoint: "203.0.113.7:51820".into(),
            ttl_secs: Some(600),
        })
        .await
        .unwrap();
    assert_eq!(got.token, "faber1.abc");
    assert_eq!(got.node_id, 2);
    assert_eq!(got.mesh_ip.to_string(), "10.99.0.2");
    assert_eq!(got.expires_at, 99);
    assert_eq!(got.name, "node-2");
    assert_eq!(got.replaces_node_id, None);
}

#[tokio::test]
async fn mesh_invite_reports_the_node_it_replaces() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "token": "faber1.abc", "node_id": 5, "mesh_ip": "10.99.0.5", "expires_at": 99,
            "name": "node-2", "replaces_node_id": 2
        })))
        .mount(&server)
        .await;

    let got = client(&server)
        .mesh_invite(&CreateInviteRequest {
            name: "node-2".into(),
            endpoint: "203.0.113.7".into(),
            ttl_secs: None,
        })
        .await
        .unwrap();
    assert_eq!(got.node_id, 5);
    assert_eq!(got.replaces_node_id, Some(2));
}

#[tokio::test]
async fn mesh_invite_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": "mesh_no_endpoints", "message": "no node has an endpoint",
            "leader_id": null, "leader_addr": null
        })))
        .mount(&server)
        .await;

    let err = client(&server)
        .mesh_invite(&CreateInviteRequest {
            name: "node-2".into(),
            endpoint: "203.0.113.7".into(),
            ttl_secs: None,
        })
        .await
        .unwrap_err();
    assert_api_error(err, 409, error_code::MESH_NO_ENDPOINTS);
}

#[tokio::test]
async fn mesh_lease_returns_the_lease() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/mesh/lease"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"lease_secs": 28_800})))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).mesh_lease().await.unwrap();
    assert_eq!(got, LeaseResponse { lease_secs: 28_800 });
}

#[tokio::test]
async fn mesh_lease_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/mesh/lease"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": "mesh_not_configured", "message": "the cluster has no mesh",
            "leader_id": null, "leader_addr": null
        })))
        .mount(&server)
        .await;

    let err = client(&server).mesh_lease().await.unwrap_err();
    assert_api_error(err, 409, error_code::MESH_NOT_CONFIGURED);
}

#[tokio::test]
async fn mesh_set_lease_puts_the_seconds_and_returns_the_lease() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/mesh/lease"))
        .and(body_json(json!({"lease_secs": 600})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"lease_secs": 600})))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).mesh_set_lease(600).await.unwrap();
    assert_eq!(got, LeaseResponse { lease_secs: 600 });
}

#[tokio::test]
async fn mesh_set_lease_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/mesh/lease"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "bad_request", "message": "lease_secs must be between 300 and 2592000",
            "leader_id": null, "leader_addr": null
        })))
        .mount(&server)
        .await;

    let err = client(&server).mesh_set_lease(1).await.unwrap_err();
    let body = assert_api_error(err, 400, error_code::BAD_REQUEST);
    assert_eq!(
        body.message.as_deref(),
        Some("lease_secs must be between 300 and 2592000")
    );
}

#[tokio::test]
async fn mesh_status_interval_returns_the_interval() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/mesh/status-interval"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"interval_secs": 5})))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).mesh_status_interval().await.unwrap();
    assert_eq!(got, StatusIntervalResponse { interval_secs: 5 });
}

#[tokio::test]
async fn mesh_set_status_interval_puts_the_seconds_and_returns_the_interval() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/mesh/status-interval"))
        .and(body_json(json!({"interval_secs": 9})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"interval_secs": 9})))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).mesh_set_status_interval(9).await.unwrap();
    assert_eq!(got, StatusIntervalResponse { interval_secs: 9 });
}

/// Read calls give up after `read`; calls that write through raft get the
/// longer `write`, because a join can take minutes on the server.
#[tokio::test]
async fn requests_time_out() {
    let server = MockServer::start().await;
    let slow = Duration::from_millis(600);
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": "ok", "node_id": 1}))
                .set_delay(slow),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/join"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(status_body())
                .set_delay(slow),
        )
        .mount(&server)
        .await;

    let timeouts = ClientTimeouts {
        read: Duration::from_millis(100),
        write: Duration::from_secs(20),
        ..ClientTimeouts::default()
    };
    let c = FaberClient::with_timeouts(&server.uri(), timeouts).unwrap();
    match c.health().await.unwrap_err() {
        ClientError::Http(e) => assert!(e.is_timeout(), "{e:?}"),
        other => panic!("unexpected error: {other:?}"),
    }
    // The slower answer still fits the write timeout.
    assert!(c.cluster_join("10.8.0.1:7401").await.unwrap().joined);

    let timeouts = ClientTimeouts {
        write: Duration::from_millis(100),
        ..ClientTimeouts::default()
    };
    let c = FaberClient::with_timeouts(&server.uri(), timeouts).unwrap();
    match c.cluster_join("10.8.0.1:7401").await.unwrap_err() {
        ClientError::Http(e) => assert!(e.is_timeout(), "{e:?}"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn default_timeouts_outlast_the_server() {
    let t = ClientTimeouts::default();
    assert_eq!(t.connect, Duration::from_secs(5));
    assert_eq!(t.read, Duration::from_secs(30));
    // A join waits up to 120 s for the leader and 10 s to settle.
    assert!(t.write > Duration::from_secs(130), "{t:?}");
}

fn project_body() -> Value {
    json!({
        "name": "shop", "created_at": 1_000,
        "environments": [
            {"name": "production", "generation": 4, "workloads": 0, "bytes": 0,
             "updated_at": 1_000},
            {"name": "staging", "generation": 7, "workloads": 2, "bytes": 512,
             "updated_at": 1_001}
        ]
    })
}

fn workload_body() -> Value {
    json!({
        "project": "shop", "environment": "staging", "name": "web",
        "fqdn": "web.staging.shop.faber.local", "revision": 7, "generation": 9,
        "spec": {"containers": [{"name": "web", "image": "nginx", "env": {"MODE": "prod"}}]}
    })
}

fn error_body(code: &str, message: &str) -> Value {
    json!({"error": code, "message": message, "leader_id": null, "leader_addr": null})
}

fn deploy_request() -> DeployRequest {
    serde_json::from_value(json!({
        "workloads": {"web": {"containers": [{"name": "web", "image": "nginx"}]}},
        "prune": true
    }))
    .expect("deploy request")
}

const WORKLOAD_PATH: &str = "/api/v1/projects/shop/environments/staging/workloads/web";
const DEPLOY_PATH: &str = "/api/v1/projects/shop/environments/staging/deploy";

/// The error names the argument and the rule, and nothing was sent.
async fn assert_refused(server: &MockServer, err: ClientError, want: &str) {
    match err {
        ClientError::Invalid(message) => assert_eq!(message, want),
        other => panic!("unexpected error: {other:?}"),
    }
    let received = server.received_requests().await.expect("recording is on");
    assert!(received.is_empty(), "sent {received:?}");
}

#[tokio::test]
async fn project_create_posts_request_and_returns_project() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/projects"))
        .and(body_json(
            json!({"name": "shop", "environments": ["staging", "production"]}),
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(project_body()))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server)
        .project_create(&CreateProjectRequest {
            name: "shop".into(),
            environments: Some(vec!["staging".into(), "production".into()]),
        })
        .await
        .unwrap();
    assert_eq!(got.name, "shop");
    assert_eq!(got.created_at, 1_000);
    assert_eq!(got.environments.len(), 2);
    assert_eq!(got.environments[1].name, "staging");
    assert_eq!(got.environments[1].generation, 7);
}

#[tokio::test]
async fn project_create_leaves_absent_environments_out() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/projects"))
        .and(body_json(json!({"name": "shop"})))
        .respond_with(ResponseTemplate::new(201).set_body_json(project_body()))
        .expect(1)
        .mount(&server)
        .await;

    let req = CreateProjectRequest {
        name: "shop".into(),
        environments: None,
    };
    client(&server).project_create(&req).await.unwrap();
}

#[tokio::test]
async fn project_create_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/projects"))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_json(error_body("project_exists", "project shop already exists")),
        )
        .mount(&server)
        .await;

    let req = CreateProjectRequest {
        name: "shop".into(),
        environments: None,
    };
    let err = client(&server).project_create(&req).await.unwrap_err();
    let body = assert_api_error(err, 409, error_code::PROJECT_EXISTS);
    assert_eq!(body.message.as_deref(), Some("project shop already exists"));
}

#[tokio::test]
async fn project_create_refuses_a_bad_name_before_sending() {
    let server = MockServer::start().await;
    let req = CreateProjectRequest {
        name: "My_Shop".into(),
        environments: None,
    };
    let err = client(&server).project_create(&req).await.unwrap_err();
    assert_refused(
        &server,
        err,
        "project name may only contain a-z, 0-9 and hyphens",
    )
    .await;
}

#[tokio::test]
async fn project_list_returns_the_projects() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/projects"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"projects": [project_body()]})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).project_list().await.unwrap();
    assert_eq!(got.projects.len(), 1);
    assert_eq!(got.projects[0].name, "shop");
    assert_eq!(got.projects[0].environments[0].name, "production");
}

#[tokio::test]
async fn project_get_returns_the_project() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/projects/shop"))
        .respond_with(ResponseTemplate::new(200).set_body_json(project_body()))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server).project_get("shop").await.unwrap();
    assert_eq!(got.name, "shop");
    assert_eq!(got.environments[1].workloads, 2);
    assert_eq!(got.environments[1].bytes, 512);
    assert_eq!(got.environments[1].updated_at, 1_001);
}

#[tokio::test]
async fn project_get_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/projects/shop"))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(error_body("not_found", "no project shop")),
        )
        .mount(&server)
        .await;

    let err = client(&server).project_get("shop").await.unwrap_err();
    let body = assert_api_error(err, 404, error_code::NOT_FOUND);
    assert_eq!(body.message.as_deref(), Some("no project shop"));
}

#[tokio::test]
async fn project_delete_accepts_204() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/projects/shop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    client(&server).project_delete("shop").await.unwrap();
    let received = server.received_requests().await.unwrap();
    assert!(received[0].body.is_empty());
}

/// A success answer is never decoded, so a body the server should not have
/// sent does no harm.
#[tokio::test]
async fn deletes_ignore_an_unexpected_success_body() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_string("deleted, not json"))
        .expect(3)
        .mount(&server)
        .await;

    let c = client(&server);
    c.project_delete("shop").await.unwrap();
    c.environment_remove("shop", "staging").await.unwrap();
    c.workload_delete("shop", "staging", "web").await.unwrap();
}

#[tokio::test]
async fn project_delete_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/projects/shop"))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_json(error_body("not_empty", "environment staging has workloads")),
        )
        .mount(&server)
        .await;

    let err = client(&server).project_delete("shop").await.unwrap_err();
    let body = assert_api_error(err, 409, error_code::NOT_EMPTY);
    assert_eq!(
        body.message.as_deref(),
        Some("environment staging has workloads")
    );
}

#[tokio::test]
async fn environment_add_puts_without_a_body_and_returns_the_project() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/projects/shop/environments/staging"))
        .respond_with(ResponseTemplate::new(200).set_body_json(project_body()))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server)
        .environment_add("shop", "staging")
        .await
        .unwrap();
    assert_eq!(got.name, "shop");
    assert_eq!(got.environments.len(), 2);
    let received = server.received_requests().await.unwrap();
    assert!(received[0].body.is_empty());
}

#[tokio::test]
async fn environment_add_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/projects/shop/environments/staging"))
        .respond_with(ResponseTemplate::new(409).set_body_json(error_body(
            "workloads_exhausted",
            "a project holds at most 16 environments",
        )))
        .mount(&server)
        .await;

    let err = client(&server)
        .environment_add("shop", "staging")
        .await
        .unwrap_err();
    assert_api_error(err, 409, error_code::WORKLOADS_EXHAUSTED);
}

#[tokio::test]
async fn environment_remove_accepts_204() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/projects/shop/environments/staging"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .environment_remove("shop", "staging")
        .await
        .unwrap();
}

#[tokio::test]
async fn environment_remove_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/projects/shop/environments/staging"))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_json(error_body("not_empty", "environment staging has workloads")),
        )
        .mount(&server)
        .await;

    let err = client(&server)
        .environment_remove("shop", "staging")
        .await
        .unwrap_err();
    assert_api_error(err, 409, error_code::NOT_EMPTY);
}

/// The body is the request as the model writes it: every default present.
#[tokio::test]
async fn deploy_posts_the_workloads_and_returns_the_result() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DEPLOY_PATH))
        .and(body_json(json!({
            "workloads": {"web": {
                "scale": {"replicas": {"count": 1, "strategy": "spread"}},
                "restart": "always",
                "expose": "environment",
                "placement": {"nodes": []},
                "volumes": {},
                "containers": [{
                    "name": "web", "image": "nginx", "command": [], "args": [], "env": {},
                    "ports": [], "mounts": [], "resources": {"cpu_millis": 1000, "memory_bytes": 1073741824},
                    "capabilities": [], "read_only_root_fs": false
                }]
            }},
            "prune": true
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "generation": 9, "created": ["web"], "updated": [], "unchanged": [],
            "removed": ["old"]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server)
        .deploy("shop", "staging", &deploy_request())
        .await
        .unwrap();
    assert_eq!(
        got,
        DeployResponse {
            generation: 9,
            created: vec!["web".into()],
            updated: vec![],
            unchanged: vec![],
            removed: vec!["old".into()],
        }
    );
}

#[tokio::test]
async fn deploy_errors_are_decoded() {
    for (status, code, message) in [
        (404, error_code::NOT_FOUND, "no environment staging"),
        (
            400,
            error_code::BAD_REQUEST,
            "web.containers[0].image: must not be empty",
        ),
        (
            409,
            error_code::WORKLOADS_EXHAUSTED,
            "an environment holds at most 64 workloads",
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(DEPLOY_PATH))
            .respond_with(ResponseTemplate::new(status).set_body_json(error_body(code, message)))
            .mount(&server)
            .await;

        let err = client(&server)
            .deploy("shop", "staging", &deploy_request())
            .await
            .unwrap_err();
        let body = assert_api_error(err, status, code);
        assert_eq!(body.message.as_deref(), Some(message));
    }
}

/// Only the filters that are set go into the query.
#[tokio::test]
async fn workload_list_sends_the_filters_that_are_set() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workloads"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"workloads": [{
                "project": "shop", "environment": "staging", "name": "web",
                "fqdn": "web.staging.shop.faber.local",
                "scale": {"replicas": {"count": 3, "strategy": "spread"}}, "expose": "cluster", "revision": 7,
                "available": 2, "desired": 3
            }]})),
        )
        .expect(4)
        .mount(&server)
        .await;

    let c = client(&server);
    let got = c
        .workload_list(&WorkloadQuery::default(), false)
        .await
        .unwrap();
    assert_eq!(got.workloads.len(), 1);
    assert_eq!(got.workloads[0].fqdn, "web.staging.shop.faber.local");
    assert_eq!(
        (got.workloads[0].available, got.workloads[0].desired),
        (2, 3)
    );
    assert_eq!(
        got.workloads[0].scale,
        Scale::Replicas {
            count: 3,
            strategy: Strategy::Spread
        }
    );
    assert_eq!(got.workloads[0].expose, Exposure::Cluster);
    assert_eq!(got.workloads[0].revision, 7);

    for (project, environment) in [
        (Some("shop"), None),
        (None, Some("staging")),
        (Some("shop"), Some("staging")),
    ] {
        let query = WorkloadQuery {
            project: project.map(String::from),
            environment: environment.map(String::from),
        };
        c.workload_list(&query, false).await.unwrap();
    }

    let received = server.received_requests().await.unwrap();
    let queries: Vec<Option<&str>> = received.iter().map(|r| r.url.query()).collect();
    assert_eq!(
        queries,
        [
            None,
            Some("project=shop"),
            Some("environment=staging"),
            Some("project=shop&environment=staging"),
        ]
    );
}

#[tokio::test]
async fn workload_list_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workloads"))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(error_body("not_found", "no project shop")),
        )
        .mount(&server)
        .await;

    let query = WorkloadQuery {
        project: Some("shop".into()),
        environment: None,
    };
    let err = client(&server)
        .workload_list(&query, false)
        .await
        .unwrap_err();
    assert_api_error(err, 404, error_code::NOT_FOUND);
}

#[tokio::test]
async fn workload_get_returns_the_workload() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(WORKLOAD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_body()))
        .expect(1)
        .mount(&server)
        .await;

    let got = client(&server)
        .workload_get("shop", "staging", "web", false)
        .await
        .unwrap();
    assert_eq!(got.project, "shop");
    assert_eq!(got.environment, "staging");
    assert_eq!(got.name, "web");
    assert_eq!(got.fqdn, "web.staging.shop.faber.local");
    assert_eq!(got.revision, 7);
    assert_eq!(got.generation, 9);
    assert_eq!(got.spec.containers[0].image, "nginx");
    assert_eq!(got.spec.containers[0].env["MODE"], "prod");
}

#[tokio::test]
async fn reveal_is_sent_only_when_asked_for() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(WORKLOAD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_body()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workloads"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"workloads": []})))
        .mount(&server)
        .await;

    let c = client(&server);
    c.workload_get("shop", "staging", "web", false)
        .await
        .unwrap();
    c.workload_get("shop", "staging", "web", true)
        .await
        .unwrap();
    let query = WorkloadQuery {
        project: Some("shop".into()),
        environment: None,
    };
    c.workload_list(&query, true).await.unwrap();

    let received = server.received_requests().await.unwrap();
    let queries: Vec<Option<&str>> = received.iter().map(|r| r.url.query()).collect();
    assert_eq!(
        queries,
        [None, Some("reveal=true"), Some("project=shop&reveal=true")]
    );
}

#[tokio::test]
async fn workload_get_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(WORKLOAD_PATH))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(error_body("not_found", "no workload web")),
        )
        .mount(&server)
        .await;

    let err = client(&server)
        .workload_get("shop", "staging", "web", false)
        .await
        .unwrap_err();
    let body = assert_api_error(err, 404, error_code::NOT_FOUND);
    assert_eq!(body.message.as_deref(), Some("no workload web"));
}

#[tokio::test]
async fn workload_delete_accepts_204() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(WORKLOAD_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .workload_delete("shop", "staging", "web")
        .await
        .unwrap();
}

#[tokio::test]
async fn workload_delete_errors_are_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(WORKLOAD_PATH))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(error_body("not_found", "no workload web")),
        )
        .mount(&server)
        .await;

    let err = client(&server)
        .workload_delete("shop", "staging", "web")
        .await
        .unwrap_err();
    assert_api_error(err, 404, error_code::NOT_FOUND);
}

/// Every argument that becomes part of a URL is refused, with its own name
/// in the error, when it is not a DNS label.
#[tokio::test]
async fn names_are_checked_before_anything_is_sent() {
    let server = MockServer::start().await;
    let c = client(&server);
    let bad = "../x";
    let chars = "name may only contain a-z, 0-9 and hyphens";
    let project = format!("project {chars}");
    let environment = format!("environment {chars}");
    let workload = format!("workload {chars}");
    let req = deploy_request();

    let err = c.project_get(bad).await.unwrap_err();
    assert_refused(&server, err, &project).await;
    let err = c.project_delete(bad).await.unwrap_err();
    assert_refused(&server, err, &project).await;

    let err = c.environment_add(bad, "staging").await.unwrap_err();
    assert_refused(&server, err, &project).await;
    let err = c.environment_add("shop", bad).await.unwrap_err();
    assert_refused(&server, err, &environment).await;
    let err = c.environment_remove(bad, "staging").await.unwrap_err();
    assert_refused(&server, err, &project).await;
    let err = c.environment_remove("shop", bad).await.unwrap_err();
    assert_refused(&server, err, &environment).await;

    let err = c.deploy(bad, "staging", &req).await.unwrap_err();
    assert_refused(&server, err, &project).await;
    let err = c.deploy("shop", bad, &req).await.unwrap_err();
    assert_refused(&server, err, &environment).await;

    let err = c
        .workload_get(bad, "staging", "web", false)
        .await
        .unwrap_err();
    assert_refused(&server, err, &project).await;
    let err = c.workload_get("shop", bad, "web", false).await.unwrap_err();
    assert_refused(&server, err, &environment).await;
    let err = c
        .workload_get("shop", "staging", bad, false)
        .await
        .unwrap_err();
    assert_refused(&server, err, &workload).await;
    let err = c.workload_delete(bad, "staging", "web").await.unwrap_err();
    assert_refused(&server, err, &project).await;
    let err = c.workload_delete("shop", bad, "web").await.unwrap_err();
    assert_refused(&server, err, &environment).await;
    let err = c.workload_delete("shop", "staging", bad).await.unwrap_err();
    assert_refused(&server, err, &workload).await;

    let query = WorkloadQuery {
        project: Some("shop&environment=production".into()),
        environment: None,
    };
    let err = c.workload_list(&query, false).await.unwrap_err();
    assert_refused(&server, err, &project).await;
    let query = WorkloadQuery {
        project: None,
        environment: Some("a b".into()),
    };
    let err = c.workload_list(&query, false).await.unwrap_err();
    assert_refused(&server, err, &environment).await;
}

/// The other rules of a DNS label reach the caller with their own message.
#[tokio::test]
async fn each_name_rule_is_reported() {
    let server = MockServer::start().await;
    let c = client(&server);
    let too_long = "a".repeat(64);
    for (name, want) in [
        ("", "project name must be between 1 and 63 characters"),
        (
            too_long.as_str(),
            "project name must be between 1 and 63 characters",
        ),
        ("-shop", "project name must not start or end with a hyphen"),
        (
            "a?b=c",
            "project name may only contain a-z, 0-9 and hyphens",
        ),
        (
            "a%2fb",
            "project name may only contain a-z, 0-9 and hyphens",
        ),
        ("a#b", "project name may only contain a-z, 0-9 and hyphens"),
        ("..", "project name may only contain a-z, 0-9 and hyphens"),
    ] {
        let err = c.project_get(name).await.unwrap_err();
        assert_eq!(err.to_string(), format!("invalid argument: {want}"));
        assert_refused(&server, err, want).await;
    }
}

/// The project and workload reads use the read timeout, and every write,
/// with or without a body, the write timeout.
#[tokio::test]
async fn project_and_workload_calls_time_out_by_kind() {
    fn assert_timeout<T: std::fmt::Debug>(result: Result<T, ClientError>) {
        match result.unwrap_err() {
            ClientError::Http(e) => assert!(e.is_timeout(), "{e:?}"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    let server = MockServer::start().await;
    let slow = Duration::from_millis(600);
    let deployed = json!({
        "generation": 9, "created": [], "updated": [], "unchanged": ["web"], "removed": []
    });
    for (verb, at, status, body) in [
        ("GET", "/api/v1/projects", 200, json!({"projects": []})),
        ("GET", "/api/v1/projects/shop", 200, project_body()),
        ("GET", "/api/v1/workloads", 200, json!({"workloads": []})),
        ("GET", WORKLOAD_PATH, 200, workload_body()),
        ("POST", "/api/v1/projects", 201, project_body()),
        (
            "PUT",
            "/api/v1/projects/shop/environments/staging",
            200,
            project_body(),
        ),
        ("POST", DEPLOY_PATH, 200, deployed),
    ] {
        Mock::given(method(verb))
            .and(path(at))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_json(body)
                    .set_delay(slow),
            )
            .mount(&server)
            .await;
    }
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204).set_delay(slow))
        .mount(&server)
        .await;

    let create = CreateProjectRequest {
        name: "shop".into(),
        environments: None,
    };
    let deploy = deploy_request();
    let all = WorkloadQuery::default();

    let timeouts = ClientTimeouts {
        read: Duration::from_millis(100),
        write: Duration::from_secs(20),
        ..ClientTimeouts::default()
    };
    let c = FaberClient::with_timeouts(&server.uri(), timeouts).unwrap();
    assert_timeout(c.project_list().await);
    assert_timeout(c.project_get("shop").await);
    assert_timeout(c.workload_list(&all, false).await);
    assert_timeout(c.workload_get("shop", "staging", "web", false).await);
    // The slower answers still fit the write timeout.
    c.project_create(&create).await.unwrap();
    c.project_delete("shop").await.unwrap();
    c.environment_add("shop", "staging").await.unwrap();
    c.environment_remove("shop", "staging").await.unwrap();
    c.deploy("shop", "staging", &deploy).await.unwrap();
    c.workload_delete("shop", "staging", "web").await.unwrap();

    let timeouts = ClientTimeouts {
        read: Duration::from_secs(20),
        write: Duration::from_millis(100),
        ..ClientTimeouts::default()
    };
    let c = FaberClient::with_timeouts(&server.uri(), timeouts).unwrap();
    assert_timeout(c.project_create(&create).await);
    assert_timeout(c.project_delete("shop").await);
    assert_timeout(c.environment_add("shop", "staging").await);
    assert_timeout(c.environment_remove("shop", "staging").await);
    assert_timeout(c.deploy("shop", "staging", &deploy).await);
    assert_timeout(c.workload_delete("shop", "staging", "web").await);
    assert!(c.project_list().await.unwrap().projects.is_empty());
    assert!(
        c.workload_list(&all, false)
            .await
            .unwrap()
            .workloads
            .is_empty()
    );
}
