//! REST API tests: `/api/v1` routes through `tower::ServiceExt::oneshot`, and
//! which paths each listener serves.

mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use faber_api_types::{ClusterStatus, ErrorBody, HealthResponse, error_code};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use tower::ServiceExt;

use common::TestNode;

fn router(node: &TestNode) -> Router {
    fabercp::api::router(node.handle().service().clone())
}

async fn call(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, Vec<u8>) {
    let mut req = Request::builder().method(method).uri(uri);
    if body.is_some() {
        req = req.header(header::CONTENT_TYPE, "application/json");
    }
    let req = req
        .body(
            body.map(|b| Body::from(b.to_string()))
                .unwrap_or_else(Body::empty),
        )
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, bytes)
}

fn parse<T: DeserializeOwned>(bytes: &[u8]) -> T {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(bytes)))
}

#[track_caller]
fn assert_error(status: StatusCode, bytes: &[u8], want_status: StatusCode, code: &str) {
    assert_eq!(status, want_status, "{}", String::from_utf8_lossy(bytes));
    assert_eq!(parse::<ErrorBody>(bytes).error, code);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_reports_node_id() {
    let mut node = TestNode::start(7).await;
    let (s, b) = call(&router(&node), Method::GET, "/api/v1/health", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        parse::<HealthResponse>(&b),
        HealthResponse {
            status: "ok".into(),
            node_id: 7
        }
    );
    node.stop().await;
}

/// A node whose raft core has stopped still serves HTTP. Health must say so
/// with a status code that a probe understands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_is_503_once_the_raft_core_has_stopped() {
    let mut node = TestNode::start(7).await;
    let app = router(&node);
    node.handle().raft().shutdown().await.unwrap();

    let (s, b) = call(&app, Method::GET, "/api/v1/health", None).await;
    assert_eq!(
        s,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        String::from_utf8_lossy(&b)
    );
    // The usual health fields, so a reader of that shape still decodes it.
    assert_eq!(
        parse::<HealthResponse>(&b),
        HealthResponse {
            status: "unavailable".into(),
            node_id: 7
        }
    );
    // And the error fields, which is what the CLI prints.
    let error: ErrorBody = parse(&b);
    assert_eq!(error.error, error_code::UNAVAILABLE);
    assert!(
        error
            .message
            .as_deref()
            .is_some_and(|m| m.contains("raft core is not running")),
        "{error:?}"
    );
    drop(app);
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_of_uninitialized_node() {
    let mut node = TestNode::start(1).await;
    let (s, b) = call(&router(&node), Method::GET, "/api/v1/cluster/status", None).await;
    assert_eq!(s, StatusCode::OK);
    let status: ClusterStatus = parse(&b);
    assert_eq!(status.node_id, 1);
    assert!(!status.joined);
    assert_eq!(status.role, "learner");
    assert_eq!(status.leader_id, None);
    assert_eq!(status.leader_addr, None);
    assert!(status.members.is_empty());
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_then_status_then_conflicts() {
    let mut node = TestNode::start(1).await;
    let app = router(&node);

    // Init takes no body; one is ignored if sent.
    let (s, b) = call(&app, Method::POST, "/api/v1/cluster/init", Some("{}")).await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&b));
    let status: ClusterStatus = parse(&b);
    assert!(status.joined);
    assert_eq!(status.role, "leader");
    assert_eq!(status.leader_id, Some(1));
    assert_eq!(status.leader_addr, Some(node.peer_addr().to_string()));
    assert_eq!(status.members.len(), 1);
    assert_eq!(status.members[0].addr, node.peer_addr().to_string());
    assert!(status.members[0].voter);

    let (s, b) = call(&app, Method::POST, "/api/v1/cluster/init", None).await;
    assert_error(s, &b, StatusCode::CONFLICT, error_code::ALREADY_INITIALIZED);

    let (s, b) = call(
        &app,
        Method::POST,
        "/api/v1/cluster/join",
        Some(r#"{"peer":"127.0.0.1:1"}"#),
    )
    .await;
    assert_error(s, &b, StatusCode::CONFLICT, error_code::ALREADY_JOINED);

    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_errors() {
    let mut node = TestNode::start(2).await;
    let app = router(&node);

    for bad in ["{not json", "{}", r#"{"peer":""}"#, r#"{"peer":"a/b"}"#] {
        let (s, b) = call(&app, Method::POST, "/api/v1/cluster/join", Some(bad)).await;
        assert_error(s, &b, StatusCode::BAD_REQUEST, error_code::BAD_REQUEST);
    }

    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = closed.local_addr().unwrap();
    drop(closed);
    let body = format!(r#"{{"peer":"{dead}"}}"#);
    let (s, b) = call(&app, Method::POST, "/api/v1/cluster/join", Some(&body)).await;
    assert_error(s, &b, StatusCode::BAD_GATEWAY, error_code::PEER_UNREACHABLE);

    // A peer that is up but has no leader answers 503, passed through.
    let mut lonely = TestNode::start(3).await;
    let body = format!(r#"{{"peer":"{}"}}"#, lonely.peer_addr());
    let (s, b) = call(&app, Method::POST, "/api/v1/cluster/join", Some(&body)).await;
    assert_error(
        s,
        &b,
        StatusCode::SERVICE_UNAVAILABLE,
        error_code::NO_LEADER,
    );

    lonely.stop().await;
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_and_unknown_routes_are_json_404s() {
    let mut node = TestNode::start(1).await;
    let app = router(&node);
    for path in [
        "/api/v1/nope",
        "/api/v1/cluster/learners",
        "/api/v1/cluster/membership",
    ] {
        let (s, b) = call(&app, Method::GET, path, None).await;
        assert_error(s, &b, StatusCode::NOT_FOUND, error_code::NOT_FOUND);
    }
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listeners_serve_the_right_paths() {
    let mut node = TestNode::start(1).await;
    let http = common::http();

    for addr in [node.api_addr(), node.peer_addr()] {
        let resp = http
            .get(format!("http://{addr}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "health on {addr}");
    }

    // /raft exists only on the peer listener.
    let local = http
        .get(format!("http://{}/raft/cluster/status", node.api_addr()))
        .send()
        .await
        .unwrap();
    assert_eq!(local.status(), StatusCode::NOT_FOUND);
    let local = http
        .post(format!("http://{}/raft/vote", node.api_addr()))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(local.status(), StatusCode::NOT_FOUND);

    let peer = http
        .get(format!("http://{}/raft/cluster/status", node.peer_addr()))
        .send()
        .await
        .unwrap();
    assert_eq!(peer.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        peer.json::<ErrorBody>().await.unwrap().error,
        error_code::NO_LEADER
    );

    node.stop().await;
}

/// The project and workload routes answer on the API listener only: a spec
/// carries env values, and the peer listener is open to the whole mesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_workload_routes_are_served_on_the_api_listener_only() {
    let mut node = common::single_node().await;
    let http = common::http();
    let staging = "/api/v1/projects/shop/environments/staging";
    let web = format!("{staging}/workloads/web");
    let deploy = serde_json::json!({"workloads": {"web": {"containers": [
        {"name": "main", "image": "nginx", "env": {"TOKEN": "s3cret"}},
    ]}}});
    // Every public route once, in an order in which each one succeeds.
    let routes = [
        (
            Method::POST,
            "/api/v1/projects".to_string(),
            Some(serde_json::json!({"name": "shop"})),
            StatusCode::CREATED,
        ),
        (
            Method::GET,
            "/api/v1/projects".to_string(),
            None,
            StatusCode::OK,
        ),
        (
            Method::GET,
            "/api/v1/projects/shop".to_string(),
            None,
            StatusCode::OK,
        ),
        (
            Method::PUT,
            "/api/v1/projects/shop/environments/dev".to_string(),
            None,
            StatusCode::OK,
        ),
        (
            Method::POST,
            format!("{staging}/deploy"),
            Some(deploy),
            StatusCode::OK,
        ),
        (
            Method::GET,
            "/api/v1/workloads?project=shop".to_string(),
            None,
            StatusCode::OK,
        ),
        (Method::GET, web.clone(), None, StatusCode::OK),
        (Method::DELETE, web, None, StatusCode::NO_CONTENT),
        (
            Method::DELETE,
            "/api/v1/projects/shop/environments/dev".to_string(),
            None,
            StatusCode::NO_CONTENT,
        ),
        (
            Method::DELETE,
            "/api/v1/projects/shop".to_string(),
            None,
            StatusCode::NO_CONTENT,
        ),
    ];
    for (method, path, body, want) in routes {
        let send = |addr: std::net::SocketAddr| {
            let mut rb = http.request(method.clone(), format!("http://{addr}{path}"));
            if let Some(body) = &body {
                rb = rb.json(body);
            }
            rb.send()
        };
        // The peer listener first: it must not have done the write.
        let resp = send(node.peer_addr()).await.unwrap();
        let (status, bytes) = (resp.status(), resp.bytes().await.unwrap());
        assert_error(status, &bytes, StatusCode::NOT_FOUND, error_code::NOT_FOUND);

        let resp = send(node.api_addr()).await.unwrap();
        let (status, bytes) = (resp.status(), resp.bytes().await.unwrap());
        assert_eq!(
            status,
            want,
            "{method} {path}: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(bytes.is_empty(), want == StatusCode::NO_CONTENT, "{path}");
    }

    // The route a follower forwards to exists on the peer listener only.
    let create = serde_json::json!({
        "op": "create_project", "name": "shop", "environments": ["staging"],
    });
    let internal = |addr: std::net::SocketAddr| {
        http.post(format!("http://{addr}/raft/cluster/workloads"))
            .json(&create)
            .send()
    };
    let resp = internal(node.api_addr()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let resp = internal(node.peer_addr()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["index"].as_u64().is_some_and(|index| index > 0),
        "{body}"
    );
    assert_eq!(body.get("applied"), None);

    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_loopback_api_listener_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    // Any non-loopback address will do; binding 0.0.0.0 is enough to be refused.
    let api = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let err = fabercp::start(common::fast_config(1, dir.path()), api, peer)
        .await
        .err()
        .expect("start must fail");
    assert!(err.to_string().contains("loopback"), "{err}");

    let api = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let err = fabercp::start(common::fast_config(1, dir.path()), api, peer)
        .await
        .err()
        .expect("start must fail");
    assert!(err.to_string().contains("WireGuard"), "{err}");
}
