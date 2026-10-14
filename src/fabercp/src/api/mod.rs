//! REST API under `/api/v1`. [`router`] is served on both the local and the
//! peer listener; [`local_router`] adds the routes that only the loopback
//! listener serves.
//!
//! TODO(security): no authentication or authorisation. Access control relies
//! on the listeners being loopback or WireGuard only, and traffic is plain
//! HTTP (WireGuard provides transport encryption between nodes).

mod apps;
mod cluster;
mod mesh;

use axum::Router;
use axum::routing::{get, post};

use crate::error::not_found;
use crate::service::FaberService;

fn base_routes() -> Router<FaberService> {
    Router::new()
        .route("/health", get(cluster::health))
        .route("/cluster/status", get(cluster::status))
        .route("/cluster/init", post(cluster::init))
        .route("/cluster/join", post(cluster::join))
        .fallback(not_found)
}

/// Routes mounted at `/api/v1`, for both listeners.
pub fn router(service: FaberService) -> Router {
    Router::new().nest("/api/v1", base_routes().with_state(service))
}

/// [`router`] plus the loopback-only routes:
///
/// - `POST /api/v1/mesh/invites` returns a private key, so it must never be
///   reachable over the mesh.
/// - `GET` and `PUT /api/v1/mesh/lease` read and set how long a node's lease
///   lasts. Setting it is an admin action on the whole cluster.
/// - `GET` and `PUT /api/v1/mesh/status-interval` read and set how often
///   nodes report their status, for the whole cluster.
/// - The project and workload routes ([`apps::routes`]): a spec carries env
///   values, and the peer listener also admits machines that only hold an
///   invite.
///
/// TODO(security): the project and workload routes have no authentication
/// either. Whoever reaches the loopback API can deploy.
pub fn local_router(service: FaberService) -> Router {
    let api = base_routes()
        .route("/mesh/invites", post(mesh::create_invite))
        .route("/mesh/lease", get(mesh::lease).put(mesh::set_lease))
        .route(
            "/mesh/status-interval",
            get(mesh::status_interval).put(mesh::set_status_interval),
        )
        .merge(apps::routes())
        .with_state(service);
    Router::new().nest("/api/v1", api)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Method, Request, header};
    use axum::middleware::from_fn_with_state;
    use faber_api_types::{ErrorBody, LeaseResponse, error_code};
    use faber_container_runtime::ContainerRuntimeSpec;
    use http_body_util::BodyExt;
    use serde::de::DeserializeOwned;
    use tower::ServiceExt;

    use super::*;
    use crate::guard::{HostGuard, host_guard};
    use crate::mesh::{MeshConfig, NodeRecord, generate_keypair};
    use crate::raft::types::Request as Command;
    use crate::testing::TestNode;

    /// The loopback listener's routes behind its guard, as `start` builds
    /// them.
    pub(super) fn local(node: &TestNode) -> Router {
        local_router(node.service.clone()).layer(from_fn_with_state(HostGuard::local(), host_guard))
    }

    /// The peer listener's routes behind its guard.
    pub(super) fn peer(node: &TestNode) -> Router {
        let source: SocketAddr = "127.0.0.1:40000".parse().unwrap();
        router(node.service.clone())
            .merge(crate::peer::router(node.service.clone()))
            .layer(from_fn_with_state(
                HostGuard::peer(node.peer_addr.ip()),
                host_guard,
            ))
            .layer(MockConnectInfo(source))
    }

    fn lease_request(method: Method, host: &str, body: Option<&str>) -> Request<Body> {
        let rb = Request::builder()
            .method(method)
            .uri("/api/v1/mesh/lease")
            .header(header::HOST, host);
        match body {
            Some(json) => rb
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json.to_string())),
            None => rb.body(Body::empty()),
        }
        .unwrap()
    }

    pub(super) async fn send<T: DeserializeOwned>(app: Router, req: Request<Body>) -> (u16, T) {
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status().as_u16();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("{status} {e}: {}", String::from_utf8_lossy(&bytes)));
        (status, body)
    }

    async fn seed_mesh(node: &TestNode) {
        let (_, public_key) = generate_keypair().unwrap();
        node.service
            .propose(Command::InitCluster {
                mesh: MeshConfig::default(),
                founder: NodeRecord {
                    node_id: 1,
                    public_key,
                    mesh_ip: "10.99.0.1".parse().unwrap(),
                    endpoint: Some("203.0.113.1:51820".into()),
                    invite_id: None,
                    instance_id: None,
                    container_subnet: None,
                    name: "node-1".into(),
                    lease_renewed_at: 0,
                },
                containerd: ContainerRuntimeSpec::default(),
            })
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_lease_routes_read_and_set_the_lease() {
        let node = TestNode::start_initialized().await;
        let get_lease = || lease_request(Method::GET, "127.0.0.1:7400", None);
        let put = |body: &str| lease_request(Method::PUT, "localhost:7400", Some(body));

        // No mesh yet.
        let (status, body): (_, ErrorBody) = send(local(&node), get_lease()).await;
        assert_eq!(
            (status, body.error.as_str()),
            (409, error_code::MESH_NOT_CONFIGURED)
        );

        seed_mesh(&node).await;
        let (status, body): (_, LeaseResponse) = send(local(&node), get_lease()).await;
        assert_eq!((status, body.lease_secs), (200, 28_800));
        let (status, body): (_, LeaseResponse) =
            send(local(&node), put(r#"{"lease_secs": 600}"#)).await;
        assert_eq!((status, body.lease_secs), (200, 600));
        let (status, body): (_, LeaseResponse) = send(local(&node), get_lease()).await;
        assert_eq!((status, body.lease_secs), (200, 600));

        // Out of bounds, and not a lease at all.
        for bad in [r#"{"lease_secs": 299}"#, r#"{"lease_secs": "soon"}"#, "{}"] {
            let (status, body): (_, ErrorBody) = send(local(&node), put(bad)).await;
            assert_eq!(
                (status, body.error.as_str()),
                (400, error_code::BAD_REQUEST),
                "{bad}"
            );
        }
        let (_, body): (_, LeaseResponse) = send(local(&node), get_lease()).await;
        assert_eq!(body.lease_secs, 600);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_lease_routes_are_guarded_like_the_invite_route() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        let body = r#"{"lease_secs": 600}"#;

        // Not served on the peer listener, whatever the method.
        let host = node.peer_addr.to_string();
        for method in [Method::GET, Method::PUT] {
            let req = lease_request(method.clone(), &host, Some(body));
            let (status, err): (_, ErrorBody) = send(peer(&node), req).await;
            assert_eq!(
                (status, err.error.as_str()),
                (404, error_code::NOT_FOUND),
                "{method}"
            );
        }

        for method in [Method::GET, Method::PUT] {
            // A Host that is not loopback: a rebound DNS name.
            let req = lease_request(method.clone(), "evil.example:7400", Some(body));
            let (status, err): (_, ErrorBody) = send(local(&node), req).await;
            assert_eq!((status, err.error.as_str()), (403, error_code::FORBIDDEN));

            // Any Origin: a browser page.
            let mut req = lease_request(method.clone(), "127.0.0.1:7400", Some(body));
            req.headers_mut()
                .insert(header::ORIGIN, "http://127.0.0.1:7400".parse().unwrap());
            let (status, err): (_, ErrorBody) = send(local(&node), req).await;
            assert_eq!((status, err.error.as_str()), (403, error_code::FORBIDDEN));
        }
        // None of them changed the lease.
        assert_eq!(node.service.lease().await.unwrap().lease_secs, 28_800);
        node.shutdown().await;
    }

    /// The internal lease route on the peer listener is what a follower
    /// forwards to.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_peer_lease_route_sets_the_lease() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        let post_lease = |secs: u64| {
            Request::builder()
                .method(Method::POST)
                .uri("/raft/cluster/lease")
                .header(header::HOST, node.peer_addr.to_string())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"lease_secs": {secs}}}"#)))
                .unwrap()
        };
        let (status, body): (_, LeaseResponse) = send(peer(&node), post_lease(600)).await;
        assert_eq!((status, body.lease_secs), (200, 600));
        let (status, err): (_, ErrorBody) = send(peer(&node), post_lease(1)).await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));
        // The loopback listener has no internal routes.
        let resp = local(&node).oneshot(post_lease(600)).await.unwrap();
        assert_ne!(resp.status().as_u16(), 200);
        node.shutdown().await;
    }

    fn json_request(method: Method, uri: &str, host: &str, body: Option<&str>) -> Request<Body> {
        let rb = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, host);
        match body {
            Some(json) => rb
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json.to_string())),
            None => rb.body(Body::empty()),
        }
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_status_interval_routes_read_and_set_the_interval() {
        use faber_api_types::StatusIntervalResponse;

        let node = TestNode::start_initialized().await;
        let uri = "/api/v1/mesh/status-interval";
        let get = || json_request(Method::GET, uri, "127.0.0.1:7400", None);
        let put = |body: &str| json_request(Method::PUT, uri, "localhost:7400", Some(body));

        let (status, body): (_, StatusIntervalResponse) = send(local(&node), get()).await;
        assert_eq!((status, body.interval_secs), (200, 5));
        let (status, body): (_, StatusIntervalResponse) =
            send(local(&node), put(r#"{"interval_secs": 30}"#)).await;
        assert_eq!((status, body.interval_secs), (200, 30));
        let (_, body): (_, StatusIntervalResponse) = send(local(&node), get()).await;
        assert_eq!(body.interval_secs, 30);

        for bad in [
            r#"{"interval_secs": 0}"#,
            r#"{"interval_secs": 301}"#,
            r#"{"interval_secs": "soon"}"#,
        ] {
            let (status, body): (_, ErrorBody) = send(local(&node), put(bad)).await;
            assert_eq!(
                (status, body.error.as_str()),
                (400, error_code::BAD_REQUEST),
                "{bad}"
            );
        }
        let (_, body): (_, StatusIntervalResponse) = send(local(&node), get()).await;
        assert_eq!(body.interval_secs, 30);

        // Loopback listener only.
        let host = node.peer_addr.to_string();
        let req = json_request(Method::GET, uri, &host, None);
        let (status, _): (_, ErrorBody) = send(peer(&node), req).await;
        assert_eq!(status, 404);
        node.shutdown().await;
    }

    /// The internal routes on the peer listener are what a follower forwards
    /// to.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_peer_status_routes_set_the_interval_and_take_a_report() {
        use faber_api_types::StatusIntervalResponse;

        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        let host = node.peer_addr.to_string();
        let post = |uri: &str, body: String| json_request(Method::POST, uri, &host, Some(&body));

        let (status, body): (_, StatusIntervalResponse) = send(
            peer(&node),
            post(
                "/raft/cluster/status-interval",
                r#"{"interval_secs": 10}"#.into(),
            ),
        )
        .await;
        assert_eq!((status, body.interval_secs), (200, 10));
        let (status, err): (_, ErrorBody) = send(
            peer(&node),
            post(
                "/raft/cluster/status-interval",
                r#"{"interval_secs": 0}"#.into(),
            ),
        )
        .await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));

        let report = |node_id: u64, instance_id: &str| {
            format!(
                r#"{{"node_id": {node_id}, "instance_id": "{instance_id}",
                    "capacity": {{"cpu_millis": 1000, "memory_bytes": 1, "disk_bytes": 2}},
                    "replicas": []}}"#
            )
        };
        let machine = "ab".repeat(16);
        let resp = peer(&node)
            .oneshot(post("/raft/cluster/status", report(1, &machine)))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 204);
        let statuses = node.state().statuses().unwrap();
        assert_eq!(statuses.keys().copied().collect::<Vec<_>>(), vec![1]);

        // Unknown node, and an instance id that is no id.
        let (status, err): (_, ErrorBody) = send(
            peer(&node),
            post("/raft/cluster/status", report(9, &machine)),
        )
        .await;
        assert_eq!(
            (status, err.error.as_str()),
            (409, error_code::MESH_NODE_REMOVED)
        );
        let (status, err): (_, ErrorBody) =
            send(peer(&node), post("/raft/cluster/status", report(1, "nope"))).await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));

        // The replication status stays a GET on the same path.
        let req = json_request(Method::GET, "/raft/cluster/status", &host, None);
        let resp = peer(&node).oneshot(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        node.shutdown().await;
    }
}
