//! Axum handlers for the Raft RPCs sent by [`super::network::HttpNetwork`].
//!
//! Each handler returns HTTP 200 with a JSON `Result<Resp, RaftError>`; the
//! transport-level status only signals whether the RPC was delivered.
//!
//! Served only on the peer listener.
//!
//! A node that joins with an invite takes no raft RPC until the mesh has
//! accepted its registration ([`RaftGate`]). A machine that holds a copy of
//! another node's token has that node's key and mesh address, so the leader's
//! RPCs for that node can reach it. It must not feed them to its own, empty
//! raft: the leader would see the node's log go backwards.
//!
//! A raft RPC is taken only from an address that may send one
//! ([`SourceCheck`]). In a mesh those are the registered nodes: the peer
//! firewall also admits machines that only hold an invite, and one forged
//! snapshot from such a machine would replace all replicated state.
//!
//! TODO(security): no authentication beyond the source address; see the
//! note in `network.rs`.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::{ConnectInfo, DefaultBodyLimit, Request, State};
use axum::http::StatusCode;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use faber_api_types::{ErrorBody, error_code};
use openraft::error::InstallSnapshotError;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::raft::types::{NodeId, Raft, RaftError, TypeConfig};

/// Large enough for a full `append` batch or one snapshot chunk (3 MiB by default).
pub(crate) const MAX_RPC_BODY: usize = 64 * 1024 * 1024;

/// Whether this node takes raft RPCs. Closed while a join with an invite has
/// not been registered, and again once the invite is rejected. Clones share
/// one state.
#[derive(Debug, Clone)]
pub struct RaftGate(Arc<AtomicBool>);

impl RaftGate {
    pub fn new(open: bool) -> Self {
        Self(Arc::new(AtomicBool::new(open)))
    }

    pub fn open(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn close(&self) {
        self.0.store(false, Ordering::SeqCst);
    }

    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Which addresses a raft RPC may come from. Clones share one rule.
#[derive(Clone)]
pub struct SourceCheck(Option<Arc<dyn Fn(IpAddr) -> bool + Send + Sync>>);

impl SourceCheck {
    /// Every source may send raft RPCs: for a router that is not served to
    /// a network (unit tests).
    pub fn any() -> Self {
        Self(None)
    }

    /// Only sources `allowed` says yes to. The router must then be served
    /// with the client address attached to each request
    /// (`into_make_service_with_connect_info::<SocketAddr>`): a request
    /// without one is refused.
    pub fn new(allowed: impl Fn(IpAddr) -> bool + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(allowed)))
    }

    fn allows(&self, source: Option<IpAddr>) -> bool {
        match (&self.0, source) {
            (None, _) => true,
            (Some(allowed), Some(source)) => allowed(source),
            (Some(_), None) => false,
        }
    }
}

/// Routes mounted at `/raft`. While `gate` is closed every one of them
/// answers 503 `not_ready`. From a source that `sources` does not allow
/// every one of them answers 403 `forbidden`. Either way the request never
/// reaches raft.
pub fn router(raft: Raft, gate: RaftGate, sources: SourceCheck) -> Router {
    Router::new()
        .route("/raft/append", post(append))
        .route("/raft/vote", post(vote))
        .route("/raft/snapshot", post(snapshot))
        .layer(DefaultBodyLimit::max(MAX_RPC_BODY))
        // The layer added last runs first: the gate, then the source.
        .route_layer(from_fn_with_state(sources, refuse_other_sources))
        .route_layer(from_fn_with_state(gate, refuse_while_closed))
        .with_state(raft)
}

/// Runs before the body is read, like [`refuse_while_closed`].
async fn refuse_other_sources(
    State(sources): State<SourceCheck>,
    req: Request,
    next: Next,
) -> Response {
    let source = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip());
    if sources.allows(source) {
        return next.run(req).await;
    }
    // Not a warning: whoever is refused decides how often this is logged.
    tracing::debug!(
        source = ?source,
        path = %req.uri().path(),
        "refused a raft RPC from an address that is no registered node"
    );
    let body = ErrorBody::new(error_code::FORBIDDEN)
        .with_message("raft RPCs are taken from registered nodes only");
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

/// Runs before the body is read, so a refused RPC costs nothing.
async fn refuse_while_closed(State(gate): State<RaftGate>, req: Request, next: Next) -> Response {
    if gate.is_open() {
        return next.run(req).await;
    }
    let body = ErrorBody::new(error_code::NOT_READY)
        .with_message("this node has not registered with the mesh, so it takes no raft RPCs");
    (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
}

async fn append(
    State(raft): State<Raft>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<NodeId>, RaftError>> {
    Json(raft.append_entries(req).await)
}

async fn vote(
    State(raft): State<Raft>,
    Json(req): Json<VoteRequest<NodeId>>,
) -> Json<Result<VoteResponse<NodeId>, RaftError>> {
    Json(raft.vote(req).await)
}

async fn snapshot(
    State(raft): State<Raft>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<NodeId>, RaftError<InstallSnapshotError>>> {
    Json(raft.install_snapshot(req).await)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use openraft::Vote;
    use tower::ServiceExt;

    use super::*;
    use crate::raft::network::HttpNetwork;
    use crate::raft::store;

    const REGISTERED: &str = "10.99.0.1:40000";
    const INVITED: &str = "10.99.0.2:40000";

    /// A raft node that has seen nothing, and its `/raft` routes with
    /// `sources`.
    async fn raft_with(gate: RaftGate, sources: SourceCheck) -> (tempfile::TempDir, Raft, Router) {
        let dir = tempfile::tempdir().unwrap();
        let s = store::open(dir.path()).unwrap();
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let network = HttpNetwork::new().unwrap();
        let raft = Raft::new(2, config, network, s.log_store, s.state_machine)
            .await
            .unwrap();
        let router = router(raft.clone(), gate, sources);
        (dir, raft, router)
    }

    /// A vote request for node 1 in term 5, as it arrives from `source`.
    /// `None`: served without the client address.
    async fn vote_from(router: &Router, source: Option<&str>) -> (StatusCode, serde_json::Value) {
        let vote = VoteRequest::<NodeId>::new(Vote::new(5, 1), None);
        let mut req = HttpRequest::post("/raft/vote")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&vote).unwrap()))
            .unwrap();
        if let Some(source) = source {
            let addr: SocketAddr = source.parse().unwrap();
            req.extensions_mut().insert(ConnectInfo(addr));
        }
        let resp = router.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    fn registered_only() -> SourceCheck {
        let registered: IpAddr = "10.99.0.1".parse().unwrap();
        SourceCheck::new(move |source| source == registered)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_raft_rpc_from_another_source_is_forbidden_and_never_reaches_raft() {
        let (_dir, raft, router) = raft_with(RaftGate::new(true), registered_only()).await;
        let before = raft.metrics().borrow().vote;

        // A machine the firewall admits but the rule does not, and a
        // request whose source is not known at all.
        for source in [Some(INVITED), None] {
            let (status, body) = vote_from(&router, source).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{source:?}");
            assert_eq!(body["error"], error_code::FORBIDDEN, "{source:?}");
        }
        assert_eq!(raft.metrics().borrow().vote, before, "raft was touched");

        let (status, body) = vote_from(&router, Some(REGISTERED)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["Ok"]["vote_granted"], true, "{body}");
        raft.wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.vote == Vote::new(5, 1), "the vote is recorded")
            .await
            .unwrap();
        raft.shutdown().await.unwrap();
    }

    /// A node behind a closed gate says so to everyone, whatever the source.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_gate_answers_before_the_source_is_looked_at() {
        let gate = RaftGate::new(false);
        let (_dir, raft, router) = raft_with(gate.clone(), registered_only()).await;
        for source in [Some(REGISTERED), Some(INVITED), None] {
            let (status, body) = vote_from(&router, source).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{source:?}");
            assert_eq!(body["error"], error_code::NOT_READY, "{source:?}");
        }
        gate.open();
        assert_eq!(
            vote_from(&router, Some(INVITED)).await.0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(vote_from(&router, Some(REGISTERED)).await.0, StatusCode::OK);
        raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn without_a_rule_every_source_is_taken() {
        let (_dir, raft, router) = raft_with(RaftGate::new(true), SourceCheck::any()).await;
        for source in [Some(INVITED), None] {
            assert_eq!(
                vote_from(&router, source).await.0,
                StatusCode::OK,
                "{source:?}"
            );
        }
        raft.shutdown().await.unwrap();
    }
}
