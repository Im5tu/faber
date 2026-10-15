//! Internal cluster endpoints under `/raft/cluster/*`, served only on the
//! peer listener.
//!
//! | route | does | leader only | source must be |
//! |---|---|---|---|
//! | `POST /raft/cluster/register` | turns an invite into a node; answers with the cluster key | forwards once | anyone the firewall admits |
//! | `POST /raft/cluster/join` | adds a node as a voter | forwards once | anyone the firewall admits |
//! | `POST /raft/cluster/invites` | reserves an invite | forwards once | a registered node |
//! | `POST /raft/cluster/endpoint` | replaces a node's WireGuard endpoint | forwards once | the node itself, or a registered node forwarding |
//! | `POST /raft/cluster/renew` | restarts a node's lease | forwards once | the node itself, or a registered node forwarding |
//! | `POST /raft/cluster/lease` | sets the lease duration | forwards once | a registered node |
//! | `POST /raft/cluster/workloads` | one of the six project and workload writes | forwards once | a registered node |
//! | `POST /raft/cluster/status` | replaces a node's status report | forwards once | the node itself, or a registered node forwarding |
//! | `POST /raft/cluster/status-interval` | sets the status report interval | forwards once | a registered node |
//! | `GET /raft/cluster/status` | replication progress | 503 elsewhere | anyone the firewall admits |
//!
//! TODO(security): no authentication; access control relies on the peer
//! listener being bound to loopback or a WireGuard interface. The checks
//! made here are by source address. On `/raft/cluster/endpoint` and
//! `/raft/cluster/renew` a node may only make the request for itself. On
//! `/raft/cluster/invites` and `/raft/cluster/lease` the request must come
//! from a registered node; a machine that only holds an invite may not
//! create invites or change the lease. `/raft/cluster/register` and
//! `/raft/cluster/join` are open to it, because that is how it joins.
//! None of this is checked when the peer listener is loopback.
//!
//! TODO(security): `/raft/cluster/workloads` has no authentication either.
//! Its source must be a registered node, and every registered node can
//! deploy any workload to the whole cluster through it.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use faber_api_types::workload::limits;
use faber_api_types::{FORWARDED_HEADER, LeaseResponse, StatusIntervalResponse};

use crate::dto::{
    PeerEndpointRequest, PeerInviteRequest, PeerInviteResponse, PeerJoinRequest, PeerLeaseRequest,
    PeerRegisterRequest, PeerRegisterResponse, PeerRenewRequest, PeerStatusIntervalRequest,
    PeerWorkloadRequest, PeerWorkloadResponse, ReplicationStatus,
};
use crate::error::ApiJson;
use crate::raft::store::status::ReportNode;
use crate::service::{FaberService, ServiceResult};

/// Largest body of `POST /raft/cluster/status`. A report of the most replicas
/// with the longest names and details is about 50 KiB.
const MAX_REPORT_BODY_BYTES: usize = 64 * 1024;

pub fn router(service: FaberService) -> Router {
    let limit = DefaultBodyLimit::max(limits::MAX_HTTP_BODY_BYTES);
    Router::new()
        .route("/raft/cluster/register", post(register))
        .route("/raft/cluster/join", post(join))
        .route("/raft/cluster/invites", post(invite))
        .route("/raft/cluster/endpoint", post(endpoint))
        .route("/raft/cluster/renew", post(renew))
        .route("/raft/cluster/lease", post(lease))
        .route("/raft/cluster/workloads", post(workloads).layer(limit))
        .route(
            "/raft/cluster/status",
            get(status).merge(post(report).layer(DefaultBodyLimit::max(MAX_REPORT_BODY_BYTES))),
        )
        .route("/raft/cluster/status-interval", post(status_interval))
        .with_state(service)
}

fn forwarded(headers: &HeaderMap) -> bool {
    headers.contains_key(FORWARDED_HEADER)
}

/// 200 once the invite is registered to the machine that asked. Leader only;
/// forwards once. The body carries the cluster key when the request comes
/// from the mesh IP of the invite, and is empty otherwise. 503 `not_ready` while a node that holds the invite's name
/// is still leaving the raft membership. An invite the mesh does not know is
/// refused without a raft write.
async fn register(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerRegisterRequest>,
) -> ServiceResult<Json<PeerRegisterResponse>> {
    let resp = svc
        .peer_register_for(req, forwarded(&headers), source)
        .await?;
    Ok(Json(resp))
}

/// 204 once `node_id` is a voter. Forwards once to the leader if needed.
/// 409 `mesh_node_removed` for a node the mesh has neither a record of nor
/// an invite for: it registered and was removed before it finished joining.
async fn join(
    State(svc): State<FaberService>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerJoinRequest>,
) -> ServiceResult<StatusCode> {
    svc.peer_join(req, forwarded(&headers)).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// 201 with what the token needs except the private key. Leader only;
/// forwards once. 403 unless the request comes from a registered node.
async fn invite(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerInviteRequest>,
) -> ServiceResult<(StatusCode, Json<PeerInviteResponse>)> {
    let resp = svc.peer_invite(req, forwarded(&headers), source).await?;
    Ok((StatusCode::CREATED, Json(resp)))
}

/// 204 once the endpoint is replicated. Leader only; forwards once. 403
/// unless the request comes from the node it names, or was forwarded by a
/// registered node.
async fn endpoint(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerEndpointRequest>,
) -> ServiceResult<StatusCode> {
    svc.peer_set_endpoint(req, forwarded(&headers), source)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// 204 once the lease is renewed. Leader only; forwards once. 403 unless the
/// request comes from the node it names, or was forwarded by a registered
/// node, and for another machine than the one the node is registered to.
/// 409 `mesh_node_removed` when the mesh no longer has the node, 409
/// `mesh_lease_expired` when it has the node and the lease has run out.
async fn renew(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerRenewRequest>,
) -> ServiceResult<StatusCode> {
    svc.peer_renew(req, forwarded(&headers), source).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// 200 with the new lease duration once it is replicated. Leader only;
/// forwards once. 403 unless the request comes from a registered node.
async fn lease(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerLeaseRequest>,
) -> ServiceResult<Json<LeaseResponse>> {
    let resp = svc.peer_set_lease(req, forwarded(&headers), source).await?;
    Ok(Json(resp))
}

/// 200 with the log index of the write, and for `apply_workloads` what it
/// did, once the leader has applied it. Leader only; forwards once. 403
/// unless the request comes from a registered node. 400 `bad_request` for
/// a name or a spec the leader's own validation refuses, and for a body
/// over 1 MiB. The request carries env values; it must never be logged.
async fn workloads(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerWorkloadRequest>,
) -> ServiceResult<Json<PeerWorkloadResponse>> {
    let resp = svc.peer_workloads(req, forwarded(&headers), source).await?;
    Ok(Json(resp))
}

/// 204 once the report is replicated. Leader only; forwards once. 403 unless
/// the request comes from the node it names, or was forwarded by a registered
/// node. 409 `mesh_node_removed` when the mesh no longer has the node, 403
/// `forbidden` for another machine than the one the node is registered to.
/// 400 `bad_request` for a report over its size limits and for a body over
/// 64 KiB.
async fn report(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<ReportNode>,
) -> ServiceResult<StatusCode> {
    svc.peer_report(req, forwarded(&headers), source).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// 200 with the new status interval once it is replicated. Leader only;
/// forwards once. 403 unless the request comes from a registered node.
async fn status_interval(
    State(svc): State<FaberService>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ApiJson(req): ApiJson<PeerStatusIntervalRequest>,
) -> ServiceResult<Json<StatusIntervalResponse>> {
    let resp = svc
        .peer_set_status_interval(req, forwarded(&headers), source)
        .await?;
    Ok(Json(resp))
}

/// Leader only; 503 `not_leader`/`no_leader` elsewhere.
async fn status(State(svc): State<FaberService>) -> ServiceResult<Json<ReplicationStatus>> {
    Ok(Json(svc.replication()?))
}
