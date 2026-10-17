//! `/api/v1/mesh/*`, served on the loopback listener only.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use faber_api_types::{
    CreateInviteRequest, InviteResponse, LeaseResponse, SetLeaseRequest, SetStatusIntervalRequest,
    StatusIntervalResponse,
};

use crate::error::ApiJson;
use crate::service::{FaberService, ServiceResult};

/// 201 with the invite token. The response carries a private key; it must
/// never be logged.
pub async fn create_invite(
    State(svc): State<FaberService>,
    ApiJson(req): ApiJson<CreateInviteRequest>,
) -> ServiceResult<(StatusCode, Json<InviteResponse>)> {
    Ok((StatusCode::CREATED, Json(svc.invite(req).await?)))
}

/// 200 with the lease duration as this node has it.
pub async fn lease(State(svc): State<FaberService>) -> ServiceResult<Json<LeaseResponse>> {
    Ok(Json(svc.lease().await?))
}

/// 200 with the new lease duration once it is replicated. 400 `bad_request`
/// outside the bounds.
pub async fn set_lease(
    State(svc): State<FaberService>,
    ApiJson(req): ApiJson<SetLeaseRequest>,
) -> ServiceResult<Json<LeaseResponse>> {
    Ok(Json(svc.set_lease(req.lease_secs, false).await?))
}

/// 200 with the status report interval as this node has it.
pub async fn status_interval(
    State(svc): State<FaberService>,
) -> ServiceResult<Json<StatusIntervalResponse>> {
    Ok(Json(svc.status_interval().await?))
}

/// 200 with the new status report interval once it is replicated. 400
/// `bad_request` outside 1 to 300 seconds.
pub async fn set_status_interval(
    State(svc): State<FaberService>,
    ApiJson(req): ApiJson<SetStatusIntervalRequest>,
) -> ServiceResult<Json<StatusIntervalResponse>> {
    Ok(Json(
        svc.set_status_interval(req.interval_secs, false).await?,
    ))
}
