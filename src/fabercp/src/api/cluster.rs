//! `/api/v1/health` and `/api/v1/cluster/*`.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use faber_api_types::{ClusterStatus, JoinRequest, error_code};

use crate::error::ApiJson;
use crate::service::{FaberService, ServiceResult};

/// 200 with [`faber_api_types::HealthResponse`]. When the raft core has
/// stopped or the storage is closed: 503 with the same two fields (`status`
/// is `unavailable`) plus the `error` and `message` of an error body, so a
/// client that reads either shape understands it.
pub async fn health(State(svc): State<FaberService>) -> Response {
    let (status, body) = health_report(&svc);
    (status, Json(body)).into_response()
}

/// The status and JSON body `/api/v1/health` answers with, as described on
/// [`health`].
pub(crate) fn health_report(svc: &FaberService) -> (StatusCode, serde_json::Value) {
    let health = svc.health();
    match svc.health_problem() {
        None => (
            StatusCode::OK,
            serde_json::to_value(health).unwrap_or_default(),
        ),
        Some(problem) => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "status": health.status,
                "node_id": health.node_id,
                "error": error_code::UNAVAILABLE,
                "message": problem,
            }),
        ),
    }
}

pub async fn status(State(svc): State<FaberService>) -> Json<ClusterStatus> {
    Json(svc.status().await)
}

/// Makes this node a single-node cluster. Any request body is ignored.
pub async fn init(State(svc): State<FaberService>) -> ServiceResult<Json<ClusterStatus>> {
    Ok(Json(svc.init().await?))
}

pub async fn join(
    State(svc): State<FaberService>,
    ApiJson(req): ApiJson<JoinRequest>,
) -> ServiceResult<Json<ClusterStatus>> {
    Ok(Json(svc.join(req).await?))
}
