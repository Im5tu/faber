//! The one place that maps [`ServiceError`] to HTTP responses.
//!
//! | error | status | `error` code |
//! |---|---|---|
//! | BadRequest | 400 | `bad_request` |
//! | NotFound | 404 | `not_found` |
//! | AlreadyInitialized | 409 | `already_initialized` |
//! | AlreadyJoined | 409 | `already_joined` |
//! | NotLeader (forwarded request hit a non-leader) | 503 | `not_leader` (+ `leader_id`, `leader_addr`) |
//! | NoLeader | 503 | `no_leader` |
//! | NotReady (retry later) | 503 | `not_ready` |
//! | PeerUnreachable | 502 | `peer_unreachable` |
//! | Rejected `invite_invalid` | 400 | `invite_invalid` |
//! | Rejected `invite_used` | 409 | `invite_used` |
//! | Rejected `invite_expired` | 410 | `invite_expired` |
//! | Rejected `mesh_*` (a refused lease renewal is `mesh_node_removed` or `mesh_lease_expired`) | 409 | the code |
//! | Rejected `project_exists`, `not_empty`, `workloads_exhausted` | 409 | the code |
//! | Rejected `forbidden` (a lease renewal by another machine) | 403 | `forbidden` |
//! | Forbidden | 403 | `forbidden` |
//! | Upstream | the peer's status | the peer's body |
//! | Internal | 500 | `internal` |
//!
//! There are no redirects: work that needs the leader is forwarded server-side.

use axum::Json;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use faber_api_types::{ErrorBody, error_code};
use serde::de::DeserializeOwned;

use crate::service::ServiceError;

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ServiceError::NotFound => {
                (StatusCode::NOT_FOUND, ErrorBody::new(error_code::NOT_FOUND))
            }
            ServiceError::BadRequest(m) => (
                StatusCode::BAD_REQUEST,
                ErrorBody::new(error_code::BAD_REQUEST).with_message(m),
            ),
            ServiceError::AlreadyInitialized => (
                StatusCode::CONFLICT,
                ErrorBody::new(error_code::ALREADY_INITIALIZED)
                    .with_message("cluster is already initialized"),
            ),
            ServiceError::AlreadyJoined => (
                StatusCode::CONFLICT,
                ErrorBody::new(error_code::ALREADY_JOINED)
                    .with_message("this node has already joined a cluster"),
            ),
            ServiceError::NotLeader {
                leader_id,
                leader_addr,
            } => {
                let mut body = ErrorBody::new(error_code::NOT_LEADER)
                    .with_message(format!("node {leader_id} at {leader_addr} is the leader"));
                body.leader_id = Some(leader_id);
                body.leader_addr = Some(leader_addr);
                (StatusCode::SERVICE_UNAVAILABLE, body)
            }
            ServiceError::NoLeader => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorBody::new(error_code::NO_LEADER).with_message("no leader is known"),
            ),
            ServiceError::NotReady(m) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorBody::new(error_code::NOT_READY).with_message(m),
            ),
            ServiceError::PeerUnreachable(m) => (
                StatusCode::BAD_GATEWAY,
                ErrorBody::new(error_code::PEER_UNREACHABLE).with_message(m),
            ),
            ServiceError::Rejected { code, message } => (
                rejection_status(&code),
                ErrorBody::new(code).with_message(message),
            ),
            ServiceError::Forbidden(m) => (
                StatusCode::FORBIDDEN,
                ErrorBody::new(error_code::FORBIDDEN).with_message(m),
            ),
            ServiceError::Upstream { status, body } => (
                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                body,
            ),
            ServiceError::Internal(m) => {
                tracing::error!(error = %m, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorBody::new(error_code::INTERNAL).with_message(m),
                )
            }
        };
        (status, Json(body)).into_response()
    }
}

/// HTTP status for a [`ServiceError::Rejected`] code.
fn rejection_status(code: &str) -> StatusCode {
    match code {
        error_code::INVITE_USED
        | error_code::MESH_NOT_CONFIGURED
        | error_code::MESH_NO_ENDPOINTS
        | error_code::MESH_EXHAUSTED
        | error_code::MESH_NODE_REMOVED
        | error_code::MESH_LEASE_EXPIRED
        | error_code::PROJECT_EXISTS
        | error_code::NOT_EMPTY
        | error_code::WORKLOADS_EXHAUSTED
        | error_code::ALREADY_INITIALIZED => StatusCode::CONFLICT,
        error_code::INVITE_EXPIRED => StatusCode::GONE,
        error_code::NOT_FOUND => StatusCode::NOT_FOUND,
        error_code::FORBIDDEN => StatusCode::FORBIDDEN,
        _ => StatusCode::BAD_REQUEST,
    }
}

/// `Json` extractor whose rejections are [`ErrorBody`] 400s.
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ServiceError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(ApiJson(v)),
            Err(e) => Err(ServiceError::BadRequest(e.body_text())),
        }
    }
}

/// Fallback for unknown `/api/v1` routes.
pub async fn not_found() -> ServiceError {
    ServiceError::NotFound
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;

    use super::*;

    async fn render(err: ServiceError) -> (u16, ErrorBody) {
        let resp = err.into_response();
        let status = resp.status().as_u16();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn status_mapping() {
        let cases = [
            (ServiceError::NotFound, 404, error_code::NOT_FOUND),
            (
                ServiceError::BadRequest("x".into()),
                400,
                error_code::BAD_REQUEST,
            ),
            (
                ServiceError::AlreadyInitialized,
                409,
                error_code::ALREADY_INITIALIZED,
            ),
            (ServiceError::AlreadyJoined, 409, error_code::ALREADY_JOINED),
            (ServiceError::NoLeader, 503, error_code::NO_LEADER),
            (
                ServiceError::NotReady("x".into()),
                503,
                error_code::NOT_READY,
            ),
            (
                ServiceError::PeerUnreachable("x".into()),
                502,
                error_code::PEER_UNREACHABLE,
            ),
            (
                ServiceError::Internal("x".into()),
                500,
                error_code::INTERNAL,
            ),
        ];
        for (err, status, code) in cases {
            let (s, body) = render(err).await;
            assert_eq!((s, body.error.as_str()), (status, code));
        }
    }

    #[tokio::test]
    async fn rejection_and_forbidden_mapping() {
        let rejected = |code: &str| ServiceError::Rejected {
            code: code.into(),
            message: "m".into(),
        };
        let cases = [
            (
                rejected(error_code::INVITE_INVALID),
                400,
                error_code::INVITE_INVALID,
            ),
            (
                rejected(error_code::INVITE_USED),
                409,
                error_code::INVITE_USED,
            ),
            (
                rejected(error_code::INVITE_EXPIRED),
                410,
                error_code::INVITE_EXPIRED,
            ),
            (
                rejected(error_code::MESH_NOT_CONFIGURED),
                409,
                error_code::MESH_NOT_CONFIGURED,
            ),
            (
                rejected(error_code::MESH_NO_ENDPOINTS),
                409,
                error_code::MESH_NO_ENDPOINTS,
            ),
            (
                rejected(error_code::MESH_EXHAUSTED),
                409,
                error_code::MESH_EXHAUSTED,
            ),
            (
                rejected(error_code::MESH_NODE_REMOVED),
                409,
                error_code::MESH_NODE_REMOVED,
            ),
            (
                rejected(error_code::MESH_LEASE_EXPIRED),
                409,
                error_code::MESH_LEASE_EXPIRED,
            ),
            (
                rejected(error_code::PROJECT_EXISTS),
                409,
                error_code::PROJECT_EXISTS,
            ),
            (rejected(error_code::NOT_EMPTY), 409, error_code::NOT_EMPTY),
            (
                rejected(error_code::WORKLOADS_EXHAUSTED),
                409,
                error_code::WORKLOADS_EXHAUSTED,
            ),
            (rejected(error_code::FORBIDDEN), 403, error_code::FORBIDDEN),
            (
                ServiceError::Forbidden("x".into()),
                403,
                error_code::FORBIDDEN,
            ),
        ];
        for (err, status, code) in cases {
            let (s, body) = render(err).await;
            assert_eq!((s, body.error.as_str()), (status, code));
        }
    }

    #[tokio::test]
    async fn not_leader_carries_leader_details() {
        let (s, body) = render(ServiceError::NotLeader {
            leader_id: 2,
            leader_addr: "10.8.0.2:7401".into(),
        })
        .await;
        assert_eq!(s, 503);
        assert_eq!(body.error, error_code::NOT_LEADER);
        assert_eq!(body.leader_id, Some(2));
        assert_eq!(body.leader_addr.as_deref(), Some("10.8.0.2:7401"));
    }

    #[tokio::test]
    async fn upstream_errors_pass_through() {
        let body = ErrorBody::new(error_code::NO_LEADER);
        let (s, got) = render(ServiceError::Upstream {
            status: 503,
            body: body.clone(),
        })
        .await;
        assert_eq!((s, got), (503, body));
    }
}
