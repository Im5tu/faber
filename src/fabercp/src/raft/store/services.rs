//! Service IP allocation commands.

use serde::{Deserialize, Serialize};

use crate::raft::types::Response;

/// Reserves a service IP for one workload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocateServiceIp {
    pub project: String,
    pub environment: String,
    pub workload: String,
}

/// Frees the service IP of one workload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReleaseServiceIp {
    pub project: String,
    pub environment: String,
    pub workload: String,
}

/// Placeholder: rejects the command and writes no state.
pub fn apply_allocate_service_ip(_cmd: AllocateServiceIp) -> Response {
    not_implemented()
}

/// Placeholder: rejects the command and writes no state.
pub fn apply_release_service_ip(_cmd: ReleaseServiceIp) -> Response {
    not_implemented()
}

fn not_implemented() -> Response {
    Response::Rejected {
        code: faber_api_types::error_code::INTERNAL.to_string(),
        message: "not implemented".to_string(),
    }
}
