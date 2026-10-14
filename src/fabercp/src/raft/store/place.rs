//! Replica placement commands.

use serde::{Deserialize, Serialize};

use crate::raft::types::Response;

/// Assigns the replicas of one workload to nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceReplicas {
    pub project: String,
    pub environment: String,
    pub workload: String,
    /// Node ID of each replica, by replica index.
    pub nodes: Vec<u64>,
    pub at: u64,
}

/// Placeholder: rejects the command and writes no state.
pub fn apply_place_replicas(_cmd: PlaceReplicas) -> Response {
    Response::Rejected {
        code: faber_api_types::error_code::INTERNAL.to_string(),
        message: "not implemented".to_string(),
    }
}
