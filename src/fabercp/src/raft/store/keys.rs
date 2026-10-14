//! Data key commands: envelope encryption key ids in the replicated state.

use serde::{Deserialize, Serialize};

use crate::raft::types::Response;

/// Stores a data key sealed under the cluster key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateDataKey {
    pub key_id: String,
    /// The data key, sealed under the cluster key.
    pub sealed: Vec<u8>,
    pub at: u64,
}

/// Names the cluster key that seals data keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetClusterKeyId {
    pub key_id: String,
}

/// Placeholder: rejects the command and writes no state.
pub fn apply_create_data_key(_cmd: CreateDataKey) -> Response {
    not_implemented()
}

/// Placeholder: rejects the command and writes no state.
pub fn apply_set_cluster_key_id(_cmd: SetClusterKeyId) -> Response {
    not_implemented()
}

fn not_implemented() -> Response {
    Response::Rejected {
        code: faber_api_types::error_code::INTERNAL.to_string(),
        message: "not implemented".to_string(),
    }
}
