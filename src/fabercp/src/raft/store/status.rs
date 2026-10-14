//! Node status report commands.

use serde::{Deserialize, Serialize};

use crate::raft::types::Response;

/// A node's status report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportNode {
    pub node_id: u64,
    pub report: serde_json::Value,
    pub at: u64,
}

/// Changes how often nodes report their status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetStatusInterval {
    pub secs: u64,
}

/// Placeholder: rejects the command and writes no state.
pub fn apply_report_node(_cmd: ReportNode) -> Response {
    not_implemented()
}

/// Placeholder: rejects the command and writes no state.
pub fn apply_set_status_interval(_cmd: SetStatusInterval) -> Response {
    not_implemented()
}

fn not_implemented() -> Response {
    Response::Rejected {
        code: faber_api_types::error_code::INTERNAL.to_string(),
        message: "not implemented".to_string(),
    }
}
