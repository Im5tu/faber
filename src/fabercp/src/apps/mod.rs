//! Projects, environments and workloads: what should run.
//!
//! [`state`] holds the replicated records and the pure functions that
//! `apply` calls on them. [`service`] holds the operations behind the
//! `/api/v1` routes and the peer route that forwards a write to the leader.
//! Nothing here runs a container; a deploy stores desired state.

pub mod service;
pub mod state;

pub use state::{
    Applied, EnvironmentIndexEntry, EnvironmentRecord, ProjectIndex, ProjectRecord, WorkloadRecord,
    WorkloadsApplied,
};
