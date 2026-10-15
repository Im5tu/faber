//! Runner: runs the replicas that placement put on this node, through
//! `faber-node`.
//!
//! Two parts share a [`RunnerState`]:
//!
//! - the containers step ([`crate::reconcile::containers`]) starts and
//!   removes replicas to match placement, in the reconcile loop;
//! - the [`Supervisor`] keeps them running on its own ticker: it detects
//!   exits, probes, restarts with backoff, drains old revisions and tells
//!   the status reporter how each replica is.
//!
//! `faber-node` is reached through a [`NodeClient`], and time through a
//! [`Clock`], so that tests use a fake node and a fake clock.

pub mod backoff;
pub mod clock;
pub mod desired;
pub mod env;
pub mod launch;
pub mod node;
pub mod probe;
pub mod state;
pub mod supervisor;
#[cfg(test)]
pub(crate) mod testing;

use std::time::Duration;

pub use backoff::{BACKOFF_MAX, BACKOFF_MIN, BACKOFF_RESET_AFTER, Backoff};
pub use clock::{Clock, MonotonicClock};
pub use desired::{DesiredContainer, DesiredReplica, DesiredVolume};
pub use env::{EnvSource, StateEnvSource};
pub use node::{DEFAULT_NODE_SOCKET, NodeCallError, NodeClient, SocketNodeClient};
pub use probe::{NetProber, ProbeOutcome, ProbeTarget, Prober};
pub use state::RunnerState;
pub use supervisor::{Supervisor, SupervisorConfig};

/// How long a replica of an older revision keeps running after the newer
/// revision of its workload is available on this node. In that time the
/// virtual IP's backends switch to the new revision, and connections that
/// are open finish. Then the old revision stops.
pub const DRAIN_PERIOD: Duration = Duration::from_secs(10);

/// Time between supervisor ticks: the longest an exited container goes
/// unnoticed.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Seconds a container gets between the stop signal and the kill when the
/// workload sets no `stop_grace_secs`.
pub const DEFAULT_STOP_GRACE_SECS: u32 = 30;
