//! Node leases: every node holds a lease on its name and subnet, renews it
//! through raft, and is removed from the mesh when it runs out.
//!
//! This module is the leader's side. The task runs on every mesh node and
//! acts only while the node is the raft leader. Every tick it:
//!
//! 1. Proposes `AdvanceClock` with the time it measured since its previous
//!    tick as leader. The lease clock is logical: it moves only through
//!    these commands, so time without a leader, or without a quorum to
//!    commit them, counts against no lease. The wall clock is never read.
//! 2. Collects the nodes whose lease has expired, other than itself. A
//!    leader whose own lease expired is not removed here. Its renewals are
//!    refused with `mesh_lease_expired`, which no node takes as removal, so
//!    it goes on leading until another node leads and removes it.
//! 3. Asks the quorum guard ([`removal_allowed`]) whether those nodes can
//!    leave the raft membership without stalling the cluster. If not, it
//!    warns and tries again on the next tick.
//! 4. Removes them from the raft membership, then proposes `RemoveNode` for
//!    each. Raft goes first: apply refuses `RemoveNode` for a raft member,
//!    because dropping the mesh record cuts WireGuard to a node whose ack
//!    may be needed to commit its own removal.
//!
//! It also removes from raft any member that has no mesh record. A node in
//! the mesh that is not a raft member yet is left alone: that is a node in
//! the middle of its join, which registers before it becomes a learner.
//!
//! Before any of that, a tick does two checks:
//!
//! - If no quorum has acknowledged this leader for longer than a write may
//!   take, the tick writes nothing. Otherwise a leader cut off from its
//!   quorum would append one uncommitted clock entry per tick.
//! - If raft is in a joint config that an earlier tick already saw, the
//!   tick finishes it, guard permitting. A voter change whose caller went
//!   away between its two steps would otherwise stay joint. While the
//!   config is joint the task removes no other raft member.
//!
//! Every failure is logged and retried on the next tick.
//!
//! With two voters, the death of one stops everything: nothing commits, so
//! no lease expires and nothing is removed. Three nodes are the smallest
//! cluster that survives a loss.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use openraft::RaftMetrics;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::mesh::MeshState;
use crate::mesh::state::DEFAULT_LEASE_SECS;
use crate::raft::store::StateReader;
use crate::raft::types::{Node, NodeId, Request};
use crate::service::{FaberService, ServiceError, ServiceResult};

const TICK_MIN: Duration = Duration::from_secs(5);
const TICK_MAX: Duration = Duration::from_secs(60);
/// A tick is one sixtieth of the lease.
const TICKS_PER_LEASE: u64 = 60;
/// How long one raft write or membership change of the task may take.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// The clocks of the leader task. [`Default`] is what production runs;
/// tests shrink them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTiming {
    /// Shortest time between two ticks.
    pub tick_min: Duration,
    /// Longest time between two ticks.
    pub tick_max: Duration,
    /// Real time that moves the lease clock by one second.
    pub second: Duration,
    /// Longest wait for one raft write or membership change. A write that
    /// takes longer is given up for this tick.
    pub write_timeout: Duration,
}

impl Default for LeaseTiming {
    fn default() -> Self {
        Self {
            tick_min: TICK_MIN,
            tick_max: TICK_MAX,
            second: Duration::from_secs(1),
            write_timeout: WRITE_TIMEOUT,
        }
    }
}

impl LeaseTiming {
    /// Time between two ticks for a lease of `lease_secs`: a sixtieth of
    /// it, but no less than `tick_min` and no more than `tick_max`.
    pub fn tick(&self, lease_secs: u64) -> Duration {
        let ticks = u32::try_from(lease_secs / TICKS_PER_LEASE).unwrap_or(u32::MAX);
        self.second
            .saturating_mul(ticks)
            .max(self.tick_min)
            .min(self.tick_max)
    }

    /// [`Self::tick`] in seconds of the lease clock, rounded up: the most
    /// the clock can run ahead of what a node has reported.
    pub fn tick_clock_secs(&self, lease_secs: u64) -> u64 {
        let second = self.second.as_nanos().max(1);
        let secs = self.tick(lease_secs).as_nanos().div_ceil(second);
        u64::try_from(secs).unwrap_or(u64::MAX)
    }
}

/// Measures the time between the ticks of one spell as leader, in whole
/// seconds of the lease clock. What is left over after a tick is carried
/// into the next, so that short ticks do not lose time.
///
/// A spell ends when the task sees that the node does not lead
/// ([`Self::forget`]), and when the raft term changes between two ticks. A
/// node can lose and regain leadership inside one slow tick without the
/// task ever seeing it as a follower. Whoever led meanwhile measured and
/// proposed that time, so it must not be proposed a second time.
#[derive(Debug, Default)]
struct TickClock {
    /// When this node last ticked as leader.
    last: Option<Instant>,
    /// The raft term of that tick.
    term: u64,
    /// Time measured but not yet proposed: less than one `second`.
    carry: Duration,
}

impl TickClock {
    /// Forgets the last tick. The next one proposes zero.
    fn forget(&mut self) {
        *self = Self::default();
    }

    /// Seconds to propose at `now`, as the leader of `term`: the time since
    /// the last tick plus the carry, capped at twice `tick`, in units of
    /// `second`. Zero on the first tick, and on the first tick in another
    /// term than the last. The cap bounds what a stalled task (a suspended
    /// VM, a long pause) can add in one step.
    fn elapsed_secs(&mut self, now: Instant, term: u64, tick: Duration, second: Duration) -> u64 {
        if self.term != term {
            self.last = None;
            self.term = term;
        }
        let Some(last) = self.last.replace(now) else {
            self.carry = Duration::ZERO;
            return 0;
        };
        let total = now
            .saturating_duration_since(last)
            .saturating_add(self.carry)
            .min(tick.saturating_mul(2));
        let unit = second.as_nanos().max(1);
        let secs = total.as_nanos() / unit;
        let rest = total.as_nanos() - secs * unit;
        self.carry = Duration::from_nanos(u64::try_from(rest).unwrap_or(u64::MAX));
        u64::try_from(secs).unwrap_or(u64::MAX)
    }
}

/// The quorum guard: whether `remove` can leave a raft config whose voters
/// are `voters` without stalling the cluster.
///
/// `caught_up` holds the voters that acknowledged a recent log entry; `me`
/// (the leader) always counts as one. The change is allowed when the
/// caught-up voters are a strict majority of both:
///
/// - the voters that would remain. Otherwise the new config could not
///   commit anything. Three voters, one dead and one expired but alive:
///   removing the live one leaves two voters of which one answers.
/// - the voters there are now. Raft changes voters through a joint config,
///   which needs a majority of the old set as well as the new. Two voters
///   with the other dead: the survivor alone cannot commit the change.
///
/// Members of `remove` that are not voters (learners) do not change the
/// voter set; the change still has to commit, so the same test applies to
/// the voters as they are. A change that would leave no voter is refused.
pub(crate) fn removal_allowed(
    voters: &BTreeSet<NodeId>,
    remove: &BTreeSet<NodeId>,
    me: NodeId,
    caught_up: &BTreeSet<NodeId>,
) -> bool {
    let majority = |set: &BTreeSet<NodeId>| {
        let answering = set
            .iter()
            .filter(|v| **v == me || caught_up.contains(v))
            .count();
        answering * 2 > set.len()
    };
    let remaining: BTreeSet<NodeId> = voters.difference(remove).copied().collect();
    !remaining.is_empty() && majority(voters) && majority(&remaining)
}

/// The nodes whose replication has reached log index `mark`, from the
/// leader's metrics. Only a leader has replication metrics; anywhere else
/// the set is empty.
fn caught_up(m: &RaftMetrics<NodeId, Node>, mark: u64) -> BTreeSet<NodeId> {
    let Some(replication) = &m.replication else {
        return BTreeSet::new();
    };
    replication
        .iter()
        .filter(|(_, matched)| matched.is_some_and(|log_id| log_id.index >= mark))
        .map(|(id, _)| *id)
        .collect()
}

/// [`removal_allowed`] against the membership in `m`, with the voters that
/// have replicated log index `mark` as the caught-up ones. While raft is in
/// a joint config, every one of its voter sets must pass.
pub(crate) fn raft_removal_allowed(
    m: &RaftMetrics<NodeId, Node>,
    remove: &BTreeSet<NodeId>,
    me: NodeId,
    mark: u64,
) -> bool {
    let caught_up = caught_up(m, mark);
    let membership = m.membership_config.membership();
    membership
        .get_joint_config()
        .iter()
        .all(|voters| removal_allowed(voters, remove, me, &caught_up))
}

/// Whether a quorum acknowledged this leader within `limit`.
/// `millis_since_quorum_ack` is the leader's metric of that name: `None`
/// until a quorum has acknowledged it at all.
fn quorum_is_recent(millis_since_quorum_ack: Option<u64>, limit: Duration) -> bool {
    millis_since_quorum_ack.is_some_and(|ms| u128::from(ms) <= limit.as_millis())
}

/// Leader-side lease task: advances the lease clock and removes expired
/// nodes. Runs until `cancel`; acts only while this node is the leader.
pub fn spawn_leader_task(
    service: FaberService,
    reader: StateReader,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    spawn_leader_task_with(service, reader, LeaseTiming::default(), cancel)
}

/// [`spawn_leader_task`] with the given clocks.
pub fn spawn_leader_task_with(
    service: FaberService,
    reader: StateReader,
    timing: LeaseTiming,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut task = LeaderTask {
            service,
            reader,
            timing,
            clock: TickClock::default(),
            mark: None,
            joint_seen: None,
        };
        loop {
            let started = Instant::now();
            let tick = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                tick = task.round() => tick,
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep_until(started + tick) => {}
            }
        }
    })
}

struct LeaderTask {
    service: FaberService,
    reader: StateReader,
    timing: LeaseTiming,
    clock: TickClock,
    /// Log index of the `AdvanceClock` entry of the previous tick. A voter
    /// that has replicated it answered within the last tick or so.
    mark: Option<u64>,
    /// Log index of the joint config the previous tick saw, if it saw one.
    joint_seen: Option<u64>,
}

impl LeaderTask {
    /// One tick. Returns the time until the next, which follows the lease
    /// duration in the replicated state.
    async fn round(&mut self) -> Duration {
        let mesh = self.read_mesh().unwrap_or_else(|e| {
            tracing::debug!(error = %e, "lease task: could not read the mesh");
            None
        });
        let lease_secs = mesh.as_ref().map_or(DEFAULT_LEASE_SECS, |m| m.lease_secs);
        let tick = self.timing.tick(lease_secs);
        if mesh.is_none() || !self.service.is_leader() {
            self.forget();
            return tick;
        }
        if let Err(e) = self.lead(tick).await {
            tracing::warn!(error = %e, "lease task: tick failed; retrying on the next one");
        }
        tick
    }

    /// Drops what this node measured as leader, so that time it did not
    /// lead is never proposed and an old mark is never trusted.
    fn forget(&mut self) {
        self.clock.forget();
        self.mark = None;
        self.joint_seen = None;
    }

    /// Finishes a voter change that stopped at its joint config: its caller
    /// went away between the two steps (the leader changed, or a takeover's
    /// joiner never came back). A joint config needs a majority of both its
    /// voter sets for every commit, so it should not last.
    ///
    /// A joint config is left alone on the tick that first sees it, because
    /// a change that is still running makes its own second step. On a later
    /// tick it is finished if the guard passes for every one of its voter
    /// sets, judged by the mark of the previous tick.
    async fn finish_joint_config(&mut self) {
        let Some(index) = self.service.joint_config_index() else {
            self.joint_seen = None;
            return;
        };
        if self.joint_seen.replace(index) != Some(index) {
            return;
        }
        let Some(mark) = self.mark else {
            return;
        };
        let finish = self.service.finish_joint_config(mark);
        match bounded(self.timing.write_timeout, finish).await {
            Ok(()) => tracing::info!("finished a membership change left at its joint config"),
            Err(ServiceError::NotReady(why)) => {
                tracing::warn!(%why, "lease task: not finishing the joint config on this tick");
            }
            Err(e) => {
                tracing::warn!(error = %e, "lease task: finishing the joint config failed");
            }
        }
    }

    fn read_mesh(&self) -> ServiceResult<Option<MeshState>> {
        self.reader
            .mesh()
            .map_err(|e| ServiceError::Internal(e.to_string()))
    }

    async fn lead(&mut self, tick: Duration) -> ServiceResult<()> {
        // A leader that has lost its quorum commits nothing. Each write it
        // made would sit in its log, one more per tick, so it makes none.
        // The time is dropped as for a failed write.
        let limit = self.timing.write_timeout;
        if !quorum_is_recent(self.service.millis_since_quorum_ack(), limit) {
            self.forget();
            return Err(ServiceError::NotReady(format!(
                "no quorum has acknowledged this leader within {limit:?}"
            )));
        }
        self.finish_joint_config().await;

        let term = self.service.term();
        let elapsed_secs = self
            .clock
            .elapsed_secs(Instant::now(), term, tick, self.timing.second);
        let advance = Request::AdvanceClock { elapsed_secs };
        let written = bounded(self.timing.write_timeout, self.service.write(advance)).await;
        let index = match written {
            Ok((index, _)) => index,
            Err(e) => {
                // The time of a tick that did not commit is not proposed
                // again: it may yet commit, and counting it twice would
                // shorten every lease.
                self.forget();
                return Err(e);
            }
        };
        let previous = self.mark.replace(index);

        let Some(mesh) = self.read_mesh()? else {
            return Ok(());
        };
        let me = self.service.node_id();
        let members = self.service.member_ids();
        let expired: BTreeSet<NodeId> = mesh
            .nodes
            .values()
            .filter(|n| n.node_id != me && mesh.lease_expired(n))
            .map(|n| n.node_id)
            .collect();
        // Raft members to remove: the expired ones, and those with no mesh
        // record at all. A mesh node that is no raft member is not touched
        // here: it may be in the middle of its join.
        let mut leave: BTreeSet<NodeId> = expired.intersection(&members).copied().collect();
        leave.extend(
            members
                .iter()
                .filter(|id| **id != me && !mesh.nodes.contains_key(id)),
        );
        // One voter change at a time: while a joint config stands, it is
        // finished first (above, on a later tick).
        if !leave.is_empty() && self.service.joint_config_index().is_none() {
            self.remove_members(&leave, previous).await;
        }

        let members = self.service.member_ids();
        for node_id in expired.difference(&members) {
            let remove = Request::RemoveNode { node_id: *node_id };
            match bounded(self.timing.write_timeout, self.service.propose(remove)).await {
                Ok(_) => tracing::info!(node_id, "removed a node whose lease expired"),
                // The membership change is committed but not applied yet.
                Err(ServiceError::NotReady(why)) => {
                    tracing::debug!(node_id, %why, "lease task: node not removable yet");
                }
                Err(e) => {
                    tracing::warn!(node_id, error = %e, "lease task: removing a node failed");
                }
            }
        }
        Ok(())
    }

    /// Takes `leave` out of the raft membership if the guard allows it.
    /// `previous` is the mark of the tick before this one; on the first
    /// tick as leader there is none, and nothing is removed.
    async fn remove_members(&self, leave: &BTreeSet<NodeId>, previous: Option<u64>) {
        let Some(mark) = previous else {
            tracing::debug!(nodes = ?leave, "lease task: first tick as leader; removing nothing");
            return;
        };
        let removal = self.service.remove_from_raft(leave, mark);
        match bounded(self.timing.write_timeout, removal).await {
            Ok(()) => {
                tracing::info!(nodes = ?leave, "removed nodes from the raft membership");
            }
            Err(ServiceError::NotReady(why)) => {
                tracing::warn!(
                    nodes = ?leave,
                    %why,
                    "lease task: not removing nodes from the raft membership on this tick"
                );
            }
            Err(e) => {
                tracing::warn!(
                    nodes = ?leave,
                    error = %e,
                    "lease task: removing nodes from the raft membership failed"
                );
            }
        }
    }
}

/// `fut`, given up with `NotReady` after `limit`. The write it made may
/// still commit later.
async fn bounded<T>(
    limit: Duration,
    fut: impl Future<Output = ServiceResult<T>>,
) -> ServiceResult<T> {
    match tokio::time::timeout(limit, fut).await {
        Ok(result) => result,
        Err(_) => Err(ServiceError::NotReady(format!(
            "no answer from raft within {limit:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_tick_in_clock_seconds_follows_the_lease() {
        let t = LeaseTiming::default();
        assert_eq!(t.tick_clock_secs(300), 5);
        assert_eq!(t.tick_clock_secs(1_800), 30);
        assert_eq!(t.tick_clock_secs(28_800), 60);
        let fast = LeaseTiming {
            tick_min: Duration::from_millis(20),
            tick_max: Duration::from_millis(100),
            second: Duration::from_millis(1),
            ..t
        };
        assert_eq!(fast.tick_clock_secs(300), 20);
        assert_eq!(fast.tick_clock_secs(28_800), 100);
    }

    use faber_api_types::{CreateInviteRequest, error_code};
    use openraft::ChangeMembers;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    use super::*;
    use crate::dto::{PeerJoinRequest, PeerRegisterRequest};
    use crate::mesh::state::MIN_LEASE_SECS;
    use crate::mesh::{InviteToken, MeshConfig, NodeRecord, generate_keypair};
    use crate::service::MeshLocal;
    use crate::testing::{TestNode, fast_config};
    use crate::{NodeHandle, start};

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    /// Node 1 is the leader in every row.
    #[test]
    fn the_quorum_guard() {
        // (voters, remove, caught up, allowed, why)
        type Row<'a> = (&'a [NodeId], &'a [NodeId], &'a [NodeId], bool, &'a str);
        let table: &[Row] = &[
            (&[1], &[], &[], true, "one voter, nothing to remove"),
            (&[1], &[9], &[], true, "one voter removes a learner"),
            (&[1], &[1], &[], false, "the only voter cannot leave"),
            (
                &[1, 2],
                &[2],
                &[],
                false,
                "two voters, the other dead: the joint config needs both",
            ),
            (&[1, 2], &[2], &[2], true, "two voters, the other alive"),
            (
                &[1, 2, 3],
                &[2],
                &[2],
                false,
                "three voters, 3 dead, 2 expired but alive: 1 and 3 would remain",
            ),
            (&[1, 2, 3], &[3], &[2], true, "three voters, 3 dead"),
            (&[1, 2, 3], &[2, 3], &[2], true, "three voters, 2 and 3 go"),
            (
                &[1, 2, 3],
                &[3],
                &[],
                false,
                "three voters, both others dead",
            ),
            (
                &[1, 2, 3, 4, 5],
                &[4, 5],
                &[2, 3],
                true,
                "five voters, two dead",
            ),
            (
                &[1, 2, 3, 4, 5],
                &[4, 5],
                &[2],
                false,
                "five voters, three dead: nothing commits",
            ),
            (
                &[1, 2, 3, 4, 5],
                &[3],
                &[2, 3],
                false,
                "five voters, two dead, a live one expired: 1 and 2 of four",
            ),
            (&[1, 2, 3], &[9], &[2], true, "a learner, quorum is there"),
            (&[1, 2, 3], &[9], &[], false, "a learner, no quorum"),
            (&[1, 2, 3], &[3, 9], &[2], true, "a voter and a learner"),
            (&[1, 2, 3], &[1], &[2, 3], true, "the leader leaves"),
            (
                &[1, 2, 3],
                &[1],
                &[2],
                false,
                "the leader leaves, one of the two that remain is dead",
            ),
        ];
        for (voters, remove, caught_up, allowed, why) in table {
            assert_eq!(
                removal_allowed(&set(voters), &set(remove), 1, &set(caught_up)),
                *allowed,
                "{why}"
            );
        }
    }

    #[test]
    fn the_tick_follows_the_lease() {
        let t = LeaseTiming::default();
        let secs = Duration::from_secs;
        assert_eq!(t.tick(DEFAULT_LEASE_SECS), secs(60));
        assert_eq!(t.tick(MIN_LEASE_SECS), secs(5));
        assert_eq!(t.tick(600), secs(10));
        assert_eq!(t.tick(1_200), secs(20));
        assert_eq!(t.tick(3_600), secs(60));
        assert_eq!(t.tick(2_592_000), secs(60));
        assert_eq!(t.tick(0), secs(5));
        assert_eq!(t.tick(u64::MAX), secs(60));
    }

    #[test]
    fn the_first_tick_proposes_nothing_and_remainders_carry_over() {
        let (tick, second) = (Duration::from_secs(5), Duration::from_secs(1));
        let step = Duration::from_millis(5_400);
        let mut clock = TickClock::default();
        let mut now = Instant::now();
        assert_eq!(clock.elapsed_secs(now, 1, tick, second), 0);
        // 5.4 s three times is 16.2 s: 5, 5 and 6, with 0.2 s left over.
        let mut total = 0;
        for expected in [5, 5, 6] {
            now += step;
            let secs = clock.elapsed_secs(now, 1, tick, second);
            assert_eq!(secs, expected);
            total += secs;
        }
        assert_eq!(total, 16);
        assert_eq!(clock.carry, Duration::from_millis(200));

        // Ticks shorter than a second still add up.
        let mut clock = TickClock::default();
        assert_eq!(clock.elapsed_secs(now, 1, tick, second), 0);
        let mut total = 0;
        for _ in 0..10 {
            now += Duration::from_millis(300);
            total += clock.elapsed_secs(now, 1, tick, second);
        }
        assert_eq!(total, 3);
    }

    #[test]
    fn a_long_gap_counts_as_two_ticks_and_a_new_spell_starts_at_zero() {
        let (tick, second) = (Duration::from_secs(5), Duration::from_secs(1));
        let mut clock = TickClock::default();
        let mut now = Instant::now();
        assert_eq!(clock.elapsed_secs(now, 1, tick, second), 0);
        now += Duration::from_secs(3_600);
        assert_eq!(clock.elapsed_secs(now, 1, tick, second), 10);
        assert_eq!(clock.carry, Duration::ZERO);

        // Not leader for a while: that time is not measured.
        clock.forget();
        now += Duration::from_secs(3_600);
        assert_eq!(clock.elapsed_secs(now, 1, tick, second), 0);
        now += Duration::from_secs(5);
        assert_eq!(clock.elapsed_secs(now, 1, tick, second), 5);
    }

    /// A leader that lost and regained leadership between two of its ticks
    /// never saw itself as anything but the leader. The time in between was
    /// measured, and proposed, by whoever led meanwhile.
    #[test]
    fn a_new_term_starts_the_tick_clock_again() {
        let (tick, second) = (Duration::from_secs(5), Duration::from_secs(1));
        let mut clock = TickClock::default();
        let mut now = Instant::now();
        assert_eq!(clock.elapsed_secs(now, 3, tick, second), 0);
        now += Duration::from_millis(5_400);
        assert_eq!(clock.elapsed_secs(now, 3, tick, second), 5);
        assert_eq!(clock.carry, Duration::from_millis(400));

        // Leader again in a later term: nothing from before it counts.
        now += Duration::from_secs(5);
        assert_eq!(clock.elapsed_secs(now, 5, tick, second), 0);
        assert_eq!(clock.carry, Duration::ZERO);
        // From there on it measures as before.
        now += Duration::from_secs(5);
        assert_eq!(clock.elapsed_secs(now, 5, tick, second), 5);
    }

    #[test]
    fn a_quorum_ack_counts_only_within_the_limit() {
        let limit = Duration::from_secs(30);
        assert!(quorum_is_recent(Some(0), limit));
        assert!(quorum_is_recent(Some(30_000), limit));
        assert!(!quorum_is_recent(Some(30_001), limit));
        assert!(!quorum_is_recent(Some(u64::MAX), limit));
        // No quorum has acknowledged this leader yet.
        assert!(!quorum_is_recent(None, limit));
    }

    // --- with a running raft ------------------------------------------------

    const WAIT: Duration = Duration::from_secs(20);

    /// A lease clock that runs a thousand times faster: the shortest lease
    /// (300 s) runs out in about a third of a second.
    fn fast() -> LeaseTiming {
        LeaseTiming {
            tick_min: Duration::from_millis(10),
            tick_max: Duration::from_millis(10),
            second: Duration::from_millis(1),
            write_timeout: Duration::from_secs(2),
        }
    }

    fn instance(id: NodeId) -> String {
        format!("{id:02x}").repeat(16)
    }

    async fn seed_mesh(service: &FaberService) {
        let (_, public_key) = generate_keypair().unwrap();
        service
            .propose(Request::InitCluster {
                mesh: MeshConfig::default(),
                founder: NodeRecord {
                    node_id: 1,
                    public_key,
                    mesh_ip: "10.99.0.1".parse().unwrap(),
                    endpoint: Some("203.0.113.1:51820".into()),
                    invite_id: None,
                    instance_id: None,
                    container_subnet: None,
                    name: "node-1".into(),
                    lease_renewed_at: 0,
                },
            })
            .await
            .unwrap();
    }

    /// Invites a node called `name` through `service` (the leader). Returns
    /// the request the new machine registers with.
    async fn invite(service: &FaberService, name: &str) -> PeerRegisterRequest {
        let invite = service
            .invite(CreateInviteRequest {
                name: name.into(),
                endpoint: "198.51.100.2".into(),
                ttl_secs: None,
            })
            .await
            .unwrap();
        let token = InviteToken::decode(&invite.token).unwrap();
        PeerRegisterRequest {
            invite_id: token.invite_id,
            node_id: token.node_id,
            endpoint: token.endpoint,
            instance_id: instance(token.node_id),
        }
    }

    async fn wait_for(what: &str, check: impl Fn() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !check() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn mesh_of(service: &FaberService) -> MeshState {
        service.read_mesh().unwrap().unwrap()
    }

    /// Lets `secs` seconds of the lease clock go by. The clock moves only
    /// through committed ticks of a running leader task, so this waits for
    /// that many ticks, and a stalled leader makes it wait longer. Use it
    /// to look for something that must not happen, instead of a sleep.
    async fn clock_runs(service: &FaberService, secs: u64) {
        let until = mesh_of(service).clock + secs;
        wait_for("the lease clock to run on", || {
            mesh_of(service).clock >= until
        })
        .await;
    }

    /// The leader task, to be driven one tick at a time with `round`.
    fn leader_task(service: &FaberService, reader: StateReader, timing: LeaseTiming) -> LeaderTask {
        LeaderTask {
            service: service.clone(),
            reader,
            timing,
            clock: TickClock::default(),
            mark: None,
            joint_seen: None,
        }
    }

    struct Member {
        id: NodeId,
        handle: NodeHandle,
        _dir: TempDir,
    }

    impl Member {
        async fn start(id: NodeId) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let handle = start(fast_config(id, dir.path()), api, peer).await.unwrap();
            Self {
                id,
                handle,
                _dir: dir,
            }
        }

        fn service(&self) -> &FaberService {
            self.handle.service()
        }

        /// A service on this node's raft that knows the machine's instance
        /// id, as a mesh node's does.
        fn mesh_service(&self) -> FaberService {
            let (_, public_key) = generate_keypair().unwrap();
            let local = MeshLocal {
                config: MeshConfig::default(),
                record: NodeRecord {
                    node_id: self.id,
                    public_key,
                    mesh_ip: "10.99.0.1".parse().unwrap(),
                    endpoint: None,
                    invite_id: None,
                    instance_id: Some(instance(self.id)),
                    container_subnet: None,
                    name: String::new(),
                    lease_renewed_at: 0,
                },
                bootstrap_peers: BTreeSet::new(),
                join_pending: crate::mesh::JoinPending::new(false),
            };
            FaberService::new(
                self.id,
                self.handle.peer_addr().to_string(),
                self.handle.raft().clone(),
                self.handle.state().clone(),
                Some(local),
            )
            .unwrap()
        }

        async fn stop(self) {
            self.handle.shutdown().await.unwrap();
        }
    }

    /// `n` voters with a mesh: node 1 founds it, the others are invited as
    /// `node-<id>`, register and join. Node 1 leads.
    async fn cluster(n: u64) -> Vec<Member> {
        let first = Member::start(1).await;
        first.service().init().await.unwrap();
        first
            .handle
            .raft()
            .wait(Some(WAIT))
            .current_leader(1, "node 1 leads")
            .await
            .unwrap();
        seed_mesh(first.service()).await;
        let mut members = vec![first];
        for id in 2..=n {
            let leader = members[0].service();
            let register = invite(leader, &format!("node-{id}")).await;
            assert_eq!(register.node_id, id);
            let member = Member::start(id).await;
            let join = PeerJoinRequest {
                node_id: id,
                addr: member.handle.peer_addr().to_string(),
                invite_id: Some(register.invite_id.clone()),
                instance_id: Some(register.instance_id.clone()),
            };
            leader.peer_register(register, false).await.unwrap();
            leader.peer_join(join, false).await.unwrap();
            members.push(member);
        }
        members
    }

    /// Renews the leases of `ids` through `service` until cancelled, the way
    /// their own renew loops would.
    fn keep_renewed(
        service: FaberService,
        ids: &[NodeId],
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let ids = ids.to_vec();
        tokio::spawn(async move {
            while !cancel.is_cancelled() {
                for id in &ids {
                    let renew = Request::RenewLease {
                        node_id: *id,
                        instance_id: instance(*id),
                    };
                    let _ = service.propose(renew).await;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    }

    async fn stop_task(cancel: &CancellationToken, task: JoinHandle<()>) {
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the task stops")
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_leader_task_stops_when_cancelled() {
        let node = TestNode::start().await;
        let cancel = CancellationToken::new();
        let task = spawn_leader_task(node.service.clone(), node.state(), cancel.clone());
        // It runs until it is told to stop.
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        stop_task(&cancel, task).await;
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_leader_advances_the_clock_and_keeps_itself() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node.service).await;
        node.service.set_lease(MIN_LEASE_SECS, false).await.unwrap();
        let cancel = CancellationToken::new();
        let task =
            spawn_leader_task_with(node.service.clone(), node.state(), fast(), cancel.clone());

        // Well past the lease of node 1, which nobody renews.
        let svc = node.service.clone();
        wait_for("the clock to pass the lease", || {
            mesh_of(&svc).clock > 2 * MIN_LEASE_SECS
        })
        .await;
        let mesh = mesh_of(&svc);
        assert!(mesh.lease_expired(&mesh.nodes[&1]));
        assert!(svc.member_ids().contains(&1));
        // Its renewal is refused, with the answer that does not say that
        // the node was removed.
        let err = svc
            .propose(Request::RenewLease {
                node_id: 1,
                instance_id: instance(1),
            })
            .await
            .unwrap_err();
        assert_eq!(err.mesh_code(), Some(error_code::MESH_LEASE_EXPIRED));

        stop_task(&cancel, task).await;
        drop(svc);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn without_a_mesh_the_task_writes_nothing() {
        let node = TestNode::start_initialized().await;
        let before = node.service.raft().metrics().borrow().last_log_index;
        let mut task = leader_task(&node.service, node.state(), fast());
        for _ in 0..20 {
            task.round().await;
        }
        let after = node.service.raft().metrics().borrow().last_log_index;
        assert_eq!(before, after);
        node.shutdown().await;
    }

    /// Node 2 registered and became a learner at an address where nothing
    /// listens, and never renews. Node 3 registered and has not reached
    /// raft at all. Node 7 is a raft learner the mesh never heard of.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_and_unknown_learners_are_removed() {
        let node = TestNode::start_initialized().await;
        let svc = node.service.clone();
        seed_mesh(&svc).await;
        // The default lease: about half a minute on the fast clock, so no
        // lease runs out before the first check below.
        for name in ["node-2", "node-3"] {
            let register = invite(&svc, name).await;
            svc.peer_register(register, false).await.unwrap();
        }
        for id in [2, 7] {
            svc.raft()
                .add_learner(id, Node::new("127.0.0.1:1"), false)
                .await
                .unwrap();
        }
        assert_eq!(svc.member_ids(), set(&[1, 2, 7]));

        let cancel = CancellationToken::new();
        let task = spawn_leader_task_with(svc.clone(), node.state(), fast(), cancel.clone());

        // The unknown learner goes at once; the mesh nodes are untouched
        // while their leases are live.
        wait_for("the unknown learner to leave raft", || {
            !svc.member_ids().contains(&7)
        })
        .await;
        let mesh = mesh_of(&svc);
        assert!(
            mesh.nodes.values().all(|n| !mesh.lease_expired(n)),
            "a lease ran out before the check; clock {}",
            mesh.clock
        );
        assert_eq!(mesh.nodes.len(), 3, "{:?}", mesh.nodes.keys());
        assert!(svc.member_ids().contains(&2));

        // Once the leases run out: 2 leaves raft and the mesh, 3 the mesh.
        svc.set_lease(MIN_LEASE_SECS, false).await.unwrap();
        wait_for("the expired nodes to leave the mesh", || {
            mesh_of(&svc).nodes.len() == 1
        })
        .await;
        assert_eq!(svc.member_ids(), set(&[1]));
        let mesh = mesh_of(&svc);
        assert!(mesh.nodes.contains_key(&1));

        stop_task(&cancel, task).await;
        drop(svc);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dead_voter_is_removed_when_its_lease_runs_out() {
        let mut members = cluster(3).await;
        let leader = members[0].service().clone();
        leader.set_lease(MIN_LEASE_SECS, false).await.unwrap();
        let dead = members.pop().unwrap();
        dead.stop().await;

        let cancel = CancellationToken::new();
        let renewals = keep_renewed(leader.clone(), &[1, 2], cancel.clone());
        // The task runs on every node; only the leader acts.
        let tasks: Vec<_> = members
            .iter()
            .map(|m| {
                let state = m.handle.state().clone();
                spawn_leader_task_with(m.service().clone(), state, fast(), cancel.clone())
            })
            .collect();

        wait_for("node 3 to leave the mesh", || {
            !mesh_of(&leader).nodes.contains_key(&3)
        })
        .await;
        assert_eq!(leader.member_ids(), set(&[1, 2]));
        let mesh = mesh_of(&leader);
        assert_eq!(mesh.nodes.keys().copied().collect::<Vec<_>>(), [1, 2]);
        // The two that remain still commit.
        leader.set_lease(600, false).await.unwrap();

        cancel.cancel();
        renewals.await.unwrap();
        for task in tasks {
            task.await.unwrap();
        }
        drop(leader);
        for m in members {
            m.stop().await;
        }
    }

    /// Node 3 is dead but its lease is kept alive; node 2 is alive and its
    /// lease runs out. Removing 2 would leave voters 1 and 3, of which only
    /// 1 answers, so nothing could commit again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_live_expired_voter_stays_while_another_voter_is_dead() {
        let mut members = cluster(3).await;
        let leader = members[0].service().clone();
        leader.set_lease(MIN_LEASE_SECS, false).await.unwrap();
        let dead = members.pop().unwrap();
        dead.stop().await;

        let cancel = CancellationToken::new();
        let renewals = keep_renewed(leader.clone(), &[1, 3], cancel.clone());
        let state = members[0].handle.state().clone();
        let task = spawn_leader_task_with(leader.clone(), state, fast(), cancel.clone());

        wait_for("the lease of node 2 to run out", || {
            let mesh = mesh_of(&leader);
            mesh.lease_expired(&mesh.nodes[&2])
        })
        .await;
        // A whole lease of ticks later node 2 is still a voter and a mesh
        // node.
        clock_runs(&leader, MIN_LEASE_SECS).await;
        assert_eq!(leader.member_ids(), set(&[1, 2, 3]));
        assert!(leader.is_voter(2));
        assert!(mesh_of(&leader).nodes.contains_key(&2));
        // And the cluster still commits.
        leader.set_lease(600, false).await.unwrap();

        cancel.cancel();
        renewals.await.unwrap();
        task.await.unwrap();
        drop(leader);
        for m in members {
            m.stop().await;
        }
    }

    /// Voters 1, 2 and 3. Node 3 is dead, node 2 is alive and its lease has
    /// run out, as after a long partition. The guard keeps node 2 in raft:
    /// without it only node 1 of the voters 1 and 3 would answer. Node 2's
    /// own renew loop is told `mesh_lease_expired`, which is no proof that
    /// it was removed, so it stays a voter and the cluster keeps committing.
    /// When node 3's lease runs out as well, both leave, and node 2 learns
    /// that from the membership.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_expired_voter_whose_renewal_is_refused_stays_a_voter() {
        let mut members = cluster(3).await;
        let leader = members[0].service().clone();
        leader.set_lease(MIN_LEASE_SECS, false).await.unwrap();
        let dead = members.pop().unwrap();
        dead.stop().await;

        let cancel = CancellationToken::new();
        let keep_3 = cancel.child_token();
        let renew_1 = keep_renewed(leader.clone(), &[1], cancel.clone());
        let renew_3 = keep_renewed(leader.clone(), &[3], keep_3.clone());
        let state = members[0].handle.state().clone();
        let task = spawn_leader_task_with(leader.clone(), state, fast(), cancel.clone());
        wait_for("the lease of node 2 to run out", || {
            let mesh = mesh_of(&leader);
            mesh.lease_expired(&mesh.nodes[&2])
        })
        .await;

        // Node 2 comes back and renews, with the loop every mesh node runs.
        let node_2 = members[1].mesh_service();
        let reader = members[1].handle.state().clone();
        let stop = cancel.clone();
        let held =
            tokio::spawn(
                async move { crate::hold_lease(&node_2, &reader, fast().second, &stop).await },
            );
        // Two lease periods of refusals.
        clock_runs(&leader, 2 * MIN_LEASE_SECS).await;
        assert!(
            !held.is_finished(),
            "a refused renewal of an expired lease is no proof of removal"
        );
        assert!(leader.is_voter(2));
        let mesh = mesh_of(&leader);
        assert!(mesh.lease_expired(&mesh.nodes[&2]));
        // Node 3 is dead, so every commit needs node 2.
        let write = Request::Set {
            key: "k".into(),
            value: serde_json::json!(1),
        };
        tokio::time::timeout(Duration::from_secs(5), leader.propose(write))
            .await
            .expect("the cluster still commits")
            .unwrap();

        // Node 3 runs out too: both can go, and node 2 sees it happen.
        keep_3.cancel();
        renew_3.await.unwrap();
        let end = tokio::time::timeout(WAIT, held)
            .await
            .expect("node 2 learns that it was removed")
            .unwrap();
        assert!(matches!(end, crate::LeaseEnd::Removed(_)), "{end:?}");
        wait_for("nodes 2 and 3 to leave the mesh", || {
            mesh_of(&leader).nodes.len() == 1
        })
        .await;
        assert_eq!(leader.member_ids(), set(&[1]));

        cancel.cancel();
        renew_1.await.unwrap();
        task.await.unwrap();
        drop(leader);
        for m in members {
            m.stop().await;
        }
    }

    fn is_joint(service: &FaberService) -> bool {
        let m = service.raft().metrics().borrow().clone();
        m.membership_config.membership().get_joint_config().len() > 1
    }

    fn last_log_index(service: &FaberService) -> Option<u64> {
        service.raft().metrics().borrow().last_log_index
    }

    /// A removal of voter 3 that got as far as the joint config: the caller
    /// went away before raft was asked for the second step, as when the
    /// leader changes or a takeover's joiner never retries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_leader_finishes_a_joint_config_left_behind() {
        let members = cluster(3).await;
        let leader = members[0].service().clone();
        let raft = leader.raft().clone();
        {
            let change = raft.change_membership(ChangeMembers::RemoveVoters(set(&[3])), false);
            let wait = raft.wait(Some(WAIT));
            let joint = wait.metrics(
                |m| m.membership_config.membership().get_joint_config().len() > 1,
                "a joint config",
            );
            // The joint config shows in the metrics when it is appended,
            // before it commits and the first step answers.
            tokio::select! {
                biased;
                seen = joint => { seen.unwrap(); }
                _ = change => panic!("the membership change ran to its end"),
            }
        }
        // Nothing finishes it on its own, not even a commit that needs a
        // majority of both its voter sets.
        let write = Request::Set {
            key: "k".into(),
            value: serde_json::json!(1),
        };
        leader.propose(write).await.unwrap();
        assert!(is_joint(&leader));
        assert_eq!(leader.member_ids(), set(&[1, 2, 3]));

        let cancel = CancellationToken::new();
        let state = members[0].handle.state().clone();
        let task = spawn_leader_task_with(leader.clone(), state, fast(), cancel.clone());
        wait_for("the joint config to be finished", || !is_joint(&leader)).await;
        assert_eq!(leader.member_ids(), set(&[1, 2]));
        // Node 3 keeps its mesh record: its lease is live.
        assert!(mesh_of(&leader).nodes.contains_key(&3));
        leader.set_lease(600, false).await.unwrap();

        stop_task(&cancel, task).await;
        drop(leader);
        for m in members {
            m.stop().await;
        }
    }

    /// The only other voter is dead, so nothing commits. The task must not
    /// append another clock entry on every tick.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_leader_without_a_quorum_stops_writing_clock_entries() {
        let mut members = cluster(2).await;
        let leader = members[0].service().clone();
        let dead = members.pop().unwrap();
        dead.stop().await;

        let timing = LeaseTiming {
            write_timeout: Duration::from_millis(200),
            ..fast()
        };
        // The last quorum ack gets older than the limit.
        wait_for("the last quorum ack to be too old", || {
            !quorum_is_recent(leader.millis_since_quorum_ack(), timing.write_timeout)
        })
        .await;
        assert!(leader.is_leader());
        let before = last_log_index(&leader);
        let mut task = leader_task(&leader, members[0].handle.state().clone(), timing);
        for _ in 0..20 {
            task.round().await;
        }
        assert_eq!(last_log_index(&leader), before);

        drop(task);
        drop(leader);
        for m in members {
            m.stop().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_follower_forwards_lease_changes_and_renewals() {
        let members = cluster(2).await;
        let leader = members[0].service().clone();
        let follower = members[1].mesh_service();
        assert!(!follower.is_leader());

        assert_eq!(
            follower.set_lease(600, false).await.unwrap().lease_secs,
            600
        );
        assert_eq!(mesh_of(&leader).lease_secs, 600);
        // The follower's own copy follows.
        let f = follower.clone();
        wait_for("the follower to apply the lease", || {
            mesh_of(&f).lease_secs == 600
        })
        .await;
        assert_eq!(follower.lease().await.unwrap().lease_secs, 600);

        // The bounds are the leader's answer, passed through.
        let err = follower.set_lease(1, false).await.unwrap_err();
        let ServiceError::Upstream { status, body } = &err else {
            panic!("{err:?}");
        };
        assert_eq!(
            (*status, body.error.as_str()),
            (400, error_code::BAD_REQUEST)
        );
        // A request that was forwarded once is not forwarded again.
        let err = follower.set_lease(600, true).await.unwrap_err();
        assert!(matches!(err, ServiceError::NotLeader { .. }), "{err:?}");

        // A renewal from the follower lands on its own record.
        leader
            .propose(Request::AdvanceClock { elapsed_secs: 30 })
            .await
            .unwrap();
        follower.renew_own_lease().await.unwrap();
        let mesh = mesh_of(&leader);
        assert_eq!(mesh.clock, 30);
        assert_eq!(mesh.nodes[&2].lease_renewed_at, 30);
        assert_eq!(mesh.nodes[&1].lease_renewed_at, 0);

        // Once its lease has run out the answer names the reason, through
        // the forward too.
        for _ in 0..10 {
            leader
                .propose(Request::AdvanceClock { elapsed_secs: 60 })
                .await
                .unwrap();
        }
        let err = follower.renew_own_lease().await.unwrap_err();
        assert!(matches!(err, ServiceError::Upstream { .. }), "{err:?}");
        assert_eq!(err.mesh_code(), Some(error_code::MESH_LEASE_EXPIRED));

        drop((leader, follower, f));
        for m in members {
            m.stop().await;
        }
    }

    /// A new machine registers with an invite for the name of node 3, whose
    /// machine is gone. It gets the name and the subnet; node 3 leaves raft
    /// and the mesh.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_takeover_removes_the_old_node_from_raft_first() {
        let mut members = cluster(3).await;
        let leader = members[0].service().clone();
        let old = mesh_of(&leader).nodes[&3].clone();
        let dead = members.pop().unwrap();
        dead.stop().await;

        let register = invite(&leader, "node-3").await;
        let new_id = register.node_id;
        assert_ne!(new_id, 3);
        leader.peer_register(register.clone(), false).await.unwrap();

        assert_eq!(leader.member_ids(), set(&[1, 2]));
        let mesh = mesh_of(&leader);
        assert!(!mesh.nodes.contains_key(&3));
        let new = &mesh.nodes[&new_id];
        assert_eq!(new.name, "node-3");
        assert_eq!(new.container_subnet, old.container_subnet);
        assert_ne!(new.mesh_ip, old.mesh_ip);
        // The same machine again: its first answer may have been lost.
        leader.peer_register(register, false).await.unwrap();

        drop(leader);
        for m in members {
            m.stop().await;
        }
    }

    /// Node 3 is dead and the name is that of node 2, which is alive.
    /// Removing node 2 would leave voters 1 and 3, so the takeover waits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_takeover_that_would_stall_raft_is_not_ready() {
        let mut members = cluster(3).await;
        let leader = members[0].service().clone();
        let register = invite(&leader, "node-2").await;
        let dead = members.pop().unwrap();
        dead.stop().await;

        let err = leader.peer_register(register, false).await.unwrap_err();
        assert!(matches!(err, ServiceError::NotReady(_)), "{err:?}");
        assert_eq!(leader.member_ids(), set(&[1, 2, 3]));
        assert_eq!(mesh_of(&leader).nodes[&2].name, "node-2");

        drop(leader);
        for m in members {
            m.stop().await;
        }
    }

    /// The name is the leader's own. It removes itself from raft, steps
    /// down and answers `not_ready`; the retry reaches the next leader,
    /// which finishes the registration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_takeover_of_the_leader_finishes_on_the_next_leader() {
        let members = cluster(3).await;
        let old_leader = members[0].service().clone();
        let register = invite(&old_leader, "node-1").await;
        let new_id = register.node_id;

        let err = old_leader
            .peer_register(register.clone(), false)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::NotReady(_)), "{err:?}");

        let others: Vec<FaberService> = members[1..].iter().map(|m| m.service().clone()).collect();
        wait_for("node 2 or 3 to lead", || {
            others.iter().any(|s| s.is_leader())
        })
        .await;
        let next = others.iter().find(|s| s.is_leader()).unwrap();
        assert_eq!(next.member_ids(), set(&[2, 3]));
        // The joiner's retry, as it would arrive at either node.
        let deadline = Instant::now() + WAIT;
        while let Err(e) = next.peer_register(register.clone(), false).await {
            assert!(
                Instant::now() < deadline,
                "the retry never succeeded: {e:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mesh = mesh_of(next);
        assert!(!mesh.nodes.contains_key(&1));
        assert_eq!(mesh.nodes[&new_id].name, "node-1");
        // The old leader no longer leads.
        assert!(!old_leader.is_leader());

        drop((old_leader, others));
        for m in members {
            m.stop().await;
        }
    }
}
