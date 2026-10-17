//! The roll gate: which replicas of a block-volume workload must wait.
//!
//! A block volume belongs to one replica index on one node, so the old
//! replica has to stop before the new one starts. Done for every index at
//! once, the service's virtual IP would have no backend for a while. Instead
//! the indices roll in order, and every node decides from replicated state
//! alone, so the nodes agree without talking to each other:
//!
//! > A node may replace replica index `i` with the newest revision only when
//! > every lower index is `available` on that revision, or has no node.
//!
//! - Index 0 is never gated. A single replica keeps its short gap.
//! - A lower index whose node stopped reporting reads `unavailable` (see
//!   [`crate::status::StatusView`]), so the roll waits for it. It does not
//!   wait for ever: when the node's lease expires placement leaves the
//!   replica unplaced (`volume_lost`), and an unplaced index no longer
//!   blocks the ones above it.
//! - A new revision that never becomes available holds the roll at that
//!   index. The higher indices keep their old replicas, which keep serving.
//! - A newer revision takes over the roll. The gate is judged against the
//!   newest revision, so it starts again at index 0, and an old replica that
//!   was not replaced yet is replaced by the newest revision directly.
//! - Only the decision to stop an old replica is gated. A replica whose old
//!   one is already stopping, or gone, starts without waiting, so a gate that
//!   closes after the stop never leaves a slot empty.

use std::collections::BTreeSet;

use faber_api_types::workload::{ReplicaState, ReplicaStatus};

/// The replica indices of one workload that must not replace their old
/// replica yet. `statuses` are the replicas of the newest revision, as
/// [`crate::status::StatusView::workload`] gives them: an unplaced one has no
/// node.
pub fn closed_gates(statuses: &[ReplicaStatus]) -> BTreeSet<u32> {
    let mut sorted: Vec<&ReplicaStatus> = statuses.iter().collect();
    sorted.sort_by_key(|s| s.replica_index);
    let mut closed = BTreeSet::new();
    let mut lower_ready = true;
    for s in sorted {
        if !lower_ready {
            closed.insert(s.replica_index);
        }
        lower_ready &= s.node.is_none() || s.state == ReplicaState::Available;
    }
    closed
}

#[cfg(test)]
mod tests {
    use ReplicaState::{Available, Deploying, Unavailable};

    use super::*;

    type Row = (u32, Option<u64>, ReplicaState);

    /// `(index, node, state)` for each replica.
    fn statuses(rows: &[Row]) -> Vec<ReplicaStatus> {
        rows.iter()
            .map(|(index, node, state)| ReplicaStatus {
                node: *node,
                replica_index: *index,
                revision: 9,
                state: *state,
                pod_ip: None,
                reason: None,
            })
            .collect()
    }

    #[test]
    fn the_gate_table() {
        let cases: &[(&str, &[Row], &[u32])] = &[
            ("no replicas", &[], &[]),
            (
                "a single replica is never gated",
                &[(0, Some(1), Deploying)],
                &[],
            ),
            (
                "all available: nothing waits",
                &[(0, Some(1), Available), (1, Some(2), Available)],
                &[],
            ),
            (
                "index 0 deploying holds the rest",
                &[
                    (0, Some(1), Deploying),
                    (1, Some(2), Available),
                    (2, Some(3), Available),
                ],
                &[1, 2],
            ),
            (
                "index 0 available, 1 deploying: only 2 waits",
                &[
                    (0, Some(1), Available),
                    (1, Some(2), Deploying),
                    (2, Some(3), Available),
                ],
                &[2],
            ),
            (
                "a stale node reads unavailable and holds the rest",
                &[(0, Some(1), Unavailable), (1, Some(2), Available)],
                &[1],
            ),
            (
                "an unplaced lower index (volume_lost) does not block",
                &[
                    (0, None, Unavailable),
                    (1, Some(2), Deploying),
                    (2, Some(3), Available),
                ],
                &[2],
            ),
            (
                "unplaced first, then a placed deploying one: it is open",
                &[(0, None, Unavailable), (1, Some(2), Deploying)],
                &[],
            ),
            (
                "unplaced in the middle does not block the next",
                &[
                    (0, Some(1), Available),
                    (1, None, Unavailable),
                    (2, Some(3), Deploying),
                ],
                &[],
            ),
            (
                "the order of the input does not matter",
                &[
                    (2, Some(3), Available),
                    (0, Some(1), Deploying),
                    (1, Some(2), Available),
                ],
                &[1, 2],
            ),
        ];
        for (name, rows, want) in cases {
            let got: Vec<u32> = closed_gates(&statuses(rows)).into_iter().collect();
            assert_eq!(got, *want, "{name}");
        }
    }
}
