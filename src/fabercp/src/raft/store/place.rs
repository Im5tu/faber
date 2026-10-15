//! Replica placement commands and records.
//!
//! The leader's planner ([`crate::placement`]) decides which node runs each
//! replica of each workload. Apply does no planning: it checks that the plan
//! is current and well formed, and stores it.
//!
//! Two kinds of key in `sm_data`, both reserved so that `Request::Set`
//! cannot write them:
//!
//! - [`place_key`] holds one [`PlacementRecord`] per environment: the
//!   generation of the environment record the plan was computed from, and
//!   per workload its uid range and the node of each replica.
//! - [`KEY_UIDS`] holds the [`UidAllocator`]: which 65536-wide host uid
//!   ranges are taken. A workload gets its range in the apply that first
//!   places it and gives it back in the apply that no longer lists it.
//!
//! Apply writes nothing else. In particular it never touches the
//! environment record, so no generation moves and the planner and the
//! reconcile loop do not wake each other up.
//!
//! The functions here are pure, like `store::status`: `apply` reads the
//! current records, calls [`place_replicas`] and writes what it returns.

use std::collections::{BTreeMap, BTreeSet};

use faber_api_types::error_code;
use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::limits::MAX_REPLICAS;
use serde::{Deserialize, Serialize};

use crate::apps::EnvironmentRecord;
use crate::mesh::{MeshState, Rejection};

/// Every placement record is under this prefix. `Request::Set` refuses it.
pub const KEY_PREFIX: &str = "place/";
/// The uid allocator. Outside [`KEY_PREFIX`] so that a scan of the records
/// never sees it. `Request::Set` refuses it.
pub const KEY_UIDS: &str = "place-uids";

/// First host uid a workload can get. `FABER_NODE_UID_MIN` of faber-node must
/// be at most this.
pub const UID_RANGE_START: u32 = 1_048_576;
/// One past the last host uid a workload can get. `FABER_NODE_UID_MAX` of
/// faber-node must be at least the last uid of the range, `UID_RANGE_END - 1`.
pub const UID_RANGE_END: u32 = 2_147_483_648;
/// Width of the uid range of one workload. A `uid_base` is a multiple of it.
pub const UID_BLOCK: u32 = 65_536;
/// How many workloads the uid range holds.
pub const UID_SLOTS: u32 = (UID_RANGE_END - UID_RANGE_START) / UID_BLOCK;

/// Longest `reason` of a replica, in bytes.
pub const MAX_REASON_BYTES: usize = 64;

/// Why a replica has no node. Fixed words, so that the same inputs always
/// give the same plan.
pub mod reason {
    /// No candidate node has the capacity left for the replica.
    pub const INSUFFICIENT_CAPACITY: &str = "insufficient_capacity";
    /// The node that held the replica's block volume is gone.
    pub const VOLUME_LOST: &str = "volume_lost";
    /// `placement.nodes` names a node the cluster does not have.
    pub const PINNED_NODE_UNKNOWN: &str = "pinned_node_unknown";
    /// No node is both allowed and alive.
    pub const NO_NODE_AVAILABLE: &str = "no_node_available";
}

/// The key of the placement record of an environment.
pub fn place_key(project: &str, environment: &str) -> String {
    format!("{KEY_PREFIX}{project}/{environment}")
}

/// Whether only the typed commands of this module may write `key`.
/// `Request::Set` rejects these.
pub fn is_reserved_key(key: &str) -> bool {
    key == KEY_UIDS || key.starts_with(KEY_PREFIX)
}

/// Where one replica runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaPlacement {
    pub index: u32,
    /// `None` while the replica is unplaced; `reason` then says why.
    pub node: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ReplicaPlacement {
    pub fn on(index: u32, node: u64) -> Self {
        Self {
            index,
            node: Some(node),
            reason: None,
        }
    }

    pub fn unplaced(index: u32, reason: &str) -> Self {
        Self {
            index,
            node: None,
            reason: Some(reason.to_string()),
        }
    }
}

/// The plan for the workloads of one environment.
///
/// Assigns the replicas of every workload of the environment to nodes.
/// `generation` is the generation of the environment record the plan was
/// computed from. A workload the command leaves out is no longer placed,
/// and its uid range is free again. With no workloads, for an environment
/// that no longer exists, the command only cleans up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceReplicas {
    pub project: String,
    pub environment: String,
    pub generation: u64,
    /// The index of the last log entry the leader had applied when it read
    /// the state the plan was computed from. A plan whose basis is older than
    /// the stored plan's is refused: it was made from older state.
    #[serde(default)]
    pub basis: u64,
    /// The replicas of each workload, by workload name.
    pub workloads: BTreeMap<String, Vec<ReplicaPlacement>>,
}

/// One workload in a [`PlacementRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadPlacement {
    /// The first host uid of the workload's range of [`UID_BLOCK`] uids. It
    /// belongs to the workload until the workload is removed.
    pub uid_base: u32,
    /// Sorted by replica index.
    pub replicas: Vec<ReplicaPlacement>,
}

/// The record under [`place_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRecord {
    /// The generation of the environment record the plan came from.
    pub generation: u64,
    /// The basis of the command that stored the plan.
    #[serde(default)]
    pub basis: u64,
    pub workloads: BTreeMap<String, WorkloadPlacement>,
}

impl PlacementRecord {
    /// The replicas of `workload`, empty when it has none placed.
    pub fn replicas(&self, workload: &str) -> &[ReplicaPlacement] {
        self.workloads
            .get(workload)
            .map_or(&[], |w| w.replicas.as_slice())
    }
}

/// Which uid ranges are taken. Deterministic: the lowest free range goes
/// first, so every replica of the state machine hands out the same ones.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UidAllocator {
    /// Slots below this one are taken or in `free`.
    pub next: u32,
    /// Slots below `next` that were given back.
    pub free: BTreeSet<u32>,
}

impl UidAllocator {
    /// The `uid_base` of a free range, now taken. `None` when all are taken.
    pub fn allocate(&mut self) -> Option<u32> {
        let slot = match self.free.pop_first() {
            Some(slot) => slot,
            None if self.next < UID_SLOTS => {
                self.next += 1;
                self.next - 1
            }
            None => return None,
        };
        Some(UID_RANGE_START + slot * UID_BLOCK)
    }

    /// Gives `uid_base` back. A value that is not a range handed out by
    /// [`Self::allocate`] changes nothing.
    pub fn release(&mut self, uid_base: u32) {
        let Some(offset) = uid_base.checked_sub(UID_RANGE_START) else {
            return;
        };
        let slot = offset / UID_BLOCK;
        if offset % UID_BLOCK == 0 && slot < self.next {
            self.free.insert(slot);
        }
    }
}

/// What `PlaceReplicas` stores: the record for the environment, or `None`
/// to delete it, and the allocator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub record: Option<PlacementRecord>,
    pub uids: UidAllocator,
}

fn bad_request(message: impl Into<String>) -> Rejection {
    Rejection::new(error_code::BAD_REQUEST, message)
}

/// `PlaceReplicas`, against the environment, the mesh, the record and the
/// allocator as the entry sees them. Checked in this order:
///
/// - A name that is no DNS label: `bad_request`.
/// - The environment does not exist: with no workloads in the command, the
///   record is deleted and its uid ranges are freed; otherwise `not_found`.
/// - A generation other than the environment's, or older than the stored
///   plan's, or a basis older than the stored plan's: `bad_request`. The plan
///   was made from state that has changed, or from older state than the one
///   stored.
/// - A workload the environment does not have, more than
///   [`MAX_REPLICAS`] replicas, a replica index twice, or a `reason` over
///   [`MAX_REASON_BYTES`]: `bad_request`.
/// - A node that is not in the mesh: `bad_request`. Without a mesh,
///   `mesh_not_configured`.
/// - All uid ranges taken: `workloads_exhausted`.
///
/// A rejection stores nothing.
pub fn place_replicas(
    environment: Option<&EnvironmentRecord>,
    mesh: Option<&MeshState>,
    stored: Option<&PlacementRecord>,
    uids: &UidAllocator,
    cmd: PlaceReplicas,
) -> Result<Placed, Rejection> {
    validate_dns_label(&cmd.project).map_err(|e| bad_request(format!("project: {e}")))?;
    validate_dns_label(&cmd.environment).map_err(|e| bad_request(format!("environment: {e}")))?;
    let mut uids = uids.clone();
    let Some(environment) = environment else {
        if !cmd.workloads.is_empty() {
            return Err(Rejection::new(
                error_code::NOT_FOUND,
                format!(
                    "environment {:?} of project {:?} does not exist",
                    cmd.environment, cmd.project
                ),
            ));
        }
        for w in stored.iter().flat_map(|r| r.workloads.values()) {
            uids.release(w.uid_base);
        }
        return Ok(Placed { record: None, uids });
    };
    if stored.is_some_and(|r| r.basis > cmd.basis) {
        return Err(bad_request(format!(
            "the plan is made from state up to entry {}, a newer plan is stored",
            cmd.basis
        )));
    }
    if cmd.generation != environment.generation
        || stored.is_some_and(|r| r.generation > cmd.generation)
    {
        return Err(bad_request(format!(
            "the plan is for generation {}, the environment is at {}",
            cmd.generation, environment.generation
        )));
    }
    for (name, replicas) in &cmd.workloads {
        if !environment.workloads.contains_key(name) {
            return Err(bad_request(format!("workload {name:?} does not exist")));
        }
        check_replicas(name, replicas, mesh)?;
    }

    let mut workloads = BTreeMap::new();
    for (name, mut replicas) in cmd.workloads {
        let uid_base = match stored.and_then(|r| r.workloads.get(&name)) {
            Some(w) => w.uid_base,
            None => uids.allocate().ok_or_else(|| {
                Rejection::new(
                    error_code::WORKLOADS_EXHAUSTED,
                    "every host uid range is taken",
                )
            })?,
        };
        replicas.sort_by_key(|r| r.index);
        workloads.insert(name, WorkloadPlacement { uid_base, replicas });
    }
    for (name, w) in stored.iter().flat_map(|r| &r.workloads) {
        if !workloads.contains_key(name) {
            uids.release(w.uid_base);
        }
    }
    let record = (!workloads.is_empty()).then_some(PlacementRecord {
        generation: cmd.generation,
        basis: cmd.basis,
        workloads,
    });
    Ok(Placed { record, uids })
}

fn check_replicas(
    workload: &str,
    replicas: &[ReplicaPlacement],
    mesh: Option<&MeshState>,
) -> Result<(), Rejection> {
    if replicas.len() > MAX_REPLICAS as usize {
        return Err(bad_request(format!(
            "workload {workload:?} has more than {MAX_REPLICAS} replicas"
        )));
    }
    let mut indexes = BTreeSet::new();
    for r in replicas {
        if !indexes.insert(r.index) {
            return Err(bad_request(format!(
                "workload {workload:?} has replica {} twice",
                r.index
            )));
        }
        if r.reason
            .as_ref()
            .is_some_and(|s| s.len() > MAX_REASON_BYTES)
        {
            return Err(bad_request(format!(
                "a reason is at most {MAX_REASON_BYTES} bytes"
            )));
        }
        if let Some(node) = r.node {
            let mesh = mesh.ok_or_else(|| {
                Rejection::new(error_code::MESH_NOT_CONFIGURED, "the cluster has no mesh")
            })?;
            if !mesh.nodes.contains_key(&node) {
                return Err(bad_request(format!("node {node} is not in the mesh")));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::WorkloadRecord;
    use crate::mesh::state::{MeshConfig, NodeRecord};

    fn node(id: u64) -> NodeRecord {
        NodeRecord {
            node_id: id,
            public_key: "k".into(),
            mesh_ip: format!("10.99.0.{id}").parse().unwrap(),
            endpoint: None,
            invite_id: None,
            instance_id: None,
            container_subnet: None,
            name: format!("node-{id}"),
            lease_renewed_at: 0,
        }
    }

    fn mesh(ids: &[u64]) -> MeshState {
        let mut mesh = MeshState::init(MeshConfig::default(), node(1)).unwrap();
        mesh.nodes = ids.iter().map(|id| (*id, node(*id))).collect();
        mesh
    }

    fn env(generation: u64, names: &[&str]) -> EnvironmentRecord {
        let spec: faber_api_types::workload::WorkloadSpec = serde_json::from_value(
            serde_json::json!({"containers": [{"name": "app", "image": "nginx"}]}),
        )
        .unwrap();
        EnvironmentRecord {
            generation,
            updated_at: 0,
            workloads: names
                .iter()
                .map(|n| {
                    (
                        n.to_string(),
                        WorkloadRecord {
                            revision: 1,
                            spec: spec.clone(),
                            env: BTreeMap::new(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn cmd(generation: u64, workloads: &[(&str, &[Option<u64>])]) -> PlaceReplicas {
        PlaceReplicas {
            project: "shop".into(),
            environment: "staging".into(),
            generation,
            basis: 0,
            workloads: workloads
                .iter()
                .map(|(name, nodes)| {
                    let replicas = nodes
                        .iter()
                        .enumerate()
                        .map(|(i, n)| match n {
                            Some(n) => ReplicaPlacement::on(i as u32, *n),
                            None => {
                                ReplicaPlacement::unplaced(i as u32, reason::INSUFFICIENT_CAPACITY)
                            }
                        })
                        .collect();
                    (name.to_string(), replicas)
                })
                .collect(),
        }
    }

    fn code(r: Result<Placed, Rejection>) -> Option<String> {
        r.err().map(|r| r.code)
    }

    #[test]
    fn a_plan_is_stored_with_the_generation_it_came_from() {
        let env = env(7, &["web"]);
        let mesh = mesh(&[1, 2]);
        let placed = place_replicas(
            Some(&env),
            Some(&mesh),
            None,
            &UidAllocator::default(),
            cmd(7, &[("web", &[Some(2), None])]),
        )
        .unwrap();
        let record = placed.record.unwrap();
        assert_eq!(record.generation, 7);
        assert_eq!(record.workloads["web"].uid_base, UID_RANGE_START);
        assert_eq!(
            record.replicas("web"),
            [
                ReplicaPlacement::on(0, 2),
                ReplicaPlacement::unplaced(1, reason::INSUFFICIENT_CAPACITY)
            ]
        );
        assert!(record.replicas("db").is_empty());
    }

    #[test]
    fn a_stale_or_future_generation_is_rejected() {
        let mesh = mesh(&[1]);
        let uids = UidAllocator::default();
        // Older than the environment's.
        let got = place_replicas(
            Some(&env(8, &["web"])),
            Some(&mesh),
            None,
            &uids,
            cmd(7, &[("web", &[Some(1)])]),
        );
        assert_eq!(code(got).as_deref(), Some(error_code::BAD_REQUEST));
        // Newer than the environment's.
        let got = place_replicas(
            Some(&env(8, &["web"])),
            Some(&mesh),
            None,
            &uids,
            cmd(9, &[("web", &[Some(1)])]),
        );
        assert!(got.is_err());
        // Older than the stored plan's, whatever the environment says.
        let stored = PlacementRecord {
            generation: 9,
            basis: 0,
            workloads: BTreeMap::new(),
        };
        let got = place_replicas(
            Some(&env(8, &["web"])),
            Some(&mesh),
            Some(&stored),
            &uids,
            cmd(8, &[("web", &[Some(1)])]),
        );
        assert!(got.is_err());
        // The same generation again is fine: capacity moved, not the spec.
        let stored = PlacementRecord {
            generation: 8,
            basis: 0,
            workloads: BTreeMap::new(),
        };
        let got = place_replicas(
            Some(&env(8, &["web"])),
            Some(&mesh),
            Some(&stored),
            &uids,
            cmd(8, &[("web", &[Some(1)])]),
        );
        assert!(got.is_ok());
    }

    #[test]
    fn a_plan_made_from_older_state_than_the_stored_one_is_rejected() {
        let env = env(8, &["web"]);
        let mesh = mesh(&[1, 2]);
        let uids = UidAllocator::default();
        let stored = PlacementRecord {
            generation: 8,
            basis: 20,
            workloads: BTreeMap::new(),
        };
        let run = |basis| {
            let mut c = cmd(8, &[("web", &[Some(2)])]);
            c.basis = basis;
            place_replicas(Some(&env), Some(&mesh), Some(&stored), &uids, c)
        };
        assert_eq!(code(run(19)).as_deref(), Some(error_code::BAD_REQUEST));
        // The same basis or a newer one is fine; the record keeps the basis.
        assert!(run(20).is_ok());
        let placed = run(25).unwrap();
        assert_eq!(placed.record.unwrap().basis, 25);
    }

    #[test]
    fn a_node_that_is_not_in_the_mesh_is_rejected() {
        let env = env(1, &["web"]);
        let uids = UidAllocator::default();
        let got = place_replicas(
            Some(&env),
            Some(&mesh(&[1, 2])),
            None,
            &uids,
            cmd(1, &[("web", &[Some(1), Some(3)])]),
        );
        assert_eq!(code(got).as_deref(), Some(error_code::BAD_REQUEST));
        let got = place_replicas(
            Some(&env),
            None,
            None,
            &uids,
            cmd(1, &[("web", &[Some(1)])]),
        );
        assert_eq!(code(got).as_deref(), Some(error_code::MESH_NOT_CONFIGURED));
        // No node assigned: no mesh needed.
        assert!(place_replicas(Some(&env), None, None, &uids, cmd(1, &[("web", &[None])])).is_ok());
    }

    #[test]
    fn malformed_plans_are_rejected() {
        let env = env(1, &["web"]);
        let mesh = mesh(&[1]);
        let uids = UidAllocator::default();
        let run = |c: PlaceReplicas| place_replicas(Some(&env), Some(&mesh), None, &uids, c);
        // A workload the environment does not have.
        assert!(run(cmd(1, &[("db", &[Some(1)])])).is_err());
        // A replica index twice.
        let mut twice = cmd(1, &[("web", &[Some(1), Some(1)])]);
        twice.workloads.get_mut("web").unwrap()[1].index = 0;
        assert!(run(twice).is_err());
        // A reason that is too long.
        let mut long = cmd(1, &[("web", &[None])]);
        long.workloads.get_mut("web").unwrap()[0].reason = Some("x".repeat(MAX_REASON_BYTES + 1));
        assert!(run(long).is_err());
        // Too many replicas.
        let many = vec![None; MAX_REPLICAS as usize + 1];
        assert!(run(cmd(1, &[("web", &many)])).is_err());
        // A name that is no label.
        let mut name = cmd(1, &[]);
        name.project = "Shop".into();
        assert!(run(name).is_err());
        // Nothing is stored for a rejection.
        assert_eq!(uids, UidAllocator::default());
    }

    #[test]
    fn uid_bases_are_aligned_unique_and_in_range() {
        let mut uids = UidAllocator::default();
        let mut seen = BTreeSet::new();
        for _ in 0..UID_SLOTS {
            let base = uids.allocate().unwrap();
            assert_eq!(base % UID_BLOCK, 0, "{base}");
            assert!(base >= UID_RANGE_START, "{base}");
            assert!(base + (UID_BLOCK - 1) < UID_RANGE_END, "{base}");
            assert!(seen.insert(base), "{base} handed out twice");
        }
        // The range is used up.
        assert_eq!(uids.allocate(), None);
        // The first range starts where faber-node's range does.
        assert_eq!(seen.first(), Some(&UID_RANGE_START));
        assert_eq!(seen.last(), Some(&(UID_RANGE_END - UID_BLOCK)));
    }

    #[test]
    fn a_released_range_is_handed_out_again_and_only_then() {
        let mut uids = UidAllocator::default();
        let a = uids.allocate().unwrap();
        let b = uids.allocate().unwrap();
        let c = uids.allocate().unwrap();
        uids.release(b);
        // Released twice, or never handed out, or not a range: no effect.
        uids.release(b);
        uids.release(b + 1);
        uids.release(0);
        uids.release(c + UID_BLOCK);
        assert_eq!(uids.allocate(), Some(b));
        let d = uids.allocate().unwrap();
        assert!(![a, b, c].contains(&d));
    }

    #[test]
    fn new_workloads_get_ranges_and_removed_ones_give_them_back() {
        let mesh = mesh(&[1]);
        let both = env(1, &["db", "web"]);
        let first = place_replicas(
            Some(&both),
            Some(&mesh),
            None,
            &UidAllocator::default(),
            cmd(1, &[("db", &[Some(1)]), ("web", &[Some(1)])]),
        )
        .unwrap();
        let record = first.record.unwrap();
        let (db, web) = (
            record.workloads["db"].uid_base,
            record.workloads["web"].uid_base,
        );
        assert_ne!(db, web);

        // A later plan keeps the ranges of the workloads it keeps.
        let again = place_replicas(
            Some(&env(2, &["db", "web"])),
            Some(&mesh),
            Some(&record),
            &first.uids,
            cmd(2, &[("db", &[None]), ("web", &[Some(1)])]),
        )
        .unwrap();
        let record = again.record.unwrap();
        assert_eq!(record.workloads["db"].uid_base, db);
        assert_eq!(record.workloads["web"].uid_base, web);
        assert_eq!(again.uids, first.uids);

        // `db` is removed: its range is free and the next workload takes it.
        let removed = place_replicas(
            Some(&env(3, &["web"])),
            Some(&mesh),
            Some(&record),
            &again.uids,
            cmd(3, &[("web", &[Some(1)])]),
        )
        .unwrap();
        let record = removed.record.unwrap();
        assert!(!record.workloads.contains_key("db"));
        assert_eq!(removed.uids.free, BTreeSet::from([0]));
        let added = place_replicas(
            Some(&env(4, &["cache", "web"])),
            Some(&mesh),
            Some(&record),
            &removed.uids,
            cmd(4, &[("cache", &[Some(1)]), ("web", &[Some(1)])]),
        )
        .unwrap();
        let record = added.record.unwrap();
        assert_eq!(record.workloads["cache"].uid_base, db);
        assert_eq!(record.workloads["web"].uid_base, web);
    }

    #[test]
    fn a_plan_for_a_missing_environment_only_cleans_up() {
        let mesh = mesh(&[1]);
        let placed = place_replicas(
            Some(&env(1, &["web"])),
            Some(&mesh),
            None,
            &UidAllocator::default(),
            cmd(1, &[("web", &[Some(1)])]),
        )
        .unwrap();
        let record = placed.record.unwrap();
        // The environment is gone and the leader says so with no workloads.
        let cleaned =
            place_replicas(None, Some(&mesh), Some(&record), &placed.uids, cmd(5, &[])).unwrap();
        assert_eq!(cleaned.record, None);
        assert_eq!(cleaned.uids.free, BTreeSet::from([0]));
        // A plan with workloads for a missing environment is refused.
        let got = place_replicas(
            None,
            Some(&mesh),
            None,
            &UidAllocator::default(),
            cmd(1, &[("web", &[Some(1)])]),
        );
        assert_eq!(code(got).as_deref(), Some(error_code::NOT_FOUND));
    }

    #[test]
    fn an_environment_with_no_workloads_has_no_record() {
        let placed = place_replicas(
            Some(&env(2, &[])),
            None,
            None,
            &UidAllocator::default(),
            cmd(2, &[]),
        )
        .unwrap();
        assert_eq!(placed.record, None);
    }

    #[test]
    fn a_full_uid_range_rejects_the_plan() {
        let full = UidAllocator {
            next: UID_SLOTS,
            free: BTreeSet::new(),
        };
        let got = place_replicas(
            Some(&env(1, &["web"])),
            Some(&mesh(&[1])),
            None,
            &full,
            cmd(1, &[("web", &[Some(1)])]),
        );
        assert_eq!(code(got).as_deref(), Some(error_code::WORKLOADS_EXHAUSTED));
    }

    #[test]
    fn reserved_keys() {
        assert!(is_reserved_key(&place_key("shop", "staging")));
        assert!(is_reserved_key(KEY_UIDS));
        assert!(!is_reserved_key("placement"));
    }

    #[test]
    fn records_have_a_pinned_shape() {
        let record = PlacementRecord {
            generation: 3,
            basis: 4,
            workloads: BTreeMap::from([(
                "web".to_string(),
                WorkloadPlacement {
                    uid_base: UID_RANGE_START,
                    replicas: vec![
                        ReplicaPlacement::on(0, 2),
                        ReplicaPlacement::unplaced(1, reason::VOLUME_LOST),
                    ],
                },
            )]),
        };
        assert_eq!(
            serde_json::to_value(&record).unwrap(),
            serde_json::json!({
                "generation": 3,
                "basis": 4,
                "workloads": {"web": {"uid_base": 1_048_576, "replicas": [
                    {"index": 0, "node": 2},
                    {"index": 1, "node": null, "reason": "volume_lost"},
                ]}}
            })
        );
    }
}
