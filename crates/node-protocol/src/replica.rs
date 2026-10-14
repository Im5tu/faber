use std::collections::{BTreeMap, HashMap};

use faber_api_types::name::validate_dns_label;
use serde::{Deserialize, Serialize};

pub const LABEL_MANAGED: &str = "faber.managed";
pub const LABEL_PROJECT: &str = "faber.project";
pub const LABEL_ENV: &str = "faber.env";
pub const LABEL_WORKLOAD: &str = "faber.workload";
pub const LABEL_REVISION: &str = "faber.revision";
pub const LABEL_REPLICA: &str = "faber.replica";
/// On containers only: the container's name in the workload spec.
pub const LABEL_CONTAINER: &str = "faber.container";

const REVISION_LEN: std::ops::RangeInclusive<usize> = 4..=32;

/// One replica of one revision of a workload.
///
/// Every name is a DNS label, so each is safe as a single path segment; the
/// revision is a short lower-case hex hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaRef {
    pub project: String,
    pub env: String,
    pub workload: String,
    pub revision: String,
    pub replica_index: u32,
}

impl ReplicaRef {
    /// Checks every name. The error says which field is wrong.
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("project", &self.project),
            ("env", &self.env),
            ("workload", &self.workload),
        ] {
            validate_dns_label(value).map_err(|e| format!("{field}: {e}"))?;
        }
        if !REVISION_LEN.contains(&self.revision.len())
            || !self
                .revision
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(format!(
                "revision: must be {} to {} lower-case hex characters",
                REVISION_LEN.start(),
                REVISION_LEN.end()
            ));
        }
        Ok(())
    }

    /// The labels that identify the replica on its sandbox.
    pub fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (LABEL_MANAGED.to_owned(), "true".to_owned()),
            (LABEL_PROJECT.to_owned(), self.project.clone()),
            (LABEL_ENV.to_owned(), self.env.clone()),
            (LABEL_WORKLOAD.to_owned(), self.workload.clone()),
            (LABEL_REVISION.to_owned(), self.revision.clone()),
            (LABEL_REPLICA.to_owned(), self.replica_index.to_string()),
        ])
    }

    /// Reads a replica back from sandbox labels. `None` when a label is
    /// missing or the result does not [`validate`](Self::validate).
    pub fn from_labels(labels: &HashMap<String, String>) -> Option<Self> {
        if labels.get(LABEL_MANAGED).map(String::as_str) != Some("true") {
            return None;
        }
        let get = |key: &str| labels.get(key).cloned();
        let replica = Self {
            project: get(LABEL_PROJECT)?,
            env: get(LABEL_ENV)?,
            workload: get(LABEL_WORKLOAD)?,
            revision: get(LABEL_REVISION)?,
            replica_index: get(LABEL_REPLICA)?.parse().ok()?,
        };
        replica.validate().ok()?;
        Some(replica)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn replica() -> ReplicaRef {
        ReplicaRef {
            project: "shop".into(),
            env: "production".into(),
            workload: "redis".into(),
            revision: "a1b2c3d4".into(),
            replica_index: 2,
        }
    }

    #[test]
    fn accepts_a_well_formed_replica() {
        assert_eq!(replica().validate(), Ok(()));
    }

    #[test]
    fn rejects_path_like_and_non_label_names() {
        for bad in [
            "", "..", "a/b", "a.b", "A", "-a", "a b", "a\0b", "../etc", "a\\b",
        ] {
            for field in 0..3 {
                let mut r = replica();
                match field {
                    0 => r.project = bad.into(),
                    1 => r.env = bad.into(),
                    _ => r.workload = bad.into(),
                }
                assert!(r.validate().is_err(), "{bad:?} in field {field}");
            }
        }
    }

    #[test]
    fn rejects_bad_revisions() {
        for bad in [
            "",
            "abc",
            "ABCD1234",
            "zzzz",
            "a1b2/c3",
            "../..",
            &"a".repeat(33),
        ] {
            let mut r = replica();
            r.revision = bad.into();
            assert!(r.validate().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn labels_round_trip() {
        let r = replica();
        let labels: HashMap<_, _> = r.labels().into_iter().collect();
        assert_eq!(labels["faber.replica"], "2");
        assert_eq!(ReplicaRef::from_labels(&labels), Some(r));
    }

    #[test]
    fn foreign_or_damaged_labels_are_not_a_replica() {
        assert_eq!(ReplicaRef::from_labels(&HashMap::new()), None);
        let mut labels: HashMap<_, _> = replica().labels().into_iter().collect();
        labels.insert(LABEL_PROJECT.into(), "../x".into());
        assert_eq!(ReplicaRef::from_labels(&labels), None);
        let mut labels: HashMap<_, _> = replica().labels().into_iter().collect();
        labels.remove(LABEL_MANAGED);
        assert_eq!(ReplicaRef::from_labels(&labels), None);
    }
}
