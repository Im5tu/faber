//! Secrets: envelope-encrypted values and their keys.
//!
//! Every container env value is sealed under the data key of its
//! environment before it is proposed to raft, so neither the log nor a
//! snapshot holds one in clear. A data key is wrapped under the cluster key,
//! which lives in a file on each node and never enters raft. See
//! `faber_crypto` for the scheme and `raft::store::keys` for the stored
//! keys.
//!
//! The cluster key comes from a [`ClusterKeyProvider`]. Production reads the
//! key file on demand ([`FileClusterKey`]); tests inject a key
//! ([`StaticClusterKey`]).

use std::collections::BTreeMap;
use std::path::PathBuf;

use faber_api_types::workload::WorkloadSpec;
use faber_crypto::{ClusterKey, CryptoError, DataKey, env_digest, open, seal};

use crate::apps::{SealedEnv, StoredWorkload, WorkloadRecord};

/// What a masked read shows in place of an env value.
pub const MASK: &str = "***";

/// Why the cluster key cannot be had. No variant holds key material.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterKeyError {
    #[error("no cluster key at {}", .0.display())]
    Missing(PathBuf),
    #[error("the cluster key file {} cannot be read", .0.display())]
    Unreadable(PathBuf),
    #[error("the cluster key file {} is not 32 bytes of base64", .0.display())]
    Malformed(PathBuf),
    #[error("this node has no cluster key")]
    NotConfigured,
}

/// Where a node gets the cluster key. Called on every use, so a key that
/// arrives or changes on disk is picked up without a restart.
pub trait ClusterKeyProvider: Send + Sync + std::fmt::Debug {
    fn cluster_key(&self) -> Result<ClusterKey, ClusterKeyError>;
}

/// Reads the key from a file of base64, e.g. `/var/lib/faber/cluster.key`.
#[derive(Debug, Clone)]
pub struct FileClusterKey {
    path: PathBuf,
}

impl FileClusterKey {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl ClusterKeyProvider for FileClusterKey {
    fn cluster_key(&self) -> Result<ClusterKey, ClusterKeyError> {
        let text = std::fs::read_to_string(&self.path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => ClusterKeyError::Missing(self.path.clone()),
            _ => ClusterKeyError::Unreadable(self.path.clone()),
        })?;
        ClusterKey::from_base64(&text).map_err(|_| ClusterKeyError::Malformed(self.path.clone()))
    }
}

/// A key held in memory, for tests.
#[derive(Debug, Clone)]
pub struct StaticClusterKey(pub ClusterKey);

impl ClusterKeyProvider for StaticClusterKey {
    fn cluster_key(&self) -> Result<ClusterKey, ClusterKeyError> {
        Ok(self.0.clone())
    }
}

/// A node with no key: every use fails. The default of a service until a
/// provider is set.
#[derive(Debug, Clone, Copy)]
pub struct NoClusterKey;

impl ClusterKeyProvider for NoClusterKey {
    fn cluster_key(&self) -> Result<ClusterKey, ClusterKeyError> {
        Err(ClusterKeyError::NotConfigured)
    }
}

/// The scope of the data key of an environment.
pub fn env_scope(project: &str, environment: &str) -> String {
    format!("{project}/{environment}")
}

/// Seals the env of every container of `spec`, the workload `name` of
/// `project`/`environment`, and clears it from the spec.
///
/// A container whose env digest equals the one in `previous` keeps its
/// stored ciphertexts, so a redeploy of the same env writes the same bytes.
pub fn seal_workload(
    key: &DataKey,
    project: &str,
    environment: &str,
    name: &str,
    spec: &WorkloadSpec,
    previous: Option<&WorkloadRecord>,
) -> Result<StoredWorkload, CryptoError> {
    let mut env = BTreeMap::new();
    let mut cleared = spec.clone();
    for container in &mut cleared.containers {
        let digest = env_digest(key, &container.env);
        let kept = previous
            .and_then(|p| p.env.get(&container.name))
            .filter(|stored| stored.digest == digest);
        let sealed = match kept {
            Some(stored) => stored.clone(),
            None => {
                let mut values = BTreeMap::new();
                for (var, value) in &container.env {
                    let aad = [project, environment, name, &container.name, var];
                    values.insert(var.clone(), seal(key, value.as_bytes(), &aad)?);
                }
                SealedEnv { digest, values }
            }
        };
        env.insert(container.name.clone(), sealed);
        container.env.clear();
    }
    Ok(StoredWorkload { spec: cleared, env })
}

/// The spec of `record` with every env value opened.
pub fn reveal_workload(
    key: &DataKey,
    project: &str,
    environment: &str,
    name: &str,
    record: &WorkloadRecord,
) -> Result<WorkloadSpec, CryptoError> {
    let mut spec = record.spec.clone();
    for container in &mut spec.containers {
        let Some(sealed) = record.env.get(&container.name) else {
            continue;
        };
        for (var, value) in &sealed.values {
            let aad = [project, environment, name, &container.name, var];
            let clear = open(key, value, &aad)?;
            let clear = String::from_utf8(clear).map_err(|_| CryptoError::Malformed)?;
            container.env.insert(var.clone(), clear);
        }
    }
    Ok(spec)
}

/// The spec of `record` with the name of every env variable and [`MASK`] as
/// its value. Needs no key.
pub fn masked_workload(record: &WorkloadRecord) -> WorkloadSpec {
    let mut spec = record.spec.clone();
    for container in &mut spec.containers {
        if let Some(sealed) = record.env.get(&container.name) {
            container.env = sealed
                .values
                .keys()
                .map(|var| (var.clone(), MASK.to_string()))
                .collect();
        }
    }
    spec
}

#[cfg(test)]
mod tests {
    use faber_api_types::workload::ContainerSpec;

    use super::*;

    fn spec(env: &[(&str, &str)]) -> WorkloadSpec {
        let mut container = ContainerSpec::new("app", "nginx:1");
        container.env = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut spec: WorkloadSpec = serde_json::from_value(serde_json::json!({
            "containers": [{"name": "app", "image": "nginx:1"}]
        }))
        .unwrap();
        spec.containers = vec![container];
        spec
    }

    fn record(stored: StoredWorkload) -> WorkloadRecord {
        WorkloadRecord {
            revision: 1,
            spec: stored.spec,
            env: stored.env,
        }
    }

    #[test]
    fn sealed_env_opens_again_and_leaves_the_spec_clear() {
        let key = DataKey::generate().unwrap();
        let plain = spec(&[("TOKEN", "hunter2"), ("MODE", "prod")]);
        let stored = seal_workload(&key, "shop", "staging", "web", &plain, None).unwrap();
        assert!(stored.spec.containers[0].env.is_empty());
        assert_eq!(stored.env["app"].values.len(), 2);

        let record = record(stored);
        let opened = reveal_workload(&key, "shop", "staging", "web", &record).unwrap();
        assert_eq!(opened, plain);
        let serialized = serde_json::to_string(&record).unwrap();
        assert!(!serialized.contains("hunter2"));
    }

    #[test]
    fn values_do_not_open_in_another_place() {
        let key = DataKey::generate().unwrap();
        let stored =
            seal_workload(&key, "shop", "staging", "web", &spec(&[("A", "1")]), None).unwrap();
        let record = record(stored);
        for (p, e, w) in [
            ("blog", "staging", "web"),
            ("shop", "production", "web"),
            ("shop", "staging", "api"),
        ] {
            assert_eq!(
                reveal_workload(&key, p, e, w, &record).unwrap_err(),
                CryptoError::Open
            );
        }
        let other = DataKey::generate().unwrap();
        assert!(reveal_workload(&other, "shop", "staging", "web", &record).is_err());
    }

    #[test]
    fn masking_keeps_the_names() {
        let key = DataKey::generate().unwrap();
        let plain = spec(&[("TOKEN", "hunter2"), ("MODE", "prod")]);
        let record = record(seal_workload(&key, "shop", "staging", "web", &plain, None).unwrap());
        let masked = masked_workload(&record);
        let env = &masked.containers[0].env;
        assert_eq!(env.len(), 2);
        assert!(env.values().all(|v| v == MASK));
        assert!(env.contains_key("TOKEN") && env.contains_key("MODE"));
    }

    #[test]
    fn an_unchanged_container_keeps_its_ciphertext() {
        let key = DataKey::generate().unwrap();
        let plain = spec(&[("A", "1")]);
        let first = record(seal_workload(&key, "shop", "staging", "web", &plain, None).unwrap());
        let again = seal_workload(&key, "shop", "staging", "web", &plain, Some(&first)).unwrap();
        assert_eq!(again.env, first.env);

        let changed = spec(&[("A", "2")]);
        let other = seal_workload(&key, "shop", "staging", "web", &changed, Some(&first)).unwrap();
        assert_ne!(other.env["app"].digest, first.env["app"].digest);
    }

    #[test]
    fn the_file_provider_reads_on_demand() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.key");
        let provider = FileClusterKey::new(&path);
        assert!(matches!(
            provider.cluster_key(),
            Err(ClusterKeyError::Missing(_))
        ));

        let key = ClusterKey::generate().unwrap();
        std::fs::write(&path, format!("{}\n", key.to_base64())).unwrap();
        assert_eq!(provider.cluster_key().unwrap(), key);

        std::fs::write(&path, "not a key").unwrap();
        assert!(matches!(
            provider.cluster_key(),
            Err(ClusterKeyError::Malformed(_))
        ));
        assert!(matches!(
            NoClusterKey.cluster_key(),
            Err(ClusterKeyError::NotConfigured)
        ));
    }
}
