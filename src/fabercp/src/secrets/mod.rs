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

pub mod delivery;

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::PathBuf;

use faber_api_types::workload::WorkloadSpec;
use faber_crypto::{ClusterKey, CryptoError, DataKey, env_digest, open, seal};
use zeroize::{Zeroize, Zeroizing};

use crate::apps::{SealedEnv, StoredWorkload, WorkloadRecord};
use crate::mesh::files::write_atomic;
pub use delivery::KeyDeliveryError;

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
    #[error(
        "the cluster key file {} must be a regular file owned by the faber user with mode 0600",
        .0.display()
    )]
    Insecure(PathBuf),
    #[error("the cluster key file {} cannot be written", .0.display())]
    Unwritable(PathBuf),
    #[error("the system has no randomness for a cluster key")]
    NoRandomness,
    #[error("this node has no cluster key")]
    NotConfigured,
}

/// Where a node gets the cluster key. Called on every use, so a key that
/// arrives or changes on disk is picked up without a restart.
pub trait ClusterKeyProvider: Send + Sync + std::fmt::Debug {
    fn cluster_key(&self) -> Result<ClusterKey, ClusterKeyError>;

    /// The key, made and stored first if the node has none. Never replaces
    /// a key that exists, even a malformed one.
    fn create_if_missing(&self) -> Result<ClusterKey, ClusterKeyError> {
        self.cluster_key()
    }

    /// Stores the key the founder delivered, replacing the one on disk.
    fn store(&self, _key: &ClusterKey) -> Result<(), ClusterKeyError> {
        Err(ClusterKeyError::NotConfigured)
    }
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

impl FileClusterKey {
    /// Reads the file into a buffer that is wiped on drop. On unix the file
    /// must be a regular file of the process's user with no group or other
    /// permission bits; the checks run on the open file, not on its path.
    fn read(&self) -> Result<Zeroizing<Vec<u8>>, ClusterKeyError> {
        let path = &self.path;
        let missing_or_unreadable = |e: std::io::Error| match e.kind() {
            std::io::ErrorKind::NotFound => ClusterKeyError::Missing(path.clone()),
            _ => ClusterKeyError::Unreadable(path.clone()),
        };
        let mut file = std::fs::File::open(path).map_err(missing_or_unreadable)?;
        let meta = file.metadata().map_err(missing_or_unreadable)?;
        if !meta.is_file() || !permissions_are_private(&meta) {
            return Err(ClusterKeyError::Insecure(path.clone()));
        }
        // A key is 44 characters; do not read an arbitrary file whole.
        let mut bytes = Zeroizing::new(Vec::with_capacity(64));
        file.by_ref()
            .take(4096)
            .read_to_end(&mut bytes)
            .map_err(missing_or_unreadable)?;
        Ok(bytes)
    }
}

/// Whether only the owner, who is the process's user, may touch the file.
#[cfg(unix)]
fn permissions_are_private(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    meta.mode() & 0o077 == 0 && meta.uid() == nix::unistd::geteuid().as_raw()
}

#[cfg(not(unix))]
fn permissions_are_private(_meta: &std::fs::Metadata) -> bool {
    true
}

impl ClusterKeyProvider for FileClusterKey {
    fn cluster_key(&self) -> Result<ClusterKey, ClusterKeyError> {
        let bytes = self.read()?;
        let malformed = || ClusterKeyError::Malformed(self.path.clone());
        let text = std::str::from_utf8(&bytes).map_err(|_| malformed())?;
        ClusterKey::from_base64(text).map_err(|_| malformed())
    }

    fn create_if_missing(&self) -> Result<ClusterKey, ClusterKeyError> {
        match self.cluster_key() {
            Err(ClusterKeyError::Missing(_)) => {
                let key = ClusterKey::generate().map_err(|_| ClusterKeyError::NoRandomness)?;
                self.store(&key)?;
                Ok(key)
            }
            other => other,
        }
    }

    fn store(&self, key: &ClusterKey) -> Result<(), ClusterKeyError> {
        let unwritable = || ClusterKeyError::Unwritable(self.path.clone());
        let dir = match self.path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => std::path::Path::new("."),
        };
        let Some(name) = self.path.file_name().and_then(|n| n.to_str()) else {
            return Err(unwritable());
        };
        let text = Zeroizing::new(format!(
            "{}
",
            *key.to_base64()
        ));
        write_atomic(dir, name, text.as_bytes(), Some(0o600)).map_err(|_| unwritable())
    }
}

/// Removes the cluster key file of a node that no longer belongs to the
/// cluster, so that the key does not linger. A missing file is fine; a
/// failure is logged and not returned, because the caller is already giving
/// up the node's identity.
pub fn remove_cluster_key_file(path: &std::path::Path) {
    match std::fs::remove_file(path) {
        Ok(()) => tracing::info!(
            key_file = %path.display(),
            "removed the cluster key of a node that left the cluster"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::error!(
            key_file = %path.display(),
            error = %e,
            "could not remove the cluster key file; remove it by hand"
        ),
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
        let digest = env_digest(
            key,
            [project, environment, name, &container.name],
            &container.env,
        );
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

/// Opens a sealed value as text. The decrypted bytes are wiped if they are
/// not text; as text they are the caller's to wipe.
fn open_text(
    key: &DataKey,
    sealed: &faber_crypto::Sealed,
    aad: &[&str],
) -> Result<String, CryptoError> {
    let clear = open(key, sealed, aad)?;
    String::from_utf8(clear).map_err(|e| {
        let mut bytes = e.into_bytes();
        bytes.zeroize();
        CryptoError::Malformed
    })
}

/// The env of one container of `record`, opened. The values are plaintext:
/// the caller wipes them (`faber_node_protocol::SecretEnv` does when it is
/// dropped).
pub fn reveal_env(
    key: &DataKey,
    project: &str,
    environment: &str,
    workload: &str,
    record: &WorkloadRecord,
    container: &str,
) -> Result<BTreeMap<String, String>, CryptoError> {
    let mut env = BTreeMap::new();
    let Some(sealed) = record.env.get(container) else {
        return Ok(env);
    };
    for (var, value) in &sealed.values {
        let aad = [project, environment, workload, container, var];
        env.insert(var.clone(), open_text(key, value, &aad)?);
    }
    Ok(env)
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
            container
                .env
                .insert(var.clone(), open_text(key, value, &aad)?);
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
        provider.store(&key).unwrap();
        assert_eq!(provider.cluster_key().unwrap(), key);

        provider.store(&key).unwrap();
        std::fs::write(&path, "not a key").unwrap();
        make_private(&path);
        assert!(matches!(
            provider.cluster_key(),
            Err(ClusterKeyError::Malformed(_))
        ));
        assert!(matches!(
            NoClusterKey.cluster_key(),
            Err(ClusterKeyError::NotConfigured)
        ));
    }

    fn make_private(path: &std::path::Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        #[cfg(not(unix))]
        let _ = path;
    }

    #[test]
    fn a_key_file_can_be_removed_and_a_missing_one_is_fine() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.key");
        FileClusterKey::new(&path).create_if_missing().unwrap();
        remove_cluster_key_file(&path);
        assert!(!path.exists());
        remove_cluster_key_file(&path);
    }

    #[test]
    fn the_key_is_made_once_and_never_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.key");
        let provider = FileClusterKey::new(&path);
        let first = provider.create_if_missing().unwrap();
        assert_eq!(provider.cluster_key().unwrap(), first);
        // A restart finds the key and keeps it.
        assert_eq!(provider.create_if_missing().unwrap(), first);
        assert_eq!(FileClusterKey::new(&path).cluster_key().unwrap(), first);

        // Even a damaged file is left for the operator.
        std::fs::write(&path, "garbage").unwrap();
        make_private(&path);
        assert!(matches!(
            provider.create_if_missing(),
            Err(ClusterKeyError::Malformed(_))
        ));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "garbage");
    }

    #[cfg(unix)]
    #[test]
    fn the_key_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.key");
        let provider = FileClusterKey::new(&path);
        provider.create_if_missing().unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn a_key_file_others_can_read_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.key");
        let provider = FileClusterKey::new(&path);
        provider.create_if_missing().unwrap();
        for mode in [0o644, 0o640, 0o604, 0o660] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(
                matches!(provider.cluster_key(), Err(ClusterKeyError::Insecure(_))),
                "{mode:o}"
            );
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
        assert!(provider.cluster_key().is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn a_key_that_is_not_a_regular_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.key");
        std::fs::create_dir(&path).unwrap();
        assert!(matches!(
            FileClusterKey::new(&path).cluster_key(),
            Err(ClusterKeyError::Insecure(_))
        ));
    }

    #[test]
    fn the_digest_is_bound_to_the_workload() {
        let key = DataKey::generate().unwrap();
        let plain = spec(&[("A", "1")]);
        let digest = |project, env, name| {
            seal_workload(&key, project, env, name, &plain, None)
                .unwrap()
                .env["app"]
                .digest
        };
        let web = digest("shop", "staging", "web");
        assert_eq!(web, digest("shop", "staging", "web"));
        assert_ne!(web, digest("shop", "staging", "api"));
        assert_ne!(web, digest("shop", "production", "web"));
        assert_ne!(web, digest("blog", "staging", "web"));
    }
}
