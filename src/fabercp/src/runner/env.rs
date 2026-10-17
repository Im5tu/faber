//! Where a container's env comes from.
//!
//! The runner keeps no env. When it makes a container it asks an
//! [`EnvSource`], puts the answer in the request and drops it; the values are
//! wiped on drop (`SecretEnv`).

use std::sync::Arc;

use faber_crypto::{DataKey, key_id, unwrap};
use faber_node_protocol::{ReplicaRef, SecretEnv};

use super::desired::parse_revision;
use crate::raft::store::StateReader;
use crate::raft::store::keys::DataKeyRecord;
use crate::secrets::{ClusterKeyProvider, env_scope, reveal_env};

pub trait EnvSource: Send + Sync + 'static {
    /// The env of `container` of `replica`, opened. An error says, for
    /// people, why not: the revision has moved on, or there is no key.
    fn env(&self, replica: &ReplicaRef, container: &str) -> Result<SecretEnv, String>;
}

/// The data key of an environment, opened with this node's cluster key.
pub fn data_key_for(
    cluster_key: &dyn ClusterKeyProvider,
    stored: Option<&DataKeyRecord>,
    scope: &str,
) -> Result<DataKey, String> {
    let cluster = cluster_key
        .cluster_key()
        .map_err(|e| format!("no cluster key to open the env: {e}"))?;
    let stored = stored.ok_or_else(|| format!("{scope} has no data key"))?;
    if stored.key_id != key_id(&cluster) {
        return Err("this node's cluster key is not the one that sealed the env".into());
    }
    unwrap(&cluster, &stored.wrapped, scope).map_err(|_| "the data key cannot be opened".into())
}

/// Opens the env from the replicated state, as of now.
pub struct StateEnvSource {
    reader: StateReader,
    cluster_key: Arc<dyn ClusterKeyProvider>,
}

impl StateEnvSource {
    pub fn new(reader: StateReader, cluster_key: Arc<dyn ClusterKeyProvider>) -> Self {
        Self {
            reader,
            cluster_key,
        }
    }
}

impl EnvSource for StateEnvSource {
    fn env(&self, replica: &ReplicaRef, container: &str) -> Result<SecretEnv, String> {
        let record = self
            .reader
            .environment(&replica.project, &replica.env)
            .map_err(|e| e.to_string())?
            .and_then(|mut e| e.workloads.remove(&replica.workload))
            .ok_or_else(|| "the workload is gone".to_owned())?;
        if parse_revision(&replica.revision) != Some(record.revision) {
            return Err("the workload has a newer revision".into());
        }
        if record
            .env
            .get(container)
            .is_none_or(|e| e.values.is_empty())
        {
            return Ok(SecretEnv::default());
        }
        let scope = env_scope(&replica.project, &replica.env);
        let stored = self.reader.data_key(&scope).map_err(|e| e.to_string())?;
        let key = data_key_for(self.cluster_key.as_ref(), stored.as_ref(), &scope)?;
        reveal_env(
            &key,
            &replica.project,
            &replica.env,
            &replica.workload,
            &record,
            container,
        )
        .map(SecretEnv)
        .map_err(|_| "the env cannot be opened".to_owned())
    }
}

/// An env source that answers the same env for every container, or an
/// error. For tests.
#[cfg(test)]
pub struct StaticEnv(pub Result<std::collections::BTreeMap<String, String>, String>);

#[cfg(test)]
impl StaticEnv {
    pub fn secret() -> Arc<dyn EnvSource> {
        Arc::new(Self(Ok([(
            "PASSWORD".to_owned(),
            crate::runner::testing::SECRET.to_owned(),
        )]
        .into())))
    }
}

#[cfg(test)]
impl EnvSource for StaticEnv {
    fn env(&self, _replica: &ReplicaRef, _container: &str) -> Result<SecretEnv, String> {
        self.0.clone().map(SecretEnv)
    }
}
