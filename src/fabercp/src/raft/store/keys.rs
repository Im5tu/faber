//! Data key commands: the wrapped data keys and the id of the cluster key in
//! the replicated state.
//!
//! Two kinds of key in `sm_data`, both reserved so that `Request::Set`
//! cannot write them:
//!
//! - [`KEY_CLUSTER_ID`] holds the [`ClusterKeyId`]: the id of the cluster
//!   key, recorded once. A node whose key file has another id knows it holds
//!   the wrong file.
//! - [`data_key_key`] holds one [`DataKeyRecord`] per scope: a data key
//!   wrapped under the cluster key.
//!
//! The functions here are pure, like `mesh::state`: `apply` reads the current
//! record, calls one of them and writes what it returns.

use faber_api_types::error_code;
use faber_api_types::name::validate_dns_label;
use faber_crypto::WrappedKey;
use serde::{Deserialize, Serialize};

use crate::mesh::Rejection;

/// `sm_data` key holding the JSON [`ClusterKeyId`].
pub const KEY_CLUSTER_ID: &str = "keys/cluster-id";
/// Prefix of the `sm_data` keys holding one JSON [`DataKeyRecord`] each.
pub const DATA_KEY_PREFIX: &str = "keys/data/";

/// The `sm_data` key of the data key of `scope`.
pub fn data_key_key(scope: &str) -> String {
    format!("{DATA_KEY_PREFIX}{scope}")
}

/// Whether only the typed commands of this module may write `key`.
/// `Request::Set` rejects these.
pub fn is_reserved_key(key: &str) -> bool {
    key == KEY_CLUSTER_ID || key.starts_with(DATA_KEY_PREFIX)
}

/// Stores a data key wrapped under the cluster key.
///
/// The first command for a scope wins; a later one for the same scope
/// changes nothing. The proposer reads the stored key back and uses that.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateDataKey {
    /// `<project>/<environment>`. It was also the associated
    /// data when the key was wrapped.
    pub scope: String,
    pub wrapped: WrappedKey,
    /// `faber_crypto::key_id` of the cluster key that wrapped it.
    pub key_id: String,
}

impl std::fmt::Debug for CreateDataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateDataKey")
            .field("scope", &self.scope)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// Names the cluster key that wraps data keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetClusterKeyId {
    pub key_id: String,
}

/// A data key as stored under [`data_key_key`].
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct DataKeyRecord {
    pub wrapped: WrappedKey,
    /// The id of the cluster key that wrapped it.
    pub key_id: String,
}

impl std::fmt::Debug for DataKeyRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataKeyRecord")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// The record under [`KEY_CLUSTER_ID`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterKeyId {
    pub key_id: String,
}

fn bad_request(message: impl Into<String>) -> Rejection {
    Rejection::new(error_code::BAD_REQUEST, message)
}

/// Whether `scope` is `<project>/<environment>` with both names DNS labels.
fn check_scope(scope: &str) -> Result<(), Rejection> {
    let ok = scope.split_once('/').is_some_and(|(project, environment)| {
        validate_dns_label(project).is_ok() && validate_dns_label(environment).is_ok()
    });
    if ok {
        Ok(())
    } else {
        Err(bad_request("a key scope is `<project>/<environment>`"))
    }
}

fn check_key_id(key_id: &str) -> Result<(), Rejection> {
    if key_id.is_empty() || key_id.len() > 64 || !key_id.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return Err(bad_request("a key id is 1 to 64 letters and digits"));
    }
    Ok(())
}

/// `CreateDataKey`: the record to write under [`data_key_key`], or `None`
/// when `existing` already holds a key for the scope. Rejected with
/// `bad_request` for a bad scope or key id.
pub fn create_data_key(
    existing: Option<&DataKeyRecord>,
    cmd: CreateDataKey,
) -> Result<Option<DataKeyRecord>, Rejection> {
    check_scope(&cmd.scope)?;
    check_key_id(&cmd.key_id)?;
    if existing.is_some() {
        return Ok(None);
    }
    Ok(Some(DataKeyRecord {
        wrapped: cmd.wrapped,
        key_id: cmd.key_id,
    }))
}

/// `SetClusterKeyId`: the record to write under [`KEY_CLUSTER_ID`], or
/// `None` when `existing` already holds this id. Rejected with `bad_request`
/// for a bad id and when `existing` holds another.
pub fn set_cluster_key_id(
    existing: Option<&ClusterKeyId>,
    cmd: SetClusterKeyId,
) -> Result<Option<ClusterKeyId>, Rejection> {
    check_key_id(&cmd.key_id)?;
    match existing {
        None => Ok(Some(ClusterKeyId { key_id: cmd.key_id })),
        Some(stored) if stored.key_id == cmd.key_id => Ok(None),
        Some(_) => Err(bad_request(
            "the cluster key id is already set to another key",
        )),
    }
}

#[cfg(test)]
mod tests {
    use faber_crypto::{ClusterKey, DataKey, key_id, wrap};

    use super::*;

    fn command(scope: &str) -> (CreateDataKey, DataKey) {
        let cluster = ClusterKey::generate().unwrap();
        let data = DataKey::generate().unwrap();
        let cmd = CreateDataKey {
            scope: scope.to_string(),
            wrapped: wrap(&cluster, &data, scope).unwrap(),
            key_id: key_id(&cluster),
        };
        (cmd, data)
    }

    #[test]
    fn reserved_keys() {
        for key in [
            KEY_CLUSTER_ID,
            "keys/data/cluster",
            "keys/data/shop/staging",
        ] {
            assert!(is_reserved_key(key), "{key}");
        }
        for key in [
            "keys",
            "keys/other",
            "mesh",
            "env/shop/staging",
            "a/keys/data/x",
        ] {
            assert!(!is_reserved_key(key), "{key}");
        }
        assert_eq!(data_key_key("shop/staging"), "keys/data/shop/staging");
    }

    #[test]
    fn the_first_data_key_for_a_scope_wins() {
        let (first, _) = command("shop/staging");
        let (second, _) = command("shop/staging");
        let written = create_data_key(None, first.clone()).unwrap().unwrap();
        assert_eq!(written.wrapped, first.wrapped);
        assert_eq!(written.key_id, first.key_id);

        // A later command for the scope writes nothing.
        assert_eq!(create_data_key(Some(&written), second).unwrap(), None);
        assert_eq!(create_data_key(Some(&written), first).unwrap(), None);
    }

    #[test]
    fn data_key_scopes_are_checked() {
        for scope in ["shop/staging", "a-b/c-d"] {
            assert!(create_data_key(None, command(scope).0).unwrap().is_some());
        }
        for scope in ["", "cluster", "shop", "shop/", "/staging", "Shop/staging", "a/b/c"] {
            let rejected = create_data_key(None, command(scope).0).unwrap_err();
            assert_eq!(rejected.code, error_code::BAD_REQUEST, "{scope:?}");
        }
        let mut cmd = command("shop/staging").0;
        cmd.key_id = String::new();
        assert!(create_data_key(None, cmd).is_err());
    }

    #[test]
    fn the_cluster_key_id_is_set_once() {
        let set = |existing: Option<&ClusterKeyId>, id: &str| {
            set_cluster_key_id(existing, SetClusterKeyId { key_id: id.into() })
        };
        let stored = set(None, "abc123").unwrap().unwrap();
        assert_eq!(stored.key_id, "abc123");
        assert_eq!(set(Some(&stored), "abc123").unwrap(), None);
        let other = set(Some(&stored), "def456").unwrap_err();
        assert_eq!(other.code, error_code::BAD_REQUEST);
        assert_eq!(set(None, "").unwrap_err().code, error_code::BAD_REQUEST);
    }

    #[test]
    fn debug_prints_no_key_material() {
        let (cmd, _) = command("shop/staging");
        let shown = format!("{cmd:?}");
        assert!(shown.contains("shop/staging"));
        assert!(!shown.contains("wrapped"));
    }
}
