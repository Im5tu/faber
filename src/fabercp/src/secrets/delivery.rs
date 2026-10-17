//! The cluster key's life across nodes: made once by the founder at
//! `cluster init`, handed to a joining node over the mesh, and checked
//! against the id that raft records.
//!
//! The key never goes into the join token, which can be copied. It is in the
//! answer to `POST /raft/cluster/register`, and only when the request comes
//! from the mesh IP that the invite is bound to.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use faber_crypto::{ClusterKey, key_id};

use crate::apps::service::key_unavailable;
use crate::dto::{PeerRegisterRequest, PeerRegisterResponse, WireKey};
use crate::raft::store::keys::SetClusterKeyId;
use crate::raft::types::{NodeId, Request};
use crate::secrets::ClusterKeyError;
use crate::service::{FaberService, ServiceError, ServiceResult};

/// How long a node that was forwarded a registration waits to see it in its
/// own copy of the mesh.
const REGISTRATION_WAIT: Duration = Duration::from_secs(5);
const REGISTRATION_POLL: Duration = Duration::from_millis(50);
/// How often a leader without a recorded key id tries again.
const KEY_ID_RETRY: Duration = Duration::from_secs(5);

/// What a joiner does with the answer to its registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStep {
    /// Store the key if there is one and record the registration.
    Proceed,
    /// The cluster sent no key although this node needs one: register again.
    Retry,
    /// Too many answers without a key.
    GiveUp,
}

/// Registrations a joiner repeats while the answer has no key.
pub const KEYLESS_ATTEMPTS: u32 = 10;

/// Decides what to do with a registration answer. `required` is whether
/// this node's cluster checks sources, so that the answer should carry the
/// key; `keyless` counts the answers without one so far.
pub fn key_step(required: bool, answer: &PeerRegisterResponse, keyless: &mut u32) -> KeyStep {
    if !required || answer.cluster_key.is_some() {
        return KeyStep::Proceed;
    }
    *keyless += 1;
    if *keyless >= KEYLESS_ATTEMPTS {
        KeyStep::GiveUp
    } else {
        KeyStep::Retry
    }
}

/// Why a delivered cluster key was not stored.
#[derive(Debug, thiserror::Error)]
pub enum KeyDeliveryError {
    /// The key is not the one the cluster recorded. Retrying cannot help.
    #[error(
        "the cluster key sent by the cluster has id {got}, but the cluster records id {expected}"
    )]
    Mismatch { expected: String, got: String },
    #[error("the cluster key sent by the cluster is not 32 bytes of base64")]
    Malformed,
    #[error(transparent)]
    Store(#[from] ClusterKeyError),
}

impl FaberService {
    /// Founder: makes the cluster key file if it is missing, never replacing
    /// one. `None` when no key file is configured.
    pub(crate) async fn create_cluster_key_file(&self) -> ServiceResult<Option<ClusterKey>> {
        let provider = self.cluster_keys().clone();
        let made = tokio::task::spawn_blocking(move || provider.create_if_missing())
            .await
            .map_err(|e| ServiceError::Internal(format!("making the cluster key: {e}")))?;
        match made {
            Ok(key) => Ok(Some(key)),
            Err(ClusterKeyError::NotConfigured) => {
                tracing::warn!("this node has no cluster key file configured; none was made");
                Ok(None)
            }
            Err(e) => Err(key_unavailable(e.to_string())),
        }
    }

    /// Founder: makes the cluster key file if it is missing and has raft
    /// record the key's id. Recording it again is harmless.
    pub(crate) async fn create_and_record_cluster_key(&self) -> ServiceResult<()> {
        let Some(key) = self.create_cluster_key_file().await? else {
            return Ok(());
        };
        let record = SetClusterKeyId {
            key_id: key_id(&key),
        };
        self.propose(Request::SetClusterKeyId(record))
            .await
            .map(drop)
    }

    /// Handles `POST /raft/cluster/register`: registers the invite, then
    /// answers with the cluster key if the request comes from the mesh IP of
    /// the node it registered.
    ///
    /// Only the node that the joiner talked to decides, from the source it
    /// sees: a leader that was forwarded the request sees the follower's
    /// address and releases nothing. Where the source is not checked (a
    /// loopback listener) nothing is released either.
    pub async fn peer_register_for(
        &self,
        req: PeerRegisterRequest,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<PeerRegisterResponse> {
        let (invite_id, node_id, instance_id) =
            (req.invite_id.clone(), req.node_id, req.instance_id.clone());
        self.peer_register(req, forwarded).await?;
        if forwarded || !self.checks_peer_source() {
            return Ok(PeerRegisterResponse::default());
        }
        let mesh_ip = self
            .registered_mesh_ip(node_id, &invite_id, &instance_id)
            .await?;
        if std::net::IpAddr::V4(mesh_ip) != source.ip().to_canonical() {
            tracing::warn!(
                node_id,
                source = %source.ip(),
                "did not release the cluster key: the request is not from the mesh IP of its invite"
            );
            return Ok(PeerRegisterResponse::default());
        }
        let resp = self.local_key_for_delivery().await?;
        if resp.cluster_key.is_some() {
            tracing::info!(
                node_id,
                "released the cluster key to the node that registered"
            );
        }
        Ok(resp)
    }

    /// The mesh IP of node `node_id` once this node's copy of the mesh shows
    /// it registered by `instance_id` with `invite_id`. A follower may need a
    /// moment to apply what the leader committed; `NotReady` after that, and
    /// the joiner registers again.
    async fn registered_mesh_ip(
        &self,
        node_id: NodeId,
        invite_id: &str,
        instance_id: &str,
    ) -> ServiceResult<std::net::Ipv4Addr> {
        let deadline = Instant::now() + REGISTRATION_WAIT;
        loop {
            let mesh = self.read_mesh()?;
            let node = mesh.as_ref().and_then(|m| m.nodes.get(&node_id));
            if let Some(node) = node
                && node.invite_id.as_deref() == Some(invite_id)
                && node.instance_id.as_deref() == Some(instance_id)
            {
                return Ok(node.mesh_ip);
            }
            if Instant::now() >= deadline {
                return Err(ServiceError::NotReady(format!(
                    "node {node_id} is not registered in this node's copy of the mesh yet"
                )));
            }
            tokio::time::sleep(REGISTRATION_POLL).await;
        }
    }

    /// This node's key and the id raft records, for a joiner. Empty when
    /// this node has no key file; `key_unavailable` when its key is not the
    /// one raft records.
    async fn local_key_for_delivery(&self) -> ServiceResult<PeerRegisterResponse> {
        let provider = self.cluster_keys().clone();
        let read = tokio::task::spawn_blocking(move || provider.cluster_key())
            .await
            .map_err(|e| ServiceError::Internal(format!("reading the cluster key: {e}")))?;
        let key = match read {
            Ok(key) => key,
            Err(ClusterKeyError::Missing(_) | ClusterKeyError::NotConfigured) => {
                tracing::warn!("this node has no cluster key to give the joining node");
                return Ok(PeerRegisterResponse::default());
            }
            Err(e) => return Err(key_unavailable(e.to_string())),
        };
        let recorded = self.recorded_key_id()?;
        if recorded.as_ref().is_some_and(|id| *id != key_id(&key)) {
            return Err(key_unavailable(
                "this node's cluster key is not the one the cluster records",
            ));
        }
        Ok(PeerRegisterResponse {
            cluster_key: Some(WireKey::new(key.to_base64())),
            key_id: recorded,
        })
    }

    /// The id of the cluster key in this node's copy of the raft state.
    pub(crate) fn recorded_key_id(&self) -> ServiceResult<Option<String>> {
        self.state()
            .cluster_key_id()
            .map(|r| r.map(|r| r.key_id))
            .map_err(|e| ServiceError::Internal(e.to_string()))
    }

    /// Joiner: stores the key that the cluster sent in `resp` once its id
    /// matches the one the cluster records, both as `resp` names it and in
    /// this node's own raft state. A response without a key is accepted: the
    /// cluster had none to give.
    pub async fn accept_cluster_key(
        &self,
        resp: &PeerRegisterResponse,
    ) -> Result<(), KeyDeliveryError> {
        let Some(wire) = &resp.cluster_key else {
            tracing::warn!(
                "the cluster sent no cluster key; deploys and reveals on this node fail until \
                 the key file is put in place"
            );
            return Ok(());
        };
        let key =
            ClusterKey::from_base64(wire.as_str()).map_err(|_| KeyDeliveryError::Malformed)?;
        let got = key_id(&key);
        let local = self
            .recorded_key_id()
            .map_err(|_| KeyDeliveryError::Malformed)?;
        for expected in resp.key_id.iter().chain(local.iter()) {
            if *expected != got {
                return Err(KeyDeliveryError::Mismatch {
                    expected: expected.clone(),
                    got: got.clone(),
                });
            }
        }
        let provider = self.cluster_keys().clone();
        tokio::task::spawn_blocking(move || provider.store(&key))
            .await
            .map_err(|_| KeyDeliveryError::Store(ClusterKeyError::NotConfigured))??;
        tracing::info!(key_id = %got, "stored the cluster key sent by the cluster");
        Ok(())
    }

    /// Records the id of this node's cluster key in raft when raft has none.
    /// `Ok(true)` when raft has an id afterwards, `Ok(false)` when it has
    /// none and this node has no usable key file to take one from. For the
    /// leader only.
    pub(crate) async fn record_missing_key_id(&self) -> ServiceResult<bool> {
        if self.recorded_key_id()?.is_some() {
            return Ok(true);
        }
        let provider = self.cluster_keys().clone();
        let read = tokio::task::spawn_blocking(move || provider.cluster_key())
            .await
            .map_err(|e| ServiceError::Internal(format!("reading the cluster key: {e}")))?;
        let Ok(key) = read else {
            return Ok(false);
        };
        let record = SetClusterKeyId {
            key_id: key_id(&key),
        };
        self.propose(Request::SetClusterKeyId(record))
            .await
            .map(|_| true)
    }

    /// Starts a task that, while this node leads an initialised cluster
    /// whose raft holds no cluster key id, records the id of the local key
    /// file. A crash between `cluster init` and the write of the id heals
    /// itself this way, at start-up and after every election.
    pub(crate) fn spawn_key_id_healer(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let svc = self.clone();
        tokio::spawn(async move {
            let mut metrics = svc.raft().metrics();
            let mut done = false;
            loop {
                let leads = {
                    let m = metrics.borrow_and_update();
                    m.current_leader == Some(svc.node_id())
                        && m.membership_config.membership().nodes().next().is_some()
                };
                if !leads {
                    done = false;
                } else if !done {
                    match svc.record_missing_key_id().await {
                        Ok(recorded) => done = recorded,
                        Err(e) => tracing::warn!(
                            error = %e,
                            "could not record the cluster key id; will retry"
                        ),
                    }
                }
                // Until the id is recorded a timer retries, so a key file
                // that appears later or a failed proposal needs no further
                // change of the raft metrics.
                let retrying = leads && !done;
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    changed = metrics.changed() => if changed.is_err() { return },
                    _ = tokio::time::sleep(KEY_ID_RETRY), if retrying => {}
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_key() -> PeerRegisterResponse {
        PeerRegisterResponse {
            cluster_key: Some(WireKey::new(
                faber_crypto::ClusterKey::from_bytes([3; faber_crypto::KEY_LEN]).to_base64(),
            )),
            key_id: None,
        }
    }

    #[test]
    fn a_keyless_answer_is_retried_a_bounded_number_of_times() {
        let none = PeerRegisterResponse::default();
        let mut keyless = 0;
        for _ in 1..KEYLESS_ATTEMPTS {
            assert_eq!(key_step(true, &none, &mut keyless), KeyStep::Retry);
        }
        assert_eq!(key_step(true, &none, &mut keyless), KeyStep::GiveUp);
    }

    #[test]
    fn the_registration_is_recorded_only_once_a_key_came() {
        let none = PeerRegisterResponse::default();
        let mut keyless = 0;
        assert_eq!(key_step(true, &none, &mut keyless), KeyStep::Retry);
        assert_eq!(key_step(true, &none, &mut keyless), KeyStep::Retry);
        assert_eq!(key_step(true, &with_key(), &mut keyless), KeyStep::Proceed);
    }

    #[test]
    fn a_node_that_cannot_be_told_apart_needs_no_key() {
        let mut keyless = 0;
        assert_eq!(
            key_step(false, &PeerRegisterResponse::default(), &mut keyless),
            KeyStep::Proceed
        );
        assert_eq!(keyless, 0);
    }
}
