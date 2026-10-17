//! Raft transport: JSON over HTTP POST to `http://{addr}/raft/{append,vote,snapshot}`.
//!
//! TODO(security): Raft RPCs are plain HTTP with no authentication. Safety
//! relies on the peer listener being bound to loopback or a WireGuard
//! interface (which encrypts and authenticates peers at the network layer).
//! Consider mutual TLS if the peer network is ever widened.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::raft::types::{Node, NodeId, RaftError, TypeConfig};

/// Shortest time between two warnings about one target that refuses this
/// node's raft RPCs.
const REFUSAL_WARN_EVERY: Duration = Duration::from_secs(60);

/// Says when to warn that a target refuses this node's raft RPCs: on the
/// first refusal, and then at most once per [`REFUSAL_WARN_EVERY`] for each
/// target. Clones share one record.
#[derive(Debug, Clone, Default)]
struct RefusalWarnings(Arc<Mutex<BTreeMap<NodeId, Instant>>>);

impl RefusalWarnings {
    /// Whether a refusal by `target` at `now` is worth a warning. Counts
    /// this call as the warning.
    fn due(&self, target: NodeId, now: Instant) -> bool {
        let mut warned = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let due = warned
            .get(&target)
            .is_none_or(|last| now.saturating_duration_since(*last) >= REFUSAL_WARN_EVERY);
        if due {
            warned.insert(target, now);
        }
        due
    }
}

/// Creates one [`HttpConnection`] per target node, sharing a connection pool.
#[derive(Debug, Clone)]
pub struct HttpNetwork {
    client: reqwest::Client,
    refusals: RefusalWarnings,
}

impl HttpNetwork {
    pub fn new() -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            // Node-to-node traffic must not go through an environment proxy.
            .no_proxy()
            .build()?;
        Ok(Self {
            client,
            refusals: RefusalWarnings::default(),
        })
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetwork {
    type Network = HttpConnection;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        HttpConnection {
            client: self.client.clone(),
            refusals: self.refusals.clone(),
            target,
            base: format!("http://{}/raft", node.addr),
        }
    }
}

pub struct HttpConnection {
    client: reqwest::Client,
    refusals: RefusalWarnings,
    target: NodeId,
    base: String,
}

type RpcResult<T, E> = Result<T, RPCError<NodeId, Node, RaftError<E>>>;

impl HttpConnection {
    async fn post<Req, Resp, E>(
        &self,
        path: &str,
        req: &Req,
        option: &RPCOption,
    ) -> RpcResult<Resp, E>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let url = format!("{}/{}", self.base, path);
        let resp = self
            .client
            .post(&url)
            .timeout(option.hard_ttl())
            .json(req)
            .send()
            .await
            .map_err(|e| {
                // Windows retries a refused connect for ~2s, so a dead peer
                // often surfaces as a timeout. Both mean "back off".
                if e.is_connect() || e.is_timeout() {
                    RPCError::Unreachable(Unreachable::new(&e))
                } else {
                    RPCError::Network(NetworkError::new(&e))
                }
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let err = std::io::Error::other(format!("{url} returned {status}: {body}"));
            // 503 is a node that takes no raft RPCs for now (it has not
            // registered with the mesh, see `rpc::RaftGate`). 403 is a node
            // that does not know this one as a registered node yet (see
            // `rpc::SourceCheck`). Like a peer that cannot be reached, both
            // are retried with a backoff.
            let later = [
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                reqwest::StatusCode::FORBIDDEN,
            ];
            // The target logs its refusal at debug, and here it is only a
            // peer that cannot be reached. Without this a node that every
            // RPC bounces off shows as nothing but lag.
            if status == reqwest::StatusCode::FORBIDDEN
                && self.refusals.due(self.target, Instant::now())
            {
                tracing::warn!(
                    target_node = self.target,
                    %url,
                    "node {} refuses this node's raft RPCs: it does not know this node as a \
                     registered mesh node. It falls behind until it learns of the registration \
                     from another leader. Seen again at most once a minute.",
                    self.target
                );
            }
            return Err(if later.contains(&status) {
                RPCError::Unreachable(Unreachable::new(&err))
            } else {
                RPCError::Network(NetworkError::new(&err))
            });
        }

        let result: Result<Resp, RaftError<E>> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<TypeConfig> for HttpConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<NodeId>, openraft::error::Infallible> {
        self.post("append", &req, &option).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<NodeId>, InstallSnapshotError> {
        self.post("snapshot", &req, &option).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<NodeId>, openraft::error::Infallible> {
        self.post("vote", &req, &option).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::{Config, Vote};

    use super::*;
    use crate::raft::rpc;
    use crate::raft::store;
    use crate::raft::types::Raft;

    async fn serve(router: axum::Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await });
        addr
    }

    fn option() -> RPCOption {
        RPCOption::new(Duration::from_secs(2))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vote_round_trips_to_a_remote_raft_node() {
        let dir = tempfile::tempdir().unwrap();
        let s = store::open(dir.path()).unwrap();
        let config = Arc::new(Config::default().validate().unwrap());
        let raft = Raft::new(
            2,
            config,
            HttpNetwork::new().unwrap(),
            s.log_store,
            s.state_machine,
        )
        .await
        .unwrap();
        let router = rpc::router(
            raft.clone(),
            rpc::RaftGate::new(true),
            rpc::SourceCheck::any(),
        );
        let addr = serve(router).await;

        let mut net = HttpNetwork::new().unwrap();
        let mut conn = net.new_client(2, &Node::new(addr.to_string())).await;
        let resp = conn
            .vote(VoteRequest::new(Vote::new(5, 1), None), option())
            .await
            .unwrap();
        assert!(resp.vote_granted);
        assert_eq!(resp.vote, Vote::new(5, 1));

        raft.shutdown().await.unwrap();
    }

    /// A node behind a closed gate takes no raft RPC: the caller sees a peer
    /// it cannot reach, which is not fatal to it, and the node's raft state
    /// does not change. Once the gate opens the same RPC goes through.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_gate_refuses_every_raft_rpc() {
        use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
        use openraft::{SnapshotMeta, StoredMembership};

        let dir = tempfile::tempdir().unwrap();
        let s = store::open(dir.path()).unwrap();
        let config = Arc::new(Config::default().validate().unwrap());
        let raft = Raft::new(
            2,
            config,
            HttpNetwork::new().unwrap(),
            s.log_store,
            s.state_machine,
        )
        .await
        .unwrap();
        let gate = rpc::RaftGate::new(false);
        let addr = serve(rpc::router(
            raft.clone(),
            gate.clone(),
            rpc::SourceCheck::any(),
        ))
        .await;
        let before = raft.metrics().borrow().vote;

        let mut net = HttpNetwork::new().unwrap();
        let mut conn = net.new_client(2, &Node::new(addr.to_string())).await;
        let vote = VoteRequest::new(Vote::new(5, 1), None);
        let err = conn.vote(vote.clone(), option()).await.unwrap_err();
        assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
        let append = AppendEntriesRequest {
            vote: Vote::new_committed(5, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let err = conn.append_entries(append, option()).await.unwrap_err();
        assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
        let snapshot = InstallSnapshotRequest {
            vote: Vote::new_committed(5, 1),
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "s".into(),
            },
            offset: 0,
            data: vec![],
            done: true,
        };
        let err = conn.install_snapshot(snapshot, option()).await.unwrap_err();
        assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
        assert_eq!(raft.metrics().borrow().vote, before, "raft was touched");

        // The body says why, in the usual error shape.
        let resp = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{addr}/raft/append"))
            .body("not even json")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let body: faber_api_types::ErrorBody = resp.json().await.unwrap();
        assert_eq!(body.error, faber_api_types::error_code::NOT_READY);

        gate.open();
        assert!(conn.vote(vote, option()).await.unwrap().vote_granted);
        gate.close();
        assert!(!gate.is_open());

        raft.shutdown().await.unwrap();
    }

    #[test]
    fn a_refusing_target_is_warned_about_once_a_minute() {
        let refusals = RefusalWarnings::default();
        let shared = refusals.clone();
        let start = Instant::now();
        assert!(refusals.due(2, start));
        // Not again for this target, from any connection.
        assert!(!shared.due(2, start + Duration::from_secs(1)));
        assert!(!refusals.due(2, start + Duration::from_secs(59)));
        // Another target has its own minute.
        assert!(shared.due(3, start + Duration::from_secs(1)));
        assert!(refusals.due(2, start + Duration::from_secs(60)));
        assert!(!refusals.due(2, start + Duration::from_secs(61)));
        assert!(!refusals.due(3, start + Duration::from_secs(60)));
    }

    /// A target that answers 403 is retried like one that cannot be
    /// reached, and the first refusal is on record for the warning.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_forbidden_answer_is_a_peer_to_retry_later() {
        let dir = tempfile::tempdir().unwrap();
        let s = store::open(dir.path()).unwrap();
        let config = Arc::new(Config::default().validate().unwrap());
        let network = HttpNetwork::new().unwrap();
        let raft = Raft::new(2, config, network, s.log_store, s.state_machine)
            .await
            .unwrap();
        // Served without client addresses, so every source is refused.
        let nobody = rpc::SourceCheck::new(|_| false);
        let addr = serve(rpc::router(raft.clone(), rpc::RaftGate::new(true), nobody)).await;

        let mut net = HttpNetwork::new().unwrap();
        let mut conn = net.new_client(2, &Node::new(addr.to_string())).await;
        let vote = VoteRequest::new(Vote::new(5, 1), None);
        let err = conn.vote(vote, option()).await.unwrap_err();
        assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
        assert!(format!("{err}").contains("403"), "{err}");
        // The warning was given: the next one is not due yet.
        assert!(!net.refusals.due(2, Instant::now()));
        assert!(net.refusals.due(9, Instant::now()));

        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn closed_port_is_unreachable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut net = HttpNetwork::new().unwrap();
        let mut conn = net.new_client(9, &Node::new(addr.to_string())).await;
        let err = conn
            .vote(VoteRequest::new(Vote::new(1, 1), None), option())
            .await
            .unwrap_err();
        assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
    }

    #[tokio::test]
    async fn non_success_status_is_a_network_error() {
        let router = axum::Router::new().route(
            "/raft/vote",
            axum::routing::post(|| async {
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom")
            }),
        );
        let addr = serve(router).await;
        let mut net = HttpNetwork::new().unwrap();
        let mut conn = net.new_client(9, &Node::new(addr.to_string())).await;
        let err = conn
            .vote(VoteRequest::new(Vote::new(1, 1), None), option())
            .await
            .unwrap_err();
        assert!(matches!(err, RPCError::Network(_)), "{err:?}");
    }

    #[tokio::test]
    async fn remote_raft_error_is_reported_as_remote_error() {
        let router = axum::Router::new().route(
            "/raft/vote",
            axum::routing::post(|| async {
                let r: Result<VoteResponse<NodeId>, RaftError> =
                    Err(RaftError::Fatal(openraft::error::Fatal::Stopped));
                axum::Json(r)
            }),
        );
        let addr = serve(router).await;
        let mut net = HttpNetwork::new().unwrap();
        let mut conn = net.new_client(9, &Node::new(addr.to_string())).await;
        let err = conn
            .vote(VoteRequest::new(Vote::new(1, 1), None), option())
            .await
            .unwrap_err();
        match err {
            RPCError::RemoteError(e) => assert_eq!(e.target, 9),
            other => panic!("unexpected {other:?}"),
        }
    }
}
