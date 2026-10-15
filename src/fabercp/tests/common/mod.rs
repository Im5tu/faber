//! Shared helpers for faber integration tests.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use faber_api_types::{ClusterStatus, JoinRequest};
use fabercp::raft::types::Request;
use fabercp::{Config, NodeHandle};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use tempfile::TempDir;
use tokio::net::TcpListener;

pub const WAIT: Duration = Duration::from_secs(20);

/// The cluster key every test node holds.
pub fn test_cluster_key() -> faber_crypto::ClusterKey {
    faber_crypto::ClusterKey::from_bytes([7; faber_crypto::KEY_LEN])
}

/// Short Raft timings so elections settle quickly in tests.
pub fn fast_config(node_id: u64, dir: &Path) -> Config {
    let mut c = Config::new(node_id, dir);
    // One cluster key for every node of a test, so that a follower opens
    // the data keys the leader made.
    std::fs::create_dir_all(dir).unwrap();
    let key_file = dir.join("cluster.key");
    std::fs::write(&key_file, test_cluster_key().to_base64().as_str()).unwrap();
    // The node refuses a key file that others can read.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    c.cluster_key_file = Some(key_file);
    c.heartbeat_ms = 50;
    c.election_min_ms = 150;
    c.election_max_ms = 300;
    c
}

pub fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// A node plus the directory holding its data. The directory outlives the
/// node so it can be restarted, and is removed only after the DB is closed.
pub struct TestNode {
    pub id: u64,
    pub dir: TempDir,
    pub handle: Option<NodeHandle>,
}

impl TestNode {
    pub async fn start(id: u64) -> Self {
        Self::start_with(id, |_| {}).await
    }

    pub async fn start_with(id: u64, tweak: impl FnOnce(&mut Config)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let handle = spawn(id, dir.path(), tweak).await;
        Self {
            id,
            dir,
            handle: Some(handle),
        }
    }

    pub fn handle(&self) -> &NodeHandle {
        self.handle.as_ref().expect("node is stopped")
    }

    pub fn api_addr(&self) -> SocketAddr {
        self.handle().api_addr()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.handle().peer_addr()
    }

    pub fn api_url(&self, path: &str) -> String {
        format!("http://{}{}", self.api_addr(), path)
    }

    pub async fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown().await.unwrap();
        }
    }

    /// Restarts with the same data directory (on fresh ports).
    pub async fn restart(&mut self) {
        self.stop().await;
        self.handle = Some(spawn(self.id, self.dir.path(), |_| {}).await);
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> (StatusCode, T) {
        let resp = http().get(self.api_url(path)).send().await.unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap())
    }

    pub async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> (StatusCode, T) {
        let mut rb = http().post(self.api_url(path));
        if let Some(b) = body {
            rb = rb.json(b);
        }
        let resp = rb.send().await.unwrap();
        let status = resp.status();
        let bytes = resp.bytes().await.unwrap();
        let parsed = serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("{status} {e}: {}", String::from_utf8_lossy(&bytes)));
        (status, parsed)
    }

    pub async fn status(&self) -> ClusterStatus {
        let (s, body) = self.get("/api/v1/cluster/status").await;
        assert_eq!(s, StatusCode::OK);
        body
    }

    pub async fn init(&self) -> ClusterStatus {
        let (s, body) = self.post("/api/v1/cluster/init", None).await;
        assert_eq!(s, StatusCode::OK);
        body
    }

    /// Joins via the REST API, pointing at `peer`'s peer listener.
    pub async fn join(&self, peer: &TestNode) -> ClusterStatus {
        let req = serde_json::to_value(JoinRequest {
            peer: peer.peer_addr().to_string(),
        })
        .unwrap();
        let (s, body): (_, serde_json::Value) = self.post("/api/v1/cluster/join", Some(&req)).await;
        assert_eq!(s, StatusCode::OK, "{body}");
        serde_json::from_value(body).unwrap()
    }

    /// Waits until this node sees `leader` as the leader.
    pub async fn wait_leader(&self, leader: u64) {
        self.handle()
            .raft()
            .wait(Some(WAIT))
            .current_leader(leader, "leader")
            .await
            .unwrap();
    }

    /// Replicates a placeholder `Set` through Raft (no API exposes writes yet).
    /// Must be called on the leader. Returns the log index.
    pub async fn write(&self, key: &str, value: serde_json::Value) -> u64 {
        self.handle()
            .raft()
            .client_write(Request::Set {
                key: key.to_string(),
                value,
            })
            .await
            .unwrap()
            .log_id
            .index
    }

    pub fn read(&self, key: &str) -> Option<serde_json::Value> {
        self.handle().state().get(key).unwrap()
    }

    pub async fn wait_applied(&self, index: u64) {
        self.handle()
            .raft()
            .wait(Some(WAIT))
            .applied_index_at_least(Some(index), "applied")
            .await
            .unwrap();
    }
}

/// Binds two loopback listeners on port 0 first so addresses are real.
pub async fn spawn(id: u64, dir: &Path, tweak: impl FnOnce(&mut Config)) -> NodeHandle {
    let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = fast_config(id, dir);
    tweak(&mut config);
    fabercp::start(config, api, peer).await.unwrap()
}

/// Starts node 1 and initialises it as a single-node cluster.
pub async fn single_node() -> TestNode {
    let node = TestNode::start(1).await;
    node.init().await;
    node.wait_leader(1).await;
    node
}
