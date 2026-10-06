//! Helpers for unit tests that need a running node.

use std::net::SocketAddr;
use std::time::Duration;

use tempfile::TempDir;
use tokio::net::TcpListener;

use crate::raft::store::StateReader;
use crate::{Config, FaberService, NodeHandle, start};

pub fn fast_config(node_id: u64, dir: &std::path::Path) -> Config {
    let mut c = Config::new(node_id, dir);
    c.heartbeat_ms = 50;
    c.election_min_ms = 150;
    c.election_max_ms = 300;
    c
}

pub struct TestNode {
    pub peer_addr: SocketAddr,
    pub service: FaberService,
    handle: NodeHandle,
    // Dropped after `handle` has shut down, so the DB is closed first.
    _dir: TempDir,
}

impl TestNode {
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let handle = start(fast_config(1, dir.path()), api, peer).await.unwrap();
        Self {
            peer_addr: handle.peer_addr(),
            service: handle.service().clone(),
            handle,
            _dir: dir,
        }
    }

    pub async fn start_initialized() -> Self {
        let node = Self::start().await;
        node.service.init().await.unwrap();
        node.handle
            .raft()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(1, "single node leader")
            .await
            .unwrap();
        node
    }

    /// A reader over the node's replicated state.
    pub fn state(&self) -> StateReader {
        self.handle.state().clone()
    }

    pub async fn shutdown(self) {
        let TestNode {
            service,
            handle,
            _dir,
            ..
        } = self;
        drop(service);
        handle.shutdown().await.unwrap();
    }
}
