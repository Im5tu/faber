//! Helpers for unit tests that need a running node.

use std::net::SocketAddr;
use std::time::Duration;

use tempfile::TempDir;
use tokio::net::TcpListener;

use crate::raft::store::StateReader;
use crate::{Config, FaberService, NodeHandle, start};

/// The cluster key of every test node, so that nodes of one test open each
/// other's data keys.
pub fn test_cluster_key() -> faber_crypto::ClusterKey {
    faber_crypto::ClusterKey::from_bytes([7; faber_crypto::KEY_LEN])
}

/// A config with short timers and a cluster key file in `dir`.
pub fn fast_config(node_id: u64, dir: &std::path::Path) -> Config {
    let mut c = Config::new(node_id, dir);
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

    /// The directory that holds the node's data and its `cluster.key`.
    pub fn dir(&self) -> &std::path::Path {
        self._dir.path()
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

/// What a test subscriber wrote, to search for log lines.
#[derive(Clone, Default)]
pub struct LogBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl LogBuf {
    /// A subscriber at INFO that writes here. Run a future under it with
    /// `tracing::instrument::WithSubscriber::with_subscriber`, which also
    /// covers the worker threads that poll the future.
    pub fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        tracing_subscriber::fmt()
            .with_writer(self.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish()
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;

    fn make_writer(&'a self) -> LogBuf {
        self.clone()
    }
}
