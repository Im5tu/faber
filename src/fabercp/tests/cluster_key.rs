//! The cluster key is made once, at `cluster init`, and survives restarts.

mod common;

use faber_api_types::{ErrorBody, error_code};
use faber_crypto::{ClusterKey, key_id};
use reqwest::StatusCode;

use common::TestNode;

fn founder_key(c: &mut fabercp::Config) {
    c.cluster_key_file = Some(c.state_dir.join("founder.key"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_makes_the_key_once_and_a_restart_keeps_it() {
    let mut node = TestNode::start_with(1, founder_key).await;
    let path = node.dir.path().join("founder.key");
    assert!(!path.exists(), "no key before init");

    node.init().await;
    node.wait_leader(1).await;
    let made = std::fs::read(&path).unwrap();
    let key = ClusterKey::from_base64(std::str::from_utf8(&made).unwrap()).unwrap();
    let recorded = node.handle().state().cluster_key_id().unwrap().unwrap();
    assert_eq!(recorded.key_id, key_id(&key));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    node.stop().await;
    node.handle = Some(common::spawn(1, node.dir.path(), founder_key).await);
    node.wait_leader(1).await;
    assert_eq!(std::fs::read(&path).unwrap(), made, "restart keeps the key");

    // A second init is refused and leaves the key alone.
    let (status, body): (_, ErrorBody) = node.post("/api/v1/cluster/init", None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body.error, error_code::ALREADY_INITIALIZED);
    assert_eq!(std::fs::read(&path).unwrap(), made);
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_keeps_a_key_the_operator_put_there() {
    let mut node = TestNode::start_with(1, founder_key).await;
    let path = node.dir.path().join("founder.key");
    let key = ClusterKey::from_bytes([9; faber_crypto::KEY_LEN]);
    std::fs::write(&path, key.to_base64().as_str()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    node.init().await;
    node.wait_leader(1).await;
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        key.to_base64().as_str()
    );
    let recorded = node.handle().state().cluster_key_id().unwrap().unwrap();
    assert_eq!(recorded.key_id, key_id(&key));
    node.stop().await;
}
