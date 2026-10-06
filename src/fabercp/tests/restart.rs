//! Restart persistence: data, vote and membership survive a clean shutdown.

mod common;

use faber_api_types::{ErrorBody, error_code};
use reqwest::StatusCode;
use serde_json::json;

use common::TestNode;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_survives_restart() {
    let mut node = common::single_node().await;
    let mut last = 0;
    for i in 0..5 {
        last = node.write(&format!("r/{i}"), json!({"i": i})).await;
    }
    let before = node.handle().raft().metrics().borrow().clone();
    let old_peer_addr = node.peer_addr().to_string();

    node.restart().await;

    // Data is on disk: readable before any election happens.
    for i in 0..5 {
        assert_eq!(node.read(&format!("r/{i}")), Some(json!({"i": i})));
    }

    let after = node.handle().raft().metrics().borrow().clone();
    assert!(
        after.vote >= before.vote,
        "{} < {}",
        after.vote,
        before.vote
    );
    let membership = after.membership_config.membership();
    assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1]);
    assert_eq!(membership.get_node(&1).unwrap().addr, old_peer_addr);
    assert!(after.last_log_index >= Some(last));

    // Still an initialised cluster: init is refused, and it elects itself
    // and takes writes.
    let (s, body): (_, ErrorBody) = node.post("/api/v1/cluster/init", None).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(body.error, error_code::ALREADY_INITIALIZED);
    node.wait_leader(1).await;
    let index = node.write("r/after", json!(true)).await;
    assert!(index > last);
    assert_eq!(node.read("r/after"), Some(json!(true)));

    let status = node.status().await;
    assert!(status.joined);
    assert_eq!(status.members.len(), 1);

    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uninitialized_node_restarts_uninitialized() {
    let mut node = TestNode::start(4).await;
    node.restart().await;
    let status = node.status().await;
    assert_eq!(status.node_id, 4);
    assert!(!status.joined);
    assert!(status.members.is_empty());
    node.stop().await;
}
