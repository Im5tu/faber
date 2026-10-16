//! Multi-node tests: forming a cluster with `init` + `join` (including joins
//! forwarded by a non-leader), follower status, leader failover and snapshot
//! catch-up.

mod common;

use std::collections::BTreeSet;

use faber_api_types::{ErrorBody, FORWARDED_HEADER, error_code};
use fabercp::dto::PeerJoinRequest;
use reqwest::StatusCode;
use serde_json::json;

use common::{TestNode, WAIT, steady};
use fabercp::raft::types::Node;
use tokio::net::TcpListener;

async fn wait_voters(node: &TestNode, want: &[u64]) {
    let want: BTreeSet<u64> = want.iter().copied().collect();
    node.handle()
        .raft()
        .wait(Some(WAIT))
        .metrics(
            |m| {
                m.membership_config
                    .membership()
                    .voter_ids()
                    .collect::<BTreeSet<_>>()
                    == want
            },
            "voters",
        )
        .await
        .unwrap();
}

/// Waits until `node` sees a leader other than `not`, and returns it.
async fn wait_new_leader(node: &TestNode, not: u64) -> u64 {
    node.handle()
        .raft()
        .wait(Some(WAIT))
        .metrics(|m| m.current_leader.is_some_and(|l| l != not), "new leader")
        .await
        .unwrap()
        .current_leader
        .unwrap()
}

/// Waits until the leader has matched log ids for all of `followers`.
async fn wait_replication(leader: &TestNode, followers: &[u64]) {
    leader
        .handle()
        .raft()
        .wait(Some(WAIT))
        .metrics(
            |m| {
                m.replication.as_ref().is_some_and(|r| {
                    followers
                        .iter()
                        .all(|f| r.get(f).is_some_and(|l| l.is_some()))
                })
            },
            "replication progress",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_forms_forwards_and_fails_over() {
    let mut nodes = vec![
        TestNode::start_with(1, steady).await,
        TestNode::start_with(2, steady).await,
        TestNode::start_with(3, steady).await,
    ];

    nodes[0].init().await;
    nodes[0].wait_leader(1).await;

    // Node 2 joins through the leader; node 3 through node 2, a non-leader
    // that forwards the join to the leader server-side.
    let s2 = nodes[1].join(&nodes[0]).await;
    assert!(s2.joined);
    nodes[1].wait_leader(1).await;
    let s3 = nodes[2].join(&nodes[1]).await;
    assert!(s3.joined);
    for n in &nodes {
        wait_voters(n, &[1, 2, 3]).await;
    }

    // Joining again is refused locally.
    let (s, body): (_, ErrorBody) = nodes[2]
        .post(
            "/api/v1/cluster/join",
            Some(&json!({"peer": nodes[0].peer_addr().to_string()})),
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(body.error, error_code::ALREADY_JOINED);

    // Status on a follower lists all three members with the leader's
    // replication progress.
    wait_replication(&nodes[0], &[2, 3]).await;
    let s = nodes[2].status().await;
    assert_eq!(s.node_id, 3);
    assert_eq!(s.role, "follower");
    assert_eq!(s.leader_id, Some(1));
    assert_eq!(s.leader_addr, Some(nodes[0].peer_addr().to_string()));
    for (m, n) in s.members.iter().zip(&nodes) {
        assert_eq!(m.node_id, n.id);
        assert_eq!(m.addr, n.peer_addr().to_string());
        assert!(m.voter);
        assert!(m.matched_index.is_some(), "{m:?}");
        assert!(m.lag.is_some(), "{m:?}");
    }

    // The loop guard: a forwarded join that lands on a non-leader is refused.
    let resp = common::http()
        .post(format!("http://{}/raft/cluster/join", nodes[1].peer_addr()))
        .header(FORWARDED_HEADER, "1")
        .json(&PeerJoinRequest {
            node_id: 9,
            addr: "127.0.0.1:1".into(),
            invite_id: None,
            instance_id: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: ErrorBody = resp.json().await.unwrap();
    assert_eq!(body.error, error_code::NOT_LEADER);
    assert_eq!(body.leader_id, Some(1));

    // Replication: a write on the leader is applied everywhere.
    let index = nodes[0].write("k1", json!({"v": 1})).await;
    for n in &nodes {
        n.wait_applied(index).await;
        assert_eq!(n.read("k1"), Some(json!({"v": 1})));
    }

    // Failover: stop the leader; a new one is elected and takes writes.
    nodes[0].stop().await;
    let new_leader = wait_new_leader(&nodes[1], 1).await;
    wait_new_leader(&nodes[2], 1).await;
    let (leader_idx, follower_idx) = if new_leader == 2 { (1, 2) } else { (2, 1) };
    assert_eq!(nodes[leader_idx].id, new_leader);

    let index = nodes[leader_idx].write("k2", json!("after failover")).await;
    nodes[follower_idx].wait_applied(index).await;
    assert_eq!(
        nodes[follower_idx].read("k2"),
        Some(json!("after failover"))
    );

    // Joining still works after failover, through the follower.
    let n4 = TestNode::start_with(4, steady).await;
    let s4 = n4.join(&nodes[follower_idx]).await;
    assert!(s4.joined);
    assert_eq!(s4.leader_id, Some(new_leader));
    wait_voters(&nodes[leader_idx], &[1, 2, 3, 4]).await;
    n4.wait_applied(index).await;
    assert_eq!(n4.read("k1"), Some(json!({"v": 1})));
    nodes.push(n4);

    for n in &mut nodes {
        n.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_joiner_catches_up_from_snapshot() {
    let snapshot_every = |c: &mut fabercp::Config| {
        c.snapshot_logs = 10;
        c.keep_logs = 0;
        steady(c);
    };
    let mut n1 = TestNode::start_with(1, snapshot_every).await;
    n1.init().await;
    n1.wait_leader(1).await;

    let mut last = 0;
    for i in 0..30 {
        last = n1.write(&format!("key/{i:02}"), json!(i)).await;
    }

    // Wait until the leader has snapshotted and purged the early logs, so the
    // joiner cannot catch up from the log alone.
    n1.handle()
        .raft()
        .wait(Some(WAIT))
        .metrics(
            |m| m.purged.is_some_and(|p| p.index >= 20),
            "logs purged after snapshot",
        )
        .await
        .unwrap();

    let mut n2 = TestNode::start_with(2, snapshot_every).await;
    n2.join(&n1).await;

    n2.wait_applied(last).await;
    let m = n2.handle().raft().metrics().borrow().clone();
    assert!(
        m.snapshot.is_some(),
        "joiner should have installed a snapshot"
    );
    for i in 0..30 {
        assert_eq!(
            n2.read(&format!("key/{i:02}")),
            Some(json!(i)),
            "key/{i:02}"
        );
    }

    n2.stop().await;
    n1.stop().await;
}

/// `joined` means "this node is a voter". A learner is in the membership and
/// replicates the log, but its join has not finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_does_not_count_as_joined() {
    let mut n1 = common::single_node().await;
    let mut n2 = TestNode::start_with(2, steady).await;
    n1.handle()
        .raft()
        .add_learner(2, Node::new(n2.peer_addr().to_string()), true)
        .await
        .unwrap();
    n2.handle()
        .raft()
        .wait(Some(WAIT))
        .metrics(
            |m| m.membership_config.membership().get_node(&2).is_some(),
            "node 2 sees itself in the membership",
        )
        .await
        .unwrap();

    let status = n2.status().await;
    assert!(!status.joined, "{status:?}");
    assert_eq!(status.role, "learner");
    let me = status.members.iter().find(|m| m.node_id == 2).unwrap();
    assert!(!me.voter);
    // The leader, a voter, is joined.
    assert!(n1.status().await.joined);

    // Once it is promoted, it is.
    assert!(n2.join(&n1).await.joined);
    wait_voters(&n2, &[1, 2]).await;
    assert!(n2.status().await.joined);

    n2.stop().await;
    n1.stop().await;
}

/// A node left as a learner by an interrupted join is promoted only once it
/// has caught up. While it cannot be reached the join answers `not_ready`,
/// which the joiner retries, and the cluster keeps its single voter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_is_promoted_only_once_it_has_caught_up() {
    let mut n1 = common::single_node().await;

    // Node 2's peer address, with nothing listening on it yet. The port is
    // held bound, so no other test can take it, and connections to it are
    // refused until the socket listens.
    let reserved = tokio::net::TcpSocket::new_v4().unwrap();
    reserved.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr2 = reserved.local_addr().unwrap();
    n1.handle()
        .raft()
        .add_learner(2, Node::new(addr2.to_string()), false)
        .await
        .unwrap();

    let join = || async {
        let resp = common::http()
            .post(format!("http://{}/raft/cluster/join", n1.peer_addr()))
            .json(&PeerJoinRequest {
                node_id: 2,
                addr: addr2.to_string(),
                invite_id: None,
                instance_id: None,
            })
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let code = if status.is_success() {
            None
        } else {
            Some(resp.json::<ErrorBody>().await.unwrap().error)
        };
        (status, code)
    };

    let (status, code) = join().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(code.as_deref(), Some(error_code::NOT_READY));
    wait_voters(&n1, &[1]).await;
    // Still writable: the learner was not counted into the quorum.
    n1.write("k", json!(1)).await;

    // Node 2 comes up at that address and catches up; the retry succeeds.
    let dir2 = tempfile::tempdir().unwrap();
    let api2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer2 = reserved.listen(1024).unwrap();
    let mut config2 = common::fast_config(2, dir2.path());
    steady(&mut config2);
    let n2 = fabercp::start(config2, api2, peer2).await.unwrap();
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let (status, code) = join().await;
        if status == StatusCode::NO_CONTENT {
            break;
        }
        assert_eq!(code.as_deref(), Some(error_code::NOT_READY), "{status}");
        assert!(tokio::time::Instant::now() < deadline, "never promoted");
    }
    wait_voters(&n1, &[1, 2]).await;

    n2.shutdown().await.unwrap();
    n1.stop().await;
}
