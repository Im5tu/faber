//! Projects, environments and workloads across nodes: writes through a
//! follower, convergence, generations and revisions, prune, the refusals a
//! follower passes on from the leader, restarts and snapshot catch-up.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use faber_api_types::workload::{
    CreateProjectRequest, DeployRequest, DeployResponse, Exposure, Scale, Strategy, WorkloadQuery,
    WorkloadSpec, fqdn,
};
use faber_api_types::{ClientError, FaberClient, error_code};
use serde_json::json;

use common::{TestNode, WAIT};

fn client(node: &TestNode) -> FaberClient {
    FaberClient::new(&format!("http://{}", node.api_addr())).unwrap()
}

/// One container that runs `image`, with an env value so that values are
/// seen to travel with the spec.
fn spec(image: &str) -> WorkloadSpec {
    serde_json::from_value(json!({
        "containers": [{"name": "main", "image": image, "env": {"MODE": "e2e"}}],
    }))
    .unwrap()
}

fn workloads(specs: &[(&str, &WorkloadSpec)]) -> BTreeMap<String, WorkloadSpec> {
    specs
        .iter()
        .map(|(name, spec)| (name.to_string(), (*spec).clone()))
        .collect()
}

async fn deploy(
    client: &FaberClient,
    project: &str,
    environment: &str,
    specs: &[(&str, &WorkloadSpec)],
    prune: bool,
) -> Result<DeployResponse, ClientError> {
    let req = DeployRequest {
        workloads: workloads(specs),
        prune,
    };
    client.deploy(project, environment, &req).await
}

fn project(name: &str, environments: Option<&[&str]>) -> CreateProjectRequest {
    CreateProjectRequest {
        name: name.to_string(),
        environments: environments.map(|e| e.iter().map(|n| n.to_string()).collect()),
    }
}

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

/// Status, code and message of a call the API must refuse.
#[track_caller]
fn refused<T: Debug>(result: Result<T, ClientError>) -> (u16, String, Option<String>) {
    match result {
        Err(ClientError::Api { status, body }) => (status, body.error, body.message),
        other => panic!("expected an API error, got {other:?}"),
    }
}

/// The generation of one environment as `node` reports it.
async fn generation(node: &TestNode, project: &str, environment: &str) -> u64 {
    let found = client(node).project_get(project).await.unwrap();
    let environment = found.environments.iter().find(|e| e.name == environment);
    environment.expect("the environment exists").generation
}

/// The index of the last entry `node` has applied.
fn last_applied(node: &TestNode) -> u64 {
    let metrics = node.handle().raft().metrics().borrow().clone();
    metrics.last_applied.expect("something is applied").index
}

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

/// Election timeouts long enough that a stalled heartbeat starts no
/// election. The tests below count on node 1 staying the leader: with the
/// 150 to 300 ms of `fast_config` a busy machine now and then elects
/// another node in the middle of a test, and a write that meets the change
/// is answered 503 `not_leader` or `no_leader`.
fn steady(c: &mut fabercp::Config) {
    c.election_min_ms = 1500;
    c.election_max_ms = 3000;
}

/// Node 1 leads, nodes 2 and 3 follow.
async fn three_nodes() -> Vec<TestNode> {
    let nodes = vec![
        TestNode::start_with(1, steady).await,
        TestNode::start_with(2, steady).await,
        TestNode::start_with(3, steady).await,
    ];
    nodes[0].init().await;
    nodes[0].wait_leader(1).await;
    assert!(nodes[1].join(&nodes[0]).await.joined);
    assert!(nodes[2].join(&nodes[0]).await.joined);
    for n in &nodes {
        wait_voters(n, &[1, 2, 3]).await;
        n.wait_leader(1).await;
    }
    nodes
}

/// Waits until every node has applied what the node that is furthest has,
/// and checks that all of them hold the same projects and environment
/// records.
async fn assert_converged(nodes: &[TestNode]) {
    let index = nodes.iter().map(last_applied).max().unwrap();
    for n in nodes {
        n.wait_applied(index).await;
    }
    let projects = nodes[0].handle().state().projects().unwrap();
    let environments = nodes[0].handle().state().environments().unwrap();
    for n in &nodes[1..] {
        assert_eq!(
            n.handle().state().projects().unwrap(),
            projects,
            "projects on node {}",
            n.id
        );
        assert_eq!(
            n.handle().state().environments().unwrap(),
            environments,
            "environments on node {}",
            n.id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projects_and_workloads_replicate_through_followers() {
    let mut nodes = three_nodes().await;
    let (leader, f2, f3) = (client(&nodes[0]), client(&nodes[1]), client(&nodes[2]));

    // A project made through a follower, read back from that follower at
    // once.
    let shop = f2.project_create(&project("shop", None)).await.unwrap();
    assert_eq!(shop.name, "shop");
    let environments: Vec<&str> = shop.environments.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(environments, ["production", "staging"]);
    let g0 = shop.environments[0].generation;
    assert!(g0 > 0);
    for e in &shop.environments {
        assert_eq!((e.generation, e.workloads), (g0, 0), "{e:?}");
    }
    assert_eq!(f2.project_get("shop").await.unwrap(), shop);

    // A deploy through the other follower, read back from it at once.
    let (web, api) = (spec("nginx:1.27"), spec("ghcr.io/acme/api:1"));
    let first = deploy(
        &f3,
        "shop",
        "staging",
        &[("web", &web), ("api", &api)],
        false,
    )
    .await
    .unwrap();
    let g1 = first.generation;
    assert!(g1 > g0, "{g1} > {g0}");
    assert_eq!(first.created, names(&["api", "web"]));
    assert!(first.updated.is_empty() && first.unchanged.is_empty() && first.removed.is_empty());
    let read = f3
        .workload_get("shop", "staging", "web", true)
        .await
        .unwrap();
    assert_eq!(read.spec, web);
    // Without `reveal` the follower shows the names and masks the values.
    let masked = f3
        .workload_get("shop", "staging", "web", false)
        .await
        .unwrap();
    assert_eq!(masked.spec.containers[0].env["MODE"], "***");
    assert_eq!((read.revision, read.generation), (g1, g1));
    assert_eq!(read.fqdn, "web.staging.shop.faber.internal");
    assert_eq!(read.fqdn, fqdn("web", "staging", "shop"));

    // All three nodes hold the same environment record at that generation.
    assert_converged(&nodes).await;
    for n in &nodes {
        let record = n.handle().state().environment("shop", "staging").unwrap();
        let record = record.expect("the record exists");
        assert_eq!(record.generation, g1, "node {}", n.id);
        assert_eq!(record.workloads.len(), 2, "node {}", n.id);
        // The record holds the env names and sealed values, never a value.
        let stored = &record.workloads["web"];
        let mut cleared = web.clone();
        cleared.containers[0].env.clear();
        assert_eq!(stored.spec, cleared, "node {}", n.id);
        let sealed: Vec<&String> = stored.env["main"].values.keys().collect();
        assert_eq!(sealed, ["MODE"], "node {}", n.id);
        // The other environment was not touched.
        assert_eq!(generation(n, "shop", "production").await, g0);
    }

    // The same deploy again changes nothing and keeps the generation. The
    // leader answers it from its state, without a log entry.
    let again = deploy(
        &f2,
        "shop",
        "staging",
        &[("web", &web), ("api", &api)],
        false,
    )
    .await
    .unwrap();
    assert_eq!(again.unchanged, names(&["api", "web"]));
    assert!(again.created.is_empty() && again.updated.is_empty() && again.removed.is_empty());
    assert_eq!(again.generation, g1);
    assert_eq!(generation(&nodes[1], "shop", "staging").await, g1);

    // Scale, placement and exposure are not part of the pod template: the
    // generation moves and the revision stays.
    let mut scaled = web.clone();
    scaled.scale = Scale::Replicas {
        count: 3,
        strategy: Strategy::Spread,
    };
    scaled.expose = Exposure::Cluster;
    let second = deploy(
        &f3,
        "shop",
        "staging",
        &[("web", &scaled), ("api", &api)],
        false,
    )
    .await
    .unwrap();
    let g2 = second.generation;
    assert!(g2 > g1);
    assert_eq!(second.updated, names(&["web"]));
    assert_eq!(second.unchanged, names(&["api"]));
    let read = f3
        .workload_get("shop", "staging", "web", true)
        .await
        .unwrap();
    assert_eq!((read.revision, read.generation), (g1, g2));
    assert_eq!(read.spec, scaled);

    // A new image is a new template: the revision moves, and only for the
    // workload that changed.
    let mut retagged = scaled.clone();
    retagged.containers[0].image = "nginx:1.28".into();
    let third = deploy(&f2, "shop", "staging", &[("web", &retagged)], false)
        .await
        .unwrap();
    let g3 = third.generation;
    assert!(g3 > g2);
    assert_eq!(third.updated, names(&["web"]));
    assert!(third.unchanged.is_empty() && third.removed.is_empty());
    let read = f2
        .workload_get("shop", "staging", "web", true)
        .await
        .unwrap();
    assert_eq!((read.revision, read.generation), (g3, g3));
    let read = f2
        .workload_get("shop", "staging", "api", true)
        .await
        .unwrap();
    assert_eq!((read.revision, read.generation), (g1, g3));

    assert_converged(&nodes).await;
    let all = WorkloadQuery::default();
    let listed = leader.workload_list(&all, false).await.unwrap();
    let rows: Vec<_> = listed
        .workloads
        .iter()
        .map(|w| (w.fqdn.as_str(), w.scale, w.expose, w.revision))
        .collect();
    assert_eq!(
        rows,
        [
            (
                "api.staging.shop.faber.internal",
                Scale::Replicas {
                    count: 1,
                    strategy: Strategy::Spread
                },
                Exposure::Environment,
                g1
            ),
            (
                "web.staging.shop.faber.internal",
                Scale::Replicas {
                    count: 3,
                    strategy: Strategy::Spread
                },
                Exposure::Cluster,
                g3
            ),
        ]
    );
    for follower in [&f2, &f3] {
        assert_eq!(follower.workload_list(&all, false).await.unwrap(), listed);
    }

    // What the leader refuses, a follower refuses with the same status,
    // code and message.
    let both = [&leader, &f3];
    let mut answers = Vec::new();
    for c in both {
        answers.push(vec![
            refused(deploy(c, "nope", "staging", &[("web", &web)], false).await),
            refused(deploy(c, "shop", "nope", &[("web", &web)], false).await),
            refused(c.project_create(&project("shop", None)).await),
            refused(c.environment_add("shop", "staging").await),
            refused(c.environment_add("nope", "staging").await),
            refused(c.project_delete("shop").await),
            refused(c.project_delete("nope").await),
            refused(c.environment_remove("shop", "staging").await),
            refused(c.workload_delete("shop", "staging", "nope").await),
        ]);
    }
    let codes: Vec<(u16, &str)> = answers[0]
        .iter()
        .map(|(status, code, _)| (*status, code.as_str()))
        .collect();
    assert_eq!(
        codes,
        [
            (404, error_code::NOT_FOUND),
            (404, error_code::NOT_FOUND),
            (409, error_code::PROJECT_EXISTS),
            (409, error_code::PROJECT_EXISTS),
            (404, error_code::NOT_FOUND),
            (409, error_code::NOT_EMPTY),
            (404, error_code::NOT_FOUND),
            (409, error_code::NOT_EMPTY),
            (404, error_code::NOT_FOUND),
        ]
    );
    assert_eq!(answers[1], answers[0], "follower against leader");
    // A spec the model refuses never reaches the leader, and is a 400 on
    // either node.
    for c in both {
        let (status, code, message) =
            refused(deploy(c, "shop", "staging", &[("web", &spec(""))], false).await);
        assert_eq!((status, code.as_str()), (400, error_code::BAD_REQUEST));
        let message = message.expect("a 400 says why");
        assert!(message.contains("web.containers[0].image"), "{message}");
    }
    // None of the refusals changed anything.
    assert_converged(&nodes).await;
    for n in &nodes {
        assert_eq!(generation(n, "shop", "staging").await, g3, "node {}", n.id);
    }

    // Prune removes what the deploy does not name.
    let pruned = deploy(&f3, "shop", "staging", &[("api", &api)], true)
        .await
        .unwrap();
    let g4 = pruned.generation;
    assert!(g4 > g3);
    assert_eq!(pruned.removed, names(&["web"]));
    assert_eq!(pruned.unchanged, names(&["api"]));
    assert_converged(&nodes).await;
    for c in [&leader, &f2, &f3] {
        let (status, code, _) = refused(c.workload_get("shop", "staging", "web", true).await);
        assert_eq!((status, code.as_str()), (404, error_code::NOT_FOUND));
        let api = c
            .workload_get("shop", "staging", "api", true)
            .await
            .unwrap();
        assert_eq!((api.revision, api.generation), (g1, g4));
    }

    // A second project that stays, so the restart below has state to keep.
    let blog = f2
        .project_create(&project("blog", Some(&["dev"])))
        .await
        .unwrap();
    assert_eq!(blog.environments.len(), 1);
    let site = spec("ghost:5");
    deploy(&f2, "blog", "dev", &[("site", &site)], false)
        .await
        .unwrap();

    // The project goes once its last workload is gone.
    let (status, code, _) = refused(f2.project_delete("shop").await);
    assert_eq!((status, code.as_str()), (409, error_code::NOT_EMPTY));
    f2.workload_delete("shop", "staging", "api").await.unwrap();
    assert!(generation(&nodes[1], "shop", "staging").await > g4);
    f2.project_delete("shop").await.unwrap();
    let (status, code, _) = refused(f2.project_get("shop").await);
    assert_eq!((status, code.as_str()), (404, error_code::NOT_FOUND));
    assert_converged(&nodes).await;
    let blog = leader.project_get("blog").await.unwrap();
    assert_eq!(blog.environments[0].workloads, 1);
    for c in [&leader, &f2, &f3] {
        assert_eq!(
            c.project_list().await.unwrap().projects,
            std::slice::from_ref(&blog)
        );
    }

    // A follower that restarts has the state on disk, and serves it before
    // it hears from a leader. This comes last: the node comes back on other
    // ports, so the leader no longer reaches it.
    let projects = nodes[2].handle().state().projects().unwrap();
    let environments = nodes[2].handle().state().environments().unwrap();
    let workload = f3.workload_get("blog", "dev", "site", true).await.unwrap();
    assert_eq!(workload.spec, site);
    nodes[2].restart().await;
    assert_eq!(nodes[2].handle().state().projects().unwrap(), projects);
    assert_eq!(
        nodes[2].handle().state().environments().unwrap(),
        environments
    );
    let restarted = client(&nodes[2]);
    assert_eq!(
        restarted
            .workload_get("blog", "dev", "site", true)
            .await
            .unwrap(),
        workload
    );
    let (status, code, _) = refused(restarted.project_get("shop").await);
    assert_eq!((status, code.as_str()), (404, error_code::NOT_FOUND));

    for n in &mut nodes {
        n.stop().await;
    }
}

/// A single node keeps projects and workloads across a restart and goes on
/// from them: the same deploy is still unchanged, and a new one gets a
/// higher generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_survives_a_restart_and_generations_go_on() {
    let mut node = common::single_node().await;
    let web = spec("nginx:1.27");
    let (shop, deployed, read) = {
        let c = client(&node);
        let shop = c.project_create(&project("shop", None)).await.unwrap();
        let deployed = deploy(&c, "shop", "staging", &[("web", &web)], false)
            .await
            .unwrap();
        let shop_after = c.project_get("shop").await.unwrap();
        assert_ne!(shop_after, shop);
        let read = c
            .workload_get("shop", "staging", "web", true)
            .await
            .unwrap();
        (shop_after, deployed, read)
    };

    node.restart().await;
    let c = client(&node);
    // Readable before any election.
    assert_eq!(c.project_get("shop").await.unwrap(), shop);
    assert_eq!(
        c.workload_get("shop", "staging", "web", true)
            .await
            .unwrap(),
        read
    );

    node.wait_leader(1).await;
    let again = deploy(&c, "shop", "staging", &[("web", &web)], false)
        .await
        .unwrap();
    assert_eq!(again.unchanged, names(&["web"]));
    assert_eq!(again.generation, deployed.generation);
    let next = deploy(
        &c,
        "shop",
        "staging",
        &[("web", &spec("nginx:1.28"))],
        false,
    )
    .await
    .unwrap();
    assert_eq!(next.updated, names(&["web"]));
    assert!(next.generation > deployed.generation);

    node.stop().await;
}

/// A node that joins after the leader has snapshotted and purged its log
/// gets the projects and the workloads from the snapshot, and can then write
/// through the leader like any follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_joiner_gets_projects_and_workloads_from_a_snapshot() {
    let snapshot_every = |c: &mut fabercp::Config| {
        c.snapshot_logs = 10;
        c.keep_logs = 0;
        steady(c);
    };
    let mut n1 = TestNode::start_with(1, snapshot_every).await;
    n1.init().await;
    n1.wait_leader(1).await;
    let c1 = client(&n1);

    c1.project_create(&project("shop", None)).await.unwrap();
    c1.project_create(&project("blog", Some(&["dev"])))
        .await
        .unwrap();
    let api = spec("ghcr.io/acme/api:1");
    let mut last = 0;
    for i in 0..30 {
        let web = spec(&format!("nginx:1.{i}"));
        let done = deploy(
            &c1,
            "shop",
            "staging",
            &[("web", &web), ("api", &api)],
            false,
        )
        .await
        .unwrap();
        last = done.generation;
    }
    // Something removed and something emptied before the snapshot, too.
    deploy(&c1, "blog", "dev", &[("site", &spec("ghost:5"))], false)
        .await
        .unwrap();
    c1.workload_delete("blog", "dev", "site").await.unwrap();
    c1.environment_add("blog", "preview").await.unwrap();
    let end = last_applied(&n1);
    assert!(end > last);

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
    n2.wait_applied(end).await;
    let m = n2.handle().raft().metrics().borrow().clone();
    assert!(
        m.snapshot.is_some(),
        "joiner should have installed a snapshot"
    );

    assert_eq!(
        n2.handle().state().projects().unwrap(),
        n1.handle().state().projects().unwrap()
    );
    assert_eq!(
        n2.handle().state().environments().unwrap(),
        n1.handle().state().environments().unwrap()
    );
    let c2 = client(&n2);
    assert_eq!(
        c2.project_list().await.unwrap(),
        c1.project_list().await.unwrap()
    );
    let web = c2
        .workload_get("shop", "staging", "web", true)
        .await
        .unwrap();
    assert_eq!(web.spec, spec("nginx:1.29"));
    assert_eq!((web.revision, web.generation), (last, last));
    assert_eq!(
        web,
        c1.workload_get("shop", "staging", "web", true)
            .await
            .unwrap()
    );
    // `api` never changed after the first deploy, so its revision is older
    // than anything left in the log.
    let api_read = c2
        .workload_get("shop", "staging", "api", true)
        .await
        .unwrap();
    assert!(api_read.revision < 20, "{}", api_read.revision);
    let blog = c2.project_get("blog").await.unwrap();
    let environments: Vec<(&str, u32)> = blog
        .environments
        .iter()
        .map(|e| (e.name.as_str(), e.workloads))
        .collect();
    assert_eq!(environments, [("dev", 0), ("preview", 0)]);

    // The joiner writes through the leader and reads its own write.
    n2.wait_leader(1).await;
    let done = deploy(&c2, "shop", "production", &[("api", &api)], false)
        .await
        .unwrap();
    assert_eq!(done.created, names(&["api"]));
    assert!(done.generation > end);
    let read = c2
        .workload_get("shop", "production", "api", true)
        .await
        .unwrap();
    assert_eq!(
        (read.revision, read.generation),
        (done.generation, done.generation)
    );

    n2.stop().await;
    n1.stop().await;
}
