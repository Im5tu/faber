//! `run()` against a wiremock server: request shapes and output formatting.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use clap::Parser;
use faber::deploy::Inputs;
use faber::{Cli, run};
use faber_api_types::workload::WorkloadSpec;
use serde_json::{Value, json};
use wiremock::matchers::{any, body_json, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn cli(server: &MockServer, args: &[&str]) -> Cli {
    let mut full = vec!["faber", "-e", server.uri().leak()];
    full.extend_from_slice(args);
    Cli::try_parse_from(full).expect("parse")
}

async fn run_ok(server: &MockServer, args: &[&str]) -> String {
    let mut out = Vec::new();
    run(cli(server, args), &mut out)
        .await
        .unwrap_or_else(|e| panic!("run {args:?}: {e:#}"));
    String::from_utf8(out).expect("utf8")
}

async fn run_err(server: &MockServer, args: &[&str]) -> (String, String) {
    let mut out = Vec::new();
    let err = run(cli(server, args), &mut out)
        .await
        .expect_err("expected an error");
    (format!("{err:#}"), String::from_utf8(out).expect("utf8"))
}

fn status_body() -> Value {
    json!({
        "node_id": 2,
        "joined": true,
        "role": "follower",
        "term": 3,
        "leader_id": 1,
        "leader_addr": "10.8.0.1:7401",
        "members": [
            {"node_id": 1, "addr": "10.8.0.1:7401", "voter": true, "matched_index": 12, "lag": 0},
            {"node_id": 2, "addr": "10.8.0.2:7401", "voter": true, "matched_index": 9, "lag": 3},
            {"node_id": 10, "addr": "10.8.0.10:7401", "voter": false, "matched_index": null, "lag": null}
        ]
    })
}

const STATUS_TEXT: &str = "\
joined:  yes
node_id: 2
role:    follower
term:    3
leader:  1@10.8.0.1:7401
members:
  node_id  name  addr            membership  matched_index  lag
  1        -     10.8.0.1:7401   voter       12             0
  2        -     10.8.0.2:7401   voter       9              3
  10       -     10.8.0.10:7401  learner     -              -
";

async fn mount_status(server: &MockServer, method_name: &str, route: &str, times: u64) {
    Mock::given(method(method_name))
        .and(path(route))
        .respond_with(ResponseTemplate::new(200).set_body_json(status_body()))
        .expect(times)
        .mount(server)
        .await;
}

// ---- health ----

#[tokio::test]
async fn health_text_and_json() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"status": "ok", "node_id": 7})),
        )
        .expect(2)
        .mount(&server)
        .await;

    let text = run_ok(&server, &["health"]).await;
    assert_eq!(text, "status:  ok\nnode_id: 7\n");

    let out = run_ok(&server, &["-o", "json", "health"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"status": "ok", "node_id": 7}));
    assert!(out.ends_with('\n'));
}

// ---- cluster status ----

#[tokio::test]
async fn cluster_status_text_shows_summary_and_members_table() {
    let server = MockServer::start().await;
    mount_status(&server, "GET", "/api/v1/cluster/status", 1).await;

    let text = run_ok(&server, &["cluster", "status"]).await;
    assert_eq!(text, STATUS_TEXT);
}

/// Members carry their mesh name when they have one; the rest show `-`.
#[tokio::test]
async fn cluster_status_text_shows_member_names() {
    let server = MockServer::start().await;
    let mut body = status_body();
    body["members"][0]["name"] = json!("node-1");
    body["members"][1]["name"] = json!("node-2");
    Mock::given(method("GET"))
        .and(path("/api/v1/cluster/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
        .expect(2)
        .mount(&server)
        .await;

    let text = run_ok(&server, &["cluster", "status"]).await;
    assert_eq!(
        text,
        "joined:  yes
node_id: 2
role:    follower
term:    3
leader:  1@10.8.0.1:7401
members:
  node_id  name    addr            membership  matched_index  lag
  1        node-1  10.8.0.1:7401   voter       12             0
  2        node-2  10.8.0.2:7401   voter       9              3
  10       -       10.8.0.10:7401  learner     -              -
"
    );

    let out = run_ok(&server, &["-o", "json", "cluster", "status"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, body);
}

#[tokio::test]
async fn cluster_status_text_of_unjoined_node() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/cluster/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "node_id": 4, "joined": false, "role": "learner", "term": 0,
            "leader_id": null, "leader_addr": null, "members": []
        })))
        .mount(&server)
        .await;

    let text = run_ok(&server, &["cluster", "status"]).await;
    assert_eq!(
        text,
        "\
joined:  no
node_id: 4
role:    learner
term:    0
leader:  -
members: -
"
    );
}

#[tokio::test]
async fn cluster_status_text_leader_without_addr() {
    let server = MockServer::start().await;
    let mut body = status_body();
    body["leader_addr"] = Value::Null;
    Mock::given(method("GET"))
        .and(path("/api/v1/cluster/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let text = run_ok(&server, &["cluster", "status"]).await;
    assert!(text.contains("leader:  1@-\n"), "{text}");
}

#[tokio::test]
async fn cluster_status_json_is_the_dto() {
    let server = MockServer::start().await;
    mount_status(&server, "GET", "/api/v1/cluster/status", 1).await;

    let out = run_ok(&server, &["-o", "json", "cluster", "status"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, status_body());
    assert!(out.ends_with('\n'));
}

// ---- cluster init ----

#[tokio::test]
async fn cluster_init_posts_and_prints_status() {
    let server = MockServer::start().await;
    mount_status(&server, "POST", "/api/v1/cluster/init", 2).await;

    let text = run_ok(&server, &["cluster", "init"]).await;
    assert_eq!(text, STATUS_TEXT);

    let out = run_ok(&server, &["-o", "json", "cluster", "init"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, status_body());
}

#[tokio::test]
async fn cluster_init_conflict_is_readable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/init"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": "already_initialized",
            "message": "cluster is already initialized"
        })))
        .mount(&server)
        .await;

    let (err, out) = run_err(&server, &["cluster", "init"]).await;
    assert_eq!(err, "already_initialized: cluster is already initialized");
    assert!(out.is_empty());
}

// ---- cluster join ----

#[tokio::test]
async fn cluster_join_posts_peer_and_prints_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/join"))
        .and(body_json(json!({"peer": "10.8.0.1:7401"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(status_body()))
        .expect(2)
        .mount(&server)
        .await;

    let text = run_ok(&server, &["cluster", "join", "10.8.0.1:7401"]).await;
    assert_eq!(text, STATUS_TEXT);

    let out = run_ok(&server, &["-o", "json", "cluster", "join", "10.8.0.1:7401"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, status_body());
}

#[tokio::test]
async fn cluster_join_sends_ipv6_peer_in_brackets() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/join"))
        .and(body_json(json!({"peer": "[fd00::1]:7401"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(status_body()))
        .expect(1)
        .mount(&server)
        .await;

    run_ok(&server, &["cluster", "join", "[fd00::1]:7401"]).await;
}

#[tokio::test]
async fn cluster_join_already_joined_is_readable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/join"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": "already_joined",
            "message": "this node has already joined a cluster",
            "leader_id": null, "leader_addr": null
        })))
        .mount(&server)
        .await;

    let (err, _) = run_err(&server, &["cluster", "join", "10.8.0.1:7401"]).await;
    assert_eq!(
        err,
        "already_joined: this node has already joined a cluster"
    );
}

#[tokio::test]
async fn cluster_join_not_leader_includes_leader_hint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cluster/join"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "error": "not_leader",
            "message": "node 1 at 10.8.0.1:7401 is the leader",
            "leader_id": 1,
            "leader_addr": "10.8.0.1:7401"
        })))
        .mount(&server)
        .await;

    let (err, _) = run_err(&server, &["cluster", "join", "10.8.0.2:7401"]).await;
    assert_eq!(
        err,
        "not_leader: node 1 at 10.8.0.1:7401 is the leader (leader: 1@10.8.0.1:7401)"
    );
}

// ---- errors ----

#[tokio::test]
async fn error_without_message_shows_code() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/cluster/status"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({"error": "no_leader"})))
        .mount(&server)
        .await;

    let (err, _) = run_err(&server, &["cluster", "status"]).await;
    assert_eq!(err, "no_leader");
}

/// `faber health` fails (and so exits non-zero) on a node whose raft core
/// has stopped, and says why.
#[tokio::test]
async fn health_of_an_unavailable_node_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "status": "unavailable", "node_id": 1,
            "error": "unavailable", "message": "the raft core is not running: raft stopped"
        })))
        .mount(&server)
        .await;

    for args in [&["health"][..], &["-o", "json", "health"][..]] {
        let (err, out) = run_err(&server, args).await;
        assert_eq!(
            err,
            "unavailable: the raft core is not running: raft stopped"
        );
        assert_eq!(out, "", "nothing on stdout");
    }
}

#[tokio::test]
async fn non_json_error_body_is_readable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let (err, _) = run_err(&server, &["health"]).await;
    assert_eq!(err, "internal: boom");
}

#[tokio::test]
async fn undecodable_success_body_names_the_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let (err, _) = run_err(&server, &["health"]).await;
    assert!(err.contains(&server.uri()), "{err}");
    assert!(err.contains("decode"), "{err}");
}

#[tokio::test]
async fn unreachable_endpoint_names_the_endpoint() {
    // Port 1 on loopback is essentially never listening.
    let cli = Cli::try_parse_from(["faber", "-e", "http://127.0.0.1:1", "health"]).unwrap();
    let mut out = Vec::new();
    let err = run(cli, &mut out).await.expect_err("should fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("http://127.0.0.1:1"), "{msg}");
}

// ---- mesh invite ----

async fn run_split(server: &MockServer, args: &[&str]) -> (String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    faber::run_with(cli(server, args), &mut out, &mut err)
        .await
        .unwrap_or_else(|e| panic!("run {args:?}: {e:#}"));
    (
        String::from_utf8(out).expect("utf8"),
        String::from_utf8(err).expect("utf8"),
    )
}

fn invite_body() -> Value {
    json!({
        "token": "faber1.abc_-", "node_id": 2, "mesh_ip": "10.99.0.2",
        "expires_at": 1700003600, "name": "node-2"
    })
}

#[tokio::test]
async fn mesh_invite_text_prints_only_the_token_on_stdout() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .and(body_json(
            json!({"name": "node-2", "endpoint": "198.51.100.2", "ttl_secs": 900}),
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(invite_body()))
        .expect(1)
        .mount(&server)
        .await;

    let (out, err) = run_split(
        &server,
        &[
            "mesh",
            "invite",
            "--name",
            "node-2",
            "--wg-endpoint",
            "198.51.100.2",
            "--ttl",
            "15m",
        ],
    )
    .await;
    assert_eq!(out, "faber1.abc_-\n");
    assert!(err.contains("node_id:"), "{err}");
    assert!(err.contains("10.99.0.2"), "{err}");
    assert!(err.contains("1700003600"), "{err}");
    assert!(!err.contains("faber1."), "{err}");
}

#[tokio::test]
async fn mesh_invite_without_ttl_leaves_it_to_the_server() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .and(body_json(
            json!({"name": "node-2", "endpoint": "198.51.100.2:51000"}),
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(invite_body()))
        .expect(1)
        .mount(&server)
        .await;

    let (out, _) = run_split(
        &server,
        &[
            "mesh",
            "invite",
            "--name",
            "node-2",
            "--wg-endpoint",
            "198.51.100.2:51000",
        ],
    )
    .await;
    assert_eq!(out, "faber1.abc_-\n");
}

#[tokio::test]
async fn mesh_invite_json_is_the_dto() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .respond_with(ResponseTemplate::new(201).set_body_json(invite_body()))
        .mount(&server)
        .await;

    let (out, err) = run_split(
        &server,
        &[
            "-o",
            "json",
            "mesh",
            "invite",
            "--name",
            "node-2",
            "--wg-endpoint",
            "198.51.100.2",
        ],
    )
    .await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, invite_body());
    assert!(out.ends_with('\n'));
    assert!(err.is_empty(), "{err}");
}

#[tokio::test]
async fn mesh_invite_errors_are_readable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": "mesh_no_endpoints",
            "message": "no mesh node has a WireGuard endpoint a new node could reach"
        })))
        .mount(&server)
        .await;

    let (err, out) = run_err(
        &server,
        &[
            "mesh",
            "invite",
            "--name",
            "node-2",
            "--wg-endpoint",
            "198.51.100.2",
        ],
    )
    .await;
    assert_eq!(
        err,
        "mesh_no_endpoints: no mesh node has a WireGuard endpoint a new node could reach"
    );
    assert!(out.is_empty());
}

// ---- mesh lease ----

#[tokio::test]
async fn mesh_lease_text_and_json() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/mesh/lease"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"lease_secs": 28_800})))
        .expect(2)
        .mount(&server)
        .await;

    let text = run_ok(&server, &["mesh", "lease"]).await;
    assert_eq!(text, "lease: 8h (28800s)\n");

    let out = run_ok(&server, &["-o", "json", "mesh", "lease"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"lease_secs": 28_800}));
    assert!(out.ends_with('\n'));
}

#[tokio::test]
async fn mesh_lease_set_puts_the_seconds_and_prints_the_new_lease() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/mesh/lease"))
        .and(body_json(json!({"lease_secs": 5400})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"lease_secs": 5400})))
        .expect(2)
        .mount(&server)
        .await;

    let text = run_ok(&server, &["mesh", "lease", "--set", "90m"]).await;
    assert_eq!(text, "lease: 1h30m (5400s)\n");

    let out = run_ok(&server, &["-o", "json", "mesh", "lease", "--set", "5400"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"lease_secs": 5400}));
    assert!(out.ends_with('\n'));
}

/// The 300 s to 30 d bounds are the server's: the CLI sends what it was
/// given and repeats the refusal as it came.
#[tokio::test]
async fn mesh_lease_set_out_of_bounds_shows_the_server_message() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/mesh/lease"))
        .and(body_json(json!({"lease_secs": 1})))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "bad_request",
            "message": "lease_secs must be between 300 and 2592000"
        })))
        .expect(2)
        .mount(&server)
        .await;

    for args in [
        &["mesh", "lease", "--set", "1s"][..],
        &["-o", "json", "mesh", "lease", "--set", "1s"][..],
    ] {
        let (err, out) = run_err(&server, args).await;
        assert_eq!(
            err,
            "bad_request: lease_secs must be between 300 and 2592000"
        );
        assert!(out.is_empty());
    }
}

#[tokio::test]
async fn mesh_lease_errors_are_readable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/mesh/lease"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({"error": "no_leader"})))
        .mount(&server)
        .await;

    let (err, out) = run_err(&server, &["mesh", "lease"]).await;
    assert_eq!(err, "no_leader");
    assert!(out.is_empty());
}

const INVITE_ARGS: [&str; 6] = [
    "mesh",
    "invite",
    "--name",
    "node-2",
    "--wg-endpoint",
    "198.51.100.2",
];

const INVITE_DETAILS: &str = "\
name:       node-2
node_id:    2
mesh_ip:    10.99.0.2
expires_at: 1700003600 (unix seconds)
";

async fn mount_invite(server: &MockServer, body: Value) {
    Mock::given(method("POST"))
        .and(path("/api/v1/mesh/invites"))
        .respond_with(ResponseTemplate::new(201).set_body_json(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn mesh_invite_text_details_name_the_node_and_carry_no_warning() {
    let server = MockServer::start().await;
    mount_invite(&server, invite_body()).await;

    let (out, err) = run_split(&server, &INVITE_ARGS).await;
    assert_eq!(out, "faber1.abc_-\n");
    assert_eq!(err, INVITE_DETAILS);
}

/// An invite for a name a live node holds takes that node over with no extra
/// flag, so text mode has to say so.
#[tokio::test]
async fn mesh_invite_text_warns_when_the_invite_replaces_a_node() {
    let server = MockServer::start().await;
    let mut body = invite_body();
    body["replaces_node_id"] = json!(7);
    mount_invite(&server, body).await;

    let (out, err) = run_split(&server, &INVITE_ARGS).await;
    assert_eq!(out, "faber1.abc_-\n");
    assert_eq!(
        err,
        format!(
            "{INVITE_DETAILS}WARNING: this invite replaces node 7 (node-2). \
             When the new machine joins, that node is removed from the cluster.\n"
        )
    );
}

#[tokio::test]
async fn mesh_invite_json_of_a_replacement_is_the_dto() {
    let server = MockServer::start().await;
    let mut body = invite_body();
    body["replaces_node_id"] = json!(7);
    mount_invite(&server, body.clone()).await;

    let mut args = vec!["-o", "json"];
    args.extend_from_slice(&INVITE_ARGS);
    let (out, err) = run_split(&server, &args).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, body);
    assert!(err.is_empty(), "{err}");
}

// ---- helpers for project, app and deploy ----

/// What one run wrote and how it ended.
struct Outcome {
    out: String,
    err: String,
    /// The error the run failed with, as `main` prints it.
    error: Option<String>,
}

impl Outcome {
    #[track_caller]
    fn ok(self) -> (String, String) {
        if let Some(e) = &self.error {
            panic!("run failed: {e}\nstderr:\n{}", self.err);
        }
        (self.out, self.err)
    }

    #[track_caller]
    fn failed(&self) -> &str {
        self.error.as_deref().expect("expected the run to fail")
    }
}

async fn run_inputs(server: &MockServer, args: &[&str], inputs: Inputs) -> Outcome {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let result = faber::run_with_inputs(cli(server, args), &mut out, &mut err, inputs).await;
    Outcome {
        out: String::from_utf8(out).expect("utf8"),
        err: String::from_utf8(err).expect("utf8"),
        error: result.err().map(|e| format!("{e:#}")),
    }
}

/// Runs with `cwd` as the current directory, `stdin` on stdin and no
/// variables.
async fn run_in(server: &MockServer, args: &[&str], cwd: &Path, stdin: &str) -> Outcome {
    let inputs = Inputs {
        cwd: cwd.to_path_buf(),
        variables: BTreeMap::new(),
        stdin: Box::new(Cursor::new(stdin.as_bytes().to_vec())),
    };
    run_inputs(server, args, inputs).await
}

/// Runs in the fixtures directory.
async fn run_fx(server: &MockServer, args: &[&str]) -> Outcome {
    run_in(server, args, &fixtures(), "").await
}

/// A path as the compose converter names it: with `/` on every platform.
fn slashes(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// The real path of `path` as the list of files read shows it: symlinks
/// resolved, and without the prefix Windows puts in front.
fn real(path: &Path) -> String {
    let real = std::fs::canonicalize(path).unwrap().display().to_string();
    real.strip_prefix(r"\\?\").map(String::from).unwrap_or(real)
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A fixture as the CLI sends it: every default written out.
fn fixture_workloads(name: &str) -> Value {
    let text = std::fs::read_to_string(fixtures().join(name)).unwrap();
    let workloads: BTreeMap<String, WorkloadSpec> = serde_json::from_str(&text).unwrap();
    serde_json::to_value(workloads).unwrap()
}

/// A server that fails the test when it gets any request.
async fn silent_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    server
}

async fn mount_json(server: &MockServer, verb: &str, route: &str, status: u16, body: Value) {
    Mock::given(method(verb))
        .and(path(route))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(server)
        .await;
}

/// The body of the only request the server got.
async fn only_request_body(server: &MockServer) -> Value {
    let requests = server.received_requests().await.expect("recording");
    assert_eq!(requests.len(), 1, "{requests:?}");
    requests[0].body_json().expect("json body")
}

const TARGET: [&str; 4] = ["--project", "shop", "--env", "staging"];

/// `faber <head...> --project shop --env staging <tail...>`
fn with_target<'a>(head: &[&'a str], tail: &[&'a str]) -> Vec<&'a str> {
    [head, &TARGET, tail].concat()
}

const NOTE_CAPABILITIES: &str = "note: containers get no Linux capabilities unless the file adds \
    them; images that expect Docker's default set may fail to start\n";

const NOTE_SECRETS: &str = "note: an env variable name looks like a secret; env values are \
    stored unencrypted in cluster state and are readable by local users on every node\n";

const WARNING_HOST_PATH: &str = "warning: host-path.json: web.volumes.data: host path /srv/data: \
    host paths are refused by nodes unless allowed in the node's host-path allow-list\n";

// ---- project ----

fn project_body() -> Value {
    json!({
        "name": "shop", "created_at": 1_700_000_000,
        "environments": [
            {"name": "production", "generation": 3, "workloads": 0, "bytes": 2,
             "updated_at": 1_700_000_000},
            {"name": "staging", "generation": 7, "workloads": 2, "bytes": 512,
             "updated_at": 1_700_003_600},
        ],
    })
}

const PROJECT_TEXT: &str = "\
name:    shop
created: 2023-11-14 22:13:20Z
environments:
  ENVIRONMENT  WORKLOADS  GENERATION  UPDATED
  production   0          3           2023-11-14 22:13:20Z
  staging      2          7           2023-11-14 23:13:20Z
";

/// Without `--env` the request names no environments, so the server picks
/// `staging` and `production`.
#[tokio::test]
async fn project_create_without_env_leaves_the_environments_to_the_server() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/projects"))
        .and(body_json(json!({"name": "shop"})))
        .respond_with(ResponseTemplate::new(201).set_body_json(project_body()))
        .expect(2)
        .mount(&server)
        .await;

    let (out, err) = run_split(&server, &["project", "create", "shop"]).await;
    assert_eq!(out, PROJECT_TEXT);
    assert_eq!(err, "");

    let out = run_ok(&server, &["-o", "json", "project", "create", "shop"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, project_body());
    assert!(out.ends_with('\n'));
}

#[tokio::test]
async fn project_create_sends_the_environments_it_was_given() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/projects"))
        .and(body_json(
            json!({"name": "shop", "environments": ["dev", "prod"]}),
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(project_body()))
        .expect(1)
        .mount(&server)
        .await;

    let args = ["project", "create", "shop", "--env", "dev", "--env", "prod"];
    assert_eq!(run_ok(&server, &args).await, PROJECT_TEXT);
}

#[tokio::test]
async fn project_ls_text_and_json() {
    let server = MockServer::start().await;
    let body = json!({"projects": [
        {"name": "blog", "created_at": 0, "environments": []},
        project_body(),
    ]});
    Mock::given(method("GET"))
        .and(path("/api/v1/projects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
        .expect(2)
        .mount(&server)
        .await;

    let text = run_ok(&server, &["project", "ls"]).await;
    assert_eq!(
        text,
        "\
NAME  ENVIRONMENTS         WORKLOADS  CREATED
blog  -                    0          -
shop  production, staging  2          2023-11-14 22:13:20Z
"
    );

    let out = run_ok(&server, &["-o", "json", "project", "ls"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, body);
}

#[tokio::test]
async fn project_ls_without_projects_prints_the_header() {
    let server = MockServer::start().await;
    mount_json(
        &server,
        "GET",
        "/api/v1/projects",
        200,
        json!({"projects": []}),
    )
    .await;

    let text = run_ok(&server, &["project", "ls"]).await;
    assert_eq!(text, "NAME  ENVIRONMENTS  WORKLOADS  CREATED\n");
}

#[tokio::test]
async fn project_show_text_and_json() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/projects/shop"))
        .respond_with(ResponseTemplate::new(200).set_body_json(project_body()))
        .expect(2)
        .mount(&server)
        .await;

    assert_eq!(
        run_ok(&server, &["project", "show", "shop"]).await,
        PROJECT_TEXT
    );

    let out = run_ok(&server, &["-o", "json", "project", "show", "shop"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, project_body());
}

#[tokio::test]
async fn project_show_of_a_project_without_environments() {
    let server = MockServer::start().await;
    let body = json!({"name": "blog", "created_at": 86_400, "environments": []});
    mount_json(&server, "GET", "/api/v1/projects/blog", 200, body).await;

    assert_eq!(
        run_ok(&server, &["project", "show", "blog"]).await,
        "\
name:         blog
created:      1970-01-02 00:00:00Z
environments: -
"
    );
}

#[tokio::test]
async fn project_rm_deletes_and_says_so() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/projects/shop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&server)
        .await;

    assert_eq!(
        run_ok(&server, &["project", "rm", "shop"]).await,
        "removed project shop\n"
    );

    let out = run_ok(&server, &["-o", "json", "project", "rm", "shop"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"removed": "shop"}));
}

#[tokio::test]
async fn project_env_add_puts_and_prints_the_project() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/projects/shop/environments/staging"))
        .respond_with(ResponseTemplate::new(200).set_body_json(project_body()))
        .expect(2)
        .mount(&server)
        .await;

    let args = ["project", "env", "add", "shop", "staging"];
    assert_eq!(run_ok(&server, &args).await, PROJECT_TEXT);

    let out = run_ok(&server, &[&["-o", "json"][..], &args].concat()).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, project_body());
}

#[tokio::test]
async fn project_env_rm_deletes_and_says_so() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/projects/shop/environments/dev"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&server)
        .await;

    let args = ["project", "env", "rm", "shop", "dev"];
    assert_eq!(
        run_ok(&server, &args).await,
        "removed environment dev from shop\n"
    );

    let out = run_ok(&server, &[&["-o", "json"][..], &args].concat()).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"project": "shop", "removed": "dev"}));
}

#[tokio::test]
async fn project_errors_are_readable() {
    let server = MockServer::start().await;
    let error = |code: &str, message: &str| json!({"error": code, "message": message});
    mount_json(
        &server,
        "GET",
        "/api/v1/projects/shop",
        404,
        error("not_found", "no project named shop"),
    )
    .await;
    mount_json(
        &server,
        "POST",
        "/api/v1/projects",
        409,
        error("project_exists", "project shop already exists"),
    )
    .await;
    mount_json(
        &server,
        "DELETE",
        "/api/v1/projects/shop",
        409,
        error("not_empty", "environment staging holds 2 workloads"),
    )
    .await;
    mount_json(
        &server,
        "PUT",
        "/api/v1/projects/shop/environments/dev",
        400,
        error("bad_request", "a project holds at most 16 environments"),
    )
    .await;
    mount_json(
        &server,
        "DELETE",
        "/api/v1/projects/shop/environments/dev",
        404,
        error("not_found", "project shop has no environment dev"),
    )
    .await;

    for (args, want) in [
        (
            &["project", "show", "shop"][..],
            "not_found: no project named shop",
        ),
        (
            &["project", "create", "shop"],
            "project_exists: project shop already exists",
        ),
        (
            &["project", "rm", "shop"],
            "not_empty: environment staging holds 2 workloads",
        ),
        (
            &["project", "env", "add", "shop", "dev"],
            "bad_request: a project holds at most 16 environments",
        ),
        (
            &["project", "env", "rm", "shop", "dev"],
            "not_found: project shop has no environment dev",
        ),
    ] {
        for format in ["text", "json"] {
            let (err, out) = run_err(&server, &[&["-o", format][..], args].concat()).await;
            assert_eq!(err, want, "{args:?}");
            assert_eq!(out, "", "{args:?}");
        }
    }
}

/// A name that is not a DNS label never reaches the server, so it cannot
/// change the path of the request either.
#[tokio::test]
async fn a_name_that_is_not_a_dns_label_is_refused_before_any_request() {
    let server = silent_server().await;
    for args in [
        &["project", "show", "Shop"][..],
        &["project", "rm", "../health"],
        &["project", "env", "add", "shop", "a/b"],
        &["project", "env", "rm", "shop", ""],
        &[
            "app",
            "show",
            "web_1",
            "--project",
            "shop",
            "--env",
            "staging",
        ],
        &["app", "rm", "web", "--project", "shop", "--env", "Staging"],
        &[
            "deploy",
            "spec",
            "spec.json",
            "--project",
            "sh.op",
            "--env",
            "staging",
        ],
    ] {
        let outcome = run_fx(&server, args).await;
        let error = outcome.failed();
        assert!(error.starts_with("invalid name: "), "{args:?}: {error}");
        assert_eq!(outcome.out, "", "{args:?}");
    }
}

// ---- app ls ----

fn workload_list_body() -> Value {
    let summary = |name: &str, scale: Value, expose: &str, revision: u64| {
        json!({
            "project": "shop", "environment": "staging", "name": name,
            "fqdn": format!("{name}.staging.shop.faber.internal"),
            "scale": scale, "expose": expose, "revision": revision,
        })
    };
    json!({"workloads": [
        summary("db", json!({"replicas": 1}), "environment", 4),
        summary("logs", json!("per_node"), "environment", 5),
        summary("web", json!({"replicas": 3}), "cluster", 7),
    ]})
}

#[tokio::test]
async fn app_ls_text_and_json() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workloads"))
        .and(query_param("project", "shop"))
        .and(query_param("environment", "staging"))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_list_body()))
        .expect(2)
        .mount(&server)
        .await;

    let args = ["app", "ls", "--project", "shop", "--env", "staging"];
    assert_eq!(
        run_ok(&server, &args).await,
        "\
NAME  PROJECT  ENVIRONMENT  SCALE     EXPOSE       FQDN
db    shop     staging      1         environment  db.staging.shop.faber.internal
logs  shop     staging      per-node  environment  logs.staging.shop.faber.internal
web   shop     staging      3         cluster      web.staging.shop.faber.internal
"
    );

    let out = run_ok(&server, &[&["-o", "json"][..], &args].concat()).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, workload_list_body());
}

#[tokio::test]
async fn app_ls_sends_only_the_filters_it_was_given() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workloads"))
        .and(query_param_is_missing("project"))
        .and(query_param_is_missing("environment"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"workloads": []})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workloads"))
        .and(query_param_is_missing("project"))
        .and(query_param("environment", "staging"))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_list_body()))
        .expect(1)
        .mount(&server)
        .await;

    assert_eq!(
        run_ok(&server, &["app", "ls"]).await,
        "NAME  PROJECT  ENVIRONMENT  SCALE  EXPOSE  FQDN\n"
    );
    let text = run_ok(&server, &["app", "ls", "--env", "staging"]).await;
    assert_eq!(text.lines().count(), 4, "{text}");
}

// ---- app show ----

const WORKLOAD_ROUTE: &str = "/api/v1/projects/shop/environments/staging/workloads/web";

/// A workload that uses every part of the model.
fn workload_body() -> Value {
    let timing = |action: Value, delay: u32| {
        json!({
            "action": action, "initial_delay_secs": delay, "period_secs": 10,
            "timeout_secs": 1, "success_threshold": 1, "failure_threshold": 3,
        })
    };
    json!({
        "project": "shop", "environment": "staging", "name": "web",
        "fqdn": "web.staging.shop.faber.internal", "revision": 7, "generation": 9,
        "spec": {
            "scale": "per_node",
            "restart": "on_failure",
            "expose": "cluster",
            "placement": {"node": "node-1"},
            "stop_grace_secs": 30,
            "volumes": {
                "data": {"type": "named", "name": "data", "per_replica": true},
                "host": {"type": "host_path", "path": "/srv/host", "create": false},
                "tmp": {"type": "ephemeral", "memory": true, "size_bytes": 1024},
                // Mounted by no container.
                "spare": {"type": "ephemeral", "memory": false},
            },
            "containers": [
                {
                    "name": "web",
                    "image": "nginx:1.27",
                    "command": ["nginx"],
                    "args": ["-g", "daemon off;"],
                    "env": {"API_KEY": "hunter2", "MODE": "prod"},
                    "working_dir": "/srv",
                    "user": "101:101",
                    "ports": [
                        {"name": "http", "port": 8080, "protocol": "tcp"},
                        {"port": 53, "protocol": "udp"},
                    ],
                    "mounts": [
                        {"volume": "data", "path": "/data", "read_only": true,
                         "sub_path": "web"},
                        {"volume": "host", "path": "/host", "read_only": false},
                        {"volume": "tmp", "path": "/tmp", "read_only": false},
                    ],
                    "resources": {
                        "limits": {"cpu_millis": 500, "memory_bytes": 268_435_456},
                        "requests": {"cpu_millis": 100},
                    },
                    "capabilities": ["NET_BIND_SERVICE", "CHOWN"],
                    "read_only_root_fs": true,
                    "liveness": timing(json!({"type": "tcp", "port": 8080}), 0),
                    "readiness": timing(json!({
                        "type": "http", "port": 8080, "path": "/ready", "scheme": "https",
                        "host": "web.local", "headers": {"x-probe": "s3cret"},
                    }), 5),
                    "startup": timing(json!({"type": "exec", "command": ["true"]}), 0),
                },
                {
                    "name": "sidecar",
                    "image": "busybox",
                    "command": [],
                    "args": [],
                    "env": {},
                    "ports": [],
                    "mounts": [],
                    "resources": {"limits": {}, "requests": {}},
                    "capabilities": [],
                    "read_only_root_fs": false,
                },
            ],
        },
    })
}

const WORKLOAD_TEXT: &str = "\
name:        web
fqdn:        web.staging.shop.faber.internal
project:     shop
environment: staging
revision:    7
generation:  9
scale:       per-node
restart:     on-failure
expose:      cluster
node:        node-1
stop_grace:  30s
volumes:
  data:  named data, per-replica
  host:  host path /srv/host
  spare: ephemeral
  tmp:   ephemeral, memory, 1024 bytes
containers:
  web:
    image:             nginx:1.27
    command:           nginx
    args:              -g \"daemon off;\"
    working_dir:       /srv
    user:              101:101
    env:               API_KEY=***
                       MODE=***
    ports:             http:8080/tcp
                       53/udp
    mounts:            data:/data (named data, per-replica, read-only, sub-path web)
                       host:/host (host path /srv/host)
                       tmp:/tmp (ephemeral, memory, 1024 bytes)
    resources:         limits: cpu 500m, memory 268435456 bytes
                       requests: cpu 100m
    capabilities:      NET_BIND_SERVICE, CHOWN
    read_only_root_fs: yes
    liveness:          tcp :8080 (delay 0s, period 10s, timeout 1s, success 1, failure 3)
    readiness:         https :8080/ready, host web.local, headers x-probe (delay 5s, period \
10s, timeout 1s, success 1, failure 3)
    startup:           exec true (delay 0s, period 10s, timeout 1s, success 1, failure 3)
  sidecar:
    image:             busybox
    command:           -
    args:              -
    working_dir:       (image default)
    user:              (image default)
    env:               -
    ports:             -
    mounts:            -
    resources:         -
    capabilities:      -
    read_only_root_fs: no
    liveness:          -
    readiness:         -
    startup:           -
";

#[tokio::test]
async fn app_show_text_masks_env_values() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(WORKLOAD_ROUTE))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_body()))
        .expect(1)
        .mount(&server)
        .await;

    let (out, err) = run_split(&server, &with_target(&["app", "show", "web"], &[])).await;
    assert_eq!(out, WORKLOAD_TEXT);
    assert_eq!(err, "");
    // Neither the env values nor the value of a probe header.
    assert!(!out.contains("hunter2") && !out.contains("s3cret"), "{out}");
}

#[tokio::test]
async fn app_show_with_show_env_prints_the_values() {
    let server = MockServer::start().await;
    mount_json(&server, "GET", WORKLOAD_ROUTE, 200, workload_body()).await;

    let args = with_target(&["app", "show", "web"], &["--show-env"]);
    let out = run_ok(&server, &args).await;
    let want = WORKLOAD_TEXT
        .replace("API_KEY=***", "API_KEY=hunter2")
        .replace("MODE=***", "MODE=prod");
    assert_eq!(out, want);
}

/// JSON is for programs: it is the response as it came, values included,
/// with or without `--show-env`.
#[tokio::test]
async fn app_show_json_is_the_dto_unmasked() {
    let server = MockServer::start().await;
    mount_json(&server, "GET", WORKLOAD_ROUTE, 200, workload_body()).await;

    for tail in [&[][..], &["--show-env"]] {
        let args = with_target(&["-o", "json", "app", "show", "web"], tail);
        let out = run_ok(&server, &args).await;
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v, workload_body());
    }
}

#[tokio::test]
async fn app_show_of_an_unknown_workload_is_readable() {
    let server = MockServer::start().await;
    let body = json!({"error": "not_found", "message": "no workload named web"});
    mount_json(&server, "GET", WORKLOAD_ROUTE, 404, body).await;

    let (err, out) = run_err(&server, &with_target(&["app", "show", "web"], &[])).await;
    assert_eq!(err, "not_found: no workload named web");
    assert_eq!(out, "");
}

// ---- control characters ----

/// Whatever is in cluster state, text output cannot carry a terminal
/// sequence or break out of its line.
#[tokio::test]
async fn text_output_escapes_control_characters_from_the_api() {
    let server = MockServer::start().await;
    let mut workload = workload_body();
    workload["fqdn"] = json!("web\u{1b}[2J.faber.internal");
    workload["spec"]["placement"]["node"] = json!("node\r\n1");
    let web = &mut workload["spec"]["containers"][0];
    web["image"] = json!("nginx\u{1b}]0;owned\u{7}");
    web["args"] = json!(["a\tb", "c\u{9b}d"]);
    web["env"] = json!({"MODE": "prod\nname: fake", "NA\u{0}ME": "x"});
    web["ports"][0]["name"] = json!("ht\u{7f}tp");
    web["mounts"][0]["path"] = json!("/da\u{1b}ta");
    web["capabilities"] = json!(["NET\u{1b}_ADMIN"]);
    web["readiness"]["action"]["path"] = json!("/rea\u{1b}dy");
    mount_json(&server, "GET", WORKLOAD_ROUTE, 200, workload).await;

    let mut list = workload_list_body();
    list["workloads"][0]["name"] = json!("d\u{1b}[31mb");
    list["workloads"][0]["project"] = json!("sh\nop");
    list["workloads"][0]["environment"] = json!("stag\u{8}ing");
    list["workloads"][0]["fqdn"] = json!("db\u{1b}c");
    mount_json(&server, "GET", "/api/v1/workloads", 200, list).await;

    let mut project = project_body();
    project["name"] = json!("sh\u{1b}op");
    project["environments"][0]["name"] = json!("prod\nuction");
    mount_json(
        &server,
        "GET",
        "/api/v1/projects/shop",
        200,
        project.clone(),
    )
    .await;
    mount_json(
        &server,
        "GET",
        "/api/v1/projects",
        200,
        json!({"projects": [project]}),
    )
    .await;

    mount_json(
        &server,
        "POST",
        "/api/v1/projects/shop/environments/staging/deploy",
        200,
        json!({"generation": 9, "created": ["we\u{1b}b"], "updated": [],
               "unchanged": ["d\nb"], "removed": []}),
    )
    .await;

    let show = with_target(&["app", "show", "web", "--show-env"], &[]);
    let deploy = with_target(&["deploy", "spec", "spec.json"], &[]);
    for args in [
        &show[..],
        &["app", "ls"],
        &["project", "show", "shop"],
        &["project", "ls"],
        &deploy[..],
    ] {
        let (out, err) = run_fx(&server, args).await.ok();
        for text in [&out, &err] {
            let control = text.chars().find(|c| c.is_control() && *c != '\n');
            assert_eq!(control, None, "{args:?}:\n{text}");
        }
    }

    let (out, _) = run_fx(&server, &show).await.ok();
    for want in [
        "fqdn:        web\\x1b[2J.faber.internal\n",
        "node:        node\\x0d\\x0a1\n",
        "image:             nginx\\x1b]0;owned\\x07\n",
        "args:              a\\x09b c\\u{9b}d\n",
        "MODE=prod\\x0aname: fake\n",
        "NA\\x00ME=x\n",
        "ht\\x7ftp:8080/tcp\n",
        "data:/da\\x1bta (",
        "capabilities:      NET\\x1b_ADMIN\n",
        "https :8080/rea\\x1bdy,",
    ] {
        assert!(out.contains(want), "missing {want:?} in:\n{out}");
    }

    let (out, _) = run_fx(&server, &["app", "ls"]).await.ok();
    assert!(
        out.contains("d\\x1b[31mb  sh\\x0aop  stag\\x08ing  1         environment  db\\x1bc\n"),
        "{out}"
    );

    let (out, _) = run_fx(&server, &["project", "ls"]).await.ok();
    assert!(
        out.contains("sh\\x1bop  prod\\x0auction, staging  2"),
        "{out}"
    );

    let (out, _) = run_fx(&server, &deploy).await.ok();
    assert!(out.contains("created:     we\\x1bb\n"), "{out}");
    assert!(out.contains("unchanged:   d\\x0ab\n"), "{out}");
}

#[tokio::test]
async fn an_error_from_the_api_is_escaped_too() {
    let server = MockServer::start().await;
    let body = json!({"error": "not_found", "message": "no project named sh\u{1b}[2Jop\r\n"});
    mount_json(&server, "GET", "/api/v1/projects/shop", 404, body).await;

    let (err, _) = run_err(&server, &["project", "show", "shop"]).await;
    assert_eq!(err, "not_found: no project named sh\\x1b[2Jop\\x0d\\x0a");
}

// ---- app rm ----

fn workload_route(name: &str) -> String {
    format!("/api/v1/projects/shop/environments/staging/workloads/{name}")
}

#[tokio::test]
async fn app_rm_deletes_each_workload_and_prints_its_fqdn() {
    let server = MockServer::start().await;
    for name in ["web", "db"] {
        Mock::given(method("DELETE"))
            .and(path(workload_route(name)))
            .respond_with(ResponseTemplate::new(204))
            .expect(2)
            .mount(&server)
            .await;
    }

    let args = with_target(&["app", "rm", "web", "db"], &[]);
    let (out, err) = run_split(&server, &args).await;
    assert_eq!(
        out,
        "removed web.staging.shop.faber.internal\nremoved db.staging.shop.faber.internal\n"
    );
    assert_eq!(err, "");

    let out = run_ok(&server, &[&["-o", "json"][..], &args].concat()).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v,
        json!({"removed": [
            "web.staging.shop.faber.internal",
            "db.staging.shop.faber.internal",
        ]})
    );
}

/// The workloads removed before the failure are reported, and the ones
/// after it are not touched.
#[tokio::test]
async fn app_rm_stops_at_the_first_failure() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(workload_route("web")))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(workload_route("gone")))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"error": "not_found", "message": "no workload named gone"})),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(workload_route("db")))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    let args = with_target(&["app", "rm", "web", "gone", "db"], &[]);
    let (err, out) = run_err(&server, &args).await;
    assert_eq!(err, "not_found: no workload named gone");
    assert_eq!(out, "removed web.staging.shop.faber.internal\n");

    let (err, out) = run_err(&server, &[&["-o", "json"][..], &args].concat()).await;
    assert_eq!(err, "not_found: no workload named gone");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"removed": ["web.staging.shop.faber.internal"]}));
}

#[tokio::test]
async fn app_rm_that_removes_nothing_prints_nothing() {
    let server = MockServer::start().await;
    let body = json!({"error": "not_found", "message": "no workload named gone"});
    mount_json(&server, "DELETE", &workload_route("gone"), 404, body).await;

    for format in ["text", "json"] {
        let args = with_target(&["-o", format, "app", "rm", "gone"], &[]);
        let (err, out) = run_err(&server, &args).await;
        assert_eq!(err, "not_found: no workload named gone");
        assert_eq!(out, "");
    }
}

// ---- deploy spec ----

const DEPLOY_ROUTE: &str = "/api/v1/projects/shop/environments/staging/deploy";

fn deploy_body() -> Value {
    json!({
        "generation": 9, "created": ["web"], "updated": [], "unchanged": ["db"],
        "removed": ["old", "older"],
    })
}

const DEPLOY_TEXT: &str = "\
project:     shop
environment: staging
generation:  9
created:     web
updated:     -
unchanged:   db
removed:     old, older
";

async fn mount_deploy(server: &MockServer, workloads: Value, prune: bool, times: u64) {
    Mock::given(method("POST"))
        .and(path(DEPLOY_ROUTE))
        .and(body_json(json!({"workloads": workloads, "prune": prune})))
        .respond_with(ResponseTemplate::new(200).set_body_json(deploy_body()))
        .expect(times)
        .mount(server)
        .await;
}

#[tokio::test]
async fn deploy_spec_text_puts_the_result_on_stdout_and_notes_on_stderr() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("spec.json"), false, 1).await;

    let args = with_target(&["deploy", "spec", "spec.json"], &[]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(out, DEPLOY_TEXT);
    assert_eq!(err, NOTE_CAPABILITIES);
}

#[tokio::test]
async fn deploy_spec_json_is_one_document_and_nothing_on_stderr() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("spec.json"), false, 1).await;
    mount_deploy(&server, fixture_workloads("host-path.json"), false, 1).await;

    let args = with_target(&["-o", "json", "deploy", "spec", "spec.json"], &[]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, "");
    assert!(out.ends_with('\n'));
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v,
        json!({
            "result": deploy_body(),
            "warnings": [],
            "notes": [NOTE_CAPABILITIES.trim_start_matches("note: ").trim_end()],
            "variables": [],
        })
    );

    let args = with_target(&["-o", "json", "deploy", "spec", "host-path.json"], &[]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v,
        json!({
            "result": deploy_body(),
            "warnings": [{
                "source": "host-path.json",
                "path": "web.volumes.data",
                "message": "host path /srv/data: host paths are refused by nodes unless \
                            allowed in the node's host-path allow-list",
            }],
            "notes": [NOTE_SECRETS.trim_start_matches("note: ").trim_end()],
            "variables": [],
        })
    );
}

#[tokio::test]
async fn deploy_prune_is_sent_only_when_asked_for() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("spec.json"), true, 1).await;

    let args = with_target(&["deploy", "spec", "spec.json"], &["--prune"]);
    let (out, _) = run_fx(&server, &args).await.ok();
    assert_eq!(out, DEPLOY_TEXT);
}

#[tokio::test]
async fn deploy_spec_reads_stdin_for_a_dash() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("spec.json"), false, 1).await;

    let text = std::fs::read_to_string(fixtures().join("spec.json")).unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let args = with_target(&["deploy", "spec", "-"], &[]);
    let (out, _) = run_in(&server, &args, elsewhere.path(), &text).await.ok();
    assert_eq!(out, DEPLOY_TEXT);
}

#[tokio::test]
async fn deploy_dry_run_prints_the_spec_and_sends_nothing() {
    let server = silent_server().await;

    let args = with_target(&["deploy", "spec", "spec.json"], &["--dry-run", "--prune"]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, format!("read: spec.json\n{NOTE_CAPABILITIES}"));
    assert!(out.starts_with("{\n  \"db\": {\n"), "{out}");
    assert!(out.ends_with("}\n"), "{out}");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, fixture_workloads("spec.json"));

    let args = with_target(
        &["-o", "json", "deploy", "spec", "host-path.json"],
        &["--dry-run"],
    );
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v,
        json!({
            "project": "shop",
            "environment": "staging",
            "spec": fixture_workloads("host-path.json"),
            "warnings": [{
                "source": "host-path.json",
                "path": "web.volumes.data",
                "message": "host path /srv/data: host paths are refused by nodes unless \
                            allowed in the node's host-path allow-list",
            }],
            "notes": [NOTE_SECRETS.trim_start_matches("note: ").trim_end()],
            "variables": [],
            "files": ["host-path.json"],
        })
    );
}

/// `--dry-run` lists every file the deploy read, so the caller sees what
/// of this machine would go into cluster state: after the warnings in text
/// mode, in `files` in JSON. A deploy that is sent lists nothing.
#[tokio::test]
async fn deploy_spec_dry_run_lists_the_file_it_read() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("host-path.json"), false, 1).await;

    let args = with_target(&["deploy", "spec", "host-path.json"], &["--dry-run"]);
    let (_, err) = run_fx(&server, &args).await.ok();
    assert_eq!(
        err,
        format!("{WARNING_HOST_PATH}read: host-path.json\n{NOTE_SECRETS}")
    );

    // An absolute path below the current directory is shown relative.
    let absolute = fixtures().join("host-path.json");
    let args = with_target(
        &["deploy", "spec", absolute.to_str().unwrap()],
        &["--dry-run"],
    );
    let (_, err) = run_fx(&server, &args).await.ok();
    assert!(err.contains("\nread: host-path.json\n"), "{err}");

    // Stdin is `-`, and a path that is not below it stays as it is.
    let text = std::fs::read_to_string(&absolute).unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let args = with_target(&["-o", "json", "deploy", "spec", "-"], &["--dry-run"]);
    let (out, _) = run_in(&server, &args, elsewhere.path(), &text).await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["files"], json!(["-"]));
    let args = with_target(
        &["-o", "json", "deploy", "spec", absolute.to_str().unwrap()],
        &["--dry-run"],
    );
    let (out, _) = run_in(&server, &args, elsewhere.path(), "").await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["files"], json!([real(&absolute)]));

    // Without --dry-run: no list, in either format.
    let args = with_target(&["deploy", "spec", "host-path.json"], &[]);
    let (_, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, format!("{WARNING_HOST_PATH}{NOTE_SECRETS}"));
}

#[tokio::test]
async fn deploy_json_of_a_sent_deploy_has_no_file_list() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("spec.json"), false, 1).await;

    let args = with_target(&["-o", "json", "deploy", "spec", "spec.json"], &[]);
    let (out, _) = run_fx(&server, &args).await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v.get("files"), None, "{out}");
}

/// The compose file, the `.env` beside it and the `env_file` of a service,
/// in the order they were read and each once, also when two services name
/// the same `env_file`.
#[tokio::test]
async fn deploy_compose_dry_run_lists_the_env_files_it_read() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("env")).unwrap();
    std::fs::write(
        dir.path().join("compose.yaml"),
        "services:\n  web:\n    image: nginx:${TAG}\n    env_file: env/web.env\n  api:\n    \
         image: nginx:${TAG}\n    env_file:\n      - env/web.env\n",
    )
    .unwrap();
    std::fs::write(dir.path().join(".env"), "TAG=1.27\n").unwrap();
    std::fs::write(dir.path().join("env").join("web.env"), "MODE=prod\n").unwrap();

    // The real path, as this platform writes it.
    let web_env = Path::new("env").join("web.env").display().to_string();
    let args = with_target(&["deploy", "compose"], &["--dry-run"]);
    let (out, err) = run_in(&server, &args, dir.path(), "").await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["web"]["containers"][0]["env"]["MODE"], "prod");
    assert_eq!(
        err,
        format!(
            "uses variable: TAG\nread: compose.yaml\nread: .env\nread: {web_env}\n\
             {NOTE_CAPABILITIES}"
        )
    );

    let args = with_target(&["-o", "json", "deploy", "compose"], &["--dry-run"]);
    let (out, err) = run_in(&server, &args, dir.path(), "").await.ok();
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["files"], json!(["compose.yaml", ".env", web_env]));
    assert_eq!(v["variables"], json!(["TAG"]));

    // `--env-file` is read instead of `.env`, which is then not listed.
    std::fs::write(dir.path().join("prod.env"), "TAG=1.28\n").unwrap();
    let args = with_target(
        &["-o", "json", "deploy", "compose"],
        &["--dry-run", "--env-file", "prod.env"],
    );
    let (out, _) = run_in(&server, &args, dir.path(), "").await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["files"], json!(["compose.yaml", "prod.env", web_env]));
}

/// A directory is listed file by file, in the order read, with stdin as
/// `-` where it came among the inputs.
#[tokio::test]
async fn deploy_k8s_dry_run_lists_the_files_of_a_directory() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    let manifests = dir.path().join("manifests");
    std::fs::create_dir(&manifests).unwrap();
    std::fs::write(manifests.join("b.yml"), deployment("b", "img:b")).unwrap();
    std::fs::write(manifests.join("a.yaml"), deployment("a", "img:a")).unwrap();
    std::fs::write(manifests.join("notes.txt"), "not read").unwrap();
    std::fs::write(
        dir.path().join("single.yaml"),
        deployment("single", "img:s"),
    )
    .unwrap();

    let in_dir = |name: &str| Path::new("manifests").join(name).display().to_string();
    let head = ["deploy", "k8s", "single.yaml", "-", "manifests"];
    let stdin = deployment("piped", "img:p");
    let (_, err) = run_in(
        &server,
        &with_target(&head, &["--dry-run"]),
        dir.path(),
        &stdin,
    )
    .await
    .ok();
    assert_eq!(
        err,
        format!(
            "read: single.yaml\nread: -\nread: {}\nread: {}\n{NOTE_CAPABILITIES}",
            in_dir("a.yaml"),
            in_dir("b.yml")
        )
    );

    let args = with_target(&[&["-o", "json"][..], &head].concat(), &["--dry-run"]);
    let (out, err) = run_in(&server, &args, dir.path(), &stdin).await.ok();
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v["files"],
        json!(["single.yaml", "-", in_dir("a.yaml"), in_dir("b.yml")])
    );
}

/// What `--dry-run` prints is what `deploy spec` reads: the same workloads
/// go out, from a file and from stdin.
#[tokio::test]
async fn deploy_spec_reads_back_what_dry_run_printed() {
    let dry = silent_server().await;
    let args = with_target(&["deploy", "spec", "host-path.json"], &["--dry-run"]);
    let (printed, _) = run_fx(&dry, &args).await.ok();

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("printed.json"), &printed).unwrap();
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("host-path.json"), false, 2).await;

    let args = with_target(&["deploy", "spec", "printed.json"], &[]);
    let (out, _) = run_in(&server, &args, dir.path(), "").await.ok();
    assert_eq!(out, DEPLOY_TEXT);

    let args = with_target(&["deploy", "spec", "-"], &[]);
    let (out, _) = run_in(&server, &args, dir.path(), &printed).await.ok();
    assert_eq!(out, DEPLOY_TEXT);

    // And a second dry run of it prints the same text again.
    let args = with_target(&["deploy", "spec", "printed.json"], &["--dry-run"]);
    let (again, _) = run_in(&dry, &args, dir.path(), "").await.ok();
    assert_eq!(again, printed);
}

#[tokio::test]
async fn deploy_expose_opens_the_named_workloads_to_the_cluster() {
    let server = MockServer::start().await;
    let mut workloads = fixture_workloads("spec.json");
    assert_eq!(workloads["web"]["expose"], "environment");
    workloads["web"]["expose"] = json!("cluster");
    mount_deploy(&server, workloads.clone(), false, 1).await;

    let args = with_target(&["deploy", "spec", "spec.json"], &["--expose", "web"]);
    let (out, _) = run_fx(&server, &args).await.ok();
    assert_eq!(out, DEPLOY_TEXT);

    let dry = silent_server().await;
    workloads["db"]["expose"] = json!("cluster");
    let args = with_target(
        &["deploy", "spec", "spec.json"],
        &["--expose", "web", "--expose", "db", "--dry-run"],
    );
    let (out, _) = run_fx(&dry, &args).await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, workloads);
}

#[tokio::test]
async fn deploy_expose_of_a_workload_the_input_does_not_have_is_an_error() {
    let server = silent_server().await;
    for format in ["text", "json"] {
        let args = with_target(
            &["-o", format, "deploy", "spec", "spec.json"],
            &["--expose", "web", "--expose", "api", "--expose", "cache"],
        );
        let outcome = run_fx(&server, &args).await;
        assert_eq!(outcome.failed(), "nothing was deployed");
        assert_eq!(outcome.out, "");
        assert_eq!(
            outcome.err,
            "error: --expose: no workload named \"api\"; the input has: db, web\n\
             error: --expose: no workload named \"cache\"; the input has: db, web\n"
        );
    }
}

#[tokio::test]
async fn deploy_warnings_go_to_stderr_and_do_not_stop_the_deploy() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("host-path.json"), false, 1).await;

    let args = with_target(&["deploy", "spec", "host-path.json"], &[]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(out, DEPLOY_TEXT);
    // The container adds a capability, so that note is absent.
    assert_eq!(err, format!("{WARNING_HOST_PATH}{NOTE_SECRETS}"));
}

#[tokio::test]
async fn deploy_strict_fails_on_a_warning_and_sends_nothing() {
    let server = silent_server().await;
    for tail in [&["--strict"][..], &["--strict", "--dry-run"]] {
        for format in ["text", "json"] {
            let args = with_target(&["-o", format, "deploy", "spec", "host-path.json"], tail);
            let outcome = run_fx(&server, &args).await;
            assert_eq!(
                outcome.failed(),
                "nothing was deployed: --strict turns a warning into an error"
            );
            assert_eq!(outcome.out, "");
            assert_eq!(outcome.err, WARNING_HOST_PATH);
        }
    }
}

/// A note is not a warning: `--strict` lets it through.
#[tokio::test]
async fn deploy_strict_ignores_notes() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("spec.json"), false, 1).await;

    let args = with_target(&["deploy", "spec", "spec.json"], &["--strict"]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(out, DEPLOY_TEXT);
    assert_eq!(err, NOTE_CAPABILITIES);
}

#[tokio::test]
async fn deploy_spec_that_does_not_decode_names_the_file() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("list.json"), "[]").unwrap();
    std::fs::write(
        dir.path().join("typo.json"),
        r#"{"web": {"containers": [{"name": "web", "image": "nginx", "privileged": true}]}}"#,
    )
    .unwrap();

    for (file, want) in [
        (
            "list.json",
            "error: list.json: not a map of workload specs: ",
        ),
        (
            "typo.json",
            "error: typo.json: not a map of workload specs: ",
        ),
        ("missing.json", "error: missing.json: cannot read: "),
        ("-", "error: -: not a map of workload specs: "),
    ] {
        let args = with_target(&["deploy", "spec", file], &[]);
        let outcome = run_in(&server, &args, dir.path(), "not json").await;
        assert_eq!(outcome.failed(), "nothing was deployed", "{file}");
        assert_eq!(outcome.out, "", "{file}");
        assert!(outcome.err.starts_with(want), "{file}: {}", outcome.err);
        assert_eq!(outcome.err.lines().count(), 1, "{file}: {}", outcome.err);
    }
    let args = with_target(&["deploy", "spec", "typo.json"], &[]);
    let outcome = run_in(&server, &args, dir.path(), "").await;
    assert!(outcome.err.contains("privileged"), "{}", outcome.err);
}

/// The CLI runs the model validation itself, and reports every violation
/// like a conversion error.
#[tokio::test]
async fn deploy_spec_that_breaks_the_model_is_not_sent() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("bad.json"),
        r#"{
            "web": {"containers": [{"name": "web", "image": ""}]},
            "Not_A_Label": {"containers": []}
        }"#,
    )
    .unwrap();

    for tail in [&[][..], &["--dry-run"]] {
        let args = with_target(&["deploy", "spec", "bad.json"], tail);
        let outcome = run_in(&server, &args, dir.path(), "").await;
        assert_eq!(outcome.failed(), "nothing was deployed");
        assert_eq!(outcome.out, "");
        let lines: Vec<&str> = outcome.err.lines().collect();
        assert!(lines.len() >= 3, "{}", outcome.err);
        for line in &lines {
            assert!(line.starts_with("error: bad.json: "), "{line}");
        }
        for path in ["web.containers[0].image: ", "Not_A_Label"] {
            assert!(outcome.err.contains(path), "{path}: {}", outcome.err);
        }
    }
}

#[tokio::test]
async fn deploy_refused_by_the_api_is_readable() {
    let server = MockServer::start().await;
    let error = |code: &str, message: &str| json!({"error": code, "message": message});
    for (environment, status, body) in [
        ("staging", 404, error("not_found", "no project named shop")),
        (
            "full",
            409,
            error("workloads_exhausted", "the environment holds 64 workloads"),
        ),
        ("bad", 400, error("bad_request", "web: image is empty")),
    ] {
        let route = format!("/api/v1/projects/shop/environments/{environment}/deploy");
        mount_json(&server, "POST", &route, status, body).await;
    }

    for (environment, want) in [
        ("staging", "not_found: no project named shop"),
        (
            "full",
            "workloads_exhausted: the environment holds 64 workloads",
        ),
        ("bad", "bad_request: web: image is empty"),
    ] {
        for format in ["text", "json"] {
            let args = [
                "-o",
                format,
                "deploy",
                "spec",
                "host-path.json",
                "--project",
                "shop",
                "--env",
                environment,
            ];
            let outcome = run_fx(&server, &args).await;
            assert_eq!(outcome.failed(), want);
            assert_eq!(outcome.out, "");
            // The warnings were printed before the request went out, so
            // they are there although it failed. JSON mode has no
            // document on stdout, so it writes one line of JSON here.
            if format == "text" {
                assert_eq!(outcome.err, format!("{WARNING_HOST_PATH}{NOTE_SECRETS}"));
                continue;
            }
            assert_eq!(outcome.err.matches('\n').count(), 1, "{}", outcome.err);
            assert!(outcome.err.ends_with('\n'), "{}", outcome.err);
            let v: Value = serde_json::from_str(&outcome.err).unwrap();
            assert_eq!(
                v,
                json!({
                    "warnings": [{
                        "source": "host-path.json",
                        "path": "web.volumes.data",
                        "message": "host path /srv/data: host paths are refused by nodes \
                                    unless allowed in the node's host-path allow-list",
                    }],
                    "notes": [NOTE_SECRETS.trim_start_matches("note: ").trim_end()],
                    "variables": [],
                })
            );
        }
    }
}

// ---- deploy compose ----

const COMPOSE: &str = "services:\n  web:\n    image: nginx:1.27\n";

fn compose_with_image(image: &str) -> String {
    format!("services:\n  web:\n    image: {image}\n")
}

/// The image of the first container of `web` in a `--dry-run` document.
fn web_image(dry_run_out: &str) -> String {
    let v: Value = serde_json::from_str(dry_run_out).unwrap();
    v["web"]["containers"][0]["image"]
        .as_str()
        .unwrap_or_else(|| panic!("no web image in {dry_run_out}"))
        .to_string()
}

#[tokio::test]
async fn deploy_compose_sends_the_converted_services() {
    let server = MockServer::start().await;
    mount_json(&server, "POST", DEPLOY_ROUTE, 200, deploy_body()).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("stack.yml"), COMPOSE).unwrap();

    let args = with_target(&["deploy", "compose", "stack.yml"], &["--expose", "web"]);
    let (out, err) = run_in(&server, &args, dir.path(), "").await.ok();
    assert_eq!(out, DEPLOY_TEXT);
    assert_eq!(err, NOTE_CAPABILITIES);

    let body = only_request_body(&server).await;
    assert_eq!(body["prune"], false);
    let workloads = body["workloads"].as_object().unwrap();
    assert_eq!(workloads.keys().collect::<Vec<_>>(), ["web"]);
    assert_eq!(workloads["web"]["containers"][0]["image"], "nginx:1.27");
    assert_eq!(workloads["web"]["expose"], "cluster");
}

/// Without FILE: `compose.yaml`, `compose.yml`, `docker-compose.yaml`,
/// `docker-compose.yml`, the first that exists.
#[tokio::test]
async fn deploy_compose_without_a_file_looks_in_the_current_directory() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    let args = with_target(&["deploy", "compose"], &["--dry-run"]);

    let names = [
        "docker-compose.yml",
        "docker-compose.yaml",
        "compose.yml",
        "compose.yaml",
    ];
    for name in names {
        // Each file names itself in the image tag, and each one added
        // comes earlier in the order than the ones already there.
        let tag = name.replace('.', "-");
        std::fs::write(
            dir.path().join(name),
            compose_with_image(&format!("nginx:{tag}")),
        )
        .unwrap();
        let (out, _) = run_in(&server, &args, dir.path(), "").await.ok();
        assert_eq!(
            web_image(&out),
            format!("nginx:{tag}"),
            "after adding {name}"
        );
    }
}

#[tokio::test]
async fn deploy_compose_without_any_compose_file_lists_the_names() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("stack.yml"), COMPOSE).unwrap();

    let args = with_target(&["deploy", "compose"], &[]);
    let outcome = run_in(&server, &args, dir.path(), "").await;
    assert_eq!(outcome.failed(), "nothing was deployed");
    assert_eq!(outcome.out, "");
    assert_eq!(
        outcome.err,
        format!(
            "error: {}: no compose file here; looked for compose.yaml, compose.yml, \
             docker-compose.yaml, docker-compose.yml\n",
            slashes(dir.path())
        )
    );
}

#[tokio::test]
async fn deploy_compose_warns_about_an_override_file_it_does_not_merge() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("compose.yaml"), COMPOSE).unwrap();
    std::fs::write(
        dir.path().join("compose.override.yaml"),
        compose_with_image("nginx:override"),
    )
    .unwrap();

    let warning = "warning: compose.override.yaml: not merged: only compose.yaml is read; \
                   `docker compose config` writes the merged file\n";
    let args = with_target(&["deploy", "compose"], &["--dry-run"]);
    let (out, err) = run_in(&server, &args, dir.path(), "").await.ok();
    assert_eq!(web_image(&out), "nginx:1.27");
    assert_eq!(
        err,
        format!("{warning}read: compose.yaml\n{NOTE_CAPABILITIES}")
    );

    // It is a warning like any other.
    let args = with_target(&["deploy", "compose"], &["--dry-run", "--strict"]);
    let outcome = run_in(&server, &args, dir.path(), "").await;
    assert_eq!(
        outcome.failed(),
        "nothing was deployed: --strict turns a warning into an error"
    );
    assert_eq!(outcome.err, warning);
    assert_eq!(outcome.out, "");
}

/// Variables come from the caller's environment and win over the env file;
/// `--env-file` replaces `.env`; `--profile` turns a profile on.
#[tokio::test]
async fn deploy_compose_passes_variables_env_file_and_profiles() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("compose.yaml"),
        "services:\n  web:\n    image: nginx:${TAG}\n  debug:\n    image: busybox:${BOX}\n    \
         profiles: [debug]\n",
    )
    .unwrap();
    std::fs::write(dir.path().join(".env"), "TAG=from-dot-env\nBOX=dot\n").unwrap();
    std::fs::write(dir.path().join("prod.env"), "TAG=from-prod-env\nBOX=prod\n").unwrap();

    let run = |tail: &'static [&'static str], variables: &[(&str, &str)]| {
        let inputs = Inputs {
            cwd: dir.path().to_path_buf(),
            variables: variables
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            stdin: Box::new(Cursor::new(Vec::new())),
        };
        let args = with_target(&["deploy", "compose"], tail);
        let server = &server;
        async move {
            let (out, _) = run_inputs(server, &args, inputs).await.ok();
            serde_json::from_str::<Value>(&out).unwrap()
        }
    };

    let v = run(&["--dry-run"], &[]).await;
    assert_eq!(v["web"]["containers"][0]["image"], "nginx:from-dot-env");
    assert_eq!(v.get("debug"), None, "{v}");

    let v = run(&["--dry-run", "--env-file", "prod.env"], &[]).await;
    assert_eq!(v["web"]["containers"][0]["image"], "nginx:from-prod-env");

    let v = run(&["--dry-run"], &[("TAG", "from-environment")]).await;
    assert_eq!(v["web"]["containers"][0]["image"], "nginx:from-environment");

    let v = run(&["--dry-run", "--profile", "debug"], &[]).await;
    assert_eq!(v["debug"]["containers"][0]["image"], "busybox:dot");
}

#[tokio::test]
async fn deploy_compose_with_an_error_prints_every_error_and_sends_nothing() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("compose.yaml"),
        "services:\n  web:\n    build: .\n  db:\n    image: postgres:16\n    privileged: true\n",
    )
    .unwrap();

    for format in ["text", "json"] {
        let args = with_target(&["-o", format, "deploy", "compose"], &[]);
        let outcome = run_in(&server, &args, dir.path(), "").await;
        assert_eq!(outcome.failed(), "nothing was deployed");
        assert_eq!(outcome.out, "");
        let errors: Vec<&str> = outcome
            .err
            .lines()
            .filter(|line| line.starts_with("error: "))
            .collect();
        assert!(errors.len() >= 2, "{}", outcome.err);
        // The file is named the way the caller would name it.
        for key in ["services.web.build: ", "services.db.privileged: "] {
            let line = format!("error: compose.yaml: {key}");
            assert!(
                errors.iter().any(|error| error.starts_with(&line)),
                "{key}: {}",
                outcome.err
            );
        }
    }
}

// ---- deploy k8s ----

fn deployment(name: &str, image: &str) -> String {
    format!(
        "apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
spec:
  replicas: 2
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
    spec:
      containers:
        - name: main
          image: {image}
"
    )
}

/// Files, a directory and stdin in one deploy: each becomes a document
/// named after where it came from.
#[tokio::test]
async fn deploy_k8s_reads_files_directories_and_stdin() {
    let server = MockServer::start().await;
    mount_json(&server, "POST", DEPLOY_ROUTE, 200, deploy_body()).await;
    let dir = tempfile::tempdir().unwrap();
    let manifests = dir.path().join("manifests");
    std::fs::create_dir(&manifests).unwrap();
    std::fs::write(manifests.join("b.yml"), deployment("b", "img:b")).unwrap();
    std::fs::write(manifests.join("a.yaml"), deployment("a", "img:a")).unwrap();
    std::fs::write(manifests.join("ignored.txt"), "not yaml: [").unwrap();
    std::fs::create_dir(manifests.join("nested")).unwrap();
    std::fs::write(
        manifests.join("nested").join("n.yaml"),
        deployment("nested", "img:n"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("single.yaml"),
        deployment("single", "img:s"),
    )
    .unwrap();

    let args = with_target(&["deploy", "k8s", "single.yaml", "manifests", "-"], &[]);
    let stdin = deployment("piped", "img:p");
    let (out, _) = run_in(&server, &args, dir.path(), &stdin).await.ok();
    assert_eq!(out, DEPLOY_TEXT);

    let body = only_request_body(&server).await;
    let workloads = body["workloads"].as_object().unwrap();
    assert_eq!(
        workloads.keys().collect::<Vec<_>>(),
        ["a", "b", "piped", "single"]
    );
    assert_eq!(workloads["a"]["containers"][0]["image"], "img:a");
    assert_eq!(workloads["piped"]["containers"][0]["image"], "img:p");
    assert_eq!(workloads["single"]["scale"], json!({"replicas": 2}));
}

/// Each input is refused under the name it was given, in the order of the
/// arguments and, within a directory, of the file names.
#[tokio::test]
async fn deploy_k8s_names_its_sources_in_directory_order() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    let manifests = dir.path().join("manifests");
    std::fs::create_dir(&manifests).unwrap();
    let job = |name: &str| format!("apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: {name}\n");
    // Written in the reverse of their name order.
    std::fs::write(manifests.join("2-job.yaml"), job("two")).unwrap();
    std::fs::write(manifests.join("1-job.yaml"), job("one")).unwrap();

    let args = with_target(&["deploy", "k8s", "-", "manifests"], &[]);
    let outcome = run_in(&server, &args, dir.path(), &job("piped")).await;
    assert_eq!(outcome.failed(), "nothing was deployed");
    assert_eq!(outcome.out, "");
    let in_dir = |name: &str| Path::new("manifests").join(name).display().to_string();
    let known = ["-".to_string(), in_dir("1-job.yaml"), in_dir("2-job.yaml")];
    // The sources in the order each first has an error.
    let mut first_seen: Vec<&String> = Vec::new();
    for line in outcome.err.lines() {
        let line = line.strip_prefix("error: ").expect(line);
        let source = known
            .iter()
            .find(|source| line.starts_with(&format!("{source}: ")));
        if let Some(source) = source.filter(|s| !first_seen.contains(s)) {
            first_seen.push(source);
        }
    }
    assert_eq!(
        first_seen,
        known.iter().collect::<Vec<_>>(),
        "{}",
        outcome.err
    );
}

#[tokio::test]
async fn deploy_k8s_dry_run_json_carries_the_warnings() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    let service =
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\nspec:\n  ports:\n    - port: 80\n";
    std::fs::write(
        dir.path().join("app.yaml"),
        format!("{}---\n{service}", deployment("web", "nginx:1.27")),
    )
    .unwrap();

    let args = with_target(&["-o", "json", "deploy", "k8s", "app.yaml"], &["--dry-run"]);
    let (out, err) = run_in(&server, &args, dir.path(), "").await.ok();
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["project"], "shop");
    assert_eq!(v["environment"], "staging");
    assert_eq!(v["spec"]["web"]["containers"][0]["image"], "nginx:1.27");
    let warnings = v["warnings"].as_array().unwrap();
    assert!(!warnings.is_empty(), "{out}");
    for warning in warnings {
        let mut keys: Vec<&String> = warning.as_object().unwrap().keys().collect();
        keys.sort();
        assert_eq!(keys, ["message", "path", "source"], "{warning}");
    }
    // The Service is the second document of the file.
    assert!(
        warnings.iter().any(|w| w["source"] == "app.yaml#2"),
        "{out}"
    );

    // Text mode: the same findings as lines, and they count for --strict.
    let args = with_target(&["deploy", "k8s", "app.yaml"], &["--dry-run", "--strict"]);
    let outcome = run_in(&server, &args, dir.path(), "").await;
    assert_eq!(
        outcome.failed(),
        "nothing was deployed: --strict turns a warning into an error"
    );
    assert_eq!(outcome.out, "");
    assert_eq!(
        outcome.err.lines().count(),
        warnings.len(),
        "{}",
        outcome.err
    );
    for line in outcome.err.lines() {
        assert!(line.starts_with("warning: app.yaml#"), "{line}");
    }
}

// ---- review cycle 1: prune, variables, external files, escaping ----

/// `--prune` with nothing to deploy would empty the environment, so it is
/// refused. Without `--prune` an empty input is an error too.
#[tokio::test]
async fn deploy_of_no_workloads_is_refused_with_or_without_prune() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("empty.json"), "{}").unwrap();

    for (tail, want) in [
        (&[][..], "error: empty.json: nothing to deploy: "),
        (&["--dry-run"], "error: empty.json: nothing to deploy: "),
        (&["--prune"], "error: --prune: the input has no workloads"),
        (
            &["--prune", "--dry-run"],
            "error: --prune: the input has no workloads",
        ),
    ] {
        let args = with_target(&["deploy", "spec", "empty.json"], tail);
        let outcome = run_in(&server, &args, dir.path(), "").await;
        assert_eq!(outcome.failed(), "nothing was deployed", "{tail:?}");
        assert_eq!(outcome.out, "", "{tail:?}");
        assert!(outcome.err.starts_with(want), "{tail:?}: {}", outcome.err);
        assert_eq!(outcome.err.lines().count(), 1, "{}", outcome.err);
    }
}

/// A service left out by its profile is not in the request, so `--prune`
/// would remove it. The error names it.
#[tokio::test]
async fn deploy_prune_is_refused_when_a_profile_skipped_a_service() {
    let server = MockServer::start().await;
    mount_json(&server, "POST", DEPLOY_ROUTE, 200, deploy_body()).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("compose.yaml"),
        "services:\n  web:\n    image: nginx:1.27\n  debug:\n    image: busybox\n    \
         profiles: [debug]\n",
    )
    .unwrap();

    for tail in [&["--prune"][..], &["--prune", "--dry-run"]] {
        let args = with_target(&["deploy", "compose"], tail);
        let outcome = run_in(&server, &args, dir.path(), "").await;
        assert_eq!(outcome.failed(), "nothing was deployed");
        assert_eq!(outcome.out, "");
        let line = outcome
            .err
            .lines()
            .find(|line| line.starts_with("error: --prune: "))
            .unwrap_or_else(|| panic!("no --prune error in {}", outcome.err));
        for want in ["debug", "--profile", "drop --prune"] {
            assert!(line.contains(want), "{want}: {line}");
        }
    }
    assert!(server.received_requests().await.unwrap().is_empty());

    // With the profile on, nothing was skipped and the deploy goes out.
    let args = with_target(&["deploy", "compose"], &["--prune", "--profile", "debug"]);
    run_in(&server, &args, dir.path(), "").await.ok();
    let body = only_request_body(&server).await;
    assert_eq!(body["prune"], true);
    let workloads = body["workloads"].as_object().unwrap();
    assert_eq!(workloads.keys().collect::<Vec<_>>(), ["debug", "web"]);
}

#[tokio::test]
async fn deploy_spec_with_a_workload_defined_twice_is_refused() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("twice.json"),
        r#"{
            "web": {"containers": [{"name": "web", "image": "nginx:1"}]},
            "web": {"containers": [{"name": "web", "image": "nginx:2"}]}
        }"#,
    )
    .unwrap();

    let args = with_target(&["deploy", "spec", "twice.json"], &["--dry-run"]);
    let outcome = run_in(&server, &args, dir.path(), "").await;
    assert_eq!(outcome.failed(), "nothing was deployed");
    assert_eq!(outcome.out, "");
    assert!(
        outcome
            .err
            .starts_with("error: twice.json: not a map of workload specs: "),
        "{}",
        outcome.err
    );
    assert!(
        outcome
            .err
            .contains(r#"the workload "web" is defined twice"#),
        "{}",
        outcome.err
    );
}

/// The caller's variables that went into the workloads are named before
/// anything is sent. Their values are never printed.
#[tokio::test]
async fn deploy_compose_names_the_variables_it_used() {
    let server = MockServer::start().await;
    mount_json(&server, "POST", DEPLOY_ROUTE, 200, deploy_body()).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("compose.yaml"),
        "services:\n  web:\n    image: nginx:${TAG}\n    environment:\n      MODE: ${MODE}\n",
    )
    .unwrap();

    let run = |head: &'static [&'static str], tail: &'static [&'static str]| {
        let inputs = Inputs {
            cwd: dir.path().to_path_buf(),
            variables: [
                ("TAG", "s3cr3t-tag"),
                ("MODE", "s3cr3t-mode"),
                ("UNUSED", "x"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            stdin: Box::new(Cursor::new(Vec::new())),
        };
        let args = with_target(head, tail);
        let server = &server;
        async move { run_inputs(server, &args, inputs).await.ok() }
    };

    let (out, err) = run(&["deploy", "compose"], &["--dry-run"]).await;
    assert_eq!(web_image(&out), "nginx:s3cr3t-tag");
    assert_eq!(
        err,
        format!("uses variable: MODE\nuses variable: TAG\nread: compose.yaml\n{NOTE_CAPABILITIES}")
    );

    let (out, err) = run(&["-o", "json", "deploy", "compose"], &["--dry-run"]).await;
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["variables"], json!(["MODE", "TAG"]));

    // A real deploy says the same on stderr, and in its JSON document.
    let (out, err) = run(&["deploy", "compose"], &[]).await;
    assert_eq!(out, DEPLOY_TEXT);
    assert_eq!(
        err,
        format!("uses variable: MODE\nuses variable: TAG\n{NOTE_CAPABILITIES}")
    );
    assert!(!err.contains("s3cr3t"), "{err}");

    let (out, err) = run(&["-o", "json", "deploy", "compose"], &[]).await;
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["variables"], json!(["MODE", "TAG"]));
    assert_eq!(v.get("files"), None);
}

/// An `env_file` outside the directory of the compose file is read only
/// with `--allow-external-env-file`.
#[tokio::test]
async fn deploy_compose_reads_an_outside_env_file_only_when_allowed() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("app");
    std::fs::create_dir(&app).unwrap();
    std::fs::write(
        app.join("compose.yaml"),
        "services:\n  web:\n    image: nginx:1.27\n    env_file: ../outside.env\n",
    )
    .unwrap();
    let outside = dir.path().join("outside.env");
    std::fs::write(&outside, "MODE=outside\n").unwrap();

    let args = with_target(&["-o", "json", "deploy", "compose"], &["--dry-run"]);
    let outcome = run_in(&server, &args, &app, "").await;
    assert_eq!(outcome.failed(), "nothing was deployed");
    assert_eq!(outcome.out, "");
    for want in [
        "error: compose.yaml: ",
        "outside.env",
        "--allow-external-env-file",
    ] {
        assert!(outcome.err.contains(want), "{want}: {}", outcome.err);
    }
    assert!(!outcome.err.contains("MODE"), "{}", outcome.err);

    let args = with_target(
        &["-o", "json", "deploy", "compose"],
        &["--dry-run", "--allow-external-env-file"],
    );
    let (out, _) = run_in(&server, &args, &app, "").await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["spec"]["web"]["containers"][0]["env"]["MODE"], "outside");
    // Outside the current directory, so the whole real path.
    assert_eq!(v["files"], json!(["compose.yaml", real(&outside)]));
}

/// A link inside the directory that leads out of it is outside.
#[cfg(unix)]
#[tokio::test]
async fn deploy_compose_follows_a_link_before_deciding_where_a_file_is() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("app");
    std::fs::create_dir(&app).unwrap();
    std::fs::write(app.join("compose.yaml"), compose_with_image("nginx:${TAG}")).unwrap();
    let outside = dir.path().join("outside.env");
    std::fs::write(&outside, "TAG=outside\n").unwrap();
    std::os::unix::fs::symlink(&outside, app.join(".env")).unwrap();

    let args = with_target(&["deploy", "compose"], &["--dry-run"]);
    let outcome = run_in(&server, &args, &app, "").await;
    assert_eq!(outcome.failed(), "nothing was deployed");
    assert_eq!(outcome.out, "");
    for want in ["outside.env", "--allow-external-env-file"] {
        assert!(outcome.err.contains(want), "{want}: {}", outcome.err);
    }

    let args = with_target(
        &["-o", "json", "deploy", "compose"],
        &["--dry-run", "--allow-external-env-file"],
    );
    let (out, _) = run_in(&server, &args, &app, "").await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["spec"]["web"]["containers"][0]["image"], "nginx:outside");
    // The list has the file that was read, not the link.
    assert_eq!(v["files"], json!(["compose.yaml", real(&outside)]));
}

/// Every input has to be a regular file of at most 8 MiB. Stdin has the
/// same limit.
#[tokio::test]
async fn deploy_refuses_an_input_that_is_not_a_regular_file_or_is_too_large() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("folder")).unwrap();
    let large = " ".repeat(8 * 1024 * 1024 + 1);
    std::fs::write(dir.path().join("large.yaml"), &large).unwrap();

    for (head, stdin, source, reason) in [
        (
            &["deploy", "spec", "folder"][..],
            "",
            "folder",
            "not a regular file",
        ),
        (
            &["deploy", "compose", "folder"],
            "",
            "folder",
            "not a regular file",
        ),
        (
            &["deploy", "spec", "large.yaml"],
            "",
            "large.yaml",
            "larger than 8 MiB",
        ),
        (
            &["deploy", "k8s", "large.yaml"],
            "",
            "large.yaml",
            "larger than 8 MiB",
        ),
        (
            &["deploy", "compose", "large.yaml"],
            "",
            "large.yaml",
            "larger than 8 MiB",
        ),
        (
            &["deploy", "spec", "-"],
            large.as_str(),
            "-",
            "larger than 8 MiB",
        ),
        (
            &["deploy", "k8s", "-"],
            large.as_str(),
            "-",
            "larger than 8 MiB",
        ),
    ] {
        let args = with_target(head, &["--dry-run"]);
        let outcome = run_in(&server, &args, dir.path(), stdin).await;
        assert_eq!(outcome.failed(), "nothing was deployed", "{head:?}");
        assert_eq!(outcome.out, "", "{head:?}");
        // The compose converter words its own reading error.
        let start = format!("error: {source}: cannot read");
        assert!(outcome.err.starts_with(&start), "{head:?}: {}", outcome.err);
        assert!(outcome.err.contains(reason), "{head:?}: {}", outcome.err);
        assert_eq!(outcome.err.lines().count(), 1, "{head:?}: {}", outcome.err);
    }
}

/// The API says what was not found, and the quotes in it stay as they are.
#[tokio::test]
async fn a_not_found_with_a_message_is_printed_whole() {
    let server = MockServer::start().await;
    let body = json!({"error": "not_found", "message": "project \"x\" does not exist"});
    mount_json(&server, "GET", "/api/v1/projects/x", 404, body.clone()).await;
    let route = "/api/v1/projects/x/environments/staging/deploy";
    mount_json(&server, "POST", route, 404, body).await;

    let (err, out) = run_err(&server, &["project", "show", "x"]).await;
    assert_eq!(err, r#"not_found: project "x" does not exist"#);
    assert_eq!(out, "");

    let args = [
        "deploy",
        "spec",
        "spec.json",
        "--project",
        "x",
        "--env",
        "staging",
    ];
    let outcome = run_fx(&server, &args).await;
    assert_eq!(outcome.failed(), r#"not_found: project "x" does not exist"#);
    assert_eq!(outcome.out, "");
}

/// The commands that were there before project, deploy and app escape what
/// the API sent as well, and text that reorders or hides characters counts.
#[tokio::test]
async fn the_older_commands_escape_what_the_api_sent() {
    let server = MockServer::start().await;
    let mut status = status_body();
    status["leader_addr"] = json!("10.8.0.1\u{1b}[2J:7401");
    status["members"][0]["name"] = json!("no\u{1b}de\u{202e}-1");
    status["members"][1]["addr"] = json!("10.8.0.2\u{200b}:7401\r\n");
    mount_json(&server, "GET", "/api/v1/cluster/status", 200, status).await;
    let health = json!({"status": "ok\u{1b}[31m\u{feff}", "node_id": 7});
    mount_json(&server, "GET", "/api/v1/health", 200, health).await;
    let mut invite = invite_body();
    invite["name"] = json!("node\u{2066}-2\n");
    mount_invite(&server, invite).await;

    // Format characters: general category Cf, as far as the tests go.
    let hidden = |c: &char| {
        (c.is_control() && *c != '\n')
            || matches!(*c, '\u{200b}'..='\u{200f}' | '\u{202a}'..='\u{202e}')
            || matches!(*c, '\u{2066}'..='\u{2069}' | '\u{feff}')
    };
    for args in [&["cluster", "status"][..], &["health"], &INVITE_ARGS] {
        let (out, err) = run_split(&server, args).await;
        for text in [&out, &err] {
            assert_eq!(text.chars().find(hidden), None, "{args:?}:\n{text}");
        }
    }

    let (out, _) = run_split(&server, &["cluster", "status"]).await;
    for want in [
        "leader:  1@10.8.0.1\\x1b[2J:7401\n",
        "no\\x1bde\\u{202e}-1",
        "10.8.0.2\\u{200b}:7401\\x0d\\x0a",
    ] {
        assert!(out.contains(want), "missing {want:?} in:\n{out}");
    }
    let (out, _) = run_split(&server, &["health"]).await;
    assert!(out.contains("ok\\x1b[31m\\u{feff}"), "{out}");
    let (_, err) = run_split(&server, &INVITE_ARGS).await;
    assert!(err.contains("name:       node\\u{2066}-2\\x0a\n"), "{err}");
}

/// A response that is not what the CLI expects is quoted in the error, so
/// the error is escaped like everything else.
#[tokio::test]
async fn a_response_that_does_not_decode_is_escaped_in_the_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/projects/shop"))
        .respond_with(ResponseTemplate::new(200).set_body_string("\u{1b}[2J\u{202e}not json"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/projects/gone"))
        .respond_with(ResponseTemplate::new(500).set_body_string("oops\u{1b}[2J\u{202e}\r\n"))
        .mount(&server)
        .await;

    for name in ["shop", "gone"] {
        let (err, out) = run_err(&server, &["project", "show", name]).await;
        assert_eq!(out, "");
        let hidden = err.chars().find(|c| c.is_control() || *c == '\u{202e}');
        assert_eq!(hidden, None, "{name}: {err}");
    }
}

/// A deploy that never reaches the API fails like a refused one: JSON mode
/// writes its one line of details to stderr, with the variable names.
#[tokio::test]
async fn deploy_json_that_cannot_connect_keeps_its_details_on_stderr() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("compose.yaml"),
        compose_with_image("nginx:${TAG}"),
    )
    .unwrap();
    let inputs = || Inputs {
        cwd: dir.path().to_path_buf(),
        variables: BTreeMap::from([("TAG".to_string(), "s3cr3t".to_string())]),
        stdin: Box::new(Cursor::new(Vec::new())),
    };
    // Nothing listens on port 1.
    let run = |format: &'static str| {
        let args = [
            "faber",
            "-e",
            "http://127.0.0.1:1",
            "-o",
            format,
            "deploy",
            "compose",
        ];
        let cli = Cli::try_parse_from(args.into_iter().chain(TARGET)).expect("parse");
        let inputs = inputs();
        async move {
            let (mut out, mut err) = (Vec::new(), Vec::new());
            let result = faber::run_with_inputs(cli, &mut out, &mut err, inputs).await;
            assert!(result.is_err());
            assert!(out.is_empty());
            String::from_utf8(err).unwrap()
        }
    };

    let err = run("json").await;
    assert_eq!(err.matches('\n').count(), 1, "{err}");
    let v: Value = serde_json::from_str(&err).unwrap();
    assert_eq!(v["variables"], json!(["TAG"]));
    assert_eq!(v["warnings"], json!([]));
    assert_eq!(
        v["notes"],
        json!([NOTE_CAPABILITIES.trim_start_matches("note: ").trim_end()])
    );
    assert!(!err.contains("s3cr3t"), "{err}");

    // Text mode said it before sending, and says it once.
    assert_eq!(
        run("text").await,
        format!("uses variable: TAG\n{NOTE_CAPABILITIES}")
    );
}

/// A compose file that is a link: the files beside the link are beside the
/// compose file, and the ones elsewhere still are not.
#[cfg(unix)]
#[tokio::test]
async fn deploy_compose_through_a_link_reads_the_env_files_beside_the_link() {
    let server = silent_server().await;
    let dir = tempfile::tempdir().unwrap();
    let (app, shared) = (dir.path().join("app"), dir.path().join("shared"));
    std::fs::create_dir(&app).unwrap();
    std::fs::create_dir(&shared).unwrap();
    std::fs::write(
        shared.join("compose.yaml"),
        "services:\n  web:\n    image: nginx:${TAG}\n    env_file: web.env\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(shared.join("compose.yaml"), app.join("compose.yaml")).unwrap();
    std::fs::write(app.join(".env"), "TAG=beside-the-link\n").unwrap();
    std::fs::write(app.join("web.env"), "MODE=app\n").unwrap();

    let args = with_target(&["-o", "json", "deploy", "compose"], &["--dry-run"]);
    let (out, _) = run_in(&server, &args, &app, "").await.ok();
    let v: Value = serde_json::from_str(&out).unwrap();
    let web = &v["spec"]["web"]["containers"][0];
    assert_eq!(web["image"], "nginx:beside-the-link");
    assert_eq!(web["env"]["MODE"], "app");
    assert_eq!(
        v["files"],
        json!([real(&shared.join("compose.yaml")), ".env", "web.env"])
    );

    // A link beside the link that leads elsewhere is still outside.
    let outside = dir.path().join("outside.env");
    std::fs::write(&outside, "MODE=outside\n").unwrap();
    std::fs::remove_file(app.join("web.env")).unwrap();
    std::os::unix::fs::symlink(&outside, app.join("web.env")).unwrap();
    let outcome = run_in(&server, &args, &app, "").await;
    assert_eq!(outcome.failed(), "nothing was deployed");
    for want in ["outside.env", "--allow-external-env-file"] {
        assert!(outcome.err.contains(want), "{want}: {}", outcome.err);
    }
}
