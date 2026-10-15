//! `run()` against a wiremock server: request shapes and output formatting.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use clap::Parser;
use faber::deploy::Inputs;
use faber::{Cli, run};
use faber_api_types::workload::WorkloadSpec;
use serde_json::{Value, json};
use wiremock::matchers::{
    any, body_json, method, path, path_regex, query_param, query_param_is_missing,
};
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
        stdin: Box::new(Cursor::new(stdin.as_bytes().to_vec())),
    };
    run_inputs(server, args, inputs).await
}

/// Runs in the fixtures directory.
async fn run_fx(server: &MockServer, args: &[&str]) -> Outcome {
    run_in(server, args, &fixtures(), "").await
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

const TARGET: [&str; 4] = ["--project", "shop", "--env", "staging"];

/// `faber <head...> --project shop --env staging <tail...>`
fn with_target<'a>(head: &[&'a str], tail: &[&'a str]) -> Vec<&'a str> {
    [head, &TARGET, tail].concat()
}

const NOTE_CAPABILITIES: &str = "note: containers get no Linux capabilities unless the file adds \
    them; images that expect Docker's default set may fail to start\n";

const NOTE_SECRETS: &str = "note: an env variable name looks like a secret; env values are \
    stored unencrypted in cluster state and are readable by local users on every node\n";

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

// ---- mesh status-interval ----

#[tokio::test]
async fn mesh_status_interval_text_and_json() {
    let server = MockServer::start().await;
    mount_json(
        &server,
        "GET",
        "/api/v1/mesh/status-interval",
        200,
        json!({"interval_secs": 5}),
    )
    .await;

    let text = run_ok(&server, &["mesh", "status-interval"]).await;
    assert_eq!(text, "status_interval: 5s\n");

    let out = run_ok(&server, &["-o", "json", "mesh", "status-interval"]).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"status_interval_secs": 5}));
    assert!(out.ends_with('\n'));
}

#[tokio::test]
async fn mesh_status_interval_set_puts_the_seconds_and_prints_the_new_interval() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/mesh/status-interval"))
        .and(body_json(json!({"interval_secs": 120})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"interval_secs": 120})))
        .expect(2)
        .mount(&server)
        .await;

    let text = run_ok(&server, &["mesh", "status-interval", "--set", "2m"]).await;
    assert_eq!(text, "status_interval: 2m (120s)\n");

    let args = ["-o", "json", "mesh", "status-interval", "--set", "120"];
    let v: Value = serde_json::from_str(&run_ok(&server, &args).await).unwrap();
    assert_eq!(v, json!({"status_interval_secs": 120}));
}

#[tokio::test]
async fn mesh_status_interval_errors_are_readable() {
    let server = MockServer::start().await;
    mount_json(
        &server,
        "GET",
        "/api/v1/mesh/status-interval",
        503,
        json!({"error": "no_leader"}),
    )
    .await;
    let (err, out) = run_err(&server, &["mesh", "status-interval"]).await;
    assert_eq!(err, "no_leader");
    assert!(out.is_empty());
}

// ---- cluster key export ----

const KEY: &str = "q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq8s=";

#[tokio::test]
async fn cluster_key_export_prints_exactly_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("cluster.key");
    // The file may end with a newline or not; the output always does.
    for content in [KEY.to_string(), format!("{KEY}\n")] {
        std::fs::write(&file, content).unwrap();
        let file = file.to_str().unwrap();
        let server = silent_server().await;

        let text = run_ok(&server, &["cluster", "key", "export", "--file", file]).await;
        assert_eq!(text, format!("{KEY}\n"));

        let args = ["-o", "json", "cluster", "key", "export", "--file", file];
        let out = run_ok(&server, &args).await;
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v, json!({"cluster_key": KEY}));
        assert!(out.ends_with('\n'));
    }
}

#[tokio::test]
async fn cluster_key_export_of_a_missing_file_names_the_file_and_no_key() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("absent.key");
    let server = silent_server().await;

    let args = ["cluster", "key", "export", "--file", file.to_str().unwrap()];
    let (err, out) = run_err(&server, &args).await;
    assert!(err.contains("absent.key"), "{err}");
    assert!(err.contains("root or faber"), "{err}");
    assert!(!err.contains(KEY), "{err}");
    assert!(out.is_empty());
}

/// A path that is not a readable file fails with an error that holds no
/// content of whatever was there.
#[tokio::test]
async fn cluster_key_export_of_an_unreadable_path_holds_no_key() {
    let dir = tempfile::tempdir().unwrap();
    let server = silent_server().await;

    let args = [
        "cluster",
        "key",
        "export",
        "--file",
        dir.path().to_str().unwrap(),
    ];
    let (err, out) = run_err(&server, &args).await;
    assert!(err.contains("not a regular file"), "{err}");
    assert!(out.is_empty());
}

#[tokio::test]
async fn cluster_key_export_of_an_empty_file_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("cluster.key");
    std::fs::write(&file, "\n").unwrap();
    let server = silent_server().await;

    let args = ["cluster", "key", "export", "--file", file.to_str().unwrap()];
    let (err, out) = run_err(&server, &args).await;
    assert!(err.contains("empty"), "{err}");
    assert!(out.is_empty());
}

// ---- app ls ----

fn workload_list_body() -> Value {
    let summary = |name: &str, scale: Value, expose: &str, revision: u64, up: u32, want: u32| {
        json!({
            "project": "shop", "environment": "staging", "name": name,
            "fqdn": format!("{name}.staging.shop.faber.local"),
            "scale": scale, "expose": expose, "revision": revision,
            "available": up, "desired": want,
        })
    };
    json!({"workloads": [
        summary("db", json!({"replicas": {"count": 1, "strategy": "spread"}}), "environment", 4, 1, 1),
        summary("logs", json!("daemon"), "environment", 5, 1, 3),
        summary("web", json!({"replicas": {"count": 3, "strategy": "pack"}}), "cluster", 7, 2, 3),
    ]})
}

#[tokio::test]
async fn app_ls_text_and_json() {
    let server = MockServer::start().await;
    // The list says it all: no workload is read on its own.
    Mock::given(method("GET"))
        .and(path_regex("^/api/v1/projects/.*/workloads/"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workloads"))
        .and(query_param("project", "shop"))
        .and(query_param("environment", "staging"))
        .and(query_param_is_missing("reveal"))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_list_body()))
        .expect(2)
        .mount(&server)
        .await;

    let args = ["app", "ls", "--project", "shop", "--env", "staging"];
    assert_eq!(
        run_ok(&server, &args).await,
        "NAME  PROJECT  ENVIRONMENT  SCALE       AVAILABLE  EXPOSE       FQDN
db    shop     staging      1 (spread)  1/1        environment  db.staging.shop.faber.local
logs  shop     staging      daemon      1/3        environment  logs.staging.shop.faber.local
web   shop     staging      3 (pack)    2/3        cluster      web.staging.shop.faber.local
"
    );

    // JSON is the list as it came.
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
        "NAME  PROJECT  ENVIRONMENT  SCALE  AVAILABLE  EXPOSE  FQDN\n"
    );
    let text = run_ok(&server, &["app", "ls", "--env", "staging"]).await;
    assert_eq!(text.lines().count(), 4, "{text}");
}

// ---- app show ----

const WORKLOAD_ROUTE: &str = "/api/v1/projects/shop/environments/staging/workloads/web";

/// A workload that uses every part of the model.
fn workload_body() -> Value {
    json!({
        "project": "shop", "environment": "staging", "name": "web",
        "fqdn": "web.staging.shop.faber.local", "revision": 7, "generation": 9,
        "spec": {
            "scale": "daemon",
            "restart": "on_failure",
            "expose": "cluster",
            "placement": {"nodes": ["node-1", "node-2"]},
            "stop_grace_secs": 30,
            "volumes": {
                "data": {"type": "block", "size_bytes": 1_073_741_824},
                "tmp": {"type": "ramdisk", "size_bytes": 1_048_576},
                // Mounted by no container.
                "spare": {"type": "block", "size_bytes": 2_097_152},
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
                        {"volume": "data", "path": "/data", "read_only": true},
                        {"volume": "tmp", "path": "/tmp", "read_only": false},
                    ],
                    "resources": {"cpu_millis": 500, "memory_bytes": 268_435_456},
                    "capabilities": ["NET_BIND_SERVICE", "CHOWN"],
                    "read_only_root_fs": true,
                    "probe": {
                        "action": {
                            "type": "http", "port": 8080, "path": "/ready", "scheme": "https",
                            "host": "web.local", "headers": {"x-probe": "s3cret"},
                        },
                        "interval_secs": 10, "timeout_secs": 2,
                    },
                },
                {
                    "name": "sidecar",
                    "image": "busybox",
                    "command": [],
                    "args": [],
                    "env": {},
                    "ports": [],
                    "mounts": [],
                    "resources": {"cpu_millis": 1000, "memory_bytes": 1_073_741_824},
                    "capabilities": [],
                    "read_only_root_fs": false,
                },
            ],
        },
        "replicas": [
            {"node": 2, "replica_index": 0, "revision": 7, "state": "available",
             "pod_ip": "10.200.2.5"},
            {"node": 3, "replica_index": 1, "revision": 7, "state": "unavailable"},
            {"node": null, "replica_index": 2, "revision": 7, "state": "unavailable",
             "reason": "insufficient_capacity"},
        ],
    })
}

const WORKLOAD_TEXT: &str = "\
name:        web
fqdn:        web.staging.shop.faber.local
project:     shop
environment: staging
revision:    7
generation:  9
scale:       daemon
restart:     on-failure
expose:      cluster
nodes:       node-1, node-2
stop_grace:  30s
replicas:
  REPLICA  NODE  REVISION  STATE        POD IP      REASON
  0        2     7         available    10.200.2.5  -
  1        3     7         unavailable  -           -
  2        -     7         unavailable  -           insufficient_capacity
volumes:
  data:  block, 1073741824 bytes
  spare: block, 2097152 bytes
  tmp:   ramdisk, 1048576 bytes
containers:
  web:
    image:             nginx:1.27
    command:           nginx
    args:              -g \"daemon off;\"
    working_dir:       /srv
    user:              101:101
    env:               API_KEY=hunter2
                       MODE=prod
    ports:             http:8080/tcp
                       53/udp
    mounts:            data:/data (block, 1073741824 bytes, read-only)
                       tmp:/tmp (ramdisk, 1048576 bytes)
    resources:         cpu 500m, memory 268435456 bytes
    capabilities:      NET_BIND_SERVICE, CHOWN
    read_only_root_fs: yes
    probe:             https :8080/ready, host web.local, headers x-probe (interval 10s, \
timeout 2s)
  sidecar:
    image:             busybox
    command:           -
    args:              -
    working_dir:       (image default)
    user:              (image default)
    env:               -
    ports:             -
    mounts:            -
    resources:         cpu 1000m, memory 1073741824 bytes
    capabilities:      -
    read_only_root_fs: no
    probe:             -
";

/// Env values are always shown, so the request always asks for them.
#[tokio::test]
async fn app_show_always_reveals_and_prints_the_values() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(WORKLOAD_ROUTE))
        .and(query_param("reveal", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_body()))
        .expect(1)
        .mount(&server)
        .await;

    let (out, err) = run_split(&server, &with_target(&["app", "show", "web"], &[])).await;
    assert_eq!(out, WORKLOAD_TEXT);
    assert_eq!(err, "");
}

/// JSON is the response as it came.
#[tokio::test]
async fn app_show_json_is_the_dto() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(WORKLOAD_ROUTE))
        .and(query_param("reveal", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(workload_body()))
        .expect(1)
        .mount(&server)
        .await;

    let args = with_target(&["-o", "json", "app", "show", "web"], &[]);
    let out = run_ok(&server, &args).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, workload_body());
}

#[tokio::test]
async fn app_show_without_replica_reports_says_so() {
    let server = MockServer::start().await;
    let mut body = workload_body();
    body["replicas"] = json!([]);
    mount_json(&server, "GET", WORKLOAD_ROUTE, 200, body).await;

    let out = run_ok(&server, &with_target(&["app", "show", "web"], &[])).await;
    assert!(
        out.contains(
            "stop_grace:  30s
replicas: -
volumes:
"
        ),
        "{out}"
    );
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
    workload["fqdn"] = json!("web\u{1b}[2J.faber.local");
    workload["spec"]["placement"]["nodes"] = json!(["node\r\n1"]);
    let web = &mut workload["spec"]["containers"][0];
    web["image"] = json!("nginx\u{1b}]0;owned\u{7}");
    web["args"] = json!(["a\tb", "c\u{9b}d"]);
    web["env"] = json!({"MODE": "prod\nname: fake", "NA\u{0}ME": "x"});
    web["ports"][0]["name"] = json!("ht\u{7f}tp");
    web["mounts"][0]["path"] = json!("/da\u{1b}ta");
    web["capabilities"] = json!(["NET\u{1b}_ADMIN"]);
    web["probe"]["action"]["path"] = json!("/rea\u{1b}dy");
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

    let show = with_target(&["app", "show", "web"], &[]);
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
        "fqdn:        web\\x1b[2J.faber.local\n",
        "nodes:       node\\x0d\\x0a1\n",
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
        out.contains(
            "d\\x1b[31mb  sh\\x0aop  stag\\x08ing  1 (spread)  1/1        environment  db\\x1bc\n"
        ),
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
        "removed web.staging.shop.faber.local\nremoved db.staging.shop.faber.local\n"
    );
    assert_eq!(err, "");

    let out = run_ok(&server, &[&["-o", "json"][..], &args].concat()).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v,
        json!({"removed": [
            "web.staging.shop.faber.local",
            "db.staging.shop.faber.local",
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
    assert_eq!(out, "removed web.staging.shop.faber.local\n");

    let (err, out) = run_err(&server, &[&["-o", "json"][..], &args].concat()).await;
    assert_eq!(err, "not_found: no workload named gone");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v, json!({"removed": ["web.staging.shop.faber.local"]}));
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
    mount_deploy(&server, fixture_workloads("volume.json"), false, 1).await;

    let args = with_target(&["-o", "json", "deploy", "spec", "spec.json"], &[]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, "");
    assert!(out.ends_with('\n'));
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v,
        json!({
            "result": deploy_body(),
            "notes": [NOTE_CAPABILITIES.trim_start_matches("note: ").trim_end()],
            "variables": [],
        })
    );

    let args = with_target(&["-o", "json", "deploy", "spec", "volume.json"], &[]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, "");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        v,
        json!({
            "result": deploy_body(),
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
        &["-o", "json", "deploy", "spec", "volume.json"],
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
            "spec": fixture_workloads("volume.json"),
            "notes": [NOTE_SECRETS.trim_start_matches("note: ").trim_end()],
            "variables": [],
            "files": ["volume.json"],
        })
    );
}

/// `--dry-run` lists every file the deploy read, so the caller sees what
/// of this machine would go into cluster state: after the warnings in text
/// mode, in `files` in JSON. A deploy that is sent lists nothing.
#[tokio::test]
async fn deploy_spec_dry_run_lists_the_file_it_read() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("volume.json"), false, 1).await;

    let args = with_target(&["deploy", "spec", "volume.json"], &["--dry-run"]);
    let (_, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, format!("read: volume.json\n{NOTE_SECRETS}"));

    // An absolute path below the current directory is shown relative.
    let absolute = fixtures().join("volume.json");
    let args = with_target(
        &["deploy", "spec", absolute.to_str().unwrap()],
        &["--dry-run"],
    );
    let (_, err) = run_fx(&server, &args).await.ok();
    assert!(err.starts_with("read: volume.json\n"), "{err}");

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
    let args = with_target(&["deploy", "spec", "volume.json"], &[]);
    let (_, err) = run_fx(&server, &args).await.ok();
    assert_eq!(err, NOTE_SECRETS);
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

/// What `--dry-run` prints is what `deploy spec` reads: the same workloads
/// go out, from a file and from stdin.
#[tokio::test]
async fn deploy_spec_reads_back_what_dry_run_printed() {
    let dry = silent_server().await;
    let args = with_target(&["deploy", "spec", "volume.json"], &["--dry-run"]);
    let (printed, _) = run_fx(&dry, &args).await.ok();

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("printed.json"), &printed).unwrap();
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("volume.json"), false, 2).await;

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
async fn deploy_notes_go_to_stderr_and_do_not_stop_the_deploy() {
    let server = MockServer::start().await;
    mount_deploy(&server, fixture_workloads("volume.json"), false, 1).await;

    let args = with_target(&["deploy", "spec", "volume.json"], &[]);
    let (out, err) = run_fx(&server, &args).await.ok();
    assert_eq!(out, DEPLOY_TEXT);
    // The container adds a capability, so that note is absent.
    assert_eq!(err, NOTE_SECRETS);
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
                "volume.json",
                "--project",
                "shop",
                "--env",
                environment,
            ];
            let outcome = run_fx(&server, &args).await;
            assert_eq!(outcome.failed(), want);
            assert_eq!(outcome.out, "");
            // The notes were printed before the request went out, so
            // they are there although it failed. JSON mode has no
            // document on stdout, so it writes one line of JSON here.
            if format == "text" {
                assert_eq!(outcome.err, NOTE_SECRETS);
                continue;
            }
            assert_eq!(outcome.err.matches('\n').count(), 1, "{}", outcome.err);
            assert!(outcome.err.ends_with('\n'), "{}", outcome.err);
            let v: Value = serde_json::from_str(&outcome.err).unwrap();
            assert_eq!(
                v,
                json!({
                    "notes": [NOTE_SECRETS.trim_start_matches("note: ").trim_end()],
                    "variables": [],
                })
            );
        }
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
            &["deploy", "spec", "large.yaml"],
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
    ] {
        let args = with_target(head, &["--dry-run"]);
        let outcome = run_in(&server, &args, dir.path(), stdin).await;
        assert_eq!(outcome.failed(), "nothing was deployed", "{head:?}");
        assert_eq!(outcome.out, "", "{head:?}");
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
/// writes its one line of details to stderr.
#[tokio::test]
async fn deploy_json_that_cannot_connect_keeps_its_details_on_stderr() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(fixtures().join("spec.json"), dir.path().join("spec.json")).unwrap();
    let inputs = || Inputs {
        cwd: dir.path().to_path_buf(),
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
            "spec",
            "spec.json",
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
    assert_eq!(v["variables"], json!([]));
    assert_eq!(
        v["notes"],
        json!([NOTE_CAPABILITIES.trim_start_matches("note: ").trim_end()])
    );

    // Text mode said it before sending, and says it once.
    assert_eq!(run("text").await, NOTE_CAPABILITIES);
}
