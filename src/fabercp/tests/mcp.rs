//! MCP endpoint: the tool set, one call per tool, the error shape, the body
//! limit, that only the loopback listener serves it, and the `Host`
//! allow-list.

mod common;

use std::collections::BTreeSet;
use std::net::SocketAddr;

use reqwest::StatusCode;
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};

use common::TestNode;

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#;

const TOOLS: [&str; 17] = [
    "app_list",
    "app_remove",
    "app_show",
    "cluster_init",
    "cluster_join",
    "cluster_status",
    "deploy_spec",
    "environment_add",
    "environment_remove",
    "health",
    "mesh_invite",
    "mesh_lease",
    "mesh_status_interval",
    "project_create",
    "project_list",
    "project_remove",
    "project_show",
];

type Client = RunningService<RoleClient, ()>;

async fn connect(addr: SocketAddr) -> Client {
    let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
    ().serve(transport).await.unwrap()
}

/// Calls `tool` and returns whether it failed, with the JSON in its text.
async fn call(client: &Client, tool: &'static str, args: Value) -> (bool, Value) {
    let mut params = CallToolRequestParams::new(tool);
    if let Value::Object(map) = args {
        params = params.with_arguments(map);
    }
    let result: CallToolResult = client.call_tool(params).await.unwrap();
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .expect("text content");
    let json = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{tool}: {e}: {text}"));
    (result.is_error == Some(true), json)
}

async fn ok(client: &Client, tool: &'static str, args: Value) -> Value {
    let (failed, body) = call(client, tool, args).await;
    assert!(!failed, "{tool} failed: {body}");
    body
}

async fn err(client: &Client, tool: &'static str, args: Value) -> Value {
    let (failed, body) = call(client, tool, args).await;
    assert!(failed, "{tool} did not fail: {body}");
    body
}

fn web() -> Value {
    json!({"web": {"containers": [{"name": "main", "image": "nginx:1.27", "env": {"MODE": "x"}}]}})
}

/// POSTs an initialize request to `addr` with an explicit `Host` header.
async fn initialize_with_host(addr: SocketAddr, host: &str) -> StatusCode {
    common::http()
        .post(format!("http://{addr}/mcp"))
        .header(reqwest::header::HOST, host)
        .header(
            reqwest::header::ACCEPT,
            "application/json, text/event-stream",
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(INITIALIZE)
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_exactly_the_tools_of_the_cli() {
    let mut node = TestNode::start(1).await;
    let client = connect(node.api_addr()).await;
    let info = client.peer_info().expect("server info after initialize");
    assert_eq!(
        info.instructions.as_deref(),
        Some(fabercp::mcp::INSTRUCTIONS)
    );
    assert!(info.capabilities.tools.is_some());

    let tools = client.list_all_tools().await.unwrap();
    let names: BTreeSet<&str> = tools.iter().map(|t| &*t.name).collect();
    assert_eq!(names, BTreeSet::from(TOOLS));
    assert_eq!(tools.len(), TOOLS.len());
    client.cancel().await.unwrap();
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_tool_runs_against_the_service() {
    let mut node = TestNode::start(1).await;
    let c = connect(node.api_addr()).await;

    let health = ok(&c, "health", json!({})).await;
    assert_eq!(health["status"], "ok");
    assert_eq!(health["node_id"], 1);

    let init = ok(&c, "cluster_init", json!({})).await;
    assert_eq!(init["node_id"], 1);
    node.wait_leader(1).await;
    assert_eq!(
        err(&c, "cluster_init", json!({})).await["error"],
        "already_initialized"
    );
    let status = ok(&c, "cluster_status", json!({})).await;
    assert_eq!(status["leader_id"], 1);
    assert_eq!(
        err(&c, "cluster_join", json!({"peer": ""})).await["error"],
        "bad_request"
    );

    // The node has no mesh, so the mesh tools reach the service and say so.
    let invite = json!({"name": "node-2", "wg_endpoint": "203.0.113.2:51820", "ttl_secs": 60});
    assert_eq!(
        err(&c, "mesh_invite", invite).await["error"],
        "mesh_not_configured"
    );
    assert_eq!(
        err(&c, "mesh_lease", json!({})).await["error"],
        "mesh_not_configured"
    );
    assert_eq!(
        err(&c, "mesh_lease", json!({"set_secs": 600})).await["error"],
        "mesh_not_configured"
    );
    let interval = ok(&c, "mesh_status_interval", json!({})).await;
    assert!(interval["interval_secs"].is_u64());
    let interval = ok(&c, "mesh_status_interval", json!({"set_secs": 7})).await;
    assert_eq!(interval["interval_secs"], 7);

    let shop = ok(
        &c,
        "project_create",
        json!({"name": "shop", "environments": ["staging"]}),
    )
    .await;
    assert_eq!(shop["name"], "shop");
    assert_eq!(
        ok(&c, "project_list", json!({})).await["projects"][0]["name"],
        "shop"
    );
    assert_eq!(ok(&c, "project_show", json!({"name": "shop"})).await, shop);
    let added = ok(
        &c,
        "environment_add",
        json!({"project": "shop", "environment": "production"}),
    )
    .await;
    assert_eq!(added["environments"].as_array().unwrap().len(), 2);

    let target = json!({"project": "shop", "environment": "staging"});
    let deploy = |extra: Value| {
        let mut args = target.clone();
        args["workloads"] = web();
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        args
    };
    let dry = ok(
        &c,
        "deploy_spec",
        deploy(json!({"dry_run": true, "expose": ["web"]})),
    )
    .await;
    assert_eq!(dry["project"], "shop");
    assert_eq!(dry["spec"]["web"]["expose"], "cluster");
    assert!(dry["notes"].is_array());
    assert_eq!(
        ok(&c, "app_list", json!({})).await["workloads"],
        json!([]),
        "a dry run deploys nothing"
    );
    let done = ok(&c, "deploy_spec", deploy(json!({}))).await;
    assert_eq!(done["created"], json!(["web"]));
    assert_eq!(done["notes"].as_array().map(Vec::len), Some(1));

    let listed = ok(&c, "app_list", json!({"project": "shop"})).await;
    assert_eq!(
        listed["workloads"][0]["fqdn"],
        "web.staging.shop.faber.local"
    );
    let mut show = target.clone();
    show["workload"] = json!("web");
    let masked = ok(&c, "app_show", show.clone()).await;
    assert_eq!(masked["spec"]["containers"][0]["env"]["MODE"], "***");
    show["reveal"] = json!(true);
    let revealed = ok(&c, "app_show", show).await;
    assert_eq!(revealed["spec"]["containers"][0]["env"]["MODE"], "x");

    let mut remove = target.clone();
    remove["workloads"] = json!(["web", "missing", "never-reached"]);
    let failed = err(&c, "app_remove", remove).await;
    assert_eq!(failed["removed"], json!(["web.staging.shop.faber.local"]));
    assert_eq!(failed["error"], "not_found");

    assert_eq!(
        ok(
            &c,
            "environment_remove",
            json!({"project": "shop", "environment": "production"})
        )
        .await,
        json!({"project": "shop", "removed": "production"})
    );
    assert_eq!(
        ok(&c, "project_remove", json!({"name": "shop"})).await,
        json!({"removed": "shop"})
    );
    c.cancel().await.unwrap();
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_call_carries_the_rest_error_body() {
    let mut node = common::single_node().await;
    let c = connect(node.api_addr()).await;

    let missing = err(&c, "project_show", json!({"name": "nope"})).await;
    assert_eq!(missing["error"], "not_found");

    // The same body the REST route answers with.
    let (status, rest): (StatusCode, Value) = node.get("/api/v1/projects/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing, rest);

    // A deploy that breaks a rule is refused before the service is called.
    let bad = err(
        &c,
        "deploy_spec",
        json!({
            "project": "shop",
            "environment": "staging",
            "workloads": {"web": {"containers": [{"name": "main", "image": ""}]}},
        }),
    )
    .await;
    assert_eq!(bad["error"], "bad_request");
    assert!(
        bad["message"]
            .as_str()
            .unwrap()
            .contains("web.containers[0].image")
    );
    let empty = err(
        &c,
        "deploy_spec",
        json!({"project": "shop", "environment": "staging", "workloads": {}, "prune": true}),
    )
    .await;
    assert_eq!(empty["error"], "bad_request");
    c.cancel().await.unwrap();
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_over_the_limit_is_refused() {
    let mut node = TestNode::start(1).await;
    let big = vec![b' '; 1024 * 1024 + 1];
    let status = common::http()
        .post(format!("http://{}/mcp", node.api_addr()))
        .header(
            reqwest::header::ACCEPT,
            "application/json, text/event-stream",
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(big)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_peer_listener_does_not_serve_mcp() {
    let mut node = TestNode::start(1).await;
    let status = initialize_with_host(node.peer_addr(), &node.peer_addr().to_string()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_host_is_rejected_until_allowed() {
    let mut node = TestNode::start(1).await;
    let addr = node.api_addr();
    let port = addr.port();
    assert_eq!(
        initialize_with_host(addr, &format!("evil.example:{port}")).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        initialize_with_host(addr, &format!("127.0.0.1:{port}")).await,
        StatusCode::OK
    );
    node.stop().await;

    let mut node = TestNode::start_with(1, |c| {
        c.mcp_allowed_hosts = vec!["evil.example".to_string()];
    })
    .await;
    let addr = node.api_addr();
    let port = addr.port();
    assert_eq!(
        initialize_with_host(addr, &format!("evil.example:{port}")).await,
        StatusCode::OK
    );
    assert_eq!(
        initialize_with_host(addr, &format!("other.example:{port}")).await,
        StatusCode::FORBIDDEN
    );
    node.stop().await;
}
