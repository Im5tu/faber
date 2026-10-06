//! MCP endpoint: initialize handshake on both listeners and the `Host`
//! allow-list.

mod common;

use std::net::SocketAddr;

use reqwest::StatusCode;
use rmcp::ServiceExt;
use rmcp::transport::StreamableHttpClientTransport;

use common::TestNode;

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#;

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
async fn rmcp_client_completes_initialize_on_both_listeners() {
    let mut node = TestNode::start(1).await;

    for addr in [node.api_addr(), node.peer_addr()] {
        let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
        let client = ().serve(transport).await.unwrap();
        let info = client.peer_info().expect("server info after initialize");
        assert_eq!(
            info.instructions.as_deref(),
            Some(fabercp::mcp::INSTRUCTIONS)
        );
        assert!(
            info.capabilities.tools.is_none(),
            "no tools are exposed yet"
        );
        client.cancel().await.unwrap();
    }

    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_host_is_rejected_until_allowed() {
    let mut node = TestNode::start(1).await;
    for addr in [node.api_addr(), node.peer_addr()] {
        let port = addr.port();
        assert_eq!(
            initialize_with_host(addr, &format!("evil.example:{port}")).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            initialize_with_host(addr, &format!("127.0.0.1:{port}")).await,
            StatusCode::OK
        );
    }
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
