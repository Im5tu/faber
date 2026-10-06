//! MCP server (streamable HTTP at `/mcp`).
//!
//! No tools yet: the domain API has not been defined. The server answers the
//! initialize handshake with its info and instructions so clients can connect.
//!
//! TODO(security): no authentication on /mcp. The `Host` allow-list only
//! stops DNS rebinding; it is not access control.

use std::sync::Arc;

use rmcp::ServerHandler;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio_util::sync::CancellationToken;

pub const INSTRUCTIONS: &str = "Faber node control plane. No tools are available yet.";

#[derive(Debug, Clone, Default)]
pub struct FaberMcp;

impl ServerHandler for FaberMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::default())
            .with_server_info(Implementation::from_build_env())
            .with_instructions(INSTRUCTIONS)
    }
}

/// The `/mcp` service. `allowed_hosts` is matched against the `Host` header.
pub fn service(
    allowed_hosts: Vec<String>,
    cancel: CancellationToken,
) -> StreamableHttpService<FaberMcp, LocalSessionManager> {
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts)
        .with_cancellation_token(cancel);
    StreamableHttpService::new(
        || Ok(FaberMcp),
        Arc::new(LocalSessionManager::default()),
        config,
    )
}
