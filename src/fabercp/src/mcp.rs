//! MCP server (streamable HTTP at `/mcp`, loopback listener only).
//!
//! One tool per action of the `faber` CLI, each calling the same
//! [`FaberService`] method as the matching REST handler. A result is the JSON
//! the REST route answers with, as text. A failure is a tool error whose text
//! is the REST [`ErrorBody`] JSON. The tools and their shapes are pinned in
//! `docs/contract.md`.
//!
//! TODO(security): no authentication on /mcp. The `Host` allow-list only
//! stops DNS rebinding; it is not access control. Whoever reaches the
//! loopback listener can deploy, read the mesh and create invites.

use std::collections::BTreeMap;
use std::sync::Arc;

use faber_api_types::workload::{
    CreateProjectRequest, DeployIssue, DeployIssueSource, DeployRequest, NOTE_SECRET_NAMES,
    WorkloadQuery, WorkloadSpec, fqdn, prepare_deploy,
};
use faber_api_types::{CreateInviteRequest, JoinRequest, error_code};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use crate::api::cluster::health_report;
use crate::error::error_parts;
use crate::service::{FaberService, ServiceError, ServiceResult};

pub const INSTRUCTIONS: &str = "Faber node control plane. Each tool does what the `faber` \
     command of the same name does. Results are JSON text; a failed call returns the error \
     body as JSON text. Env values of a workload read *** unless app_show is called with \
     reveal.";

#[derive(Debug, Deserialize, JsonSchema)]
pub struct JoinArgs {
    /// Peer listener address of any node already in the cluster, as ip:port.
    pub peer: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InviteArgs {
    /// The name the new server will hold in the mesh: a DNS label.
    pub name: String,
    /// The new server's public WireGuard endpoint as existing nodes see it:
    /// an IP address with an optional port.
    pub wg_endpoint: String,
    /// How long the invite is valid, in seconds. The server default applies
    /// when absent.
    pub ttl_secs: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SetSecsArgs {
    /// Set the value, in seconds. Without it the current value is returned.
    pub set_secs: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProjectCreateArgs {
    /// The name of the project: a DNS label.
    pub name: String,
    /// The environments the project starts with, each a DNS label. Without
    /// it the project gets staging and production.
    pub environments: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProjectArgs {
    /// The name of the project.
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct EnvironmentArgs {
    /// The project.
    pub project: String,
    /// The environment of the project: a DNS label.
    pub environment: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeploySpecArgs {
    /// The project.
    pub project: String,
    /// The environment of the project.
    pub environment: String,
    /// A JSON object of workload specs by name, as `faber deploy spec
    /// --dry-run` prints it. A name that comes twice is not detected: the
    /// last one wins.
    pub workloads: Map<String, Value>,
    /// Names of workloads to make reachable from every workload in the
    /// cluster.
    pub expose: Option<Vec<String>>,
    /// Also remove the workloads of the environment that `workloads` does
    /// not name.
    pub prune: Option<bool>,
    /// Validate and return what would be deployed, and send nothing.
    pub dry_run: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AppListArgs {
    /// Only the workloads of this project.
    pub project: Option<String>,
    /// Only the workloads of this environment.
    pub environment: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AppShowArgs {
    /// The project.
    pub project: String,
    /// The environment of the project.
    pub environment: String,
    /// The workload.
    pub workload: String,
    /// Return env values in clear. They read *** otherwise (the default).
    pub reveal: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AppRemoveArgs {
    /// The project.
    pub project: String,
    /// The environment of the project.
    pub environment: String,
    /// The workloads to remove, one after the other, stopping at the first
    /// failure.
    pub workloads: Vec<String>,
}

/// The tools of the control plane, over one node's service.
#[derive(Clone)]
pub struct FaberMcp {
    svc: FaberService,
    tool_router: ToolRouter<Self>,
}

impl FaberMcp {
    pub fn new(svc: FaberService) -> Self {
        Self {
            svc,
            tool_router: Self::tool_router(),
        }
    }
}

/// `value` as the text of a successful result.
fn success(value: &impl Serialize) -> CallToolResult {
    match serde_json::to_string(value) {
        Ok(text) => CallToolResult::success(vec![ContentBlock::text(text)]),
        Err(e) => failure(ServiceError::Internal(format!(
            "cannot write the result: {e}"
        ))),
    }
}

/// A tool error whose text is `body`.
fn error_result(body: &impl Serialize) -> CallToolResult {
    let text = serde_json::to_string(body)
        .unwrap_or_else(|_| format!(r#"{{"error":"{}"}}"#, error_code::INTERNAL));
    CallToolResult::error(vec![ContentBlock::text(text)])
}

/// The REST error body of `err`, as a tool error.
fn failure(err: ServiceError) -> CallToolResult {
    error_result(&error_parts(err).1)
}

fn reply<T: Serialize>(result: ServiceResult<T>) -> CallToolResult {
    match result {
        Ok(value) => success(&value),
        Err(err) => failure(err),
    }
}

/// `issues` as one `bad_request`, a line each.
fn deploy_refused(issues: &[DeployIssue]) -> CallToolResult {
    let lines: Vec<String> = issues
        .iter()
        .map(|issue| {
            let place = match issue.source {
                DeployIssueSource::Input => issue.path.as_str(),
                DeployIssueSource::Prune => "prune",
                DeployIssueSource::Expose => "expose",
            };
            if issue.source == DeployIssueSource::Prune {
                format!(
                    "{}; set prune to false, or remove them by name with app_remove",
                    issue.message
                )
            } else if place.is_empty() {
                issue.message.clone()
            } else {
                format!("{place}: {}", issue.message)
            }
        })
        .collect();
    failure(ServiceError::BadRequest(lines.join("\n")))
}

#[tool_router]
impl FaberMcp {
    #[tool(
        name = "health",
        description = "Check that the node is up. Same as `faber health`."
    )]
    async fn health(&self) -> CallToolResult {
        let (status, body) = health_report(&self.svc);
        if status.is_success() {
            success(&body)
        } else {
            error_result(&body)
        }
    }

    #[tool(
        name = "cluster_status",
        description = "Show this node's role, the leader and replication progress of each member. Same as `faber cluster status`."
    )]
    async fn cluster_status(&self) -> CallToolResult {
        success(&self.svc.status().await)
    }

    #[tool(
        name = "cluster_init",
        description = "Make this node a new single-node cluster. Same as `faber cluster init`."
    )]
    async fn cluster_init(&self) -> CallToolResult {
        reply(self.svc.init().await)
    }

    #[tool(
        name = "cluster_join",
        description = "Join the cluster that the peer listener address belongs to. Same as `faber cluster join`."
    )]
    async fn cluster_join(&self, Parameters(args): Parameters<JoinArgs>) -> CallToolResult {
        reply(self.svc.join(JoinRequest { peer: args.peer }).await)
    }

    #[tool(
        name = "mesh_invite",
        description = "Create a single-use invite token for a new server. The token carries a private key. Same as `faber mesh invite`."
    )]
    async fn mesh_invite(&self, Parameters(args): Parameters<InviteArgs>) -> CallToolResult {
        let req = CreateInviteRequest {
            name: args.name,
            endpoint: args.wg_endpoint,
            ttl_secs: args.ttl_secs,
        };
        reply(self.svc.invite(req).await)
    }

    #[tool(
        name = "mesh_lease",
        description = "Show or set (with set_secs) the node lease duration. Same as `faber mesh lease`."
    )]
    async fn mesh_lease(&self, Parameters(args): Parameters<SetSecsArgs>) -> CallToolResult {
        reply(match args.set_secs {
            Some(secs) => self.svc.set_lease(secs, false).await,
            None => self.svc.lease().await,
        })
    }

    #[tool(
        name = "mesh_status_interval",
        description = "Show or set (with set_secs) how often nodes report the status of their replicas. Same as `faber mesh status-interval`."
    )]
    async fn mesh_status_interval(
        &self,
        Parameters(args): Parameters<SetSecsArgs>,
    ) -> CallToolResult {
        reply(match args.set_secs {
            Some(secs) => self.svc.set_status_interval(secs, false).await,
            None => self.svc.status_interval().await,
        })
    }

    #[tool(
        name = "project_create",
        description = "Create a project. Same as `faber project create`."
    )]
    async fn project_create(
        &self,
        Parameters(args): Parameters<ProjectCreateArgs>,
    ) -> CallToolResult {
        let req = CreateProjectRequest {
            name: args.name,
            environments: args.environments,
        };
        reply(self.svc.create_project(req).await)
    }

    #[tool(
        name = "project_list",
        description = "List the projects. Same as `faber project ls`."
    )]
    async fn project_list(&self) -> CallToolResult {
        reply(self.svc.list_projects().await)
    }

    #[tool(
        name = "project_show",
        description = "Show a project and its environments. Same as `faber project show`."
    )]
    async fn project_show(&self, Parameters(args): Parameters<ProjectArgs>) -> CallToolResult {
        reply(self.svc.project(&args.name).await)
    }

    #[tool(
        name = "project_remove",
        description = "Remove a project. Its environments must hold no workloads. Same as `faber project rm`."
    )]
    async fn project_remove(&self, Parameters(args): Parameters<ProjectArgs>) -> CallToolResult {
        reply(
            self.svc
                .delete_project(&args.name)
                .await
                .map(|()| json!({ "removed": args.name })),
        )
    }

    #[tool(
        name = "environment_add",
        description = "Add an environment to a project. Same as `faber project env add`."
    )]
    async fn environment_add(
        &self,
        Parameters(args): Parameters<EnvironmentArgs>,
    ) -> CallToolResult {
        reply(
            self.svc
                .add_environment(&args.project, &args.environment)
                .await,
        )
    }

    #[tool(
        name = "environment_remove",
        description = "Remove an environment that holds no workloads. Same as `faber project env rm`."
    )]
    async fn environment_remove(
        &self,
        Parameters(args): Parameters<EnvironmentArgs>,
    ) -> CallToolResult {
        reply(
            self.svc
                .remove_environment(&args.project, &args.environment)
                .await
                .map(|()| json!({ "project": args.project, "removed": args.environment })),
        )
    }

    #[tool(
        name = "deploy_spec",
        description = "Deploy workloads to one environment of a project: creates and updates the workloads given and leaves the others alone unless prune is set. Same as `faber deploy spec`."
    )]
    async fn deploy_spec(&self, Parameters(args): Parameters<DeploySpecArgs>) -> CallToolResult {
        let DeploySpecArgs {
            project,
            environment,
            workloads,
            expose,
            prune,
            dry_run,
        } = args;
        let prune = prune.unwrap_or(false);
        let mut workloads: BTreeMap<String, WorkloadSpec> =
            match serde_json::from_value(Value::Object(workloads)) {
                Ok(workloads) => workloads,
                Err(e) => {
                    return failure(ServiceError::BadRequest(format!(
                        "not a map of workload specs: {e}"
                    )));
                }
            };
        let notes = match prepare_deploy(&mut workloads, &expose.unwrap_or_default(), prune) {
            Ok(notes) => notes
                .into_iter()
                .map(|note| {
                    if note == NOTE_SECRET_NAMES {
                        format!("{note}; app_show masks them unless reveal is true")
                    } else {
                        note
                    }
                })
                .collect::<Vec<_>>(),
            Err(issues) => return deploy_refused(&issues),
        };
        if dry_run.unwrap_or(false) {
            return success(&json!({
                "project": project,
                "environment": environment,
                "spec": workloads,
                "notes": notes,
            }));
        }
        let req = DeployRequest { workloads, prune };
        match self.svc.deploy(&project, &environment, req).await {
            Ok(done) => {
                let mut body = serde_json::to_value(done).unwrap_or_default();
                body["notes"] = json!(notes);
                success(&body)
            }
            Err(err) => failure(err),
        }
    }

    #[tool(
        name = "app_list",
        description = "List the workloads, of every project unless a filter is given. Same as `faber app ls`."
    )]
    async fn app_list(&self, Parameters(args): Parameters<AppListArgs>) -> CallToolResult {
        let query = WorkloadQuery {
            project: args.project,
            environment: args.environment,
        };
        reply(self.svc.list_workloads(query).await)
    }

    #[tool(
        name = "app_show",
        description = "Show one workload with its spec and the status of each replica. Env values read *** unless reveal is true. Same as `faber app show`."
    )]
    async fn app_show(&self, Parameters(args): Parameters<AppShowArgs>) -> CallToolResult {
        reply(
            self.svc
                .workload(
                    &args.project,
                    &args.environment,
                    &args.workload,
                    args.reveal.unwrap_or(false),
                )
                .await,
        )
    }

    #[tool(
        name = "app_remove",
        description = "Remove workloads, one after the other, stopping at the first failure. Returns the removed names; a failure also lists what was removed before it. Same as `faber app rm`."
    )]
    async fn app_remove(&self, Parameters(args): Parameters<AppRemoveArgs>) -> CallToolResult {
        let mut removed = Vec::new();
        for workload in &args.workloads {
            match self
                .svc
                .remove_workload(&args.project, &args.environment, workload)
                .await
            {
                Ok(()) => removed.push(fqdn(workload, &args.environment, &args.project)),
                Err(err) => {
                    let mut body = serde_json::to_value(error_parts(err).1).unwrap_or_default();
                    body["removed"] = json!(removed);
                    return error_result(&body);
                }
            }
        }
        success(&json!({ "removed": removed }))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FaberMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_instructions(INSTRUCTIONS)
    }
}

/// The `/mcp` service. `allowed_hosts` is matched against the `Host` header.
pub fn service(
    svc: FaberService,
    allowed_hosts: Vec<String>,
    cancel: CancellationToken,
) -> StreamableHttpService<FaberMcp, LocalSessionManager> {
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts)
        .with_cancellation_token(cancel);
    StreamableHttpService::new(
        move || Ok(FaberMcp::new(svc.clone())),
        Arc::new(LocalSessionManager::default()),
        config,
    )
}
