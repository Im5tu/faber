//! HTTP client for the faber REST API (feature `client`).

use std::time::Duration;

use reqwest::{Method, RequestBuilder, Response, redirect};
use serde::{Serialize, de::DeserializeOwned};

use crate::name::validate_dns_label;
use crate::workload::{
    CreateProjectRequest, DeployRequest, DeployResponse, ProjectListResponse, ProjectResponse,
    WorkloadListResponse, WorkloadQuery, WorkloadResponse,
};
use crate::{
    ClusterStatus, CreateInviteRequest, ErrorBody, HealthResponse, InviteResponse, JoinRequest,
    LeaseResponse, SetLeaseRequest, error_code,
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("api error {status}: {}", body.message.as_deref().unwrap_or(&body.error))]
    Api { status: u16, body: ErrorBody },
    #[error("failed to decode response: {0}")]
    Decode(#[from] serde_json::Error),
    /// An argument the client refuses before it sends anything: a project,
    /// environment or workload name that is not a DNS label. The check also
    /// keeps `/` and `..` out of the URL path.
    #[error("invalid argument: {0}")]
    Invalid(String),
}

/// How long a [`FaberClient`] waits before a call fails with a timeout
/// ([`ClientError::Http`], `is_timeout()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTimeouts {
    /// Opening the TCP connection.
    pub connect: Duration,
    /// A whole call that only reads: `health`, `cluster_status`,
    /// `mesh_lease`, `project_list`, `project_get`, `workload_list` and
    /// `workload_get`.
    pub read: Duration,
    /// A whole call that writes through raft: `cluster_init`, `cluster_join`,
    /// `mesh_invite`, `mesh_set_lease`, `project_create`, `project_delete`,
    /// `environment_add`, `environment_remove`, `deploy` and
    /// `workload_delete`. The server waits up to 120 s for a join to finish
    /// and 10 s more for the node to see the result, so this must be longer.
    pub write: Duration,
}

impl Default for ClientTimeouts {
    /// 5 s to connect, 30 s for a read and 150 s for a write.
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(5),
            read: Duration::from_secs(30),
            write: Duration::from_secs(150),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FaberClient {
    base: String,
    http: reqwest::Client,
    timeouts: ClientTimeouts,
}

impl FaberClient {
    /// `endpoint` is a base URL such as `http://127.0.0.1:7400`. A bare
    /// `host:port` gets `http://` prepended.
    pub fn new(endpoint: &str) -> Result<Self, ClientError> {
        Self::with_timeouts(endpoint, ClientTimeouts::default())
    }

    /// [`Self::new`] with other timeouts than the defaults.
    pub fn with_timeouts(endpoint: &str, timeouts: ClientTimeouts) -> Result<Self, ClientError> {
        let trimmed = endpoint.trim().trim_end_matches('/');
        let base = if trimmed.contains("://") {
            trimmed.to_string()
        } else {
            format!("http://{trimmed}")
        };
        // The server forwards to the leader itself and never redirects, so a
        // redirect is reported as an error rather than followed.
        // The API is loopback or WireGuard only; an environment proxy must
        // never see these requests.
        let http = reqwest::Client::builder()
            .redirect(redirect::Policy::none())
            .no_proxy()
            .connect_timeout(timeouts.connect)
            .build()?;
        Ok(Self {
            base,
            http,
            timeouts,
        })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// `GET /api/v1/health`
    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        send_json(self.request(Method::GET, "/api/v1/health")).await
    }

    /// `GET /api/v1/cluster/status`
    pub async fn cluster_status(&self) -> Result<ClusterStatus, ClientError> {
        send_json(self.request(Method::GET, "/api/v1/cluster/status")).await
    }

    /// `POST /api/v1/cluster/init`: makes the node a single-node cluster.
    pub async fn cluster_init(&self) -> Result<ClusterStatus, ClientError> {
        send_json(self.request(Method::POST, "/api/v1/cluster/init")).await
    }

    /// `POST /api/v1/cluster/join`: joins the cluster that `peer` (a peer
    /// listener `ip:port`) belongs to.
    pub async fn cluster_join(&self, peer: &str) -> Result<ClusterStatus, ClientError> {
        let req = JoinRequest {
            peer: peer.to_string(),
        };
        send_json(self.request_json(Method::POST, "/api/v1/cluster/join", &req)?).await
    }

    /// `POST /api/v1/mesh/invites`: creates a single-use invite token for a
    /// new node, which will hold the name `req.name`. Only served on the
    /// loopback listener.
    pub async fn mesh_invite(
        &self,
        req: &CreateInviteRequest,
    ) -> Result<InviteResponse, ClientError> {
        send_json(self.request_json(Method::POST, "/api/v1/mesh/invites", req)?).await
    }

    /// `GET /api/v1/mesh/lease`: how long a node's lease lasts. Only served
    /// on the loopback listener.
    pub async fn mesh_lease(&self) -> Result<LeaseResponse, ClientError> {
        send_json(self.request(Method::GET, "/api/v1/mesh/lease")).await
    }

    /// `PUT /api/v1/mesh/lease`: sets the lease duration to `secs` seconds
    /// for the whole mesh. Only served on the loopback listener.
    pub async fn mesh_set_lease(&self, secs: u64) -> Result<LeaseResponse, ClientError> {
        let req = SetLeaseRequest { lease_secs: secs };
        send_json(self.request_json(Method::PUT, "/api/v1/mesh/lease", &req)?).await
    }

    /// `POST /api/v1/projects`: creates a project with the environments
    /// `req` names, or `staging` and `production` when it names none. Only
    /// served on the loopback listener, like every project and workload
    /// route.
    ///
    /// Every method that takes a project, environment or workload name
    /// fails with [`ClientError::Invalid`], before it sends anything, when
    /// the name is not a DNS label.
    pub async fn project_create(
        &self,
        req: &CreateProjectRequest,
    ) -> Result<ProjectResponse, ClientError> {
        check_name("project", &req.name)?;
        send_json(self.request_json(Method::POST, "/api/v1/projects", req)?).await
    }

    /// `GET /api/v1/projects`
    pub async fn project_list(&self) -> Result<ProjectListResponse, ClientError> {
        send_json(self.request(Method::GET, "/api/v1/projects")).await
    }

    /// `GET /api/v1/projects/{project}`
    pub async fn project_get(&self, project: &str) -> Result<ProjectResponse, ClientError> {
        let path = project_path(project)?;
        send_json(self.request(Method::GET, &path)).await
    }

    /// `DELETE /api/v1/projects/{project}`: removes a project whose
    /// environments are all empty.
    pub async fn project_delete(&self, project: &str) -> Result<(), ClientError> {
        let path = project_path(project)?;
        send_empty(self.request(Method::DELETE, &path)).await
    }

    /// `PUT /api/v1/projects/{project}/environments/{environment}`: adds an
    /// environment and returns the project as it is then.
    pub async fn environment_add(
        &self,
        project: &str,
        environment: &str,
    ) -> Result<ProjectResponse, ClientError> {
        let path = environment_path(project, environment)?;
        send_json(self.request(Method::PUT, &path)).await
    }

    /// `DELETE /api/v1/projects/{project}/environments/{environment}`:
    /// removes an empty environment.
    pub async fn environment_remove(
        &self,
        project: &str,
        environment: &str,
    ) -> Result<(), ClientError> {
        let path = environment_path(project, environment)?;
        send_empty(self.request(Method::DELETE, &path)).await
    }

    /// `POST /api/v1/projects/{project}/environments/{environment}/deploy`:
    /// creates or updates the workloads in `req`, and with `req.prune`
    /// removes the others.
    pub async fn deploy(
        &self,
        project: &str,
        environment: &str,
        req: &DeployRequest,
    ) -> Result<DeployResponse, ClientError> {
        let path = format!("{}/deploy", environment_path(project, environment)?);
        send_json(self.request_json(Method::POST, &path, req)?).await
    }

    /// `GET /api/v1/workloads`, filtered by `query.project` and
    /// `query.environment` when they are set.
    pub async fn workload_list(
        &self,
        query: &WorkloadQuery,
    ) -> Result<WorkloadListResponse, ClientError> {
        // A DNS label needs no escaping in a query either.
        let mut filters = Vec::new();
        if let Some(project) = &query.project {
            check_name("project", project)?;
            filters.push(format!("project={project}"));
        }
        if let Some(environment) = &query.environment {
            check_name("environment", environment)?;
            filters.push(format!("environment={environment}"));
        }
        let path = if filters.is_empty() {
            "/api/v1/workloads".to_string()
        } else {
            format!("/api/v1/workloads?{}", filters.join("&"))
        };
        send_json(self.request(Method::GET, &path)).await
    }

    /// `GET /api/v1/projects/{project}/environments/{environment}/workloads/{workload}`.
    /// The answer carries env values in clear.
    pub async fn workload_get(
        &self,
        project: &str,
        environment: &str,
        workload: &str,
    ) -> Result<WorkloadResponse, ClientError> {
        let path = workload_path(project, environment, workload)?;
        send_json(self.request(Method::GET, &path)).await
    }

    /// `DELETE /api/v1/projects/{project}/environments/{environment}/workloads/{workload}`
    pub async fn workload_delete(
        &self,
        project: &str,
        environment: &str,
        workload: &str,
    ) -> Result<(), ClientError> {
        let path = workload_path(project, environment, workload)?;
        send_empty(self.request(Method::DELETE, &path)).await
    }

    /// A request with the timeout of its kind: every `GET` only reads, and
    /// every other method writes through raft.
    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let timeout = if method == Method::GET {
            self.timeouts.read
        } else {
            self.timeouts.write
        };
        self.http
            .request(method, format!("{}{}", self.base, path))
            .timeout(timeout)
    }

    fn request_json<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: &T,
    ) -> Result<RequestBuilder, ClientError> {
        let bytes = serde_json::to_vec(body)?;
        Ok(self
            .request(method, path)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes))
    }
}

/// Refuses a name that is not a DNS label, saying which argument (`what`)
/// it was. Every name that goes into a URL passes through here first.
fn check_name(what: &str, name: &str) -> Result<(), ClientError> {
    // The rule's message starts with "name".
    validate_dns_label(name).map_err(|e| ClientError::Invalid(format!("{what} {e}")))
}

/// `/api/v1/projects/{project}`
fn project_path(project: &str) -> Result<String, ClientError> {
    check_name("project", project)?;
    Ok(format!("/api/v1/projects/{project}"))
}

/// `/api/v1/projects/{project}/environments/{environment}`
fn environment_path(project: &str, environment: &str) -> Result<String, ClientError> {
    let project = project_path(project)?;
    check_name("environment", environment)?;
    Ok(format!("{project}/environments/{environment}"))
}

/// `/api/v1/projects/{project}/environments/{environment}/workloads/{workload}`
fn workload_path(project: &str, environment: &str, workload: &str) -> Result<String, ClientError> {
    let environment = environment_path(project, environment)?;
    check_name("workload", workload)?;
    Ok(format!("{environment}/workloads/{workload}"))
}

async fn send_json<T: DeserializeOwned>(rb: RequestBuilder) -> Result<T, ClientError> {
    let resp = rb.send().await?;
    if !resp.status().is_success() {
        return Err(api_error(resp).await);
    }
    let bytes = resp.bytes().await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Sends a request whose 2xx answer has no body (204).
async fn send_empty(rb: RequestBuilder) -> Result<(), ClientError> {
    let resp = rb.send().await?;
    if !resp.status().is_success() {
        return Err(api_error(resp).await);
    }
    Ok(())
}

/// Builds a [`ClientError::Api`], falling back to an `internal` body holding
/// the raw text when the server did not send an [`ErrorBody`].
async fn api_error(resp: Response) -> ClientError {
    let status = resp.status().as_u16();
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => return ClientError::Http(e),
    };
    let body = serde_json::from_slice::<ErrorBody>(&bytes).unwrap_or_else(|_| {
        let text = String::from_utf8_lossy(&bytes).trim().to_string();
        let body = ErrorBody::new(error_code::INTERNAL);
        if text.is_empty() {
            body
        } else {
            body.with_message(text)
        }
    });
    ClientError::Api { status, body }
}
