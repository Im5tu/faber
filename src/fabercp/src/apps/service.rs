//! Project, environment and workload operations of [`FaberService`].
//!
//! Reads answer from this node's copy of the replicated state. Writes go
//! through raft on the leader. A follower forwards a write once, to the
//! leader's `POST /raft/cluster/workloads` ([`FaberService::peer_workloads`]),
//! and then waits up to 5 s until it has applied the entry the leader
//! wrote, so a read on the same node sees the write.
//!
//! The service runs the full model validation before it proposes anything.
//! The leader also runs the command on a copy of its state first: what
//! apply would refuse, and a deploy that would change nothing, is answered
//! from there and costs no log entry. Apply stays the judge of everything
//! that is proposed. No spec and no env value is ever logged.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use faber_api_types::error_code;
use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::{
    CreateProjectRequest, DEFAULT_ENVIRONMENTS, DeployRequest, DeployResponse, EnvironmentSummary,
    ProjectListResponse, ProjectResponse, Violation, WorkloadListResponse, WorkloadQuery,
    WorkloadResponse, WorkloadSummary, fqdn, limits, validate_workloads,
};

use faber_crypto::{ClusterKey, DataKey, key_id, unwrap, wrap};

use crate::apps::state::{KEY_PROJECTS, env_key};
use crate::apps::{Applied, EnvironmentRecord, ProjectIndex, ProjectRecord, StoredWorkload};
use crate::dto::{PeerWorkloadRequest, PeerWorkloadResponse};
use crate::mesh::Rejection;
use crate::raft::store::ReadError;
use crate::raft::store::keys::CreateDataKey;
use crate::raft::types::{Request, Response};
use crate::secrets::{env_scope, masked_workload, reveal_workload, seal_workload};
use crate::service::{FaberService, ServiceError, ServiceResult, now_secs, source_is_registered};

/// How long a follower waits to apply a write it forwarded before it
/// answers its own client.
const APPLY_WAIT: Duration = Duration::from_secs(5);
/// How many broken rules a refused deploy lists; the rest are counted.
const MAX_VIOLATIONS_LISTED: usize = 20;
/// How much of a refused name an error message repeats, in characters.
const MAX_NAME_SHOWN: usize = 64;

/// A write the leader has answered for.
struct Written {
    /// The log index of the entry that carried it. For a deploy that
    /// changed nothing and was not written, the generation of the
    /// environment.
    index: u64,
    /// What `ApplyWorkloads` did; `None` for the other five.
    applied: Option<DeployResponse>,
    /// The project after `CreateProject` and `AddEnvironment`, as the
    /// leader had it once it had applied the write.
    project: Option<ProjectResponse>,
}

/// What running a write on a copy of this node's state says about it.
enum Verdict {
    /// It would change something, or the state cannot tell: propose it.
    Propose,
    /// Apply would refuse it, with this error.
    Refuse(ServiceError),
    /// A deploy that would change nothing.
    Unchanged(DeployResponse),
}

impl FaberService {
    /// `POST /api/v1/projects`: creates a project with the environments
    /// the request names, or `staging` and `production` when it names
    /// none. `bad_request` for a name that is not a DNS label,
    /// `project_exists`, `workloads_exhausted`.
    pub async fn create_project(
        &self,
        req: CreateProjectRequest,
    ) -> ServiceResult<ProjectResponse> {
        let name = req.name;
        // An empty list is not an absent one: the write check refuses it.
        let environments = req
            .environments
            .unwrap_or_else(|| DEFAULT_ENVIRONMENTS.map(String::from).to_vec());
        let create = PeerWorkloadRequest::CreateProject {
            name: name.clone(),
            environments,
        };
        let written = self.write_workloads(create, false).await?;
        project_reply(written.project, || self.read_project(&name), &name)
    }

    /// `GET /api/v1/projects`: every project, sorted by name.
    pub async fn list_projects(&self) -> ServiceResult<ProjectListResponse> {
        let index = self.read_index()?;
        let projects = index
            .projects
            .iter()
            .map(|(name, record)| project_response(name, record))
            .collect();
        Ok(ProjectListResponse { projects })
    }

    /// `GET /api/v1/projects/{project}`. `NotFound` for an unknown project.
    pub async fn project(&self, project: &str) -> ServiceResult<ProjectResponse> {
        check_name("project", project)?;
        self.read_project(project)?
            .ok_or_else(|| missing_project(project))
    }

    /// `DELETE /api/v1/projects/{project}`. `NotFound` for an unknown
    /// project, `not_empty` while an environment holds a workload.
    pub async fn delete_project(&self, project: &str) -> ServiceResult<()> {
        let delete = PeerWorkloadRequest::DeleteProject {
            name: project.to_string(),
        };
        self.write_workloads(delete, false).await.map(drop)
    }

    /// `PUT /api/v1/projects/{project}/environments/{environment}`: adds
    /// the environment and returns the project as it is then.
    /// `project_exists` when the project already has it.
    pub async fn add_environment(
        &self,
        project: &str,
        environment: &str,
    ) -> ServiceResult<ProjectResponse> {
        let add = PeerWorkloadRequest::AddEnvironment {
            project: project.to_string(),
            name: environment.to_string(),
        };
        let written = self.write_workloads(add, false).await?;
        project_reply(written.project, || self.read_project(project), project)
    }

    /// `DELETE /api/v1/projects/{project}/environments/{environment}`.
    /// `not_empty` while the environment holds a workload.
    pub async fn remove_environment(&self, project: &str, environment: &str) -> ServiceResult<()> {
        let remove = PeerWorkloadRequest::RemoveEnvironment {
            project: project.to_string(),
            name: environment.to_string(),
        };
        self.write_workloads(remove, false).await.map(drop)
    }

    /// `POST /api/v1/projects/{project}/environments/{environment}/deploy`:
    /// validates the workloads and proposes `ApplyWorkloads`. `BadRequest`
    /// lists every rule the request breaks.
    pub async fn deploy(
        &self,
        project: &str,
        environment: &str,
        req: DeployRequest,
    ) -> ServiceResult<DeployResponse> {
        let apply = PeerWorkloadRequest::ApplyWorkloads {
            project: project.to_string(),
            environment: environment.to_string(),
            workloads: req.workloads,
            prune: req.prune,
        };
        let written = self.write_workloads(apply, false).await?;
        written
            .applied
            .ok_or_else(|| ServiceError::Internal("the deploy was written without a result".into()))
    }

    /// `GET /api/v1/workloads`: the workloads the query matches, sorted by
    /// project, environment and name. A filter that names a project or an
    /// environment the cluster does not have matches nothing.
    pub async fn list_workloads(
        &self,
        query: WorkloadQuery,
    ) -> ServiceResult<WorkloadListResponse> {
        if let Some(project) = &query.project {
            check_name("project", project)?;
        }
        if let Some(environment) = &query.environment {
            check_name("environment", environment)?;
        }
        let wanted =
            |filter: &Option<String>, name: &str| filter.as_deref().is_none_or(|want| want == name);
        // One read of every record, and no look at the index: the two
        // could be of different apply batches.
        let mut workloads = Vec::new();
        let records = self.state().environments();
        for (project, environment, record) in records.map_err(|e| read_error("env/*", e))? {
            if !wanted(&query.project, &project) || !wanted(&query.environment, &environment) {
                continue;
            }
            for (name, workload) in record.workloads {
                workloads.push(WorkloadSummary {
                    fqdn: fqdn(&name, &environment, &project),
                    project: project.clone(),
                    environment: environment.clone(),
                    name,
                    scale: workload.spec.scale,
                    expose: workload.spec.expose,
                    revision: workload.revision,
                });
            }
        }
        Ok(WorkloadListResponse { workloads })
    }

    /// `GET /api/v1/projects/{project}/environments/{environment}/workloads/{workload}`
    ///
    /// Every env value reads `***` unless `reveal`; then they are opened
    /// with this node's cluster key, and a node without it answers
    /// `key_unavailable`.
    pub async fn workload(
        &self,
        project: &str,
        environment: &str,
        workload: &str,
        reveal: bool,
    ) -> ServiceResult<WorkloadResponse> {
        check_name("project", project)?;
        check_name("environment", environment)?;
        check_name("workload", workload)?;
        let record = self.read_environment(project, environment)?;
        let found = record.and_then(|mut record| {
            let stored = record.workloads.remove(workload)?;
            Some((record.generation, stored))
        });
        let Some((generation, stored)) = found else {
            // Say what is missing: the project, the environment or the
            // workload.
            let index = self.read_index()?;
            let missing = missing_in(&index, project, Some(environment));
            return Err(missing.unwrap_or_else(|| {
                not_found(format!(
                    "workload {workload:?} of environment {environment:?} of project \
                     {project:?} does not exist"
                ))
            }));
        };
        let has_values = stored.env.values().any(|env| !env.values.is_empty());
        let spec = if reveal && has_values {
            let key = self.data_key(project, environment, false).await?;
            reveal_workload(&key, project, environment, workload, &stored).map_err(|_| {
                key_unavailable(format!(
                    "the env of workload {workload:?} cannot be opened with this node's cluster key"
                ))
            })?
        } else {
            masked_workload(&stored)
        };
        // Reports are stamped with the lease clock, so staleness is judged on
        // it. Without a mesh nothing has reported.
        let replicas = self.status_view()?.workload(project, environment, workload);
        Ok(WorkloadResponse {
            project: project.to_string(),
            environment: environment.to_string(),
            name: workload.to_string(),
            fqdn: fqdn(workload, environment, project),
            revision: stored.revision,
            generation,
            spec,
            replicas,
        })
    }

    /// `DELETE /api/v1/projects/{project}/environments/{environment}/workloads/{workload}`
    pub async fn remove_workload(
        &self,
        project: &str,
        environment: &str,
        workload: &str,
    ) -> ServiceResult<()> {
        let remove = PeerWorkloadRequest::RemoveWorkloads {
            project: project.to_string(),
            environment: environment.to_string(),
            names: vec![workload.to_string()],
        };
        self.write_workloads(remove, false).await.map(drop)
    }

    /// Handles `POST /raft/cluster/workloads`: any of the six writes, for
    /// a request from another node. The leader stamps `at` and proposes
    /// the command; a node that is not the leader forwards once unless the
    /// request was already `forwarded`.
    ///
    /// The only node with a reason to send it is a follower forwarding its
    /// own loopback request, so `source` must be the mesh IP of a
    /// registered node, forwarded or not. Anything else is `Forbidden`: the
    /// peer firewall also lets in machines that only hold an invite. Not
    /// checked when this node's own peer listener is loopback.
    pub async fn peer_workloads(
        &self,
        req: PeerWorkloadRequest,
        forwarded: bool,
        source: SocketAddr,
    ) -> ServiceResult<PeerWorkloadResponse> {
        if self.checks_peer_source() {
            let mesh = self.read_mesh()?;
            if !source_is_registered(mesh.as_ref(), source.ip()) {
                tracing::warn!(
                    source = %source.ip(),
                    forwarded,
                    op = op(&req),
                    "refused a workload write from an address that is no registered node"
                );
                return Err(ServiceError::Forbidden(format!(
                    "{} may not change projects or workloads",
                    source.ip()
                )));
            }
        }
        let written = self.write_workloads(req, forwarded).await?;
        Ok(PeerWorkloadResponse {
            index: written.index,
            applied: written.applied,
            project: written.project,
        })
    }

    /// One of the six writes, from this node's loopback API or from the
    /// peer route: the leader writes it ([`Self::write_as_leader`]), a
    /// follower forwards the request once and then waits until it has
    /// applied the entry itself.
    ///
    /// `req` is checked first, wherever it came from. Apply does not run
    /// the model validation, so the node that proposes always has, and a
    /// follower does not forward what the leader would refuse.
    async fn write_workloads(
        &self,
        req: PeerWorkloadRequest,
        forwarded: bool,
    ) -> ServiceResult<Written> {
        check_write(&req)?;
        let Some(leader_addr) = self.forward_target(forwarded)? else {
            return self.write_as_leader(req).await;
        };

        let bytes = self
            .post_peer(&leader_addr, "/raft/cluster/workloads", &req, true)
            .await?;
        let resp: PeerWorkloadResponse = serde_json::from_slice(&bytes).map_err(|e| {
            ServiceError::Internal(format!("bad workload response from {leader_addr}: {e}"))
        })?;
        // The leader has answered, and the reply to the client is all in
        // its answer. The wait is only there so that a read on this node
        // sees the write: the raft metrics say when this node has applied
        // it, and an index it never reaches costs the wait and no more.
        let wait = self.raft().wait(Some(APPLY_WAIT));
        let applied = wait.applied_index_at_least(Some(resp.index), "a forwarded write is applied");
        if applied.await.is_err() {
            tracing::debug!(
                index = resp.index,
                "a forwarded workload write is committed, but this node has not applied it \
                 within {APPLY_WAIT:?}"
            );
        }
        Ok(Written {
            index: resp.index,
            applied: resp.applied,
            project: resp.project,
        })
    }

    /// Proposes a checked write, unless this node's state already says how
    /// it ends: a write that apply would refuse and a deploy that would
    /// change nothing are answered here and cost no log entry.
    async fn write_as_leader(&self, req: PeerWorkloadRequest) -> ServiceResult<Written> {
        // Sealed once: the dry run and the proposal see the same bytes.
        let sealed = self.seal_deploy(&req).await?;
        match self.settled_verdict(&req, sealed.as_ref()).await? {
            Verdict::Refuse(refusal) => return Err(refusal),
            Verdict::Unchanged(applied) => {
                return Ok(Written {
                    index: applied.generation,
                    applied: Some(applied),
                    project: None,
                });
            }
            Verdict::Propose => {}
        }

        let op = op(&req);
        let answers_with = match &req {
            PeerWorkloadRequest::CreateProject { name, .. } => Some(name.clone()),
            PeerWorkloadRequest::AddEnvironment { project, .. } => Some(project.clone()),
            _ => None,
        };
        let (index, resp) = self.write(command(req, sealed, now_secs())?).await?;
        let applied = match resp {
            Response::Applied {
                generation,
                created,
                updated,
                unchanged,
                removed,
            } => Some(DeployResponse {
                generation,
                created,
                updated,
                unchanged,
                removed,
            }),
            Response::None => None,
            // Not printed: a response can hold a stored value.
            _ => {
                return Err(ServiceError::Internal(format!(
                    "unexpected response to {op} at index {index}"
                )));
            }
        };
        tracing::info!(op, index, "workload write applied");
        // `write` answers once this node has applied the entry, so its
        // state shows the write, even while the raft metrics still lag.
        let project = match answers_with {
            Some(name) => self.read_project(&name)?,
            None => None,
        };
        Ok(Written {
            index,
            applied,
            project,
        })
    }

    /// [`Self::dry_run`] of a write on a leader that has applied everything
    /// committed. A newly elected leader can be behind what the cluster
    /// agreed on, and must not refuse a write, or call a deploy unchanged,
    /// from such a state. So anything but `Propose` is looked at again
    /// after [`Self::settle`]. When that cannot confirm in time, the write
    /// is proposed and apply judges it.
    async fn settled_verdict(
        &self,
        req: &PeerWorkloadRequest,
        sealed: Option<&BTreeMap<String, StoredWorkload>>,
    ) -> ServiceResult<Verdict> {
        if matches!(self.dry_run(req, sealed)?, Verdict::Propose) || self.settle().await.is_err() {
            return Ok(Verdict::Propose);
        }
        self.dry_run(req, sealed)
    }

    /// Runs `req` on a copy of this node's state, with the functions apply
    /// uses, and writes nothing. `sealed` is what a deploy proposes in place
    /// of its plaintext workloads.
    fn dry_run(
        &self,
        req: &PeerWorkloadRequest,
        sealed: Option<&BTreeMap<String, StoredWorkload>>,
    ) -> ServiceResult<Verdict> {
        let mut index = self.read_index()?;
        if let Some(missing) = missing(&index, req) {
            return Ok(Verdict::Refuse(missing));
        }
        // The log index and the time do not matter to what is refused.
        let result = match req {
            PeerWorkloadRequest::CreateProject { name, environments } => {
                index.create_project(name, environments, 0, 0).map(|_| None)
            }
            PeerWorkloadRequest::DeleteProject { name } => index.delete_project(name).map(|_| None),
            PeerWorkloadRequest::AddEnvironment { project, name } => {
                index.add_environment(project, name, 0, 0).map(|_| None)
            }
            PeerWorkloadRequest::RemoveEnvironment { project, name } => {
                index.remove_environment(project, name).map(|()| None)
            }
            PeerWorkloadRequest::ApplyWorkloads {
                project,
                environment,
                prune,
                ..
            } => {
                let record = self.read_environment(project, environment)?;
                if !same_batch(&index, project, environment, record.as_ref()) {
                    return Ok(Verdict::Propose);
                }
                let workloads = sealed.cloned().ok_or_else(unsealed)?;
                index
                    .apply_workloads(record, project, environment, workloads, *prune, 0, 0)
                    // No record to write: nothing would change.
                    .map(|done| done.record.is_none().then_some(done.applied))
            }
            PeerWorkloadRequest::RemoveWorkloads {
                project,
                environment,
                names,
            } => {
                let record = self.read_environment(project, environment)?;
                if !same_batch(&index, project, environment, record.as_ref()) {
                    return Ok(Verdict::Propose);
                }
                index
                    .remove_workloads(record, project, environment, names, 0, 0)
                    .map(|_| None)
            }
        };
        Ok(match result {
            Err(rejection) => Verdict::Refuse(rejected(rejection)),
            Ok(Some(applied)) => Verdict::Unchanged(deploy_response(applied)),
            Ok(None) => Verdict::Propose,
        })
    }

    /// The workloads of a deploy with their env sealed under the data key of
    /// the environment, which the first deploy to it creates. `None` for
    /// every other write.
    ///
    /// A deploy to a project or environment that does not exist is refused
    /// here, so that no key is made for it.
    async fn seal_deploy(
        &self,
        req: &PeerWorkloadRequest,
    ) -> ServiceResult<Option<BTreeMap<String, StoredWorkload>>> {
        let PeerWorkloadRequest::ApplyWorkloads {
            project,
            environment,
            workloads,
            ..
        } = req
        else {
            return Ok(None);
        };
        if missing(&self.read_index()?, req).is_some() && self.settle().await.is_ok() {
            // Looked at again on a leader that has applied everything.
            if let Some(refusal) = missing(&self.read_index()?, req) {
                return Err(refusal);
            }
        }
        let key = self.data_key(project, environment, true).await?;
        let record = self.read_environment(project, environment)?;
        let mut sealed = BTreeMap::new();
        for (name, spec) in workloads {
            let previous = record.as_ref().and_then(|r| r.workloads.get(name));
            let workload = seal_workload(&key, project, environment, name, spec, previous)
                .map_err(|e| ServiceError::Internal(format!("sealing workload {name:?}: {e}")))?;
            sealed.insert(name.clone(), workload);
        }
        Ok(Some(sealed))
    }

    /// The cluster key of this node. The file is read on a blocking thread.
    async fn cluster_key(&self) -> ServiceResult<ClusterKey> {
        let provider = self.cluster_keys().clone();
        let read = tokio::task::spawn_blocking(move || provider.cluster_key()).await;
        read.map_err(|e| ServiceError::Internal(format!("reading the cluster key: {e}")))?
            .map_err(|e| key_unavailable(e.to_string()))
    }

    /// The data key of an environment, opened with the cluster key. With
    /// `create` the first call makes it: a new key is proposed and then
    /// the stored one is read back and used, so that two proposers agree on
    /// the winner's key.
    async fn data_key(
        &self,
        project: &str,
        environment: &str,
        create: bool,
    ) -> ServiceResult<DataKey> {
        let cluster = self.cluster_key().await?;
        let scope = env_scope(project, environment);
        let stored = match self.read_data_key(&scope)? {
            Some(stored) => stored,
            None if create => {
                let internal = |e: faber_crypto::CryptoError| {
                    ServiceError::Internal(format!("making a data key: {e}"))
                };
                let data = DataKey::generate().map_err(internal)?;
                let create = CreateDataKey {
                    scope: scope.clone(),
                    wrapped: wrap(&cluster, &data, &scope).map_err(internal)?,
                    key_id: key_id(&cluster),
                };
                self.propose(Request::CreateDataKey(create)).await?;
                self.read_data_key(&scope)?.ok_or_else(|| {
                    ServiceError::Internal("the data key is missing after it was written".into())
                })?
            }
            None => {
                return Err(key_unavailable(format!(
                    "environment {environment:?} of project {project:?} has no data key on this node"
                )));
            }
        };
        if stored.key_id != key_id(&cluster) {
            return Err(key_unavailable(
                "this node's cluster key is not the one that sealed the cluster's data keys",
            ));
        }
        unwrap(&cluster, &stored.wrapped, &scope).map_err(|_| {
            key_unavailable("this node's cluster key cannot open the data key of the environment")
        })
    }

    fn read_data_key(
        &self,
        scope: &str,
    ) -> ServiceResult<Option<crate::raft::store::keys::DataKeyRecord>> {
        let stored = self.state().data_key(scope);
        stored.map_err(|e| read_error(&crate::raft::store::keys::data_key_key(scope), e))
    }

    /// This node's copy of the project index.
    fn read_index(&self) -> ServiceResult<ProjectIndex> {
        let index = self.state().projects();
        index.map_err(|e| read_error(KEY_PROJECTS, e))
    }

    /// This node's copy of the record of an environment. Both names must
    /// have been checked.
    fn read_environment(
        &self,
        project: &str,
        environment: &str,
    ) -> ServiceResult<Option<EnvironmentRecord>> {
        let record = self.state().environment(project, environment);
        record.map_err(|e| read_error(&env_key(project, environment), e))
    }

    /// Project `name` in this node's copy of the index. `name` must have
    /// been checked.
    fn read_project(&self, name: &str) -> ServiceResult<Option<ProjectResponse>> {
        let index = self.read_index()?;
        Ok(index
            .projects
            .get(name)
            .map(|record| project_response(name, record)))
    }
}

/// The reply to `CreateProject` and `AddEnvironment`: project `name` as the
/// leader had it after the write, which it `relayed` with its answer. It
/// does not depend on what this node has applied, so a follower that is
/// behind answers a committed write like any other.
///
/// The leader sends none when the project was deleted again before it
/// looked. Then `local`, this node's own state, is asked.
fn project_reply(
    relayed: Option<ProjectResponse>,
    local: impl FnOnce() -> ServiceResult<Option<ProjectResponse>>,
    name: &str,
) -> ServiceResult<ProjectResponse> {
    match relayed {
        Some(project) => Ok(project),
        None => local()?.ok_or_else(|| missing_project(name)),
    }
}

/// A failed read of the record under `key`. The storage's own text is left
/// out: for a record that does not decode it can quote what is stored, and
/// an internal error is logged.
fn read_error(key: &str, e: ReadError) -> ServiceError {
    let what = match e {
        ReadError::Closed => "the storage is closed",
        ReadError::Storage(_) => "the stored record cannot be read",
    };
    ServiceError::Internal(format!("{key}: {what}"))
}

/// A 404 that says what is missing.
fn not_found(message: String) -> ServiceError {
    ServiceError::Rejected {
        code: error_code::NOT_FOUND.into(),
        message,
    }
}

fn missing_project(project: &str) -> ServiceError {
    not_found(format!("project {project:?} does not exist"))
}

/// The `not_found` for `project`, or for `environment` of it, when `index`
/// does not have it. The names must have been checked.
fn missing_in(
    index: &ProjectIndex,
    project: &str,
    environment: Option<&str>,
) -> Option<ServiceError> {
    let Some(record) = index.projects.get(project) else {
        return Some(missing_project(project));
    };
    let environment = environment.filter(|name| !record.environments.contains_key(*name))?;
    Some(not_found(format!(
        "environment {environment:?} of project {project:?} does not exist"
    )))
}

/// The `not_found` for the project or the environment `req` works on, when
/// `index` does not have it.
fn missing(index: &ProjectIndex, req: &PeerWorkloadRequest) -> Option<ServiceError> {
    let (project, environment) = match req {
        PeerWorkloadRequest::CreateProject { .. } => return None,
        PeerWorkloadRequest::DeleteProject { name } => (name, None),
        PeerWorkloadRequest::AddEnvironment { project, .. } => (project, None),
        PeerWorkloadRequest::RemoveEnvironment { project, name } => (project, Some(name)),
        PeerWorkloadRequest::ApplyWorkloads {
            project,
            environment,
            ..
        }
        | PeerWorkloadRequest::RemoveWorkloads {
            project,
            environment,
            ..
        } => (project, Some(environment)),
    };
    missing_in(index, project, environment.map(String::as_str))
}

/// Whether `record` is the one `index` describes. The two are separate
/// reads, and an apply batch can land between them.
fn same_batch(
    index: &ProjectIndex,
    project: &str,
    environment: &str,
    record: Option<&EnvironmentRecord>,
) -> bool {
    let entry = index
        .projects
        .get(project)
        .and_then(|p| p.environments.get(environment));
    match (entry, record) {
        (Some(entry), Some(record)) => entry.generation == record.generation,
        _ => false,
    }
}

/// What apply would refuse, as the error its `Response::Rejected` becomes.
fn rejected(rejection: Rejection) -> ServiceError {
    let Rejection { code, message } = rejection;
    match code.as_str() {
        error_code::BAD_REQUEST => ServiceError::BadRequest(message),
        _ => ServiceError::Rejected { code, message },
    }
}

fn deploy_response(applied: Applied) -> DeployResponse {
    DeployResponse {
        generation: applied.generation,
        created: applied.created,
        updated: applied.updated,
        unchanged: applied.unchanged,
        removed: applied.removed,
    }
}

fn project_response(name: &str, record: &ProjectRecord) -> ProjectResponse {
    ProjectResponse {
        name: name.to_string(),
        created_at: record.created_at,
        environments: record
            .environments
            .iter()
            .map(|(name, entry)| EnvironmentSummary {
                name: name.clone(),
                generation: entry.generation,
                workloads: entry.workloads,
                bytes: entry.bytes,
                updated_at: entry.updated_at,
            })
            .collect(),
    }
}

/// Checks that `name` is a DNS label. `what` says what it names. The state
/// reader builds storage keys from names as they are, so this runs before
/// every read as well as before every write.
fn check_name(what: &str, name: &str) -> ServiceResult<()> {
    validate_dns_label(name).map_err(|message| {
        // The name can be anything of any length here.
        let shown: String = name.chars().take(MAX_NAME_SHOWN).collect();
        ServiceError::BadRequest(format!("{what} {shown:?}: {message}"))
    })
}

/// Everything the service checks of a write before it proposes or forwards
/// it: every name, and for a deploy the full model validation.
fn check_write(req: &PeerWorkloadRequest) -> ServiceResult<()> {
    match req {
        PeerWorkloadRequest::CreateProject { name, environments } => {
            check_name("project", name)?;
            if environments.is_empty() {
                return Err(ServiceError::BadRequest(
                    "a project needs at least one environment".into(),
                ));
            }
            if environments.len() > limits::MAX_ENVIRONMENTS_PER_PROJECT {
                return Err(ServiceError::BadRequest(format!(
                    "a project has at most {} environments",
                    limits::MAX_ENVIRONMENTS_PER_PROJECT
                )));
            }
            environments
                .iter()
                .try_for_each(|environment| check_name("environment", environment))
        }
        PeerWorkloadRequest::DeleteProject { name } => check_name("project", name),
        PeerWorkloadRequest::AddEnvironment { project, name }
        | PeerWorkloadRequest::RemoveEnvironment { project, name } => {
            check_name("project", project)?;
            check_name("environment", name)
        }
        PeerWorkloadRequest::ApplyWorkloads {
            project,
            environment,
            workloads,
            ..
        } => {
            check_name("project", project)?;
            check_name("environment", environment)?;
            validate_workloads(workloads).map_err(invalid_workloads)
        }
        PeerWorkloadRequest::RemoveWorkloads {
            project,
            environment,
            names,
        } => {
            check_name("project", project)?;
            check_name("environment", environment)?;
            if names.is_empty() || names.len() > limits::MAX_WORKLOADS_PER_ENVIRONMENT {
                return Err(ServiceError::BadRequest(format!(
                    "between 1 and {} workloads can be removed at once",
                    limits::MAX_WORKLOADS_PER_ENVIRONMENT
                )));
            }
            names
                .iter()
                .try_for_each(|name| check_name("workload", name))
        }
    }
}

/// The `BadRequest` for a deploy the model refuses: the first
/// [`MAX_VIOLATIONS_LISTED`] rules it breaks as `path: message`, and how
/// many more there are.
fn invalid_workloads(violations: Vec<Violation>) -> ServiceError {
    let listed: Vec<String> = violations
        .iter()
        .take(MAX_VIOLATIONS_LISTED)
        .map(Violation::to_string)
        .collect();
    let mut message = format!("invalid workloads: {}", listed.join("; "));
    if violations.len() > listed.len() {
        message.push_str(&format!("; and {} more", violations.len() - listed.len()));
    }
    ServiceError::BadRequest(message)
}

/// The raft command of `req`, stamped with `at`. A deploy carries `sealed`
/// in place of the plaintext workloads of `req`, which never leave this
/// function.
fn command(
    req: PeerWorkloadRequest,
    sealed: Option<BTreeMap<String, StoredWorkload>>,
    at: u64,
) -> ServiceResult<Request> {
    Ok(match req {
        PeerWorkloadRequest::CreateProject { name, environments } => Request::CreateProject {
            name,
            environments,
            at,
        },
        PeerWorkloadRequest::DeleteProject { name } => Request::DeleteProject { name },
        PeerWorkloadRequest::AddEnvironment { project, name } => {
            Request::AddEnvironment { project, name, at }
        }
        PeerWorkloadRequest::RemoveEnvironment { project, name } => {
            Request::RemoveEnvironment { project, name }
        }
        PeerWorkloadRequest::ApplyWorkloads {
            project,
            environment,
            prune,
            ..
        } => Request::ApplyWorkloads {
            project,
            environment,
            workloads: sealed.ok_or_else(unsealed)?,
            prune,
            at,
        },
        PeerWorkloadRequest::RemoveWorkloads {
            project,
            environment,
            names,
        } => Request::RemoveWorkloads {
            project,
            environment,
            names,
            at,
        },
    })
}

fn unsealed() -> ServiceError {
    ServiceError::Internal("a deploy reached the log step without its env sealed".into())
}

/// The error for a cluster key that is missing or does not fit. The
/// message never holds key material.
fn key_unavailable(message: impl Into<String>) -> ServiceError {
    ServiceError::Rejected {
        code: error_code::KEY_UNAVAILABLE.into(),
        message: message.into(),
    }
}

/// The name of the write, for logs. A request is never logged whole: it
/// can carry specs with env values.
fn op(req: &PeerWorkloadRequest) -> &'static str {
    match req {
        PeerWorkloadRequest::CreateProject { .. } => "create_project",
        PeerWorkloadRequest::DeleteProject { .. } => "delete_project",
        PeerWorkloadRequest::AddEnvironment { .. } => "add_environment",
        PeerWorkloadRequest::RemoveEnvironment { .. } => "remove_environment",
        PeerWorkloadRequest::ApplyWorkloads { .. } => "apply_workloads",
        PeerWorkloadRequest::RemoveWorkloads { .. } => "remove_workloads",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use faber_api_types::workload::{Exposure, Scale, Strategy, WorkloadSpec, validate_workloads};
    use faber_api_types::{CreateInviteRequest, error_code};
    use faber_container_runtime::ContainerRuntimeSpec;
    use serde_json::json;

    use super::*;
    use crate::mesh::{MeshConfig, NodeRecord, generate_keypair};
    use crate::raft::types::Request;
    use crate::service::ServiceError;
    use crate::testing::TestNode;

    /// A workload of one container that runs `image`.
    fn spec(image: &str) -> WorkloadSpec {
        serde_json::from_value(json!({"containers": [{"name": "main", "image": image}]})).unwrap()
    }

    /// A workload the model validation refuses: it has no container.
    fn no_containers() -> WorkloadSpec {
        serde_json::from_value(json!({"containers": []})).unwrap()
    }

    fn workloads(named: &[(&str, WorkloadSpec)]) -> BTreeMap<String, WorkloadSpec> {
        named
            .iter()
            .map(|(name, spec)| (name.to_string(), spec.clone()))
            .collect()
    }

    fn deploy_req(named: &[(&str, WorkloadSpec)], prune: bool) -> DeployRequest {
        DeployRequest {
            workloads: workloads(named),
            prune,
        }
    }

    fn create(name: &str, environments: Option<&[&str]>) -> CreateProjectRequest {
        CreateProjectRequest {
            name: name.into(),
            environments: environments.map(|e| e.iter().map(|e| e.to_string()).collect()),
        }
    }

    /// `ApplyWorkloads` to `staging` of `project`, without `prune`.
    fn apply_req(project: &str, named: &[(&str, WorkloadSpec)]) -> PeerWorkloadRequest {
        PeerWorkloadRequest::ApplyWorkloads {
            project: project.into(),
            environment: "staging".into(),
            workloads: workloads(named),
            prune: false,
        }
    }

    fn query(project: Option<&str>, environment: Option<&str>) -> WorkloadQuery {
        WorkloadQuery {
            project: project.map(str::to_string),
            environment: environment.map(str::to_string),
        }
    }

    fn source() -> SocketAddr {
        "127.0.0.1:40000".parse().unwrap()
    }

    /// A node that leads a cluster of one, with project `shop`.
    async fn shop() -> TestNode {
        let node = TestNode::start_initialized().await;
        node.service
            .create_project(create("shop", None))
            .await
            .unwrap();
        node
    }

    /// The workloads `query` matches, as `project/environment/name`.
    async fn list(node: &TestNode, query: WorkloadQuery) -> Vec<String> {
        let found = node.service.list_workloads(query).await.unwrap();
        found
            .workloads
            .into_iter()
            .map(|w| format!("{}/{}/{}", w.project, w.environment, w.name))
            .collect()
    }

    /// The index of a new log entry on `node`. Two calls differ by one
    /// when nothing was written in between.
    async fn next_index(node: &TestNode) -> u64 {
        let probe = Request::Set {
            key: "probe".into(),
            value: json!(1),
        };
        node.service.write(probe).await.unwrap().0
    }

    /// The message of the `not_found` that `got` must be.
    #[track_caller]
    fn missing<T: std::fmt::Debug>(got: ServiceResult<T>) -> String {
        match got {
            Err(ServiceError::Rejected { code, message }) if code == error_code::NOT_FOUND => {
                message
            }
            other => panic!("{other:?}"),
        }
    }

    /// The message of the `BadRequest` that `got` must be.
    #[track_caller]
    fn bad_request<T: std::fmt::Debug>(got: ServiceResult<T>, what: &str) -> String {
        match got {
            Err(ServiceError::BadRequest(message)) => message,
            other => panic!("{what:?}: {other:?}"),
        }
    }

    #[track_caller]
    fn assert_rejected<T: std::fmt::Debug>(got: ServiceResult<T>, code: &str) {
        match got {
            Err(ServiceError::Rejected { code: got, .. }) => assert_eq!(got, code),
            other => panic!("{code}: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_project_fills_the_default_environments() {
        let node = TestNode::start_initialized().await;
        let svc = &node.service;
        assert_eq!(svc.list_projects().await.unwrap().projects, vec![]);

        let shop = svc.create_project(create("shop", None)).await.unwrap();
        assert_eq!(shop.name, "shop");
        assert!(shop.created_at > 0);
        let names: Vec<&str> = shop.environments.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["production", "staging"]);
        for environment in &shop.environments {
            assert_eq!(environment.workloads, 0);
            assert!(environment.generation > 0);
            assert_eq!(environment.updated_at, shop.created_at);
        }
        // The reply is the project as a read gives it.
        assert_eq!(svc.project("shop").await.unwrap(), shop);

        let blog = svc
            .create_project(create("blog", Some(&["dev"])))
            .await
            .unwrap();
        assert_eq!(blog.environments.len(), 1);
        assert_eq!(blog.environments[0].name, "dev");
        // Sorted by name.
        assert_eq!(
            svc.list_projects().await.unwrap().projects,
            vec![blog, shop]
        );

        // An explicitly empty list is not an absent one.
        let empty = svc.create_project(create("wiki", Some(&[]))).await;
        bad_request(empty, "no environments");
        assert_eq!(
            missing(svc.project("wiki").await),
            r#"project "wiki" does not exist"#
        );
        assert_rejected(
            svc.create_project(create("shop", None)).await,
            error_code::PROJECT_EXISTS,
        );
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn environments_are_added_and_removed() {
        let node = shop().await;
        let svc = &node.service;
        let got = svc.add_environment("shop", "dev").await.unwrap();
        let names: Vec<&str> = got.environments.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["dev", "production", "staging"]);
        assert_eq!(svc.project("shop").await.unwrap(), got);

        assert_rejected(
            svc.add_environment("shop", "dev").await,
            error_code::PROJECT_EXISTS,
        );
        assert_eq!(
            missing(svc.add_environment("blog", "dev").await),
            r#"project "blog" does not exist"#
        );
        svc.remove_environment("shop", "dev").await.unwrap();
        assert_eq!(
            missing(svc.remove_environment("shop", "dev").await),
            r#"environment "dev" of project "shop" does not exist"#
        );

        let web = deploy_req(&[("web", spec("nginx"))], false);
        svc.deploy("shop", "staging", web).await.unwrap();
        assert_rejected(
            svc.remove_environment("shop", "staging").await,
            error_code::NOT_EMPTY,
        );
        assert_rejected(svc.delete_project("shop").await, error_code::NOT_EMPTY);
        svc.remove_workload("shop", "staging", "web").await.unwrap();
        svc.delete_project("shop").await.unwrap();
        missing(svc.project("shop").await);
        missing(svc.delete_project("shop").await);
        node.shutdown().await;
    }

    /// A deploy that changes nothing keeps the generation of the
    /// environment and is not written: the reply carries the stored
    /// generation, and the peer reply has it as its index too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unchanged_deploy_answers_the_stored_generation() {
        let node = shop().await;
        let svc = &node.service;
        let web = [("web", spec("nginx"))];

        let first = svc
            .deploy("shop", "staging", deploy_req(&web, false))
            .await
            .unwrap();
        assert_eq!(first.created, vec!["web"]);
        let stored = svc.workload("shop", "staging", "web", true).await.unwrap();
        assert_eq!(stored.generation, first.generation);
        assert_eq!(stored.revision, first.generation);

        let again = svc
            .deploy("shop", "staging", deploy_req(&web, false))
            .await
            .unwrap();
        assert_eq!(
            again,
            DeployResponse {
                generation: first.generation,
                unchanged: vec!["web".into()],
                ..DeployResponse::default()
            }
        );

        // The same through the peer route, forwarded or not: `index` is
        // the entry that last changed the environment, so a follower that
        // waits for it reads what the reply describes.
        let before = next_index(&node).await;
        for forwarded in [false, true] {
            let got = svc
                .peer_workloads(apply_req("shop", &web), forwarded, source())
                .await
                .unwrap();
            assert_eq!(got.index, first.generation, "{got:?}");
            assert_eq!(got.applied, Some(again.clone()));
            assert_eq!(got.project, None);
        }
        // With `prune` too, as long as nothing is pruned; and a deploy of
        // nothing.
        let pruning = deploy_req(&web, true);
        let pruned = svc.deploy("shop", "staging", pruning).await.unwrap();
        assert_eq!(pruned, again);
        let nothing = deploy_req(&[], false);
        let nothing = svc.deploy("shop", "staging", nothing).await.unwrap();
        assert_eq!(nothing.generation, first.generation);
        assert_eq!(nothing.unchanged, Vec::<String>::new());
        // None of them was written.
        assert_eq!(next_index(&node).await, before + 1);
        let stored = svc.workload("shop", "staging", "web", true).await.unwrap();
        assert_eq!(stored.generation, first.generation);

        // A change moves it to the index of its entry.
        let changed = apply_req("shop", &[("web", spec("nginx:2"))]);
        let changed = svc.peer_workloads(changed, false, source()).await.unwrap();
        let applied = changed.applied.unwrap();
        assert_eq!(applied.generation, changed.index);
        assert_eq!(applied.updated, vec!["web"]);
        node.shutdown().await;
    }

    /// Only `ApplyWorkloads` reports what it did, and only the two writes
    /// that answer with the project carry it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_other_peer_writes_answer_their_index_only() {
        let node = TestNode::start_initialized().await;
        let svc = &node.service;
        let mut last = 0;
        for req in [
            PeerWorkloadRequest::CreateProject {
                name: "shop".into(),
                environments: vec!["staging".into()],
            },
            PeerWorkloadRequest::AddEnvironment {
                project: "shop".into(),
                name: "dev".into(),
            },
            PeerWorkloadRequest::RemoveEnvironment {
                project: "shop".into(),
                name: "dev".into(),
            },
            apply_req("shop", &[("web", spec("nginx"))]),
            PeerWorkloadRequest::RemoveWorkloads {
                project: "shop".into(),
                environment: "staging".into(),
                names: vec!["web".into()],
            },
            PeerWorkloadRequest::DeleteProject {
                name: "shop".into(),
            },
        ] {
            let apply = matches!(req, PeerWorkloadRequest::ApplyWorkloads { .. });
            let environments = match &req {
                PeerWorkloadRequest::CreateProject { .. } => Some(vec!["staging"]),
                PeerWorkloadRequest::AddEnvironment { .. } => Some(vec!["dev", "staging"]),
                _ => None,
            };
            let got = svc.peer_workloads(req, false, source()).await.unwrap();
            assert!(got.index > last, "{got:?}");
            assert_eq!(got.applied.is_some(), apply, "{got:?}");
            // The project as the leader has it right after its own apply.
            let project = got.project.as_ref();
            let names = project.map(|p| p.environments.iter().map(|e| e.name.as_str()));
            assert_eq!(names.map(Vec::from_iter), environments, "{got:?}");
            if let Some(project) = project {
                assert_eq!(svc.project("shop").await.as_ref(), Ok(project));
                let newest = project.environments.iter().map(|e| e.generation).max();
                assert_eq!(newest, Some(got.index));
            }
            // `index` is one this node applies: what a follower waits for.
            svc.raft()
                .wait(Some(APPLY_WAIT))
                .applied_index_at_least(Some(got.index), "the write is applied")
                .await
                .unwrap();
            last = got.index;
        }
        assert_eq!(svc.list_projects().await.unwrap().projects, vec![]);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deploy_lists_what_the_model_refuses() {
        let node = shop().await;
        let svc = &node.service;

        let one = deploy_req(&[("web", spec("nginx")), ("api", no_containers())], false);
        let message = bad_request(svc.deploy("shop", "staging", one).await, "one");
        assert!(message.contains("api.containers: "), "{message}");
        assert!(!message.contains(" more"), "{message}");

        // More than are listed: the first 20 and a count of the rest.
        let named: Vec<(String, WorkloadSpec)> = (10..40)
            .map(|n| (format!("w{n}"), no_containers()))
            .collect();
        let named: Vec<(&str, WorkloadSpec)> =
            named.iter().map(|(n, s)| (n.as_str(), s.clone())).collect();
        let total = validate_workloads(&workloads(&named)).unwrap_err().len();
        assert_eq!(total, 30);
        let many = deploy_req(&named, true);
        let message = bad_request(svc.deploy("shop", "staging", many).await, "many");
        assert!(message.contains("w10.containers: "), "{message}");
        assert!(message.contains("w29.containers: "), "{message}");
        assert!(!message.contains("w30.containers"), "{message}");
        assert!(message.ends_with("; and 10 more"), "{message}");

        // Nothing of either was stored, not even the valid workload.
        assert_eq!(list(&node, query(None, None)).await, Vec::<String>::new());
        node.shutdown().await;
    }

    /// Apply does not run the model validation, so the node that proposes
    /// must: a request posted straight to the peer route is checked there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_peer_route_validates_before_it_proposes() {
        let node = shop().await;
        let svc = &node.service;
        let before = next_index(&node).await;

        let bad_writes = [
            apply_req("shop", &[("web", no_containers())]),
            apply_req("shop", &[("Web", spec("nginx"))]),
            apply_req("Shop", &[("web", spec("nginx"))]),
            PeerWorkloadRequest::ApplyWorkloads {
                project: "shop".into(),
                environment: "../staging".into(),
                workloads: workloads(&[("web", spec("nginx"))]),
                prune: false,
            },
            PeerWorkloadRequest::CreateProject {
                name: "my_blog".into(),
                environments: vec!["dev".into()],
            },
            PeerWorkloadRequest::CreateProject {
                name: "blog".into(),
                environments: vec!["dev".into(), "a/b".into()],
            },
            PeerWorkloadRequest::CreateProject {
                name: "blog".into(),
                environments: vec![],
            },
            PeerWorkloadRequest::CreateProject {
                name: "blog".into(),
                environments: (0..17).map(|n| format!("e{n}")).collect(),
            },
            PeerWorkloadRequest::DeleteProject { name: "".into() },
            PeerWorkloadRequest::AddEnvironment {
                project: "shop".into(),
                name: "a".repeat(64),
            },
            PeerWorkloadRequest::AddEnvironment {
                project: "-shop".into(),
                name: "dev".into(),
            },
            PeerWorkloadRequest::RemoveEnvironment {
                project: "shop".into(),
                name: "Staging".into(),
            },
            PeerWorkloadRequest::RemoveEnvironment {
                project: "shop.".into(),
                name: "staging".into(),
            },
            PeerWorkloadRequest::RemoveWorkloads {
                project: "shop".into(),
                environment: "staging".into(),
                names: vec!["web".into(), "db_backup".into()],
            },
            PeerWorkloadRequest::RemoveWorkloads {
                project: "shop".into(),
                environment: "staging".into(),
                names: vec![],
            },
            PeerWorkloadRequest::RemoveWorkloads {
                project: "shop".into(),
                environment: "staging".into(),
                names: vec!["web".to_string(); 65],
            },
            PeerWorkloadRequest::RemoveWorkloads {
                project: "shop".into(),
                environment: "sta ging".into(),
                names: vec!["web".into()],
            },
        ];
        for req in bad_writes {
            for forwarded in [false, true] {
                let what = format!("{req:?}");
                let got = svc.peer_workloads(req.clone(), forwarded, source()).await;
                bad_request(got, &what);
            }
        }
        // None of them reached the log.
        assert_eq!(next_index(&node).await, before + 1);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn names_are_checked_before_anything_is_read_or_written() {
        let node = shop().await;
        let svc = &node.service;
        let before = next_index(&node).await;
        let web = || deploy_req(&[("web", spec("nginx"))], false);
        let too_long = "a".repeat(64);
        for bad in ["", "Shop", "a_b", "a.b", "a/b", "..", "-a", "a-", &too_long] {
            bad_request(svc.create_project(create(bad, None)).await, bad);
            let blog = create("blog", Some(&["dev", bad]));
            bad_request(svc.create_project(blog).await, bad);
            bad_request(svc.project(bad).await, bad);
            bad_request(svc.delete_project(bad).await, bad);
            bad_request(svc.add_environment(bad, "dev").await, bad);
            bad_request(svc.add_environment("shop", bad).await, bad);
            bad_request(svc.remove_environment(bad, "staging").await, bad);
            bad_request(svc.remove_environment("shop", bad).await, bad);
            bad_request(svc.deploy(bad, "staging", web()).await, bad);
            bad_request(svc.deploy("shop", bad, web()).await, bad);
            let named = deploy_req(&[(bad, spec("nginx"))], false);
            bad_request(svc.deploy("shop", "staging", named).await, bad);
            bad_request(svc.workload(bad, "staging", "web", true).await, bad);
            bad_request(svc.workload("shop", bad, "web", true).await, bad);
            bad_request(svc.workload("shop", "staging", bad, true).await, bad);
            bad_request(svc.remove_workload(bad, "staging", "web").await, bad);
            bad_request(svc.remove_workload("shop", bad, "web").await, bad);
            bad_request(svc.remove_workload("shop", "staging", bad).await, bad);
            bad_request(svc.list_workloads(query(Some(bad), None)).await, bad);
            bad_request(svc.list_workloads(query(None, Some(bad))).await, bad);
        }
        // A long name is not echoed whole.
        let huge = "x".repeat(100_000);
        let message = bad_request(svc.project(&huge).await, "huge");
        assert!(message.len() < 300, "{}", message.len());
        assert_eq!(next_index(&node).await, before + 1);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workloads_are_read_from_state() {
        let node = shop().await;
        let svc = &node.service;
        svc.create_project(create("blog", Some(&["staging", "dev"])))
            .await
            .unwrap();
        let mut exposed = spec("api:1");
        exposed.expose = Exposure::Cluster;
        exposed.scale = Scale::Replicas {
            count: 3,
            strategy: Strategy::Spread,
        };
        let both = deploy_req(&[("web", spec("nginx")), ("api", exposed.clone())], false);
        let staging = svc.deploy("shop", "staging", both).await.unwrap();
        let web = || deploy_req(&[("web", spec("nginx"))], false);
        svc.deploy("shop", "production", web()).await.unwrap();
        let site = deploy_req(&[("site", spec("ghost"))], false);
        svc.deploy("blog", "staging", site).await.unwrap();

        // Sorted by project, environment and name.
        assert_eq!(
            list(&node, query(None, None)).await,
            vec![
                "blog/staging/site",
                "shop/production/web",
                "shop/staging/api",
                "shop/staging/web",
            ]
        );
        assert_eq!(
            list(&node, query(Some("shop"), None)).await,
            vec![
                "shop/production/web",
                "shop/staging/api",
                "shop/staging/web"
            ]
        );
        // An environment without a project is that one in every project.
        assert_eq!(
            list(&node, query(None, Some("staging"))).await,
            vec!["blog/staging/site", "shop/staging/api", "shop/staging/web"]
        );
        assert_eq!(
            list(&node, query(Some("shop"), Some("production"))).await,
            vec!["shop/production/web"]
        );
        // A filter that names nothing the cluster has matches nothing, as
        // does one that names an environment without workloads.
        for (project, environment) in [
            (Some("wiki"), None),
            (None, Some("qa")),
            (Some("blog"), Some("production")),
            (Some("wiki"), Some("staging")),
            (Some("blog"), Some("dev")),
        ] {
            assert_eq!(
                list(&node, query(project, environment)).await,
                Vec::<String>::new(),
                "{project:?} {environment:?}"
            );
        }

        let all = svc.list_workloads(query(None, None)).await.unwrap();
        let api = &all.workloads[2];
        assert_eq!(api.fqdn, "api.staging.shop.faber.internal");
        assert_eq!(api.scale, exposed.scale);
        assert_eq!(api.expose, exposed.expose);
        assert_eq!(api.revision, staging.generation);

        assert_eq!(
            svc.workload("shop", "staging", "api", true).await.unwrap(),
            WorkloadResponse {
                project: "shop".into(),
                environment: "staging".into(),
                name: "api".into(),
                fqdn: "api.staging.shop.faber.internal".into(),
                revision: staging.generation,
                generation: staging.generation,
                spec: exposed,
                replicas: Vec::new(),
            }
        );
        for (project, environment, workload) in [
            ("wiki", "staging", "web"),
            ("shop", "dev", "web"),
            ("shop", "staging", "db"),
            ("shop", "production", "api"),
        ] {
            // Both say what is missing, each in its own words.
            let read = missing(svc.workload(project, environment, workload, true).await);
            let removed = missing(svc.remove_workload(project, environment, workload).await);
            let want = match (project, environment) {
                ("wiki", _) => r#"project "wiki" does not exist"#.to_string(),
                (_, "dev") => r#"environment "dev" of project "shop" does not exist"#.to_string(),
                _ => format!("workload {workload:?}"),
            };
            assert!(read.starts_with(&want), "{read}");
            assert!(removed.starts_with(&want), "{removed}");
        }
        assert_eq!(
            missing(svc.deploy("wiki", "staging", web()).await),
            r#"project "wiki" does not exist"#
        );
        assert_eq!(
            missing(svc.deploy("shop", "dev", web()).await),
            r#"environment "dev" of project "shop" does not exist"#
        );

        // `prune` removes what the deploy does not name.
        let newer = deploy_req(&[("web", spec("nginx:2"))], true);
        let pruned = svc.deploy("shop", "staging", newer).await.unwrap();
        assert_eq!(pruned.updated, vec!["web"]);
        assert_eq!(pruned.removed, vec!["api"]);
        assert_eq!(
            list(&node, query(Some("shop"), Some("staging"))).await,
            vec!["shop/staging/web"]
        );
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_without_a_leader_is_no_leader() {
        let node = TestNode::start().await;
        let svc = &node.service;
        assert_eq!(
            svc.create_project(create("shop", None)).await,
            Err(ServiceError::NoLeader)
        );
        assert_eq!(
            svc.delete_project("shop").await,
            Err(ServiceError::NoLeader)
        );
        for forwarded in [false, true] {
            let req = apply_req("shop", &[("web", spec("nginx"))]);
            assert_eq!(
                svc.peer_workloads(req, forwarded, source()).await,
                Err(ServiceError::NoLeader)
            );
        }
        // Reads answer from what this node has.
        assert_eq!(svc.list_projects().await.unwrap().projects, vec![]);
        missing(svc.project("shop").await);
        assert_eq!(list(&node, query(None, None)).await, Vec::<String>::new());
        node.shutdown().await;
    }

    /// What apply would refuse is refused by the leader from its own
    /// state: a synced log entry on every node is too much for a request
    /// that changes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_the_state_refuses_is_not_proposed() {
        let node = shop().await;
        let svc = &node.service;
        let web = || deploy_req(&[("web", spec("nginx"))], false);
        svc.deploy("shop", "staging", web()).await.unwrap();
        let before = next_index(&node).await;

        // Something that does not exist.
        missing(svc.delete_project("blog").await);
        missing(svc.add_environment("blog", "dev").await);
        missing(svc.remove_environment("blog", "staging").await);
        missing(svc.remove_environment("shop", "dev").await);
        missing(svc.deploy("blog", "staging", web()).await);
        missing(svc.deploy("shop", "dev", web()).await);
        missing(svc.remove_workload("blog", "staging", "web").await);
        missing(svc.remove_workload("shop", "dev", "web").await);
        missing(svc.remove_workload("shop", "staging", "api").await);
        missing(svc.remove_workload("shop", "production", "web").await);

        // Something that exists already, or is not empty.
        assert_rejected(
            svc.create_project(create("shop", Some(&["dev"]))).await,
            error_code::PROJECT_EXISTS,
        );
        assert_rejected(
            svc.add_environment("shop", "staging").await,
            error_code::PROJECT_EXISTS,
        );
        for got in [
            svc.delete_project("shop").await,
            svc.remove_environment("shop", "staging").await,
        ] {
            match got {
                Err(ServiceError::Rejected { code, message }) => {
                    assert_eq!(code, error_code::NOT_EMPTY);
                    assert!(message.ends_with("has 1 workload"), "{message}");
                }
                other => panic!("{other:?}"),
            }
        }
        // A limit.
        let full: Vec<String> = (0..16).map(|n| format!("e{n}")).collect();
        let full: Vec<&str> = full.iter().map(String::as_str).collect();
        svc.create_project(create("full", Some(&full)))
            .await
            .unwrap();
        assert_rejected(
            svc.add_environment("full", "one-more").await,
            error_code::WORKLOADS_EXHAUSTED,
        );

        // The same through the peer route, forwarded or not.
        for forwarded in [false, true] {
            let unknown = apply_req("blog", &[("web", spec("nginx"))]);
            missing(svc.peer_workloads(unknown, forwarded, source()).await);
            let taken = PeerWorkloadRequest::CreateProject {
                name: "shop".into(),
                environments: vec!["dev".into()],
            };
            assert_rejected(
                svc.peer_workloads(taken, forwarded, source()).await,
                error_code::PROJECT_EXISTS,
            );
        }

        // Only `full` was written since.
        assert_eq!(next_index(&node).await, before + 2);
        // And what the state does not refuse is still written.
        let changed = deploy_req(&[("web", spec("nginx:2"))], false);
        let changed = svc.deploy("shop", "staging", changed).await.unwrap();
        assert_eq!(changed.generation, before + 3);
        node.shutdown().await;
    }

    /// A follower answers `CreateProject` and `AddEnvironment` with the
    /// project the leader sent along. Its own state is not asked, so it
    /// does not matter that it has not applied the write.
    #[test]
    fn a_relayed_project_is_the_reply_whatever_this_node_has_applied() {
        let shop = ProjectResponse {
            name: "shop".into(),
            created_at: 1_000,
            environments: vec![EnvironmentSummary {
                name: "staging".into(),
                generation: 7,
                workloads: 0,
                bytes: 53,
                updated_at: 1_000,
            }],
        };
        let behind = || -> ServiceResult<Option<ProjectResponse>> {
            panic!("the state of this node is not asked")
        };
        assert_eq!(
            project_reply(Some(shop.clone()), behind, "shop"),
            Ok(shop.clone())
        );

        // Without one, this node's state answers.
        let mut newer = shop.clone();
        newer.created_at = 2_000;
        let local = || Ok(Some(newer.clone()));
        assert_eq!(project_reply(None, local, "shop"), Ok(newer.clone()));
        assert_eq!(
            missing(project_reply(None, || Ok(None), "shop")),
            r#"project "shop" does not exist"#
        );
        let closed = || Err(ServiceError::Internal("closed".into()));
        assert_eq!(
            project_reply(None, closed, "shop"),
            Err(ServiceError::Internal("closed".into()))
        );
    }

    /// The text of a storage error can quote a stored record, env values
    /// and all. An internal error is logged, so it gets the key and a fixed
    /// description.
    #[test]
    fn a_failed_read_does_not_repeat_the_storage_error() {
        let decode = ReadError::Storage(r#"invalid type: string "s3cret", expected u32"#.into());
        assert_eq!(
            read_error("env/shop/staging", decode),
            ServiceError::Internal("env/shop/staging: the stored record cannot be read".into())
        );
        assert_eq!(
            read_error("projects", ReadError::Closed),
            ServiceError::Internal("projects: the storage is closed".into())
        );
    }

    async fn seed_mesh(node: &TestNode) {
        let (_, public_key) = generate_keypair().unwrap();
        node.service
            .propose(Request::InitCluster {
                mesh: MeshConfig::default(),
                founder: NodeRecord {
                    node_id: 1,
                    public_key,
                    mesh_ip: "10.99.0.1".parse().unwrap(),
                    endpoint: Some("203.0.113.1:51820".into()),
                    invite_id: None,
                    instance_id: None,
                    container_subnet: None,
                    name: "node-1".into(),
                    lease_renewed_at: 0,
                },
                containerd: ContainerRuntimeSpec::default(),
            })
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_workload_shows_what_its_nodes_reported() {
        use crate::raft::store::status::{Capacity, ReplicaReport, ReportNode};
        use faber_api_types::workload::{ReplicaState, ReplicaStatus};

        let node = shop().await;
        let svc = &node.service;
        svc.deploy(
            "shop",
            "staging",
            deploy_req(&[("web", spec("nginx"))], false),
        )
        .await
        .unwrap();
        // Nothing reported: no replicas, and no mesh to read a clock from.
        assert!(
            svc.workload("shop", "staging", "web", false)
                .await
                .unwrap()
                .replicas
                .is_empty()
        );

        seed_mesh(&node).await;
        let report = |index: u32, state, pod_ip: Option<&str>| ReplicaReport {
            project: "shop".into(),
            environment: "staging".into(),
            workload: "web".into(),
            revision: 1,
            replica_index: index,
            state,
            pod_ip: pod_ip.map(|ip| ip.parse().unwrap()),
            detail: None,
        };
        svc.propose(Request::ReportNode(ReportNode {
            node_id: 1,
            instance_id: "ab".repeat(16),
            capacity: Capacity::default(),
            replicas: vec![
                report(1, ReplicaState::Deploying, None),
                report(0, ReplicaState::Available, Some("10.200.1.2")),
            ],
        }))
        .await
        .unwrap();
        let want = |state| {
            vec![
                ReplicaStatus {
                    node: 1,
                    replica_index: 0,
                    revision: 1,
                    state: if state {
                        ReplicaState::Available
                    } else {
                        ReplicaState::Unavailable
                    },
                    pod_ip: Some("10.200.1.2".parse().unwrap()),
                },
                ReplicaStatus {
                    node: 1,
                    replica_index: 1,
                    revision: 1,
                    state: if state {
                        ReplicaState::Deploying
                    } else {
                        ReplicaState::Unavailable
                    },
                    pod_ip: None,
                },
            ]
        };
        let got = svc.workload("shop", "staging", "web", false).await.unwrap();
        assert_eq!(got.replicas, want(true));
        // Another workload of the environment shows none of them.
        let other = deploy_req(&[("db", spec("pg"))], false);
        svc.deploy("shop", "staging", other).await.unwrap();
        assert!(
            svc.workload("shop", "staging", "db", false)
                .await
                .unwrap()
                .replicas
                .is_empty()
        );

        // Three intervals of five seconds and a 60 second tick of the lease
        // clock later, with no new report. Just inside that is still fine.
        svc.propose(Request::AdvanceClock { elapsed_secs: 75 })
            .await
            .unwrap();
        let got = svc.workload("shop", "staging", "web", false).await.unwrap();
        assert_eq!(got.replicas, want(true));
        svc.propose(Request::AdvanceClock { elapsed_secs: 1 })
            .await
            .unwrap();
        let got = svc.workload("shop", "staging", "web", false).await.unwrap();
        assert_eq!(got.replicas, want(false));
        node.shutdown().await;
    }

    /// Node 1 is registered; node 2 only holds an invite, so its mesh IP
    /// passes the peer firewall without being a node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_workload_write_must_come_from_a_registered_node() {
        let node = TestNode::start_initialized().await;
        seed_mesh(&node).await;
        node.service
            .invite(CreateInviteRequest {
                name: "node-2".into(),
                endpoint: "198.51.100.2".into(),
                ttl_secs: None,
            })
            .await
            .unwrap();
        let mesh = node.service.read_mesh().unwrap().unwrap();
        let invitee = mesh.invites.values().next().unwrap().mesh_ip;
        // A service on the raft of `node` whose peer listener counts as a
        // mesh address, so the source is checked.
        let strict = FaberService::new(
            1,
            "10.99.0.1:7401".into(),
            node.service.raft().clone(),
            node.state(),
            None,
        )
        .unwrap()
        .with_cluster_keys(std::sync::Arc::new(crate::secrets::StaticClusterKey(
            crate::testing::test_cluster_key(),
        )));
        let create_shop = || PeerWorkloadRequest::CreateProject {
            name: "shop".into(),
            environments: vec!["staging".into()],
        };

        let holder = format!("{invitee}:40000");
        for source in [holder.as_str(), "198.51.100.7:40000", "127.0.0.1:40000"] {
            for forwarded in [false, true] {
                let err = strict
                    .peer_workloads(create_shop(), forwarded, source.parse().unwrap())
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, ServiceError::Forbidden(_)),
                    "{source}: {err:?}"
                );
            }
        }
        assert_eq!(strict.list_projects().await.unwrap().projects, vec![]);

        // A registered node, which is what a forwarding follower is.
        let registered = "10.99.0.1:40000".parse().unwrap();
        strict
            .peer_workloads(create_shop(), true, registered)
            .await
            .unwrap();
        let mapped = "[::ffff:10.99.0.1]:40000".parse().unwrap();
        let web = [("web", spec("nginx"))];
        strict
            .peer_workloads(apply_req("shop", &web), false, mapped)
            .await
            .unwrap();
        // The source is checked first: a stranger does not learn whether
        // its request was a valid one.
        let stranger = "198.51.100.7:40000".parse().unwrap();
        let invalid = apply_req("shop", &[("web", no_containers())]);
        let err = strict
            .peer_workloads(invalid, true, stranger)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::Forbidden(_)), "{err:?}");

        // A loopback peer listener does not check the source.
        let anyone = "127.0.0.1:40000".parse().unwrap();
        node.service
            .peer_workloads(apply_req("shop", &web), false, anyone)
            .await
            .unwrap();

        drop(strict);
        node.shutdown().await;
    }

    const MARKER: &str = "plain-marker-7f3a9c";

    /// A workload that runs `image` with the env `TOKEN`.
    fn with_token(image: &str, token: &str) -> WorkloadSpec {
        let mut s = spec(image);
        s.containers[0].env.insert("TOKEN".into(), token.into());
        s.containers[0].env.insert("MODE".into(), "prod".into());
        s
    }

    fn env_of(read: &WorkloadResponse) -> Vec<(String, String)> {
        read.spec.containers[0]
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_masks_env_values_unless_revealed() {
        let node = shop().await;
        let svc = &node.service;
        let web = [("web", with_token("nginx", MARKER))];
        svc.deploy("shop", "staging", deploy_req(&web, false))
            .await
            .unwrap();

        let masked = svc.workload("shop", "staging", "web", false).await.unwrap();
        assert_eq!(
            env_of(&masked),
            [
                ("MODE".into(), "***".into()),
                ("TOKEN".into(), "***".into())
            ]
        );
        let shown = svc.workload("shop", "staging", "web", true).await.unwrap();
        assert_eq!(
            env_of(&shown),
            [
                ("MODE".into(), "prod".into()),
                ("TOKEN".into(), MARKER.into())
            ]
        );
        // Nothing else differs.
        let mut unmasked = shown.clone();
        unmasked.spec.containers[0].env = masked.spec.containers[0].env.clone();
        assert_eq!(unmasked, masked);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_revision_follows_the_env_by_digest_not_by_ciphertext() {
        let node = shop().await;
        let svc = &node.service;
        let web = [("web", with_token("nginx", MARKER))];
        let first = svc
            .deploy("shop", "staging", deploy_req(&web, false))
            .await
            .unwrap();
        let read = svc.workload("shop", "staging", "web", false).await.unwrap();
        assert_eq!(read.revision, first.generation);
        let before = next_index(&node).await;

        // The same env deployed again is unchanged: same revision, same
        // generation, and no log entry.
        let again = svc
            .deploy("shop", "staging", deploy_req(&web, false))
            .await
            .unwrap();
        assert_eq!(again.unchanged, vec!["web"]);
        assert_eq!(next_index(&node).await, before + 1);
        let read = svc.workload("shop", "staging", "web", false).await.unwrap();
        assert_eq!(read.revision, first.generation);

        // A scale change leaves the revision; an env change moves it.
        let mut scaled = with_token("nginx", MARKER);
        scaled.scale = Scale::Replicas {
            count: 2,
            strategy: Strategy::Spread,
        };
        let scaled = svc
            .deploy("shop", "staging", deploy_req(&[("web", scaled)], false))
            .await
            .unwrap();
        assert_eq!(scaled.updated, vec!["web"]);
        let read = svc.workload("shop", "staging", "web", false).await.unwrap();
        assert_eq!(read.revision, first.generation);

        let mut changed = with_token("nginx", "another");
        changed.scale = Scale::Replicas {
            count: 2,
            strategy: Strategy::Spread,
        };
        let changed = svc
            .deploy("shop", "staging", deploy_req(&[("web", changed)], false))
            .await
            .unwrap();
        assert_eq!(changed.updated, vec!["web"]);
        let read = svc.workload("shop", "staging", "web", true).await.unwrap();
        assert_eq!(read.revision, changed.generation);
        assert_eq!(env_of(&read)[1], ("TOKEN".into(), "another".into()));
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_environment_keeps_one_data_key() {
        let node = shop().await;
        let svc = &node.service;
        assert_eq!(node.state().data_key("shop/staging").unwrap(), None);
        let web = deploy_req(&[("web", with_token("nginx", MARKER))], false);
        svc.deploy("shop", "staging", web).await.unwrap();
        let first = node.state().data_key("shop/staging").unwrap().unwrap();
        assert_eq!(
            first.key_id,
            faber_crypto::key_id(&crate::testing::test_cluster_key())
        );

        let api = deploy_req(&[("api", with_token("api:1", "x"))], false);
        svc.deploy("shop", "staging", api).await.unwrap();
        assert_eq!(
            node.state().data_key("shop/staging").unwrap().unwrap(),
            first
        );
        // Another environment gets its own.
        let prod = deploy_req(&[("web", with_token("nginx", "y"))], false);
        svc.deploy("shop", "production", prod).await.unwrap();
        let other = node.state().data_key("shop/production").unwrap().unwrap();
        assert_ne!(other.wrapped, first.wrapped);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deploy_to_a_missing_environment_makes_no_key() {
        let node = shop().await;
        let svc = &node.service;
        let web = deploy_req(&[("web", with_token("nginx", MARKER))], false);
        missing(svc.deploy("shop", "nowhere", web.clone()).await);
        missing(svc.deploy("blog", "staging", web).await);
        assert_eq!(node.state().data_key("shop/nowhere").unwrap(), None);
        assert_eq!(node.state().data_key("blog/staging").unwrap(), None);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_log_and_snapshot_hold_no_plaintext_env() {
        let node = shop().await;
        let svc = &node.service;
        let web = [("web", with_token("nginx", MARKER))];

        // The command a deploy proposes is what the log carries.
        let req = apply_req("shop", &web);
        let sealed = svc.seal_deploy(&req).await.unwrap();
        let logged = command(req, sealed, 1).unwrap();
        let bytes = serde_json::to_vec(&logged).unwrap();
        assert!(!bytes.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()));
        assert!(format!("{logged:?}").contains("TOKEN"));

        svc.deploy("shop", "staging", deploy_req(&web, false))
            .await
            .unwrap();
        let raft = svc.raft();
        raft.trigger().snapshot().await.unwrap();
        let mut built = None;
        for _ in 0..100 {
            built = raft.get_snapshot().await.unwrap();
            if built.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let bytes = built.expect("the snapshot is built").snapshot.into_inner();
        assert!(
            bytes.windows(5).any(|w| w == b"TOKEN"),
            "the snapshot has the env"
        );
        assert!(!bytes.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()));

        let records = node.state().environments().unwrap();
        let stored = serde_json::to_vec(&records).unwrap();
        assert!(!stored.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()));
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deploy_without_a_cluster_key_fails_cleanly() {
        let node = shop().await;
        let svc = node.service.clone().with_cluster_keys(std::sync::Arc::new(
            crate::secrets::FileClusterKey::new("no/such/cluster.key"),
        ));
        let web = deploy_req(&[("web", with_token("nginx", MARKER))], false);
        let before = next_index(&node).await;
        let err = svc.deploy("shop", "staging", web).await.unwrap_err();
        match &err {
            ServiceError::Rejected { code, message } => {
                assert_eq!(code, error_code::KEY_UNAVAILABLE);
                assert!(message.contains("cluster key"), "{message}");
                assert!(!message.contains(MARKER));
            }
            other => panic!("{other:?}"),
        }
        // Nothing was written, and no key was made.
        assert_eq!(next_index(&node).await, before + 1);
        assert_eq!(node.state().data_key("shop/staging").unwrap(), None);
        assert!(
            svc.list_workloads(query(None, None))
                .await
                .unwrap()
                .workloads
                .is_empty()
        );
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_revealing_read_needs_the_cluster_key_a_masked_one_does_not() {
        let node = shop().await;
        let web = deploy_req(&[("web", with_token("nginx", MARKER))], false);
        node.service.deploy("shop", "staging", web).await.unwrap();

        let without = node
            .service
            .clone()
            .with_cluster_keys(std::sync::Arc::new(crate::secrets::NoClusterKey));
        let masked = without
            .workload("shop", "staging", "web", false)
            .await
            .unwrap();
        assert!(env_of(&masked).iter().all(|(_, v)| v == "***"));
        let err = without
            .workload("shop", "staging", "web", true)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, ServiceError::Rejected { code, .. } if code == error_code::KEY_UNAVAILABLE),
            "{err:?}"
        );

        // Another cluster key is told apart from a missing one.
        let other = faber_crypto::ClusterKey::from_bytes([1; faber_crypto::KEY_LEN]);
        let wrong = node
            .service
            .clone()
            .with_cluster_keys(std::sync::Arc::new(crate::secrets::StaticClusterKey(other)));
        let err = wrong
            .workload("shop", "staging", "web", true)
            .await
            .unwrap_err();
        match err {
            ServiceError::Rejected { code, message } => {
                assert_eq!(code, error_code::KEY_UNAVAILABLE);
                assert!(message.contains("not the one"), "{message}");
            }
            other => panic!("{other:?}"),
        }
        node.shutdown().await;
    }
}
