//! Replicated project and workload records, stored as JSON in `sm_data`.
//!
//! Two kinds of key:
//!
//! - [`KEY_PROJECTS`] holds the [`ProjectIndex`]: every project, its
//!   environments, and per environment the generation, size and workload
//!   count. It is small, so limits and listings never scan.
//! - [`env_key`] holds one [`EnvironmentRecord`] per environment, from the
//!   command that creates the environment to the one that removes it. An
//!   environment without workloads has a record with an empty map.
//!
//! A command that changes an environment writes its record and the index
//! in one batch, so the two never disagree.
//!
//! Everything here is pure, like `mesh::state`: `apply` passes the log
//! index of the entry and the time the leader stamped on the command (`at`,
//! Unix seconds), and nothing reads a clock. A method that returns `Err`
//! may have changed `self`; the caller discards it.
//!
//! Apply checks a small fixed set of rules: names, sizes, counts and
//! existence. The full model validation
//! (`faber_api_types::workload::validate_workloads`) runs in the service
//! and the CLI only, so changing a model rule never changes how an old log
//! replays.

use std::collections::BTreeMap;

use faber_api_types::error_code;
use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::{Exposure, Placement, Scale, WorkloadSpec, limits};
use faber_crypto::Sealed;
use serde::{Deserialize, Serialize};

use crate::mesh::Rejection;

/// `sm_data` key holding the JSON [`ProjectIndex`].
pub const KEY_PROJECTS: &str = "projects";
/// Prefix of the `sm_data` keys holding one JSON [`EnvironmentRecord`] each.
pub const ENV_PREFIX: &str = "env/";

/// The `sm_data` key of an environment: `env/<project>/<environment>`. Both
/// names are DNS labels, so neither holds a `/`.
pub fn env_key(project: &str, environment: &str) -> String {
    format!("{ENV_PREFIX}{project}/{environment}")
}

/// Whether only the typed commands of this module may write `key`:
/// [`KEY_PROJECTS`] and every key under [`ENV_PREFIX`]. `Request::Set`
/// rejects these.
pub fn is_reserved_key(key: &str) -> bool {
    key == KEY_PROJECTS || key.starts_with(ENV_PREFIX)
}

/// Every project of the cluster, by name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectIndex {
    #[serde(default)]
    pub projects: BTreeMap<String, ProjectRecord>,
}

/// One project in the [`ProjectIndex`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRecord {
    /// `at` of the `CreateProject` that made it.
    pub created_at: u64,
    /// Its environments, by name.
    #[serde(default)]
    pub environments: BTreeMap<String, EnvironmentIndexEntry>,
}

/// What the index keeps about one environment: a copy of the figures of
/// its [`EnvironmentRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentIndexEntry {
    /// [`EnvironmentRecord::generation`].
    pub generation: u64,
    /// Size of the stored record as JSON.
    pub bytes: u64,
    /// How many workloads the record holds.
    pub workloads: u32,
    /// [`EnvironmentRecord::updated_at`].
    pub updated_at: u64,
}

/// One environment and its workloads, under [`env_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentRecord {
    /// The raft log index of the entry that last changed the environment,
    /// starting with the one that created it. It never repeats, also
    /// across a delete and a recreate.
    pub generation: u64,
    /// `at` of the command that last changed the environment.
    pub updated_at: u64,
    /// The workloads, by name.
    #[serde(default)]
    pub workloads: BTreeMap<String, WorkloadRecord>,
}

/// One workload of an [`EnvironmentRecord`].
///
/// `spec` never holds an env value: every container's `env` is empty, and
/// the values are in `env`, sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadRecord {
    /// The raft log index of the entry that last changed the pod template:
    /// everything in the spec except `scale`, `placement` and `expose`, and
    /// the env. The runner restarts pods only when it moves.
    pub revision: u64,
    pub spec: WorkloadSpec,
    /// The sealed env of each container, by container name.
    #[serde(default)]
    pub env: BTreeMap<String, SealedEnv>,
}

/// The env of one container, sealed under the data key of the environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedEnv {
    /// `faber_crypto::env_digest` of the plaintext env. Two envs are the
    /// same when their digests are, whatever their ciphertexts.
    #[serde(with = "faber_crypto::digest_serde")]
    pub digest: [u8; 32],
    /// The sealed value of each variable, by name.
    pub values: BTreeMap<String, Sealed>,
}

/// What `ApplyWorkloads` carries for one workload: a [`WorkloadRecord`]
/// without its revision. The service seals the env before it proposes, so
/// the log holds no plaintext and `apply` stays deterministic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredWorkload {
    /// The spec with every container's `env` empty.
    pub spec: WorkloadSpec,
    /// The sealed env of each container, by container name.
    #[serde(default)]
    pub env: BTreeMap<String, SealedEnv>,
}

impl StoredWorkload {
    /// Whether every container's `env` is empty, as it must be.
    fn is_sealed(&self) -> bool {
        self.spec.containers.iter().all(|c| c.env.is_empty())
    }
}

/// The digest of each container's env, by container name.
fn digests(env: &BTreeMap<String, SealedEnv>) -> impl Iterator<Item = (&String, &[u8; 32])> {
    env.iter().map(|(container, env)| (container, &env.digest))
}

impl WorkloadRecord {
    /// Whether `new` is the same workload, the env compared by digest: the
    /// ciphertexts differ on every seal.
    fn is_same_as(&self, new: &StoredWorkload) -> bool {
        self.spec == new.spec && self.same_env_as(new)
    }

    fn same_env_as(&self, new: &StoredWorkload) -> bool {
        digests(&self.env).eq(digests(&new.env))
    }

    /// Whether the pod template differs: see [`pod_template`]. A changed
    /// env is a changed template.
    fn template_differs_from(&self, new: &StoredWorkload) -> bool {
        pod_template(&self.spec) != pod_template(&new.spec) || !self.same_env_as(new)
    }
}

/// What `ApplyWorkloads` did, as `Response::Applied` reports it. Every
/// workload of the command is in exactly one of `created`, `updated` and
/// `unchanged`. Each list is sorted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// The environment's generation after the command.
    pub generation: u64,
    pub created: Vec<String>,
    pub updated: Vec<String>,
    pub unchanged: Vec<String>,
    /// Workloads removed by `prune`.
    pub removed: Vec<String>,
}

/// The result of [`ProjectIndex::apply_workloads`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadsApplied {
    pub applied: Applied,
    /// The environment record to write, together with the index. `None`
    /// when the command changed nothing: then nothing is written and the
    /// generation stays.
    pub record: Option<EnvironmentRecord>,
}

/// Counts the bytes written to it.
struct ByteCount(u64);

impl std::io::Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The size of `value` as compact JSON, which is how `sm_data` stores it.
fn json_len<T: Serialize>(value: &T) -> Result<u64, Rejection> {
    let mut count = ByteCount(0);
    serde_json::to_writer(&mut count, value)
        .map_err(|e| Rejection::new(error_code::INTERNAL, format!("cannot encode: {e}")))?;
    Ok(count.0)
}

/// Checks that `name` is a DNS label. `what` says what it names.
fn check_name(what: &str, name: &str) -> Result<(), Rejection> {
    validate_dns_label(name).map_err(|message| {
        Rejection::new(
            error_code::BAD_REQUEST,
            format!("{what} {name:?}: {message}"),
        )
    })
}

fn not_found(what: &str, name: &str) -> Rejection {
    Rejection::new(
        error_code::NOT_FOUND,
        format!("{what} {name:?} does not exist"),
    )
}

/// `1 workload`, `2 workloads`.
fn count_of_workloads(count: u32) -> String {
    let noun = if count == 1 { "workload" } else { "workloads" };
    format!("{count} {noun}")
}

/// The pod template of `spec`: the spec with `scale`, `placement` and
/// `expose` at their defaults. Two specs with the same template differ
/// only in how many pods run, where, and who may reach them.
fn pod_template(spec: &WorkloadSpec) -> WorkloadSpec {
    WorkloadSpec {
        scale: Scale::default(),
        placement: Placement::default(),
        expose: Exposure::default(),
        ..spec.clone()
    }
}

/// What the index keeps about `record`, which is `bytes` long as JSON.
fn index_entry(record: &EnvironmentRecord, bytes: u64) -> EnvironmentIndexEntry {
    EnvironmentIndexEntry {
        generation: record.generation,
        bytes,
        // At most `MAX_WORKLOADS_PER_ENVIRONMENT`.
        workloads: record.workloads.len() as u32,
        updated_at: record.updated_at,
    }
}

/// An environment without workloads, made by the entry at `index`.
fn empty_environment(
    index: u64,
    at: u64,
) -> Result<(EnvironmentRecord, EnvironmentIndexEntry), Rejection> {
    let record = EnvironmentRecord {
        generation: index,
        updated_at: at,
        workloads: BTreeMap::new(),
    };
    let entry = index_entry(&record, json_len(&record)?);
    Ok((record, entry))
}

impl ProjectIndex {
    /// `CreateProject`: adds project `name` with `environments`, each
    /// empty and at generation `index`. Returns the record to write for
    /// each environment, by environment name.
    ///
    /// Rejected with `bad_request` when a name is not a DNS label, an
    /// environment is named twice, none is named, or more are named than a
    /// project may have; with `project_exists` when the project exists;
    /// with `workloads_exhausted` at the limit of projects.
    pub fn create_project(
        &mut self,
        name: &str,
        environments: &[String],
        index: u64,
        at: u64,
    ) -> Result<BTreeMap<String, EnvironmentRecord>, Rejection> {
        check_name("project", name)?;
        if environments.is_empty() {
            return Err(Rejection::new(
                error_code::BAD_REQUEST,
                "a project needs at least one environment",
            ));
        }
        if environments.len() > limits::MAX_ENVIRONMENTS_PER_PROJECT {
            return Err(Rejection::new(
                error_code::BAD_REQUEST,
                format!(
                    "a project has at most {} environments",
                    limits::MAX_ENVIRONMENTS_PER_PROJECT
                ),
            ));
        }
        let mut records = BTreeMap::new();
        let mut entries = BTreeMap::new();
        for environment in environments {
            check_name("environment", environment)?;
            let (record, entry) = empty_environment(index, at)?;
            if records.insert(environment.clone(), record).is_some() {
                return Err(Rejection::new(
                    error_code::BAD_REQUEST,
                    format!("environment {environment:?} is named twice"),
                ));
            }
            entries.insert(environment.clone(), entry);
        }
        if self.projects.contains_key(name) {
            return Err(Rejection::new(
                error_code::PROJECT_EXISTS,
                format!("project {name:?} exists"),
            ));
        }
        if self.projects.len() >= limits::MAX_PROJECTS {
            return Err(Rejection::new(
                error_code::WORKLOADS_EXHAUSTED,
                format!("the cluster has {} projects already", limits::MAX_PROJECTS),
            ));
        }
        self.projects.insert(
            name.to_string(),
            ProjectRecord {
                created_at: at,
                environments: entries,
            },
        );
        Ok(records)
    }

    /// `DeleteProject`: removes project `name`. Returns the names of its
    /// environments, whose records the caller deletes.
    ///
    /// Rejected with `not_found` for an unknown project and with
    /// `not_empty` while one of its environments holds a workload.
    pub fn delete_project(&mut self, name: &str) -> Result<Vec<String>, Rejection> {
        let project = self
            .projects
            .remove(name)
            .ok_or_else(|| not_found("project", name))?;
        if let Some((environment, entry)) =
            project.environments.iter().find(|(_, e)| e.workloads > 0)
        {
            return Err(Rejection::new(
                error_code::NOT_EMPTY,
                format!(
                    "environment {environment:?} of project {name:?} has {}",
                    count_of_workloads(entry.workloads)
                ),
            ));
        }
        Ok(project.environments.into_keys().collect())
    }

    /// `AddEnvironment`: adds the empty environment `name` to `project`, at
    /// generation `index`. Returns the record to write.
    ///
    /// Rejected with `not_found` for an unknown project, with `bad_request`
    /// when `name` is not a DNS label, with `project_exists` when the
    /// project has the environment, and with `workloads_exhausted` at the
    /// limit of environments per project.
    pub fn add_environment(
        &mut self,
        project: &str,
        name: &str,
        index: u64,
        at: u64,
    ) -> Result<EnvironmentRecord, Rejection> {
        let environments = &mut self
            .projects
            .get_mut(project)
            .ok_or_else(|| not_found("project", project))?
            .environments;
        check_name("environment", name)?;
        if environments.contains_key(name) {
            return Err(Rejection::new(
                error_code::PROJECT_EXISTS,
                format!("project {project:?} has the environment {name:?} already"),
            ));
        }
        if environments.len() >= limits::MAX_ENVIRONMENTS_PER_PROJECT {
            return Err(Rejection::new(
                error_code::WORKLOADS_EXHAUSTED,
                format!(
                    "project {project:?} has {} environments already",
                    limits::MAX_ENVIRONMENTS_PER_PROJECT
                ),
            ));
        }
        let (record, entry) = empty_environment(index, at)?;
        environments.insert(name.to_string(), entry);
        Ok(record)
    }

    /// `RemoveEnvironment`: removes environment `name` of `project`. The
    /// caller deletes its record.
    ///
    /// Rejected with `not_found` for an unknown project or environment and
    /// with `not_empty` while the environment holds a workload.
    pub fn remove_environment(&mut self, project: &str, name: &str) -> Result<(), Rejection> {
        let workloads = self.environment(project, name)?.workloads;
        if workloads > 0 {
            return Err(Rejection::new(
                error_code::NOT_EMPTY,
                format!(
                    "environment {name:?} of project {project:?} has {}",
                    count_of_workloads(workloads)
                ),
            ));
        }
        if let Some(p) = self.projects.get_mut(project) {
            p.environments.remove(name);
        }
        Ok(())
    }

    /// `ApplyWorkloads`: creates or updates each of `workloads` in
    /// `record`, the stored record of the environment (`None` when
    /// [`env_key`] holds nothing, which counts as an empty record). With
    /// `prune` it removes every other workload. A workload whose spec and
    /// env digests are those of the stored one is unchanged; the ciphertexts
    /// are not compared. One whose pod template or env changed gets
    /// `revision = index`.
    ///
    /// When anything changed, the environment's generation becomes `index`
    /// and its `updated_at` becomes `at`, in the returned record and in
    /// the index.
    ///
    /// Rejected with `not_found` for an unknown project or environment;
    /// with `bad_request` when a workload name is not a DNS label or the
    /// command is over a size limit; with `workloads_exhausted` when the
    /// environment would hold too many workloads, or its record or all
    /// records together too many bytes.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_workloads(
        &mut self,
        record: Option<EnvironmentRecord>,
        project: &str,
        environment: &str,
        workloads: BTreeMap<String, StoredWorkload>,
        prune: bool,
        index: u64,
        at: u64,
    ) -> Result<WorkloadsApplied, Rejection> {
        let mut record = self.record_or_empty(record, project, environment)?;

        // The name and the size of the request are all that apply checks
        // of a spec. `WorkloadSpec::validate` and `validate_workloads` are
        // not called here, on purpose: the service runs them before it
        // proposes, and a rule that changes there must not change how a
        // logged command replays.
        for (name, workload) in &workloads {
            check_name("workload", name)?;
            // The log must never hold a plaintext env value.
            if !workload.is_sealed() {
                return Err(Rejection::new(
                    error_code::BAD_REQUEST,
                    format!("workload {name:?} carries env values that are not sealed"),
                ));
            }
        }
        if json_len(&workloads)? > limits::MAX_DEPLOY_REQUEST_BYTES as u64 {
            return Err(Rejection::new(
                error_code::BAD_REQUEST,
                format!(
                    "the workloads of one deploy are over {} bytes",
                    limits::MAX_DEPLOY_REQUEST_BYTES
                ),
            ));
        }

        let mut applied = Applied::default();
        if prune {
            applied.removed = record
                .workloads
                .keys()
                .filter(|name| !workloads.contains_key(*name))
                .cloned()
                .collect();
            for name in &applied.removed {
                record.workloads.remove(name);
            }
        }
        for (name, workload) in workloads {
            match record.workloads.get_mut(&name) {
                None => {
                    let StoredWorkload { spec, env } = workload;
                    let created = WorkloadRecord {
                        revision: index,
                        spec,
                        env,
                    };
                    record.workloads.insert(name.clone(), created);
                    applied.created.push(name);
                }
                // The same by digest: the stored ciphertext stays.
                Some(stored) if stored.is_same_as(&workload) => applied.unchanged.push(name),
                Some(stored) => {
                    if stored.template_differs_from(&workload) {
                        stored.revision = index;
                    }
                    let StoredWorkload { spec, env } = workload;
                    stored.spec = spec;
                    stored.env = env;
                    applied.updated.push(name);
                }
            }
        }
        if applied.created.is_empty() && applied.updated.is_empty() && applied.removed.is_empty() {
            applied.generation = record.generation;
            return Ok(WorkloadsApplied {
                applied,
                record: None,
            });
        }

        if record.workloads.len() > limits::MAX_WORKLOADS_PER_ENVIRONMENT {
            return Err(Rejection::new(
                error_code::WORKLOADS_EXHAUSTED,
                format!(
                    "environment {environment:?} of project {project:?} would hold more than {} \
                     workloads",
                    limits::MAX_WORKLOADS_PER_ENVIRONMENT
                ),
            ));
        }
        record.generation = index;
        record.updated_at = at;
        let bytes = json_len(&record)?;
        if bytes > limits::MAX_ENVIRONMENT_RECORD_BYTES as u64 {
            return Err(Rejection::new(
                error_code::WORKLOADS_EXHAUSTED,
                format!(
                    "environment {environment:?} of project {project:?} would be over {} bytes",
                    limits::MAX_ENVIRONMENT_RECORD_BYTES
                ),
            ));
        }
        *self.environment_mut(project, environment)? = index_entry(&record, bytes);
        if self.total_bytes() > limits::MAX_TOTAL_BYTES as u64 {
            return Err(Rejection::new(
                error_code::WORKLOADS_EXHAUSTED,
                format!(
                    "all environments together would be over {} bytes",
                    limits::MAX_TOTAL_BYTES
                ),
            ));
        }
        applied.generation = index;
        Ok(WorkloadsApplied {
            applied,
            record: Some(record),
        })
    }

    /// `RemoveWorkloads`: removes the workloads `names` from `record`, the
    /// stored record of the environment. A name given twice is removed
    /// once. Returns the record to write, at generation `index` and with
    /// `updated_at = at`; with no name at all, the record as it is.
    ///
    /// Rejected with `not_found` for an unknown project, environment or
    /// workload. Nothing is removed then.
    pub fn remove_workloads(
        &mut self,
        record: Option<EnvironmentRecord>,
        project: &str,
        environment: &str,
        names: &[String],
        index: u64,
        at: u64,
    ) -> Result<EnvironmentRecord, Rejection> {
        let mut record = self.record_or_empty(record, project, environment)?;
        if let Some(unknown) = names.iter().find(|n| !record.workloads.contains_key(*n)) {
            return Err(not_found("workload", unknown));
        }
        if names.is_empty() {
            return Ok(record);
        }
        for name in names {
            record.workloads.remove(name);
        }
        record.generation = index;
        record.updated_at = at;
        *self.environment_mut(project, environment)? = index_entry(&record, json_len(&record)?);
        Ok(record)
    }

    /// The index entry of an environment, or the `not_found` rejection
    /// that names what is missing.
    fn environment(
        &self,
        project: &str,
        environment: &str,
    ) -> Result<&EnvironmentIndexEntry, Rejection> {
        self.projects
            .get(project)
            .ok_or_else(|| not_found("project", project))?
            .environments
            .get(environment)
            .ok_or_else(|| not_found("environment", environment))
    }

    fn environment_mut(
        &mut self,
        project: &str,
        environment: &str,
    ) -> Result<&mut EnvironmentIndexEntry, Rejection> {
        self.projects
            .get_mut(project)
            .ok_or_else(|| not_found("project", project))?
            .environments
            .get_mut(environment)
            .ok_or_else(|| not_found("environment", environment))
    }

    /// The stored record of an environment the index knows. Every
    /// environment has one; should it be missing, the environment counts
    /// as empty, at the generation the index has for it.
    fn record_or_empty(
        &self,
        record: Option<EnvironmentRecord>,
        project: &str,
        environment: &str,
    ) -> Result<EnvironmentRecord, Rejection> {
        let entry = self.environment(project, environment)?;
        Ok(record.unwrap_or_else(|| EnvironmentRecord {
            generation: entry.generation,
            updated_at: entry.updated_at,
            workloads: BTreeMap::new(),
        }))
    }

    /// Bytes of all environment records together, by the index.
    fn total_bytes(&self) -> u64 {
        self.projects
            .values()
            .flat_map(|p| p.environments.values())
            .map(|e| e.bytes)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use faber_api_types::error_code as ec;
    use faber_api_types::workload::{ContainerSpec, RestartPolicy, Strategy};
    use serde_json::json;

    use super::*;

    /// A workload of one container that runs `image`, with everything else
    /// at its default.
    fn spec(image: &str) -> WorkloadSpec {
        WorkloadSpec {
            scale: Scale::default(),
            restart: RestartPolicy::default(),
            expose: Exposure::default(),
            placement: Placement::default(),
            stop_grace_secs: None,
            volumes: BTreeMap::new(),
            containers: vec![ContainerSpec::new("main", image)],
        }
    }

    /// A workload that exists for its size: `pad` bytes of image name. The
    /// size is exact, which sealed env values (base64) would not be.
    fn padded(pad: usize) -> WorkloadSpec {
        spec(&"x".repeat(pad))
    }

    /// `spec` with its env sealed under a fixed key, as the service does.
    fn sealed(spec: &WorkloadSpec) -> StoredWorkload {
        let key = faber_crypto::DataKey::from_bytes([9; 32]);
        crate::secrets::seal_workload(&key, "shop", "staging", "w", spec, None).unwrap()
    }

    fn strings(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn json_len<T: Serialize>(value: &T) -> u64 {
        serde_json::to_vec(value).unwrap().len() as u64
    }

    /// The index and the records as the state machine keeps them. Every
    /// command runs on a copy of the index, as `apply` does: a rejected one
    /// leaves nothing behind.
    #[derive(Debug, Clone, Default, PartialEq)]
    struct World {
        index: ProjectIndex,
        records: BTreeMap<(String, String), EnvironmentRecord>,
    }

    fn key(project: &str, environment: &str) -> (String, String) {
        (project.to_string(), environment.to_string())
    }

    impl World {
        fn create(
            &mut self,
            name: &str,
            environments: &[&str],
            index: u64,
            at: u64,
        ) -> Result<(), Rejection> {
            let mut next = self.index.clone();
            let records = next.create_project(name, &strings(environments), index, at)?;
            for (environment, record) in records {
                self.records.insert(key(name, &environment), record);
            }
            self.index = next;
            Ok(())
        }

        fn delete(&mut self, name: &str) -> Result<Vec<String>, Rejection> {
            let mut next = self.index.clone();
            let environments = next.delete_project(name)?;
            for environment in &environments {
                self.records.remove(&key(name, environment));
            }
            self.index = next;
            Ok(environments)
        }

        fn add_env(
            &mut self,
            project: &str,
            name: &str,
            index: u64,
            at: u64,
        ) -> Result<(), Rejection> {
            let mut next = self.index.clone();
            let record = next.add_environment(project, name, index, at)?;
            self.records.insert(key(project, name), record);
            self.index = next;
            Ok(())
        }

        fn remove_env(&mut self, project: &str, name: &str) -> Result<(), Rejection> {
            let mut next = self.index.clone();
            next.remove_environment(project, name)?;
            self.records.remove(&key(project, name));
            self.index = next;
            Ok(())
        }

        fn apply(
            &mut self,
            project: &str,
            environment: &str,
            workloads: &[(&str, WorkloadSpec)],
            prune: bool,
            index: u64,
            at: u64,
        ) -> Result<Applied, Rejection> {
            let mut next = self.index.clone();
            let stored = self.records.get(&key(project, environment)).cloned();
            let workloads = workloads
                .iter()
                .map(|(name, spec)| (name.to_string(), sealed(spec)))
                .collect();
            let done =
                next.apply_workloads(stored, project, environment, workloads, prune, index, at)?;
            match done.record {
                Some(record) => {
                    self.records.insert(key(project, environment), record);
                    self.index = next;
                }
                // Nothing to write: the index must not have moved either.
                None => assert_eq!(next, self.index),
            }
            Ok(done.applied)
        }

        fn remove(
            &mut self,
            project: &str,
            environment: &str,
            names: &[&str],
            index: u64,
            at: u64,
        ) -> Result<(), Rejection> {
            let mut next = self.index.clone();
            let stored = self.records.get(&key(project, environment)).cloned();
            let record =
                next.remove_workloads(stored, project, environment, &strings(names), index, at)?;
            self.records.insert(key(project, environment), record);
            self.index = next;
            Ok(())
        }

        fn record(&self, project: &str, environment: &str) -> &EnvironmentRecord {
            &self.records[&key(project, environment)]
        }

        fn entry(&self, project: &str, environment: &str) -> &EnvironmentIndexEntry {
            &self.index.projects[project].environments[environment]
        }

        fn total_bytes(&self) -> u64 {
            self.index
                .projects
                .values()
                .flat_map(|p| p.environments.values())
                .map(|e| e.bytes)
                .sum()
        }

        /// The size the record of an environment would have after a deploy
        /// of `name` with `pad` bytes at `index`.
        fn bytes_after(
            &self,
            project: &str,
            environment: &str,
            name: &str,
            pad: usize,
            index: u64,
        ) -> u64 {
            let done = self
                .index
                .clone()
                .apply_workloads(
                    self.records.get(&key(project, environment)).cloned(),
                    project,
                    environment,
                    BTreeMap::from([(name.to_string(), sealed(&padded(pad)))]),
                    false,
                    index,
                    1,
                )
                .unwrap();
            json_len(&done.record.unwrap())
        }

        /// Grows an environment with padded workloads, all deployed at
        /// `index`, and stops one deploy short of `target` bytes. Returns
        /// the name and the padding of the workload that brings the record
        /// to exactly `target` when it is deployed at `index` too.
        fn grow_towards(
            &mut self,
            project: &str,
            environment: &str,
            target: u64,
            index: u64,
        ) -> (String, usize) {
            let mut n = 0;
            while target - self.entry(project, environment).bytes > 115_000 {
                let name = format!("pad-{n}");
                self.apply(
                    project,
                    environment,
                    &[(name.as_str(), padded(100_000))],
                    false,
                    index,
                    1,
                )
                .unwrap();
                n += 1;
            }
            let name = format!("pad-{n}");
            let pad = target - self.bytes_after(project, environment, &name, 0, index);
            (name, pad as usize)
        }

        /// Grows an environment to exactly `target` bytes.
        fn grow_to(&mut self, project: &str, environment: &str, target: u64, index: u64) {
            let (name, pad) = self.grow_towards(project, environment, target, index);
            self.apply(
                project,
                environment,
                &[(name.as_str(), padded(pad))],
                false,
                index,
                1,
            )
            .unwrap();
            assert_eq!(self.entry(project, environment).bytes, target);
        }
    }

    /// What must hold after every command: the index and the records name
    /// the same environments, the figures in the index are those of the
    /// record, and every limit holds.
    fn assert_invariants(w: &World) {
        assert!(w.index.projects.len() <= limits::MAX_PROJECTS);
        let mut indexed = Vec::new();
        for (project, p) in &w.index.projects {
            assert_eq!(validate_dns_label(project), Ok(()), "{project:?}");
            assert!(!p.environments.is_empty(), "{project}");
            assert!(p.environments.len() <= limits::MAX_ENVIRONMENTS_PER_PROJECT);
            for (environment, entry) in &p.environments {
                assert_eq!(validate_dns_label(environment), Ok(()), "{environment:?}");
                indexed.push(key(project, environment));
                let at = format!("{project}/{environment}");
                let record = w
                    .records
                    .get(&key(project, environment))
                    .unwrap_or_else(|| panic!("no record for {at}"));
                assert_eq!(entry.generation, record.generation, "{at}");
                assert_eq!(entry.updated_at, record.updated_at, "{at}");
                assert_eq!(entry.workloads as usize, record.workloads.len(), "{at}");
                assert_eq!(entry.bytes, json_len(record), "{at}");
                assert!(record.workloads.len() <= limits::MAX_WORKLOADS_PER_ENVIRONMENT);
                assert!(entry.bytes <= limits::MAX_ENVIRONMENT_RECORD_BYTES as u64);
                for (name, workload) in &record.workloads {
                    assert_eq!(validate_dns_label(name), Ok(()), "{name:?}");
                    assert!(workload.revision <= record.generation, "{at}/{name}");
                }
            }
        }
        assert_eq!(indexed, w.records.keys().cloned().collect::<Vec<_>>());
        assert!(w.total_bytes() <= limits::MAX_TOTAL_BYTES as u64);
    }

    /// Project `shop` with `staging` and `production`, created at index 1.
    fn shop() -> World {
        let mut w = World::default();
        w.create("shop", &["staging", "production"], 1, 1_000)
            .unwrap();
        w
    }

    /// Runs `command` on `w` and checks that it is refused with `code` and
    /// leaves nothing behind.
    fn refused<T: std::fmt::Debug>(
        w: &mut World,
        code: &str,
        command: impl FnOnce(&mut World) -> Result<T, Rejection>,
    ) {
        let before = w.clone();
        let err = command(w).unwrap_err();
        assert_eq!(err.code, code, "{err}");
        assert_eq!(*w, before);
    }

    #[test]
    fn keys() {
        assert_eq!(KEY_PROJECTS, "projects");
        assert_eq!(ENV_PREFIX, "env/");
        assert_eq!(env_key("shop", "staging"), "env/shop/staging");
    }

    #[test]
    fn the_index_and_every_environment_key_are_reserved() {
        for key in ["projects", "env/", "env/shop/staging", "env/x"] {
            assert!(is_reserved_key(key), "{key}");
        }
        for key in [
            "project",
            "projects/x",
            "env",
            "environment",
            "mesh",
            "a/env/b",
        ] {
            assert!(!is_reserved_key(key), "{key}");
        }
    }

    #[test]
    fn records_have_pinned_shapes() {
        let index = ProjectIndex {
            projects: BTreeMap::from([(
                "shop".to_string(),
                ProjectRecord {
                    created_at: 1_000,
                    environments: BTreeMap::from([(
                        "staging".to_string(),
                        EnvironmentIndexEntry {
                            generation: 7,
                            bytes: 512,
                            workloads: 1,
                            updated_at: 1_001,
                        },
                    )]),
                },
            )]),
        };
        let want = json!({"projects": {"shop": {
            "created_at": 1_000,
            "environments": {"staging": {
                "generation": 7, "bytes": 512, "workloads": 1, "updated_at": 1_001,
            }},
        }}});
        assert_eq!(serde_json::to_value(&index).unwrap(), want);
        assert_eq!(serde_json::from_value::<ProjectIndex>(want).unwrap(), index);

        let record: EnvironmentRecord = serde_json::from_value(json!({
            "generation": 7, "updated_at": 1_001,
            "workloads": {"web": {
                "revision": 5,
                "spec": {"containers": [{"name": "web", "image": "nginx"}]},
            }},
        }))
        .unwrap();
        assert_eq!(record.workloads["web"].revision, 5);
        let v = serde_json::to_value(&record).unwrap();
        assert_eq!(v["generation"], 7);
        assert_eq!(v["updated_at"], 1_001);
        assert_eq!(
            v["workloads"]["web"]["spec"]["scale"],
            json!({"replicas": {"count": 1, "strategy": "spread"}})
        );
        assert_eq!(
            serde_json::from_value::<EnvironmentRecord>(v).unwrap(),
            record
        );
    }

    /// The sealed env of a workload: a digest and the values by name.
    #[test]
    fn a_workload_record_keeps_sealed_env_by_container() {
        let mut with_env = spec("nginx");
        with_env.containers[0]
            .env
            .insert("TOKEN".into(), "x".into());
        let record = WorkloadRecord {
            revision: 5,
            spec: sealed(&with_env).spec,
            env: sealed(&with_env).env,
        };
        let v = serde_json::to_value(&record).unwrap();
        assert_eq!(v["spec"]["containers"][0]["env"], json!({}));
        let env = &v["env"]["main"];
        assert!(env["digest"].is_string());
        assert!(env["values"]["TOKEN"].is_string());
        assert_eq!(serde_json::from_value::<WorkloadRecord>(v).unwrap(), record);
    }

    /// The ciphertexts differ on every seal: sameness is the digest's.
    #[test]
    fn an_env_is_compared_by_digest_not_by_ciphertext() {
        let with = |value: &str| {
            let mut s = spec("nginx");
            s.containers[0].env.insert("TOKEN".into(), value.into());
            s
        };
        let mut w = shop();
        let apply = |w: &mut World, value: &str, index: u64| {
            w.apply("shop", "staging", &[("web", with(value))], false, index, 1)
                .unwrap()
        };
        assert_eq!(apply(&mut w, "a", 2).created, strings(&["web"]));
        let sealed_once = w.record("shop", "staging").workloads["web"].env.clone();

        // Sealed again, the same env is unchanged: nothing is written and
        // the stored ciphertext stays.
        assert_ne!(sealed(&with("a")).env, sealed_once);
        assert_eq!(apply(&mut w, "a", 3).unchanged, strings(&["web"]));
        let record = w.record("shop", "staging");
        assert_eq!(record.workloads["web"].env, sealed_once);
        assert_eq!(
            (record.generation, record.workloads["web"].revision),
            (2, 2)
        );

        // Another value is a template change.
        assert_eq!(apply(&mut w, "b", 4).updated, strings(&["web"]));
        let record = w.record("shop", "staging");
        assert_eq!(
            (record.generation, record.workloads["web"].revision),
            (4, 4)
        );
        assert_ne!(record.workloads["web"].env, sealed_once);
    }

    #[test]
    fn an_empty_index_is_the_default() {
        assert_eq!(
            serde_json::from_value::<ProjectIndex>(json!({})).unwrap(),
            ProjectIndex::default()
        );
        assert_eq!(
            serde_json::to_value(ProjectIndex::default()).unwrap(),
            json!({"projects": {}})
        );
    }

    /// The maps are defaulted, so a record stored without one still
    /// decodes, as empty.
    #[test]
    fn records_without_their_maps_decode() {
        assert_eq!(
            serde_json::from_value::<ProjectRecord>(json!({"created_at": 5})).unwrap(),
            ProjectRecord {
                created_at: 5,
                environments: BTreeMap::new(),
            }
        );
        assert_eq!(
            serde_json::from_value::<EnvironmentRecord>(json!({
                "generation": 7, "updated_at": 9
            }))
            .unwrap(),
            EnvironmentRecord {
                generation: 7,
                updated_at: 9,
                workloads: BTreeMap::new(),
            }
        );
    }

    // --- projects and environments ------------------------------------------

    #[test]
    fn create_project_adds_empty_environments() {
        let mut w = World::default();
        let records = w
            .index
            .clone()
            .create_project("shop", &strings(&["staging", "production"]), 4, 1_000)
            .unwrap();
        let empty = EnvironmentRecord {
            generation: 4,
            updated_at: 1_000,
            workloads: BTreeMap::new(),
        };
        assert_eq!(
            records,
            BTreeMap::from([
                ("production".to_string(), empty.clone()),
                ("staging".to_string(), empty.clone()),
            ])
        );

        w.create("shop", &["staging", "production"], 4, 1_000)
            .unwrap();
        let project = &w.index.projects["shop"];
        assert_eq!(project.created_at, 1_000);
        assert_eq!(
            project.environments["staging"],
            EnvironmentIndexEntry {
                generation: 4,
                bytes: json_len(&empty),
                workloads: 0,
                updated_at: 1_000,
            }
        );
        assert_eq!(project.environments.len(), 2);

        // A second project does not touch the first.
        let before = w.index.projects["shop"].clone();
        w.create("blog", &["dev"], 5, 1_001).unwrap();
        assert_eq!(w.index.projects["shop"], before);
        assert_eq!(w.record("blog", "dev").generation, 5);
        assert_invariants(&w);
    }

    #[test]
    fn create_project_rejections() {
        let mut w = shop();
        let too_long = "a".repeat(64);
        for bad in ["", "Shop", "my_shop", "a.b", "a/b", "-a", &too_long] {
            refused(&mut w, ec::BAD_REQUEST, |w| w.create(bad, &["dev"], 2, 1));
            refused(&mut w, ec::BAD_REQUEST, |w| {
                w.create("blog", &["dev", bad], 2, 1)
            });
        }
        // No environment, or one named twice.
        refused(&mut w, ec::BAD_REQUEST, |w| w.create("blog", &[], 2, 1));
        refused(&mut w, ec::BAD_REQUEST, |w| {
            w.create("blog", &["dev", "prod", "dev"], 2, 1)
        });
        // The name is taken, whatever the environments.
        refused(&mut w, ec::PROJECT_EXISTS, |w| {
            w.create("shop", &["dev"], 2, 1)
        });
        // A bad request is reported before the name clash.
        refused(&mut w, ec::BAD_REQUEST, |w| w.create("shop", &[], 2, 1));
        assert_invariants(&w);
    }

    #[test]
    fn projects_stop_at_the_limit() {
        let mut w = World::default();
        for n in 0..limits::MAX_PROJECTS {
            w.create(&format!("p{n}"), &["dev"], n as u64 + 1, 1)
                .unwrap();
        }
        assert_eq!(w.index.projects.len(), limits::MAX_PROJECTS);
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.create("one-more", &["dev"], 100, 1)
        });
        // An existing name is still reported as such at the limit.
        refused(&mut w, ec::PROJECT_EXISTS, |w| {
            w.create("p0", &["dev"], 100, 1)
        });
        // Deleting one makes room.
        w.delete("p0").unwrap();
        w.create("one-more", &["dev"], 101, 1).unwrap();
        assert_invariants(&w);
    }

    #[test]
    fn environments_stop_at_the_limit() {
        let names: Vec<String> = (0..=limits::MAX_ENVIRONMENTS_PER_PROJECT)
            .map(|n| format!("e{n}"))
            .collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let (most, one_more) = names.split_at(limits::MAX_ENVIRONMENTS_PER_PROJECT);

        // A creation that names too many can never work: a bad request.
        let mut w = World::default();
        refused(&mut w, ec::BAD_REQUEST, |w| w.create("big", &names, 1, 1));
        w.create("big", most, 1, 1).unwrap();
        assert_eq!(
            w.index.projects["big"].environments.len(),
            limits::MAX_ENVIRONMENTS_PER_PROJECT
        );

        // A full project has no room for another.
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.add_env("big", one_more[0], 2, 1)
        });
        // The duplicate is still reported as such at the limit.
        refused(&mut w, ec::PROJECT_EXISTS, |w| w.add_env("big", "e0", 2, 1));
        w.remove_env("big", "e0").unwrap();
        w.add_env("big", one_more[0], 3, 1).unwrap();

        // One below the limit, the last one fits.
        w.create("small", &most[1..], 4, 1).unwrap();
        w.add_env("small", "e0", 5, 1).unwrap();
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.add_env("small", one_more[0], 6, 1)
        });
        assert_invariants(&w);
    }

    #[test]
    fn add_environment_adds_an_empty_one() {
        let mut w = shop();
        let record = w
            .index
            .clone()
            .add_environment("shop", "dev", 6, 1_005)
            .unwrap();
        assert_eq!(
            record,
            EnvironmentRecord {
                generation: 6,
                updated_at: 1_005,
                workloads: BTreeMap::new(),
            }
        );
        let before = w.clone();
        w.add_env("shop", "dev", 6, 1_005).unwrap();
        assert_eq!(
            *w.entry("shop", "dev"),
            EnvironmentIndexEntry {
                generation: 6,
                bytes: json_len(&record),
                workloads: 0,
                updated_at: 1_005,
            }
        );
        // The project and its other environments are as they were.
        assert_eq!(w.index.projects["shop"].created_at, 1_000);
        assert_eq!(w.entry("shop", "staging"), before.entry("shop", "staging"));
        assert_eq!(
            w.record("shop", "staging"),
            before.record("shop", "staging")
        );
        assert_invariants(&w);
    }

    #[test]
    fn add_environment_rejections() {
        let mut w = shop();
        refused(&mut w, ec::NOT_FOUND, |w| w.add_env("blog", "dev", 2, 1));
        for bad in ["", "Dev", "a_b", "a/b", "dev-"] {
            refused(&mut w, ec::BAD_REQUEST, |w| w.add_env("shop", bad, 2, 1));
        }
        refused(&mut w, ec::PROJECT_EXISTS, |w| {
            w.add_env("shop", "staging", 2, 1)
        });
        // The unknown project is reported before the bad name.
        refused(&mut w, ec::NOT_FOUND, |w| w.add_env("blog", "Dev", 2, 1));
        assert_invariants(&w);
    }

    #[test]
    fn remove_environment_removes_an_empty_one() {
        let mut w = shop();
        w.remove_env("shop", "staging").unwrap();
        assert_eq!(
            w.index.projects["shop"]
                .environments
                .keys()
                .collect::<Vec<_>>(),
            vec!["production"]
        );
        // The same name again starts from the new index.
        w.add_env("shop", "staging", 9, 1_009).unwrap();
        assert_eq!(w.record("shop", "staging").generation, 9);
        assert_invariants(&w);
    }

    #[test]
    fn remove_environment_rejections() {
        let mut w = shop();
        w.apply("shop", "staging", &[("web", spec("nginx"))], false, 2, 1)
            .unwrap();
        refused(&mut w, ec::NOT_FOUND, |w| w.remove_env("blog", "staging"));
        refused(&mut w, ec::NOT_FOUND, |w| w.remove_env("shop", "dev"));
        refused(&mut w, ec::NOT_EMPTY, |w| w.remove_env("shop", "staging"));
        // Once its workload is gone it can go.
        w.remove("shop", "staging", &["web"], 3, 1).unwrap();
        w.remove_env("shop", "staging").unwrap();
        assert_invariants(&w);
    }

    #[test]
    fn delete_project_returns_its_environments() {
        let mut w = shop();
        w.create("blog", &["dev"], 2, 1).unwrap();
        assert_eq!(
            w.delete("shop").unwrap(),
            strings(&["production", "staging"])
        );
        assert_eq!(w.index.projects.keys().collect::<Vec<_>>(), vec!["blog"]);
        assert_invariants(&w);
    }

    #[test]
    fn delete_project_rejections() {
        let mut w = shop();
        w.apply("shop", "production", &[("web", spec("nginx"))], false, 2, 1)
            .unwrap();
        refused(&mut w, ec::NOT_FOUND, |w| w.delete("blog"));
        refused(&mut w, ec::NOT_EMPTY, |w| w.delete("shop"));
        w.remove("shop", "production", &["web"], 3, 1).unwrap();
        w.delete("shop").unwrap();
        assert_eq!(w, World::default());
        assert_invariants(&w);
    }

    /// A generation is a log index, so a project made again under the same
    /// name never repeats one.
    #[test]
    fn delete_and_recreate_gives_a_higher_generation() {
        let mut w = shop();
        w.apply("shop", "staging", &[("web", spec("nginx"))], false, 2, 1)
            .unwrap();
        w.remove("shop", "staging", &["web"], 3, 1).unwrap();
        let old = w.record("shop", "staging").generation;
        w.delete("shop").unwrap();

        w.create("shop", &["staging", "production"], 5, 2_000)
            .unwrap();
        assert!(w.record("shop", "staging").generation > old);
        assert_eq!(w.record("shop", "staging").generation, 5);
        assert_eq!(w.index.projects["shop"].created_at, 2_000);
        let applied = w
            .apply("shop", "staging", &[("web", spec("nginx"))], false, 6, 1)
            .unwrap();
        assert_eq!(applied.created, strings(&["web"]));
        assert_eq!(w.record("shop", "staging").workloads["web"].revision, 6);
        assert_invariants(&w);
    }

    // --- workloads ----------------------------------------------------------

    #[test]
    fn apply_creates_workloads() {
        let mut w = shop();
        let applied = w
            .apply(
                "shop",
                "staging",
                &[("web", spec("nginx")), ("api", spec("api:1"))],
                false,
                7,
                1_007,
            )
            .unwrap();
        assert_eq!(
            applied,
            Applied {
                generation: 7,
                created: strings(&["api", "web"]),
                ..Applied::default()
            }
        );
        let record = w.record("shop", "staging");
        assert_eq!((record.generation, record.updated_at), (7, 1_007));
        assert_eq!(
            record.workloads["web"],
            WorkloadRecord {
                revision: 7,
                spec: spec("nginx"),
                env: sealed(&spec("nginx")).env,
            }
        );
        assert_eq!(record.workloads["api"].revision, 7);
        assert_eq!(w.entry("shop", "staging").workloads, 2);
        // The other environment did not move.
        assert_eq!(w.record("shop", "production").generation, 1);
        assert_eq!(w.entry("shop", "production").updated_at, 1_000);
        assert_invariants(&w);
    }

    #[test]
    fn an_identical_apply_is_unchanged_and_writes_nothing() {
        let mut w = shop();
        let workloads = [("web", spec("nginx")), ("api", spec("api:1"))];
        w.apply("shop", "staging", &workloads, false, 2, 1_002)
            .unwrap();
        let before = w.clone();

        for prune in [false, true] {
            let done = w
                .index
                .clone()
                .apply_workloads(
                    Some(w.record("shop", "staging").clone()),
                    "shop",
                    "staging",
                    workloads
                        .iter()
                        .map(|(n, s)| (n.to_string(), sealed(s)))
                        .collect(),
                    prune,
                    9,
                    1_009,
                )
                .unwrap();
            assert_eq!(done.record, None);
            assert_eq!(
                done.applied,
                Applied {
                    // The stored generation, not this entry's index.
                    generation: 2,
                    unchanged: strings(&["api", "web"]),
                    ..Applied::default()
                }
            );
            // `World::apply` checks that the index did not move.
            w.apply("shop", "staging", &workloads, prune, 9, 1_009)
                .unwrap();
            assert_eq!(w, before);
        }

        // A part of what is stored is unchanged too, without `prune`.
        let applied = w
            .apply("shop", "staging", &[("web", spec("nginx"))], false, 10, 1)
            .unwrap();
        assert_eq!(applied.unchanged, strings(&["web"]));
        assert_eq!(applied.generation, 2);
        // And so is a deploy of nothing.
        let applied = w.apply("shop", "staging", &[], false, 11, 1).unwrap();
        assert_eq!(
            applied,
            Applied {
                generation: 2,
                ..Applied::default()
            }
        );
        assert_eq!(w, before);
        assert_invariants(&w);
    }

    /// `scale`, `placement` and `expose` are not part of the pod template:
    /// changing one is an update that leaves `revision` alone.
    #[test]
    fn a_change_outside_the_pod_template_keeps_the_revision() {
        let mut scaled = spec("nginx");
        scaled.scale = Scale::Replicas {
            count: 3,
            strategy: Strategy::Spread,
        };
        let mut daemon = spec("nginx");
        daemon.scale = Scale::Daemon;
        let mut placed = spec("nginx");
        placed.placement.nodes = vec!["node-2".into()];
        let mut exposed = spec("nginx");
        exposed.expose = Exposure::Cluster;
        let mut all = scaled.clone();
        all.placement.nodes = vec!["node-3".into()];
        all.expose = Exposure::Cluster;

        let mut w = shop();
        w.apply("shop", "staging", &[("web", spec("nginx"))], false, 2, 1)
            .unwrap();
        let changes = [scaled, daemon, placed, exposed, all, spec("nginx")];
        for (index, changed) in (3..).zip(changes) {
            let applied = w
                .apply(
                    "shop",
                    "staging",
                    &[("web", changed.clone())],
                    false,
                    index,
                    index + 1_000,
                )
                .unwrap();
            assert_eq!(
                applied,
                Applied {
                    generation: index,
                    updated: strings(&["web"]),
                    ..Applied::default()
                }
            );
            let record = w.record("shop", "staging");
            assert_eq!(record.workloads["web"].revision, 2, "{changed:?}");
            assert_eq!(record.workloads["web"].spec, changed);
            assert_eq!(
                (record.generation, record.updated_at),
                (index, index + 1_000)
            );
        }
        assert_invariants(&w);
    }

    #[test]
    fn a_pod_template_change_moves_the_revision() {
        let mut image = spec("nginx:2");
        let mut env = spec("nginx");
        env.containers[0].env.insert("A".into(), "1".into());
        let mut restart = spec("nginx");
        restart.restart = RestartPolicy::Never;
        let mut grace = spec("nginx");
        grace.stop_grace_secs = Some(5);
        let mut sidecar = spec("nginx");
        sidecar.containers.push(ContainerSpec::new("log", "fluent"));

        let mut w = shop();
        let mut index = 2;
        for changed in [env, restart, grace, sidecar, image.clone()] {
            w.apply(
                "shop",
                "staging",
                &[("web", spec("nginx"))],
                false,
                index,
                1,
            )
            .unwrap();
            let applied = w
                .apply(
                    "shop",
                    "staging",
                    &[("web", changed.clone())],
                    false,
                    index + 1,
                    1,
                )
                .unwrap();
            assert_eq!(applied.updated, strings(&["web"]), "{changed:?}");
            assert_eq!(
                w.record("shop", "staging").workloads["web"].revision,
                index + 1,
                "{changed:?}"
            );
            index += 2;
        }

        // A scale change keeps it. A template change that comes with a
        // scale change moves it, and the next scale change keeps that.
        image.scale = Scale::Replicas {
            count: 4,
            strategy: Strategy::Spread,
        };
        w.apply("shop", "staging", &[("web", image.clone())], false, 50, 1)
            .unwrap();
        assert_eq!(w.record("shop", "staging").workloads["web"].revision, 11);
        image.containers[0].image = "nginx:3".into();
        image.scale = Scale::Replicas {
            count: 5,
            strategy: Strategy::Spread,
        };
        w.apply("shop", "staging", &[("web", image.clone())], false, 51, 1)
            .unwrap();
        assert_eq!(w.record("shop", "staging").workloads["web"].revision, 51);
        image.scale = Scale::Replicas {
            count: 6,
            strategy: Strategy::Spread,
        };
        w.apply("shop", "staging", &[("web", image)], false, 52, 1)
            .unwrap();
        assert_eq!(w.record("shop", "staging").workloads["web"].revision, 51);
        assert_eq!(w.record("shop", "staging").generation, 52);
        assert_invariants(&w);
    }

    #[test]
    fn apply_sorts_every_workload_into_one_list() {
        let mut w = shop();
        w.apply(
            "shop",
            "staging",
            &[
                ("web", spec("nginx")),
                ("api", spec("api:1")),
                ("db", spec("postgres")),
                ("cache", spec("redis")),
            ],
            false,
            2,
            1,
        )
        .unwrap();
        let applied = w
            .apply(
                "shop",
                "staging",
                &[
                    ("worker", spec("worker")),
                    ("web", spec("nginx:2")),
                    ("db", spec("postgres")),
                    ("api", spec("api:2")),
                    ("admin", spec("admin")),
                ],
                true,
                3,
                1,
            )
            .unwrap();
        assert_eq!(
            applied,
            Applied {
                generation: 3,
                created: strings(&["admin", "worker"]),
                updated: strings(&["api", "web"]),
                unchanged: strings(&["db"]),
                removed: strings(&["cache"]),
            }
        );
        let record = w.record("shop", "staging");
        let revisions: Vec<(&str, u64)> = record
            .workloads
            .iter()
            .map(|(name, workload)| (name.as_str(), workload.revision))
            .collect();
        assert_eq!(
            revisions,
            vec![
                ("admin", 3),
                ("api", 3),
                ("db", 2),
                ("web", 3),
                ("worker", 3)
            ]
        );
        assert_invariants(&w);
    }

    #[test]
    fn prune_removes_what_the_deploy_does_not_name() {
        let mut w = shop();
        let three = [
            ("web", spec("nginx")),
            ("api", spec("api:1")),
            ("db", spec("postgres")),
        ];
        w.apply("shop", "staging", &three, false, 2, 1).unwrap();
        w.apply("shop", "production", &three, false, 3, 1).unwrap();

        // Only removals: still a change.
        let applied = w
            .apply("shop", "staging", &three[..1], true, 4, 1_004)
            .unwrap();
        assert_eq!(
            applied,
            Applied {
                generation: 4,
                unchanged: strings(&["web"]),
                removed: strings(&["api", "db"]),
                ..Applied::default()
            }
        );
        let record = w.record("shop", "staging");
        assert_eq!(record.workloads.keys().collect::<Vec<_>>(), vec!["web"]);
        assert_eq!(record.workloads["web"].revision, 2);
        assert_eq!((record.generation, record.updated_at), (4, 1_004));
        // The other environment keeps all of its own.
        assert_eq!(w.entry("shop", "production").workloads, 3);
        assert_eq!(w.record("shop", "production").generation, 3);

        // A pruning deploy of nothing empties the environment.
        let applied = w.apply("shop", "staging", &[], true, 5, 1).unwrap();
        assert_eq!(applied.removed, strings(&["web"]));
        assert_eq!(w.entry("shop", "staging").workloads, 0);
        assert_eq!(w.entry("shop", "staging").generation, 5);
        assert_invariants(&w);
    }

    #[test]
    fn apply_without_prune_leaves_other_workloads_alone() {
        let mut w = shop();
        w.apply(
            "shop",
            "staging",
            &[("web", spec("nginx")), ("db", spec("postgres"))],
            false,
            2,
            1,
        )
        .unwrap();
        let db = w.record("shop", "staging").workloads["db"].clone();

        let applied = w
            .apply(
                "shop",
                "staging",
                &[("web", spec("nginx:2")), ("api", spec("api:1"))],
                false,
                3,
                1,
            )
            .unwrap();
        assert_eq!(applied.removed, Vec::<String>::new());
        let record = w.record("shop", "staging");
        assert_eq!(record.workloads["db"], db);
        assert_eq!(
            record.workloads.keys().collect::<Vec<_>>(),
            vec!["api", "db", "web"]
        );
        assert_invariants(&w);
    }

    #[test]
    fn apply_workloads_rejections() {
        let mut w = shop();
        let web = [("web", spec("nginx"))];
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.apply("blog", "staging", &web, false, 2, 1)
        });
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.apply("shop", "dev", &web, false, 2, 1)
        });
        let too_long = "a".repeat(64);
        for bad in ["", "Web", "db_backup", "a.b", "a/b", "-web", &too_long] {
            // One bad name refuses the whole deploy.
            refused(&mut w, ec::BAD_REQUEST, |w| {
                w.apply(
                    "shop",
                    "staging",
                    &[("web", spec("nginx")), (bad, spec("nginx"))],
                    false,
                    2,
                    1,
                )
            });
        }
        // The unknown environment is reported before the bad name.
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.apply("shop", "dev", &[("Web", spec("nginx"))], true, 2, 1)
        });
        assert_invariants(&w);
    }

    /// Apply checks names, sizes, counts and existence, and nothing else: a
    /// spec the model validation refuses is stored as it is. The service
    /// validates before it proposes.
    #[test]
    fn apply_does_not_run_the_model_validation() {
        let mut none = spec("nginx");
        none.containers.clear();
        let mut odd = spec("");
        odd.containers[0].name = "Not A Label".into();
        odd.scale = Scale::Replicas {
            count: u32::MAX,
            strategy: Strategy::Spread,
        };
        odd.containers.push(odd.containers[0].clone());

        let mut w = shop();
        let applied = w
            .apply(
                "shop",
                "staging",
                &[("none", none.clone()), ("odd", odd.clone())],
                false,
                2,
                1,
            )
            .unwrap();
        assert_eq!(applied.created, strings(&["none", "odd"]));
        assert_eq!(w.record("shop", "staging").workloads["none"].spec, none);
        assert_eq!(w.record("shop", "staging").workloads["odd"].spec, odd);
        assert_invariants(&w);
    }

    #[test]
    fn a_deploy_stops_at_the_request_size_limit() {
        let limit = limits::MAX_DEPLOY_REQUEST_BYTES as u64;
        let request = |pad: usize| BTreeMap::from([("big".to_string(), padded(pad))]);
        let pad = (limit - json_len(&request(0))) as usize;
        assert_eq!(json_len(&request(pad)), limit);

        let mut w = shop();
        refused(&mut w, ec::BAD_REQUEST, |w| {
            w.apply("shop", "staging", &[("big", padded(pad + 1))], false, 2, 1)
        });
        w.apply("shop", "staging", &[("big", padded(pad))], false, 2, 1)
            .unwrap();
        // The limit is on the request: one that is too large is refused
        // even when it would change nothing or shrink the environment.
        w.apply("shop", "staging", &[("big", padded(pad))], false, 3, 1)
            .unwrap();
        refused(&mut w, ec::BAD_REQUEST, |w| {
            w.apply(
                "shop",
                "staging",
                &[("big", padded(pad)), ("b", spec("b"))],
                true,
                4,
                1,
            )
        });
        assert_invariants(&w);
    }

    #[test]
    fn workloads_stop_at_the_limit() {
        let max = limits::MAX_WORKLOADS_PER_ENVIRONMENT;
        let named: Vec<(String, WorkloadSpec)> =
            (0..=max).map(|n| (format!("w{n}"), spec("img"))).collect();
        let named: Vec<(&str, WorkloadSpec)> =
            named.iter().map(|(n, s)| (n.as_str(), s.clone())).collect();
        let (most, one_more) = named.split_at(max);

        // Too many in one deploy.
        let mut w = shop();
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.apply("shop", "staging", &named, false, 2, 1)
        });
        w.apply("shop", "staging", most, false, 2, 1).unwrap();
        assert_eq!(w.entry("shop", "staging").workloads as usize, max);

        // Too many together with what is stored.
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.apply("shop", "staging", one_more, false, 3, 1)
        });
        // An update of a full environment is not an addition.
        w.apply("shop", "staging", &[("w0", spec("img:2"))], false, 3, 1)
            .unwrap();
        // The limit is on the result: with `prune` the new one replaces
        // the others.
        w.apply("shop", "staging", one_more, true, 4, 1).unwrap();
        assert_eq!(w.entry("shop", "staging").workloads, 1);
        // The other environment has its own count.
        w.apply("shop", "production", most, false, 5, 1).unwrap();
        assert_invariants(&w);
    }

    #[test]
    fn an_environment_stops_at_the_record_size_limit() {
        let limit = limits::MAX_ENVIRONMENT_RECORD_BYTES as u64;
        let mut w = shop();
        let (name, pad) = w.grow_towards("shop", "staging", limit, 2);

        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.apply(
                "shop",
                "staging",
                &[(name.as_str(), padded(pad + 1))],
                false,
                2,
                1,
            )
        });
        w.apply(
            "shop",
            "staging",
            &[(name.as_str(), padded(pad))],
            false,
            2,
            1,
        )
        .unwrap();
        assert_eq!(w.entry("shop", "staging").bytes, limit);

        // Full: nothing more fits, but a smaller spec does.
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.apply("shop", "staging", &[("one-more", spec("img"))], false, 2, 1)
        });
        w.apply(
            "shop",
            "staging",
            &[(name.as_str(), padded(0))],
            false,
            2,
            1,
        )
        .unwrap();
        // The other environment has its own size.
        w.apply("shop", "production", &[("web", spec("nginx"))], false, 2, 1)
            .unwrap();
        assert_invariants(&w);
    }

    #[test]
    fn the_cluster_stops_at_the_total_size_limit() {
        let limit = limits::MAX_TOTAL_BYTES as u64;
        let environments: Vec<String> = (0..limits::MAX_ENVIRONMENTS_PER_PROJECT)
            .map(|n| format!("e{n}"))
            .collect();
        let environments: Vec<&str> = environments.iter().map(String::as_str).collect();
        let mut w = World::default();
        for project in ["p0", "p1", "p2"] {
            w.create(project, &environments, 1, 1).unwrap();
        }
        // 32 environments of 500 KiB leave less than one full record.
        for project in ["p0", "p1"] {
            for environment in &environments {
                w.grow_to(project, environment, 500 * 1024, 2);
            }
        }
        let others = w.total_bytes() - w.entry("p2", "e0").bytes;
        let target = limit - others;
        assert!(target + 1 < limits::MAX_ENVIRONMENT_RECORD_BYTES as u64);
        let (name, pad) = w.grow_towards("p2", "e0", target, 2);

        // The record itself would fit; all of them together would not.
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.apply("p2", "e0", &[(name.as_str(), padded(pad + 1))], false, 2, 1)
        });
        w.apply("p2", "e0", &[(name.as_str(), padded(pad))], false, 2, 1)
            .unwrap();
        assert_eq!(w.total_bytes(), limit);

        // Full, in every environment.
        refused(&mut w, ec::WORKLOADS_EXHAUSTED, |w| {
            w.apply("p2", "e1", &[("web", spec("nginx"))], false, 3, 1)
        });
        // Removing a workload anywhere makes room.
        w.remove("p0", "e0", &["pad-0"], 4, 1).unwrap();
        w.apply("p2", "e1", &[("web", spec("nginx"))], false, 5, 1)
            .unwrap();
        assert_invariants(&w);
    }

    #[test]
    fn remove_workloads_removes_them() {
        let mut w = shop();
        w.apply(
            "shop",
            "staging",
            &[
                ("web", spec("nginx")),
                ("api", spec("api:1")),
                ("db", spec("postgres")),
            ],
            false,
            2,
            1,
        )
        .unwrap();
        let db = w.record("shop", "staging").workloads["db"].clone();

        // A name given twice is removed once.
        w.remove("shop", "staging", &["web", "api", "web"], 8, 1_008)
            .unwrap();
        let record = w.record("shop", "staging");
        assert_eq!(record.workloads, BTreeMap::from([("db".to_string(), db)]));
        assert_eq!((record.generation, record.updated_at), (8, 1_008));
        assert_eq!(w.entry("shop", "staging").workloads, 1);

        // The name is free again, and the workload is new.
        let applied = w
            .apply("shop", "staging", &[("web", spec("nginx"))], false, 9, 1)
            .unwrap();
        assert_eq!(applied.created, strings(&["web"]));
        assert_eq!(w.record("shop", "staging").workloads["web"].revision, 9);
        assert_invariants(&w);
    }

    #[test]
    fn remove_workloads_rejections() {
        let mut w = shop();
        w.apply(
            "shop",
            "staging",
            &[("web", spec("nginx")), ("api", spec("api:1"))],
            false,
            2,
            1,
        )
        .unwrap();
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.remove("blog", "staging", &["web"], 3, 1)
        });
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.remove("shop", "dev", &["web"], 3, 1)
        });
        // One unknown name: none of them is removed.
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.remove("shop", "staging", &["web", "db", "api"], 3, 1)
        });
        // The workload is in the other environment only.
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.remove("shop", "production", &["web"], 3, 1)
        });
        assert_eq!(w.entry("shop", "staging").workloads, 2);
        assert_invariants(&w);
    }

    /// Removing no workload changes nothing, so the generation stays.
    #[test]
    fn remove_workloads_of_nothing_keeps_the_generation() {
        let mut w = shop();
        w.apply(
            "shop",
            "staging",
            &[("web", spec("nginx"))],
            false,
            2,
            1_002,
        )
        .unwrap();
        let before = w.clone();
        w.remove("shop", "staging", &[], 3, 1_003).unwrap();
        assert_eq!(w, before);
        assert_invariants(&w);
    }

    /// Every environment has a record. Should one be missing, the commands
    /// take it as empty instead of failing.
    #[test]
    fn a_missing_record_counts_as_empty() {
        let mut w = shop();
        w.records.remove(&key("shop", "staging"));
        refused(&mut w, ec::NOT_FOUND, |w| {
            w.remove("shop", "staging", &["web"], 2, 1)
        });
        let applied = w
            .apply("shop", "staging", &[("web", spec("nginx"))], false, 2, 1)
            .unwrap();
        assert_eq!(applied.created, strings(&["web"]));
        assert_invariants(&w);
    }
}
