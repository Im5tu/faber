//! The workload model: what a deploy stores and what the runner will read.
//!
//! A workload is pod-shaped: replicas of one or more containers that share
//! a network namespace. It lives in one environment of one project and is
//! named `<workload>.<environment>.<project>.faber.local` ([`fqdn`]).
//!
//! Every type refuses unknown fields. A field with a default may be left
//! out on input and is always written on output, so a default that changes
//! later never changes stored data. Only absent values (`Option`) are left
//! out. Validation never rewrites a spec, so two specs are the same exactly
//! when they are `==`.
//!
//! The runner contract: every container runs unprivileged with all
//! capabilities dropped and no-new-privileges set, and gets only the
//! capabilities its spec lists.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

/// Suffix of every workload name in DNS.
pub const DNS_SUFFIX: &str = "faber.local";

/// The environments a project gets when its creation names none.
pub const DEFAULT_ENVIRONMENTS: [&str; 2] = ["staging", "production"];

/// Size and count limits of the model. The service and the CLI check all of
/// them; apply checks the counts and the sizes again.
pub mod limits {
    /// Projects in a cluster.
    pub const MAX_PROJECTS: usize = 64;
    /// Environments in one project.
    pub const MAX_ENVIRONMENTS_PER_PROJECT: usize = 16;
    /// Workloads in one environment.
    pub const MAX_WORKLOADS_PER_ENVIRONMENT: usize = 64;
    /// Containers in one workload.
    pub const MAX_CONTAINERS_PER_WORKLOAD: usize = 16;
    /// Highest replica count of one workload.
    pub const MAX_REPLICAS: u32 = 1024;
    /// Environment variables of one container.
    pub const MAX_ENV_VARS: usize = 256;
    /// Bytes in the value of one environment variable: 32 KiB.
    pub const MAX_ENV_VALUE_BYTES: usize = 32 * 1024;
    /// Bytes of the `workloads` map of one deploy request, as JSON: 128 KiB.
    pub const MAX_DEPLOY_REQUEST_BYTES: usize = 128 * 1024;
    /// Bytes of one stored environment record, as JSON: 512 KiB.
    pub const MAX_ENVIRONMENT_RECORD_BYTES: usize = 512 * 1024;
    /// Bytes of all stored environment records together: 16 MiB.
    pub const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
    /// Bytes of an HTTP request body on the workload routes: 1 MiB.
    pub const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
    /// Volumes of one workload.
    pub const MAX_VOLUMES_PER_WORKLOAD: usize = 32;
    /// Mounts of one container.
    pub const MAX_MOUNTS_PER_CONTAINER: usize = 32;
    /// Declared ports of one container.
    pub const MAX_PORTS_PER_CONTAINER: usize = 64;
    /// Bytes in an image reference.
    pub const MAX_IMAGE_BYTES: usize = 512;
    /// Bytes in the name of one environment variable.
    pub const MAX_ENV_NAME_BYTES: usize = 256;
    /// Entries of `command` and `args` of one container together.
    pub const MAX_COMMAND_ENTRIES: usize = 256;
    /// Bytes in one entry of a command: 8 KiB.
    pub const MAX_COMMAND_ENTRY_BYTES: usize = 8 * 1024;
    /// Bytes in a path.
    pub const MAX_PATH_BYTES: usize = 4096;
    /// Highest `stop_grace_secs`: one hour.
    pub const MAX_STOP_GRACE_SECS: u32 = 3600;
    /// Highest `cpu_millis`: 256 cores.
    pub const MAX_CPU_MILLIS: u64 = 256_000;
    /// Highest `memory_bytes`: 4 TiB.
    pub const MAX_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
    /// Lowest `size_bytes` of a volume: 1 MiB.
    pub const MIN_VOLUME_BYTES: u64 = 1024 * 1024;
    /// Highest `size_bytes` of a block volume: 1 TiB.
    pub const MAX_BLOCK_VOLUME_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
    /// Highest `size_bytes` of a ramdisk volume: 64 GiB.
    pub const MAX_RAMDISK_VOLUME_BYTES: u64 = 64 * 1024 * 1024 * 1024;
    /// Bytes of all block volumes of one workload together: 4 TiB.
    pub const MAX_TOTAL_BLOCK_BYTES_PER_WORKLOAD: u64 = 4 * 1024 * 1024 * 1024 * 1024;
    /// Bytes of all ramdisk volumes of one workload together: 256 GiB.
    pub const MAX_TOTAL_RAMDISK_BYTES_PER_WORKLOAD: u64 = 256 * 1024 * 1024 * 1024;
    /// Nodes in `placement.nodes`.
    pub const MAX_PLACEMENT_NODES: usize = 64;
    /// Highest `interval_secs` of a probe: one hour.
    pub const MAX_PROBE_INTERVAL_SECS: u32 = 3600;
    /// Failed probes in a row that make the runner restart the container.
    /// The first failure already takes the replica out of DNS.
    pub const PROBE_FAILURES_BEFORE_RESTART: u32 = 3;
}

/// The capabilities a container may list in
/// [`ContainerSpec::capabilities`]. Every other one of [`CAPABILITIES`]
/// gives its holder control of the node and is refused.
pub const ALLOWED_CAPABILITIES: &[&str] = &[
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "KILL",
    "NET_BIND_SERVICE",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYS_CHROOT",
];

/// Every capability the kernel has: its names in upper case, without the
/// `CAP_` prefix. For telling a known name from a misspelt one; a container
/// may list only [`ALLOWED_CAPABILITIES`].
pub const CAPABILITIES: &[&str] = &[
    "CHOWN",
    "DAC_OVERRIDE",
    "DAC_READ_SEARCH",
    "FOWNER",
    "FSETID",
    "KILL",
    "SETGID",
    "SETUID",
    "SETPCAP",
    "LINUX_IMMUTABLE",
    "NET_BIND_SERVICE",
    "NET_BROADCAST",
    "NET_ADMIN",
    "NET_RAW",
    "IPC_LOCK",
    "IPC_OWNER",
    "SYS_MODULE",
    "SYS_RAWIO",
    "SYS_CHROOT",
    "SYS_PTRACE",
    "SYS_PACCT",
    "SYS_ADMIN",
    "SYS_BOOT",
    "SYS_NICE",
    "SYS_RESOURCE",
    "SYS_TIME",
    "SYS_TTY_CONFIG",
    "MKNOD",
    "LEASE",
    "AUDIT_WRITE",
    "AUDIT_CONTROL",
    "SETFCAP",
    "MAC_OVERRIDE",
    "MAC_ADMIN",
    "SYSLOG",
    "WAKE_ALARM",
    "BLOCK_SUSPEND",
    "AUDIT_READ",
    "PERFMON",
    "BPF",
    "CHECKPOINT_RESTORE",
];

/// The DNS name of a workload:
/// `<workload>.<environment>.<project>.faber.local`.
pub fn fqdn(workload: &str, environment: &str, project: &str) -> String {
    format!("{workload}.{environment}.{project}.{DNS_SUFFIX}")
}

/// One rule a spec breaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// Where, in the JSON of what was validated: `containers[0].image`,
    /// `volumes.data.path`. Empty for a rule about the whole value.
    pub path: String,
    pub message: String,
}

impl Violation {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Violation {
    /// `<path>: <message>`, or the message alone without a path.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.path, self.message)
        }
    }
}

/// Desired state of one workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    #[serde(default)]
    pub scale: Scale,
    #[serde(default)]
    pub restart: RestartPolicy,
    /// Who may resolve and reach the workload. Stored, not enforced yet.
    #[serde(default)]
    pub expose: Exposure,
    #[serde(default)]
    pub placement: Placement,
    /// Seconds between the stop signal and the kill. The runner's default
    /// applies when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_grace_secs: Option<u32>,
    /// Volumes of the pod by name. Containers mount them with
    /// [`MountSpec::volume`].
    #[serde(default)]
    pub volumes: BTreeMap<String, VolumeSource>,
    /// 1 to 16 containers with unique names.
    pub containers: Vec<ContainerSpec>,
}

impl WorkloadSpec {
    /// Checks every rule of the model and returns all the rules the spec
    /// breaks, with paths relative to the spec. Pure: it reads nothing but
    /// `self` and never changes it.
    pub fn validate(&self) -> Result<(), Vec<Violation>> {
        let mut out = Vec::new();
        check_spec(self, &mut out);
        if out.is_empty() { Ok(()) } else { Err(out) }
    }
}

/// Checks the `workloads` map of a deploy: every name is a DNS label, there
/// are at most [`limits::MAX_WORKLOADS_PER_ENVIRONMENT`] of them, every spec
/// passes [`WorkloadSpec::validate`], and the map is at most
/// [`limits::MAX_DEPLOY_REQUEST_BYTES`] as JSON.
///
/// Paths are relative to the map: `web.containers[0].image` for a spec,
/// `web` for a name, empty for the size.
pub fn validate_workloads(
    workloads: &BTreeMap<String, WorkloadSpec>,
) -> Result<(), Vec<Violation>> {
    let mut out = Vec::new();
    if workloads.len() > limits::MAX_WORKLOADS_PER_ENVIRONMENT {
        out.push(Violation::new(
            "",
            format!(
                "must hold at most {} workloads",
                limits::MAX_WORKLOADS_PER_ENVIRONMENT
            ),
        ));
    }
    for (name, spec) in workloads {
        let at = segment(name);
        check_label(&at, name, &mut out);
        let mut found = Vec::new();
        check_spec(spec, &mut found);
        out.extend(
            found
                .into_iter()
                .map(|v| Violation::new(format!("{at}.{}", v.path), v.message)),
        );
    }
    // Counted, not built: the map may be far over the limit.
    let mut size = ByteCount(0);
    match serde_json::to_writer(&mut size, workloads) {
        Ok(()) if size.0 > limits::MAX_DEPLOY_REQUEST_BYTES => out.push(Violation::new(
            "",
            format!(
                "is {} bytes as JSON, at most {} are allowed",
                size.0,
                limits::MAX_DEPLOY_REQUEST_BYTES
            ),
        )),
        Ok(()) => {}
        Err(_) => out.push(Violation::new("", "cannot be written as JSON")),
    }
    if out.is_empty() { Ok(()) } else { Err(out) }
}

/// Parts of an env variable name that suggest its value is a secret.
const SECRET_NAME_PARTS: [&str; 6] = [
    "PASSWORD",
    "PASSWD",
    "SECRET",
    "TOKEN",
    "API_KEY",
    "PRIVATE_KEY",
];

const NOTE_NO_CAPABILITIES: &str = "containers get no Linux capabilities unless the file adds \
     them; images that expect Docker's default set may fail to start";

/// Front ends add what they do with the values after this text.
pub const NOTE_SECRET_NAMES: &str =
    "an env variable name looks like a secret; env values are encrypted at rest";

/// What a [`DeployIssue`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployIssueSource {
    /// The workloads themselves.
    Input,
    /// The prune option.
    Prune,
    /// The expose option.
    Expose,
}

/// One finding of [`prepare_deploy`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployIssue {
    pub source: DeployIssueSource,
    /// Where in the workloads, as in [`Violation::path`]. Empty for the whole
    /// input or for an option.
    pub path: String,
    pub message: String,
}

impl DeployIssue {
    fn new(source: DeployIssueSource, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            source,
            path: path.into(),
            message: message.into(),
        }
    }
}

/// The checks every deploy front end makes before it sends the workloads.
///
/// Sets `expose = cluster` on the workloads named in `expose`, refuses an
/// empty input (and `prune` with an empty input, which would remove every
/// workload of the environment) and runs [`validate_workloads`]. Returns the
/// notes for the user, or every issue found, at least one.
pub fn prepare_deploy(
    workloads: &mut BTreeMap<String, WorkloadSpec>,
    expose: &[String],
    prune: bool,
) -> Result<Vec<String>, Vec<DeployIssue>> {
    let mut issues = Vec::new();
    if workloads.is_empty() && !prune {
        issues.push(DeployIssue::new(
            DeployIssueSource::Input,
            "",
            "nothing to deploy: the input has no workloads",
        ));
    } else if workloads.is_empty() {
        issues.push(DeployIssue::new(
            DeployIssueSource::Prune,
            "",
            "the input has no workloads, so every workload of the environment would be removed",
        ));
    }
    for name in expose {
        match workloads.get_mut(name) {
            Some(spec) => spec.expose = Exposure::Cluster,
            None => {
                let known: Vec<&str> = workloads.keys().map(String::as_str).collect();
                issues.push(DeployIssue::new(
                    DeployIssueSource::Expose,
                    "",
                    format!(
                        "no workload named {name:?}; the input has: {}",
                        known.join(", ")
                    ),
                ));
            }
        }
    }
    if let Err(violations) = validate_workloads(workloads) {
        issues.extend(
            violations
                .into_iter()
                .map(|v| DeployIssue::new(DeployIssueSource::Input, v.path, v.message)),
        );
    }
    if issues.is_empty() {
        Ok(deploy_notes(workloads))
    } else {
        Err(issues)
    }
}

fn deploy_notes(workloads: &BTreeMap<String, WorkloadSpec>) -> Vec<String> {
    let mut containers = workloads.values().flat_map(|spec| &spec.containers);
    let mut notes = Vec::new();
    if containers.clone().all(|c| c.capabilities.is_empty()) {
        notes.push(NOTE_NO_CAPABILITIES.to_string());
    }
    if containers.any(|c| c.env.keys().any(|name| looks_like_a_secret(name))) {
        notes.push(NOTE_SECRET_NAMES.to_string());
    }
    notes
}

fn looks_like_a_secret(env_name: &str) -> bool {
    let upper = env_name.to_ascii_uppercase();
    SECRET_NAME_PARTS.iter().any(|part| upper.contains(part))
}

/// A writer that keeps only the number of bytes written to it.
struct ByteCount(usize);

impl std::io::Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Longest map key printed in a [`Violation::path`], in characters.
const MAX_SEGMENT_CHARS: usize = 64;

/// A map key as one segment of a [`Violation::path`]. A key that breaks its
/// rule can be any string, so control characters become `?` and a long key
/// is cut. A DNS label always comes through whole.
fn segment(key: &str) -> String {
    let mut out: String = key
        .chars()
        .take(MAX_SEGMENT_CHARS)
        .map(|c| if c.is_control() { '?' } else { c })
        .collect();
    if key.chars().nth(MAX_SEGMENT_CHARS).is_some() {
        out.push_str("...");
    }
    out
}

fn check_label(at: &str, name: &str, out: &mut Vec<Violation>) {
    if let Err(message) = crate::name::validate_dns_label(name) {
        out.push(Violation::new(at, message));
    }
}

fn check_spec(spec: &WorkloadSpec, out: &mut Vec<Violation>) {
    if let Scale::Replicas { count, .. } = spec.scale
        && count > limits::MAX_REPLICAS
    {
        out.push(Violation::new(
            "scale.replicas.count",
            format!("must be at most {}", limits::MAX_REPLICAS),
        ));
    }
    if spec.placement.nodes.len() > limits::MAX_PLACEMENT_NODES {
        out.push(Violation::new(
            "placement.nodes",
            format!("must hold at most {} nodes", limits::MAX_PLACEMENT_NODES),
        ));
    }
    let mut nodes = BTreeSet::new();
    for (i, node) in spec.placement.nodes.iter().enumerate() {
        let at = format!("placement.nodes[{i}]");
        check_label(&at, node, out);
        if !nodes.insert(node.as_str()) {
            out.push(Violation::new(at, "duplicate node"));
        }
    }
    if spec
        .stop_grace_secs
        .is_some_and(|secs| secs > limits::MAX_STOP_GRACE_SECS)
    {
        out.push(Violation::new(
            "stop_grace_secs",
            format!("must be at most {}", limits::MAX_STOP_GRACE_SECS),
        ));
    }

    if spec.volumes.len() > limits::MAX_VOLUMES_PER_WORKLOAD {
        out.push(Violation::new(
            "volumes",
            format!(
                "must hold at most {} volumes",
                limits::MAX_VOLUMES_PER_WORKLOAD
            ),
        ));
    }
    let (mut block_total, mut ramdisk_total) = (0u128, 0u128);
    for (name, source) in &spec.volumes {
        let at = format!("volumes.{}", segment(name));
        check_label(&at, name, out);
        let (kind, size_bytes, most) = match source {
            VolumeSource::Block { size_bytes } => {
                ("block", *size_bytes, limits::MAX_BLOCK_VOLUME_BYTES)
            }
            VolumeSource::Ramdisk { size_bytes } => {
                ("ramdisk", *size_bytes, limits::MAX_RAMDISK_VOLUME_BYTES)
            }
        };
        match source {
            VolumeSource::Block { .. } => block_total += u128::from(size_bytes),
            VolumeSource::Ramdisk { .. } => ramdisk_total += u128::from(size_bytes),
        }
        let at = format!("{at}.size_bytes");
        if size_bytes < limits::MIN_VOLUME_BYTES {
            out.push(Violation::new(
                at,
                format!("must be at least {}", limits::MIN_VOLUME_BYTES),
            ));
        } else if size_bytes > most {
            out.push(Violation::new(
                at,
                format!("must be at most {most} for a {kind} volume"),
            ));
        }
    }
    for (kind, total, most) in [
        (
            "block",
            block_total,
            limits::MAX_TOTAL_BLOCK_BYTES_PER_WORKLOAD,
        ),
        (
            "ramdisk",
            ramdisk_total,
            limits::MAX_TOTAL_RAMDISK_BYTES_PER_WORKLOAD,
        ),
    ] {
        if total > u128::from(most) {
            out.push(Violation::new(
                "volumes",
                format!("{kind} volumes total {total} bytes, at most {most} are allowed"),
            ));
        }
    }

    if spec.containers.is_empty() {
        out.push(Violation::new(
            "containers",
            "must hold at least one container",
        ));
    }
    if spec.containers.len() > limits::MAX_CONTAINERS_PER_WORKLOAD {
        out.push(Violation::new(
            "containers",
            format!(
                "must hold at most {} containers",
                limits::MAX_CONTAINERS_PER_WORKLOAD
            ),
        ));
    }
    let mut names = BTreeSet::new();
    for (i, container) in spec.containers.iter().enumerate() {
        let at = format!("containers[{i}]");
        check_label(&format!("{at}.name"), &container.name, out);
        if !names.insert(container.name.as_str()) {
            out.push(Violation::new(
                format!("{at}.name"),
                "duplicate container name",
            ));
        }
        check_container(&at, container, &spec.volumes, out);
    }
}

/// Everything about one container except its name.
fn check_container(
    at: &str,
    container: &ContainerSpec,
    volumes: &BTreeMap<String, VolumeSource>,
    out: &mut Vec<Violation>,
) {
    let image = &container.image;
    if image.is_empty() {
        out.push(Violation::new(format!("{at}.image"), "must not be empty"));
    }
    if image.len() > limits::MAX_IMAGE_BYTES {
        out.push(Violation::new(
            format!("{at}.image"),
            format!("must be at most {} bytes", limits::MAX_IMAGE_BYTES),
        ));
    }
    check_ascii_word(&format!("{at}.image"), image, out);

    if container.command.len() + container.args.len() > limits::MAX_COMMAND_ENTRIES {
        out.push(Violation::new(
            format!("{at}.command"),
            format!(
                "command and args together must hold at most {} entries",
                limits::MAX_COMMAND_ENTRIES
            ),
        ));
    }
    check_command_entries(&format!("{at}.command"), &container.command, out);
    check_command_entries(&format!("{at}.args"), &container.args, out);

    if container.env.len() > limits::MAX_ENV_VARS {
        out.push(Violation::new(
            format!("{at}.env"),
            format!("must hold at most {} variables", limits::MAX_ENV_VARS),
        ));
    }
    for (name, value) in &container.env {
        let at = format!("{at}.env.{}", segment(name));
        if !is_env_name(name) {
            out.push(Violation::new(
                at.as_str(),
                "name must match [A-Za-z_][A-Za-z0-9_.-]*",
            ));
        }
        if name.len() > limits::MAX_ENV_NAME_BYTES {
            out.push(Violation::new(
                at.as_str(),
                format!("name must be at most {} bytes", limits::MAX_ENV_NAME_BYTES),
            ));
        }
        // The value is never part of a message: it may be a secret.
        if value.len() > limits::MAX_ENV_VALUE_BYTES {
            out.push(Violation::new(
                at.as_str(),
                format!(
                    "value must be at most {} bytes",
                    limits::MAX_ENV_VALUE_BYTES
                ),
            ));
        }
        if value.contains('\0') {
            out.push(Violation::new(at, "value must not contain a NUL character"));
        }
    }

    if let Some(dir) = &container.working_dir {
        check_absolute_path(&format!("{at}.working_dir"), dir, out);
    }
    if let Some(user) = &container.user {
        check_user(&format!("{at}.user"), user, out);
    }

    if container.ports.len() > limits::MAX_PORTS_PER_CONTAINER {
        out.push(Violation::new(
            format!("{at}.ports"),
            format!(
                "must hold at most {} ports",
                limits::MAX_PORTS_PER_CONTAINER
            ),
        ));
    }
    let mut port_names = BTreeSet::new();
    let mut ports = HashSet::new();
    for (i, port) in container.ports.iter().enumerate() {
        let at = format!("{at}.ports[{i}]");
        if let Some(name) = &port.name {
            check_label(&format!("{at}.name"), name, out);
            if !port_names.insert(name.as_str()) {
                out.push(Violation::new(format!("{at}.name"), "duplicate port name"));
            }
        }
        if port.port == 0 {
            out.push(Violation::new(format!("{at}.port"), "must not be 0"));
        }
        if !ports.insert((port.port, port.protocol)) {
            out.push(Violation::new(at, "duplicate port and protocol"));
        }
    }

    if container.mounts.len() > limits::MAX_MOUNTS_PER_CONTAINER {
        out.push(Violation::new(
            format!("{at}.mounts"),
            format!(
                "must hold at most {} mounts",
                limits::MAX_MOUNTS_PER_CONTAINER
            ),
        ));
    }
    let mut mount_paths = BTreeSet::new();
    for (i, mount) in container.mounts.iter().enumerate() {
        let at = format!("{at}.mounts[{i}]");
        if !volumes.contains_key(&mount.volume) {
            out.push(Violation::new(
                format!("{at}.volume"),
                "is not a volume of the workload",
            ));
        }
        check_absolute_path(&format!("{at}.path"), &mount.path, out);
        if mount.path == "/" {
            out.push(Violation::new(format!("{at}.path"), "must not be /"));
        }
        if is_kernel_directory(&mount.path) {
            out.push(Violation::new(
                format!("{at}.path"),
                "must not be /proc, /sys or /dev or under them, except /dev/shm",
            ));
        }
        if !mount_paths.insert(mount.path.as_str()) {
            out.push(Violation::new(format!("{at}.path"), "duplicate mount path"));
        }
    }
    // A mount under another hides it or is hidden by it. Equal paths are
    // reported as duplicates above.
    for (i, mount) in container.mounts.iter().enumerate() {
        let inside = |outer: &str, inner: &str| {
            inner
                .strip_prefix(outer)
                .is_some_and(|rest| rest.starts_with('/'))
        };
        if container.mounts[..i]
            .iter()
            .any(|earlier| inside(&earlier.path, &mount.path) || inside(&mount.path, &earlier.path))
        {
            out.push(Violation::new(
                format!("{at}.mounts[{i}].path"),
                "overlaps another mount path",
            ));
        }
    }
    // A ramdisk is memory of the container's cgroup.
    let mut ramdisks = BTreeSet::new();
    let mut ramdisk_bytes = 0u128;
    for mount in &container.mounts {
        if let Some(VolumeSource::Ramdisk { size_bytes }) = volumes.get(&mount.volume)
            && ramdisks.insert(mount.volume.as_str())
        {
            ramdisk_bytes += u128::from(*size_bytes);
        }
    }
    if ramdisk_bytes > u128::from(container.resources.memory_bytes) {
        out.push(Violation::new(
            format!("{at}.mounts"),
            format!(
                "ramdisk volumes total {ramdisk_bytes} bytes, more than memory_bytes ({})",
                container.resources.memory_bytes
            ),
        ));
    }

    let at_resources = format!("{at}.resources");
    check_amount(
        &format!("{at_resources}.cpu_millis"),
        container.resources.cpu_millis,
        limits::MAX_CPU_MILLIS,
        out,
    );
    check_amount(
        &format!("{at_resources}.memory_bytes"),
        container.resources.memory_bytes,
        limits::MAX_MEMORY_BYTES,
        out,
    );

    let mut capabilities = BTreeSet::new();
    for (i, capability) in container.capabilities.iter().enumerate() {
        let at = format!("{at}.capabilities[{i}]");
        if capability.eq_ignore_ascii_case("ALL") {
            out.push(Violation::new(
                at,
                "ALL is refused: list the capabilities the container needs",
            ));
        } else if !CAPABILITIES.contains(&capability.as_str()) {
            out.push(Violation::new(
                at,
                "is not a Linux capability: use upper case without the CAP_ prefix",
            ));
        } else if !ALLOWED_CAPABILITIES.contains(&capability.as_str()) {
            // A kernel name, so safe to print.
            out.push(Violation::new(
                at,
                format!(
                    "capability {capability} is not allowed: it gives control of the node. \
                     Allowed: {}",
                    ALLOWED_CAPABILITIES.join(", ")
                ),
            ));
        } else if !capabilities.insert(capability.as_str()) {
            out.push(Violation::new(at, "duplicate capability"));
        }
    }

    if let Some(probe) = &container.probe {
        check_probe(&format!("{at}.probe"), probe, out);
    }
}

/// The entries of a command: their sizes and their bytes, not their count.
fn check_command_entries(at: &str, entries: &[String], out: &mut Vec<Violation>) {
    for (i, entry) in entries.iter().enumerate() {
        if entry.len() > limits::MAX_COMMAND_ENTRY_BYTES {
            out.push(Violation::new(
                format!("{at}[{i}]"),
                format!("must be at most {} bytes", limits::MAX_COMMAND_ENTRY_BYTES),
            ));
        }
        if entry.contains('\0') {
            out.push(Violation::new(
                format!("{at}[{i}]"),
                "must not contain a NUL character",
            ));
        }
    }
}

/// `[A-Za-z_][A-Za-z0-9_.-]*`
fn is_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// The rules of both kinds of path: its size and its characters.
fn check_path_bytes(at: &str, path: &str, out: &mut Vec<Violation>) {
    if path.len() > limits::MAX_PATH_BYTES {
        out.push(Violation::new(
            at,
            format!("must be at most {} bytes", limits::MAX_PATH_BYTES),
        ));
    }
    if path.bytes().any(|b| b.is_ascii_control()) {
        out.push(Violation::new(at, "must not contain control characters"));
    }
    check_ascii(at, path, out);
}

/// A field that names something on a node or in an image holds ASCII only,
/// so no invisible or text-reordering character can hide in it.
fn check_ascii(at: &str, text: &str, out: &mut Vec<Violation>) {
    if !text.is_ascii() {
        out.push(Violation::new(at, "must only contain ASCII characters"));
    }
}

/// An identifier in one piece: ASCII with no whitespace and no control
/// character.
fn check_ascii_word(at: &str, text: &str, out: &mut Vec<Violation>) {
    if text
        .bytes()
        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        out.push(Violation::new(
            at,
            "must not contain whitespace or control characters",
        ));
    }
    check_ascii(at, text, out);
}

/// `/proc`, `/sys` and `/dev` and what is under them, except `/dev/shm`. A
/// volume mounted there would hide or replace what the runtime puts there.
fn is_kernel_directory(path: &str) -> bool {
    let under = |dir: &str| {
        path.strip_prefix(dir)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    };
    (under("/proc") || under("/sys") || under("/dev")) && !under("/dev/shm")
}

/// A path on a node or in a container: absolute and already normalised, so
/// one directory has one spelling.
fn check_absolute_path(at: &str, path: &str, out: &mut Vec<Violation>) {
    if !path.starts_with('/') {
        out.push(Violation::new(at, "must be an absolute path"));
    }
    check_path_bytes(at, path, out);
    if path.split('/').any(|part| part == "." || part == "..") {
        out.push(Violation::new(at, "must not contain . or .. components"));
    }
    if path.contains("//") {
        out.push(Violation::new(at, "must not contain //"));
    }
    if path.len() > 1 && path.ends_with('/') {
        out.push(Violation::new(at, "must not end with /"));
    }
}

/// A decimal `u32` in its one spelling: digits only, no leading zero.
fn is_id(text: &str) -> bool {
    !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'))
        && text.parse::<u32>().is_ok()
}

/// `[a-z_][a-z0-9_-]{0,31}`
fn is_user_name(text: &str) -> bool {
    let mut bytes = text.bytes();
    text.len() <= 32
        && bytes
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

fn check_user(at: &str, user: &str, out: &mut Vec<Violation>) {
    match user.split_once(':') {
        // A group name would need the image's /etc/group to resolve.
        Some((uid, gid)) => {
            if !is_id(uid) || !is_id(gid) {
                out.push(Violation::new(
                    at,
                    "must be uid:gid in decimal: a group name is not supported",
                ));
            }
        }
        None => {
            if !is_id(user) && !is_user_name(user) {
                out.push(Violation::new(at, "must be a uid, uid:gid or a user name"));
            }
        }
    }
}

/// An amount is from 1 to `most`.
fn check_amount(at: &str, amount: u64, most: u64, out: &mut Vec<Violation>) {
    match amount {
        0 => out.push(Violation::new(at, "must be greater than 0")),
        n if n > most => out.push(Violation::new(at, format!("must be at most {most}"))),
        _ => {}
    }
}

/// Longest `host` of an HTTP probe: the length limit of a DNS name.
const MAX_PROBE_HOST_BYTES: usize = 253;

fn check_probe(at: &str, probe: &Probe, out: &mut Vec<Violation>) {
    if probe.interval_secs == 0 {
        out.push(Violation::new(
            format!("{at}.interval_secs"),
            "must be at least 1",
        ));
    }
    if probe.interval_secs > limits::MAX_PROBE_INTERVAL_SECS {
        out.push(Violation::new(
            format!("{at}.interval_secs"),
            format!("must be at most {}", limits::MAX_PROBE_INTERVAL_SECS),
        ));
    }
    if probe.timeout_secs == 0 {
        out.push(Violation::new(
            format!("{at}.timeout_secs"),
            "must be at least 1",
        ));
    } else if probe.timeout_secs >= probe.interval_secs {
        out.push(Violation::new(
            format!("{at}.timeout_secs"),
            "must be less than interval_secs (the default timeout_secs is 1 s, so interval_secs \
             must be at least 2)",
        ));
    }
    let at = format!("{at}.action");
    match &probe.action {
        ProbeAction::Http {
            port,
            path,
            host,
            headers,
            ..
        } => {
            if *port == 0 {
                out.push(Violation::new(format!("{at}.port"), "must not be 0"));
            }
            // Path, host and headers go into the request as they are, so
            // none of them may be able to start a new line of it.
            if !path.starts_with('/') {
                out.push(Violation::new(format!("{at}.path"), "must start with /"));
            }
            if path.len() > limits::MAX_PATH_BYTES {
                out.push(Violation::new(
                    format!("{at}.path"),
                    format!("must be at most {} bytes", limits::MAX_PATH_BYTES),
                ));
            }
            check_ascii_word(&format!("{at}.path"), path, out);
            if let Some(host) = host {
                if host.is_empty() {
                    out.push(Violation::new(format!("{at}.host"), "must not be empty"));
                }
                if host.len() > MAX_PROBE_HOST_BYTES {
                    out.push(Violation::new(
                        format!("{at}.host"),
                        format!("must be at most {MAX_PROBE_HOST_BYTES} bytes"),
                    ));
                }
                check_ascii_word(&format!("{at}.host"), host, out);
                let clean = host.is_ascii()
                    && !host
                        .bytes()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control());
                if clean && !host.is_empty() && !is_host_or_host_port(host) {
                    out.push(Violation::new(
                        format!("{at}.host"),
                        "must be a hostname or host:port",
                    ));
                }
            }
            for (name, value) in headers {
                let at = format!("{at}.headers.{}", segment(name));
                if !is_http_token(name) {
                    out.push(Violation::new(at.as_str(), "name must be an HTTP token"));
                } else if RESERVED_PROBE_HEADERS
                    .iter()
                    .any(|reserved| name.eq_ignore_ascii_case(reserved))
                {
                    out.push(Violation::new(
                        at.as_str(),
                        "name is reserved: the runner sets it",
                    ));
                }
                if value.contains(['\r', '\n', '\0']) {
                    out.push(Violation::new(at, "value must not contain CR, LF or NUL"));
                }
            }
        }
        ProbeAction::Tcp { port } => {
            if *port == 0 {
                out.push(Violation::new(format!("{at}.port"), "must not be 0"));
            }
        }
    }
}

/// Headers the runner sets itself; a probe cannot override them. The runner
/// skips them too, for a spec stored before this rule was checked.
pub const RESERVED_PROBE_HEADERS: [&str; 4] =
    ["Host", "Content-Length", "Transfer-Encoding", "Connection"];

/// `name`, `name:port`, `[v6]` or `[v6]:port`. A name is dot-separated DNS
/// labels (letters, digits, hyphens; no hyphen at either end); a port is
/// 1 to 65535.
fn is_host_or_host_port(host: &str) -> bool {
    let is_port = |p: &str| {
        !p.is_empty()
            && p.bytes().all(|b| b.is_ascii_digit())
            && p.parse::<u32>().is_ok_and(|n| (1..=65535).contains(&n))
    };
    if let Some(rest) = host.strip_prefix('[') {
        let Some((addr, tail)) = rest.split_once(']') else {
            return false;
        };
        let addr_ok = !addr.is_empty()
            && addr.contains(':')
            && addr
                .bytes()
                .all(|b| b.is_ascii_hexdigit() || matches!(b, b':' | b'.'));
        return addr_ok && (tail.is_empty() || tail.strip_prefix(':').is_some_and(is_port));
    }
    let (name, port) = match host.split_once(':') {
        Some((name, port)) => (name, Some(port)),
        None => (host, None),
    };
    port.is_none_or(is_port)
        && !name.is_empty()
        && name.split('.').all(|label| {
            (1..=63).contains(&label.len())
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

/// A `token` of RFC 9110: what the name of a header may be.
fn is_http_token(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

/// How many pods a workload runs, and where.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Scale {
    /// This many pods: `{"replicas": {"count": 3, "strategy": "spread"}}`.
    /// `count` is 0 to 1024.
    Replicas {
        count: u32,
        #[serde(default)]
        strategy: Strategy,
    },
    /// One pod on every node: `"daemon"`. With `placement.nodes`, one pod on
    /// each listed node.
    Daemon,
}

impl Default for Scale {
    /// One replica, spread.
    fn default() -> Self {
        Scale::Replicas {
            count: 1,
            strategy: Strategy::Spread,
        }
    }
}

/// How the replicas of a workload are laid out over the nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    /// As few replicas per node as capacity allows.
    #[default]
    Spread,
    /// As many replicas per node as capacity allows.
    Pack,
}

/// When the runner restarts a container that has exited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    #[default]
    Always,
    /// Only after a non-zero exit.
    OnFailure,
    Never,
}

/// Who may resolve and reach a workload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exposure {
    /// The workloads of its own project and environment.
    #[default]
    Environment,
    /// Every workload in the cluster.
    Cluster,
}

/// Where the pods of a workload may run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    /// Names of the nodes that may run the workload: unique DNS labels, at
    /// most 64. Any node when empty. With [`Scale::Daemon`] the workload runs
    /// once on each listed node.
    #[serde(default)]
    pub nodes: Vec<String>,
}

/// A volume of the pod. Both kinds have a fixed size from 1 MiB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum VolumeSource {
    /// A fixed-size filesystem image on the node, up to 1 TiB. It is local to
    /// the node and outlives the pod.
    Block { size_bytes: u64 },
    /// Memory (tmpfs), up to 64 GiB. It is gone when the pod stops.
    Ramdisk { size_bytes: u64 },
}

/// One container of a workload.
///
/// `env` holds literal values, which may be secrets, so `Debug` prints the
/// names only.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerSpec {
    /// Unique within the workload: a DNS label.
    pub name: String,
    pub image: String,
    /// Replaces the image's entrypoint. `command` and `args` have their CRI
    /// meaning: the image's own entrypoint and arguments run only when both
    /// are empty.
    #[serde(default)]
    pub command: Vec<String>,
    /// Replaces the image's arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables as literal values.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    /// `uid`, `uid:gid` or a user name of the image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Ports the container listens on. Metadata only: nothing is opened or
    /// closed by declaring one.
    #[serde(default)]
    pub ports: Vec<PortSpec>,
    #[serde(default)]
    pub mounts: Vec<MountSpec>,
    #[serde(default)]
    pub resources: Resources,
    /// Capabilities added to an empty set, without the `CAP_` prefix:
    /// `NET_BIND_SERVICE`. `ALL` is refused.
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub read_only_root_fs: bool,
    /// One failure takes the replica out of DNS;
    /// [`limits::PROBE_FAILURES_BEFORE_RESTART`] in a row restart the
    /// container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<Probe>,
}

impl ContainerSpec {
    /// A container that runs `image` as the image defines it, with every
    /// other field at its default.
    pub fn new(name: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            image: image.into(),
            command: Vec::new(),
            args: Vec::new(),
            env: BTreeMap::new(),
            working_dir: None,
            user: None,
            ports: Vec::new(),
            mounts: Vec::new(),
            resources: Resources::default(),
            capabilities: Vec::new(),
            read_only_root_fs: false,
            probe: None,
        }
    }
}

impl fmt::Debug for ContainerSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Names only: the values may be secrets.
        let env_names: Vec<&String> = self.env.keys().collect();
        f.debug_struct("ContainerSpec")
            .field("name", &self.name)
            .field("image", &self.image)
            .field("command", &self.command)
            .field("args", &self.args)
            .field("env", &env_names)
            .field("working_dir", &self.working_dir)
            .field("user", &self.user)
            .field("ports", &self.ports)
            .field("mounts", &self.mounts)
            .field("resources", &self.resources)
            .field("capabilities", &self.capabilities)
            .field("read_only_root_fs", &self.read_only_root_fs)
            .field("probe", &self.probe)
            .finish()
    }
}

/// A port a container listens on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub port: u16,
    #[serde(default)]
    pub protocol: Protocol,
}

/// Transport protocol of a [`PortSpec`], serialised in lowercase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    #[default]
    Tcp,
    Udp,
}

/// A volume of the pod mounted into one container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountSpec {
    /// A key of [`WorkloadSpec::volumes`].
    pub volume: String,
    /// Absolute path inside the container.
    pub path: String,
    #[serde(default)]
    pub read_only: bool,
}

/// The most a container may use. They are maximums: the kernel limit of the
/// container, and what placement reserves on the node for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    /// Thousandths of a CPU: 500 is half a core. Default 1000.
    #[serde(default = "default_cpu_millis")]
    pub cpu_millis: u64,
    /// Default 1 GiB.
    #[serde(default = "default_memory_bytes")]
    pub memory_bytes: u64,
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            cpu_millis: default_cpu_millis(),
            memory_bytes: default_memory_bytes(),
        }
    }
}

fn default_cpu_millis() -> u64 {
    1000
}

fn default_memory_bytes() -> u64 {
    1024 * 1024 * 1024
}

/// A check against a container. One failure takes the replica out of DNS;
/// [`limits::PROBE_FAILURES_BEFORE_RESTART`] in a row restart the container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probe {
    pub action: ProbeAction,
    /// Seconds between checks, 1 to 3600. Also the delay before the first
    /// check. Default 5.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u32,
    /// Seconds after which a check counts as failed: at least 1 and less than
    /// `interval_secs`. Default 1.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u32,
}

impl Probe {
    /// A probe that runs `action` with the default timings.
    pub fn new(action: ProbeAction) -> Self {
        Self {
            action,
            interval_secs: default_interval_secs(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

fn default_interval_secs() -> u32 {
    5
}

fn default_timeout_secs() -> u32 {
    1
}

/// What a [`Probe`] does.
///
/// A header of an HTTP probe may carry a credential, so `Debug` prints the
/// header names only.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProbeAction {
    /// Sends a GET to the pod. A status from 200 to 399 passes.
    Http {
        port: u16,
        #[serde(default = "default_http_path")]
        path: String,
        #[serde(default)]
        scheme: HttpScheme,
        /// The `Host` header. The pod's address when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// Opens a TCP connection to the pod. Connecting passes.
    Tcp { port: u16 },
}

impl fmt::Debug for ProbeAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProbeAction::Http {
                port,
                path,
                scheme,
                host,
                headers,
            } => {
                // Names only: the values may be secrets.
                let header_names: Vec<&String> = headers.keys().collect();
                f.debug_struct("Http")
                    .field("port", port)
                    .field("path", path)
                    .field("scheme", scheme)
                    .field("host", host)
                    .field("headers", &header_names)
                    .finish()
            }
            ProbeAction::Tcp { port } => f.debug_struct("Tcp").field("port", port).finish(),
        }
    }
}

fn default_http_path() -> String {
    "/".to_string()
}

/// Scheme of an HTTP probe, serialised in lowercase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpScheme {
    #[default]
    Http,
    Https,
}

/// `POST /api/v1/projects` (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateProjectRequest {
    /// A DNS label.
    pub name: String,
    /// The environments the project starts with, each a DNS label.
    /// [`DEFAULT_ENVIRONMENTS`] when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environments: Option<Vec<String>>,
}

/// 201 body of `POST /api/v1/projects`, `GET /api/v1/projects/{project}`,
/// and the 200 body of `PUT /api/v1/projects/{project}/environments/{environment}`:
/// the project as it is after the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResponse {
    pub name: String,
    /// Unix seconds, by the leader's clock.
    pub created_at: u64,
    /// Sorted by name.
    pub environments: Vec<EnvironmentSummary>,
}

/// One entry in [`ProjectResponse::environments`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentSummary {
    pub name: String,
    /// Moves every time the environment changes and never repeats.
    pub generation: u64,
    /// How many workloads the environment holds.
    pub workloads: u32,
    /// Size of the stored environment as JSON.
    pub bytes: u64,
    /// Unix seconds of the last change, by the leader's clock.
    pub updated_at: u64,
}

/// `GET /api/v1/projects` (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectListResponse {
    /// Sorted by name.
    pub projects: Vec<ProjectResponse>,
}

/// `POST /api/v1/projects/{project}/environments/{environment}/deploy`
/// (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployRequest {
    /// The workloads to create or update, by name. Each name is a DNS label.
    pub workloads: BTreeMap<String, WorkloadSpec>,
    /// Also remove every workload of the environment that `workloads` does
    /// not name.
    #[serde(default)]
    pub prune: bool,
}

/// 200 body of `POST /api/v1/projects/{project}/environments/{environment}/deploy`.
/// Every workload of the request is in exactly one of `created`, `updated`
/// and `unchanged`. Each list is sorted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployResponse {
    /// The environment's generation after the deploy. A deploy that changed
    /// nothing leaves it as it was.
    pub generation: u64,
    pub created: Vec<String>,
    pub updated: Vec<String>,
    pub unchanged: Vec<String>,
    /// Workloads removed by `prune`.
    pub removed: Vec<String>,
}

/// Query of `GET /api/v1/workloads`. Both filters are optional;
/// `environment` without `project` matches that environment in every
/// project.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
}

/// `GET /api/v1/workloads` (loopback listener only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadListResponse {
    /// Sorted by project, environment and name.
    pub workloads: Vec<WorkloadSummary>,
}

/// One entry in [`WorkloadListResponse::workloads`]. It carries no part of
/// the spec that can hold a secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSummary {
    pub project: String,
    pub environment: String,
    pub name: String,
    /// `<name>.<environment>.<project>.faber.local`.
    pub fqdn: String,
    pub scale: Scale,
    pub expose: Exposure,
    /// Moves when the pod template changes: everything in the spec except
    /// `scale`, `placement` and `expose`.
    pub revision: u64,
    /// Replicas whose state is `available`.
    pub available: u32,
    /// Replicas wanted: the replica count, or for a daemon the replicas the
    /// leader has placed. A replica without a node counts here and not in
    /// `available`.
    pub desired: u32,
}

/// `GET /api/v1/projects/{project}/environments/{environment}/workloads/{workload}`
/// (loopback listener only). `spec` carries env values in clear.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadResponse {
    pub project: String,
    pub environment: String,
    pub name: String,
    /// `<name>.<environment>.<project>.faber.local`.
    pub fqdn: String,
    /// See [`WorkloadSummary::revision`].
    pub revision: u64,
    /// The generation of the environment the workload was read from.
    pub generation: u64,
    pub spec: WorkloadSpec,
    /// The pods of each replica the leader has placed, with what its node last
    /// reported, sorted by replica index and then newest revision first. Each
    /// row carries the revision that pod runs, which can be older than
    /// `revision` during a roll. A replica with no report at `revision` also
    /// has a `deploying` row at `revision`. Empty until the leader has placed
    /// the workload. A replica whose node has not reported for three report
    /// intervals reads `unavailable`.
    #[serde(default)]
    pub replicas: Vec<ReplicaStatus>,
}

/// State of one replica as its node reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaState {
    Deploying,
    Available,
    Unavailable,
}

/// One replica in [`WorkloadResponse::replicas`]: where the leader placed it
/// and what that node reported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaStatus {
    /// The node the replica is placed on. `null` while it is unplaced; then
    /// `reason` says why and `state` is `unavailable`.
    pub node: Option<u64>,
    pub replica_index: u32,
    /// The revision the replica runs.
    pub revision: u64,
    /// `deploying` until the node reports the replica.
    pub state: ReplicaState,
    /// The replica's pod address, once it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<Ipv4Addr>,
    /// Why the replica is unplaced: `insufficient_capacity`, `volume_lost`,
    /// `pinned_node_unknown` or `no_node_available`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn default_environments_are_dns_labels() {
        for name in DEFAULT_ENVIRONMENTS {
            assert_eq!(crate::name::validate_dns_label(name), Ok(()));
        }
    }

    #[test]
    fn violation_displays_path_and_message() {
        assert_eq!(
            Violation::new("containers[0].image", "must not be empty").to_string(),
            "containers[0].image: must not be empty"
        );
        assert_eq!(Violation::new("", "too large").to_string(), "too large");
    }

    /// The smallest spec a client may send, and what is stored for it:
    /// every value default written out, every absent value left out.
    #[test]
    fn a_minimal_spec_decodes_and_writes_its_defaults() {
        let spec: WorkloadSpec =
            serde_json::from_value(json!({"containers": [{"name": "web", "image": "nginx"}]}))
                .unwrap();
        assert_eq!(spec.scale, Scale::default());
        assert_eq!(
            spec.scale,
            Scale::Replicas {
                count: 1,
                strategy: Strategy::Spread
            }
        );
        assert_eq!(spec.restart, RestartPolicy::Always);
        assert_eq!(spec.expose, Exposure::Environment);
        assert_eq!(spec.containers, vec![ContainerSpec::new("web", "nginx")]);
        assert_eq!(
            serde_json::to_value(&spec).unwrap(),
            json!({
                "scale": {"replicas": {"count": 1, "strategy": "spread"}},
                "restart": "always",
                "expose": "environment",
                "placement": {"nodes": []},
                "volumes": {},
                "containers": [{
                    "name": "web",
                    "image": "nginx",
                    "command": [],
                    "args": [],
                    "env": {},
                    "ports": [],
                    "mounts": [],
                    "resources": {"cpu_millis": 1000, "memory_bytes": 1_073_741_824},
                    "capabilities": [],
                    "read_only_root_fs": false,
                }],
            })
        );
    }

    #[test]
    fn a_full_spec_has_a_pinned_shape() {
        let mut web = ContainerSpec::new("web", "nginx:1.27");
        web.command = vec!["nginx".into()];
        web.args = vec!["-g".into(), "daemon off;".into()];
        web.env = BTreeMap::from([("MODE".to_string(), "prod".to_string())]);
        web.working_dir = Some("/srv".into());
        web.user = Some("101:101".into());
        web.ports = vec![PortSpec {
            name: Some("http".into()),
            port: 8080,
            protocol: Protocol::Tcp,
        }];
        web.mounts = vec![MountSpec {
            volume: "data".into(),
            path: "/data".into(),
            read_only: true,
        }];
        web.resources = Resources {
            cpu_millis: 500,
            memory_bytes: 268_435_456,
        };
        web.capabilities = vec!["NET_BIND_SERVICE".into()];
        web.read_only_root_fs = true;
        web.probe = Some(Probe {
            interval_secs: 10,
            timeout_secs: 2,
            ..Probe::new(ProbeAction::Http {
                port: 8080,
                path: "/ready".into(),
                scheme: HttpScheme::Https,
                host: Some("web.local".into()),
                headers: BTreeMap::from([("x-probe".to_string(), "1".to_string())]),
            })
        });
        let spec = WorkloadSpec {
            scale: Scale::Replicas {
                count: 3,
                strategy: Strategy::Pack,
            },
            restart: RestartPolicy::OnFailure,
            expose: Exposure::Cluster,
            placement: Placement {
                nodes: vec!["node-1".into(), "node-2".into()],
            },
            stop_grace_secs: Some(30),
            volumes: BTreeMap::from([
                (
                    "data".to_string(),
                    VolumeSource::Block {
                        size_bytes: 1_073_741_824,
                    },
                ),
                (
                    "tmp".to_string(),
                    VolumeSource::Ramdisk {
                        size_bytes: 1_048_576,
                    },
                ),
            ]),
            containers: vec![web],
        };
        let want = json!({
            "scale": {"replicas": {"count": 3, "strategy": "pack"}},
            "restart": "on_failure",
            "expose": "cluster",
            "placement": {"nodes": ["node-1", "node-2"]},
            "stop_grace_secs": 30,
            "volumes": {
                "data": {"type": "block", "size_bytes": 1_073_741_824},
                "tmp": {"type": "ramdisk", "size_bytes": 1_048_576},
            },
            "containers": [{
                "name": "web",
                "image": "nginx:1.27",
                "command": ["nginx"],
                "args": ["-g", "daemon off;"],
                "env": {"MODE": "prod"},
                "working_dir": "/srv",
                "user": "101:101",
                "ports": [{"name": "http", "port": 8080, "protocol": "tcp"}],
                "mounts": [{
                    "volume": "data", "path": "/data", "read_only": true,
                }],
                "resources": {"cpu_millis": 500, "memory_bytes": 268_435_456},
                "capabilities": ["NET_BIND_SERVICE"],
                "read_only_root_fs": true,
                "capabilities": ["NET_BIND_SERVICE"],
                "read_only_root_fs": true,
                "probe": {
                    "action": {
                        "type": "http", "port": 8080, "path": "/ready", "scheme": "https",
                        "host": "web.local", "headers": {"x-probe": "1"},
                    },
                    "interval_secs": 10, "timeout_secs": 2,
                },
            }],
        });
        assert_eq!(serde_json::to_value(&spec).unwrap(), want);
        assert_eq!(serde_json::from_value::<WorkloadSpec>(want).unwrap(), spec);
    }

    /// A spec with one container `web`, whose JSON gets `extra` merged in.
    fn container_with(extra: serde_json::Value) -> serde_json::Value {
        let mut c = json!({"name": "web", "image": "nginx"});
        c.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!({ "containers": [c] })
    }

    #[test]
    fn unknown_fields_are_refused_at_every_level() {
        let container = json!({"name": "web", "image": "nginx"});
        for bad in [
            json!({"containers": [container.clone()], "replicas": 2}),
            json!({"containers": [{"name": "web", "image": "nginx", "privileged": true}]}),
            json!({"containers": [container.clone()], "placement": {"zone": "a"}}),
            json!({"containers": [container.clone()],
                   "volumes": {"v": {"type": "ramdisk", "size_bytes": 1048576, "medium": "x"}}}),
            json!({"containers": [container.clone()],
                   "volumes": {"v": {"type": "nfs", "server": "x"}}}),
            container_with(json!({"probe": {"action": {"type": "tcp", "port": 1}, "grpc": 1}})),
            container_with(json!({"probe": {"action": {"type": "tcp", "port": 1, "host": "x"}}})),
            container_with(json!({"resources": {"gpu": 1}})),
            container_with(
                json!({"mounts": [{"volume": "data", "path": "/data", "sub_path": "web"}]}),
            ),
            json!({"containers": [container.clone()],
                   "scale": {"replicas": {"count": 1, "zone": "a"}}}),
        ] {
            assert!(
                serde_json::from_value::<WorkloadSpec>(bad.clone()).is_err(),
                "{bad}"
            );
        }
    }

    /// Shapes of earlier versions are refused, not migrated.
    #[test]
    fn old_shapes_are_refused() {
        let web = json!([{"name": "web", "image": "nginx"}]);
        let tcp = json!({"action": {"type": "tcp", "port": 1}});
        for (case, old) in [
            (
                "replicas as a number",
                json!({"scale": {"replicas": 3}, "containers": web}),
            ),
            ("per_node", json!({"scale": "per_node", "containers": web})),
            (
                "placement.node",
                json!({"placement": {"node": "n1"}, "containers": web}),
            ),
            (
                "host_path",
                json!({"volumes": {"v": {"type": "host_path", "path": "/srv"}}, "containers": web}),
            ),
            (
                "named",
                json!({"volumes": {"v": {"type": "named", "name": "v"}}, "containers": web}),
            ),
            (
                "ephemeral",
                json!({"volumes": {"v": {"type": "ephemeral"}}, "containers": web}),
            ),
            (
                "block without a size",
                json!({"volumes": {"v": {"type": "block"}}, "containers": web}),
            ),
            ("liveness", container_with(json!({"liveness": tcp.clone()}))),
            (
                "readiness",
                container_with(json!({"readiness": tcp.clone()})),
            ),
            ("startup", container_with(json!({"startup": tcp.clone()}))),
            (
                "exec probe",
                container_with(json!({"probe":
                    {"action": {"type": "exec", "command": ["true"]}}})),
            ),
            (
                "period_secs",
                container_with(json!({"probe":
                    {"action": {"type": "tcp", "port": 1}, "period_secs": 5}})),
            ),
            (
                "failure_threshold",
                container_with(json!({"probe":
                    {"action": {"type": "tcp", "port": 1}, "failure_threshold": 3}})),
            ),
            (
                "initial_delay_secs",
                container_with(json!({"probe":
                    {"action": {"type": "tcp", "port": 1}, "initial_delay_secs": 3}})),
            ),
            (
                "limits",
                container_with(json!({"resources": {"limits": {"cpu_millis": 1}}})),
            ),
            (
                "requests",
                container_with(json!({"resources": {"requests": {}}})),
            ),
        ] {
            assert!(
                serde_json::from_value::<WorkloadSpec>(old.clone()).is_err(),
                "{case}: {old}"
            );
        }
    }

    #[test]
    fn container_debug_prints_env_names_only() {
        let mut c = ContainerSpec::new("web", "nginx");
        c.env = BTreeMap::from([
            ("API_KEY".to_string(), "hunter2".to_string()),
            ("MODE".to_string(), "prod-secret".to_string()),
        ]);
        let spec = WorkloadSpec {
            scale: Scale::default(),
            restart: RestartPolicy::default(),
            expose: Exposure::default(),
            placement: Placement::default(),
            stop_grace_secs: None,
            volumes: BTreeMap::new(),
            containers: vec![c],
        };
        // Through every type that holds a container.
        let request = DeployRequest {
            workloads: BTreeMap::from([("web".to_string(), spec)]),
            prune: false,
        };
        let debug = format!("{request:?}");
        assert!(
            debug.contains("API_KEY") && debug.contains("MODE"),
            "{debug}"
        );
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(!debug.contains("prod-secret"), "{debug}");
    }

    #[test]
    fn limits_are_the_documented_ones() {
        assert_eq!(limits::MAX_PROJECTS, 64);
        assert_eq!(limits::MAX_ENVIRONMENTS_PER_PROJECT, 16);
        assert_eq!(limits::MAX_WORKLOADS_PER_ENVIRONMENT, 64);
        assert_eq!(limits::MAX_CONTAINERS_PER_WORKLOAD, 16);
        assert_eq!(limits::MAX_REPLICAS, 1024);
        assert_eq!(limits::MAX_ENV_VARS, 256);
        assert_eq!(limits::MAX_ENV_VALUE_BYTES, 32_768);
        assert_eq!(limits::MAX_DEPLOY_REQUEST_BYTES, 131_072);
        assert_eq!(limits::MAX_ENVIRONMENT_RECORD_BYTES, 524_288);
        assert_eq!(limits::MAX_TOTAL_BYTES, 16_777_216);
        assert_eq!(limits::MAX_HTTP_BODY_BYTES, 1_048_576);
        assert_eq!(limits::MAX_VOLUMES_PER_WORKLOAD, 32);
        assert_eq!(limits::MAX_MOUNTS_PER_CONTAINER, 32);
        assert_eq!(limits::MAX_PORTS_PER_CONTAINER, 64);
        assert_eq!(limits::MAX_IMAGE_BYTES, 512);
        assert_eq!(limits::MAX_ENV_NAME_BYTES, 256);
        assert_eq!(limits::MAX_COMMAND_ENTRIES, 256);
        assert_eq!(limits::MAX_COMMAND_ENTRY_BYTES, 8_192);
        assert_eq!(limits::MAX_PATH_BYTES, 4_096);
        assert_eq!(limits::MAX_STOP_GRACE_SECS, 3_600);
        assert_eq!(limits::MAX_CPU_MILLIS, 256_000);
        assert_eq!(limits::MAX_MEMORY_BYTES, 4_398_046_511_104);
        assert_eq!(limits::MIN_VOLUME_BYTES, 1_048_576);
        assert_eq!(limits::MAX_BLOCK_VOLUME_BYTES, 1 << 40);
        assert_eq!(limits::MAX_RAMDISK_VOLUME_BYTES, 64 << 30);
        assert_eq!(limits::MAX_PLACEMENT_NODES, 64);
        assert_eq!(limits::MAX_PROBE_INTERVAL_SECS, 3_600);
        assert_eq!(limits::PROBE_FAILURES_BEFORE_RESTART, 3);
    }

    const LABEL_LENGTH: &str = "name must be between 1 and 63 characters";
    const LABEL_CHARS: &str = "name may only contain a-z, 0-9 and hyphens";
    const LABEL_HYPHEN: &str = "name must not start or end with a hyphen";
    const OVERLAP: &str = "overlaps another mount path";
    const MOUNT_RESERVED: &str = "must not be /proc, /sys or /dev or under them, except /dev/shm";

    /// One container `web` running `nginx`, everything else at its default.
    fn base() -> WorkloadSpec {
        WorkloadSpec {
            scale: Scale::default(),
            restart: RestartPolicy::default(),
            expose: Exposure::default(),
            placement: Placement::default(),
            stop_grace_secs: None,
            volumes: BTreeMap::new(),
            containers: vec![ContainerSpec::new("web", "nginx")],
        }
    }

    fn ramdisk() -> VolumeSource {
        VolumeSource::Ramdisk {
            size_bytes: limits::MIN_VOLUME_BYTES,
        }
    }

    fn block(size_bytes: u64) -> VolumeSource {
        VolumeSource::Block { size_bytes }
    }

    fn mount(volume: &str, path: &str) -> MountSpec {
        MountSpec {
            volume: volume.to_string(),
            path: path.to_string(),
            read_only: false,
        }
    }

    fn port(port: u16, protocol: Protocol) -> PortSpec {
        PortSpec {
            name: None,
            port,
            protocol,
        }
    }

    fn http(path: &str) -> ProbeAction {
        ProbeAction::Http {
            port: 80,
            path: path.to_string(),
            scheme: HttpScheme::Http,
            host: None,
            headers: BTreeMap::new(),
        }
    }

    fn http_host(host: &str) -> ProbeAction {
        ProbeAction::Http {
            port: 80,
            path: "/".to_string(),
            scheme: HttpScheme::Http,
            host: Some(host.to_string()),
            headers: BTreeMap::new(),
        }
    }

    fn http_header(name: &str, value: &str) -> ProbeAction {
        ProbeAction::Http {
            port: 80,
            path: "/".to_string(),
            scheme: HttpScheme::Http,
            host: None,
            headers: BTreeMap::from([(name.to_string(), value.to_string())]),
        }
    }

    /// Three labels of 63 characters and one of `last`: 253 bytes for 61.
    fn long_host(last: usize) -> String {
        let label = "a".repeat(63);
        format!("{label}.{label}.{label}.{}", "a".repeat(last))
    }

    fn replicas(count: u32, strategy: Strategy) -> Scale {
        Scale::Replicas { count, strategy }
    }

    fn violations(spec: &WorkloadSpec) -> Vec<(String, String)> {
        match spec.validate() {
            Ok(()) => Vec::new(),
            Err(found) => found.into_iter().map(|v| (v.path, v.message)).collect(),
        }
    }

    /// One row per rule and side of its boundary: what to change in
    /// [`base`], and every violation that must come back. No violation
    /// means the spec is valid.
    type Row = (
        &'static str,
        fn(&mut WorkloadSpec),
        &'static [(&'static str, &'static str)],
    );

    fn check_rows(rows: &[Row]) {
        for (case, change, want) in rows {
            let mut spec = base();
            change(&mut spec);
            let before = spec.clone();
            let got = violations(&spec);
            let want: Vec<(String, String)> = want
                .iter()
                .map(|(path, message)| (path.to_string(), message.to_string()))
                .collect();
            assert_eq!(got, want, "{case}");
            assert_eq!(spec, before, "{case}: validation changed the spec");
        }
    }

    #[test]
    fn the_base_spec_and_the_full_spec_are_valid() {
        assert_eq!(base().validate(), Ok(()));
        let full: WorkloadSpec = serde_json::from_value(json!({
            "scale": {"replicas": {"count": 3, "strategy": "pack"}},
            "placement": {"nodes": ["node-1", "node-2"]},
            "stop_grace_secs": 30,
            "volumes": {
                "data": {"type": "block", "size_bytes": 1048576},
                "tmp": {"type": "ramdisk", "size_bytes": 1048576},
            },
            "containers": [{
                "name": "web",
                "image": "registry.example:5000/team/nginx:1.27@sha256:abc",
                "command": ["nginx"],
                "args": ["-g", "daemon off;"],
                "env": {"MODE": "prod", "_a.b-c": ""},
                "working_dir": "/srv",
                "user": "101:101",
                "ports": [{"name": "http", "port": 8080}, {"port": 8080, "protocol": "udp"}],
                "mounts": [
                    {"volume": "data", "path": "/data"},
                    {"volume": "tmp", "path": "/tmp", "read_only": true},
                ],
                "resources": {"cpu_millis": 500, "memory_bytes": 2097152},
                "capabilities": ["NET_BIND_SERVICE", "CHOWN"],
                "probe": {"action": {
                    "type": "http", "port": 8080, "path": "/ready?x=1",
                    "host": "web.local", "headers": {"X-Probe": "a b"},
                }, "interval_secs": 3, "timeout_secs": 2},
            }, {
                "name": "sidecar",
                "image": "envoy",
            }],
        }))
        .unwrap();
        assert_eq!(full.validate(), Ok(()));
    }

    #[test]
    fn workload_level_rules() {
        check_rows(&[
            (
                "zero replicas",
                |s| s.scale = replicas(0, Strategy::Spread),
                &[],
            ),
            (
                "most replicas, packed",
                |s| s.scale = replicas(limits::MAX_REPLICAS, Strategy::Pack),
                &[],
            ),
            (
                "too many replicas",
                |s| s.scale = replicas(limits::MAX_REPLICAS + 1, Strategy::Spread),
                &[("scale.replicas.count", "must be at most 1024")],
            ),
            ("daemon", |s| s.scale = Scale::Daemon, &[]),
            (
                "daemon on listed nodes",
                |s| {
                    s.scale = Scale::Daemon;
                    s.placement.nodes = vec!["node-1".into(), "node-2".into()];
                },
                &[],
            ),
            (
                "replicas on listed nodes",
                |s| s.placement.nodes = vec!["node-1".into()],
                &[],
            ),
            (
                "node that is no label",
                |s| s.placement.nodes = vec!["node-1".into(), "Node_1".into()],
                &[("placement.nodes[1]", LABEL_CHARS)],
            ),
            (
                "empty node name",
                |s| s.placement.nodes = vec![String::new()],
                &[("placement.nodes[0]", LABEL_LENGTH)],
            ),
            (
                "node listed twice",
                |s| {
                    s.placement.nodes = vec!["a".into(), "b".into(), "a".into()];
                },
                &[("placement.nodes[2]", "duplicate node")],
            ),
            (
                "most nodes",
                |s| {
                    s.placement.nodes = (0..limits::MAX_PLACEMENT_NODES)
                        .map(|i| format!("n{i}"))
                        .collect();
                },
                &[],
            ),
            (
                "too many nodes",
                |s| {
                    s.placement.nodes = (0..=limits::MAX_PLACEMENT_NODES)
                        .map(|i| format!("n{i}"))
                        .collect();
                },
                &[("placement.nodes", "must hold at most 64 nodes")],
            ),
            (
                "longest stop grace",
                |s| s.stop_grace_secs = Some(3_600),
                &[],
            ),
            ("no stop grace", |s| s.stop_grace_secs = Some(0), &[]),
            (
                "stop grace too long",
                |s| s.stop_grace_secs = Some(3_601),
                &[("stop_grace_secs", "must be at most 3600")],
            ),
            (
                "no container",
                |s| s.containers.clear(),
                &[("containers", "must hold at least one container")],
            ),
            (
                "most containers",
                |s| {
                    s.containers = (0..limits::MAX_CONTAINERS_PER_WORKLOAD)
                        .map(|i| ContainerSpec::new(format!("c{i}"), "nginx"))
                        .collect();
                },
                &[],
            ),
            (
                "too many containers",
                |s| {
                    s.containers = (0..=limits::MAX_CONTAINERS_PER_WORKLOAD)
                        .map(|i| ContainerSpec::new(format!("c{i}"), "nginx"))
                        .collect();
                },
                &[("containers", "must hold at most 16 containers")],
            ),
            (
                "two containers with one name",
                |s| {
                    s.containers.push(ContainerSpec::new("db", "postgres"));
                    s.containers.push(ContainerSpec::new("web", "caddy"));
                },
                &[("containers[2].name", "duplicate container name")],
            ),
            (
                "container name that is no label",
                |s| s.containers[0].name = "db_backup".into(),
                &[("containers[0].name", LABEL_CHARS)],
            ),
            (
                "container name with a leading hyphen",
                |s| s.containers[0].name = "-web".into(),
                &[("containers[0].name", LABEL_HYPHEN)],
            ),
            (
                "longest container name",
                |s| s.containers[0].name = "a".repeat(63),
                &[],
            ),
            (
                "container name too long",
                |s| s.containers[0].name = "a".repeat(64),
                &[("containers[0].name", LABEL_LENGTH)],
            ),
        ]);
    }

    #[test]
    fn volume_rules() {
        const MIN: u64 = limits::MIN_VOLUME_BYTES;
        check_rows(&[
            (
                "most volumes",
                |s| {
                    s.volumes = (0..limits::MAX_VOLUMES_PER_WORKLOAD)
                        .map(|i| (format!("v{i}"), ramdisk()))
                        .collect();
                },
                &[],
            ),
            (
                "too many volumes",
                |s| {
                    s.volumes = (0..=limits::MAX_VOLUMES_PER_WORKLOAD)
                        .map(|i| (format!("v{i}"), ramdisk()))
                        .collect();
                },
                &[("volumes", "must hold at most 32 volumes")],
            ),
            (
                "volume name that is no label",
                |s| {
                    s.volumes.insert("My.Data".into(), ramdisk());
                },
                &[("volumes.My.Data", LABEL_CHARS)],
            ),
            (
                "empty volume name",
                |s| {
                    s.volumes.insert(String::new(), ramdisk());
                },
                &[("volumes.", LABEL_LENGTH)],
            ),
            (
                "smallest block volume",
                |s| {
                    s.volumes.insert("data".into(), block(MIN));
                },
                &[],
            ),
            (
                "largest block volume",
                |s| {
                    s.volumes
                        .insert("data".into(), block(limits::MAX_BLOCK_VOLUME_BYTES));
                },
                &[],
            ),
            (
                "block volume below 1 MiB",
                |s| {
                    s.volumes.insert("data".into(), block(MIN - 1));
                },
                &[("volumes.data.size_bytes", "must be at least 1048576")],
            ),
            (
                "block volume of size zero",
                |s| {
                    s.volumes.insert("data".into(), block(0));
                },
                &[("volumes.data.size_bytes", "must be at least 1048576")],
            ),
            (
                "block volume above 1 TiB",
                |s| {
                    s.volumes
                        .insert("data".into(), block(limits::MAX_BLOCK_VOLUME_BYTES + 1));
                },
                &[(
                    "volumes.data.size_bytes",
                    "must be at most 1099511627776 for a block volume",
                )],
            ),
            (
                "smallest ramdisk volume",
                |s| {
                    s.volumes
                        .insert("tmp".into(), VolumeSource::Ramdisk { size_bytes: MIN });
                },
                &[],
            ),
            (
                "largest ramdisk volume",
                |s| {
                    s.volumes.insert(
                        "tmp".into(),
                        VolumeSource::Ramdisk {
                            size_bytes: limits::MAX_RAMDISK_VOLUME_BYTES,
                        },
                    );
                },
                &[],
            ),
            (
                "ramdisk volume below 1 MiB",
                |s| {
                    s.volumes.insert(
                        "tmp".into(),
                        VolumeSource::Ramdisk {
                            size_bytes: MIN - 1,
                        },
                    );
                },
                &[("volumes.tmp.size_bytes", "must be at least 1048576")],
            ),
            (
                "ramdisk volume above 64 GiB",
                |s| {
                    s.volumes.insert(
                        "tmp".into(),
                        VolumeSource::Ramdisk {
                            size_bytes: limits::MAX_RAMDISK_VOLUME_BYTES + 1,
                        },
                    );
                },
                &[(
                    "volumes.tmp.size_bytes",
                    "must be at most 68719476736 for a ramdisk volume",
                )],
            ),
            (
                "mount of a volume that is not declared",
                |s| s.containers[0].mounts = vec![mount("data", "/data")],
                &[(
                    "containers[0].mounts[0].volume",
                    "is not a volume of the workload",
                )],
            ),
        ]);
    }

    #[test]
    fn image_command_and_env_rules() {
        check_rows(&[
            (
                "empty image",
                |s| s.containers[0].image = String::new(),
                &[("containers[0].image", "must not be empty")],
            ),
            (
                "longest image",
                |s| s.containers[0].image = "a".repeat(limits::MAX_IMAGE_BYTES),
                &[],
            ),
            (
                "image too long",
                |s| s.containers[0].image = "a".repeat(limits::MAX_IMAGE_BYTES + 1),
                &[("containers[0].image", "must be at most 512 bytes")],
            ),
            (
                "image with a space",
                |s| s.containers[0].image = "nginx latest".into(),
                &[(
                    "containers[0].image",
                    "must not contain whitespace or control characters",
                )],
            ),
            (
                "image with a control character",
                |s| s.containers[0].image = "nginx\u{7}".into(),
                &[(
                    "containers[0].image",
                    "must not contain whitespace or control characters",
                )],
            ),
            (
                "most command entries",
                |s| {
                    s.containers[0].command = vec!["a".into(); 200];
                    s.containers[0].args = vec![String::new(); 56];
                },
                &[],
            ),
            (
                "too many command entries",
                |s| {
                    s.containers[0].command = vec!["a".into(); 200];
                    s.containers[0].args = vec!["b".into(); 57];
                },
                &[(
                    "containers[0].command",
                    "command and args together must hold at most 256 entries",
                )],
            ),
            (
                "longest command entry",
                |s| {
                    s.containers[0].command = vec!["a".repeat(limits::MAX_COMMAND_ENTRY_BYTES)];
                    s.containers[0].args = vec!["a".repeat(limits::MAX_COMMAND_ENTRY_BYTES)];
                },
                &[],
            ),
            (
                "command entry too long",
                |s| {
                    s.containers[0].command =
                        vec!["sh".into(), "a".repeat(limits::MAX_COMMAND_ENTRY_BYTES + 1)];
                },
                &[("containers[0].command[1]", "must be at most 8192 bytes")],
            ),
            (
                "args entry too long",
                |s| {
                    s.containers[0].args = vec!["a".repeat(limits::MAX_COMMAND_ENTRY_BYTES + 1)];
                },
                &[("containers[0].args[0]", "must be at most 8192 bytes")],
            ),
            (
                "command entry with a NUL",
                |s| s.containers[0].command = vec!["a\0b".into()],
                &[(
                    "containers[0].command[0]",
                    "must not contain a NUL character",
                )],
            ),
            (
                "args entry with a NUL",
                |s| s.containers[0].args = vec!["ok\n".into(), "\0".into()],
                &[("containers[0].args[1]", "must not contain a NUL character")],
            ),
            (
                "most env vars",
                |s| {
                    s.containers[0].env = (0..limits::MAX_ENV_VARS)
                        .map(|i| (format!("V{i}"), String::new()))
                        .collect();
                },
                &[],
            ),
            (
                "too many env vars",
                |s| {
                    s.containers[0].env = (0..=limits::MAX_ENV_VARS)
                        .map(|i| (format!("V{i}"), String::new()))
                        .collect();
                },
                &[("containers[0].env", "must hold at most 256 variables")],
            ),
            (
                "env names of every allowed character",
                |s| {
                    s.containers[0].env = BTreeMap::from([
                        ("_".to_string(), "a".to_string()),
                        ("a.b-c_D9".to_string(), "multi\nline \u{1} é".to_string()),
                        ("a".repeat(limits::MAX_ENV_NAME_BYTES), String::new()),
                    ]);
                },
                &[],
            ),
            (
                "empty env name",
                |s| {
                    s.containers[0].env.insert(String::new(), "x".into());
                },
                &[(
                    "containers[0].env.",
                    "name must match [A-Za-z_][A-Za-z0-9_.-]*",
                )],
            ),
            (
                "env name starting with a digit",
                |s| {
                    s.containers[0].env.insert("1A".into(), "x".into());
                },
                &[(
                    "containers[0].env.1A",
                    "name must match [A-Za-z_][A-Za-z0-9_.-]*",
                )],
            ),
            (
                "env name with an equals sign",
                |s| {
                    s.containers[0].env.insert("A=B".into(), "x".into());
                },
                &[(
                    "containers[0].env.A=B",
                    "name must match [A-Za-z_][A-Za-z0-9_.-]*",
                )],
            ),
            (
                "env name starting with a dot",
                |s| {
                    s.containers[0].env.insert(".A".into(), "x".into());
                },
                &[(
                    "containers[0].env..A",
                    "name must match [A-Za-z_][A-Za-z0-9_.-]*",
                )],
            ),
            (
                "env name that is not ASCII",
                |s| {
                    s.containers[0].env.insert("CAFÉ".into(), "x".into());
                },
                &[(
                    "containers[0].env.CAFÉ",
                    "name must match [A-Za-z_][A-Za-z0-9_.-]*",
                )],
            ),
            (
                "longest env value",
                |s| {
                    s.containers[0]
                        .env
                        .insert("FOO".into(), "v".repeat(limits::MAX_ENV_VALUE_BYTES));
                },
                &[],
            ),
            (
                "env value too long",
                |s| {
                    s.containers[0]
                        .env
                        .insert("FOO".into(), "v".repeat(limits::MAX_ENV_VALUE_BYTES + 1));
                },
                &[("containers[0].env.FOO", "value must be at most 32768 bytes")],
            ),
            (
                "env value with a NUL",
                |s| {
                    s.containers[0].env.insert("FOO".into(), "a\0b".into());
                },
                &[(
                    "containers[0].env.FOO",
                    "value must not contain a NUL character",
                )],
            ),
        ]);
    }

    /// A name too long for the rule is cut in the path, so the path stays
    /// short however long the name is.
    #[test]
    fn an_env_name_too_long_is_refused_and_cut_in_the_path() {
        let mut spec = base();
        let name = "a".repeat(limits::MAX_ENV_NAME_BYTES + 1);
        spec.containers[0].env.insert(name, "x".into());
        let found = spec.validate().unwrap_err();
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].message, "name must be at most 256 bytes");
        assert_eq!(
            found[0].path,
            format!("containers[0].env.{}...", "a".repeat(64))
        );

        // Control characters never reach a path either.
        let mut spec = base();
        spec.volumes.insert("a\nb\u{1b}[0m".into(), ramdisk());
        let found = spec.validate().unwrap_err();
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].path, "volumes.a?b?[0m");
    }

    #[test]
    fn working_dir_and_user_rules() {
        check_rows(&[
            (
                "working dir",
                |s| s.containers[0].working_dir = Some("/srv/app".into()),
                &[],
            ),
            (
                "working dir of the root",
                |s| s.containers[0].working_dir = Some("/".into()),
                &[],
            ),
            (
                "relative working dir",
                |s| s.containers[0].working_dir = Some("app".into()),
                &[("containers[0].working_dir", "must be an absolute path")],
            ),
            (
                "working dir with a parent component",
                |s| s.containers[0].working_dir = Some("/srv/..".into()),
                &[(
                    "containers[0].working_dir",
                    "must not contain . or .. components",
                )],
            ),
            (
                "working dir with a trailing slash",
                |s| s.containers[0].working_dir = Some("/srv/".into()),
                &[("containers[0].working_dir", "must not end with /")],
            ),
            ("uid", |s| s.containers[0].user = Some("0".into()), &[]),
            (
                "highest uid",
                |s| s.containers[0].user = Some("4294967295".into()),
                &[],
            ),
            (
                "uid past u32",
                |s| s.containers[0].user = Some("4294967296".into()),
                &[(
                    "containers[0].user",
                    "must be a uid, uid:gid or a user name",
                )],
            ),
            (
                "uid and gid",
                |s| s.containers[0].user = Some("1000:4294967295".into()),
                &[],
            ),
            (
                "gid past u32",
                |s| s.containers[0].user = Some("1000:4294967296".into()),
                &[(
                    "containers[0].user",
                    "must be uid:gid in decimal: a group name is not supported",
                )],
            ),
            (
                "uid with a sign",
                |s| s.containers[0].user = Some("+5".into()),
                &[(
                    "containers[0].user",
                    "must be a uid, uid:gid or a user name",
                )],
            ),
            (
                "uid with a leading zero",
                |s| s.containers[0].user = Some("0755".into()),
                &[(
                    "containers[0].user",
                    "must be a uid, uid:gid or a user name",
                )],
            ),
            (
                "user name",
                |s| s.containers[0].user = Some("_www-data1".into()),
                &[],
            ),
            (
                "longest user name",
                |s| s.containers[0].user = Some("a".repeat(32)),
                &[],
            ),
            (
                "user name too long",
                |s| s.containers[0].user = Some("a".repeat(33)),
                &[(
                    "containers[0].user",
                    "must be a uid, uid:gid or a user name",
                )],
            ),
            (
                "user name in upper case",
                |s| s.containers[0].user = Some("Root".into()),
                &[(
                    "containers[0].user",
                    "must be a uid, uid:gid or a user name",
                )],
            ),
            (
                "empty user",
                |s| s.containers[0].user = Some(String::new()),
                &[(
                    "containers[0].user",
                    "must be a uid, uid:gid or a user name",
                )],
            ),
            (
                "user name and group name",
                |s| s.containers[0].user = Some("www:www".into()),
                &[(
                    "containers[0].user",
                    "must be uid:gid in decimal: a group name is not supported",
                )],
            ),
            (
                "uid and group name",
                |s| s.containers[0].user = Some("1000:www".into()),
                &[(
                    "containers[0].user",
                    "must be uid:gid in decimal: a group name is not supported",
                )],
            ),
            (
                "uid and empty gid",
                |s| s.containers[0].user = Some("1000:".into()),
                &[(
                    "containers[0].user",
                    "must be uid:gid in decimal: a group name is not supported",
                )],
            ),
            (
                "three parts",
                |s| s.containers[0].user = Some("1:2:3".into()),
                &[(
                    "containers[0].user",
                    "must be uid:gid in decimal: a group name is not supported",
                )],
            ),
        ]);
    }

    #[test]
    fn port_and_mount_rules() {
        check_rows(&[
            (
                "most ports",
                |s| {
                    s.containers[0].ports = (1..=limits::MAX_PORTS_PER_CONTAINER as u16)
                        .map(|p| port(p, Protocol::Tcp))
                        .collect();
                },
                &[],
            ),
            (
                "too many ports",
                |s| {
                    s.containers[0].ports = (0..=limits::MAX_PORTS_PER_CONTAINER as u16)
                        .map(|p| port(p + 1, Protocol::Tcp))
                        .collect();
                },
                &[("containers[0].ports", "must hold at most 64 ports")],
            ),
            (
                "highest port",
                |s| s.containers[0].ports = vec![port(65_535, Protocol::Udp)],
                &[],
            ),
            (
                "port zero",
                |s| s.containers[0].ports = vec![port(80, Protocol::Tcp), port(0, Protocol::Tcp)],
                &[("containers[0].ports[1].port", "must not be 0")],
            ),
            (
                "one port on both protocols",
                |s| s.containers[0].ports = vec![port(53, Protocol::Tcp), port(53, Protocol::Udp)],
                &[],
            ),
            (
                "one port and protocol twice",
                |s| {
                    s.containers[0].ports = vec![
                        port(53, Protocol::Udp),
                        port(80, Protocol::Tcp),
                        port(53, Protocol::Udp),
                    ];
                },
                &[("containers[0].ports[2]", "duplicate port and protocol")],
            ),
            (
                "one port in two containers",
                |s| {
                    s.containers[0].ports = vec![port(80, Protocol::Tcp)];
                    let mut other = ContainerSpec::new("other", "nginx");
                    other.ports = vec![port(80, Protocol::Tcp)];
                    s.containers.push(other);
                },
                &[],
            ),
            (
                "port names",
                |s| {
                    s.containers[0].ports = vec![port(80, Protocol::Tcp), port(443, Protocol::Tcp)];
                    s.containers[0].ports[0].name = Some("http".into());
                    s.containers[0].ports[1].name = Some("https".into());
                },
                &[],
            ),
            (
                "port name that is no label",
                |s| {
                    s.containers[0].ports = vec![port(80, Protocol::Tcp)];
                    s.containers[0].ports[0].name = Some("HTTP".into());
                },
                &[("containers[0].ports[0].name", LABEL_CHARS)],
            ),
            (
                "one port name twice",
                |s| {
                    s.containers[0].ports = vec![port(80, Protocol::Tcp), port(81, Protocol::Tcp)];
                    s.containers[0].ports[0].name = Some("http".into());
                    s.containers[0].ports[1].name = Some("http".into());
                },
                &[("containers[0].ports[1].name", "duplicate port name")],
            ),
            (
                "most mounts",
                |s| {
                    s.volumes.insert("data".into(), ramdisk());
                    s.containers[0].mounts = (0..limits::MAX_MOUNTS_PER_CONTAINER)
                        .map(|i| mount("data", &format!("/m{i}")))
                        .collect();
                },
                &[],
            ),
            (
                "too many mounts",
                |s| {
                    s.volumes.insert("data".into(), ramdisk());
                    s.containers[0].mounts = (0..=limits::MAX_MOUNTS_PER_CONTAINER)
                        .map(|i| mount("data", &format!("/m{i}")))
                        .collect();
                },
                &[("containers[0].mounts", "must hold at most 32 mounts")],
            ),
            (
                "mount of an undeclared volume",
                |s| {
                    s.volumes.insert("data".into(), ramdisk());
                    s.containers[0].mounts = vec![mount("data", "/a"), mount("cache", "/b")];
                },
                &[(
                    "containers[0].mounts[1].volume",
                    "is not a volume of the workload",
                )],
            ),
            (
                "one mount path twice",
                |s| {
                    s.volumes.insert("a".into(), ramdisk());
                    s.volumes.insert("b".into(), ramdisk());
                    s.containers[0].mounts = vec![mount("a", "/data"), mount("b", "/data")];
                },
                &[("containers[0].mounts[1].path", "duplicate mount path")],
            ),
            (
                "mount paths beside the kernel's directories",
                |s| {
                    s.volumes.insert("a".into(), ramdisk());
                    s.containers[0].mounts = [
                        "/dev/shm/cache",
                        "/device",
                        "/procfs",
                        "/system",
                        "/srv/proc",
                        "/srv/dev/null",
                    ]
                    .map(|path| mount("a", path))
                    .to_vec();
                },
                &[],
            ),
            (
                "mount over the root",
                |s| {
                    s.volumes.insert("a".into(), ramdisk());
                    s.containers[0].mounts = vec![mount("a", "/")];
                },
                &[("containers[0].mounts[0].path", "must not be /")],
            ),
            (
                "mounts over the kernel's directories",
                |s| {
                    s.volumes.insert("a".into(), ramdisk());
                    s.containers[0].mounts = [
                        "/proc",
                        "/proc/sys/kernel",
                        "/sys",
                        "/sys/fs/cgroup",
                        "/dev",
                        "/dev/null",
                        "/dev/shmem",
                    ]
                    .map(|path| mount("a", path))
                    .to_vec();
                },
                &[
                    ("containers[0].mounts[0].path", MOUNT_RESERVED),
                    ("containers[0].mounts[1].path", MOUNT_RESERVED),
                    ("containers[0].mounts[2].path", MOUNT_RESERVED),
                    ("containers[0].mounts[3].path", MOUNT_RESERVED),
                    ("containers[0].mounts[4].path", MOUNT_RESERVED),
                    ("containers[0].mounts[5].path", MOUNT_RESERVED),
                    ("containers[0].mounts[6].path", MOUNT_RESERVED),
                    ("containers[0].mounts[1].path", OVERLAP),
                    ("containers[0].mounts[3].path", OVERLAP),
                    ("containers[0].mounts[5].path", OVERLAP),
                    ("containers[0].mounts[6].path", OVERLAP),
                ],
            ),
            (
                "one mount path in two containers",
                |s| {
                    s.volumes.insert("a".into(), ramdisk());
                    s.containers[0].mounts = vec![mount("a", "/data")];
                    let mut other = ContainerSpec::new("other", "nginx");
                    other.mounts = vec![mount("a", "/data")];
                    s.containers.push(other);
                },
                &[],
            ),
            (
                "relative mount path",
                |s| {
                    s.volumes.insert("a".into(), ramdisk());
                    s.containers[0].mounts = vec![mount("a", "data")];
                },
                &[("containers[0].mounts[0].path", "must be an absolute path")],
            ),
            (
                "mount path that is not normalised",
                |s| {
                    s.volumes.insert("a".into(), ramdisk());
                    s.containers[0].mounts = vec![mount("a", "/data/./x")];
                },
                &[(
                    "containers[0].mounts[0].path",
                    "must not contain . or .. components",
                )],
            ),
        ]);
    }

    #[test]
    fn resource_and_capability_rules() {
        check_rows(&[
            ("default resources", |_| {}, &[]),
            (
                "smallest resources",
                |s| {
                    s.containers[0].resources = Resources {
                        cpu_millis: 1,
                        memory_bytes: 1,
                    };
                },
                &[],
            ),
            (
                "highest resources",
                |s| {
                    s.containers[0].resources = Resources {
                        cpu_millis: limits::MAX_CPU_MILLIS,
                        memory_bytes: limits::MAX_MEMORY_BYTES,
                    };
                },
                &[],
            ),
            (
                "resources too high",
                |s| {
                    s.containers[0].resources = Resources {
                        cpu_millis: limits::MAX_CPU_MILLIS + 1,
                        memory_bytes: limits::MAX_MEMORY_BYTES + 1,
                    };
                },
                &[
                    (
                        "containers[0].resources.cpu_millis",
                        "must be at most 256000",
                    ),
                    (
                        "containers[0].resources.memory_bytes",
                        "must be at most 4398046511104",
                    ),
                ],
            ),
            (
                "resources of zero",
                |s| {
                    s.containers[0].resources = Resources {
                        cpu_millis: 0,
                        memory_bytes: 0,
                    };
                },
                &[
                    (
                        "containers[0].resources.cpu_millis",
                        "must be greater than 0",
                    ),
                    (
                        "containers[0].resources.memory_bytes",
                        "must be greater than 0",
                    ),
                ],
            ),
            (
                "every allowed capability",
                |s| {
                    s.containers[0].capabilities =
                        ALLOWED_CAPABILITIES.iter().map(|c| c.to_string()).collect();
                },
                &[],
            ),
            (
                "capability that gives control of the node",
                |s| s.containers[0].capabilities = vec!["CHOWN".into(), "SYS_ADMIN".into()],
                &[("containers[0].capabilities[1]", SYS_ADMIN_REFUSED)],
            ),
            (
                "capability that is not allowed, twice",
                |s| s.containers[0].capabilities = vec!["SYS_ADMIN".into(), "SYS_ADMIN".into()],
                &[
                    ("containers[0].capabilities[0]", SYS_ADMIN_REFUSED),
                    ("containers[0].capabilities[1]", SYS_ADMIN_REFUSED),
                ],
            ),
            (
                "all capabilities",
                |s| s.containers[0].capabilities = vec!["ALL".into()],
                &[(
                    "containers[0].capabilities[0]",
                    "ALL is refused: list the capabilities the container needs",
                )],
            ),
            (
                "all capabilities in lower case",
                |s| s.containers[0].capabilities = vec!["all".into()],
                &[(
                    "containers[0].capabilities[0]",
                    "ALL is refused: list the capabilities the container needs",
                )],
            ),
            (
                "capability with the prefix",
                |s| s.containers[0].capabilities = vec!["CAP_NET_ADMIN".into()],
                &[(
                    "containers[0].capabilities[0]",
                    "is not a Linux capability: use upper case without the CAP_ prefix",
                )],
            ),
            (
                "capability in lower case",
                |s| s.containers[0].capabilities = vec!["CHOWN".into(), "net_admin".into()],
                &[(
                    "containers[0].capabilities[1]",
                    "is not a Linux capability: use upper case without the CAP_ prefix",
                )],
            ),
            (
                "unknown capability",
                |s| s.containers[0].capabilities = vec!["SYS_EVERYTHING".into()],
                &[(
                    "containers[0].capabilities[0]",
                    "is not a Linux capability: use upper case without the CAP_ prefix",
                )],
            ),
            (
                "empty capability",
                |s| s.containers[0].capabilities = vec![String::new()],
                &[(
                    "containers[0].capabilities[0]",
                    "is not a Linux capability: use upper case without the CAP_ prefix",
                )],
            ),
            (
                "one capability twice",
                |s| {
                    s.containers[0].capabilities =
                        vec!["CHOWN".into(), "KILL".into(), "CHOWN".into()];
                },
                &[("containers[0].capabilities[2]", "duplicate capability")],
            ),
        ]);
    }

    #[test]
    fn capabilities_are_the_kernel_list() {
        assert_eq!(CAPABILITIES.len(), 41);
        let unique: std::collections::BTreeSet<&str> = CAPABILITIES.iter().copied().collect();
        assert_eq!(unique.len(), CAPABILITIES.len());
        for name in CAPABILITIES {
            assert!(!name.starts_with("CAP_"), "{name}");
            assert!(
                name.bytes().all(|b| b.is_ascii_uppercase() || b == b'_'),
                "{name}"
            );
        }
        for name in [
            "CHOWN",
            "NET_BIND_SERVICE",
            "SYS_ADMIN",
            "CHECKPOINT_RESTORE",
        ] {
            assert!(CAPABILITIES.contains(&name), "{name}");
        }
        assert!(!CAPABILITIES.contains(&"ALL"));
    }

    const SYS_ADMIN_REFUSED: &str = "capability SYS_ADMIN is not allowed: it gives control of \
        the node. Allowed: CHOWN, DAC_OVERRIDE, FOWNER, FSETID, KILL, NET_BIND_SERVICE, SETFCAP, \
        SETGID, SETPCAP, SETUID, SYS_CHROOT";

    /// Only the capabilities that leave the node alone may be asked for.
    #[test]
    fn every_capability_outside_the_allowed_set_is_refused() {
        assert_eq!(
            ALLOWED_CAPABILITIES,
            [
                "CHOWN",
                "DAC_OVERRIDE",
                "FOWNER",
                "FSETID",
                "KILL",
                "NET_BIND_SERVICE",
                "SETFCAP",
                "SETGID",
                "SETPCAP",
                "SETUID",
                "SYS_CHROOT",
            ]
        );
        for name in CAPABILITIES {
            let mut spec = base();
            spec.containers[0].capabilities = vec![name.to_string()];
            if ALLOWED_CAPABILITIES.contains(name) {
                assert_eq!(spec.validate(), Ok(()), "{name}");
                continue;
            }
            assert_eq!(
                spec.validate(),
                Err(vec![Violation::new(
                    "containers[0].capabilities[0]",
                    format!(
                        "capability {name} is not allowed: it gives control of the node. \
                         Allowed: {}",
                        ALLOWED_CAPABILITIES.join(", ")
                    ),
                )]),
                "{name}"
            );
        }
        // Every allowed name is a kernel name.
        for name in ALLOWED_CAPABILITIES {
            assert!(CAPABILITIES.contains(name), "{name}");
        }
    }

    /// The fields that name something on a node or in an image hold plain
    /// ASCII, so nothing invisible or reordering can hide in them.
    #[test]
    fn identifiers_are_ascii() {
        const ASCII: &str = "must only contain ASCII characters";
        // A bidi override, a zero-width space, a byte order mark, and a
        // letter that only looks like ASCII.
        for hidden in [
            "\u{202e}", "\u{2066}", "\u{200f}", "\u{200b}", "\u{feff}", "\u{430}",
        ] {
            let mut spec = base();
            spec.volumes.insert("data".into(), ramdisk());
            let c = &mut spec.containers[0];
            c.image = format!("nginx{hidden}");
            c.working_dir = Some(format!("/srv/{hidden}"));
            c.user = Some(format!("www{hidden}"));
            c.mounts = vec![mount("data", &format!("/data{hidden}"))];
            c.probe = Some(Probe::new(ProbeAction::Http {
                port: 80,
                path: format!("/{hidden}"),
                scheme: HttpScheme::Http,
                host: Some(format!("web{hidden}.local")),
                headers: BTreeMap::new(),
            }));
            // Commands and env values may hold any text.
            c.command = vec![format!("echo{hidden}")];
            c.args = vec![hidden.to_string()];
            c.env.insert("GREETING".into(), hidden.to_string());
            assert_eq!(
                violations(&spec),
                [
                    ("containers[0].image", ASCII),
                    ("containers[0].working_dir", ASCII),
                    (
                        "containers[0].user",
                        "must be a uid, uid:gid or a user name"
                    ),
                    ("containers[0].mounts[0].path", ASCII),
                    ("containers[0].probe.action.path", ASCII),
                    ("containers[0].probe.action.host", ASCII),
                ]
                .map(|(path, message)| (path.to_string(), message.to_string())),
                "{hidden:?}"
            );
        }
    }

    /// Neither an env value nor a header value of a probe is ever printed.
    #[test]
    fn debug_prints_no_env_value_and_no_header_value() {
        let action = ProbeAction::Http {
            port: 80,
            path: "/ready".into(),
            scheme: HttpScheme::Https,
            host: Some("web.local".into()),
            headers: BTreeMap::from([
                (
                    "Authorization".to_string(),
                    "Bearer s3cr3t-token".to_string(),
                ),
                ("X-Probe".to_string(), "probe-secret".to_string()),
            ]),
        };
        let mut container = ContainerSpec::new("web", "nginx");
        container.env = BTreeMap::from([("API_KEY".to_string(), "hunter2".to_string())]);
        container.probe = Some(Probe::new(action.clone()));
        let spec = WorkloadSpec {
            containers: vec![container.clone()],
            ..base()
        };
        let request = DeployRequest {
            workloads: BTreeMap::from([("web".to_string(), spec.clone())]),
            prune: false,
        };
        let response = WorkloadResponse {
            project: "shop".into(),
            environment: "staging".into(),
            name: "web".into(),
            fqdn: fqdn("web", "staging", "shop"),
            revision: 1,
            generation: 1,
            spec: spec.clone(),
            replicas: Vec::new(),
        };
        for debug in [
            format!("{action:?}"),
            format!("{:?}", Probe::new(action.clone())),
            format!("{container:?}"),
            format!("{spec:?}"),
            format!("{request:?}"),
            format!("{response:?}"),
            format!("{request:#?}"),
        ] {
            for secret in ["hunter2", "s3cr3t-token", "Bearer", "probe-secret"] {
                assert!(!debug.contains(secret), "{secret}: {debug}");
            }
            // The names and the rest of the action still show.
            assert!(debug.contains("Authorization"), "{debug}");
            assert!(debug.contains("X-Probe"), "{debug}");
            assert!(debug.contains("/ready"), "{debug}");
            assert!(debug.contains("web.local"), "{debug}");
        }
        assert_eq!(
            format!("{:?}", ProbeAction::Tcp { port: 80 }),
            "Tcp { port: 80 }"
        );
    }

    #[test]
    fn probe_rules() {
        check_rows(&[
            (
                "lowest interval and timeout",
                |s| {
                    s.containers[0].probe = Some(Probe {
                        interval_secs: 2,
                        timeout_secs: 1,
                        ..Probe::new(ProbeAction::Tcp { port: 1 })
                    });
                },
                &[],
            ),
            (
                "highest interval",
                |s| {
                    s.containers[0].probe = Some(Probe {
                        interval_secs: limits::MAX_PROBE_INTERVAL_SECS,
                        timeout_secs: 3_599,
                        ..Probe::new(ProbeAction::Tcp { port: 1 })
                    });
                },
                &[],
            ),
            (
                "interval too long",
                |s| {
                    s.containers[0].probe = Some(Probe {
                        interval_secs: limits::MAX_PROBE_INTERVAL_SECS + 1,
                        ..Probe::new(ProbeAction::Tcp { port: 1 })
                    });
                },
                &[("containers[0].probe.interval_secs", "must be at most 3600")],
            ),
            (
                "timings of zero",
                |s| {
                    s.containers[0].probe = Some(Probe {
                        interval_secs: 0,
                        timeout_secs: 0,
                        ..Probe::new(ProbeAction::Tcp { port: 1 })
                    });
                },
                &[
                    ("containers[0].probe.interval_secs", "must be at least 1"),
                    ("containers[0].probe.timeout_secs", "must be at least 1"),
                ],
            ),
            (
                "timeout equal to the interval",
                |s| {
                    s.containers[0].probe = Some(Probe {
                        interval_secs: 3,
                        timeout_secs: 3,
                        ..Probe::new(ProbeAction::Tcp { port: 1 })
                    });
                },
                &[(
                    "containers[0].probe.timeout_secs",
                    "must be less than interval_secs (the default timeout_secs is 1 s, so interval_secs must be at least 2)",
                )],
            ),
            (
                "timeout above the interval",
                |s| {
                    s.containers[0].probe = Some(Probe {
                        interval_secs: 3,
                        timeout_secs: 4,
                        ..Probe::new(ProbeAction::Tcp { port: 1 })
                    });
                },
                &[(
                    "containers[0].probe.timeout_secs",
                    "must be less than interval_secs (the default timeout_secs is 1 s, so interval_secs must be at least 2)",
                )],
            ),
            (
                "default timings",
                |s| s.containers[0].probe = Some(Probe::new(ProbeAction::Tcp { port: 1 })),
                &[],
            ),
            (
                "tcp probe of port zero",
                |s| s.containers[0].probe = Some(Probe::new(ProbeAction::Tcp { port: 0 })),
                &[("containers[0].probe.action.port", "must not be 0")],
            ),
            (
                "http probe",
                |s| s.containers[0].probe = Some(Probe::new(http("/healthz?full=1"))),
                &[],
            ),
            (
                "http probe of port zero",
                |s| {
                    s.containers[0].probe = Some(Probe::new(ProbeAction::Http {
                        port: 0,
                        path: "/".into(),
                        scheme: HttpScheme::Https,
                        host: None,
                        headers: BTreeMap::new(),
                    }));
                },
                &[("containers[0].probe.action.port", "must not be 0")],
            ),
            (
                "http probe with a relative path",
                |s| s.containers[0].probe = Some(Probe::new(http("healthz"))),
                &[("containers[0].probe.action.path", "must start with /")],
            ),
            (
                "http probe with an empty path",
                |s| s.containers[0].probe = Some(Probe::new(http(""))),
                &[("containers[0].probe.action.path", "must start with /")],
            ),
            (
                "http probe with a line break in the path",
                |s| s.containers[0].probe = Some(Probe::new(http("/a\r\nX-Evil: 1"))),
                &[(
                    "containers[0].probe.action.path",
                    "must not contain whitespace or control characters",
                )],
            ),
            (
                "http probe with the longest path",
                |s| {
                    let path = format!("/{}", "a".repeat(limits::MAX_PATH_BYTES - 1));
                    s.containers[0].probe = Some(Probe::new(http(&path)));
                },
                &[],
            ),
            (
                "http probe with a path too long",
                |s| {
                    let path = format!("/{}", "a".repeat(limits::MAX_PATH_BYTES));
                    s.containers[0].probe = Some(Probe::new(http(&path)));
                },
                &[(
                    "containers[0].probe.action.path",
                    "must be at most 4096 bytes",
                )],
            ),
            (
                "http probe with a host",
                |s| s.containers[0].probe = Some(Probe::new(http_host("web.local:8080"))),
                &[],
            ),
            (
                "http probe with an empty host",
                |s| s.containers[0].probe = Some(Probe::new(http_host(""))),
                &[("containers[0].probe.action.host", "must not be empty")],
            ),
            (
                "http probe with a line break in the host",
                |s| s.containers[0].probe = Some(Probe::new(http_host("a\r\nX-Evil: 1"))),
                &[(
                    "containers[0].probe.action.host",
                    "must not contain whitespace or control characters",
                )],
            ),
            (
                "http probe with the longest host",
                |s| s.containers[0].probe = Some(Probe::new(http_host(&long_host(61)))),
                &[],
            ),
            (
                "http probe with a host too long",
                |s| s.containers[0].probe = Some(Probe::new(http_host(&long_host(62)))),
                &[(
                    "containers[0].probe.action.host",
                    "must be at most 253 bytes",
                )],
            ),
            (
                "http probe with headers",
                |s| {
                    s.containers[0].probe = Some(Probe::new(http_header("X-Probe_1.a~", "a b\tc")));
                },
                &[],
            ),
            (
                "http probe with an empty header name",
                |s| s.containers[0].probe = Some(Probe::new(http_header("", "1"))),
                &[(
                    "containers[0].probe.action.headers.",
                    "name must be an HTTP token",
                )],
            ),
            (
                "http probe with a colon in a header name",
                |s| s.containers[0].probe = Some(Probe::new(http_header("X:Y", "1"))),
                &[(
                    "containers[0].probe.action.headers.X:Y",
                    "name must be an HTTP token",
                )],
            ),
            (
                "http probe with a space in a header name",
                |s| s.containers[0].probe = Some(Probe::new(http_header("X Y", "1"))),
                &[(
                    "containers[0].probe.action.headers.X Y",
                    "name must be an HTTP token",
                )],
            ),
            (
                "http probe with a line feed in a header value",
                |s| s.containers[0].probe = Some(Probe::new(http_header("X-A", "1\nX-B: 2"))),
                &[(
                    "containers[0].probe.action.headers.X-A",
                    "value must not contain CR, LF or NUL",
                )],
            ),
            (
                "http probe with a carriage return in a header value",
                |s| s.containers[0].probe = Some(Probe::new(http_header("X-A", "1\r"))),
                &[(
                    "containers[0].probe.action.headers.X-A",
                    "value must not contain CR, LF or NUL",
                )],
            ),
        ]);
    }

    /// Nothing stops at the first broken rule, in a container or after it.
    #[test]
    fn every_violation_is_collected_in_field_order() {
        let mut spec = base();
        spec.scale = replicas(5_000, Strategy::Spread);
        spec.stop_grace_secs = Some(9_999);
        spec.volumes.insert("Bad".into(), block(1));
        spec.containers[0].image = String::new();
        spec.containers[0].user = Some("a:b".into());
        let mut second = ContainerSpec::new("web", "ok");
        second.capabilities = vec!["ALL".into()];
        second.probe = Some(Probe::new(ProbeAction::Tcp { port: 0 }));
        spec.containers.push(second);
        assert_eq!(
            violations(&spec)
                .iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>(),
            [
                "scale.replicas.count",
                "stop_grace_secs",
                "volumes.Bad",
                "volumes.Bad.size_bytes",
                "containers[0].image",
                "containers[0].user",
                "containers[1].name",
                "containers[1].capabilities[0]",
                "containers[1].probe.action.port",
            ]
        );
    }

    #[test]
    fn a_deploy_may_be_empty_and_holds_at_most_64_workloads() {
        // A deploy with `prune` and no workload empties the environment.
        assert_eq!(validate_workloads(&BTreeMap::new()), Ok(()));

        let many = |n: usize| -> BTreeMap<String, WorkloadSpec> {
            (0..n).map(|i| (format!("w{i}"), base())).collect()
        };
        assert_eq!(
            validate_workloads(&many(limits::MAX_WORKLOADS_PER_ENVIRONMENT)),
            Ok(())
        );
        assert_eq!(
            validate_workloads(&many(limits::MAX_WORKLOADS_PER_ENVIRONMENT + 1)),
            Err(vec![Violation::new("", "must hold at most 64 workloads")])
        );
    }

    #[test]
    fn a_deploy_reports_names_and_specs_under_the_workload_name() {
        let mut bad = base();
        bad.containers[0].image = String::new();
        bad.containers.push(ContainerSpec::new("Side", "envoy"));
        let workloads = BTreeMap::from([
            ("api".to_string(), base()),
            ("db_backup".to_string(), base()),
            ("web".to_string(), bad),
        ]);
        assert_eq!(
            validate_workloads(&workloads),
            Err(vec![
                Violation::new("db_backup", LABEL_CHARS),
                Violation::new("web.containers[0].image", "must not be empty"),
                Violation::new("web.containers[1].name", LABEL_CHARS),
            ])
        );
    }

    #[test]
    fn a_deploy_holds_at_most_128_kib_of_json() {
        // Three values of the largest size, and one that fills the rest.
        let mut spec = base();
        for name in ["A", "B", "C"] {
            spec.containers[0]
                .env
                .insert(name.into(), "v".repeat(limits::MAX_ENV_VALUE_BYTES));
        }
        spec.containers[0].env.insert("FILL".into(), String::new());
        let mut workloads = BTreeMap::from([("web".to_string(), spec)]);
        let empty = serde_json::to_vec(&workloads).unwrap().len();
        let fill = limits::MAX_DEPLOY_REQUEST_BYTES - empty;
        assert!(fill < limits::MAX_ENV_VALUE_BYTES, "{fill}");

        let set_fill = |workloads: &mut BTreeMap<String, WorkloadSpec>, n: usize| {
            let env = &mut workloads.get_mut("web").unwrap().containers[0].env;
            env.insert("FILL".into(), "v".repeat(n));
        };
        set_fill(&mut workloads, fill);
        assert_eq!(
            serde_json::to_vec(&workloads).unwrap().len(),
            limits::MAX_DEPLOY_REQUEST_BYTES
        );
        assert_eq!(validate_workloads(&workloads), Ok(()));

        set_fill(&mut workloads, fill + 1);
        assert_eq!(
            validate_workloads(&workloads),
            Err(vec![Violation::new(
                "",
                "is 131073 bytes as JSON, at most 131072 are allowed"
            )])
        );
    }

    #[test]
    fn prepare_deploy_exposes_named_workloads_and_adds_notes() {
        let mut workloads =
            BTreeMap::from([("api".to_string(), base()), ("web".to_string(), base())]);
        let notes = prepare_deploy(&mut workloads, &["web".to_string()], false).unwrap();
        assert_eq!(workloads["web"].expose, Exposure::Cluster);
        assert_eq!(workloads["api"].expose, Exposure::default());
        assert_eq!(notes, [NOTE_NO_CAPABILITIES]);

        workloads.get_mut("api").unwrap().containers[0]
            .env
            .insert("DB_PASSWORD".into(), "x".into());
        workloads.get_mut("api").unwrap().containers[0]
            .capabilities
            .push("NET_BIND_SERVICE".into());
        let notes = prepare_deploy(&mut workloads, &[], false).unwrap();
        assert_eq!(notes, [NOTE_SECRET_NAMES]);
    }

    #[test]
    fn prepare_deploy_refuses_empty_input_and_prune_of_nothing() {
        let issue = |source, message: &str| DeployIssue::new(source, "", message);
        assert_eq!(
            prepare_deploy(&mut BTreeMap::new(), &[], false),
            Err(vec![issue(
                DeployIssueSource::Input,
                "nothing to deploy: the input has no workloads"
            )])
        );
        let refused = prepare_deploy(&mut BTreeMap::new(), &[], true).unwrap_err();
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].source, DeployIssueSource::Prune);
        assert!(
            refused[0]
                .message
                .starts_with("the input has no workloads, so every")
        );
    }

    #[test]
    fn prepare_deploy_reports_unknown_expose_and_violations_together() {
        let mut bad = base();
        bad.containers[0].image = String::new();
        let mut workloads = BTreeMap::from([("web".to_string(), bad)]);
        let issues = prepare_deploy(&mut workloads, &["nope".to_string()], false).unwrap_err();
        assert_eq!(
            issues,
            vec![
                DeployIssue::new(
                    DeployIssueSource::Expose,
                    "",
                    "no workload named \"nope\"; the input has: web"
                ),
                DeployIssue::new(
                    DeployIssueSource::Input,
                    "web.containers[0].image",
                    "must not be empty"
                ),
            ]
        );
    }

    #[test]
    fn secret_names_are_matched_in_any_case() {
        for name in [
            "DB_PASSWORD",
            "db_passwd",
            "ClientSecret",
            "GITHUB_TOKEN",
            "stripe_api_key",
            "SSH_PRIVATE_KEY",
        ] {
            assert!(looks_like_a_secret(name), "{name}");
        }
        for name in ["MODE", "PORT", "KEYBOARD", "API_URL", "PASS"] {
            assert!(!looks_like_a_secret(name), "{name}");
        }
    }

    #[test]
    fn route_dtos_have_pinned_shapes() {
        assert_eq!(
            serde_json::to_value(CreateProjectRequest {
                name: "shop".into(),
                environments: None,
            })
            .unwrap(),
            json!({"name": "shop"})
        );
        assert_eq!(
            serde_json::from_value::<CreateProjectRequest>(
                json!({"name": "shop", "environments": ["dev"]})
            )
            .unwrap()
            .environments,
            Some(vec!["dev".to_string()])
        );
        let project = ProjectResponse {
            name: "shop".into(),
            created_at: 1_000,
            environments: vec![EnvironmentSummary {
                name: "staging".into(),
                generation: 7,
                workloads: 2,
                bytes: 512,
                updated_at: 1_001,
            }],
        };
        let want = json!({"projects": [{
            "name": "shop", "created_at": 1_000,
            "environments": [{
                "name": "staging", "generation": 7, "workloads": 2, "bytes": 512,
                "updated_at": 1_001,
            }],
        }]});
        let list = ProjectListResponse {
            projects: vec![project],
        };
        assert_eq!(serde_json::to_value(&list).unwrap(), want);
        assert_eq!(
            serde_json::from_value::<ProjectListResponse>(want).unwrap(),
            list
        );

        // `prune` may be left out.
        let deploy: DeployRequest = serde_json::from_value(json!({"workloads": {}})).unwrap();
        assert!(!deploy.prune);
        assert_eq!(
            serde_json::to_value(&deploy).unwrap(),
            json!({"workloads": {}, "prune": false})
        );
        assert_eq!(
            serde_json::to_value(DeployResponse {
                generation: 9,
                created: vec!["web".into()],
                updated: vec![],
                unchanged: vec!["db".into()],
                removed: vec!["old".into()],
            })
            .unwrap(),
            json!({"generation": 9, "created": ["web"], "updated": [],
                   "unchanged": ["db"], "removed": ["old"]})
        );

        assert_eq!(
            serde_json::to_value(WorkloadQuery::default()).unwrap(),
            json!({})
        );
        let summary = WorkloadSummary {
            project: "shop".into(),
            environment: "staging".into(),
            name: "web".into(),
            fqdn: fqdn("web", "staging", "shop"),
            scale: replicas(3, Strategy::Spread),
            expose: Exposure::Cluster,
            revision: 7,
            available: 2,
            desired: 3,
        };
        assert_eq!(
            serde_json::to_value(WorkloadListResponse {
                workloads: vec![summary],
            })
            .unwrap(),
            json!({"workloads": [{
                "project": "shop", "environment": "staging", "name": "web",
                "fqdn": "web.staging.shop.faber.local",
                "scale": {"replicas": {"count": 3, "strategy": "spread"}}, "expose": "cluster", "revision": 7,
                "available": 2, "desired": 3,
            }]})
        );
        let one: WorkloadResponse = serde_json::from_value(json!({
            "project": "shop", "environment": "staging", "name": "web",
            "fqdn": "web.staging.shop.faber.local", "revision": 7, "generation": 9,
            "spec": {"containers": [{"name": "web", "image": "nginx"}]},
        }))
        .unwrap();
        assert_eq!(one.spec.containers[0].image, "nginx");
        assert_eq!(serde_json::to_value(&one).unwrap()["generation"], 9);
    }

    /// Mounts every volume of `s` into the first container, each under
    /// `/v/<name>`.
    fn mount_all(s: &mut WorkloadSpec) {
        s.containers[0].mounts = s
            .volumes
            .keys()
            .map(|name| mount(name, &format!("/v/{name}")))
            .collect();
    }

    fn ram(size_bytes: u64) -> VolumeSource {
        VolumeSource::Ramdisk { size_bytes }
    }

    #[test]
    fn aggregate_volume_rules() {
        const GIB: u64 = 1 << 30;
        check_rows(&[
            (
                "block volumes at the total cap",
                |s| {
                    for i in 0..4 {
                        s.volumes
                            .insert(format!("b{i}"), block(limits::MAX_BLOCK_VOLUME_BYTES));
                    }
                },
                &[],
            ),
            (
                "block volumes above the total cap",
                |s| {
                    for i in 0..4 {
                        s.volumes
                            .insert(format!("b{i}"), block(limits::MAX_BLOCK_VOLUME_BYTES));
                    }
                    s.volumes
                        .insert("extra".into(), block(limits::MIN_VOLUME_BYTES));
                },
                &[(
                    "volumes",
                    "block volumes total 4398047559680 bytes, at most 4398046511104 are allowed",
                )],
            ),
            (
                "ramdisk volumes at the total cap",
                |s| {
                    for i in 0..4 {
                        s.volumes.insert(format!("r{i}"), ram(64 * GIB));
                    }
                },
                &[],
            ),
            (
                "ramdisk volumes above the total cap",
                |s| {
                    for i in 0..4 {
                        s.volumes.insert(format!("r{i}"), ram(64 * GIB));
                    }
                    s.volumes.insert("extra".into(), ramdisk());
                },
                &[(
                    "volumes",
                    "ramdisk volumes total 274878955520 bytes, at most 274877906944 are allowed",
                )],
            ),
            (
                "ramdisks mounted within the memory",
                |s| {
                    s.volumes.insert("a".into(), ram(512 << 20));
                    s.volumes.insert("b".into(), ram(512 << 20));
                    mount_all(s);
                },
                &[],
            ),
            (
                "ramdisks mounted beyond the memory",
                |s| {
                    s.volumes.insert("a".into(), ram(512 << 20));
                    s.volumes.insert("b".into(), ram(513 << 20));
                    mount_all(s);
                },
                &[(
                    "containers[0].mounts",
                    "ramdisk volumes total 1074790400 bytes, more than memory_bytes (1073741824)",
                )],
            ),
            (
                "one ramdisk mounted twice counts once",
                |s| {
                    s.volumes.insert("a".into(), ram(800 << 20));
                    s.containers[0].mounts = vec![mount("a", "/one"), mount("a", "/two")];
                },
                &[],
            ),
            (
                "ramdisk mounted by no container is not counted",
                |s| {
                    s.volumes.insert("a".into(), ram(8 * GIB));
                },
                &[],
            ),
            (
                "block volumes do not count against the memory",
                |s| {
                    s.volumes.insert("a".into(), block(8 * GIB));
                    mount_all(s);
                },
                &[],
            ),
        ]);
    }

    #[test]
    fn nested_and_overlapping_mounts_are_refused() {
        fn with(paths: &[&str]) -> Vec<(String, String)> {
            let mut s = base();
            s.volumes.insert("a".into(), ramdisk());
            s.containers[0].mounts = paths.iter().map(|p| mount("a", p)).collect();
            violations(&s)
        }
        assert_eq!(with(&["/a", "/ab", "/b/a"]), vec![]);
        let nested = |at: usize| {
            vec![(
                format!("containers[0].mounts[{at}].path"),
                "overlaps another mount path".to_string(),
            )]
        };
        assert_eq!(with(&["/a", "/a/b"]), nested(1));
        assert_eq!(with(&["/a/b", "/a"]), nested(1));
        assert_eq!(with(&["/a", "/x", "/a/b/c"]), nested(2));
    }

    #[test]
    fn probe_host_and_header_rules() {
        let probe = |action| {
            let mut s = base();
            s.containers[0].probe = Some(Probe::new(action));
            violations(&s)
        };
        let host_msg = "must be a hostname or host:port";
        let at = "containers[0].probe.action";
        for ok in [
            "web.local",
            "web.local:8080",
            "10.0.0.1:80",
            "a",
            "[::1]:80",
            "[::1]",
        ] {
            assert_eq!(probe(http_host(ok)), vec![], "{ok}");
        }
        for bad in [
            "-a.b", "a.b-", "a..b", "a:b:c", "a:", "a:0", "a:65536", "a:x", ":80", "[::1", "[]:80",
            "a_b!",
        ] {
            assert_eq!(
                probe(http_host(bad)),
                vec![(format!("{at}.host"), host_msg.to_string())],
                "{bad}"
            );
        }
        for name in [
            "Host",
            "host",
            "Content-Length",
            "TRANSFER-ENCODING",
            "connection",
        ] {
            assert_eq!(
                probe(http_header(name, "x")),
                vec![(
                    format!("{at}.headers.{name}"),
                    "name is reserved: the runner sets it".to_string()
                )],
                "{name}"
            );
        }
        assert_eq!(probe(http_header("X-Host", "x")), vec![]);
    }

    #[test]
    fn a_replica_count_of_zero_alone_is_valid() {
        let spec: WorkloadSpec = serde_json::from_value(json!({
            "scale": {"replicas": {"count": 0}},
            "containers": [{"name": "web", "image": "nginx"}],
        }))
        .unwrap();
        assert_eq!(spec.scale, replicas(0, Strategy::Spread));
        assert_eq!(spec.validate(), Ok(()));
    }

    #[test]
    fn an_interval_of_one_second_says_what_the_default_timeout_is() {
        let mut s = base();
        s.containers[0].probe = Some(Probe {
            interval_secs: 1,
            ..Probe::new(ProbeAction::Tcp { port: 1 })
        });
        assert_eq!(
            violations(&s),
            vec![(
                "containers[0].probe.timeout_secs".to_string(),
                "must be less than interval_secs (the default timeout_secs is 1 s, so interval_secs must be at least 2)"
                    .to_string()
            )]
        );
    }

    /// One test per shape of an earlier version.
    macro_rules! old_shape_refused {
        ($($name:ident: $json:expr;)*) => {$(
            #[test]
            fn $name() {
                let err = serde_json::from_value::<WorkloadSpec>($json).unwrap_err();
                assert!(!err.to_string().is_empty());
            }
        )*};
    }

    old_shape_refused! {
        refuses_replicas_as_a_number: json!({"scale": {"replicas": 3},
            "containers": [{"name": "web", "image": "nginx"}]});
        refuses_per_node: json!({"scale": "per_node",
            "containers": [{"name": "web", "image": "nginx"}]});
        refuses_placement_node: json!({"placement": {"node": "n1"},
            "containers": [{"name": "web", "image": "nginx"}]});
        refuses_host_path_volume: json!({"volumes": {"v": {"type": "host_path", "path": "/srv"}},
            "containers": [{"name": "web", "image": "nginx"}]});
        refuses_named_volume: json!({"volumes": {"v": {"type": "named", "name": "v"}},
            "containers": [{"name": "web", "image": "nginx"}]});
        refuses_ephemeral_volume: json!({"volumes": {"v": {"type": "ephemeral"}},
            "containers": [{"name": "web", "image": "nginx"}]});
        refuses_resource_limits: container_with(json!({"resources": {"limits": {"cpu_millis": 1}}}));
        refuses_resource_requests: container_with(json!({"resources": {"requests": {}}}));
        refuses_exec_probe: container_with(json!({"probe":
            {"action": {"type": "exec", "command": ["true"]}}}));
        refuses_liveness: container_with(json!({"liveness":
            {"action": {"type": "tcp", "port": 1}}}));
        refuses_readiness: container_with(json!({"readiness":
            {"action": {"type": "tcp", "port": 1}}}));
        refuses_startup: container_with(json!({"startup":
            {"action": {"type": "tcp", "port": 1}}}));
    }
}
