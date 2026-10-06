//! The compose converter: one compose file in, one workload per service out.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::{
    ALLOWED_CAPABILITIES, CAPABILITIES, ContainerSpec, Exposure, MountSpec, Placement, PortSpec,
    Probe, ProbeAction, Protocol, ResourceAmounts, Resources, RestartPolicy, Scale, Violation,
    VolumeSource, WorkloadSpec, validate_workloads,
};
use serde_json::Value;

use crate::envfile::{self, Entry};
use crate::interpolate::{InterpolateError, Interpolated, interpolate};
use crate::walk::{Cursor, Walk, child_path};
use crate::{ComposeInput, Conversion, ConvertError, Diagnostic, Diagnostics, Files, units, yaml};

/// Service keys that change nothing here. Labels are metadata, and the
/// runner drops every capability whether `cap_drop` asks for it or not.
const IGNORED: &[&str] = &["labels", "annotations", "label_file", "cap_drop"];

/// Service keys that are skipped with a warning: they do not change how
/// the container runs.
const SKIPPED: &[(&str, &str)] = &[
    (
        "depends_on",
        "skipped: faber starts the workloads of a deploy in no particular order",
    ),
    (
        "links",
        "skipped: the workloads of one environment reach each other by name",
    ),
    (
        "external_links",
        "skipped: the workloads of one environment reach each other by name",
    ),
    ("container_name", "skipped: faber names containers itself"),
    (
        "hostname",
        "skipped: a workload is reached by the name of its service",
    ),
    (
        "domainname",
        "skipped: a workload is reached by the name of its service",
    ),
    ("logging", "skipped: logging drivers cannot be configured"),
    (
        "pull_policy",
        "skipped: the pull policy cannot be configured",
    ),
    (
        "develop",
        "skipped: it only matters to `docker compose watch`",
    ),
    ("attach", "skipped: it only matters to `docker compose up`"),
];

const NETWORKS_SKIPPED: &str =
    "skipped: faber has no networks to choose from; the workloads of one environment share one";

/// Service keys that are refused, and why. A key at its no-op value is
/// not: `privileged: false` is fine.
const REFUSED: &[(&str, &str)] = &[
    (
        "build",
        "not supported: faber does not build images; build and push the image, then name it under `image`",
    ),
    (
        "extends",
        "not supported: run `docker compose config` and deploy its output",
    ),
    (
        "privileged",
        "not supported: containers run unprivileged; list the capabilities the service needs under `cap_add`",
    ),
    ("devices", "not supported: containers get no host devices"),
    (
        "device_cgroup_rules",
        "not supported: containers get no host devices",
    ),
    ("gpus", "not supported: containers get no host devices"),
    ("runtime", "not supported: the container runtime is fixed"),
    (
        "sysctls",
        "not supported: kernel parameters cannot be set for a container",
    ),
    (
        "ulimits",
        "not supported: only CPU and memory can be limited",
    ),
    (
        "group_add",
        "not supported: groups cannot be added; set `user` to `uid:gid`",
    ),
    ("userns_mode", "not supported: the user namespace is fixed"),
    (
        "credential_spec",
        "not supported: it applies to Windows containers only",
    ),
    ("pid", "not supported: a workload has its own namespaces"),
    ("ipc", "not supported: a workload has its own namespaces"),
    ("uts", "not supported: a workload has its own namespaces"),
    ("cgroup", "not supported: a workload has its own namespaces"),
    ("cgroup_parent", "not supported: faber places the cgroups"),
    ("isolation", "not supported: the isolation is fixed"),
    ("dns", "not supported: faber sets the DNS of a container"),
    (
        "dns_opt",
        "not supported: faber sets the DNS of a container",
    ),
    (
        "dns_search",
        "not supported: faber sets the DNS of a container",
    ),
    (
        "extra_hosts",
        "not supported: host entries cannot be added to a container",
    ),
    (
        "mac_address",
        "not supported: faber assigns the addresses of a container",
    ),
    (
        "init",
        "not supported: no init process is added; start one in the image if the service needs it",
    ),
    ("tty", "not supported: containers run without a terminal"),
    (
        "stdin_open",
        "not supported: containers run without standard input",
    ),
    (
        "stop_signal",
        "not supported: a container is stopped with the signal of its image (`STOPSIGNAL`)",
    ),
    (
        "shm_size",
        "not supported: the size of /dev/shm cannot be set",
    ),
    (
        "platform",
        "not supported: an image runs on the platform of its node",
    ),
    ("post_start", "not supported: lifecycle hooks do not run"),
    ("pre_stop", "not supported: lifecycle hooks do not run"),
    ("cpu_shares", CPU_TUNING),
    ("cpu_quota", CPU_TUNING),
    ("cpu_period", CPU_TUNING),
    ("cpu_rt_runtime", CPU_TUNING),
    ("cpu_rt_period", CPU_TUNING),
    ("cpu_count", CPU_TUNING),
    ("cpu_percent", CPU_TUNING),
    ("cpuset", CPU_TUNING),
    ("memswap_limit", MEMORY_TUNING),
    ("mem_swappiness", MEMORY_TUNING),
    ("oom_kill_disable", MEMORY_TUNING),
    ("oom_score_adj", MEMORY_TUNING),
    ("blkio_config", "not supported: block I/O cannot be tuned"),
    (
        "storage_opt",
        "not supported: storage driver options cannot be set",
    ),
    (
        "volumes_from",
        "not supported: list the volumes under `volumes` of this service",
    ),
    (
        "configs",
        "not supported: faber cannot hand a config to a container yet",
    ),
    (
        "secrets",
        "not supported: faber cannot hand a secret to a container yet",
    ),
    ("models", "not supported: faber does not run models"),
    (
        "provider",
        "not supported: faber runs containers, not provider services",
    ),
    (
        "use_api_socket",
        "not supported: a container gets no access to the container runtime",
    ),
];

const CPU_TUNING: &str = "not supported: `cpus` is the one CPU setting that is used";
const MEMORY_TUNING: &str =
    "not supported: `mem_limit` and `mem_reservation` are the memory settings that are used";
const PIDS: &str = "not supported: the number of processes cannot be limited";
const SELINUX: &str = "skipped: SELinux labels are not changed";
const NOCOPY: &str =
    "skipped: a new volume starts empty, without the files of the image at that path";

/// Docker's timings for a health check that sets none.
const HEALTH_INTERVAL_SECS: u32 = 30;
const HEALTH_TIMEOUT_SECS: u32 = 30;
const HEALTH_RETRIES: u32 = 3;

/// Converts the compose file `input.file`, read through `files`.
///
/// Each service that is active under `input.profiles` becomes a workload
/// of one container, named after the service. Variables are replaced from
/// `input.variables` and then from the env file. Every file read (the
/// compose file, the env file, each `env_file` of a service) goes through
/// `files`.
///
/// `docs/contract.md` lists, per key, whether it is mapped, ignored,
/// skipped with a warning or refused.
pub fn convert(input: &ComposeInput, files: &dyn Files) -> Result<Conversion, ConvertError> {
    let source = slashes(&input.file);
    let mut found = Diagnostics::default();
    let document = match read_document(input, files) {
        Ok(document) => document,
        Err(message) => {
            found.errors.push(Diagnostic::new(source, "", message));
            return found.finish(BTreeMap::new());
        }
    };
    let dir = Dir::of(&input.file);
    let variables = project_variables(input, files, &dir, &mut found);

    let walk = Walk::new(source.clone());
    let document = interpolate_document(&walk, document, &variables);
    let mut converter = Converter {
        input,
        files,
        variables: &variables,
        dir: &dir,
        declared: BTreeSet::new(),
        skipped: Vec::new(),
        origins: RefCell::new(BTreeMap::new()),
    };
    let workloads = converter.top(&walk.root(&document));
    found.extend(walk.finish());
    let written = variables.written.take();

    // The model's own rules, once the file itself has nothing wrong: a
    // spec built from a refused file is not worth checking.
    if found.errors.is_empty()
        && let Err(violations) = validate_workloads(&workloads)
    {
        let origins = converter.origins.borrow();
        found.errors.extend(
            violations
                .into_iter()
                .map(|violation| invalid(&source, violation, &origins)),
        );
    }
    hide_values(&mut found, &source, &written);
    let mut done = found.finish(workloads)?;
    done.skipped = converter.skipped;
    done.variables = variables.used.into_inner().into_iter().collect();
    Ok(done)
}

/// The one document of the compose file, which must be a map.
fn read_document(input: &ComposeInput, files: &dyn Files) -> Result<Value, String> {
    let text = files
        .read(&input.file)
        .map_err(|e| format!("cannot read the compose file: {e}"))?;
    let mut documents = yaml::parse_documents(&text).map_err(|e| format!("not valid YAML: {e}"))?;
    match documents.len() {
        1 => {}
        0 => return Err("the file is empty; a compose file needs `services`".to_string()),
        n => {
            return Err(format!(
                "expected one YAML document, found {n}; put all services into one"
            ));
        }
    }
    let document = documents.remove(0);
    if !document.is_object() {
        return Err("expected a map with `services` at the top".to_string());
    }
    Ok(document)
}

/// A rule of the model that a converted workload breaks, reported where
/// the file set what breaks it. `origins` maps paths of the model
/// (`web.containers[0].mounts[1]`) to paths of the file
/// (`services.web.volumes[1]`); a violation at or below one of them goes
/// to that place, and any other to its service.
fn invalid(source: &str, violation: Violation, origins: &BTreeMap<String, String>) -> Diagnostic {
    let path = violation.path.as_str();
    let below = |model: &str| {
        let rest = path.strip_prefix(model)?;
        (rest.is_empty() || rest.starts_with(['.', '['])).then_some(rest)
    };
    let closest = origins
        .iter()
        .filter_map(|(model, origin)| Some((model.len(), origin, below(model)?)))
        .max_by_key(|(len, ..)| *len);
    if let Some((_, origin, rest)) = closest {
        let rest = rest.trim_start_matches('.');
        let message = if rest.is_empty() {
            violation.message
        } else {
            format!("{rest}: {}", violation.message)
        };
        return Diagnostic::new(source, origin.as_str(), message);
    }
    let (name, rest) = path.split_once('.').unwrap_or((path, ""));
    if name.is_empty() {
        let message = format!(
            "the converted workloads are not valid: {}",
            violation.message
        );
        return Diagnostic::new(source, "", message);
    }
    let message = if rest.is_empty() {
        violation.message
    } else {
        format!(
            "the converted workload is not valid: {rest}: {}",
            violation.message
        )
    };
    Diagnostic::new(source, child_path("services", name), message)
}

/// The variables `${VAR}` is replaced from, and which of them were read.
#[derive(Default)]
struct Variables {
    /// The caller's, and below them those of the env file.
    values: BTreeMap<String, String>,
    /// For a variable of the env file whose value refers to others: those
    /// others, all the way down.
    built_from: BTreeMap<String, BTreeSet<String>>,
    /// The variables whose value went into the result.
    used: RefCell<BTreeSet<String>>,
    /// The values of the file that a variable went into, by their path.
    written: RefCell<BTreeMap<String, Written>>,
}

/// A value of the file before and after a variable was inserted into it.
struct Written {
    /// As the file has it: `${TAG}`.
    written: String,
    resolved: String,
    /// What was inserted.
    values: Vec<String>,
}

/// Shortest piece of an inserted value that is looked for on its own.
const MIN_HIDDEN: usize = 3;

impl Written {
    /// `message` without anything a variable brought in: a message about
    /// this value shows it as the file has it. The value of a variable may
    /// be a secret, and a diagnostic ends up in terminals and logs.
    fn hide(&self, message: &str) -> String {
        // What was inserted, whole and in the pieces a parser may have
        // cut it into.
        let mut pieces = Vec::new();
        for value in &self.values {
            pieces.push(value.as_str());
            pieces.extend(value.split(|c: char| ":,=/".contains(c) || c.is_whitespace()));
        }
        if self.resolved != self.written {
            pieces.push(&self.resolved);
        }
        pieces.retain(|piece| piece.chars().count() >= MIN_HIDDEN);
        // Longest first, so that a piece does not break up a whole.
        pieces.sort_by_key(|piece| std::cmp::Reverse(piece.len()));
        pieces.dedup();
        let mut message = message.to_string();
        for piece in pieces {
            message = replace_ignoring_case(&message, piece, &self.written);
        }

        // What is quoted comes from the value, also where it is shorter
        // than a piece, only a part of one, or has one in it (`7s`).
        let resolved = self.resolved.to_ascii_lowercase();
        let written = self.written.to_ascii_lowercase();
        let mut out = String::with_capacity(message.len());
        for (i, part) in message.split('`').enumerate() {
            if i > 0 {
                out.push('`');
            }
            let quoted = part.to_ascii_lowercase();
            let from_value = (resolved.contains(&quoted) && !written.contains(&quoted))
                || self
                    .values
                    .iter()
                    .any(|value| !value.is_empty() && quoted.contains(&value.to_ascii_lowercase()));
            let inserted = i % 2 == 1 && !part.is_empty() && from_value;
            out.push_str(if inserted { &self.written } else { part });
        }
        out
    }
}

/// `text` with every `piece` replaced, whatever its case.
fn replace_ignoring_case(text: &str, piece: &str, with: &str) -> String {
    // ASCII case only, so the offsets are those of `text`.
    let (lower, piece) = (text.to_ascii_lowercase(), piece.to_ascii_lowercase());
    let mut out = String::with_capacity(text.len());
    let mut from = 0;
    while let Some(at) = lower[from..].find(&piece) {
        out.push_str(&text[from..from + at]);
        out.push_str(with);
        from += at + piece.len();
    }
    out.push_str(&text[from..]);
    out
}

/// Takes the values of variables out of the diagnostics of the compose
/// file. A diagnostic may quote the value it is about, a part of it, or a
/// value below or above it, so each is checked against all of these.
fn hide_values(found: &mut Diagnostics, source: &str, written: &BTreeMap<String, Written>) {
    if written.is_empty() {
        return;
    }
    let below = |path: &str, parent: &str| {
        path.strip_prefix(parent).is_some_and(|rest| {
            parent.is_empty() || rest.is_empty() || rest.starts_with(['.', '['])
        })
    };
    for diagnostic in found.errors.iter_mut().chain(&mut found.warnings) {
        if diagnostic.source != source {
            continue;
        }
        for (path, value) in written {
            if below(&diagnostic.path, path) || below(path, &diagnostic.path) {
                diagnostic.message = value.hide(&diagnostic.message);
            }
        }
    }
}

impl Variables {
    /// The value of `name`, which counts as used from here on.
    fn get(&self, name: &str) -> Option<String> {
        let value = self.values.get(name)?;
        self.mark(name);
        Some(value.clone())
    }

    /// Interpolates `text`. What it inserts counts as used.
    fn expand(&self, text: &str) -> Result<Interpolated, InterpolateError> {
        let done = interpolate(text, &self.values)?;
        for name in &done.used {
            self.mark(name);
        }
        Ok(done)
    }

    /// Notes that the value at `path` of the file, `written` there, has
    /// the values of variables in it.
    fn inserted(&self, path: &str, written: &str, done: &Interpolated) {
        let values = done.used.iter().filter_map(|name| self.values.get(name));
        let value = Written {
            written: written.to_string(),
            resolved: done.text.clone(),
            values: values.cloned().collect(),
        };
        self.written.borrow_mut().insert(path.to_string(), value);
    }

    fn mark(&self, name: &str) {
        let mut used = self.used.borrow_mut();
        used.insert(name.to_string());
        if let Some(parts) = self.built_from.get(name) {
            used.extend(parts.iter().cloned());
        }
    }
}

fn outside_message(resolved: &str) -> String {
    format!(
        "`{resolved}` is outside the directory of the compose file, and what it holds \
         would go into the deploy; pass `--allow-external-env-file` to read it"
    )
}

/// The caller's variables, and below them those of the env file.
fn project_variables(
    input: &ComposeInput,
    files: &dyn Files,
    dir: &Dir,
    found: &mut Diagnostics,
) -> Variables {
    let mut variables = Variables {
        values: input.variables.clone(),
        ..Variables::default()
    };
    let (path, named) = match &input.env_file {
        Some(path) => (normalize(&slashes(path)), true),
        None => (dir.resolve(".env"), false),
    };
    if !dir.contains(&path) {
        if !input.allow_external_env_files {
            let message = outside_message(&path);
            found.errors.push(Diagnostic::new(path, "", message));
            return variables;
        }
        let message = "is outside the directory of the compose file";
        found
            .warnings
            .push(Diagnostic::new(path.as_str(), "", message));
    }
    let text = match files.read(Path::new(&path)) {
        Ok(text) => text,
        // Only the file the caller named has to be there.
        Err(e) if e.kind() == io::ErrorKind::NotFound && !named => return variables,
        Err(e) => {
            let message = format!("cannot read the env file: {e}");
            found.errors.push(Diagnostic::new(path, "", message));
            return variables;
        }
    };
    let entries = match envfile::parse_entries(&text) {
        Ok(entries) => entries,
        Err(e) => {
            found.errors.push(Diagnostic::new(path, "", e.to_string()));
            return variables;
        }
    };
    for entry in entries {
        // A bare `KEY` means the caller's variable, which is there or not.
        let Some(value) = &entry.value else {
            continue;
        };
        if input.variables.contains_key(&entry.key) {
            continue;
        }
        if entry.literal {
            variables.built_from.remove(&entry.key);
            variables.values.insert(entry.key, value.clone());
            continue;
        }
        // Not through `expand`: a variable of the file is used only once
        // the compose file reads it.
        match interpolate(value, &variables.values) {
            Ok(done) => {
                for name in &done.unset {
                    let message = unset_message(name);
                    found.warnings.push(Diagnostic::new(
                        path.as_str(),
                        entry.key.as_str(),
                        message,
                    ));
                }
                let mut parts = BTreeSet::new();
                for name in done.used {
                    if let Some(deeper) = variables.built_from.get(&name) {
                        parts.extend(deeper.iter().cloned());
                    }
                    parts.insert(name);
                }
                variables.built_from.insert(entry.key.clone(), parts);
                variables.values.insert(entry.key, done.text);
            }
            Err(e) => {
                let message = interpolate_message(&e);
                found
                    .errors
                    .push(Diagnostic::new(path.as_str(), entry.key, message));
            }
        }
    }
    variables
}

/// The value of an env file entry after interpolation. A single-quoted
/// value is taken as written; a bare `KEY` has none.
fn entry_value(
    entry: &Entry,
    variables: &Variables,
) -> Result<Option<Interpolated>, InterpolateError> {
    match &entry.value {
        None => Ok(None),
        Some(value) if entry.literal => Ok(Some(Interpolated {
            text: value.clone(),
            unset: Vec::new(),
            used: Vec::new(),
        })),
        Some(value) => variables.expand(value).map(Some),
    }
}

fn unset_message(name: &str) -> String {
    format!(
        "variable {name} is not set and is replaced by an empty string; \
         set it in the environment or in the .env file"
    )
}

/// What went wrong, without any value: the name of a variable, and for a
/// required one the message of the file as it is written there.
fn interpolate_message(error: &InterpolateError) -> String {
    match error {
        InterpolateError::Required { name, message } if message.is_empty() => {
            format!("required variable {name} is not set")
        }
        other => other.to_string(),
    }
}

/// Interpolates every string value of the document. Keys are left alone,
/// and so is everything under a top-level `x-` key: that is where anchors
/// live, and what they hold is interpolated where it is used.
fn interpolate_document(walk: &Walk, document: Value, variables: &Variables) -> Value {
    let Value::Object(map) = document else {
        return document;
    };
    let map = map.into_iter().map(|(key, value)| {
        let value = if key.starts_with("x-") {
            value
        } else {
            interpolate_value(walk, &child_path("", &key), value, variables)
        };
        (key, value)
    });
    Value::Object(map.collect())
}

fn interpolate_value(walk: &Walk, path: &str, value: Value, variables: &Variables) -> Value {
    match value {
        Value::String(text) if text.contains('$') => match variables.expand(&text) {
            Ok(done) => {
                for name in &done.unset {
                    walk.warn(path, unset_message(name));
                }
                if !done.used.is_empty() {
                    variables.inserted(path, &text, &done);
                }
                Value::String(done.text)
            }
            Err(e) => {
                walk.error(path, interpolate_message(&e));
                Value::String(text)
            }
        },
        Value::Array(items) => {
            let items = items
                .into_iter()
                .enumerate()
                .map(|(i, item)| interpolate_value(walk, &format!("{path}[{i}]"), item, variables));
            Value::Array(items.collect())
        }
        Value::Object(map) => {
            let map = map.into_iter().map(|(key, value)| {
                let value = interpolate_value(walk, &child_path(path, &key), value, variables);
                (key, value)
            });
            Value::Object(map.collect())
        }
        other => other,
    }
}

/// A path with `/` between its parts, whatever the platform writes.
fn slashes(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Whether `path` starts with a Windows drive: `C:\data`, `c:/data`, `C:`.
fn has_drive(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes.get(2), None | Some(b'/' | b'\\'))
}

/// `path` without `.`, `..` and empty parts, as far as the text alone
/// tells. A relative path keeps the `..` it starts with.
fn normalize(path: &str) -> String {
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => match parts.last() {
                Some(last) if *last != ".." && !has_drive(last) => {
                    parts.pop();
                }
                Some(last) if has_drive(last) => {}
                None if rooted => {}
                _ => parts.push(".."),
            },
            part => parts.push(part),
        }
    }
    let joined = parts.join("/");
    if rooted { format!("/{joined}") } else { joined }
}

/// The directory of the compose file. Relative paths in the file are
/// resolved against it.
struct Dir(String);

impl Dir {
    fn of(file: &Path) -> Self {
        Dir(normalize(&file.parent().map(slashes).unwrap_or_default()))
    }

    /// `path` as the compose file means it, normalised.
    fn resolve(&self, path: &str) -> String {
        let path = path.replace('\\', "/");
        if path.starts_with('/') || has_drive(&path) || self.0.is_empty() {
            normalize(&path)
        } else {
            normalize(&format!("{}/{path}", self.0))
        }
    }

    /// Whether a resolved path is in the directory or below it.
    fn contains(&self, resolved: &str) -> bool {
        if self.0.is_empty() {
            return !resolved.starts_with('/')
                && !has_drive(resolved)
                && resolved != ".."
                && !resolved.starts_with("../");
        }
        resolved
            .strip_prefix(self.0.trim_end_matches('/'))
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    }
}

/// Whether a value is one a key has when it does nothing: `false`, null,
/// `""`, `[]` or `{}`.
fn is_noop(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(false) => true,
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(map) => map.is_empty(),
        Value::Bool(true) | Value::Number(_) => false,
    }
}

/// Takes `key` and returns its value unless that is a no-op.
fn present<'a>(map: &Cursor<'a>, key: &str) -> Option<Cursor<'a>> {
    map.get(key).filter(|at| !is_noop(at.value()))
}

/// Takes the `x-` keys of a map, which compose allows anywhere.
fn skip_extensions(map: &Cursor<'_>) {
    for key in map.keys() {
        if key.starts_with("x-") {
            map.ignore(key);
        }
    }
}

/// The map under `key`, with its `x-` keys taken.
fn sub<'a>(map: &Cursor<'a>, key: &str) -> Option<Cursor<'a>> {
    let at = map.get(key)?;
    skip_extensions(&at);
    Some(at)
}

/// `key` of a map that may be absent.
fn child<'a>(map: &Option<Cursor<'a>>, key: &str) -> Option<Cursor<'a>> {
    map.as_ref().and_then(|map| map.get(key))
}

/// A boolean, also as the string an interpolation leaves behind.
fn boolean(at: &Cursor<'_>) -> Option<bool> {
    match at.as_str() {
        Some("true") => Some(true),
        Some("false") => Some(false),
        _ => at.expect_bool(),
    }
}

/// A whole number, also as the string an interpolation leaves behind.
fn whole(at: &Cursor<'_>) -> Option<u64> {
    match at.as_str().map(|text| text.trim().parse()) {
        Some(Ok(number)) => Some(number),
        _ => at.expect_u64(),
    }
}

/// The items of a key that takes one value or a list of them.
fn one_or_more<'a>(at: &Cursor<'a>) -> Vec<Cursor<'a>> {
    if at.is_list() {
        at.items()
    } else {
        vec![at.clone()]
    }
}

/// A command as a list of words. The string form is split as a shell
/// splits it.
fn words(at: &Cursor<'_>) -> Option<Vec<String>> {
    if let Some(text) = at.as_str() {
        let split = shlex::split(text);
        if split.is_none() {
            at.error("cannot be split into words: a quote is not closed; write it as a list");
        }
        return split;
    }
    if !at.is_list() {
        at.error(format!("expected a string or a list, found {}", at.kind()));
        return None;
    }
    at.items().iter().map(Cursor::expect_text).collect()
}

fn cpu_millis(at: &Cursor<'_>) -> Option<u64> {
    // `cpus: 0.5` is a number in YAML, and here the amount is all that
    // matters, not how it was written.
    let text = match at.as_f64() {
        Some(number) if at.as_text().is_none() => number.to_string(),
        _ => at.expect_text()?,
    };
    units::docker_cpu_millis(&text)
        .map_err(|e| at.error(e.0))
        .ok()
}

fn memory_bytes(at: &Cursor<'_>) -> Option<u64> {
    let text = at.expect_text()?;
    units::docker_bytes(&text).map_err(|e| at.error(e.0)).ok()
}

fn seconds(at: &Cursor<'_>) -> Option<u32> {
    let text = at.expect_text()?;
    units::docker_duration_secs(&text)
        .map_err(|e| at.error(e.0))
        .ok()
}

/// The one amount several keys set. Zero means "no limit" to Docker, so
/// it counts as not set. Two different amounts are an error.
fn agreed(sources: &[Option<Cursor<'_>>], read: fn(&Cursor<'_>) -> Option<u64>) -> Option<u64> {
    let mut first: Option<(&Cursor<'_>, u64)> = None;
    for source in sources.iter().flatten() {
        let Some(amount) = read(source).filter(|amount| *amount > 0) else {
            continue;
        };
        match first {
            None => first = Some((source, amount)),
            Some((other, set)) if set != amount => source.error(format!(
                "conflicts with `{}`, which sets a different amount; keep one of them",
                other.path()
            )),
            Some(_) => {}
        }
    }
    first.map(|(_, amount)| amount)
}

/// Why a service or volume name is refused, and a name that would pass.
fn name_message(what: &str, name: &str) -> String {
    let rule = format!(
        "{what} name `{name}` is not allowed: a name is 1 to 63 lowercase letters, digits \
         and hyphens, and does not start or end with a hyphen"
    );
    let fixed = name.to_ascii_lowercase().replace(['_', '.'], "-");
    if fixed != name && validate_dns_label(&fixed).is_ok() {
        format!("{rule}; rename it, for example to `{fixed}`")
    } else {
        format!("{rule}; rename it")
    }
}

/// Whether a `networks` value names nothing but the default network, as
/// the output of `docker compose config` does.
fn is_default_network(value: &Value, project: Option<&str>) -> bool {
    match value {
        Value::Array(items) => items.len() == 1 && items[0] == "default",
        Value::Object(map) if map.len() == 1 => match map.get("default") {
            Some(Value::Object(settings)) => settings.iter().all(|(key, value)| {
                key == "name" && project.is_some_and(|p| *value == format!("{p}_default"))
            }),
            Some(Value::Null) => true,
            _ => false,
        },
        _ => false,
    }
}

/// `node.hostname == NAME`, with or without the spaces.
fn pinned_node(constraint: &str) -> Option<&str> {
    let node = constraint
        .trim()
        .strip_prefix("node.hostname")?
        .trim_start()
        .strip_prefix("==")?
        .trim();
    (!node.is_empty() && !node.contains(char::is_whitespace)).then_some(node)
}

fn parse_protocol(text: &str) -> Result<Protocol, String> {
    match text.to_ascii_lowercase().as_str() {
        "tcp" => Ok(Protocol::Tcp),
        "udp" => Ok(Protocol::Udp),
        "sctp" => Err("sctp is not supported: a port is `tcp` or `udp`".to_string()),
        _ => Err(format!(
            "unknown protocol `{text}`: a port is `tcp` or `udp`"
        )),
    }
}

/// `80` or `8000-8010`: the first and the last port.
fn parse_port_range(text: &str) -> Result<(u16, u16), String> {
    let port = |text: &str| match text.parse::<u16>() {
        Ok(port) if port > 0 => Ok(port),
        _ => Err(format!("`{text}` is not a port: expected 1 to 65535")),
    };
    match text.split_once('-') {
        None => port(text).map(|p| (p, p)),
        Some((first, last)) => {
            let (first, last) = (port(first)?, port(last)?);
            if first > last {
                return Err(format!("the port range `{text}` ends before it starts"));
            }
            Ok((first, last))
        }
    }
}

/// One entry of `ports` in the short form.
#[derive(Debug, PartialEq, Eq)]
struct ShortPort {
    host_ip: Option<String>,
    /// The host port or range, as written.
    published: Option<String>,
    /// The first and the last container port.
    target: (u16, u16),
    protocol: Protocol,
}

/// Parses `[ip:][published:]target[/protocol]`. An IPv6 address stands in
/// brackets.
fn parse_short_port(text: &str) -> Result<ShortPort, String> {
    const FORM: &str = "expected `[ip:][published:]target[/protocol]`";
    let (ports, protocol) = match text.rsplit_once('/') {
        Some((ports, protocol)) => (ports, parse_protocol(protocol)?),
        None => (text, Protocol::Tcp),
    };
    let (host_ip, ports) = match ports.strip_prefix('[') {
        Some(rest) => {
            let (ip, ports) = rest
                .split_once("]:")
                .ok_or_else(|| format!("the `[` of the address is not closed; {FORM}"))?;
            (Some(ip), ports)
        }
        None => match ports.matches(':').count() {
            0 | 1 => (None, ports),
            2 => {
                let (ip, ports) = ports.split_once(':').unwrap_or_default();
                (Some(ip), ports)
            }
            _ => {
                return Err(format!(
                    "too many colons; {FORM}, with an IPv6 address in brackets"
                ));
            }
        },
    };
    if host_ip.is_some_and(str::is_empty) {
        return Err(format!("the host address is empty; {FORM}"));
    }
    let (published, target) = match ports.split_once(':') {
        Some((published, target)) => (Some(published).filter(|p| !p.is_empty()), target),
        None => (None, ports),
    };
    if host_ip.is_some() && !ports.contains(':') {
        return Err(format!("an address needs a port after it; {FORM}"));
    }
    if let Some(published) = published {
        parse_port_range(published)?;
    }
    Ok(ShortPort {
        host_ip: host_ip.map(str::to_string),
        published: published.map(str::to_string),
        target: parse_port_range(target)?,
        protocol,
    })
}

/// One entry of `volumes` in the short form.
#[derive(Debug, PartialEq, Eq)]
struct ShortVolume<'a> {
    source: Option<&'a str>,
    target: &'a str,
    /// The comma-separated parts of the mode: `ro`, `z`.
    options: Vec<&'a str>,
}

const WINDOWS_PATH: &str =
    "Windows paths are not supported: nodes run Linux; write the path as it is on the node";

/// Parses `[SOURCE:]TARGET[:MODE]`.
fn parse_short_volume(text: &str) -> Result<ShortVolume<'_>, String> {
    if has_drive(text) || text.contains('\\') {
        return Err(WINDOWS_PATH.to_string());
    }
    let parts: Vec<&str> = text.split(':').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return Err("a part is empty; expected `[SOURCE:]TARGET[:MODE]`".to_string());
    }
    let (source, target, mode) = match parts.as_slice() {
        [target] => (None, *target, ""),
        [source, target] => (Some(*source), *target, ""),
        [source, target, mode] => (Some(*source), *target, *mode),
        _ => return Err("too many colons; expected `[SOURCE:]TARGET[:MODE]`".to_string()),
    };
    Ok(ShortVolume {
        source,
        target,
        options: mode.split(',').filter(|o| !o.is_empty()).collect(),
    })
}

/// What a mount takes its data from, before it is a volume of the pod.
enum Source<'s> {
    /// A volume nobody named: scratch space on disk.
    Anonymous,
    /// A path on the node, and whether to create it.
    Path(&'s str, bool),
    /// A volume declared at the top level.
    Named(&'s str),
    /// Scratch space in memory, with its size.
    Tmpfs(Option<u64>),
}

/// The volumes and mounts of one service as they are collected.
#[derive(Default)]
struct Mounts {
    volumes: BTreeMap<String, VolumeSource>,
    mounts: Vec<MountSpec>,
    /// How many `v<n>` names were given out.
    generated: usize,
    /// Where each volume and mount comes from: its path in the spec
    /// (`volumes.v0`, `containers[0].mounts[0]`) and in the file.
    origins: Vec<(String, String)>,
}

struct Converter<'i> {
    input: &'i ComposeInput,
    files: &'i dyn Files,
    variables: &'i Variables,
    dir: &'i Dir,
    /// The named volumes of the top-level `volumes`.
    declared: BTreeSet<String>,
    /// The services left out by their profiles.
    skipped: Vec<String>,
    /// Paths of the converted workloads and the paths of the file they
    /// were made from. See [`invalid`].
    origins: RefCell<BTreeMap<String, String>>,
}

impl Converter<'_> {
    /// The top level: everything but `services` first, since the services
    /// refer to the volumes.
    fn top(&mut self, root: &Cursor<'_>) -> BTreeMap<String, WorkloadSpec> {
        let mut workloads = BTreeMap::new();
        if let Some(faber) = root.get("x-faber") {
            faber.error("x-faber belongs on a service: move it to `services.<name>.x-faber`");
        }
        skip_extensions(root);
        root.ignore("version");
        // The project comes from the deploy, not from the file.
        let project = root.get("name").and_then(|name| name.as_str());

        if let Some(include) = present(root, "include") {
            include.error("not supported: run `docker compose config` and deploy its output");
        }
        if let Some(networks) = present(root, "networks")
            && !is_default_network(networks.value(), project)
        {
            networks.warn(NETWORKS_SKIPPED);
        }
        self.declare_volumes(root, project);

        let Some(services) = root.get("services") else {
            root.error("`services` is missing: a compose file needs at least one service");
            return workloads;
        };
        let entries = services.entries();
        if entries.is_empty() && services.is_map() {
            services.error("holds no service: a compose file needs at least one");
        }
        for (key, what) in [("configs", "config"), ("secrets", "secret")] {
            let used = entries
                .iter()
                .any(|(_, service)| service.value().get(key).is_some_and(|v| !is_noop(v)));
            // A service that uses one is an error there, which says more.
            if let Some(unused) = present(root, key)
                && !used
            {
                unused.warn(format!("skipped: no service uses a {what}"));
            }
        }

        for (name, service) in &entries {
            if let Some(profiles) = self.inactive_profiles(service) {
                service.warn(format!(
                    "skipped: none of its profiles ({}) is active; pass `--profile {}` to deploy it",
                    profiles.join(", "),
                    profiles[0]
                ));
                self.skipped.push(name.to_string());
                continue;
            }
            if validate_dns_label(name).is_err() {
                service.error(name_message("service", name));
            }
            if !service.is_map() && !service.is_null() {
                service.error(format!("expected a map, found {}", service.kind()));
                continue;
            }
            workloads.insert(name.to_string(), self.service(name, service));
        }
        if !entries.is_empty() && self.skipped.len() == entries.len() {
            services.warn("every service was skipped by its profiles, so nothing is deployed");
        }
        workloads
    }

    /// The top-level `volumes`: each entry declares a named volume.
    fn declare_volumes(&mut self, root: &Cursor<'_>, project: Option<&str>) {
        let Some(volumes) = root.get("volumes") else {
            return;
        };
        for (name, volume) in volumes.entries() {
            if validate_dns_label(name).is_err() {
                volume.error(name_message("volume", name));
            }
            self.declared.insert(name.to_string());
            skip_extensions(&volume);
            volume.ignore("labels");
            if let Some(driver) = present(&volume, "driver")
                && driver.as_str() != Some("local")
            {
                driver.error("not supported: a volume is a directory on its node (`local`)");
            }
            if let Some(options) = present(&volume, "driver_opts") {
                options
                    .error("not supported: a volume is a directory on its node, without options");
            }
            if let Some(external) = present(&volume, "external") {
                external.error(
                    "not supported: faber creates the volume itself, per project and environment",
                );
            }
            // `docker compose config` writes the name compose would pick
            // anyway, `<project>_<volume>`. Any other name asks for one
            // volume across projects, which does not exist here.
            if let Some(custom) = present(&volume, "name")
                && !project.is_some_and(|p| custom.as_str() == Some(&format!("{p}_{name}")))
            {
                custom.error(
                    "not supported: a volume is known by its key, per project and environment; \
                     remove `name`",
                );
            }
        }
    }

    /// The profiles of a service when it has some and none is active.
    fn inactive_profiles<'a>(&self, service: &Cursor<'a>) -> Option<Vec<&'a str>> {
        let profiles = service.value().get("profiles")?.as_array()?;
        // Anything but strings is left to the service to report.
        let profiles: Vec<&str> = profiles.iter().map(Value::as_str).collect::<Option<_>>()?;
        let active = profiles.is_empty()
            || self
                .input
                .profiles
                .iter()
                .any(|p| p == "*" || profiles.contains(&p.as_str()));
        (!active).then_some(profiles)
    }

    /// One service as a workload of one container.
    fn service(&self, name: &str, service: &Cursor<'_>) -> WorkloadSpec {
        for key in service.keys() {
            if key.starts_with("x-") && key != "x-faber" {
                service.ignore(key);
            }
        }
        for key in IGNORED {
            service.ignore(key);
        }
        for (key, message) in REFUSED {
            if let Some(refused) = present(service, key) {
                refused.error(*message);
            }
        }
        for (key, message) in SKIPPED {
            if let Some(skipped) = present(service, key) {
                skipped.warn(*message);
            }
        }
        if let Some(networks) = present(service, "networks")
            && !is_default_network(networks.value(), None)
        {
            networks.warn(NETWORKS_SKIPPED);
        }
        if let Some(profiles) = service.get("profiles") {
            for profile in profiles.items() {
                profile.expect_str();
            }
        }
        self.isolation(service);

        let origin = |spec: &str, at: &Cursor<'_>| {
            let spec = format!("{name}.{spec}");
            self.origins
                .borrow_mut()
                .insert(spec, at.path().to_string());
        };
        let image = match service.get("image") {
            Some(image) => {
                origin("containers[0].image", &image);
                let text = image.expect_str().unwrap_or_default();
                if image.as_str() == Some("") {
                    image.error("must not be empty");
                }
                text
            }
            None => {
                service.error("`image` is missing: faber runs images and does not build them");
                ""
            }
        };
        let mut container = ContainerSpec::new(name, image);
        self.command(service, &mut container);
        container.env = self.environment(service);
        if let Some(dir) = present(service, "working_dir") {
            origin("containers[0].working_dir", &dir);
            // Written one way, like a mount path. A relative one is left
            // for the model to refuse.
            container.working_dir = dir.expect_str().map(|dir| match dir.starts_with('/') {
                true => normalize(dir),
                false => dir.to_string(),
            });
        }
        if let Some(user) = service.get("user") {
            origin("containers[0].user", &user);
        }
        container.user = self.user(service);

        let deploy = sub(service, "deploy");
        if let Some(deploy) = &deploy {
            deploy.ignore("labels");
            for key in ["update_config", "rollback_config"] {
                if let Some(rollout) = present(deploy, key) {
                    rollout.warn("skipped: how an update rolls out cannot be configured");
                }
            }
            if let Some(mode) = present(deploy, "endpoint_mode") {
                mode.warn("skipped: a workload is always reached through DNS");
            }
        }
        container.resources = self.resources(service, &deploy);
        container.readiness = self.healthcheck(service);
        if let Some(check) = service.get("healthcheck") {
            origin("containers[0].readiness", &check);
        }
        self.ports(name, service, &mut container);
        let mut mounts = Mounts::default();
        self.volumes(service, &mut mounts);
        self.tmpfs(service, &mut mounts);
        container.mounts = mounts.mounts;
        for (spec, at) in mounts.origins {
            self.origins
                .borrow_mut()
                .insert(format!("{name}.{spec}"), at);
        }

        if let Some(capabilities) = service.get("cap_add") {
            for item in capabilities.items() {
                let Some(text) = item.expect_str() else {
                    continue;
                };
                let upper = text.to_ascii_uppercase();
                let capability = upper.strip_prefix("CAP_").unwrap_or(&upper);
                if capability == "ALL" {
                    item.error("not supported: list the capabilities the service needs one by one");
                } else if !CAPABILITIES.contains(&capability) {
                    item.error(format!("`{text}` is not a Linux capability"));
                } else if !ALLOWED_CAPABILITIES.contains(&capability) {
                    item.error(format!(
                        "capability `{capability}` is not allowed: a container may add {} \
                         and nothing else",
                        ALLOWED_CAPABILITIES.join(", ")
                    ));
                } else if !container.capabilities.iter().any(|c| c == capability) {
                    let index = container.capabilities.len();
                    origin(&format!("containers[0].capabilities[{index}]"), &item);
                    container.capabilities.push(capability.to_string());
                }
            }
        }
        if let Some(read_only) = service.get("read_only") {
            container.read_only_root_fs = boolean(&read_only).unwrap_or(false);
        }

        let mut expose = Exposure::default();
        if let Some(faber) = service.get("x-faber")
            && let Some(at) = faber.get("expose")
        {
            match at.expect_str() {
                Some("cluster") => expose = Exposure::Cluster,
                Some("environment") | None => {}
                Some(_) => at.error("expected `cluster` or `environment`"),
            }
        }
        for (spec, at) in [
            (
                "scale",
                child(&deploy, "replicas").or_else(|| service.get("scale")),
            ),
            ("stop_grace_secs", service.get("stop_grace_period")),
            (
                "placement.node",
                deploy.as_ref().and_then(|deploy| deploy.get("placement")),
            ),
        ] {
            if let Some(at) = at {
                origin(spec, &at);
            }
        }
        WorkloadSpec {
            scale: self.scale(service, &deploy),
            restart: self.restart(service, &deploy),
            expose,
            placement: self.placement(&deploy),
            stop_grace_secs: present(service, "stop_grace_period").and_then(|at| seconds(&at)),
            volumes: mounts.volumes,
            containers: vec![container],
        }
    }

    /// `security_opt` and `network_mode`: fine where they ask for what
    /// every container gets, refused where they ask for less isolation.
    fn isolation(&self, service: &Cursor<'_>) {
        if let Some(options) = service.get("security_opt") {
            for option in options.items() {
                let fine = [
                    "no-new-privileges",
                    "no-new-privileges:true",
                    "no-new-privileges=true",
                ];
                if option.expect_str().is_some_and(|o| !fine.contains(&o)) {
                    option.error(
                        "not supported: the security settings of a container are fixed, \
                         with no-new-privileges on",
                    );
                }
            }
        }
        if let Some(mode) = present(service, "network_mode")
            && mode.expect_str().is_some_and(|m| m != "bridge")
        {
            mode.error("not supported: a workload always has its own network namespace");
        }
    }

    /// `entrypoint` becomes `command` and `command` becomes `args`. Both
    /// empty would mean "as the image says", so an empty list that clears
    /// one half of the image's own command cannot always be written down.
    fn command(&self, service: &Cursor<'_>, container: &mut ContainerSpec) {
        let read = |key: &str| {
            let at = service.get(key)?;
            let words = words(&at)?;
            Some((at, words))
        };
        match (read("entrypoint"), read("command")) {
            (Some((at, entrypoint)), command) if entrypoint.is_empty() => {
                match command.filter(|(_, words)| !words.is_empty()) {
                    Some((_, words)) => container.command = words,
                    None => at.error(
                        "an empty entrypoint clears the entrypoint of the image, which only \
                         works together with a `command`; set `command`, or remove `entrypoint`",
                    ),
                }
            }
            (Some((_, entrypoint)), command) => {
                container.command = entrypoint;
                container.args = command.map(|(_, words)| words).unwrap_or_default();
            }
            (None, Some((at, words))) if words.is_empty() => at.error(
                "an empty command clears the arguments of the image, which only works \
                 together with an `entrypoint`; set `entrypoint`, or remove `command`",
            ),
            (None, Some((_, words))) => container.args = words,
            (None, None) => {}
        }
    }

    /// `env_file` in order, then `environment` over it.
    fn environment(&self, service: &Cursor<'_>) -> BTreeMap<String, String> {
        let mut env = BTreeMap::new();
        if let Some(files) = service.get("env_file") {
            for item in one_or_more(&files) {
                self.env_file(&item, &mut env);
            }
        }
        let Some(environment) = service.get("environment") else {
            return env;
        };
        if environment.is_list() {
            for item in environment.items() {
                let Some(entry) = item.expect_str() else {
                    continue;
                };
                let (key, value) = match entry.split_once('=') {
                    Some((key, value)) => (key, Some(value.to_string())),
                    None => (entry, None),
                };
                self.set_env(&item, key, value, &mut env);
            }
        } else {
            for (key, at) in environment.entries() {
                let value = if at.is_null() {
                    None
                } else {
                    // A float has lost how it was written: `1.20` is 1.2.
                    let Some(text) = at.expect_text() else {
                        continue;
                    };
                    Some(text)
                };
                self.set_env(&at, key, value, &mut env);
            }
        }
        env
    }

    /// Sets one variable. Without a value it takes the caller's, and
    /// without that it is left out, as compose does.
    fn set_env(
        &self,
        at: &Cursor<'_>,
        key: &str,
        value: Option<String>,
        env: &mut BTreeMap<String, String>,
    ) {
        if key.is_empty() {
            at.error("the variable has no name");
            return;
        }
        match value.or_else(|| self.variables.get(key)) {
            Some(value) => {
                env.insert(key.to_string(), value);
            }
            None => {
                env.remove(key);
                at.warn(format!(
                    "variable {key} has no value here and is not set in the environment, \
                     so the container does not get it"
                ));
            }
        }
    }

    /// One entry of `env_file`: a path, or `{path, required}`.
    fn env_file(&self, item: &Cursor<'_>, env: &mut BTreeMap<String, String>) {
        let (at, required) = if item.is_map() {
            skip_extensions(item);
            if let Some(format) = present(item, "format") {
                format.error("not supported: an env file is read in the default format only");
            }
            let required = item.get("required").and_then(|r| boolean(&r));
            let Some(path) = item.get("path") else {
                item.error("`path` is missing");
                return;
            };
            (path, required.unwrap_or(true))
        } else {
            (item.clone(), true)
        };
        let Some(path) = at.expect_str() else {
            return;
        };
        let resolved = self.dir.resolve(path);
        // Before anything is read: the file is on the caller's machine,
        // and what it holds would end up in the cluster.
        let outside = !self.dir.contains(&resolved);
        if outside && !self.input.allow_external_env_files {
            at.error(outside_message(&resolved));
            return;
        }
        let text = match self.files.read(Path::new(&resolved)) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound && !required => return,
            Err(e) => {
                at.error(format!("cannot read the env file `{resolved}`: {e}"));
                return;
            }
        };
        if outside {
            at.warn(format!(
                "reads `{resolved}`, which is outside the directory of the compose file"
            ));
        }
        let entries = match envfile::parse_entries(&text) {
            Ok(entries) => entries,
            Err(e) => {
                at.error(format!("`{resolved}`: {e}"));
                return;
            }
        };
        for entry in entries {
            let key = &entry.key;
            let value = match entry_value(&entry, self.variables) {
                Ok(Some(value)) => {
                    for name in &value.unset {
                        at.warn(format!("`{resolved}`: {key}: {}", unset_message(name)));
                    }
                    Some(value.text)
                }
                Ok(None) => self.variables.get(key),
                Err(e) => {
                    at.error(format!("`{resolved}`: {key}: {}", interpolate_message(&e)));
                    continue;
                }
            };
            match value {
                Some(value) => {
                    env.insert(entry.key, value);
                }
                None => {
                    env.remove(key);
                    at.warn(format!(
                        "`{resolved}`: variable {key} has no value there and is not set in \
                         the environment, so the container does not get it"
                    ));
                }
            }
        }
    }

    fn user(&self, service: &Cursor<'_>) -> Option<String> {
        let at = present(service, "user")?;
        let user = at.expect_text()?;
        let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
        if let Some((uid, gid)) = user.split_once(':')
            && !(digits(uid) && digits(gid))
        {
            at.error(
                "a user or group name next to a `:` is not supported: write `uid:gid` \
                 in numbers, or the user alone",
            );
            return None;
        }
        Some(user)
    }

    /// `scale`, `deploy.replicas` and `deploy.mode`.
    fn scale(&self, service: &Cursor<'_>, deploy: &Option<Cursor<'_>>) -> Scale {
        let count = |at: &Cursor<'_>| {
            let count = u32::try_from(whole(at)?);
            if count.is_err() {
                at.error("is too large");
            }
            count.ok()
        };
        let scale = service.get("scale");
        let replicas = child(deploy, "replicas");
        let mut set = scale.as_ref().and_then(count);
        if let Some(replicas) = &replicas
            && let Some(count) = count(replicas)
        {
            if set.is_some_and(|scale| scale != count) {
                replicas.error("conflicts with `scale`, which sets a different count; keep one");
            }
            set = Some(count);
        }
        if let Some(mode) = child(deploy, "mode") {
            match mode.expect_str() {
                Some("replicated") | None => {}
                Some("global") => {
                    if let Some(count) = scale.or(replicas) {
                        count.error(
                            "cannot be set together with `deploy.mode: global`, which runs one \
                             per node",
                        );
                    }
                    return Scale::PerNode;
                }
                Some("replicated-job" | "global-job") => {
                    mode.error("not supported: faber runs services, not jobs that end");
                }
                Some(_) => mode.error("expected `replicated` or `global`"),
            }
        }
        set.map_or_else(Scale::default, Scale::Replicas)
    }

    /// `restart` and `deploy.restart_policy`. Docker restarts nothing
    /// unless told to, so that is what an absent key means here too.
    fn restart(&self, service: &Cursor<'_>, deploy: &Option<Cursor<'_>>) -> RestartPolicy {
        let mut set = None;
        if let Some(at) = present(service, "restart") {
            set = match at.expect_text().as_deref() {
                None => None,
                Some("no") => Some(RestartPolicy::Never),
                Some("always" | "unless-stopped") => Some(RestartPolicy::Always),
                Some("on-failure") => Some(RestartPolicy::OnFailure),
                Some(other)
                    if other
                        .strip_prefix("on-failure:")
                        .is_some_and(|n| n.parse::<u32>().is_ok()) =>
                {
                    at.warn(
                        "the retry count is not kept: the container is restarted after every \
                         failure",
                    );
                    Some(RestartPolicy::OnFailure)
                }
                Some(_) => {
                    at.error("expected `no`, `always`, `unless-stopped` or `on-failure`");
                    None
                }
            };
        }
        let policy = deploy
            .as_ref()
            .and_then(|deploy| sub(deploy, "restart_policy"));
        if let Some(policy) = &policy {
            for key in ["delay", "max_attempts", "window"] {
                if let Some(timing) = present(policy, key) {
                    timing.warn(
                        "skipped: when and how often a container is restarted cannot be \
                         configured",
                    );
                }
            }
        }
        // A policy without a condition is compose's `any`.
        if set.is_none()
            && let Some(policy) = &policy
            && !is_noop(policy.value())
            && !policy.has("condition")
        {
            set = Some(RestartPolicy::Always);
        }
        if let Some(at) = child(&policy, "condition") {
            let condition = match at.expect_str() {
                None => None,
                Some("none") => Some(RestartPolicy::Never),
                Some("on-failure") => Some(RestartPolicy::OnFailure),
                Some("any") => Some(RestartPolicy::Always),
                Some(_) => {
                    at.error("expected `none`, `on-failure` or `any`");
                    None
                }
            };
            match (set, condition) {
                (Some(restart), Some(condition)) if restart != condition => {
                    at.error("conflicts with `restart`, which asks for something else; keep one");
                }
                (None, Some(_)) => set = condition,
                _ => {}
            }
        }
        set.unwrap_or(RestartPolicy::Never)
    }

    fn placement(&self, deploy: &Option<Cursor<'_>>) -> Placement {
        const FORM: &str = "not supported: the one constraint there is pins the service \
                            to a node, `node.hostname == NAME`";
        let mut placement = Placement::default();
        let Some(at) = deploy.as_ref().and_then(|deploy| sub(deploy, "placement")) else {
            return placement;
        };
        for key in ["preferences", "max_replicas_per_node"] {
            if let Some(hint) = present(&at, key) {
                hint.warn("skipped: faber decides where replicas run");
            }
        }
        if let Some(constraints) = present(&at, "constraints") {
            let items = constraints.items();
            match items.as_slice() {
                [one] => match one.expect_str().map(pinned_node) {
                    Some(Some(node)) => placement.node = Some(node.to_string()),
                    Some(None) => one.error(FORM),
                    None => {}
                },
                [] => {}
                _ => constraints.error(FORM),
            }
        }
        placement
    }

    /// CPU and memory, which compose can set in two places each.
    fn resources(&self, service: &Cursor<'_>, deploy: &Option<Cursor<'_>>) -> Resources {
        let resources = deploy.as_ref().and_then(|deploy| sub(deploy, "resources"));
        let limits = resources.as_ref().and_then(|r| sub(r, "limits"));
        let reservations = resources.as_ref().and_then(|r| sub(r, "reservations"));

        // 0 and -1 both mean "no limit", which is what a container has.
        for pids in [child(&limits, "pids"), service.get("pids_limit")] {
            if let Some(pids) = pids
                && !matches!(whole_or_negative(&pids), Some(0 | -1))
            {
                pids.error(PIDS);
            }
        }
        if let Some(reservations) = &reservations {
            for key in ["devices", "generic_resources"] {
                if let Some(device) = present(reservations, key) {
                    device.error("not supported: containers get no host devices");
                }
            }
        }
        Resources {
            limits: ResourceAmounts {
                cpu_millis: agreed(&[service.get("cpus"), child(&limits, "cpus")], cpu_millis),
                memory_bytes: agreed(
                    &[service.get("mem_limit"), child(&limits, "memory")],
                    memory_bytes,
                ),
            },
            requests: ResourceAmounts {
                cpu_millis: agreed(&[child(&reservations, "cpus")], cpu_millis),
                memory_bytes: agreed(
                    &[
                        service.get("mem_reservation"),
                        child(&reservations, "memory"),
                    ],
                    memory_bytes,
                ),
            },
        }
    }

    /// `healthcheck` as a readiness probe: it never restarts the
    /// container, as in plain compose.
    fn healthcheck(&self, service: &Cursor<'_>) -> Option<Probe> {
        let check = present(service, "healthcheck")?;
        skip_extensions(&check);
        let take_rest = || {
            for key in check.keys() {
                check.ignore(key);
            }
        };
        if check.get("disable").and_then(|at| boolean(&at)) == Some(true) {
            take_rest();
            return None;
        }
        let Some(test) = check.get("test") else {
            check.warn("skipped: it has no `test`, and the health check of the image is not used");
            take_rest();
            return None;
        };
        const FORM: &str = "expected a string, `[CMD, program, argument...]`, \
                            `[CMD-SHELL, command]` or `[NONE]`";
        let command = if let Some(shell) = test.as_str() {
            Some(shell_command(shell))
        } else if !test.is_list() {
            test.error(FORM);
            None
        } else {
            let items = test.items();
            let words: Option<Vec<String>> = items.iter().map(Cursor::expect_text).collect();
            match words.as_deref().map(<[String]>::split_first) {
                Some(Some((kind, []))) if kind == "NONE" => {
                    take_rest();
                    return None;
                }
                Some(Some((kind, [shell]))) if kind == "CMD-SHELL" => Some(shell_command(shell)),
                Some(Some((kind, command))) if kind == "CMD" && !command.is_empty() => {
                    Some(command.to_vec())
                }
                Some(_) => {
                    test.error(FORM);
                    None
                }
                // An item that is no text: `expect_text` has said so.
                None => None,
            }
        };

        // The timings are read whatever became of `test`, so that each
        // key is taken and each mistake is reported once.
        let mut probe = Probe {
            action: ProbeAction::Exec {
                command: command.clone().unwrap_or_default(),
            },
            initial_delay_secs: 0,
            period_secs: HEALTH_INTERVAL_SECS,
            timeout_secs: HEALTH_TIMEOUT_SECS,
            success_threshold: 1,
            failure_threshold: HEALTH_RETRIES,
        };
        // Zero is Docker's "not set", so its default stays.
        let timing = |key: &str| {
            let at = check.get(key)?;
            let secs = seconds(&at).filter(|secs| *secs > 0)?;
            Some((at, secs))
        };
        if let Some((_, interval)) = timing("interval") {
            probe.period_secs = interval;
        }
        if let Some((_, timeout)) = timing("timeout") {
            probe.timeout_secs = timeout;
        }
        if let Some(at) = check.get("retries")
            && let Some(retries) = whole(&at)
        {
            match u32::try_from(retries) {
                // Docker reads 0 as "the default".
                Ok(0) => {}
                Ok(retries) => probe.failure_threshold = retries,
                Err(_) => at.error("is too large"),
            }
        }
        if let Some((at, delay)) = timing("start_period") {
            probe.initial_delay_secs = delay;
            at.warn(
                "approximated: no check runs for this long after the start, where Docker \
                 checks and does not count the failures",
            );
        }
        if let Some(at) = present(&check, "start_interval") {
            at.warn("skipped: checks run every `interval` from the start");
        }
        command.map(|_| probe)
    }

    /// `ports` and `expose`: both only declare what the container listens
    /// on.
    fn ports(&self, name: &str, service: &Cursor<'_>, container: &mut ContainerSpec) {
        let unpublished = || {
            format!(
                "the port is not published on the host: faber has no ingress yet; use \
                 `--expose {name}` to make the service reachable cluster-wide"
            )
        };
        let mut declare = |name: Option<&str>, (first, last): (u16, u16), protocol: Protocol| {
            for port in first..=last {
                let known = |p: &PortSpec| p.port == port && p.protocol == protocol;
                if !container.ports.iter().any(known) {
                    container.ports.push(PortSpec {
                        name: name.map(str::to_string),
                        port,
                        protocol,
                    });
                }
            }
        };
        let ports = service.get("ports").map(|at| at.items());
        for item in ports.unwrap_or_default() {
            if !item.is_map() {
                let Some(text) = item.expect_text() else {
                    continue;
                };
                match parse_short_port(&text) {
                    Ok(port) => {
                        if port.published.is_some() || port.host_ip.is_some() {
                            item.warn(unpublished());
                        }
                        declare(None, port.target, port.protocol);
                    }
                    Err(message) => item.error(message),
                }
                continue;
            }
            skip_extensions(&item);
            item.ignore("app_protocol");
            let read = |key: &str, parse: fn(&str) -> Result<(u16, u16), String>| {
                let at = present(&item, key)?;
                parse(&at.expect_text()?)
                    .map_err(|message| at.error(message))
                    .ok()
            };
            let published = read("published", parse_port_range);
            let single = |text: &str| match parse_port_range(text)? {
                (first, last) if first == last => Ok((first, last)),
                _ => Err("expected one port, not a range".to_string()),
            };
            let target = read("target", single);
            let host_ip = present(&item, "host_ip");
            if let Some(mode) = present(&item, "mode")
                && mode
                    .expect_str()
                    .is_some_and(|m| m != "ingress" && m != "host")
            {
                mode.error("expected `ingress` or `host`");
            }
            let protocol = match present(&item, "protocol") {
                None => Some(Protocol::Tcp),
                Some(at) => at.expect_str().and_then(|text| {
                    parse_protocol(text)
                        .map_err(|message| at.error(message))
                        .ok()
                }),
            };
            let port_name = present(&item, "name").and_then(|at| at.expect_str());
            if published.is_some() || host_ip.is_some() {
                item.warn(unpublished());
            }
            match (target, protocol) {
                (Some(target), Some(protocol)) => declare(port_name, target, protocol),
                (None, _) if !item.has("target") => item.error("`target` is missing"),
                _ => {}
            }
        }
        let exposed = service.get("expose").map(|at| at.items());
        for item in exposed.unwrap_or_default() {
            let Some(text) = item.expect_text() else {
                continue;
            };
            match parse_short_port(&text) {
                Ok(port) if port.published.is_none() && port.host_ip.is_none() => {
                    declare(None, port.target, port.protocol);
                }
                Ok(_) => item.error("expected `port` or `port/protocol`"),
                Err(message) => item.error(message),
            }
        }
    }

    fn volumes(&self, service: &Cursor<'_>, out: &mut Mounts) {
        let items = service.get("volumes").map(|at| at.items());
        for item in items.unwrap_or_default() {
            if item.is_map() {
                self.long_volume(&item, out);
            } else if let Some(text) = item.expect_str() {
                self.short_volume(&item, text, out);
            }
        }
    }

    fn short_volume(&self, at: &Cursor<'_>, text: &str, out: &mut Mounts) {
        let volume = match parse_short_volume(text) {
            Ok(volume) => volume,
            Err(message) => {
                at.error(message);
                return;
            }
        };
        let mut read_only = false;
        for option in &volume.options {
            match *option {
                "ro" => read_only = true,
                "rw" => read_only = false,
                "z" | "Z" => at.warn(SELINUX),
                "nocopy" => at.warn(NOCOPY),
                // Hints to Docker Desktop, and the default propagation.
                "cached" | "delegated" | "consistent" | "rprivate" => {}
                other => {
                    at.error(format!(
                        "mount option `{other}` is not supported; a mount is `ro` or `rw`"
                    ));
                    return;
                }
            }
        }
        let source = match volume.source {
            None => Source::Anonymous,
            Some(path) if path.starts_with(['/', '~', '.']) => Source::Path(path, true),
            Some(name) => Source::Named(name),
        };
        if let Some(key) = self.volume(at, source, out) {
            self.mount(at, key, volume.target, read_only, None, out);
        }
    }

    fn long_volume(&self, item: &Cursor<'_>, out: &mut Mounts) {
        skip_extensions(item);
        item.ignore("consistency");
        let kind = item.get("type");
        let source = item.get("source");
        let target = item.get("target");
        let read_only = item.get("read_only").and_then(|at| boolean(&at));
        let bind = sub(item, "bind");
        let volume = sub(item, "volume");
        let tmpfs = sub(item, "tmpfs");

        let create = child(&bind, "create_host_path").and_then(|at| boolean(&at));
        if let Some(propagation) = child(&bind, "propagation")
            && propagation.expect_str().is_some_and(|p| p != "rprivate")
        {
            propagation.error("not supported: mounts do not propagate (`rprivate`)");
        }
        if let Some(selinux) = bind.as_ref().and_then(|bind| present(bind, "selinux")) {
            selinux.warn(SELINUX);
        }
        if let Some(nocopy) = child(&volume, "nocopy")
            && boolean(&nocopy) == Some(true)
        {
            nocopy.warn(NOCOPY);
        }
        let sub_path = child(&volume, "subpath")
            .and_then(|at| at.expect_str())
            .filter(|path| !path.is_empty());
        let size = child(&tmpfs, "size")
            .and_then(|at| memory_bytes(&at))
            .filter(|size| *size > 0);
        if let Some(mode) = tmpfs.as_ref().and_then(|tmpfs| present(tmpfs, "mode")) {
            mode.error("not supported: the mode of a tmpfs mount cannot be set");
        }

        let Some(kind) = kind else {
            item.error("`type` is missing: `bind`, `volume` or `tmpfs`");
            return;
        };
        let Some(target) = target else {
            item.error("`target` is missing");
            return;
        };
        let (Some(kind_text), Some(target)) = (kind.expect_str(), target.expect_str()) else {
            return;
        };
        let source_text = match &source {
            Some(source) => match source.expect_str() {
                Some(text) => Some(text),
                None => return,
            },
            None => None,
        };
        let from = match (kind_text, source_text) {
            ("bind", Some(path)) => Source::Path(path, create.unwrap_or(true)),
            ("bind", None) => {
                item.error("`source` is missing: a bind mount needs a path on the node");
                return;
            }
            ("volume", Some(name)) => Source::Named(name),
            ("volume", None) => Source::Anonymous,
            ("tmpfs", _) => Source::Tmpfs(size),
            ("npipe" | "cluster" | "image", _) => {
                kind.error("not supported: a mount is a `bind`, a `volume` or a `tmpfs`");
                return;
            }
            _ => {
                kind.error("expected `bind`, `volume` or `tmpfs`");
                return;
            }
        };
        let at = source.as_ref().unwrap_or(item);
        if let Some(key) = self.volume(at, from, out) {
            let read_only = read_only.unwrap_or(false);
            self.mount(item, key, target, read_only, sub_path, out);
        }
    }

    /// `tmpfs`: scratch space in memory at each path.
    fn tmpfs(&self, service: &Cursor<'_>, out: &mut Mounts) {
        let Some(tmpfs) = present(service, "tmpfs") else {
            return;
        };
        'items: for item in one_or_more(&tmpfs) {
            let Some(text) = item.expect_str() else {
                continue;
            };
            let (path, options) = text.split_once(':').unwrap_or((text, ""));
            let mut size = None;
            for option in options.split(',').filter(|o| !o.is_empty()) {
                match option.strip_prefix("size=") {
                    Some(text) => match units::docker_bytes(text) {
                        Ok(bytes) => size = Some(bytes).filter(|bytes| *bytes > 0),
                        Err(e) => {
                            item.error(e.0);
                            continue 'items;
                        }
                    },
                    None if option == "rw" => {}
                    None => {
                        item.error(format!(
                            "tmpfs option `{option}` is not supported; `size=` is the one that is"
                        ));
                        continue 'items;
                    }
                }
            }
            if let Some(key) = self.volume(&item, Source::Tmpfs(size), out) {
                self.mount(&item, key, path, false, None, out);
            }
        }
    }

    /// Adds the volume a mount needs to the pod and returns its key.
    ///
    /// A named volume keeps its name as the key. Everything else gets
    /// `v0`, `v1` and so on, in the order of the file.
    fn volume(&self, at: &Cursor<'_>, source: Source<'_>, out: &mut Mounts) -> Option<String> {
        let volume = match source {
            Source::Named(name) => {
                if !self.declared.contains(name) {
                    at.error(format!(
                        "volume `{name}` is not declared: add `{name}:` under the top-level \
                         `volumes`, or write a path that starts with `/` or `./`"
                    ));
                    return None;
                }
                let volume = VolumeSource::Named {
                    name: name.to_string(),
                    per_replica: false,
                };
                out.volumes.entry(name.to_string()).or_insert(volume);
                out.origins
                    .push((format!("volumes.{name}"), at.path().to_string()));
                return Some(name.to_string());
            }
            Source::Anonymous => VolumeSource::Ephemeral {
                memory: false,
                size_bytes: None,
            },
            Source::Tmpfs(size_bytes) => VolumeSource::Ephemeral {
                memory: true,
                size_bytes,
            },
            Source::Path(path, create) => VolumeSource::HostPath {
                path: self.host_path(at, path)?,
                create,
            },
        };
        let key = loop {
            let key = format!("v{}", out.generated);
            out.generated += 1;
            if !self.declared.contains(&key) {
                break key;
            }
        };
        out.volumes.insert(key.clone(), volume);
        out.origins
            .push((format!("volumes.{key}"), at.path().to_string()));
        Some(key)
    }

    /// The path of a bind mount on the node. A relative one is resolved
    /// against the directory of the compose file, as compose does.
    fn host_path(&self, at: &Cursor<'_>, path: &str) -> Option<String> {
        if path.starts_with('~') {
            at.error("`~` is not expanded: write the path as it is on the node");
            return None;
        }
        if has_drive(path) || path.contains('\\') {
            at.error(WINDOWS_PATH);
            return None;
        }
        if path.starts_with('/') {
            return Some(normalize(path));
        }
        let resolved = self.dir.resolve(path);
        if !resolved.starts_with('/') {
            at.error(format!(
                "the relative path resolves to `{resolved}`, which is not an absolute path \
                 on a node; write the path as it is on the node"
            ));
            return None;
        }
        at.warn(format!(
            "the relative path resolves to `{resolved}`, which is read on the node that \
             runs the service, not on this machine"
        ));
        Some(resolved)
    }

    fn mount(
        &self,
        at: &Cursor<'_>,
        volume: String,
        target: &str,
        read_only: bool,
        sub_path: Option<&str>,
        out: &mut Mounts,
    ) {
        if !target.starts_with('/') {
            at.error(format!("the container path `{target}` must be absolute"));
            return;
        }
        let spec = format!("containers[0].mounts[{}]", out.mounts.len());
        out.origins.push((spec, at.path().to_string()));
        out.mounts.push(MountSpec {
            volume,
            path: normalize(target),
            read_only,
            sub_path: sub_path.map(str::to_string),
        });
    }
}

/// A whole number that may be negative, also as a string.
fn whole_or_negative(at: &Cursor<'_>) -> Option<i64> {
    at.as_i64()
        .or_else(|| at.as_str().and_then(|text| text.trim().parse().ok()))
}

/// How Docker runs a health check written as one string.
fn shell_command(shell: &str) -> Vec<String> {
    vec!["/bin/sh".to_string(), "-c".to_string(), shell.to_string()]
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::*;

    const FILE: &str = "/srv/app/compose.yaml";

    /// The compose file and the env files the rows refer to.
    fn files(compose: &str) -> BTreeMap<PathBuf, String> {
        BTreeMap::from([
            (PathBuf::from(FILE), compose.to_string()),
            (
                PathBuf::from("/srv/app/web.env"),
                "A=file\nB=file\n".to_string(),
            ),
            (PathBuf::from("/srv/app/more.env"), "B=more\n".to_string()),
            (PathBuf::from("/srv/outside.env"), "O=1\n".to_string()),
        ])
    }

    fn input() -> ComposeInput {
        ComposeInput {
            file: PathBuf::from(FILE),
            variables: BTreeMap::from([("HOST_VAR".to_string(), "from-caller".to_string())]),
            ..ComposeInput::default()
        }
    }

    fn run(compose: &str) -> Result<Conversion, ConvertError> {
        convert(&input(), &files(compose))
    }

    /// A file with the one service `web`, whose image is `nginx`, and
    /// `body` as the rest of its keys.
    fn service(body: &str) -> String {
        let mut doc = String::from("services:\n  web:\n    image: nginx\n");
        for line in body.lines() {
            doc.push_str(&format!("    {line}\n"));
        }
        doc
    }

    /// [`service`], with the named volume `data` declared.
    fn with_volume(body: &str) -> String {
        format!("{}volumes:\n  data:\n", service(body))
    }

    fn workload(done: &Conversion) -> Value {
        serde_json::to_value(&done.workloads["web"]).unwrap()
    }

    fn paths(found: &[Diagnostic]) -> Vec<&str> {
        found.iter().map(|d| d.path.as_str()).collect()
    }

    fn lines(found: &[Diagnostic]) -> Vec<String> {
        found.iter().map(ToString::to_string).collect()
    }

    /// What a row of the contract says about a snippet.
    enum Want {
        /// Converted without a warning: the value at a JSON pointer into
        /// the workload `web`.
        Mapped(&'static str, Value),
        /// [`Want::Mapped`], with one warning at a path.
        MappedWarn(&'static str, Value, &'static str),
        /// Converted without a warning, to the same workload as without
        /// the snippet.
        Ignored,
        /// [`Want::Ignored`], with one warning at a path.
        Warn(&'static str),
        /// Refused with one error at a path.
        Error(&'static str),
    }
    use Want::{Error, Ignored, Mapped, MappedWarn, Warn};

    fn check(doc: &str, want: &Want) {
        let result = run(doc);
        let (mapped, warning) = match want {
            Error(path) => {
                let err = match result {
                    Err(err) => err,
                    Ok(done) => panic!("converted:\n{doc}\n{:?}", done.warnings),
                };
                assert_eq!(paths(&err.errors), [*path], "{doc}\n{err}");
                return;
            }
            Mapped(pointer, value) => (Some((pointer, value)), None),
            MappedWarn(pointer, value, path) => (Some((pointer, value)), Some(*path)),
            Ignored => (None, None),
            Warn(path) => (None, Some(*path)),
        };
        let done = result.unwrap_or_else(|err| panic!("refused:\n{doc}\n{err}"));
        let warnings: Vec<&str> = warning.into_iter().collect();
        assert_eq!(
            paths(&done.warnings),
            warnings,
            "{doc}\n{:?}",
            done.warnings
        );
        let web = workload(&done);
        match mapped {
            Some((pointer, value)) => {
                assert_eq!(web.pointer(pointer), Some(value), "{doc}\n{web:#}");
            }
            None => assert_eq!(web, workload(&run(&service("")).unwrap()), "{doc}"),
        }
    }

    fn check_services(rows: &[(&str, Want)]) {
        for (body, want) in rows {
            check(&service(body), want);
        }
    }

    /// One row per key: `<key>: <value>` in the service `web`.
    fn check_keys(keys: &[&str], values: &[&str], want: fn(&'static str) -> Want) {
        for key in keys {
            let path: &'static str = format!("services.web.{key}").leak();
            for value in values {
                check(&service(&format!("{key}: {value}")), &want(path));
            }
        }
    }

    #[test]
    fn a_service_with_an_image_is_a_workload_of_one_container() {
        let done = run(&service("")).unwrap();
        assert!(done.warnings.is_empty());
        assert_eq!(
            serde_json::to_value(&done.workloads).unwrap(),
            json!({"web": {
                "scale": {"replicas": 1},
                // Docker restarts nothing unless told to.
                "restart": "never",
                "expose": "environment",
                "placement": {},
                "volumes": {},
                "containers": [{
                    "name": "web", "image": "nginx", "command": [], "args": [], "env": {},
                    "ports": [], "mounts": [],
                    "resources": {"limits": {}, "requests": {}},
                    "capabilities": [], "read_only_root_fs": false,
                }],
            }})
        );
    }

    #[test]
    fn image_and_build() {
        check("services:\n  web: {}\n", &Error("services.web"));
        check(
            "services:\n  web:\n    image: ''\n",
            &Error("services.web.image"),
        );
        check_services(&[
            ("", Mapped("/containers/0/image", json!("nginx"))),
            ("", Mapped("/containers/0/name", json!("web"))),
            // Also when `image` is set.
            ("build: .", Error("services.web.build")),
            (
                "build: {context: ., dockerfile: Dockerfile}",
                Error("services.web.build"),
            ),
        ]);
    }

    #[test]
    fn entrypoint_and_command() {
        let command = |words: Value| Mapped("/containers/0/command", words);
        let args = |words: Value| Mapped("/containers/0/args", words);
        check_services(&[
            (
                "entrypoint: /bin/app --flag 'a b'",
                command(json!(["/bin/app", "--flag", "a b"])),
            ),
            (
                "entrypoint: [a, b]\ncommand: [c]",
                command(json!(["a", "b"])),
            ),
            ("entrypoint: [a, b]\ncommand: [c]", args(json!(["c"]))),
            ("entrypoint: [a]", args(json!([]))),
            (
                "command: run --port 80",
                args(json!(["run", "--port", "80"])),
            ),
            (
                "command: [run, 80, true]",
                args(json!(["run", "80", "true"])),
            ),
            ("command: run", command(json!([]))),
            // An empty entrypoint clears the image's, so the command is
            // all that runs.
            (
                "entrypoint: []\ncommand: [run, it]",
                command(json!(["run", "it"])),
            ),
            ("entrypoint: []\ncommand: [run, it]", args(json!([]))),
            (
                "entrypoint: ''\ncommand: run it",
                command(json!(["run", "it"])),
            ),
            ("entrypoint: []", Error("services.web.entrypoint")),
            ("entrypoint: ''", Error("services.web.entrypoint")),
            (
                "entrypoint: []\ncommand: []",
                Error("services.web.entrypoint"),
            ),
            ("command: []", Error("services.web.command")),
            ("entrypoint:\ncommand:", Ignored),
            ("command: \"run 'unclosed\"", Error("services.web.command")),
            ("command: {a: b}", Error("services.web.command")),
            ("command: [run, 1.5]", Error("services.web.command[1]")),
        ]);
    }

    #[test]
    fn environment_and_env_file() {
        let env = |env: Value| Mapped("/containers/0/env", env);
        check_services(&[
            (
                "environment:\n  A: x\n  B: true\n  C: 8080\n  D: '1.20'\n  E: ''",
                env(json!({"A": "x", "B": "true", "C": "8080", "D": "1.20", "E": ""})),
            ),
            (
                "environment:\n  - A=x=y\n  - B=",
                env(json!({"A": "x=y", "B": ""})),
            ),
            (
                "environment:\n  V: 1.20",
                Error("services.web.environment.V"),
            ),
            (
                "environment:\n  V: [a]",
                Error("services.web.environment.V"),
            ),
            ("environment:\n  - =x", Error("services.web.environment[0]")),
            // No value: the caller's, or left out.
            (
                "environment:\n  HOST_VAR:",
                env(json!({"HOST_VAR": "from-caller"})),
            ),
            (
                "environment:\n  - HOST_VAR",
                env(json!({"HOST_VAR": "from-caller"})),
            ),
            (
                "environment:\n  NOT_SET:",
                Warn("services.web.environment.NOT_SET"),
            ),
            (
                "environment:\n  - NOT_SET",
                Warn("services.web.environment[0]"),
            ),
            ("env_file: web.env", env(json!({"A": "file", "B": "file"}))),
            // Later files win, and `environment` wins over all of them.
            (
                "env_file: [web.env, ./more.env]",
                env(json!({"A": "file", "B": "more"})),
            ),
            (
                "env_file:\n  - path: web.env\n    required: true\nenvironment: {A: env}",
                env(json!({"A": "env", "B": "file"})),
            ),
            (
                "env_file:\n  - path: missing.env\n    required: false",
                Ignored,
            ),
            ("env_file: missing.env", Error("services.web.env_file")),
            (
                "env_file: [web.env, missing.env]",
                Error("services.web.env_file[1]"),
            ),
            (
                "env_file:\n  - path: missing.env",
                Error("services.web.env_file[0].path"),
            ),
            (
                "env_file:\n  - required: false",
                Error("services.web.env_file[0]"),
            ),
            (
                "env_file:\n  - path: web.env\n    format: raw",
                Error("services.web.env_file[0].format"),
            ),
            // Outside the directory of the compose file.
            ("env_file: ../outside.env", Error("services.web.env_file")),
            ("env_file: /srv/outside.env", Error("services.web.env_file")),
            (
                "env_file: [web.env, sub/../../outside.env]",
                Error("services.web.env_file[1]"),
            ),
            (
                "env_file:\n  - path: ../nope.env\n    required: false",
                Error("services.web.env_file[0].path"),
            ),
        ]);
    }

    #[test]
    fn an_env_file_outside_the_directory_needs_the_flag() {
        let err = run(&service("env_file: ../outside.env")).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/compose.yaml: services.web.env_file: `/srv/outside.env` is outside the \
             directory of the compose file, and what it holds would go into the deploy; pass \
             `--allow-external-env-file` to read it"
        );
        let allowed = ComposeInput {
            allow_external_env_files: true,
            ..input()
        };
        for body in ["env_file: ../outside.env", "env_file: /srv/outside.env"] {
            let done = convert(&allowed, &files(&service(body))).unwrap();
            assert_eq!(workload(&done)["containers"][0]["env"], json!({"O": "1"}));
            assert_eq!(
                lines(&done.warnings),
                [
                    "/srv/app/compose.yaml: services.web.env_file: reads `/srv/outside.env`, \
                  which is outside the directory of the compose file"
                ]
            );
        }

        // The same for the file of `--env-file`.
        let mut files = files(&service("environment:\n  B: ${FROM_FILE:-none}"));
        files.insert(
            PathBuf::from("/etc/prod.env"),
            "FROM_FILE=prod\n".to_string(),
        );
        for path in ["/etc/prod.env", "/srv/app/../../etc/prod.env"] {
            let named = ComposeInput {
                env_file: Some(PathBuf::from(path)),
                ..input()
            };
            let err = convert(&named, &files).unwrap_err();
            assert_eq!(
                err.to_string(),
                "/etc/prod.env: `/etc/prod.env` is outside the directory of the compose file, \
                 and what it holds would go into the deploy; pass `--allow-external-env-file` \
                 to read it"
            );
            let named = ComposeInput {
                allow_external_env_files: true,
                ..named
            };
            let done = convert(&named, &files).unwrap();
            assert_eq!(workload(&done)["containers"][0]["env"]["B"], "prod");
            assert_eq!(
                lines(&done.warnings),
                ["/etc/prod.env: is outside the directory of the compose file"]
            );
        }
    }

    #[test]
    fn an_env_file_of_a_service_is_interpolated_as_compose_does() {
        let mut files = files(&service("env_file: web.env"));
        files.insert(
            PathBuf::from("/srv/app/web.env"),
            "A=$HOST_VAR\nB='$HOST_VAR'\nC=\"${NOPE:-d}\"\nHOST_VAR\nD=$NOPE\nNOT_SET\n"
                .to_string(),
        );
        let done = convert(&input(), &files).unwrap();
        assert_eq!(
            workload(&done)["containers"][0]["env"],
            json!({
                "A": "from-caller", "B": "$HOST_VAR", "C": "d", "D": "",
                "HOST_VAR": "from-caller",
            })
        );
        assert_eq!(
            lines(&done.warnings),
            [
                "/srv/app/compose.yaml: services.web.env_file: `/srv/app/web.env`: D: variable \
                 NOPE is not set and is replaced by an empty string; set it in the environment \
                 or in the .env file",
                "/srv/app/compose.yaml: services.web.env_file: `/srv/app/web.env`: variable \
                 NOT_SET has no value there and is not set in the environment, so the \
                 container does not get it",
            ]
        );

        files.insert(
            PathBuf::from("/srv/app/web.env"),
            "A=1\nB='hunter2\n".to_string(),
        );
        let err = convert(&input(), &files).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/compose.yaml: services.web.env_file: `/srv/app/web.env`: line 2: \
             the single quote is not closed on this line"
        );
    }

    #[test]
    fn working_dir_and_user() {
        let user = |user: &str| Mapped("/containers/0/user", json!(user));
        check_services(&[
            (
                "working_dir: /srv",
                Mapped("/containers/0/working_dir", json!("/srv")),
            ),
            // Written one way, like the path of a mount.
            (
                "working_dir: /app/",
                Mapped("/containers/0/working_dir", json!("/app")),
            ),
            (
                "working_dir: //srv/./app//",
                Mapped("/containers/0/working_dir", json!("/srv/app")),
            ),
            (
                "working_dir: relative/dir",
                Error("services.web.working_dir"),
            ),
            ("working_dir: ''", Ignored),
            ("user: '1000:1000'", user("1000:1000")),
            ("user: 1000", user("1000")),
            ("user: www-data", user("www-data")),
            ("user: www-data:www-data", Error("services.web.user")),
            ("user: '1000:staff'", Error("services.web.user")),
            ("user: ''", Ignored),
        ]);
    }

    #[test]
    fn restart_and_scale() {
        let restart = |policy: &str| Mapped("/restart", json!(policy));
        let replicas = |count: u32| Mapped("/scale", json!({"replicas": count}));
        check_services(&[
            ("", restart("never")),
            ("restart: no", restart("never")),
            ("restart: 'no'", restart("never")),
            ("restart: false", restart("never")),
            ("restart: always", restart("always")),
            ("restart: unless-stopped", restart("always")),
            ("restart: on-failure", restart("on_failure")),
            (
                "restart: on-failure:3",
                MappedWarn("/restart", json!("on_failure"), "services.web.restart"),
            ),
            ("restart: sometimes", Error("services.web.restart")),
            ("restart: on-failure:often", Error("services.web.restart")),
            ("scale: 3", replicas(3)),
            ("scale: 0", replicas(0)),
            ("deploy:\n  replicas: 2", replicas(2)),
            ("scale: 2\ndeploy:\n  replicas: 2", replicas(2)),
            (
                "scale: 2\ndeploy:\n  replicas: 3",
                Error("services.web.deploy.replicas"),
            ),
            ("scale: -1", Error("services.web.scale")),
            ("scale: 4294967296", Error("services.web.scale")),
            ("deploy:\n  mode: replicated", Ignored),
            (
                "deploy:\n  mode: global",
                Mapped("/scale", json!("per_node")),
            ),
            (
                "deploy:\n  mode: global\n  replicas: 2",
                Error("services.web.deploy.replicas"),
            ),
            (
                "deploy:\n  mode: replicated-job",
                Error("services.web.deploy.mode"),
            ),
            (
                "deploy:\n  mode: global-job",
                Error("services.web.deploy.mode"),
            ),
            ("deploy:\n  mode: other", Error("services.web.deploy.mode")),
        ]);
    }

    #[test]
    fn deploy_restart_policy_and_placement() {
        let restart = |policy: &str| Mapped("/restart", json!(policy));
        check_services(&[
            (
                "deploy:\n  restart_policy:\n    condition: none",
                restart("never"),
            ),
            (
                "deploy:\n  restart_policy:\n    condition: on-failure",
                restart("on_failure"),
            ),
            (
                "deploy:\n  restart_policy:\n    condition: any",
                restart("always"),
            ),
            (
                "restart: always\ndeploy:\n  restart_policy:\n    condition: any",
                restart("always"),
            ),
            (
                "restart: always\ndeploy:\n  restart_policy:\n    condition: none",
                Error("services.web.deploy.restart_policy.condition"),
            ),
            (
                "deploy:\n  restart_policy:\n    condition: never",
                Error("services.web.deploy.restart_policy.condition"),
            ),
            // A policy without a condition is compose's `any`.
            ("deploy:\n  restart_policy: {}", Ignored),
            (
                "restart: on-failure\ndeploy:\n  restart_policy:\n    delay: 5s",
                MappedWarn(
                    "/restart",
                    json!("on_failure"),
                    "services.web.deploy.restart_policy.delay",
                ),
            ),
            (
                "deploy:\n  restart_policy:\n    delay: 5s",
                MappedWarn(
                    "/restart",
                    json!("always"),
                    "services.web.deploy.restart_policy.delay",
                ),
            ),
            (
                "deploy:\n  restart_policy:\n    max_attempts: 3",
                MappedWarn(
                    "/restart",
                    json!("always"),
                    "services.web.deploy.restart_policy.max_attempts",
                ),
            ),
            (
                "deploy:\n  restart_policy:\n    window: 120s",
                MappedWarn(
                    "/restart",
                    json!("always"),
                    "services.web.deploy.restart_policy.window",
                ),
            ),
            (
                "deploy:\n  placement:\n    constraints: [node.hostname == n1]",
                Mapped("/placement", json!({"node": "n1"})),
            ),
            (
                "deploy:\n  placement:\n    constraints: ['node.hostname==n1']",
                Mapped("/placement", json!({"node": "n1"})),
            ),
            ("deploy:\n  placement:\n    constraints: []", Ignored),
            (
                "deploy:\n  placement:\n    constraints: [node.role == manager]",
                Error("services.web.deploy.placement.constraints[0]"),
            ),
            (
                "deploy:\n  placement:\n    constraints: [node.hostname != n1]",
                Error("services.web.deploy.placement.constraints[0]"),
            ),
            (
                "deploy:\n  placement:\n    constraints: [node.hostname == a, node.hostname == b]",
                Error("services.web.deploy.placement.constraints"),
            ),
            (
                "deploy:\n  placement:\n    preferences: [{spread: node.labels.zone}]",
                Warn("services.web.deploy.placement.preferences"),
            ),
            (
                "deploy:\n  placement:\n    max_replicas_per_node: 1",
                Warn("services.web.deploy.placement.max_replicas_per_node"),
            ),
            (
                "deploy:\n  update_config: {parallelism: 2}",
                Warn("services.web.deploy.update_config"),
            ),
            (
                "deploy:\n  rollback_config: {parallelism: 2}",
                Warn("services.web.deploy.rollback_config"),
            ),
            (
                "deploy:\n  endpoint_mode: vip",
                Warn("services.web.deploy.endpoint_mode"),
            ),
            ("deploy:\n  labels: {a: b}", Ignored),
            ("deploy:\n  x-note: 1", Ignored),
            (
                "deploy:\n  frobnicate: 1",
                Error("services.web.deploy.frobnicate"),
            ),
        ]);
    }

    #[test]
    fn resources() {
        let limits = |amounts: Value| Mapped("/containers/0/resources/limits", amounts);
        let requests = |amounts: Value| Mapped("/containers/0/resources/requests", amounts);
        check_services(&[
            (
                "deploy:\n  resources:\n    limits:\n      cpus: '0.5'\n      memory: 512M",
                limits(json!({"cpu_millis": 500, "memory_bytes": 536_870_912})),
            ),
            ("cpus: 1.5", limits(json!({"cpu_millis": 1500}))),
            ("cpus: 2", limits(json!({"cpu_millis": 2000}))),
            ("cpus: '0.0001'", limits(json!({"cpu_millis": 1}))),
            (
                "mem_limit: 1gb",
                limits(json!({"memory_bytes": 1_073_741_824})),
            ),
            (
                "mem_limit: 1048576",
                limits(json!({"memory_bytes": 1_048_576})),
            ),
            // The same amount twice is no conflict, however it is written.
            (
                "cpus: 0.5\nmem_limit: 1024k\ndeploy:\n  resources:\n    limits: {cpus: '0.50', memory: 1m}",
                limits(json!({"cpu_millis": 500, "memory_bytes": 1_048_576})),
            ),
            (
                "cpus: 1\ndeploy:\n  resources:\n    limits: {cpus: '2'}",
                Error("services.web.deploy.resources.limits.cpus"),
            ),
            (
                "mem_limit: 1g\ndeploy:\n  resources:\n    limits: {memory: 2g}",
                Error("services.web.deploy.resources.limits.memory"),
            ),
            (
                "deploy:\n  resources:\n    reservations: {cpus: '0.25', memory: 64m}",
                requests(json!({"cpu_millis": 250, "memory_bytes": 67_108_864})),
            ),
            (
                "mem_reservation: 64m",
                requests(json!({"memory_bytes": 67_108_864})),
            ),
            (
                "mem_reservation: 64m\ndeploy:\n  resources:\n    reservations: {memory: 32m}",
                Error("services.web.deploy.resources.reservations.memory"),
            ),
            // Zero is Docker's "no limit".
            ("cpus: 0\nmem_limit: 0", Ignored),
            ("cpus: lots", Error("services.web.cpus")),
            ("mem_limit: 1.5", Error("services.web.mem_limit")),
            ("mem_limit: 12 parsecs", Error("services.web.mem_limit")),
            (
                "deploy:\n  resources:\n    limits: {pids: 100}",
                Error("services.web.deploy.resources.limits.pids"),
            ),
            ("pids_limit: 100", Error("services.web.pids_limit")),
            ("pids_limit: -1", Ignored),
            ("pids_limit: 0", Ignored),
            (
                "deploy:\n  resources:\n    reservations:\n      devices: [{capabilities: [gpu]}]",
                Error("services.web.deploy.resources.reservations.devices"),
            ),
            (
                "deploy:\n  resources:\n    reservations:\n      generic_resources: [{kind: gpu}]",
                Error("services.web.deploy.resources.reservations.generic_resources"),
            ),
        ]);
    }

    #[test]
    fn healthcheck() {
        let action = |command: Value| {
            Mapped(
                "/containers/0/readiness/action",
                json!({"type": "exec", "command": command}),
            )
        };
        let probe = |period: u32, timeout: u32, failures: u32| {
            Mapped(
                "/containers/0/readiness",
                json!({
                    "action": {"type": "exec", "command": ["true"]},
                    "initial_delay_secs": 0, "period_secs": period, "timeout_secs": timeout,
                    "success_threshold": 1, "failure_threshold": failures,
                }),
            )
        };
        check_services(&[
            (
                "healthcheck:\n  test: curl -f http://localhost",
                action(json!(["/bin/sh", "-c", "curl -f http://localhost"])),
            ),
            (
                "healthcheck:\n  test: [CMD-SHELL, curl -f http://localhost]",
                action(json!(["/bin/sh", "-c", "curl -f http://localhost"])),
            ),
            (
                "healthcheck:\n  test: [CMD, pg_isready, -U, app]",
                action(json!(["pg_isready", "-U", "app"])),
            ),
            // Docker's timings where the file sets none.
            ("healthcheck:\n  test: [CMD, 'true']", probe(30, 30, 3)),
            (
                "healthcheck:\n  test: [CMD, 'true']\n  interval: 1m30s\n  timeout: 500ms\n  retries: 5",
                probe(90, 1, 5),
            ),
            // Zero is Docker's "not set": its default applies.
            (
                "healthcheck:\n  test: [CMD, 'true']\n  interval: 0s\n  timeout: 0s\n  retries: 0",
                probe(30, 30, 3),
            ),
            // One mistake in `test` is one error, whatever stands next to it.
            (
                "healthcheck:\n  test: 5\n  interval: 5s\n  timeout: 1s\n  retries: 2",
                Error("services.web.healthcheck.test"),
            ),
            (
                "healthcheck:\n  test: {cmd: x}\n  start_period: 5s\n  start_interval: 1s",
                Error("services.web.healthcheck.test"),
            ),
            (
                "healthcheck:\n  test: [CMD, 1, [x]]\n  interval: 5s",
                Error("services.web.healthcheck.test[2]"),
            ),
            (
                "healthcheck:\n  test: [RUN, x]\n  interval: 5s\n  retries: 2",
                Error("services.web.healthcheck.test"),
            ),
            (
                "healthcheck:\n  test: x\n  start_period: 40s",
                MappedWarn(
                    "/containers/0/readiness/initial_delay_secs",
                    json!(40),
                    "services.web.healthcheck.start_period",
                ),
            ),
            (
                "healthcheck:\n  test: x\n  start_period: 0s",
                Mapped("/containers/0/readiness/initial_delay_secs", json!(0)),
            ),
            (
                "healthcheck:\n  test: x\n  start_interval: 5s",
                MappedWarn(
                    "/containers/0/readiness/period_secs",
                    json!(30),
                    "services.web.healthcheck.start_interval",
                ),
            ),
            ("healthcheck:\n  test: [NONE]", Ignored),
            ("healthcheck:\n  test: [NONE]\n  interval: 5s", Ignored),
            ("healthcheck:\n  disable: true", Ignored),
            (
                "healthcheck:\n  disable: true\n  test: [CMD, x]\n  interval: 5s",
                Ignored,
            ),
            ("healthcheck: {}", Ignored),
            // The health check of the image is not used.
            (
                "healthcheck:\n  interval: 5s",
                Warn("services.web.healthcheck"),
            ),
            (
                "healthcheck:\n  test: [RUN, x]",
                Error("services.web.healthcheck.test"),
            ),
            (
                "healthcheck:\n  test: [CMD]",
                Error("services.web.healthcheck.test"),
            ),
            (
                "healthcheck:\n  test: [CMD-SHELL, a, b]",
                Error("services.web.healthcheck.test"),
            ),
            (
                "healthcheck:\n  test: {a: b}",
                Error("services.web.healthcheck.test"),
            ),
            (
                "healthcheck:\n  test: x\n  interval: 30",
                Error("services.web.healthcheck.interval"),
            ),
            (
                "healthcheck:\n  test: x\n  timeout: soon",
                Error("services.web.healthcheck.timeout"),
            ),
            (
                "healthcheck:\n  test: x\n  retries: many",
                Error("services.web.healthcheck.retries"),
            ),
            (
                "healthcheck:\n  test: x\n  frobnicate: 1",
                Error("services.web.healthcheck.frobnicate"),
            ),
        ]);
    }

    #[test]
    fn ports_and_expose() {
        let tcp = |port: u16| json!({"port": port, "protocol": "tcp"});
        let udp = |port: u16| json!({"port": port, "protocol": "udp"});
        let ports = |ports: Value| Mapped("/containers/0/ports", ports);
        // A published port is declared, and nothing more.
        let unpublished =
            |ports: Value| MappedWarn("/containers/0/ports", ports, "services.web.ports[0]");
        check_services(&[
            ("ports: ['80']", ports(json!([tcp(80)]))),
            ("ports: [80]", ports(json!([tcp(80)]))),
            ("ports: ['53/udp']", ports(json!([udp(53)]))),
            (
                "ports: ['8000-8002']",
                ports(json!([tcp(8000), tcp(8001), tcp(8002)])),
            ),
            ("ports: ['8080:80']", unpublished(json!([tcp(80)]))),
            ("ports: ['127.0.0.1::80']", unpublished(json!([tcp(80)]))),
            (
                "ports: ['127.0.0.1:5353:53/udp', '80']",
                unpublished(json!([udp(53), tcp(80)])),
            ),
            (
                "ports: ['9000-9001:8000-8001']",
                unpublished(json!([tcp(8000), tcp(8001)])),
            ),
            (
                "ports:\n  - target: 80\n    protocol: udp\n    name: dns\n    app_protocol: http\n    mode: ingress",
                ports(json!([{"name": "dns", "port": 80, "protocol": "udp"}])),
            ),
            (
                "ports:\n  - target: 80\n    published: '8080'\n    protocol: tcp\n    mode: ingress",
                unpublished(json!([tcp(80)])),
            ),
            (
                "ports:\n  - target: '80'\n    published: 8080-8090\n    host_ip: 127.0.0.1\n    mode: host",
                unpublished(json!([tcp(80)])),
            ),
            (
                "ports:\n  - target: 80\n    host_ip: 127.0.0.1",
                unpublished(json!([tcp(80)])),
            ),
            ("ports: ['80/sctp']", Error("services.web.ports[0]")),
            ("ports: ['80/icmp']", Error("services.web.ports[0]")),
            ("ports: ['0']", Error("services.web.ports[0]")),
            ("ports: ['web:80']", Error("services.web.ports[0]")),
            ("ports: [80.5]", Error("services.web.ports[0]")),
            (
                "ports:\n  - target: 80\n    protocol: sctp",
                Error("services.web.ports[0].protocol"),
            ),
            ("ports:\n  - protocol: tcp", Error("services.web.ports[0]")),
            (
                "ports:\n  - target: 80-81",
                Error("services.web.ports[0].target"),
            ),
            (
                "ports:\n  - target: 80\n    mode: mesh",
                Error("services.web.ports[0].mode"),
            ),
            (
                "ports:\n  - target: 80\n    frobnicate: 1",
                Error("services.web.ports[0].frobnicate"),
            ),
            (
                "expose: [80, '53/udp', '9000-9001']",
                ports(json!([tcp(80), udp(53), tcp(9000), tcp(9001)])),
            ),
            // One port is declared once.
            (
                "ports: ['80', '80/udp']\nexpose: ['80', '81']",
                ports(json!([tcp(80), udp(80), tcp(81)])),
            ),
            ("expose: ['8080:80']", Error("services.web.expose[0]")),
            ("expose: ['80/sctp']", Error("services.web.expose[0]")),
        ]);
    }

    #[test]
    fn the_warning_about_a_published_port_names_the_flag() {
        let done = run(&service("ports: ['8080:80']")).unwrap();
        assert_eq!(
            lines(&done.warnings),
            [
                "/srv/app/compose.yaml: services.web.ports[0]: the port is not published on \
                 the host: faber has no ingress yet; use `--expose web` to make the service \
                 reachable cluster-wide"
            ]
        );
    }

    #[test]
    fn volumes_in_the_short_form() {
        let host = |path: &str| json!({"type": "host_path", "path": path, "create": true});
        let scratch = json!({"type": "ephemeral", "memory": false});
        let mount = |volume: &str, read_only: bool| {
            Mapped(
                "/containers/0/mounts",
                json!([{"volume": volume, "path": "/data", "read_only": read_only}]),
            )
        };
        let relative = |path: &str| {
            let volumes = json!({"v0": host(path)});
            MappedWarn("/volumes", volumes, "services.web.volumes[0]")
        };
        let skipped = |volume: &str, read_only: bool| {
            MappedWarn(
                "/containers/0/mounts",
                json!([{"volume": volume, "path": "/data", "read_only": read_only}]),
                "services.web.volumes[0]",
            )
        };
        check_services(&[
            (
                "volumes: ['/host/dir:/data']",
                Mapped("/volumes", json!({"v0": host("/host/dir")})),
            ),
            ("volumes: ['/host/dir:/data']", mount("v0", false)),
            ("volumes: ['/host/dir:/data:ro']", mount("v0", true)),
            ("volumes: ['/host/dir:/data:rw']", mount("v0", false)),
            // Paths are written one way, so that the same spec is `==`.
            (
                "volumes: ['/host//dir/:/data/']",
                Mapped("/volumes", json!({"v0": host("/host/dir")})),
            ),
            ("volumes: ['/host//dir/:/data/']", mount("v0", false)),
            ("volumes: ['./data:/data:ro']", relative("/srv/app/data")),
            ("volumes: ['../shared:/data']", relative("/srv/shared")),
            ("volumes: ['.:/data']", relative("/srv/app")),
            (
                "volumes: ['/data']",
                Mapped("/volumes", json!({"v0": scratch.clone()})),
            ),
            // `v0`, `v1` and so on, in the order of the file.
            (
                "volumes: ['/a:/x', '/y', '/b:/z']",
                Mapped(
                    "/volumes",
                    json!({"v0": host("/a"), "v1": scratch, "v2": host("/b")}),
                ),
            ),
            ("volumes: ['/host:/data:z']", skipped("v0", false)),
            ("volumes: ['/host:/data:ro,Z']", skipped("v0", true)),
            ("volumes: ['/host:/data:cached']", mount("v0", false)),
            ("volumes: ['/host:/data:delegated']", mount("v0", false)),
            ("volumes: ['/host:/data:ro,consistent']", mount("v0", true)),
            (
                "volumes: ['/host:/data:rshared']",
                Error("services.web.volumes[0]"),
            ),
            ("volumes: ['~/dir:/data']", Error("services.web.volumes[0]")),
            (
                "volumes: ['C:\\data:/data']",
                Error("services.web.volumes[0]"),
            ),
            // Compose refuses a volume that is not declared, too.
            (
                "volumes: ['undeclared:/data']",
                Error("services.web.volumes[0]"),
            ),
            ("volumes: ['/host:data']", Error("services.web.volumes[0]")),
            ("volumes: ['/a:/b:ro:z']", Error("services.web.volumes[0]")),
            ("volumes: [5]", Error("services.web.volumes[0]")),
        ]);
        let named = json!({"data": {"type": "named", "name": "data", "per_replica": false}});
        for (body, want) in [
            ("volumes: ['data:/data']", Mapped("/volumes", named.clone())),
            ("volumes: ['data:/data:ro']", mount("data", true)),
            // One volume of the pod, mounted twice.
            ("volumes: ['data:/a', 'data:/b']", Mapped("/volumes", named)),
        ] {
            check(&with_volume(body), &want);
        }
    }

    #[test]
    fn a_relative_bind_path_needs_an_absolute_compose_file() {
        let doc = service("volumes: ['./data:/data']");
        let files = BTreeMap::from([(PathBuf::from("compose.yaml"), doc)]);
        let input = ComposeInput {
            file: PathBuf::from("compose.yaml"),
            ..ComposeInput::default()
        };
        let err = convert(&input, &files).unwrap_err();
        assert_eq!(
            err.to_string(),
            "compose.yaml: services.web.volumes[0]: the relative path resolves to `data`, \
             which is not an absolute path on a node; write the path as it is on the node"
        );
    }

    #[test]
    fn generated_volume_names_step_around_declared_ones() {
        let doc = format!(
            "{}volumes:\n  v0:\n  v2:\n",
            service("volumes: ['/a:/a', 'v0:/b', '/c:/c']")
        );
        let web = workload(&run(&doc).unwrap());
        let keys: Vec<&String> = web["volumes"].as_object().unwrap().keys().collect();
        assert_eq!(keys, ["v0", "v1", "v3"]);
        assert_eq!(web["volumes"]["v0"]["type"], "named");
        assert_eq!(web["containers"][0]["mounts"][0]["volume"], "v1");
        assert_eq!(web["containers"][0]["mounts"][2]["volume"], "v3");
    }

    #[test]
    fn volumes_in_the_long_form() {
        let host = |path: &str, create: bool| json!({"v0": {"type": "host_path", "path": path, "create": create}});
        let scratch = |memory: bool| json!({"v0": {"type": "ephemeral", "memory": memory}});
        let sized =
            |size: u64| json!({"v0": {"type": "ephemeral", "memory": true, "size_bytes": size}});
        check_services(&[
            (
                "volumes:\n  - {type: bind, source: /host, target: /data, read_only: true}",
                Mapped("/volumes", host("/host", true)),
            ),
            (
                "volumes:\n  - {type: bind, source: /host, target: /data, read_only: true}",
                Mapped(
                    "/containers/0/mounts",
                    json!([{"volume": "v0", "path": "/data", "read_only": true}]),
                ),
            ),
            (
                "volumes:\n  - type: bind\n    source: /host\n    target: /data\n    bind: {create_host_path: false, propagation: rprivate}",
                Mapped("/volumes", host("/host", false)),
            ),
            (
                "volumes:\n  - {type: bind, source: ./conf, target: /data, consistency: cached}",
                MappedWarn(
                    "/volumes",
                    host("/srv/app/conf", true),
                    "services.web.volumes[0].source",
                ),
            ),
            (
                "volumes:\n  - {type: bind, source: /host, target: /data, bind: {selinux: z}}",
                MappedWarn(
                    "/volumes",
                    host("/host", true),
                    "services.web.volumes[0].bind.selinux",
                ),
            ),
            (
                "volumes:\n  - {type: volume, target: /data, volume: {}}",
                Mapped("/volumes", scratch(false)),
            ),
            (
                "volumes:\n  - {type: tmpfs, target: /data, tmpfs: {size: 64m}}",
                Mapped("/volumes", sized(67_108_864)),
            ),
            (
                "volumes:\n  - {type: tmpfs, target: /data, tmpfs: {size: 1048576}}",
                Mapped("/volumes", sized(1_048_576)),
            ),
            (
                "volumes:\n  - {type: tmpfs, target: /data}",
                Mapped("/volumes", scratch(true)),
            ),
            (
                "volumes:\n  - {type: bind, source: /host, target: /data, bind: {propagation: rshared}}",
                Error("services.web.volumes[0].bind.propagation"),
            ),
            (
                "volumes:\n  - {type: tmpfs, target: /data, tmpfs: {mode: 0o1777}}",
                Error("services.web.volumes[0].tmpfs.mode"),
            ),
            (
                "volumes:\n  - {type: npipe, source: /p, target: /data}",
                Error("services.web.volumes[0].type"),
            ),
            (
                "volumes:\n  - {type: cluster, source: v, target: /data}",
                Error("services.web.volumes[0].type"),
            ),
            (
                "volumes:\n  - {type: image, source: nginx, target: /data}",
                Error("services.web.volumes[0].type"),
            ),
            (
                "volumes:\n  - {type: nfs, source: v, target: /data}",
                Error("services.web.volumes[0].type"),
            ),
            (
                "volumes:\n  - {source: /host, target: /data}",
                Error("services.web.volumes[0]"),
            ),
            (
                "volumes:\n  - {type: bind, source: /host}",
                Error("services.web.volumes[0]"),
            ),
            (
                "volumes:\n  - {type: bind, target: /data}",
                Error("services.web.volumes[0]"),
            ),
            (
                "volumes:\n  - {type: bind, source: ~/x, target: /data}",
                Error("services.web.volumes[0].source"),
            ),
            (
                "volumes:\n  - {type: volume, source: undeclared, target: /data}",
                Error("services.web.volumes[0].source"),
            ),
            (
                "volumes:\n  - {type: bind, source: /host, target: /data, frobnicate: 1}",
                Error("services.web.volumes[0].frobnicate"),
            ),
        ]);
        let mounted = |sub_path: Option<&str>| {
            let mut mount = json!({"volume": "data", "path": "/data", "read_only": false});
            if let Some(sub_path) = sub_path {
                mount["sub_path"] = json!(sub_path);
            }
            json!([mount])
        };
        for (body, want) in [
            (
                "volumes:\n  - {type: volume, source: data, target: /data, volume: {subpath: pg}}",
                Mapped("/containers/0/mounts", mounted(Some("pg"))),
            ),
            // The model has no empty sub path.
            (
                "volumes:\n  - {type: volume, source: data, target: /data, volume: {subpath: ''}}",
                Mapped("/containers/0/mounts", mounted(None)),
            ),
            (
                "volumes:\n  - {type: volume, source: data, target: /data, volume: {nocopy: true}}",
                MappedWarn(
                    "/containers/0/mounts",
                    mounted(None),
                    "services.web.volumes[0].volume.nocopy",
                ),
            ),
            (
                "volumes:\n  - {type: volume, source: data, target: /data, volume: {nocopy: false}}",
                Mapped("/containers/0/mounts", mounted(None)),
            ),
        ] {
            check(&with_volume(body), &want);
        }
    }

    #[test]
    fn tmpfs() {
        let memory = |size: Option<u64>| match size {
            Some(size) => json!({"type": "ephemeral", "memory": true, "size_bytes": size}),
            None => json!({"type": "ephemeral", "memory": true}),
        };
        check_services(&[
            (
                "tmpfs: /run",
                Mapped("/volumes", json!({"v0": memory(None)})),
            ),
            (
                "tmpfs: /run",
                Mapped(
                    "/containers/0/mounts",
                    json!([{"volume": "v0", "path": "/run", "read_only": false}]),
                ),
            ),
            (
                "tmpfs: ['/run:size=64m', '/tmp:rw,size=1g']",
                Mapped(
                    "/volumes",
                    json!({"v0": memory(Some(67_108_864)), "v1": memory(Some(1_073_741_824))}),
                ),
            ),
            // Numbered after the entries of `volumes`.
            (
                "volumes: ['/a:/a']\ntmpfs: /run",
                Mapped("/volumes/v1", memory(None)),
            ),
            ("tmpfs: /run:noexec", Error("services.web.tmpfs")),
            ("tmpfs: ['/run:mode=1777']", Error("services.web.tmpfs[0]")),
            ("tmpfs: ['/run:size=big']", Error("services.web.tmpfs[0]")),
            ("tmpfs: run", Error("services.web.tmpfs")),
        ]);
    }

    #[test]
    fn the_remaining_mapped_keys() {
        check_services(&[
            (
                "stop_grace_period: 1m30s",
                Mapped("/stop_grace_secs", json!(90)),
            ),
            (
                "stop_grace_period: 500ms",
                Mapped("/stop_grace_secs", json!(1)),
            ),
            (
                "stop_grace_period: 10",
                Error("services.web.stop_grace_period"),
            ),
            (
                "cap_add: [CHOWN, cap_setuid, CAP_NET_BIND_SERVICE, chown]",
                Mapped(
                    "/containers/0/capabilities",
                    json!(["CHOWN", "SETUID", "NET_BIND_SERVICE"]),
                ),
            ),
            // A capability Linux has, and a container may not add.
            ("cap_add: [NET_ADMIN]", Error("services.web.cap_add[0]")),
            (
                "cap_add: [CHOWN, cap_sys_admin]",
                Error("services.web.cap_add[1]"),
            ),
            ("cap_add: [ALL]", Error("services.web.cap_add[0]")),
            (
                "cap_add: [CHOWN, CAP_ALL]",
                Error("services.web.cap_add[1]"),
            ),
            ("cap_add: [FLY]", Error("services.web.cap_add[0]")),
            ("cap_add: []", Ignored),
            (
                "read_only: true",
                Mapped("/containers/0/read_only_root_fs", json!(true)),
            ),
            ("read_only: false", Ignored),
            ("read_only: maybe", Error("services.web.read_only")),
            (
                "x-faber: {expose: cluster}",
                Mapped("/expose", json!("cluster")),
            ),
            ("x-faber: {expose: environment}", Ignored),
            (
                "x-faber: {expose: world}",
                Error("services.web.x-faber.expose"),
            ),
            (
                "x-faber: {expose: cluster, other: 1}",
                Error("services.web.x-faber.other"),
            ),
            ("x-faber: {other: 1}", Error("services.web.x-faber.other")),
            ("x-other: {anything: [goes]}", Ignored),
            ("frobnicate: 1", Error("services.web.frobnicate")),
        ]);
    }

    #[test]
    fn keys_that_are_skipped_with_a_warning() {
        let keys = [
            "depends_on",
            "links",
            "external_links",
            "networks",
            "container_name",
            "hostname",
            "domainname",
            "logging",
            "pull_policy",
            "develop",
            "attach",
        ];
        check_keys(&keys, &["x", "[x]", "{a: b}"], Warn);
        // At a no-op value there is nothing to say.
        check_keys(&keys, &["", "[]", "{}", "false"], |_| Ignored);
        check_services(&[
            // What `docker compose config` writes for every service.
            ("networks:\n  default: null", Ignored),
            ("networks: [default]", Ignored),
            (
                "networks:\n  default: {aliases: [www]}",
                Warn("services.web.networks"),
            ),
        ]);
    }

    #[test]
    fn keys_that_are_ignored() {
        check_services(&[
            ("labels: {a: b}", Ignored),
            ("labels: [a=b]", Ignored),
            ("annotations: {a: b}", Ignored),
            ("label_file: ./labels", Ignored),
            ("cap_drop: [ALL]", Ignored),
            ("security_opt: [no-new-privileges]", Ignored),
            ("security_opt: ['no-new-privileges:true']", Ignored),
            ("network_mode: bridge", Ignored),
        ]);
    }

    #[test]
    fn keys_that_are_refused() {
        let keys = [
            "privileged",
            "devices",
            "device_cgroup_rules",
            "gpus",
            "runtime",
            "sysctls",
            "ulimits",
            "group_add",
            "userns_mode",
            "credential_spec",
            "pid",
            "ipc",
            "uts",
            "cgroup",
            "cgroup_parent",
            "isolation",
            "dns",
            "dns_opt",
            "dns_search",
            "extra_hosts",
            "mac_address",
            "init",
            "tty",
            "stdin_open",
            "stop_signal",
            "shm_size",
            "platform",
            "post_start",
            "pre_stop",
            "cpu_shares",
            "cpu_quota",
            "cpu_period",
            "cpu_rt_runtime",
            "cpu_rt_period",
            "cpu_count",
            "cpu_percent",
            "cpuset",
            "memswap_limit",
            "mem_swappiness",
            "oom_kill_disable",
            "oom_score_adj",
            "blkio_config",
            "storage_opt",
            "extends",
            "volumes_from",
            "configs",
            "secrets",
            "models",
            "provider",
            "use_api_socket",
        ];
        check_keys(&keys, &["true", "x", "1", "[x]", "{a: b}"], Error);
        // `privileged: false` asks for nothing.
        check_keys(&keys, &["false", "", "~", "[]", "{}", "''"], |_| Ignored);
        check_services(&[
            (
                "security_opt: ['seccomp:unconfined']",
                Error("services.web.security_opt[0]"),
            ),
            (
                "security_opt: [no-new-privileges, 'no-new-privileges:false']",
                Error("services.web.security_opt[1]"),
            ),
            (
                "security_opt: [apparmor=unconfined]",
                Error("services.web.security_opt[0]"),
            ),
            ("network_mode: host", Error("services.web.network_mode")),
            ("network_mode: none", Error("services.web.network_mode")),
            (
                "network_mode: service:db",
                Error("services.web.network_mode"),
            ),
            (
                "network_mode: container:abc",
                Error("services.web.network_mode"),
            ),
        ]);
        for doc in [
            service("extends: {file: base.yaml, service: web}"),
            format!("include: [base.yaml]\n{}", service("")),
        ] {
            let err = run(&doc).unwrap_err();
            assert!(err.to_string().contains("`docker compose config`"), "{err}");
        }
    }

    #[test]
    fn the_top_level() {
        let plain = service("");
        let named = service("volumes: ['data:/data']");
        let volume = || {
            let named = json!({"type": "named", "name": "data", "per_replica": false});
            Mapped("/volumes", json!({"data": named}))
        };
        let rows: Vec<(String, Want)> = vec![
            (format!("version: '3.8'\n{plain}"), Ignored),
            (format!("name: shop\n{plain}"), Ignored),
            // Nothing below a top-level `x-` key is read or interpolated.
            (
                format!("x-common: {{a: $NOPE, b: '${{'}}\n{plain}"),
                Ignored,
            ),
            (
                format!("{plain}networks:\n  front: {{}}\n"),
                Warn("networks"),
            ),
            (format!("{plain}networks: {{}}\n"), Ignored),
            // What `docker compose config` writes.
            (
                format!("name: shop\n{plain}networks:\n  default:\n    name: shop_default\n"),
                Ignored,
            ),
            (
                format!("name: shop\n{plain}networks:\n  default:\n    name: other\n"),
                Warn("networks"),
            ),
            (
                format!("{plain}configs:\n  site: {{file: ./site.conf}}\n"),
                Warn("configs"),
            ),
            (
                format!("{plain}secrets:\n  key: {{file: ./key}}\n"),
                Warn("secrets"),
            ),
            // A reference is the error, and the declaration needs no word.
            (
                format!(
                    "{}configs:\n  site: {{file: ./site.conf}}\n",
                    service("configs: [site]")
                ),
                Error("services.web.configs"),
            ),
            (
                format!(
                    "{}secrets:\n  key: {{file: ./key}}\n",
                    service("secrets: [key]")
                ),
                Error("services.web.secrets"),
            ),
            (format!("include: [other.yaml]\n{plain}"), Error("include")),
            (format!("include: []\n{plain}"), Ignored),
            (format!("{plain}frobnicate: 1\n"), Error("frobnicate")),
            (format!("{named}volumes:\n  data:\n"), volume()),
            (format!("{named}volumes:\n  data: {{}}\n"), volume()),
            (
                format!(
                    "{named}volumes:\n  data:\n    driver: local\n    external: false\n    \
                     labels: {{a: b}}\n    x-note: 1\n"
                ),
                volume(),
            ),
            (
                format!("{named}volumes:\n  data: {{driver: nfs}}\n"),
                Error("volumes.data.driver"),
            ),
            (
                format!("{named}volumes:\n  data: {{driver_opts: {{type: nfs}}}}\n"),
                Error("volumes.data.driver_opts"),
            ),
            (
                format!("{named}volumes:\n  data: {{external: true}}\n"),
                Error("volumes.data.external"),
            ),
            (
                format!("{named}volumes:\n  data: {{name: shared}}\n"),
                Error("volumes.data.name"),
            ),
            // `docker compose config` writes the name compose picks anyway.
            (
                format!("name: shop\n{named}volumes:\n  data: {{name: shop_data}}\n"),
                volume(),
            ),
            (
                format!("name: shop\n{named}volumes:\n  data: {{name: other_data}}\n"),
                Error("volumes.data.name"),
            ),
            (
                format!("{named}volumes:\n  data: {{frobnicate: 1}}\n"),
                Error("volumes.data.frobnicate"),
            ),
            // A volume nobody mounts is checked all the same.
            (
                format!("{plain}volumes:\n  data:\n  db_data:\n"),
                Error("volumes.db_data"),
            ),
            ("version: '3'\n".to_string(), Error("")),
            ("services:\n".to_string(), Error("")),
            ("services: {}\n".to_string(), Error("services")),
            ("services: [web]\n".to_string(), Error("services")),
            (
                "services:\n  web: nginx\n".to_string(),
                Error("services.web"),
            ),
        ];
        for (doc, want) in &rows {
            check(doc, want);
        }
    }

    #[test]
    fn a_name_that_is_not_a_dns_label_is_refused_with_the_rule() {
        let err = run("services:\n  db_backup:\n    image: postgres\n").unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/compose.yaml: services.db_backup: service name `db_backup` is not \
             allowed: a name is 1 to 63 lowercase letters, digits and hyphens, and does not \
             start or end with a hyphen; rename it, for example to `db-backup`"
        );
        let err = run(&format!("{}volumes:\n  Db.Data:\n  -x:\n", service(""))).unwrap_err();
        assert_eq!(paths(&err.errors), ["volumes.-x", "volumes[\"Db.Data\"]"]);
        assert!(
            err.errors[0].message.ends_with("hyphen; rename it"),
            "{err}"
        );
        assert!(
            err.errors[1].message.ends_with("for example to `db-data`"),
            "{err}"
        );
    }

    #[test]
    fn profiles_decide_which_services_are_converted() {
        let doc = concat!(
            "services:\n",
            "  web:\n    image: nginx\n",
            "  debug:\n    image: busybox\n    profiles: [debug, tools]\n    privileged: true\n",
            "  seed:\n    image: busybox\n    profiles: [tools]\n",
            "  none:\n    image: busybox\n    profiles: []\n",
        );
        let with = |profiles: &[&str]| {
            let input = ComposeInput {
                profiles: profiles.iter().map(ToString::to_string).collect(),
                ..input()
            };
            convert(&input, &files(doc))
        };
        // A skipped service is not looked at, so what is wrong in it does
        // not count.
        let done = with(&[]).unwrap();
        assert_eq!(done.workloads.keys().collect::<Vec<_>>(), ["none", "web"]);
        assert_eq!(
            lines(&done.warnings),
            [
                "/srv/app/compose.yaml: services.debug: skipped: none of its profiles \
                 (debug, tools) is active; pass `--profile debug` to deploy it",
                "/srv/app/compose.yaml: services.seed: skipped: none of its profiles \
                 (tools) is active; pass `--profile tools` to deploy it",
            ]
        );
        for active in ["tools", "*"] {
            let err = with(&[active]).unwrap_err();
            assert_eq!(paths(&err.errors), ["services.debug.privileged"]);
            assert!(err.warnings.is_empty());
        }

        // Everything filtered is a warning, not an error.
        let done = run("services:\n  seed:\n    image: busybox\n    profiles: [tools]\n").unwrap();
        assert!(done.workloads.is_empty());
        assert_eq!(paths(&done.warnings), ["services.seed", "services"]);
        check(
            &service("profiles: [1]"),
            &Error("services.web.profiles[0]"),
        );
    }

    #[test]
    fn the_result_names_the_skipped_services_and_the_variables_it_used() {
        let done = run(&service("")).unwrap();
        assert!(done.variables.is_empty() && done.skipped.is_empty());

        let doc = concat!(
            "services:\n",
            "  web:\n    image: nginx\n",
            "  seed:\n    image: busybox\n    profiles: [tools]\n",
            "  debug:\n    image: busybox\n    profiles: [debug]\n",
        );
        assert_eq!(run(doc).unwrap().skipped, ["debug", "seed"]);

        let used = |body: &str, env: &str| {
            let mut files = files(&service(body));
            files.insert(PathBuf::from("/srv/app/.env"), env.to_string());
            files.insert(
                PathBuf::from("/srv/app/web.env"),
                "A=$IN_FILE\nHOST_VAR\nB='$OTHER'\n".to_string(),
            );
            convert(&input(), &files).unwrap().variables
        };
        let env = "IN_FILE=1\nOTHER=2\nBUILT=${IN_FILE}-${HOST_VAR}\nDEEP=$BUILT\n";
        let none: [&str; 0] = [];
        for (body, want) in [
            ("working_dir: /srv/$HOST_VAR", &["HOST_VAR"][..]),
            ("working_dir: /srv/${IN_FILE}$OTHER", &["IN_FILE", "OTHER"]),
            ("working_dir: /srv/${NOPE:-$OTHER}", &["OTHER"]),
            // A bare name and a null value take the caller's value.
            ("environment: [HOST_VAR, NOPE, X=1]", &["HOST_VAR"]),
            ("environment:\n  OTHER:\n  X: 1", &["OTHER"]),
            ("env_file: web.env", &["HOST_VAR", "IN_FILE"]),
            // A variable built from others gives their values away too.
            (
                "working_dir: /srv/$BUILT",
                &["BUILT", "HOST_VAR", "IN_FILE"],
            ),
            (
                "working_dir: /srv/$DEEP",
                &["BUILT", "DEEP", "HOST_VAR", "IN_FILE"],
            ),
            // Nothing of the value is in the result.
            ("working_dir: /srv/${HOST_VAR:+set}", &none),
            ("working_dir: /srv/$$HOST_VAR", &none),
            ("environment: {HOST_VAR: mine}", &none),
        ] {
            assert_eq!(used(body, env), want, "{body}");
        }
    }

    #[test]
    fn what_goes_wrong_with_a_variable_never_prints_a_value() {
        let secret = "hunter2-do-not-print";
        let input = ComposeInput {
            variables: BTreeMap::from([("SECRET".to_string(), secret.to_string())]),
            ..input()
        };
        let mut files = files("");
        files.insert(
            PathBuf::from("/srv/app/web.env"),
            format!("A=${{NOPE:?$SECRET}}\nB={secret}\n"),
        );
        for (body, env) in [
            ("working_dir: '${NOPE:?${SECRET}}'", ""),
            ("working_dir: '${NOPE?no $SECRET}'", ""),
            ("working_dir: '/srv/${SECRET'", ""),
            ("working_dir: '${NOPE:-${SECRET:?}} ${SECRET+${'", ""),
            ("working_dir: $SECRET", ""),
            ("user: $SECRET:$SECRET", ""),
            ("env_file: web.env", ""),
            ("image: nginx", "A=${NOPE:?$SECRET}\n"),
            ("image: nginx", &format!("A='{secret}\n")),
            ("image: nginx", &format!("{secret} = x\n")),
        ] {
            files.insert(PathBuf::from(FILE), service(body));
            files.insert(PathBuf::from("/srv/app/.env"), env.to_string());
            let err = convert(&input, &files).unwrap_err();
            assert!(!err.to_string().contains(secret), "{body}\n{env}\n{err}");
            assert!(!format!("{:?}", err.warnings).contains(secret), "{body}");
        }
        files.insert(
            PathBuf::from(FILE),
            service("working_dir: '${NOPE:?${SECRET}}'"),
        );
        files.insert(PathBuf::from("/srv/app/.env"), String::new());
        assert_eq!(
            convert(&input, &files).unwrap_err().to_string(),
            "/srv/app/compose.yaml: services.web.working_dir: required variable NOPE is not \
             set: ${SECRET}"
        );
    }

    #[test]
    fn a_diagnostic_shows_an_interpolated_value_as_the_file_has_it() {
        let secret = "hunter2-Do-Not-Print";
        let variables = [
            ("S", secret.to_string()),
            // Cut into pieces by whatever reads the value.
            ("CUT", format!("{secret}:{secret}/x,{secret}={secret}")),
            ("N", "7".to_string()),
        ];
        let input = ComposeInput {
            variables: variables
                .map(|(name, value)| (name.to_string(), value))
                .into(),
            allow_external_env_files: true,
            ..input()
        };
        let mut seen = 0;
        for body in [
            "cap_add: [$S]",
            "cap_add: ['CAP_${S}']",
            "cap_add: [$CUT]",
            "volumes: ['data:/data:$S']",
            "volumes: ['data:/data:ro,$S']",
            "volumes: ['$S:/data']",
            "volumes: ['$S']",
            "volumes: ['data:$S']",
            "volumes: ['./$S:/data']",
            "volumes: ['./$S/../x:/data']",
            "volumes: ['$CUT']",
            "volumes:\n  - type: $S\n    source: data\n    target: /data",
            "volumes:\n  - type: volume\n    source: $S\n    target: /data",
            "volumes:\n  - type: volume\n    source: data\n    target: $S",
            "volumes:\n  - type: bind\n    source: ./$S\n    target: /data",
            "volumes:\n  - type: bind\n    source: ./x\n    target: /data\n    bind: {propagation: $S}",
            "ports: [$S]",
            "ports: ['$S:80']",
            "ports: ['80:$S']",
            "ports: ['80/$S']",
            "ports: ['$CUT']",
            "ports:\n  - target: $S",
            "ports:\n  - target: 80\n    protocol: $S",
            "ports:\n  - target: 80\n    published: $S",
            "ports:\n  - target: 80\n    host_ip: $S",
            "ports:\n  - target: 80\n    mode: $S",
            "expose: [$S]",
            "expose: ['80/$S']",
            "mem_limit: $S",
            "mem_limit: ${N}$S",
            "mem_reservation: $S",
            "cpus: $S",
            "cpus: $CUT",
            "deploy:\n  resources:\n    limits: {cpus: $S, memory: $S}",
            "deploy:\n  replicas: $S",
            "deploy:\n  mode: $S",
            "deploy:\n  restart_policy: {condition: $S}",
            "deploy:\n  placement:\n    constraints: [$S]",
            "deploy:\n  placement:\n    constraints: ['node.hostname == $S $S']",
            "scale: $S",
            "stop_grace_period: $S",
            "stop_grace_period: ${N}$S",
            "healthcheck:\n  test: x\n  interval: $S",
            "healthcheck:\n  test: x\n  timeout: $S\n  start_period: $S\n  retries: $S",
            "healthcheck:\n  test: [$S, x]",
            "healthcheck:\n  test: x\n  disable: $S",
            "tmpfs: /run:$S",
            "tmpfs: ['/run:size=$S']",
            "tmpfs: ['/run:$CUT']",
            "tmpfs: $S",
            "env_file: $S",
            "env_file: ../$S",
            "env_file: /etc/$S/x.env",
            "env_file: [$CUT]",
            "env_file:\n  - path: $S\n    format: $S",
            "environment: [$S]",
            "restart: $S",
            "restart: on-failure:$S",
            "read_only: $S",
            "network_mode: $S",
            "networks: [$S]",
            "profiles: [$S]",
            "user: $S:$S",
            "working_dir: $S",
            "image: '$S $S'",
            "container_name: $S",
            "hostname: $S",
            "pull_policy: $S",
            "x-faber: {expose: $S}",
            "entrypoint: \"'$S\"",
            "command: \"'$S\"",
            "privileged: $S",
            "depends_on: [$S]",
        ] {
            let done = convert(&input, &files(&with_volume(body)));
            let found = match &done {
                Ok(done) => format!("{:?}", done.warnings),
                Err(err) => format!("{:?} {:?}", err.errors, err.warnings),
            };
            for piece in [secret, "hunter2", "Do-Not-Print", "HUNTER2", "DO-NOT-PRINT"] {
                assert!(!found.contains(piece), "{body}\n{found}");
            }
            seen += usize::from(found.contains('$'));
        }
        // Most of them say what the file has instead.
        assert!(seen > 30, "{seen}");

        let line = |body: &str| {
            let err = convert(&input, &files(&with_volume(body))).unwrap_err();
            err.to_string()
        };
        assert_eq!(
            line("cap_add: [$S]"),
            "/srv/app/compose.yaml: services.web.cap_add[0]: `$S` is not a Linux capability"
        );
        assert_eq!(
            line("mem_limit: 1$S"),
            "/srv/app/compose.yaml: services.web.mem_limit: `1$S` is not a size; write bytes \
             (`1048576`) or a number with a unit (`512k`, `256m`, `1g`), where each unit is \
             1024 of the one before"
        );
        // A quote with an inserted value in it goes as a whole.
        assert_eq!(
            line("stop_grace_period: ${N}"),
            "/srv/app/compose.yaml: services.web.stop_grace_period: `${N}` has a number without \
             a unit; write `${N}` for seconds"
        );
        // A value no variable went into is shown as before.
        assert_eq!(
            line("cap_add: ['${NOPE:-fly}']"),
            "/srv/app/compose.yaml: services.web.cap_add[0]: `fly` is not a Linux capability"
        );
    }

    #[test]
    fn three_messages_in_full() {
        let err = run(&service("scale: 2\ndeploy:\n  mode: global")).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/compose.yaml: services.web.scale: cannot be set together with \
             `deploy.mode: global`, which runs one per node"
        );
        let done = run(&service("restart: on-failure:3")).unwrap();
        assert_eq!(
            lines(&done.warnings),
            [
                "/srv/app/compose.yaml: services.web.restart: the retry count is not kept: the \
              container is restarted after every failure"
            ]
        );
        let done = run(&service("deploy:\n  restart_policy:\n    delay: 5s")).unwrap();
        assert_eq!(
            lines(&done.warnings),
            [
                "/srv/app/compose.yaml: services.web.deploy.restart_policy.delay: skipped: when \
              and how often a container is restarted cannot be configured"
            ]
        );
    }

    #[test]
    fn a_value_that_grows_past_the_limit_is_refused_where_it_stands() {
        let input = ComposeInput {
            variables: BTreeMap::from([("BIG".to_string(), "x".repeat(4096))]),
            ..input()
        };
        let doc = service(&format!("environment:\n  A: '{}'", "$BIG".repeat(17)));
        let err = convert(&input, &files(&doc)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/compose.yaml: services.web.environment.A: invalid interpolation: the \
             result is larger than 64 KiB"
        );
    }

    #[test]
    fn x_faber_belongs_on_a_service() {
        let err = run(&format!("{}x-faber: {{expose: cluster}}\n", service(""))).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/compose.yaml: x-faber: x-faber belongs on a service: move it to \
             `services.<name>.x-faber`"
        );
        check(
            &format!("{}x-other: {{x-faber: 1}}\n", service("")),
            &Ignored,
        );
    }

    #[test]
    fn interpolation_runs_on_values_and_types_follow_it() {
        check_services(&[
            (
                "working_dir: /srv/${HOST_VAR}",
                Mapped("/containers/0/working_dir", json!("/srv/from-caller")),
            ),
            (
                "working_dir: /srv/x$NOPE",
                MappedWarn(
                    "/containers/0/working_dir",
                    json!("/srv/x"),
                    "services.web.working_dir",
                ),
            ),
            (
                "working_dir: '${NOPE:?set NOPE first}'",
                Error("services.web.working_dir"),
            ),
            ("working_dir: '/srv/${'", Error("services.web.working_dir")),
            (
                "command: echo $$HOME",
                Mapped("/containers/0/args", json!(["echo", "$HOME"])),
            ),
            (
                "command: [echo, '${NOPE:-fallback}']",
                Mapped("/containers/0/args", json!(["echo", "fallback"])),
            ),
            // A number or a boolean may come out of a variable.
            ("scale: '${N:-3}'", Mapped("/scale", json!({"replicas": 3}))),
            (
                "read_only: '${RO:-true}'",
                Mapped("/containers/0/read_only_root_fs", json!(true)),
            ),
            (
                "ports: ['${PORT:-8080}']",
                Mapped("/containers/0/ports/0/port", json!(8080)),
            ),
            (
                "healthcheck:\n  test: x\n  retries: '${R:-7}'",
                Mapped("/containers/0/readiness/failure_threshold", json!(7)),
            ),
            (
                "environment:\n  A: 'pa$$word-${HOST_VAR}'",
                Mapped("/containers/0/env/A", json!("pa$word-from-caller")),
            ),
        ]);
        let err = run(&service("working_dir: '${NOPE:?set NOPE first}'")).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/compose.yaml: services.web.working_dir: required variable NOPE is not \
             set: set NOPE first"
        );
        // Keys are never interpolated.
        let walk = Walk::new("f");
        let doc = json!({"$A": {"${B}": ["$HOST_VAR", 1, true]}});
        let variables = Variables {
            values: input().variables,
            ..Variables::default()
        };
        assert_eq!(
            interpolate_document(&walk, doc, &variables),
            json!({"$A": {"${B}": ["from-caller", 1, true]}})
        );
        assert_eq!(walk.finish(), Diagnostics::default());
    }

    #[test]
    fn variables_come_from_the_caller_then_from_the_env_file() {
        let doc = service(concat!(
            "environment:\n",
            "  A: $HOST_VAR\n  B: $FROM_FILE\n  C: $CHAINED\n  D: $LITERAL\n  E: ${QUOTED}\n",
        ));
        let env = concat!(
            "# the caller's value wins\n",
            "HOST_VAR=from-file\n",
            "FROM_FILE=file\n",
            "export CHAINED=${FROM_FILE}-${HOST_VAR}\n",
            "LITERAL='$FROM_FILE'\n",
            "QUOTED=\"a b\" # comment\n",
            "BARE\n",
        );
        let mut files = files(&doc);
        files.insert(PathBuf::from("/srv/app/.env"), env.to_string());
        let done = convert(&input(), &files).unwrap();
        assert!(done.warnings.is_empty(), "{:?}", done.warnings);
        assert_eq!(
            workload(&done)["containers"][0]["env"],
            json!({
                "A": "from-caller", "B": "file", "C": "file-from-caller", "D": "$FROM_FILE",
                "E": "a b",
            })
        );
        assert_eq!(
            done.variables,
            ["CHAINED", "FROM_FILE", "HOST_VAR", "LITERAL", "QUOTED"]
        );

        // `--env-file` replaces `.env`, and has to be there.
        files.insert(
            PathBuf::from("/etc/prod.env"),
            "FROM_FILE=prod\n".to_string(),
        );
        let named = ComposeInput {
            env_file: Some(PathBuf::from("/etc/prod.env")),
            allow_external_env_files: true,
            ..input()
        };
        let done = convert(&named, &files).unwrap();
        assert_eq!(workload(&done)["containers"][0]["env"]["B"], "prod");
        assert_eq!(
            paths(&done.warnings),
            [
                "",
                "services.web.environment.C",
                "services.web.environment.D",
                "services.web.environment.E"
            ]
        );
        let missing = ComposeInput {
            env_file: Some(PathBuf::from("/srv/app/nope.env")),
            ..input()
        };
        let err = convert(&missing, &files).unwrap_err();
        assert_eq!(err.errors[0].source, "/srv/app/nope.env");
        assert!(
            err.errors[0]
                .message
                .starts_with("cannot read the env file"),
            "{err}"
        );

        // What is wrong in the env file is reported against it.
        files.insert(
            PathBuf::from("/srv/app/.env"),
            "FROM_FILE=$NOPE\nCHAINED=${NOPE:?needed}\nLITERAL=x\nQUOTED=x\n".to_string(),
        );
        let err = convert(&input(), &files).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/.env: CHAINED: required variable NOPE is not set: needed"
        );
        assert_eq!(err.warnings[0].source, "/srv/app/.env");
        assert_eq!(err.warnings[0].path, "FROM_FILE");
        files.insert(
            PathBuf::from("/srv/app/.env"),
            "A=1\nB=\"hunter2\n".to_string(),
        );
        let err = convert(&input(), &files).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/srv/app/.env: line 2: the double quote is not closed on this line"
        );
    }

    #[test]
    fn a_file_that_cannot_be_used_at_all_is_one_error() {
        let message = |files: &BTreeMap<PathBuf, String>| {
            let err = convert(&input(), files).unwrap_err();
            assert_eq!(err.errors.len(), 1, "{err}");
            assert_eq!(err.errors[0].source, FILE);
            assert_eq!(err.errors[0].path, "");
            err.errors[0].message.clone()
        };
        assert!(message(&BTreeMap::new()).starts_with("cannot read the compose file: "));
        assert!(message(&files("services: [unclosed\n")).starts_with("not valid YAML: "));
        assert!(message(&files("a: 1\na: 2\n")).contains("duplicate"));
        assert_eq!(
            message(&files("")),
            "the file is empty; a compose file needs `services`"
        );
        assert_eq!(
            message(&files("services: {}\n---\nservices: {}\n")),
            "expected one YAML document, found 2; put all services into one"
        );
        assert_eq!(
            message(&files("- web\n")),
            "expected a map with `services` at the top"
        );
    }

    #[test]
    fn every_error_is_collected_across_services() {
        let doc = concat!(
            "services:\n",
            "  web:\n    build: .\n    privileged: true\n    ports: ['80/sctp', '8080:80']\n",
            "  db_1:\n    image: postgres\n    volumes: ['pgdata:/var/lib/postgresql/data']\n",
            "  cache:\n    image: redis\n    network_mode: host\n    frobnicate: 1\n",
        );
        let err = run(doc).unwrap_err();
        assert_eq!(
            paths(&err.errors),
            [
                "services.cache.network_mode",
                "services.db_1",
                "services.db_1.volumes[0]",
                "services.web.build",
                "services.web.privileged",
                "services.web",
                "services.web.ports[0]",
                // Keys nobody knows come last.
                "services.cache.frobnicate",
            ]
        );
        assert_eq!(paths(&err.warnings), ["services.web.ports[1]"]);
    }

    #[test]
    fn the_rules_of_the_model_are_checked_at_the_end() {
        // Each is reported where the file sets what breaks the rule.
        for (body, want) in [
            (
                "working_dir: relative/dir",
                "services.web.working_dir: must be an absolute path",
            ),
            ("user: '0100'", "services.web.user: "),
            ("scale: 5000", "services.web.scale: "),
            (
                "deploy:\n  replicas: 5000",
                "services.web.deploy.replicas: ",
            ),
            ("volumes: ['./data:/']", "services.web.volumes[0]: path: "),
            (
                "volumes: ['./a:/a', './data:/proc']",
                "services.web.volumes[1]: path: ",
            ),
            (
                "volumes: ['./data:/sys']",
                "services.web.volumes[0]: path: ",
            ),
            (
                "volumes: ['./data:/dev']",
                "services.web.volumes[0]: path: ",
            ),
            (
                "volumes:\n  - type: bind\n    source: ./data\n    target: /dev/pts",
                "services.web.volumes[0]: path: ",
            ),
            ("tmpfs: /proc", "services.web.tmpfs: path: "),
            ("volumes: ['/:/host']", "services.web.volumes[0]: "),
            (
                "deploy:\n  mode: global\n  placement:\n    constraints: [node.hostname == n1]",
                "services.web.deploy.placement: ",
            ),
        ] {
            let err = run(&service(body)).unwrap_err();
            assert_eq!(err.errors.len(), 1, "{body}\n{err}");
            let line = err.to_string();
            let want = format!("/srv/app/compose.yaml: {want}");
            assert!(line.starts_with(&want), "{body}\n{line}");
        }
        check_services(&[
            (
                "volumes: ['./data:/dev/shm']",
                MappedWarn(
                    "/containers/0/mounts/0/path",
                    json!("/dev/shm"),
                    "services.web.volumes[0]",
                ),
            ),
            (
                "tmpfs: /dev/shm",
                Mapped("/containers/0/mounts/0/path", json!("/dev/shm")),
            ),
        ]);
    }

    #[test]
    fn short_ports_parse() {
        let port = |host_ip: Option<&str>, published: Option<&str>, target, protocol| ShortPort {
            host_ip: host_ip.map(str::to_string),
            published: published.map(str::to_string),
            target,
            protocol,
        };
        let (tcp, udp) = (Protocol::Tcp, Protocol::Udp);
        for (text, want) in [
            ("80", port(None, None, (80, 80), tcp)),
            ("65535", port(None, None, (65535, 65535), tcp)),
            ("8080:80", port(None, Some("8080"), (80, 80), tcp)),
            ("53/udp", port(None, None, (53, 53), udp)),
            ("53/UDP", port(None, None, (53, 53), udp)),
            ("80/tcp", port(None, None, (80, 80), tcp)),
            ("3000-3005", port(None, None, (3000, 3005), tcp)),
            (
                "8000-8010:9000-9010/udp",
                port(None, Some("8000-8010"), (9000, 9010), udp),
            ),
            (
                "127.0.0.1:8080:80",
                port(Some("127.0.0.1"), Some("8080"), (80, 80), tcp),
            ),
            (
                "127.0.0.1::80",
                port(Some("127.0.0.1"), None, (80, 80), tcp),
            ),
            (
                "[::1]:8080:80",
                port(Some("::1"), Some("8080"), (80, 80), tcp),
            ),
            ("[::1]::80/udp", port(Some("::1"), None, (80, 80), udp)),
        ] {
            assert_eq!(parse_short_port(text), Ok(want), "{text:?}");
        }
        for text in [
            "",
            "0",
            "65536",
            "-1",
            "http",
            "80/sctp",
            "80/icmp",
            "80/",
            "a:80",
            "80:b",
            "9-8",
            "1-",
            "127.0.0.1:80",
            "1:2:3:4",
            "::1:8080:80",
            "[::1:8080:80",
            "[::1]:80",
            ":80:80",
        ] {
            assert!(parse_short_port(text).is_err(), "{text:?}");
        }
        assert_eq!(
            parse_short_port("80/sctp").unwrap_err(),
            "sctp is not supported: a port is `tcp` or `udp`"
        );
    }

    #[test]
    fn short_volumes_parse() {
        for (text, source, target, options) in [
            ("/data", None, "/data", vec![]),
            ("data:/var/lib", Some("data"), "/var/lib", vec![]),
            ("/host:/data:ro", Some("/host"), "/data", vec!["ro"]),
            ("./a:/b:ro,z", Some("./a"), "/b", vec!["ro", "z"]),
            ("~/a:/b", Some("~/a"), "/b", vec![]),
            // Two parts are always a source and a target.
            ("/data:ro", Some("/data"), "ro", vec![]),
        ] {
            let want = ShortVolume {
                source,
                target,
                options,
            };
            assert_eq!(parse_short_volume(text), Ok(want), "{text:?}");
        }
        for text in ["", ":", "a::b", "a:b:", "a:b:c:d"] {
            assert!(parse_short_volume(text).is_err(), "{text:?}");
        }
        for text in [
            "C:\\data:/data",
            "c:/data:/data",
            "d:",
            "\\\\host\\share:/data",
        ] {
            assert_eq!(
                parse_short_volume(text).unwrap_err(),
                WINDOWS_PATH,
                "{text:?}"
            );
        }
    }

    #[test]
    fn paths_resolve_against_the_directory_of_the_compose_file() {
        for (path, want) in [
            ("/a/b/../c", "/a/c"),
            ("/a/./b/", "/a/b"),
            ("/a//b", "/a/b"),
            ("/../a", "/a"),
            ("/", "/"),
            ("a/../../b", "../b"),
            ("./a", "a"),
            ("../../a", "../../a"),
            ("", ""),
            ("D:/x/../../y", "D:/y"),
        ] {
            assert_eq!(normalize(path), want, "{path:?}");
        }
        let dir = Dir::of(Path::new("/srv/app/compose.yaml"));
        for (path, want, inside) in [
            ("web.env", "/srv/app/web.env", true),
            ("./conf/web.env", "/srv/app/conf/web.env", true),
            ("conf/../web.env", "/srv/app/web.env", true),
            (".", "/srv/app", true),
            ("../web.env", "/srv/web.env", false),
            ("../application/web.env", "/srv/application/web.env", false),
            ("conf/../../../etc/passwd", "/etc/passwd", false),
            ("/etc/passwd", "/etc/passwd", false),
            ("/srv/app/web.env", "/srv/app/web.env", true),
            ("C:\\Users\\me\\.env", "C:/Users/me/.env", false),
        ] {
            let resolved = dir.resolve(path);
            assert_eq!(
                (resolved.as_str(), dir.contains(&resolved)),
                (want, inside),
                "{path:?}"
            );
        }
        // A compose file named without a directory.
        let dir = Dir::of(Path::new("compose.yaml"));
        for (path, want, inside) in [
            (".env", ".env", true),
            ("./conf/web.env", "conf/web.env", true),
            ("../web.env", "../web.env", false),
            ("/etc/passwd", "/etc/passwd", false),
        ] {
            let resolved = dir.resolve(path);
            assert_eq!(
                (resolved.as_str(), dir.contains(&resolved)),
                (want, inside),
                "{path:?}"
            );
        }
    }
}
