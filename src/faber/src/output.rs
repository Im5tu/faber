//! Renders API responses as text or JSON. Every function returns the full
//! output including the trailing newline.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use clap::ValueEnum;
use faber_api_types::workload::{
    ContainerSpec, DeployResponse, Exposure, HttpScheme, MountSpec, PortSpec, Probe, ProbeAction,
    ProjectListResponse, ProjectResponse, Protocol, Resources, RestartPolicy, Scale, Strategy,
    VolumeSource, WorkloadListResponse, WorkloadResponse, WorkloadSpec,
};
use faber_api_types::workload::{ReplicaState, ReplicaStatus};
use faber_api_types::{
    ClusterStatus, HealthResponse, InviteResponse, LeaseResponse, StatusIntervalResponse,
};
use serde::Serialize;
use zeroize::Zeroizing;

use crate::deploy::Diagnostic;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-friendly text.
    Text,
    /// Pretty-printed JSON.
    Json,
}

type Result<T> = serde_json::Result<T>;

/// Shown in place of a missing value.
const NONE: &str = "-";

fn pretty<T: Serialize + ?Sized>(v: &T) -> Result<String> {
    Ok(serde_json::to_string_pretty(v)? + "\n")
}

/// `key: value` lines with the values lined up in one column.
fn aligned(rows: &[(&str, String)]) -> String {
    let width = rows.iter().map(|(k, _)| k.len() + 1).max().unwrap_or(0);
    let mut s = String::new();
    for (k, v) in rows {
        let key = format!("{k}:");
        let _ = writeln!(s, "{key:<width$} {v}");
    }
    s
}

fn opt(v: Option<u64>) -> String {
    v.map_or_else(|| NONE.to_string(), |v| v.to_string())
}

/// `id@addr`, with `-` for either missing part, or just `-` if both are.
pub(crate) fn leader(id: Option<u64>, addr: Option<&str>) -> String {
    if id.is_none() && addr.is_none() {
        return NONE.to_string();
    }
    format!("{}@{}", opt(id), addr.map_or_else(none, esc))
}

/// Left-aligned columns separated by two spaces, each line starting with
/// `indent`.
fn table<const N: usize>(indent: &str, rows: &[[String; N]]) -> String {
    let mut widths = [0usize; N];
    for row in rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let mut s = String::new();
    for row in rows {
        let mut line = String::from(indent);
        for (i, cell) in row.iter().enumerate() {
            let w = if i + 1 == row.len() { 0 } else { widths[i] };
            let _ = write!(line, "{cell:<w$}  ");
        }
        s.push_str(line.trim_end());
        s.push('\n');
    }
    s
}

fn none() -> String {
    NONE.to_string()
}

/// A string from the API or from an input file, made safe to print on a
/// terminal. Every control character, ESC included, becomes `\xNN`, or
/// `\u{..}` outside ASCII, so state and input files cannot inject terminal
/// sequences or break a table with a newline. Every invisible format
/// character becomes `\u{..}` too, so text cannot be shown in another order
/// than it is stored in (the bidi controls) or hide a difference between
/// two names (zero-width characters).
pub(crate) fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_control() && c.is_ascii() {
            let _ = write!(out, "\\x{:02x}", c as u32);
        } else if c.is_control() || is_format(c) {
            let _ = write!(out, "\\u{{{:x}}}", c as u32);
        } else {
            out.push(c);
        }
    }
    out
}

/// Whether `c` is drawn as nothing and changes how the text around it is
/// drawn: the Unicode format characters (category `Cf`) and the two line
/// separators.
fn is_format(c: char) -> bool {
    matches!(
        c,
        '\u{ad}'                    // soft hyphen
        | '\u{600}'..='\u{605}'     // Arabic number signs
        | '\u{61c}'                 // Arabic letter mark
        | '\u{6dd}'
        | '\u{70f}'
        | '\u{890}'..='\u{891}'
        | '\u{8e2}'
        | '\u{180e}'                // Mongolian vowel separator
        | '\u{200b}'..='\u{200f}'   // zero width, LRM, RLM
        | '\u{2028}'..='\u{202e}'   // line separators, bidi embeddings
        | '\u{2060}'..='\u{206f}'   // word joiner, bidi isolates
        | '\u{feff}'                // byte order mark
        | '\u{fff9}'..='\u{fffb}'   // interlinear annotation
        | '\u{110bd}'
        | '\u{110cd}'
        | '\u{13430}'..='\u{1343f}' // hieroglyph format controls
        | '\u{1bca0}'..='\u{1bca3}' // shorthand format controls
        | '\u{1d173}'..='\u{1d17a}' // musical format controls
        | '\u{e0001}'               // language tag
        | '\u{e0020}'..='\u{e007f}' // tag characters
    )
}

/// Unix seconds as UTC, `2023-11-14 22:13:20Z`. 0 is "never" and prints `-`.
fn timestamp(secs: u64) -> String {
    if secs == 0 {
        return NONE.to_string();
    }
    let (days, rest) = (secs / 86_400, secs % 86_400);
    // Days since 1970-01-01 to a date of the Gregorian calendar, counted in
    // 400-year eras that start on 1 March (Howard Hinnant's
    // `civil_from_days`).
    let z = days + 719_468;
    let (era, day_of_era) = (z / 146_097, z % 146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}Z",
        rest / 3_600,
        rest % 3_600 / 60,
        rest % 60
    )
}

pub fn health(format: OutputFormat, h: &HealthResponse) -> Result<String> {
    match format {
        OutputFormat::Json => pretty(h),
        OutputFormat::Text => Ok(aligned(&[
            ("status", esc(&h.status)),
            ("node_id", h.node_id.to_string()),
        ])),
    }
}

/// Used for `cluster status`, `init` and `join`, which all return the status.
pub fn status(format: OutputFormat, st: &ClusterStatus) -> Result<String> {
    if format == OutputFormat::Json {
        return pretty(st);
    }
    let joined = if st.joined { "yes" } else { "no" };
    let mut rows = vec![
        ("joined", joined.to_string()),
        ("node_id", st.node_id.to_string()),
        ("role", st.role.to_string()),
        ("term", st.term.to_string()),
        ("leader", leader(st.leader_id, st.leader_addr.as_deref())),
    ];
    if st.members.is_empty() {
        rows.push(("members", NONE.to_string()));
        return Ok(aligned(&rows));
    }
    let mut s = aligned(&rows);
    s.push_str("members:\n");
    let header = [
        "node_id",
        "name",
        "addr",
        "membership",
        "matched_index",
        "lag",
    ]
    .map(String::from);
    let members = st.members.iter().map(|m| {
        [
            m.node_id.to_string(),
            m.name.as_deref().map_or_else(none, esc),
            esc(&m.addr),
            if m.voter { "voter" } else { "learner" }.to_string(),
            opt(m.matched_index),
            opt(m.lag),
        ]
    });
    let all: Vec<[String; 6]> = std::iter::once(header).chain(members).collect();
    s.push_str(&table("  ", &all));
    Ok(s)
}

/// `(stdout, stderr)` for `mesh invite`. Text: the token alone on stdout and
/// the details on stderr, with a warning when the invite takes over a name
/// that a node holds. JSON: the whole response on stdout.
pub fn invite(format: OutputFormat, inv: &InviteResponse) -> Result<(String, String)> {
    if format == OutputFormat::Json {
        return Ok((pretty(inv)?, String::new()));
    }
    let mut details = aligned(&[
        ("name", esc(&inv.name)),
        ("node_id", inv.node_id.to_string()),
        ("mesh_ip", inv.mesh_ip.to_string()),
        ("expires_at", format!("{} (unix seconds)", inv.expires_at)),
    ]);
    if let Some(old) = inv.replaces_node_id {
        let _ = writeln!(
            details,
            "WARNING: this invite replaces node {old} ({}). When the new machine joins, \
             that node is removed from the cluster.",
            esc(&inv.name)
        );
    }
    Ok((format!("{}\n", esc(&inv.token)), details))
}

/// Seconds as whole days, hours, minutes and seconds, leaving out the units
/// that are zero: `8h`, `1h30m`, `45s`. For display only: the duration flags
/// take a single unit, so `1h30m` is not valid input.
fn duration(secs: u64) -> String {
    const UNITS: [(u64, char); 4] = [(24 * 60 * 60, 'd'), (60 * 60, 'h'), (60, 'm'), (1, 's')];
    if secs == 0 {
        return "0s".to_string();
    }
    let mut s = String::new();
    let mut rest = secs;
    for (size, unit) in UNITS {
        let n = rest / size;
        rest %= size;
        if n > 0 {
            let _ = write!(s, "{n}{unit}");
        }
    }
    s
}

/// Used for `mesh lease` with and without `--set`, which both return the
/// lease.
pub fn lease(format: OutputFormat, l: &LeaseResponse) -> Result<String> {
    match format {
        OutputFormat::Json => pretty(l),
        OutputFormat::Text => Ok(aligned(&[(
            "lease",
            format!("{} ({}s)", duration(l.lease_secs), l.lease_secs),
        )])),
    }
}

/// `mesh status-interval` with and without `--set`. The JSON key says what the
/// number is, unlike the wire DTO's `interval_secs`.
pub fn status_interval(format: OutputFormat, i: &StatusIntervalResponse) -> Result<String> {
    match format {
        OutputFormat::Json => pretty(&serde_json::json!({"status_interval_secs": i.interval_secs})),
        OutputFormat::Text => {
            let shown = duration(i.interval_secs);
            let value = if shown == format!("{}s", i.interval_secs) {
                shown
            } else {
                format!("{shown} ({}s)", i.interval_secs)
            };
            Ok(aligned(&[("status_interval", value)]))
        }
    }
}

/// `cluster key export`: the key alone in text, so `$(faber cluster key
/// export)` captures it.
///
/// The output is built in one zeroizing buffer sized up front, so no plain
/// copy of the key is made or left behind by a regrow. Canonical base64 has
/// nothing to escape in JSON.
pub fn cluster_key(format: OutputFormat, key: &str) -> Result<Zeroizing<String>> {
    const JSON_HEAD: &str = "{\n  \"cluster_key\": \"";
    const JSON_TAIL: &str = "\"\n}\n";
    let mut out = Zeroizing::new(String::with_capacity(
        JSON_HEAD.len() + key.len() + JSON_TAIL.len(),
    ));
    match format {
        OutputFormat::Json => {
            out.push_str(JSON_HEAD);
            out.push_str(key);
            out.push_str(JSON_TAIL);
        }
        OutputFormat::Text => {
            out.push_str(key);
            out.push('\n');
        }
    }
    Ok(out)
}

/// Names from the API separated by commas, or `-` for none.
fn names<'a>(names: impl IntoIterator<Item = &'a String>) -> String {
    let all: Vec<String> = names.into_iter().map(|n| esc(n)).collect();
    if all.is_empty() {
        NONE.to_string()
    } else {
        all.join(", ")
    }
}

/// Used for `project show`, `create` and `env add`, which all return the
/// project.
pub fn project(format: OutputFormat, p: &ProjectResponse) -> Result<String> {
    if format == OutputFormat::Json {
        return pretty(p);
    }
    let mut rows = vec![("name", esc(&p.name)), ("created", timestamp(p.created_at))];
    if p.environments.is_empty() {
        rows.push(("environments", NONE.to_string()));
        return Ok(aligned(&rows));
    }
    let mut s = aligned(&rows);
    s.push_str("environments:\n");
    let header = ["ENVIRONMENT", "WORKLOADS", "GENERATION", "UPDATED"].map(String::from);
    let environments = p.environments.iter().map(|e| {
        [
            esc(&e.name),
            e.workloads.to_string(),
            e.generation.to_string(),
            timestamp(e.updated_at),
        ]
    });
    let all: Vec<[String; 4]> = std::iter::once(header).chain(environments).collect();
    s.push_str(&table("  ", &all));
    Ok(s)
}

pub fn projects(format: OutputFormat, list: &ProjectListResponse) -> Result<String> {
    if format == OutputFormat::Json {
        return pretty(list);
    }
    let header = ["NAME", "ENVIRONMENTS", "WORKLOADS", "CREATED"].map(String::from);
    let projects = list.projects.iter().map(|p| {
        let workloads: u64 = p.environments.iter().map(|e| u64::from(e.workloads)).sum();
        [
            esc(&p.name),
            names(p.environments.iter().map(|e| &e.name)),
            workloads.to_string(),
            timestamp(p.created_at),
        ]
    });
    let all: Vec<[String; 4]> = std::iter::once(header).chain(projects).collect();
    Ok(table("", &all))
}

pub fn project_removed(format: OutputFormat, name: &str) -> Result<String> {
    match format {
        OutputFormat::Json => pretty(&serde_json::json!({ "removed": name })),
        OutputFormat::Text => Ok(format!("removed project {}\n", esc(name))),
    }
}

pub fn environment_removed(
    format: OutputFormat,
    project: &str,
    environment: &str,
) -> Result<String> {
    match format {
        OutputFormat::Json => {
            pretty(&serde_json::json!({ "project": project, "removed": environment }))
        }
        OutputFormat::Text => Ok(format!(
            "removed environment {} from {}\n",
            esc(environment),
            esc(project)
        )),
    }
}

fn scale(scale: Scale) -> String {
    match scale {
        Scale::Replicas { count, strategy } => {
            let strategy = match strategy {
                Strategy::Spread => "spread",
                Strategy::Pack => "pack",
            };
            format!("{count} ({strategy})")
        }
        Scale::Daemon => "daemon".to_string(),
    }
}

fn exposure(expose: Exposure) -> &'static str {
    match expose {
        Exposure::Environment => "environment",
        Exposure::Cluster => "cluster",
    }
}

/// `app ls`. The AVAILABLE column is the list's own `available/desired`.
pub fn workloads(format: OutputFormat, list: &WorkloadListResponse) -> Result<String> {
    if format == OutputFormat::Json {
        return pretty(list);
    }
    let header = [
        "NAME",
        "PROJECT",
        "ENVIRONMENT",
        "SCALE",
        "AVAILABLE",
        "EXPOSE",
        "FQDN",
    ]
    .map(String::from);
    let workloads = list.workloads.iter().map(|w| {
        [
            esc(&w.name),
            esc(&w.project),
            esc(&w.environment),
            scale(w.scale),
            format!("{}/{}", w.available, w.desired),
            exposure(w.expose).to_string(),
            esc(&w.fqdn),
        ]
    });
    let all: Vec<[String; 7]> = std::iter::once(header).chain(workloads).collect();
    Ok(table("", &all))
}

/// The `replicas:` block of `app show`.
fn replicas(replicas: &[ReplicaStatus]) -> String {
    if replicas.is_empty() {
        return "replicas: -\n".to_string();
    }
    let header = ["REPLICA", "NODE", "REVISION", "STATE", "POD IP", "REASON"].map(String::from);
    let rows = replicas.iter().map(|r| {
        let state = match r.state {
            ReplicaState::Deploying => "deploying",
            ReplicaState::Available => "available",
            ReplicaState::Unavailable => "unavailable",
        };
        [
            r.replica_index.to_string(),
            r.node.map_or_else(none, |n| n.to_string()),
            r.revision.to_string(),
            state.to_string(),
            r.pod_ip.map_or_else(none, |ip| ip.to_string()),
            r.reason.clone().unwrap_or_else(none),
        ]
    });
    let all: Vec<[String; 6]> = std::iter::once(header).chain(rows).collect();
    format!("replicas:\n{}", table("  ", &all))
}

/// `key: value` lines under `indent` for keys with any number of values:
/// none prints `-`, and each value after the first gets its own line under
/// the first.
fn fields(indent: &str, rows: &[(&str, Vec<String>)]) -> String {
    let width = rows.iter().map(|(k, _)| k.len() + 1).max().unwrap_or(0);
    let mut s = String::new();
    for (k, values) in rows {
        let mut key = format!("{k}:");
        if values.is_empty() {
            let _ = writeln!(s, "{indent}{key:<width$} {NONE}");
        }
        for v in values {
            let _ = writeln!(s, "{indent}{key:<width$} {v}");
            key.clear();
        }
    }
    s
}

/// Words of a command line on one line, or nothing for none. A word that is
/// empty or holds a space or a quote is put in double quotes, so the words
/// can still be told apart.
fn words(words: &[String]) -> Vec<String> {
    if words.is_empty() {
        return Vec::new();
    }
    let quoted: Vec<String> = words
        .iter()
        .map(|w| {
            let w = esc(w);
            if w.is_empty() || w.contains([' ', '"']) {
                format!("\"{}\"", w.replace('"', "\\\""))
            } else {
                w
            }
        })
        .collect();
    vec![quoted.join(" ")]
}

/// `http:8080/tcp`, without the name when the port has none.
fn port(p: &PortSpec) -> String {
    let protocol = match p.protocol {
        Protocol::Tcp => "tcp",
        Protocol::Udp => "udp",
    };
    match &p.name {
        Some(name) => format!("{}:{}/{protocol}", esc(name), p.port),
        None => format!("{}/{protocol}", p.port),
    }
}

/// What a volume is, in parts: `block`, `1073741824 bytes`.
fn volume(source: &VolumeSource) -> Vec<String> {
    let (kind, size_bytes) = match source {
        VolumeSource::Block { size_bytes } => ("block", size_bytes),
        VolumeSource::Ramdisk { size_bytes } => ("ramdisk", size_bytes),
    };
    vec![kind.to_string(), format!("{size_bytes} bytes")]
}

/// `data:/data (named data, read-only)`: the volume, where it is mounted,
/// and what the volume is.
fn mount(m: &MountSpec, volumes: &BTreeMap<String, VolumeSource>) -> String {
    let mut details = match volumes.get(&m.volume) {
        Some(source) => volume(source),
        None => vec!["unknown volume".to_string()],
    };
    if m.read_only {
        details.push("read-only".to_string());
    }
    format!(
        "{}:{} ({})",
        esc(&m.volume),
        esc(&m.path),
        details.join(", ")
    )
}

/// `cpu 500m, memory 268435456 bytes`: the most the container may use.
fn resources(r: &Resources) -> String {
    format!("cpu {}m, memory {} bytes", r.cpu_millis, r.memory_bytes)
}

/// What a probe does and how often. An HTTP probe shows the names of its
/// headers only: the values may be credentials.
fn probe(p: Option<&Probe>) -> Vec<String> {
    let Some(p) = p else {
        return Vec::new();
    };
    let action = match &p.action {
        ProbeAction::Http {
            port,
            path,
            scheme,
            host,
            headers,
        } => {
            let mut s = match scheme {
                HttpScheme::Http => "http".to_string(),
                HttpScheme::Https => "https".to_string(),
            };
            let _ = write!(s, " :{port}{}", esc(path));
            if let Some(host) = host {
                let _ = write!(s, ", host {}", esc(host));
            }
            if !headers.is_empty() {
                let _ = write!(s, ", headers {}", names(headers.keys()));
            }
            s
        }
        ProbeAction::Tcp { port } => format!("tcp :{port}"),
    };
    vec![format!(
        "{action} (interval {}s, timeout {}s)",
        p.interval_secs, p.timeout_secs
    )]
}

/// A value the image decides when the spec sets none.
fn image_default(value: Option<&str>) -> String {
    value.map_or_else(|| "(image default)".to_string(), esc)
}

fn yes_no(value: bool) -> String {
    if value { "yes" } else { "no" }.to_string()
}

fn container(c: &ContainerSpec, spec: &WorkloadSpec) -> String {
    let env = c
        .env
        .iter()
        .map(|(name, value)| format!("{}={}", esc(name), esc(value)))
        .collect();
    let mut s = format!("  {}:\n", esc(&c.name));
    s.push_str(&fields(
        "    ",
        &[
            ("image", vec![esc(&c.image)]),
            ("command", words(&c.command)),
            ("args", words(&c.args)),
            ("working_dir", vec![image_default(c.working_dir.as_deref())]),
            ("user", vec![image_default(c.user.as_deref())]),
            ("env", env),
            ("ports", c.ports.iter().map(port).collect()),
            (
                "mounts",
                c.mounts.iter().map(|m| mount(m, &spec.volumes)).collect(),
            ),
            ("resources", vec![resources(&c.resources)]),
            ("capabilities", vec![names(&c.capabilities)]),
            ("read_only_root_fs", vec![yes_no(c.read_only_root_fs)]),
            ("probe", probe(c.probe.as_ref())),
        ],
    ));
    s
}

/// `app show`. Text shows the env values; JSON is the response as it came.
pub fn workload(format: OutputFormat, w: &WorkloadResponse) -> Result<String> {
    if format == OutputFormat::Json {
        return pretty(w);
    }
    let spec = &w.spec;
    let restart = match spec.restart {
        RestartPolicy::Always => "always",
        RestartPolicy::OnFailure => "on-failure",
        RestartPolicy::Never => "never",
    };
    let mut s = aligned(&[
        ("name", esc(&w.name)),
        ("fqdn", esc(&w.fqdn)),
        ("project", esc(&w.project)),
        ("environment", esc(&w.environment)),
        ("revision", w.revision.to_string()),
        ("generation", w.generation.to_string()),
        ("scale", scale(spec.scale)),
        ("restart", restart.to_string()),
        ("expose", exposure(spec.expose).to_string()),
        ("egress", if spec.egress { "on" } else { "off" }.to_string()),
        ("nodes", names(&spec.placement.nodes)),
        (
            "stop_grace",
            spec.stop_grace_secs
                .map_or_else(|| "(runner default)".to_string(), |secs| format!("{secs}s")),
        ),
    ]);
    s.push_str(&replicas(&w.replicas));
    // Every volume, mounted or not: an unmounted one is still created.
    if spec.volumes.is_empty() {
        s.push_str("volumes: -\n");
    } else {
        s.push_str("volumes:\n");
        let names: Vec<String> = spec.volumes.keys().map(|name| esc(name)).collect();
        let rows: Vec<(&str, Vec<String>)> = names
            .iter()
            .zip(spec.volumes.values())
            .map(|(name, source)| (name.as_str(), vec![volume(source).join(", ")]))
            .collect();
        s.push_str(&fields("  ", &rows));
    }
    s.push_str("containers:\n");
    for c in &spec.containers {
        s.push_str(&container(c, spec));
    }
    Ok(s)
}

/// `app rm`: one line per removed workload.
pub fn workloads_removed(format: OutputFormat, fqdns: &[String]) -> Result<String> {
    match format {
        OutputFormat::Json => pretty(&serde_json::json!({ "removed": fqdns })),
        OutputFormat::Text => Ok(fqdns
            .iter()
            .map(|fqdn| format!("removed {}\n", esc(fqdn)))
            .collect()),
    }
}

/// The stderr lines of a deploy that failed: every error.
pub fn failure_text(errors: &[Diagnostic]) -> String {
    let mut s = String::new();
    for e in errors {
        let _ = writeln!(s, "error: {}", esc(&e.to_string()));
    }
    s
}

/// What a deploy says about itself, apart from its result.
pub struct DeployDetails<'a> {
    /// The files read. Only a dry run lists them.
    pub files: Option<&'a [String]>,
    pub notes: &'a [String],
}

impl DeployDetails<'_> {
    /// The stderr lines of text mode, which are written before anything is
    /// sent: `read: <path>` and `note: ...`, one each. JSON mode has them in its document.
    pub fn text(&self, format: OutputFormat) -> String {
        if format == OutputFormat::Json {
            return String::new();
        }
        let mut s = String::new();
        for file in self.files.unwrap_or_default() {
            let _ = writeln!(s, "read: {}", esc(file));
        }
        for n in self.notes {
            let _ = writeln!(s, "note: {}", esc(n));
        }
        s
    }

    /// The stderr line of JSON mode for a deploy that was sent and failed:
    /// one document on one line, because the document of stdout that would
    /// have had these is never written. Empty in text mode, which has
    /// printed them already.
    pub fn failure_line(&self, format: OutputFormat) -> Result<String> {
        #[derive(Serialize)]
        struct Details<'a> {
            notes: &'a [String],
        }
        if format != OutputFormat::Json {
            return Ok(String::new());
        }
        let mut line = serde_json::to_string(&Details { notes: self.notes })?;
        line.push('\n');
        Ok(line)
    }
}

/// The stdout of a deploy that was sent. Text: the result. JSON: one
/// document with the result and the details.
pub fn deployed(
    format: OutputFormat,
    project: &str,
    environment: &str,
    result: &DeployResponse,
    details: &DeployDetails<'_>,
) -> Result<String> {
    #[derive(Serialize)]
    struct Deployed<'a> {
        result: &'a DeployResponse,
        notes: &'a [String],
    }
    if format == OutputFormat::Json {
        return pretty(&Deployed {
            result,
            notes: details.notes,
        });
    }
    Ok(aligned(&[
        ("project", esc(project)),
        ("environment", esc(environment)),
        ("generation", result.generation.to_string()),
        ("created", names(&result.created)),
        ("updated", names(&result.updated)),
        ("unchanged", names(&result.unchanged)),
        ("removed", names(&result.removed)),
    ]))
}

/// The stdout of a deploy with `--dry-run`. Text: the workloads map, which
/// `deploy spec` reads back. JSON: one document with the map and the
/// details, the files read among them.
pub fn dry_run(
    format: OutputFormat,
    project: &str,
    environment: &str,
    spec: &BTreeMap<String, WorkloadSpec>,
    details: &DeployDetails<'_>,
) -> Result<String> {
    #[derive(Serialize)]
    struct DryRun<'a> {
        project: &'a str,
        environment: &'a str,
        spec: &'a BTreeMap<String, WorkloadSpec>,
        notes: &'a [String],
        files: &'a [String],
    }
    if format == OutputFormat::Json {
        return pretty(&DryRun {
            project,
            environment,
            spec,
            notes: details.notes,
            files: details.files.unwrap_or_default(),
        });
    }
    pretty(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_key_output_is_zeroized_and_keeps_its_shape() {
        let text: zeroize::Zeroizing<String> = cluster_key(OutputFormat::Text, "abc=").unwrap();
        assert_eq!(*text, "abc=\n");
        let json: zeroize::Zeroizing<String> = cluster_key(OutputFormat::Json, "abc=").unwrap();
        assert_eq!(*json, "{\n  \"cluster_key\": \"abc=\"\n}\n");
    }

    #[test]
    fn esc_replaces_control_characters_and_nothing_else() {
        assert_eq!(esc("plain text, ünïcode ✓"), "plain text, ünïcode ✓");
        assert_eq!(esc("a\x1b[31mb"), "a\\x1b[31mb");
        assert_eq!(
            esc("line\r\nnext\ttab\0"),
            "line\\x0d\\x0anext\\x09tab\\x00"
        );
        assert_eq!(esc("del\x7f"), "del\\x7f");
        // C1 controls: U+009B is a one-character CSI on some terminals.
        assert_eq!(esc("c1\u{9b}31m"), "c1\\u{9b}31m");
    }

    /// Characters that draw nothing: a bidi override shows `web-gro.exe` as
    /// `web-exe.org`, and a zero-width space makes two names look the same.
    #[test]
    fn esc_replaces_invisible_format_characters() {
        assert_eq!(esc("web-\u{202e}gro.exe"), "web-\\u{202e}gro.exe");
        assert_eq!(esc("pro\u{200b}d"), "pro\\u{200b}d");
        for c in [
            '\u{ad}',
            '\u{61c}',
            '\u{180e}',
            '\u{200b}',
            '\u{200c}',
            '\u{200d}',
            '\u{200e}',
            '\u{200f}',
            '\u{2028}',
            '\u{2029}',
            '\u{202a}',
            '\u{202b}',
            '\u{202c}',
            '\u{202d}',
            '\u{202e}',
            '\u{2060}',
            '\u{2066}',
            '\u{2067}',
            '\u{2068}',
            '\u{2069}',
            '\u{feff}',
            '\u{fff9}',
            '\u{e0001}',
            '\u{e0041}',
        ] {
            let text = format!("a{c}b");
            assert_eq!(esc(&text), format!("a\\u{{{:x}}}b", c as u32));
        }
        // Their neighbours, and text that only looks unusual, are left alone.
        for kept in [
            "\u{2010}",
            "\u{202f}",
            "\u{2070}",
            "\u{fffd}",
            "עברית",
            "日本語",
            "🚀",
        ] {
            assert_eq!(esc(kept), kept);
        }
    }

    #[test]
    fn timestamp_is_utc_and_zero_is_none() {
        for (secs, want) in [
            (0, "-"),
            (1, "1970-01-01 00:00:01Z"),
            (86_399, "1970-01-01 23:59:59Z"),
            (86_400, "1970-01-02 00:00:00Z"),
            (951_782_400, "2000-02-29 00:00:00Z"),
            (951_868_800, "2000-03-01 00:00:00Z"),
            (1_700_000_000, "2023-11-14 22:13:20Z"),
            (1_709_251_199, "2024-02-29 23:59:59Z"),
            (1_735_689_600, "2025-01-01 00:00:00Z"),
            (4_102_444_800, "2100-01-01 00:00:00Z"),
            (4_107_542_400, "2100-03-01 00:00:00Z"),
        ] {
            assert_eq!(timestamp(secs), want, "{secs}");
        }
    }

    #[test]
    fn timestamp_of_the_largest_value_does_not_overflow() {
        assert!(timestamp(u64::MAX).ends_with("07:00:15Z"));
    }

    #[test]
    fn table_takes_any_number_of_columns_and_an_indent() {
        let two = [["A", "BB"], ["ccc", "d"]].map(|row| row.map(String::from));
        assert_eq!(table("", &two), "A    BB\nccc  d\n");
        let three = [["x", "", "z"]].map(|row| row.map(String::from));
        assert_eq!(table("  ", &three), "  x    z\n");
        // Width counts characters, not bytes.
        let wide = [["é", "1"], ["ab", "2"]].map(|row| row.map(String::from));
        assert_eq!(table("", &wide), "é   1\nab  2\n");
    }

    /// An input file can put anything in a key or a value
    /// that ends up in an error or a detail.
    #[test]
    fn findings_escape_what_came_from_the_input() {
        let error = Diagnostic::new("f", "", "bad \x1b]0;title\x07");
        assert_eq!(failure_text(&[error]), "error: f: bad \\x1b]0;title\\x07\n");
        let details = DeployDetails {
            files: Some(&["a\nb.env".to_string()]),
            notes: &["note\x1b".to_string()],
        };
        assert_eq!(
            details.text(OutputFormat::Text),
            "read: a\\x0ab.env\n\
             note: note\\x1b\n"
        );
        assert_eq!(details.text(OutputFormat::Json), "");
    }

    /// The strings of the commands that were there before `deploy`.
    #[test]
    fn the_older_commands_escape_what_the_api_sent() {
        let h = HealthResponse {
            status: "o\x1bk".into(),
            node_id: 1,
        };
        assert!(
            health(OutputFormat::Text, &h)
                .unwrap()
                .contains("o\\x1bk\n")
        );
        assert_eq!(
            leader(Some(1), Some("10.0.0.1\x1b[2J")),
            "1@10.0.0.1\\x1b[2J"
        );
        assert_eq!(leader(None, Some("a\u{202e}b")), "-@a\\u{202e}b");
    }

    #[test]
    fn words_quotes_only_what_would_be_ambiguous() {
        assert_eq!(words(&[]), Vec::<String>::new());
        let line = words(&["sh", "-c", "echo \"hi\"", ""].map(String::from));
        assert_eq!(line, vec![r#"sh -c "echo \"hi\"" """#.to_string()]);
    }

    #[test]
    fn duration_uses_the_largest_whole_units() {
        for (secs, want) in [
            (0, "0s"),
            (1, "1s"),
            (59, "59s"),
            (60, "1m"),
            (90, "1m30s"),
            (300, "5m"),
            (2700, "45m"),
            (3600, "1h"),
            (5400, "1h30m"),
            (28_800, "8h"),
            (86_400, "1d"),
            (90_000, "1d1h"),
            (93_784, "1d2h3m4s"),
            (86_401, "1d1s"),
            (2_592_000, "30d"),
        ] {
            assert_eq!(duration(secs), want, "{secs}s");
        }
    }

    #[test]
    fn duration_of_the_largest_value_does_not_overflow() {
        assert_eq!(duration(u64::MAX), "213503982334601d7h15s");
    }
}
