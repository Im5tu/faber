//! The local half of `faber deploy`: finds and reads the input, converts it
//! to workload specs and checks the result. Nothing here talks to the API.
//!
//! Everything the conversion reads from the machine comes in through
//! [`Inputs`]: the directory relative paths start from, the variables of a
//! compose file, and stdin.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use faber_api_types::workload::{Exposure, VolumeSource, WorkloadSpec, validate_workloads};
use faber_workload_import::{
    ComposeInput, ConvertError, Diagnostic, Files, SourceDocument, compose, k8s,
};
use serde::de::{Deserialize, Deserializer, Error as _, MapAccess, Visitor};

/// The largest input file, and the most that is read from stdin: 8 MiB. A
/// deploy request holds far less.
const MAX_INPUT_BYTES: u64 = 8 * 1024 * 1024;

/// The names `deploy compose` looks for without a FILE, in this order.
pub const COMPOSE_FILE_NAMES: [&str; 4] = [
    "compose.yaml",
    "compose.yml",
    "docker-compose.yaml",
    "docker-compose.yml",
];

/// Files `docker compose` would merge into the compose file. Faber does not.
const COMPOSE_OVERRIDE_NAMES: [&str; 4] = [
    "compose.override.yaml",
    "compose.override.yml",
    "docker-compose.override.yaml",
    "docker-compose.override.yml",
];

/// File extensions of the manifests `deploy k8s` takes from a directory.
const MANIFEST_EXTENSIONS: [&str; 3] = ["yaml", "yml", "json"];

/// Parts of an env variable name that suggest its value is a secret.
const SECRET_NAME_PARTS: [&str; 6] = [
    "PASSWORD",
    "PASSWD",
    "SECRET",
    "TOKEN",
    "API_KEY",
    "PRIVATE_KEY",
];

/// What diagnostics about the converted workloads name as their source,
/// when the input was not already faber's own format.
const SPEC_SOURCE: &str = "spec";

const NOTE_NO_CAPABILITIES: &str = "containers get no Linux capabilities unless the file adds \
     them; images that expect Docker's default set may fail to start";

const NOTE_SECRET_NAMES: &str = "an env variable name looks like a secret; env values are \
     stored unencrypted in cluster state and are readable by local users on every node";

/// What a deploy reads from the machine it runs on.
pub struct Inputs {
    /// Relative paths start here, and `deploy compose` without a FILE looks
    /// here.
    pub cwd: PathBuf,
    /// The variables `${VAR}` in a compose file is replaced from.
    pub variables: BTreeMap<String, String>,
    /// Read for an input of `-`.
    pub stdin: Box<dyn Read>,
}

impl Inputs {
    /// The current directory, the environment and stdin of this process.
    /// Environment variables that are not UTF-8 are left out.
    pub fn from_process() -> Self {
        Self {
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            variables: std::env::vars_os()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                .collect(),
            stdin: Box::new(io::stdin()),
        }
    }
}

/// Where the workloads of a deploy come from.
pub(crate) enum Source<'a> {
    Compose {
        file: Option<&'a Path>,
        env_file: Option<&'a Path>,
        profiles: &'a [String],
        /// Read env files outside the directory of the compose file.
        allow_external_env_files: bool,
    },
    K8s {
        inputs: &'a [PathBuf],
    },
    Spec {
        file: &'a Path,
    },
}

/// A deploy that is ready to send.
#[derive(Debug)]
pub(crate) struct Plan {
    pub workloads: BTreeMap<String, WorkloadSpec>,
    /// What the conversion skipped, and what a node may refuse.
    pub warnings: Vec<Diagnostic>,
    /// Things to know that are no fault of the input. `--strict` ignores
    /// them.
    pub notes: Vec<String>,
    /// The names of the caller's variables whose value went into the
    /// workloads, sorted. Never the values.
    pub variables: Vec<String>,
    /// Every file the deploy read, in the order of the first read, with
    /// `-` for stdin. Each is the real path, with symlinks resolved, and
    /// relative to the current directory when it is below it.
    pub files: Vec<String>,
}

/// Why there is nothing to send: every error found, at least one.
#[derive(Debug)]
pub(crate) struct Failure {
    pub errors: Vec<Diagnostic>,
    pub warnings: Vec<Diagnostic>,
}

impl Failure {
    fn of(error: Diagnostic) -> Self {
        Self {
            errors: vec![error],
            warnings: Vec::new(),
        }
    }
}

impl From<ConvertError> for Failure {
    fn from(e: ConvertError) -> Self {
        Self {
            errors: e.errors,
            warnings: e.warnings,
        }
    }
}

/// Reads and converts `source`, sets `expose = cluster` on the workloads
/// named in `expose`, and validates the result. `prune` is refused when it
/// would remove workloads the input only left out.
pub(crate) fn plan(
    source: Source<'_>,
    expose: &[String],
    prune: bool,
    inputs: &mut Inputs,
) -> Result<Plan, Failure> {
    // The source that diagnostics about the result are filed under.
    let mut result_source = SPEC_SOURCE.to_string();
    let mut reader = Reader::new(&inputs.cwd);
    let mut variables = Vec::new();
    let mut skipped = Vec::new();
    let (mut workloads, mut warnings) = match source {
        Source::Compose {
            file,
            env_file,
            profiles,
            allow_external_env_files,
        } => {
            // Absolute, so a relative bind path in the file resolves to an
            // absolute one.
            let file = inputs.cwd.join(match file {
                Some(file) => file.to_path_buf(),
                None => find_compose_file(&inputs.cwd)?,
            });
            let mut warnings = override_warnings(&file);
            if !allow_external_env_files {
                // A file that is not there has no directory. The converter
                // reports it when it reads it.
                reader.within = compose_directories(&file);
            }
            let input = ComposeInput {
                file,
                variables: inputs.variables.clone(),
                profiles: profiles.to_vec(),
                env_file: env_file.map(|f| inputs.cwd.join(f)),
                allow_external_env_files,
            };
            match compose::convert(&input, &reader) {
                Ok(c) => {
                    warnings.extend(c.warnings);
                    variables = c.variables;
                    skipped = c.skipped;
                    (c.workloads, relative_sources(warnings, &inputs.cwd))
                }
                Err(e) => {
                    warnings.extend(e.warnings);
                    return Err(Failure {
                        errors: relative_sources(e.errors, &inputs.cwd),
                        warnings: relative_sources(warnings, &inputs.cwd),
                    });
                }
            }
        }
        Source::K8s { inputs: paths } => {
            let documents = read_manifests(paths, &reader, &mut inputs.stdin)?;
            let c = k8s::convert(&documents)?;
            (c.workloads, c.warnings)
        }
        Source::Spec { file } => {
            let mut stdin_used = false;
            let text = read_input(file, &reader, &mut inputs.stdin, &mut stdin_used)
                .map_err(Failure::of)?;
            result_source = file.display().to_string();
            let workloads = serde_json::from_str::<UniqueWorkloads>(&text).map_err(|e| {
                Failure::of(Diagnostic::new(
                    &result_source,
                    "",
                    format!("not a map of workload specs: {e}"),
                ))
            })?;
            (workloads.0, Vec::new())
        }
    };

    let mut errors = Vec::new();
    if workloads.is_empty() && !prune {
        errors.push(Diagnostic::new(
            &result_source,
            "",
            "nothing to deploy: the input has no workloads",
        ));
    } else if workloads.is_empty() {
        errors.push(Diagnostic::new(
            "--prune",
            "",
            "the input has no workloads, so every workload of the environment would be \
             removed; drop --prune, or remove them by name with `faber app rm`",
        ));
    } else if prune && !skipped.is_empty() {
        errors.push(Diagnostic::new(
            "--prune",
            "",
            format!(
                "these services were skipped by their profiles and would be removed: {}; \
                 pass their --profile, or drop --prune",
                skipped.join(", ")
            ),
        ));
    }
    for name in expose {
        match workloads.get_mut(name) {
            Some(spec) => spec.expose = Exposure::Cluster,
            None => {
                let known: Vec<&str> = workloads.keys().map(String::as_str).collect();
                errors.push(Diagnostic::new(
                    "--expose",
                    "",
                    format!(
                        "no workload named {name:?}; the input has: {}",
                        known.join(", ")
                    ),
                ));
            }
        }
    }
    if let Err(violations) = validate_workloads(&workloads) {
        errors.extend(
            violations
                .into_iter()
                .map(|v| Diagnostic::new(&result_source, v.path, v.message)),
        );
    }
    if !errors.is_empty() {
        return Err(Failure { errors, warnings });
    }

    warnings.extend(host_path_warnings(&workloads, &result_source));
    Ok(Plan {
        notes: notes(&workloads),
        workloads,
        warnings,
        variables,
        files: reader.read.into_inner(),
    })
}

/// The `workloads` map of `deploy spec`. Unlike a plain map it refuses a
/// name that comes twice, where JSON parsers keep the last and say nothing.
struct UniqueWorkloads(BTreeMap<String, WorkloadSpec>);

impl<'de> Deserialize<'de> for UniqueWorkloads {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Workloads;

        impl<'de> Visitor<'de> for Workloads {
            type Value = UniqueWorkloads;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object of workload specs by name")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut workloads = BTreeMap::new();
                while let Some(name) = map.next_key::<String>()? {
                    if workloads.contains_key(&name) {
                        return Err(A::Error::custom(format!(
                            "the workload {name:?} is defined twice"
                        )));
                    }
                    workloads.insert(name, map.next_value()?);
                }
                Ok(UniqueWorkloads(workloads))
            }
        }

        deserializer.deserialize_map(Workloads)
    }
}

/// The real file system, and the only way a deploy reads from it.
///
/// A path is resolved to its real one first, so a symlink cannot hide where
/// a file is. Only regular files of at most [`MAX_INPUT_BYTES`] are read:
/// no device, FIFO or directory. Every file read is kept in `read`.
struct Reader {
    /// A relative path starts here.
    cwd: PathBuf,
    /// The real path of `cwd`, which the files read are shown relative to.
    real_cwd: PathBuf,
    /// When set, a file whose real path is below none of these directories
    /// is refused: the directories of the compose file. The compose
    /// converter makes the same check on the text of a path, and cannot
    /// see a symlink that leads out.
    within: Option<Vec<PathBuf>>,
    read: RefCell<Vec<String>>,
}

impl Reader {
    fn new(cwd: &Path) -> Self {
        Self {
            cwd: cwd.to_path_buf(),
            real_cwd: fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf()),
            within: None,
            read: RefCell::default(),
        }
    }

    fn file(&self, path: &Path) -> io::Result<String> {
        let real = fs::canonicalize(self.cwd.join(path))?;
        let shown = match real.strip_prefix(&self.real_cwd) {
            Ok(below) => below.display().to_string(),
            Err(_) => plain(&real),
        };
        if self
            .within
            .as_ref()
            .is_some_and(|dirs| !dirs.iter().any(|dir| real.starts_with(dir)))
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "it is `{}`, which is outside the directory of the compose file; pass \
                     --allow-external-env-file to read it",
                    plain(&real)
                ),
            ));
        }
        // Checked before the file is opened, because opening a FIFO blocks,
        // and again on the open file, which is the one that is read.
        check_regular(&fs::metadata(&real)?)?;
        let file = fs::File::open(&real)?;
        check_regular(&file.metadata()?)?;
        let text = read_capped(file)?;
        self.record(shown);
        Ok(text)
    }

    fn stdin(&self, stdin: &mut dyn Read) -> io::Result<String> {
        let text = read_capped(stdin)?;
        self.record("-".to_string());
        Ok(text)
    }

    fn record(&self, shown: String) {
        let mut read = self.read.borrow_mut();
        if !read.contains(&shown) {
            read.push(shown);
        }
    }
}

/// The real directories a compose file is in: the one its path names and,
/// when the file is a symlink, the one the real file is in. `None` for a
/// file that is not there, which the converter reports when it reads it.
fn compose_directories(file: &Path) -> Option<Vec<PathBuf>> {
    let real = fs::canonicalize(file).ok()?;
    let mut dirs = vec![real.parent()?.to_path_buf()];
    if let Some(named) = file.parent().and_then(|dir| fs::canonicalize(dir).ok())
        && !dirs.contains(&named)
    {
        dirs.push(named);
    }
    Some(dirs)
}

impl Files for Reader {
    fn read(&self, path: &Path) -> io::Result<String> {
        self.file(path)
    }
}

/// A real path as a person writes it: without the `\\?\` that Windows puts
/// in front of one.
fn plain(real: &Path) -> String {
    let text = real.display().to_string();
    match text.strip_prefix(r"\\?\") {
        Some(rest) if !rest.starts_with("UNC\\") => rest.to_string(),
        _ => text,
    }
}

fn check_regular(metadata: &fs::Metadata) -> io::Result<()> {
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    if metadata.len() > MAX_INPUT_BYTES {
        return Err(too_large());
    }
    Ok(())
}

fn too_large() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "larger than 8 MiB")
}

/// All of `reader` as text, unless it holds more than [`MAX_INPUT_BYTES`].
fn read_capped(reader: impl Read) -> io::Result<String> {
    let mut text = String::new();
    reader.take(MAX_INPUT_BYTES + 1).read_to_string(&mut text)?;
    if text.len() as u64 > MAX_INPUT_BYTES {
        return Err(too_large());
    }
    Ok(text)
}

/// The first of [`COMPOSE_FILE_NAMES`] that is a file in `cwd`.
fn find_compose_file(cwd: &Path) -> Result<PathBuf, Failure> {
    COMPOSE_FILE_NAMES
        .iter()
        .find(|name| cwd.join(name).is_file())
        .map(PathBuf::from)
        .ok_or_else(|| {
            Failure::of(Diagnostic::new(
                slashes(cwd),
                "",
                format!(
                    "no compose file here; looked for {}",
                    COMPOSE_FILE_NAMES.join(", ")
                ),
            ))
        })
}

/// A path as the compose converter writes its sources: with `/` between
/// the parts on every platform.
fn slashes(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// `found` with every source below `cwd` named relative to it, the way the
/// caller would name the file. The converter got absolute paths.
fn relative_sources(mut found: Vec<Diagnostic>, cwd: &Path) -> Vec<Diagnostic> {
    let prefix = format!("{}/", slashes(cwd).trim_end_matches('/'));
    for finding in &mut found {
        if let Some(below) = finding.source.strip_prefix(&prefix) {
            finding.source = below.to_string();
        }
    }
    found
}

/// One warning per override file beside the compose file `file`.
fn override_warnings(file: &Path) -> Vec<Diagnostic> {
    let dir = file.parent().unwrap_or(Path::new(""));
    let read = file.file_name().unwrap_or(file.as_os_str());
    COMPOSE_OVERRIDE_NAMES
        .iter()
        .map(|name| dir.join(name))
        .filter(|path| path.is_file())
        .map(|path| {
            Diagnostic::new(
                slashes(&path),
                "",
                format!(
                    "not merged: only {} is read; `docker compose config` writes the merged file",
                    read.to_string_lossy()
                ),
            )
        })
        .collect()
}

/// The text of the input `path`: a file, or stdin for `-`, which may be
/// read once.
fn read_input(
    path: &Path,
    reader: &Reader,
    stdin: &mut dyn Read,
    stdin_used: &mut bool,
) -> Result<String, Diagnostic> {
    let source = path.display().to_string();
    let read = if path == Path::new("-") {
        if std::mem::replace(stdin_used, true) {
            return Err(Diagnostic::new(source, "", "stdin can be read only once"));
        }
        reader.stdin(stdin)
    } else {
        reader.file(path)
    };
    read.map_err(|e| Diagnostic::new(source, "", format!("cannot read: {e}")))
}

/// The inputs of `deploy k8s` as documents, in the order given. A directory
/// gives its `*.yaml`, `*.yml` and `*.json` files sorted by name, without
/// its subdirectories. Every input that cannot be read is reported.
fn read_manifests(
    paths: &[PathBuf],
    reader: &Reader,
    stdin: &mut dyn Read,
) -> Result<Vec<SourceDocument>, Failure> {
    let mut documents = Vec::new();
    let mut errors = Vec::new();
    let mut stdin_used = false;
    for path in paths {
        let files = if reader.cwd.join(path).is_dir() {
            match manifests_in(&reader.cwd, path) {
                Ok(files) => files,
                Err(e) => {
                    errors.push(e);
                    continue;
                }
            }
        } else {
            vec![path.clone()]
        };
        for file in files {
            match read_input(&file, reader, stdin, &mut stdin_used) {
                Ok(text) => documents.push(SourceDocument {
                    source: file.display().to_string(),
                    text,
                }),
                Err(e) => errors.push(e),
            }
        }
    }
    if errors.is_empty() {
        Ok(documents)
    } else {
        Err(Failure {
            errors,
            warnings: Vec::new(),
        })
    }
}

/// The manifest files directly in the directory `dir`, sorted by name, each
/// as `dir` joined with the name.
fn manifests_in(cwd: &Path, dir: &Path) -> Result<Vec<PathBuf>, Diagnostic> {
    let source = dir.display().to_string();
    let mut names = Vec::new();
    let list = |e: io::Error| Diagnostic::new(&source, "", format!("cannot list: {e}"));
    for entry in fs::read_dir(cwd.join(dir)).map_err(list)? {
        let entry = entry.map_err(list)?;
        let is_manifest = Path::new(&entry.file_name())
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| MANIFEST_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()));
        if is_manifest && entry.path().is_file() {
            names.push(entry.file_name());
        }
    }
    if names.is_empty() {
        return Err(Diagnostic::new(
            source,
            "",
            "no .yaml, .yml or .json file in this directory",
        ));
    }
    names.sort();
    Ok(names.into_iter().map(|name| dir.join(name)).collect())
}

/// One warning per host-path volume: a node refuses it unless the node's
/// own allow-list covers the path, which the CLI cannot know.
fn host_path_warnings(workloads: &BTreeMap<String, WorkloadSpec>, source: &str) -> Vec<Diagnostic> {
    let mut warnings = Vec::new();
    for (workload, spec) in workloads {
        for (volume, volume_source) in &spec.volumes {
            if let VolumeSource::HostPath { path, .. } = volume_source {
                warnings.push(Diagnostic::new(
                    source,
                    format!("{workload}.volumes.{volume}"),
                    format!(
                        "host path {path}: host paths are refused by nodes unless allowed in \
                         the node's host-path allow-list"
                    ),
                ));
            }
        }
    }
    warnings
}

fn notes(workloads: &BTreeMap<String, WorkloadSpec>) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(dir: &Path, name: &str) {
        fs::write(dir.join(name), format!("# {name}\n")).unwrap();
    }

    #[test]
    fn compose_lookup_takes_the_first_name_in_the_documented_order() {
        let dir = tempfile::tempdir().unwrap();
        // Added from the last name to the first: each one takes over.
        for name in COMPOSE_FILE_NAMES.iter().rev() {
            touch(dir.path(), name);
            assert_eq!(
                find_compose_file(dir.path()).unwrap(),
                PathBuf::from(name),
                "after adding {name}"
            );
        }
    }

    #[test]
    fn compose_lookup_without_a_file_lists_the_four_names() {
        let dir = tempfile::tempdir().unwrap();
        // A directory with the right name is not a compose file.
        fs::create_dir(dir.path().join("compose.yaml")).unwrap();
        let failure = find_compose_file(dir.path()).unwrap_err();
        assert_eq!(failure.errors.len(), 1);
        assert_eq!(
            failure.errors[0].to_string(),
            format!(
                "{}: no compose file here; looked for compose.yaml, compose.yml, \
                 docker-compose.yaml, docker-compose.yml",
                slashes(dir.path())
            )
        );
    }

    #[test]
    fn every_override_file_beside_the_compose_file_gets_a_warning() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        let file = dir.path().join("compose.yaml");
        assert_eq!(override_warnings(&file), vec![]);

        touch(dir.path(), "compose.override.yaml");
        touch(dir.path(), "docker-compose.override.yml");
        // Beside another compose file, not this one.
        touch(&sub, "compose.override.yml");
        let beside = |dir: &Path, name: &str, read: &str| {
            Diagnostic::new(
                slashes(&dir.join(name)),
                "",
                format!(
                    "not merged: only {read} is read; `docker compose config` writes the \
                     merged file"
                ),
            )
        };
        assert_eq!(
            override_warnings(&file),
            [
                beside(dir.path(), "compose.override.yaml", "compose.yaml"),
                beside(dir.path(), "docker-compose.override.yml", "compose.yaml"),
            ]
        );
        assert_eq!(
            override_warnings(&sub.join("stack.yml")),
            [beside(&sub, "compose.override.yml", "stack.yml")]
        );
    }

    #[test]
    fn sources_below_the_current_directory_are_named_relative_to_it() {
        let cwd = Path::new("/work/shop");
        let found = ["/work/shop/compose.yaml", "/work/shop/env/prod.env"]
            .into_iter()
            // Not below it: a sibling with the same start, and its parent.
            .chain(["/work/shop-2/compose.yaml", "/work/.env", "compose.yaml"])
            .map(|source| Diagnostic::new(source, "services", "m"))
            .collect();
        let sources: Vec<String> = relative_sources(found, cwd)
            .into_iter()
            .map(|d| d.source)
            .collect();
        assert_eq!(
            sources,
            [
                "compose.yaml",
                "env/prod.env",
                "/work/shop-2/compose.yaml",
                "/work/.env",
                "compose.yaml",
            ]
        );
    }

    fn stdin(text: &str) -> io::Cursor<Vec<u8>> {
        io::Cursor::new(text.as_bytes().to_vec())
    }

    fn files_read(reader: Reader) -> Vec<String> {
        reader.read.into_inner()
    }

    /// The real path of `path` as the list of files read shows it.
    fn real(path: &Path) -> String {
        plain(&fs::canonicalize(path).unwrap())
    }

    #[test]
    fn a_directory_gives_its_manifests_sorted_by_name_and_not_recursively() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        fs::create_dir(&manifests).unwrap();
        for name in ["b.yml", "c.json", "a.yaml", "D.YAML", "notes.txt", "yaml"] {
            touch(&manifests, name);
        }
        fs::create_dir(manifests.join("nested.yaml")).unwrap();
        touch(&manifests.join("nested.yaml"), "z.yaml");
        touch(dir.path(), "single.yaml");

        let paths = [PathBuf::from("single.yaml"), PathBuf::from("manifests")];
        let reader = Reader::new(dir.path());
        let documents = read_manifests(&paths, &reader, &mut stdin("")).unwrap();
        let sources: Vec<&str> = documents.iter().map(|d| d.source.as_str()).collect();
        let in_dir = |name: &str| Path::new("manifests").join(name).display().to_string();
        let want = [
            "single.yaml".to_string(),
            in_dir("D.YAML"),
            in_dir("a.yaml"),
            in_dir("b.yml"),
            in_dir("c.json"),
        ];
        assert_eq!(sources, want);
        assert_eq!(documents[0].text, "# single.yaml\n");
        assert_eq!(documents[2].text, "# a.yaml\n");
        assert_eq!(files_read(reader), want);
    }

    #[test]
    fn a_dash_is_stdin_and_keeps_its_place_among_the_inputs() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "a.yaml");
        let paths = [PathBuf::from("-"), PathBuf::from("a.yaml")];
        let reader = Reader::new(dir.path());
        let documents = read_manifests(&paths, &reader, &mut stdin("kind: Pod\n")).unwrap();
        assert_eq!(
            documents,
            [
                SourceDocument {
                    source: "-".into(),
                    text: "kind: Pod\n".into(),
                },
                SourceDocument {
                    source: "a.yaml".into(),
                    text: "# a.yaml\n".into(),
                },
            ]
        );
        assert_eq!(files_read(reader), ["-", "a.yaml"]);
    }

    #[test]
    fn every_unreadable_input_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("empty")).unwrap();
        touch(&dir.path().join("empty"), "readme.md");
        let paths = ["missing.yaml", "empty", "-", "-"].map(PathBuf::from);
        let reader = Reader::new(dir.path());
        let failure = read_manifests(&paths, &reader, &mut stdin("")).unwrap_err();
        let errors: Vec<String> = failure.errors.iter().map(ToString::to_string).collect();
        assert_eq!(errors.len(), 3, "{errors:?}");
        assert!(
            errors[0].starts_with("missing.yaml: cannot read: "),
            "{errors:?}"
        );
        assert_eq!(
            errors[1],
            "empty: no .yaml, .yml or .json file in this directory"
        );
        assert_eq!(errors[2], "-: stdin can be read only once");
    }

    #[test]
    fn an_absolute_input_path_is_read_as_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        touch(elsewhere.path(), "a.yaml");
        let file = elsewhere.path().join("a.yaml");
        let reader = Reader::new(dir.path());
        let documents =
            read_manifests(std::slice::from_ref(&file), &reader, &mut stdin("")).unwrap();
        assert_eq!(documents[0].source, file.display().to_string());
        assert_eq!(documents[0].text, "# a.yaml\n");
        // Not below the current directory, so not made relative.
        assert_eq!(files_read(reader), [real(&file)]);
    }

    #[test]
    fn files_are_read_relative_to_the_current_directory() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), ".env");
        let reader = Reader::new(dir.path());
        assert_eq!(reader.read(Path::new(".env")).unwrap(), "# .env\n");
        assert_eq!(reader.read(&dir.path().join(".env")).unwrap(), "# .env\n");
        let missing = reader.read(Path::new("other.env")).unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);
        // Read twice and listed once; the file that is not there is not
        // listed.
        assert_eq!(files_read(reader), [".env"]);
    }

    /// Reading a directory would fail anyway; a device or a FIFO would not,
    /// and could block or never end.
    #[test]
    fn only_regular_files_are_read() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("sub.yaml")).unwrap();
        let reader = Reader::new(dir.path());
        let refused = reader.file(Path::new("sub.yaml")).unwrap_err();
        assert_eq!(refused.to_string(), "not a regular file");

        let diagnostic =
            read_input(Path::new("sub.yaml"), &reader, &mut stdin(""), &mut false).unwrap_err();
        assert_eq!(
            diagnostic.to_string(),
            "sub.yaml: cannot read: not a regular file"
        );
        assert_eq!(files_read(reader), Vec::<String>::new());
    }

    #[cfg(unix)]
    #[test]
    fn devices_and_fifos_are_refused_without_being_opened() {
        let dir = tempfile::tempdir().unwrap();
        let reader = Reader::new(dir.path());
        for device in ["/dev/zero", "/dev/null"] {
            let refused = reader.file(Path::new(device)).unwrap_err();
            assert_eq!(refused.to_string(), "not a regular file", "{device}");
        }
        // A FIFO with no writer: opening it would block for ever.
        let fifo = dir.path().join("fifo.yaml");
        let made = std::process::Command::new("mkfifo").arg(&fifo).status();
        if made.is_ok_and(|status| status.success()) {
            let refused = reader.file(&fifo).unwrap_err();
            assert_eq!(refused.to_string(), "not a regular file");
        }
    }

    #[test]
    fn a_file_over_the_size_limit_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        // Sparse: the length is set, nothing is written.
        let at_limit = fs::File::create(dir.path().join("limit.yaml")).unwrap();
        at_limit.set_len(MAX_INPUT_BYTES).unwrap();
        let over = fs::File::create(dir.path().join("over.yaml")).unwrap();
        over.set_len(MAX_INPUT_BYTES + 1).unwrap();

        let reader = Reader::new(dir.path());
        let text = reader.file(Path::new("limit.yaml")).unwrap();
        assert_eq!(text.len() as u64, MAX_INPUT_BYTES);
        let refused = reader.file(Path::new("over.yaml")).unwrap_err();
        assert_eq!(refused.to_string(), "larger than 8 MiB");
        assert_eq!(files_read(reader), ["limit.yaml"]);
    }

    #[test]
    fn stdin_over_the_size_limit_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let reader = Reader::new(dir.path());
        let mut at_limit = io::repeat(b' ').take(MAX_INPUT_BYTES);
        assert!(reader.stdin(&mut at_limit).is_ok());

        // Endless input stops at the limit.
        let refused = reader.stdin(&mut io::repeat(b' ')).unwrap_err();
        assert_eq!(refused.to_string(), "larger than 8 MiB");
        let diagnostic =
            read_input(Path::new("-"), &reader, &mut io::repeat(b' '), &mut false).unwrap_err();
        assert_eq!(diagnostic.to_string(), "-: cannot read: larger than 8 MiB");
    }

    /// With `within` set, as it is for a compose file, a file whose real
    /// path is elsewhere is refused, and the error names it and the flag.
    #[test]
    fn a_file_outside_the_compose_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        fs::create_dir_all(project.join("env")).unwrap();
        touch(&project, "compose.yaml");
        touch(&project.join("env"), "web.env");
        touch(dir.path(), "outside.env");

        let mut reader = Reader::new(&project);
        reader.within = Some(vec![fs::canonicalize(&project).unwrap()]);
        assert!(reader.file(Path::new("compose.yaml")).is_ok());
        assert!(reader.file(Path::new("env/web.env")).is_ok());
        for path in [Path::new("../outside.env"), &dir.path().join("outside.env")] {
            let refused = reader.file(path).unwrap_err();
            assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
            assert_eq!(
                refused.to_string(),
                format!(
                    "it is `{}`, which is outside the directory of the compose file; pass \
                     --allow-external-env-file to read it",
                    real(&dir.path().join("outside.env"))
                )
            );
        }
        // A sibling whose name starts like the directory is not inside it.
        fs::create_dir(dir.path().join("project-2")).unwrap();
        touch(&dir.path().join("project-2"), "x.env");
        assert!(reader.file(Path::new("../project-2/x.env")).is_err());

        reader.within = None;
        assert!(reader.file(Path::new("../outside.env")).is_ok());
    }

    /// The case only the real path shows: the path is inside the directory
    /// and the file is not.
    #[cfg(unix)]
    #[test]
    fn a_symlink_that_leads_out_of_the_compose_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        fs::create_dir(&project).unwrap();
        fs::write(dir.path().join("secrets"), "KEY=1\n").unwrap();
        std::os::unix::fs::symlink(dir.path().join("secrets"), project.join(".env")).unwrap();
        fs::write(project.join("real.env"), "A=1\n").unwrap();
        std::os::unix::fs::symlink(project.join("real.env"), project.join("link.env")).unwrap();

        let mut reader = Reader::new(&project);
        reader.within = Some(vec![fs::canonicalize(&project).unwrap()]);
        let refused = reader.file(Path::new(".env")).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
        assert!(refused.to_string().contains("/secrets`"), "{refused}");
        // A symlink that stays inside is read, and listed by its real name.
        assert_eq!(reader.file(Path::new("link.env")).unwrap(), "A=1\n");
        assert_eq!(files_read(reader), ["real.env"]);
    }

    /// A compose file that is a symlink has two directories, and a file in
    /// either is beside it.
    #[cfg(unix)]
    #[test]
    fn a_linked_compose_file_is_in_the_directory_of_the_link_and_of_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let (app, shared) = (dir.path().join("app"), dir.path().join("shared"));
        fs::create_dir(&app).unwrap();
        fs::create_dir(&shared).unwrap();
        touch(&shared, "compose.yaml");
        std::os::unix::fs::symlink(shared.join("compose.yaml"), app.join("compose.yaml")).unwrap();
        let real_dir = |dir: &Path| fs::canonicalize(dir).unwrap();

        assert_eq!(
            compose_directories(&app.join("compose.yaml")),
            Some(vec![real_dir(&shared), real_dir(&app)])
        );
        assert_eq!(
            compose_directories(&shared.join("compose.yaml")),
            Some(vec![real_dir(&shared)])
        );
        assert_eq!(compose_directories(&app.join("missing.yaml")), None);
    }

    #[test]
    fn a_spec_with_a_workload_defined_twice_is_refused() {
        let spec = r#"{"containers": [{"name": "c", "image": "nginx"}]}"#;
        let once = format!(r#"{{"web": {spec}, "db": {spec}}}"#);
        let parsed: UniqueWorkloads = serde_json::from_str(&once).unwrap();
        assert_eq!(parsed.0.keys().collect::<Vec<_>>(), ["db", "web"]);

        let twice = format!(r#"{{"web": {spec}, "db": {spec}, "web": {spec}}}"#);
        let error = serde_json::from_str::<UniqueWorkloads>(&twice)
            .err()
            .expect("a duplicate is an error");
        assert!(
            error
                .to_string()
                .starts_with("the workload \"web\" is defined twice"),
            "{error}"
        );
        // The same name spelt with an escape is still the same name.
        let escaped = format!(r#"{{"web": {spec}, "w\u0065b": {spec}}}"#);
        assert!(serde_json::from_str::<UniqueWorkloads>(&escaped).is_err());
        for not_a_map in ["[]", "null", "\"web\""] {
            assert!(serde_json::from_str::<UniqueWorkloads>(not_a_map).is_err());
        }
    }

    #[test]
    fn a_windows_real_path_is_shown_without_its_prefix() {
        assert_eq!(plain(Path::new(r"\\?\C:\work\a.yaml")), r"C:\work\a.yaml");
        assert_eq!(
            plain(Path::new(r"\\?\UNC\server\share\a.yaml")),
            r"\\?\UNC\server\share\a.yaml"
        );
        assert_eq!(plain(Path::new("/work/a.yaml")), "/work/a.yaml");
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
}
