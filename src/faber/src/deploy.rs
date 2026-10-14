//! The local half of `faber deploy`: reads the input, decodes it to workload
//! specs and checks the result. Nothing here talks to the API.
//!
//! Everything a deploy reads from the machine comes in through [`Inputs`]:
//! the directory relative paths start from, and stdin.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use faber_api_types::workload::{Exposure, VolumeSource, WorkloadSpec, validate_workloads};
use serde::de::{Deserialize, Deserializer, Error as _, MapAccess, Visitor};

/// The largest input file, and the most that is read from stdin: 8 MiB. A
/// deploy request holds far less.
const MAX_INPUT_BYTES: u64 = 8 * 1024 * 1024;

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

const NOTE_SECRET_NAMES: &str = "an env variable name looks like a secret; env values are \
     stored unencrypted in cluster state and are readable by local users on every node";

/// What a deploy reads from the machine it runs on.
pub struct Inputs {
    /// Relative paths start here.
    pub cwd: PathBuf,
    /// Read for an input of `-`.
    pub stdin: Box<dyn Read>,
}

impl Inputs {
    /// The current directory and stdin of this process.
    pub fn from_process() -> Self {
        Self {
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            stdin: Box::new(io::stdin()),
        }
    }
}

/// One finding about the input: an error or a warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// The file the finding is about, or `-` for stdin.
    pub source: String,
    /// Where in that file. Empty for the whole file.
    pub path: String,
    pub message: String,
}

impl Diagnostic {
    pub fn new(
        source: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            source: source.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Diagnostic {
    /// `<source>: <path>: <message>`, without the path when it is empty.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            write!(f, "{}: {}", self.source, self.message)
        } else {
            write!(f, "{}: {}: {}", self.source, self.path, self.message)
        }
    }
}

/// A deploy that is ready to send.
#[derive(Debug)]
pub(crate) struct Plan {
    pub workloads: BTreeMap<String, WorkloadSpec>,
    /// What a node may refuse.
    pub warnings: Vec<Diagnostic>,
    /// Things to know that are no fault of the input. `--strict` ignores
    /// them.
    pub notes: Vec<String>,
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

/// Reads the workloads of the spec `file` (`-` for stdin), sets
/// `expose = cluster` on the workloads named in `expose`, and validates the
/// result. `prune` is refused when the input has no workloads.
pub(crate) fn plan(
    file: &Path,
    expose: &[String],
    prune: bool,
    inputs: &mut Inputs,
) -> Result<Plan, Failure> {
    let reader = Reader::new(&inputs.cwd);
    let result_source = file.display().to_string();
    let text = read_input(file, &reader, &mut inputs.stdin, &mut false).map_err(Failure::of)?;
    let mut workloads = serde_json::from_str::<UniqueWorkloads>(&text)
        .map_err(|e| {
            Failure::of(Diagnostic::new(
                &result_source,
                "",
                format!("not a map of workload specs: {e}"),
            ))
        })?
        .0;
    let mut warnings = Vec::new();

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
    read: RefCell<Vec<String>>,
}

impl Reader {
    fn new(cwd: &Path) -> Self {
        Self {
            cwd: cwd.to_path_buf(),
            real_cwd: fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf()),
            read: RefCell::default(),
        }
    }

    fn file(&self, path: &Path) -> io::Result<String> {
        let real = fs::canonicalize(self.cwd.join(path))?;
        let shown = match real.strip_prefix(&self.real_cwd) {
            Ok(below) => below.display().to_string(),
            Err(_) => plain(&real),
        };
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

    fn stdin(text: &str) -> io::Cursor<Vec<u8>> {
        io::Cursor::new(text.as_bytes().to_vec())
    }

    fn files_read(reader: Reader) -> Vec<String> {
        reader.read.into_inner()
    }

    #[test]
    fn files_are_read_relative_to_the_current_directory() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), ".env");
        let reader = Reader::new(dir.path());
        assert_eq!(reader.file(Path::new(".env")).unwrap(), "# .env\n");
        assert_eq!(reader.file(&dir.path().join(".env")).unwrap(), "# .env\n");
        let missing = reader.file(Path::new("other.env")).unwrap_err();
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
