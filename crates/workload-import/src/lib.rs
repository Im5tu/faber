//! Converts compose files and Kubernetes manifests into faber workload
//! specs.
//!
//! The crate does no I/O and reads no environment. The caller passes the
//! text, the variables and a [`Files`] to read further files through, so
//! every file a conversion touches is one the caller handed over.
//!
//! A conversion never drops input silently. Each key is mapped, ignored on
//! purpose, skipped with a warning, or refused with an error, and a key no
//! converter knows is an error with its path ([`walk`]). Errors are
//! collected, not first-fail: [`ConvertError`] holds every one of them.
//!
//! The result has no project and no environment. A deploy names both, and
//! they are not part of a spec.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use faber_api_types::workload::WorkloadSpec;

pub mod compose;
pub mod envfile;
pub mod interpolate;
pub mod k8s;
pub mod units;
pub mod walk;
pub mod yaml;

/// How a converter reads the files its input refers to: the compose file
/// itself, an `env_file`, the `.env` file next to it.
///
/// The converter does not touch the file system. It resolves a relative
/// path against the directory of the compose file and passes the result
/// here, so an implementation sees every path that is read and can list
/// or refuse it.
pub trait Files {
    /// The content of the file at `path` as UTF-8 text.
    fn read(&self, path: &Path) -> io::Result<String>;
}

/// Files held in memory, by exact path. A path that is not a key is
/// `NotFound`.
impl Files for BTreeMap<PathBuf, String> {
    fn read(&self, path: &Path) -> io::Result<String> {
        self.get(path).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}: no such file", path.display()),
            )
        })
    }
}

/// One finding about the input: an error or a warning.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Diagnostic {
    /// The file or document the finding is about: `compose.yaml`,
    /// `app.yaml#2`, `-`.
    pub source: String,
    /// Where in that document: `services.web.build`,
    /// `spec.template.spec.containers[0].image`. Empty for the whole
    /// document.
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

/// Errors and warnings collected during a conversion.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diagnostics {
    pub errors: Vec<Diagnostic>,
    pub warnings: Vec<Diagnostic>,
}

impl Diagnostics {
    /// Adds everything in `other`, after what is already here.
    pub fn extend(&mut self, other: Diagnostics) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
    }

    /// The result of a conversion that produced `workloads`: a
    /// [`Conversion`] when there is no error, else a [`ConvertError`]. The
    /// warnings go into either. `variables` and `skipped` of the
    /// conversion start empty; the compose converter fills them in.
    pub fn finish(
        self,
        workloads: BTreeMap<String, WorkloadSpec>,
    ) -> Result<Conversion, ConvertError> {
        if self.errors.is_empty() {
            Ok(Conversion {
                workloads,
                warnings: self.warnings,
                variables: Vec::new(),
                skipped: Vec::new(),
            })
        } else {
            Err(ConvertError {
                errors: self.errors,
                warnings: self.warnings,
            })
        }
    }
}

/// What a converter made of its input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conversion {
    /// The workloads by name. Every name is a DNS label, and every spec
    /// passes `WorkloadSpec::validate`.
    pub workloads: BTreeMap<String, WorkloadSpec>,
    /// What was skipped, in input order.
    pub warnings: Vec<Diagnostic>,
    /// The names of the caller's variables whose value went into the
    /// result, sorted and without repeats: those of
    /// [`ComposeInput::variables`] and of the env file that an
    /// interpolation, a bare name in `environment` or a null value there
    /// read. Their values may now be in a spec. Empty for Kubernetes.
    pub variables: Vec<String>,
    /// The compose services left out because none of their profiles was
    /// active, sorted. Empty for Kubernetes.
    pub skipped: Vec<String>,
}

/// Why a conversion produced nothing: every error found, at least one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertError {
    pub errors: Vec<Diagnostic>,
    /// The warnings found before and between the errors.
    pub warnings: Vec<Diagnostic>,
}

impl fmt::Display for ConvertError {
    /// One error per line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, error) in self.errors.iter().enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConvertError {}

/// Input of [`compose::convert`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ComposeInput {
    /// The compose file, read through [`Files`]. Relative paths inside it
    /// are resolved against its directory.
    pub file: PathBuf,
    /// The variables `${VAR}` is replaced from: the caller's environment.
    /// They win over the `.env` file.
    pub variables: BTreeMap<String, String>,
    /// The active profiles (`--profile`). A service that names profiles
    /// is converted only when one of them is active.
    pub profiles: Vec<String>,
    /// The file `${VAR}` defaults are read from (`--env-file`). When
    /// absent, `.env` next to the compose file is used if it exists.
    pub env_file: Option<PathBuf>,
    /// Allow env files outside the directory of the compose file
    /// (`--allow-external-env-file`): `env_file` here and `env_file` of a
    /// service. Without it such a path is an error, because its content
    /// would go from the caller's machine into the cluster.
    pub allow_external_env_files: bool,
}

/// One YAML stream handed to [`k8s::convert`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDocument {
    /// What diagnostics call it: the file name, or `-` for stdin. A
    /// stream with several documents gets `#<n>` appended per document,
    /// counted from 1.
    pub source: String,
    /// The YAML text, which may hold several documents.
    pub text: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_diagnostic_displays_source_path_and_message() {
        assert_eq!(
            Diagnostic::new("compose.yaml", "services.web.build", "not supported").to_string(),
            "compose.yaml: services.web.build: not supported"
        );
        assert_eq!(
            Diagnostic::new("-", "", "no workload").to_string(),
            "-: no workload"
        );
    }

    #[test]
    fn diagnostics_finish_as_a_conversion_only_without_errors() {
        let warning = Diagnostic::new("f", "a", "skipped");
        let mut d = Diagnostics::default();
        d.warnings.push(warning.clone());
        assert_eq!(
            d.clone().finish(BTreeMap::new()),
            Ok(Conversion {
                workloads: BTreeMap::new(),
                warnings: vec![warning.clone()],
                variables: Vec::new(),
                skipped: Vec::new(),
            })
        );

        let mut more = Diagnostics::default();
        more.errors.push(Diagnostic::new("f", "b", "bad"));
        more.errors.push(Diagnostic::new("f", "c", "worse"));
        d.extend(more);
        let err = d.finish(BTreeMap::new()).unwrap_err();
        assert_eq!(err.warnings, vec![warning]);
        assert_eq!(err.to_string(), "f: b: bad\nf: c: worse");
    }

    #[test]
    fn a_map_serves_as_files() {
        let files = BTreeMap::from([(
            PathBuf::from("dir/compose.yaml"),
            "services: {}".to_string(),
        )]);
        assert_eq!(
            files.read(Path::new("dir/compose.yaml")).unwrap(),
            "services: {}"
        );
        let err = files.read(Path::new("dir/.env")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
