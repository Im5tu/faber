//! Runs the compose converter over `tests/fixtures/compose/<name>/`.
//!
//! A fixture holds `compose.yaml` and the env files it reads. The test
//! serves them from memory as the directory `/srv/app`, so a relative
//! path in a fixture resolves the same way on every machine. An optional
//! `input.json` gives the caller's side: `variables`, `profiles` and
//! `env_file`. `expected.warnings` lists the warnings, one per line.
//! `expected.json` holds the workloads of a conversion that succeeds, and
//! `expected.errors` the errors of one that does not.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use faber_workload_import::ComposeInput;
use faber_workload_import::compose::convert;

const DIR: &str = "/srv/app";

fn lines(path: &Path) -> Vec<String> {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

fn fixture(name: &str) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/compose")
        .join(name);
    let mut files = BTreeMap::new();
    let mut input = ComposeInput {
        file: PathBuf::from(format!("{DIR}/compose.yaml")),
        ..ComposeInput::default()
    };
    for entry in fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.unwrap().path();
        let file = path.file_name().unwrap().to_string_lossy().into_owned();
        let text = fs::read_to_string(&path).unwrap();
        if file == "input.json" {
            let caller: serde_json::Value = serde_json::from_str(&text).unwrap();
            let text = |value: &serde_json::Value| value.as_str().unwrap().to_string();
            if let Some(variables) = caller["variables"].as_object() {
                input.variables = variables
                    .iter()
                    .map(|(name, value)| (name.clone(), text(value)))
                    .collect();
            }
            if let Some(profiles) = caller["profiles"].as_array() {
                input.profiles = profiles.iter().map(text).collect();
            }
            input.env_file = caller["env_file"].as_str().map(PathBuf::from);
        } else if !file.starts_with("expected.") {
            files.insert(PathBuf::from(format!("{DIR}/{file}")), text);
        }
    }

    let (workloads, errors, warnings) = match convert(&input, &files) {
        Ok(done) => (Some(done.workloads), Vec::new(), done.warnings),
        Err(err) => (None, err.errors, err.warnings),
    };
    let errors: Vec<String> = errors.iter().map(ToString::to_string).collect();
    let warnings: Vec<String> = warnings.iter().map(ToString::to_string).collect();
    assert_eq!(
        warnings,
        lines(&dir.join("expected.warnings")),
        "{name}: warnings"
    );

    let expected_errors = dir.join("expected.errors");
    if expected_errors.exists() {
        assert_eq!(errors, lines(&expected_errors), "{name}: errors");
        assert!(
            !dir.join("expected.json").exists(),
            "{name}: has both expected.errors and expected.json"
        );
        return;
    }
    assert_eq!(errors, Vec::<String>::new(), "{name}: errors");
    let expected = fs::read_to_string(dir.join("expected.json")).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();
    assert_eq!(
        serde_json::to_value(workloads.unwrap()).unwrap(),
        expected,
        "{name}: workloads"
    );
}

/// A web service and its database: a named volume, a relative bind
/// mount, a health check, a published port and `x-faber`.
#[test]
fn web_db() {
    fixture("web-db");
}

/// A file in the shape `docker compose config` prints: long-form ports
/// and volumes, `networks: {default: null}` on every service, and the
/// project name in the names of the network and the volume.
#[test]
fn config_output() {
    fixture("config-output");
}

/// Anchors under `x-` keys, merged into services with `<<`.
#[test]
fn anchors() {
    fixture("anchors");
}

/// Variables from the caller and from `.env`, and two env files of a
/// service, one of them optional and absent.
#[test]
fn interpolation() {
    fixture("interpolation");
}

/// Three services with something wrong in each, and at the top level:
/// every error is reported, not only the first.
#[test]
fn errors_in_several_services() {
    fixture("errors-in-several-services");
}
