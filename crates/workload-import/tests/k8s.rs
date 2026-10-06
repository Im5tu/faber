//! Runs the Kubernetes converter over `tests/fixtures/k8s/<name>/`.
//!
//! A fixture holds its input as one or more files whose names start with
//! `input`, read in name order. `expected.warnings` lists the warnings,
//! one per line. `expected.json` holds the workloads of a conversion that
//! succeeds, and `expected.errors` the errors of one that does not.

use std::fs;
use std::path::{Path, PathBuf};

use faber_workload_import::SourceDocument;
use faber_workload_import::k8s::convert;

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
        .join("tests/fixtures/k8s")
        .join(name);
    let mut inputs: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("input")
        })
        .collect();
    inputs.sort();
    assert!(!inputs.is_empty(), "{name}: no input file");
    let documents: Vec<SourceDocument> = inputs
        .iter()
        .map(|path| SourceDocument {
            source: path.file_name().unwrap().to_string_lossy().into_owned(),
            text: fs::read_to_string(path).unwrap(),
        })
        .collect();

    let (workloads, errors, warnings) = match convert(&documents) {
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

/// A Deployment that reads a ConfigMap through `configMapKeyRef` and
/// `envFrom`, with the Service next to it, in one multi-document file.
#[test]
fn deployment_service_configmap() {
    fixture("deployment-service-configmap");
}

#[test]
fn statefulset_with_a_claim_template() {
    fixture("statefulset-claim");
}

#[test]
fn daemonset() {
    fixture("daemonset");
}

/// A bare Pod whose two containers share an `emptyDir`.
#[test]
fn pod_with_a_sidecar() {
    fixture("pod-sidecar");
}

#[test]
fn kind_list() {
    fixture("list");
}

#[test]
fn json_manifest() {
    fixture("json-manifest");
}

/// The errors of every object are reported, not only the first.
#[test]
fn errors_in_several_objects() {
    fixture("errors-in-several-objects");
}

#[test]
fn only_kinds_that_are_no_workload() {
    fixture("no-workload");
}

/// A ConfigMap in one file resolves a reference from another.
#[test]
fn several_files() {
    fixture("several-files");
}

/// Every fixture directory has a test above.
#[test]
fn every_fixture_is_run() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/k8s");
    assert_eq!(fs::read_dir(dir).unwrap().count(), 9);
}
