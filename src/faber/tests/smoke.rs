//! Runs the real `faber` binary.

use assert_cmd::Command;

fn faber() -> Command {
    let mut cmd = Command::cargo_bin("faber").expect("binary");
    cmd.env_remove("FABER_ENDPOINT");
    cmd
}

#[test]
fn help_lists_subcommands() {
    let out = faber().arg("--help").assert().success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    for sub in ["health", "cluster", "mesh", "project", "deploy", "app"] {
        assert!(stdout.contains(sub), "missing {sub} in:\n{stdout}");
    }
    assert!(!stdout.contains("kv"), "{stdout}");
    assert!(stdout.contains("FABER_ENDPOINT"), "{stdout}");
}

#[test]
fn cluster_help_lists_status_init_join() {
    let out = faber().args(["cluster", "--help"]).assert().success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    for sub in ["status", "init", "join"] {
        assert!(stdout.contains(sub), "missing {sub} in:\n{stdout}");
    }
}

#[test]
fn join_with_bad_peer_fails_before_any_request() {
    // Port 1 is never contacted: the peer is rejected while parsing.
    let out = faber()
        .args(["-e", "http://127.0.0.1:1", "cluster", "join", "not-an-addr"])
        .assert()
        .failure()
        .code(2);
    assert!(out.get_output().stdout.is_empty());
}

/// The real binary with the real current directory and file system: it
/// reads the fixture, prints the spec and contacts nobody (nothing listens
/// on port 1).
#[test]
fn deploy_spec_dry_run_prints_the_spec_without_a_server() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let out = faber()
        .current_dir(&fixtures)
        .args(["-e", "http://127.0.0.1:1", "deploy", "spec", "spec.json"])
        .args(["--project", "shop", "--env", "staging", "--dry-run"])
        .assert()
        .success();
    let output = out.get_output();
    let spec: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(spec["db"]["containers"][0]["image"], "postgres:16");
    assert_eq!(
        spec["web"]["scale"],
        serde_json::json!({"replicas": {"count": 2, "strategy": "spread"}})
    );
    // Defaults are written out, so the output is the whole spec.
    assert_eq!(spec["web"]["restart"], "always");
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    assert!(stderr.starts_with("read: spec.json\nnote: "), "{stderr}");
    assert!(!stderr.contains("error"), "{stderr}");
}

#[test]
fn deploy_failure_exits_1_with_the_errors_on_stderr() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let out = faber()
        .current_dir(&fixtures)
        .args(["deploy", "spec", "missing.json", "--project", "shop"])
        .args(["--env", "staging"])
        .assert()
        .failure()
        .code(1);
    let output = out.get_output();
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    let lines: Vec<&str> = stderr.lines().collect();
    assert_eq!(lines.len(), 2, "{stderr}");
    assert!(
        lines[0].starts_with("error: missing.json: cannot read: "),
        "{stderr}"
    );
    assert_eq!(lines[1], "error: nothing was deployed");
}

#[test]
fn deploy_without_project_or_env_is_a_usage_error() {
    for args in [
        &["deploy", "spec", "spec.json", "--project", "shop"][..],
        &["deploy", "spec", "spec.json", "--env", "staging"],
    ] {
        let out = faber().args(args).assert().failure().code(2);
        assert!(out.get_output().stdout.is_empty());
    }
}

#[test]
fn endpoint_comes_from_env() {
    // Nothing listens on port 1, so the error names the endpoint taken from
    // the environment, proving the env var was read.
    let out = faber()
        .env("FABER_ENDPOINT", "http://127.0.0.1:1")
        .arg("health")
        .assert()
        .failure()
        .code(1);
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(stderr.contains("http://127.0.0.1:1"), "{stderr}");
}
