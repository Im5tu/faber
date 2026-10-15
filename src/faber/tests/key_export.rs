//! `faber cluster key export` against the real binary: what it refuses and
//! what it prints.

use std::path::Path;

use assert_cmd::Command;

const KEY: &str = "q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq8s=";

fn export(file: &Path) -> assert_cmd::assert::Assert {
    Command::cargo_bin("faber")
        .expect("binary")
        .env_remove("FABER_ENDPOINT")
        .env_remove("FABER_CLUSTER_KEY_FILE")
        .args(["cluster", "key", "export", "--file"])
        .arg(file)
        .assert()
}

/// Fails with a message on stderr that holds `want` and none of `secret`,
/// and prints nothing.
fn refused(file: &Path, want: &str, secret: &str) {
    let out = export(file).failure();
    let out = out.get_output();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains(want), "{err}");
    assert!(!secret.is_empty() && !err.contains(secret), "{err}");
    assert!(out.stdout.is_empty());
}

#[test]
fn prints_a_valid_key_with_or_without_a_newline() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("cluster.key");
    for content in [KEY.to_string(), format!("{KEY}\n")] {
        std::fs::write(&file, content).unwrap();
        export(&file).success().stdout(format!("{KEY}\n"));
    }
}

#[test]
fn refuses_a_directory() {
    let dir = tempfile::tempdir().unwrap();
    refused(dir.path(), "not a regular file", "zzzz");
}

#[cfg(unix)]
#[test]
fn refuses_a_symlink_even_to_a_valid_key() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real.key");
    std::fs::write(&real, KEY).unwrap();
    let link = dir.path().join("link.key");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    refused(&link, "not a regular file", KEY);
}

#[test]
fn refuses_a_file_over_the_size_cap_without_printing_it() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("big.key");
    std::fs::write(&file, format!("{KEY}\n{}", "A".repeat(8192))).unwrap();
    refused(&file, "too large", KEY);
}

#[test]
fn refuses_content_that_is_not_base64() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("cluster.key");
    let secret = "this is not base64 !!";
    std::fs::write(&file, secret).unwrap();
    refused(&file, "valid cluster key", secret);
}

#[test]
fn refuses_base64_of_the_wrong_length() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("cluster.key");
    // 16 bytes.
    let short = "q83vEjRWeJq83vEjRWeJqw==";
    std::fs::write(&file, short).unwrap();
    refused(&file, "valid cluster key", short);
}
