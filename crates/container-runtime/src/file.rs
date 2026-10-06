//! Atomic writes and removals for the config files this crate owns.

use std::io;
use std::path::Path;

/// Writes `content` to `<dir>/<file_name>` unless the file already holds
/// exactly that. Returns whether it wrote.
///
/// The write is atomic: a temp file in `dir`, fsync, rename over the target,
/// then fsync of `dir` (Unix). The file is mode 0644 on Unix. A leftover temp
/// file is removed on error. The temp name ends in `.tmp`, which neither
/// containerd's `*.toml` import nor libcni's config loader picks up.
pub(crate) fn write_if_changed(dir: &Path, file_name: &str, content: &[u8]) -> io::Result<bool> {
    let target = dir.join(file_name);
    match std::fs::read(&target) {
        Ok(existing) if existing == content => return Ok(false),
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let tmp = dir.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let result = write_and_rename(&tmp, &target, content);
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result?;
    sync_dir(dir)?;
    Ok(true)
}

/// Removes `<dir>/<file_name>` and fsyncs `dir` (Unix). Returns whether there
/// was a file to remove; a missing file or a missing `dir` is `Ok(false)`.
pub(crate) fn remove_if_exists(dir: &Path, file_name: &str) -> io::Result<bool> {
    match std::fs::remove_file(dir.join(file_name)) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    }
    sync_dir(dir)?;
    Ok(true)
}

fn write_and_rename(tmp: &Path, target: &Path, content: &[u8]) -> io::Result<()> {
    use std::io::Write as _;

    // A temp file left by a crashed run with the same pid must not wedge
    // later writes; removing it first and then using `create_new` also means
    // we never write through a symlink planted at the temp path.
    match std::fs::remove_file(tmp) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o644);
    }
    let mut file = opts.open(tmp)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // `mode` above is filtered by the umask; set it explicitly.
        file.set_permissions(std::fs::Permissions::from_mode(0o644))?;
    }
    file.write_all(content)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(tmp, target)
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_if_changed_writes_the_named_file_then_skips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.conf");

        assert!(write_if_changed(dir.path(), "a.conf", b"one").unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"one");
        assert!(!write_if_changed(dir.path(), "a.conf", b"one").unwrap());
        assert!(write_if_changed(dir.path(), "a.conf", b"two").unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"two");

        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from("a.conf")]);
    }

    #[test]
    fn remove_if_exists_removes_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.conf");
        std::fs::write(&path, "x").unwrap();

        assert!(remove_if_exists(dir.path(), "a.conf").unwrap());
        assert!(!path.exists());
        assert!(!remove_if_exists(dir.path(), "a.conf").unwrap());
    }

    #[test]
    fn remove_if_exists_missing_dir_is_no_change() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!remove_if_exists(&dir.path().join("nope"), "a.conf").unwrap());
    }
}
