//! Atomic writes and removals for the config files this crate owns.
//!
//! The files are loaded by root, so nothing here follows a symlink: the
//! target is read with `O_NOFOLLOW` (a symlink there is replaced, never read
//! through or written through), and the temp file has a random name and is
//! created with `O_EXCL | O_NOFOLLOW`.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher as _, Hasher as _};
use std::io;
use std::path::Path;

/// `O_NOFOLLOW`, or nothing where the flag is not known.
#[cfg(target_os = "linux")]
fn no_follow() -> i32 {
    nix::fcntl::OFlag::O_NOFOLLOW.bits()
}

#[cfg(not(target_os = "linux"))]
fn no_follow() -> i32 {
    0
}

/// 128 random bits as hex, from the keys the standard library seeds from the
/// operating system.
fn random_suffix() -> String {
    let word = || RandomState::new().build_hasher().finish();
    format!("{:016x}{:016x}", word(), word())
}

/// The content of `path`, or `None` when there is no regular file to compare
/// with: it is missing, or it is a symlink (which is not read through).
fn read_existing(path: &Path) -> io::Result<Option<Vec<u8>>> {
    use std::io::Read as _;

    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.custom_flags(no_follow());
    }
    let mut file = match opts.open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        #[cfg(target_os = "linux")]
        Err(e) if e.raw_os_error() == Some(nix::errno::Errno::ELOOP as i32) => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut content = Vec::new();
    file.read_to_end(&mut content)?;
    Ok(Some(content))
}

/// Writes `content` to `<dir>/<file_name>` unless the file already holds
/// exactly that. Returns whether it wrote.
///
/// The write is atomic: a temp file in `dir`, fsync, rename over the target,
/// then fsync of `dir` (Unix). The file is mode 0644 on Unix. A leftover temp
/// file is removed on error. The temp name ends in `.tmp`, which neither
/// containerd's `*.toml` import nor libcni's config loader picks up.
pub(crate) fn write_if_changed(dir: &Path, file_name: &str, content: &[u8]) -> io::Result<bool> {
    let target = dir.join(file_name);
    if read_existing(&target)?.is_some_and(|existing| existing == content) {
        return Ok(false);
    }
    let tmp = dir.join(format!(".{file_name}.{}.tmp", random_suffix()));
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

    // `create_new` fails on anything already at the path, a symlink
    // included, and the random name cannot be guessed to plant one.
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o644).custom_flags(no_follow());
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

    #[test]
    fn temp_names_are_random_and_long() {
        let (a, b) = (random_suffix(), random_suffix());
        assert_eq!(a.len(), 32);
        assert_ne!(a, b);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_symlink_at_the_target_is_replaced_not_followed() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        // The link points at a file that already holds the new content, so
        // reading through it would report "unchanged" and write nothing.
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "new").unwrap();
        let target = dir.path().join("a.conf");
        symlink(&victim, &target).unwrap();

        assert!(write_if_changed(dir.path(), "a.conf", b"new").unwrap());
        assert!(!std::fs::symlink_metadata(&target).unwrap().is_symlink());
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        assert_eq!(std::fs::read(&victim).unwrap(), b"new");

        // A link to a file with other content: the file is left alone.
        std::fs::remove_file(&target).unwrap();
        std::fs::write(&victim, "precious").unwrap();
        symlink(&victim, &target).unwrap();
        assert!(write_if_changed(dir.path(), "a.conf", b"new").unwrap());
        assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn a_planted_temp_name_is_never_written_through() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "precious").unwrap();
        // The name the old scheme used: pid, no randomness.
        let old = dir
            .path()
            .join(format!(".a.conf.{}.tmp", std::process::id()));
        symlink(&victim, &old).unwrap();

        assert!(write_if_changed(dir.path(), "a.conf", b"new").unwrap());
        assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
    }
}
