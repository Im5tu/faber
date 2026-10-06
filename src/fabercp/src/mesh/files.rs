//! Small files in the state dir, written so that a crash leaves either the
//! old file or the new one, never a partial one.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Writes `dir/name` atomically: a new temp file in `dir`, fsync, rename
/// over the old file, then fsync `dir` (unix). `mode` is the file mode on
/// unix; `None` leaves it to the umask. A symlink at `dir/name` is replaced,
/// not followed.
pub(crate) fn write_atomic(
    dir: &Path,
    name: &str,
    contents: &[u8],
    mode: Option<u32>,
) -> io::Result<()> {
    let mut suffix = [0u8; 8];
    getrandom::fill(&mut suffix).map_err(|e| io::Error::other(e.to_string()))?;
    let suffix: String = suffix.iter().map(|b| format!("{b:02x}")).collect();
    let tmp = dir.join(format!(".{name}.{suffix}.tmp"));

    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;

    let write = || -> io::Result<()> {
        let mut f = opts.open(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, dir.join(name))
    };
    if let Err(e) = write() {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    sync_dir(dir)
}

/// Removes `dir/name` and fsyncs `dir`. A missing file is fine.
pub(crate) fn remove(dir: &Path, name: &str) -> io::Result<()> {
    match fs::remove_file(dir.join(name)) {
        Ok(()) => sync_dir(dir),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// fsyncs `dir` so a rename, create or unlink inside it is durable (unix; a
/// no-op elsewhere).
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn write_replaces_the_file_and_leaves_no_temp_file() {
        let dir = TempDir::new().unwrap();
        write_atomic(dir.path(), "marker", b"one\n", Some(0o600)).unwrap();
        write_atomic(dir.path(), "marker", b"two\n", Some(0o600)).unwrap();
        assert_eq!(fs::read(dir.path().join("marker")).unwrap(), b"two\n");
        assert_eq!(names(dir.path()), vec!["marker"]);
    }

    #[test]
    fn write_into_a_missing_dir_fails() {
        let dir = TempDir::new().unwrap();
        assert!(write_atomic(&dir.path().join("nope"), "marker", b"x", None).is_err());
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = TempDir::new().unwrap();
        write_atomic(dir.path(), "marker", b"x", None).unwrap();
        remove(dir.path(), "marker").unwrap();
        remove(dir.path(), "marker").unwrap();
        assert!(names(dir.path()).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn write_sets_the_mode_and_replaces_a_symlink() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target");
        fs::write(&target, "untouched").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join("marker")).unwrap();

        write_atomic(dir.path(), "marker", b"x\n", Some(0o600)).unwrap();
        let meta = fs::symlink_metadata(dir.path().join("marker")).unwrap();
        assert!(meta.file_type().is_file());
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        assert_eq!(fs::read_to_string(&target).unwrap(), "untouched");
    }
}
