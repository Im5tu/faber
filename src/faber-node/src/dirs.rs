//! The directories root loads configuration from (containerd's drop-in
//! directory, the CNI config directory, the CNI plugin directory).
//!
//! Anyone who can change one of them, or a directory above it, can run code
//! as root: a plugin is executed by containerd, and a config names one. So
//! every directory of the path, up to `/`, must belong to the trusted user
//! and be closed to group and others, and the leaf is not a symlink. The
//! plugins themselves are checked the same way: every file in the plugin
//! directory must belong to the trusted user and be closed to group and
//! others, and so must the target of a symlink and the directories above it.
//! The checks run at startup and again before every write.

use std::path::{Path, PathBuf};

use faber_container_runtime::Containerd;

/// Who may own the directories, and where the walk up stops.
#[derive(Debug, Clone)]
pub struct DirPolicy {
    owner_uid: u32,
    /// The directories above this one are not looked at. `None` walks to `/`.
    stop_at: Option<PathBuf>,
}

/// Mode `0755`, which every directory made here gets.
#[cfg(unix)]
const DIR_MODE: u32 = 0o755;

impl DirPolicy {
    /// Root owns every directory, all the way up.
    pub fn root() -> Self {
        Self {
            owner_uid: 0,
            stop_at: None,
        }
    }

    /// `uid` owns every directory below `stop_at`. For tests, which cannot
    /// be root and sit in a temp directory that others can write.
    #[cfg(test)]
    pub fn owned_by(uid: u32, stop_at: PathBuf) -> Self {
        Self {
            owner_uid: uid,
            stop_at: Some(stop_at),
        }
    }

    /// Makes `dir` as `0755` if there is nothing at its path, parents
    /// included. Anything that is there is left to [`check`](Self::check).
    #[cfg(unix)]
    pub fn create_if_missing(&self, dir: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};

        match std::fs::symlink_metadata(dir) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(DIR_MODE)
                    .create(dir)?;
                // The umask may have narrowed the mode of the leaf.
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(DIR_MODE))
            }
            Err(e) => Err(e),
        }
    }

    #[cfg(not(unix))]
    pub fn create_if_missing(&self, _dir: &Path) -> std::io::Result<()> {
        Err(unsupported())
    }

    /// Whether `dir` is a directory (not a symlink) and so are all its
    /// parents, each owned by the trusted user and not writable by group or
    /// others. The error names the first one that is not.
    #[cfg(unix)]
    pub fn check(&self, dir: &Path) -> Result<(), String> {
        use std::os::unix::fs::MetadataExt as _;

        let leaf = std::fs::symlink_metadata(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        if leaf.file_type().is_symlink() {
            return Err(format!("{} is a symlink", dir.display()));
        }
        // Symlinks above the leaf are resolved, and the directories they
        // lead through are the ones that get checked.
        let real = std::fs::canonicalize(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let stop = self
            .stop_at
            .as_ref()
            .map(|s| std::fs::canonicalize(s).unwrap_or_else(|_| s.clone()));
        for level in real.ancestors() {
            if stop.as_deref() == Some(level) {
                break;
            }
            let meta = std::fs::metadata(level).map_err(|e| format!("{}: {e}", level.display()))?;
            if !meta.is_dir() {
                return Err(format!("{} is not a directory", level.display()));
            }
            if meta.uid() != self.owner_uid {
                return Err(format!(
                    "{} is owned by uid {}, not {}",
                    level.display(),
                    meta.uid(),
                    self.owner_uid
                ));
            }
            if meta.mode() & 0o022 != 0 {
                return Err(format!(
                    "{} is writable by group or others (mode {:o})",
                    level.display(),
                    meta.mode() & 0o7777
                ));
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn check(&self, _dir: &Path) -> Result<(), String> {
        Err(unsupported().to_string())
    }

    /// Whether every file of the plugin directory `dir` belongs to the
    /// trusted user and is closed to group and others. A symlink is followed
    /// only to a regular file that passes the same test, in a directory
    /// whose whole path passes [`check`](Self::check); a symlink that leads
    /// anywhere else, or nowhere, is refused. Subdirectories are not looked
    /// into: nothing runs from them.
    #[cfg(unix)]
    pub fn check_plugins(&self, dir: &Path) -> Result<(), String> {
        use std::os::unix::fs::MetadataExt as _;

        let entries = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        for entry in entries {
            let path = entry.map_err(|e| format!("{}: {e}", dir.display()))?.path();
            let meta =
                std::fs::symlink_metadata(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            if meta.is_dir() {
                continue;
            }
            let (file, meta) = if meta.file_type().is_symlink() {
                let target = std::fs::canonicalize(&path).map_err(|e| {
                    format!("{} is a symlink that leads nowhere: {e}", path.display())
                })?;
                let parent = target
                    .parent()
                    .ok_or_else(|| format!("{} leads to /", path.display()))?;
                self.check(parent)
                    .map_err(|e| format!("{} leads to an untrusted place: {e}", path.display()))?;
                let meta =
                    std::fs::metadata(&target).map_err(|e| format!("{}: {e}", target.display()))?;
                (target, meta)
            } else {
                (path, meta)
            };
            if !meta.is_file() {
                return Err(format!("{} is not a regular file", file.display()));
            }
            if meta.uid() != self.owner_uid {
                return Err(format!(
                    "{} is owned by uid {}, not {}",
                    file.display(),
                    meta.uid(),
                    self.owner_uid
                ));
            }
            if meta.mode() & 0o022 != 0 {
                return Err(format!(
                    "{} is writable by group or others (mode {:o})",
                    file.display(),
                    meta.mode() & 0o7777
                ));
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn check_plugins(&self, _dir: &Path) -> Result<(), String> {
        Err(unsupported().to_string())
    }

    /// Checks the three directories `containerd` writes to or names, and the
    /// plugins in the last. The error names the role and the reason, and the
    /// first one that fails wins.
    pub fn check_runtime_dirs(&self, containerd: &Containerd) -> Result<(), RuntimeDirError> {
        for (role, dir) in [
            ("containerd conf dir", &containerd.conf_dir),
            ("CNI conf dir", &containerd.cni_conf_dir),
            ("CNI bin dir", &containerd.cni_bin_dir),
        ] {
            self.check(dir)
                .map_err(|detail| RuntimeDirError { role, detail })?;
        }
        self.check_plugins(&containerd.cni_bin_dir)
            .map_err(|detail| RuntimeDirError {
                role: "CNI bin dir",
                detail,
            })
    }
}

#[cfg(not(unix))]
fn unsupported() -> std::io::Error {
    std::io::Error::other("directory ownership needs a Unix host")
}

/// A runtime directory that cannot be trusted.
#[derive(Debug, PartialEq, Eq)]
pub struct RuntimeDirError {
    /// Which directory, by what it is for.
    pub role: &'static str,
    /// Why, naming the path. For faber-node's log and its own startup
    /// error, not for a response.
    pub detail: String,
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    use super::*;

    fn my_uid(dir: &Path) -> u32 {
        use std::os::unix::fs::MetadataExt as _;
        std::fs::metadata(dir).unwrap().uid()
    }

    fn tmp() -> (tempfile::TempDir, DirPolicy) {
        let tmp = tempfile::tempdir().unwrap();
        let policy = DirPolicy::owned_by(my_uid(tmp.path()), tmp.path().to_path_buf());
        (tmp, policy)
    }

    fn make(dir: &Path, mode: u32) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn a_closed_directory_chain_passes() {
        let (tmp, policy) = tmp();
        let dir = tmp.path().join("a/b/c");
        make(&dir, 0o755);
        make(&tmp.path().join("a/b"), 0o755);
        make(&tmp.path().join("a"), 0o755);
        assert_eq!(policy.check(&dir), Ok(()));
    }

    #[test]
    fn a_leaf_open_to_group_or_others_is_refused() {
        let (tmp, policy) = tmp();
        for mode in [0o775, 0o757, 0o777, 0o770] {
            let dir = tmp.path().join(format!("d{mode:o}"));
            make(&dir, mode);
            let err = policy.check(&dir).unwrap_err();
            assert!(
                err.contains("writable by group or others"),
                "{mode:o}: {err}"
            );
        }
    }

    #[test]
    fn a_parent_open_to_others_is_refused() {
        let (tmp, policy) = tmp();
        let open = tmp.path().join("open");
        let dir = open.join("conf");
        make(&dir, 0o755);
        make(&open, 0o777);
        let err = policy.check(&dir).unwrap_err();
        assert!(err.contains(open.to_str().unwrap()), "{err}");
        // Closing it makes the same leaf pass.
        make(&open, 0o755);
        assert_eq!(policy.check(&dir), Ok(()));
    }

    #[test]
    fn another_owner_is_refused() {
        let (tmp, _) = tmp();
        let dir = tmp.path().join("d");
        make(&dir, 0o755);
        let policy = DirPolicy::owned_by(my_uid(&dir) + 1, tmp.path().to_path_buf());
        let err = policy.check(&dir).unwrap_err();
        assert!(err.contains("is owned by uid"), "{err}");
    }

    #[test]
    fn a_symlink_leaf_a_file_and_a_missing_path_are_refused() {
        let (tmp, policy) = tmp();
        let real = tmp.path().join("real");
        make(&real, 0o755);
        let link = tmp.path().join("link");
        symlink(&real, &link).unwrap();
        assert!(policy.check(&link).unwrap_err().contains("symlink"));

        let file = tmp.path().join("file");
        std::fs::write(&file, "x").unwrap();
        assert!(policy.check(&file).is_err());
        assert!(policy.check(&tmp.path().join("missing")).is_err());
    }

    #[test]
    fn the_real_root_policy_refuses_a_test_directory() {
        // Not root-owned (unless the tests run as root), and under /tmp.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("d");
        make(&dir, 0o755);
        if my_uid(&dir) != 0 {
            assert!(DirPolicy::root().check(&dir).is_err());
        }
    }

    #[test]
    fn missing_directories_are_made_0755_and_nothing_else_is_touched() {
        let (tmp, policy) = tmp();
        let dir = tmp.path().join("x/y/z");
        policy.create_if_missing(&dir).unwrap();
        for level in [&dir, &tmp.path().join("x/y"), &tmp.path().join("x")] {
            let mode = std::fs::metadata(level).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o755, "{}", level.display());
        }
        assert_eq!(policy.check(&dir), Ok(()));

        // An existing directory keeps its mode: the check decides, not this.
        let open = tmp.path().join("open");
        make(&open, 0o777);
        policy.create_if_missing(&open).unwrap();
        let mode = std::fs::metadata(&open).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o777);
        assert!(policy.check(&open).is_err());

        // A symlink at the path is left alone too.
        let link = tmp.path().join("link");
        symlink(&dir, &link).unwrap();
        policy.create_if_missing(&link).unwrap();
        assert!(policy.check(&link).is_err());
    }

    fn plugin(dir: &Path, name: &str, mode: u32) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[test]
    fn plugin_files_must_be_closed_to_group_and_others() {
        let (tmp, policy) = tmp();
        let bin = tmp.path().join("bin");
        make(&bin, 0o755);
        plugin(&bin, "bridge", 0o755);
        plugin(&bin, "host-local", 0o750);
        plugin(&bin, ".version", 0o644);
        make(&bin.join("subdir-is-skipped"), 0o777);
        assert_eq!(policy.check_plugins(&bin), Ok(()));

        for mode in [0o775, 0o757, 0o777, 0o666, 0o722] {
            let bad = plugin(&bin, "loopback", mode);
            let err = policy.check_plugins(&bin).unwrap_err();
            assert!(
                err.contains("loopback") && err.contains("writable by group or others"),
                "{mode:o}: {err}"
            );
            std::fs::remove_file(bad).unwrap();
        }
        assert_eq!(policy.check_plugins(&bin), Ok(()));
    }

    #[test]
    fn a_plugin_of_another_owner_is_refused() {
        let (tmp, _) = tmp();
        let bin = tmp.path().join("bin");
        make(&bin, 0o755);
        plugin(&bin, "bridge", 0o755);
        let policy = DirPolicy::owned_by(my_uid(&bin) + 1, tmp.path().to_path_buf());
        let err = policy.check_plugins(&bin).unwrap_err();
        assert!(err.contains("is owned by uid"), "{err}");
    }

    #[test]
    fn a_symlinked_plugin_is_followed_only_to_a_trusted_file() {
        let (tmp, policy) = tmp();
        let bin = tmp.path().join("bin");
        let libexec = tmp.path().join("libexec");
        make(&bin, 0o755);
        make(&libexec, 0o755);
        let good = plugin(&libexec, "real", 0o755);
        symlink(&good, bin.join("bridge")).unwrap();
        assert_eq!(policy.check_plugins(&bin), Ok(()));

        // The target is writable by others.
        let loose = plugin(&libexec, "loose", 0o666);
        symlink(&loose, bin.join("loose")).unwrap();
        let err = policy.check_plugins(&bin).unwrap_err();
        assert!(err.contains("writable by group or others"), "{err}");
        std::fs::remove_file(bin.join("loose")).unwrap();

        // The target is fine, but its directory is open to others.
        let open = tmp.path().join("open");
        make(&open, 0o777);
        let inside = plugin(&open, "inside", 0o755);
        symlink(&inside, bin.join("inside")).unwrap();
        let err = policy.check_plugins(&bin).unwrap_err();
        assert!(err.contains("untrusted place"), "{err}");
        std::fs::remove_file(bin.join("inside")).unwrap();

        // A link to a directory, to nothing, and to a link of those.
        symlink(&libexec, bin.join("dir")).unwrap();
        assert!(
            policy
                .check_plugins(&bin)
                .unwrap_err()
                .contains("not a regular file")
        );
        std::fs::remove_file(bin.join("dir")).unwrap();
        symlink(tmp.path().join("missing"), bin.join("dangling")).unwrap();
        assert!(
            policy
                .check_plugins(&bin)
                .unwrap_err()
                .contains("leads nowhere")
        );
        std::fs::remove_file(bin.join("dangling")).unwrap();
        assert_eq!(policy.check_plugins(&bin), Ok(()));
    }

    #[test]
    fn a_loose_plugin_makes_the_runtime_dirs_untrusted() {
        let (tmp, policy) = tmp();
        let (conf, cni, bin) = (
            tmp.path().join("conf.d"),
            tmp.path().join("net.d"),
            tmp.path().join("bin"),
        );
        for d in [&conf, &cni, &bin] {
            make(d, 0o755);
        }
        let containerd = Containerd {
            conf_dir: conf,
            cni_conf_dir: cni,
            cni_bin_dir: bin.clone(),
            sandbox_image: "p:1".into(),
            bridge: "faberbr0".into(),
        };
        plugin(&bin, "bridge", 0o666);
        let err = policy.check_runtime_dirs(&containerd).unwrap_err();
        assert_eq!(err.role, "CNI bin dir");
        assert!(err.detail.contains("bridge"), "{}", err.detail);
    }

    #[test]
    fn the_runtime_dirs_are_checked_by_role() {
        let (tmp, policy) = tmp();
        let (conf, cni, bin) = (
            tmp.path().join("conf.d"),
            tmp.path().join("net.d"),
            tmp.path().join("bin"),
        );
        for d in [&conf, &cni, &bin] {
            make(d, 0o755);
        }
        let containerd = Containerd {
            conf_dir: conf.clone(),
            cni_conf_dir: cni.clone(),
            cni_bin_dir: bin.clone(),
            sandbox_image: "p:1".into(),
            bridge: "faberbr0".into(),
        };
        assert_eq!(policy.check_runtime_dirs(&containerd), Ok(()));

        make(&bin, 0o777);
        let err = policy.check_runtime_dirs(&containerd).unwrap_err();
        assert_eq!(err.role, "CNI bin dir");
        make(&bin, 0o755);

        std::fs::remove_dir(&cni).unwrap();
        let err = policy.check_runtime_dirs(&containerd).unwrap_err();
        assert_eq!(err.role, "CNI conf dir");
    }
}
