//! The `bootstrap-error` marker: why this node is not in its mesh.
//!
//! When fabercp gives up on a join, or finds that the mesh has removed this
//! node, it stays up but idle, so nothing but the log says why. install.sh
//! runs as root and cannot ask an idle node, so fabercp leaves
//! `<state_dir>/bootstrap-error` holding one fixed code and a newline. The
//! next start that resolves an identity removes it.

use std::io;
use std::path::{Path, PathBuf};

use super::files;

/// File name inside the state dir.
pub const BOOTSTRAP_ERROR_FILE: &str = "bootstrap-error";

/// Why the node is idle. [`Self::code`] is what the marker holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapError {
    /// The join token had expired when fabercp started.
    TokenExpired,
    /// The join token could not be decoded, or what it holds is not a valid
    /// identity.
    TokenInvalid,
    /// The mesh answered the join with `invite_invalid`, `invite_used` or
    /// `invite_expired`.
    InviteRejected,
    /// The mesh removed this node after it had joined: its lease ran out,
    /// or another machine took over its name.
    NodeRemoved,
}

impl BootstrapError {
    /// Every code, for readers that match the marker against the fixed set.
    pub const ALL: [BootstrapError; 4] = [
        BootstrapError::TokenExpired,
        BootstrapError::TokenInvalid,
        BootstrapError::InviteRejected,
        BootstrapError::NodeRemoved,
    ];

    pub fn code(self) -> &'static str {
        match self {
            BootstrapError::TokenExpired => "token_expired",
            BootstrapError::TokenInvalid => "token_invalid",
            BootstrapError::InviteRejected => "invite_rejected",
            BootstrapError::NodeRemoved => "node_removed",
        }
    }
}

/// `<state_dir>/bootstrap-error`.
pub fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(BOOTSTRAP_ERROR_FILE)
}

/// Writes the marker: the code and a newline, atomically, mode 0600 (unix).
/// Replaces an earlier marker.
pub fn write(state_dir: &Path, error: BootstrapError) -> io::Result<()> {
    let contents = format!("{}\n", error.code());
    files::write_atomic(
        state_dir,
        BOOTSTRAP_ERROR_FILE,
        contents.as_bytes(),
        Some(0o600),
    )
}

/// Reads the marker. `None` when there is none, and when it holds anything
/// but one of the fixed codes.
pub fn read(state_dir: &Path) -> io::Result<Option<BootstrapError>> {
    let text = match std::fs::read_to_string(path(state_dir)) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let code = text.trim_end_matches(['\r', '\n']);
    Ok(BootstrapError::ALL.into_iter().find(|e| e.code() == code))
}

/// Removes the marker. A missing marker is fine.
pub fn clear(state_dir: &Path) -> io::Result<()> {
    files::remove(state_dir, BOOTSTRAP_ERROR_FILE)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn the_marker_holds_one_code_and_a_newline() {
        let dir = TempDir::new().unwrap();
        for (error, text) in [
            (BootstrapError::TokenExpired, "token_expired\n"),
            (BootstrapError::TokenInvalid, "token_invalid\n"),
            (BootstrapError::InviteRejected, "invite_rejected\n"),
        ] {
            write(dir.path(), error).unwrap();
            assert_eq!(fs::read_to_string(path(dir.path())).unwrap(), text);
        }
        assert_eq!(BootstrapError::ALL.len(), 4);
        // Only the marker is left in the directory.
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn read_gives_back_what_write_stored() {
        let dir = TempDir::new().unwrap();
        assert_eq!(read(dir.path()).unwrap(), None);
        for error in BootstrapError::ALL {
            write(dir.path(), error).unwrap();
            assert_eq!(read(dir.path()).unwrap(), Some(error));
        }
        // Anything else is no reason.
        for junk in ["", "node_removed and more\n", "NODE_REMOVED\n", "\n"] {
            fs::write(path(dir.path()), junk).unwrap();
            assert_eq!(read(dir.path()).unwrap(), None, "{junk:?}");
        }
        clear(dir.path()).unwrap();
        assert_eq!(read(dir.path()).unwrap(), None);
    }

    #[test]
    fn clear_removes_the_marker() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), BootstrapError::TokenExpired).unwrap();
        clear(dir.path()).unwrap();
        assert!(!path(dir.path()).exists());
        clear(dir.path()).unwrap();
    }

    #[test]
    fn write_into_a_missing_dir_fails() {
        let dir = TempDir::new().unwrap();
        assert!(write(&dir.path().join("nope"), BootstrapError::TokenExpired).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn the_marker_is_a_regular_file_with_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        write(dir.path(), BootstrapError::InviteRejected).unwrap();
        let meta = fs::symlink_metadata(path(dir.path())).unwrap();
        assert!(meta.file_type().is_file());
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
    }
}
