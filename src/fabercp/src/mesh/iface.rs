//! The record that this node created its WireGuard interface.
//!
//! fabercp only ever configures an interface it created itself. The proof is
//! `<state_dir>/iface`, a file holding the interface name. It is written
//! after `ip link show` found no such link and before `ip link add` runs, so
//! a crash between the two leaves a record without a link, which is the same
//! state as after a reboot: the link is created again. An interface that
//! exists without this record belongs to someone else and startup refuses to
//! touch it.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::files;

/// File name inside the state dir.
pub const IFACE_FILE: &str = "iface";

/// `<state_dir>/iface`.
pub fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(IFACE_FILE)
}

/// Whether the record says this node created `iface`. A record naming
/// another interface does not count.
pub fn is_owned(state_dir: &Path, iface: &str) -> io::Result<bool> {
    match fs::read_to_string(path(state_dir)) {
        Ok(recorded) => Ok(recorded.trim() == iface),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Records that this node creates `iface`, replacing any earlier record.
/// Written atomically and synced to disk before returning.
pub fn record(state_dir: &Path, iface: &str) -> io::Result<()> {
    files::write_atomic(state_dir, IFACE_FILE, format!("{iface}\n").as_bytes(), None)
}

/// Removes the record. A missing record is fine.
pub fn forget(state_dir: &Path) -> io::Result<()> {
    files::remove(state_dir, IFACE_FILE)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn no_record_means_not_owned() {
        let dir = TempDir::new().unwrap();
        assert!(!is_owned(dir.path(), "faber0").unwrap());
    }

    #[test]
    fn record_names_one_interface() {
        let dir = TempDir::new().unwrap();
        record(dir.path(), "faber0").unwrap();
        assert!(is_owned(dir.path(), "faber0").unwrap());
        assert!(!is_owned(dir.path(), "wg0").unwrap());
        assert_eq!(fs::read_to_string(path(dir.path())).unwrap(), "faber0\n");
        // A new interface name replaces the record.
        record(dir.path(), "faber1").unwrap();
        assert!(is_owned(dir.path(), "faber1").unwrap());
        assert!(!is_owned(dir.path(), "faber0").unwrap());
    }

    #[test]
    fn forget_removes_the_record() {
        let dir = TempDir::new().unwrap();
        record(dir.path(), "faber0").unwrap();
        forget(dir.path()).unwrap();
        assert!(!is_owned(dir.path(), "faber0").unwrap());
        assert!(!path(dir.path()).exists());
        // Nothing to forget is fine.
        forget(dir.path()).unwrap();
    }

    #[test]
    fn unreadable_record_is_an_error() {
        let dir = TempDir::new().unwrap();
        fs::create_dir(path(dir.path())).unwrap();
        assert!(is_owned(dir.path(), "faber0").is_err());
        assert!(record(&dir.path().join("missing"), "faber0").is_err());
    }
}
