//! `faber-node`'s own configuration, read from a root-owned env file.
//!
//! The uid range is the trust anchor: fabercp may choose a base inside it
//! and nowhere else, so a compromised fabercp cannot map container root to
//! host root.

use std::collections::BTreeMap;
use std::path::PathBuf;

use faber_container_runtime::Containerd;
use faber_node_protocol::USERNS_LENGTH;

pub const DEFAULT_SOCKET: &str = "/run/faber/node.sock";
pub const DEFAULT_VOLUME_ROOT: &str = "/var/lib/faber-node/volumes";
pub const DEFAULT_ENV_FILE: &str = "/etc/faber/faber-node.env";
pub const DEFAULT_CONTAINERD_CONF_DIR: &str = "/etc/containerd/conf.d";
pub const DEFAULT_CNI_CONF_DIR: &str = "/etc/cni/net.d";
pub const DEFAULT_CNI_BIN_DIR: &str = "/opt/cni/bin";

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0} is required")]
    Missing(&'static str),
    #[error("{key}: {reason}")]
    Invalid { key: &'static str, reason: String },
    #[error("line {line}: expected KEY=VALUE")]
    Syntax { line: usize },
}

/// Whether a file or directory with this owner and mode can only be changed
/// by root. `faber-node` trusts its env file and its socket directory only
/// when this holds: anyone who can write there could redirect root.
pub fn root_only_writable(uid: u32, mode: u32) -> bool {
    uid == 0 && mode & 0o022 == 0
}

/// Host ids that pods may map: `[min, max)`, in whole blocks of
/// [`USERNS_LENGTH`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UidRange {
    min: u64,
    max: u64,
}

impl UidRange {
    /// `min` is the first id of the range and `max` the first id after it.
    /// The hard floor is one block: id 0 and the first 65535 ids are never
    /// mappable, whatever the file says.
    pub fn new(min: u64, max: u64) -> Result<Self, String> {
        let block = u64::from(USERNS_LENGTH);
        if min < block {
            return Err(format!("min must be at least {block}"));
        }
        if !min.is_multiple_of(block) {
            return Err(format!("min must be a multiple of {block}"));
        }
        if max > u64::from(u32::MAX) + 1 {
            return Err("max must be at most 4294967296".to_owned());
        }
        if max < min + block {
            return Err(format!("max must leave room for a block of {block} ids"));
        }
        Ok(Self { min, max })
    }

    pub fn min(&self) -> u64 {
        self.min
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    /// Whether `base` starts a whole block of the range.
    pub fn check_base(&self, base: u32) -> Result<(), String> {
        let block = u64::from(USERNS_LENGTH);
        let base = u64::from(base);
        if base == 0 {
            return Err("uid_base must not be 0".to_owned());
        }
        if !base.is_multiple_of(block) {
            return Err(format!("uid_base must be a multiple of {block}"));
        }
        if base < self.min || base + block > self.max {
            return Err("uid_base is outside the range this node may map".to_owned());
        }
        Ok(())
    }

    fn contains(&self, id: u32) -> bool {
        (self.min..self.max).contains(&u64::from(id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub socket: PathBuf,
    pub volume_root: PathBuf,
    /// Where the containerd drop-in `faber.toml` is written. Root-owned.
    pub containerd_conf_dir: PathBuf,
    /// Where the CNI network config is written, and where containerd is
    /// told to read it. Root-owned.
    pub cni_conf_dir: PathBuf,
    /// Where containerd is told to find the CNI plugins it runs as root.
    pub cni_bin_dir: PathBuf,
    pub containerd_socket: PathBuf,
    /// Directories of the host tools, run by absolute path.
    pub bin_dir: PathBuf,
    pub sbin_dir: PathBuf,
    /// CRI runtime handler; empty for the runtime's default.
    pub runtime_handler: String,
    pub uid_range: UidRange,
    /// The unprivileged user fabercp runs as: allowed on the socket.
    pub faber_uid: u32,
    /// Group of the socket and of block volumes.
    pub faber_gid: u32,
}

impl NodeConfig {
    /// containerd's config files, in the directories this config names.
    pub fn containerd(&self) -> Containerd {
        Containerd {
            conf_dir: self.containerd_conf_dir.clone(),
            cni_conf_dir: self.cni_conf_dir.clone(),
            cni_bin_dir: self.cni_bin_dir.clone(),
        }
    }

    /// Parses an env file: `KEY=VALUE` lines, `#` comments, blank lines, and
    /// optional single or double quotes around a value.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let mut map = BTreeMap::new();
        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .ok_or(ConfigError::Syntax { line: i + 1 })?;
            let value = value.trim();
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                .unwrap_or(value);
            map.insert(key.trim().to_owned(), value.to_owned());
        }
        Self::from_map(&map)
    }

    fn from_map(map: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let number = |key: &'static str| -> Result<u64, ConfigError> {
            map.get(key)
                .ok_or(ConfigError::Missing(key))?
                .parse()
                .map_err(|_| ConfigError::Invalid {
                    key,
                    reason: "must be a number".to_owned(),
                })
        };
        let id = |key: &'static str| -> Result<u32, ConfigError> {
            u32::try_from(number(key)?).map_err(|_| ConfigError::Invalid {
                key,
                reason: "does not fit a uid".to_owned(),
            })
        };
        let path = |key: &'static str, default: &str| -> Result<PathBuf, ConfigError> {
            let value = map.get(key).map_or(default, String::as_str);
            // Checked as text: the target is Linux, and a Windows dev host
            // does not treat `/run` as absolute.
            if value.starts_with('/') && !value.split('/').any(|part| part == "..") {
                Ok(PathBuf::from(value))
            } else {
                Err(ConfigError::Invalid {
                    key,
                    reason: "must be an absolute path without ..".to_owned(),
                })
            }
        };

        let uid_range = UidRange::new(number("FABER_NODE_UID_MIN")?, number("FABER_NODE_UID_MAX")?)
            .map_err(|reason| ConfigError::Invalid {
                key: "FABER_NODE_UID_MIN",
                reason,
            })?;
        let faber_uid = id("FABER_NODE_FABER_UID")?;
        let faber_gid = id("FABER_NODE_FABER_GID")?;
        if uid_range.contains(faber_uid) || uid_range.contains(faber_gid) {
            return Err(ConfigError::Invalid {
                key: "FABER_NODE_FABER_UID",
                reason: "the faber user and group must lie outside the pod uid range".to_owned(),
            });
        }
        Ok(Self {
            socket: path("FABER_NODE_SOCKET", DEFAULT_SOCKET)?,
            volume_root: path("FABER_NODE_VOLUME_ROOT", DEFAULT_VOLUME_ROOT)?,
            containerd_conf_dir: path(
                "FABER_NODE_CONTAINERD_CONF_DIR",
                DEFAULT_CONTAINERD_CONF_DIR,
            )?,
            cni_conf_dir: path("FABER_NODE_CNI_CONF_DIR", DEFAULT_CNI_CONF_DIR)?,
            cni_bin_dir: path("FABER_NODE_CNI_BIN_DIR", DEFAULT_CNI_BIN_DIR)?,
            containerd_socket: path("FABER_NODE_CONTAINERD_SOCKET", faber_cri::DEFAULT_SOCKET)?,
            bin_dir: path("FABER_NODE_BIN_DIR", "/usr/bin")?,
            sbin_dir: path("FABER_NODE_SBIN_DIR", "/usr/sbin")?,
            runtime_handler: map
                .get("FABER_NODE_RUNTIME_HANDLER")
                .cloned()
                .unwrap_or_default(),
            uid_range,
            faber_uid,
            faber_gid,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "\
# faber-node
FABER_NODE_UID_MIN=65536
FABER_NODE_UID_MAX=\"4294967296\"
FABER_NODE_FABER_UID=990
FABER_NODE_FABER_GID=989
";

    #[test]
    fn parses_a_file_with_defaults() {
        let cfg = NodeConfig::parse(GOOD).unwrap();
        assert_eq!(cfg.uid_range.min(), 65536);
        assert_eq!(cfg.uid_range.max(), 1 << 32);
        assert_eq!(cfg.socket, PathBuf::from("/run/faber/node.sock"));
        assert_eq!(
            cfg.volume_root,
            PathBuf::from("/var/lib/faber-node/volumes")
        );
        assert_eq!(
            cfg.containerd_conf_dir,
            PathBuf::from("/etc/containerd/conf.d")
        );
        assert_eq!(cfg.cni_conf_dir, PathBuf::from("/etc/cni/net.d"));
        assert_eq!(cfg.cni_bin_dir, PathBuf::from("/opt/cni/bin"));
        assert_eq!((cfg.faber_uid, cfg.faber_gid), (990, 989));
        assert_eq!(cfg.bin_dir, PathBuf::from("/usr/bin"));
        assert_eq!(cfg.sbin_dir, PathBuf::from("/usr/sbin"));
    }

    #[test]
    fn refuses_missing_and_malformed_settings() {
        assert!(matches!(
            NodeConfig::parse("FABER_NODE_UID_MIN=65536\n"),
            Err(ConfigError::Missing("FABER_NODE_UID_MAX"))
        ));
        assert!(matches!(
            NodeConfig::parse("garbage"),
            Err(ConfigError::Syntax { line: 1 })
        ));
        let relative = format!("{GOOD}FABER_NODE_VOLUME_ROOT=var/volumes\n");
        assert!(matches!(
            NodeConfig::parse(&relative),
            Err(ConfigError::Invalid {
                key: "FABER_NODE_VOLUME_ROOT",
                ..
            })
        ));
    }

    #[test]
    fn the_runtime_directories_come_from_the_file() {
        let text = format!(
            "{GOOD}FABER_NODE_CONTAINERD_CONF_DIR=/c\nFABER_NODE_CNI_CONF_DIR=/n\n\
             FABER_NODE_CNI_BIN_DIR=/b\n"
        );
        let cfg = NodeConfig::parse(&text).unwrap();
        assert_eq!(cfg.containerd_conf_dir, PathBuf::from("/c"));
        assert_eq!(cfg.cni_conf_dir, PathBuf::from("/n"));
        assert_eq!(cfg.cni_bin_dir, PathBuf::from("/b"));
        for key in [
            "FABER_NODE_CONTAINERD_CONF_DIR",
            "FABER_NODE_CNI_CONF_DIR",
            "FABER_NODE_CNI_BIN_DIR",
        ] {
            for bad in ["rel/dir", "/a/../b"] {
                let text = format!("{GOOD}{key}={bad}\n");
                assert!(
                    matches!(
                        NodeConfig::parse(&text),
                        Err(ConfigError::Invalid { key: k, .. }) if k == key
                    ),
                    "{key}={bad}"
                );
            }
        }
    }

    #[test]
    fn only_root_owned_unshared_paths_are_trusted() {
        assert!(root_only_writable(0, 0o755));
        assert!(root_only_writable(0, 0o600));
        assert!(root_only_writable(0, 0o40755));
        for (uid, mode) in [
            (990, 0o755),
            (0, 0o775),
            (0, 0o757),
            (0, 0o777),
            (1000, 0o600),
        ] {
            assert!(!root_only_writable(uid, mode), "{uid} {mode:o}");
        }
    }

    #[test]
    fn the_floor_is_one_block_whatever_the_file_says() {
        for (min, max) in [(0, 1 << 20), (1000, 1 << 20), (65537, 1 << 20)] {
            assert!(UidRange::new(min, max).is_err(), "{min}");
        }
        assert!(UidRange::new(65536, 65536 + 100).is_err());
        assert!(UidRange::new(65536, (1 << 32) + 1).is_err());
        let zero = GOOD.replace("65536", "0");
        assert!(NodeConfig::parse(&zero).is_err());
    }

    #[test]
    fn faber_ids_inside_the_pod_range_are_refused() {
        let text = GOOD.replace("990", "70000");
        assert!(NodeConfig::parse(&text).is_err());
    }

    #[test]
    fn base_checks() {
        let range = UidRange::new(65536 * 2, 65536 * 10).unwrap();
        assert!(range.check_base(65536 * 2).is_ok());
        assert!(range.check_base(65536 * 9).is_ok());
        for bad in [
            0,
            65536,         // below the minimum
            65536 * 2 + 1, // unaligned
            65536 * 10,    // block would end past max
            65536 * 11,    // outside
            u32::MAX,      // outside, unaligned
        ] {
            assert!(range.check_base(bad).is_err(), "{bad}");
        }
    }
}
