//! The containerd drop-in `<conf_dir>/faber.toml`.
//!
//! install.sh writes the root `/etc/containerd/config.toml` (`version = 2`,
//! `imports = ["/etc/containerd/conf.d/*.toml"]`). [`Containerd`] writes the
//! drop-in, as temp file plus rename and only when the content changes; the
//! `faber-containerd-reload.path` unit restarts containerd.
//!
//! [`Containerd`] also keeps `<cni_conf_dir>/10-faber.conflist` (see
//! [`cni`]) in step with the node's container network, written the same way
//! and removed when the node has no subnet. containerd watches that
//! directory, so the conflist needs no restart.

use std::io;
use std::path::{Path, PathBuf};

use crate::cni::{self, CONFLIST_FILE};
use crate::{BoxFuture, ContainerRuntime, NodeRuntimeSpec, PodNetworkSpec, RuntimeError, file};

/// File name of the drop-in inside the conf dir.
pub const DROP_IN_FILE: &str = "faber.toml";
/// First line of every file faber writes for containerd.
const MANAGED_MARKER: &str = "# managed by faber";

const WRITE_DROP_IN: &str = "writing the containerd drop-in";
const WRITE_CONFLIST: &str = "writing the CNI network config";
const REMOVE_CONFLIST: &str = "removing the CNI network config";
const RECONCILE: &str = "configuring containerd";

/// containerd, configured through the drop-in `<conf_dir>/faber.toml` and the
/// CNI network config `<cni_conf_dir>/10-faber.conflist`. The drop-in tells
/// containerd to read CNI configs from `cni_conf_dir` and to run CNI plugins
/// from `cni_bin_dir`, to use the systemd cgroup driver, and which pause image
/// to use. The CNI config names `bridge`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Containerd {
    /// Directory the drop-in is written to.
    pub conf_dir: PathBuf,
    /// Directory the CNI network config is written to, and the one
    /// containerd is told to read.
    pub cni_conf_dir: PathBuf,
    /// Directory containerd is told to run CNI plugins from.
    pub cni_bin_dir: PathBuf,
    /// The pause image of every pod sandbox.
    pub sandbox_image: String,
    /// Name of the bridge containers attach to.
    pub bridge: String,
}

impl ContainerRuntime for Containerd {
    /// On a blocking thread: writes the drop-in if it changed, then writes the conflist from `spec.pod_network` if it
    /// changed, or removes it when there is no network. Returns whether
    /// either file changed.
    ///
    /// The drop-in comes first, so a failure in the CNI step leaves it
    /// written.
    fn reconcile<'a>(
        &'a self,
        spec: &'a NodeRuntimeSpec,
    ) -> BoxFuture<'a, Result<bool, RuntimeError>> {
        let this = self.clone();
        let spec = spec.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let drop_in = write_if_changed(
                    &this.conf_dir,
                    &this.sandbox_image,
                    &this.cni_bin_dir,
                    &this.cni_conf_dir,
                )
                .map_err(|e| (WRITE_DROP_IN, e))?;
                let conflist =
                    apply_pod_network(&this.cni_conf_dir, &this.bridge, spec.pod_network.as_ref())?;
                Ok(drop_in || conflist)
            })
            .await
            .map_err(|source| RuntimeError::Task {
                action: RECONCILE,
                source,
            })?
            .map_err(|(action, source)| RuntimeError::Io { action, source })
        })
    }

    /// On a blocking thread: removes the conflist if it is there. The drop-in
    /// is not read or written. A missing `cni_conf_dir` is not an error.
    fn remove_pod_network(&self) -> BoxFuture<'_, Result<bool, RuntimeError>> {
        let cni_conf_dir = self.cni_conf_dir.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                file::remove_if_exists(&cni_conf_dir, CONFLIST_FILE)
            })
            .await
            .map_err(|source| RuntimeError::Task {
                action: REMOVE_CONFLIST,
                source,
            })?
            .map_err(|source| RuntimeError::Io {
                action: REMOVE_CONFLIST,
                source,
            })
        })
    }
}

/// The drop-in, containerd config **version 2** CRI keys:
/// - `[plugins."io.containerd.grpc.v1.cri"] sandbox_image`
/// - `[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]`
///   `runtime_type = "io.containerd.runc.v2"`
/// - `[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]`
///   `SystemdCgroup = true`, which the pod cgroup parent (a systemd slice)
///   needs
/// - `[plugins."io.containerd.grpc.v1.cri".cni] bin_dir` and `conf_dir`
///
/// Starts with [`MANAGED_MARKER`] and `version = 2`. Strings are TOML-escaped.
/// Deterministic for equal input.
fn render(sandbox_image: &str, cni_bin_dir: &Path, cni_conf_dir: &Path) -> String {
    const CRI: &str = r#"plugins."io.containerd.grpc.v1.cri""#;
    format!(
        "{MANAGED_MARKER}\n\
         version = 2\n\
         \n\
         [{CRI}]\n  sandbox_image = {sandbox}\n\
         \n\
         [{CRI}.containerd.runtimes.runc]\n  runtime_type = \"io.containerd.runc.v2\"\n\
         \n\
         [{CRI}.containerd.runtimes.runc.options]\n  SystemdCgroup = true\n\
         \n\
         [{CRI}.cni]\n  bin_dir = {bin}\n  conf_dir = {conf}\n",
        sandbox = toml_string(sandbox_image),
        bin = toml_string(&cni_bin_dir.to_string_lossy()),
        conf = toml_string(&cni_conf_dir.to_string_lossy()),
    )
}

/// `value` as a TOML basic string, quotes included. Escapes `"`, `\` and
/// every control character (U+0000..=U+001F and U+007F).
fn toml_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if c.is_control() && (c as u32) <= 0x7f => {
                out.push_str(&format!("\\u{:04X}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Writes [`render`] to `<dir>/faber.toml` unless the file already holds
/// exactly that (`file::write_if_changed`). Returns whether it wrote.
fn write_if_changed(
    dir: &Path,
    sandbox_image: &str,
    cni_bin_dir: &Path,
    cni_conf_dir: &Path,
) -> io::Result<bool> {
    file::write_if_changed(
        dir,
        DROP_IN_FILE,
        render(sandbox_image, cni_bin_dir, cni_conf_dir).as_bytes(),
    )
}

/// Brings `<dir>/10-faber.conflist` to `network`: written if it differs,
/// removed when there is no network. Returns whether the directory changed.
///
/// A node without a subnet must not keep a config for one that may now belong
/// to another node. A missing `dir` is an error only when there is a file to
/// write.
fn apply_pod_network(
    dir: &Path,
    bridge: &str,
    network: Option<&PodNetworkSpec>,
) -> Result<bool, (&'static str, io::Error)> {
    match network {
        Some(network) => {
            file::write_if_changed(dir, CONFLIST_FILE, cni::render(network, bridge).as_bytes())
                .map_err(|e| (WRITE_CONFLIST, e))
        }
        None => file::remove_if_exists(dir, CONFLIST_FILE).map_err(|e| (REMOVE_CONFLIST, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIN: &str = "/opt/cni/bin";
    const CONF: &str = "/etc/cni/net.d";

    const IMAGE: &str = "registry.k8s.io/pause:3.10";

    fn render_with(sandbox_image: &str) -> String {
        render(sandbox_image, Path::new(BIN), Path::new(CONF))
    }

    fn write_with(dir: &Path, sandbox_image: &str) -> io::Result<bool> {
        write_if_changed(dir, sandbox_image, Path::new(BIN), Path::new(CONF))
    }

    /// containerd with these directories and the default image and bridge.
    fn containerd(conf_dir: impl Into<PathBuf>, cni_conf_dir: impl Into<PathBuf>) -> Containerd {
        Containerd {
            conf_dir: conf_dir.into(),
            cni_conf_dir: cni_conf_dir.into(),
            cni_bin_dir: BIN.into(),
            sandbox_image: IMAGE.into(),
            bridge: "faberbr0".into(),
        }
    }

    /// No container network.
    fn node_spec() -> NodeRuntimeSpec {
        NodeRuntimeSpec { pod_network: None }
    }

    #[test]
    fn render_golden_defaults() {
        let expected = r#"# managed by faber
version = 2

[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true

[plugins."io.containerd.grpc.v1.cri".cni]
  bin_dir = "/opt/cni/bin"
  conf_dir = "/etc/cni/net.d"
"#;
        assert_eq!(render_with(IMAGE), expected);
    }

    #[test]
    fn render_custom_values_escaped() {
        let out = render(
            "reg.example/pause:\"x\"",
            Path::new("C:\\cni\\bin"),
            Path::new("/etc/cni\n[evil]"),
        );
        assert!(
            out.starts_with("# managed by faber\nversion = 2\n"),
            "{out}"
        );
        // The cgroup driver is not a setting: the pod cgroup parent is a
        // systemd slice.
        assert!(out.contains("  SystemdCgroup = true\n"), "{out}");
        assert!(
            out.contains(r#"  sandbox_image = "reg.example/pause:\"x\"""#),
            "{out}"
        );
        assert!(out.contains(r#"  bin_dir = "C:\\cni\\bin""#), "{out}");
        assert!(out.contains(r#"  conf_dir = "/etc/cni\n[evil]""#), "{out}");
        assert!(!out.contains("\n[evil]"), "{out}");
    }

    #[test]
    fn toml_string_escapes() {
        assert_eq!(toml_string(""), r#""""#);
        assert_eq!(toml_string("plain /a/b:c"), r#""plain /a/b:c""#);
        assert_eq!(toml_string(r#"a"b"#), r#""a\"b""#);
        assert_eq!(toml_string(r"a\b"), r#""a\\b""#);
        assert_eq!(toml_string("\u{8}\t\n\u{c}\r"), r#""\b\t\n\f\r""#);
        assert_eq!(toml_string("\u{0}\u{1f}\u{7f}"), r#""\u0000\u001F\u007F""#);
        assert_eq!(
            toml_string("caf\u{e9} \u{1F600}"),
            "\"caf\u{e9} \u{1F600}\""
        );
    }

    #[test]
    fn write_if_changed_writes_then_skips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DROP_IN_FILE);

        assert!(write_with(dir.path(), IMAGE).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), render_with(IMAGE));
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();

        assert!(!write_with(dir.path(), IMAGE).unwrap());
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);

        assert!(write_with(dir.path(), "other/pause:1").unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            render_with("other/pause:1")
        );

        // Only the drop-in is left; no temp files.
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from(DROP_IN_FILE)]);
    }

    #[test]
    fn write_if_changed_replaces_foreign_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DROP_IN_FILE);
        std::fs::write(&path, "junk").unwrap();
        assert!(write_with(dir.path(), IMAGE).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), render_with(IMAGE));
    }

    #[test]
    fn write_if_changed_missing_dir_errors() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(write_with(&missing, IMAGE).is_err());
    }

    #[tokio::test]
    async fn reconcile_writes_once_then_reports_no_change() {
        let dir = tempfile::tempdir().unwrap();
        let runtime: std::sync::Arc<dyn ContainerRuntime> =
            std::sync::Arc::new(containerd(dir.path().to_path_buf(), dir.path().join("cni")));
        let spec = node_spec();
        assert!(runtime.reconcile(&spec).await.unwrap());
        assert_eq!(
            std::fs::read_to_string(dir.path().join(DROP_IN_FILE)).unwrap(),
            render(IMAGE, Path::new(BIN), &dir.path().join("cni"))
        );
        assert!(!runtime.reconcile(&spec).await.unwrap());
    }

    #[tokio::test]
    async fn reconcile_reports_what_failed_and_why() {
        use std::error::Error as _;
        let dir = tempfile::tempdir().unwrap();
        let runtime = containerd(dir.path().join("nope"), dir.path().join("cni"));
        let err = runtime.reconcile(&node_spec()).await.unwrap_err();
        assert_eq!(err.to_string(), "writing the containerd drop-in");
        assert!(err.source().is_some());
    }

    /// A runtime with both directories present, and the conflist path.
    fn runtime_with_cni(dir: &Path) -> (Containerd, PathBuf) {
        let cni_conf_dir = dir.join("cni");
        std::fs::create_dir(&cni_conf_dir).unwrap();
        let conflist = cni_conf_dir.join(CONFLIST_FILE);
        let runtime = containerd(dir.to_path_buf(), cni_conf_dir);
        (runtime, conflist)
    }

    fn node_spec_with_subnet(subnet: &str) -> NodeRuntimeSpec {
        NodeRuntimeSpec {
            pod_network: Some(PodNetworkSpec {
                subnet: subnet.parse().unwrap(),
                mtu: 1420,
            }),
        }
    }

    #[tokio::test]
    async fn reconcile_writes_the_conflist_then_reports_no_change() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        let spec = node_spec_with_subnet("10.200.3.0/24");

        assert!(runtime.reconcile(&spec).await.unwrap());
        assert_eq!(
            std::fs::read_to_string(&conflist).unwrap(),
            cni::render(spec.pod_network.as_ref().unwrap(), "faberbr0")
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join(DROP_IN_FILE)).unwrap(),
            render(IMAGE, Path::new(BIN), &dir.path().join("cni"))
        );
        assert!(!runtime.reconcile(&spec).await.unwrap());

        // Only the conflist is left; no temp files.
        let names: Vec<_> = std::fs::read_dir(conflist.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from(CONFLIST_FILE)]);
    }

    #[tokio::test]
    async fn reconcile_reports_a_change_when_only_the_conflist_is_new() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        assert!(runtime.reconcile(&node_spec()).await.unwrap());
        assert!(!conflist.exists());

        // The drop-in is already in place; the subnet arrives later.
        let spec = node_spec_with_subnet("10.200.3.0/24");
        assert!(runtime.reconcile(&spec).await.unwrap());
        assert!(conflist.exists());
    }

    #[tokio::test]
    async fn reconcile_rewrites_the_conflist_when_the_subnet_changes() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        assert!(
            runtime
                .reconcile(&node_spec_with_subnet("10.200.3.0/24"))
                .await
                .unwrap()
        );

        let moved = node_spec_with_subnet("10.200.4.0/24");
        assert!(runtime.reconcile(&moved).await.unwrap());
        assert_eq!(
            std::fs::read_to_string(&conflist).unwrap(),
            cni::render(moved.pod_network.as_ref().unwrap(), "faberbr0")
        );
        assert!(!runtime.reconcile(&moved).await.unwrap());
    }

    #[tokio::test]
    async fn reconcile_removes_the_conflist_without_a_pod_network() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        assert!(
            runtime
                .reconcile(&node_spec_with_subnet("10.200.3.0/24"))
                .await
                .unwrap()
        );

        assert!(runtime.reconcile(&node_spec()).await.unwrap());
        assert!(!conflist.exists());
        assert!(dir.path().join(DROP_IN_FILE).exists());
    }

    #[tokio::test]
    async fn reconcile_without_a_pod_network_or_conflist_reports_no_change() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        assert!(runtime.reconcile(&node_spec()).await.unwrap());
        assert!(!runtime.reconcile(&node_spec()).await.unwrap());
        assert!(!conflist.exists());
    }

    #[tokio::test]
    async fn reconcile_leaves_other_cni_files_alone() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        let other = conflist.with_file_name("99-other.conflist");
        std::fs::write(&other, "{}").unwrap();

        runtime
            .reconcile(&node_spec_with_subnet("10.200.3.0/24"))
            .await
            .unwrap();
        runtime.reconcile(&node_spec()).await.unwrap();
        assert_eq!(std::fs::read_to_string(&other).unwrap(), "{}");
    }

    #[tokio::test]
    async fn reconcile_missing_cni_dir_errors_after_writing_the_drop_in() {
        use std::error::Error as _;
        let dir = tempfile::tempdir().unwrap();
        let runtime = containerd(dir.path().to_path_buf(), dir.path().join("nope"));
        let spec = node_spec_with_subnet("10.200.3.0/24");
        let err = runtime.reconcile(&spec).await.unwrap_err();
        assert_eq!(err.to_string(), "writing the CNI network config");
        assert!(err.source().is_some());
        assert_eq!(
            std::fs::read_to_string(dir.path().join(DROP_IN_FILE)).unwrap(),
            render(IMAGE, Path::new(BIN), &dir.path().join("nope"))
        );
    }

    #[tokio::test]
    async fn reconcile_missing_cni_dir_is_fine_without_a_pod_network() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = containerd(dir.path().to_path_buf(), dir.path().join("nope"));
        assert!(runtime.reconcile(&node_spec()).await.unwrap());
        assert!(!runtime.reconcile(&node_spec()).await.unwrap());
    }

    #[tokio::test]
    async fn remove_pod_network_removes_the_conflist_and_leaves_the_drop_in() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        let spec = node_spec_with_subnet("10.200.3.0/24");
        runtime.reconcile(&spec).await.unwrap();
        // Content `reconcile` would replace, so a rewrite would show.
        let drop_in = dir.path().join(DROP_IN_FILE);
        std::fs::write(&drop_in, "edited").unwrap();

        let runtime: std::sync::Arc<dyn ContainerRuntime> = std::sync::Arc::new(runtime);
        assert!(runtime.remove_pod_network().await.unwrap());
        assert!(!conflist.exists());
        assert_eq!(std::fs::read_to_string(&drop_in).unwrap(), "edited");

        assert!(!runtime.remove_pod_network().await.unwrap());
    }

    #[tokio::test]
    async fn remove_pod_network_without_a_conflist_reports_no_change() {
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        let other = conflist.with_file_name("99-other.conflist");
        std::fs::write(&other, "{}").unwrap();

        assert!(!runtime.remove_pod_network().await.unwrap());
        assert!(other.exists());
        // Nothing was configured, so there is no drop-in to write.
        assert!(!dir.path().join(DROP_IN_FILE).exists());
    }

    #[tokio::test]
    async fn remove_pod_network_needs_neither_directory() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = containerd(dir.path().join("nope"), dir.path().join("nor-this"));
        assert!(!runtime.remove_pod_network().await.unwrap());
    }

    #[tokio::test]
    async fn remove_pod_network_works_when_the_drop_in_cannot_be_written() {
        let dir = tempfile::tempdir().unwrap();
        let cni_conf_dir = dir.path().join("cni");
        std::fs::create_dir(&cni_conf_dir).unwrap();
        let conflist = cni_conf_dir.join(CONFLIST_FILE);
        std::fs::write(&conflist, "{}").unwrap();
        let runtime = containerd(dir.path().join("nope"), cni_conf_dir);
        assert!(runtime.remove_pod_network().await.unwrap());
        assert!(!conflist.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconcile_sets_conflist_mode_0644() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let (runtime, conflist) = runtime_with_cni(dir.path());
        runtime
            .reconcile(&node_spec_with_subnet("10.200.3.0/24"))
            .await
            .unwrap();
        let mode = std::fs::metadata(&conflist).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o644);
    }

    #[cfg(unix)]
    #[test]
    fn write_if_changed_sets_mode_0644() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        write_with(dir.path(), IMAGE).unwrap();
        let mode = std::fs::metadata(dir.path().join(DROP_IN_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o644);
    }
}
