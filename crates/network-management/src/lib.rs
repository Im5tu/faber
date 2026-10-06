//! Host network configuration for a faber node: the WireGuard interface and
//! the `inet faber` nftables table.
//!
//! The caller describes the network it wants as a [`NetworkSpec`] and hands
//! it to [`NetworkManager::reconcile`], which brings the host to that state.
//! How that happens stays in this crate.
//!
//! Nothing here links to netlink; it shells out to `ip`, `wg` and `nft`
//! through [`CommandRunner`], so tests swap in [`fake::RecordingRunner`].
//! `CAP_NET_ADMIN` granted to the calling service by systemd is ambient, so
//! it carries through exec. Secrets (the WireGuard private key) go on stdin,
//! never argv.

mod nftables;
mod wireguard;

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

pub use nftables::{ContainerFirewall, FirewallSpec, endpoint_ip};
pub use wireguard::{WireGuardPeer, WireGuardSpec};

/// Default WireGuard interface name.
pub const DEFAULT_WG_INTERFACE: &str = "faber0";
/// MTU set on the WireGuard interface.
pub const WG_MTU: u16 = 1420;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The network state one node should have. `Debug` redacts the WireGuard
/// private key; the serialized form contains it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkSpec {
    /// WireGuard interface name, e.g. `faber0`.
    pub interface: String,
    pub wireguard: WireGuardSpec,
    pub firewall: FirewallSpec,
}

/// Applies a [`NetworkSpec`] to this host.
#[derive(Clone)]
pub struct NetworkManager {
    runner: Arc<dyn CommandRunner>,
}

impl NetworkManager {
    /// Runs host commands through `runner`.
    pub fn new(runner: Arc<dyn CommandRunner>) -> Self {
        Self { runner }
    }

    /// Runs real processes ([`SystemRunner`]).
    pub fn system() -> Self {
        Self::new(Arc::new(SystemRunner))
    }

    /// Whether a link named `interface` exists, whoever made it. Runs only
    /// `ip link show`.
    pub async fn interface_exists(&self, interface: &str) -> Result<bool, NetworkError> {
        wireguard::exists(self.runner.as_ref(), interface).await
    }

    /// Creates the WireGuard link `interface` and nothing else. Fails with
    /// [`NetworkError::InterfaceExists`] if a link of that name is already
    /// there. A caller that must not take over someone else's interface
    /// calls this once, records that it did, and only then uses
    /// [`Self::reconcile`].
    pub async fn create_interface(&self, interface: &str) -> Result<(), NetworkError> {
        wireguard::create(self.runner.as_ref(), interface).await
    }

    /// Deletes the link `interface` (`ip link del`), taking its key, peers
    /// and addresses with it. A link that is already gone is fine. For a
    /// caller that created the link and must stop using the key on it.
    pub async fn teardown(&self, interface: &str) -> Result<(), NetworkError> {
        wireguard::remove(self.runner.as_ref(), interface).await
    }

    /// Brings `spec.interface` to `spec.wireguard` (link, address,
    /// `wg syncconf`, MTU and up, routes), then loads `spec.firewall` with
    /// `nft -f -`. Idempotent. Stops at the first failing command.
    ///
    /// A missing link is created; an existing one is used as it is, with
    /// every IPv4 address other than the wanted one removed. Whether the
    /// caller may use an existing link is the caller's decision (see
    /// [`Self::create_interface`]).
    pub async fn reconcile(&self, spec: &NetworkSpec) -> Result<(), NetworkError> {
        let runner = self.runner.as_ref();
        wireguard::ensure_up(runner, &spec.interface, &spec.wireguard).await?;
        nftables::apply(runner, &spec.interface, &spec.firewall).await
    }
}

impl fmt::Debug for NetworkManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetworkManager").finish_non_exhaustive()
    }
}

/// Result of one finished command.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandOutput {
    /// The command exited with status 0.
    pub status_ok: bool,
    /// Exit code; `None` when killed by a signal.
    pub code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    /// Exit 0 with empty output.
    pub fn success() -> Self {
        Self::success_with(Vec::new())
    }

    /// Exit 0 with `stdout`.
    pub fn success_with(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            status_ok: true,
            code: Some(0),
            stdout: stdout.into(),
            stderr: Vec::new(),
        }
    }

    /// Exit `code` (non-zero) with `stderr`.
    pub fn failure(code: i32, stderr: impl Into<Vec<u8>>) -> Self {
        Self {
            status_ok: false,
            code: Some(code),
            stdout: Vec::new(),
            stderr: stderr.into(),
        }
    }
}

/// Runs an external program to completion. Object safe, so it is shared as
/// `Arc<dyn CommandRunner>`.
///
/// A non-zero exit is `Ok` with `status_ok == false`; `Err` means the program
/// could not be started or its pipes failed.
pub trait CommandRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        prog: &'a str,
        args: &'a [&'a str],
        stdin: Option<Vec<u8>>,
    ) -> BoxFuture<'a, io::Result<CommandOutput>>;
}

/// Runs real processes with `tokio::process`. stdin is piped only when given,
/// otherwise it is null; stdout and stderr are captured.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemRunner;

impl CommandRunner for SystemRunner {
    fn run<'a>(
        &'a self,
        prog: &'a str,
        args: &'a [&'a str],
        stdin: Option<Vec<u8>>,
    ) -> BoxFuture<'a, io::Result<CommandOutput>> {
        Box::pin(async move {
            let mut cmd = tokio::process::Command::new(prog);
            cmd.args(args)
                .stdin(if stdin.is_some() {
                    Stdio::piped()
                } else {
                    Stdio::null()
                })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            let mut child = cmd.spawn()?;
            if let Some(input) = stdin {
                let mut pipe = child
                    .stdin
                    .take()
                    .ok_or_else(|| io::Error::other("stdin not piped"))?;
                pipe.write_all(&input).await?;
                pipe.shutdown().await?;
                drop(pipe);
            }
            let out = child.wait_with_output().await?;
            Ok(CommandOutput {
                status_ok: out.status.success(),
                code: out.status.code(),
                stdout: out.stdout,
                stderr: out.stderr,
            })
        })
    }
}

/// Why a host command failed. `cmd` is the argv joined with spaces; stdin is
/// never part of it, so secrets passed on stdin do not end up in errors.
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("invalid interface name {0:?}")]
    InvalidInterface(String),
    #[error("interface {0} exists and was not created by faber")]
    InterfaceExists(String),
    #[error("container bridge {0} has the same name as the mesh interface")]
    BridgeIsMeshInterface(String),
    #[error("`{cmd}` could not be run: {source}")]
    Spawn {
        cmd: String,
        #[source]
        source: io::Error,
    },
    #[error("`{cmd}` failed ({}): {stderr}", describe_code(.code))]
    Failed {
        cmd: String,
        code: Option<i32>,
        /// Trimmed, lossy UTF-8 stderr of the command.
        stderr: String,
    },
}

fn describe_code(code: &Option<i32>) -> String {
    match code {
        Some(c) => format!("exit {c}"),
        None => "killed by signal".to_string(),
    }
}

/// Runs `prog args` and maps a spawn error or non-zero exit to [`NetworkError`].
pub(crate) async fn run_checked(
    runner: &dyn CommandRunner,
    prog: &str,
    args: &[&str],
    stdin: Option<Vec<u8>>,
) -> Result<CommandOutput, NetworkError> {
    let cmd = || {
        std::iter::once(prog)
            .chain(args.iter().copied())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let out = runner
        .run(prog, args, stdin)
        .await
        .map_err(|source| NetworkError::Spawn { cmd: cmd(), source })?;
    if out.status_ok {
        Ok(out)
    } else {
        Err(NetworkError::Failed {
            cmd: cmd(),
            code: out.code,
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        })
    }
}

/// A Linux interface name that is safe to put in argv and in nft scripts:
/// 1 to 15 bytes of `[A-Za-z0-9_.-]`, and not `.` or `..`.
pub(crate) fn is_valid_iface_name(name: &str) -> bool {
    (1..=15).contains(&name.len())
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// Errors unless [`is_valid_iface_name`].
pub(crate) fn check_iface_name(name: &str) -> Result<(), NetworkError> {
    if is_valid_iface_name(name) {
        Ok(())
    } else {
        Err(NetworkError::InvalidInterface(name.to_string()))
    }
}

/// Test double for [`CommandRunner`]. Public (but hidden) so integration
/// tests under `tests/` can use it.
#[doc(hidden)]
pub mod fake {
    use std::io;
    use std::sync::Mutex;

    use super::{BoxFuture, CommandOutput, CommandRunner};

    /// One recorded invocation.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Call {
        pub prog: String,
        pub args: Vec<String>,
        pub stdin: Option<Vec<u8>>,
    }

    impl Call {
        /// `prog arg1 arg2 ...`, for readable assertions. Excludes stdin.
        pub fn cmdline(&self) -> String {
            std::iter::once(self.prog.as_str())
                .chain(self.args.iter().map(String::as_str))
                .collect::<Vec<_>>()
                .join(" ")
        }
    }

    type Scripted = Result<CommandOutput, io::ErrorKind>;

    #[derive(Debug)]
    struct Script {
        prog: String,
        first_arg: Option<String>,
        result: Scripted,
    }

    /// Records every call and answers with scripted outputs.
    ///
    /// A call matches a script when `prog` is equal and, if the script gave
    /// one, the first argument is equal. The most recently added matching
    /// script wins, and scripts are not consumed. With no match the call
    /// succeeds with empty output.
    #[derive(Debug, Default)]
    pub struct RecordingRunner {
        calls: Mutex<Vec<Call>>,
        scripts: Mutex<Vec<Script>>,
    }

    impl RecordingRunner {
        pub fn new() -> Self {
            Self::default()
        }

        /// Answer calls to `prog` (with `first_arg`, if given) with `output`.
        pub fn script(&self, prog: &str, first_arg: Option<&str>, output: CommandOutput) {
            self.push(prog, first_arg, Ok(output));
        }

        /// Make calls to `prog` (with `first_arg`, if given) fail to start.
        pub fn script_error(&self, prog: &str, first_arg: Option<&str>, kind: io::ErrorKind) {
            self.push(prog, first_arg, Err(kind));
        }

        /// Every call so far, in order.
        pub fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }

        /// [`Call::cmdline`] of every call so far.
        pub fn cmdlines(&self) -> Vec<String> {
            self.calls().iter().map(Call::cmdline).collect()
        }

        /// Forgets recorded calls; scripts stay.
        pub fn clear(&self) {
            self.calls.lock().unwrap().clear();
        }

        fn push(&self, prog: &str, first_arg: Option<&str>, result: Scripted) {
            self.scripts.lock().unwrap().push(Script {
                prog: prog.to_string(),
                first_arg: first_arg.map(str::to_string),
                result,
            });
        }

        fn answer(&self, prog: &str, args: &[&str]) -> Scripted {
            let scripts = self.scripts.lock().unwrap();
            scripts
                .iter()
                .rev()
                .find(|s| {
                    s.prog == prog
                        && s.first_arg
                            .as_deref()
                            .is_none_or(|a| args.first() == Some(&a))
                })
                .map(|s| s.result.clone())
                .unwrap_or_else(|| Ok(CommandOutput::success()))
        }
    }

    impl CommandRunner for RecordingRunner {
        fn run<'a>(
            &'a self,
            prog: &'a str,
            args: &'a [&'a str],
            stdin: Option<Vec<u8>>,
        ) -> BoxFuture<'a, io::Result<CommandOutput>> {
            self.calls.lock().unwrap().push(Call {
                prog: prog.to_string(),
                args: args.iter().map(|a| a.to_string()).collect(),
                stdin,
            });
            let result = self.answer(prog, args).map_err(io::Error::from);
            Box::pin(async move { result })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::fake::{Call, RecordingRunner};
    use super::*;

    #[tokio::test]
    async fn teardown_deletes_the_interface() {
        let runner = Arc::new(RecordingRunner::new());
        let net = NetworkManager::new(runner.clone());
        net.teardown("faber0").await.unwrap();
        assert_eq!(
            runner.cmdlines(),
            vec!["ip link show faber0", "ip link del faber0"]
        );
    }

    #[tokio::test]
    async fn recording_runner_records_calls_and_defaults_to_success() {
        let r = RecordingRunner::new();
        let out = r
            .run(
                "wg",
                &["syncconf", "faber0", "/dev/stdin"],
                Some(b"secret".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(out, CommandOutput::success());
        r.run("ip", &["link", "show", "faber0"], None)
            .await
            .unwrap();
        assert_eq!(
            r.calls(),
            vec![
                Call {
                    prog: "wg".into(),
                    args: vec!["syncconf".into(), "faber0".into(), "/dev/stdin".into()],
                    stdin: Some(b"secret".to_vec()),
                },
                Call {
                    prog: "ip".into(),
                    args: vec!["link".into(), "show".into(), "faber0".into()],
                    stdin: None,
                },
            ]
        );
        assert_eq!(
            r.cmdlines(),
            vec!["wg syncconf faber0 /dev/stdin", "ip link show faber0"]
        );
    }

    #[tokio::test]
    async fn recording_runner_matches_prog_and_first_arg() {
        let r = RecordingRunner::new();
        r.script(
            "ip",
            Some("link"),
            CommandOutput::failure(1, "no such device"),
        );
        r.script("nft", None, CommandOutput::success_with("table"));

        let link = r
            .run("ip", &["link", "show", "faber0"], None)
            .await
            .unwrap();
        assert!(!link.status_ok);
        assert_eq!(link.code, Some(1));
        assert_eq!(link.stderr, b"no such device");

        let addr = r.run("ip", &["address", "replace"], None).await.unwrap();
        assert!(addr.status_ok);

        let nft = r.run("nft", &["-f", "-"], None).await.unwrap();
        assert_eq!(nft.stdout, b"table");
    }

    #[tokio::test]
    async fn recording_runner_latest_script_wins() {
        let r = RecordingRunner::new();
        r.script("ip", Some("link"), CommandOutput::failure(1, ""));
        r.script("ip", Some("link"), CommandOutput::success_with("up"));
        let out = r.run("ip", &["link"], None).await.unwrap();
        assert_eq!(out.stdout, b"up");
    }

    #[tokio::test]
    async fn recording_runner_scripted_errors_and_clear() {
        let r = RecordingRunner::new();
        r.script_error("wg", None, io::ErrorKind::NotFound);
        let err = r.run("wg", &["show"], None).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(r.calls().len(), 1);
        r.clear();
        assert!(r.calls().is_empty());
        // Scripts survive `clear`.
        assert!(r.run("wg", &[], None).await.is_err());
    }

    #[tokio::test]
    async fn recording_runner_works_behind_dyn() {
        let r = Arc::new(RecordingRunner::new());
        let runner: Arc<dyn CommandRunner> = r.clone();
        runner.run("ip", &["link"], None).await.unwrap();
        assert_eq!(r.cmdlines(), vec!["ip link"]);
    }

    const PRIVATE_KEY: &str = "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=";

    fn network_spec(interface: &str) -> NetworkSpec {
        NetworkSpec {
            interface: interface.into(),
            wireguard: WireGuardSpec {
                private_key: PRIVATE_KEY.into(),
                listen_port: 51820,
                address: "10.99.0.2/16".parse().unwrap(),
                peers: vec![WireGuardPeer {
                    public_key: "xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg=".into(),
                    endpoint: Some("203.0.113.1:51820".into()),
                    allowed_ips: vec!["10.99.0.1/32".parse().unwrap()],
                    keepalive: 25,
                }],
                routes: vec![],
            },
            firewall: FirewallSpec {
                wg_port: 51820,
                peer_port: 7401,
                peer_endpoint_ips: ["203.0.113.1".parse().unwrap()].into_iter().collect(),
                peer_mesh_ips: ["10.99.0.1".parse().unwrap()].into_iter().collect(),
                containers: None,
            },
        }
    }

    #[tokio::test]
    async fn reconcile_brings_up_wireguard_then_loads_the_firewall() {
        let r = Arc::new(RecordingRunner::new());
        let spec = network_spec("wg9");
        NetworkManager::new(r.clone())
            .reconcile(&spec)
            .await
            .unwrap();
        assert_eq!(
            r.cmdlines(),
            vec![
                "ip link show wg9",
                "ip -o -4 address show dev wg9",
                "ip address replace 10.99.0.2/16 dev wg9",
                "wg syncconf wg9 /dev/stdin",
                "ip link set wg9 mtu 1420 up",
                "nft -f -",
            ]
        );
        let calls = r.calls();
        assert_eq!(
            calls[3].stdin.as_deref(),
            Some(wireguard::render_syncconf(&spec.wireguard).as_bytes())
        );
        assert_eq!(
            calls[5].stdin.as_deref(),
            Some(nftables::render("wg9", &spec.firewall).as_bytes())
        );
    }

    #[tokio::test]
    async fn reconcile_adds_routes_and_loads_the_container_firewall() {
        let r = Arc::new(RecordingRunner::new());
        let mut spec = network_spec("faber0");
        spec.wireguard.peers[0]
            .allowed_ips
            .push("10.200.0.0/24".parse().unwrap());
        spec.wireguard.routes = vec!["10.200.0.0/16".parse().unwrap()];
        spec.firewall.containers = Some(ContainerFirewall {
            cidr: "10.200.0.0/16".parse().unwrap(),
            local_subnet: Some("10.200.1.0/24".parse().unwrap()),
            node_mesh_ips: ["10.99.0.1".parse().unwrap()].into_iter().collect(),
            bridge: "faberbr0".into(),
        });
        NetworkManager::new(r.clone())
            .reconcile(&spec)
            .await
            .unwrap();
        assert_eq!(
            r.cmdlines(),
            vec![
                "ip link show faber0",
                "ip -o -4 address show dev faber0",
                "ip address replace 10.99.0.2/16 dev faber0",
                "wg syncconf faber0 /dev/stdin",
                "ip link set faber0 mtu 1420 up",
                "ip route replace 10.200.0.0/16 dev faber0",
                "nft -f -",
            ]
        );
        let calls = r.calls();
        let syncconf = String::from_utf8(calls[3].stdin.clone().unwrap()).unwrap();
        assert!(
            syncconf.contains("\nAllowedIPs = 10.99.0.1/32, 10.200.0.0/24\n"),
            "{syncconf}"
        );
        let script = String::from_utf8(calls[6].stdin.clone().unwrap()).unwrap();
        assert_eq!(script, nftables::render("faber0", &spec.firewall));
        assert!(script.contains("\tset node_mesh_ips {\n"), "{script}");
        assert!(
            script.contains("\t\tip saddr 10.200.1.0/24 ip daddr != 10.200.0.0/16 drop\n"),
            "{script}"
        );
    }

    #[tokio::test]
    async fn reconcile_stops_before_the_firewall_when_wireguard_fails() {
        let r = Arc::new(RecordingRunner::new());
        r.script("wg", None, CommandOutput::failure(1, "boom"));
        let err = NetworkManager::new(r.clone())
            .reconcile(&network_spec("faber0"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("wg syncconf faber0"), "{err}");
        assert!(!r.cmdlines().iter().any(|l| l == "nft -f -"));

        let r = Arc::new(RecordingRunner::new());
        let err = NetworkManager::new(r.clone())
            .reconcile(&network_spec("faber0; rm"))
            .await
            .unwrap_err();
        assert!(matches!(err, NetworkError::InvalidInterface(_)), "{err:?}");
        assert!(r.calls().is_empty());
    }

    #[test]
    fn default_interface_is_faber0() {
        assert_eq!(DEFAULT_WG_INTERFACE, "faber0");
        assert!(is_valid_iface_name(DEFAULT_WG_INTERFACE));
    }

    #[tokio::test]
    async fn create_interface_refuses_an_existing_one() {
        let r = Arc::new(RecordingRunner::new());
        let err = NetworkManager::new(r.clone())
            .create_interface("faber0")
            .await
            .unwrap_err();
        assert!(matches!(err, NetworkError::InterfaceExists(_)), "{err:?}");
        // Nothing beyond the existence check ran.
        assert_eq!(r.cmdlines(), vec!["ip link show faber0"]);
    }

    #[tokio::test]
    async fn interface_exists_only_looks() {
        let r = Arc::new(RecordingRunner::new());
        let net = NetworkManager::new(r.clone());
        assert!(net.interface_exists("faber0").await.unwrap());
        r.script(
            "ip",
            Some("link"),
            CommandOutput::failure(1, "no such device"),
        );
        assert!(!net.interface_exists("faber0").await.unwrap());
        assert_eq!(
            r.cmdlines(),
            vec!["ip link show faber0", "ip link show faber0"]
        );
        let err = net.interface_exists("faber0; rm").await.unwrap_err();
        assert!(matches!(err, NetworkError::InvalidInterface(_)), "{err:?}");
    }

    #[test]
    fn network_spec_round_trips_and_debug_redacts_the_key() {
        let spec = network_spec("faber0");
        let json = serde_json::to_string(&spec).unwrap();
        assert_eq!(serde_json::from_str::<NetworkSpec>(&json).unwrap(), spec);
        let dbg = format!("{spec:?}");
        assert!(!dbg.contains(PRIVATE_KEY), "{dbg}");
        assert!(dbg.contains("faber0"), "{dbg}");
        assert!(!format!("{:?}", NetworkManager::system()).contains(PRIVATE_KEY));
    }

    #[tokio::test]
    async fn system_runner_reports_missing_program() {
        let err = SystemRunner
            .run("faber-no-such-program-xyz", &[], None)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn run_checked_maps_failure_with_stderr() {
        let r = RecordingRunner::new();
        r.script("ip", Some("link"), CommandOutput::failure(2, "  boom\n"));
        let err = run_checked(&r, "ip", &["link", "add"], Some(b"secret".to_vec()))
            .await
            .unwrap_err();
        match &err {
            NetworkError::Failed { cmd, code, stderr } => {
                assert_eq!(cmd, "ip link add");
                assert_eq!(*code, Some(2));
                assert_eq!(stderr, "boom");
            }
            other => panic!("unexpected {other:?}"),
        }
        let msg = err.to_string();
        assert!(msg.contains("exit 2") && msg.contains("boom"), "{msg}");
        assert!(!msg.contains("secret"), "{msg}");
    }

    #[tokio::test]
    async fn run_checked_maps_spawn_error_and_success() {
        let r = RecordingRunner::new();
        r.script_error("wg", None, io::ErrorKind::NotFound);
        let err = run_checked(&r, "wg", &["show"], None).await.unwrap_err();
        assert!(matches!(err, NetworkError::Spawn { ref cmd, .. } if cmd == "wg show"));
        let out = run_checked(&r, "ip", &["a"], None).await.unwrap();
        assert!(out.status_ok);
    }

    #[test]
    fn iface_name_validation() {
        for ok in [
            "faber0",
            "wg-faber",
            "a",
            "eth0.100",
            "x_y",
            "abcdefghijklmno",
        ] {
            assert!(is_valid_iface_name(ok), "{ok}");
        }
        for bad in [
            "",
            ".",
            "..",
            "abcdefghijklmnop",
            "wg 0",
            "wg\"0",
            "wg/0",
            "faber0\n",
            "wg:0",
            "w\u{e9}",
        ] {
            assert!(!is_valid_iface_name(bad), "{bad:?}");
        }
        assert!(check_iface_name("faber0").is_ok());
        assert!(matches!(
            check_iface_name("a b"),
            Err(NetworkError::InvalidInterface(n)) if n == "a b"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn system_runner_pipes_stdin_and_captures_output() {
        let out = SystemRunner
            .run("cat", &[], Some(b"hello".to_vec()))
            .await
            .unwrap();
        assert!(out.status_ok);
        assert_eq!(out.stdout, b"hello");

        let out = SystemRunner
            .run("sh", &["-c", "echo oops >&2; exit 3"], None)
            .await
            .unwrap();
        assert!(!out.status_ok);
        assert_eq!(out.code, Some(3));
        assert_eq!(out.stderr, b"oops\n");
    }
}
