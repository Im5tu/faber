//! WireGuard interface management.
//!
//! Bring-up order:
//! 1. `ip link add <iface> type wireguard`, only if the link is missing
//! 2. `ip -o -4 address show dev <iface>`, then `ip address del` for every
//!    IPv4 address on the link other than the wanted one
//! 3. `ip address replace <address> dev <iface>`
//! 4. `wg syncconf <iface> /dev/stdin`, with [`render_syncconf`] on stdin
//! 5. `ip link set <iface> mtu 1420 up`
//! 6. `ip route replace <net> dev <iface>` for every entry of
//!    [`WireGuardSpec::routes`]
//!
//! [`create`] is the one way to make the link for a caller that must not
//! take over a link it did not create. [`remove`] deletes the link.
//!
//! The caller chooses the peers and the networks in each one's AllowedIPs:
//! the peer's mesh `/32`, plus its container subnet once it has one. A
//! route sends a network into the interface; AllowedIPs then picks the peer
//! that gets the packet, and limits the source addresses a peer may send
//! from. A peer with no networks gets no `AllowedIPs` line, so nothing is
//! routed to it and nothing is accepted from it.
//!
//! Routes that left the spec are not removed. The container range does not
//! change once a mesh exists, and deleting the link removes its routes.

use std::fmt;
use std::net::Ipv4Addr;

use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

use crate::{CommandRunner, NetworkError, check_iface_name, run_checked};

/// The full desired WireGuard state of one node.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireGuardSpec {
    /// This node's private key, standard base64. Redacted in `Debug`.
    pub private_key: String,
    pub listen_port: u16,
    /// This node's mesh IP with the mesh prefix length, e.g. `10.99.0.2/16`.
    /// Used by `ip address replace`; not part of the syncconf text.
    pub address: Ipv4Net,
    /// Sorted by the caller so renders are deterministic.
    pub peers: Vec<WireGuardPeer>,
    /// Networks routed through the interface with `ip route replace`, in
    /// this order, after the link is up.
    #[serde(default)]
    pub routes: Vec<Ipv4Net>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireGuardPeer {
    pub public_key: String,
    /// `ip:port`; `None` omits `Endpoint`.
    pub endpoint: Option<String>,
    /// Rendered as `AllowedIPs = <net>, <net>`, in this order. Empty omits
    /// the line.
    pub allowed_ips: Vec<Ipv4Net>,
    /// `PersistentKeepalive`, seconds.
    pub keepalive: u16,
}

impl fmt::Debug for WireGuardSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WireGuardSpec")
            .field("private_key", &"<redacted>")
            .field("listen_port", &self.listen_port)
            .field("address", &self.address)
            .field("peers", &self.peers)
            .field("routes", &self.routes)
            .finish()
    }
}

/// The `wg syncconf` input: an `[Interface]` section with `PrivateKey` and
/// `ListenPort`, then one `[Peer]` section per peer with `PublicKey`,
/// optional `Endpoint`, `AllowedIPs` (the peer's networks joined with `, `,
/// left out when there are none) and `PersistentKeepalive`.
/// Deterministic for equal input.
///
/// Values are written verbatim; callers must only pass validated keys and
/// endpoints, since a newline in a value would start a new config line.
pub fn render_syncconf(desired: &WireGuardSpec) -> String {
    let mut out = format!(
        "[Interface]\nPrivateKey = {}\nListenPort = {}\n",
        desired.private_key, desired.listen_port
    );
    for peer in &desired.peers {
        out.push_str("\n[Peer]\n");
        out.push_str(&format!("PublicKey = {}\n", peer.public_key));
        if let Some(endpoint) = &peer.endpoint {
            out.push_str(&format!("Endpoint = {endpoint}\n"));
        }
        if !peer.allowed_ips.is_empty() {
            let allowed: Vec<String> = peer.allowed_ips.iter().map(Ipv4Net::to_string).collect();
            out.push_str(&format!("AllowedIPs = {}\n", allowed.join(", ")));
        }
        out.push_str(&format!("PersistentKeepalive = {}\n", peer.keepalive));
    }
    out
}

/// Brings `iface` to `desired`, in the contract order. Idempotent. The
/// private key only ever goes to `wg` on stdin.
pub async fn ensure_up(
    runner: &dyn CommandRunner,
    iface: &str,
    desired: &WireGuardSpec,
) -> Result<(), NetworkError> {
    check_iface_name(iface)?;
    if !link_present(runner, iface).await? {
        add_link(runner, iface).await?;
    }
    remove_stale_addresses(runner, iface, desired.address).await?;
    let address = desired.address.to_string();
    run_checked(
        runner,
        "ip",
        &["address", "replace", &address, "dev", iface],
        None,
    )
    .await?;
    syncconf(runner, iface, desired).await?;
    let mtu = crate::WG_MTU.to_string();
    run_checked(
        runner,
        "ip",
        &["link", "set", iface, "mtu", &mtu, "up"],
        None,
    )
    .await?;
    // After link up: a route through a link that is down is rejected.
    for route in &desired.routes {
        let route = route.to_string();
        run_checked(
            runner,
            "ip",
            &["route", "replace", &route, "dev", iface],
            None,
        )
        .await?;
    }
    Ok(())
}

/// Creates the WireGuard link `iface`. Fails with
/// [`NetworkError::InterfaceExists`] when a link of that name is already
/// there, whoever made it, and runs nothing else.
pub async fn create(runner: &dyn CommandRunner, iface: &str) -> Result<(), NetworkError> {
    check_iface_name(iface)?;
    if link_present(runner, iface).await? {
        return Err(NetworkError::InterfaceExists(iface.to_string()));
    }
    add_link(runner, iface).await
}

/// Deletes the link `iface` (`ip link del`), and with it the key, the peers
/// and the addresses it carries. A link that is already gone is fine.
pub async fn remove(runner: &dyn CommandRunner, iface: &str) -> Result<(), NetworkError> {
    check_iface_name(iface)?;
    if !link_present(runner, iface).await? {
        return Ok(());
    }
    run_checked(runner, "ip", &["link", "del", iface], None).await?;
    Ok(())
}

/// Whether a link named `iface` exists. Checks the name, then runs only
/// `ip link show`.
pub async fn exists(runner: &dyn CommandRunner, iface: &str) -> Result<bool, NetworkError> {
    check_iface_name(iface)?;
    link_present(runner, iface).await
}

/// Whether `ip link show <iface>` finds the link.
async fn link_present(runner: &dyn CommandRunner, iface: &str) -> Result<bool, NetworkError> {
    let out = runner
        .run("ip", &["link", "show", iface], None)
        .await
        .map_err(|source| NetworkError::Spawn {
            cmd: format!("ip link show {iface}"),
            source,
        })?;
    Ok(out.status_ok)
}

async fn add_link(runner: &dyn CommandRunner, iface: &str) -> Result<(), NetworkError> {
    run_checked(
        runner,
        "ip",
        &["link", "add", iface, "type", "wireguard"],
        None,
    )
    .await?;
    Ok(())
}

/// Deletes every IPv4 address on `iface` other than `keep`, so an address
/// left over from an earlier identity cannot stay reachable. Runs before
/// `ip address replace`: deleting a primary address also removes the
/// secondaries in its subnet, and that can include `keep`. Setting `keep`
/// afterwards puts it back.
async fn remove_stale_addresses(
    runner: &dyn CommandRunner,
    iface: &str,
    keep: Ipv4Net,
) -> Result<(), NetworkError> {
    let listed = run_checked(
        runner,
        "ip",
        &["-o", "-4", "address", "show", "dev", iface],
        None,
    )
    .await?;
    for stale in parse_addresses(&String::from_utf8_lossy(&listed.stdout)) {
        if stale == keep {
            continue;
        }
        let stale = stale.to_string();
        run_checked(
            runner,
            "ip",
            &["address", "del", &stale, "dev", iface],
            None,
        )
        .await?;
    }
    Ok(())
}

/// The addresses in `ip -o -4 address show` output: the token after `inet`
/// on each line, as `ip/prefix` (a bare address is a `/32`). Tokens that are
/// not an address are skipped, so nothing unparsed reaches argv.
fn parse_addresses(output: &str) -> Vec<Ipv4Net> {
    output
        .lines()
        .filter_map(|line| {
            let mut tokens = line.split_whitespace();
            tokens.find(|t| *t == "inet")?;
            let token = tokens.next()?;
            token.parse::<Ipv4Net>().ok().or_else(|| {
                let ip = token.parse::<Ipv4Addr>().ok()?;
                Ipv4Net::new(ip, 32).ok()
            })
        })
        .collect()
}

/// `wg syncconf <iface> /dev/stdin`. `wg` echoes a malformed key in its
/// error, so the private key is scrubbed from stderr.
async fn syncconf(
    runner: &dyn CommandRunner,
    iface: &str,
    desired: &WireGuardSpec,
) -> Result<(), NetworkError> {
    let config = render_syncconf(desired).into_bytes();
    match run_checked(
        runner,
        "wg",
        &["syncconf", iface, "/dev/stdin"],
        Some(config),
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(NetworkError::Failed { cmd, code, stderr }) => Err(NetworkError::Failed {
            cmd,
            code,
            stderr: redact(&stderr, &desired.private_key),
        }),
        Err(e) => Err(e),
    }
}

fn redact(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        text.to_string()
    } else {
        text.replace(secret, "<redacted>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::RecordingRunner;
    use crate::{BoxFuture, CommandOutput};

    const PRIV: &str = "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=";

    fn peer(pk: &str, ep: Option<&str>, ip: &str) -> WireGuardPeer {
        WireGuardPeer {
            public_key: pk.into(),
            endpoint: ep.map(str::to_string),
            allowed_ips: vec![Ipv4Net::new(ip.parse().unwrap(), 32).unwrap()],
            keepalive: 25,
        }
    }

    fn desired() -> WireGuardSpec {
        WireGuardSpec {
            private_key: PRIV.into(),
            listen_port: 51820,
            address: "10.99.0.2/16".parse().unwrap(),
            peers: vec![
                peer(
                    "xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg=",
                    Some("203.0.113.1:51820"),
                    "10.99.0.1",
                ),
                peer(
                    "TrMvSoP4jYQlY6RIzBgbssQqY3vxI2Pi+y71lOWWXX0=",
                    None,
                    "10.99.0.3",
                ),
            ],
            routes: vec![],
        }
    }

    #[test]
    fn debug_redacts_private_key() {
        let d = WireGuardSpec {
            private_key: "c2VjcmV0LWtleS1tYXRlcmlhbA==".into(),
            listen_port: 51820,
            address: "10.99.0.2/16".parse().unwrap(),
            peers: vec![],
            routes: vec![],
        };
        let dbg = format!("{d:?}");
        assert!(!dbg.contains("c2VjcmV0"), "{dbg}");
        assert!(dbg.contains("51820"), "{dbg}");
    }

    #[test]
    fn render_syncconf_golden() {
        let expected = "\
[Interface]
PrivateKey = yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=
ListenPort = 51820

[Peer]
PublicKey = xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg=
Endpoint = 203.0.113.1:51820
AllowedIPs = 10.99.0.1/32
PersistentKeepalive = 25

[Peer]
PublicKey = TrMvSoP4jYQlY6RIzBgbssQqY3vxI2Pi+y71lOWWXX0=
AllowedIPs = 10.99.0.3/32
PersistentKeepalive = 25
";
        assert_eq!(render_syncconf(&desired()), expected);
    }

    #[test]
    fn render_syncconf_without_peers() {
        let mut d = desired();
        d.peers.clear();
        d.listen_port = 12345;
        assert_eq!(
            render_syncconf(&d),
            format!("[Interface]\nPrivateKey = {PRIV}\nListenPort = 12345\n")
        );
    }

    #[test]
    fn render_syncconf_ipv6_endpoint() {
        let mut d = desired();
        d.peers = vec![peer("pk", Some("[2001:db8::1]:51820"), "10.99.0.9")];
        assert!(render_syncconf(&d).contains("\nEndpoint = [2001:db8::1]:51820\n"));
    }

    #[test]
    fn render_syncconf_joins_a_peers_networks() {
        let mut d = desired();
        d.peers.truncate(1);
        d.peers[0]
            .allowed_ips
            .push("10.200.0.0/24".parse().unwrap());
        let expected = "\
[Interface]
PrivateKey = yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=
ListenPort = 51820

[Peer]
PublicKey = xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg=
Endpoint = 203.0.113.1:51820
AllowedIPs = 10.99.0.1/32, 10.200.0.0/24
PersistentKeepalive = 25
";
        assert_eq!(render_syncconf(&d), expected);
    }

    /// `AllowedIPs = ` with no value is not a valid config line, so the line
    /// is left out. `wg syncconf` then leaves the peer with no allowed IPs.
    #[test]
    fn render_syncconf_omits_allowed_ips_for_a_peer_without_networks() {
        let mut d = desired();
        d.peers.truncate(1);
        d.peers[0].allowed_ips.clear();
        let expected = "\
[Interface]
PrivateKey = yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=
ListenPort = 51820

[Peer]
PublicKey = xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg=
Endpoint = 203.0.113.1:51820
PersistentKeepalive = 25
";
        assert_eq!(render_syncconf(&d), expected);
    }

    #[tokio::test]
    async fn ensure_up_replaces_routes_in_order_after_link_up() {
        let r = RecordingRunner::new();
        let mut d = desired();
        d.routes = vec![
            "10.200.0.0/16".parse().unwrap(),
            "10.150.0.0/24".parse().unwrap(),
        ];
        ensure_up(&r, "faber0", &d).await.unwrap();
        assert_eq!(
            r.cmdlines(),
            vec![
                "ip link show faber0",
                "ip -o -4 address show dev faber0",
                "ip address replace 10.99.0.2/16 dev faber0",
                "wg syncconf faber0 /dev/stdin",
                "ip link set faber0 mtu 1420 up",
                "ip route replace 10.200.0.0/16 dev faber0",
                "ip route replace 10.150.0.0/24 dev faber0",
            ]
        );
        assert_key_only_on_stdin(&r);
    }

    #[tokio::test]
    async fn ensure_up_runs_no_route_command_without_routes() {
        let r = RecordingRunner::new();
        ensure_up(&r, "faber0", &desired()).await.unwrap();
        assert!(
            !r.cmdlines().iter().any(|l| l.starts_with("ip route")),
            "{:?}",
            r.cmdlines()
        );
    }

    #[tokio::test]
    async fn a_failing_route_command_is_reported() {
        let r = RecordingRunner::new();
        r.script(
            "ip",
            Some("route"),
            CommandOutput::failure(2, "RTNETLINK answers: Operation not permitted"),
        );
        let mut d = desired();
        d.routes = vec![
            "10.200.0.0/16".parse().unwrap(),
            "10.150.0.0/24".parse().unwrap(),
        ];
        let err = ensure_up(&r, "faber0", &d).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ip route replace 10.200.0.0/16 dev faber0"),
            "{msg}"
        );
        assert!(msg.contains("Operation not permitted"), "{msg}");
        // The first failure stops the bring-up.
        assert_eq!(
            r.cmdlines().last().map(String::as_str),
            Some("ip route replace 10.200.0.0/16 dev faber0")
        );
    }

    fn assert_key_only_on_stdin(r: &RecordingRunner) {
        for call in r.calls() {
            assert!(
                call.args.iter().all(|a| !a.contains(PRIV)),
                "key in argv: {}",
                call.cmdline()
            );
            if call.prog != "wg" {
                assert!(call.stdin.is_none(), "stdin on {}", call.cmdline());
            }
        }
    }

    #[tokio::test]
    async fn ensure_up_creates_missing_link_in_order() {
        let r = ShowFails::default();
        ensure_up(&r, "faber0", &desired()).await.unwrap();
        assert_eq!(
            r.inner.cmdlines(),
            vec![
                "ip link show faber0",
                "ip link add faber0 type wireguard",
                "ip -o -4 address show dev faber0",
                "ip address replace 10.99.0.2/16 dev faber0",
                "wg syncconf faber0 /dev/stdin",
                "ip link set faber0 mtu 1420 up",
            ]
        );
        let calls = r.inner.calls();
        assert_eq!(
            calls[4].stdin.as_deref(),
            Some(render_syncconf(&desired()).as_bytes())
        );
        assert_key_only_on_stdin(&r.inner);
    }

    #[tokio::test]
    async fn ensure_up_skips_link_add_when_present() {
        let r = RecordingRunner::new();
        ensure_up(&r, "faber0", &desired()).await.unwrap();
        assert_eq!(
            r.cmdlines(),
            vec![
                "ip link show faber0",
                "ip -o -4 address show dev faber0",
                "ip address replace 10.99.0.2/16 dev faber0",
                "wg syncconf faber0 /dev/stdin",
                "ip link set faber0 mtu 1420 up",
            ]
        );
        assert_key_only_on_stdin(&r);
    }

    /// Deleting a primary address can take the secondaries in its subnet
    /// with it, so the stale ones go first and the wanted one is set last.
    #[tokio::test]
    async fn ensure_up_removes_stale_addresses_before_setting_the_wanted_one() {
        let r = RecordingRunner::new();
        r.script(
            "ip",
            Some("-o"),
            CommandOutput::success_with(concat!(
                "7: faber0    inet 10.99.0.9/16 scope global faber0\\       valid_lft forever\n",
                "7: faber0    inet 10.99.0.2/16 scope global faber0\\       valid_lft forever\n",
                "7: faber0    inet 192.0.2.7 peer 192.0.2.8/32 scope global faber0\n",
            )),
        );
        ensure_up(&r, "faber0", &desired()).await.unwrap();
        assert_eq!(
            r.cmdlines(),
            vec![
                "ip link show faber0",
                "ip -o -4 address show dev faber0",
                "ip address del 10.99.0.9/16 dev faber0",
                "ip address del 192.0.2.7/32 dev faber0",
                "ip address replace 10.99.0.2/16 dev faber0",
                "wg syncconf faber0 /dev/stdin",
                "ip link set faber0 mtu 1420 up",
            ]
        );
    }

    #[test]
    fn listed_addresses_are_parsed_strictly() {
        let out = concat!(
            "7: faber0    inet 10.99.0.2/16 brd 10.99.255.255 scope global faber0\n",
            "7: faber0    inet 10.0.0.1 peer 10.0.0.2/32 scope global faber0\n",
            "7: faber0    inet6 fe80::1/64 scope link\n",
            "7: faber0    inet junk;rm scope global\n",
            "\n",
        );
        assert_eq!(
            parse_addresses(out),
            vec![
                "10.99.0.2/16".parse::<Ipv4Net>().unwrap(),
                "10.0.0.1/32".parse::<Ipv4Net>().unwrap(),
            ]
        );
        assert!(parse_addresses("").is_empty());
    }

    #[tokio::test]
    async fn a_failing_address_listing_stops_the_bring_up() {
        let r = RecordingRunner::new();
        r.script(
            "ip",
            Some("-o"),
            CommandOutput::failure(1, "Cannot open netlink"),
        );
        let err = ensure_up(&r, "faber0", &desired()).await.unwrap_err();
        assert!(err.to_string().contains("Cannot open netlink"), "{err}");
        assert!(!r.cmdlines().iter().any(|l| l.starts_with("wg ")));
    }

    #[tokio::test]
    async fn create_adds_a_missing_link() {
        let r = ShowFails::default();
        create(&r, "faber0").await.unwrap();
        assert_eq!(
            r.inner.cmdlines(),
            vec!["ip link show faber0", "ip link add faber0 type wireguard"]
        );
    }

    #[tokio::test]
    async fn create_refuses_an_existing_link() {
        let r = RecordingRunner::new();
        let err = create(&r, "faber0").await.unwrap_err();
        assert!(
            matches!(&err, NetworkError::InterfaceExists(n) if n == "faber0"),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            "interface faber0 exists and was not created by faber"
        );
        assert_eq!(r.cmdlines(), vec!["ip link show faber0"]);
        assert!(matches!(
            create(&r, "bad name").await,
            Err(NetworkError::InvalidInterface(_))
        ));
    }

    #[tokio::test]
    async fn remove_deletes_an_existing_link() {
        let r = RecordingRunner::new();
        remove(&r, "faber0").await.unwrap();
        assert_eq!(
            r.cmdlines(),
            vec!["ip link show faber0", "ip link del faber0"]
        );
    }

    #[tokio::test]
    async fn remove_of_a_missing_link_runs_nothing_else() {
        let r = ShowFails::default();
        remove(&r, "faber0").await.unwrap();
        assert_eq!(r.inner.cmdlines(), vec!["ip link show faber0"]);
        assert!(matches!(
            remove(&r, "bad name").await,
            Err(NetworkError::InvalidInterface(_))
        ));
    }

    #[tokio::test]
    async fn remove_reports_a_failing_delete() {
        let r = RecordingRunner::new();
        r.script(
            "ip",
            Some("link"),
            CommandOutput::failure(2, "RTNETLINK answers: Operation not permitted"),
        );
        // The script matches `ip link show` too; the wrapper lets that one
        // succeed, so only the delete fails.
        let r = FailDel { inner: r };
        let err = remove(&r, "faber0").await.unwrap_err();
        assert!(err.to_string().contains("Operation not permitted"), "{err}");
    }

    #[tokio::test]
    async fn ensure_up_stops_at_first_failure_with_stderr() {
        let r = RecordingRunner::new();
        r.script(
            "ip",
            Some("address"),
            CommandOutput::failure(2, "RTNETLINK: bad"),
        );
        let err = ensure_up(&r, "faber0", &desired()).await.unwrap_err();
        assert!(err.to_string().contains("RTNETLINK: bad"), "{err}");
        assert_eq!(
            r.cmdlines(),
            vec![
                "ip link show faber0",
                "ip -o -4 address show dev faber0",
                "ip address replace 10.99.0.2/16 dev faber0"
            ]
        );
    }

    #[tokio::test]
    async fn link_add_failure_is_reported() {
        let r = ShowFails::default();
        r.inner.script(
            "ip",
            Some("link"),
            CommandOutput::failure(2, "Unknown device type."),
        );
        let err = ensure_up(&r, "faber0", &desired()).await.unwrap_err();
        assert!(err.to_string().contains("Unknown device type."), "{err}");
        assert_eq!(r.inner.calls().len(), 2);
    }

    #[tokio::test]
    async fn syncconf_failure_redacts_private_key_from_stderr() {
        let r = RecordingRunner::new();
        r.script(
            "wg",
            Some("syncconf"),
            CommandOutput::failure(
                1,
                format!("Key is not the correct length or format: `{PRIV}'"),
            ),
        );
        let err = ensure_up(&r, "faber0", &desired()).await.unwrap_err();
        let msg = err.to_string();
        assert!(!msg.contains(PRIV), "{msg}");
        assert!(msg.contains("<redacted>"), "{msg}");
        assert!(!format!("{err:?}").contains(PRIV));
    }

    #[tokio::test]
    async fn rejects_invalid_iface_without_running_anything() {
        let r = RecordingRunner::new();
        assert!(matches!(
            ensure_up(&r, "faber0; rm", &desired()).await,
            Err(NetworkError::InvalidInterface(_))
        ));
        assert!(ensure_up(&r, "", &desired()).await.is_err());
        assert!(r.calls().is_empty());
    }

    #[tokio::test]
    async fn spawn_error_is_reported() {
        let r = RecordingRunner::new();
        r.script_error("wg", None, std::io::ErrorKind::NotFound);
        let err = ensure_up(&r, "faber0", &desired()).await.unwrap_err();
        assert!(matches!(err, NetworkError::Spawn { .. }), "{err:?}");
    }

    /// `ip link show` succeeds whatever the inner runner says; everything
    /// else is the inner runner's answer.
    struct FailDel {
        inner: RecordingRunner,
    }

    impl CommandRunner for FailDel {
        fn run<'a>(
            &'a self,
            prog: &'a str,
            args: &'a [&'a str],
            stdin: Option<Vec<u8>>,
        ) -> BoxFuture<'a, std::io::Result<CommandOutput>> {
            Box::pin(async move {
                let out = self.inner.run(prog, args, stdin).await?;
                if prog == "ip" && args.get(1) == Some(&"show") {
                    Ok(CommandOutput::success())
                } else {
                    Ok(out)
                }
            })
        }
    }

    /// Records like [`RecordingRunner`], but `ip link show` always fails as
    /// if the link were missing. (`RecordingRunner` matches on the first arg
    /// only, so it cannot fail `link show` while `link add` succeeds.)
    #[derive(Default)]
    struct ShowFails {
        inner: RecordingRunner,
    }

    impl CommandRunner for ShowFails {
        fn run<'a>(
            &'a self,
            prog: &'a str,
            args: &'a [&'a str],
            stdin: Option<Vec<u8>>,
        ) -> BoxFuture<'a, std::io::Result<CommandOutput>> {
            Box::pin(async move {
                let out = self.inner.run(prog, args, stdin).await?;
                if prog == "ip" && args.get(1) == Some(&"show") {
                    Ok(CommandOutput::failure(
                        1,
                        "Device \"faber0\" does not exist.",
                    ))
                } else {
                    Ok(out)
                }
            })
        }
    }
}
