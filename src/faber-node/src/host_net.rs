//! What the host already uses of the IPv4 address space, so a container
//! subnet never lands on top of it.
//!
//! Read with `ip -j -4 addr show` and `ip -j -4 route show table all`, run by
//! absolute path like every other host tool of faber-node. The commands sit behind
//! [`HostNetworks`] so tests supply their own answer.

use std::sync::Arc;

use faber_cri::BoxFuture;
use faber_network_management::CommandRunner;
use ipnet::Ipv4Net;
use serde_json::Value;

use crate::volume::ToolPaths;

/// What a [`Claim`] is made of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimKind {
    /// The network of an address on an interface.
    Address,
    /// The destination of a route that sends traffic somewhere.
    Route,
    /// The destination of a route that drops it: `blackhole`, `unreachable`,
    /// `prohibit` or `throw`.
    Dropped,
}

/// A range of addresses the host uses, and the interface that uses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub dev: String,
    pub net: Ipv4Net,
    pub kind: ClaimKind,
}

/// Reads the addresses and routes of this host.
pub trait HostNetworks: Send + Sync {
    fn claims(&self) -> BoxFuture<'_, Result<Vec<Claim>, String>>;
}

/// Whether `subnet` can be a container subnet on this host. `claims` is what
/// the host uses (every routing table), `bridge` the faber bridge and
/// `mesh_iface` the WireGuard interface. The error says why, naming the
/// network and interface: for faber-node's log, not for a response.
///
/// Claims of `bridge` are the faber bridge's own, from the subnet it has now,
/// and are ignored for the overlap rules.
///
/// - An address on any other interface conflicts when its network overlaps
///   `subnet` either way round: containers would get addresses of a network
///   the host sits on.
/// - A route that drops traffic (blackhole, unreachable, prohibit, throw)
///   conflicts when it overlaps `subnet` either way round.
/// - Any other route conflicts when its destination lies inside `subnet` and
///   is longer than it: the bridge route would hide more specific routes that
///   lead somewhere else.
///
/// And the subnet must already be routed where containers' traffic goes:
/// inside a route on `mesh_iface` (fabercp routes the whole container range
/// into the tunnel before it asks for a subnet) or inside a route of the
/// bridge. A subnet that only sits inside a route of some other interface,
/// the `default` route or a broad LAN route included, would pull that
/// traffic into the bridge and is refused.
pub fn check_subnet_free(
    claims: &[Claim],
    subnet: Ipv4Net,
    bridge: &str,
    mesh_iface: &str,
) -> Result<(), String> {
    for claim in claims.iter().filter(|c| c.dev != bridge) {
        let overlaps =
            subnet.contains(&claim.net.network()) || claim.net.contains(&subnet.network());
        let (conflict, what) = match claim.kind {
            ClaimKind::Address => (overlaps, "an address"),
            ClaimKind::Dropped => (overlaps, "a route that drops traffic"),
            ClaimKind::Route => (
                subnet.contains(&claim.net) && claim.net.prefix_len() > subnet.prefix_len(),
                "a more specific route",
            ),
        };
        if conflict {
            return Err(format!(
                "subnet {subnet} overlaps {what} of the host: {} on {}",
                claim.net,
                if claim.dev.is_empty() {
                    "no interface"
                } else {
                    &claim.dev
                }
            ));
        }
    }
    let routed = claims.iter().any(|c| {
        c.kind == ClaimKind::Route
            && (c.dev == mesh_iface || c.dev == bridge)
            && c.net.contains(&subnet)
    });
    if routed {
        Ok(())
    } else {
        Err(format!(
            "subnet {subnet} is inside no route of {mesh_iface} or {bridge}"
        ))
    }
}

/// `ip -j -4 addr show`: `[{"ifname": "eth0", "addr_info": [{"family":
/// "inet", "local": "10.0.0.5", "prefixlen": 24}]}]`.
fn parse_addresses(json: &[u8]) -> Result<Vec<Claim>, String> {
    let root: Value = serde_json::from_slice(json).map_err(|e| format!("ip addr: {e}"))?;
    let mut claims = Vec::new();
    for iface in root.as_array().ok_or("ip addr: not a list")? {
        let dev = iface["ifname"].as_str().unwrap_or_default();
        for info in iface["addr_info"].as_array().into_iter().flatten() {
            if info["family"].as_str() != Some("inet") {
                continue;
            }
            let (Some(local), Some(prefix)) = (info["local"].as_str(), info["prefixlen"].as_u64())
            else {
                continue;
            };
            let net: Ipv4Net = format!("{local}/{prefix}")
                .parse()
                .map_err(|_| "ip addr: an address is not IPv4".to_owned())?;
            claims.push(Claim {
                dev: dev.to_owned(),
                net: net.trunc(),
                kind: ClaimKind::Address,
            });
        }
    }
    Ok(claims)
}

/// `ip -j -4 route show table all`: `[{"dst": "default" | "10.0.0.0/24" |
/// "10.0.0.7", "dev": "eth0", "type": "blackhole"}]`. A destination without a
/// prefix is a host route. The `default` route is left out: it overlaps
/// everything and routes nothing of its own.
fn parse_routes(json: &[u8]) -> Result<Vec<Claim>, String> {
    let root: Value = serde_json::from_slice(json).map_err(|e| format!("ip route: {e}"))?;
    let mut claims = Vec::new();
    for route in root.as_array().ok_or("ip route: not a list")? {
        let Some(dst) = route["dst"].as_str() else {
            continue;
        };
        if dst == "default" {
            continue;
        }
        let net: Ipv4Net = if dst.contains('/') {
            dst.parse()
        } else {
            format!("{dst}/32").parse()
        }
        .map_err(|_| "ip route: a destination is not IPv4".to_owned())?;
        let kind = match route["type"].as_str() {
            Some("blackhole" | "unreachable" | "prohibit" | "throw") => ClaimKind::Dropped,
            _ => ClaimKind::Route,
        };
        claims.push(Claim {
            dev: route["dev"].as_str().unwrap_or_default().to_owned(),
            net: net.trunc(),
            kind,
        });
    }
    Ok(claims)
}

/// [`HostNetworks`] over the `ip` tool.
pub struct IpHostNetworks {
    runner: Arc<dyn CommandRunner>,
    ip: String,
}

impl IpHostNetworks {
    pub fn new(runner: Arc<dyn CommandRunner>, tools: &ToolPaths) -> Self {
        Self {
            runner,
            ip: tools.sbin("ip"),
        }
    }

    async fn run(&self, what: &'static str, args: &[&str]) -> Result<Vec<u8>, String> {
        let out = self
            .runner
            .run(&self.ip, args, None)
            .await
            .map_err(|e| format!("running ip {what}: {e}"))?;
        if out.status_ok {
            Ok(out.stdout)
        } else {
            Err(format!("ip {what} failed"))
        }
    }
}

impl HostNetworks for IpHostNetworks {
    fn claims(&self) -> BoxFuture<'_, Result<Vec<Claim>, String>> {
        Box::pin(async move {
            let addr = self.run("addr", &["-j", "-4", "addr", "show"]).await?;
            let route = self
                .run("route", &["-j", "-4", "route", "show", "table", "all"])
                .await?;
            let mut claims = parse_addresses(&addr)?;
            claims.extend(parse_routes(&route)?);
            Ok(claims)
        })
    }
}

#[cfg(test)]
pub(crate) mod fake {
    use std::sync::Mutex;

    use super::*;

    /// A host with the claims it is given.
    pub struct FakeHostNetworks(Mutex<Result<Vec<Claim>, String>>);

    impl FakeHostNetworks {
        pub fn with(claims: Vec<Claim>) -> Self {
            Self(Mutex::new(Ok(claims)))
        }

        /// From now on the host answers with these claims.
        pub fn set(&self, claims: Vec<Claim>) {
            *self.0.lock().unwrap() = Ok(claims);
        }

        /// From now on reading the host fails with `message`.
        pub fn fail(&self, message: &str) {
            *self.0.lock().unwrap() = Err(message.to_owned());
        }
    }

    impl Default for FakeHostNetworks {
        /// A host that routes the container range into the tunnel, as
        /// fabercp does before it asks for a subnet.
        fn default() -> Self {
            Self::with(vec![mesh_route()])
        }
    }

    impl HostNetworks for FakeHostNetworks {
        fn claims(&self) -> BoxFuture<'_, Result<Vec<Claim>, String>> {
            let answer = self.0.lock().unwrap().clone();
            Box::pin(async move { answer })
        }
    }

    /// The route of the container range into the WireGuard interface.
    pub fn mesh_route() -> Claim {
        route("faber0", "10.200.0.0/16")
    }

    pub fn dropped(net: &str) -> Claim {
        Claim {
            dev: String::new(),
            net: net.parse().unwrap(),
            kind: ClaimKind::Dropped,
        }
    }

    pub fn address(dev: &str, net: &str) -> Claim {
        Claim {
            dev: dev.into(),
            net: net.parse().unwrap(),
            kind: ClaimKind::Address,
        }
    }

    pub fn route(dev: &str, net: &str) -> Claim {
        Claim {
            dev: dev.into(),
            net: net.parse().unwrap(),
            kind: ClaimKind::Route,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use faber_network_management::{CommandOutput, CommandRunner};

    use super::fake::{address, mesh_route, route};
    use super::*;

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    const ADDR: &str = r#"[
      {"ifindex":1,"ifname":"lo","addr_info":[{"family":"inet","local":"127.0.0.1","prefixlen":8}]},
      {"ifindex":2,"ifname":"eth0","addr_info":[
        {"family":"inet","local":"192.168.1.20","prefixlen":24},
        {"family":"inet6","local":"fe80::1","prefixlen":64}]},
      {"ifindex":3,"ifname":"faberbr0","addr_info":[{"family":"inet","local":"10.200.3.1","prefixlen":24}]},
      {"ifindex":4,"ifname":"down0"}
    ]"#;

    const ROUTE: &str = r#"[
      {"dst":"default","gateway":"192.168.1.1","dev":"eth0"},
      {"dst":"10.200.0.0/16","dev":"faber0","scope":"link"},
      {"dst":"10.200.3.0/24","dev":"faberbr0","scope":"link"},
      {"dst":"172.17.0.0/16","dev":"docker0","scope":"link"},
      {"dst":"203.0.113.7","dev":"eth0","scope":"link"},
      {"type":"blackhole","dst":"198.51.100.0/24"},
      {"type":"unreachable","dst":"192.0.2.0/24","table":"100"},
      {"type":"local","dst":"192.168.1.20","dev":"eth0","table":"local"},
      {"dev":"eth0"}
    ]"#;

    #[test]
    fn addresses_are_read_as_networks() {
        let claims = parse_addresses(ADDR.as_bytes()).unwrap();
        let shown: Vec<_> = claims
            .iter()
            .map(|c| (c.dev.as_str(), c.net.to_string()))
            .collect();
        assert_eq!(
            shown,
            [
                ("lo", "127.0.0.0/8".to_owned()),
                ("eth0", "192.168.1.0/24".to_owned()),
                ("faberbr0", "10.200.3.0/24".to_owned()),
            ]
        );
        assert!(claims.iter().all(|c| c.kind == ClaimKind::Address));
    }

    #[test]
    fn routes_skip_the_default_and_read_host_routes() {
        let claims = parse_routes(ROUTE.as_bytes()).unwrap();
        let shown: Vec<_> = claims
            .iter()
            .map(|c| (c.dev.as_str(), c.net.to_string()))
            .collect();
        assert_eq!(
            shown,
            [
                ("faber0", "10.200.0.0/16".to_owned()),
                ("faberbr0", "10.200.3.0/24".to_owned()),
                ("docker0", "172.17.0.0/16".to_owned()),
                ("eth0", "203.0.113.7/32".to_owned()),
                ("", "198.51.100.0/24".to_owned()),
                ("", "192.0.2.0/24".to_owned()),
                ("eth0", "192.168.1.20/32".to_owned()),
            ]
        );
        let kinds: Vec<_> = claims.iter().map(|c| c.kind).collect();
        assert_eq!(kinds[4], ClaimKind::Dropped);
        assert_eq!(kinds[5], ClaimKind::Dropped);
        assert_eq!(kinds[6], ClaimKind::Route);
        assert_eq!(kinds[..4], [ClaimKind::Route; 4]);
    }

    #[test]
    fn unreadable_output_is_an_error() {
        assert!(parse_addresses(b"nope").is_err());
        assert!(parse_addresses(br#"{"a":1}"#).is_err());
        assert!(parse_routes(b"").is_err());
        assert!(parse_routes(br#"[{"dst":"fe80::/64","dev":"x"}]"#).is_err());
    }

    const BR: &str = "faberbr0";
    const MESH: &str = "faber0";

    fn free(claims: &[Claim], subnet: &str) -> Result<(), String> {
        check_subnet_free(claims, net(subnet), BR, MESH)
    }

    #[test]
    fn a_subnet_on_a_host_address_is_refused_either_way_round() {
        let claims = [
            mesh_route(),
            address("eth0", "10.200.9.0/24"),
            address("lo", "127.0.0.0/8"),
        ];
        for bad in ["10.200.9.0/24", "10.200.9.128/25", "10.200.0.0/16"] {
            assert!(free(&claims, bad).is_err(), "{bad}");
        }
        let err = free(&claims, "10.200.9.0/28").unwrap_err();
        assert!(
            err.contains("10.200.9.0/24") && err.contains("eth0"),
            "{err}"
        );
        assert!(free(&claims, "10.200.3.0/24").is_ok());
    }

    #[test]
    fn a_route_more_specific_than_the_subnet_is_refused() {
        let claims = [
            mesh_route(),
            route("eth0", "10.200.3.128/25"),
            route("eth0", "10.200.4.7/32"),
        ];
        assert!(free(&claims, "10.200.3.0/24").is_err());
        assert!(free(&claims, "10.200.4.0/24").is_err());
        assert!(free(&claims, "10.200.5.0/24").is_ok());
    }

    #[test]
    fn a_route_that_drops_traffic_is_refused_either_way_round() {
        for dropped in [
            "10.200.3.0/25",
            "10.200.3.0/24",
            "10.200.0.0/17",
            "10.0.0.0/8",
        ] {
            let claims = [mesh_route(), super::fake::dropped(dropped)];
            assert!(free(&claims, "10.200.3.0/24").is_err(), "{dropped}");
        }
        let claims = [mesh_route(), super::fake::dropped("192.168.0.0/16")];
        assert!(free(&claims, "10.200.3.0/24").is_ok());
    }

    #[test]
    fn the_subnet_must_sit_inside_a_route_of_the_mesh_interface() {
        // The route of the whole container range, or of exactly the subnet.
        assert!(free(&[mesh_route()], "10.200.3.0/24").is_ok());
        assert!(free(&[route(MESH, "10.200.3.0/24")], "10.200.3.0/24").is_ok());
        // No mesh route at all.
        let err = free(&[], "10.200.3.0/24").unwrap_err();
        assert!(err.contains("inside no route"), "{err}");
        // A route of the range on some other interface is not it, however
        // broad: such a subnet would pull the LAN's traffic into the bridge.
        for other in ["10.0.0.0/8", "10.200.0.0/16", "8.8.8.0/24", "0.0.0.0/0"] {
            let claims = [route("eth0", other)];
            assert!(free(&claims, "10.200.3.0/24").is_err(), "{other}");
        }
        assert!(free(&[mesh_route()], "8.8.8.0/24").is_err());
        assert!(free(&[mesh_route()], "10.5.5.0/24").is_err());
        // The mesh interface's route of another range does not do either.
        assert!(free(&[route(MESH, "10.201.0.0/16")], "10.200.3.0/24").is_err());
    }

    #[test]
    fn the_bridges_own_route_also_routes_the_subnet() {
        let claims = [
            address(BR, "10.200.3.0/24"),
            route(BR, "10.200.3.0/24"),
            route(BR, "10.200.3.9/32"),
        ];
        assert!(free(&claims, "10.200.3.0/24").is_ok());
        // The same claims on another interface conflict.
        let claims = [mesh_route(), address("eth9", "10.200.3.0/24")];
        assert!(free(&claims, "10.200.3.0/24").is_err());
    }

    #[test]
    fn the_default_route_is_not_a_route_of_the_subnet() {
        let claims = parse_routes(ROUTE.as_bytes()).unwrap();
        assert!(free(&claims, "10.200.3.0/24").is_ok());
        assert!(free(&claims, "203.0.113.0/24").is_err());
    }

    /// Answers `ip -j -4 addr|route show` with canned JSON and records calls.
    struct CannedIp {
        calls: Mutex<Vec<String>>,
        route_fails: bool,
    }

    impl CommandRunner for CannedIp {
        fn run<'a>(
            &'a self,
            prog: &'a str,
            args: &'a [&'a str],
            _stdin: Option<Vec<u8>>,
        ) -> BoxFuture<'a, std::io::Result<CommandOutput>> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("{prog} {}", args.join(" ")));
            let out = if args[2] == "addr" {
                CommandOutput::success_with(ADDR)
            } else if self.route_fails {
                CommandOutput {
                    status_ok: false,
                    code: Some(1),
                    stdout: Vec::new(),
                    stderr: b"boom".to_vec(),
                }
            } else {
                CommandOutput::success_with(ROUTE)
            };
            Box::pin(async move { Ok(out) })
        }
    }

    #[tokio::test]
    async fn the_ip_tool_is_run_by_absolute_path() {
        let runner = Arc::new(CannedIp {
            calls: Mutex::default(),
            route_fails: false,
        });
        let networks = IpHostNetworks::new(runner.clone(), &ToolPaths::default());
        let claims = networks.claims().await.unwrap();
        assert_eq!(claims.len(), 3 + 7);
        assert_eq!(
            *runner.calls.lock().unwrap(),
            [
                "/usr/sbin/ip -j -4 addr show",
                "/usr/sbin/ip -j -4 route show table all"
            ]
        );
    }

    #[tokio::test]
    async fn a_failing_ip_is_an_error() {
        let runner = Arc::new(CannedIp {
            calls: Mutex::default(),
            route_fails: true,
        });
        let networks = IpHostNetworks::new(runner, &ToolPaths::default());
        assert_eq!(networks.claims().await.unwrap_err(), "ip route failed");
    }
}
