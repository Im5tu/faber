//! What the host already uses of the IPv4 address space, so a container
//! subnet never lands on top of it.
//!
//! Read with `ip -j -4 addr show` and `ip -j -4 route show`, run by absolute
//! path like every other host tool of faber-node. The commands sit behind
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
    /// The destination of a route.
    Route,
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

/// Whether `subnet` can be a container subnet next to what the host uses.
/// Claims of `bridge` are the faber bridge's own, from the subnet it has now,
/// and are ignored.
///
/// - An address on any other interface conflicts when its network overlaps
///   `subnet` either way round: containers would get addresses of a network
///   the host sits on.
/// - A route conflicts when its destination lies inside `subnet` and is
///   longer than it: the bridge route would hide more specific routes that
///   lead somewhere else. A route that equals or holds `subnet` does not
///   conflict. The route of the whole container range into the WireGuard
///   interface is exactly that, and the bridge's own, more specific, route
///   wins over it.
pub fn check_subnet_free(claims: &[Claim], subnet: Ipv4Net, bridge: &str) -> Result<(), String> {
    for claim in claims.iter().filter(|c| c.dev != bridge) {
        let conflict = match claim.kind {
            ClaimKind::Address => {
                subnet.contains(&claim.net.network()) || claim.net.contains(&subnet.network())
            }
            ClaimKind::Route => {
                subnet.contains(&claim.net) && claim.net.prefix_len() > subnet.prefix_len()
            }
        };
        if conflict {
            let what = match claim.kind {
                ClaimKind::Address => "an address",
                ClaimKind::Route => "a route",
            };
            return Err(format!(
                "subnet {subnet} overlaps {what} of the host: {} on {}",
                claim.net, claim.dev
            ));
        }
    }
    Ok(())
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

/// `ip -j -4 route show`: `[{"dst": "default" | "10.0.0.0/24" | "10.0.0.7",
/// "dev": "eth0"}]`. A destination without a prefix is a host route.
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
        claims.push(Claim {
            dev: route["dev"].as_str().unwrap_or_default().to_owned(),
            net: net.trunc(),
            kind: ClaimKind::Route,
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

    async fn run(&self, what: &'static str) -> Result<Vec<u8>, String> {
        let out = self
            .runner
            .run(&self.ip, &["-j", "-4", what, "show"], None)
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
            let mut claims = parse_addresses(&self.run("addr").await?)?;
            claims.extend(parse_routes(&self.run("route").await?)?);
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
        fn default() -> Self {
            Self::with(Vec::new())
        }
    }

    impl HostNetworks for FakeHostNetworks {
        fn claims(&self) -> BoxFuture<'_, Result<Vec<Claim>, String>> {
            let answer = self.0.lock().unwrap().clone();
            Box::pin(async move { answer })
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

    use super::fake::{address, route};
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
            ]
        );
    }

    #[test]
    fn unreadable_output_is_an_error() {
        assert!(parse_addresses(b"nope").is_err());
        assert!(parse_addresses(br#"{"a":1}"#).is_err());
        assert!(parse_routes(b"").is_err());
        assert!(parse_routes(br#"[{"dst":"fe80::/64","dev":"x"}]"#).is_err());
    }

    #[test]
    fn a_subnet_on_a_host_address_is_refused_either_way_round() {
        let claims = [
            address("eth0", "192.168.1.0/24"),
            address("lo", "127.0.0.0/8"),
        ];
        for bad in [
            "192.168.1.0/24",
            "192.168.1.128/25",
            "192.168.0.0/16",
            "10.0.0.0/8",
        ] {
            let r = check_subnet_free(&claims, net(bad), "faberbr0");
            assert_eq!(r.is_err(), bad != "10.0.0.0/8", "{bad}: {r:?}");
        }
        let err = check_subnet_free(&claims, net("192.168.1.0/28"), "faberbr0").unwrap_err();
        assert!(
            err.contains("192.168.1.0/24") && err.contains("eth0"),
            "{err}"
        );
    }

    #[test]
    fn a_route_more_specific_than_the_subnet_is_refused() {
        let claims = [
            route("eth0", "10.200.3.128/25"),
            route("eth0", "203.0.113.7/32"),
        ];
        assert!(check_subnet_free(&claims, net("10.200.3.0/24"), "faberbr0").is_err());
        assert!(check_subnet_free(&claims, net("203.0.113.0/24"), "faberbr0").is_err());
        assert!(check_subnet_free(&claims, net("10.200.4.0/24"), "faberbr0").is_ok());
    }

    #[test]
    fn a_route_that_equals_or_holds_the_subnet_is_fine() {
        // The route of the whole container range into the tunnel.
        let claims = [
            route("faber0", "10.200.0.0/16"),
            route("eth0", "10.200.3.0/24"),
        ];
        assert!(check_subnet_free(&claims, net("10.200.3.0/24"), "faberbr0").is_ok());
    }

    #[test]
    fn the_bridges_own_claims_are_ignored() {
        let claims = [
            address("faberbr0", "10.200.3.0/24"),
            route("faberbr0", "10.200.3.0/24"),
            route("faberbr0", "10.200.3.9/32"),
        ];
        assert!(check_subnet_free(&claims, net("10.200.3.0/24"), "faberbr0").is_ok());
        // The same claims on another interface conflict.
        let claims = [address("eth9", "10.200.3.0/24")];
        assert!(check_subnet_free(&claims, net("10.200.3.0/24"), "faberbr0").is_err());
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
        assert_eq!(claims.len(), 3 + 4);
        assert_eq!(
            *runner.calls.lock().unwrap(),
            [
                "/usr/sbin/ip -j -4 addr show",
                "/usr/sbin/ip -j -4 route show"
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
