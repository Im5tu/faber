//! The `inet faber` nftables table.
//!
//! fabercp owns this table only; the rest of the host firewall is the admin's.
//! It is loaded with `nft -f -` (the rendered text on stdin) and rebuilt from
//! raft state whenever it changes. The rules:
//! - UDP `wg_port` arriving on any interface other than `iface` is accepted
//!   only from `@peer_endpoints` (current nodes plus live invites); the rest
//!   is dropped.
//! - On `iifname <iface>`: established/related accepted, new traffic accepted
//!   only from `@peer_mesh_ips`, the rest dropped.
//! - `iifname != { lo, <iface> } tcp dport <peer_port> drop`, loaded before
//!   the peer listener binds.
//! - A forward chain drops everything routed into or out of `iface`, so a
//!   host with `ip_forward` on does not become a router for the mesh.
//!
//! With [`FirewallSpec::containers`] set, the forward chain lets container
//! traffic through the tunnel and nothing else. `<bridge>` is the container
//! bridge, `<cidr>` the cluster-wide container range, `<local>` this node's
//! subnet of it:
//! 1. `iifname <bridge> oifname != { <iface>, <bridge> } drop` and
//!    `oifname <bridge> iifname != { <iface>, <bridge> } drop`: the bridge
//!    exchanges packets with itself and the tunnel only. These rules match
//!    no address, so they cover IPv6 and a container that forges its source,
//!    and nothing reaches a container from the host's other interfaces.
//!    Interfaces are matched by name, so the rules also hold once `iface` is
//!    deleted: container traffic then follows the default route and is
//!    dropped, not sent in clear text. Replies are dropped too; no
//!    connection these rules cut off was allowed to start.
//! 2. `iifname <iface> oifname <iface> drop`: nothing is relayed from one
//!    peer to another. The route for `<cidr>` points at `iface`, so without
//!    this a packet for the local subnet would go back out of the tunnel
//!    while the bridge does not exist yet.
//! 3. Established/related accepted in both directions of `iface`.
//! 4. `ip saddr <local> ip daddr != <cidr> drop`: a container opens no
//!    connection to anything outside the container range, on any interface.
//!    Container egress is off by default and there is no NAT. This comes
//!    after rule 3, or it would drop a container's replies to a node. For
//!    packets from the bridge, rules 1 and 7 already do this; the rule stays
//!    for a container attached to some other interface.
//! 5. New traffic from `iface` to `<bridge>` for `<local>`, from
//!    `@node_mesh_ips` (registered nodes only, so the holder of an unused
//!    invite gets no way in) and from `<cidr>`. These are two rules because
//!    a named set cannot be an element of an anonymous one. The local
//!    subnet is only behind the bridge; naming it as the output keeps a
//!    peer from reaching another interface through a more specific route.
//! 6. New traffic from `<bridge>` to `iface`, from `<local>` to `<cidr>`.
//!    The bridge is named as the input because a remote peer's AllowedIPs
//!    trust any packet with a `<local>` source that comes out of this
//!    node's tunnel. A host on another interface that forges such a source
//!    must not get in.
//! 7. Everything else into or out of `iface` is dropped, as before.
//!
//! Until the node has a subnet, the rules that name `<local>` (4, 5 and 6)
//! are left out. Traffic between two containers on the bridge has the bridge
//! as both interfaces (it only passes this chain with `br_netfilter` loaded)
//! and matches no rule. The input chain is the same with and without
//! containers, so a container reaches no other node's mesh IP.
//!
//! With containers there are also three nat chains, `prerouting`, `output` and
//! `postrouting`, in the same table:
//! - Packets from `<bridge>` addressed to the bridge gateway (the first host
//!   address of `<local>`, where the CNI plugin puts it) on port 53, udp and
//!   tcp, are redirected to [`DNS_REDIRECT_PORT`], so the DNS server needs no
//!   privileged port.
//! - Each [`ServiceNat`] adds a `dnat` rule per `(protocol, port)` to
//!   `prerouting` (only for packets from `<bridge>`, so the node never
//!   relays virtual IP traffic for a peer) and to `output` (the host's own
//!   packets). The backend is picked per new connection with
//!   `numgen random mod N` over the backends that serve that port. A port
//!   with no backend gets no rule (`mod 0` is invalid). The port number is
//!   not changed.
//! - A pod that reaches its own service and is sent back to itself would see
//!   its own address as the source. `postrouting` masquerades exactly that
//!   case, for every backend that is on `<local>`.
//!
//! No default-drop for anything else. A later replicated `config/firewall`
//! key feeds the same [`render`].
//!
//! nftables semantics to keep in mind: an `accept` here only ends evaluation
//! of *this* base chain. Other tables' chains on the same hook still run and
//! can drop the packet, so our accepts never open a port the admin's firewall
//! closes. A `drop` is final everywhere, so our drops always hold.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

use crate::{CommandRunner, NetworkError, check_iface_name, run_checked};

/// nft address family of the table.
pub const TABLE_FAMILY: &str = "inet";
/// nft table name.
pub const TABLE_NAME: &str = "faber";
/// Set of peer WireGuard endpoint IPs (`type ipv4_addr`; IPv6 endpoints go in
/// a sibling `peer_endpoints6` set of `type ipv6_addr`).
pub const SET_PEER_ENDPOINTS: &str = "peer_endpoints";
/// IPv6 companion of [`SET_PEER_ENDPOINTS`].
pub const SET_PEER_ENDPOINTS6: &str = "peer_endpoints6";
/// Set of peer mesh IPs (`type ipv4_addr`).
pub const SET_PEER_MESH_IPS: &str = "peer_mesh_ips";
/// Port the DNS server listens on at the bridge gateway. Port 53 on the
/// gateway is redirected to it. Not 5353, which mDNS daemons bind, and not
/// the default peer port.
pub const DNS_REDIRECT_PORT: u16 = 5300;
/// Set of registered nodes' mesh IPs (`type ipv4_addr`). Only in the table
/// when [`FirewallSpec::containers`] is set.
pub const SET_NODE_MESH_IPS: &str = "node_mesh_ips";

/// Everything the table is rendered from, apart from the WireGuard interface
/// name (`NetworkSpec::interface`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirewallSpec {
    pub wg_port: u16,
    /// TCP port of the peer listener.
    pub peer_port: u16,
    /// IPs (no ports) of every node endpoint and live invite endpoint,
    /// excluding this node.
    pub peer_endpoint_ips: BTreeSet<IpAddr>,
    /// Mesh IPs of every other node and live invite.
    pub peer_mesh_ips: BTreeSet<Ipv4Addr>,
    /// Container networking. `None` renders the table without the
    /// `node_mesh_ips` set and with a forward chain that only drops.
    #[serde(default)]
    pub containers: Option<ContainerFirewall>,
}

/// What the container rules of the table are rendered from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerFirewall {
    /// Cluster-wide container range.
    pub cidr: Ipv4Net,
    /// This node's container subnet, once raft has allocated one.
    pub local_subnet: Option<Ipv4Net>,
    /// Mesh IPs of registered nodes other than this one. Excludes invites.
    pub node_mesh_ips: BTreeSet<Ipv4Addr>,
    /// Name of the bridge the containers are attached to, e.g. `faberbr0`.
    /// Written into the script verbatim; [`apply`] rejects a name that
    /// fails [`crate::is_valid_iface_name`] or equals the mesh interface.
    pub bridge: String,
}

/// Transport protocol of a service port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NatProtocol {
    Tcp,
    Udp,
}

impl NatProtocol {
    fn keyword(self) -> &'static str {
        match self {
            NatProtocol::Tcp => "tcp",
            NatProtocol::Udp => "udp",
        }
    }
}

/// One container that serves a port of a service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NatBackend {
    /// The pod address.
    pub ip: Ipv4Addr,
    /// The port, which is also the port of the virtual IP.
    pub port: u16,
    pub protocol: NatProtocol,
}

/// A service's virtual IP and the containers that serve it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceNat {
    pub vip: Ipv4Addr,
    /// May be empty: then the service has no rule.
    pub backends: Vec<NatBackend>,
}

/// The full `nft -f -` script: deletes and recreates `table inet faber`
/// atomically (`table inet faber {}` + `delete table inet faber` + the
/// definition), so reloading is idempotent. Deterministic for equal input.
///
/// `iface` (the WireGuard interface) and the container bridge name are
/// written into the script verbatim; [`apply`] rejects names that fail
/// [`crate::is_valid_iface_name`].
///
/// `services` add the nat rules to the nat chains, which exist only when
/// [`FirewallSpec::containers`] is set.
pub fn render_with_services(iface: &str, spec: &FirewallSpec, services: &[ServiceNat]) -> String {
    let v4: Vec<String> = spec
        .peer_endpoint_ips
        .iter()
        .filter(|ip| ip.is_ipv4())
        .map(IpAddr::to_string)
        .collect();
    let v6: Vec<String> = spec
        .peer_endpoint_ips
        .iter()
        .filter(|ip| ip.is_ipv6())
        .map(IpAddr::to_string)
        .collect();
    let mesh: Vec<String> = spec.peer_mesh_ips.iter().map(Ipv4Addr::to_string).collect();

    let table = format!("table {TABLE_FAMILY} {TABLE_NAME}");
    let mut out = String::new();
    // Declaring the table first makes the delete succeed on the first load;
    // nft applies the whole script as one transaction.
    let _ = writeln!(out, "{table} {{}}");
    let _ = writeln!(out, "delete {table}");
    let _ = writeln!(out, "{table} {{");
    write_set(&mut out, SET_PEER_ENDPOINTS, "ipv4_addr", &v4);
    write_set(&mut out, SET_PEER_ENDPOINTS6, "ipv6_addr", &v6);
    write_set(&mut out, SET_PEER_MESH_IPS, "ipv4_addr", &mesh);
    if let Some(containers) = &spec.containers {
        let nodes: Vec<String> = containers
            .node_mesh_ips
            .iter()
            .map(Ipv4Addr::to_string)
            .collect();
        write_set(&mut out, SET_NODE_MESH_IPS, "ipv4_addr", &nodes);
    }

    let wg = spec.wg_port;
    let rules = [
        // (a) WireGuard handshakes from outside only from known endpoints.
        format!("iifname != \"{iface}\" udp dport {wg} ip saddr @{SET_PEER_ENDPOINTS} accept"),
        format!("iifname != \"{iface}\" udp dport {wg} ip6 saddr @{SET_PEER_ENDPOINTS6} accept"),
        format!("iifname != \"{iface}\" udp dport {wg} drop"),
        // (b) Inside the tunnel: replies, then new traffic from known peers.
        format!("iifname \"{iface}\" ct state established,related accept"),
        format!("iifname \"{iface}\" ip saddr @{SET_PEER_MESH_IPS} accept"),
        format!("iifname \"{iface}\" drop"),
        // (c) The peer listener is reachable only over the mesh (and lo).
        format!(
            "iifname != {{ \"lo\", \"{iface}\" }} tcp dport {} drop",
            spec.peer_port
        ),
    ];
    out.push_str("\tchain input {\n");
    out.push_str("\t\ttype filter hook input priority filter - 10; policy accept;\n");
    for rule in rules {
        let _ = writeln!(out, "\t\t{rule}");
    }
    out.push_str("\t}\n");
    out.push_str("\tchain forward {\n");
    out.push_str("\t\ttype filter hook forward priority filter - 10; policy accept;\n");
    for rule in forward_rules(iface, spec.containers.as_ref()) {
        let _ = writeln!(out, "\t\t{rule}");
    }
    out.push_str("\t}\n");
    if let Some(containers) = &spec.containers {
        for (header, rules) in nat_chains(containers, services) {
            let _ = writeln!(out, "\tchain {header}");
            for rule in rules {
                let _ = writeln!(out, "\t\t{rule}");
            }
            out.push_str("\t}\n");
        }
    }
    out.push_str("}\n");
    out
}

/// The nat chains as `(name and hook lines, rules)`. See the module docs.
fn nat_chains(
    containers: &ContainerFirewall,
    services: &[ServiceNat],
) -> [(String, Vec<String>); 3] {
    let bridge = &containers.bridge;
    // One group per virtual IP and port, the backends sorted and unique.
    let mut groups: BTreeMap<(Ipv4Addr, NatProtocol, u16), BTreeSet<Ipv4Addr>> = BTreeMap::new();
    for service in services {
        for b in &service.backends {
            groups
                .entry((service.vip, b.protocol, b.port))
                .or_default()
                .insert(b.ip);
        }
    }
    let dnat: Vec<String> = groups
        .iter()
        .map(|((vip, protocol, port), ips)| {
            let map = ips
                .iter()
                .enumerate()
                .map(|(i, ip)| format!("{i} : {ip}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "ip daddr {vip} {} dport {port} dnat ip to numgen random mod {} map {{ {map} }}",
                protocol.keyword(),
                ips.len()
            )
        })
        .collect();

    let mut pre = Vec::new();
    let mut post = Vec::new();
    if let Some(local) = containers.local_subnet {
        if let Some(gateway) = local.hosts().next() {
            for protocol in ["udp", "tcp"] {
                pre.push(format!(
                    "iifname \"{bridge}\" ip daddr {gateway} {protocol} dport 53 redirect to :{DNS_REDIRECT_PORT}"
                ));
            }
        }
        // A rule per backend on this node: nft cannot compare the source
        // with the destination.
        let local_backends: BTreeSet<Ipv4Addr> = groups
            .values()
            .flatten()
            .copied()
            .filter(|ip| local.contains(ip))
            .collect();
        for ip in local_backends {
            post.push(format!(
                "oifname \"{bridge}\" ip saddr {ip} ip daddr {ip} ct status dnat masquerade"
            ));
        }
    }
    pre.extend(
        dnat.iter()
            .map(|rule| format!("iifname \"{bridge}\" {rule}")),
    );
    [
        (
            "prerouting {\n\t\ttype nat hook prerouting priority dstnat; policy accept;".into(),
            pre,
        ),
        (
            "output {\n\t\ttype nat hook output priority dstnat; policy accept;".into(),
            dnat,
        ),
        (
            "postrouting {\n\t\ttype nat hook postrouting priority srcnat; policy accept;".into(),
            post,
        ),
    ]
}

/// The forward chain's rules, in order. See the module docs for what each
/// container rule is for.
fn forward_rules(iface: &str, containers: Option<&ContainerFirewall>) -> Vec<String> {
    // (d) Nothing is routed into or out of the tunnel, whatever the host's
    // ip_forward setting, unless a container rule accepted it first.
    let drop_rest = [
        format!("oifname \"{iface}\" drop"),
        format!("iifname \"{iface}\" drop"),
    ];
    let Some(containers) = containers else {
        return drop_rest.into();
    };
    let cidr = containers.cidr;
    let bridge = &containers.bridge;
    let mut rules = vec![
        // The bridge exchanges packets with itself and the tunnel only.
        // These match no address, so they hold for IPv6, for a forged
        // source, and while the tunnel interface does not exist.
        format!("iifname \"{bridge}\" oifname != {{ \"{iface}\", \"{bridge}\" }} drop"),
        format!("oifname \"{bridge}\" iifname != {{ \"{iface}\", \"{bridge}\" }} drop"),
        format!("iifname \"{iface}\" oifname \"{iface}\" drop"),
        format!("iifname \"{iface}\" ct state established,related accept"),
        format!("oifname \"{iface}\" ct state established,related accept"),
    ];
    if let Some(local) = containers.local_subnet {
        rules.extend([
            // Egress default-deny. After the established accepts, so that a
            // container can still answer a node that connected to it.
            format!("ip saddr {local} ip daddr != {cidr} drop"),
            // Both interfaces are named in every accept: the local subnet is
            // behind the bridge, and an address alone can be forged or
            // routed elsewhere.
            format!(
                "iifname \"{iface}\" oifname \"{bridge}\" ip saddr @{SET_NODE_MESH_IPS} ip daddr {local} accept"
            ),
            format!(
                "iifname \"{iface}\" oifname \"{bridge}\" ip saddr {cidr} ip daddr {local} accept"
            ),
            format!(
                "iifname \"{bridge}\" oifname \"{iface}\" ip saddr {local} ip daddr {cidr} accept"
            ),
        ]);
    }
    rules.extend(drop_rest);
    rules
}

fn write_set(out: &mut String, name: &str, ty: &str, elements: &[String]) {
    let _ = writeln!(out, "\tset {name} {{");
    let _ = writeln!(out, "\t\ttype {ty}");
    if !elements.is_empty() {
        let _ = writeln!(out, "\t\telements = {{ {} }}", elements.join(", "));
    }
    out.push_str("\t}\n");
}

/// Loads [`render_with_services`] with `nft -f -`, the script on stdin. Runs nothing when
/// `iface` or the container bridge name is not a valid interface name, or
/// when the two are the same.
pub async fn apply(
    runner: &dyn CommandRunner,
    iface: &str,
    spec: &FirewallSpec,
    services: &[ServiceNat],
) -> Result<(), NetworkError> {
    check_iface_name(iface)?;
    if let Some(containers) = &spec.containers {
        check_iface_name(&containers.bridge)?;
        // The same name twice would make the bridge rules contradict the
        // tunnel rules, and nft rejects the duplicate set element.
        if containers.bridge == iface {
            return Err(NetworkError::BridgeIsMeshInterface(iface.to_string()));
        }
    }
    run_checked(
        runner,
        "nft",
        &["-f", "-"],
        Some(render_with_services(iface, spec, services).into_bytes()),
    )
    .await?;
    Ok(())
}

/// The IP of a WireGuard endpoint string: `a.b.c.d:port` or `[v6]:port`.
/// IPv4-mapped IPv6 addresses become IPv4. Hostnames and anything else that
/// does not parse give `None`.
pub fn endpoint_ip(endpoint: &str) -> Option<IpAddr> {
    endpoint
        .parse::<SocketAddr>()
        .ok()
        .map(|sa| sa.ip().to_canonical())
}

/// [`render_with_services`] without services.
#[cfg(test)]
pub(crate) fn render(iface: &str, spec: &FirewallSpec) -> String {
    render_with_services(iface, spec, &[])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CommandOutput;
    use crate::fake::RecordingRunner;

    fn spec() -> FirewallSpec {
        FirewallSpec {
            wg_port: 51820,
            peer_port: 7401,
            peer_endpoint_ips: [
                "203.0.113.1".parse().unwrap(),
                "198.51.100.7".parse().unwrap(),
                "2001:db8::1".parse().unwrap(),
            ]
            .into_iter()
            .collect(),
            peer_mesh_ips: ["10.99.0.3".parse().unwrap(), "10.99.0.1".parse().unwrap()]
                .into_iter()
                .collect(),
            containers: None,
        }
    }

    #[test]
    fn render_golden() {
        let expected = r#"table inet faber {}
delete table inet faber
table inet faber {
	set peer_endpoints {
		type ipv4_addr
		elements = { 198.51.100.7, 203.0.113.1 }
	}
	set peer_endpoints6 {
		type ipv6_addr
		elements = { 2001:db8::1 }
	}
	set peer_mesh_ips {
		type ipv4_addr
		elements = { 10.99.0.1, 10.99.0.3 }
	}
	chain input {
		type filter hook input priority filter - 10; policy accept;
		iifname != "faber0" udp dport 51820 ip saddr @peer_endpoints accept
		iifname != "faber0" udp dport 51820 ip6 saddr @peer_endpoints6 accept
		iifname != "faber0" udp dport 51820 drop
		iifname "faber0" ct state established,related accept
		iifname "faber0" ip saddr @peer_mesh_ips accept
		iifname "faber0" drop
		iifname != { "lo", "faber0" } tcp dport 7401 drop
	}
	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		oifname "faber0" drop
		iifname "faber0" drop
	}
}
"#;
        assert_eq!(render("faber0", &spec()), expected);
    }

    #[test]
    fn render_empty_sets_omit_elements() {
        let s = FirewallSpec {
            wg_port: 1000,
            peer_port: 2000,
            peer_endpoint_ips: BTreeSet::new(),
            peer_mesh_ips: BTreeSet::new(),
            containers: None,
        };
        let expected = r#"table inet faber {}
delete table inet faber
table inet faber {
	set peer_endpoints {
		type ipv4_addr
	}
	set peer_endpoints6 {
		type ipv6_addr
	}
	set peer_mesh_ips {
		type ipv4_addr
	}
	chain input {
		type filter hook input priority filter - 10; policy accept;
		iifname != "wg9" udp dport 1000 ip saddr @peer_endpoints accept
		iifname != "wg9" udp dport 1000 ip6 saddr @peer_endpoints6 accept
		iifname != "wg9" udp dport 1000 drop
		iifname "wg9" ct state established,related accept
		iifname "wg9" ip saddr @peer_mesh_ips accept
		iifname "wg9" drop
		iifname != { "lo", "wg9" } tcp dport 2000 drop
	}
	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		oifname "wg9" drop
		iifname "wg9" drop
	}
}
"#;
        assert_eq!(render("wg9", &s), expected);
    }

    fn container_spec(local_subnet: Option<&str>, node_mesh_ips: &[&str]) -> FirewallSpec {
        FirewallSpec {
            containers: Some(ContainerFirewall {
                cidr: "10.200.0.0/16".parse().unwrap(),
                local_subnet: local_subnet.map(|s| s.parse().unwrap()),
                node_mesh_ips: node_mesh_ips.iter().map(|ip| ip.parse().unwrap()).collect(),
                bridge: "faberbr0".into(),
            }),
            ..spec()
        }
    }

    /// The part of every container render above the forward chain. The input
    /// chain is the one in `render_golden`.
    fn container_head(node_mesh_ips_set: &str) -> String {
        format!(
            r#"table inet faber {{}}
delete table inet faber
table inet faber {{
	set peer_endpoints {{
		type ipv4_addr
		elements = {{ 198.51.100.7, 203.0.113.1 }}
	}}
	set peer_endpoints6 {{
		type ipv6_addr
		elements = {{ 2001:db8::1 }}
	}}
	set peer_mesh_ips {{
		type ipv4_addr
		elements = {{ 10.99.0.1, 10.99.0.3 }}
	}}
{node_mesh_ips_set}	chain input {{
		type filter hook input priority filter - 10; policy accept;
		iifname != "faber0" udp dport 51820 ip saddr @peer_endpoints accept
		iifname != "faber0" udp dport 51820 ip6 saddr @peer_endpoints6 accept
		iifname != "faber0" udp dport 51820 drop
		iifname "faber0" ct state established,related accept
		iifname "faber0" ip saddr @peer_mesh_ips accept
		iifname "faber0" drop
		iifname != {{ "lo", "faber0" }} tcp dport 7401 drop
	}}
"#
        )
    }

    #[test]
    fn render_containers_without_local_subnet_golden() {
        let head = container_head(
            "\tset node_mesh_ips {\n\t\ttype ipv4_addr\n\t\telements = { 10.99.0.1 }\n\t}\n",
        );
        let forward = r#"	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		iifname "faberbr0" oifname != { "faber0", "faberbr0" } drop
		oifname "faberbr0" iifname != { "faber0", "faberbr0" } drop
		iifname "faber0" oifname "faber0" drop
		iifname "faber0" ct state established,related accept
		oifname "faber0" ct state established,related accept
		oifname "faber0" drop
		iifname "faber0" drop
	}
	chain prerouting {
		type nat hook prerouting priority dstnat; policy accept;
	}
	chain output {
		type nat hook output priority dstnat; policy accept;
	}
	chain postrouting {
		type nat hook postrouting priority srcnat; policy accept;
	}
}
"#;
        assert_eq!(
            render("faber0", &container_spec(None, &["10.99.0.1"])),
            format!("{head}{forward}")
        );
    }

    #[test]
    fn render_containers_with_local_subnet_golden() {
        let expected = r#"table inet faber {}
delete table inet faber
table inet faber {
	set peer_endpoints {
		type ipv4_addr
		elements = { 198.51.100.7, 203.0.113.1 }
	}
	set peer_endpoints6 {
		type ipv6_addr
		elements = { 2001:db8::1 }
	}
	set peer_mesh_ips {
		type ipv4_addr
		elements = { 10.99.0.1, 10.99.0.3 }
	}
	set node_mesh_ips {
		type ipv4_addr
		elements = { 10.99.0.1, 10.99.0.4 }
	}
	chain input {
		type filter hook input priority filter - 10; policy accept;
		iifname != "faber0" udp dport 51820 ip saddr @peer_endpoints accept
		iifname != "faber0" udp dport 51820 ip6 saddr @peer_endpoints6 accept
		iifname != "faber0" udp dport 51820 drop
		iifname "faber0" ct state established,related accept
		iifname "faber0" ip saddr @peer_mesh_ips accept
		iifname "faber0" drop
		iifname != { "lo", "faber0" } tcp dport 7401 drop
	}
	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		iifname "faberbr0" oifname != { "faber0", "faberbr0" } drop
		oifname "faberbr0" iifname != { "faber0", "faberbr0" } drop
		iifname "faber0" oifname "faber0" drop
		iifname "faber0" ct state established,related accept
		oifname "faber0" ct state established,related accept
		ip saddr 10.200.1.0/24 ip daddr != 10.200.0.0/16 drop
		iifname "faber0" oifname "faberbr0" ip saddr @node_mesh_ips ip daddr 10.200.1.0/24 accept
		iifname "faber0" oifname "faberbr0" ip saddr 10.200.0.0/16 ip daddr 10.200.1.0/24 accept
		iifname "faberbr0" oifname "faber0" ip saddr 10.200.1.0/24 ip daddr 10.200.0.0/16 accept
		oifname "faber0" drop
		iifname "faber0" drop
	}
	chain prerouting {
		type nat hook prerouting priority dstnat; policy accept;
		iifname "faberbr0" ip daddr 10.200.1.1 udp dport 53 redirect to :5300
		iifname "faberbr0" ip daddr 10.200.1.1 tcp dport 53 redirect to :5300
	}
	chain output {
		type nat hook output priority dstnat; policy accept;
	}
	chain postrouting {
		type nat hook postrouting priority srcnat; policy accept;
	}
}
"#;
        let s = container_spec(Some("10.200.1.0/24"), &["10.99.0.4", "10.99.0.1"]);
        assert_eq!(render("faber0", &s), expected);
    }

    #[test]
    fn render_containers_with_no_node_mesh_ips_golden() {
        let head = container_head("\tset node_mesh_ips {\n\t\ttype ipv4_addr\n\t}\n");
        let forward = r#"	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		iifname "faberbr0" oifname != { "faber0", "faberbr0" } drop
		oifname "faberbr0" iifname != { "faber0", "faberbr0" } drop
		iifname "faber0" oifname "faber0" drop
		iifname "faber0" ct state established,related accept
		oifname "faber0" ct state established,related accept
		ip saddr 10.200.0.0/24 ip daddr != 10.200.0.0/16 drop
		iifname "faber0" oifname "faberbr0" ip saddr @node_mesh_ips ip daddr 10.200.0.0/24 accept
		iifname "faber0" oifname "faberbr0" ip saddr 10.200.0.0/16 ip daddr 10.200.0.0/24 accept
		iifname "faberbr0" oifname "faber0" ip saddr 10.200.0.0/24 ip daddr 10.200.0.0/16 accept
		oifname "faber0" drop
		iifname "faber0" drop
	}
	chain prerouting {
		type nat hook prerouting priority dstnat; policy accept;
		iifname "faberbr0" ip daddr 10.200.0.1 udp dport 53 redirect to :5300
		iifname "faberbr0" ip daddr 10.200.0.1 tcp dport 53 redirect to :5300
	}
	chain output {
		type nat hook output priority dstnat; policy accept;
	}
	chain postrouting {
		type nat hook postrouting priority srcnat; policy accept;
	}
}
"#;
        assert_eq!(
            render("faber0", &container_spec(Some("10.200.0.0/24"), &[])),
            format!("{head}{forward}")
        );
    }

    fn backend(ip: &str, port: u16, protocol: NatProtocol) -> NatBackend {
        NatBackend {
            ip: ip.parse().unwrap(),
            port,
            protocol,
        }
    }

    fn service(vip: &str, backends: Vec<NatBackend>) -> ServiceNat {
        ServiceNat {
            vip: vip.parse().unwrap(),
            backends,
        }
    }

    const NAT_START: &str = "\tchain prerouting {";

    #[test]
    fn render_service_nat_golden() {
        let services = [
            // Two replicas, listed out of order, one of them on this node.
            service(
                "10.201.0.2",
                vec![
                    backend("10.200.2.5", 6379, NatProtocol::Tcp),
                    backend("10.200.1.4", 6379, NatProtocol::Tcp),
                ],
            ),
            // Two protocols on one port, one replica.
            service(
                "10.201.0.1",
                vec![
                    backend("10.200.1.2", 53, NatProtocol::Udp),
                    backend("10.200.1.2", 53, NatProtocol::Tcp),
                ],
            ),
            // No backend: no rule.
            service("10.201.0.3", vec![]),
        ];
        let s = container_spec(Some("10.200.1.0/24"), &["10.99.0.1"]);
        let expected = r#"	chain prerouting {
		type nat hook prerouting priority dstnat; policy accept;
		iifname "faberbr0" ip daddr 10.200.1.1 udp dport 53 redirect to :5300
		iifname "faberbr0" ip daddr 10.200.1.1 tcp dport 53 redirect to :5300
		iifname "faberbr0" ip daddr 10.201.0.1 tcp dport 53 dnat ip to numgen random mod 1 map { 0 : 10.200.1.2 }
		iifname "faberbr0" ip daddr 10.201.0.1 udp dport 53 dnat ip to numgen random mod 1 map { 0 : 10.200.1.2 }
		iifname "faberbr0" ip daddr 10.201.0.2 tcp dport 6379 dnat ip to numgen random mod 2 map { 0 : 10.200.1.4, 1 : 10.200.2.5 }
	}
	chain output {
		type nat hook output priority dstnat; policy accept;
		ip daddr 10.201.0.1 tcp dport 53 dnat ip to numgen random mod 1 map { 0 : 10.200.1.2 }
		ip daddr 10.201.0.1 udp dport 53 dnat ip to numgen random mod 1 map { 0 : 10.200.1.2 }
		ip daddr 10.201.0.2 tcp dport 6379 dnat ip to numgen random mod 2 map { 0 : 10.200.1.4, 1 : 10.200.2.5 }
	}
	chain postrouting {
		type nat hook postrouting priority srcnat; policy accept;
		oifname "faberbr0" ip saddr 10.200.1.2 ip daddr 10.200.1.2 ct status dnat masquerade
		oifname "faberbr0" ip saddr 10.200.1.4 ip daddr 10.200.1.4 ct status dnat masquerade
	}
}
"#;
        let script = render_with_services("faber0", &s, &services);
        assert_eq!(&script[script.find(NAT_START).unwrap()..], expected);
        // Above the nat chains the table is the one without services.
        let plain = render("faber0", &s);
        assert_eq!(
            script[..script.find(NAT_START).unwrap()],
            plain[..plain.find(NAT_START).unwrap()]
        );
    }

    #[test]
    fn service_nat_does_not_depend_on_input_order() {
        let a = service(
            "10.201.0.1",
            vec![
                backend("10.200.1.2", 80, NatProtocol::Tcp),
                backend("10.200.2.2", 80, NatProtocol::Tcp),
            ],
        );
        let b = service(
            "10.201.0.2",
            vec![backend("10.200.1.3", 80, NatProtocol::Tcp)],
        );
        let mut a_rev = a.clone();
        a_rev.backends.reverse();
        let s = container_spec(Some("10.200.1.0/24"), &[]);
        assert_eq!(
            render_with_services("faber0", &s, &[a.clone(), b.clone()]),
            render_with_services("faber0", &s, &[b, a_rev])
        );
    }

    #[test]
    fn a_service_without_backends_has_no_rule() {
        let s = container_spec(Some("10.200.1.0/24"), &[]);
        let script = render_with_services("faber0", &s, &[service("10.201.0.1", vec![])]);
        assert!(!script.contains("mod 0"), "{script}");
        assert_eq!(script, render("faber0", &s));
    }

    #[test]
    fn nat_rules_come_only_with_containers() {
        let services = [service(
            "10.201.0.1",
            vec![backend("10.200.1.2", 80, NatProtocol::Tcp)],
        )];
        assert_eq!(
            render_with_services("faber0", &spec(), &services),
            render("faber0", &spec())
        );
    }

    /// A node must not relay virtual IP traffic for a peer: the rules in
    /// `prerouting` match the bridge, the rules in `output` match no input.
    #[test]
    fn dnat_applies_only_to_the_bridge_and_the_host() {
        let services = [service(
            "10.201.0.1",
            vec![backend("10.200.1.2", 80, NatProtocol::Tcp)],
        )];
        let s = container_spec(Some("10.200.1.0/24"), &[]);
        let script = render_with_services("faber0", &s, &services);
        let dnat: Vec<&str> = script
            .lines()
            .filter(|l| l.contains("dnat ip to"))
            .collect();
        assert_eq!(dnat.len(), 2, "{script}");
        assert!(dnat[0].trim_start().starts_with("iifname \"faberbr0\" "));
        assert!(!dnat[1].contains("iifname"));
    }

    #[test]
    fn the_dns_port_is_not_the_mdns_port() {
        assert_ne!(DNS_REDIRECT_PORT, 5353);
        assert!(DNS_REDIRECT_PORT > 1024);
    }

    /// The container rules never touch the input chain: a container must not
    /// reach another node's mesh IP.
    #[test]
    fn containers_leave_the_input_chain_unchanged() {
        fn input_chain(script: &str) -> &str {
            let start = script.find("\tchain input {").unwrap();
            let end = script.find("\tchain forward {").unwrap();
            &script[start..end]
        }
        let without = render("faber0", &spec());
        let with = render(
            "faber0",
            &container_spec(Some("10.200.1.0/24"), &["10.99.0.1"]),
        );
        assert_eq!(input_chain(&with), input_chain(&without));
    }

    /// A forwarded packet, as far as the forward chain looks at it. `v4` is
    /// source and destination, `None` for an IPv6 packet.
    struct Packet {
        iif: &'static str,
        oif: &'static str,
        v4: Option<(&'static str, &'static str)>,
        established: bool,
    }

    fn new_v4(
        iif: &'static str,
        oif: &'static str,
        src: &'static str,
        dst: &'static str,
    ) -> Packet {
        Packet {
            iif,
            oif,
            v4: Some((src, dst)),
            established: false,
        }
    }

    fn reply_v4(
        iif: &'static str,
        oif: &'static str,
        src: &'static str,
        dst: &'static str,
    ) -> Packet {
        Packet {
            established: true,
            ..new_v4(iif, oif, src, dst)
        }
    }

    fn new_v6(iif: &'static str, oif: &'static str) -> Packet {
        Packet {
            iif,
            oif,
            v4: None,
            established: false,
        }
    }

    /// The verdict of the rendered forward chain for `packet`: the first
    /// rule whose matches all hold decides, and the chain policy is accept.
    /// Knows only the expressions the forward chain uses and panics on any
    /// other, so a new kind of rule has to be taught here first.
    fn forward_verdict(spec: &FirewallSpec, packet: &Packet) -> &'static str {
        let script = render("faber0", spec);
        let chain = script.split_once("\tchain forward {\n").unwrap().1;
        let nodes = &spec.containers.as_ref().unwrap().node_mesh_ips;
        let name = |t: &str| t.trim_matches(|c| c == '"' || c == ',').to_string();
        for rule in chain.lines().skip(1).take_while(|l| l.trim() != "}") {
            let mut tokens = rule.split_whitespace().peekable();
            let mut matches = true;
            while let Some(token) = tokens.next() {
                let negated = |tokens: &mut std::iter::Peekable<_>| {
                    tokens.next_if(|t: &&str| *t == "!=").is_some()
                };
                match token {
                    "iifname" | "oifname" => {
                        let actual = if token == "iifname" {
                            packet.iif
                        } else {
                            packet.oif
                        };
                        let not = negated(&mut tokens);
                        let mut names = Vec::new();
                        if tokens.next_if_eq(&"{").is_some() {
                            for t in tokens.by_ref().take_while(|t| *t != "}") {
                                names.push(name(t));
                            }
                        } else {
                            names.push(name(tokens.next().unwrap()));
                        }
                        matches &= names.iter().any(|n| n == actual) != not;
                    }
                    "ip" => {
                        let field = tokens.next().unwrap();
                        let not = negated(&mut tokens);
                        let value = tokens.next().unwrap();
                        let Some((src, dst)) = packet.v4 else {
                            // An `ip` match never holds for an IPv6 packet.
                            matches = false;
                            continue;
                        };
                        let addr: Ipv4Addr = match field {
                            "saddr" => src.parse().unwrap(),
                            "daddr" => dst.parse().unwrap(),
                            other => panic!("unknown ip field {other} in {rule}"),
                        };
                        let hit = match value.strip_prefix('@') {
                            Some(SET_NODE_MESH_IPS) => nodes.contains(&addr),
                            Some(other) => panic!("unknown set {other} in {rule}"),
                            None => value.parse::<Ipv4Net>().unwrap().contains(&addr),
                        };
                        matches &= hit != not;
                    }
                    "ct" => {
                        assert_eq!(tokens.next(), Some("state"), "{rule}");
                        assert_eq!(tokens.next(), Some("established,related"), "{rule}");
                        matches &= packet.established;
                    }
                    "accept" if matches => return "accept",
                    "drop" if matches => return "drop",
                    "accept" | "drop" => {}
                    other => panic!("unknown token {other} in {rule}"),
                }
            }
        }
        "accept"
    }

    /// Node 10.99.0.2 with subnet 10.200.1.0/24; 10.99.0.1 is a registered
    /// node, 10.99.0.3 only holds an invite.
    fn with_subnet() -> FirewallSpec {
        container_spec(Some("10.200.1.0/24"), &["10.99.0.1"])
    }

    fn without_subnet() -> FirewallSpec {
        container_spec(None, &["10.99.0.1"])
    }

    /// Finding 1: a LAN host that routes the container subnet through this
    /// node must not get in through the public interface.
    #[test]
    fn nothing_enters_the_bridge_from_outside_the_mesh() {
        for spec in [with_subnet(), without_subnet()] {
            let lan = new_v4("eth0", "faberbr0", "192.0.2.9", "10.200.1.5");
            assert_eq!(forward_verdict(&spec, &lan), "drop");
            // A forged container source does not help either.
            let forged = new_v4("eth0", "faberbr0", "10.200.2.7", "10.200.1.5");
            assert_eq!(forward_verdict(&spec, &forged), "drop");
            let reply = reply_v4("eth0", "faberbr0", "192.0.2.9", "10.200.1.5");
            assert_eq!(forward_verdict(&spec, &reply), "drop");
        }
    }

    /// Finding 2: the egress drop matches on the container's address, so a
    /// container that forges its source used to get past it.
    #[test]
    fn a_forged_source_gives_a_container_no_egress() {
        for spec in [with_subnet(), without_subnet()] {
            let metadata = new_v4("faberbr0", "eth0", "192.0.2.9", "169.254.169.254");
            assert_eq!(forward_verdict(&spec, &metadata), "drop");
            let tunnel = new_v4("faberbr0", "faber0", "192.0.2.9", "10.99.0.1");
            assert_eq!(forward_verdict(&spec, &tunnel), "drop");
            let other = new_v4("faberbr0", "faber0", "10.99.0.1", "10.200.2.7");
            assert_eq!(forward_verdict(&spec, &other), "drop");
        }
    }

    /// Finding 3: the address rules are IPv4 only; the interface rules hold
    /// for every family.
    #[test]
    fn ipv6_does_not_cross_the_bridge_boundary() {
        for spec in [with_subnet(), without_subnet()] {
            assert_eq!(forward_verdict(&spec, &new_v6("faberbr0", "eth0")), "drop");
            assert_eq!(forward_verdict(&spec, &new_v6("eth0", "faberbr0")), "drop");
            assert_eq!(
                forward_verdict(&spec, &new_v6("faberbr0", "faber0")),
                "drop"
            );
            assert_eq!(
                forward_verdict(&spec, &new_v6("faber0", "faberbr0")),
                "drop"
            );
        }
    }

    /// Finding 4: once the mesh interface is gone, traffic for another
    /// node's containers follows the default route. It must not leave in
    /// clear text. The rules match interfaces by name, so they hold while
    /// the interface does not exist.
    #[test]
    fn container_traffic_does_not_leave_by_the_public_interface() {
        for spec in [with_subnet(), without_subnet()] {
            let leaked = new_v4("faberbr0", "eth0", "10.200.1.5", "10.200.2.7");
            assert_eq!(forward_verdict(&spec, &leaked), "drop");
        }
        let egress = new_v4("faberbr0", "eth0", "10.200.1.5", "198.51.100.7");
        assert_eq!(forward_verdict(&with_subnet(), &egress), "drop");
    }

    /// With `br_netfilter` loaded, traffic between two containers on the
    /// bridge passes the forward hook with the bridge as both interfaces.
    #[test]
    fn same_bridge_traffic_is_left_alone() {
        for spec in [with_subnet(), without_subnet()] {
            let v4 = new_v4("faberbr0", "faberbr0", "10.200.1.5", "10.200.1.6");
            assert_eq!(forward_verdict(&spec, &v4), "accept");
            assert_eq!(
                forward_verdict(&spec, &new_v6("faberbr0", "faberbr0")),
                "accept"
            );
        }
    }

    #[test]
    fn containers_reach_remote_containers_and_get_replies() {
        let spec = with_subnet();
        let out = new_v4("faberbr0", "faber0", "10.200.1.5", "10.200.2.7");
        assert_eq!(forward_verdict(&spec, &out), "accept");
        let back = reply_v4("faber0", "faberbr0", "10.200.2.7", "10.200.1.5");
        assert_eq!(forward_verdict(&spec, &back), "accept");
        let inbound = new_v4("faber0", "faberbr0", "10.200.2.7", "10.200.1.5");
        assert_eq!(forward_verdict(&spec, &inbound), "accept");
        let answer = reply_v4("faberbr0", "faber0", "10.200.1.5", "10.200.2.7");
        assert_eq!(forward_verdict(&spec, &answer), "accept");
    }

    /// The reply has a container source and a destination outside the
    /// container range, so it must meet the established accept before the
    /// egress drop.
    #[test]
    fn nodes_reach_remote_containers_and_get_replies() {
        let spec = with_subnet();
        let request = new_v4("faber0", "faberbr0", "10.99.0.1", "10.200.1.5");
        assert_eq!(forward_verdict(&spec, &request), "accept");
        let reply = reply_v4("faberbr0", "faber0", "10.200.1.5", "10.99.0.1");
        assert_eq!(forward_verdict(&spec, &reply), "accept");
    }

    #[test]
    fn the_tunnel_carries_nothing_else() {
        let spec = with_subnet();
        // A container opens nothing towards a node's mesh IP.
        let to_node = new_v4("faberbr0", "faber0", "10.200.1.5", "10.99.0.1");
        assert_eq!(forward_verdict(&spec, &to_node), "drop");
        // The holder of an unused invite is a peer, not a node.
        let invite = new_v4("faber0", "faberbr0", "10.99.0.3", "10.200.1.5");
        assert_eq!(forward_verdict(&spec, &invite), "drop");
        // Nothing is relayed between peers or routed on to the LAN.
        let relay = new_v4("faber0", "faber0", "10.200.2.7", "10.200.1.5");
        assert_eq!(forward_verdict(&spec, &relay), "drop");
        let lan = new_v4("faber0", "eth0", "10.99.0.1", "192.0.2.9");
        assert_eq!(forward_verdict(&spec, &lan), "drop");
        // Forwarding that involves neither interface is the admin's.
        let other = new_v4("eth0", "eth1", "192.0.2.9", "198.51.100.7");
        assert_eq!(forward_verdict(&spec, &other), "accept");
    }

    /// A LAN host that routes the container range through this node and
    /// forges a source in the local subnet must not get into the tunnel:
    /// the remote peer's AllowedIPs would accept that source.
    #[test]
    fn only_the_bridge_sends_container_traffic_into_the_tunnel() {
        let spec = with_subnet();
        let forged = new_v4("eth0", "faber0", "10.200.1.5", "10.200.2.7");
        assert_eq!(forward_verdict(&spec, &forged), "drop");
        let genuine = new_v4("faberbr0", "faber0", "10.200.1.5", "10.200.2.7");
        assert_eq!(forward_verdict(&spec, &genuine), "accept");
    }

    /// The local subnet is behind the bridge. If a more specific route
    /// sends part of it elsewhere, a peer must not use this node to reach
    /// that interface.
    #[test]
    fn the_tunnel_delivers_to_the_local_subnet_through_the_bridge_only() {
        let spec = with_subnet();
        for src in ["10.99.0.1", "10.200.2.7"] {
            let elsewhere = new_v4("faber0", "eth0", src, "10.200.1.5");
            assert_eq!(forward_verdict(&spec, &elsewhere), "drop", "{src}");
            let bridge = new_v4("faber0", "faberbr0", src, "10.200.1.5");
            assert_eq!(forward_verdict(&spec, &bridge), "accept", "{src}");
        }
    }

    #[tokio::test]
    async fn apply_rejects_a_bridge_named_like_the_mesh_interface() {
        let r = RecordingRunner::new();
        let mut s = with_subnet();
        s.containers.as_mut().unwrap().bridge = "faber0".into();
        let err = apply(&r, "faber0", &s, &[]).await.unwrap_err();
        assert!(
            matches!(&err, NetworkError::BridgeIsMeshInterface(n) if n == "faber0"),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            "container bridge faber0 has the same name as the mesh interface"
        );
        assert!(r.calls().is_empty());
        // A bridge with another name is fine.
        apply(&r, "faber0", &with_subnet(), &[]).await.unwrap();
        assert_eq!(r.cmdlines(), vec!["nft -f -"]);
    }

    #[tokio::test]
    async fn apply_rejects_a_bad_bridge_name() {
        let r = RecordingRunner::new();
        let mut s = with_subnet();
        s.containers.as_mut().unwrap().bridge = "br0\" accept".into();
        let err = apply(&r, "faber0", &s, &[]).await.unwrap_err();
        assert!(
            matches!(&err, NetworkError::InvalidInterface(n) if n == "br0\" accept"),
            "{err:?}"
        );
        assert!(r.calls().is_empty());
    }

    #[tokio::test]
    async fn apply_pipes_script_on_stdin() {
        let r = RecordingRunner::new();
        apply(&r, "faber0", &spec(), &[]).await.unwrap();
        let calls = r.calls();
        assert_eq!(r.cmdlines(), vec!["nft -f -"]);
        assert_eq!(
            calls[0].stdin.as_deref(),
            Some(render("faber0", &spec()).as_bytes())
        );
    }

    #[tokio::test]
    async fn apply_reports_stderr() {
        let r = RecordingRunner::new();
        r.script(
            "nft",
            None,
            CommandOutput::failure(1, "Error: syntax error, unexpected junk"),
        );
        let err = apply(&r, "faber0", &spec(), &[]).await.unwrap_err();
        assert!(err.to_string().contains("unexpected junk"), "{err}");
    }

    #[tokio::test]
    async fn apply_rejects_bad_iface() {
        let r = RecordingRunner::new();
        let s = spec();
        assert!(matches!(
            apply(&r, "faber0\" accept", &s, &[]).await,
            Err(NetworkError::InvalidInterface(_))
        ));
        assert!(r.calls().is_empty());
    }

    #[test]
    fn endpoint_ip_parses_v4_v6_and_mapped() {
        assert_eq!(
            endpoint_ip("203.0.113.1:51820"),
            Some("203.0.113.1".parse().unwrap())
        );
        assert_eq!(
            endpoint_ip("[2001:db8::1]:51820"),
            Some("2001:db8::1".parse().unwrap())
        );
        assert_eq!(
            endpoint_ip("[::ffff:198.51.100.2]:51820"),
            Some("198.51.100.2".parse().unwrap())
        );
    }

    #[test]
    fn endpoint_ip_rejects_hostnames_and_junk() {
        for bad in [
            "",
            "example.com:51820",
            "203.0.113.1",
            "2001:db8::1:51820",
            "203.0.113.1:99999",
            "203.0.113.1:51820\n",
            " 203.0.113.1:51820",
        ] {
            assert_eq!(endpoint_ip(bad), None, "{bad:?}");
        }
    }
}
