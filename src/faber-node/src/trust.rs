//! The checks `faber-node` runs on every request, whatever fabercp sends.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::{
    ALLOWED_CAPABILITIES, ContainerSpec, PortSpec, VolumeSource, WorkloadSpec, limits,
};
use faber_node_protocol::{
    DnsConfig, ErrorCode, NodeError, PodNetwork, ReplicaRef, SecretEnv, VolumeKind,
};
use ipnet::Ipv4Net;

use crate::config::UidRange;

const MAX_NAMESERVERS: usize = 3;
const MAX_SEARCHES: usize = 6;
const MAX_DNS_NAME_BYTES: usize = 253;
const MAX_ID_BYTES: usize = 128;

fn invalid(message: impl Into<String>) -> NodeError {
    NodeError::new(ErrorCode::InvalidRequest, message)
}

fn forbidden(message: impl Into<String>) -> NodeError {
    NodeError::new(ErrorCode::Forbidden, message)
}

pub fn check_replica(replica: &ReplicaRef) -> Result<(), NodeError> {
    replica.validate().map_err(invalid)
}

/// A name that becomes one path segment: a DNS label.
pub fn check_volume_name(name: &str) -> Result<(), NodeError> {
    validate_dns_label(name).map_err(|e| invalid(format!("volume_name: {e}")))
}

/// The base of a user namespace: aligned, in the configured range, not 0.
pub fn check_uid_base(range: &UidRange, uid_base: u32) -> Result<(), NodeError> {
    range.check_base(uid_base).map_err(forbidden)
}

/// An id handed out by the runtime. Only ever compared and passed back to
/// it, but kept to a plain alphabet all the same.
pub fn check_runtime_id(what: &str, id: &str) -> Result<(), NodeError> {
    let ok = !id.is_empty()
        && id.len() <= MAX_ID_BYTES
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
    if ok {
        Ok(())
    } else {
        Err(invalid(format!("{what} is not a runtime id")))
    }
}

pub fn check_image(image: &str) -> Result<(), NodeError> {
    let spec = ContainerSpec::new("image-check", image);
    check_container_rules(&spec, &[])
}

pub fn check_dns(dns: &DnsConfig) -> Result<(), NodeError> {
    if dns.nameservers.len() > MAX_NAMESERVERS {
        return Err(invalid(format!(
            "dns: at most {MAX_NAMESERVERS} nameservers"
        )));
    }
    for ip in &dns.nameservers {
        if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() || ip.is_broadcast() {
            return Err(invalid(format!("dns: {ip} cannot be a nameserver")));
        }
    }
    if dns.searches.len() > MAX_SEARCHES {
        return Err(invalid(format!("dns: at most {MAX_SEARCHES} search names")));
    }
    for name in &dns.searches {
        let ok = name.len() <= MAX_DNS_NAME_BYTES
            && !name.is_empty()
            && name
                .split('.')
                .all(|label| validate_dns_label(label).is_ok());
        if !ok {
            return Err(invalid("dns: a search name is not a valid DNS name"));
        }
    }
    Ok(())
}

pub fn check_ports(ports: &[PortSpec]) -> Result<(), NodeError> {
    let most = limits::MAX_PORTS_PER_CONTAINER * limits::MAX_CONTAINERS_PER_WORKLOAD;
    if ports.len() > most {
        return Err(invalid(format!("ports: at most {most}")));
    }
    if ports.iter().any(|p| p.port == 0) {
        return Err(invalid("ports: port must not be 0"));
    }
    Ok(())
}

pub fn check_volume_size(kind: VolumeKind, size_bytes: u64) -> Result<(), NodeError> {
    let most = match kind {
        VolumeKind::Block => limits::MAX_BLOCK_VOLUME_BYTES,
        VolumeKind::Ramdisk => limits::MAX_RAMDISK_VOLUME_BYTES,
    };
    if size_bytes < limits::MIN_VOLUME_BYTES || size_bytes > most {
        return Err(invalid(format!(
            "size_bytes: must be from {} to {most}",
            limits::MIN_VOLUME_BYTES
        )));
    }
    Ok(())
}

/// Every bridge faber makes starts with this, so the CNI config can never
/// name a host interface such as `eth0` or the WireGuard link.
const BRIDGE_PREFIX: &str = "faber";
/// The longest interface name Linux accepts (`IFNAMSIZ` less the NUL).
const MAX_BRIDGE_BYTES: usize = 15;
/// Prefix lengths a node's container subnet may have.
const SUBNET_PREFIX: std::ops::RangeInclusive<u8> = 16..=28;
/// Subnets a node's containers may use: the private ranges and carrier-grade
/// NAT space, never public, loopback or link-local addresses.
const SUBNET_RANGES: [Ipv4Net; 4] = [
    Ipv4Net::new_assert(Ipv4Addr::new(10, 0, 0, 0), 8),
    Ipv4Net::new_assert(Ipv4Addr::new(172, 16, 0, 0), 12),
    Ipv4Net::new_assert(Ipv4Addr::new(192, 168, 0, 0), 16),
    Ipv4Net::new_assert(Ipv4Addr::new(100, 64, 0, 0), 10),
];
/// MTUs a bridge may have: the IPv4 minimum up to jumbo frames.
const MTU: std::ops::RangeInclusive<u16> = 576..=9000;

/// The containerd drop-in settings: only the sandbox image is free text, and
/// it goes through the rules of any image.
pub fn check_sandbox_image(image: &str) -> Result<(), NodeError> {
    check_image(image).map_err(|e| invalid(format!("sandbox_image: {}", e.message)))
}

/// A container network that is safe to write into the CNI config.
pub fn check_pod_network(network: &PodNetwork) -> Result<(), NodeError> {
    let name = &network.bridge;
    let name_ok = name.len() <= MAX_BRIDGE_BYTES
        && name.starts_with(BRIDGE_PREFIX)
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_'));
    if !name_ok {
        return Err(invalid(format!(
            "bridge: must be at most {MAX_BRIDGE_BYTES} bytes of a-z, 0-9, - and _, \
             starting with {BRIDGE_PREFIX}"
        )));
    }
    let subnet = network.subnet;
    if !SUBNET_PREFIX.contains(&subnet.prefix_len()) {
        return Err(invalid(format!(
            "subnet: the prefix length must be from {} to {}",
            SUBNET_PREFIX.start(),
            SUBNET_PREFIX.end()
        )));
    }
    if subnet != subnet.trunc() {
        return Err(invalid("subnet: host bits are set"));
    }
    if !SUBNET_RANGES.iter().any(|range| range.contains(&subnet)) {
        return Err(invalid("subnet: must be a private or shared address range"));
    }
    if !MTU.contains(&network.mtu) {
        return Err(invalid(format!(
            "mtu: must be from {} to {}",
            MTU.start(),
            MTU.end()
        )));
    }
    Ok(())
}

/// Checks a container for creation.
///
/// Capabilities outside the allow-list are `Forbidden`, whatever else is
/// wrong. The rest of the model's rules are re-run on a one-container
/// workload whose volumes are the ones the mounts name, and whose `env` is
/// the request's, so env names, sizes and NULs are checked too.
pub fn check_container(container: &ContainerSpec, env: &SecretEnv) -> Result<(), NodeError> {
    if let Some(cap) = container
        .capabilities
        .iter()
        .find(|c| !ALLOWED_CAPABILITIES.contains(&c.as_str()))
    {
        // Printing the name is safe only when it is short and plain.
        let shown: String = cap
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .take(32)
            .collect();
        return Err(forbidden(format!("capability {shown} is not allowed")));
    }
    let mut checked = container.clone();
    checked.env = env.0.clone();
    let volumes: Vec<&str> = container.mounts.iter().map(|m| m.volume.as_str()).collect();
    check_container_rules(&checked, &volumes)
}

fn check_container_rules(container: &ContainerSpec, volumes: &[&str]) -> Result<(), NodeError> {
    let spec = WorkloadSpec {
        scale: Default::default(),
        restart: Default::default(),
        expose: Default::default(),
        placement: Default::default(),
        stop_grace_secs: None,
        // Sizes are unknown here; the ramdisk-versus-memory rule needs
        // block volumes to stay out of it.
        volumes: volumes
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    VolumeSource::Block {
                        size_bytes: limits::MIN_VOLUME_BYTES,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
        containers: vec![container.clone()],
    };
    spec.validate().map_err(|violations| {
        // Violations never carry env values (the model guarantees it).
        let first = violations
            .iter()
            .take(3)
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        invalid(format!("container: {first}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use faber_api_types::workload::MountSpec;
    use std::net::Ipv4Addr;

    fn container() -> ContainerSpec {
        ContainerSpec::new("redis", "redis:7")
    }

    fn code(r: Result<(), NodeError>) -> Option<ErrorCode> {
        r.err().map(|e| e.code)
    }

    #[test]
    fn capabilities_outside_the_allow_list_are_forbidden() {
        for bad in ["SYS_ADMIN", "NET_ADMIN", "ALL", "all", "BOGUS", "CAP_CHOWN"] {
            let mut c = container();
            c.capabilities = vec![bad.into()];
            assert_eq!(
                code(check_container(&c, &SecretEnv::default())),
                Some(ErrorCode::Forbidden),
                "{bad}"
            );
        }
        let mut c = container();
        c.capabilities = ALLOWED_CAPABILITIES
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(check_container(&c, &SecretEnv::default()), Ok(()));
    }

    #[test]
    fn model_rules_are_re_run() {
        let mut c = container();
        c.name = "../x".into();
        assert_eq!(
            code(check_container(&c, &SecretEnv::default())),
            Some(ErrorCode::InvalidRequest)
        );
        let mut c = container();
        c.image = "a b".into();
        assert!(check_container(&c, &SecretEnv::default()).is_err());
        let mut c = container();
        c.mounts = vec![MountSpec {
            volume: "data".into(),
            path: "/proc/x".into(),
            read_only: false,
        }];
        assert!(check_container(&c, &SecretEnv::default()).is_err());
        c.mounts[0].path = "/data".into();
        assert_eq!(check_container(&c, &SecretEnv::default()), Ok(()));
    }

    #[test]
    fn the_request_env_is_checked_and_never_echoed() {
        let env = SecretEnv(BTreeMap::from([("BAD=NAME".into(), "hunter2".into())]));
        let err = check_container(&container(), &env).unwrap_err();
        assert!(!err.message.contains("hunter2"));
        let env = SecretEnv(BTreeMap::from([("A".into(), "x\0y".into())]));
        assert!(check_container(&container(), &env).is_err());
        let env = SecretEnv(BTreeMap::from([("A".into(), "fine".into())]));
        assert_eq!(check_container(&container(), &env), Ok(()));
    }

    #[test]
    fn uid_bases() {
        let range = UidRange::new(65536, 65536 * 4).unwrap();
        assert_eq!(check_uid_base(&range, 65536), Ok(()));
        for bad in [0, 1, 65535, 65537, 65536 * 4, u32::MAX] {
            assert_eq!(
                code(check_uid_base(&range, bad)),
                Some(ErrorCode::Forbidden),
                "{bad}"
            );
        }
    }

    #[test]
    fn names_ids_and_images() {
        assert!(check_volume_name("data").is_ok());
        for bad in ["", "..", "a/b", "a.b", "A", "../../etc", "a b"] {
            assert!(check_volume_name(bad).is_err(), "{bad:?}");
        }
        assert!(check_runtime_id("id", "abc123_-.:").is_ok());
        for bad in ["", "a/b", "a b", &"a".repeat(129), "a\0"] {
            assert!(check_runtime_id("id", bad).is_err(), "{bad:?}");
        }
        assert!(check_image("docker.io/library/redis:7").is_ok());
        for bad in ["", "a b", "a\nb", &"a".repeat(513)] {
            assert!(check_image(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn dns_rules() {
        let ok = DnsConfig {
            nameservers: vec![Ipv4Addr::new(10, 200, 0, 1)],
            searches: vec!["prod.shop.faber.local".into()],
        };
        assert!(check_dns(&ok).is_ok());
        for ip in [
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::LOCALHOST,
            Ipv4Addr::BROADCAST,
            Ipv4Addr::new(224, 0, 0, 1),
        ] {
            let mut d = ok.clone();
            d.nameservers = vec![ip];
            assert!(check_dns(&d).is_err(), "{ip}");
        }
        for bad in ["", "a..b", "a/b", "a.b.", "UP.case", "x y"] {
            let mut d = ok.clone();
            d.searches = vec![bad.into()];
            assert!(check_dns(&d).is_err(), "{bad:?}");
        }
        let mut d = ok.clone();
        d.nameservers = vec![Ipv4Addr::new(10, 0, 0, 1); 4];
        assert!(check_dns(&d).is_err());
    }

    #[test]
    fn volume_sizes() {
        assert!(check_volume_size(VolumeKind::Block, limits::MIN_VOLUME_BYTES).is_ok());
        assert!(check_volume_size(VolumeKind::Block, limits::MIN_VOLUME_BYTES - 1).is_err());
        assert!(
            check_volume_size(VolumeKind::Ramdisk, limits::MAX_RAMDISK_VOLUME_BYTES + 1).is_err()
        );
        assert!(check_volume_size(VolumeKind::Block, limits::MAX_RAMDISK_VOLUME_BYTES + 1).is_ok());
    }

    fn network(bridge: &str, subnet: &str, mtu: u16) -> PodNetwork {
        PodNetwork {
            bridge: bridge.into(),
            subnet: subnet.parse().unwrap(),
            mtu,
        }
    }

    #[test]
    fn the_pod_network_fabercp_sends_today_passes() {
        assert!(check_pod_network(&network("faberbr0", "10.200.3.0/24", 1420)).is_ok());
    }

    #[test]
    fn pod_network_bridge_names() {
        for ok in ["faberbr0", "faber", "faber-br_1", "faber0123456789"] {
            assert!(
                check_pod_network(&network(ok, "10.200.3.0/24", 1420)).is_ok(),
                "{ok}"
            );
        }
        for bad in [
            "",
            "eth0",
            "wg0",
            "Faberbr0",
            "faber0123456789a", // 16 bytes
            "faber br",
            "faber.br",
            "faber/br",
            "faber\"br",
            "faber\nbr",
            "faberbr\u{e9}",
            "../faber",
        ] {
            assert_eq!(
                code(check_pod_network(&network(bad, "10.200.3.0/24", 1420))),
                Some(ErrorCode::InvalidRequest),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn pod_network_subnets() {
        for ok in [
            "10.200.3.0/24",
            "10.0.0.0/16",
            "172.16.0.0/28",
            "172.31.255.0/24",
            "192.168.1.0/24",
            "100.64.0.0/16",
        ] {
            assert!(
                check_pod_network(&network("faberbr0", ok, 1420)).is_ok(),
                "{ok}"
            );
        }
        for bad in [
            "0.0.0.0/0",
            "10.0.0.0/8",     // too wide
            "10.200.3.0/15",  // too wide
            "10.200.3.0/29",  // too narrow
            "10.200.3.0/32",  // one host
            "10.200.3.4/24",  // host bits set
            "8.8.8.0/24",     // public
            "127.0.0.0/24",   // loopback
            "169.254.1.0/24", // link-local
            "172.32.0.0/24",  // just outside 172.16/12
            "100.128.0.0/24", // just outside 100.64/10
            "224.0.0.0/24",   // multicast
        ] {
            assert_eq!(
                code(check_pod_network(&network("faberbr0", bad, 1420))),
                Some(ErrorCode::InvalidRequest),
                "{bad}"
            );
        }
    }

    #[test]
    fn pod_network_mtus() {
        for ok in [576, 1280, 1420, 1500, 9000] {
            assert!(
                check_pod_network(&network("faberbr0", "10.200.3.0/24", ok)).is_ok(),
                "{ok}"
            );
        }
        for bad in [0, 575, 9001, u16::MAX] {
            assert!(
                check_pod_network(&network("faberbr0", "10.200.3.0/24", bad)).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn sandbox_images() {
        assert!(check_sandbox_image("registry.k8s.io/pause:3.10").is_ok());
        for bad in ["", "a b", "x\"\ny"] {
            assert_eq!(
                code(check_sandbox_image(bad)),
                Some(ErrorCode::InvalidRequest),
                "{bad:?}"
            );
        }
    }
}
