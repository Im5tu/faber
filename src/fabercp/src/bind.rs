//! Listener address validation. faber only listens on loopback or on a
//! WireGuard interface, never on a LAN or public address.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// One address on one network interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetInterface {
    pub name: String,
    pub ip: IpAddr,
    pub wireguard: bool,
}

/// Source of the host's interfaces; injectable so tests can fake WireGuard.
pub trait InterfaceLookup {
    fn interfaces(&self) -> io::Result<Vec<NetInterface>>;
}

/// The real interfaces of this host.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemInterfaces;

impl InterfaceLookup for SystemInterfaces {
    fn interfaces(&self) -> io::Result<Vec<NetInterface>> {
        Ok(if_addrs::get_if_addrs()?
            .into_iter()
            .map(|i| NetInterface {
                wireguard: is_wireguard(&i.name),
                ip: i.ip(),
                name: i.name,
            })
            .collect())
    }
}

/// Linux marks WireGuard links with `DEVTYPE=wireguard` in sysfs.
#[cfg(target_os = "linux")]
fn is_wireguard(name: &str) -> bool {
    // Interface names never contain '/', but don't build a path from one that does.
    if name.contains('/') {
        return false;
    }
    std::fs::read_to_string(format!("/sys/class/net/{name}/uevent"))
        .map(|s| s.lines().any(|l| l.trim() == "DEVTYPE=wireguard"))
        .unwrap_or(false)
}

/// Only Linux can be verified; elsewhere only loopback passes (dev and tests).
#[cfg(not(target_os = "linux"))]
fn is_wireguard(_name: &str) -> bool {
    false
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    #[error("API listener {0} must be a loopback address")]
    ApiNotLoopback(SocketAddr),
    #[error("peer listener IP {0} is neither loopback nor on a WireGuard interface")]
    PeerNotAllowed(IpAddr),
    #[error("network interface {0} not found or has no address")]
    InterfaceNotFound(String),
    #[error("network interface {0} is not a WireGuard interface")]
    NotWireGuard(String),
    #[error("listing network interfaces failed: {0}")]
    Lookup(String),
}

/// The local API listener must be loopback.
pub fn check_api_addr(addr: SocketAddr) -> Result<(), BindError> {
    if addr.ip().is_loopback() {
        Ok(())
    } else {
        Err(BindError::ApiNotLoopback(addr))
    }
}

/// The peer listener must be loopback or an address on a WireGuard interface.
pub fn check_peer_ip(ip: IpAddr, lookup: &dyn InterfaceLookup) -> Result<(), BindError> {
    if ip.is_loopback() {
        return Ok(());
    }
    if ip.is_unspecified() {
        return Err(BindError::PeerNotAllowed(ip));
    }
    let ifaces = lookup
        .interfaces()
        .map_err(|e| BindError::Lookup(e.to_string()))?;
    if ifaces.iter().any(|i| i.ip == ip && i.wireguard) {
        Ok(())
    } else {
        Err(BindError::PeerNotAllowed(ip))
    }
}

/// Address of the WireGuard interface `name`, preferring IPv4.
pub fn resolve_wg_interface(name: &str, lookup: &dyn InterfaceLookup) -> Result<IpAddr, BindError> {
    let ifaces: Vec<_> = lookup
        .interfaces()
        .map_err(|e| BindError::Lookup(e.to_string()))?
        .into_iter()
        .filter(|i| i.name == name)
        .collect();
    if ifaces.is_empty() {
        return Err(BindError::InterfaceNotFound(name.to_string()));
    }
    if !ifaces.iter().all(|i| i.wireguard) {
        return Err(BindError::NotWireGuard(name.to_string()));
    }
    ifaces
        .iter()
        .find(|i| i.ip.is_ipv4())
        .or_else(|| ifaces.first())
        .map(|i| i.ip)
        .ok_or_else(|| BindError::InterfaceNotFound(name.to_string()))
}

/// Chooses the peer listener address: `wg_interface` + `peer_port`, else
/// `peer_listen`, else loopback on `peer_port` (single-host development).
pub fn peer_addr(
    wg_interface: Option<&str>,
    peer_listen: Option<SocketAddr>,
    peer_port: u16,
    lookup: &dyn InterfaceLookup,
) -> Result<SocketAddr, BindError> {
    match (wg_interface, peer_listen) {
        (Some(name), _) => Ok(SocketAddr::new(
            resolve_wg_interface(name, lookup)?,
            peer_port,
        )),
        (None, Some(addr)) => {
            check_peer_ip(addr.ip(), lookup)?;
            Ok(addr)
        }
        (None, None) => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), peer_port)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake(Vec<NetInterface>);

    impl InterfaceLookup for Fake {
        fn interfaces(&self) -> io::Result<Vec<NetInterface>> {
            Ok(self.0.clone())
        }
    }

    fn iface(name: &str, ip: &str, wireguard: bool) -> NetInterface {
        NetInterface {
            name: name.into(),
            ip: ip.parse().unwrap(),
            wireguard,
        }
    }

    fn host() -> Fake {
        Fake(vec![
            iface("lo", "127.0.0.1", false),
            iface("eth0", "192.168.1.5", false),
            iface("faber0", "fd00::1", true),
            iface("faber0", "10.8.0.1", true),
        ])
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn api_accepts_only_loopback() {
        assert_eq!(check_api_addr(addr("127.0.0.1:7400")), Ok(()));
        assert_eq!(check_api_addr(addr("[::1]:7400")), Ok(()));
        for bad in [
            "0.0.0.0:7400",
            "[::]:7400",
            "192.168.1.5:7400",
            "10.8.0.1:7400",
        ] {
            assert_eq!(
                check_api_addr(addr(bad)),
                Err(BindError::ApiNotLoopback(addr(bad)))
            );
        }
    }

    #[test]
    fn peer_accepts_only_loopback_and_wireguard_addresses() {
        let h = host();
        for ok in ["127.0.0.1", "::1", "10.8.0.1", "fd00::1"] {
            assert_eq!(check_peer_ip(ok.parse().unwrap(), &h), Ok(()), "{ok}");
        }
        for bad in ["0.0.0.0", "::", "192.168.1.5", "10.8.0.2"] {
            let ip: IpAddr = bad.parse().unwrap();
            assert_eq!(check_peer_ip(ip, &h), Err(BindError::PeerNotAllowed(ip)));
        }
    }

    #[test]
    fn wg_interface_resolves_to_its_ipv4_address() {
        assert_eq!(
            resolve_wg_interface("faber0", &host()),
            Ok("10.8.0.1".parse().unwrap())
        );
        assert_eq!(
            resolve_wg_interface("eth0", &host()),
            Err(BindError::NotWireGuard("eth0".into()))
        );
        assert_eq!(
            resolve_wg_interface("wg9", &host()),
            Err(BindError::InterfaceNotFound("wg9".into()))
        );
    }

    #[test]
    fn peer_addr_selection() {
        let h = host();
        assert_eq!(
            peer_addr(Some("faber0"), None, 7401, &h),
            Ok(addr("10.8.0.1:7401"))
        );
        assert_eq!(
            peer_addr(None, Some(addr("10.8.0.1:9000")), 7401, &h),
            Ok(addr("10.8.0.1:9000"))
        );
        assert_eq!(
            peer_addr(None, Some(addr("192.168.1.5:9000")), 7401, &h),
            Err(BindError::PeerNotAllowed("192.168.1.5".parse().unwrap()))
        );
        assert_eq!(peer_addr(None, None, 7401, &h), Ok(addr("127.0.0.1:7401")));
    }

    #[test]
    fn lookup_failure_is_reported() {
        struct Broken;
        impl InterfaceLookup for Broken {
            fn interfaces(&self) -> io::Result<Vec<NetInterface>> {
                Err(io::Error::other("nope"))
            }
        }
        assert!(matches!(
            check_peer_ip("10.8.0.1".parse().unwrap(), &Broken),
            Err(BindError::Lookup(_))
        ));
    }

    #[test]
    fn system_lookup_lists_loopback() {
        let ifaces = SystemInterfaces.interfaces().unwrap();
        assert!(ifaces.iter().any(|i| i.ip.is_loopback()), "{ifaces:?}");
    }
}
