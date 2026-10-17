//! The rule for a container subnet, shared by fabercp (which hands subnets
//! out) and faber-node (which writes them into the CNI config), so the two
//! cannot disagree.

use std::net::Ipv4Addr;

use ipnet::Ipv4Net;

/// Longest prefix of a container subnet: a /30 is the smallest subnet with a
/// gateway and one container address.
pub const MAX_CONTAINER_SUBNET_PREFIX: u8 = 30;

/// Ranges no container address may come from: this network, loopback,
/// link-local, multicast and the reserved class E block, which holds the
/// broadcast address.
pub const RESERVED: [Ipv4Net; 5] = [
    Ipv4Net::new_assert(Ipv4Addr::new(0, 0, 0, 0), 8),
    Ipv4Net::new_assert(Ipv4Addr::new(127, 0, 0, 0), 8),
    Ipv4Net::new_assert(Ipv4Addr::new(169, 254, 0, 0), 16),
    Ipv4Net::new_assert(Ipv4Addr::new(224, 0, 0, 0), 4),
    Ipv4Net::new_assert(Ipv4Addr::new(240, 0, 0, 0), 4),
];

/// Whether `net` can be a container range or one node's container subnet:
/// no host bits, a prefix of at most [`MAX_CONTAINER_SUBNET_PREFIX`], and no
/// address in a reserved range. Any other IPv4 range is accepted.
pub fn check_container_subnet(net: Ipv4Net) -> Result<(), String> {
    if net != net.trunc() {
        return Err(format!(
            "{net} has host bits set; did you mean {}?",
            net.trunc()
        ));
    }
    if net.prefix_len() > MAX_CONTAINER_SUBNET_PREFIX {
        return Err(format!(
            "{net} is longer than /{MAX_CONTAINER_SUBNET_PREFIX}"
        ));
    }
    if let Some(range) = RESERVED
        .iter()
        .find(|r| r.contains(&net.network()) || net.contains(&r.network()))
    {
        return Err(format!("{net} overlaps the reserved range {range}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    #[test]
    fn ordinary_ranges_pass() {
        for ok in [
            "10.200.0.0/16",
            "10.200.3.0/24",
            "172.16.0.0/12",
            "192.168.1.4/30",
            "100.64.0.0/10",
            "8.8.8.0/24", // public: allowed, as for the container CIDR
            "126.0.0.0/8",
        ] {
            assert!(check_container_subnet(net(ok)).is_ok(), "{ok}");
        }
        // A range that holds a reserved one is refused too.
        assert!(check_container_subnet(net("128.0.0.0/2")).is_err());
    }

    #[test]
    fn reserved_ranges_and_their_neighbours() {
        for bad in [
            "0.0.0.0/0",
            "0.0.0.0/8",
            "0.1.2.0/24",
            "127.0.0.0/8",
            "127.1.0.0/24",
            "169.254.0.0/16",
            "169.254.9.0/24",
            "224.0.0.0/4",
            "239.1.1.0/24",
            "240.0.0.0/4",
            "255.255.255.252/30",
            "0.0.0.0/1",
            "128.0.0.0/1",
        ] {
            assert!(check_container_subnet(net(bad)).is_err(), "{bad}");
        }
        for ok in [
            "1.0.0.0/8",
            "126.255.255.0/24",
            "128.0.0.0/24",
            "169.253.0.0/16",
            "223.255.255.0/24",
        ] {
            assert!(check_container_subnet(net(ok)).is_ok(), "{ok}");
        }
    }

    #[test]
    fn host_bits_and_long_prefixes_are_refused() {
        assert!(check_container_subnet(net("10.200.3.1/24")).is_err());
        assert!(check_container_subnet(net("10.200.3.0/30")).is_ok());
        for bad in ["10.200.3.0/31", "10.200.3.1/32"] {
            assert!(check_container_subnet(net(bad)).is_err(), "{bad}");
        }
    }
}
