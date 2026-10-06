//! WireGuard endpoints as [`SocketAddr`].
//!
//! An endpoint is typed wherever this node handles its own (`--wg-endpoint`,
//! `identity.json`, an invite request). On the wire and in replicated state
//! it stays a string, always the canonical form [`SocketAddr`] prints, so
//! that two spellings of one address (IPv6 case, an IPv4-mapped address)
//! compare equal as text.

use std::net::{IpAddr, Ipv6Addr, SocketAddr};

/// Why a string is not a usable endpoint.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EndpointError {
    #[error(
        "endpoint {0:?} must be an IP address with an optional port; \
         hostnames are not allowed because peers are allowed by IP"
    )]
    NotAnAddress(String),
    #[error("endpoint {0:?} needs a specific IP and port")]
    Unspecified(String),
}

/// Parses a WireGuard endpoint. Accepts `ip:port`, `[v6]:port`, a bare IPv4
/// or IPv6 address, or `[v6]`; a missing port becomes `default_port`.
/// Surrounding whitespace is ignored. An IPv4-mapped IPv6 address becomes
/// IPv4, as it does in the firewall's endpoint set.
///
/// Hostnames are refused: the firewall allows peers by IP. So are the
/// unspecified address and port 0.
pub fn parse_endpoint(input: &str, default_port: u16) -> Result<SocketAddr, EndpointError> {
    let s = input.trim();
    let not_an_address = || EndpointError::NotAnAddress(input.to_string());
    let addr = if let Ok(sa) = s.parse::<SocketAddr>() {
        sa
    } else if let Some(v6) = s.strip_prefix('[').and_then(|r| r.strip_suffix(']')) {
        let ip: Ipv6Addr = v6.parse().map_err(|_| not_an_address())?;
        SocketAddr::new(IpAddr::V6(ip), default_port)
    } else if let Ok(ip) = s.parse::<IpAddr>() {
        SocketAddr::new(ip, default_port)
    } else {
        return Err(not_an_address());
    };
    if addr.ip().is_unspecified() || addr.port() == 0 {
        return Err(EndpointError::Unspecified(input.to_string()));
    }
    Ok(SocketAddr::new(addr.ip().to_canonical(), addr.port()))
}

/// Parses an endpoint that must already carry its port (`ip:port` or
/// `[v6]:port`), as internal requests and stored values do.
pub fn parse_full_endpoint(input: &str) -> Result<SocketAddr, EndpointError> {
    if input.trim().parse::<SocketAddr>().is_err() {
        return Err(EndpointError::NotAnAddress(input.to_string()));
    }
    // The port is there, so the default is never used.
    parse_endpoint(input, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_forms_and_the_default_port() {
        let ok = [
            ("198.51.100.2", "198.51.100.2:51820"),
            (" 198.51.100.2 ", "198.51.100.2:51820"),
            ("198.51.100.2:51000", "198.51.100.2:51000"),
            ("2001:db8::2", "[2001:db8::2]:51820"),
            ("[2001:db8::2]", "[2001:db8::2]:51820"),
            ("[2001:db8::2]:51000", "[2001:db8::2]:51000"),
        ];
        for (input, want) in ok {
            assert_eq!(
                parse_endpoint(input, 51820).map(|a| a.to_string()),
                Ok(want.to_string()),
                "{input}"
            );
        }
    }

    #[test]
    fn hostnames_and_junk_are_refused() {
        for bad in [
            "",
            "node2.example",
            "node2.example:51820",
            "[2001:db8::2",
            "[node2]",
            "198.51.100.2:51820:1",
            "198.51.100.2/24",
            "198.51.100.2:99999",
        ] {
            assert!(
                matches!(
                    parse_endpoint(bad, 51820),
                    Err(EndpointError::NotAnAddress(_))
                ),
                "{bad}"
            );
        }
        for bad in ["0.0.0.0", "198.51.100.2:0", "[::]:51820"] {
            assert!(
                matches!(
                    parse_endpoint(bad, 51820),
                    Err(EndpointError::Unspecified(_))
                ),
                "{bad}"
            );
        }
        let msg = parse_endpoint("node2.example", 51820)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("hostnames are not allowed"), "{msg}");
    }

    #[test]
    fn spellings_of_one_address_are_equal() {
        let upper = parse_endpoint("[2001:DB8::2]:51820", 1).unwrap();
        let lower = parse_endpoint("[2001:db8::2]:51820", 1).unwrap();
        let long = parse_endpoint("[2001:0db8:0:0:0:0:0:2]:51820", 1).unwrap();
        assert_eq!(upper, lower);
        assert_eq!(long, lower);
        assert_eq!(upper.to_string(), "[2001:db8::2]:51820");
        assert_eq!(long.to_string(), lower.to_string());
        // An IPv4-mapped address is the IPv4 address.
        assert_eq!(
            parse_endpoint("[::ffff:198.51.100.2]:51820", 1).unwrap(),
            parse_endpoint("198.51.100.2:51820", 1).unwrap()
        );
    }

    #[test]
    fn a_full_endpoint_needs_its_port() {
        assert_eq!(
            parse_full_endpoint("[2001:DB8::2]:51820").map(|a| a.to_string()),
            Ok("[2001:db8::2]:51820".to_string())
        );
        for bad in ["198.51.100.2", "[2001:db8::2]", "node2.example:51820", ""] {
            assert!(parse_full_endpoint(bad).is_err(), "{bad}");
        }
        assert!(parse_full_endpoint("198.51.100.2:0").is_err());
    }
}
