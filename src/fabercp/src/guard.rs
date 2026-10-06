//! Host/Origin guard against DNS rebinding and cross-site requests.
//!
//! `POST /api/v1/mesh/invites` returns a private key and `PUT
//! /api/v1/mesh/lease` changes the whole cluster, so a browser page must
//! never be able to reach the API, even through a rebound DNS name:
//! - `Host` must name loopback (`localhost`, `127.0.0.1`, `[::1]`, any port)
//!   or, on the peer listener, the listener's own IP.
//! - Any request with an `Origin` header, or with `Sec-Fetch-Site` other than
//!   `none`, is refused. Browsers send these; faber's own clients do not.
//!
//! Both answer 403 `forbidden`.
//!
//! `/mcp` keeps its own `Host` allow-list (it accepts configured extra hosts),
//! so it gets only [`origin_guard`].

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use axum::extract::{Request, State};
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::service::ServiceError;

/// Which `Host` values a listener accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostGuard {
    /// Accepted in addition to the loopback names: the peer listener's IP.
    extra_ip: Option<IpAddr>,
}

impl HostGuard {
    /// Loopback names only.
    pub fn local() -> Self {
        Self { extra_ip: None }
    }

    /// Loopback names plus `ip` (the peer listener's address).
    pub fn peer(ip: IpAddr) -> Self {
        Self { extra_ip: Some(ip) }
    }

    /// Whether `host` (a `Host` header value) is accepted.
    pub fn allows(&self, host: &str) -> bool {
        let Some(name) = host_name(host) else {
            return false;
        };
        if name.eq_ignore_ascii_case("localhost") {
            return true;
        }
        match name.parse::<IpAddr>() {
            Ok(ip) => {
                ip == IpAddr::V4(Ipv4Addr::LOCALHOST)
                    || ip == IpAddr::V6(Ipv6Addr::LOCALHOST)
                    || Some(ip) == self.extra_ip
            }
            Err(_) => false,
        }
    }
}

/// The host part of a `Host` value: `name`, `name:port`, `[v6]` or
/// `[v6]:port`. `None` for anything malformed, including an unbracketed IPv6
/// address.
fn host_name(host: &str) -> Option<&str> {
    let valid_port =
        |p: &str| !p.is_empty() && p.len() <= 5 && p.bytes().all(|b| b.is_ascii_digit());
    if let Some(rest) = host.strip_prefix('[') {
        let (ip, after) = rest.split_once(']')?;
        let ok = after.is_empty() || after.strip_prefix(':').is_some_and(valid_port);
        return (ok && ip.parse::<Ipv6Addr>().is_ok()).then_some(ip);
    }
    let name = match host.split_once(':') {
        Some((name, port)) if valid_port(port) => name,
        Some(_) => return None,
        None => host,
    };
    (!name.is_empty()).then_some(name)
}

/// Refuses requests that a browser made: any `Origin`, or `Sec-Fetch-Site`
/// other than `none`.
fn check_origin(headers: &HeaderMap) -> Result<(), ServiceError> {
    if headers.contains_key(header::ORIGIN) {
        return Err(ServiceError::Forbidden(
            "cross-origin requests are not allowed".into(),
        ));
    }
    if let Some(site) = headers.get("sec-fetch-site")
        && site.as_bytes() != b"none"
    {
        return Err(ServiceError::Forbidden(
            "browser requests are not allowed".into(),
        ));
    }
    Ok(())
}

/// Checks the `Host` header, or for HTTP/2 the URI authority.
fn check_host(guard: &HostGuard, req: &Request) -> Result<(), ServiceError> {
    let host = match req.headers().get(header::HOST) {
        Some(h) => h.to_str().unwrap_or(""),
        None => req.uri().authority().map_or("", |a| a.as_str()),
    };
    if guard.allows(host) {
        Ok(())
    } else {
        Err(ServiceError::Forbidden("Host header not allowed".into()))
    }
}

/// Middleware: [`HostGuard`] plus the Origin check.
pub async fn host_guard(State(guard): State<HostGuard>, req: Request, next: Next) -> Response {
    match check_host(&guard, &req).and_then(|()| check_origin(req.headers())) {
        Ok(()) => next.run(req).await,
        Err(e) => e.into_response(),
    }
}

/// Middleware: the Origin check only (for `/mcp`, which checks `Host` itself).
pub async fn origin_guard(req: Request, next: Next) -> Response {
    match check_origin(req.headers()) {
        Ok(()) => next.run(req).await,
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn local_hosts() {
        let g = HostGuard::local();
        for ok in [
            "localhost",
            "LOCALHOST:7400",
            "127.0.0.1",
            "127.0.0.1:7400",
            "[::1]",
            "[::1]:7400",
        ] {
            assert!(g.allows(ok), "{ok}");
        }
        for bad in [
            "",
            "evil.example",
            "evil.example:7400",
            "localhost.evil.example",
            "127.0.0.2",
            "::1",
            "[::1",
            "[::1]x",
            "[::1]:",
            "127.0.0.1:",
            "127.0.0.1:x",
            "127.0.0.1:1:2",
            "10.99.0.1:7401",
            "[localhost]",
        ] {
            assert!(!g.allows(bad), "{bad}");
        }
    }

    #[test]
    fn peer_hosts_add_the_listener_ip() {
        let g = HostGuard::peer("10.99.0.1".parse().unwrap());
        assert!(g.allows("10.99.0.1"));
        assert!(g.allows("10.99.0.1:7401"));
        assert!(g.allows("localhost:7401"));
        assert!(!g.allows("10.99.0.2:7401"));
        let g6 = HostGuard::peer("fd00::1".parse().unwrap());
        assert!(g6.allows("[fd00::1]:7401"));
        assert!(!g6.allows("fd00::1"));
    }

    #[test]
    fn origin_and_fetch_site() {
        let mut h = HeaderMap::new();
        assert!(check_origin(&h).is_ok());
        h.insert("sec-fetch-site", HeaderValue::from_static("none"));
        assert!(check_origin(&h).is_ok());
        for site in ["same-origin", "same-site", "cross-site", ""] {
            h.insert("sec-fetch-site", HeaderValue::from_str(site).unwrap());
            assert!(check_origin(&h).is_err(), "{site}");
        }
        let mut h = HeaderMap::new();
        h.insert(header::ORIGIN, HeaderValue::from_static("null"));
        assert!(matches!(check_origin(&h), Err(ServiceError::Forbidden(_))));
    }

    #[test]
    fn missing_host_is_refused() {
        let req = Request::builder()
            .uri("/x")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(check_host(&HostGuard::local(), &req).is_err());
        let req = Request::builder()
            .uri("http://127.0.0.1:7400/x")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(check_host(&HostGuard::local(), &req).is_ok());
    }
}
