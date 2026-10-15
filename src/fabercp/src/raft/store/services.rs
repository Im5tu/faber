//! Service virtual IPs: one address of the mesh's service range per workload.
//!
//! A service is a workload, named by (project, environment, workload). It
//! gets an address from `MeshConfig::service_cidr` when the planner first
//! sees the workload ([`crate::placement`] proposes `AllocateServiceIp`), keeps
//! it across revisions, and gives it back when the workload is gone
//! (`ReleaseServiceIp`). Nodes program the address into nftables and DNS
//! answers with it, so it must not change while the workload exists.
//!
//! All addresses are in one record under [`KEY_IPS`] (the allocation of one
//! entry has to see the others, including those of earlier entries in the
//! same apply batch), reserved so that `Request::Set` cannot write it.
//!
//! Apply is deterministic: it reads the mesh, the environment record and the
//! record, and nothing else. Addresses are handed out in a ring. The cursor
//! moves past each address given out, so an address that was released is not
//! reused until the ring has gone round, which keeps a stale DNS answer or
//! connection from reaching an unrelated service.
//!
//! Both commands are safe against a leader whose view is old: `Allocate` is
//! refused (`not_found`) for a workload the environment does not have and
//! changes nothing for one that has an address; `Release` changes nothing for
//! a workload that still exists.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use faber_api_types::error_code;
use faber_api_types::name::validate_dns_label;
use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

use crate::apps::EnvironmentRecord;
use crate::mesh::{MeshState, Rejection};

/// The record of every service address. `Request::Set` refuses it.
pub const KEY_IPS: &str = "services/ips";
/// Prefix of the keys of this module, all reserved.
pub const KEY_PREFIX: &str = "services/";

/// Whether only the typed commands of this module may write `key`.
pub fn is_reserved_key(key: &str) -> bool {
    key.starts_with(KEY_PREFIX)
}

/// Reserves a service IP for one workload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocateServiceIp {
    pub project: String,
    pub environment: String,
    pub workload: String,
}

/// Frees the service IP of one workload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReleaseServiceIp {
    pub project: String,
    pub environment: String,
    pub workload: String,
}

/// The key of a service in [`ServiceIps`]: `<project>/<environment>/<workload>`.
/// The names are DNS labels, so the separator cannot be part of one.
pub fn service_key(project: &str, environment: &str, workload: &str) -> String {
    format!("{project}/{environment}/{workload}")
}

/// Splits a key made by [`service_key`].
pub fn split_service_key(key: &str) -> Option<(&str, &str, &str)> {
    let (project, rest) = key.split_once('/')?;
    let (environment, workload) = rest.split_once('/')?;
    (!workload.contains('/')).then_some((project, environment, workload))
}

/// The addresses handed out, under [`KEY_IPS`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceIps {
    /// By [`service_key`].
    pub ips: BTreeMap<String, Ipv4Addr>,
    /// Where the next search starts: an offset from the first host address
    /// of the range.
    #[serde(default)]
    pub cursor: u32,
}

/// How many host addresses `range` holds (without network and broadcast).
fn capacity(range: Ipv4Net) -> u64 {
    (1u64 << (32 - u32::from(range.prefix_len()))).saturating_sub(2)
}

impl ServiceIps {
    /// The address of a service.
    pub fn get(&self, project: &str, environment: &str, workload: &str) -> Option<Ipv4Addr> {
        self.ips
            .get(&service_key(project, environment, workload))
            .copied()
    }

    /// How many addresses of `range` no service holds.
    pub fn free(&self, range: Ipv4Net) -> u64 {
        let held = self.ips.values().filter(|ip| range.contains(*ip)).count();
        capacity(range).saturating_sub(held as u64)
    }

    /// Gives `key` the next free address of `range` after the cursor, or
    /// returns the one it already has. `None` when every address is held.
    pub fn allocate(&mut self, range: Ipv4Net, key: &str) -> Option<Ipv4Addr> {
        if let Some(ip) = self.ips.get(key) {
            return Some(*ip);
        }
        let total = capacity(range);
        if total == 0 {
            return None;
        }
        let held: BTreeSet<Ipv4Addr> = self.ips.values().copied().collect();
        let first = u64::from(u32::from(range.network())) + 1;
        let start = u64::from(self.cursor) % total;
        for step in 0..total {
            let offset = (start + step) % total;
            // At most `u32::MAX`: the range is an IPv4 network.
            let ip = Ipv4Addr::from(u32::try_from(first + offset).ok()?);
            if !held.contains(&ip) {
                self.cursor = u32::try_from((offset + 1) % total).ok()?;
                self.ips.insert(key.to_string(), ip);
                return Some(ip);
            }
        }
        None
    }

    /// Takes the address of `key` back. The cursor stays.
    pub fn release(&mut self, key: &str) -> Option<Ipv4Addr> {
        self.ips.remove(key)
    }
}

fn bad_request(message: impl Into<String>) -> Rejection {
    Rejection::new(error_code::BAD_REQUEST, message)
}

fn check_names(project: &str, environment: &str, workload: &str) -> Result<(), Rejection> {
    for (what, name) in [
        ("project", project),
        ("environment", environment),
        ("workload", workload),
    ] {
        validate_dns_label(name).map_err(|e| bad_request(format!("{what}: {e}")))?;
    }
    Ok(())
}

/// `AllocateServiceIp` against the mesh, the environment and the record as
/// the entry sees them. The record to store, or `None` when nothing changes
/// (the workload already has an address). Checked in this order:
///
/// - A name that is no DNS label: `bad_request`.
/// - No mesh: `mesh_not_configured`. A mesh without a service range:
///   `bad_request`.
/// - The environment or the workload does not exist: `not_found`.
/// - Every address of the range is held: `mesh_exhausted`.
pub fn apply_allocate_service_ip(
    mesh: Option<&MeshState>,
    environment: Option<&EnvironmentRecord>,
    stored: &ServiceIps,
    cmd: AllocateServiceIp,
) -> Result<Option<ServiceIps>, Rejection> {
    check_names(&cmd.project, &cmd.environment, &cmd.workload)?;
    let mesh = mesh.ok_or_else(|| {
        Rejection::new(error_code::MESH_NOT_CONFIGURED, "the cluster has no mesh")
    })?;
    let range = mesh
        .config
        .service_cidr
        .ok_or_else(|| bad_request("the mesh has no service range"))?;
    if !environment.is_some_and(|e| e.workloads.contains_key(&cmd.workload)) {
        return Err(Rejection::new(
            error_code::NOT_FOUND,
            format!(
                "workload {:?} of environment {:?} of project {:?} does not exist",
                cmd.workload, cmd.environment, cmd.project
            ),
        ));
    }
    let key = service_key(&cmd.project, &cmd.environment, &cmd.workload);
    if stored.ips.contains_key(&key) {
        return Ok(None);
    }
    let mut next = stored.clone();
    next.allocate(range, &key).ok_or_else(|| {
        Rejection::new(
            error_code::MESH_EXHAUSTED,
            format!("no free address in the service range {range}"),
        )
    })?;
    Ok(Some(next))
}

/// `ReleaseServiceIp` against the environment and the record as the entry
/// sees them. The record to store, or `None` when nothing changes: the
/// workload still exists (the leader's view was old), or it has no address.
/// A name that is no DNS label: `bad_request`.
pub fn apply_release_service_ip(
    environment: Option<&EnvironmentRecord>,
    stored: &ServiceIps,
    cmd: ReleaseServiceIp,
) -> Result<Option<ServiceIps>, Rejection> {
    check_names(&cmd.project, &cmd.environment, &cmd.workload)?;
    if environment.is_some_and(|e| e.workloads.contains_key(&cmd.workload)) {
        return Ok(None);
    }
    let key = service_key(&cmd.project, &cmd.environment, &cmd.workload);
    let mut next = stored.clone();
    Ok(next.release(&key).map(|_| next))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    #[test]
    fn addresses_are_handed_out_in_order_from_the_first_host() {
        let mut ips = ServiceIps::default();
        let r = range("10.201.0.0/16");
        assert_eq!(ips.allocate(r, "a/b/web"), Some(ip("10.201.0.1")));
        assert_eq!(ips.allocate(r, "a/b/db"), Some(ip("10.201.0.2")));
        assert_eq!(ips.allocate(r, "a/c/web"), Some(ip("10.201.0.3")));
    }

    #[test]
    fn a_service_keeps_its_address() {
        let mut ips = ServiceIps::default();
        let r = range("10.201.0.0/16");
        let first = ips.allocate(r, "a/b/web");
        ips.allocate(r, "a/b/db");
        assert_eq!(ips.allocate(r, "a/b/web"), first);
        assert_eq!(ips.ips.len(), 2);
    }

    #[test]
    fn a_held_address_is_never_given_twice() {
        let mut ips = ServiceIps::default();
        let r = range("10.201.0.0/28");
        let mut seen = BTreeSet::new();
        for n in 0..14 {
            let ip = ips.allocate(r, &format!("p/e/w{n}")).unwrap();
            assert!(r.contains(&ip) && ip != r.network() && ip != r.broadcast());
            assert!(seen.insert(ip), "{ip} twice");
        }
        assert_eq!(ips.allocate(r, "p/e/more"), None);
        assert_eq!(ips.free(r), 0);
    }

    #[test]
    fn a_released_address_is_not_reused_until_the_ring_has_gone_round() {
        let mut ips = ServiceIps::default();
        let r = range("10.201.0.0/29");
        for n in 0..3 {
            ips.allocate(r, &format!("p/e/w{n}"));
        }
        assert_eq!(ips.release("p/e/w0"), Some(ip("10.201.0.1")));
        assert_eq!(ips.free(r), 4);
        // 4, 5 and 6 are free ahead of the cursor, so .1 waits.
        let next: Vec<_> = (0..3)
            .map(|n| ips.allocate(r, &format!("p/e/n{n}")).unwrap())
            .collect();
        assert_eq!(next, [ip("10.201.0.4"), ip("10.201.0.5"), ip("10.201.0.6")]);
        // Now only .1 is free.
        assert_eq!(ips.allocate(r, "p/e/last"), Some(ip("10.201.0.1")));
        assert_eq!(ips.allocate(r, "p/e/none"), None);
    }

    #[test]
    fn a_range_of_two_hosts_is_exhausted_by_two_services() {
        let mut ips = ServiceIps::default();
        let r = range("10.201.0.0/30");
        assert_eq!(ips.allocate(r, "p/e/a"), Some(ip("10.201.0.1")));
        assert_eq!(ips.allocate(r, "p/e/b"), Some(ip("10.201.0.2")));
        assert_eq!(ips.allocate(r, "p/e/c"), None);
        // A failed allocation changes nothing.
        assert_eq!(ips.ips.len(), 2);
    }

    #[test]
    fn service_keys_split_back() {
        let key = service_key("shop", "staging", "web");
        assert_eq!(split_service_key(&key), Some(("shop", "staging", "web")));
        assert_eq!(split_service_key("shop/staging"), None);
        assert_eq!(split_service_key("a/b/c/d"), None);
    }

    #[test]
    fn the_record_round_trips() {
        let mut ips = ServiceIps::default();
        ips.allocate(range("10.201.0.0/16"), "p/e/w");
        let json = serde_json::to_string(&ips).unwrap();
        assert_eq!(serde_json::from_str::<ServiceIps>(&json).unwrap(), ips);
    }

    #[test]
    fn reserved_keys() {
        assert!(is_reserved_key(KEY_IPS));
        assert!(is_reserved_key("services/anything"));
        assert!(!is_reserved_key("service"));
    }
}
