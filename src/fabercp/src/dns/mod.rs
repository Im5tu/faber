//! Cluster DNS for service names.
//!
//! `<svc>.<env>.<project>.faber.local` answers with the service's virtual IP;
//! any other name under `faber.local` is NXDOMAIN; every other name goes to
//! the host's upstream resolvers. The server listens on [`DEFAULT_PORT`] at
//! the node's bridge gateway, and nftables redirects port 53 there.
//! [`spawn_node`] runs it against the replicated state.

mod answer;
mod node;
mod server;
mod upstream;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use ipnet::Ipv4Net;

pub use node::{StateServiceIps, spawn_node};
pub use server::{DnsServer, bind};
pub use upstream::{parse_resolv_conf, system_upstreams};

/// Port the server listens on: the one nftables redirects port 53 on the
/// bridge gateway to. It is above 1023 so fabercp needs no extra capability.
pub use faber_network_management::DNS_REDIRECT_PORT as DEFAULT_PORT;

/// How long a forwarded query may take in all, whatever the number of
/// upstream resolvers.
pub const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(2);

/// The store behind [`ServiceIps`] could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("service address lookup failed")]
pub struct LookupFailed;

/// The virtual IP of a service. It never changes for a service, so clients
/// can cache the answer.
pub trait ServiceIps: Send + Sync + 'static {
    /// The virtual IP of service `svc` in environment `env` of `project`,
    /// all lowercase DNS labels. `Ok(None)` when there is no such service or
    /// it has no address yet; `Err` when that cannot be told, which clients
    /// get as SERVFAIL, not as a missing name.
    fn lookup(&self, project: &str, env: &str, svc: &str)
    -> Result<Option<Ipv4Addr>, LookupFailed>;
}

/// A [`ServiceIps`] held in memory, for tests.
#[derive(Debug, Default)]
pub struct InMemoryServiceIps {
    ips: Mutex<HashMap<(String, String, String), Ipv4Addr>>,
    failing: AtomicBool,
}

impl InMemoryServiceIps {
    /// Gives service `svc` in `env` of `project` the address `ip`.
    pub fn set(&self, project: &str, env: &str, svc: &str, ip: Ipv4Addr) {
        self.ips
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((project.into(), env.into(), svc.into()), ip);
    }

    /// Makes every lookup fail, as an unreadable store would.
    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::Relaxed);
    }
}

impl ServiceIps for InMemoryServiceIps {
    fn lookup(
        &self,
        project: &str,
        env: &str,
        svc: &str,
    ) -> Result<Option<Ipv4Addr>, LookupFailed> {
        if self.failing.load(Ordering::Relaxed) {
            return Err(LookupFailed);
        }
        Ok(self
            .ips
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(project.into(), env.into(), svc.into()))
            .copied())
    }
}

/// What the server needs to run.
#[derive(Debug, Clone)]
pub struct DnsConfig {
    /// Where to listen, over both UDP and TCP.
    pub listen: SocketAddr,
    /// Resolvers for names outside `faber.local`, tried in order.
    pub upstreams: Vec<SocketAddr>,
    /// How long each upstream gets to answer.
    pub upstream_timeout: Duration,
}

/// Where the node's DNS listens: the bridge gateway, which is the first host
/// of the node's container `subnet`, at [`DEFAULT_PORT`].
pub fn listen_addr(subnet: Ipv4Net) -> SocketAddr {
    let gateway = Ipv4Addr::from(u32::from(subnet.network()).saturating_add(1));
    SocketAddr::new(gateway.into(), DEFAULT_PORT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_listens_on_the_first_host_of_the_container_subnet() {
        let subnet: Ipv4Net = "10.200.3.0/24".parse().unwrap();
        assert_eq!(
            listen_addr(subnet),
            SocketAddr::from((Ipv4Addr::new(10, 200, 3, 1), DEFAULT_PORT))
        );
    }
}
