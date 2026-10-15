//! Cluster DNS for service names.
//!
//! `<svc>.<env>.<project>.faber.local` answers with the service's virtual IP;
//! any other name under `faber.local` is NXDOMAIN; every other name goes to
//! the host's upstream resolvers. The server listens on [`DEFAULT_PORT`] at
//! the node's bridge gateway, and nftables redirects port 53 there.

mod answer;
mod server;
mod upstream;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ipnet::Ipv4Net;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use server::{DnsServer, bind};
pub use upstream::{parse_resolv_conf, system_upstreams};

/// Port the server listens on. It is above 1023 so fabercp needs no extra
/// capability; nftables redirects port 53 on the bridge gateway to it.
pub const DEFAULT_PORT: u16 = 5380;

/// How long one upstream resolver gets to answer before the next is tried.
pub const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(2);

/// The virtual IP of a service. It never changes for a service, so clients
/// can cache the answer.
pub trait ServiceIps: Send + Sync + 'static {
    /// The virtual IP of service `svc` in environment `env` of `project`,
    /// all lowercase DNS labels. `None` when there is no such service.
    fn lookup(&self, project: &str, env: &str, svc: &str) -> Option<Ipv4Addr>;
}

/// Placeholder until the replicated store backs [`ServiceIps`]: no service
/// has an address, so every service name is NXDOMAIN.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoServiceIps;

impl ServiceIps for NoServiceIps {
    fn lookup(&self, _project: &str, _env: &str, _svc: &str) -> Option<Ipv4Addr> {
        None
    }
}

/// A [`ServiceIps`] held in memory, for tests.
#[derive(Debug, Default)]
pub struct InMemoryServiceIps {
    ips: Mutex<HashMap<(String, String, String), Ipv4Addr>>,
}

impl InMemoryServiceIps {
    /// Gives service `svc` in `env` of `project` the address `ip`.
    pub fn set(&self, project: &str, env: &str, svc: &str, ip: Ipv4Addr) {
        self.ips
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((project.into(), env.into(), svc.into()), ip);
    }
}

impl ServiceIps for InMemoryServiceIps {
    fn lookup(&self, project: &str, env: &str, svc: &str) -> Option<Ipv4Addr> {
        self.ips
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(project.into(), env.into(), svc.into()))
            .copied()
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

/// Runs the DNS server of a node until `cancel` fires. A bind failure is
/// logged and leaves the node without DNS, not down.
///
/// `listen` is the node's bridge gateway at [`DEFAULT_PORT`]. It is `None`
/// until the caller can work that address out, and the task then only waits
/// for `cancel`.
pub fn spawn(
    listen: Option<SocketAddr>,
    ips: Arc<dyn ServiceIps>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(listen) = listen else {
            tracing::info!("no bridge gateway address for DNS yet, not listening");
            cancel.cancelled().await;
            return;
        };
        let config = DnsConfig {
            listen,
            upstreams: system_upstreams(Some(listen.ip())),
            upstream_timeout: UPSTREAM_TIMEOUT,
        };
        match DnsServer::start(config, ips, cancel) {
            Ok(server) => server.wait().await,
            Err(e) => tracing::warn!(%listen, error = %e, "cannot start DNS"),
        }
    })
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
