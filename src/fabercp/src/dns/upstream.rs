//! The host's upstream resolvers: finding them and relaying queries to them.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::{Instant, timeout, timeout_at};

/// Port a resolver listens on when `resolv.conf` does not say.
const DNS_PORT: u16 = 53;

/// Largest DNS message: the TCP length prefix is 16 bits.
const MAX_MESSAGE: usize = u16::MAX as usize;

const HEADER_LEN: usize = 12;

/// `resolv.conf` of the host.
const RESOLV_CONF: &str = "/etc/resolv.conf";

/// Where systemd-resolved lists the real resolvers, since `/etc/resolv.conf`
/// then names its loopback stub.
const SYSTEMD_RESOLV_CONF: &str = "/run/systemd/resolve/resolv.conf";

/// The upstream resolvers named by the `nameserver` lines of `text`, in
/// order. Loopback and unspecified addresses are skipped, since they would
/// loop back to this node, and so is `bridge`, the address this server
/// answers on. Repeats are dropped.
pub fn parse_resolv_conf(text: &str, bridge: Option<IpAddr>) -> Vec<SocketAddr> {
    let mut found: Vec<SocketAddr> = Vec::new();
    for line in text.lines() {
        let mut words = line.split_whitespace();
        if words.next() != Some("nameserver") {
            continue;
        }
        // A scoped address (`fe80::1%eth0`) does not parse and is skipped.
        let Some(ip) = words.next().and_then(|w| w.parse::<IpAddr>().ok()) else {
            continue;
        };
        if ip.is_loopback() || ip.is_unspecified() || Some(ip) == bridge {
            continue;
        }
        let addr = SocketAddr::new(ip, DNS_PORT);
        if !found.contains(&addr) {
            found.push(addr);
        }
    }
    found
}

/// The upstream resolvers of this host: `/etc/resolv.conf`, or when that
/// leaves none (a loopback stub) the file systemd-resolved keeps. Empty when
/// neither can be read or names a usable resolver.
pub fn system_upstreams(bridge: Option<IpAddr>) -> Vec<SocketAddr> {
    upstreams_from(
        &[Path::new(RESOLV_CONF), Path::new(SYSTEMD_RESOLV_CONF)],
        bridge,
    )
}

/// The resolvers of the first file in `paths` that names any.
fn upstreams_from(paths: &[&Path], bridge: Option<IpAddr>) -> Vec<SocketAddr> {
    paths
        .iter()
        .map(|path| read_upstreams(path, bridge))
        .find(|found| !found.is_empty())
        .unwrap_or_default()
}

fn read_upstreams(path: &Path, bridge: Option<IpAddr>) -> Vec<SocketAddr> {
    match std::fs::read_to_string(path) {
        Ok(text) => parse_resolv_conf(&text, bridge),
        Err(e) => {
            if e.kind() != io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %e, "cannot read resolv.conf");
            }
            Vec::new()
        }
    }
}

/// Relays `query` to each upstream in turn over UDP, `wait` for each, and
/// returns the first answer. `None` when none answers.
pub async fn forward_udp(
    query: &[u8],
    upstreams: &[SocketAddr],
    wait: Duration,
) -> Option<Vec<u8>> {
    for &upstream in upstreams {
        match ask_udp(query, upstream, wait).await {
            Ok(Some(response)) => return Some(response),
            Ok(None) => tracing::debug!(%upstream, "upstream resolver timed out"),
            Err(e) => tracing::debug!(%upstream, error = %e, "upstream resolver failed"),
        }
    }
    None
}

/// Like [`forward_udp`] but over TCP, for clients that asked over TCP.
pub async fn forward_tcp(
    query: &[u8],
    upstreams: &[SocketAddr],
    wait: Duration,
) -> Option<Vec<u8>> {
    for &upstream in upstreams {
        match timeout(wait, ask_tcp(query, upstream)).await {
            Ok(Ok(response)) => return Some(response),
            Ok(Err(e)) => tracing::debug!(%upstream, error = %e, "upstream resolver failed"),
            Err(_) => tracing::debug!(%upstream, "upstream resolver timed out"),
        }
    }
    None
}

async fn ask_udp(
    query: &[u8],
    upstream: SocketAddr,
    wait: Duration,
) -> io::Result<Option<Vec<u8>>> {
    let local: IpAddr = match upstream {
        SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    };
    // A fresh socket gets a fresh random source port, and `connect` drops
    // datagrams from anyone but the upstream.
    let socket = UdpSocket::bind((local, 0)).await?;
    socket.connect(upstream).await?;
    socket.send(query).await?;
    let id = query.get(..2);
    let deadline = Instant::now() + wait;
    let mut buf = vec![0u8; MAX_MESSAGE];
    loop {
        let Ok(n) = timeout_at(deadline, socket.recv(&mut buf)).await else {
            return Ok(None);
        };
        let response = &buf[..n?];
        // Skip a stray datagram: too short, not a response, or for another query.
        let is_response = response.len() >= HEADER_LEN && response[2] & 0x80 != 0;
        if is_response && response.get(..2) == id {
            return Ok(Some(response.to_vec()));
        }
    }
}

async fn ask_tcp(query: &[u8], upstream: SocketAddr) -> io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(upstream).await?;
    let len = u16::try_from(query.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
    let mut framed = Vec::with_capacity(query.len() + 2);
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(query);
    stream.write_all(&framed).await?;
    let mut len = [0u8; 2];
    stream.read_exact(&mut len).await?;
    let mut response = vec![0u8; usize::from(u16::from_be_bytes(len))];
    stream.read_exact(&mut response).await?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn filter_skips_loopback_bridge_and_repeats() {
        let text = "\
# comment
search example.com
nameserver 127.0.0.53
nameserver ::1
nameserver 0.0.0.0
nameserver 10.200.0.1
nameserver 192.168.1.1
nameserver 192.168.1.1
nameserver fe80::1%eth0
nameserver 2001:db8::53
nameserver
options edns0
";
        let bridge: IpAddr = "10.200.0.1".parse().unwrap();
        let found = parse_resolv_conf(text, Some(bridge));
        assert_eq!(
            found,
            vec![
                "192.168.1.1:53".parse::<SocketAddr>().unwrap(),
                "[2001:db8::53]:53".parse().unwrap(),
            ]
        );
        // Without a bridge address the bridge is just another resolver.
        assert!(parse_resolv_conf(text, None).contains(&"10.200.0.1:53".parse().unwrap()));
    }

    #[test]
    fn dns_upstreams_fall_back_past_a_loopback_stub() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("stub");
        let real = dir.path().join("real");
        let missing = dir.path().join("missing");
        std::fs::write(&stub, "nameserver 127.0.0.53\n").unwrap();
        std::fs::write(&real, "nameserver 192.168.1.1\n").unwrap();
        let expected: Vec<SocketAddr> = vec!["192.168.1.1:53".parse().unwrap()];
        assert_eq!(upstreams_from(&[&stub, &real], None), expected);
        assert_eq!(upstreams_from(&[&missing, &real], None), expected);
        assert!(upstreams_from(&[&stub, &missing], None).is_empty());
    }

    #[tokio::test]
    async fn udp_answer_is_relayed_and_stray_datagrams_are_ignored() {
        let fake = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = fake.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let (n, from) = fake.recv_from(&mut buf).await.unwrap();
            let mut response = buf[..n].to_vec();
            // A datagram for another id first, then the real answer.
            let mut stray = response.clone();
            stray[0] ^= 0xff;
            stray[2] |= 0x80;
            fake.send_to(&stray, from).await.unwrap();
            response[2] |= 0x80;
            response.push(0xab);
            fake.send_to(&response, from).await.unwrap();
        });
        let query = [0x12, 0x34, 0x01, 0x00, 0, 0, 0, 0, 0, 0, 0, 0];
        let got = forward_udp(&query, &[addr], Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(&got[..2], &[0x12, 0x34]);
        assert_eq!(got.last(), Some(&0xab));
    }

    #[tokio::test]
    async fn silent_upstream_is_skipped_then_none() {
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = silent.local_addr().unwrap();
        let query = [0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(
            forward_udp(&query, &[addr], Duration::from_millis(100))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn tcp_answer_is_relayed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut len = [0u8; 2];
            s.read_exact(&mut len).await.unwrap();
            let mut q = vec![0u8; usize::from(u16::from_be_bytes(len))];
            s.read_exact(&mut q).await.unwrap();
            q[2] |= 0x80;
            s.write_all(&len).await.unwrap();
            s.write_all(&q).await.unwrap();
        });
        let query = [0, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let got = forward_tcp(&query, &[addr], Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(got, [0, 5, 0x81, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }
}
