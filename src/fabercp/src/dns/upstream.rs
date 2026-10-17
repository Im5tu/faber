//! The host's upstream resolvers: finding them and relaying queries to them.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use hickory_proto::op::{Message, MessageType};
use ipnet::Ipv4Net;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::{Instant, timeout_at};

/// Port a resolver listens on when `resolv.conf` does not say.
const DNS_PORT: u16 = 53;

/// Largest DNS message: the TCP length prefix is 16 bits.
const MAX_MESSAGE: usize = u16::MAX as usize;

/// Longest one upstream gets to answer, so a dead first resolver leaves time
/// for the next within the total.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);

/// `resolv.conf` of the host.
const RESOLV_CONF: &str = "/etc/resolv.conf";

/// Where systemd-resolved lists the real resolvers, since `/etc/resolv.conf`
/// then names its loopback stub.
const SYSTEMD_RESOLV_CONF: &str = "/run/systemd/resolve/resolv.conf";

/// The upstream resolvers named by the `nameserver` lines of `text`, in
/// order. Loopback and unspecified addresses are skipped, since they would
/// loop back to this node, and so is `bridge`, the address this server
/// answers on, and any address inside `excluded` (the container and service
/// ranges, where a resolver would be a workload or a virtual IP). Repeats
/// are dropped.
pub fn parse_resolv_conf(
    text: &str,
    bridge: Option<IpAddr>,
    excluded: &[Ipv4Net],
) -> Vec<SocketAddr> {
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
        let in_excluded = match ip {
            IpAddr::V4(v4) => excluded.iter().any(|net| net.contains(&v4)),
            IpAddr::V6(_) => false,
        };
        if ip.is_loopback() || ip.is_unspecified() || Some(ip) == bridge || in_excluded {
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
pub fn system_upstreams(bridge: Option<IpAddr>, excluded: &[Ipv4Net]) -> Vec<SocketAddr> {
    upstreams_from(
        &[Path::new(RESOLV_CONF), Path::new(SYSTEMD_RESOLV_CONF)],
        bridge,
        excluded,
    )
}

/// The resolvers of the first file in `paths` that names any.
fn upstreams_from(
    paths: &[&Path],
    bridge: Option<IpAddr>,
    excluded: &[Ipv4Net],
) -> Vec<SocketAddr> {
    paths
        .iter()
        .map(|path| read_upstreams(path, bridge, excluded))
        .find(|found| !found.is_empty())
        .unwrap_or_default()
}

fn read_upstreams(path: &Path, bridge: Option<IpAddr>, excluded: &[Ipv4Net]) -> Vec<SocketAddr> {
    match std::fs::read_to_string(path) {
        Ok(text) => parse_resolv_conf(&text, bridge, excluded),
        Err(e) => {
            if e.kind() != io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %e, "cannot read resolv.conf");
            }
            Vec::new()
        }
    }
}

/// Relays `query` to each upstream in turn over UDP and returns the first
/// answer. All of it, however many upstreams, takes at most `total`; one
/// upstream gets at most [`ATTEMPT_TIMEOUT`] of that. `None` when none
/// answers.
pub async fn forward_udp(
    query: &[u8],
    upstreams: &[SocketAddr],
    total: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + total;
    for &upstream in upstreams {
        let attempt = attempt_deadline(deadline)?;
        match ask_udp(query, upstream, attempt).await {
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
    total: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + total;
    for &upstream in upstreams {
        let attempt = attempt_deadline(deadline)?;
        match timeout_at(attempt, ask_tcp(query, upstream)).await {
            Ok(Ok(response)) => return Some(response),
            Ok(Err(e)) => tracing::debug!(%upstream, error = %e, "upstream resolver failed"),
            Err(_) => tracing::debug!(%upstream, "upstream resolver timed out"),
        }
    }
    None
}

/// When the next attempt must end: [`ATTEMPT_TIMEOUT`] from now, but not
/// past `deadline`. `None` once `deadline` has passed.
fn attempt_deadline(deadline: Instant) -> Option<Instant> {
    let now = Instant::now();
    (now < deadline).then(|| (now + ATTEMPT_TIMEOUT).min(deadline))
}

/// Whether `response` answers `query`: a response with the same id and the
/// same question section. Anything else is not trusted.
fn answers(query: &Message, response: &[u8]) -> bool {
    Message::from_vec(response).is_ok_and(|r| {
        r.metadata.message_type == MessageType::Response
            && r.metadata.id == query.metadata.id
            && r.queries == query.queries
    })
}

async fn ask_udp(
    query: &[u8],
    upstream: SocketAddr,
    deadline: Instant,
) -> io::Result<Option<Vec<u8>>> {
    let request = Message::from_vec(query).map_err(|_| io::ErrorKind::InvalidInput)?;
    let local: IpAddr = match upstream {
        SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    };
    // A fresh socket gets a fresh random source port, and `connect` drops
    // datagrams from anyone but the upstream.
    let socket = UdpSocket::bind((local, 0)).await?;
    socket.connect(upstream).await?;
    socket.send(query).await?;
    let mut buf = vec![0u8; MAX_MESSAGE];
    loop {
        let Ok(n) = timeout_at(deadline, socket.recv(&mut buf)).await else {
            return Ok(None);
        };
        let response = &buf[..n?];
        // Skip a stray datagram: not an answer to this query.
        if answers(&request, response) {
            return Ok(Some(response.to_vec()));
        }
    }
}

async fn ask_tcp(query: &[u8], upstream: SocketAddr) -> io::Result<Vec<u8>> {
    let request = Message::from_vec(query).map_err(|_| io::ErrorKind::InvalidInput)?;
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
    if !answers(&request, &response) {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

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
        let found = parse_resolv_conf(text, Some(bridge), &[]);
        assert_eq!(
            found,
            vec![
                "192.168.1.1:53".parse::<SocketAddr>().unwrap(),
                "[2001:db8::53]:53".parse().unwrap(),
            ]
        );
        // Without a bridge address the bridge is just another resolver.
        assert!(parse_resolv_conf(text, None, &[]).contains(&"10.200.0.1:53".parse().unwrap()));
    }

    #[test]
    fn dns_filter_skips_the_container_and_service_ranges() {
        let text = "nameserver 10.200.5.9\nnameserver 10.201.0.2\nnameserver 10.202.0.1\n";
        let nets: Vec<Ipv4Net> = vec![
            "10.200.0.0/16".parse().unwrap(),
            "10.201.0.0/16".parse().unwrap(),
        ];
        assert_eq!(
            parse_resolv_conf(text, None, &nets),
            vec!["10.202.0.1:53".parse::<SocketAddr>().unwrap()]
        );
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
        assert_eq!(upstreams_from(&[&stub, &real], None, &[]), expected);
        assert_eq!(upstreams_from(&[&missing, &real], None, &[]), expected);
        assert!(upstreams_from(&[&stub, &missing], None, &[]).is_empty());
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
            // Recursion available marks the real answer.
            response[3] |= 0x80;
            fake.send_to(&response, from).await.unwrap();
        });
        let query = [0x12, 0x34, 0x01, 0x00, 0, 0, 0, 0, 0, 0, 0, 0];
        let got = forward_udp(&query, &[addr], Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(&got[..2], &[0x12, 0x34]);
        assert_eq!(got[3] & 0x80, 0x80);
    }

    fn query_for(name: &str, id: u16) -> Vec<u8> {
        let mut m = Message::query();
        m.metadata.id = id;
        m.add_query(hickory_proto::op::Query::query(
            hickory_proto::rr::Name::from_ascii(name).unwrap(),
            hickory_proto::rr::RecordType::A,
        ));
        m.to_vec().unwrap()
    }

    /// `query` as the response to a query for `name` with the same id.
    fn response_for(name: &str, id: u16, flag: u8) -> Vec<u8> {
        let mut m = Message::from_vec(&query_for(name, id)).unwrap();
        m.metadata.message_type = MessageType::Response;
        let mut bytes = m.to_vec().unwrap();
        bytes[3] |= flag;
        bytes
    }

    #[tokio::test]
    async fn dns_reply_for_another_question_is_ignored() {
        let fake = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = fake.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let (_, from) = fake.recv_from(&mut buf).await.unwrap();
            // Right id, wrong question first; then the real answer.
            fake.send_to(&response_for("evil.example.", 77, 0), from)
                .await
                .unwrap();
            fake.send_to(&response_for("example.com.", 77, 0x80), from)
                .await
                .unwrap();
        });
        let query = query_for("example.com.", 77);
        let got = forward_udp(&query, &[addr], Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(got[3] & 0x80, 0x80);
    }

    #[tokio::test]
    async fn dns_tcp_reply_with_the_wrong_id_is_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut len = [0u8; 2];
            s.read_exact(&mut len).await.unwrap();
            let mut q = vec![0u8; usize::from(u16::from_be_bytes(len))];
            s.read_exact(&mut q).await.unwrap();
            let wrong = response_for("example.com.", 99, 0);
            s.write_all(&(wrong.len() as u16).to_be_bytes())
                .await
                .unwrap();
            s.write_all(&wrong).await.unwrap();
        });
        let query = query_for("example.com.", 5);
        assert!(
            forward_tcp(&query, &[addr], Duration::from_secs(2))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn dns_all_upstreams_share_one_deadline() {
        let silent: Vec<UdpSocket> = vec![
            UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            UdpSocket::bind("127.0.0.1:0").await.unwrap(),
        ];
        let addrs: Vec<SocketAddr> = silent.iter().map(|s| s.local_addr().unwrap()).collect();
        let started = Instant::now();
        let got = forward_udp(
            &query_for("example.com.", 1),
            &addrs,
            Duration::from_millis(300),
        )
        .await;
        assert!(got.is_none());
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "{:?}",
            started.elapsed()
        );
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
