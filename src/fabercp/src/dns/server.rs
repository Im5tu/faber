//! The UDP and TCP listeners and the per-query handling.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hickory_proto::op::ResponseCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::answer::{Outcome, answer, error_reply, fit_udp};
use super::upstream::{forward_tcp, forward_udp};
use super::{DnsConfig, ServiceIps};

/// Forwarded UDP queries in flight at once; more are dropped, so a flood
/// cannot grow the task count without bound. Names under `faber.local` are
/// answered without a permit.
const MAX_UDP_FORWARDS: usize = 512;

/// TCP connections open at once; more are refused. A pool of its own, so a
/// flood on one transport does not starve the other.
const MAX_TCP_CONNECTIONS: usize = 256;

/// How long a TCP client may stay silent before its connection is closed.
const TCP_IDLE: Duration = Duration::from_secs(10);

/// Longest a TCP connection lives, however busy.
const TCP_LIFETIME: Duration = Duration::from_secs(30);

/// Queries answered on one TCP connection before it is closed.
const TCP_MAX_QUERIES: usize = 100;

/// Pause after a failed receive or accept, so a persistent error (such as
/// running out of file descriptors) does not spin the loop.
const ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// Biggest query datagram read; a query is a header and one question.
const MAX_QUERY: usize = 4096;

/// Where each query goes, shared by the listeners.
struct Handler {
    ips: Arc<dyn ServiceIps>,
    upstreams: Vec<SocketAddr>,
    upstream_timeout: Duration,
}

impl Handler {
    /// What the zone says about `query`; [`Outcome::Forward`] when the name
    /// is not ours. Cheap and never waits.
    fn local(&self, query: &[u8]) -> Outcome {
        answer(query, self.ips.as_ref())
    }

    /// The upstreams' answer to `query`, or SERVFAIL when none answers in
    /// time. `tcp` picks the transport used towards the upstreams.
    async fn forward(&self, query: &[u8], tcp: bool) -> Option<Vec<u8>> {
        let forwarded = if tcp {
            forward_tcp(query, &self.upstreams, self.upstream_timeout).await
        } else {
            forward_udp(query, &self.upstreams, self.upstream_timeout).await
        };
        forwarded.or_else(|| error_reply(query, ResponseCode::ServFail))
    }
}

/// Sleeps [`ERROR_BACKOFF`], or until `cancel` fires.
async fn back_off(cancel: &CancellationToken) {
    tokio::select! {
        () = cancel.cancelled() => {}
        () = tokio::time::sleep(ERROR_BACKOFF) => {}
    }
}

/// A running DNS server.
#[derive(Debug)]
pub struct DnsServer {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl DnsServer {
    /// Binds `config.listen` over UDP and TCP and serves until `cancel`
    /// fires. Needs a running tokio runtime.
    pub fn start(
        config: DnsConfig,
        ips: Arc<dyn ServiceIps>,
        cancel: CancellationToken,
    ) -> io::Result<Self> {
        let (udp, tcp) = bind(config.listen)?;
        let addr = tcp.local_addr()?;
        let handler = Arc::new(Handler {
            ips,
            upstreams: config.upstreams,
            upstream_timeout: config.upstream_timeout,
        });
        let task = tokio::spawn(async move {
            tokio::join!(
                serve_udp(udp, handler.clone(), cancel.clone()),
                serve_tcp(tcp, handler, cancel),
            );
        });
        Ok(Self { addr, task })
    }

    /// The address both listeners are bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Waits for the server to stop.
    pub async fn wait(self) {
        if let Err(e) = self.task.await
            && !e.is_cancelled()
        {
            tracing::error!(error = %e, "DNS server task failed");
        }
    }
}

/// Binds a UDP socket and a TCP listener to `addr`, on one port even when
/// `addr` asks for any. On Linux the address need not exist yet
/// (`IP_FREEBIND`), because the bridge may come up after fabercp.
pub fn bind(addr: SocketAddr) -> io::Result<(UdpSocket, TcpListener)> {
    let tcp = bind_socket(addr, socket2::Type::STREAM)?;
    tcp.listen(1024)?;
    let port = tcp
        .local_addr()?
        .as_socket()
        .map(|a| a.port())
        .unwrap_or(addr.port());
    let udp = bind_socket(SocketAddr::new(addr.ip(), port), socket2::Type::DGRAM)?;
    Ok((
        UdpSocket::from_std(udp.into())?,
        TcpListener::from_std(tcp.into())?,
    ))
}

fn bind_socket(addr: SocketAddr, kind: socket2::Type) -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(socket2::Domain::for_address(addr), kind, None)?;
    // For a restart during TIME_WAIT. Not on UDP, where it would let another
    // process share the port and steal datagrams; not on Windows, where it
    // lets a process take over a port in use.
    #[cfg(unix)]
    if kind == socket2::Type::STREAM {
        socket.set_reuse_address(true)?;
    }
    #[cfg(target_os = "linux")]
    match addr {
        SocketAddr::V4(_) => socket.set_freebind_v4(true)?,
        SocketAddr::V6(_) => socket.set_freebind_v6(true)?,
    }
    if addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    Ok(socket)
}

async fn serve_udp(socket: UdpSocket, handler: Arc<Handler>, cancel: CancellationToken) {
    let socket = Arc::new(socket);
    let forwards = Arc::new(Semaphore::new(MAX_UDP_FORWARDS));
    // Dropping the set on return aborts the queries still in flight.
    let mut tasks = JoinSet::new();
    let mut buf = vec![0u8; MAX_QUERY];
    loop {
        while tasks.try_join_next().is_some() {}
        let (n, from) = tokio::select! {
            () = cancel.cancelled() => return,
            received = socket.recv_from(&mut buf) => match received {
                Ok(received) => received,
                Err(e) => {
                    tracing::debug!(error = %e, "DNS UDP receive failed");
                    back_off(&cancel).await;
                    continue;
                }
            },
        };
        let query = &buf[..n];
        match handler.local(query) {
            Outcome::Drop => {}
            Outcome::Reply(response) => send_udp(&socket, query, response, from).await,
            Outcome::Forward => {
                let Ok(permit) = forwards.clone().try_acquire_owned() else {
                    tracing::debug!(%from, "DNS busy, dropping a query");
                    continue;
                };
                let (query, handler, socket) = (query.to_vec(), handler.clone(), socket.clone());
                tasks.spawn(async move {
                    if let Some(response) = handler.forward(&query, false).await {
                        send_udp(&socket, &query, response, from).await;
                    }
                    drop(permit);
                });
            }
        }
    }
}

/// Sends `response` to `to`, cut to what the sender of `query` can take.
async fn send_udp(socket: &UdpSocket, query: &[u8], response: Vec<u8>, to: SocketAddr) {
    let response = fit_udp(query, response);
    if let Err(e) = socket.send_to(&response, to).await {
        tracing::debug!(%to, error = %e, "DNS UDP send failed");
    }
}

async fn serve_tcp(listener: TcpListener, handler: Arc<Handler>, cancel: CancellationToken) {
    let connections = Arc::new(Semaphore::new(MAX_TCP_CONNECTIONS));
    let mut tasks = JoinSet::new();
    loop {
        while tasks.try_join_next().is_some() {}
        let (stream, from) = tokio::select! {
            () = cancel.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    tracing::debug!(error = %e, "DNS TCP accept failed");
                    back_off(&cancel).await;
                    continue;
                }
            },
        };
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            tracing::debug!(%from, "DNS busy, dropping a connection");
            continue;
        };
        let handler = handler.clone();
        tasks.spawn(async move {
            // A client that keeps a connection busy still lets go of it.
            let _ = timeout(TCP_LIFETIME, serve_connection(stream, &handler)).await;
            drop(permit);
        });
    }
}

/// Answers the length-prefixed queries on `stream` one after another until
/// the client closes it, errs, stays silent for [`TCP_IDLE`] or has had
/// [`TCP_MAX_QUERIES`] answered.
async fn serve_connection(mut stream: TcpStream, handler: &Handler) {
    for _ in 0..TCP_MAX_QUERIES {
        let Ok(Ok(query)) = timeout(TCP_IDLE, read_frame(&mut stream)).await else {
            return;
        };
        let response = match handler.local(&query) {
            Outcome::Drop => continue,
            Outcome::Reply(response) => response,
            Outcome::Forward => match handler.forward(&query, true).await {
                Some(response) => response,
                None => continue,
            },
        };
        let Ok(len) = u16::try_from(response.len()) else {
            return;
        };
        let mut framed = Vec::with_capacity(response.len() + 2);
        framed.extend_from_slice(&len.to_be_bytes());
        framed.extend_from_slice(&response);
        if stream.write_all(&framed).await.is_err() {
            return;
        }
    }
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 2];
    stream.read_exact(&mut len).await?;
    let mut query = vec![0u8; usize::from(u16::from_be_bytes(len))];
    stream.read_exact(&mut query).await?;
    Ok(query)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use hickory_proto::op::{Message, Query};
    use hickory_proto::rr::rdata::A;
    use hickory_proto::rr::{Name, RData, Record, RecordType};

    use super::*;
    use crate::dns::InMemoryServiceIps;

    const SERVICE_IP: Ipv4Addr = Ipv4Addr::new(10, 201, 0, 7);
    const PUBLIC_IP: Ipv4Addr = Ipv4Addr::new(93, 184, 216, 34);

    /// A UDP resolver that answers every A query with [`PUBLIC_IP`].
    async fn fake_upstream() -> SocketAddr {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            while let Ok((n, from)) = socket.recv_from(&mut buf).await {
                let query = Message::from_vec(&buf[..n]).unwrap();
                let mut response = Message::response(query.metadata.id, query.metadata.op_code);
                response.queries.clone_from(&query.queries);
                response.add_answer(Record::from_rdata(
                    query.queries[0].name().clone(),
                    60,
                    RData::A(A(PUBLIC_IP)),
                ));
                socket
                    .send_to(&response.to_vec().unwrap(), from)
                    .await
                    .unwrap();
            }
        });
        addr
    }

    fn start(upstreams: Vec<SocketAddr>) -> (DnsServer, CancellationToken) {
        let ips = InMemoryServiceIps::default();
        ips.set("shop", "prod", "web", SERVICE_IP);
        let cancel = CancellationToken::new();
        let server = DnsServer::start(
            DnsConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                upstreams,
                upstream_timeout: Duration::from_millis(200),
            },
            Arc::new(ips),
            cancel.clone(),
        )
        .unwrap();
        (server, cancel)
    }

    fn query(name: &str, kind: RecordType) -> Vec<u8> {
        let mut m = Message::query();
        m.add_query(Query::query(Name::from_ascii(name).unwrap(), kind));
        m.to_vec().unwrap()
    }

    async fn ask_udp(server: SocketAddr, query: &[u8]) -> Message {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.send_to(query, server).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let (n, _) = timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        Message::from_vec(&buf[..n]).unwrap()
    }

    fn only_ip(m: &Message) -> Ipv4Addr {
        match &m.answers[..] {
            [record] => match record.data {
                RData::A(A(ip)) => ip,
                ref other => panic!("not an A record: {other:?}"),
            },
            other => panic!("expected one answer, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dns_service_name_answers_with_its_virtual_ip() {
        let (server, cancel) = start(vec![]);
        let m = ask_udp(
            server.local_addr(),
            &query("web.prod.shop.faber.local.", RecordType::A),
        )
        .await;
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert_eq!(only_ip(&m), SERVICE_IP);
        cancel.cancel();
        server.wait().await;
    }

    #[tokio::test]
    async fn dns_other_names_are_forwarded_and_the_answer_relayed() {
        let (server, cancel) = start(vec![fake_upstream().await]);
        let m = ask_udp(server.local_addr(), &query("example.com.", RecordType::A)).await;
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert_eq!(only_ip(&m), PUBLIC_IP);
        cancel.cancel();
        server.wait().await;
    }

    #[tokio::test]
    async fn dns_upstream_timeout_is_servfail() {
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (server, cancel) = start(vec![silent.local_addr().unwrap()]);
        let q = query("example.com.", RecordType::A);
        let m = ask_udp(server.local_addr(), &q).await;
        assert_eq!(m.metadata.response_code, ResponseCode::ServFail);
        assert_eq!(m.queries.len(), 1);
        // No upstream at all is the same.
        let (bare, bare_cancel) = start(vec![]);
        let m = ask_udp(bare.local_addr(), &q).await;
        assert_eq!(m.metadata.response_code, ResponseCode::ServFail);
        cancel.cancel();
        bare_cancel.cancel();
        server.wait().await;
        bare.wait().await;
    }

    #[tokio::test]
    async fn dns_tcp_answers_service_names_and_stops_on_cancel() {
        let (server, cancel) = start(vec![]);
        let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
        for _ in 0..2 {
            let q = query("web.prod.shop.faber.local.", RecordType::A);
            stream
                .write_all(&(q.len() as u16).to_be_bytes())
                .await
                .unwrap();
            stream.write_all(&q).await.unwrap();
            let response = read_frame(&mut stream).await.unwrap();
            assert_eq!(only_ip(&Message::from_vec(&response).unwrap()), SERVICE_IP);
        }
        cancel.cancel();
        timeout(Duration::from_secs(5), server.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dns_tcp_connection_closes_after_its_query_cap() {
        let (server, cancel) = start(vec![]);
        let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
        let q = query("web.prod.shop.faber.local.", RecordType::A);
        for _ in 0..TCP_MAX_QUERIES {
            stream
                .write_all(&(q.len() as u16).to_be_bytes())
                .await
                .unwrap();
            stream.write_all(&q).await.unwrap();
            read_frame(&mut stream).await.unwrap();
        }
        // The server has let go: the next read hits the end of the stream.
        assert!(read_frame(&mut stream).await.is_err());
        cancel.cancel();
        server.wait().await;
    }

    #[tokio::test]
    async fn dns_service_names_are_answered_while_forwards_wait() {
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (server, cancel) = start(vec![silent.local_addr().unwrap()]);
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client
            .send_to(&query("example.com.", RecordType::A), server.local_addr())
            .await
            .unwrap();
        client
            .send_to(
                &query("web.prod.shop.faber.local.", RecordType::A),
                server.local_addr(),
            )
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let (n, _) = timeout(Duration::from_secs(5), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        // The first reply is the service answer, not the SERVFAIL that
        // comes only after the upstream timeout.
        let m = Message::from_vec(&buf[..n]).unwrap();
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert_eq!(only_ip(&m), SERVICE_IP);
        cancel.cancel();
        server.wait().await;
    }

    #[test]
    fn dns_bind_shares_one_port_for_udp_and_tcp() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = rt.enter();
        let (udp, tcp) = bind((IpAddr::from(Ipv4Addr::LOCALHOST), 0).into()).unwrap();
        assert_eq!(udp.local_addr().unwrap(), tcp.local_addr().unwrap());
    }
}
