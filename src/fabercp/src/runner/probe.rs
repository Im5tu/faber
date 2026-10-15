//! Probes: a TCP connect, or an HTTP GET, to a pod.
//!
//! fabercp probes because it can reach the pod IPs over the bridge. A probe
//! passes when the connection opens (`tcp`) or the status is 2xx or 3xx
//! (`http`, `https`). A redirect is judged, not followed, and the body is
//! never read: the status line is all a probe needs.
//!
//! An `https` probe does the TLS handshake with the certificate not
//! verified: a pod's certificate is its own, usually self-signed, and the
//! probe asks whether the service answers, not who it is. The connection
//! goes to a pod address on the node's own bridge.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use faber_api_types::workload::{HttpScheme, ProbeAction, RESERVED_PROBE_HEADERS};
use reqwest::header::{HeaderName, HeaderValue};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// What to probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeTarget {
    pub ip: Ipv4Addr,
    pub action: ProbeAction,
    /// The whole probe, connect included, takes at most this long.
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeOutcome {
    Pass,
    /// Failed, and why.
    Fail(String),
}

pub trait Prober: Send + Sync + 'static {
    fn probe(&self, target: ProbeTarget) -> impl Future<Output = ProbeOutcome> + Send;
}

/// Accepts any certificate. See the module documentation for why that is
/// right for a probe.
#[derive(Debug)]
struct AcceptAnyCertificate(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Longest status line read: `HTTP/1.1 200 ` plus a reason is far shorter.
const STATUS_LINE_MAX: usize = 256;

/// Probes over the network.
#[derive(Clone)]
pub struct NetProber {
    tls: tokio_rustls::TlsConnector,
}

impl std::fmt::Debug for NetProber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetProber").finish_non_exhaustive()
    }
}

impl NetProber {
    pub fn new() -> Result<Self, rustls::Error> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate(provider)))
            .with_no_client_auth();
        Ok(Self {
            tls: tokio_rustls::TlsConnector::from(Arc::new(config)),
        })
    }

    async fn tcp(ip: Ipv4Addr, port: u16) -> ProbeOutcome {
        match tokio::net::TcpStream::connect((ip, port)).await {
            Ok(_) => ProbeOutcome::Pass,
            Err(e) => ProbeOutcome::Fail(format!("connect to port {port}: {e}")),
        }
    }

    async fn get(
        &self,
        ip: Ipv4Addr,
        port: u16,
        tls: bool,
        path: &str,
        host: Option<&str>,
        headers: &BTreeMap<String, String>,
    ) -> ProbeOutcome {
        let request = match request_bytes(ip, port, path, host, headers) {
            Ok(request) => request,
            Err(why) => return ProbeOutcome::Fail(why),
        };
        let stream = match tokio::net::TcpStream::connect((ip, port)).await {
            Ok(stream) => stream,
            Err(e) => return ProbeOutcome::Fail(format!("connect to port {port}: {e}")),
        };
        let result = if tls {
            let name = ServerName::IpAddress(IpAddr::V4(ip).into());
            match self.tls.connect(name, stream).await {
                Ok(stream) => status_of(stream, &request).await,
                Err(e) => return ProbeOutcome::Fail(format!("tls handshake: {e}")),
            }
        } else {
            status_of(stream, &request).await
        };
        match result {
            Ok(status) if (200..400).contains(&status) => ProbeOutcome::Pass,
            Ok(status) => ProbeOutcome::Fail(format!("status {status}")),
            Err(why) => ProbeOutcome::Fail(why),
        }
    }
}

/// The request text. Rejects what could break out of the request line or a
/// header: the values come from the workload spec.
fn request_bytes(
    ip: Ipv4Addr,
    port: u16,
    path: &str,
    host: Option<&str>,
    headers: &BTreeMap<String, String>,
) -> Result<Vec<u8>, String> {
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    if path.bytes().any(|b| b <= b' ' || b == 0x7f) {
        return Err("the probe path is not valid".into());
    }
    let host = host.map_or_else(|| format!("{ip}:{port}"), str::to_owned);
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nUser-Agent: faber-probe\r\n",
        valid_value(&host)?
    );
    for (name, value) in headers {
        // The model refuses these; a spec stored before that rule may have
        // them, and the runner's own values must stand.
        if RESERVED_PROBE_HEADERS
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
        {
            continue;
        }
        HeaderName::try_from(name).map_err(|_| format!("the header {name:?} is not valid"))?;
        request.push_str(&format!("{name}: {}\r\n", valid_value(value)?));
    }
    request.push_str("\r\n");
    Ok(request.into_bytes())
}

fn valid_value(value: &str) -> Result<&str, String> {
    HeaderValue::from_str(value).map_err(|_| "a header value is not valid".to_owned())?;
    Ok(value)
}

/// Sends `request` and reads the status line, and no more.
async fn status_of<S>(mut stream: S, request: &[u8]) -> Result<u16, String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    stream
        .write_all(request)
        .await
        .map_err(|e| format!("request: {e}"))?;
    stream.flush().await.map_err(|e| format!("request: {e}"))?;
    let mut line = Vec::with_capacity(64);
    let mut chunk = [0u8; 64];
    while !line.windows(2).any(|w| w == b"\r\n") && line.len() < STATUS_LINE_MAX {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|e| format!("response: {e}"))?;
        if n == 0 {
            break;
        }
        line.extend_from_slice(&chunk[..n]);
    }
    parse_status(&line).ok_or_else(|| "the answer is not an HTTP status line".to_owned())
}

fn parse_status(line: &[u8]) -> Option<u16> {
    let text = std::str::from_utf8(line.split(|b| *b == b'\r').next()?).ok()?;
    let mut parts = text.split(' ');
    if !parts.next()?.starts_with("HTTP/1.") {
        return None;
    }
    let code = parts.next()?;
    if code.len() != 3 {
        return None;
    }
    code.parse().ok()
}

impl Prober for NetProber {
    async fn probe(&self, target: ProbeTarget) -> ProbeOutcome {
        let run = async {
            match &target.action {
                ProbeAction::Tcp { port } => Self::tcp(target.ip, *port).await,
                ProbeAction::Http {
                    port,
                    path,
                    scheme,
                    host,
                    headers,
                } => {
                    self.get(
                        target.ip,
                        *port,
                        *scheme == HttpScheme::Https,
                        path,
                        host.as_deref(),
                        headers,
                    )
                    .await
                }
            }
        };
        match tokio::time::timeout(target.timeout, run).await {
            Ok(outcome) => outcome,
            Err(_) => ProbeOutcome::Fail(format!("no answer within {:?}", target.timeout)),
        }
    }
}

#[cfg(test)]
mod tests {
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject;
    use tokio::net::TcpListener;

    use super::*;

    const LOCAL: Ipv4Addr = Ipv4Addr::LOCALHOST;
    const SHORT: Duration = Duration::from_millis(300);
    const CERT: &[u8] = include_bytes!("testdata/probe-test-cert.pem");
    const KEY: &[u8] = include_bytes!("testdata/probe-test-key.pem");

    fn http_action(port: u16, scheme: HttpScheme) -> ProbeAction {
        ProbeAction::Http {
            port,
            path: "/health".into(),
            scheme,
            host: None,
            headers: BTreeMap::new(),
        }
    }

    fn acceptor() -> tokio_rustls::TlsAcceptor {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let certs = CertificateDer::pem_slice_iter(CERT)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_slice(KEY).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    }

    /// A server that answers every request with `status` (or never, when
    /// `None`), over TLS when `tls`. Sends a body that is never read.
    /// Returns the port.
    async fn server(status: Option<u16>, tls: bool) -> u16 {
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = acceptor();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if tls {
                        if let Ok(stream) = acceptor.accept(stream).await {
                            answer(stream, status).await;
                        }
                    } else {
                        answer(stream, status).await;
                    }
                });
            }
        });
        port
    }

    async fn answer<S>(mut stream: S, status: Option<u16>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await;
        match status {
            Some(code) => {
                let reply = format!(
                    "HTTP/1.1 {code} X\r\ncontent-length: 5\r\nlocation: /elsewhere\r\nconnection: close\r\n\r\nhello"
                );
                let _ = stream.write_all(reply.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
            None => std::future::pending::<()>().await,
        }
    }

    async fn run(action: ProbeAction) -> ProbeOutcome {
        NetProber::new()
            .unwrap()
            .probe(ProbeTarget {
                ip: LOCAL,
                action,
                timeout: SHORT,
            })
            .await
    }

    #[tokio::test]
    async fn tcp_passes_when_the_port_accepts_and_fails_when_refused() {
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        assert_eq!(run(ProbeAction::Tcp { port }).await, ProbeOutcome::Pass);
        drop(listener);
        assert!(matches!(
            run(ProbeAction::Tcp { port }).await,
            ProbeOutcome::Fail(_)
        ));
    }

    #[tokio::test]
    async fn http_and_https_judge_the_status() {
        let table = [
            (200, true),
            (204, true),
            (301, true),
            (399, true),
            (400, false),
            (500, false),
            (404, false),
        ];
        for tls in [false, true] {
            for (status, passes) in table {
                let port = server(Some(status), tls).await;
                let scheme = if tls {
                    HttpScheme::Https
                } else {
                    HttpScheme::Http
                };
                let outcome = run(http_action(port, scheme)).await;
                assert_eq!(
                    outcome == ProbeOutcome::Pass,
                    passes,
                    "status {status}, tls {tls}: {outcome:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn that_never_answers_times_out_over_http_and_https() {
        for (tls, scheme) in [(false, HttpScheme::Http), (true, HttpScheme::Https)] {
            let port = server(None, tls).await;
            let ProbeOutcome::Fail(why) = run(http_action(port, scheme)).await else {
                panic!("a silent server must fail the probe");
            };
            assert!(why.contains("no answer"), "{why}");
        }
    }

    #[tokio::test]
    async fn https_to_a_plain_http_port_fails_the_handshake() {
        let port = server(Some(200), false).await;
        assert!(matches!(
            run(http_action(port, HttpScheme::Https)).await,
            ProbeOutcome::Fail(_)
        ));
    }

    #[tokio::test]
    async fn a_refused_https_port_fails() {
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        assert!(matches!(
            run(http_action(port, HttpScheme::Https)).await,
            ProbeOutcome::Fail(_)
        ));
    }

    #[test]
    fn status_lines_are_parsed_strictly() {
        assert_eq!(parse_status(b"HTTP/1.1 204 No Content\r\n"), Some(204));
        assert_eq!(parse_status(b"HTTP/1.0 301 \r\nx"), Some(301));
        for bad in [
            &b""[..],
            b"SSH-2.0-x\r\n",
            b"HTTP/1.1 20 X\r\n",
            b"HTTP/1.1 abc\r\n",
        ] {
            assert_eq!(parse_status(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn the_request_refuses_what_could_break_out_of_it() {
        let ok = request_bytes(LOCAL, 80, "/h", None, &BTreeMap::new()).unwrap();
        assert!(ok.starts_with(b"GET /h HTTP/1.1\r\nHost: 127.0.0.1:80\r\n"));
        assert!(request_bytes(LOCAL, 80, "/a b", None, &BTreeMap::new()).is_err());
        assert!(request_bytes(LOCAL, 80, "/a\r\nX: y", None, &BTreeMap::new()).is_err());
        let bad_value = BTreeMap::from([("X-A".to_owned(), "1\r\nX-B: 2".to_owned())]);
        assert!(request_bytes(LOCAL, 80, "/", None, &bad_value).is_err());
        let reserved: BTreeMap<String, String> =
            ["host", "Connection", "CONTENT-LENGTH", "transfer-encoding"]
                .into_iter()
                .map(|n| (n.to_owned(), "x".to_owned()))
                .chain([("X-Kept".to_owned(), "1".to_owned())])
                .collect();
        let text =
            String::from_utf8(request_bytes(LOCAL, 80, "/", None, &reserved).unwrap()).unwrap();
        assert_eq!(text.matches("Host:").count(), 1, "{text}");
        assert_eq!(text.matches("Connection:").count(), 1, "{text}");
        assert!(!text.to_lowercase().contains("content-length"), "{text}");
        assert!(!text.to_lowercase().contains("transfer-encoding"), "{text}");
        assert!(text.contains("X-Kept: 1\r\n"), "{text}");
        let bad_name = BTreeMap::from([("X A".to_owned(), "1".to_owned())]);
        assert!(request_bytes(LOCAL, 80, "/", None, &bad_name).is_err());
        assert!(request_bytes(LOCAL, 80, "/", Some("a\r\nb"), &BTreeMap::new()).is_err());
    }
}
