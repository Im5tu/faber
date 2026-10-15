//! One client connection: peer check, then framed request/response
//! exchanges until the client leaves.

use std::time::Duration;

use faber_cri::CriClient;
use faber_node_protocol::{
    ErrorCode, FrameError, MAX_FRAME_BYTES, Request, Response, read_frame, write_frame,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::node::Node;
use crate::peer::{Authorizer, PeerCredentials};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Largest request frame accepted.
    pub max_frame: usize,
    /// How long to wait for the next request before dropping the client.
    pub idle: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame: MAX_FRAME_BYTES,
            idle: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The peer is not allowed; nothing was read from it.
    Refused,
    /// The client left, timed out, or broke the protocol.
    Finished,
}

pub async fn serve_connection<S, C>(
    mut stream: S,
    node: &Node<C>,
    auth: &Authorizer,
    limits: Limits,
) -> Outcome
where
    S: AsyncRead + AsyncWrite + Unpin + PeerCredentials,
    C: CriClient + Clone,
{
    match stream.peer() {
        Ok(peer) if auth.allows(&peer) => {}
        Ok(peer) => {
            tracing::warn!(uid = peer.uid, gid = peer.gid, "refused a caller");
            return Outcome::Refused;
        }
        Err(e) => {
            tracing::warn!("refused a caller whose credentials could not be read: {e}");
            return Outcome::Refused;
        }
    }

    let mut first = true;
    loop {
        let read = tokio::time::timeout(
            limits.idle,
            read_frame::<_, Request>(&mut stream, limits.max_frame),
        )
        .await;
        let request = match read {
            Err(_elapsed) => return Outcome::Finished,
            Ok(Ok(Some(request))) => request,
            Ok(Ok(None)) => return Outcome::Finished,
            Ok(Err(FrameError::Io(_))) => return Outcome::Finished,
            Ok(Err(e @ (FrameError::TooLarge { .. } | FrameError::Json(_)))) => {
                // The stream may be out of step after a frame is refused, so
                // the answer is the last thing sent.
                let _ = write_frame(
                    &mut stream,
                    &Response::error(ErrorCode::InvalidRequest, e.to_string()),
                )
                .await;
                return Outcome::Finished;
            }
        };
        if first && !matches!(request, Request::Version { .. }) {
            let _ = write_frame(
                &mut stream,
                &Response::error(
                    ErrorCode::InvalidRequest,
                    "the first request must be version",
                ),
            )
            .await;
            return Outcome::Finished;
        }
        let response = node.handle(request).await;
        if write_frame(&mut stream, &response).await.is_err() {
            return Outcome::Finished;
        }
        // The handshake is done only by a version that was accepted.
        if first {
            if !matches!(response, Response::Version { .. }) {
                return Outcome::Finished;
            }
            first = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use faber_container_runtime::Containerd;
    use faber_cri::fake::FakeCri;
    use faber_node_protocol::{PROTOCOL_VERSION, encode_frame};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use super::*;
    use crate::config::UidRange;
    use crate::dirs::DirPolicy;
    use crate::host_net::fake::FakeHostNetworks;
    use crate::peer::Peer;
    use crate::peer::fake::FakeStream;
    use crate::volume::fake::FakeVolumes;

    fn node(cri: FakeCri) -> Node<FakeCri> {
        Node::new(
            UidRange::new(65536, 65536 * 8).unwrap(),
            String::new(),
            cri,
            Arc::new(FakeVolumes::new("/vol")),
            Containerd {
                conf_dir: "/nonexistent/conf.d".into(),
                cni_conf_dir: "/nonexistent/net.d".into(),
                cni_bin_dir: "/opt/cni/bin".into(),
                sandbox_image: "pause:3".into(),
                bridge: "faberbr0".into(),
            },
            DirPolicy::root(),
            Arc::new(FakeHostNetworks::default()),
        )
    }

    fn peer(uid: u32) -> Peer {
        Peer {
            uid,
            gid: 0,
            pid: None,
        }
    }

    async fn exchange(client: &mut DuplexStream, request: &Request) -> Response {
        write_frame(client, request).await.unwrap();
        read_frame(client, MAX_FRAME_BYTES).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn a_caller_with_the_wrong_uid_is_refused_before_anything_is_read() {
        let cri = FakeCri::new();
        let node = node(cri.clone());
        let (server, mut client) = FakeStream::pair(peer(1000));
        client
            .write_all(&encode_frame(&Request::ListReplicas).unwrap())
            .await
            .unwrap();
        let outcome =
            serve_connection(server, &node, &Authorizer::new(990), Limits::default()).await;
        assert_eq!(outcome, Outcome::Refused);
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "no answer for a refused caller");
        assert!(cri.calls().is_empty());
    }

    #[tokio::test]
    async fn unreadable_credentials_are_refused() {
        let node = node(FakeCri::new());
        let (mut server, _client) = FakeStream::pair(peer(0));
        server.peer = Err(std::io::ErrorKind::Other.into());
        let outcome =
            serve_connection(server, &node, &Authorizer::new(990), Limits::default()).await;
        assert_eq!(outcome, Outcome::Refused);
    }

    #[tokio::test]
    async fn root_and_the_faber_uid_may_talk_and_must_open_with_version() {
        for uid in [0, 990] {
            let node = node(FakeCri::new());
            let (server, mut client) = FakeStream::pair(peer(uid));
            let task = tokio::spawn(async move {
                serve_connection(server, &node, &Authorizer::new(990), Limits::default()).await
            });
            let reply = exchange(
                &mut client,
                &Request::Version {
                    client_version: PROTOCOL_VERSION,
                },
            )
            .await;
            assert!(matches!(
                reply,
                Response::Version {
                    protocol_version: 1,
                    ..
                }
            ));
            let reply = exchange(&mut client, &Request::ListReplicas).await;
            assert_eq!(reply, Response::Replicas { replicas: vec![] });
            drop(client);
            assert_eq!(task.await.unwrap(), Outcome::Finished);
        }
    }

    #[tokio::test]
    async fn the_first_request_must_be_version() {
        let node = node(FakeCri::new());
        let (server, mut client) = FakeStream::pair(peer(0));
        let task = tokio::spawn(async move {
            serve_connection(server, &node, &Authorizer::new(990), Limits::default()).await
        });
        let reply = exchange(&mut client, &Request::ListReplicas).await;
        assert!(matches!(
            reply,
            Response::Error {
                code: ErrorCode::InvalidRequest,
                ..
            }
        ));
        assert_eq!(task.await.unwrap(), Outcome::Finished);
    }

    #[tokio::test]
    async fn a_version_mismatch_is_a_typed_error() {
        let node = node(FakeCri::new());
        let reply = node.handle(Request::Version { client_version: 99 }).await;
        assert!(matches!(
            reply,
            Response::Error {
                code: ErrorCode::VersionMismatch,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused_without_reading_it() {
        let node = node(FakeCri::new());
        let (server, mut client) = FakeStream::pair(peer(0));
        let limits = Limits {
            max_frame: 1024,
            ..Limits::default()
        };
        let task = tokio::spawn(async move {
            serve_connection(server, &node, &Authorizer::new(990), limits).await
        });
        // Only a header claiming 4 MiB: the body is never sent.
        client
            .write_all(&(4u32 * 1024 * 1024).to_be_bytes())
            .await
            .unwrap();
        let reply: Response = read_frame(&mut client, MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            reply,
            Response::Error {
                code: ErrorCode::InvalidRequest,
                ..
            }
        ));
        assert_eq!(task.await.unwrap(), Outcome::Finished);
    }

    #[tokio::test]
    async fn unknown_requests_and_extra_fields_are_refused() {
        // `privileged` is not a field of a container: the frame does not parse.
        let bodies = [
            r#"{"type":"create_container","replica":{"project":"a","env":"b","workload":"c","revision":"abcd","replica_index":0},"sandbox_id":"s","container":{"name":"c","image":"i","privileged":true},"env":{},"uid_base":65536}"#,
            r#"{"type":"exec","command":"sh"}"#,
            r#"{"type":"version","client_version":1,"path":"/etc"}"#,
        ];
        for body in bodies {
            let cri = FakeCri::new();
            let node = node(cri.clone());
            let (server, mut client) = FakeStream::pair(peer(0));
            let task = tokio::spawn(async move {
                serve_connection(server, &node, &Authorizer::new(990), Limits::default()).await
            });
            let mut frame = (body.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(body.as_bytes());
            client.write_all(&frame).await.unwrap();
            let reply: Response = read_frame(&mut client, MAX_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(
                    reply,
                    Response::Error {
                        code: ErrorCode::InvalidRequest,
                        ..
                    }
                ),
                "{body}"
            );
            task.await.unwrap();
            assert!(cri.calls().is_empty());
        }
    }

    #[tokio::test]
    async fn an_idle_client_is_dropped() {
        let node = node(FakeCri::new());
        let (server, _client) = FakeStream::pair(peer(0));
        let limits = Limits {
            idle: Duration::from_millis(20),
            ..Limits::default()
        };
        let outcome = serve_connection(server, &node, &Authorizer::new(990), limits).await;
        assert_eq!(outcome, Outcome::Finished);
    }
}
