//! Who is on the other end of the socket.

use std::io;

/// Credentials the kernel recorded when the peer connected (`SO_PEERCRED`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    pub uid: u32,
    pub gid: u32,
    pub pid: Option<i32>,
}

pub trait PeerCredentials {
    fn peer(&self) -> io::Result<Peer>;
}

#[cfg(target_os = "linux")]
impl PeerCredentials for tokio::net::UnixStream {
    fn peer(&self) -> io::Result<Peer> {
        let cred = self.peer_cred()?;
        Ok(Peer {
            uid: cred.uid(),
            gid: cred.gid(),
            pid: cred.pid(),
        })
    }
}

/// Root and the configured faber user may use the socket; nobody else.
#[derive(Debug, Clone, Copy)]
pub struct Authorizer {
    faber_uid: u32,
}

impl Authorizer {
    pub fn new(faber_uid: u32) -> Self {
        Self { faber_uid }
    }

    pub fn allows(&self, peer: &Peer) -> bool {
        peer.uid == 0 || peer.uid == self.faber_uid
    }
}

/// A duplex stream with fixed credentials, for tests.
#[doc(hidden)]
pub mod fake {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

    use super::*;

    pub struct FakeStream {
        pub inner: DuplexStream,
        pub peer: io::Result<Peer>,
    }

    impl FakeStream {
        /// A connected pair: the server side reports `peer`; the other side
        /// is the client's plain stream.
        pub fn pair(peer: Peer) -> (Self, DuplexStream) {
            let (server, client) = tokio::io::duplex(64 * 1024);
            (
                Self {
                    inner: server,
                    peer: Ok(peer),
                },
                client,
            )
        }
    }

    impl PeerCredentials for FakeStream {
        fn peer(&self) -> io::Result<Peer> {
            match &self.peer {
                Ok(p) => Ok(*p),
                Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
            }
        }
    }

    impl AsyncRead for FakeStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for FakeStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_root_and_the_faber_uid_are_allowed() {
        let auth = Authorizer::new(990);
        let peer = |uid| Peer {
            uid,
            gid: 0,
            pid: None,
        };
        assert!(auth.allows(&peer(0)));
        assert!(auth.allows(&peer(990)));
        for other in [1, 989, 991, 1000, 65534, u32::MAX] {
            assert!(!auth.allows(&peer(other)), "{other}");
        }
    }
}
