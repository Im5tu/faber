//! The Unix socket: owner, mode, accept loop.

use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use faber_cri::CriClient;
use nix::sys::stat::{Mode, umask};
use tokio::net::UnixListener;
use tokio::sync::Semaphore;

use crate::config::{NodeConfig, root_only_writable};
use crate::connection::{Limits, serve_connection};
use crate::node::Node;
use crate::peer::Authorizer;

/// Most connections served at once.
const MAX_CONNECTIONS: usize = 64;

/// Binds the socket as `root:<faber gid>` 0660.
pub fn bind(config: &NodeConfig) -> anyhow::Result<UnixListener> {
    let path: &Path = &config.socket;
    let parent = path.parent().context("the socket path has no directory")?;
    std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    // The socket is created, removed and given an owner by path. If anyone
    // but root could write the directory, they could swap in a symlink and
    // have root hand a file of theirs or root's to the faber group.
    let meta = std::fs::symlink_metadata(parent)
        .with_context(|| format!("checking {}", parent.display()))?;
    if !meta.is_dir() || !root_only_writable(meta.uid(), meta.mode()) {
        anyhow::bail!(
            "{} must be a directory owned by root and not writable by group or others",
            parent.display()
        );
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path)
            .with_context(|| format!("removing the stale socket {}", path.display()))?,
        Ok(_) => anyhow::bail!("{} exists and is not a socket", path.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("checking {}", path.display())),
    }
    // Nobody but the owner and group can even see the socket while it is
    // being set up.
    let previous = umask(Mode::from_bits_truncate(0o117));
    let bound = UnixListener::bind(path);
    umask(previous);
    let listener = bound.with_context(|| format!("binding {}", path.display()))?;
    // The mode is already 0660 from the umask. `lchown`, so a swapped-in
    // symlink is never followed.
    std::os::unix::fs::lchown(path, Some(0), Some(config.faber_gid))
        .context("setting the socket owner")?;
    Ok(listener)
}

pub async fn serve<C>(
    listener: UnixListener,
    node: Arc<Node<C>>,
    auth: Authorizer,
) -> anyhow::Result<()>
where
    C: CriClient + Clone + 'static,
{
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, _) = listener.accept().await.context("accepting")?;
        let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
            tracing::warn!("too many connections: dropped one");
            continue;
        };
        let node = Arc::clone(&node);
        tokio::spawn(async move {
            serve_connection(stream, &*node, &auth, Limits::default()).await;
            drop(permit);
        });
    }
}
