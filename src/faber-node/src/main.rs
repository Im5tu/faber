//! `faber-node` daemon. Runs as root; see the library docs.

use std::path::PathBuf;

use clap::Parser;
use faber_node::config::DEFAULT_ENV_FILE;

#[derive(Parser)]
#[command(
    version,
    about = "Root helper that performs node operations for fabercp"
)]
struct Args {
    /// Root-owned env file with the uid range and paths.
    #[arg(long, env = "FABER_NODE_ENV_FILE", default_value = DEFAULT_ENV_FILE)]
    env_file: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    #[cfg(target_os = "linux")]
    {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(linux::run(&args.env_file))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = args;
        anyhow::bail!("faber-node runs on Linux only")
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::sync::Arc;

    use anyhow::Context;
    use faber_cri::GrpcCri;
    use faber_network_management::SystemRunner;
    use faber_node::config::NodeConfig;
    use faber_node::listener;
    use faber_node::node::Node;
    use faber_node::peer::Authorizer;
    use faber_node::volume::{CommandMounter, HostVolumes};

    /// The env file decides what pods may map, so it must be root's alone.
    fn read_config(path: &Path) -> anyhow::Result<NodeConfig> {
        let meta =
            std::fs::metadata(path).with_context(|| format!("reading {}", path.display()))?;
        if meta.uid() != 0 || meta.mode() & 0o022 != 0 {
            anyhow::bail!(
                "{} must be owned by root and not writable by group or others",
                path.display()
            );
        }
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        NodeConfig::parse(&text).with_context(|| format!("parsing {}", path.display()))
    }

    pub async fn run(env_file: &Path) -> anyhow::Result<()> {
        let config = read_config(env_file)?;
        let cri = GrpcCri::connect(&config.containerd_socket)
            .await
            .context("connecting to containerd")?;
        let runner = Arc::new(SystemRunner);
        let volumes = HostVolumes::new(
            config.volume_root.clone(),
            config.faber_gid,
            runner.clone(),
            Arc::new(CommandMounter::new(runner)),
        );
        std::fs::create_dir_all(&config.volume_root).context("creating the volume root")?;
        let node = Arc::new(Node::new(
            config.uid_range,
            config.runtime_handler.clone(),
            cri,
            Arc::new(volumes),
        ));
        let listener = listener::bind(&config)?;
        tracing::info!(socket = %config.socket.display(), "faber-node listening");

        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            served = listener::serve(listener, node, Authorizer::new(config.faber_uid)) => served,
            _ = term.recv() => Ok(()),
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    }
}
