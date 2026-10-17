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
    use faber_node::config::{NodeConfig, root_only_writable};
    use faber_node::dirs::DirPolicy;
    use faber_node::host_net::IpHostNetworks;
    use faber_node::listener;
    use faber_node::node::{Node, RuntimeHost};
    use faber_node::peer::Authorizer;
    use faber_node::volume::{CommandMounter, HostVolumes, ToolPaths};

    /// The env file decides what pods may map, so it must be root's alone.
    fn read_config(path: &Path) -> anyhow::Result<NodeConfig> {
        let meta =
            std::fs::metadata(path).with_context(|| format!("reading {}", path.display()))?;
        if !root_only_writable(meta.uid(), meta.mode()) {
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
        let tools = ToolPaths {
            bin_dir: config.bin_dir.clone(),
            sbin_dir: config.sbin_dir.clone(),
        };
        std::fs::create_dir_all(&config.volume_root).context("creating the volume root")?;
        // Volumes are made and removed by path under here; anyone who could
        // write the root could redirect root's file operations.
        let meta =
            std::fs::symlink_metadata(&config.volume_root).context("checking the volume root")?;
        if !meta.is_dir() || !root_only_writable(meta.uid(), meta.mode()) {
            anyhow::bail!(
                "{} must be a directory owned by root and not writable by group or others",
                config.volume_root.display()
            );
        }
        // Root loads what is written here (containerd config, CNI config) and
        // runs the CNI plugins, so nobody else may be able to replace any of
        // it, or any directory above it. The config directories are made if
        // they are missing; the plugin directory is the installer's.
        let policy = DirPolicy::root();
        let containerd = config.containerd();
        for dir in [&containerd.conf_dir, &containerd.cni_conf_dir] {
            policy
                .create_if_missing(dir)
                .with_context(|| format!("creating {}", dir.display()))?;
        }
        policy.check_runtime_dirs(&containerd).map_err(|e| {
            anyhow::anyhow!(
                "the {} cannot be trusted: {} (every directory of the path must be owned by \
                 root and not writable by group or others)",
                e.role,
                e.detail
            )
        })?;
        let networks = IpHostNetworks::new(runner.clone(), &tools);
        let volumes = HostVolumes::new(
            config.volume_root.clone(),
            config.faber_gid,
            tools.clone(),
            runner.clone(),
            Arc::new(CommandMounter::new(tools, runner)),
        );
        let node = Arc::new(Node::new(
            config.uid_range,
            config.runtime_handler.clone(),
            cri,
            Arc::new(volumes),
            RuntimeHost {
                containerd,
                dirs: policy,
                networks: Arc::new(networks),
                mesh_interface: config.mesh_interface.clone(),
            },
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
