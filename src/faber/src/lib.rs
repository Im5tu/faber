//! `faber`: command-line client for the faber control plane (`fabercp`).

pub mod deploy;
mod output;

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use faber_api_types::workload::{CreateProjectRequest, DeployRequest, WorkloadQuery, fqdn};
use faber_api_types::{ClientError, CreateInviteRequest, ErrorBody, FaberClient};

pub use output::OutputFormat;

fn default_endpoint() -> String {
    format!("http://127.0.0.1:{}", faber_api_types::DEFAULT_PORT)
}

#[derive(Debug, Clone, PartialEq, Eq, Parser)]
#[command(
    name = "faber",
    version,
    about = "Command-line client for the faber control plane"
)]
pub struct Cli {
    /// Base URL of the local fabercp API (a bare host:port gets http:// prepended).
    #[arg(
        short,
        long,
        global = true,
        env = "FABER_ENDPOINT",
        default_value_t = default_endpoint()
    )]
    pub endpoint: String,

    /// Output format.
    #[arg(short, long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Check that the node is up.
    Health,
    /// Inspect this node's cluster, create one, or join one.
    #[command(subcommand)]
    Cluster(ClusterCommand),
    /// Manage the WireGuard mesh.
    #[command(subcommand)]
    Mesh(MeshCommand),
    /// Manage projects and their environments.
    #[command(subcommand)]
    Project(ProjectCommand),
    /// Deploy workloads to one environment of a project.
    ///
    /// A deploy creates and updates the workloads in the input and leaves
    /// the other workloads of the environment alone, unless --prune is set.
    /// When the input has an error, every error is printed and nothing is
    /// deployed.
    #[command(subcommand)]
    Deploy(DeployCommand),
    /// List, inspect and remove deployed workloads.
    #[command(subcommand)]
    App(AppCommand),
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ProjectCommand {
    /// Create a project.
    Create {
        /// The name of the project: a DNS label.
        #[arg(value_name = "NAME")]
        name: String,
        /// An environment the project starts with: a DNS label. May be
        /// repeated. Without it the project gets staging and production.
        #[arg(long = "env", value_name = "ENV")]
        environments: Vec<String>,
    },
    /// List the projects.
    Ls,
    /// Show a project and its environments.
    Show {
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Remove a project. Its environments must hold no workloads.
    Rm {
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Add or remove an environment of a project.
    #[command(subcommand)]
    Env(ProjectEnvCommand),
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ProjectEnvCommand {
    /// Add an environment to a project.
    Add {
        #[arg(value_name = "PROJECT")]
        project: String,
        /// The name of the new environment: a DNS label.
        #[arg(value_name = "ENV")]
        environment: String,
    },
    /// Remove an environment that holds no workloads.
    Rm {
        #[arg(value_name = "PROJECT")]
        project: String,
        #[arg(value_name = "ENV")]
        environment: String,
    },
}

/// The project and environment a command works in.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct Target {
    /// The project. There is no default.
    #[arg(long, value_name = "PROJECT")]
    pub project: String,
    /// The environment of the project. There is no default.
    #[arg(long = "env", value_name = "ENV")]
    pub environment: String,
}

/// The flags every `deploy` subcommand takes.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct DeployOptions {
    /// Make this workload reachable from every workload in the cluster,
    /// not only from its own project and environment. May be repeated.
    #[arg(long, value_name = "WORKLOAD")]
    pub expose: Vec<String>,
    /// Also remove the workloads of the environment that the input does
    /// not have.
    #[arg(long)]
    pub prune: bool,
    /// Print the converted workloads as JSON and send nothing. `deploy spec`
    /// reads that JSON back.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum DeployCommand {
    /// Deploy workloads written in faber's own format.
    Spec {
        /// A JSON object of workload specs by name, as --dry-run prints it,
        /// or - for stdin.
        #[arg(value_name = "FILE|-")]
        file: PathBuf,
        #[command(flatten)]
        target: Target,
        #[command(flatten)]
        options: DeployOptions,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum AppCommand {
    /// List the workloads, of every project unless a filter is given.
    Ls {
        /// Only the workloads of this project.
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
        /// Only the workloads of this environment.
        #[arg(long = "env", value_name = "ENV")]
        environment: Option<String>,
    },
    /// Show one workload, with env values and the status of each replica.
    Show {
        #[arg(value_name = "WORKLOAD")]
        workload: String,
        #[command(flatten)]
        target: Target,
    },
    /// Remove workloads, one after the other, stopping at the first failure.
    Rm {
        #[arg(value_name = "WORKLOAD", required = true)]
        workloads: Vec<String>,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum MeshCommand {
    /// Create a single-use invite token for a new server. In text mode only
    /// the token goes to stdout, so `$(faber mesh invite ...)` captures it.
    Invite {
        /// The name the new server will hold in the mesh: a DNS label. A
        /// node that already holds it is replaced when the new server joins.
        #[arg(long, value_name = "NAME")]
        name: String,
        /// The new server's public WireGuard endpoint as existing nodes see
        /// it: an IP address with an optional port (the mesh WireGuard port
        /// by default). Hostnames are refused.
        #[arg(long, value_name = "IP[:PORT]")]
        wg_endpoint: String,
        /// How long the invite is valid: seconds, or a number with s, m, h or
        /// d. The server default is 1h and the maximum 7d.
        #[arg(long, value_name = "DURATION", value_parser = parse_ttl)]
        ttl: Option<u64>,
    },
    /// Show or set how often nodes report the status of their replicas.
    StatusInterval {
        /// Set the interval: seconds, or a number with s or m. The server
        /// accepts 1s to 300s.
        #[arg(long, value_name = "DURATION", value_parser = parse_interval)]
        set: Option<u64>,
    },
    /// Show or set the node lease duration.
    ///
    /// The lease is how long a node may go without renewing before the
    /// cluster removes it. Each renewal restarts the node's lease. Changing
    /// the duration restarts every node's lease.
    Lease {
        /// Set the lease duration: seconds, or a number with s, m, h or d.
        /// The server accepts 300s to 30d.
        #[arg(long, value_name = "DURATION", value_parser = parse_ttl)]
        set: Option<u64>,
    },
}

/// `90`, `90s`, `15m`, `1h` or `7d`, as seconds. Zero is refused.
fn parse_ttl(s: &str) -> Result<u64, String> {
    let (digits, unit) = match s.char_indices().last() {
        Some((i, c)) if c.is_ascii_alphabetic() => (&s[..i], c),
        _ => (s, 's'),
    };
    let factor = match unit {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 24 * 60 * 60,
        _ => return Err(format!("unknown unit {unit:?}; use s, m, h or d")),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err("expected a whole number with an optional s, m, h or d".into());
    }
    let n: u64 = digits
        .parse()
        .map_err(|_| "number is too large".to_string())?;
    let secs = n.checked_mul(factor).ok_or("duration is too large")?;
    if secs == 0 {
        return Err("duration must be more than zero".into());
    }
    Ok(secs)
}

/// `90` or `90s` or `2m`, as seconds, from 1 s to 300 s.
fn parse_interval(s: &str) -> Result<u64, String> {
    if s.ends_with(['h', 'd']) {
        return Err("use seconds, or a number with s or m".into());
    }
    let secs = parse_ttl(s)?;
    if !(1..=300).contains(&secs) {
        return Err("the interval must be from 1s to 300s".into());
    }
    Ok(secs)
}

/// Where the cluster key lives when neither the flag nor the variable says.
const DEFAULT_KEY_FILE: &str = "/var/lib/faber/cluster.key";

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum KeyCommand {
    /// Print the cluster key (base64) from its file on this node. The key
    /// seals env values; keep it secret. Reads the file directly, so run it
    /// as root or as the faber user.
    Export {
        /// The key file. Defaults to /var/lib/faber/cluster.key.
        #[arg(long, value_name = "PATH", env = "FABER_CLUSTER_KEY_FILE")]
        file: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ClusterCommand {
    /// Show this node's role, the leader and replication progress of each member.
    Status,
    /// Make this node a new single-node cluster.
    Init,
    /// Join the cluster that PEER belongs to.
    Join {
        /// Peer listener address of any node already in the cluster, as ip:port.
        #[arg(value_name = "PEER")]
        peer: SocketAddr,
    },
    /// Work with the cluster key.
    #[command(subcommand)]
    Key(KeyCommand),
}

/// Executes a parsed command, writing its result to `out` and any extra
/// details to the process's stderr.
pub async fn run(cli: Cli, out: &mut impl Write) -> anyhow::Result<()> {
    run_with(cli, out, &mut std::io::stderr()).await
}

/// Executes a parsed command, writing its result to `out` and extra details
/// that must not mix with the result (such as the invite's node ID next to
/// the token, or the warnings of a deploy) to `err`.
pub async fn run_with(cli: Cli, out: &mut impl Write, err: &mut impl Write) -> anyhow::Result<()> {
    run_with_inputs(cli, out, err, deploy::Inputs::from_process()).await
}

/// [`run_with`], with what a deploy reads from the machine (the current
/// directory, the variables and stdin) taken from `inputs` instead of from
/// this process.
pub async fn run_with_inputs(
    cli: Cli,
    out: &mut impl Write,
    err: &mut impl Write,
    mut inputs: deploy::Inputs,
) -> anyhow::Result<()> {
    let format = cli.output;
    // No request: the key is read from its file.
    if let Command::Cluster(ClusterCommand::Key(KeyCommand::Export { file })) = &cli.command {
        let path = file
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_KEY_FILE));
        let key = read_key(&path).await?;
        out.write_all(output::cluster_key(format, &key)?.as_bytes())?;
        out.flush()?;
        return Ok(());
    }
    let client = FaberClient::new(&cli.endpoint)
        .with_context(|| format!("invalid endpoint {}", cli.endpoint))?;
    let endpoint = client.base().to_string();
    let api = |e: ClientError| describe_error(e, &endpoint);

    let rendered = match cli.command {
        Command::Health => output::health(format, &client.health().await.map_err(api)?)?,
        Command::Cluster(cmd) => {
            let status = match cmd {
                ClusterCommand::Status => client.cluster_status().await,
                ClusterCommand::Init => client.cluster_init().await,
                ClusterCommand::Join { peer } => client.cluster_join(&peer.to_string()).await,
                ClusterCommand::Key(_) => unreachable!("handled before the client"),
            }
            .map_err(api)?;
            output::status(format, &status)?
        }
        Command::Mesh(MeshCommand::Invite {
            name,
            wg_endpoint,
            ttl,
        }) => {
            let req = CreateInviteRequest {
                name,
                endpoint: wg_endpoint,
                ttl_secs: ttl,
            };
            let invite = client.mesh_invite(&req).await.map_err(api)?;
            let (stdout, details) = output::invite(format, &invite)?;
            err.write_all(details.as_bytes())?;
            err.flush()?;
            stdout
        }
        Command::Mesh(MeshCommand::Lease { set }) => {
            let lease = match set {
                Some(secs) => client.mesh_set_lease(secs).await,
                None => client.mesh_lease().await,
            }
            .map_err(api)?;
            output::lease(format, &lease)?
        }
        Command::Mesh(MeshCommand::StatusInterval { set }) => {
            let interval = match set {
                Some(secs) => client.mesh_set_status_interval(secs).await,
                None => client.mesh_status_interval().await,
            }
            .map_err(api)?;
            output::status_interval(format, &interval)?
        }
        Command::Project(cmd) => match cmd {
            ProjectCommand::Create { name, environments } => {
                let req = CreateProjectRequest {
                    name,
                    // None leaves the choice to the server.
                    environments: (!environments.is_empty()).then_some(environments),
                };
                output::project(format, &client.project_create(&req).await.map_err(api)?)?
            }
            ProjectCommand::Ls => {
                output::projects(format, &client.project_list().await.map_err(api)?)?
            }
            ProjectCommand::Show { name } => {
                output::project(format, &client.project_get(&name).await.map_err(api)?)?
            }
            ProjectCommand::Rm { name } => {
                client.project_delete(&name).await.map_err(api)?;
                output::project_removed(format, &name)?
            }
            ProjectCommand::Env(ProjectEnvCommand::Add {
                project,
                environment,
            }) => {
                let project = client.environment_add(&project, &environment).await;
                output::project(format, &project.map_err(api)?)?
            }
            ProjectCommand::Env(ProjectEnvCommand::Rm {
                project,
                environment,
            }) => {
                let removed = client.environment_remove(&project, &environment).await;
                removed.map_err(api)?;
                output::environment_removed(format, &project, &environment)?
            }
        },
        Command::Deploy(cmd) => {
            let DeployCommand::Spec {
                file,
                target,
                options,
            } = &cmd;
            let plan = match deploy::plan(file, &options.expose, options.prune, &mut inputs) {
                Ok(plan) => plan,
                Err(failure) => {
                    let lines = output::failure_text(&failure.errors);
                    err.write_all(lines.as_bytes())?;
                    err.flush()?;
                    bail!("nothing was deployed");
                }
            };
            let details = output::DeployDetails {
                files: options.dry_run.then_some(&plan.files[..]),
                notes: &plan.notes,
            };
            // Before anything is sent, so the notes are seen even if the
            // request fails.
            err.write_all(details.text(format).as_bytes())?;
            err.flush()?;
            let (project, environment) = (&target.project, &target.environment);
            if options.dry_run {
                output::dry_run(format, project, environment, &plan.workloads, &details)?
            } else {
                let req = DeployRequest {
                    workloads: plan.workloads,
                    prune: options.prune,
                };
                let result = match client.deploy(project, environment, &req).await {
                    Ok(result) => result,
                    Err(e) => {
                        // JSON mode has no document on stdout now, so
                        // what it would have carried goes here.
                        err.write_all(details.failure_line(format)?.as_bytes())?;
                        err.flush()?;
                        return Err(api(e));
                    }
                };
                output::deployed(format, project, environment, &result, &details)?
            }
        }
        Command::App(AppCommand::Ls {
            project,
            environment,
        }) => {
            let query = WorkloadQuery {
                project,
                environment,
            };
            let list = client.workload_list(&query, false).await.map_err(api)?;
            output::workloads(format, &list)?
        }
        Command::App(AppCommand::Show { workload, target }) => {
            // Env values are always shown, so they are always asked for.
            let found = client
                .workload_get(&target.project, &target.environment, &workload, true)
                .await;
            output::workload(format, &found.map_err(api)?)?
        }
        Command::App(AppCommand::Rm { workloads, target }) => {
            let (project, environment) = (&target.project, &target.environment);
            let mut removed = Vec::new();
            let mut failed = None;
            for workload in &workloads {
                match client.workload_delete(project, environment, workload).await {
                    Ok(()) => removed.push(fqdn(workload, environment, project)),
                    Err(e) => {
                        failed = Some(api(e));
                        break;
                    }
                }
            }
            if let Some(e) = failed {
                // What was removed before the failure is still reported.
                if !removed.is_empty() {
                    out.write_all(output::workloads_removed(format, &removed)?.as_bytes())?;
                    out.flush()?;
                }
                return Err(e);
            }
            output::workloads_removed(format, &removed)?
        }
    };

    out.write_all(rendered.as_bytes())?;
    out.flush()?;
    Ok(())
}

/// The most a key file may hold: a base64 key is 44 bytes.
const MAX_KEY_FILE_BYTES: u64 = 4096;

/// Whether both are the same file on disk. Where the platform gives no file
/// identity, this cannot tell and says yes.
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (a.dev(), a.ino()) == (b.dev(), b.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        true
    }
}

/// The key in `path` as canonical base64. The path must be a regular file
/// and not a symlink, at most [`MAX_KEY_FILE_BYTES`] long, and hold the
/// base64 of a whole key. No error here carries any of the file's content.
/// The key stays in zeroizing buffers from the read on.
async fn read_key(path: &std::path::Path) -> anyhow::Result<zeroize::Zeroizing<String>> {
    use tokio::io::AsyncReadExt;

    let shown = output::esc(&path.display().to_string());
    let unreadable = |e: std::io::Error| {
        anyhow!(
            "cannot read the cluster key file {shown}: {e} (run as root or faber, or pass --file)"
        )
    };
    // The link itself is looked at, never what it points to.
    let meta = tokio::fs::symlink_metadata(path)
        .await
        .map_err(unreadable)?;
    if !meta.is_file() {
        bail!("the cluster key file {shown} is not a regular file");
    }
    let file = tokio::fs::File::open(path).await.map_err(unreadable)?;
    // The path may have been swapped since the check above: what was opened
    // must be the file that was looked at.
    let opened = file.metadata().await.map_err(unreadable)?;
    if !opened.is_file() || !same_file(&meta, &opened) {
        bail!("the cluster key file {shown} is not a regular file");
    }
    // Sized for the longest read, so the buffer never regrows and leaves a
    // plain copy of the key behind.
    let cap = MAX_KEY_FILE_BYTES + 1;
    let mut bytes = zeroize::Zeroizing::new(Vec::with_capacity(cap as usize));
    // One byte over the cap tells a long file from one of exactly the cap.
    file.take(cap)
        .read_to_end(&mut bytes)
        .await
        .map_err(unreadable)?;
    if bytes.len() as u64 > MAX_KEY_FILE_BYTES {
        bail!("the cluster key file {shown} is too large (over {MAX_KEY_FILE_BYTES} bytes)");
    }
    let text = std::str::from_utf8(&bytes[..])
        .map_err(|_| anyhow!("the cluster key file {shown} does not hold a valid cluster key"))?
        .trim();
    if text.is_empty() {
        bail!("the cluster key file {shown} is empty");
    }
    let key = faber_crypto::ClusterKey::from_base64(text)
        .map_err(|_| anyhow!("the cluster key file {shown} does not hold a valid cluster key"))?;
    Ok(key.to_base64())
}

/// Turns a client error into a message a person can act on.
fn describe_error(err: ClientError, endpoint: &str) -> anyhow::Error {
    match err {
        ClientError::Api { body, .. } => anyhow!(describe_body(&body)),
        // The text of either error can repeat what the server sent, so it is
        // escaped like everything else that came from there.
        ClientError::Http(e) => {
            let cause = output::esc(&format!("{:#}", anyhow!(e)));
            anyhow!("request to {} failed: {cause}", output::esc(endpoint))
        }
        ClientError::Decode(e) => {
            let cause = output::esc(&e.to_string());
            let endpoint = output::esc(endpoint);
            anyhow!("could not decode the response from {endpoint}: {cause}")
        }
        // Refused by the client before anything was sent.
        ClientError::Invalid(message) => anyhow!("invalid name: {}", output::esc(&message)),
    }
}

/// `code: message (leader: id@addr)`, leaving out the parts that are absent.
fn describe_body(body: &ErrorBody) -> String {
    // Both come from the server, which repeats names it was given.
    let mut msg = output::esc(&body.error);
    if let Some(m) = body.message.as_deref().filter(|m| !m.is_empty()) {
        msg.push_str(": ");
        msg.push_str(&output::esc(m));
    }
    if body.leader_id.is_some() || body.leader_addr.is_some() {
        msg.push_str(&format!(
            " (leader: {})",
            output::leader(body.leader_id, body.leader_addr.as_deref())
        ));
    }
    msg
}
