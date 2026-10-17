# faber

faber runs containers on a small cluster of Linux servers. The servers form a WireGuard mesh and keep their state in a Raft cluster. Each deployed service gets a stable virtual IP and a DNS name, `<service>.<env>.<project>.faber.local`.

It is early software. Clusters made with one version may not work with the next, and there is no upgrade path yet.

## What it does

- **Mesh:** the first server creates a WireGuard mesh, and each new server joins with a single-use invite token. A server that stops renewing its lease is removed.
- **Workloads:** you deploy JSON specs to an environment of a project. The leader places replicas across the nodes, spread or packed, or one per node (`daemon`), and stores the result in Raft. containerd runs them through CRI.
- **Services:** every service gets a virtual IP from the service range, programmed in nftables on every node. DNS inside fabercp answers the `.faber.local` names and forwards the rest upstream.
- **Deploys:** a new revision takes over once it is available (blue/green). Workloads with block volumes roll one replica at a time.
- **Volumes:** `block`, a fixed-size filesystem image local to its node, or `ramdisk`, a tmpfs that goes away with the pod.
- **Probes and restarts:** `tcp` or `http(s)` probes. One failure takes a replica out of the service, and three in a row restart it. The restart policies are `always`, `on_failure` and `never`.
- **Secrets:** every env value is encrypted at rest with a cluster key, and the Raft log never holds plaintext.
- **Isolation:** each service runs in its own user namespace, mapped to a host uid range of its own. Containers are unprivileged, drop all capabilities except the ones you allow, and can't reach the internet.

## Requirements

- Linux 6.3 or later on each node.
- containerd 2.0 or later. The installer installs or upgrades it.
- A UDP port the other nodes can reach for WireGuard.

## Install

### Download a release

Each release has a tarball for Linux `x86_64` and `aarch64`. It holds `fabercp`, `faber`, `faber-node` and `install.sh`. Download one from the [releases page](https://github.com/Im5tu/faber/releases), or with the GitHub CLI:

```sh
gh release download v0.1.0 --repo Im5tu/faber --pattern '*linux-x86_64*'
tar -xzf faber-0.1.0-linux-x86_64.tar.gz
cd faber-0.1.0-linux-x86_64
```

The binaries are already next to `install.sh`, so skip to the installer step below.

### Verify a release

Every asset has a signed build provenance attestation, and each tarball has a signed SBOM (CycloneDX). Check them with the GitHub CLI:

```sh
gh release verify v0.1.0 --repo Im5tu/faber
gh release verify-asset v0.1.0 faber-0.1.0-linux-x86_64.tar.gz --repo Im5tu/faber
gh attestation verify faber-0.1.0-linux-x86_64.tar.gz --repo Im5tu/faber \
  --signer-workflow Im5tu/faber/.github/workflows/_build.yml
sha256sum -c SHA256SUMS --ignore-missing
```

Add `--predicate-type https://cyclonedx.org/bom` to the attestation check to verify the SBOM instead. The binaries also carry their dependency list, so `cargo audit bin fabercp` works on them.

### Build from source

Build the three binaries on Linux:

```sh
cargo build --release -p fabercp -p faber -p faber-node
```

### Install on the first server

Put `fabercp`, `faber` and `faber-node` next to `src/install.sh` on the first server, then create the mesh:

```sh
sudo ./install.sh --name node1
```

The installer installs containerd, WireGuard, nftables and the CNI plugins, creates the `faber` user, installs the systemd units and runs `faber cluster init`. Use `--check` to see what it detects, or `--dry-run` to print what it would change. `./install.sh --help` lists the address-range options (`--mesh-cidr`, `--container-cidr`, `--service-cidr`).

To add a server, create an invite on a node that's already in the mesh:

```sh
faber mesh invite --name node2 --wg-endpoint 203.0.113.12 > token
```

Then run the installer on the new server, with the token on stdin:

```sh
sudo ./install.sh --join - < token
```

## Deploy

Create a project. Without `--env` it gets `staging` and `production`:

```sh
faber project create shop
```

Write a spec. This one runs redis with two replicas, a block volume and a probe. `command` skips the image's entrypoint, which would switch to a user that can't write the volume:

```json
{
  "redis": {
    "scale": { "replicas": { "count": 2 } },
    "volumes": {
      "data": { "type": "block", "size_bytes": 268435456 }
    },
    "containers": [{
      "name": "redis",
      "image": "redis:7-alpine",
      "command": ["redis-server"],
      "args": ["--appendonly", "yes", "--dir", "/data"],
      "ports": [{ "name": "redis", "port": 6379 }],
      "mounts": [{ "volume": "data", "path": "/data" }],
      "probe": { "action": { "type": "tcp", "port": 6379 } }
    }]
  }
}
```

Deploy it and follow the rollout:

```sh
faber deploy spec redis.json --project shop --env staging
faber app show redis --project shop --env staging --watch
```

Other containers reach it at `redis.staging.shop.faber.local:6379`.

## Commands

| Command | Does |
|---|---|
| `faber health` | Check that the local node is up |
| `faber cluster status` | Show the node's role, the leader and replication progress |
| `faber cluster key export` | Print the cluster key; run as root or `faber` |
| `faber mesh invite` | Create a single-use invite for a new server |
| `faber mesh lease`, `faber mesh status-interval` | Show or set the node lease and the status report interval |
| `faber project create\|ls\|show\|rm` | Manage projects |
| `faber project env add\|rm` | Manage a project's environments |
| `faber deploy spec` | Deploy workloads; `--prune` removes the ones not in the file, `--dry-run` sends nothing |
| `faber app ls` | List workloads with their available and desired replicas |
| `faber app show` | Show a workload with env values and one row per running replica; `--watch` redraws on change |
| `faber app rm` | Remove workloads |

Every command takes `--output json`. The CLI talks to the fabercp API on the same node, at `127.0.0.1` unless `--endpoint` says otherwise.

## Not there yet

- **No auth on the local API:** any user on a node can use it and read env values through it.
- **No ingress yet:** services can't be reached from outside the cluster (#3).
- **No container egress yet:** containers can't reach the internet (#2).
- **MCP:** the endpoint exists but has no tools (#4).
- **Image pulls are anonymous only.**
- **No logs, exec, metrics or web UI.**
- **Block volumes:** they live on one node, with no backup or move.

## Repository

| Path | Contents |
|---|---|
| `src/fabercp` | Control plane: Raft, REST API, placement, reconcile loop, DNS, runner |
| `src/faber-node` | Root helper for CRI calls, volumes and mounts, over a Unix socket |
| `src/faber` | CLI |
| `src/install.sh` | Node installer |
| `crates/` | API types, node protocol, CRI client, image pulls, encryption, WireGuard and nftables |
| `docs/contract.md` | The names, paths and formats shared between processes and scripts |
| `tests/install`, `tests/e2e` | Installer tests (bats) and the end-to-end suite |

`CLAUDE.md` has the build and test commands.

## License

MIT or Apache-2.0, as set in `Cargo.toml`.
