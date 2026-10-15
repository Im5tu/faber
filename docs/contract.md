# Mesh and workload contract

Shared names and formats for the WireGuard mesh, the container network, node leases, the containerd config, and projects, environments and workloads. Code is the source of truth for types; this file pins everything that crosses a process, file or script boundary.

All nodes in a cluster must run the same fabercp version. The raft log carries the mesh commands and the project and workload commands, and older builds cannot decode them.

fabercp stores workloads and does not run them yet. Nothing in this version starts a container from a workload record.

## Crate layout

| Crate | Path | Holds |
|---|---|---|
| `fabercp` | `src/fabercp` | raft, API, mesh state, node leases, project and workload state, and the reconcile loop |
| `faber` | `src/faber` | CLI. Depends on `faber-api-types` and `faber-workload-import` |
| `faber-api-types` | `crates/api-types` | DTOs, the workload model and its validation, error codes, HTTP client |
| `faber-network-management` | `crates/network-management` | WireGuard (`ip`, `wg`) and nftables (`nft`) |
| `faber-container-runtime` | `crates/container-runtime` | the runtime spec, the containerd drop-in writer and the CNI network config writer |

fabercp does not know how WireGuard, nftables or containerd are driven. It builds two serde shapes from raft state and passes each to a `reconcile` call that applies it idempotently.

- `NetworkSpec { interface, wireguard: WireGuardSpec, firewall: FirewallSpec }` goes to `NetworkManager::reconcile`, which brings WireGuard up and then loads `inet faber`. `Debug` redacts the private key. `reconcile` uses an existing link as it finds it. Whether fabercp may use that link is decided beforehand with `NetworkManager::create_interface`, which fails if the link already exists (see "Interface ownership").
  - `WireGuardSpec { private_key, listen_port, address, peers, routes }`. `routes` lists the networks that `ip route replace` sends into the interface. It defaults to empty when absent.
  - `WireGuardPeer { public_key, endpoint?, allowed_ips, keepalive }`. `allowed_ips` is a list of IPv4 networks, rendered as one `AllowedIPs = <net>, <net>` line in list order. A peer with an empty list gets no `AllowedIPs` line.
  - `FirewallSpec { wg_port, peer_port, peer_endpoint_ips, peer_mesh_ips, containers? }`. `containers` is `ContainerFirewall { cidr, local_subnet?, node_mesh_ips, bridge }`: the container range, this node's subnet once it has one, the mesh IPs of the other registered nodes, and the name of the container bridge (`faberbr0`). The bridge name goes into the nft script as it is, so `apply` refuses one that is not a valid interface name, and one that equals the mesh interface name. Without `containers` the table has no `node_mesh_ips` set and a forward chain that only drops.
- `NodeRuntimeSpec { settings: ContainerRuntimeSpec, pod_network: PodNetworkSpec? }` goes to `ContainerRuntime::reconcile`, which returns whether the host changed. `settings` is `ContainerRuntimeSpec { systemd_cgroup, sandbox_image, cni_bin_dir, cni_conf_dir }`, replicated as it is. `pod_network` is `PodNetworkSpec { bridge, subnet, mtu }` and is absent until raft has given the node a subnet. `Containerd { conf_dir, cni_conf_dir }` is the one implementation: `conf_dir` is where it writes the drop-in, `cni_conf_dir` where it writes the CNI network config. The trait has a second method, `remove_pod_network`, for a node that is leaving the mesh. It removes the CNI network config and nothing else, needs no settings, does not read or write the drop-in, and returns whether it removed a file. Another runtime is another implementation of the trait, with no change to fabercp.

`Containerd.cni_conf_dir` comes from `FABER_CNI_CONF_DIR` and is the directory this host writes to. `ContainerRuntimeSpec.cni_conf_dir` is replicated and is the directory containerd is told to read. fabercp logs a warning when the two differ, because containerd would then never see the file.

`fabercp::reconcile` holds the mapping (`desired_network`, `desired_runtime`) and one loop. The loop runs after a state change, once changes have been quiet for 1s or 5s after the first one, and every `FABER_RECONCILE_INTERVAL_SECS`. It also wakes when the earliest live invite expires, so an expired invite leaves the wg peer list and the nft sets within a second or so. Each run calls `network.reconcile` and then `runtime.reconcile`. A failure in one does not skip the other. Startup makes the same two calls before the peer listener binds, and there a failure exits. The loop starts from the state startup applied, so startup applies it once.

`faber-network-management` owns `CommandRunner`, `SystemRunner` and the recording fake that tests inject.

## Files

| Path | Owner:group | Mode | Written by | Contents |
|---|---|---|---|---|
| `/etc/faber/` | root:faber | 0750 | install.sh | directory |
| `/etc/faber/fabercp.env` | root:faber | 0640 | install.sh | env vars below |
| `/etc/faber/join.token` | root:faber | 0640 | install.sh (mktemp, chmod/chgrp, `mv -fT`) | the invite token; install.sh removes it once the node has joined |
| `/var/lib/faber/` | faber:faber | 0700 | systemd `StateDirectory=faber`; fabercp creates it (0700) if it is missing | state dir |
| `/var/lib/faber/identity.json` | faber:faber | 0600 | fabercp, temp file + rename | see below |
| `/var/lib/faber/iface` | faber:faber | 0644 | fabercp, temp file + rename | name of the WireGuard interface fabercp created, one line. Removed together with the interface when an invite is rejected |
| `/var/lib/faber/join-failed` | faber:faber | 0644 | fabercp, after a rejected invite or a removal from the mesh. `install.sh --join <token> --reset`, staged in a root-only directory and renamed into place with `mv -fT` | empty marker: the identity and raft data are to be removed at the next start, see "Rejected invite" |
| `/var/lib/faber/bootstrap-error` | faber:faber | 0600 | fabercp, temp file + rename | why this node is not in its mesh: one fixed code and a newline, see "bootstrap-error" |
| `/var/lib/faber/data/` | faber:faber | 0700 | fabercp | RocksDB (`FABER_DATA_DIR`) |
| `/etc/containerd/config.toml` | root:root | 0644 | install.sh | root config |
| `/etc/containerd/conf.d/` | faber:faber | 0755 | install.sh | drop-in dir |
| `/etc/containerd/conf.d/faber.toml` | faber:faber | 0644 | fabercp, temp file + rename, only on change | drop-in |
| `/etc/cni/net.d/` | faber:faber | 0755 | install.sh, which refuses a symlink or a non-directory at that path and touches nothing inside | CNI config dir (`CNI_CONF_DIR` for install.sh, `FABER_CNI_CONF_DIR` for fabercp) |
| `/etc/cni/net.d/10-faber.conflist` | faber:faber | 0644 | fabercp, temp file + rename, only on change. fabercp removes it while the node has no container subnet | CNI network config, see "Container network" |

containerd loads the lexically first config in `/etc/cni/net.d`. install.sh warns and names every `.conf`, `.conflist` or `.json` file there that is not `10-faber.conflist`.

"Bootstrapped" means `/var/lib/faber/identity.json` is a regular file and `fabercp.env` has no pending bootstrap line (`FABER_JOIN_TOKEN_FILE`, `FABER_NEW_MESH`, `FABER_MESH_CIDR`, `FABER_NODE_NAME`, `FABER_CONTAINER_CIDR`). install.sh strips those lines and removes `join.token` only after the cluster was created or joined. Until then a restart still carries them, and fabercp ignores them because an existing identity wins. The one exception is a join that is still pending when the token file holds a different invite (see "Replacing a pending join").

fabercp writes `identity.json` before it serves the API, so a node that answers `faber health` without an `identity.json` has had its invite rejected.

## Environment (`fabercp.env`)

Each variable maps to a fabercp flag.

| Variable | Flag | Value |
|---|---|---|
| `FABER_WG_INTERFACE` | `--wg-interface` | `faber0` (also the default) |
| `FABER_STATE_DIR` | `--state-dir` | `/var/lib/faber`. In mesh mode it must not be `FABER_DATA_DIR` or a directory inside it, or startup fails: discarding a join deletes everything in the data dir |
| `FABER_DATA_DIR` | `--data-dir` | `/var/lib/faber/data` |
| `FABER_WG_ENDPOINT` | `--wg-endpoint` | this node's public `ip:port`. A bare IP gets `FABER_WG_PORT`. A hostname or anything else that is not an IP makes startup fail |
| `FABER_WG_PORT` | `--wg-port` | `51820` |
| `FABER_MANAGE_HOST` | `--manage-host` | `1` |
| `FABER_CONTAINERD_CONF_DIR` | `--containerd-conf-dir` | `/etc/containerd/conf.d` |
| `FABER_CNI_CONF_DIR` | `--cni-conf-dir` | `/etc/cni/net.d` (also the default). Where this node writes `10-faber.conflist`. install.sh always writes it |
| `FABER_NEW_MESH` | `--new-mesh` | `1`; unconfigured founder only |
| `FABER_MESH_CIDR` | `--mesh-cidr` | default `10.99.0.0/16`; with `FABER_NEW_MESH` only |
| `FABER_CONTAINER_CIDR` | `--container-cidr` | default `10.200.0.0/16`; with `FABER_NEW_MESH` only. install.sh writes it only when `--container-cidr` is given |
| `FABER_NODE_NAME` | `--node-name` | the founder's name; with `FABER_NEW_MESH` only. fabercp falls back to `node-1` when it is unset. install.sh always writes it for a new mesh |
| `FABER_JOIN_TOKEN_FILE` | `--join-token-file` | `/etc/faber/join.token`; an unconfigured joiner, or a joiner replacing a join that is still pending |
| `FABER_RECONCILE_INTERVAL_SECS` | `--reconcile-interval-secs` | seconds between full host re-applies, default `300`, at least `1`; not written by install.sh |

`--node-id` becomes optional and is read from the identity. Dev mode (no `--manage-host`, no identity) still requires it.

`FABER_CONTAINER_CIDR` and `FABER_NODE_NAME` are bootstrap settings, like `FABER_MESH_CIDR`. fabercp checks them at startup only when `FABER_NEW_MESH` is set, and a value that fails stops startup:

- `FABER_CONTAINER_CIDR` must be an IPv4 CIDR with a prefix length of at most 24 (every node gets a /24 of it), with no host bits set, and it must not overlap `FABER_MESH_CIDR`. `InitCluster` rejects the same ranges, but by then raft is initialised and the cluster could not be seeded, so the check runs before anything starts. The range is fixed once the mesh exists. A joiner and a configured node take it from the replicated state and ignore the variable.
- `FABER_NODE_NAME` must be a DNS label: 1 to 63 characters of `a-z`, `0-9` and `-`, with no hyphen at either end. fabercp does not store it in `identity.json`. It reads it when `faber cluster init` runs, so the variable must still be set then. Every other node gets its name from its invite.

install.sh makes the same checks on `--container-cidr` and `--name` before it changes anything, and removes both lines with the other bootstrap lines once the cluster is created.

## identity.json

Written before the API is served on first start. Contains the private key.

```json
{
  "v": 1,
  "node_id": 2,
  "private_key": "<base64 wg private key>",
  "mesh_ip": "10.99.0.2",
  "mesh_cidr": "10.99.0.0/16",
  "wg_port": 51820,
  "endpoint": "198.51.100.2:51820",
  "instance_id": "<32 hex chars>",
  "pending_join": {
    "invite_id": "<32 hex chars>",
    "join_addrs": ["10.99.0.1:7401"],
    "bootstrap_peers": [
      {"node_id": 1, "public_key": "<base64>", "endpoint": "203.0.113.1:51820", "mesh_ip": "10.99.0.1"}
    ],
    "registered": true
  }
}
```

- `instance_id` names this machine. A node makes it (128 random bits, lowercase hex) when it creates its identity, from a token or as the founder. A joiner sends it with every register and join request, and every node renews its lease with it. The founder's `InitCluster` records it in the founder's node record. An `identity.json` that predates the field, a joiner's or a founder's, gets one at startup.
- The node's name and container subnet are not in `identity.json`. fabercp reads both from the replicated `mesh` each time it needs them.
- `pending_join` is absent for the founder. A joiner removes it once its own raft shows this node as a voter. A 2xx answer to the join request is not enough.
- `pending_join.registered` is absent until the mesh has accepted this machine's registration (see "Joining with an invite"). It is saved before the join request is sent.
- `endpoint` is written in canonical form (what Rust's `SocketAddr` prints), for example `[2001:db8::2]:51820`. Every endpoint in the log, the token and `identity.json` uses that form, so `[2001:DB8::2]:51820` and `[2001:db8::2]:51820` are the same endpoint.
- `bootstrap_peers` has the `TokenPeer` shape. While the join is pending, these peers are WireGuard peers next to whatever part of `mesh` has replicated, each with its mesh `/32` alone, and they are never in `node_mesh_ips`. A joiner applies the log a batch at a time, and the mesh of the first batch need not have the node that leads now. A token peer whose key or mesh IP the replicated state already has is left out. Once the node sees itself as a voter, the peer set comes from `mesh` alone.
- Startup without an identity, token or `--new-mesh` exits with "node not bootstrapped".

### Rejected invite

When the mesh answers a register or join request with `invite_invalid`, `invite_used` or `invite_expired`, the node never joined and retrying cannot help. Its WireGuard interface also carries a key that belongs to an invite it may not use, possibly the key of another node. fabercp then:

1. logs an error that says how to recover,
2. refuses raft RPCs from then on (see "Joining with an invite"),
3. stops the host reconcile loop, removes `10-faber.conflist` if there is one (a `remove_cni_config` request to faber-node, which leaves the containerd drop-in alone), removes the cluster key file, deletes the WireGuard interface (`ip link show faber0`, then `ip link del faber0` if it exists) and removes `/var/lib/faber/iface`. If the delete fails, the record stays and the log says to remove the interface by hand. A conflist that cannot be removed is logged and the interface goes all the same,
4. writes `invite_rejected` to `/var/lib/faber/bootstrap-error`,
5. creates `/var/lib/faber/join-failed` and deletes `identity.json` straight away,
6. stays up but idle. It does not exit, so systemd does not restart it in a loop, and it does not recreate the identity from the old token while it runs.

At the next start, before it resolves the identity, fabercp sees the marker, deletes everything inside `FABER_DATA_DIR`, and removes the marker. The database is open until the process stops, which is why the raft data waits for the restart. The node then starts as if new. With a new `FABER_JOIN_TOKEN_FILE` it gets a new identity and empty raft storage. With the old token still in place it tries again and is rejected again.

The interface and its ownership record are gone, so the next start that has a token creates the interface again, after the same check that nobody else has made one of that name.

The teardown keeps the `inet faber` table. Containers that are still running stay behind its forward chain, which drops what they send beyond the container bridge (see "nftables").

A start that has nothing to make a new identity from stays idle as well. fabercp does not create the interface, binds no listener and leaves `bootstrap-error` as it is, when all of these hold:

- `bootstrap-error` says `invite_rejected` or `node_removed`.
- Neither `FABER_JOIN_TOKEN_FILE` nor `FABER_NEW_MESH` is set.
- There is no `identity.json`, or `join-failed` says it is to be discarded.

That start still removes what `join-failed` marks. A `bootstrap-error` next to a whole identity and no `join-failed` is a teardown that was cut short, and the node starts as itself and finds out again. Without this rule the start would create the interface and then exit for want of an identity, every few seconds under systemd. `faber health` gets no answer from such a node.

A join token that cannot work when fabercp starts gets similar treatment. That is a token that has expired (`token_expired`), or one that does not decode or does not hold a valid identity (`token_invalid`: wrong prefix or version, bad base64 or JSON, a bad key, a mesh IP outside the mesh CIDR, an endpoint that is not `ip:port`). fabercp logs the error, writes the code to `bootstrap-error` and stays up with no listeners instead of exiting, because `Restart=always` would retry a token that can never become valid. `faber health` gets no answer from such a node. Recovery is a new token and a restart.

Every other startup failure still exits, including a token file that cannot be read and a `FABER_WG_ENDPOINT` that differs from the endpoint in the token.

A node that already appears in the raft membership never discards its identity because of an `invite_*` answer. For it, that answer is retried like any other error.

### Removed node

A member the mesh has removed takes the same steps as a node whose invite was rejected, with `node_removed` in `bootstrap-error`: it refuses raft RPCs, stops the host reconcile loop, removes `10-faber.conflist`, deletes the WireGuard interface and its ownership record, writes the marker, creates `join-failed`, deletes `identity.json` and stays up idle. "Node lease" says what counts as evidence of a removal. The conflist goes because it names a subnet that is no longer this node's, and while the file is there containerd keeps giving new containers addresses from it.

A joiner can be removed before its join finishes, because nothing renews a lease while a join is pending. Its machine was off for longer than the lease, or another machine took its name. The leader then answers its join request with `mesh_node_removed`, and the joiner takes the same steps, whatever membership its own log still shows.

install.sh refuses a run without `--join` on a node whose `bootstrap-error` says `node_removed`. Such a node has no identity and nothing pending, so the run would take it for a new server and start a second, separate mesh on it. The two ways out are `--join <token> --reset` with a new invite, or, to start a new mesh on the machine on purpose, removing `/var/lib/faber/bootstrap-error` and running again. The marker counts only while it is newer than `join.token`, so a run that continues a join started after the removal is not refused.

The admin then creates a new invite on a mesh node and runs `install.sh --join <token> --reset` on the removed one. With `identity.json` already gone, `--reset` finds nothing to reset and the run is a plain join. `--reset` is what matters for a node that was removed without learning it (see "Node lease"). That node still has its identity, and fabercp never lets a new token replace the identity of a member.

`install.sh --join <token> --reset` on a node that has an identity:

1. writes the token and `fabercp.env`,
2. stops fabercp (`systemctl stop fabercp`),
3. writes the empty `join-failed` marker,
4. starts fabercp. That start sees the marker, removes `identity.json` and everything inside `FABER_DATA_DIR`, removes the marker and creates a new identity from the token.

Nothing is reset when the stored token is the one given and the identity is that of its unfinished join. That is a re-run of the same command, and a second reset would give the machine a new `instance_id` for an invite it has already registered with. `--reset` without `--join` exits 2. A node whose new mesh has not finished bootstrapping still refuses `--join`.

### Replacing a pending join

A join can get stuck without the mesh ever rejecting it. If another node used the invite, or the invite expired while this node was still joining, the other nodes' firewalls drop this node and it never receives an `invite_*` answer. It retries for ever with `pending_join` still set.

The way out is a new invite. At startup, before it resolves the identity, fabercp removes `identity.json` and everything inside `FABER_DATA_DIR` (the same steps as for a rejected invite) and then creates the identity from the new token, if all of these hold:

- `identity.json` exists and has `pending_join`.
- `FABER_JOIN_TOKEN_FILE` is set and the file can be read.
- The token in it is usable (it decodes, has not expired, and holds a valid identity whose endpoint matches `FABER_WG_ENDPOINT` if that is set) and its `invite_id` differs from `pending_join.invite_id`.
- The node is in no raft membership, as a voter or as a learner. fabercp reads this from the raft storage on disk: the last applied membership and any membership entry still in the log.

Otherwise the identity stays. With the same token the node keeps retrying the pending join. A node that is in a membership never discards its identity, whatever the token file holds.

With a token for another invite that is not usable, the node logs a warning, keeps retrying the pending join and writes the reason to `bootstrap-error`: `token_expired` for an expired token, `token_invalid` for everything else, including an endpoint that differs from `FABER_WG_ENDPOINT`. The start that writes the marker does not remove it. The operator supplied that token to get out of a stuck join, and without the marker install.sh could only report a timeout.

For install.sh this means `--join <different token>` is safe to accept while `identity.json` has `pending_join`: write the new `join.token`, then `systemctl try-restart fabercp`. If the node then joins with its old `node_id`, it was already in the cluster and fabercp ignored the new token. install.sh reads `node_id` from `identity.json` before and after to tell, and says that the new invite was not used.

### bootstrap-error

`/var/lib/faber/bootstrap-error` says why fabercp gave up on joining, or why it left the mesh. It holds exactly one of these codes followed by a newline:

| Code | Written when | Node state |
|---|---|---|
| `token_expired` | startup, the join token has expired | no identity: idle, no listeners. Pending join kept: running, still joining |
| `token_invalid` | startup, the join token does not decode or does not hold a valid identity. With a pending join kept, also a token whose endpoint differs from `FABER_WG_ENDPOINT` | as above |
| `invite_rejected` | the mesh answered a register or join request with `invite_*` | idle, listeners up, no `identity.json`, no WireGuard interface, no conflist. After a restart without a token: idle, no listeners |
| `key_mismatch` | the joiner was sent a cluster key whose id is not the one raft records (see "Encryption") | as for `invite_rejected` |
| `key_unavailable` | the cluster sent no cluster key in ten answers to the registration | as for `invite_rejected` |
| `node_removed` | the mesh removed this node: the leader answered a lease renewal or a join request with `mesh_node_removed`, or the raft membership this node has applied no longer has it as a voter | as for `invite_rejected` |

fabercp writes the file with mode 0600 through a temp file and a rename, so a reader sees a whole code or no file. For `invite_rejected` and `node_removed` the file is on disk before `identity.json` is deleted. fabercp removes the file at the next start that loads or creates an identity, unless that start wrote it about a replacement token (see "Replacing a pending join"), and when a pending join completes. A start that stays idle for want of a token leaves it in place.

install.sh reads it as root, so it must not trust the file further than this: read it only if it is a regular file and not a symlink, read at most 64 bytes, and compare the content with the codes it knows (`token_expired`, `token_invalid`, `invite_rejected` and `node_removed`; it does not know the two key codes). Anything else counts as no marker. install.sh reads the file when a join fails, and in preflight of a run without `--join` (see "Removed node"). It ignores one that is older than `join.token`, because that file is about an earlier token. For `node_removed` it tells the admin to create a new invite and rejoin with `--join <token> --reset`.

## Replicated state (`sm_data`)

| Key | Type | Written by |
|---|---|---|
| `mesh` | `mesh::MeshState` | `InitCluster`, `CreateInvite`, `RegisterNode`, `SetEndpoint`, `AdvanceClock`, `RenewLease`, `RemoveNode`, `SetLease`, and the first `ReportNode` of a node |
| `projects` | `apps::state::ProjectIndex` | `CreateProject`, `DeleteProject`, `AddEnvironment`, `RemoveEnvironment`, `ApplyWorkloads`, `RemoveWorkloads` |
| `env/<project>/<environment>` | `apps::state::EnvironmentRecord` | the same six commands |
| `keys/cluster-id` | `ClusterKeyId` | `SetClusterKeyId` |
| `keys/data/<scope>` | `DataKeyRecord` | `CreateDataKey` |
| `place/<project>/<environment>` | `PlacementRecord` | `PlaceReplicas` |
| `place-uids` | `UidAllocator` | `PlaceReplicas` |
| `status/<node_id>` | `NodeStatus` | `ReportNode` |
| `config/status_interval` | `IntervalSetting` | `SetStatusInterval` |
| `services/ips` | `ServiceIps` | `AllocateServiceIp`, `ReleaseServiceIp` |

`Request::Set` rejects every key above with `bad_request`. The reserved keys are `mesh`, `projects`, `keys/cluster-id`, `place-uids`, `config/status_interval`, and everything under `env/`, `keys/data/`, `place/`, `status/` and `services/`, whether or not a record exists under them. The snapshot format is unchanged, since snapshots are all of `sm_data`. A node builds a snapshot after every `--snapshot-logs` new log entries (`FABER_SNAPSHOT_LOGS`, default 5000) and keeps `--keep-logs` entries from before it (`FABER_KEEP_LOGS`, default 1000), so the log that status reports add stays bounded.

No record holds a plaintext env value (see "Encryption").

### Records

| Type | Fields |
|---|---|
| `MeshState` | `config`, `nodes` (by node id), `invites` (by invite id), `next_node_id`, `clock`, `lease_secs` |
| `MeshConfig` | `cidr`, `peer_port`, `wg_port`, `container_cidr?`, `container_subnet_prefix`, `service_cidr?` |
| `NodeRecord` | `node_id`, `public_key`, `mesh_ip`, `endpoint?`, `invite_id?`, `instance_id?`, `container_subnet?`, `name`, `lease_renewed_at` |
| `InviteRecord` | `invite_id`, `node_id`, `public_key`, `mesh_ip`, `endpoint`, `expires_at`, `name` |
| `ProjectIndex` | `projects` (by project name) |
| `ProjectRecord` | `created_at`, `environments` (by environment name) |
| `EnvironmentIndexEntry` | `generation`, `bytes`, `workloads`, `updated_at` |
| `EnvironmentRecord` | `generation`, `updated_at`, `workloads` (by workload name) |
| `WorkloadRecord` | `revision`, `spec` (a `WorkloadSpec` whose containers all have an empty `env`, see "Projects, environments and workloads"), `env` (by container name, a `SealedEnv`) |
| `SealedEnv` | `digest`, `values` (a sealed value by variable name) |
| `ClusterKeyId` | `key_id` |
| `DataKeyRecord` | `wrapped`, `key_id` |
| `PlacementRecord` | `generation`, `basis`, `workloads` (by workload name: `uid_base` and `replicas`, each `{index, node?, reason?}`) |
| `UidAllocator` | `next`, `free` |
| `NodeStatus` | `reported_at`, `capacity` (`cpu_millis`, `memory_bytes`, `disk_bytes`), `replicas` |
| `IntervalSetting` | `secs`, `previous_secs`, `changed_at` |
| `ServiceIps` | `ips` (an address by `<project>/<environment>/<workload>`), `cursor` |

- `MeshConfig.container_cidr` is the range every node's container subnet lies in. `InitCluster` fixes it. It is absent in a mesh created before containers were networked, and then no node gets a subnet.
- `MeshConfig.container_subnet_prefix` is the prefix length of each node's subnet, default 24. It is stored in the state and not read from a constant, so replaying the log always allocates the same way. No flag sets it.
- `MeshConfig.service_cidr` is the range the virtual IPs of services are taken from (see "Virtual IPs"). `InitCluster` fixes it. It is left out of the stored JSON when absent, and then no service gets an address.
- `NodeRecord.container_subnet` is the subnet raft gave the node: the founder's in `InitCluster`, every other node's in `RegisterNode`.
- `NodeRecord.name` is the name the node holds a lease on. Names are unique among nodes. `InviteRecord.name` is the name the new node will hold.
- `NodeRecord.lease_renewed_at` is the value of `MeshState.clock` when the lease was last renewed.
- `MeshState.clock` is the lease clock, in seconds. `MeshState.lease_secs` is how long a lease lasts without a renewal: default 28800 (8 hours), at least 300, at most 2592000 (30 days).

There is no migration. The workload model changed shape, so a cluster that holds older records is created again. Within the current shape, a field that a mesh record gained later defaults when absent: no service range, no container range, a prefix of 24, no subnet, an empty name, `lease_renewed_at` and `clock` at 0, and `lease_secs` at 28800. A node record without an `instance_id` matches no machine until a renewal gives it one. An empty name is held by nobody.

A lease has expired when `clock - lease_renewed_at >= lease_secs`. A node holds its name, mesh IP and container subnet from registration until its record is deleted, whether or not its lease has expired.

### Commands

The leader stamps `at` (Unix seconds) on the invite commands, and apply never reads a clock. The lease commands carry no time. A rejected command changes nothing unless the table says otherwise.

| Command | Apply | Rejected with |
|---|---|---|
| `InitCluster{mesh, founder}` | Writes `mesh` with the founder as the only node, `clock = 0` and `lease_secs = 28800`. The founder gets the first subnet of the container range, or none without a range, and its lease starts at clock 0. Apply overwrites `founder.container_subnet` and `founder.lease_renewed_at`. An empty `founder.name` becomes `node-1` | `already_initialized`: `mesh` exists. `bad_request`: the founder's mesh IP is not a host address of the mesh CIDR; the name is not a DNS label; the subnet prefix is above 30 or shorter than the range's own prefix; the range has host bits set or overlaps the mesh CIDR; the service range has host bits set, a prefix longer than /30, or overlaps the mesh or the container range |
| `CreateInvite{invite_id, public_key, endpoint, name, ttl_secs, at}` | Drops expired invites. Drops a live invite that carries the same name, so a lost token cannot block a name. Reserves `next_node_id` and the lowest free host address of the mesh CIDR. Stores the invite with `expires_at = at + ttl_secs`. Changes no node. Answers `Invited` | `bad_request`: `ttl_secs` is 0 or above 604800; the name is empty or not a DNS label; the invite id or the public key is in use. `mesh_exhausted`: no free host address in the mesh CIDR, or no container subnet left for this invite (see below). A rejection keeps the prune of expired invites and nothing else |
| `RegisterNode{invite_id, node_id, endpoint, instance_id, at}` | Moves the invite into `nodes` under the invite's name, node id and mesh IP, registered to `instance_id`, with its lease starting at the current `clock`. If no node holds the name, the node gets the lowest free subnet of the container range. If another node holds the name, this is a takeover: apply deletes that node's record and gives its subnet to the new node in the same write | `invite_used`, `invite_invalid`, `invite_expired`: see below. `not_ready`: the node that holds the name is still in the raft membership. `mesh_exhausted`: no subnet to take over and none free |
| `SetEndpoint{node_id, endpoint}` | Replaces the node's endpoint | `not_found`: unknown node |
| `AdvanceClock{elapsed_secs}` | Adds `min(elapsed_secs, max(1, lease_secs / 10))` to `clock`. The clock saturates and never goes back | never |
| `RenewLease{node_id, instance_id}` | Sets the node's `lease_renewed_at` to `clock`. A record without an `instance_id` adopts the one given | Checked in this order. `mesh_node_removed`: unknown node. `mesh_lease_expired`: the node is known and its lease has already expired. `forbidden`: the record is registered to another `instance_id` |
| `RemoveNode{node_id}` | Deletes the node's record, which frees its mesh IP, subnet and name. An unknown node is not an error, so a new leader can repeat it | `not_ready`: the lease has not expired, or the node is still in the raft membership |
| `SetLease{secs}` | Sets `lease_secs` and restarts the lease of every node that had not expired under the old duration | `bad_request`: `secs` is below 300 or above 2592000 |
| `CreateDataKey{scope, wrapped, key_id}` | Stores a data key wrapped under the cluster key at `keys/data/<scope>`. The first command for a scope wins. A later one changes nothing, and its proposer reads the stored key back and seals again with that (see "Encryption") | `bad_request`: the scope is not `cluster` or `<project>/<environment>` with both DNS labels; `key_id` is not 1 to 64 letters and digits |
| `SetClusterKeyId{key_id}` | Records the id of the cluster key once. The same id again changes nothing | `bad_request`: a bad id; the id differs from the one recorded |
| `PlaceReplicas{project, environment, generation, basis, workloads}` | Stores the plan under `place/<project>/<environment>`. A workload that is new to the plan gets the lowest free host uid range, and a workload the command leaves out gives its range back. With no workloads, for an environment that no longer exists, the record is deleted. Writes nothing else, so no generation moves (see "Placement") | `bad_request`: a name is not a DNS label; the generation is not the environment's, or is older than the stored plan's; the basis is older than the stored plan's; a workload the environment does not have; more than 1024 replicas; a replica index twice; a `reason` over 64 bytes; a node the mesh does not have. `not_found`: workloads for an environment that does not exist. `mesh_not_configured`: a replica names a node and the cluster has no mesh. `workloads_exhausted`: every uid range is taken |
| `ReportNode{node_id, instance_id, capacity, replicas}` | Replaces `status/<node_id>` and stamps it with `clock` (see "Status reporting"). A node without an `instance_id` adopts the one given, and `mesh` is written then | In this order: `mesh_not_configured`; `mesh_node_removed`: unknown node; `mesh_lease_expired`; `forbidden`: another `instance_id`; `bad_request`: over 100 replicas, a name over 63 bytes, a `detail` over 200 bytes, a capacity over 4096000 CPU millis or 1 PiB, a name that is no DNS label, a `pod_ip` outside the node's container subnet or equal to its first host address |
| `SetStatusInterval{secs}` | Sets the report interval and keeps the previous one for the readers (see "Status reporting"). The same value changes nothing | `bad_request`: `secs` is below 1 or above 300 |
| `AllocateServiceIp{project, environment, workload}` | Gives the workload the next free host address of the service range after the cursor (see "Virtual IPs"). No change when it has one | `bad_request`: a bad name, or a mesh without a service range. `mesh_not_configured`. `not_found`: the workload is not in the environment. `mesh_exhausted`: no free address |
| `ReleaseServiceIp{project, environment, workload}` | Frees the address. No change when the workload still exists or has no address | `bad_request`: a bad name |

`RegisterNode` and `RemoveNode` are checked against the raft membership as the entry sees it: the voters and learners of the membership entry applied latest, in the same batch or an earlier one. Every replica applies the same entries in the same order, so each computes the same answer.

`RegisterNode` checks, in this order:
- A node was already registered from this invite with the same `instance_id`, `node_id` and `endpoint`: nothing changes and the command succeeds. This is a retry by the same machine. It returns before anything is allocated or taken over, so the node keeps its subnet and a retry never removes a second node.
- A node was already registered from this invite by another `instance_id`, by none, or with other details: `invite_used`.
- No such invite: `invite_invalid`. `at >= expires_at`: `invite_expired`. `node_id` or `endpoint` differs from the invite: `invite_invalid`.
- Another node holds the invite's name and is in the raft membership: `not_ready`.
- Otherwise the node is registered. A command logged before `instance_id` existed registers the node without one, and never counts as a retry.

`CreateInvite` keeps a container subnet free for every live invite, so `RegisterNode` cannot run out. With a container range, it rejects with `mesh_exhausted` when the nodes that hold a subnet, plus the live invites that need a new one, plus this invite if it needs one, number more than the range has subnets. An invite needs no new subnet when a node holds its name and a subnet, because it will take that subnet over. The check is at invite time because a joiner retries `mesh_exhausted` for ever.

### Project and workload commands

The index `projects` lists every project and, for each of its environments, a copy of four figures of the environment's record: `generation`, `bytes` (the record's size as compact JSON), `workloads` (how many it holds) and `updated_at`. The workloads themselves are in `env/<project>/<environment>`, one record per environment. Lists, limits and the `not_empty` checks read the index alone.

The leader stamps `at` (Unix seconds) on the commands that carry it. `index` below is the raft log index of the command's own entry. Apply writes the index and the records of one command in one batch. A rejected command writes nothing. Apply needs no `mesh` for these commands, so they also work in a cluster without one.

The leader runs the same checks on its own state before it proposes (see "HTTP routes"). A command that apply would refuse, and a deploy that would change nothing, then get no log entry at all. The table still holds for every entry that does reach the log, which happens when the leader could not confirm its state in time.

Each cell of "Rejected with" lists its checks in the order apply makes them.

| Command | Apply | Rejected with |
|---|---|---|
| `CreateProject{name, environments, at}` | Adds the project with `created_at = at`. Writes an empty record for each environment, with `generation = index` and `updated_at = at` | `bad_request`: the project name is not a DNS label; `environments` is empty; it has more than 16 entries; an environment name is not a DNS label or is there twice. `project_exists`: the project exists. `workloads_exhausted`: the cluster has 64 projects |
| `DeleteProject{name}` | Removes the project from the index and deletes the record of each of its environments | `not_found`: unknown project. `not_empty`: one of its environments holds a workload |
| `AddEnvironment{project, name, at}` | Adds the environment and writes its empty record, with `generation = index` and `updated_at = at` | `not_found`: unknown project. `bad_request`: the name is not a DNS label. `project_exists`: the project has the environment. `workloads_exhausted`: the project has 16 environments |
| `RemoveEnvironment{project, name}` | Removes the environment from the index and deletes its record | `not_found`: unknown project or environment. `not_empty`: the environment holds a workload |
| `ApplyWorkloads{project, environment, workloads, prune, at}` | `workloads` maps each name to a `StoredWorkload{spec, env}`: the spec with every container's `env` empty, and the sealed env of each container (see "Encryption"). With `prune`, removes every stored workload the command does not name. Then, for each workload of the command: a new name is created with `revision = index`; a workload equal to the stored one is unchanged, the env compared by digest and not by ciphertext; any other one replaces the stored one, and `revision` becomes `index` if the pod template or an env digest differs. If nothing was created, updated or removed, apply writes nothing and answers with the generation the environment has. Otherwise the record gets `generation = index` and `updated_at = at`. Answers `Applied` | `not_found`: unknown project or environment. `bad_request`: a workload name is not a DNS label; a workload carries env values that are not sealed; `workloads` is over 128 KiB as JSON. `workloads_exhausted`: the environment would hold more than 64 workloads; its record would be over 512 KiB; all records together would be over 16 MiB. The three `workloads_exhausted` checks run only for a command that changes something |
| `RemoveWorkloads{project, environment, names, at}` | Removes the named workloads and sets `generation = index` and `updated_at = at`. A name given twice is removed once. With no name at all the record stays as it is | `not_found`: unknown project or environment; a name the environment does not hold. Nothing is removed then, also not the names it does hold |

The pod template of a spec is the spec with `scale`, `placement` and `expose` set to their defaults. `revision` moves only when the template or an env value changes, so a deploy that changes the replica count, the nodes or the exposure leaves it alone.

Apply checks a fixed set of things about a workload: its name and the size of the command. It does not run the model's validation (`WorkloadSpec::validate`, `validate_workloads`). The service runs that before it proposes or forwards a command. The reason is replay. Every replica must compute the same answer for a logged command, today and after an upgrade. A validation rule that changes between versions would make an old entry apply on one build and fail on another. The fixed set in apply does not change. The price is that a spec which only apply has seen is not known to be valid, and a reader of `env/...` must not assume it is.

`Response` is `None | Previous(Option<Value>) | Invited{node_id, mesh_ip, expires_at, name, replaces?} | Rejected{code, message} | Applied{generation, created, updated, unchanged, removed}`. `Invited.replaces` is the node that holds the name now, expired or not, and that `RegisterNode` will delete. `Applied` is the answer of `ApplyWorkloads`: the environment's generation after the command, and the name of every workload of the command in exactly one of `created`, `updated` and `unchanged`, each list sorted. `removed` names what `prune` removed. The other commands answer `None`. A rejected command still advances `last_applied`.

### Lease clock

Invites expire by `at`, the leader's wall clock. Leases do not. `MeshState.clock` is a logical clock that only `AdvanceClock` moves, and the leader proposes it with the time it measured on its monotonic clock since its previous tick. The reasons:

- A leader whose wall clock jumps forward would expire every lease at once.
- Time the cluster spent without a leader, or without a quorum to commit the command, adds nothing to the clock. A cluster that was off overnight comes back with every lease as it left it.
- Apply clamps one command to a tenth of the lease, so one leader that measured a huge gap cannot expire a lease in one step.

Expiry is sticky. `RenewLease` for an expired lease is rejected with `mesh_lease_expired`, because the leader may already be removing the node, and a late renewal would leave the node in the mesh but out of raft. `SetLease` does not restart an expired lease either.

The two refusals of a renewal mean different things. A record is deleted only after its node has left the raft membership, so `mesh_node_removed` (no record) proves to the node that the mesh removed it. `mesh_lease_expired` proves nothing of the kind: the node is still in the mesh and may still be a voter.

Removal goes through the raft membership first and the mesh record second, and apply enforces that order. Deleting the mesh record first would cut WireGuard to a node whose acknowledgement may be needed to commit its own removal.

### Takeover

An invite for a name that a node already holds takes that node over when the new machine registers. No extra flag asks for it. `InviteResponse.replaces_node_id` names the node at invite time, and nothing changes until the registration.

The new node has its own node id, key and mesh IP, all from the invite. Only the name and the container subnet move. The leader removes the old node from the raft membership before it proposes `RegisterNode`, under the quorum guard that the lease task uses (see "Node lease"), and answers `not_ready` until that has worked. `RegisterNode` then deletes the old record and gives the subnet to the new one in one write, so no two nodes ever hold a subnet. Whether the old node's lease has expired makes no difference.

When the old node is the leader, it removes itself from the membership, steps down once that is applied, and answers `not_ready`. The joiner's retry reaches the next leader, which finishes the registration.

## Invite token

`faber1.` + base64url, no padding, of JSON:

```json
{
  "v": 1,
  "invite_id": "<32 lowercase hex chars>",
  "expires_at": 1700003600,
  "node_id": 2,
  "mesh_ip": "10.99.0.2",
  "mesh_cidr": "10.99.0.0/16",
  "endpoint": "198.51.100.2:51820",
  "private_key": "<base64 wg private key>",
  "peers": [{"node_id": 1, "public_key": "<base64>", "endpoint": "203.0.113.1:51820", "mesh_ip": "10.99.0.1"}],
  "join_addrs": ["10.99.0.1:7401"]
}
```

- Shape check (install.sh): `^faber1\.[A-Za-z0-9_-]+$`. Decoders reject another prefix, any `v` other than 1, and tokens over 64 KiB.
- `endpoint` is the new node's own endpoint from `mesh invite --wg-endpoint`. The joiner must register with exactly this value.
- `peers[].endpoint` may be `null`.
- `join_addrs` lists the leader first.
- The format has not changed with container networking or leases. The token holds neither the node's name nor its container subnet. The name stays in the invite record on the mesh. Raft allocates the subnet when the node registers, and the node reads it from the replicated state. A joiner reaches raft with the mesh `/32` addresses the token already has.
- TTL: default 3600 s, max 604800 s, measured by the leader's clock. Single use, by one machine (see "Joining with an invite").
- The token holds a permanent private key. It must not be logged, and `Debug` redacts it.

## Projects, environments and workloads

A cluster holds projects. A project holds environments. An environment holds workloads. A workload is one pod template and a scale: one or more containers that run together, and how many copies of them.

| Thing | Name | Unique within |
|---|---|---|
| project | DNS label | the cluster |
| environment | DNS label | its project |
| workload | DNS label | its environment |
| container | DNS label | its workload |
| volume | DNS label | its workload |

A DNS label is 1 to 63 characters of `a-z`, `0-9` and `-`, with no hyphen at either end. The DNS name of a workload is `<workload>.<environment>.<project>.faber.local`. The API returns it as `fqdn`. It resolves to the workload's virtual IP (see "Virtual IPs" and "DNS").

`POST /api/v1/projects` without `environments` creates `staging` and `production`. A project always has at least one environment at creation. Removing environments later can leave it with none.

### Generation and revision

- An environment's `generation` is the raft log index of the entry that last changed it, starting with the entry that created it. It only grows, and it does not repeat when an environment is removed and created again under the same name.
- A deploy that changes nothing keeps the generation and writes nothing. "Changes nothing" means every spec of the request equals the stored one, every env value is unchanged, and `prune` removes nothing.
- A workload's `revision` is the log index of the entry that last changed its pod template. The pod template is the spec with `scale`, `placement` and `expose` left out, together with the env values. A deploy that changes only `scale`, `placement` or `expose` updates the workload, moves the generation and keeps the revision. The runner starts the containers of a new revision when the revision moves and at no other time.
- Removing a workload moves the generation.
- A deploy with workloads needs the cluster key on the leader, even when no workload has env, because the leader seals the env with the data key of the environment, and makes that key on the first deploy (see "Encryption").

### Workload model

The types are in `faber_api_types::workload`. Every type refuses unknown fields, so a spec written for an older shape, for example one with a `host_path` volume, a `liveness` probe or a `requests` resource, is refused. A field with a default may be left out of a request, and a reply always carries it. An optional field (`?`) is absent from a reply when unset. Two specs are equal when every field is equal after the defaults are filled in.

`WorkloadSpec`:

| Field | Default | Rule |
|---|---|---|
| `scale` | `{"replicas": {"count": 1, "strategy": "spread"}}` | `{"replicas": {"count": N, "strategy": S}}` with N from 0 to 1024 and S `spread` (the default) or `pack`, or `"daemon"` for one copy on every node |
| `restart` | `always` | `always`, `on_failure` or `never`, see "The runner" |
| `expose` | `environment` | `environment` or `cluster`, see "Exposure" |
| `placement` | `{}` | `{nodes}`. `nodes` is a list of at most 64 unique node names, each a DNS label. Empty means any node. With `"daemon"` the workload runs once on each listed node |
| `stop_grace_secs?` | none | at most 3600. The runner uses 30 when it is absent |
| `volumes` | `{}` | at most 32, by name. A workload has no volume unless its spec declares one. Each is one of the two kinds below |
| `containers` | required | 1 to 16, with unique names |

A volume has a `type` and a `size_bytes`:

| `type` | Fields | Rule |
|---|---|---|
| `block` | `size_bytes` | a fixed-size filesystem image on the node the replica runs on. It is local to that node and outlives the pod. `size_bytes` is from 1 MiB to 1 TiB |
| `ramdisk` | `size_bytes` | tmpfs. It is gone when the pod stops. `size_bytes` is from 1 MiB to 64 GiB |

The block volumes of one workload total at most 4 TiB and its ramdisks 256 GiB. The ramdisks that a container mounts together are at most its `memory_bytes`, because tmpfs pages count against the memory of the container's cgroup. See "Volumes" for how they are made and who owns them.

> **Block volumes and downtime.** A workload with a block volume never has two revisions of one replica running, because they would share the volume. On a revision change the old replica of each slot stops, after the drain period, before the new one starts, and that slot has no running replica in between. In this version every replica of the workload is replaced at the same moment, on every node, so the whole service can be unavailable during a deploy that changes the pod template. A replica with a block volume also cannot move: if its node is removed, it stays unplaced (`volume_lost`) until the workload is removed. Run several replicas, on different nodes, so that the service survives the loss of one replica or one node, and plan a template change to such a workload as a short outage.

`ContainerSpec`:

| Field | Default | Rule |
|---|---|---|
| `name` | required | DNS label |
| `image` | required | 1 to 512 bytes of ASCII, with no whitespace and no control character. Public images only: faber pulls anonymously |
| `command` | `[]` | replaces the image's entrypoint. `command` and `args` together have at most 256 entries of at most 8 KiB each, with no NUL byte |
| `args` | `[]` | replaces the image's command |
| `env` | `{}` | at most 256 variables. A name matches `[A-Za-z_][A-Za-z0-9_.-]*` and is at most 256 bytes. A value is at most 32 KiB and has no NUL byte. Every value is encrypted at rest (see "Encryption") |
| `working_dir?` | none | an absolute, normalised ASCII path of at most 4096 bytes |
| `user?` | none | `uid`, `uid:gid`, or a user name matching `[a-z_][a-z0-9_-]{0,31}`. Ids are decimal without a leading zero. A group name is refused |
| `ports` | `[]` | at most 64 of `{name?, port, protocol}`. `port` is 1 to 65535. `protocol` is `tcp` (the default) or `udp`. A `name` is a DNS label and unique in the container. A pair of port and protocol appears once. These are the ports the workload's virtual IP serves |
| `mounts` | `[]` | at most 32 of `{volume, path, read_only}`. `volume` names a volume of the workload. `path` is absolute, normalised, ASCII and unique in the container. It is not `/`, and it is not `/proc`, `/sys` or `/dev` or anything under them, except `/dev/shm` and what is under that. No mount path lies inside another. `read_only` defaults to `false` |
| `resources` | `{"cpu_millis": 1000, "memory_bytes": 1073741824}` | the most the container may use. `cpu_millis` is from 1 to 256000. `memory_bytes` is from 1 to 4 TiB |
| `capabilities` | `[]` | Linux capabilities to add, see below |
| `read_only_root_fs` | `false` | |
| `probe?` | none | a `Probe` |

A normalised path has no `.` or `..` component, no `//` and no trailing `/`.

`resources` are maximums, not requests. The runner sets them as the kernel limits of the container: a CPU quota of `cpu_millis` and a memory limit of `memory_bytes` with no swap. Placement reserves the sum over the containers of a replica on the node it chooses (see "Placement").

The image, every path, and the `path` and `host` of an HTTP probe hold ASCII only. No invisible or text-reordering character can then hide in a name that someone reads on a terminal. A volume mounted on `/proc`, `/sys` or `/dev` would hide or replace what the runtime puts there, which is why the model refuses those mount paths.

`capabilities` takes the kernel's names in upper case without the `CAP_` prefix, each at most once. A container may add these 11 and no others: `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `FSETID`, `KILL`, `NET_BIND_SERVICE`, `SETFCAP`, `SETGID`, `SETPCAP`, `SETUID` and `SYS_CHROOT`. The list is `ALLOWED_CAPABILITIES`. Every other capability the kernel has, `SYS_ADMIN`, `NET_ADMIN`, `NET_RAW` and `SYS_PTRACE` among them, gives its holder control of the node. The validation refuses such a name with `capability <NAME> is not allowed` and the allowed list. `ALL` is refused. A name the kernel does not have is refused as `is not a Linux capability`. faber-node checks the list again (see "faber-node").

`Probe` is `{action, interval_secs, timeout_secs}`. `interval_secs` is from 1 to 3600 and defaults to 5. It is also the delay before the first check. `timeout_secs` defaults to 1 and is at least 1 and less than `interval_secs`, so an interval of 1 is refused. One failed check takes the replica out of DNS, and 3 in a row (`PROBE_FAILURES_BEFORE_RESTART`) restart the container. A container has at most one probe. `action` has a `type`:

| `type` | Fields | Rule |
|---|---|---|
| `http` | `port`, `path` (default `/`), `scheme` (default `http`), `host?`, `headers` (default `{}`) | sends a GET to the pod. A status from 200 to 399 passes, and a redirect is not followed. `port` is 1 to 65535. `path` starts with `/`, is at most 4096 bytes, and is ASCII with no whitespace and no control character. `scheme` is `http` or `https`. `host` is the `Host` header: a host name or `host:port` of at most 253 bytes. A header name is an HTTP token and not `Host`, `Content-Length`, `Transfer-Encoding` or `Connection`, which the runner sets. A value has no CR, LF or NUL |
| `tcp` | `port` | opens a connection to the pod. Connecting passes. `port` is 1 to 65535 |

An `https` probe does a real TLS handshake and then the GET, with certificate verification off, because a pod's certificate is its own. Only the status line is read.

There are no exec probes, startup probes or split readiness and liveness probes.

`Debug` of a `ContainerSpec` prints the names of its env variables and never their values. `Debug` of an HTTP probe action prints the names of its headers and never their values. Either can be a secret. Requests that carry a spec must not be logged.

### Limits

| What | Limit | Checked by |
|---|---|---|
| projects in a cluster | 64 | apply |
| environments in a project | 16 | apply |
| workloads in an environment | 64 | apply |
| `workloads` of one deploy, as JSON | 128 KiB | service and apply |
| one environment record, as JSON | 512 KiB | apply |
| all environment records together | 16 MiB | apply |
| HTTP body on the project and workload routes | 1 MiB | the listener |
| workloads named by one removal | 64 | service |
| containers in a workload | 16 | service |
| replicas of a workload | 1024 | service |
| nodes in `placement.nodes` | 64 | service |
| volumes of a workload | 32 | service |
| `size_bytes` of a volume | 1 MiB to 1 TiB (block) or 64 GiB (ramdisk) | service |
| block volumes of a workload together; ramdisks together | 4 TiB; 256 GiB | service |
| mounts of a container | 32 | service |
| ports of a container | 64 | service |
| env variables of a container | 256 | service |
| env variable name | 256 bytes | service |
| env variable value | 32 KiB | service |
| image reference | 512 bytes | service |
| `command` and `args` entries together | 256 | service |
| one command entry | 8 KiB | service |
| a path | 4096 bytes | service |
| `stop_grace_secs` | 3600 | service |
| `interval_secs` of a probe | 1 to 3600 | service |
| `cpu_millis` of a container | 1 to 256000 | service |
| `memory_bytes` of a container | 1 to 4 TiB | service |

A limit that apply checks answers 409 `workloads_exhausted`, except the 128 KiB of a deploy, which is 400 `bad_request` from either side. A limit that only the service checks answers 400 `bad_request`. The CLI runs the same validation before it sends anything.

### Exposure

`expose` is stored and not enforced. Nothing reads it yet. DNS answers for every service, and every container on the container network can reach every other one. The intent, for a later version: a workload that is not exposed will be neither resolvable nor reachable from outside its environment, and `cluster` makes it both for every environment of every project.

### Known limits

- Env values are encrypted at rest and masked on read, but a node that runs a replica holds the cluster key and the replica's plaintext env. Root on that node can read both, and so can any local user through the unauthenticated loopback API with `?reveal=true`. Do not put in `env` a secret that such a user may not see (see "Encryption").
- The API has no authentication. Whoever can reach a node's loopback listener can deploy, and a deploy runs containers on every node it is placed on. The right to deploy is the right to run any public image on the cluster, inside a user namespace and with the limits in "faber-node". A registered node can also write through `/raft/cluster/workloads`.
- `expose` is not enforced.
- Container egress is off. A container reaches the other containers and the services of the cluster and nothing else. Images are pulled by containerd on the host, so pulls are not affected.
- Images are pulled anonymously from public registries. There is no registry authentication.
- There is no log collection.
- A block volume cannot be resized: faber-node refuses a changed `size_bytes` on an existing volume. The runner never removes a block volume (see "Volumes").
- A write that meets a leader change answers 503 `no_leader` or `not_leader`. Nothing retries it, and the CLI prints the error.
- A node that joins late may fail to catch up once the state has grown. The raft snapshot and append timeouts are openraft's defaults. A snapshot travels in 3 MiB chunks with 200 ms for each, and an append has the heartbeat interval. Snapshot bytes are sent as JSON arrays of numbers, about 3.6 times their size. On a slow link this can fail once the state passes roughly 1 MiB, although the limit on all environment records is 16 MiB. This piece of work does not fix it.
- `Debug` of a spec hides env values and probe header values and nothing else. `command` and `args` are printed, and they can hold a password given as an argument. Nothing may log a spec.
- There is no data migration. A cluster that holds records of an older shape must be created again.

## HTTP routes

| Route | Listener | Body | Reply |
|---|---|---|---|
| `POST /api/v1/mesh/invites` | loopback only (`api::local_router`) | `CreateInviteRequest{name, endpoint, ttl_secs?}` | 201 `InviteResponse{token, node_id, mesh_ip, expires_at, name, replaces_node_id?}`. `name` is required and must be a DNS label, or the answer is 400 `bad_request`. `replaces_node_id` is the node that holds the name now and that the new node replaces when it registers. It is absent when nobody holds the name |
| `GET /api/v1/mesh/lease` | loopback only | none | 200 `LeaseResponse{lease_secs}`, from this node's copy of the replicated state; 409 `mesh_not_configured` |
| `GET /api/v1/mesh/status-interval` | loopback only | none | 200 `StatusIntervalResponse{interval_secs}`, from this node's copy of the replicated state. 5 until an admin sets another value |
| `PUT /api/v1/mesh/status-interval` | loopback only; a follower forwards once to the leader's `/raft/cluster/status-interval` | `SetStatusIntervalRequest{interval_secs}` | 200 `StatusIntervalResponse{interval_secs}` once `SetStatusInterval` is committed; 400 `bad_request` outside 1 to 300 |
| `PUT /api/v1/mesh/lease` | loopback only; a follower forwards once to the leader's `/raft/cluster/lease` | `SetLeaseRequest{lease_secs}` | 200 `LeaseResponse{lease_secs}` once `SetLease` is committed; 400 `bad_request` outside 300 to 2592000 |
| `POST /raft/cluster/invites` | peer only; leader, a follower forwards once | `PeerInviteRequest{invite_id, public_key, endpoint, name, ttl_secs}` | 201 `PeerInviteResponse{node_id, mesh_ip, expires_at, mesh_cidr, peers, join_addrs, name, replaces?}`; 403 `forbidden` unless the source is a registered node |
| `POST /raft/cluster/register` | peer only; leader, a follower forwards once | `PeerRegisterRequest{invite_id, node_id, endpoint, instance_id}` | 200 `PeerRegisterResponse{cluster_key?, key_id?}` once `RegisterNode` is committed. The body is `{}` unless the cluster releases its key to this joiner (see "Encryption"). `invite_*` as `RegisterNode` answers. An invite id that is neither a live invite nor the invite of a registered node gets `invite_invalid` or `invite_expired` from the leader without a raft write; 503 `not_ready` while a node that holds the invite's name is still leaving the raft membership; 400 `bad_request` unless both ids are 32 lowercase hex characters |
| `POST /raft/cluster/join` | peer only; leader, a follower forwards once | `PeerJoinRequest{node_id, addr, invite_id?, instance_id?}` | 204 once the node is a voter; 503 `not_ready` while the node is not registered, or is a learner that has not caught up; 409 `mesh_node_removed` when the mesh has neither a record of the node nor an invite for it |
| `POST /raft/append`, `/raft/vote`, `/raft/snapshot` | peer only | openraft RPC | 200 with the RPC result; 503 `not_ready` from a node that has not registered; 403 `forbidden` unless the source address check below passes |
| `GET /api/v1/health` | both | none | 200 `HealthResponse{status: "ok", node_id}`; 503 `{status: "unavailable", node_id, error: "unavailable", message}` when the raft core has stopped or the storage is closed |
| `POST /raft/cluster/endpoint` | peer only; leader, a follower forwards once | `PeerEndpointRequest{node_id, endpoint}` | 204; proposes `SetEndpoint` (used at startup when `--wg-endpoint` differs from the replicated endpoint). 403 `forbidden` unless the source address check below passes |
| `POST /raft/cluster/renew` | peer only; leader, a follower forwards once | `PeerRenewRequest{node_id, instance_id}` | 204 once `RenewLease` is committed; 409 `mesh_node_removed` when the mesh has no record of the node; 409 `mesh_lease_expired` when it has the node and the lease has run out; 403 `forbidden` unless the source address check passes, and for another machine than the one the node is registered to; 400 `bad_request` unless `instance_id` is 32 lowercase hex characters |
| `POST /raft/cluster/lease` | peer only; leader, a follower forwards once | `PeerLeaseRequest{lease_secs}` | 200 `LeaseResponse{lease_secs}` once `SetLease` is committed; 400 `bad_request` outside the bounds; 403 `forbidden` unless the source is a registered node |
| `POST /raft/cluster/status` | peer only; leader, a follower forwards once | `ReportNode{node_id, instance_id, capacity, replicas}`, at most 64 KiB | 204 once the report is committed. 400 `bad_request` for a report over its limits (see "Status reporting"). 409 `mesh_node_removed` when the mesh has no record of the node, 409 `mesh_lease_expired` when its lease has run out. 403 `forbidden` unless the source is the node itself or a registered node that forwards, and for another machine than the one the node is registered to |
| `POST /raft/cluster/status-interval` | peer only; leader, a follower forwards once | `PeerStatusIntervalRequest{interval_secs}` | 200 `StatusIntervalResponse{interval_secs}` once `SetStatusInterval` is committed; 400 `bad_request` outside 1 to 300; 403 `forbidden` unless the source is a registered node |
| `POST /api/v1/projects` | loopback only; write | `CreateProjectRequest{name, environments?}` | 201 `ProjectResponse`. Without `environments` the project gets `staging` and `production`. 400 `bad_request`: a name is not a DNS label, `environments` is empty, has more than 16 entries or names one twice. 409 `project_exists`. 409 `workloads_exhausted` at 64 projects |
| `GET /api/v1/projects` | loopback only; read | none | 200 `ProjectListResponse{projects}`, sorted by name |
| `GET /api/v1/projects/{project}` | loopback only; read | none | 200 `ProjectResponse{name, created_at, environments}`. Each environment is `{name, generation, workloads, bytes, updated_at}`, sorted by name. 404 `not_found` |
| `DELETE /api/v1/projects/{project}` | loopback only; write | none | 204. 404 `not_found`. 409 `not_empty` while an environment holds a workload |
| `PUT /api/v1/projects/{project}/environments/{environment}` | loopback only; write | none | 200 `ProjectResponse`, the project with the new environment. 404 `not_found`: unknown project. 409 `project_exists`: the project has the environment. 409 `workloads_exhausted` at 16 environments |
| `DELETE /api/v1/projects/{project}/environments/{environment}` | loopback only; write | none | 204. 404 `not_found`: unknown project or environment. 409 `not_empty` while the environment holds a workload |
| `POST /api/v1/projects/{project}/environments/{environment}/deploy` | loopback only; write | `DeployRequest{workloads, prune}`. `workloads` maps each name to a `WorkloadSpec` with its env in clear (over the loopback listener, and from a follower to the leader over the mesh, which WireGuard encrypts). `prune` defaults to `false` | 200 `DeployResponse{generation, created, updated, unchanged, removed}`, four lists of names. 400 `bad_request`: a spec breaks a rule of the model, or `workloads` is over 128 KiB. 404 `not_found`: unknown project or environment. 409 `workloads_exhausted`. 503 `key_unavailable`: the leader has no usable cluster key, or its key is not the one the cluster records |
| `GET /api/v1/workloads?project=&environment=` | loopback only; read | none | 200 `WorkloadListResponse{workloads}`. Each is `WorkloadSummary{project, environment, name, fqdn, scale, expose, revision, available, desired}`, sorted by project, environment and name. `available` counts the replicas whose reported state is `available`; a replica without a node, and a replica on a node whose report is stale, does not count. `desired` is the replica count of a `replicas` scale, or for a `daemon` the replicas the leader has placed, so a daemon with no placement yet reads 0. An unplaced replica counts in `desired`. Both fields are required, so a client built for this shape cannot decode a list from a server that lacks them. Both filters are optional. A filter that names a project or an environment the cluster does not have gives an empty list, not a 404 |
| `GET /api/v1/projects/{project}/environments/{environment}/workloads/{workload}?reveal=` | loopback only; read | none | 200 `WorkloadResponse{project, environment, name, fqdn, revision, generation, spec, replicas}`. Every env value reads `***` unless `reveal=true`, which opens them with this node's cluster key and leaves one log line that names the workload and no value. `replicas` is one `ReplicaStatus{node?, replica_index, revision, state, pod_ip?, reason?}` per replica the leader placed, sorted by index (see "Placement" and "Status reporting"). 404 `not_found`. 503 `key_unavailable`: `reveal=true` on a node without the cluster key, with the wrong one, or without the environment's data key. Another value of `reveal` than `true` or `false` is 400 `bad_request` |
| `DELETE /api/v1/projects/{project}/environments/{environment}/workloads/{workload}` | loopback only; write | none | 204. 404 `not_found`: unknown project, environment or workload |
| `POST /raft/cluster/workloads` | peer only; leader, a follower forwards once | `PeerWorkloadRequest`, one of the six writes: an object with `op` set to `create_project`, `delete_project`, `add_environment`, `remove_environment`, `apply_workloads` or `remove_workloads`, and the fields of that command without `at`. The leader stamps `at` | 200 `PeerWorkloadResponse{index, applied?, project?}` once the leader has applied the command. `index` is its log index. For a deploy that changes nothing, which is not written, `index` is the generation the environment has. `applied` is the `DeployResponse` of `apply_workloads` and absent for the others. `project` is the `ProjectResponse` after `create_project` and `add_environment`, as the leader had it once it had applied the write, and absent for the other four. Errors as on the loopback routes. 400 `bad_request` when `remove_workloads` names no workload or more than 64. 403 `forbidden` unless the source is a registered node |

The project and workload routes share these rules:

- A read answers from this node's copy of the replicated state. On a follower it can be behind the leader by what the follower has not applied yet.
- A write is checked on the node that receives it: every name, and for a deploy the model's validation. The node then proposes it if it is the leader, or forwards it once to the leader's `/raft/cluster/workloads`. A follower does not forward what its own check refuses. The leader checks again.
- Before it proposes, the leader runs the write on a copy of its own state, with the functions apply uses. A write that apply would refuse is refused there, and a deploy that would change nothing is answered there with the stored generation. Neither costs a log entry. A leader that was just elected can be behind what the cluster committed, so it first waits until it has applied everything committed, and only then trusts a refusal or an "unchanged". If it cannot confirm that in time, it proposes the write and apply judges it.
- The leader answers once it has applied the command. A follower that forwarded then waits up to 5 s until it has applied the same log index, so that a client that writes and then reads through one node sees its own write.
- The reply to the client is all in the leader's answer. For a project create and an environment add the leader sends the `ProjectResponse` along, and the follower relays it. A committed write therefore always answers success, whatever the follower has applied. If the 5 s run out, the follower answers the same way, and only a read on it straight afterwards can still miss the write.
- Every name in a path, a query or a body must be a DNS label, or the answer is 400 `bad_request` before anything is read or written.
- A body over 1 MiB, a body that is not JSON, and a field the type does not have all answer 400 `bad_request`.
- A deploy that the model refuses answers 400 `bad_request` with the message `invalid workloads: <path>: <message>; ...`. It lists the first 20 broken rules and counts the rest as `and N more`. The path names the workload and the field, for example `web.containers[0].image`.
- An error from the leader reaches the client with the leader's status and body unchanged.
- A 404 `not_found` from these routes says what is missing: `project "shop" does not exist`, `environment "staging" of project "shop" does not exist`, or `workload "web" of environment "staging" of project "shop" does not exist`.
- A request body and a `WorkloadResponse` hold env values. Neither is logged.

The internal routes check the TCP source address of the request, because the peer firewall also admits machines that only hold an invite. Each route has one of four rules:

| Route | The source must be |
|---|---|
| `POST /raft/append`, `/raft/vote`, `/raft/snapshot` | a node that may send raft RPCs, see below |
| `POST /raft/cluster/register` | anyone the firewall admits. This is how a machine with an invite joins |
| `POST /raft/cluster/join` | anyone the firewall admits |
| `POST /raft/cluster/invites` | a registered node |
| `POST /raft/cluster/endpoint` | the node itself, or a registered node that forwards |
| `POST /raft/cluster/renew` | the node itself, or a registered node that forwards |
| `POST /raft/cluster/lease` | a registered node |
| `POST /raft/cluster/workloads` | a registered node |
| `POST /raft/cluster/status` | the node itself, or a registered node that forwards |
| `POST /raft/cluster/status-interval` | a registered node |
| `GET /raft/cluster/status` | anyone the firewall admits. Leader only, 503 elsewhere |

"The node itself, or a registered node that forwards":
- A request without the `x-faber-forwarded` header must come from the mesh IP of `node_id`, as the replicated `mesh` records it. A node can change only its own endpoint and renew only its own lease.
- A request with the header was forwarded by a node that made that check. It must come from the mesh IP of a registered node. The mesh IP of a pending invite does not count.

"A registered node": the request must come from the mesh IP of a node in the replicated `mesh`, with or without the header. The only node with a reason to send either request is a follower forwarding its own loopback request. A machine that only holds an invite may not create invites, change the lease or write projects and workloads. Without this rule it could invite itself under the name of a live node and take that node over, or deploy to the cluster before it has joined. Every registered node can deploy any workload to the whole cluster through `/raft/cluster/workloads`. The route has no authentication beyond the source address.

For both rules:
- Anything else, including any request while this node has no `mesh`, gets 403 `forbidden`.
- A node whose peer listener is on loopback (development and single-host tests) skips the check, as it skips the join address check, because every node there has the same address.

"A node that may send raft RPCs". A machine that only holds an invite could otherwise post a forged snapshot and replace all replicated state, or a membership without some node, which that node would take as proof of its removal. The source is allowed when it is one of:
- the mesh IP of a registered node in this node's copy of `mesh`;
- while this node's join is pending, the mesh IP of a peer in its join token. Those were the registered nodes when the invite was made. They count next to the other two rules, and not only before the node has a copy of `mesh`: the copy a joiner has applied so far need not have the node that leads now, and that leader's appends are the only way forward;
- the address of a voter or learner of the raft membership in this node's log. A node becomes a member only after it has registered, and the log can be ahead of what this node has applied.

Anything else gets 403 `forbidden` and the request does not reach raft. This node's own mesh IP is always refused, whatever the three rules say: raft never sends to itself, so such a request comes from another process on the machine. A node this one knows only as an invite is refused even if it has registered since, until this node hears of the registration from another member. The sender treats a 403 like a peer it cannot reach and retries with a backoff. It also logs a warning that names the node that refuses it, at most once a minute per node, because the target logs its refusal only at debug level. The check is off when this node's peer listener is on loopback, and outside mesh mode, where there are no invites. The registration gate runs first: a node that has not registered answers 503 `not_ready` whatever the source.

A registered node can still send the header itself and name another node. The check keeps invited and unknown addresses out. It does not protect the mesh from one of its own members.

### Joining with an invite

A token can be copied, and it holds a node's private key and mesh address. A second machine started from a token that a node already joined with would otherwise look like that node: WireGuard on the other nodes follows the key to the new address, and the leader's raft RPCs for the node arrive at a machine with an empty log. So an invite belongs to the first machine that registers with it, and a join takes two requests.

1. `POST /raft/cluster/register` commits `RegisterNode` with the joiner's `instance_id`. On 204 the joiner saves `pending_join.registered` and starts to accept raft RPCs. The leader proposes nothing for an invite id the mesh does not know or whose invite has expired, and answers `invite_invalid` or `invite_expired` itself. `RegisterNode` is a synced write on every node, and the route is open to every machine the firewall admits. The leader gives that refusal only once it has applied everything committed before the request, and answers 503 `not_ready` if it cannot confirm that within 5 s. If another node holds the invite's name, the leader first removes that node from the raft membership and answers 503 `not_ready` until it has (see "Takeover").
2. `POST /raft/cluster/join` adds the node as a learner and then makes it a voter. In a mesh the leader first checks the node record of `node_id`:
   - no record, and the invite is still in the state: 503 `not_ready`. The node has not registered yet.
   - no record and no invite: the leader first waits, for up to 5 s, until it has applied everything committed before the request, and answers 503 `not_ready` if it cannot confirm that. A leader that was just elected may not have applied the registration yet. If there is still neither, the mesh has removed the node and the answer is 409 `mesh_node_removed`. That answer makes the joiner give up its identity, so the leader never gives it on a guess.
   - no `invite_id` or `instance_id` in the request, or the record was registered with another invite: `invite_invalid`.
   - the record has another `instance_id`: `invite_used`.
   - on a mesh listener, `addr` is not the record's mesh IP on the mesh peer port: `invite_invalid`.

The join no longer registers anything. Both requests are safe to repeat, and the joiner repeats them, against each address in `join_addrs`, until its own raft shows it as a voter. A joiner restarted after step 1 skips it. One restarted before it saved `registered` registers again, which the mesh accepts from the same `instance_id`.

Outside a mesh (no `mesh` key) a join needs neither id, as before.

While a node has `pending_join` without `registered`, its `/raft/append`, `/raft/vote` and `/raft/snapshot` answer 503 `not_ready` and the request does not reach raft. The exception is a node whose own raft storage already has it in a membership: it got that log from the cluster. A node whose invite was rejected, or that the mesh has removed, refuses raft RPCs again until it restarts. The sender treats a 503 like a peer it cannot reach and retries with a backoff, so an unregistered machine costs the leader nothing but those retries.

Host/Origin guard:
- Loopback listener: `Host` must be `localhost`, `127.0.0.1` or `[::1]` (any port).
- Peer listener: the same hosts, plus this node's mesh IP.
- A request with any `Origin` header, or with `Sec-Fetch-Site` other than `none`, gets 403 `forbidden`.
- `/mcp` keeps its own `Host` allow-list (loopback, the peer listener address and `--mcp-allowed-host`); the Origin/`Sec-Fetch-Site` rule applies to it too.

## Error codes

| Code | HTTP | When |
|---|---|---|
| `invite_invalid` | 400 | unknown invite, or `node_id`/`endpoint` does not match it; wg-mode join without an invite |
| `invite_used` | 409 | invite already registered to another machine (another `instance_id`), or to the same machine with other details |
| `invite_expired` | 410 | `at >= expires_at` |
| `mesh_not_configured` | 409 | no `mesh` key |
| `mesh_no_endpoints` | 409 | no existing node has an endpoint |
| `mesh_exhausted` | 409 | no free host address in the mesh CIDR, or every container subnet is held by a node or kept for a live invite |
| `mesh_node_removed` | 409 | the mesh has no record of the node: a lease renewal for such a node, or a join for a node with no record and no invite. The node treats it as proof that the mesh removed it |
| `mesh_lease_expired` | 409 | a lease renewal for a node the mesh still has, whose lease has expired. It is no proof of removal, and the node does not tear down |
| `forbidden` | 403 | Host/Origin guard; a raft RPC, endpoint change, lease renewal, invite, lease change or workload write from an address that may not make it; a lease renewal by another machine than the one the node is registered to |
| `not_ready` | 503 | retry later. The status is 503 whether the service or apply returns it. Returned by a register or a join when the leader cannot confirm within 5 s that it has applied everything committed. Returned by a join for a node that has not registered, or that is already a learner and has not replicated the leader's log within 5 s. Returned by a register that takes over a name while the node that holds it is still in the raft membership: too few voters have caught up within 5 s to remove it, the membership change failed, or the holder was the leader and has just removed itself. Returned by apply for `RemoveNode` while the node's lease is live or the node is still a raft member. Also returned for raft RPCs by a node that has not registered. |
| `key_unavailable` | 503 | the cluster key is missing, unreadable or not the one raft records on the node that has to seal or open env values, or a stored data key cannot be opened with it. A deploy needs it on the leader and `?reveal=true` on the answering node |
| `unavailable` | 503 | `GET /api/v1/health` only: the raft core has stopped or the storage is closed |
| `project_exists` | 409 | a project create for a name that is taken, or an environment add for an environment the project has |
| `not_empty` | 409 | removing a project while one of its environments holds a workload, or an environment while it holds one. Remove the workloads first |
| `workloads_exhausted` | 409 | a limit of the stored state: 64 projects, 16 environments in a project, 64 workloads in an environment, 512 KiB for one environment record, 16 MiB for all of them |

A join first adds the node as a learner and then makes it a voter. If the first attempt stops in between, the retry finds a learner. The leader promotes it only once it has caught up, because a voter that cannot be reached can cost the cluster its quorum. The joiner's pending-join loop retries `not_ready`, like every error other than `invite_*` and `mesh_node_removed`.

`ClusterStatus.joined` is true when this node is a voter. A learner is listed in `members` with `voter: false` and has not joined. install.sh waits for `"joined": true`. Each entry of `members` carries the member's `name` from the mesh. It is absent outside a mesh and for a member with no mesh record.

Existing codes (`bad_request`, `already_initialized`, `not_found`, …) keep their meanings. A `not_found` from a project or workload route carries a message that names the missing project, environment or workload.

## CLI

- `faber mesh invite --name <name> --wg-endpoint <ip[:port]> [--ttl 1h]`: `--name` and `--wg-endpoint` are required (`--endpoint` is taken by the global API URL flag `-e/--endpoint`). `--name` is the name the new node will hold, a DNS label. If a node already holds it, the new node replaces that node when it joins. `--wg-endpoint` takes IPv4, IPv6 or `[IPv6]`, with an optional port that defaults to the mesh `wg_port`; hostnames are refused. `--ttl` takes seconds or a number with `s`, `m`, `h` or `d`. In text mode stdout carries only the token, so `$(faber mesh invite ...)` works. Everything else goes to stderr: `name`, `node_id`, `mesh_ip` and `expires_at`, and, when the invite takes over a name, `WARNING: this invite replaces node <id> (<name>). When the new machine joins, that node is removed from the cluster.` In JSON mode the whole response goes to stdout.
- `faber cluster status`: the members table in text mode has the columns `node_id`, `name`, `addr`, `membership`, `matched_index` and `lag`. `name` is `-` for a member without one.
- `faber mesh lease [--set <duration>]`: without `--set` it prints the lease duration, in text mode as `lease: 8h (28800s)`. `--set` takes seconds or a number with `s`, `m`, `h` or `d`, changes the duration for the whole mesh and prints the new value. The server accepts 300 s to 30 d. Changing the duration restarts the lease of every node that has not expired.
- `faber project create <NAME> [--env <ENV>]...`: creates a project. `--env` names an environment it starts with and may be repeated. Without it the project gets `staging` and `production`. Prints the project like `project show`.
- `faber project ls`: a table with the columns `NAME`, `ENVIRONMENTS`, `WORKLOADS` and `CREATED`. `ENVIRONMENTS` is the names separated by `, `, or `-`. `WORKLOADS` is the sum over the environments. JSON mode prints the `ProjectListResponse`.
- `faber project show <NAME>`: `name:` and `created:`, then `environments:` and an indented table with the columns `ENVIRONMENT`, `WORKLOADS`, `GENERATION` and `UPDATED`. JSON mode prints the `ProjectResponse`.
- `faber project rm <NAME>`: prints `removed project <NAME>`, in JSON mode `{"removed": "<NAME>"}`. Fails with `not_empty` while an environment holds a workload.
- `faber project env add <PROJECT> <ENV>`: adds an environment and prints the project.
- `faber project env rm <PROJECT> <ENV>`: prints `removed environment <ENV> from <PROJECT>`, in JSON mode `{"project": "<PROJECT>", "removed": "<ENV>"}`.
- `faber mesh status-interval [--set <duration>]`: without `--set` it prints the status report interval, in text mode as `status_interval: 5s`. `--set` takes seconds or a number with `s` or `m`, from 1 s to 300 s, and prints the new value. JSON mode prints `{"status_interval_secs": N}`.
- `faber cluster key export [--file <PATH>]`: prints the cluster key, base64 on one line, so `$(faber cluster key export)` captures it. It sends no request. It reads the file named by `--file`, by `FABER_CLUSTER_KEY_FILE`, or `/var/lib/faber/cluster.key`, so it must run as root or as the faber user. The path must be a regular file, checked with `lstat`, so a symlink is refused even when it points at a regular file, and the file opened must be the one that was checked (`not a regular file`). At most 4096 bytes are read (`too large`). The content, trimmed, must be standard base64 of exactly 32 bytes (`valid cluster key`; an empty file says `empty`). Errors name the path and never hold file content. JSON mode prints `{"cluster_key": "..."}`. The key is a secret: whoever has it can open every env value of the cluster.
- `faber deploy spec <FILE|->` takes `--project <PROJECT>` and `--env <ENV>`. Both are required, and neither has a default or reads an environment variable. `FILE` is a JSON object that maps workload names to `WorkloadSpec`s, or `-` for stdin. A name that is in the object twice is an error: `the workload "web" is defined twice`. It also takes:
  - `--expose <WORKLOAD>`, which may be repeated: sets `expose` to `cluster` on that workload. A name the input does not have is an error.
  - `--prune`: also removes the workloads of the environment that the input does not have. Without it a deploy leaves them alone. An input with no workloads at all is refused before anything is sent, because it would remove every workload: `--prune: the input has no workloads, so every workload of the environment would be removed`. Without `--prune` the same input is refused as `<source>: nothing to deploy: the input has no workloads`.
  - `--dry-run`: validates, prints and sends nothing. In text mode stdout has the workloads as pretty JSON, which `deploy spec` reads back, so the output of a dry run can be reviewed, saved and deployed later.
- Every file a deploy reads must be a regular file of at most 8 MiB. A device or a socket is refused as `not a regular file`. Stdin is read up to 8 MiB too. Messages and the `read:` lines show a file by its real path, with symlinks resolved, relative to the current directory when the file is below it.
- What a deploy prints, in text mode:
  - stdout has the result, one aligned line each: `project:`, `environment:`, `generation:`, `created:`, `updated:`, `unchanged:` and `removed:`. The last four are names separated by `, `, or `-` for none.
  - stderr has one `note: <message>` line for each thing that is no fault of the input, printed before the request is sent, so it is on the screen even when the API then refuses the deploy. With `--dry-run` stderr also has one `read: <path>` for every file the command read, before the notes. Stdin is `-`.
- What a deploy prints, in JSON mode: one document on stdout and nothing on stderr. A deploy prints `{"result": DeployResponse, "notes": [...], "variables": [...]}`. A dry run prints `{"project", "environment", "spec", "notes", "variables", "files"}`, where `spec` is the workloads map. `variables` is always empty. That document is not input for `deploy spec`. Only its `spec` field is. When the API refuses a deploy that was sent, stdout gets no document. stderr then has one line of JSON, `{"notes", "variables"}`, and after it the `error:` line.
- When the input cannot be read or breaks a rule of the model, stderr has every error as `error: <source>: <path>: <message>`, then `error: nothing was deployed`. Nothing is sent and the exit code is 1, in both output modes.
- Two notes exist. One says that containers get no Linux capabilities unless the input adds them. It appears when no container of the deploy adds any. The other appears when the name of an env variable contains `PASSWORD`, `PASSWD`, `SECRET`, `TOKEN`, `API_KEY` or `PRIVATE_KEY`, in any case.
- `faber app ls [--project <PROJECT>] [--env <ENV>]`: a table with the columns `NAME`, `PROJECT`, `ENVIRONMENT`, `SCALE`, `AVAILABLE`, `EXPOSE` and `FQDN`. `SCALE` is `<count> (<strategy>)` or `daemon`. `AVAILABLE` is `available/desired`, taken from the list alone: the command makes one request, not one per row. Both flags are optional filters. Without them it lists every workload of the cluster. JSON mode prints the `WorkloadListResponse`.
- `faber app show <WORKLOAD> --project <PROJECT> --env <ENV>`: one workload. The CLI always sends `reveal=true`, so env values are always printed in clear, and the command fails with `key_unavailable` on a node that cannot open them. Masking is for the API. It keeps nothing from a user who can run the command. Text mode prints every field that changes how the workload runs:
  - `name`, `fqdn`, `project`, `environment`, `revision`, `generation`, `scale`, `restart`, `expose`, `nodes` (`-` without a pin) and `stop_grace` (`(runner default)` when unset).
  - `replicas:` with a table of `REPLICA`, `NODE`, `REVISION`, `STATE`, `POD IP` and `REASON`, or `-` before the leader has placed the workload. `NODE` is `-` while a replica is unplaced, and `REASON` then says why (see "Placement").
  - `volumes:` with one line per volume, mounted or not: the name, `block` or `ramdisk` and the size in bytes. `-` when there is none.
  - `containers:` and for each one `image`, `command`, `args`, `working_dir` and `user` (`(image default)` when unset), `env`, `ports`, `mounts` with the kind and size of the volume and `read-only`, `resources`, `capabilities`, `read_only_root_fs` and `probe`. The values of HTTP probe headers are never printed. JSON mode prints the `WorkloadResponse`.
- `faber app rm <WORKLOAD>... --project <PROJECT> --env <ENV>`: removes the workloads one after the other and prints `removed <fqdn>` for each, in JSON mode `{"removed": ["<fqdn>", ...]}`. It stops at the first failure, still prints what it removed before that, and exits 1.
- Text output replaces control characters in names and messages with `\xNN` or `\u{NN}`, so a name cannot move the cursor or recolour the terminal. Unicode format characters are replaced the same way: the bidirectional controls and the zero-width characters, which could reorder or hide part of a line.
- Exit codes: 0 on success. 1 for every failure, with `error: <text>` on stderr. For an answer from the API the text is `<code>: <message>`, or the code alone when the body has no message. An unknown project prints `error: not_found: project "nope" does not exist`. 2 for a usage error, from the argument parser.
- The CLI checks that a project, environment or workload name is a DNS label before it sends a request.
- `install.sh --join <token|->`: `-` reads the token from stdin.
- `install.sh --endpoint <host:port>`: autodetected from `ip -4 route get` if omitted. A re-run keeps the value in the env file. For a joiner it defaults to the token's `endpoint` and must match it.
- `install.sh --mesh-cidr <cidr>`: new mesh only. Combined with `--join` it exits 2.
- install.sh refuses an IPv4 address with a leading zero in an octet, such as `010.0.0.5`, in `--endpoint`, `--mesh-cidr` and `--container-cidr`. A lone `0` is fine. fabercp's parser refuses such an address, and the value goes into `fabercp.env`, so accepting it would leave the service failing at every start.
- `install.sh --container-cidr <cidr>`: new mesh only, default `10.200.0.0/16`. It must be a network address with a prefix length of at most 24 and must not overlap the mesh CIDR. Combined with `--join` it exits 2: a joining node gets its subnet from the cluster.
- `install.sh --name <name>`: new mesh only, the founder's name. It defaults to the short hostname in lower case, and the run stops in preflight if that is not a DNS label. Combined with `--join` it exits 2: a joining node's name is set by its invite.
- `install.sh --reset`: only with `--join`. It discards the identity and raft data this node has, so that a node the cluster removed can join again (see "Removed node").
- `install.sh` without `--join` stops in preflight on a node whose `bootstrap-error` says `node_removed` (see "Removed node").
- install.sh warns when `nft list ruleset` shows a forward chain with policy drop, and names the chains. It never changes the host firewall (see "Container network").
- A node behind NAT must use the public IP that its peers actually see.
- `faber health` exits non-zero when the node answers 503, and prints `unavailable: <message>`. The 503 body has the fields of `HealthResponse` and of the error body, so a client that reads either shape can decode it.
- `install.sh` stages the `faber` CLI in the parent of `FABER_CLI_DIR` and refuses a parent that root does not own or that is group/world-writable. `/usr/local` is `root:staff 2775` on some Debian releases. The error names the way out: run with `FABER_CLI_DIR` set to a directory whose parent only root can write, for example `FABER_CLI_DIR=/usr/bin`.
- The `faber` client (`FaberClient`) gives up on a connection after 5 s. `health`, `cluster status` and `mesh lease` time out after 30 s. `cluster init`, `cluster join`, `mesh invite` and `mesh lease --set` time out after 150 s, because the server waits up to 120 s for a join and 10 s more for the node to see the result. The project and workload commands follow the same split: `project ls`, `project show`, `app ls` and `app show` are reads and time out after 30 s, and every command that writes times out after 150 s.

## Encryption

Every container env value is encrypted at rest. The scheme is envelope encryption, in `faber_crypto`:

- The **cluster key** is 32 random bytes. It lives in a file on each node, `/var/lib/faber/cluster.key` (`--cluster-key-file`, `FABER_CLUSTER_KEY_FILE`), as base64 with a newline, owned by the faber user with mode 0600. It never enters raft, the invite token or a log.
- A **data key** is 32 random bytes, one per scope: `cluster` or `<project>/<environment>`. It is stored in raft as `keys/data/<scope>`, wrapped under the cluster key. A deploy to an environment that has none makes one. Nothing makes the `cluster` data key yet.
- Both seal with XChaCha20-Poly1305 and a random 24-byte nonce. The associated data is length-prefixed and starts with a label of its own for each use, so a wrapped key cannot pass as a sealed value. A data key is wrapped with its scope as associated data, so it opens only for that scope. An env value is sealed with the project, environment, workload, container and variable name, so a value cannot be moved to another variable.
- `CreateDataKey` is first-writer-wins. A proposer that loses reads the stored key back and seals again with it.
- The cluster key has an id: the first 8 bytes of an HMAC-SHA256 of a fixed label under the key, in hex. Raft records it once (`keys/cluster-id`), so a node whose key file holds another key is detected and refuses to seal or open (`key_unavailable`).
- The leader seals the env before it proposes, so neither the log nor a snapshot holds a plaintext value. A stored workload keeps its spec with empty `env`, and a `SealedEnv{digest, values}` per container. The `digest` is an HMAC of the plaintext env under the data key, with its location. It tells that an env is unchanged without comparing ciphertexts, which differ on every seal, and without a guessable hash of a low-entropy secret. A redeploy of an unchanged env keeps the stored ciphertext and the revision.
- Key material is zeroized on drop and prints as `[redacted]`.

**Masking.** An API read of a workload shows every env value as `***`, with the variable names. `?reveal=true` opens them with the cluster key of the node that answers, and writes one log line that names the workload and no value. `faber app show` always asks for `reveal=true`.

**Creation.** `faber cluster init` makes the cluster key file if it is missing, never replacing one, and has raft record its id. A leader without a recorded id records the id of its own key file at start and after every election.

**Delivery to a joiner.** The key is not in the join token, which can be copied. It is in the 200 answer to `POST /raft/cluster/register`, and only when the node that received the request sees it come from the mesh IP the invite is bound to. A request that a follower forwarded, or that arrives on a listener that skips source checks, gets none. The joiner checks the id of the key it was sent against the id raft records, stores it before it becomes a voter, and only then records the registration. If the id differs, the join fails with `key_mismatch`. If ten answers hold no key, it fails with `key_unavailable`. Both are written to `bootstrap-error`, and the node discards its identity and key file like one whose invite was rejected. A node that leaves the cluster removes its key file.

**The join token is a bearer secret.** Whoever registers with an invite first receives the cluster key, and can join the cluster anyway. Treat a token like the key: it is single-use and expires, so create it close to its use, send it over a private channel and do not log it. There is no operator approval before the key is released.

**Key export.** `faber cluster key export` prints the key from the file, with no request (see "CLI"). Keep a copy somewhere safe: without the key file on the nodes, no env can be opened, and a cluster whose every key file is lost cannot read its env values.

**Limits.** On a node that runs a replica, root can read the cluster key and the replica's env, and so can any local user through the loopback API with `?reveal=true`. A follower forwards a deploy to the leader with the env in clear over the mesh, which WireGuard encrypts. The key file is not rotated; there is no re-keying.

## faber-node

`faber-node` (`src/faber-node`) is a root service on each node. fabercp runs unprivileged, and faber-node does the work that needs root: CRI calls to containerd, mounts, chown, and the containerd and CNI config. It is the trust boundary. Nothing in a request is taken on trust, so a compromised fabercp can start containers only inside faber-node's limits.

**Socket.** `/run/faber/node.sock` (`FABER_NODE_SOCKET`, `--node-socket` of fabercp), created by faber-node as `root:faber` 0660 in a root-owned directory that group and others cannot write. faber-node refuses a socket directory that is not root's alone. It checks the caller with `SO_PEERCRED` and serves root and the configured faber uid only. fabercp checks that the process on the socket is owned by root before it sends anything.

**Protocol** (`faber-node-protocol`, `PROTOCOL_VERSION` 1). A frame is a `u32` big-endian length and that many bytes of JSON, at most 4 MiB (`MAX_FRAME_BYTES`). One request frame gets one response frame. The first request on a connection must be `version`. A connection idle for 60 s is dropped, and at most 64 are served at once. A request is a typed enum with an unknown-field refusal, tagged `type`:

| Request | Does |
|---|---|
| `version{client_version}` | the handshake. A client version faber-node does not speak is `version_mismatch` |
| `pull_image{image}` | pulls a public image |
| `ensure_sandbox{replica, uid_base, dns, ports}` | creates the replica's pod sandbox, or returns the ready one |
| `create_container{replica, sandbox_id, container, env, uid_base}` | creates a container. `env` replaces the spec's, which is ignored |
| `start_container`, `stop_container{container_id, grace_secs}`, `remove_container`, `remove_sandbox` | lifecycle. Only containers and sandboxes that faber made are touched |
| `ensure_volume{replica, volume_name, kind, size_bytes, uid_base}`, `remove_volume{replica, volume_name}` | see "Volumes" |
| `list_replicas`, `list_volumes` | what is on the node |
| `configure_runtime{pod_network}`, `remove_cni_config` | see "containerd and CNI config" |

A `ReplicaRef` is `{project, env, workload, revision, replica_index}`. The names are DNS labels, and `revision` is 4 to 32 lower-case hex characters. fabercp sends the stored revision as 16 hex digits (`{:016x}`). Env values are wiped from memory after use, and no `Debug` output holds them.

Responses are `version`, `image`, `sandbox`, `container`, `done`, `changed`, `replicas`, `volumes` and `error{code, message}`. The codes are `invalid_request`, `forbidden`, `not_found`, `version_mismatch`, `runtime` and `internal`.

**Trust checks**, on every request, whatever fabercp sends:

- The model's validation is run again on the container, and the request's env is checked with it.
- Capabilities outside `ALLOWED_CAPABILITIES` are `forbidden`.
- The container is never privileged. All capabilities are dropped and only the listed ones are added. `no_new_privs` is set, the runtime's default seccomp profile applies, and the pod runs in a user namespace (see below).
- Mounts are `nosuid,nodev`.
- A volume path comes only from (project, env, workload, replica index, volume) under faber-node's own volume root. All are DNS labels or numbers. A raw path is never accepted.
- `uid_base` must be exactly the start of a 65536-wide block (`USERNS_LENGTH`), a multiple of 65536, not 0, and inside the range faber-node owns (`FABER_NODE_UID_MIN` to `FABER_NODE_UID_MAX`, in its root-owned env file). The same check covers the owner of a block volume. A compromised fabercp therefore cannot map container root to host root.
- `create_container` is refused unless the sandbox carries the replica's labels and the same `uid_base`.
- The pod runs in a user namespace (CRI `userns_options`, POD mode) that maps container root to `uid_base` for 65536 ids, so root in a container is an unprivileged host user. It needs containerd 2.x and Linux 6.3 or later.
- Sandboxes are placed in `faber-<project>-<env>-<workload>.slice` (a dash inside a name becomes `_`), which the systemd cgroup driver needs.
- DNS: at most 3 nameservers and 6 search names, valid DNS names, no loopback, unspecified or multicast nameserver.
- The runtime config is the one in "containerd and CNI config".

**Config.** `/etc/faber/faber-node.env` (`--env-file`, `FABER_NODE_ENV_FILE`), `KEY=VALUE` lines. faber-node reads it by path and refuses it unless root alone can write it.

| Key | Default | Meaning |
|---|---|---|
| `FABER_NODE_UID_MIN`, `FABER_NODE_UID_MAX` | required | the host ids pods may map, `[min, max)`. The floor is one block. The installer writes 1048576 and 2147483648, the range placement hands out |
| `FABER_NODE_FABER_UID`, `FABER_NODE_FABER_GID` | required | the faber user. Must lie outside the pod range. The group owns the socket and block volumes |
| `FABER_NODE_SOCKET` | `/run/faber/node.sock` | |
| `FABER_NODE_VOLUME_ROOT` | `/var/lib/faber-node/volumes` | must be root-owned and not writable by others |
| `FABER_NODE_CONTAINERD_SOCKET` | containerd's | |
| `FABER_NODE_RUNTIME_HANDLER` | empty | CRI runtime handler |
| `FABER_NODE_BIN_DIR`, `FABER_NODE_SBIN_DIR` | `/usr/bin`, `/usr/sbin` | where `mount`, `umount`, `mountpoint`, `chmod` and `mkfs.ext4` are run from, by absolute path |
| `FABER_NODE_CONTAINERD_CONF_DIR`, `FABER_NODE_CNI_CONF_DIR`, `FABER_NODE_CNI_BIN_DIR` | `/etc/containerd/conf.d`, `/etc/cni/net.d`, `/opt/cni/bin` | see "containerd and CNI config" |
| `FABER_NODE_SANDBOX_IMAGE` | `registry.k8s.io/pause:3.10` | the pause image |
| `FABER_NODE_MESH_INTERFACE` | `faber0` | the WireGuard interface. Must match fabercp's `FABER_WG_INTERFACE` |

**Unit.** `faber-node.service` runs as root, `After=containerd.service`. It has `NoNewPrivileges`, `LockPersonality`, `RestrictRealtime`, `SystemCallArchitectures=native`, `RestrictAddressFamilies=AF_UNIX AF_NETLINK` and `CapabilityBoundingSet=CAP_SYS_ADMIN CAP_CHOWN CAP_FOWNER CAP_DAC_OVERRIDE`. It deliberately has no `ProtectSystem`, `ReadWritePaths`, `PrivateTmp` or `MountFlags`: each puts the unit in its own mount namespace, systemd.exec(5) says mounts made there do not propagate to the host, and `MountFlags=shared` does not restore that, so containerd would never see the volumes faber-node mounts. The hardening is limited to what needs no mount namespace.

faber-node runs on Linux only. Its tests and the Linux-only code of the other crates are built on Linux.

## Volumes

faber-node makes volumes under `<volume root>/<project>/<env>/<workload>/<replica index>/<volume>/`, one per replica, so each replica of a service has its own on its own node.

- A **block** volume is a sparse ext4 image (`disk.img`, made with `mkfs.ext4`) loop-mounted with `nosuid,nodev`. The root of the filesystem is owned on disk by `uid_base:faber_gid`, mode 0770. The pod's user namespace maps container root to `uid_base`, so the container can write, no other service's range can reach it, and the faber group can read it from the host. It is a plain bind mount into the container, not an idmapped mount. A volume that exists with another size is refused, so there is no resize. The runner never removes a block volume: it outlives the pod and the replica, and the disk stays until someone removes it by hand.
- A **ramdisk** is a tmpfs of `size_bytes`, mode 0770, owned like a block volume. It lasts until `remove_volume`. The containers step removes the ramdisks of replicas that are no longer on the node, found with `list_volumes`.
- A marker file records the kind and `uid_base` of a volume. `list_volumes` finds volumes by walking the root, never following a symlink.

## Status reporting

Each node reports on one interval: `faber mesh status-interval` and `PUT /api/v1/mesh/status-interval` set it for the cluster, 1 s to 300 s, 5 s by default.

- Once per interval a node sends one `ReportNode` through the leader (`POST /raft/cluster/status`): its capacity (CPU in millicores from the available CPUs, `MemTotal`, and the free space of the file system under `--data-dir`) and every replica its runner knows, at most 100. Capacity is measured at most once a minute.
- A replica is `{project, environment, workload, revision, replica_index, state, pod_ip?, detail?}`. `state` is `deploying`, `available` or `unavailable`. `detail` says why, in at most 200 bytes. `pod_ip` must lie in the reporter's own container subnet and not be its gateway.
- The node sends no time. Apply stamps the record with the lease clock (`MeshState.clock`), so a node's wall clock cannot lie. Status and lease renewals share one instance-id check, so another machine cannot report for a node.
- A report replaces the whole record of its node under `status/<node_id>` and writes nothing else, so no environment generation moves and the host loop sees no change.
- A record counts as stale when it is older than three intervals plus one tick of the lease clock (5 to 60 s). A replica on a stale node reads `unavailable` whatever it last said, and does not count as `available`. Nothing writes when a node goes quiet: staleness is worked out when the record is read. When the interval changes, readers use the larger of the new and the previous interval until reports made under the old one have gone stale.
- A node that reports nothing is not removed. Replicas move only when its lease expires (see "Placement").

## Placement

The leader decides which node runs each replica and stores the result in raft. It runs the planner when the replicated state changes: a deploy, a node report, a lease expiry. The plan is a pure function of the environments, the nodes (name, lease, last reported capacity) and the stored plan, so the same input gives the same plan and the leader proposes only when something changed.

- Candidates are nodes with a live lease and a capacity report, narrowed to `placement.nodes` when the workload lists any.
- An assignment is kept while its node is alive, still a candidate and the capacity still fits. A replica moves only when its node's lease has expired or the node is gone, never because its status went stale.
- `spread` prefers the node that runs the fewest replicas of the workload, then a node that already hosts the environment, then the least loaded. `pack` prefers a node that already runs the workload, then a node that hosts the environment, then the most loaded one that still fits. An environment's workloads therefore stay together while capacity allows. `daemon` is one replica per candidate node.
- Each replica reserves the sum of its containers' `cpu_millis` and `memory_bytes`, and the sizes of its block volumes against the reported disk, so resources are maximums that placement promises. Ramdisks count as memory.
- A replica of a workload with a block volume never moves once placed. If its node is removed, it stays unplaced with `volume_lost` until the workload is removed. There is no command to discard a lost volume.
- A replica with no node is `unavailable` and carries a `reason`. A reason is a prefix, optionally followed by detail, at most 64 bytes in all: `insufficient_capacity` (no candidate has the CPU, memory or disk left), `volume_lost`, `pinned_node_unknown: <names>` (`placement.nodes` names a node the cluster does not have, followed by the unknown names) and `no_node_available` (no allowed node is alive). Match on the prefix.
- The command is `PlaceReplicas{project, environment, generation, basis, workloads}`. Apply refuses a plan for another generation than the environment's, and one made from state older than the stored plan, and stores nothing else. `basis` is the index of the last entry the leader had applied when it read the state.
- Each workload gets a host uid range of 65536 ids from `[1048576, 2147483648)`, aligned to 65536, allocated in raft (`place-uids`) when the workload is first placed and released when it is removed. The lowest free range goes first. It is the `uid_base` of its pods. All replicas of a workload share it, and no two workloads share one.

## Virtual IPs

Each service (a workload) gets a virtual IP from the **service CIDR**, default `10.201.0.0/16`, set when the mesh is created (`--service-cidr`, `FABER_SERVICE_CIDR`, `install.sh --service-cidr`) and fixed after that. It must have no host bits, a prefix of /30 or shorter, and must not overlap the mesh or the container range. fabercp checks it at startup with `--new-mesh`, as it checks `--container-cidr`, and `InitCluster` rejects the same ranges with `bad_request`.

- The leader's planner proposes `AllocateServiceIp` for a workload that has no address, never more than there are free addresses, and `ReleaseServiceIp` for an address whose workload is gone. Addresses go out in a ring starting at the first host address, so a released address is reused only after the ring has gone round. A workload keeps its address across revisions, so a deploy does not wait on a DNS cache. A full range answers `mesh_exhausted`.
- Every node programs every service into nftables, so a container reaches a service from any node without an extra hop.
- A **backend** is a replica that is placed, reported `available`, has a pod IP and is on a node whose report is not stale. The service serves the ports its containers declare, on the same port numbers.
- **Active revision.** The newest revision (not newer than the stored one) whose placed replicas are all available. Otherwise the newest revision with at least one available replica. A new revision takes over only when it is complete, so the backends switch from the old revision to the new in one step (blue/green). Open connections keep their conntrack entry to the old replicas, which stop after the drain period.
- A replica that fails a probe leaves the backends at once.
- The services step runs after the network and runtime steps. Its desired value is, per service with an address, the address and the sorted `(pod_ip, port, protocol)` list, with no timestamps, so a status tick that changes nothing causes no update.
- A node without nftables nat support fails to start.

nftables chains (see "nftables") do the work:

- `prerouting` (hook prerouting, priority dstnat), `output` (hook output, priority -100) and `postrouting` (priority srcnat) are nat chains in `inet faber`, present when container networking is on.
- Per service and per (protocol, port): `ip daddr <vip> <proto> dport <port> dnat ip to numgen random mod N map { 0 : <ip>, ... }`. In `prerouting` it applies only to packets from the bridge, and in `output` to the host's own. A port with no backend gets no rule.
- Hairpin: in `postrouting`, `oifname <bridge> ip saddr <b> ip daddr <b> ct status dnat masquerade` for each backend `b` in the local subnet, so a container that reaches its own service sees a source that is not itself.
- A host connection to a service that reaches a pod on another node is masqueraded to the tunnel address: `oifname <iface> ip saddr != <container cidr> ip daddr <container cidr> ct status dnat masquerade`.
- Two filter chains, `service_reject_prerouting` (from the bridge) and `service_reject_output`, reject any packet still addressed to the service range after the nat chains: an address with no backend, or one no service holds.
- The forward chain needs no new rule: container-to-container traffic across nodes is already allowed, and `br_netfilter`, which install.sh loads, makes bridged traffic pass these chains.
- `NetworkManager::set_services` keeps the service rules, so a network reload keeps them.

## DNS

fabercp answers DNS for the cluster on every mesh node, from its own server.

- **Zone.** `faber.local`. A service is `<svc>.<env>.<project>.faber.local`: exactly five labels, each a DNS label, matched case-insensitively.
- **Listen address.** The node's bridge gateway (the first host address of the node's container subnet), UDP and TCP, port 5300 (`DNS_REDIRECT_PORT`, `dns::DEFAULT_PORT`). The server binds with `IP_FREEBIND` because the bridge may not exist yet. nftables redirects port 53 on the gateway to it, from the bridge, so fabercp needs no privileged port: `iifname <bridge> ip daddr <gateway> udp dport 53 redirect to :5300`, and the same for tcp. Pods get the gateway as their only nameserver and `<env>.<project>.faber.local` as their search domain, set in CRI `dns_config` by `ensure_sandbox`.
- **Answers** for a name under `faber.local` are authoritative (AA set, no SOA): a service with a virtual IP and type A is NOERROR with one A record, TTL 30 s. Another type (AAAA, TXT, ANY) is NOERROR with an empty answer. Any other name under `faber.local`, including `faber.local` itself, a wrong label count, an invalid label or an unknown service, is NXDOMAIN. A class other than IN is REFUSED. If the store cannot be read, SERVFAIL.
- **Other names** are relayed unchanged to the host's upstream resolvers and the answer is relayed back. UDP clients are forwarded over UDP, TCP over TCP. Each upstream gets 1 s, and the whole forward is bounded by 2 s (`UPSTREAM_TIMEOUT`). A reply counts only if its id and question match. If none answers, SERVFAIL. Upstreams are the `nameserver` lines of `/etc/resolv.conf` (or `/run/systemd/resolve/resolv.conf` when that leaves none), port 53, skipping loopback, unspecified and scoped addresses, the node's bridge address and any address inside the container or service range.
- **Limits.** UDP replies over the client's EDNS size (512 bytes without EDNS) are cut to the question with TC set. Other opcodes get NOTIMP, a query that does not hold exactly one question gets FORMERR, and a packet with the QR bit set is dropped. At most 512 forwarded UDP queries and 256 TCP connections are handled at once, and a TCP connection closes after 10 s idle, 30 s in all or 100 queries.
- The server follows the replicated mesh, starts once the node has a container subnet and restarts when the subnet or the ranges change.
- This DNS layer is expected to be replaced later. `expose` is not enforced: DNS answers for every service.

## The runner

The runner runs the replicas that placement put on this node, through faber-node. It has two parts. The **containers step** is in the reconcile loop and makes sandboxes and containers match placement. The **supervisor** is its own task on its own ticker, independent of the 300 s resync. fabercp runs the probes itself, because it reaches the pod IPs over the bridge. The step order is network, runtime, services, containers. Services runs before containers so that a long image pull never delays a virtual IP update, and the containers step is not applied at startup: the loop does it on its first pass.

**What the containers step sends**, per placed replica in this order: `ensure_volume` for each declared volume in name order, `pull_image` for each distinct image, `ensure_sandbox` (the DNS above, and every port of every container), then `create_container` and `start_container` for each container that does not exist yet. Container names are `<workload>-<revision>-<container>` with the revision as 16 hex digits. `stop_container` carries the workload's `stop_grace_secs`, 30 s when unset (`DEFAULT_STOP_GRACE_SECS`). The env is fetched when a container is made, and the runner holds none. A workload whose env cannot be opened (no cluster key, the wrong key, no data key) is held: its pods are neither started nor removed, and a warning is logged.

**The supervisor** polls `list_replicas` every second (`POLL_INTERVAL`), so an exit is noticed within one poll.

- **Probes** go to the pod IP, only inside this node's container subnet. The first probe is one `interval_secs` after the container is seen up. One failure makes the replica report `unavailable`. Three in a row stop the container (`stop_container`), and it then restarts under the restart policy as if it had exited with code 137, so `never` leaves it stopped.
- **Restart policy.** `always` restarts on any exit. `on_failure` restarts on an exit code other than 0, and a container that vanished counts as a failure. `never` does not restart. There is no retry limit.
- **Backoff** per container starts at 100 ms (`BACKOFF_MIN`), doubles on each restart up to 60 s (`BACKOFF_MAX`), and goes back to 100 ms when the previous run was up for at least 10 s (`BACKOFF_RESET_AFTER`). A restart is `ensure_sandbox`, `remove_container` of the old one, `create_container`, `start_container`.
- **Replica state** in a report: `deploying` until the step has started the replica and while a container is up and its first probe has not passed, `available` when every container is up and every probe passed (or it has no probe), `unavailable` otherwise. `pod_ip` is the sandbox address. A report holds every replica the runner knows, including older revisions that are draining, at most 100.

**Stop order.** Every stop of a replica, whether placement removed it, the workload was scaled down, a newer revision replaced it or a probe failed three times, goes in this order: the replica reports `unavailable` at once, which removes it from the virtual IP's backends, then the supervisor waits the drain period, then it sends `stop_container`, then `remove_sandbox`, then removes its ramdisks. The drain period is 10 s (`DRAIN_PERIOD`), so connections that are open finish. Probe-triggered and removal stops run off the supervisor tick.

**New revisions.** When placement has a newer revision of a workload on this node, replicas of older revisions are marked superseded and keep running. Once every replica of the newest revision on this node reports `available`, the older ones run for the drain period and are then stopped. The virtual IP's backends switch in that window. If the newer revision never becomes available, the older one runs on.

> **Block volumes and downtime.** For a workload with a block volume, revisions do not overlap: the old replica of a slot is stopped, after the drain period, before the new one starts, because they would share the volume. The slot is not running in between, and in this version every replica of the workload is replaced at the same moment, so a deploy that changes the pod template can make the whole service unavailable for the length of a stop and a start. Run several replicas, on different nodes, so that losing one replica or one node does not take the service down, and plan a template change to such a workload as a short outage.

## containerd and CNI config

faber-node writes the containerd drop-in and the CNI network config. fabercp does not, and has no setting for either, so a compromised fabercp has no path to config that root loads.

- **Requests.** `configure_runtime{pod_network}` is answered with `changed{changed}`. `pod_network` is `{subnet, mtu}`, or null to remove the CNI config. `remove_cni_config` removes the CNI config and nothing else, for a node that is leaving the mesh. Unknown fields are refused, so no path, file body, bridge name, image or cgroup setting can be sent.
- **faber-node alone decides** the pause image (`FABER_NODE_SANDBOX_IMAGE`), `SystemdCgroup = true`, the bridge name (`faberbr0`, `POD_BRIDGE`, which fabercp also uses for the firewall), the CNI plugin directory and both config directories.
- **Checks on `configure_runtime`.** The subnet follows the rule that fabercp also applies to the container CIDR (`check_container_subnet`): no host bits, a prefix of at most /30, no overlap with `0.0.0.0/8`, `127.0.0.0/8`, `169.254.0.0/16`, `224.0.0.0/4` or `240.0.0.0/4`. It is also checked against the host, read with `ip -j -4 addr show` and `ip -j -4 route show table all` by absolute path: refused (`forbidden`, "the subnet is not allowed on this host") when it overlaps an address of an interface other than `faberbr0`, or holds a longer route on another interface, or conflicts with a `blackhole`, `unreachable`, `prohibit` or `throw` route in any table. It must lie inside a route of the mesh interface or the bridge, so it can never sit only inside `default` or a LAN route. fabercp installs `ip route replace <container range> dev <wg iface>` in the network step, which runs first, and if that route failed the runtime step is refused and the loop retries. `mtu` is 576 to 9000.
- **Ownership.** `/etc/containerd/conf.d/` and `/etc/cni/net.d/` are `root:root` 0755. `faber.toml` and `10-faber.conflist` are written by faber-node, mode 0644. At startup and before every write, faber-node checks the containerd conf dir, the CNI conf dir and the CNI bin dir, and every directory above each: owned by root, not writable by group or others, and the leaf not a symlink. A missing conf dir is created `root 0755` at startup, a missing bin dir stops faber-node, and a directory that fails the check answers `forbidden` and nothing is written. Every regular file in the CNI bin dir must be owned by root and not writable by group or others. A symlink there is followed only to such a file in a directory whose path passes the check, and a dangling one stops runtime writes (fail closed).
- **Writes** go to a temp file with a random name (`O_EXCL|O_NOFOLLOW`), the target is read with `O_NOFOLLOW` (a symlink there is replaced, not followed), then fsync, rename and fsync of the directory.
- **Accepted limit.** fabercp holds `CAP_NET_ADMIN`, so it can change host routes itself. The subnet check guards the CNI path only.
- The runtime step is not required at startup, since faber-node may start later. The loop applies it, and applies nothing while the mesh is not replicated.

## systemd units

`fabercp.service`:

```
[Unit]
After=network-online.target containerd.service
Wants=network-online.target

[Service]
User=faber
Group=faber
EnvironmentFile=/etc/faber/fabercp.env
Restart=always
RestartSec=2
AmbientCapabilities=CAP_NET_ADMIN
CapabilityBoundingSet=CAP_NET_ADMIN
NoNewPrivileges=yes
StateDirectory=faber
StateDirectoryMode=0700
ProtectSystem=strict
ReadWritePaths=/etc/containerd/conf.d
ReadWritePaths=/etc/cni/net.d
ProtectHome=yes
PrivateTmp=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK
```

The second `ReadWritePaths=` line lets fabercp write `10-faber.conflist` under `ProtectSystem=strict`. systemd merges repeated `ReadWritePaths=` lines. The capabilities are unchanged: root CNI plugins create the bridge, not fabercp.

fabercp shuts down cleanly on SIGTERM (what `systemctl stop` sends) as well as on SIGINT.

If the raft core stops while fabercp runs (a fatal storage error, a panic inside raft), fabercp logs the reason, shuts down what it can and exits non-zero, and `Restart=always` starts it again. Until the process is gone, `GET /api/v1/health` answers 503.

`faber-containerd-reload.path`: `PathChanged=/etc/containerd/conf.d/faber.toml`, starting `faber-containerd-reload.service`.

`faber-containerd-reload.service`: `Type=oneshot`, `ExecStart=/usr/bin/systemctl try-restart containerd`, with `StartLimitIntervalSec` and `StartLimitBurst` set.

## Host commands (fabercp, when `FABER_MANAGE_HOST=1`)

Order at startup, all before the peer listener binds:

1. The ownership check, before the identity is loaded or created (see below). On first start it runs `ip link show faber0`, writes the ownership record, then runs `ip link show faber0` again and `ip link add faber0 type wireguard`.
2. `ip link show faber0`, then `ip link add faber0 type wireguard` only if the link is missing (after a reboot)
3. `ip -o -4 address show dev faber0`, then `ip address del <addr> dev faber0` for every IPv4 address on the link other than the wanted one
4. `ip address replace <mesh_ip>/<prefix> dev faber0`. This comes after the deletes: deleting a primary address also removes the secondaries in its subnet, which can include the wanted address
5. `wg syncconf faber0 /dev/stdin`. The config, including the private key, goes on stdin, never argv.
6. `ip link set faber0 mtu 1420 up`
7. `ip route replace <container_cidr> dev faber0`, when the replicated `mesh` has a container range. It comes after the link is up, because the kernel rejects a route through a link that is down. fabercp never removes the route: the range does not change once a mesh exists, and deleting the link removes its routes
8. `nft -f -` with the `inet faber` table on stdin
9. write the containerd drop-in if it changed
10. write `10-faber.conflist` if it changed, or remove it when this node has no container subnet. A missing CNI directory is an error only when there is a file to write

Steps 9 and 10 run once `config/containerd` is replicated. A fresh joiner has no `mesh` at startup, so it runs no step 7 and gets its route, container rules and conflist from the reconcile loop once the state reaches it.

Any failure exits the process, and systemd restarts it.

The peer listener then binds `<mesh_ip>:<peer_port>` from `identity.json`. It does not take whichever address the interface happens to have.

### Interface ownership

fabercp only configures a WireGuard interface it created. The record is `/var/lib/faber/iface`, which holds the interface name. fabercp writes it (temp file, fsync, rename) after `ip link show` found no such link and before `ip link add` runs.

- No record and no link: fabercp writes the record, then creates the link. If `ip link add` fails, it removes the record again and startup fails.
- No record, but the link exists: startup fails with `interface faber0 exists and was not created by faber`. It has run only `ip link show`. It writes no record and no identity, and runs no `ip address`, `wg` or `nft` command. Remove the interface or set `FABER_WG_INTERFACE` to another name.
- Record present: the link is fabercp's. If it is missing, step 2 creates it. That is the state after a reboot, and also after a kill between writing the record and `ip link add`.

The record goes first because the other order is not crash safe. A kill after `ip link add` and before the record would leave a link without a record, and every later start would refuse it. The price is a small window: if fabercp dies between the record and `ip link add`, and someone else creates an interface of that name before fabercp restarts, fabercp takes that interface for its own.

`NetworkManager::teardown` deletes the interface (`ip link show`, then `ip link del` if the link exists). fabercp calls it when an invite is rejected and then removes the record, see "Rejected invite".

A record that names a different interface does not count. install.sh skips its own "foreign `faber0`" preflight when `fabercp.env` already has `FABER_WG_INTERFACE=faber0`.

## nftables

Table `inet faber`, replaced atomically on each load. fabercp owns only this table and adds no default drop for the rest of the host.

| Set | Type | Members |
|---|---|---|
| `peer_endpoints` | `ipv4_addr` | endpoint IPs of the other nodes and live invites |
| `peer_endpoints6` | `ipv6_addr` | IPv6 endpoint IPs, same rule (usually empty) |
| `peer_mesh_ips` | `ipv4_addr` | mesh IPs of the other nodes and live invites |
| `node_mesh_ips` | `ipv4_addr` | mesh IPs of the other registered nodes. Invites are not in it, so a machine that only holds an invite has no way to the containers. The set is in the table only when the mesh has a container range |

Rules, chain `input` (`type filter hook input priority filter - 10; policy accept;`):
- `iifname != "faber0" udp dport <wg_port>`: accept from `@peer_endpoints`/`@peer_endpoints6`, drop the rest.
- `iifname "faber0"`: accept `ct state established,related`, accept new traffic from `@peer_mesh_ips`, drop the rest.
- `iifname != { "lo", "faber0" } tcp dport <peer_port> drop`.

The input chain is the same with and without a container range.

Rules, chain `forward` (`type filter hook forward priority filter - 10; policy accept;`), in a mesh without a container range:
- `oifname "faber0" drop`
- `iifname "faber0" drop`

That chain stops a host with `ip_forward` enabled from routing traffic into or out of the mesh.

In a mesh with a container range, the forward chain lets container traffic through the tunnel and nothing else. This is the chain as rendered for the range `10.200.0.0/16` on a node whose subnet is `10.200.1.0/24`. Exact text is set by the golden tests for `faber_network_management::nftables::render`.

```
	set node_mesh_ips {
		type ipv4_addr
		elements = { 10.99.0.1, 10.99.0.4 }
	}
	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		iifname "faberbr0" oifname != { "faber0", "faberbr0" } drop
		oifname "faberbr0" iifname != { "faber0", "faberbr0" } drop
		iifname "faber0" oifname "faber0" drop
		iifname "faber0" ct state established,related accept
		oifname "faber0" ct state established,related accept
		ip saddr 10.200.1.0/24 ip daddr != 10.200.0.0/16 drop
		iifname "faber0" oifname "faberbr0" ip saddr @node_mesh_ips ip daddr 10.200.1.0/24 accept
		iifname "faber0" oifname "faberbr0" ip saddr 10.200.0.0/16 ip daddr 10.200.1.0/24 accept
		iifname "faberbr0" oifname "faber0" ip saddr 10.200.1.0/24 ip daddr 10.200.0.0/16 accept
		oifname "faber0" drop
		iifname "faber0" drop
	}
```

One line per rule, in order:

1. `iifname "faberbr0" oifname != { "faber0", "faberbr0" } drop`: what comes from the bridge leaves through the bridge or the tunnel, and through no other interface.
2. `oifname "faberbr0" iifname != { "faber0", "faberbr0" } drop`: what goes to the bridge came from the bridge or the tunnel. Nothing reaches a container from the host's other interfaces.
3. `iifname "faber0" oifname "faber0" drop`: no node relays traffic from one peer to another. The route for the range points at `faber0`, so without this rule a packet for the local subnet would go back out of the tunnel while the bridge does not exist yet.
4. `iifname "faber0" ct state established,related accept`: replies coming in from the tunnel.
5. `oifname "faber0" ct state established,related accept`: replies going out into the tunnel.
6. `ip saddr <local> ip daddr != <cidr> drop`: a local container opens no connection to anything outside the container range, on any interface. Container egress is off. The rule sits after rules 4 and 5, or it would drop a container's replies to a node. For packets from the bridge, rules 1 and 10 already do this. The rule stays for a container attached to some other interface.
7. `iifname "faber0" oifname "faberbr0" ip saddr @node_mesh_ips ip daddr <local> accept`: new traffic from a registered node to a local container.
8. `iifname "faber0" oifname "faberbr0" ip saddr <cidr> ip daddr <local> accept`: new traffic from a container on another node to a local container. Rules 7 and 8 are separate because a named set cannot be an element of an anonymous one. Both name the bridge as the output: the local subnet is only behind the bridge, and this keeps a peer from reaching another interface through a more specific route.
9. `iifname "faberbr0" oifname "faber0" ip saddr <local> ip daddr <cidr> accept`: new traffic from a local container to a container on another node. It names the bridge as the input because the `AllowedIPs` on the other nodes trust any packet with a `<local>` source that comes out of this node's tunnel. A host on another interface that forges such a source must not get in.
10. `oifname "faber0" drop`: everything else routed into the tunnel.
11. `iifname "faber0" drop`: everything else routed out of the tunnel.

Rules 1 and 2 match interfaces by name and no address. Three things follow:

- They cover IPv6 and a container that forges its source address. Rules 6 to 9 match IPv4 addresses only.
- They hold once `faber0` is deleted, as after a teardown that keeps the table. Container traffic then follows the default route and is dropped, not sent in clear text.
- They drop replies too. No connection they cut off was allowed to start.

Rules 6 to 9 name the node's own subnet. fabercp leaves them out until raft has given the node a subnet, and the chain is then rules 1 to 5, 10 and 11.

Traffic between two containers on the same bridge has the bridge as both interfaces and matches no rule. It passes this chain at all only with `br_netfilter` loaded.

An `accept` in this table only ends evaluation of this base chain. Chains of other tables on the same hook still run and can drop the packet, so these accepts never open what the admin's firewall closes. A `drop` is final.

A handshake from an unlisted IP is dropped, so a changed endpoint (roaming, a new NAT address) needs `SetEndpoint`. UDP 51820 must also be open in cloud security groups and host firewalls, which `inet faber` does not manage.

## Container network

Each node has its own container subnet, and the nodes route those subnets to each other over the WireGuard mesh. A container on one node reaches a container on another by its own address, with no NAT.

### Range and subnets

The container range is `MeshConfig.container_cidr`, default `10.200.0.0/16`, set with `install.sh --container-cidr` when the mesh is created and fixed from then on. Each node gets one subnet of it with the prefix length `MeshConfig.container_subnet_prefix`, which is 24. The default range therefore holds 256 nodes.

Raft allocates the subnets in apply, so every replica agrees:

- The founder gets the first subnet of the range in `InitCluster`.
- Every other node gets the lowest free subnet in `RegisterNode`, or the subnet of the node it takes over (see "Takeover").
- A node keeps its subnet until its record is deleted. An expired lease does not free it.
- An invite holds no subnet. `CreateInvite` only keeps one free for it and answers `mesh_exhausted` when it cannot.

The subnet is not in the invite token and not in `identity.json`. A node reads its own subnet and every peer's from the replicated `mesh` on each reconcile, so a joiner has no container network until `RegisterNode` has replicated to it.

### What each node configures

From the replicated `mesh`, on every reconcile:

- The route `ip route replace <container_cidr> dev faber0`. A destination in the node's own subnet goes to the bridge instead, because the bridge's route has the longer prefix.
- WireGuard `AllowedIPs` for every other registered node: its mesh `/32` and its container subnet. Peers that are live invites, and the bootstrap peers of a joiner, keep the `/32` alone. The route sends the range into the interface, and `AllowedIPs` picks the peer that gets a packet and limits the source addresses a peer may send from.
- The forward chain and the `node_mesh_ips` set of `inet faber` (see "nftables").
- `/etc/cni/net.d/10-faber.conflist` for the node's own subnet, with the bridge `faberbr0` and the tunnel's MTU of 1420.

fabercp skips a subnet that does not fit, and logs why: one whose prefix length is not `container_subnet_prefix`, that has host bits set, that lies outside the range, that is this node's own subnet on a peer, or that an earlier peer already has. WireGuard gives an `AllowedIP` that two peers list to the last of them without an error, so a duplicate must not reach it. If the node's own subnet does not fit, the node gets no container rules that name it and no conflist.

In a mesh without a container range there is no route, no container rule and no conflist.

### CNI network config

fabercp writes this file for a node whose subnet is `10.200.3.0/24`. Exact text is set by the golden test for `faber_container_runtime::cni::render`.

```json
{
  "cniVersion": "1.0.0",
  "name": "faber",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "faberbr0",
      "isGateway": true,
      "isDefaultGateway": true,
      "forceAddress": true,
      "ipMasq": false,
      "hairpinMode": true,
      "mtu": 1420,
      "ipam": {
        "type": "host-local",
        "ranges": [
          [
            {
              "subnet": "10.200.3.0/24"
            }
          ]
        ],
        "dataDir": "/run/cni/networks"
      }
    }
  ]
}
```

- `cniVersion` is `1.0.0` because containerd 1.6 rejects `1.1.0`.
- `ipMasq` is false: there is no NAT.
- `dataDir` is under `/run`, so `host-local` address leases do not survive a reboot.
- containerd watches the directory, so a new or removed file needs no restart. The reload path unit does not watch it.
- fabercp removes the file while the node has no subnet. A node must not keep a config for a subnet that may now belong to another node.

### Who can reach what

| From | To | Result |
|---|---|---|
| a container | a container on any node | allowed |
| a registered node | a container on any node | allowed |
| a machine that only holds an invite | any container | dropped: it is not in `node_mesh_ips` and its `AllowedIPs` are its `/32` alone |
| a container | the mesh IP of another node | dropped by rule 6 of the forward chain on the container's own node |
| a container | anything outside the container range | dropped on the container's own node: by rule 1 when the packet would leave through any interface other than the tunnel, by rule 6 otherwise |
| anything behind another interface of the host | a container | dropped by rule 2 |
| a container, over IPv6 | anything beyond the bridge | dropped. Rule 1 drops what would leave through another interface, and rule 10 what would enter the tunnel. No rule accepts IPv6 |
| a container with a forged source address | anything beyond the bridge and the tunnel | dropped by rule 1, which matches no address |
| a host on another interface of the node, with a forged source in the node's subnet | a container on another node | dropped by rule 10. Rule 9 accepts that source only from the bridge, so the packet never enters the tunnel |
| a peer, through the tunnel | an address of the node's subnet that is routed to another interface | dropped by rule 11. Rules 7 and 8 accept only towards the bridge |
| a container, after `faber0` is deleted | anything beyond the bridge | dropped by rule 1. The traffic follows the default route and never leaves in clear text |
| a container | its own node | the input chain applies: TCP `<peer_port>` and UDP `<wg_port>` are dropped, and `inet faber` drops nothing else |

Limits:

- Container egress is off. There is no NAT and no rule that lets a container reach an address outside the container range. Image pulls are not affected, because containerd pulls on the host.
- There is no per-container policy. Any registered node can reach every container, and any container can reach every other container.
- A host firewall whose forward chain has policy drop (ufw, Docker and firewalld commonly set one) blocks container traffic between nodes, because that traffic is forwarded between `faber0` and `faberbr0` and a drop in any chain on the hook is final. install.sh warns and names the chains. faber never edits that firewall: the admin has to allow forwarding between `faber0` and `faberbr0` in both directions.

## Node lease

Every node holds a lease on its place in the mesh: its record, and with it its name, mesh IP and container subnet. The node renews the lease through raft. When the lease runs out, the leader removes the whole node: its raft membership first, then its mesh record. The other nodes drop its WireGuard peer and its firewall entries on their next reconcile.

The lease lasts `MeshState.lease_secs`, 8 hours by default. An admin reads it with `faber mesh lease` and changes it on a running cluster with `faber mesh lease --set <duration>`, between 300 s and 30 d. It is a sliding window: each renewal restarts it, so the duration is also the time a node has to come back after it loses contact.

### Renewal

Each member runs a renewal loop once its join has finished and the mesh has replicated to it:

- It renews at once, and then every quarter of the lease, measured on the monotonic clock. With the default lease that is every 2 hours.
- It sends `RenewLease` with its `node_id` and `instance_id`: directly if it leads, otherwise to the leader's `POST /raft/cluster/renew`.
- When `lease_secs` changes, it renews at once and follows the new duration.
- `mesh_node_removed` ends the loop: the node has been removed (see "A removed node").
- `mesh_lease_expired` does not. The node logs a warning once, keeps its raft gate open and its interface up, and asks again once per renewal period, with no growing delay. The lease has run out, but the node is still in the mesh and may be a voter the others need: the leader keeps an expired voter while removing it would cost the cluster its quorum, and a node that tore itself down on this answer would take away the vote the leader kept it for. Later the answer becomes `mesh_node_removed` once the record is gone, or a renewal succeeds again if an admin lengthens the lease far enough.
- Any other failure (no leader, an unreachable peer, a timeout, `not_ready`) is retried, after a delay that grows from 200 ms to 10 s, or at once when raft reports a new leader. One attempt gives up after 30 s. Nothing is torn down.
- A refusal that no retry can change (`forbidden`, `bad_request`) is retried the same way and logged as a warning at once.
- Once half a lease has passed without a renewal the node logs a warning, then again once per lease period, and keeps trying.

### Leader task

Every mesh node runs the task. It acts only while the node is the raft leader and the mesh exists. A tick comes every sixtieth of the lease, no less than 5 s and no more than 60 s apart. With the default lease that is every 60 s. On each tick the leader:

1. Writes nothing if no quorum has acknowledged it within the last 30 s. A leader cut off from its quorum would otherwise append one uncommitted entry per tick.
2. Finishes a voter change that an earlier tick already saw stuck at its joint config, if the quorum guard allows it. While the config is joint, it removes no other raft member.
3. Proposes `AdvanceClock` with the whole seconds since its previous tick as leader, capped at two ticks. The first tick of a spell as leader proposes zero, so time the node did not lead is never counted. The time of a tick whose write failed is dropped, not proposed again.
4. Collects the nodes whose lease has expired, other than itself.
5. Removes from the raft membership the expired nodes that are members, and any raft member that has no mesh record, if the quorum guard allows it. On its first tick as leader it removes nothing.
6. Proposes `RemoveNode` for each expired node that is no longer a raft member.

A mesh node that is not a raft member yet is left alone until its lease expires. That is a node in the middle of its join, which registers before it becomes a learner. Every failure is logged and tried again on the next tick.

The leader never removes itself through expiry, and it does not tear itself down either. Its own renewals are refused with `mesh_lease_expired`, which no node takes as removal, so it goes on leading with an expired lease until another node leads and removes it.

The quorum guard decides whether a set of nodes can leave the raft membership without stalling the cluster. A voter counts as caught up when it has replicated the `AdvanceClock` entry of the previous tick, and the leader always counts. The removal is allowed when the caught-up voters are a strict majority of both:

- the voters that would remain. Otherwise the new config could not commit anything. With three voters, one dead and one expired but alive, removing the live one would leave two voters of which one answers.
- the voters there are now. Raft changes voters through a joint config, which needs a majority of the old set as well as the new one.

While raft is in a joint config, every voter set of that config must pass. When the guard refuses, the leader logs a warning and tries again on the next tick.

### A removed node

A node tears itself down only on evidence that the mesh has removed it. Two things count:

- The leader answers a renewal with `mesh_node_removed`: the mesh has no record of the node, and a record goes only after its node has left raft. A joiner takes the same answer to its join request as evidence.
- The raft membership the node has applied no longer has it at all, or is a joint config in which the node is a voter of an older voter set and not of the newest. The second form is what a removed follower sees: the leader stops replicating to a node once the membership without it is in the leader's log, so the joint config is the last membership the node receives. A membership that is in the log but not applied does not count, because the next leader can still drop it. An empty membership does not count either.

The node then does what "Removed node" describes: it refuses raft RPCs, stops the host reconcile loop, removes its conflist, deletes the WireGuard interface, writes `node_removed` to `bootstrap-error`, discards its identity and stays up idle. The `inet faber` table stays loaded. The same happens to the old node of a takeover that is still running.

`mesh_lease_expired` is not evidence (see "Renewal"). Silence is not evidence either. A node that cannot renew for a whole lease may have been removed, or the network may be down for every node at once, and wiping on silence would then destroy the cluster.

### Known limits

- A node that is removed while it is cut off does not learn it. It keeps its identity, its interface and its containers, on a subnet the mesh may give to another node, until the admin runs `install.sh --join <token> --reset` with a new invite. The other nodes no longer list it as a peer, so their firewalls drop its handshakes and it stays isolated.
- A removed follower learns of its removal only if the joint config reached it. The leader stops replicating to it once the membership without it is in the leader's log. A follower that missed the joint config sees no membership change, and once its record is deleted the other nodes drop its renewals at the firewall, so it gets no `mesh_node_removed` either. It is then in the position of the node above.
- A cluster of two voters cannot recover from the death of one on its own. Nothing commits, so the clock does not advance, no lease expires and no node is removed, invited or taken over. Three nodes are the smallest cluster that survives a loss.
- Taking over the only voter never completes. The quorum guard refuses a change that would leave no voter, so the register answers `not_ready` on every retry until the invite expires.
- A member that was down while a new node registered, joined and became leader refuses that leader's raft messages when it comes back, because it knows the leader only as an invite. Its lease renewals still succeed, so it stays a voter that cannot catch up for as long as that node leads. With another voter down, the cluster cannot commit. Recovery is a leadership change to a node the member knows. Do not add nodes while a member is down.
- The fix for a joiner that refused its new leader (token peers count while the join is pending) is covered by service-level and unit tests only. The in-process test harness binds loopback, so it runs with the source check off.
- A leader whose own lease expires is not removed while it leads. It leads with an expired lease until another node leads and removes it.
- A real lease expiry is not covered by the e2e suite. The suite checks names, the lease setting and a takeover. Expiry and removal are covered by the Rust tests, which run the lease clock faster.
- A cluster that has no leader, or no quorum, expires nothing. Every lease is as it was when the cluster comes back, and each node renews when it starts.
- Lengthening the lease can revive an expired record. `SetLease` leaves the `lease_renewed_at` of an expired node alone, but `clock - lease_renewed_at` may fall below the new, longer duration. Its removal then waits until the longer lease runs out, and the node can renew again in the meantime.
- Shortening the lease expires nobody at once, because `SetLease` restarts every lease that was still live.
- A node whose renewal loop is stuck is removed after one lease, even if it is otherwise healthy.

## containerd

Root `/etc/containerd/config.toml` (install.sh). A config without the marker is first backed up to `config.toml.faber-bak.<epoch>`.

```toml
# managed by faber
version = 2
imports = ["/etc/containerd/conf.d/*.toml"]
```

Drop-in `/etc/containerd/conf.d/faber.toml` (fabercp), version 2. The keys are fixed. Exact whitespace is set by the golden test for `faber_container_runtime::containerd::render`.

```toml
# managed by faber
version = 2

[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true

[plugins."io.containerd.grpc.v1.cri".cni]
  bin_dir = "/opt/cni/bin"
  conf_dir = "/etc/cni/net.d"
```

Version 2 is used everywhere because containerd 2.0 and 2.1 do not migrate a v2 import under a v3 root.
