#!/usr/bin/env bash
# Faber node installer.
#
# Prepares a Linux host for the Faber node agent by installing or upgrading:
#   - containerd        (distro package; containerd.io from Docker CE on EL,
#                        and kept as containerd.io wherever Docker's package
#                        is already installed, so Docker is not removed)
#   - wireguard-tools   (distro package)
#   - nftables          (distro package)
#   - iproute2          (distro package, 'iproute' on dnf; fabercp and the
#                        endpoint autodetect need ip)
#   - CNI plugins       (GitHub release tarball, sha256-verified, pinned)
#   - fabercp           (from next to this script -> $FABER_HOME/fabercp,
#                        faber:faber 0750)
#   - faber CLI         (from next to this script -> $FABER_CLI_DIR/faber,
#                        root:root 0755)
# It creates the 'faber' system user/group (nologin, no home created) and the
# $FABER_HOME directory (faber:faber 0750, enforced every run), and writes
# kernel module / sysctl prerequisites.
#
# Mesh and services (see docs/contract.md for the paths, modes and names):
#   - $FABER_ETC (root:faber 0750) with fabercp.env (root:faber 0640). Until
#     the bootstrap has finished the env file also carries FABER_NEW_MESH,
#     FABER_MESH_CIDR, FABER_NODE_NAME and (with --container-cidr)
#     FABER_CONTAINER_CIDR (new mesh) or FABER_JOIN_TOKEN_FILE (--join), and
#     the token goes to $FABER_ETC/join.token (root:faber 0640).
#   - The founder's node name comes from --name (new mesh only), else from
#     the short hostname in lower case. It must be a DNS label; a hostname
#     that is not one stops the run in preflight. A joining node's name is
#     set by its invite.
#   - The WireGuard interface is faber0 (FABER_WG_INTERFACE). fabercp creates
#     it. If faber0 already exists on a node with no identity.json and no
#     fabercp.env naming it (faber never ran here), the script refuses to
#     continue: in preflight, or right after the packages are installed when
#     the ip command was missing until then.
#   - containerd root config (root 0644, '# managed by faber'), which only
#     imports $CONTAINERD_CONF_DIR/*.toml (faber:faber 0755; fabercp writes
#     the drop-in). A config without the marker is backed up first to
#     config.toml.faber-bak.<epoch>.
#   - $CNI_CONF_DIR (faber:faber 0755; fabercp writes 10-faber.conflist
#     there). containerd loads the lexically first config in that directory,
#     so the script warns when it holds a config faber did not write.
#   - Container subnets come from --container-cidr (new mesh only; fabercp
#     has the default), which must not overlap the mesh CIDR. Container
#     traffic between nodes is forwarded between faber0 and the bridge
#     faberbr0, so the script warns when `nft list ruleset` shows a forward
#     chain with policy drop. It never changes the host firewall.
#   - systemd units fabercp.service, faber-containerd-reload.path and
#     faber-containerd-reload.service.
#   - With systemd: daemon-reload when a unit changed, try-restart of
#     containerd/fabercp when their config or binary changed, then
#     enable --now. Without systemd these steps and the bootstrap are skipped.
#   - Bootstrap: wait for 'faber health', then 'faber cluster init' (new
#     mesh) or wait for 'joined' (--join). See "Bootstrap state" below.
#
# Bootstrap state. Two facts decide what a run does:
#   identity   $FABER_STATE_DIR/identity.json is a regular file (fabercp
#              writes it on first start, and deletes it again when the leader
#              rejects the invite)
#   pending    fabercp.env carries FABER_NEW_MESH/FABER_MESH_CIDR/
#              FABER_NODE_NAME/FABER_CONTAINER_CIDR (pending new mesh) or
#              FABER_JOIN_TOKEN_FILE (pending join). Only this script
#              writes that root-owned file, and it removes those settings and
#              join.token only after the cluster result is 'created' or
#              'joined'. A health timeout, a failed 'faber cluster init', a
#              join timeout or a rejected invite leaves them in place.
# The node is bootstrapped when identity is there and nothing is pending. Then
# a run only upgrades, and --join (without --reset), --mesh-cidr,
# --container-cidr and --name are refused. Otherwise the run bootstraps
# (--container-cidr and --name follow --mesh-cidr in every row):
#   identity, pending new mesh    retry 'faber cluster init' with the stored
#                                 mesh CIDR; --join and a different
#                                 --mesh-cidr are refused
#   identity, pending join        without --join, or with --join and the stored
#                                 token: keep waiting for that join (fabercp
#                                 is not restarted). --join with another
#                                 token: replace the stored token, drop a
#                                 stored endpoint unless --endpoint is given,
#                                 and try-restart fabercp, which discards the
#                                 identity of the join that never finished
#                                 (this is the way out of an expired or
#                                 invalid token). If the node had in fact
#                                 joined already, fabercp keeps its identity
#                                 and the script says that the new invite was
#                                 not used. --mesh-cidr is refused
#   no identity, --join           join with the given token; stale bootstrap
#                                 settings and an old token are overwritten
#                                 (this is the way out of a rejected invite)
#   no identity, pending join     continue with the stored token; if the token
#                                 file is gone, --join is required
#   no identity, --mesh-cidr      new mesh with that CIDR
#   no identity, otherwise        new mesh (stored or default CIDR)
# While waiting for the join the script stops early if identity.json is gone:
# fabercp is healthy by then, so only a rejected invite removes it.
#
# --join <token> --reset is for a node the cluster removed (its lease ran out
# while it was cut off). Such a node still has its identity and takes itself
# for a member, and fabercp never lets a new token replace the identity of a
# member. With --reset the script accepts --join on a node that has an
# identity (bootstrapped, or with a stored join), and after the token and the
# env file are written it:
#   1. stops fabercp (systemctl stop fabercp; without systemd it warns),
#   2. writes the empty marker $FABER_STATE_DIR/join-failed (faber:faber
#      0644), the one fabercp leaves after a rejected invite,
#   3. goes on with the join. At its next start fabercp sees the marker,
#      removes identity.json and everything in $FABER_STATE_DIR/data, removes
#      the marker and makes a new identity from the token.
# The state directory belongs to the faber user, so the marker is staged in a
# root-only directory next to it and renamed into place with mv -fT, like the
# binaries; a symlinked state directory is refused. Nothing is reset when
# there is no identity, or when the stored token is the one given and the
# identity is that of its unfinished join (a re-run of the same command). A
# new mesh whose bootstrap has not finished still refuses --join.
#
# When fabercp idles because of the token it writes one of token_expired,
# token_invalid or invite_rejected to $FABER_STATE_DIR/bootstrap-error, and
# node_removed when the cluster removed the node. If a join fails (health
# timeout, join timeout or early stop), the script reads that file to say
# what is wrong. The directory is writable by the faber user, so the file is
# read only when it is a regular non-symlink file, at most 64 bytes of it,
# and only to compare against those four codes; its content is never printed.
# A file older than join.token belongs to an earlier token and is ignored.
# While that file says node_removed and the node has no identity, a run
# without --join stops in preflight: it would start a second mesh on the node.
# The way back in is --join <token> --reset. fabercp removes the file when it
# next starts with an identity; to start a new mesh on the server instead,
# remove the file by hand.
# Config files are written to a mktemp file in the (root-only) target
# directory, given their owner and mode there, then renamed into place with
# mv -fT. The join token is never printed, logged or passed on a command line.
#
# The fabercp and faber binaries must sit next to this script (or in
# FABER_BIN_SRC_DIR); the script refuses to change anything if either is
# missing, or if an existing 'faber' user has a primary group other than
# 'faber'.
#
# Binary installs never go through a path the faber user controls: each file
# is staged in a fresh root-only directory (mktemp -d) in the parent of the
# target directory (e.g. /opt for /opt/faber), created there with its final
# owner and mode, then renamed over the target with mv -T (which replaces a
# symlink instead of following it). A target is left alone only when it is a
# regular, non-symlink file with identical content and the right owner, group
# and mode; anything else is replaced. The parent must be owned by root and
# not group/world-writable. Where /usr/local is group-writable (root:staff
# 2775 on some Debian releases) the script refuses to stage the faber CLI
# there; run it with FABER_CLI_DIR set to a directory whose parent only root
# can write, for example FABER_CLI_DIR=/usr/bin.
#
# The installed fabercp is writable by the faber user, so it is never executed
# (not in a real run, --dry-run or --check). Versions shown for fabercp and
# faber come from the source binaries next to this script:
#   <version>     installed file has the same content as the source
#   present       installed file differs from the source (version not probed)
#   missing       not installed
#   unknown       target directory not searchable (non-root --check/--dry-run)
#
# Usage: install.sh [options]   (see --help)
#
# Summary statuses:
#   installed     component was missing and is now present
#   upgraded      version changed
#   up-to-date    version unchanged (fabercp/faber: file identical to source)
#   failed        component still missing after install
# For fabercp/faber, "upgraded" means the installed file was replaced because
# its content, type, owner or mode differed (a same-version rebuild counts),
# and "unknown" means the state could not be inspected without root.
# In --dry-run nothing changes, so "after" equals "before" and the status is a
# prediction: would-install (missing), would-upgrade (CNI version differs from
# target, or binary differs from source), up-to-date (CNI already at target,
# or binary identical), or would-check (package is present; the package
# manager decides whether a newer candidate exists).
#
# After the table the script prints "cluster: created|joined|existing|
# skipped (no systemd)|failed" (dry-run: would-create|would-join). A failed
# bootstrap exits 1 after the summary; re-running the script retries it.
#
# Test overrides: OS_RELEASE_FILE, CNI_BIN_DIR, FABER_ARCH, FABER_EUID,
# FABER_BIN_SRC_DIR, FABER_HOME, FABER_CLI_DIR, FABER_ETC, FABER_STATE_DIR,
# SYSTEMD_UNIT_DIR, SYSTEMD_RUN_DIR, CONTAINERD_ROOT_CONFIG,
# CONTAINERD_CONF_DIR, CNI_CONF_DIR, MODULES_LOAD_FILE, SYSCTL_FILE,
# FABER_HEALTH_TIMEOUT, FABER_JOIN_TIMEOUT, FABER_POLL_INTERVAL.

set -euo pipefail

readonly DEFAULT_CNI_VERSION="v1.9.1"
readonly CNI_RELEASES_URL="https://github.com/containernetworking/plugins/releases/download"
readonly DOCKER_CE_EL_REPO="https://download.docker.com/linux/centos/docker-ce.repo"
readonly KERNEL_MODULES=(overlay br_netfilter wireguard)
readonly COMPONENTS=(containerd wireguard nftables cni fabercp faber)
readonly FABER_USER="faber"
readonly FABER_BINARIES=(fabercp faber)
readonly WG_INTERFACE="faber0"
readonly WG_PORT=51820
readonly DEFAULT_MESH_CIDR="10.99.0.0/16"
# fabercp's default container range. Only used for the usage text and the
# overlap check; the env file carries a container CIDR only with the flag.
readonly DEFAULT_CONTAINER_CIDR="10.200.0.0/16"
readonly CONTAINER_CIDR_MAX_PREFIX=24
readonly CONTAINER_BRIDGE="faberbr0"
readonly CNI_CONFLIST="10-faber.conflist"
# fabercp's marker for "remove the identity and the raft data at the next
# start" (JOIN_FAILED_FILE in src/fabercp/src/mesh/identity.rs).
readonly JOIN_FAILED_MARKER="join-failed"
readonly MANAGED_MARKER="# managed by faber"
readonly TOKEN_RE='^faber1\.[A-Za-z0-9_-]+$'
readonly TOKEN_MAX_LEN=65536
readonly FABERCP_UNIT="fabercp.service"
readonly RELOAD_PATH_UNIT="faber-containerd-reload.path"
readonly RELOAD_SERVICE_UNIT="faber-containerd-reload.service"

CNI_BIN_DIR="${CNI_BIN_DIR:-/opt/cni/bin}"
FABER_HOME="${FABER_HOME:-/opt/faber}"
FABER_CLI_DIR="${FABER_CLI_DIR:-/usr/local/bin}"
FABER_ETC="${FABER_ETC:-/etc/faber}"
FABER_STATE_DIR="${FABER_STATE_DIR:-/var/lib/faber}"
SYSTEMD_UNIT_DIR="${SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
SYSTEMD_RUN_DIR="${SYSTEMD_RUN_DIR:-/run/systemd/system}"
CONTAINERD_ROOT_CONFIG="${CONTAINERD_ROOT_CONFIG:-/etc/containerd/config.toml}"
CONTAINERD_CONF_DIR="${CONTAINERD_CONF_DIR:-/etc/containerd/conf.d}"
CNI_CONF_DIR="${CNI_CONF_DIR:-/etc/cni/net.d}"
MODULES_LOAD_FILE="${MODULES_LOAD_FILE:-/etc/modules-load.d/faber.conf}"
SYSCTL_FILE="${SYSCTL_FILE:-/etc/sysctl.d/99-faber.conf}"
FABER_HEALTH_TIMEOUT="${FABER_HEALTH_TIMEOUT:-30}"
FABER_JOIN_TIMEOUT="${FABER_JOIN_TIMEOUT:-120}"
FABER_POLL_INTERVAL="${FABER_POLL_INTERVAL:-1}"

DRY_RUN=0
CHECK=0
WORKDIR=""
STAGE_DIR=""
WRITE_TMP=""
# Mesh options. JOIN_TOKEN holds the invite token; it is only ever written to
# the token file with the printf builtin and never passed to a child process.
JOIN_REQUESTED=0
JOIN_TOKEN=""
ENDPOINT_ARG=""
MESH_CIDR_ARG=""
CONTAINER_CIDR_ARG=""
NODE_NAME_ARG=""
RESET_REQUESTED=0
# Resolved in preflight.
CONFIGURED=""   # yes|no|unknown: identity.json present when the run started
PENDING=""      # new|join|none|unknown: bootstrap settings in the env file
MODE=""         # new|join|existing
RESUME=0        # 1: continuing the bootstrap recorded in the env file
# node_id of the identity whose unfinished join --join <other token> replaces;
# empty when this run replaces no token.
REPLACED_JOIN_NODE_ID=""
MESH_CIDR=""
CONTAINER_CIDR=""  # empty: not given, fabercp uses its default
NODE_NAME=""       # new mesh only: the founder's node name
RESET_IDENTITY=0   # 1: --reset discards the identity this node has
WG_ENDPOINT=""
DEFER_ENDPOINT=0  # 1: no ip command at preflight; autodetect after packages
DEFER_WG_CHECK=0  # 1: no ip command at preflight; check faber0 after packages
ENV_NAMED_INTERFACE=0  # 1: the env file named faber0 when the run started
# Set by write_file: 1 when the file was (or in --dry-run would be) written.
FILE_CHANGED=0
ENV_CHANGED=0
TOKEN_CHANGED=0
ROOT_CONFIG_CHANGED=0
UNITS_CHANGED=0
FABERCP_UNIT_CHANGED=0
CLUSTER_RESULT=""

# --- logging ----------------------------------------------------------------

log() { printf '[faber-install] %s\n' "$*" >&2; }
warn() { printf '[faber-install] WARN: %s\n' "$*" >&2; }
die() {
  printf '[faber-install] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<EOF
Usage: install.sh [options]

Install or upgrade containerd, wireguard-tools, nftables, CNI plugins
(${DEFAULT_CNI_VERSION}) and the Faber binaries, then start fabercp and either
create a new WireGuard mesh (default) or join an existing one (--join).

Installs:
  containerd, wireguard-tools, nftables  distro packages
  CNI plugins                            ${DEFAULT_CNI_VERSION} into \$CNI_BIN_DIR
  user and group '${FABER_USER}'                 system account, nologin shell
  \$FABER_HOME                            faber:faber 0750
  fabercp                                \$FABER_HOME/fabercp, faber:faber 0750
  faber                                  \$FABER_CLI_DIR/faber, root:root 0755
  \$FABER_ETC/fabercp.env                 root:faber 0640
  containerd root config                 imports \$CONTAINERD_CONF_DIR/*.toml
  \$CNI_CONF_DIR                          faber:faber 0755 (fabercp writes the
                                         CNI network config there)
  systemd units                          ${FABERCP_UNIT}, ${RELOAD_PATH_UNIT},
                                         ${RELOAD_SERVICE_UNIT}
fabercp and faber must be next to this script (or in \$FABER_BIN_SRC_DIR).

On a node that is not bootstrapped yet the script bootstraps the cluster:
'faber cluster init' for a new mesh, or waits for the join to finish with
--join. If that fails, re-run the script: it retries with the settings kept in
\$FABER_ETC (no --join needed). After a rejected invite or an expired token,
re-run with --join and a new token. A re-run on a bootstrapped node only
upgrades and keeps the existing cluster. A node that the cluster removed (its
lease ran out while it was cut off) joins again with --join <token> --reset.
WireGuard interface: ${WG_INTERFACE}.

Options:
  --join <token|->        Join the mesh with an invite token from
                          'faber mesh invite'; '-' reads it from stdin
                          (preferred: keeps it out of the process list)
  --endpoint <ip[:port]>  Public WireGuard endpoint of this node, as peers
                          see it (port defaults to ${WG_PORT}). New mesh:
                          autodetected when omitted. Join: taken from the
                          token when omitted; must match the invite. Other
                          re-runs keep the value already configured.
  --mesh-cidr <cidr>      Mesh address range for a new mesh
                          (default ${DEFAULT_MESH_CIDR}); not with --join
  --container-cidr <cidr> Container address range for a new mesh, one /24 per
                          node (default ${DEFAULT_CONTAINER_CIDR}); prefix length
                          at most ${CONTAINER_CIDR_MAX_PREFIX}; must not overlap the mesh range; not
                          with --join
  --name <name>           Name of this node in a new mesh: a DNS label (1 to 63
                          characters of a-z, 0-9 and '-', not starting or
                          ending with '-'). Default: the short hostname in
                          lower case. Not with --join (the invite names a
                          joining node)
  --reset                 Only with --join: discard the identity and raft data
                          this node has, then join with the token. For a node
                          that the cluster removed
  --dry-run               Print every mutating command instead of running it
  --check                 Report detected OS and current versions; change nothing
  -h, --help              Show this help

Environment:
  OS_RELEASE_FILE         os-release path (default: /etc/os-release)
  CNI_BIN_DIR             CNI plugin directory (default: /opt/cni/bin)
  FABER_ARCH              Machine architecture (default: uname -m)
  FABER_BIN_SRC_DIR       Directory holding fabercp and faber (default: script dir)
  FABER_HOME              Faber home directory (default: /opt/faber)
  FABER_CLI_DIR           Directory for the faber CLI (default: /usr/local/bin)
  FABER_ETC               fabercp config directory (default: /etc/faber)
  FABER_STATE_DIR         fabercp state directory (default: /var/lib/faber)
  SYSTEMD_UNIT_DIR        systemd unit directory (default: /etc/systemd/system)
  CONTAINERD_ROOT_CONFIG  containerd config (default: /etc/containerd/config.toml)
  CONTAINERD_CONF_DIR     containerd drop-in directory (default: /etc/containerd/conf.d)
  CNI_CONF_DIR            CNI network config directory (default: /etc/cni/net.d)
EOF
}

usage_error() {
  printf '[faber-install] ERROR: %s\n\n' "$*" >&2
  usage >&2
  exit 2
}

# run_cmd: execute a mutating command, or print it in --dry-run mode.
# (Named run_cmd rather than run so the script can be sourced by bats, whose
# own `run` helper would otherwise be shadowed.)
run_cmd() {
  if [[ "$DRY_RUN" == 1 ]]; then
    local out="+" arg
    for arg in "$@"; do
      out+=" $(printf '%q' "$arg")"
    done
    printf '%s\n' "$out"
    return 0
  fi
  "$@"
}

# run_quiet: like run_cmd but discards the command's output when executing.
run_quiet() {
  if [[ "$DRY_RUN" == 1 ]]; then
    run_cmd "$@"
    return 0
  fi
  "$@" >/dev/null 2>&1
}

have() { command -v "$1" >/dev/null 2>&1; }

# --- argument parsing -------------------------------------------------------

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --dry-run) DRY_RUN=1 ;;
      --check) CHECK=1 ;;
      --reset) RESET_REQUESTED=1 ;;
      --join | --endpoint | --mesh-cidr | --container-cidr | --name)
        (($# >= 2)) || usage_error "$1 needs a value"
        set_option "$1" "$2"
        shift
        ;;
      --join=* | --endpoint=* | --mesh-cidr=* | --container-cidr=* | --name=*) set_option "${1%%=*}" "${1#*=}" ;;
      -h | --help)
        usage
        exit 0
        ;;
      faber1.*) usage_error "unexpected argument (looks like a join token; use --join - to pass it on stdin)" ;;
      *) usage_error "unknown option: $1" ;;
    esac
    shift
  done
  validate_args
}

# set_option NAME VALUE: store a valued option. A value starting with '-' is
# taken as a missing value (except '--join -').
set_option() {
  local name="$1" value="$2"
  if [[ -z "$value" || ("$value" == -* && "$name $value" != "--join -") ]]; then
    usage_error "$name needs a value"
  fi
  case "$name" in
    --join)
      JOIN_REQUESTED=1
      JOIN_TOKEN="$value"
      ;;
    --endpoint) ENDPOINT_ARG="$value" ;;
    --mesh-cidr) MESH_CIDR_ARG="$value" ;;
    --container-cidr) CONTAINER_CIDR_ARG="$value" ;;
    --name) NODE_NAME_ARG="$value" ;;
  esac
}

validate_args() {
  local normalized
  if [[ "$JOIN_REQUESTED" == 1 && -n "$MESH_CIDR_ARG" ]]; then
    usage_error "--join and --mesh-cidr cannot be combined (a joining node gets the mesh CIDR from the token)"
  fi
  if [[ "$JOIN_REQUESTED" == 1 && -n "$CONTAINER_CIDR_ARG" ]]; then
    usage_error "--join and --container-cidr cannot be combined (a joining node gets its container subnet from the cluster)"
  fi
  if [[ "$JOIN_REQUESTED" == 1 && -n "$NODE_NAME_ARG" ]]; then
    usage_error "--join and --name cannot be combined (a joining node's name is set by its invite)"
  fi
  if [[ "$RESET_REQUESTED" == 1 && "$JOIN_REQUESTED" == 0 ]]; then
    usage_error "--reset needs --join <token> (it discards this node's identity so that the node can join again)"
  fi
  if [[ -n "$ENDPOINT_ARG" ]]; then
    normalized="$(normalize_endpoint "$ENDPOINT_ARG")" ||
      usage_error "--endpoint must be <ipv4>[:port] or [<ipv6>][:port]"
    ENDPOINT_ARG="$normalized"
  fi
  if [[ -n "$MESH_CIDR_ARG" ]] && ! valid_mesh_cidr "$MESH_CIDR_ARG"; then
    usage_error "--mesh-cidr must be an IPv4 CIDR such as $DEFAULT_MESH_CIDR"
  fi
  if [[ -n "$CONTAINER_CIDR_ARG" ]] && ! valid_container_cidr "$CONTAINER_CIDR_ARG"; then
    usage_error "--container-cidr must be an IPv4 CIDR that is a network address (no host bits set) with a prefix length of at most /$CONTAINER_CIDR_MAX_PREFIX, such as $DEFAULT_CONTAINER_CIDR"
  fi
  if [[ -n "$NODE_NAME_ARG" ]] && ! valid_node_name "$NODE_NAME_ARG"; then
    usage_error "--name must be a DNS label: 1 to 63 characters of a-z, 0-9 and '-', not starting or ending with '-'"
  fi
  # --check never uses the token, so it does not read stdin either.
  if [[ "$JOIN_REQUESTED" == 1 && "$CHECK" == 0 ]]; then
    load_join_token
  fi
}

# load_join_token: read the token from stdin for '--join -', strip whitespace
# (including a CR from a pasted line) and check its shape. The value is never
# echoed, not even in errors.
load_join_token() {
  if [[ "$JOIN_TOKEN" == - ]]; then
    JOIN_TOKEN=""
    if [[ -t 0 ]]; then
      printf '[faber-install] paste the join token and press Enter: ' >&2
      IFS= read -rs JOIN_TOKEN || true
      printf '\n' >&2
    else
      IFS= read -r JOIN_TOKEN || true
    fi
  fi
  JOIN_TOKEN="${JOIN_TOKEN#"${JOIN_TOKEN%%[![:space:]]*}"}"
  JOIN_TOKEN="${JOIN_TOKEN%"${JOIN_TOKEN##*[![:space:]]}"}"
  [[ -n "$JOIN_TOKEN" ]] || usage_error "--join: no join token given (stdin was empty?)"
  if ((${#JOIN_TOKEN} > TOKEN_MAX_LEN)) || [[ ! "$JOIN_TOKEN" =~ $TOKEN_RE ]]; then
    usage_error "invalid join token: expected the output of 'faber mesh invite' (value not shown)"
  fi
}

# valid_ipv4 ADDR: four octets of 0 to 255, none written with a leading zero
# (a lone 0 is fine). fabercp's parser refuses 010.0.0.5, and the value goes
# into fabercp.env, so accepting it here would leave the service failing at
# every start.
valid_ipv4() {
  local ip="$1" o
  local -a octets
  [[ "$ip" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || return 1
  IFS=. read -r -a octets <<<"$ip"
  for o in "${octets[@]}"; do
    [[ "$o" == 0 || "$o" != 0* ]] || return 1
    ((10#$o <= 255)) || return 1
  done
}

# normalize_endpoint VALUE: print VALUE as ip:port (default port $WG_PORT).
# Accepts IPv4 or a bracketed IPv6 address; hostnames are rejected because
# the value must match the invite and the peers' firewall sets exactly.
normalize_endpoint() {
  local value="$1" host port
  if [[ "$value" =~ ^(\[[0-9A-Fa-f:.]+\])(:([0-9]{1,5}))?$ ]]; then
    host="${BASH_REMATCH[1]}"
    port="${BASH_REMATCH[3]}"
    [[ "$host" == *:* ]] || return 1
  elif [[ "$value" =~ ^([0-9.]+)(:([0-9]{1,5}))?$ ]]; then
    host="${BASH_REMATCH[1]}"
    port="${BASH_REMATCH[3]}"
    valid_ipv4 "$host" || return 1
  else
    return 1
  fi
  port="${port:-$WG_PORT}"
  ((10#$port >= 1 && 10#$port <= 65535)) || return 1
  printf '%s:%s\n' "$host" "$((10#$port))"
}

# valid_mesh_cidr VALUE: IPv4 address/prefix.
valid_mesh_cidr() {
  local value="$1" addr prefix
  [[ "$value" =~ ^([0-9.]+)/([0-9]{1,2})$ ]] || return 1
  addr="${BASH_REMATCH[1]}"
  prefix="${BASH_REMATCH[2]}"
  valid_ipv4 "$addr" && ((10#$prefix <= 32))
}

# valid_container_cidr VALUE: IPv4 address/prefix with a prefix length of at
# most $CONTAINER_CIDR_MAX_PREFIX (every node takes a /24 out of the range)
# whose address is the network address. fabercp rejects a range with host
# bits at cluster creation, and by then the env file is written.
valid_container_cidr() {
  valid_mesh_cidr "$1" && ((10#${1#*/} <= CONTAINER_CIDR_MAX_PREFIX)) &&
    cidr_is_network "$1"
}

# cidr_is_network CIDR: the address of a (valid) IPv4 CIDR has no bit set
# beyond the prefix.
cidr_is_network() {
  local addr hostmask
  addr="$(ipv4_to_int "${1%/*}")"
  hostmask=$((0xFFFFFFFF >> 10#${1#*/}))
  (( (addr & hostmask) == 0 ))
}

# valid_node_name NAME: a DNS label, 1 to 63 characters of a-z, 0-9 and '-',
# not starting or ending with '-'. The C locale keeps a-z to ASCII lower case.
valid_node_name() {
  local LC_ALL=C
  ((${#1} >= 1 && ${#1} <= 63)) && [[ "$1" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ ]]
}

# ipv4_to_int ADDR: print a (valid) dotted-quad address as a number.
ipv4_to_int() {
  local -a o
  IFS=. read -r -a o <<<"$1"
  printf '%s\n' "$(((10#${o[0]} << 24) | (10#${o[1]} << 16) | (10#${o[2]} << 8) | 10#${o[3]}))"
}

# cidrs_overlap A B: succeed when the two (valid) IPv4 CIDRs share an address.
# Two CIDR ranges overlap exactly when one contains the other, which is when
# both addresses agree on the bits of the shorter prefix. Host bits are
# ignored.
cidrs_overlap() {
  local a b len mask
  a="$(ipv4_to_int "${1%/*}")"
  b="$(ipv4_to_int "${2%/*}")"
  len=$((10#${1#*/}))
  ((10#${2#*/} < len)) && len=$((10#${2#*/}))
  mask=$(((0xFFFFFFFF << (32 - len)) & 0xFFFFFFFF))
  (( (a & mask) == (b & mask) ))
}

require_root() {
  if [[ "$DRY_RUN" == 1 || "$CHECK" == 1 ]]; then
    return 0
  fi
  local euid="${FABER_EUID:-$EUID}"
  if [[ "$euid" != 0 ]]; then
    die "must be run as root (try: sudo $0)"
  fi
}

# --- OS / arch detection ----------------------------------------------------

# os_release_value KEY FILE: print the unquoted value of KEY without sourcing
# the file, so nothing leaks into the caller's globals.
os_release_value() {
  local key="$1" file="$2" line
  line="$(grep -E "^${key}=" "$file" | head -n 1 || true)"
  line="${line#*=}"
  line="${line#\"}"
  line="${line%\"}"
  line="${line#\'}"
  line="${line%\'}"
  printf '%s\n' "$line"
}

# pkg_family ID ID_LIKE: map a distro to a package-manager family.
pkg_family() {
  local id="$1" id_like="$2"
  case "$id" in
    ubuntu | debian) echo apt; return 0 ;;
    fedora) echo dnf; return 0 ;;
    rhel | centos | rocky | almalinux) echo dnf-el; return 0 ;;
    arch) echo pacman; return 0 ;;
    opensuse* | sles) echo zypper; return 0 ;;
  esac
  case " $id_like " in
    *debian* | *ubuntu*) echo apt ;;
    *rhel* | *centos*) echo dnf-el ;;
    *arch*) echo pacman ;;
    *suse*) echo zypper ;;
    *) die "unsupported distro: ${id:-unknown}" ;;
  esac
}

os_release_file() { printf '%s\n' "${OS_RELEASE_FILE:-/etc/os-release}"; }

detect_os() {
  local file id id_like
  file="$(os_release_file)"
  [[ -r "$file" ]] || die "cannot read os-release file: $file"
  id="$(os_release_value ID "$file")"
  id_like="$(os_release_value ID_LIKE "$file")"
  pkg_family "$id" "$id_like"
}

cni_arch() {
  local machine="${FABER_ARCH:-$(uname -m)}"
  case "$machine" in
    x86_64 | amd64) echo amd64 ;;
    aarch64 | arm64) echo arm64 ;;
    armv7l | armhf) echo arm ;;
    riscv64 | s390x | ppc64le) echo "$machine" ;;
    *) die "unsupported architecture: $machine" ;;
  esac
}

# --- version detection ------------------------------------------------------

# parse_version STRING: print the first token that looks like a version
# (optional leading 'v' removed), or 'unknown'.
parse_version() {
  local tok
  local -a toks
  read -r -a toks <<<"$1" || true
  for tok in "${toks[@]}"; do
    if [[ "$tok" =~ ^v?([0-9]+(\.[0-9]+)+[^[:space:]]*)$ ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
      return 0
    fi
  done
  echo unknown
}

component_version() {
  local v
  case "$1" in
    containerd)
      have containerd || { echo missing; return 0; }
      parse_version "$(containerd --version 2>/dev/null || true)"
      ;;
    wireguard)
      have wg || { echo missing; return 0; }
      parse_version "$(wg --version 2>/dev/null || true)"
      ;;
    nftables)
      have nft || { echo missing; return 0; }
      parse_version "$(nft --version 2>/dev/null || true)"
      ;;
    cni)
      if [[ -r "$CNI_BIN_DIR/.version" ]]; then
        v="$(tr -d '[:space:]' <"$CNI_BIN_DIR/.version")"
        echo "${v:-unknown}"
      else
        echo missing
      fi
      ;;
    fabercp | faber)
      binary_version "$(binary_state_of "$1")" "$(bin_src_dir)/$1"
      ;;
    *) die "unknown component: $1" ;;
  esac
}

# compute_status BEFORE AFTER
compute_status() {
  local before="$1" after="$2"
  if [[ "$after" == missing ]]; then
    echo failed
  elif [[ "$before" == missing ]]; then
    echo installed
  elif [[ "$before" == "$after" ]]; then
    echo up-to-date
  else
    echo upgraded
  fi
}

# dry_run_status COMPONENT BEFORE CNI_TARGET
dry_run_status() {
  local comp="$1" before="$2" target="$3"
  if [[ "$before" == missing ]]; then
    echo would-install
  elif [[ "$comp" == cni ]]; then
    if [[ "$before" == "$target" ]]; then echo up-to-date; else echo would-upgrade; fi
  else
    echo would-check
  fi
}

# source_version SRC: version printed by the (root-controlled) source binary.
source_version() {
  if [[ -f "$1" && -x "$1" ]]; then
    parse_version "$("$1" --version 2>/dev/null || true)"
  else
    echo unknown
  fi
}

# binary_version STATE SRC: version column for an installed binary. The
# installed file is never executed; see the header comment.
binary_version() {
  case "$1" in
    missing | unknown) echo "$1" ;;
    same | perms) source_version "$2" ;;
    *) echo present ;;
  esac
}

# binary_status STATE AFTER: STATE is binary_state taken before installing.
binary_status() {
  local state="$1" after="$2"
  if [[ "$state" == unknown ]]; then
    echo unknown
  elif [[ "$DRY_RUN" == 1 ]]; then
    case "$state" in
      missing) echo would-install ;;
      differs | perms) echo would-upgrade ;;
      *) echo up-to-date ;;
    esac
  elif [[ "$after" == missing ]]; then
    echo failed
  else
    case "$state" in
      missing) echo installed ;;
      differs | perms) echo upgraded ;;
      *) echo up-to-date ;;
    esac
  fi
}

# --- package installation ---------------------------------------------------

# containerd_package FAMILY: Docker's containerd.io on EL, or wherever it is
# already installed (requesting distro 'containerd' there would make the
# package manager remove containerd.io and with it docker-ce); otherwise the
# distro's containerd.
containerd_package() {
  case "$1" in
    dnf-el)
      echo containerd.io
      return 0
      ;;
    apt)
      # shellcheck disable=SC2016 # ${Status} is a dpkg-query format field
      if have dpkg-query &&
        [[ "$(dpkg-query -W -f='${Status}' containerd.io 2>/dev/null)" == "install ok installed" ]]; then
        echo containerd.io
        return 0
      fi
      ;;
    dnf)
      if have rpm && rpm -q containerd.io >/dev/null 2>&1; then
        echo containerd.io
        return 0
      fi
      ;;
  esac
  echo containerd
}

install_packages() {
  local family="$1" containerd_pkg
  local -a pkgs extra

  containerd_pkg="$(containerd_package "$family")"
  log "containerd package: $containerd_pkg"
  pkgs=("$containerd_pkg" wireguard-tools nftables)

  case "$family" in
    apt)
      run_cmd env DEBIAN_FRONTEND=noninteractive apt-get update
      run_cmd env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        "${pkgs[@]}" ca-certificates curl tar iproute2
      ;;
    dnf | dnf-el)
      # Only request curl when absent: EL/Fedora images often ship
      # curl-minimal, which conflicts with the full curl package.
      extra=()
      have curl || extra+=(curl)
      extra+=(tar iproute)
      if [[ "$family" == dnf-el ]]; then
        run_cmd dnf install -y dnf-plugins-core
        if [[ ! -f /etc/yum.repos.d/docker-ce.repo ]]; then
          run_cmd dnf config-manager --add-repo "$DOCKER_CE_EL_REPO"
        fi
      fi
      run_cmd dnf install -y "${pkgs[@]}" "${extra[@]}"
      run_cmd dnf upgrade -y "${pkgs[@]}"
      ;;
    pacman)
      # Full sync (-Syu): Arch does not support partial upgrades.
      run_cmd pacman -Syu --needed --noconfirm "${pkgs[@]}" curl tar iproute2
      ;;
    zypper)
      run_cmd zypper -n in "${pkgs[@]}" curl tar iproute2
      run_cmd zypper -n up "${pkgs[@]}"
      ;;
    *) die "unknown package family: $family" ;;
  esac
}

# --- CNI plugins ------------------------------------------------------------

# cleanup_tmp: remove the CNI download dir and any binary staging dir.
cleanup_tmp() {
  if [[ -n "$WORKDIR" && -d "$WORKDIR" ]]; then
    rm -rf "$WORKDIR"
  fi
  if [[ -n "$STAGE_DIR" && -d "$STAGE_DIR" ]]; then
    rm -rf "$STAGE_DIR"
  fi
  if [[ -n "$WRITE_TMP" && -f "$WRITE_TMP" ]]; then
    rm -f "$WRITE_TMP"
  fi
}

# verify_sha256 FILE SUMFILE: compare the hash field only, since the sum file
# names the original download rather than our local path.
verify_sha256() {
  local file="$1" sumfile="$2" expected actual
  expected="$(awk 'NR == 1 { print tolower($1) }' "$sumfile")"
  actual="$(sha256sum "$file" | awk '{ print tolower($1) }')"
  if [[ ! "$expected" =~ ^[0-9a-f]{64}$ ]]; then
    warn "malformed checksum file: $sumfile"
    return 1
  fi
  if [[ "$expected" != "$actual" ]]; then
    warn "sha256 mismatch for $file: expected $expected, got $actual"
    return 1
  fi
}

# install_cni VERSION ARCH
install_cni() {
  local ver="$1" arch="$2" current tgz base dir
  current="$(component_version cni)"
  if [[ "$current" == "$ver" ]]; then
    log "cni: $ver already installed in $CNI_BIN_DIR (up-to-date)"
    return 0
  fi
  log "cni: installing $ver (current: $current)"

  tgz="cni-plugins-linux-${arch}-${ver}.tgz"
  base="${CNI_RELEASES_URL}/${ver}"
  if [[ "$DRY_RUN" == 1 ]]; then
    dir="/tmp/faber-cni.XXXXXX"
  else
    have curl || die "curl is required to download CNI plugins"
    WORKDIR="$(mktemp -d)"
    trap cleanup_tmp EXIT
    dir="$WORKDIR"
  fi

  run_cmd curl -fsSL --retry 3 -o "$dir/$tgz" "$base/$tgz"
  run_cmd curl -fsSL --retry 3 -o "$dir/$tgz.sha256" "$base/$tgz.sha256"
  if [[ "$DRY_RUN" == 1 ]]; then
    run_cmd verify_sha256 "$dir/$tgz" "$dir/$tgz.sha256"
  else
    verify_sha256 "$dir/$tgz" "$dir/$tgz.sha256" || die "checksum verification failed for $tgz"
  fi
  run_cmd install -d -m 0755 "$CNI_BIN_DIR"
  run_cmd tar -C "$CNI_BIN_DIR" -xzf "$dir/$tgz"
  if [[ "$DRY_RUN" == 1 ]]; then
    printf '+ echo %s > %s\n' "$ver" "$CNI_BIN_DIR/.version"
  else
    printf '%s\n' "$ver" >"$CNI_BIN_DIR/.version"
  fi
}

# --- configuration ----------------------------------------------------------

# write_if_changed PATH CONTENT: write CONTENT (plus trailing newline) only if
# the file is missing or differs.
write_if_changed() {
  local path="$1" content="$2"
  if [[ -f "$path" && "$(cat "$path")" == "$content" ]]; then
    log "$path unchanged"
    return 0
  fi
  if [[ "$DRY_RUN" == 1 ]]; then
    printf '+ write %s\n' "$path"
    return 0
  fi
  install -d -m 0755 "$(dirname "$path")"
  printf '%s\n' "$content" >"$path"
  log "wrote $path"
}

# write_file PATH CONTENT OWNER GROUP MODE [LABEL]: make PATH a regular file
# holding CONTENT (plus trailing newline) with the given owner, group and mode.
# Nothing happens when that is already so; otherwise the content goes to a
# mktemp file in PATH's directory, gets its owner and mode there, and is
# renamed over PATH with mv -fT (which replaces a symlink rather than writing
# through it). Only use this in directories writable by root alone. Sets
# FILE_CHANGED. --dry-run prints "+ write LABEL" (LABEL defaults to PATH), so
# secret content never reaches the output.
write_file() {
  local path="$1" content="$2" owner="$3" group="$4" mode="$5" label="${6:-$1}"
  FILE_CHANGED=0
  if [[ -f "$path" && ! -L "$path" ]] &&
    [[ "$(cat -- "$path" 2>/dev/null)" == "$content" ]] &&
    meta_matches "$path" "$owner" "$group" "$mode" 2>/dev/null; then
    log "$path unchanged"
    return 0
  fi
  FILE_CHANGED=1
  if [[ "$DRY_RUN" == 1 ]]; then
    printf '+ write %s\n' "$label"
    return 0
  fi
  trap cleanup_tmp EXIT
  WRITE_TMP="$(mktemp "$(dirname "$path")/.faber-tmp.XXXXXX")"
  printf '%s\n' "$content" >"$WRITE_TMP"
  chmod "$mode" "$WRITE_TMP"
  chown "$owner:$group" "$WRITE_TMP"
  mv -fT "$WRITE_TMP" "$path"
  WRITE_TMP=""
  log "wrote $path"
}

# check_real_dir PATH: refuse a symlink or a non-directory at PATH (a missing
# PATH is fine; it is created later).
check_real_dir() {
  if [[ -L "$1" ]]; then
    die "$1 is a symlink; refusing to use it"
  fi
  if [[ -e "$1" && ! -d "$1" ]]; then
    die "$1 exists but is not a directory"
  fi
}

# check_config_paths: the directories this script writes config into.
check_config_paths() {
  check_real_dir "$FABER_ETC"
  check_real_dir "$(dirname "$CONTAINERD_ROOT_CONFIG")"
  check_real_dir "$CONTAINERD_CONF_DIR"
  check_real_dir "$CNI_CONF_DIR"
  check_real_dir "$SYSTEMD_UNIT_DIR"
}

# --- mesh configuration -----------------------------------------------------

env_file() { printf '%s\n' "$FABER_ETC/fabercp.env"; }
token_file() { printf '%s\n' "$FABER_ETC/join.token"; }
identity_file() { printf '%s\n' "$FABER_STATE_DIR/identity.json"; }

# node_configured: yes when identity.json is a regular, non-symlink file;
# unknown when the state directory cannot be searched (non-root dry-run).
node_configured() {
  local f
  f="$(identity_file)"
  if [[ -d "$FABER_STATE_DIR" && ! -x "$FABER_STATE_DIR" ]]; then
    echo unknown
  elif [[ -f "$f" && ! -L "$f" ]]; then
    echo yes
  else
    echo no
  fi
}

# identity_node_id: the node_id in identity.json, or nothing. The file sits in
# a directory the faber user can write, so it is read only when it is a
# regular file and not a symlink, at most 4096 bytes of it, and the result is
# a number of at most 20 digits, never anything else from the file.
identity_node_id() {
  local f content
  f="$(identity_file)"
  [[ -f "$f" && ! -L "$f" && -r "$f" ]] || return 0
  # NUL bytes become '?' so they cannot match (and bash does not warn).
  content="$(head -c 4096 -- "$f" 2>/dev/null | tr '\0' '?')" || return 0
  if [[ "$content" =~ \"node_id\"[[:space:]]*:[[:space:]]*([0-9]{1,20})[^0-9] ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
  fi
  return 0
}

# identity_pending_join: identity.json has a pending_join object, so it is
# the identity of a join that has not finished (fabercp removes pending_join
# once the node is a voter; the founder never has one). Read like
# identity_node_id; pending_join comes before the peer list it holds.
identity_pending_join() {
  local f content
  f="$(identity_file)"
  [[ -f "$f" && ! -L "$f" && -r "$f" ]] || return 1
  content="$(head -c 4096 -- "$f" 2>/dev/null | tr '\0' '?')" || return 1
  [[ "$content" =~ \"pending_join\"[[:space:]]*:[[:space:]]*\{ ]]
}

# env_file_value KEY: the value of the first KEY= line in the env file (read
# as text, never sourced); nothing when the file or the line is absent.
env_file_value() {
  local f line
  f="$(env_file)"
  [[ -f "$f" && ! -L "$f" && -r "$f" ]] || return 0
  line="$(grep -m 1 "^$1=" "$f" 2>/dev/null || true)"
  [[ -n "$line" ]] || return 0
  printf '%s\n' "${line#*=}"
}

# pending_bootstrap: the bootstrap recorded in the env file.
#   new      FABER_NEW_MESH, FABER_MESH_CIDR, FABER_NODE_NAME or
#            FABER_CONTAINER_CIDR is set
#   join     FABER_JOIN_TOKEN_FILE is set
#   none     neither (or no env file)
#   unknown  the env file cannot be read (non-root --check/--dry-run)
pending_bootstrap() {
  local f
  f="$(env_file)"
  if [[ -d "$FABER_ETC" && ! -x "$FABER_ETC" ]] || [[ -f "$f" && ! -L "$f" && ! -r "$f" ]]; then
    echo unknown
  elif [[ ! -f "$f" || -L "$f" ]]; then
    echo none
  elif grep -q '^FABER_JOIN_TOKEN_FILE=' "$f" 2>/dev/null; then
    echo join
  elif grep -Eq '^FABER_(NEW_MESH|MESH_CIDR|NODE_NAME|CONTAINER_CIDR)=' "$f" 2>/dev/null; then
    echo new
  else
    echo none
  fi
}

# stored_token_usable: join.token is a regular, non-empty, readable file.
stored_token_usable() {
  local f
  f="$(token_file)"
  [[ -f "$f" && ! -L "$f" && -r "$f" && -s "$f" ]]
}

# stored_token_matches: join.token holds exactly $JOIN_TOKEN. Compared inside
# this shell; the value reaches no child process and no output.
stored_token_matches() {
  local stored
  stored_token_usable || return 1
  stored="$(<"$(token_file)")"
  [[ "$stored" == "$JOIN_TOKEN" ]]
}

# stored_mesh_cidr: FABER_MESH_CIDR from the env file when it is well-formed.
stored_mesh_cidr() {
  local value
  value="$(env_file_value FABER_MESH_CIDR)"
  if [[ -n "$value" ]] && valid_mesh_cidr "$value"; then
    printf '%s\n' "$value"
  fi
}

# stored_container_cidr: FABER_CONTAINER_CIDR from the env file when it is
# well-formed.
stored_container_cidr() {
  local value
  value="$(env_file_value FABER_CONTAINER_CIDR)"
  if [[ -n "$value" ]] && valid_container_cidr "$value"; then
    printf '%s\n' "$value"
  fi
}

# stored_node_name: FABER_NODE_NAME from the env file when it is well-formed.
stored_node_name() {
  local value
  value="$(env_file_value FABER_NODE_NAME)"
  if [[ -n "$value" ]] && valid_node_name "$value"; then
    printf '%s\n' "$value"
  fi
}

# default_node_name: this host's short hostname in lower case (`hostname -s`,
# else `hostname` with the domain stripped); nothing when neither works. The
# result is not validated here.
default_node_name() {
  local name=""
  have hostname || return 0
  name="$(hostname -s 2>/dev/null)" || name=""
  if [[ -z "$name" ]]; then
    name="$(hostname 2>/dev/null)" || name=""
  fi
  name="${name%%.*}"
  printf '%s\n' "${name,,}"
}

# hostname_node_name: default_node_name, or die when it is not a DNS label.
hostname_node_name() {
  local name
  name="$(default_node_name)"
  valid_node_name "$name" ||
    die "cannot name this node after its hostname ($(printf '%q' "$name") is not a DNS label: 1 to 63 characters of a-z, 0-9 and '-', not starting or ending with '-'); pass --name <name>"
  printf '%s\n' "$name"
}

# check_cidr_overlap: a new mesh needs a container range that is disjoint
# from the mesh range (fabercp rejects the cluster init otherwise). Without
# --container-cidr the range is fabercp's default.
check_cidr_overlap() {
  local container="${CONTAINER_CIDR:-$DEFAULT_CONTAINER_CIDR}"
  [[ "$MODE" == new && -n "$MESH_CIDR" ]] || return 0
  if cidrs_overlap "$container" "$MESH_CIDR"; then
    die "container CIDR $container overlaps the mesh CIDR $MESH_CIDR; pick ranges that do not overlap with --container-cidr or --mesh-cidr"
  fi
}

# check_node_removed: a node the cluster removed has no identity and nothing
# pending, so a run without --join would take it for a new server and start a
# second, separate mesh on it. Refuse that run while fabercp's record says
# node_removed. --join is the way back in. The record counts only while it is
# newer than join.token (bootstrap_error_code), so a run that continues a join
# started after the removal is not refused. It also counts only on a node
# without an identity: with identity.json in place the run starts no mesh, and
# a record left next to it must not stop an upgrade. resolve_mode sets
# CONFIGURED before it calls this.
check_node_removed() {
  [[ "$JOIN_REQUESTED" == 0 ]] || return 0
  [[ "$CONFIGURED" != yes ]] || return 0
  [[ "$(bootstrap_error_code)" == node_removed ]] || return 0
  die "the cluster removed this node, and a run without --join would start a new, separate mesh on it; to rejoin, create a new invite (faber mesh invite ...) on an existing node and re-run with --join <token> --reset; to start a new mesh here instead, remove $FABER_STATE_DIR/bootstrap-error and re-run"
}

# resolve_mode: decide between a new mesh, a join and an existing node, using
# the rule in the header ("Bootstrap state"). Runs in preflight, so a refused
# combination changes nothing. Sets CONFIGURED, PENDING, MODE, RESUME,
# MESH_CIDR, CONTAINER_CIDR, NODE_NAME, RESET_IDENTITY and
# REPLACED_JOIN_NODE_ID.
resolve_mode() {
  local stored_cidr stored_container stored_name
  CONFIGURED="$(node_configured)"
  PENDING="$(pending_bootstrap)"
  RESUME=0
  MESH_CIDR=""
  CONTAINER_CIDR=""
  NODE_NAME=""
  RESET_IDENTITY=0
  REPLACED_JOIN_NODE_ID=""
  if [[ "$CONFIGURED" == unknown || "$PENDING" == unknown ]]; then
    log "cannot inspect $FABER_STATE_DIR or $(env_file); run --dry-run as root for an accurate prediction"
  fi
  stored_cidr="$(stored_mesh_cidr)"
  stored_container="$(stored_container_cidr)"
  stored_name="$(stored_node_name)"
  check_node_removed

  if [[ "$CONFIGURED" == yes && "$PENDING" == new ]]; then
    if [[ "$JOIN_REQUESTED" == 1 ]]; then
      die "this node started a new mesh ($(identity_file) exists) and its bootstrap has not finished; refusing --join. Re-run without --join to finish it."
    fi
    if [[ -n "$MESH_CIDR_ARG" && -n "$stored_cidr" && "$MESH_CIDR_ARG" != "$stored_cidr" ]]; then
      die "this node already started a new mesh with --mesh-cidr $stored_cidr; re-run without --mesh-cidr to finish it"
    fi
    if [[ -n "$CONTAINER_CIDR_ARG" && -n "$stored_container" && "$CONTAINER_CIDR_ARG" != "$stored_container" ]]; then
      die "this node already started a new mesh with --container-cidr $stored_container; re-run without --container-cidr to finish it"
    fi
    if [[ -n "$NODE_NAME_ARG" && -n "$stored_name" && "$NODE_NAME_ARG" != "$stored_name" ]]; then
      die "this node already started a new mesh with --name $stored_name; re-run without --name to finish it"
    fi
    MODE="new"
    RESUME=1
    MESH_CIDR="${stored_cidr:-${MESH_CIDR_ARG:-$DEFAULT_MESH_CIDR}}"
    CONTAINER_CIDR="${stored_container:-$CONTAINER_CIDR_ARG}"
    NODE_NAME="${stored_name:-$NODE_NAME_ARG}"
  elif [[ "$CONFIGURED" == yes && "$PENDING" == join ]]; then
    if [[ -n "$MESH_CIDR_ARG" ]]; then
      die "this node is joining a mesh ($(identity_file) exists); --mesh-cidr only applies to a new mesh"
    fi
    if [[ -n "$CONTAINER_CIDR_ARG" ]]; then
      die "this node is joining a mesh ($(identity_file) exists); --container-cidr only applies to a new mesh"
    fi
    if [[ -n "$NODE_NAME_ARG" ]]; then
      die "this node is joining a mesh ($(identity_file) exists); --name only applies to a new mesh"
    fi
    MODE="join"
    if [[ "$RESET_REQUESTED" == 1 ]] && stored_token_matches && identity_pending_join; then
      # A re-run of the same --join <token> --reset. Resetting again would
      # give this machine a new instance id for an invite it has already
      # registered with, and the cluster would reject it.
      RESUME=1
      log "--reset: this node is already joining with this token; nothing to reset"
    elif [[ "$RESET_REQUESTED" == 1 ]]; then
      # RESUME stays 0: the stored endpoint belongs to the old identity.
      RESET_IDENTITY=1
    elif [[ "$JOIN_REQUESTED" == 1 ]] && ! stored_token_matches; then
      # RESUME stays 0: the stored endpoint belongs to the old invite.
      log "this node has an unfinished join; replacing its stored join token (fabercp discards the identity of the join that never finished)"
      REPLACED_JOIN_NODE_ID="$(identity_node_id)"
    else
      RESUME=1
    fi
  elif [[ "$CONFIGURED" == yes && "$JOIN_REQUESTED" == 1 && "$RESET_REQUESTED" == 1 ]]; then
    # validate_args has refused --mesh-cidr, --container-cidr and --name.
    MODE="join"
    RESET_IDENTITY=1
  elif [[ "$CONFIGURED" == yes ]]; then
    if [[ "$JOIN_REQUESTED" == 1 ]]; then
      die "this node is already configured ($(identity_file) exists); refusing --join. Re-run without --join to upgrade it."
    fi
    if [[ -n "$MESH_CIDR_ARG" ]]; then
      die "this node is already configured; --mesh-cidr only applies to a new mesh"
    fi
    if [[ -n "$CONTAINER_CIDR_ARG" ]]; then
      die "this node is already configured; --container-cidr only applies to a new mesh"
    fi
    if [[ -n "$NODE_NAME_ARG" ]]; then
      die "this node is already configured; --name only applies to a new mesh"
    fi
    MODE="existing"
  elif [[ "$JOIN_REQUESTED" == 1 ]]; then
    MODE="join"
    if [[ "$PENDING" == join ]] && stored_token_matches; then
      RESUME=1
    fi
    if [[ "$RESET_REQUESTED" == 1 && "$CONFIGURED" == no ]]; then
      log "--reset: this node has no identity ($(identity_file)); nothing to reset"
    elif [[ "$RESET_REQUESTED" == 1 ]]; then
      # The state directory cannot be inspected (non-root --dry-run): show
      # what a run on a node with an identity would do.
      RESUME=0
      RESET_IDENTITY=1
    fi
  elif [[ "$PENDING" == join && -z "$MESH_CIDR_ARG" && -z "$CONTAINER_CIDR_ARG" && -z "$NODE_NAME_ARG" ]]; then
    stored_token_usable ||
      die "a join was started on this node but $(token_file) is missing; re-run with --join <token>, or with --mesh-cidr <cidr> to create a new mesh instead"
    MODE="join"
    RESUME=1
    log "continuing the join recorded in $(env_file) (pass --join for a new token, or --mesh-cidr to create a new mesh instead)"
  else
    MODE="new"
    if [[ "$PENDING" == new && -z "$MESH_CIDR_ARG" && -z "$CONTAINER_CIDR_ARG" && -z "$NODE_NAME_ARG" ]]; then
      RESUME=1
    fi
    MESH_CIDR="${MESH_CIDR_ARG:-${stored_cidr:-$DEFAULT_MESH_CIDR}}"
    CONTAINER_CIDR="${CONTAINER_CIDR_ARG:-$stored_container}"
    NODE_NAME="${NODE_NAME_ARG:-$stored_name}"
  fi
  if [[ "$MODE" == new && -z "$NODE_NAME" ]]; then
    NODE_NAME="$(hostname_node_name)" || exit 1
    log "node name: $NODE_NAME (from the hostname; pass --name to choose another)"
  fi
  if [[ "$RESET_IDENTITY" == 1 ]]; then
    check_real_dir "$FABER_STATE_DIR"
    log "--reset: this node's identity and raft data will be discarded before it joins with the new token"
  fi
  check_cidr_overlap
  if [[ "$RESUME" == 1 ]]; then
    log "mode: $MODE (continuing an unfinished bootstrap)"
  else
    log "mode: $MODE"
  fi
}

# env_names_interface: the env file written by an earlier run tells fabercp to
# use $WG_INTERFACE, so an existing interface of that name is faber's.
env_names_interface() {
  [[ "$(env_file_value FABER_WG_INTERFACE)" == "$WG_INTERFACE" ]]
}

# wg_interface_exists: `ip link show` finds $WG_INTERFACE.
wg_interface_exists() { ip link show dev "$WG_INTERFACE" >/dev/null 2>&1; }

die_foreign_interface() {
  die "interface $WG_INTERFACE already exists and was not created by faber; remove it (ip link delete $WG_INTERFACE) or rename it, then re-run"
}

# check_wg_interface: fabercp takes over $WG_INTERFACE, so on a node faber has
# never run on (no identity.json, no env file naming the interface) an
# existing interface of that name belongs to someone else. Without the ip
# command the check is repeated after install_packages.
check_wg_interface() {
  DEFER_WG_CHECK=0
  ENV_NAMED_INTERFACE=0
  if env_names_interface; then
    ENV_NAMED_INTERFACE=1
  fi
  [[ "$CONFIGURED" == no && "$PENDING" != unknown ]] || return 0
  [[ "$ENV_NAMED_INTERFACE" == 0 ]] || return 0
  if ! have ip; then
    DEFER_WG_CHECK=1
    log "ip not found; $WG_INTERFACE will be checked after installing packages"
    return 0
  fi
  if wg_interface_exists; then
    die_foreign_interface
  fi
}

# warn_unowned_interface: preflight accepts an existing $WG_INTERFACE once the
# env file names it. fabercp records the interface it created in
# $FABER_STATE_DIR/iface; when the link exists without that record, say how to
# remove it. Only the existence of the record is tested.
warn_unowned_interface() {
  local record="$FABER_STATE_DIR/iface"
  [[ "$ENV_NAMED_INTERFACE" == 1 ]] || return 0
  [[ ! -e "$record" && ! -L "$record" ]] || return 0
  have ip || return 0
  wg_interface_exists || return 0
  warn "interface $WG_INTERFACE exists but faber has no ownership record; remove it with: ip link delete $WG_INTERFACE"
}

# check_deferred_wg_interface: run after install_packages when preflight had
# no ip command. Dies before any faber-specific file is written.
check_deferred_wg_interface() {
  [[ "$DEFER_WG_CHECK" == 1 ]] || return 0
  if [[ "$DRY_RUN" == 1 ]]; then
    log "would check now that $WG_INTERFACE does not exist yet (after installing packages)"
    return 0
  fi
  have ip ||
    die "ip command not found after installing packages; cannot check whether $WG_INTERFACE already exists"
  if wg_interface_exists; then
    die_foreign_interface
  fi
}

# env_file_endpoint: FABER_WG_ENDPOINT from an existing env file (read as
# text, never sourced), normalized; nothing if absent or malformed.
env_file_endpoint() {
  local value
  value="$(env_file_value FABER_WG_ENDPOINT)"
  [[ -n "$value" ]] || return 0
  if ! normalize_endpoint "$value" 2>/dev/null; then
    warn "ignoring malformed FABER_WG_ENDPOINT in $(env_file)"
  fi
  return 0
}

# detect_endpoint: this host's source address towards the internet, from
# `ip -4 route get`, as ip:$WG_PORT.
detect_endpoint() {
  local out src="" i
  local -a words
  have ip || return 1
  out="$(ip -4 route get 1.1.1.1 2>/dev/null)" || return 1
  read -r -a words <<<"${out//$'\n'/ }"
  for ((i = 0; i + 1 < ${#words[@]}; i++)); do
    if [[ "${words[i]}" == src ]]; then
      src="${words[i + 1]}"
      break
    fi
  done
  valid_ipv4 "$src" || return 1
  printf '%s:%s\n' "$src" "$WG_PORT"
}

# resolve_endpoint: --endpoint, else the value already in the env file, else
# (new mesh only) autodetect. A joiner with a new token uses --endpoint or
# nothing: fabercp then takes the endpoint from the token, and a value left in
# the env file by an earlier run must not contradict it. A continued join
# (RESUME=1, same token) keeps the value its first run wrote.
resolve_endpoint() {
  local kept
  WG_ENDPOINT=""
  if [[ -n "$ENDPOINT_ARG" ]]; then
    WG_ENDPOINT="$ENDPOINT_ARG"
    log "endpoint: $WG_ENDPOINT (from --endpoint)"
    return 0
  fi
  [[ "$MODE" != join || "$RESUME" == 1 ]] || return 0
  kept="$(env_file_endpoint)"
  if [[ -n "$kept" ]]; then
    WG_ENDPOINT="$kept"
    log "keeping endpoint $WG_ENDPOINT from $(env_file)"
    return 0
  fi
  if [[ "$MODE" == new ]]; then
    if have ip; then
      autodetect_endpoint
    else
      # iproute2 is installed with the packages; detect right after that,
      # still before anything faber-specific is written.
      DEFER_ENDPOINT=1
      log "ip not found; the endpoint will be autodetected after installing packages"
    fi
  fi
}

# autodetect_endpoint: set WG_ENDPOINT from detect_endpoint or die.
autodetect_endpoint() {
  WG_ENDPOINT="$(detect_endpoint)" ||
    die "could not autodetect this node's WireGuard endpoint (ip -4 route get failed or gave no source address); pass --endpoint <public-ip>[:port]"
  log "autodetected endpoint $WG_ENDPOINT (pass --endpoint if peers reach this host at another address, e.g. behind NAT)"
}

# resolve_deferred_endpoint: run after install_packages when preflight had no
# ip command. Dies before any faber-specific file is written.
resolve_deferred_endpoint() {
  [[ "$DEFER_ENDPOINT" == 1 ]] || return 0
  if [[ "$DRY_RUN" == 1 ]]; then
    log "would autodetect the endpoint now (after installing packages)"
    return 0
  fi
  autodetect_endpoint
}

# render_env BOOTSTRAP(0|1): fabercp.env content. With BOOTSTRAP=1 it also
# carries the new-mesh or join settings, which mark the bootstrap as pending.
render_env() {
  local bootstrap="$1"
  printf '%s\n' "# managed by faber install.sh; changes are overwritten"
  printf 'FABER_WG_INTERFACE=%s\n' "$WG_INTERFACE"
  printf 'FABER_STATE_DIR=%s\n' "$FABER_STATE_DIR"
  printf 'FABER_DATA_DIR=%s\n' "$FABER_STATE_DIR/data"
  if [[ -n "$WG_ENDPOINT" ]]; then
    printf 'FABER_WG_ENDPOINT=%s\n' "$WG_ENDPOINT"
  fi
  printf 'FABER_WG_PORT=%s\n' "$WG_PORT"
  printf 'FABER_MANAGE_HOST=1\n'
  printf 'FABER_CONTAINERD_CONF_DIR=%s\n' "$CONTAINERD_CONF_DIR"
  printf 'FABER_CNI_CONF_DIR=%s\n' "$CNI_CONF_DIR"
  if [[ "$bootstrap" == 1 ]]; then
    case "$MODE" in
      new)
        printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=%s\n' "$MESH_CIDR"
        if [[ -n "$NODE_NAME" ]]; then
          printf 'FABER_NODE_NAME=%s\n' "$NODE_NAME"
        fi
        if [[ -n "$CONTAINER_CIDR" ]]; then
          printf 'FABER_CONTAINER_CIDR=%s\n' "$CONTAINER_CIDR"
        fi
        ;;
      join) printf 'FABER_JOIN_TOKEN_FILE=%s\n' "$(token_file)" ;;
    esac
  fi
}

render_containerd_root_config() {
  printf '%s\n' "$MANAGED_MARKER" 'version = 2' \
    "imports = [\"$CONTAINERD_CONF_DIR/*.toml\"]"
}

render_fabercp_unit() {
  cat <<EOF
$MANAGED_MARKER
[Unit]
Description=Faber control plane
After=network-online.target containerd.service
Wants=network-online.target

[Service]
User=$FABER_USER
Group=$FABER_USER
EnvironmentFile=$(env_file)
ExecStart=$FABER_HOME/fabercp
Restart=always
RestartSec=2
AmbientCapabilities=CAP_NET_ADMIN
CapabilityBoundingSet=CAP_NET_ADMIN
NoNewPrivileges=yes
StateDirectory=faber
StateDirectoryMode=0700
ProtectSystem=strict
ReadWritePaths=$CONTAINERD_CONF_DIR
ReadWritePaths=$CNI_CONF_DIR
ProtectHome=yes
PrivateTmp=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK

[Install]
WantedBy=multi-user.target
EOF
}

render_reload_path_unit() {
  cat <<EOF
$MANAGED_MARKER
[Unit]
Description=Watch the faber containerd drop-in

[Path]
PathChanged=$CONTAINERD_CONF_DIR/faber.toml
Unit=$RELOAD_SERVICE_UNIT

[Install]
WantedBy=multi-user.target
EOF
}

render_reload_service_unit() {
  cat <<EOF
$MANAGED_MARKER
[Unit]
Description=Restart containerd after the faber drop-in changes
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl try-restart containerd.service
EOF
}

# configure_faber_etc: $FABER_ETC, the env file and (joining only) the token
# file. Sets ENV_CHANGED and TOKEN_CHANGED.
configure_faber_etc() {
  local token
  token="$(token_file)"
  run_cmd install -d -o root -g "$FABER_USER" -m 0750 "$FABER_ETC"
  write_file "$(env_file)" "$(render_env 1)" root "$FABER_USER" 0640
  ENV_CHANGED="$FILE_CHANGED"
  TOKEN_CHANGED=0
  if [[ "$MODE" == join ]]; then
    if [[ "$JOIN_REQUESTED" == 1 ]]; then
      write_file "$token" "$JOIN_TOKEN" root "$FABER_USER" 0640 "$token (redacted)"
      TOKEN_CHANGED="$FILE_CHANGED"
    else
      # Continuing an earlier --join: the token it stored stays as it is.
      log "keeping the join token in $token"
    fi
  elif [[ -e "$token" || -L "$token" ]]; then
    # A stale token from an earlier run is not needed any more.
    run_cmd rm -f -- "$token"
    log "removed stale $token"
  fi
}

# is_managed_file PATH: a regular file carrying the faber marker line.
is_managed_file() {
  [[ -f "$1" && ! -L "$1" ]] && grep -Fxq -- "$MANAGED_MARKER" "$1" 2>/dev/null
}

# configure_containerd_root: containerd's root config only imports the
# drop-in directory, which fabercp fills. A config without the faber marker is
# backed up first. Sets ROOT_CONFIG_CHANGED.
configure_containerd_root() {
  local conf="$CONTAINERD_ROOT_CONFIG" dir bak n=0
  dir="$(dirname "$conf")"
  [[ -d "$dir" ]] || run_cmd install -d -m 0755 "$dir"
  run_cmd install -d -o "$FABER_USER" -g "$FABER_USER" -m 0755 "$CONTAINERD_CONF_DIR"
  if [[ -e "$conf" || -L "$conf" ]] && ! is_managed_file "$conf"; then
    bak="$conf.faber-bak.$(date +%s)"
    while [[ -e "$bak" || -L "$bak" ]]; do
      n=$((n + 1))
      bak="$conf.faber-bak.$(date +%s).$n"
    done
    run_cmd cp -pP -- "$conf" "$bak"
    log "backed up existing $conf to $bak"
  fi
  write_file "$conf" "$(render_containerd_root_config)" root root 0644
  ROOT_CONFIG_CHANGED="$FILE_CHANGED"
}

# foreign_cni_configs: names of the network configs in $CNI_CONF_DIR that
# faber did not write, one per line in glob (sorted) order. Only the files
# containerd would load count: regular files and symlinks ending in .conf,
# .conflist or .json, dot files included (fabercp's own temp file ends in
# .tmp).
foreign_cni_configs() {
  local f name d="$CNI_CONF_DIR"
  [[ -d "$d" && ! -L "$d" ]] || return 0
  for f in "$d"/*.conf "$d"/*.conflist "$d"/*.json "$d"/.*.conf "$d"/.*.conflist "$d"/.*.json; do
    [[ -f "$f" || -L "$f" ]] || continue
    name="${f##*/}"
    [[ "$name" == "$CNI_CONFLIST" ]] || printf '%s\n' "$name"
  done | LC_ALL=C sort
}

# quote_names: read names, one per line, and print them on one line, each
# quoted for the shell so that control characters in a name are not printed
# raw.
quote_names() {
  local name out=""
  while IFS= read -r name; do
    out+="${out:+ }$(printf '%q' "$name")"
  done
  printf '%s\n' "$out"
}

# configure_cni_conf_dir: $CNI_CONF_DIR belongs to the faber user, because
# fabercp writes $CNI_CONFLIST there (preflight has already refused a symlink
# or non-directory). Nothing in it is touched. containerd loads the lexically
# first config in the directory, so a config from someone else can win over
# faber's: warn and name it.
configure_cni_conf_dir() {
  local parent foreign
  parent="$(dirname "$CNI_CONF_DIR")"
  [[ -d "$parent" ]] || run_cmd install -d -m 0755 "$parent"
  run_cmd install -d -o "$FABER_USER" -g "$FABER_USER" -m 0755 "$CNI_CONF_DIR"
  foreign="$(foreign_cni_configs)"
  if [[ -n "$foreign" ]]; then
    warn "$CNI_CONF_DIR holds CNI configs that faber did not write: $(quote_names <<<"$foreign")"
    warn "  containerd loads the lexically first config in that directory, so one of these can be used instead of $CNI_CONFLIST; move them away if containers get the wrong network"
  fi
}

# install_units: write the systemd units. Sets UNITS_CHANGED and
# FABERCP_UNIT_CHANGED.
install_units() {
  UNITS_CHANGED=0
  [[ -d "$SYSTEMD_UNIT_DIR" ]] || run_cmd install -d -m 0755 "$SYSTEMD_UNIT_DIR"
  write_file "$SYSTEMD_UNIT_DIR/$FABERCP_UNIT" "$(render_fabercp_unit)" root root 0644
  FABERCP_UNIT_CHANGED="$FILE_CHANGED"
  ((FILE_CHANGED)) && UNITS_CHANGED=1
  write_file "$SYSTEMD_UNIT_DIR/$RELOAD_PATH_UNIT" "$(render_reload_path_unit)" root root 0644
  ((FILE_CHANGED)) && UNITS_CHANGED=1
  write_file "$SYSTEMD_UNIT_DIR/$RELOAD_SERVICE_UNIT" "$(render_reload_service_unit)" root root 0644
  ((FILE_CHANGED)) && UNITS_CHANGED=1
  return 0
}

systemd_present() { [[ -d "$SYSTEMD_RUN_DIR" ]]; }

# reset_node_identity: the --reset step (see the header); does nothing unless
# preflight set RESET_IDENTITY. Runs after the token and the env file are
# written and before the services are started, so the start that finds the
# marker also finds the token. fabercp is stopped first: the identity and the
# raft data must not change under a running fabercp, and a stopped service is
# started by `enable --now` rather than left to a try-restart. The marker is
# created with its final owner and mode in a root-only staging directory next
# to $FABER_STATE_DIR and renamed into place, so nothing the faber user put
# at that path is written through.
reset_node_identity() {
  local marker parent stage
  [[ "$RESET_IDENTITY" == 1 ]] || return 0
  marker="$FABER_STATE_DIR/$JOIN_FAILED_MARKER"
  parent="$(dirname "$FABER_STATE_DIR")"
  if systemd_present; then
    run_cmd systemctl stop fabercp ||
      die "could not stop fabercp; nothing was reset. Stop it and re-run with --join <token> --reset"
  else
    warn "systemd not detected; stop fabercp yourself: the identity is discarded the next time fabercp starts"
  fi
  if [[ "$DRY_RUN" == 1 ]]; then
    stage="$parent/.faber-stage.XXXXXX"
    printf '+ mktemp -d %s\n' "$stage"
  else
    check_real_dir "$FABER_STATE_DIR"
    [[ -d "$FABER_STATE_DIR" ]] || die "$FABER_STATE_DIR does not exist; nothing to reset"
    check_stage_parent "$parent"
    STAGE_DIR="$(mktemp -d "$parent/.faber-stage.XXXXXX")"
    trap cleanup_tmp EXIT
    stage="$STAGE_DIR"
  fi
  run_cmd install -o "$FABER_USER" -g "$FABER_USER" -m 0644 /dev/null "$stage/$JOIN_FAILED_MARKER" ||
    die "could not stage $marker"
  run_cmd mv -fT "$stage/$JOIN_FAILED_MARKER" "$marker" || die "could not write $marker"
  run_cmd rm -rf "$stage"
  STAGE_DIR=""
  log "wrote $marker; fabercp discards $(identity_file) and the raft data in $FABER_STATE_DIR/data when it starts"
}

# manage_services RESTART_CONTAINERD RESTART_FABERCP: reload units, restart
# what changed (try-restart only touches running services, and runs before
# enable --now so a service is never started twice), then enable and start.
manage_services() {
  local restart_containerd="$1" restart_fabercp="$2"
  if [[ "$UNITS_CHANGED" == 1 ]]; then
    run_cmd systemctl daemon-reload
  fi
  if [[ "$restart_containerd" == 1 ]]; then
    run_cmd systemctl try-restart containerd
  fi
  if [[ "$restart_fabercp" == 1 ]]; then
    run_cmd systemctl try-restart fabercp
  fi
  run_cmd systemctl enable --now containerd "$RELOAD_PATH_UNIT" fabercp
}

# poll_until TIMEOUT CMD...: run CMD until it succeeds or TIMEOUT seconds pass.
poll_until() {
  local timeout="$1" deadline
  shift
  deadline=$((SECONDS + timeout))
  while :; do
    "$@" && return 0
    ((SECONDS >= deadline)) && return 1
    sleep "$FABER_POLL_INTERVAL"
  done
}

# The faber CLI is root-owned (root:root 0755 in a root-owned directory), so
# running it as root is safe; the faber-writable fabercp never is.
faber_cli() { "$FABER_CLI_DIR/faber" "$@"; }
faber_healthy() { faber_cli health >/dev/null 2>&1; }
faber_joined() {
  local out
  out="$(faber_cli -o json cluster status 2>/dev/null)" || return 1
  [[ "$out" =~ \"joined\"[[:space:]]*:[[:space:]]*true ]]
}

# wait_for_join: poll 'faber -o json cluster status' for up to
# $FABER_JOIN_TIMEOUT seconds. Returns 0 once joined, 1 on timeout, and 2 as
# soon as identity.json is gone: fabercp writes it before it answers 'faber
# health' and deletes it only when the leader rejects the invite.
wait_for_join() {
  local deadline=$((SECONDS + FABER_JOIN_TIMEOUT))
  while :; do
    faber_joined && return 0
    [[ "$(node_configured)" == no ]] && return 2
    ((SECONDS >= deadline)) && return 1
    sleep "$FABER_POLL_INTERVAL"
  done
}

# bootstrap_error_code: print token_expired, token_invalid, invite_rejected
# or node_removed when fabercp recorded that in
# $FABER_STATE_DIR/bootstrap-error, else nothing.
# The file sits in a directory the faber user can write, so it is read only
# when it is a regular file and not a symlink, at most 64 bytes of it, and the
# result is one of the four literals below, never the file's content. A file
# older than join.token was written about an earlier token and is ignored.
bootstrap_error_code() {
  local f="$FABER_STATE_DIR/bootstrap-error" token content
  token="$(token_file)"
  [[ -f "$f" && ! -L "$f" ]] || return 0
  if [[ -f "$token" && ! -L "$token" && "$token" -nt "$f" ]]; then
    return 0
  fi
  # NUL bytes become '?' so they cannot match (and bash does not warn).
  content="$(head -c 64 -- "$f" 2>/dev/null | tr '\0' '?')" || return 0
  case "$content" in
    token_expired | token_invalid | invite_rejected | node_removed) printf '%s\n' "$content" ;;
  esac
  return 0
}

# warn_bootstrap_error: when a join failed because of its token, or because
# the cluster removed this node, say so and return 0; return 1 when fabercp
# recorded no (known) reason.
warn_bootstrap_error() {
  [[ "$MODE" == join ]] || return 1
  case "$(bootstrap_error_code)" in
    token_expired)
      warn "the join token has expired; create a new invite (faber mesh invite ...) on an existing node and re-run with --join <token>"
      ;;
    token_invalid)
      warn "the join token is not valid; create a new invite (faber mesh invite ...) on an existing node and re-run with --join <token>"
      ;;
    invite_rejected)
      warn "the invite was rejected by the cluster; create a new invite and re-run with --join <token>"
      ;;
    node_removed)
      warn "the cluster removed this node; create a new invite (faber mesh invite ...) on an existing node and rejoin with --join <token> --reset"
      ;;
    *) return 1 ;;
  esac
}

# note_unused_invite: --join replaced the token of an unfinished join, and the
# node has now joined. If identity.json still names the node of the earlier
# invite, that join had got far enough for fabercp to keep its identity, and
# the new invite was never used. Say so: the operator made that invite for
# this node, and it keeps a mesh address reserved until it expires.
note_unused_invite() {
  [[ -n "$REPLACED_JOIN_NODE_ID" ]] || return 0
  if [[ "$(identity_node_id)" == "$REPLACED_JOIN_NODE_ID" ]]; then
    warn "this node had already joined the cluster with its earlier invite (node $REPLACED_JOIN_NODE_ID), so the invite given with --join was not used; it expires on its own"
  fi
}

# finish_bootstrap: called only after the cluster result is 'created' or
# 'joined'. Removes the bootstrap settings from the env file (which marks the
# node as bootstrapped for later runs), then the token.
finish_bootstrap() {
  local token
  token="$(token_file)"
  if [[ "$DRY_RUN" == 1 ]]; then
    printf '+ write %s\n' "$(env_file)"
    [[ "$MODE" == join ]] && printf '+ rm -f -- %s\n' "$token"
    return 0
  fi
  if [[ "$(node_configured)" != yes ]]; then
    warn "$(identity_file) not found yet; leaving the bootstrap settings in $(env_file)"
    return 0
  fi
  write_file "$(env_file)" "$(render_env 0)" root "$FABER_USER" 0640
  if [[ -e "$token" || -L "$token" ]]; then
    rm -f -- "$token"
    log "removed $token"
  fi
}

# bootstrap_cluster: runs until a run ends with 'created' or 'joined' (MODE is
# 'existing' after that). Sets CLUSTER_RESULT. On 'failed' the env file and
# the token stay untouched, so the next run retries.
bootstrap_cluster() {
  local out rc
  if [[ "$MODE" == existing ]]; then
    CLUSTER_RESULT=existing
    return 0
  fi
  if [[ "$DRY_RUN" == 1 ]]; then
    run_cmd "$FABER_CLI_DIR/faber" health
    if [[ "$MODE" == new ]]; then
      run_cmd "$FABER_CLI_DIR/faber" cluster init
      CLUSTER_RESULT=would-create
    else
      run_cmd "$FABER_CLI_DIR/faber" -o json cluster status
      CLUSTER_RESULT=would-join
    fi
    finish_bootstrap
    return 0
  fi

  CLUSTER_RESULT=failed
  log "waiting up to ${FABER_HEALTH_TIMEOUT}s for fabercp"
  if ! poll_until "$FABER_HEALTH_TIMEOUT" faber_healthy; then
    warn "fabercp did not answer 'faber health' within ${FABER_HEALTH_TIMEOUT}s; check: journalctl -u fabercp"
    warn_unowned_interface
    if ! warn_bootstrap_error; then
      warn "the bootstrap settings stay in $(env_file); re-run this script to try again"
    fi
    return 0
  fi

  if [[ "$MODE" == new ]]; then
    rc=0
    out="$(faber_cli cluster init 2>&1)" || rc=$?
    if [[ "$rc" == 0 || "$out" == *already_initialized* ]]; then
      CLUSTER_RESULT=created
    else
      warn "faber cluster init failed: $out"
      warn "the bootstrap settings stay in $(env_file); re-run this script to try again"
    fi
  else
    log "waiting up to ${FABER_JOIN_TIMEOUT}s for this node to join the cluster"
    rc=0
    wait_for_join || rc=$?
    case "$rc" in
      0)
        CLUSTER_RESULT=joined
        note_unused_invite
        ;;
      2)
        if ! warn_bootstrap_error; then
          warn "the invite was rejected; create a new invite and re-run with --join <token>"
          warn "  (fabercp removed $(identity_file); journalctl -u fabercp has the reason: invalid, used or expired)"
        fi
        ;;
      *)
        if [[ -n "$(bootstrap_error_code)" ]]; then
          warn "this node has not joined the cluster after ${FABER_JOIN_TIMEOUT}s; check: journalctl -u fabercp"
          warn_bootstrap_error || true
        else
          warn "this node has not joined the cluster after ${FABER_JOIN_TIMEOUT}s. fabercp keeps retrying; check that:"
          warn "  - UDP $WG_PORT on this host is reachable from the existing nodes (cloud security groups, host firewalls)"
          warn "  - the invite's --endpoint is the public IP that peers see for this host (NAT)"
          warn "  - journalctl -u fabercp shows no errors"
          warn "the token stays in $(token_file); re-run this script (no --join needed) to wait again"
        fi
        ;;
    esac
  fi
  if [[ "$CLUSTER_RESULT" != failed ]]; then
    finish_bootstrap
  fi
}

configure_kernel() {
  local m
  write_if_changed "$MODULES_LOAD_FILE" "$(printf '%s\n' "${KERNEL_MODULES[@]}")"
  write_if_changed "$SYSCTL_FILE" "$(printf '%s\n' \
    'net.ipv4.ip_forward = 1' 'net.ipv6.conf.all.forwarding = 1')"
  for m in "${KERNEL_MODULES[@]}"; do
    run_quiet modprobe "$m" || warn "modprobe $m failed (container or missing module?)"
  done
  run_quiet sysctl --system || warn "sysctl --system failed (expected inside containers)"
}

# forward_drop_chains: read `nft list ruleset` output on stdin and print
# "FAMILY TABLE CHAIN" for every base chain on the forward hook whose policy
# is drop.
forward_drop_chains() {
  local line table="" chain=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^[[:space:]]*table[[:space:]]+([^[:space:]]+)[[:space:]]+([^[:space:]]+)[[:space:]]*\{ ]]; then
      table="${BASH_REMATCH[1]} ${BASH_REMATCH[2]}"
      chain=""
    elif [[ "$line" =~ ^[[:space:]]*chain[[:space:]]+([^[:space:]]+)[[:space:]]*\{ ]]; then
      chain="${BASH_REMATCH[1]}"
    elif [[ "$line" =~ ^[[:space:]]*type[[:space:]].*[[:space:]]hook[[:space:]]+forward[[:space:]].*policy[[:space:]]+drop[[:space:]]*\; ]]; then
      printf '%s %s\n' "$table" "$chain"
    fi
  done
}

# quote_chains: join "FAMILY TABLE CHAIN" lines with ", ", each word quoted
# for the shell.
quote_chains() {
  local family table chain out=""
  while read -r family table chain; do
    out+="${out:+, }$(printf '%q %q %q' "$family" "$table" "$chain")"
  done
  printf '%s\n' "$out"
}

# warn_forward_drop: ufw, Docker and firewalld commonly give the forward hook
# a drop policy, and a drop in any chain on that hook is final, whatever
# faber's own table accepts. Container traffic between nodes is forwarded
# between $WG_INTERFACE and $CONTAINER_BRIDGE, so say that it will be blocked.
# The host firewall belongs to the admin: this only reads the ruleset and
# never changes it. Silent in --dry-run, without nft, and when the ruleset
# cannot be listed.
warn_forward_drop() {
  local ruleset chains
  [[ "$DRY_RUN" == 0 ]] || return 0
  have nft || return 0
  ruleset="$(nft list ruleset 2>/dev/null)" || return 0
  chains="$(forward_drop_chains <<<"$ruleset" | quote_chains)"
  [[ -n "$chains" ]] || return 0
  warn "the host firewall drops forwarded packets by default (forward chain with policy drop: $chains)"
  warn "  container traffic between nodes will be blocked until forwarding between $WG_INTERFACE and the container bridge $CONTAINER_BRIDGE is allowed in both directions; faber does not change this firewall"
}

# --- faber user, home and binaries -----------------------------------------

# script_dir: directory containing this script, with symlinks resolved.
script_dir() {
  local src="${BASH_SOURCE[0]}" resolved
  if resolved="$(readlink -f "$src" 2>/dev/null)" && [[ -n "$resolved" ]]; then
    src="$resolved"
  fi
  (CDPATH='' cd -P "$(dirname "$src")" && pwd)
}

bin_src_dir() {
  if [[ -n "${FABER_BIN_SRC_DIR:-}" ]]; then
    printf '%s\n' "$FABER_BIN_SRC_DIR"
  else
    script_dir
  fi
}

# binary_spec NAME: "DEST OWNER GROUP MODE" for a Faber binary.
binary_spec() {
  case "$1" in
    fabercp) printf '%s %s %s %s\n' "$FABER_HOME/fabercp" "$FABER_USER" "$FABER_USER" 0750 ;;
    faber) printf '%s %s %s %s\n' "$FABER_CLI_DIR/faber" root root 0755 ;;
    *) die "unknown binary: $1" ;;
  esac
}

# missing_binaries: print the source binaries that are not readable regular
# files, one per line.
missing_binaries() {
  local dir name
  dir="$(bin_src_dir)"
  for name in "${FABER_BINARIES[@]}"; do
    [[ -f "$dir/$name" && -r "$dir/$name" ]] || printf '%s\n' "$dir/$name"
  done
}

# verify_binaries: die unless every source binary is a readable regular file.
verify_binaries() {
  local missing
  missing="$(missing_binaries)"
  [[ -z "$missing" ]] ||
    die "missing faber binary: ${missing//$'\n'/, } (place fabercp and faber next to install.sh or set FABER_BIN_SRC_DIR)"
}

# check_faber_home: refuse a symlinked or non-directory $FABER_HOME.
check_faber_home() { check_real_dir "$FABER_HOME"; }

# check_faber_account: an existing faber user must have primary group faber.
check_faber_account() {
  local group
  id -u "$FABER_USER" >/dev/null 2>&1 || return 0
  group="$(id -gn "$FABER_USER" 2>/dev/null || true)"
  if [[ "$group" != "$FABER_USER" ]]; then
    die "user $FABER_USER exists but its primary group is '${group:-unknown}', expected '$FABER_USER'; fix the account (e.g. usermod -g $FABER_USER $FABER_USER) and re-run"
  fi
}

# preflight: checks that must pass before anything is changed.
preflight() {
  verify_binaries
  check_faber_home
  check_faber_account
  check_config_paths
  check_stage_parents
  resolve_mode
  check_wg_interface
  resolve_endpoint
}

nologin_shell() {
  local s
  for s in /usr/sbin/nologin /sbin/nologin /usr/bin/nologin; do
    if [[ -x "$s" ]]; then
      printf '%s\n' "$s"
      return 0
    fi
  done
  echo /usr/sbin/nologin
}

ensure_faber_user() {
  local -a group_opt
  if id -u "$FABER_USER" >/dev/null 2>&1; then
    log "user $FABER_USER exists"
    return 0
  fi
  if getent group "$FABER_USER" >/dev/null 2>&1; then
    group_opt=(-g "$FABER_USER")
  else
    group_opt=(--user-group)
  fi
  run_cmd useradd --system --home-dir "$FABER_HOME" --no-create-home \
    --shell "$(nologin_shell)" "${group_opt[@]}" "$FABER_USER"
}

# ensure_faber_home: create $FABER_HOME and enforce owner/mode on every run
# (preflight has already refused a symlink or non-directory).
ensure_faber_home() {
  run_cmd install -d -o "$FABER_USER" -g "$FABER_USER" -m 0750 "$FABER_HOME"
}

# files_identical A B (falls back to sha256sum where cmp is not installed).
files_identical() {
  if have cmp; then
    cmp -s "$1" "$2"
  else
    [[ "$(sha256sum <"$1")" == "$(sha256sum <"$2")" ]]
  fi
}

# meta_matches PATH OWNER GROUP MODE: owner/group (name or id) and mode match.
meta_matches() {
  local path="$1" owner="$2" group="$3" mode="$4" uid gid uname gname m
  read -r uid gid uname gname m < <(stat -c '%u %g %U %G %a' "$path") || return 1
  [[ "$owner" == "$uid" || "$owner" == "$uname" ]] &&
    [[ "$group" == "$gid" || "$group" == "$gname" ]] &&
    ((8#$m == 8#$mode))
}

# binary_state SRC DEST OWNER GROUP MODE:
#   unknown  DEST's directory cannot be searched (non-root)
#   missing  nothing at DEST
#   differs  DEST is a symlink, not a regular file, or has other content
#   perms    same content, wrong owner/group/mode
#   same     regular file, same content, right owner/group/mode
# Symlinks are checked first so nothing is compared through one.
binary_state() {
  local src="$1" dest="$2" owner="$3" group="$4" mode="$5" dir
  dir="$(dirname "$dest")"
  if [[ -d "$dir" && ! -x "$dir" ]]; then
    echo unknown
  elif [[ -L "$dest" ]]; then
    echo differs
  elif [[ ! -e "$dest" ]]; then
    echo missing
  elif [[ ! -f "$dest" ]] || ! files_identical "$src" "$dest"; then
    echo differs
  elif ! meta_matches "$dest" "$owner" "$group" "$mode"; then
    echo perms
  else
    echo same
  fi
}

# binary_state_of NAME: binary_state for a Faber binary against its source.
binary_state_of() {
  local dest owner group mode
  read -r dest owner group mode < <(binary_spec "$1")
  binary_state "$(bin_src_dir)/$1" "$dest" "$owner" "$group" "$mode"
}

# check_stage_parent DIR [NAME]: the staging parent must be a directory owned
# by us (root) and not group/world-writable, so nobody else can tamper with
# it. NAME is the binary staged there. For the faber CLI the error names the
# way out, because its default parent /usr/local is group-writable on some
# distributions (root:staff 2775 on some Debian releases).
check_stage_parent() {
  local dir="$1" name="${2:-}" uid m hint=""
  [[ -d "$dir" ]] || die "staging directory parent $dir does not exist"
  read -r uid m < <(stat -L -c '%u %a' "$dir")
  if [[ "$uid" != "$EUID" ]] || (((8#$m & 8#022) != 0)); then
    if [[ "$name" == faber ]]; then
      hint="; to install the faber CLI somewhere else, re-run with FABER_CLI_DIR set to a directory whose parent only root can write, for example FABER_CLI_DIR=/usr/bin"
    fi
    die "$dir must be owned by uid $EUID and not group/world-writable (used to stage binaries)$hint"
  fi
}

# check_stage_parents: preflight form of check_stage_parent, so a bad staging
# parent stops the run before any package is installed. Covers each binary
# that will be staged (anything but an identical, correctly owned target). A
# parent that does not exist yet is created by this script later, and
# install_binary checks it again right before staging. --dry-run only checks
# as root, where the answer matches what a real run would see.
check_stage_parents() {
  local name dest owner group mode
  if [[ "$DRY_RUN" == 1 && "$EUID" != 0 ]]; then
    return 0
  fi
  for name in "${FABER_BINARIES[@]}"; do
    read -r dest owner group mode < <(binary_spec "$name")
    [[ "$(binary_state_of "$name")" != same ]] || continue
    dest="$(dirname "$(dirname "$dest")")"
    [[ -e "$dest" ]] || continue
    check_stage_parent "$dest" "$name"
  done
}

# install_binary SRC DEST OWNER GROUP MODE [STATE]: replace DEST unless STATE
# (computed when omitted) is 'same'. The file is created with its final
# owner/mode in a fresh root-only staging dir next to DEST's directory, then
# renamed over DEST with mv -T, so no path the faber user controls is ever
# written through, chowned or chmodded.
install_binary() {
  local src="$1" dest="$2" owner="$3" group="$4" mode="$5" state="${6:-}"
  local name parent stage
  [[ -n "$state" ]] || state="$(binary_state "$src" "$dest" "$owner" "$group" "$mode")"
  if [[ -d "$dest" && ! -L "$dest" ]]; then
    die "$dest is a directory; refusing to replace it"
  fi
  case "$state" in
    same)
      log "$dest up-to-date"
      return 0
      ;;
    unknown)
      if [[ "$DRY_RUN" == 1 ]]; then
        log "cannot inspect $dest; run --dry-run as root for an accurate prediction"
        return 0
      fi
      ;;
  esac
  name="$(basename "$dest")"
  parent="$(dirname "$(dirname "$dest")")"
  if [[ "$DRY_RUN" == 1 ]]; then
    stage="$parent/.faber-stage.XXXXXX"
    printf '+ mktemp -d %s\n' "$stage"
  else
    check_stage_parent "$parent" "$name"
    STAGE_DIR="$(mktemp -d "$parent/.faber-stage.XXXXXX")"
    trap cleanup_tmp EXIT
    stage="$STAGE_DIR"
  fi
  run_cmd install -o "$owner" -g "$group" -m "$mode" "$src" "$stage/$name" ||
    die "could not stage $src"
  run_cmd mv -fT "$stage/$name" "$dest" || die "could not move $name into place at $dest"
  run_cmd rm -rf "$stage"
  STAGE_DIR=""
  log "installed $dest"
}

# install_faber_binaries FABERCP_STATE FABER_STATE (states from binary_state_of;
# computed when omitted).
install_faber_binaries() {
  local src name dest owner group mode
  local -a states=("${1:-}" "${2:-}")
  local i=0
  src="$(bin_src_dir)"
  ensure_faber_user
  ensure_faber_home
  [[ -d "$FABER_CLI_DIR" ]] || run_cmd install -d -m 0755 "$FABER_CLI_DIR"
  for name in "${FABER_BINARIES[@]}"; do
    read -r dest owner group mode < <(binary_spec "$name")
    install_binary "$src/$name" "$dest" "$owner" "$group" "$mode" "${states[i]}"
    i=$((i + 1))
  done
}

# NOTE: nftables.service is deliberately NOT enabled. On most distros it loads
# /etc/nftables.conf at boot, which typically begins with "flush ruleset" and
# would wipe the rules Faber manages at runtime.

# --- reporting --------------------------------------------------------------

print_row() { printf '%-11s | %-14s | %-14s | %s\n' "$@"; }

# cni_conf_dir_state: $CNI_CONF_DIR for --check.
cni_conf_dir_state() {
  local foreign
  if [[ -L "$CNI_CONF_DIR" ]]; then
    echo symlink
  elif [[ ! -e "$CNI_CONF_DIR" ]]; then
    echo missing
  elif [[ ! -d "$CNI_CONF_DIR" ]]; then
    echo "not a directory"
  else
    foreign="$(foreign_cni_configs)"
    if [[ -n "$foreign" ]]; then
      printf 'other configs: %s\n' "$(quote_names <<<"$foreign")"
    else
      echo ok
    fi
  fi
}

# bootstrap_state: the "Bootstrap state" rule from the header, for --check.
bootstrap_state() {
  local configured pending
  configured="$(node_configured)"
  pending="$(pending_bootstrap)"
  case "$pending" in
    new) echo "pending (new mesh)" ;;
    join) echo "pending (join)" ;;
    unknown) echo unknown ;;
    *)
      case "$configured" in
        yes) echo complete ;;
        no) echo "not started" ;;
        *) echo unknown ;;
      esac
      ;;
  esac
}

check_report() {
  local file id id_like pretty family arch comp missing
  file="$(os_release_file)"
  if [[ -r "$file" ]]; then
    id="$(os_release_value ID "$file")"
    id_like="$(os_release_value ID_LIKE "$file")"
    pretty="$(os_release_value PRETTY_NAME "$file")"
    family="$(pkg_family "$id" "$id_like" 2>/dev/null)" || family="unsupported"
  else
    id="unknown" pretty="unknown ($file not readable)" family="unsupported"
  fi
  arch="$(cni_arch 2>/dev/null)" || arch="unsupported (${FABER_ARCH:-$(uname -m)})"

  printf 'os:              %s (%s)\n' "${id:-unknown}" "${pretty:-unknown}"
  printf 'package manager: %s\n' "$family"
  printf 'arch:            %s\n' "$arch"
  missing="$(missing_binaries)"
  printf 'cni target:      %s\n' "$DEFAULT_CNI_VERSION"
  printf 'cni bin dir:     %s\n' "$CNI_BIN_DIR"
  printf 'cni conf dir:    %s (%s)\n' "$CNI_CONF_DIR" "$(cni_conf_dir_state)"
  printf 'faber home:      %s\n' "$FABER_HOME"
  printf 'faber cli dir:   %s\n' "$FABER_CLI_DIR"
  if [[ -n "$missing" ]]; then
    printf 'binary source:   %s (missing: %s)\n' "$(bin_src_dir)" "${missing//$'\n'/ }"
  else
    printf 'binary source:   %s (ok)\n' "$(bin_src_dir)"
  fi
  printf 'configured:      %s\n' "$(node_configured)"
  printf 'bootstrap:       %s\n' "$(bootstrap_state)"
  if have systemctl; then
    printf 'fabercp.service: %s\n' "$(systemctl is-active "$FABERCP_UNIT" 2>/dev/null || true)"
  else
    printf 'fabercp.service: n/a (no systemctl)\n'
  fi
  if ! have ip; then
    printf '%-17s%s\n' "$WG_INTERFACE:" "unknown (no ip command)"
  elif ip link show dev "$WG_INTERFACE" >/dev/null 2>&1; then
    printf '%-17s%s\n' "$WG_INTERFACE:" present
  else
    printf '%-17s%s\n' "$WG_INTERFACE:" absent
  fi
  printf '\n%-11s | %s\n' component current
  printf '%-11s-+-%s\n' '-----------' '--------------'
  for comp in "${COMPONENTS[@]}"; do
    printf '%-11s | %s\n' "$comp" "$(component_version "$comp")"
  done
}

# --- main -------------------------------------------------------------------

main() {
  local family arch comp restart_containerd=0 restart_fabercp=0
  local cni_target="$DEFAULT_CNI_VERSION"
  local -A before=() after=() status=() bin_state=()

  parse_args "$@"
  if [[ "$CHECK" == 1 ]]; then
    check_report
    return 0
  fi
  require_root
  preflight

  family="$(detect_os)"
  log "detected package family: $family"
  arch="$(cni_arch)"
  [[ "$DRY_RUN" == 1 ]] && log "dry-run: no changes will be made"

  for comp in "${FABER_BINARIES[@]}"; do
    bin_state[$comp]="$(binary_state_of "$comp")"
    before[$comp]="$(binary_version "${bin_state[$comp]}" "$(bin_src_dir)/$comp")"
  done
  for comp in "${COMPONENTS[@]}"; do
    [[ -n "${before[$comp]:-}" ]] || before[$comp]="$(component_version "$comp")"
  done

  install_packages "$family"
  resolve_deferred_endpoint
  check_deferred_wg_interface
  install_cni "$cni_target" "$arch"
  install_faber_binaries "${bin_state[fabercp]}" "${bin_state[faber]}"

  for comp in "${COMPONENTS[@]}"; do
    if [[ "$DRY_RUN" == 1 ]]; then
      after[$comp]="${before[$comp]}"
    else
      after[$comp]="$(component_version "$comp")"
    fi
    if [[ -n "${bin_state[$comp]:-}" ]]; then
      status[$comp]="$(binary_status "${bin_state[$comp]}" "${after[$comp]}")"
    elif [[ "$DRY_RUN" == 1 ]]; then
      status[$comp]="$(dry_run_status "$comp" "${before[$comp]}" "$cni_target")"
    else
      status[$comp]="$(compute_status "${before[$comp]}" "${after[$comp]}")"
    fi
  done

  configure_faber_etc
  configure_containerd_root
  configure_cni_conf_dir
  install_units
  configure_kernel
  warn_forward_drop

  if [[ "${status[containerd]}" == upgraded || "$ROOT_CONFIG_CHANGED" == 1 ]]; then
    restart_containerd=1
  fi
  case "${bin_state[fabercp]}" in
    same | unknown) ;;
    *) restart_fabercp=1 ;;
  esac
  if [[ "$ENV_CHANGED" == 1 || "$TOKEN_CHANGED" == 1 || "$FABERCP_UNIT_CHANGED" == 1 ]]; then
    restart_fabercp=1
  fi

  reset_node_identity
  if systemd_present; then
    manage_services "$restart_containerd" "$restart_fabercp"
    bootstrap_cluster
  else
    warn "systemd not detected; skipping service start and cluster bootstrap (start containerd and fabercp yourself)"
    CLUSTER_RESULT="skipped (no systemd)"
  fi

  printf '\n'
  print_row component before after status
  printf '%s\n' '------------+----------------+----------------+-----------'
  for comp in "${COMPONENTS[@]}"; do
    print_row "$comp" "${before[$comp]}" "${after[$comp]}" "${status[$comp]}"
  done
  printf '\ncluster: %s\n' "$CLUSTER_RESULT"

  for comp in "${COMPONENTS[@]}"; do
    [[ "${status[$comp]}" == failed ]] && die "$comp failed to install"
  done
  if [[ "$CLUSTER_RESULT" == failed ]]; then
    die "cluster bootstrap did not complete; see the warnings above"
  fi
  return 0
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
