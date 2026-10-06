#!/usr/bin/env bash
# End-to-end test of install.sh, fabercp and the faber CLI on real systemd
# nodes: Ubuntu 24.04 containers with systemd as PID 1, on one Docker network.
# Opt-in; it needs Docker with a kernel that has WireGuard (Docker Desktop
# does) and network access for apt and the CNI download.
#
#   tests/e2e/run.sh              build the binaries and images, run everything
#
# Environment:
#   E2E_SKIP_BUILD=1   reuse tmp/e2e-bin/{fabercp,faber} instead of building
#   E2E_KEEP=1         leave the containers and the network up afterwards
#   E2E_PHASES="..."   phases to run, default "A B H J C D E F I G", always
#                      in that order (B needs A; H, J, C, D, E, F and I need B)
#
# Phases:
#   A  new mesh on node A; containerd config and the reload path unit
#   B  invite, join node B, mesh traffic, firewall, and a machine that only
#      holds an invite is refused on the raft RPC routes
#   H  container network between A and B: subnets in WireGuard, the route,
#      the CNI config, the forward chain, and pod sandboxes started with
#      crictl that reach each other across the nodes and nothing else; the
#      bridge takes nothing from the Docker network, a forged source address
#      or IPv6 (the test gives the nodes IPv6 addresses for that check)
#   J  projects, environments and workloads through the CLI on A and B:
#      compose, Kubernetes and spec deploys, --dry-run and --strict, files
#      that are refused, removal, and the records after a restart of
#      fabercp. Nothing runs a container here: the phase checks the records
#   C  install.sh re-run changes nothing
#   D  restart and stop of fabercp
#   E  hardening of the service and the files
#   F  failure paths on a third node (foreign faber0, expired, invalid and
#      used tokens), then a fresh invite that replaces the stuck join; on a
#      fourth node, a used token that reaches the mesh and is rejected
#   I  node names, the lease setting, a new server that takes over the name
#      of a running node, the removed node's way back, and --join --reset on
#      a configured node (joins C and E first when F did not run). No lease
#      is left to run out: the shortest one is 300 s
#   G  phase A again on a node with Docker's containerd.io 2.x preinstalled,
#      and a pod sandbox on that node
#
# crictl is downloaded into the nodes by this test only (cri-tools
# $CRICTL_VERSION, checked against the sha256 its release publishes).
#
# Every assertion prints PASS or FAIL. Logs go to tests/e2e/logs/<run>/, and
# after a failure they include the journal, systemctl status, wg, nft and
# ip addr of every node. The exit code is 1 when an assertion failed.
#
# Images (faber-e2e-node:*) and the build volumes are kept. Containers and
# the network are removed at the end.

set -uo pipefail

# Git Bash would rewrite /paths in docker arguments.
export MSYS_NO_PATHCONV=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

# host_path PATH: the form docker.exe understands on Windows (D:/...).
host_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s\n' "$1"; fi
}

REPO_H="$(host_path "$REPO")"
BIN_DIR="$REPO/tmp/e2e-bin"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="$HERE/logs/$RUN_ID"

NET="faber-e2e"
IMAGE="faber-e2e-node:latest"
IMAGE_CIO="faber-e2e-node:containerd-io"
NODE_A="faber-e2e-a"
NODE_B="faber-e2e-b"
NODE_C="faber-e2e-c"
NODE_D="faber-e2e-d"
NODE_E="faber-e2e-e"
NODE_F="faber-e2e-f"
ALL_NODES=("$NODE_A" "$NODE_B" "$NODE_C" "$NODE_D" "$NODE_E" "$NODE_F")
SRC="/root/faber-src"
PHASES="${E2E_PHASES:-A B H J C D E F I G}"

WG_PORT=51820
PEER_PORT=7401
MESH_A="10.99.0.1"
MESH_B="10.99.0.2"
# The founder is named after its hostname; every other node by its invite.
NAME_A="$NODE_A"
NAME_B="e2e-b"
NAME_C="e2e-c"
NAME_E="e2e-e"
CONTAINER_CIDR="10.200.0.0/16"
# Raft gives the founder the first /24 of the range and B the next one.
SUBNET_A="10.200.0.0/24"
SUBNET_B="10.200.1.0/24"
CNI_CONFLIST="/etc/cni/net.d/10-faber.conflist"
# cri-tools 1.34 talks CRI v1 to both containerd releases under test: 2.2
# from Ubuntu 24.04 and 2.3 from Docker's containerd.io.
CRICTL_VERSION="v1.34.0"
CRICTL_URL="https://github.com/kubernetes-sigs/cri-tools/releases/download"
CRICTL_SHA256_AMD64="a8ff2a3edb37a98daf3aba7c3b284fe0aa5bff24166d896ab9ef64c8913c9f51"
CRICTL_SHA256_ARM64="c31d252e203df5f4cf37f314bd3092eb79087e791631c1e607087c74b6d0423f"
# CAP_NET_ADMIN is bit 12.
CAP_NET_ADMIN_MASK="0000000000001000"

PASS=0
FAIL=0
# How the pod sandboxes of phases H and G were made, for the result.
SANDBOX_PATH=""
FAILED=()
STARTED=()

# --- output -----------------------------------------------------------------

section() { printf '\n=== %s ===\n' "$*"; }
info() { printf '      %s\n' "$*"; }

# evidence TEXT: print TEXT indented, at most 25 lines.
evidence() {
  printf '%s\n' "$1" | head -n 25 | sed 's/^/      | /'
}

pass() {
  PASS=$((PASS + 1))
  printf 'PASS  %s\n' "$1"
}

fail() {
  FAIL=$((FAIL + 1))
  FAILED+=("$1")
  printf 'FAIL  %s\n' "$1"
  if [[ -n "${2:-}" ]]; then evidence "$2"; fi
}

# assert_eq DESC ACTUAL EXPECTED
assert_eq() {
  if [[ "$2" == "$3" ]]; then pass "$1 [$2]"; else fail "$1" "expected: $3"$'\n'"actual:   $2"; fi
}

# assert_ne DESC ACTUAL UNWANTED
assert_ne() {
  if [[ "$2" != "$3" ]]; then pass "$1 [$2]"; else fail "$1" "unexpectedly equal to: $3"; fi
}

# assert_has DESC TEXT REGEX: TEXT has a line matching the extended REGEX.
assert_has() {
  if grep -Eq -- "$3" <<<"$2"; then pass "$1"; else fail "$1" "no match for /$3/ in:"$'\n'"$2"; fi
}

# assert_hasnt DESC TEXT REGEX
assert_hasnt() {
  if grep -Eq -- "$3" <<<"$2"; then
    fail "$1" "unexpected match for /$3/:"$'\n'"$(grep -E -- "$3" <<<"$2")"
  else
    pass "$1"
  fi
}

# assert_ok DESC RC [DETAIL]
assert_ok() {
  if [[ "$2" == 0 ]]; then pass "$1"; else fail "$1" "exit code $2"$'\n'"${3:-}"; fi
}

# assert_fails DESC RC [DETAIL]
assert_fails() {
  if [[ "$2" != 0 ]]; then pass "$1 [exit $2]"; else fail "$1" "exit code 0"$'\n'"${3:-}"; fi
}

# --- docker helpers ---------------------------------------------------------

# dx NODE SCRIPT: run SCRIPT with bash in NODE; prints stdout and stderr.
dx() { docker exec "$1" bash -c "$2" 2>&1 | tr -d '\r'; }

# dxq NODE SCRIPT: run quietly, return the exit code.
dxq() { docker exec "$1" bash -c "$2" >/dev/null 2>&1; }

# wait_for SECONDS NODE SCRIPT: poll until SCRIPT succeeds in NODE.
wait_for() {
  local deadline=$((SECONDS + $1))
  while :; do
    dxq "$2" "$3" && return 0
    ((SECONDS >= deadline)) && return 1
    sleep 1
  done
}

node_ip() {
  docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$1" | tr -d '\r'
}

# start_node NAME IMAGE: boot a systemd node and copy install.sh and the
# binaries into $SRC.
start_node() {
  local name="$1" image="$2" state=""
  docker rm -fv "$name" >/dev/null 2>&1
  docker run -d --name "$name" --hostname "$name" --network "$NET" \
    --privileged --cgroupns=host \
    --tmpfs /run --tmpfs /run/lock \
    -v /var/lib/containerd \
    "$image" >/dev/null || { fail "start $name"; return 1; }
  STARTED+=("$name")
  # "degraded" is normal in a container (a few units cannot start).
  wait_for 60 "$name" 'case "$(systemctl is-system-running 2>/dev/null)" in running|degraded) exit 0 ;; *) exit 1 ;; esac'
  state="$(dx "$name" 'systemctl is-system-running')"
  docker exec "$name" mkdir -p "$SRC"
  docker cp -q "$(host_path "$REPO/src/install.sh")" "$name:$SRC/install.sh"
  docker cp -q "$(host_path "$BIN_DIR/fabercp")" "$name:$SRC/fabercp"
  docker cp -q "$(host_path "$BIN_DIR/faber")" "$name:$SRC/faber"
  docker exec "$name" bash -c "chown -R root:root $SRC && chmod 0755 $SRC/install.sh $SRC/fabercp $SRC/faber"
  info "$name up: ip $(node_ip "$name"), systemd $state"
}

# run_install NODE LOGNAME STDIN [docker-exec-options...] -- [install args...]
# Sets INSTALL_OUT and INSTALL_RC. STDIN is piped to the script when not empty.
run_install() {
  local node="$1" logname="$2" stdin="$3"
  shift 3
  local -a opts=()
  while (($# > 0)) && [[ "$1" != -- ]]; do
    opts+=("$1")
    shift
  done
  shift
  if [[ -n "$stdin" ]]; then
    INSTALL_OUT="$(printf %s "$stdin" | docker exec -i "${opts[@]}" "$node" "$SRC/install.sh" "$@" 2>&1)"
    INSTALL_RC=$?
  else
    INSTALL_OUT="$(docker exec "${opts[@]}" "$node" "$SRC/install.sh" "$@" 2>&1 </dev/null)"
    INSTALL_RC=$?
  fi
  INSTALL_OUT="$(tr -d '\r' <<<"$INSTALL_OUT")"
  printf '%s\n' "$INSTALL_OUT" >"$LOG_DIR/$logname.log"
  info "install.sh on $node: exit $INSTALL_RC, full output in logs/$RUN_ID/$logname.log"
  if [[ "$INSTALL_RC" == 0 ]]; then
    evidence "$(tail -n 12 <<<"$INSTALL_OUT")"
  else
    evidence "$(grep -E '^cluster:|WARN|ERROR' <<<"$INSTALL_OUT" | grep -v modprobe)"
  fi
}

voters() { dx "$1" "faber -o json cluster status | grep -c '\"voter\": true'"; }
unit_stamp() { dx "$1" "systemctl show -p MainPID -p NRestarts -p ActiveEnterTimestampMonotonic $2 | sort | tr '\n' ' '"; }
main_pid() { dx "$1" "systemctl show -p MainPID --value $2"; }

collect_logs() {
  local node file
  section "collecting logs into tests/e2e/logs/$RUN_ID"
  for node in "${STARTED[@]}"; do
    file="$LOG_DIR/node-$node.log"
    {
      for cmd in \
        'journalctl -u fabercp --no-pager' \
        'journalctl -u containerd -u faber-containerd-reload.service -u faber-containerd-reload.path --no-pager' \
        'systemctl status --no-pager fabercp containerd faber-containerd-reload.path faber-containerd-reload.service' \
        'wg show' \
        'nft list ruleset' \
        'ip addr' \
        'ip route' \
        'cat /etc/faber/fabercp.env' \
        'ls -la /etc/faber /var/lib/faber /etc/containerd /etc/containerd/conf.d' \
        'containerd --version; containerd config dump' \
        "ls -la /etc/cni/net.d; cat $CNI_CONFLIST" \
        'ip -d link show faberbr0; ip -4 addr show faberbr0' \
        'faber cluster status; faber -o json cluster status; faber mesh lease' \
        'command -v crictl && crictl pods && crictl info'; do
        printf '\n##### %s\n' "$cmd"
        docker exec "$node" bash -c "$cmd" 2>&1
      done
    } >"$file" 2>&1
    info "$file"
  done
}

cleanup() {
  if [[ "${E2E_KEEP:-0}" == 1 ]]; then
    info "E2E_KEEP=1: leaving ${STARTED[*]:-no containers} and network $NET"
    return 0
  fi
  local node
  for node in "${ALL_NODES[@]}"; do
    docker rm -fv "$node" >/dev/null 2>&1
  done
  docker network rm "$NET" >/dev/null 2>&1
}

# --- setup ------------------------------------------------------------------

build_binaries() {
  if [[ "${E2E_SKIP_BUILD:-0}" == 1 && -f "$BIN_DIR/fabercp" && -f "$BIN_DIR/faber" ]]; then
    info "E2E_SKIP_BUILD=1: using the binaries in $BIN_DIR"
    return 0
  fi
  mkdir -p "$BIN_DIR"
  # rust:latest is Debian; the binaries need glibc <= 2.39 to run on
  # Ubuntu 24.04, which check_binaries verifies by running them there.
  docker run --rm \
    -v "$REPO_H:/code" \
    -v faber-linux-target:/target \
    -v faber-cargo-registry:/usr/local/cargo/registry \
    -e CARGO_TARGET_DIR=/target -w /code rust:latest bash -c '
      set -e
      apt-get update -qq
      apt-get install -y -qq clang libclang-dev cmake >/dev/null
      cargo build -p fabercp -p faber
      cp /target/debug/fabercp /target/debug/faber /code/tmp/e2e-bin/'
}

build_images() {
  docker build -q -t "$IMAGE" "$(host_path "$HERE")" >/dev/null || return 1
  if [[ " $PHASES " == *" G "* ]]; then
    docker build -q --build-arg CONTAINERD_IO=1 -t "$IMAGE_CIO" "$(host_path "$HERE")" >/dev/null || return 1
  fi
}

check_binaries() {
  docker run --rm -v "$(host_path "$BIN_DIR"):/b:ro" "$IMAGE" bash -c '
    cp /b/fabercp /b/faber /tmp/ && chmod +x /tmp/fabercp /tmp/faber &&
    /tmp/fabercp --version && /tmp/faber --version' 2>&1
}

# --- phase A: new mesh ------------------------------------------------------

# containerd_checks TAG NODE: the root config, the drop-in and what
# containerd makes of them.
containerd_checks() {
  local t="$1" n="$2" out dump
  out="$(dx "$n" 'stat -c "%U:%G %a" /etc/containerd/conf.d/faber.toml')"
  assert_eq "$t containerd: conf.d/faber.toml exists, faber:faber 0644" "$out" "faber:faber 644"
  out="$(dx "$n" 'cat /etc/containerd/config.toml')"
  assert_has "$t containerd: root config is version 2 and imports conf.d" "$out" '^imports = \["/etc/containerd/conf.d/\*\.toml"\]$'
  info "containerd version: $(dx "$n" 'containerd --version')"
  info "containerd package: $(dx "$n" "dpkg-query -W -f='\${Package} \${Version} (\${Status})\n' containerd containerd.io 2>/dev/null | grep 'ok installed'")"

  # fabercp writes the drop-in after 'cluster init'; the path unit then
  # restarts containerd. Wait for that to settle.
  wait_for 30 "$n" 'systemctl is-active -q containerd && containerd config dump 2>/dev/null | grep -q "SystemdCgroup = true"'
  dump="$(dx "$n" 'containerd config dump')"
  printf '%s\n' "$dump" >"$LOG_DIR/containerd-config-dump-$n.log"
  evidence "$(grep -En 'SystemdCgroup|pause:|sandbox|bin_dir|conf_dir|^version|^imports' <<<"$dump")"
  assert_has "$t containerd config dump: SystemdCgroup = true" "$dump" 'SystemdCgroup = true'
  assert_has "$t containerd config dump: sandbox image registry.k8s.io/pause:3.10" "$dump" "registry\.k8s\.io/pause:3\.10['\"]"
  assert_has "$t containerd config dump: CNI bin dir /opt/cni/bin" "$dump" 'bin_dirs? = .*/opt/cni/bin'
  assert_has "$t containerd config dump: CNI conf dir /etc/cni/net.d" "$dump" "conf_dir = ['\"]/etc/cni/net\.d['\"]"
  assert_has "$t containerd config dump: imports the conf.d drop-ins" "$dump" "^imports = \[['\"]/etc/containerd/conf\.d/"
  out="$(dx "$n" 'ctr plugins ls 2>&1 | grep -E "io.containerd.grpc.v1 +cri|TYPE"')"
  evidence "$out"
  assert_has "$t containerd: CRI plugin loaded (status ok)" "$out" 'cri .* ok *$'
}

# reload_path_checks TAG NODE: a changed drop-in restarts containerd, and
# containerd then runs with the changed value. Uses a sandbox image that is
# no default of any containerd version, so the import is proven to be read.
reload_path_checks() {
  local t="$1" n="$2" before_pid after_pid before_run after_run out dump
  assert_eq "$t path unit: faber-containerd-reload.path is active" \
    "$(dx "$n" 'systemctl is-active faber-containerd-reload.path')" "active"

  # Let the reload from the first drop-in write finish.
  wait_for 30 "$n" '! systemctl is-active -q faber-containerd-reload.service && systemctl is-active -q containerd'
  sleep 2
  before_pid="$(main_pid "$n" containerd)"
  before_run="$(dx "$n" 'systemctl show -p ExecMainStartTimestampMonotonic --value faber-containerd-reload.service')"

  dx "$n" 'set -e
    cd /etc/containerd/conf.d
    sed "s#pause:3.10#pause:3.9-e2e#" faber.toml > .e2e-new.toml
    chown faber:faber .e2e-new.toml && chmod 0644 .e2e-new.toml
    mv -f .e2e-new.toml faber.toml' >/dev/null
  wait_for 30 "$n" "systemctl is-active -q containerd && [[ \"\$(systemctl show -p MainPID --value containerd)\" != \"$before_pid\" ]] && containerd config dump | grep -q 'pause:3.9-e2e'"
  after_pid="$(main_pid "$n" containerd)"
  after_run="$(dx "$n" 'systemctl show -p ExecMainStartTimestampMonotonic --value faber-containerd-reload.service')"
  assert_ne "$t path unit: replacing faber.toml ran faber-containerd-reload.service (start stamp $before_run ->)" "$after_run" "$before_run"
  assert_ne "$t path unit: containerd restarted (MainPID $before_pid ->)" "$after_pid" "$before_pid"
  dump="$(dx "$n" 'containerd config dump')"
  assert_has "$t containerd uses the changed drop-in (pause:3.9-e2e in config dump)" "$dump" 'pause:3\.9-e2e'
  out="$(dx "$n" 'journalctl -u faber-containerd-reload.service --no-pager -o cat | tail -n 6')"
  evidence "$out"
  assert_has "$t path unit: journal shows the reload service finished" "$out" 'Finished|Deactivated successfully'

  # fabercp puts its own content back at startup; that write must trigger
  # the path unit as well.
  before_pid="$after_pid"
  dx "$n" 'systemctl restart fabercp' >/dev/null
  wait_for 30 "$n" 'faber health'
  wait_for 30 "$n" "grep -q 'pause:3.10\"' /etc/containerd/conf.d/faber.toml && systemctl is-active -q containerd && [[ \"\$(systemctl show -p MainPID --value containerd)\" != \"$before_pid\" ]] && containerd config dump | grep -q 'pause:3.10'"
  out="$(dx "$n" 'grep sandbox_image /etc/containerd/conf.d/faber.toml')"
  assert_has "$t fabercp restores the drop-in on restart" "$out" 'registry\.k8s\.io/pause:3\.10"'
  assert_ne "$t path unit: fabercp's own write restarted containerd (MainPID $before_pid ->)" "$(main_pid "$n" containerd)" "$before_pid"
  dump="$(dx "$n" 'containerd config dump')"
  assert_hasnt "$t containerd is back on pause:3.10" "$dump" 'pause:3\.9-e2e'
  assert_eq "$t founder still joined with 1 voter after its restart" "$(voters "$n")" "1"
}

# new_mesh_checks TAG NODE IP LOGNAME
new_mesh_checks() {
  local t="$1" n="$2" ip="$3" logname="$4" out pid

  run_install "$n" "$logname" "" -- --endpoint "$ip"
  assert_ok "$t install.sh --endpoint $ip exits 0" "$INSTALL_RC"
  assert_has "$t install.sh prints 'cluster: created'" "$INSTALL_OUT" '^cluster: created$'

  assert_eq "$t fabercp.service is active" "$(dx "$n" 'systemctl is-active fabercp')" "active"
  pid="$(main_pid "$n" fabercp)"
  assert_eq "$t fabercp runs as user faber (unit User=, /proc owner)" \
    "$(dx "$n" "systemctl show -p User --value fabercp; stat -c %U /proc/$pid" | tr '\n' ' ')" "faber faber "

  out="$(dx "$n" 'ip -o link show faber0; ip -o -4 addr show dev faber0')"
  evidence "$out"
  assert_has "$t faber0 is up" "$out" 'faber0: <[^>]*\bUP\b[^>]*>'
  assert_has "$t faber0 has $MESH_A/16" "$out" "inet ${MESH_A//./\\.}/16 "

  assert_eq "$t /var/lib/faber/iface records faber0" "$(dx "$n" 'cat /var/lib/faber/iface')" "faber0"

  out="$(dx "$n" 'wg show faber0')"
  evidence "$out"
  assert_has "$t wg show faber0: public key" "$out" 'public key: [A-Za-z0-9+/]{43}='
  assert_has "$t wg show faber0: listening port $WG_PORT" "$out" "listening port: $WG_PORT"

  out="$(dx "$n" 'nft list table inet faber')"
  evidence "$out"
  assert_has "$t nft: set peer_endpoints" "$out" 'set peer_endpoints \{'
  assert_has "$t nft: set peer_endpoints6" "$out" 'set peer_endpoints6 \{'
  assert_has "$t nft: set peer_mesh_ips" "$out" 'set peer_mesh_ips \{'
  assert_has "$t nft: set node_mesh_ips" "$out" 'set node_mesh_ips \{'
  assert_has "$t nft: chain input hooks input" "$out" 'type filter hook input priority'
  assert_has "$t nft: chain forward hooks forward" "$out" 'type filter hook forward priority'

  out="$(dx "$n" 'faber cluster status')"
  evidence "$out"
  assert_has "$t faber cluster status: joined" "$out" '^joined: +yes$'
  assert_eq "$t faber cluster status: 1 voter" "$(voters "$n")" "1"

  containerd_checks "$t" "$n"
  reload_path_checks "$t" "$n"
}

phase_a() {
  section "A: new mesh on $NODE_A ($IP_A)"
  new_mesh_checks "A" "$NODE_A" "$IP_A" "install-a-1"
}

# --- phase B: join ----------------------------------------------------------

phase_b() {
  local out rc pub_b
  section "B: invite on $NODE_A, join on $NODE_B ($IP_B)"

  TOKEN="$(docker exec "$NODE_A" faber mesh invite --name "$NAME_B" --wg-endpoint "$IP_B" 2>"$LOG_DIR/invite-b.stderr" | tr -d '\r\n')"
  out="$(tr -d '\r' <"$LOG_DIR/invite-b.stderr")"
  evidence "$out"
  assert_has "B faber mesh invite names the new node on stderr" "$out" "^name: +$NAME_B\$"
  assert_hasnt "B faber mesh invite for a free name warns about no takeover" "$out" 'WARNING'
  if [[ "$TOKEN" =~ ^faber1\.[A-Za-z0-9_-]+$ ]]; then
    pass "B faber mesh invite prints only a token on stdout (${#TOKEN} chars, not shown)"
  else
    fail "B faber mesh invite prints only a token on stdout" "stdout had ${#TOKEN} chars and is not token-shaped"
  fi

  run_install "$NODE_B" "install-b-1" "$TOKEN" -- --join -
  assert_ok "B install.sh --join - exits 0" "$INSTALL_RC"
  assert_has "B install.sh prints 'cluster: joined'" "$INSTALL_OUT" '^cluster: joined$'
  if grep -Fq -- "$TOKEN" <<<"$INSTALL_OUT"; then
    fail "B install.sh output does not contain the token"
  else
    pass "B install.sh output does not contain the token"
  fi

  wait_for 30 "$NODE_A" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 2"
  wait_for 30 "$NODE_B" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 2"
  out="$(dx "$NODE_A" 'faber cluster status')"
  evidence "$out"
  assert_eq "B $NODE_A sees 2 voters" "$(voters "$NODE_A")" "2"
  out="$(dx "$NODE_B" 'faber cluster status')"
  evidence "$out"
  assert_eq "B $NODE_B sees 2 voters" "$(voters "$NODE_B")" "2"

  for n in "$NODE_A" "$NODE_B"; do
    out="$(dx "$n" 'wg show faber0 latest-handshakes')"
    evidence "$out"
    if awk '$2 > 0 { n++ } END { exit !(n >= 1) }' <<<"$out"; then
      pass "B $n: wg latest-handshake is non-zero"
    else
      fail "B $n: wg latest-handshake is non-zero" "$out"
    fi
  done

  out="$(dx "$NODE_A" 'nft list set inet faber peer_endpoints; nft list set inet faber peer_mesh_ips')"
  evidence "$out"
  assert_has "B $NODE_A nft peer_endpoints has B's endpoint IP $IP_B" "$out" "elements = \{[^}]*\b${IP_B//./\\.}\b"
  assert_has "B $NODE_A nft peer_mesh_ips has $MESH_B" "$out" "elements = \{[^}]*\b${MESH_B//./\\.}\b"
  out="$(dx "$NODE_B" 'nft list set inet faber peer_endpoints; nft list set inet faber peer_mesh_ips')"
  evidence "$out"
  assert_has "B $NODE_B nft peer_endpoints has A's endpoint IP $IP_A" "$out" "elements = \{[^}]*\b${IP_A//./\\.}\b"
  assert_has "B $NODE_B nft peer_mesh_ips has $MESH_A" "$out" "elements = \{[^}]*\b${MESH_A//./\\.}\b"

  out="$(dx "$NODE_A" "ping -c 3 -W 2 -I faber0 $MESH_B")"
  rc=$?
  evidence "$(tail -n 3 <<<"$out")"
  assert_has "B ping $MESH_B from $NODE_A over faber0" "$out" ' 0% packet loss'

  out="$(dx "$NODE_A" "curl -sS --max-time 5 -w '\nhttp=%{http_code}' http://$MESH_B:$PEER_PORT/api/v1/health")"
  evidence "$out"
  assert_has "B curl http://$MESH_B:$PEER_PORT/api/v1/health from $NODE_A over the mesh is 200" "$out" '^http=200$'

  # Off-mesh. curl exit 7 is "refused" (nothing listens on that address),
  # 28 is "timed out" (the firewall dropped it). Both keep the port closed.
  dxq "$NODE_A" "curl -sS --max-time 4 -o /dev/null http://$IP_B:$PEER_PORT/api/v1/health"
  rc=$?
  assert_fails "B $NODE_A cannot reach B's peer port on B's docker IP $IP_B:$PEER_PORT (curl 7=refused, 28=dropped)" "$rc"

  endpoint_auth_checks
  cli_timeout_check
  third_party_checks
  invite_holder_checks
}

# post_endpoint FROM_NODE TARGET_MESH_IP NODE_ID ENDPOINT: POST
# /raft/cluster/endpoint from FROM_NODE; prints the body and "http=<code>".
post_endpoint() {
  local body="{\"node_id\":$3,\"endpoint\":\"$4\"}"
  docker exec "$1" curl -sS --max-time 20 -w '\nhttp=%{http_code}\n' -X POST \
    -H 'content-type: application/json' -d "$body" \
    "http://$2:$PEER_PORT/raft/cluster/endpoint" 2>&1 | tr -d '\r'
}

# endpoint_auth_checks: a node may change only its own WireGuard endpoint.
endpoint_auth_checks() {
  local out bogus="203.0.113.9"
  out="$(post_endpoint "$NODE_B" "$MESH_A" 1 "$bogus:$WG_PORT")"
  evidence "$out"
  assert_has "B endpoint auth: B changing node 1's endpoint via the leader is 403" "$out" '^http=403$'
  out="$(post_endpoint "$NODE_B" "$MESH_B" 1 "$bogus:$WG_PORT")"
  evidence "$out"
  assert_has "B endpoint auth: B changing node 1's endpoint via its own listener is 403" "$out" '^http=403$'
  sleep 3
  out="$(dx "$NODE_B" 'wg show faber0 endpoints; nft list set inet faber peer_endpoints')"
  evidence "$out"
  assert_has "B endpoint auth: B's nft peer_endpoints still has A's $IP_A" "$out" "elements = \{[^}]*\b${IP_A//./\\.}\b"
  assert_has "B endpoint auth: B's wg peer endpoint for A is still $IP_A:$WG_PORT" "$out" "=[[:space:]]+${IP_A//./\\.}:$WG_PORT$"
  assert_hasnt "B endpoint auth: the bogus endpoint is nowhere on B" "$out" "${bogus//./\\.}"
  out="$(dx "$NODE_A" 'tr "," "\n" < /var/lib/faber/identity.json | grep endpoint; nft list set inet faber peer_endpoints; cat /etc/faber/fabercp.env')"
  assert_hasnt "B endpoint auth: the bogus endpoint is nowhere on A" "$out" "${bogus//./\\.}"

  out="$(post_endpoint "$NODE_B" "$MESH_A" 2 "$IP_B:$WG_PORT")"
  evidence "$out"
  assert_has "B endpoint auth: B setting its own (unchanged) endpoint is 204" "$out" '^http=204$'
  sleep 2
  out="$(dx "$NODE_A" 'wg show faber0 endpoints')"
  evidence "$out"
  assert_has "B endpoint auth: A still has B's endpoint $IP_B:$WG_PORT" "$out" "=[[:space:]]+${IP_B//./\\.}:$WG_PORT$"
  assert_eq "B endpoint auth: still 2 voters" "$(voters "$NODE_A")" "2"
}

# cli_timeout_check: the CLI gives up on an address that never answers
# (connect timeout 5 s). A table of our own drops the SYNs.
cli_timeout_check() {
  local out
  dx "$NODE_A" 'nft add table inet e2ecli
    nft add chain inet e2ecli output "{ type filter hook output priority 0; policy accept; }"
    nft add rule inet e2ecli output ip daddr 192.0.2.1 drop' >/dev/null
  out="$(dx "$NODE_A" 's=$(date +%s%N); faber -e http://192.0.2.1:7400 health; rc=$?; echo "rc=$rc ms=$(( ($(date +%s%N) - s) / 1000000 ))"')"
  dx "$NODE_A" 'nft delete table inet e2ecli' >/dev/null
  evidence "$out"
  if [[ "$out" =~ rc=([0-9]+)\ ms=([0-9]+) ]] && ((BASH_REMATCH[1] != 0 && BASH_REMATCH[2] >= 4000 && BASH_REMATCH[2] <= 9000)); then
    pass "B CLI: 'faber health' against a silent address fails after about 5 s (${BASH_REMATCH[2]} ms)"
  else
    fail "B CLI: 'faber health' against a silent address fails after about 5 s" "$out"
  fi
}

# third_party_checks: $NODE_C is on the same network but not in the mesh.
# A probe chain on B that runs after 'inet faber' counts what gets through.
third_party_checks() {
  local out rc pub_b
  info "$NODE_C ($IP_C) is on the docker network but is no mesh peer"
  dx "$NODE_B" "nft -f - <<'EOF'
table inet e2eprobe {
  chain input {
    type filter hook input priority 50; policy accept;
    ip saddr $IP_C udp dport $WG_PORT counter comment \"c-udp\"
    ip saddr $IP_C tcp dport $PEER_PORT counter comment \"c-tcp\"
    ip saddr $IP_A udp dport $WG_PORT counter comment \"a-udp\"
    ip saddr $IP_C icmp type echo-request counter comment \"c-icmp\"
  }
}
EOF" >/dev/null

  dxq "$NODE_C" "curl -sS --max-time 4 -o /dev/null http://$IP_B:$PEER_PORT/api/v1/health"
  rc=$?
  assert_fails "B non-peer cannot reach $IP_B:$PEER_PORT (curl 7=refused, 28=dropped)" "$rc"

  # Route B's mesh IP at B over the docker network: the packet arrives on
  # B's eth0 for an address B owns, so only the firewall keeps it out.
  dx "$NODE_C" "ip route replace $MESH_B/32 via $IP_B" >/dev/null
  dxq "$NODE_C" "curl -sS --max-time 4 -o /dev/null http://$MESH_B:$PEER_PORT/api/v1/health"
  rc=$?
  assert_eq "B non-peer routing $MESH_B via B's eth0 gets no answer from the peer port (curl 28 = dropped)" "$rc" "28"
  dx "$NODE_C" "ip route del $MESH_B/32" >/dev/null

  # A WireGuard handshake from an unknown host, using B's real public key.
  pub_b="$(dx "$NODE_B" 'wg show faber0 public-key')"
  out="$(dx "$NODE_C" "set -e
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq >/dev/null 2>&1
    apt-get install -y -qq --no-install-recommends wireguard-tools >/dev/null 2>&1
    ip link add e2erogue type wireguard
    (umask 077; wg genkey > /tmp/rogue.key)
    wg set e2erogue private-key /tmp/rogue.key peer '$pub_b' endpoint $IP_B:$WG_PORT allowed-ips $MESH_B/32
    ip addr add 10.99.0.77/16 dev e2erogue
    ip link set e2erogue up
    ping -c 3 -W 1 -I e2erogue $MESH_B >/dev/null 2>&1 || true
    sleep 6
    echo handshake: \$(wg show e2erogue latest-handshakes)
    echo transfer peer-received-sent: \$(wg show e2erogue transfer)
    ip link del e2erogue; rm -f /tmp/rogue.key")"
  evidence "$out"
  if grep -Eq '^handshake: [A-Za-z0-9+/=]+[[:space:]]+0$' <<<"$out"; then
    pass "B non-peer gets no WireGuard handshake from B"
  else
    fail "B non-peer gets no WireGuard handshake from B" "$out"
  fi
  # ICMP is not filtered by 'inet faber', so this shows the probe counts.
  dxq "$NODE_C" "ping -c 2 -W 1 $IP_B"
  # Make sure A sent B something on UDP since the probe was loaded.
  dxq "$NODE_A" "ping -c 2 -W 1 -I faber0 $MESH_B"

  out="$(dx "$NODE_B" 'nft list table inet e2eprobe')"
  evidence "$(grep counter <<<"$out")"
  assert_has "B probe behind 'inet faber': 0 UDP $WG_PORT packets from the non-peer got through" "$out" 'counter packets 0 bytes 0 comment "c-udp"'
  assert_has "B probe behind 'inet faber': 0 TCP $PEER_PORT packets from the non-peer got through" "$out" 'counter packets 0 bytes 0 comment "c-tcp"'
  assert_has "B probe control: UDP $WG_PORT from peer A does get through" "$out" 'counter packets [1-9][0-9]* bytes [0-9]+ comment "a-udp"'
  assert_has "B probe control: the probe sees the non-peer's ICMP" "$out" 'counter packets [1-9][0-9]* bytes [0-9]+ comment "c-icmp"'
  dx "$NODE_B" 'nft delete table inet e2eprobe' >/dev/null
}

# invite_holder_checks: $NODE_C holds a live invite it has not registered, and
# brings up WireGuard with the invite's key by hand. The peer firewall admits
# it, but it is no registered node, so the raft RPC routes must refuse it.
# The token goes to the node on stdin and its key is never printed.
invite_holder_checks() {
  local out json tok mesh_c pub_a pub_b n ip route
  json="$(docker exec "$NODE_A" faber -o json mesh invite --name e2e-holder --wg-endpoint "$IP_C" --ttl 45s 2>/dev/null | tr -d '\r')"
  tok="$(sed -n 's/.*"token": "\([^"]*\)".*/\1/p' <<<"$json")"
  mesh_c="$(sed -n 's/.*"mesh_ip": "\([^"]*\)".*/\1/p' <<<"$json")"
  pub_a="$(dx "$NODE_A" 'wg show faber0 public-key')"
  pub_b="$(dx "$NODE_B" 'wg show faber0 public-key')"
  info "$NODE_C holds an unregistered invite for $mesh_c (45 s)"
  out="$(printf %s "$tok" | docker exec -i "$NODE_C" bash -c "set -e
    umask 077
    b=\"\$(cat)\"
    b=\"\${b#faber1.}\"
    b=\"\$(printf %s \"\$b\" | tr '_-' '/+')\"
    while [ \$(( \${#b} % 4 )) -ne 0 ]; do b=\"\$b=\"; done
    printf %s \"\$b\" | base64 -d | sed -n 's/.*\"private_key\":\"\\([^\"]*\\)\".*/\\1/p' > /tmp/inv.key
    [ -s /tmp/inv.key ]
    ip link add e2einv type wireguard
    wg set e2einv private-key /tmp/inv.key \
      peer '$pub_a' endpoint $IP_A:$WG_PORT allowed-ips $MESH_A/32 \
      peer '$pub_b' endpoint $IP_B:$WG_PORT allowed-ips $MESH_B/32
    rm -f /tmp/inv.key
    ip addr add $mesh_c/16 dev e2einv
    ip link set e2einv up
    echo up" 2>&1 | tr -d '\r')"
  assert_eq "B invite holder: $NODE_C brought up WireGuard with the invite's key" "$out" "up"

  for n in "$NODE_A" "$NODE_B"; do
    ip="$MESH_A"
    [[ "$n" == "$NODE_B" ]] && ip="$MESH_B"
    wait_for 20 "$NODE_C" "curl -sS --max-time 3 -o /dev/null http://$ip:$PEER_PORT/api/v1/health"
    out="$(dx "$NODE_C" "curl -sS --max-time 5 -o /dev/null -w 'http=%{http_code}' http://$ip:$PEER_PORT/api/v1/health")"
    assert_eq "B invite holder control: it reaches the peer listener of $n over the mesh" "$out" "http=200"
    out="$(dx "$NODE_C" "curl -sS --max-time 5 -o /dev/null -w 'http=%{http_code}' http://$ip:$PEER_PORT/raft/cluster/status")"
    assert_has "B invite holder control: GET /raft/cluster/status on $n is not refused by source [$out]" "$out" '^http=(200|503)$'
    out="$(dx "$NODE_C" "curl -sS --max-time 5 -w '\nhttp=%{http_code}\n' -X POST -H 'content-type: application/json' -d '{}' http://$ip:$PEER_PORT/raft/vote")"
    evidence "$out"
    assert_has "B invite holder: POST /raft/vote on $n is 403" "$out" '^http=403$'
    assert_has "B invite holder: POST /raft/vote on $n says forbidden" "$out" 'forbidden'
    for route in append snapshot; do
      out="$(dx "$NODE_C" "curl -sS --max-time 5 -o /dev/null -w 'http=%{http_code}' -X POST -H 'content-type: application/json' -d '{}' http://$ip:$PEER_PORT/raft/$route")"
      assert_eq "B invite holder: POST /raft/$route on $n is 403" "$out" "http=403"
    done
  done
  assert_eq "B invite holder: the mesh still has 2 voters and a leader" \
    "$(voters "$NODE_A") $(dx "$NODE_A" 'faber cluster status | grep -c "^leader: *[0-9]"')" "2 1"

  dx "$NODE_C" 'ip link del e2einv' >/dev/null
  # Later phases need $NODE_C to be a stranger again.
  if wait_for 90 "$NODE_A" "! nft list set inet faber peer_endpoints | grep -qF '$IP_C'"; then
    pass "B invite holder: once the invite expired, $NODE_A's firewall dropped $IP_C again"
  else
    fail "B invite holder: once the invite expired, $NODE_A's firewall dropped $IP_C again" "$(dx "$NODE_A" 'nft list set inet faber peer_endpoints')"
  fi
}

# --- phase H: container network ---------------------------------------------

# node_subnet NODE: the subnet in NODE's CNI config, or nothing.
node_subnet() {
  dx "$1" "grep -o '\"subnet\": *\"[^\"]*\"' $CNI_CONFLIST 2>/dev/null | grep -Eo '[0-9]+(\.[0-9]+){3}/[0-9]+' | head -n 1"
}

# in_subnet24 IP SUBNET: IP is an address of the /24 SUBNET.
in_subnet24() {
  [[ "$2" == */24 && "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ && "${1%.*}" == "${2%.*}" ]]
}

# mesh_ip NODE: the address on NODE's faber0.
mesh_ip() {
  dx "$1" 'ip -o -4 addr show dev faber0' | awk '{ split($4, a, "/"); print a[1] }'
}

# allowed_ips NODE PUBKEY: the AllowedIPs NODE has for that peer, sorted, each
# followed by a space.
allowed_ips() {
  dx "$1" 'wg show faber0 allowed-ips' |
    awk -v k="$2" '$1 == k { for (i = 2; i <= NF; i++) print $i }' | sort | tr '\n' ' '
}

# forward_rules NODE: the rules of the forward chain of 'inet faber', one per
# line, without indentation. nft versions print the ct states in two ways.
forward_rules() {
  dx "$1" 'nft list chain inet faber forward' |
    sed -e 's/^[[:space:]]*//' -e 's/{ established, related }/established,related/' |
    grep -v -e '^table ' -e '^chain ' -e '^type ' -e '^}' -e '^$'
}

# install_crictl NODE: download the pinned crictl into NODE, check it against
# the published sha256 and point it at containerd. Prints its version.
install_crictl() {
  dx "$1" "set -e
    if ! command -v crictl >/dev/null 2>&1; then
      case \"\$(uname -m)\" in
        x86_64) arch=amd64 sum=$CRICTL_SHA256_AMD64 ;;
        aarch64) arch=arm64 sum=$CRICTL_SHA256_ARM64 ;;
        *) echo \"no pinned crictl for \$(uname -m)\"; exit 1 ;;
      esac
      f=crictl-$CRICTL_VERSION-linux-\$arch.tar.gz
      curl -fsSL --retry 3 -o /tmp/\$f $CRICTL_URL/$CRICTL_VERSION/\$f
      echo \"\$sum  /tmp/\$f\" | sha256sum -c - >/dev/null
      tar -C /usr/local/bin -xzf /tmp/\$f crictl
      rm -f /tmp/\$f
    fi
    printf 'runtime-endpoint: unix:///run/containerd/containerd.sock\nimage-endpoint: unix:///run/containerd/containerd.sock\ntimeout: 120\n' > /etc/crictl.yaml
    crictl --version"
}

# container_node_checks TAG NODE SUBNET [PEER_MESH_IP]: what fabercp set up on
# NODE for its container subnet, and what containerd makes of it.
container_node_checks() {
  local t="$1" n="$2" subnet="$3" peer_mesh="${4:-}" out rc cond
  local cidr_re="${CONTAINER_CIDR//./\\.}"

  wait_for 30 "$n" "grep -q '\"subnet\"' $CNI_CONFLIST"
  assert_eq "$t $n: $CNI_CONFLIST is faber:faber 0644" "$(dx "$n" "stat -c '%U:%G %a' $CNI_CONFLIST")" "faber:faber 644"
  out="$(dx "$n" "cat $CNI_CONFLIST")"
  printf '%s\n' "$out" >"$LOG_DIR/conflist-$n.log"
  evidence "$(grep -E '"(cniVersion|name|type|bridge|ipMasq|mtu|subnet|dataDir)"' <<<"$out")"
  assert_eq "$t $n: the CNI config holds this node's subnet" "$(node_subnet "$n")" "$subnet"
  assert_has "$t $n: the CNI config uses the bridge faberbr0" "$out" '"bridge": "faberbr0"'

  out="$(dx "$n" 'ip route show dev faber0')"
  evidence "$out"
  assert_has "$t $n: the container range $CONTAINER_CIDR is routed through faber0" "$out" "^$cidr_re( |\$)"

  out="$(dx "$n" 'nft list set inet faber node_mesh_ips')"
  evidence "$out"
  if [[ -n "$peer_mesh" ]]; then
    assert_has "$t $n: nft node_mesh_ips has the other node's mesh IP $peer_mesh" "$out" "elements = \{[^}]*\b${peer_mesh//./\\.}\b"
  else
    assert_has "$t $n: nft has the set node_mesh_ips" "$out" 'set node_mesh_ips \{'
  fi
  # fabercp loads the table with 'nft -f -'. It has done so with the current
  # rules if the listed chain is the expected one and nothing was refused.
  out="$(dx "$n" 'journalctl -u fabercp --no-pager -o cat | grep -iE "nft|nftables" | grep -iE "error|fail|syntax" | cut -c1-300 | tail -n 4')"
  assert_eq "$t $n: fabercp's journal has no nft load error" "$out" ""
  out="$(forward_rules "$n")"
  evidence "$out"
  assert_eq "$t $n: the forward chain is the two bridge rules and the nine container rules for $subnet, in order" "$out" \
    "iifname \"faberbr0\" oifname != { \"faber0\", \"faberbr0\" } drop
oifname \"faberbr0\" iifname != { \"faber0\", \"faberbr0\" } drop
iifname \"faber0\" oifname \"faber0\" drop
iifname \"faber0\" ct state established,related accept
oifname \"faber0\" ct state established,related accept
ip saddr $subnet ip daddr != $CONTAINER_CIDR drop
iifname \"faber0\" oifname \"faberbr0\" ip saddr @node_mesh_ips ip daddr $subnet accept
iifname \"faber0\" oifname \"faberbr0\" ip saddr $CONTAINER_CIDR ip daddr $subnet accept
iifname \"faberbr0\" oifname \"faber0\" ip saddr $subnet ip daddr $CONTAINER_CIDR accept
oifname \"faber0\" drop
iifname \"faber0\" drop"

  out="$(install_crictl "$n")"
  rc=$?
  evidence "$out"
  assert_ok "$t $n: crictl $CRICTL_VERSION downloaded, sha256 as published" "$rc"
  # The condition is one JSON object; its key order differs between versions.
  wait_for 60 "$n" 'crictl info 2>/dev/null | tr -d " \n" | grep -o "{[^{}]*\"type\":\"NetworkReady\"[^{}]*}" | grep -q "\"status\":true"'
  out="$(dx "$n" 'crictl info')"
  printf '%s\n' "$out" >"$LOG_DIR/crictl-info-$n.log"
  cond="$(tr -d ' \n' <<<"$out" | grep -o '{[^{}]*"type":"NetworkReady"[^{}]*}')"
  evidence "$cond"
  assert_has "$t $n: crictl info reports the network ready ($(dx "$n" 'containerd --version' | awk '{ print $1, $3 }'))" "$cond" '"status":true'
}

# run_sandbox TAG NODE NAME: start a pod sandbox with crictl. Sets POD_ID,
# POD_IP and POD_NETNS; returns 1 when the sandbox did not start. The pod
# names a cgroup parent that is a slice, as the kubelet does: fabercp sets
# SystemdCgroup, and runc then refuses a sandbox without one.
run_sandbox() {
  local t="$1" n="$2" name="$3" out rc
  POD_ID=""
  POD_IP=""
  POD_NETNS=""
  dx "$n" "mkdir -p /tmp/e2e-pod && printf '%s\n' '{\"metadata\":{\"name\":\"$name\",\"namespace\":\"e2e\",\"uid\":\"$name-uid\",\"attempt\":1},\"log_directory\":\"/tmp/e2e-pod/$name\",\"linux\":{\"cgroup_parent\":\"e2epods.slice\"}}' > /tmp/e2e-pod/$name.json" >/dev/null
  out="$(dx "$n" "crictl runp /tmp/e2e-pod/$name.json")"
  rc=$?
  printf '%s\n' "$out" >"$LOG_DIR/crictl-runp-$n.log"
  if [[ "$rc" != 0 ]]; then
    fail "$t $n: crictl runp starts a pod sandbox (pause image pulled by containerd)" "$out"
    return 1
  fi
  POD_ID="$(tail -n 1 <<<"$out")"
  pass "$t $n: crictl runp starts a pod sandbox (pause image pulled by containerd) [${POD_ID:0:13}]"
  dx "$n" "crictl inspectp $POD_ID" >"$LOG_DIR/crictl-inspectp-$n.log"
  POD_IP="$(dx "$n" "crictl inspectp -o go-template --template '{{.status.network.ip}}' $POD_ID")"
  POD_NETNS="$(dx "$n" "crictl inspectp -o go-template --template '{{range .info.runtimeSpec.linux.namespaces}}{{if eq .type \"network\"}}{{.path}}{{end}}{{end}}' $POD_ID")"
  info "$n: pod $name has IP $POD_IP, network namespace $POD_NETNS"
  SANDBOX_PATH="real CRI sandboxes (crictl runp); the manual CNI fallback was not needed"
}

# remove_sandbox NODE POD_ID
remove_sandbox() {
  [[ -n "$2" ]] || return 0
  dx "$1" "crictl stopp $2 && crictl rmp $2" >/dev/null
}

phase_h() {
  local out rc gw pub_a pub_b pod_a="" ip_a="" ns_a="" pod_b="" ip_b="" ns_b=""
  section "H: container network between $NODE_A and $NODE_B"
  gw="$(docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' "$NET" | tr -d '\r')"

  container_node_checks "H" "$NODE_A" "$SUBNET_A" "$MESH_B"
  container_node_checks "H" "$NODE_B" "$SUBNET_B" "$MESH_A"

  pub_a="$(dx "$NODE_A" 'wg show faber0 public-key')"
  pub_b="$(dx "$NODE_B" 'wg show faber0 public-key')"
  wait_for 30 "$NODE_A" "wg show faber0 allowed-ips | grep -qF '$SUBNET_B'"
  wait_for 30 "$NODE_B" "wg show faber0 allowed-ips | grep -qF '$SUBNET_A'"
  evidence "$(dx "$NODE_A" 'wg show faber0 allowed-ips')"
  assert_eq "H $NODE_A: WireGuard AllowedIPs for B are its subnet and its mesh /32" "$(allowed_ips "$NODE_A" "$pub_b")" "$SUBNET_B $MESH_B/32 "
  evidence "$(dx "$NODE_B" 'wg show faber0 allowed-ips')"
  assert_eq "H $NODE_B: WireGuard AllowedIPs for A are its subnet and its mesh /32" "$(allowed_ips "$NODE_B" "$pub_a")" "$SUBNET_A $MESH_A/32 "

  if run_sandbox "H" "$NODE_A" e2e-pod-a; then
    pod_a="$POD_ID" ip_a="$POD_IP" ns_a="$POD_NETNS"
    if in_subnet24 "$ip_a" "$SUBNET_A"; then pass "H pod on $NODE_A has an address in $SUBNET_A [$ip_a]"; else fail "H pod on $NODE_A has an address in $SUBNET_A" "pod IP: $ip_a"; fi
  fi
  if run_sandbox "H" "$NODE_B" e2e-pod-b; then
    pod_b="$POD_ID" ip_b="$POD_IP" ns_b="$POD_NETNS"
    if in_subnet24 "$ip_b" "$SUBNET_B"; then pass "H pod on $NODE_B has an address in $SUBNET_B [$ip_b]"; else fail "H pod on $NODE_B has an address in $SUBNET_B" "pod IP: $ip_b"; fi
  fi
  if [[ -z "$ns_a" || -z "$ns_b" || -z "$ip_a" || -z "$ip_b" ]]; then
    fail "H pod sandboxes on both nodes, needed for the traffic checks" "pod A: id '$pod_a' ip '$ip_a' netns '$ns_a'"$'\n'"pod B: id '$pod_b' ip '$ip_b' netns '$ns_b'"
    remove_sandbox "$NODE_A" "$pod_a"
    remove_sandbox "$NODE_B" "$pod_b"
    return 0
  fi
  evidence "$(dx "$NODE_A" 'ip -o -4 addr show dev faberbr0; ip route show dev faberbr0')"

  # A probe chain on A that runs after 'inet faber' counts what the forward
  # chain lets through from A's containers.
  dx "$NODE_A" "nft -f - <<'EOF'
table inet e2eegress {
  chain forward {
    type filter hook forward priority 50; policy accept;
    ip saddr $SUBNET_A ip daddr != $CONTAINER_CIDR counter comment \"egress\"
    ip saddr $SUBNET_A ip daddr $SUBNET_B counter comment \"pod-to-pod\"
  }
}
EOF" >/dev/null

  # The first packet waits for ARP on both bridges.
  wait_for 15 "$NODE_A" "nsenter --net=$ns_a ping -c 1 -W 1 $ip_b"
  out="$(dx "$NODE_A" "nsenter --net=$ns_a ping -c 3 -W 2 $ip_b")"
  evidence "$(tail -n 3 <<<"$out")"
  assert_has "H pod on $NODE_A ($ip_a) pings the pod on $NODE_B ($ip_b)" "$out" ' 0% packet loss'
  out="$(dx "$NODE_B" "nsenter --net=$ns_b ping -c 3 -W 2 $ip_a")"
  assert_has "H pod on $NODE_B ($ip_b) pings the pod on $NODE_A ($ip_a)" "$out" ' 0% packet loss'

  out="$(dx "$NODE_A" "ping -c 3 -W 2 $ip_b")"
  evidence "$(tail -n 3 <<<"$out")"
  assert_has "H $NODE_A itself pings the pod on $NODE_B" "$out" ' 0% packet loss'
  out="$(dx "$NODE_A" "ip route get $ip_b")"
  evidence "$out"
  assert_has "H $NODE_A reaches that pod through faber0 from its mesh IP $MESH_A" "$out" "dev faber0 src ${MESH_A//./\\.}( |\$)"

  # Containers do not reach a node's mesh IP on another server, and egress is
  # off. curl exit 28 is "timed out": the forward chain dropped the SYN.
  dxq "$NODE_A" "nsenter --net=$ns_a curl -sS --max-time 4 -o /dev/null http://$MESH_B:$PEER_PORT/api/v1/health"
  rc=$?
  assert_eq "H pod on $NODE_A cannot connect to B's mesh IP $MESH_B:$PEER_PORT (curl 28 = dropped)" "$rc" "28"
  out="$(dx "$NODE_A" "curl -sS --max-time 5 -o /dev/null -w 'http=%{http_code}' http://$MESH_B:$PEER_PORT/api/v1/health")"
  assert_eq "H control: $NODE_A itself does reach $MESH_B:$PEER_PORT" "$out" "http=200"
  out="$(dx "$NODE_A" "nsenter --net=$ns_a ping -c 2 -W 2 $gw; echo \"rc=\$?\"")"
  evidence "$(tail -n 3 <<<"$out")"
  assert_has "H pod on $NODE_A cannot ping the Docker network gateway $gw (egress is off)" "$out" '^rc=[1-9]'
  out="$(dx "$NODE_A" "ping -c 2 -W 2 $gw")"
  assert_has "H control: $NODE_A itself does ping the gateway $gw" "$out" ' 0% packet loss'
  out="$(dx "$NODE_A" "nsenter --net=$ns_a ping -c 2 -W 2 $IP_C; echo \"rc=\$?\"")"
  assert_has "H pod on $NODE_A cannot ping $NODE_C ($IP_C) on the Docker network" "$out" '^rc=[1-9]'
  out="$(dx "$NODE_A" "ping -c 2 -W 2 $IP_C")"
  assert_has "H control: $NODE_A itself does ping $NODE_C" "$out" ' 0% packet loss'

  out="$(dx "$NODE_A" 'nft list table inet e2eegress')"
  evidence "$(grep counter <<<"$out")"
  assert_has "H probe behind 'inet faber': 0 packets from A's containers left the container range" "$out" 'counter packets 0 bytes 0 comment "egress"'
  assert_has "H probe control: pod-to-pod packets do pass the forward chain" "$out" 'counter packets [1-9][0-9]* bytes [0-9]+ comment "pod-to-pod"'
  dx "$NODE_A" 'nft delete table inet e2eegress' >/dev/null

  # --- what the two bridge rules close ---
  # A second probe table on A. The prerouting chain runs before routing and
  # before 'inet faber', so it shows that a packet reached A. The forward
  # chain runs after 'inet faber', so it shows what that table let through.
  local v6_note v6_tested lan_forged="${SUBNET_A%.*}.77" forged="192.0.2.99" v6_a="fd00:e2e:a::a" v6_c="fd00:e2e:a::c" v6_br="fd00:e2e:b::1" v6_pod="fd00:e2e:b::2"
  dx "$NODE_A" "nft -f - <<'EOF'
table inet e2eleak {
  chain prerouting {
    type filter hook prerouting priority -300; policy accept;
    ip saddr $IP_C ip daddr $SUBNET_A counter comment \"lan-arrived\"
    iifname != \"faberbr0\" ip saddr $lan_forged ip daddr $SUBNET_B counter comment \"lan-forged-arrived\"
    iifname \"faberbr0\" ip saddr $forged counter comment \"forged-arrived\"
    iifname \"faberbr0\" ip6 saddr $v6_pod ip6 daddr $v6_c counter comment \"v6-arrived\"
  }
  chain forward {
    type filter hook forward priority 50; policy accept;
    ip saddr $IP_C ip daddr $SUBNET_A counter comment \"lan-forwarded\"
    ip saddr $lan_forged counter comment \"lan-forged-forwarded\"
    ip saddr $forged counter comment \"forged-forwarded\"
    ip6 saddr $v6_pod ip6 daddr $v6_c counter comment \"v6-forwarded\"
  }
}
EOF" >/dev/null

  # 1. A machine on the Docker network that is no mesh node routes A's
  # container subnet through A.
  dx "$NODE_C" "ip route replace $SUBNET_A via $IP_A" >/dev/null
  out="$(dx "$NODE_C" "ping -c 3 -W 1 $ip_a; echo \"rc=\$?\"")"
  evidence "$(tail -n 3 <<<"$out")"
  assert_has "H leak: $NODE_C, no mesh node, routing $SUBNET_A via $NODE_A cannot ping the pod on $NODE_A" "$out" '^rc=[1-9]'
  dx "$NODE_C" "ip route del $SUBNET_A" >/dev/null

  # 1b. The same machine sends to the pod on B through A, from an address it
  # made up inside A's container subnet. Only A's bridge may be the source of
  # such packets. A counter on B shows whether any came out of the tunnel.
  dx "$NODE_B" "nft -f - <<'EOF'
table inet e2eleak {
  chain prerouting {
    type filter hook prerouting priority -300; policy accept;
    ip saddr $lan_forged counter comment \"lan-forged-at-b\"
  }
}
EOF" >/dev/null
  dx "$NODE_C" "ip addr add $lan_forged/32 dev eth0; ip route replace $CONTAINER_CIDR via $IP_A" >/dev/null
  out="$(dx "$NODE_C" "ping -c 3 -W 1 -I $lan_forged $ip_b; echo \"rc=\$?\"")"
  evidence "$(tail -n 3 <<<"$out")"
  assert_has "H leak: $NODE_C, sending from $lan_forged (forged, in A's subnet) through $NODE_A, cannot ping the pod on $NODE_B" "$out" '^rc=[1-9]'
  dx "$NODE_C" "ip route del $CONTAINER_CIDR; ip addr del $lan_forged/32 dev eth0" >/dev/null
  out="$(dx "$NODE_B" 'nft list table inet e2eleak')"
  evidence "$(grep counter <<<"$out")"
  assert_has "H leak: none of those packets came out of the tunnel on $NODE_B" "$out" 'counter packets 0 bytes 0 comment "lan-forged-at-b"'
  dx "$NODE_B" 'nft delete table inet e2eleak' >/dev/null

  # 2. The pod sends from an address outside its subnet, which the egress
  # rule does not match.
  dx "$NODE_A" "nsenter --net=$ns_a ip addr add $forged/32 dev eth0" >/dev/null
  out="$(dx "$NODE_A" "nsenter --net=$ns_a ping -c 3 -W 1 -I $forged $gw; echo \"rc=\$?\"")"
  evidence "$(tail -n 3 <<<"$out")"
  assert_has "H leak: the pod on $NODE_A, sending from the forged source $forged, cannot ping the gateway $gw" "$out" '^rc=[1-9]'
  dx "$NODE_A" "nsenter --net=$ns_a ip addr del $forged/32 dev eth0" >/dev/null
  info "$NODE_A rp_filter all/faberbr0: $(dx "$NODE_A" 'cat /proc/sys/net/ipv4/conf/all/rp_filter /proc/sys/net/ipv4/conf/faberbr0/rp_filter' | tr '\n' ' ')(0 off, 1 strict, 2 loose; only strict would stop the forged packets before the forward chain)"

  # 3. IPv6. The Docker network has none, so the test gives A and C an
  # address on it, and the pod and the bridge one each, by hand.
  v6_note="$(dx "$NODE_A" "set -e
    sysctl -qw net.ipv6.conf.eth0.disable_ipv6=0 net.ipv6.conf.faberbr0.disable_ipv6=0
    ip -6 addr add $v6_a/64 dev eth0 nodad
    ip -6 addr add $v6_br/64 dev faberbr0 nodad
    nsenter --net=$ns_a sysctl -qw net.ipv6.conf.eth0.disable_ipv6=0
    nsenter --net=$ns_a ip -6 addr add $v6_pod/64 dev eth0 nodad
    nsenter --net=$ns_a ip -6 route replace default via $v6_br dev eth0
    echo forwarding=\$(cat /proc/sys/net/ipv6/conf/all/forwarding)")"
  dx "$NODE_C" "sysctl -qw net.ipv6.conf.eth0.disable_ipv6=0
    ip -6 addr add $v6_c/64 dev eth0 nodad
    ip -6 route replace fd00:e2e:b::/64 via $v6_a dev eth0" >/dev/null
  if [[ "$v6_note" == "forwarding=1" ]] && wait_for 10 "$NODE_A" "ping -6 -c 1 -W 1 $v6_c" &&
    wait_for 10 "$NODE_A" "nsenter --net=$ns_a ping -6 -c 1 -W 1 $v6_br"; then
    pass "H leak control (IPv6 set up by the test): $NODE_A pings $NODE_C at $v6_c, the pod pings its bridge at $v6_br, and A forwards IPv6"
    out="$(dx "$NODE_A" "nsenter --net=$ns_a ping -6 -c 3 -W 1 $v6_c; echo \"rc=\$?\"")"
    evidence "$(tail -n 3 <<<"$out")"
    assert_has "H leak: the pod on $NODE_A cannot ping $NODE_C over IPv6" "$out" '^rc=[1-9]'
    v6_tested=1
  else
    info "IPv6 was not testable here: $v6_note; $(dx "$NODE_A" "ping -6 -c 1 -W 1 $v6_c 2>&1 | tail -n 1")"
    v6_tested=0
  fi
  dx "$NODE_C" "ip -6 route del fd00:e2e:b::/64; ip -6 addr del $v6_c/64 dev eth0" >/dev/null
  dx "$NODE_A" "ip -6 addr del $v6_a/64 dev eth0; ip -6 addr del $v6_br/64 dev faberbr0" >/dev/null

  out="$(dx "$NODE_A" 'nft list table inet e2eleak')"
  evidence "$(grep counter <<<"$out")"
  assert_has "H leak control: the pings from $NODE_C did reach $NODE_A" "$out" 'counter packets [1-9][0-9]* bytes [0-9]+ comment "lan-arrived"'
  assert_has "H leak: 'inet faber' forwarded none of them to the bridge" "$out" 'counter packets 0 bytes 0 comment "lan-forwarded"'
  assert_has "H leak control: the packets $NODE_C sent from $lan_forged did reach $NODE_A" "$out" 'counter packets [1-9][0-9]* bytes [0-9]+ comment "lan-forged-arrived"'
  assert_has "H leak: 'inet faber' forwarded none of them into faber0" "$out" 'counter packets 0 bytes 0 comment "lan-forged-forwarded"'
  assert_has "H leak control: the forged-source packets did reach $NODE_A from the bridge" "$out" 'counter packets [1-9][0-9]* bytes [0-9]+ comment "forged-arrived"'
  assert_has "H leak: 'inet faber' forwarded none of the forged-source packets" "$out" 'counter packets 0 bytes 0 comment "forged-forwarded"'
  if [[ "$v6_tested" == 1 ]]; then
    assert_has "H leak control: the pod's IPv6 packets did reach $NODE_A from the bridge" "$out" 'counter packets [1-9][0-9]* bytes [0-9]+ comment "v6-arrived"'
    assert_has "H leak: 'inet faber' forwarded none of the pod's IPv6 packets" "$out" 'counter packets 0 bytes 0 comment "v6-forwarded"'
  fi
  dx "$NODE_A" 'nft delete table inet e2eleak' >/dev/null

  remove_sandbox "$NODE_A" "$pod_a"
  remove_sandbox "$NODE_B" "$pod_b"
}

# --- phase J: projects, deploys and workloads -------------------------------

J_DIR="/root/e2e-j"

# j_put NODE NAME: write stdin to $J_DIR/NAME in NODE.
j_put() { docker exec -i "$1" bash -c "mkdir -p $J_DIR && cat >$J_DIR/$2"; }

# j_gen NODE ENVIRONMENT: the generation of that environment of project shop,
# as NODE has it.
j_gen() { dx "$1" "faber project show shop | awk -v e=$2 '\$1 == e { print \$3 }'"; }

# j_compose TAG: the compose file of this phase, with nginx:TAG for web.
j_compose() {
  cat <<EOF
services:
  web:
    image: nginx:$1
    ports: ["80"]
    environment:
      GREETING: hello
  api:
    image: ghcr.io/example/api:1
    command: ["serve"]
    deploy:
      replicas: 2
EOF
}

phase_j() {
  local out rc gen gen2 n before
  section "J: projects, deploys and workloads (records only, nothing runs)"

  # Projects.
  out="$(dx "$NODE_A" 'faber project create shop')"
  rc=$?
  evidence "$out"
  assert_ok "J $NODE_A: faber project create shop exits 0" "$rc" "$out"
  assert_has "J project create prints the project" "$out" '^name: +shop$'
  assert_has "J project shop has the environment staging" "$out" '^ +staging +0 +[0-9]+ '
  assert_has "J project shop has the environment production" "$out" '^ +production +0 +[0-9]+ '
  wait_for 10 "$NODE_B" 'faber project ls | grep -q "^shop "'
  out="$(dx "$NODE_B" 'faber project ls')"
  evidence "$out"
  assert_has "J $NODE_B: faber project ls shows shop with both environments and no workload" "$out" '^shop +production, staging +0 '
  out="$(dx "$NODE_B" 'faber project create shop')"
  rc=$?
  assert_fails "J $NODE_B: a second project create shop fails" "$rc" "$out"
  assert_has "J the second create answers project_exists (forwarded from the leader)" "$out" '^error: project_exists'

  # A compose file, deployed on the leader.
  j_compose 1.27 | j_put "$NODE_A" compose.yaml
  out="$(dx "$NODE_A" "faber deploy compose $J_DIR/compose.yaml --project shop --env staging")"
  rc=$?
  evidence "$out"
  assert_ok "J $NODE_A: faber deploy compose exits 0" "$rc" "$out"
  assert_has "J deploy compose created api and web" "$out" '^created: +api, web$'
  assert_has "J deploy compose prints the generation" "$out" '^generation: +[0-9]+$'
  assert_has "J deploy compose notes that containers get no capabilities" "$out" '^note: .*capabilities'
  wait_for 10 "$NODE_B" 'faber app ls --project shop | grep -q "^web "'
  out="$(dx "$NODE_B" 'faber app ls')"
  evidence "$out"
  assert_has "J $NODE_B: faber app ls names web.staging.shop.faber.internal" "$out" '^web +shop +staging +1 +environment +web\.staging\.shop\.faber\.internal$'
  assert_has "J $NODE_B: faber app ls names api.staging.shop.faber.internal with scale 2" "$out" '^api +shop +staging +2 +environment +api\.staging\.shop\.faber\.internal$'
  assert_eq "J both nodes have the same generation of staging" "$(j_gen "$NODE_B" staging)" "$(j_gen "$NODE_A" staging)"

  # A changed file, deployed on the follower. B forwards the write and has
  # applied it by the time the command returns.
  gen="$(j_gen "$NODE_B" staging)"
  j_compose 1.28 | j_put "$NODE_B" compose.yaml
  out="$(dx "$NODE_B" "faber deploy compose $J_DIR/compose.yaml --project shop --env staging && faber -o json app show web --project shop --env staging")"
  rc=$?
  evidence "$out"
  assert_ok "J $NODE_B: deploy of the changed file (forwarded) exits 0" "$rc" "$out"
  assert_has "J the changed deploy updated web" "$out" '^updated: +web$'
  assert_has "J the changed deploy left api unchanged" "$out" '^unchanged: +api$'
  assert_has "J $NODE_B reads the new image back at once" "$out" '"image": "nginx:1\.28"'
  gen2="$(j_gen "$NODE_B" staging)"
  assert_ne "J the generation of staging moved (was $gen)" "$gen2" "$gen"
  out="$(dx "$NODE_B" "faber deploy compose $J_DIR/compose.yaml --project shop --env staging")"
  assert_has "J the same file again changes nothing" "$out" '^unchanged: +api, web$'
  assert_eq "J the same file again keeps the generation" "$(j_gen "$NODE_B" staging)" "$gen2"
  gen="$gen2"

  # --dry-run sends nothing.
  j_compose 1.29 | j_put "$NODE_B" dry.yaml
  dx "$NODE_B" "cd $J_DIR && faber deploy compose dry.yaml --project shop --env staging --dry-run >dry.json 2>dry.err" >/dev/null
  rc=$?
  assert_ok "J --dry-run exits 0" "$rc"
  out="$(dx "$NODE_B" "cat $J_DIR/dry.json")"
  assert_has "J --dry-run prints the converted workloads on stdout" "$out" '"image": "nginx:1\.29"'
  out="$(dx "$NODE_B" "cat $J_DIR/dry.err")"
  evidence "$out"
  assert_has "J --dry-run lists the file it read on stderr" "$out" '^read: .*dry\.yaml$'
  assert_eq "J --dry-run keeps the generation" "$(j_gen "$NODE_B" staging)" "$gen"
  out="$(dx "$NODE_A" 'faber -o json app show web --project shop --env staging')"
  assert_has "J --dry-run changed no workload" "$out" '"image": "nginx:1\.28"'

  # A file that cannot be converted, and a warning under --strict.
  j_put "$NODE_A" build.yaml <<'EOF'
services:
  web:
    image: nginx:1.30
    build: .
EOF
  out="$(dx "$NODE_A" "faber deploy compose $J_DIR/build.yaml --project shop --env staging")"
  rc=$?
  evidence "$out"
  assert_eq "J a compose file with build: exits 1" "$rc" "1"
  assert_has "J the error names services.web.build" "$out" '^error: .*services\.web\.build'
  assert_has "J the error says nothing was deployed" "$out" '^error: nothing was deployed$'
  assert_eq "J the refused file keeps the generation" "$(j_gen "$NODE_A" staging)" "$gen"

  j_put "$NODE_A" strict.yaml <<'EOF'
services:
  web:
    image: nginx:1.30
    depends_on: [api]
  api:
    image: ghcr.io/example/api:1
EOF
  out="$(dx "$NODE_A" "faber deploy compose $J_DIR/strict.yaml --project shop --env staging --strict")"
  rc=$?
  evidence "$out"
  assert_eq "J --strict with a warning exits 1" "$rc" "1"
  assert_has "J --strict prints the warning" "$out" '^warning: .*services\.web\.depends_on'
  assert_has "J --strict says nothing was deployed" "$out" '^error: nothing was deployed: --strict'
  assert_eq "J --strict keeps the generation" "$(j_gen "$NODE_A" staging)" "$gen"
  out="$(dx "$NODE_A" 'faber -o json app show web --project shop --env staging')"
  assert_has "J neither refused file changed web" "$out" '"image": "nginx:1\.28"'

  # Capabilities: one outside the allowed list is refused, one inside it is
  # stored. The workload edge is removed again, so staging ends as it was.
  j_put "$NODE_A" cap-refused.yaml <<'EOF'
services:
  edge:
    image: nginx:1.28
    cap_add: [SYS_ADMIN]
EOF
  out="$(dx "$NODE_A" "faber deploy compose $J_DIR/cap-refused.yaml --project shop --env staging")"
  rc=$?
  evidence "$out"
  assert_eq "J a compose file with cap_add: [SYS_ADMIN] exits 1" "$rc" "1"
  assert_has "J the error says SYS_ADMIN is not allowed" "$out" '^error: .*cap_add.*`SYS_ADMIN` is not allowed'
  assert_has "J the SYS_ADMIN file deployed nothing" "$out" '^error: nothing was deployed$'
  assert_eq "J the SYS_ADMIN file keeps the generation" "$(j_gen "$NODE_A" staging)" "$gen"
  out="$(dx "$NODE_A" 'faber app ls --project shop --env staging')"
  assert_hasnt "J the SYS_ADMIN file made no workload" "$out" '^edge '

  j_put "$NODE_A" cap-allowed.yaml <<'EOF'
services:
  edge:
    image: nginx:1.28
    cap_add: [NET_BIND_SERVICE]
EOF
  out="$(dx "$NODE_A" "faber deploy compose $J_DIR/cap-allowed.yaml --project shop --env staging")"
  rc=$?
  evidence "$out"
  assert_ok "J a compose file with cap_add: [NET_BIND_SERVICE] exits 0" "$rc" "$out"
  assert_has "J the NET_BIND_SERVICE file created edge" "$out" '^created: +edge$'
  out="$(dx "$NODE_A" 'faber app show edge --project shop --env staging')"
  evidence "$out"
  assert_has "J app show edge lists the capability" "$out" 'capabilities: +NET_BIND_SERVICE'
  out="$(dx "$NODE_A" 'faber app rm edge --project shop --env staging')"
  assert_has "J app rm prints removed edge.staging.shop.faber.internal" "$out" '^removed edge\.staging\.shop\.faber\.internal$'
  wait_for 10 "$NODE_B" '! faber app ls --project shop --env staging | grep -q "^edge "'
  gen="$(j_gen "$NODE_A" staging)"
  assert_eq "J both nodes have the same generation of staging after edge" "$(j_gen "$NODE_B" staging)" "$gen"

  # --prune with every service behind a profile that is not active.
  j_put "$NODE_A" profiles.yaml <<'EOF'
services:
  web:
    image: nginx:1.30
    profiles: [debug]
  api:
    image: ghcr.io/example/api:1
    profiles: [debug]
EOF
  out="$(dx "$NODE_A" "faber deploy compose $J_DIR/profiles.yaml --project shop --env staging --prune")"
  rc=$?
  evidence "$out"
  assert_eq "J --prune with every service behind an inactive profile exits 1" "$rc" "1"
  assert_has "J the error names --prune and what would be removed" "$out" '^error: --prune: .*removed'
  assert_has "J the --prune refusal deployed nothing" "$out" '^error: nothing was deployed$'
  assert_eq "J the --prune refusal keeps the generation" "$(j_gen "$NODE_A" staging)" "$gen"
  out="$(dx "$NODE_A" 'faber app ls --project shop --env staging')"
  evidence "$out"
  assert_has "J the --prune refusal left web" "$out" '^web +shop +staging '
  assert_has "J the --prune refusal left api" "$out" '^api +shop +staging '

  # A variable the file takes from the environment of faber is named, and
  # named before the request is sent.
  j_compose '${J_TAG}' | j_put "$NODE_B" var.yaml
  out="$(dx "$NODE_B" "J_TAG=1.28 faber deploy compose $J_DIR/var.yaml --project shop --env staging")"
  rc=$?
  evidence "$out"
  assert_ok "J a compose file with an interpolated variable exits 0" "$rc" "$out"
  assert_has "J the deploy names the variable it used" "$out" '^uses variable: J_TAG$'
  assert_hasnt "J the deploy does not print the value of the variable" "$out" '^uses variable: .*1\.28'
  assert_has "J the variable gave the image that is deployed already" "$out" '^unchanged: +api, web$'
  assert_eq "J the variable is named before the result" \
    "$(grep -nE '^(uses variable|generation):' <<<"$out" | cut -d: -f2 | tr '\n' ' ')" \
    "uses variable generation "
  assert_eq "J the unchanged deploy keeps the generation" "$(j_gen "$NODE_B" staging)" "$gen"

  # A Kubernetes manifest into the other environment.
  j_put "$NODE_A" k8s.yaml <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cache
spec:
  replicas: 2
  selector:
    matchLabels: {app: cache}
  template:
    metadata:
      labels: {app: cache}
    spec:
      containers:
        - name: redis
          image: redis:7
          ports:
            - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: cache
spec:
  selector: {app: cache}
  ports:
    - port: 6379
EOF
  out="$(dx "$NODE_A" "faber deploy k8s $J_DIR/k8s.yaml --project shop --env production")"
  rc=$?
  evidence "$out"
  assert_ok "J faber deploy k8s exits 0" "$rc" "$out"
  assert_has "J deploy k8s warns that the Service is skipped" "$out" '^warning: .*Service .cache. is skipped'
  assert_has "J deploy k8s created cache" "$out" '^created: +cache$'
  wait_for 10 "$NODE_B" 'faber app ls --env production | grep -q "^cache "'
  out="$(dx "$NODE_B" 'faber app ls --project shop --env production')"
  evidence "$out"
  assert_has "J $NODE_B lists cache.production.shop.faber.internal with scale 2" "$out" '^cache +shop +production +2 +environment +cache\.production\.shop\.faber\.internal$'
  assert_hasnt "J the environment filter leaves staging out" "$out" 'staging'
  assert_eq "J the deploy to production left staging alone" "$(j_gen "$NODE_A" staging)" "$gen"

  # The --dry-run output is what deploy spec reads.
  dx "$NODE_A" "cd $J_DIR && faber deploy compose compose.yaml --project shop --env staging --dry-run >was.json 2>/dev/null" >/dev/null
  dx "$NODE_B" "cd $J_DIR && faber deploy compose compose.yaml --project shop --env staging --dry-run >spec.json 2>/dev/null" >/dev/null
  rc=$?
  assert_ok "J --dry-run of the deployed file exits 0" "$rc"
  out="$(dx "$NODE_B" "faber deploy spec $J_DIR/spec.json --project shop --env staging")"
  rc=$?
  evidence "$out"
  assert_ok "J faber deploy spec of the saved --dry-run output exits 0" "$rc" "$out"
  assert_has "J deploy spec finds both workloads unchanged" "$out" '^unchanged: +api, web$'
  assert_eq "J deploy spec keeps the generation" "$(j_gen "$NODE_B" staging)" "$gen"
  out="$(dx "$NODE_A" "faber deploy spec $J_DIR/was.json --project shop --env staging")"
  evidence "$out"
  assert_has "J deploy spec of the older file puts web back" "$out" '^updated: +web$'
  out="$(dx "$NODE_B" "faber deploy spec - --project shop --env staging <$J_DIR/spec.json")"
  assert_has "J deploy spec from stdin updates web again" "$out" '^updated: +web$'
  wait_for 10 "$NODE_A" "faber -o json app show web --project shop --env staging | grep -q 'nginx:1.28'"
  gen="$(j_gen "$NODE_B" staging)"

  # Unknown targets.
  out="$(dx "$NODE_B" "faber deploy spec $J_DIR/spec.json --project nope --env staging")"
  rc=$?
  assert_eq "J deploy to an unknown project exits 1" "$rc" "1"
  evidence "$out"
  assert_has "J deploy to an unknown project answers not_found and says what is missing" "$out" '^error: not_found: project "nope" does not exist'
  out="$(dx "$NODE_B" "faber deploy spec $J_DIR/spec.json --project shop --env nope")"
  rc=$?
  assert_eq "J deploy to an unknown environment exits 1" "$rc" "1"
  evidence "$out"
  assert_has "J deploy to an unknown environment answers not_found and says what is missing" "$out" '^error: not_found: environment "nope" of project "shop" does not exist'
  out="$(dx "$NODE_A" 'faber project ls')"
  assert_hasnt "J neither failed deploy made a project" "$out" '^nope '

  # The records survive a restart. Only B is restarted, as in phase D, so
  # that A stays the leader for the phases that follow.
  n="$NODE_B"
  before="$(main_pid "$n" fabercp)"
  dx "$n" 'systemctl restart fabercp' >/dev/null
  wait_for 30 "$n" 'systemctl is-active -q fabercp && faber health'
  wait_for 30 "$n" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 2"
  wait_for 30 "$NODE_A" "faber -o json cluster status | grep -q '\"leader_id\": [0-9]'"
  wait_for 30 "$n" "faber -o json cluster status | grep -q '\"leader_id\": [0-9]'"
  assert_ne "J $n: fabercp has a new MainPID after systemctl restart (was $before)" "$(main_pid "$n" fabercp)" "$before"
  out="$(dx "$n" 'faber app ls')"
  evidence "$out"
  assert_eq "J $n lists the same 3 workloads after the restart" \
    "$(awk 'NR > 1 { print $NF }' <<<"$out" | sort | tr '\n' ' ')" \
    "api.staging.shop.faber.internal cache.production.shop.faber.internal web.staging.shop.faber.internal "
  assert_eq "J $n has the same generation of staging after the restart" "$(j_gen "$n" staging)" "$gen"
  out="$(dx "$n" 'faber -o json app show web --project shop --env staging')"
  assert_has "J $n still has the spec of web after the restart" "$out" '"image": "nginx:1\.28"'

  # Removal.
  out="$(dx "$NODE_B" 'faber project rm shop')"
  rc=$?
  evidence "$out"
  assert_eq "J faber project rm of a project with workloads exits 1" "$rc" "1"
  assert_has "J faber project rm of a project with workloads answers not_empty" "$out" '^error: not_empty'
  out="$(dx "$NODE_B" 'faber app rm web api --project shop --env staging')"
  rc=$?
  evidence "$out"
  assert_ok "J $NODE_B: faber app rm web api exits 0" "$rc" "$out"
  assert_has "J app rm prints removed web.staging.shop.faber.internal" "$out" '^removed web\.staging\.shop\.faber\.internal$'
  assert_has "J app rm prints removed api.staging.shop.faber.internal" "$out" '^removed api\.staging\.shop\.faber\.internal$'
  out="$(dx "$NODE_B" 'faber app rm web --project shop --env staging')"
  rc=$?
  assert_eq "J faber app rm of a removed workload exits 1" "$rc" "1"
  assert_has "J faber app rm of a removed workload answers not_found" "$out" '^error: not_found'
  out="$(dx "$NODE_A" 'faber project rm shop')"
  rc=$?
  assert_eq "J faber project rm still fails while production has cache" "$rc" "1"
  assert_has "J that failure is not_empty too" "$out" '^error: not_empty'
  out="$(dx "$NODE_A" 'faber app rm cache --project shop --env production')"
  assert_has "J app rm prints removed cache.production.shop.faber.internal" "$out" '^removed cache\.production\.shop\.faber\.internal$'
  out="$(dx "$NODE_A" 'faber project rm shop')"
  rc=$?
  evidence "$out"
  assert_ok "J faber project rm of the empty project exits 0" "$rc" "$out"
  assert_has "J project rm prints removed project shop" "$out" '^removed project shop$'
  wait_for 10 "$NODE_B" '! faber project ls | grep -q "^shop "'
  out="$(dx "$NODE_B" 'faber project ls; faber app ls')"
  evidence "$out"
  assert_hasnt "J $NODE_B has no project shop and no workload of it left" "$out" 'shop'
  assert_eq "J still 2 voters at the end" "$(voters "$NODE_A")" "2"
}

# --- phase C: idempotency ---------------------------------------------------

phase_c() {
  local n logname before after cbefore cafter out
  section "C: install.sh re-run changes nothing"
  for n in "$NODE_A" "$NODE_B"; do
    logname="install-${n##*-}-2"
    before="$(unit_stamp "$n" fabercp)"
    cbefore="$(unit_stamp "$n" containerd)"
    run_install "$n" "$logname" "" --
    assert_ok "C $n: re-run exits 0" "$INSTALL_RC"
    assert_has "C $n: re-run prints 'cluster: existing'" "$INSTALL_OUT" '^cluster: existing$'
    after="$(unit_stamp "$n" fabercp)"
    cafter="$(unit_stamp "$n" containerd)"
    assert_eq "C $n: fabercp was not restarted" "$after" "$before"
    assert_eq "C $n: containerd was not restarted" "$cafter" "$cbefore"
    assert_eq "C $n: /etc/faber/join.token is gone" "$(dx "$n" '[[ -e /etc/faber/join.token ]] && echo present || echo absent')" "absent"
    out="$(dx "$n" 'cat /etc/faber/fabercp.env')"
    evidence "$out"
    assert_hasnt "C $n: fabercp.env has no bootstrap variables" "$out" '^FABER_(JOIN_TOKEN_FILE|NEW_MESH|MESH_CIDR|NODE_NAME|CONTAINER_CIDR)='
    assert_has "C $n: fabercp.env names the CNI config directory" "$out" '^FABER_CNI_CONF_DIR=/etc/cni/net\.d$'
    assert_eq "C $n: fabercp.env is root:faber 0640" "$(dx "$n" 'stat -c "%U:%G %a" /etc/faber/fabercp.env')" "root:faber 640"
    assert_eq "C $n: still 2 voters" "$(voters "$n")" "2"
  done
}

# --- phase D: restart and stop ----------------------------------------------

phase_d() {
  local out before since ms rc
  section "D: restart and stop of fabercp on $NODE_B"

  before="$(main_pid "$NODE_B" fabercp)"
  dx "$NODE_B" 'systemctl restart fabercp' >/dev/null
  wait_for 30 "$NODE_B" 'systemctl is-active -q fabercp && faber health'
  assert_eq "D fabercp is active after restart" "$(dx "$NODE_B" 'systemctl is-active fabercp')" "active"
  assert_ne "D fabercp has a new MainPID (was $before)" "$(main_pid "$NODE_B" fabercp)" "$before"
  wait_for 30 "$NODE_B" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 2"
  wait_for 30 "$NODE_A" "faber -o json cluster status | grep -q '\"leader_id\": [0-9]'"
  out="$(dx "$NODE_B" 'faber cluster status')"
  evidence "$out"
  assert_has "D $NODE_B is joined after restart" "$out" '^joined: +yes$'
  assert_eq "D $NODE_B sees 2 voters after restart" "$(voters "$NODE_B")" "2"
  assert_eq "D $NODE_A sees 2 voters after B's restart" "$(voters "$NODE_A")" "2"
  out="$(dx "$NODE_B" "ping -c 3 -W 2 -I faber0 $MESH_A")"
  assert_has "D ping $MESH_A from $NODE_B over faber0 after restart" "$out" ' 0% packet loss'
  out="$(dx "$NODE_A" "curl -sS --max-time 5 -w '\nhttp=%{http_code}' http://$MESH_B:$PEER_PORT/api/v1/health")"
  assert_has "D $NODE_A reaches B's peer API over the mesh after restart" "$out" '^http=200$'
  out="$(dx "$NODE_B" 'wg show faber0 latest-handshakes')"
  evidence "$out"
  if awk '$2 > 0 { n++ } END { exit !(n >= 1) }' <<<"$out"; then
    pass "D $NODE_B: wg handshake present after restart"
  else
    fail "D $NODE_B: wg handshake present after restart" "$out"
  fi

  since="$(dx "$NODE_B" 'date +%s')"
  sleep 1
  ms="$(dx "$NODE_B" 's=$(date +%s%N); systemctl stop fabercp; echo $(( ($(date +%s%N) - s) / 1000000 ))')"
  if [[ "$ms" =~ ^[0-9]+$ ]] && ((ms < 10000)); then
    pass "D systemctl stop fabercp returned in ${ms} ms"
  else
    fail "D systemctl stop fabercp returns within 10 s" "took: $ms ms"
  fi
  out="$(dx "$NODE_B" "journalctl -u fabercp --no-pager -o cat --since @$since")"
  evidence "$(tail -n 8 <<<"$out")"
  assert_has "D journal: fabercp logs a shutdown on SIGTERM" "$out" 'shutting down.*SIGTERM|SIGTERM.*shutting down'
  assert_hasnt "D journal: no SIGKILL, timeout or failure on stop" "$out" 'SIGKILL|[Tt]imed out|Failed with result|Killing process'
  out="$(dx "$NODE_B" 'systemctl show -p ActiveState -p Result -p ExecMainCode -p ExecMainStatus fabercp | sort | tr "\n" " "')"
  assert_eq "D fabercp stopped cleanly (exited 0, result success)" "$out" "ActiveState=inactive ExecMainCode=1 ExecMainStatus=0 Result=success "

  dx "$NODE_B" 'systemctl start fabercp' >/dev/null
  wait_for 30 "$NODE_B" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 2"
  wait_for 30 "$NODE_A" "faber -o json cluster status | grep -q '\"leader_id\": [0-9]'"
  assert_eq "D $NODE_B sees 2 voters after stop and start" "$(voters "$NODE_B")" "2"
  out="$(dx "$NODE_A" 'faber cluster status')"
  evidence "$out"
  assert_hasnt "D $NODE_A has a leader again after B came back" "$out" '^leader: +-$'
}

# --- phase E: hardening -----------------------------------------------------

phase_e() {
  local n pid out rc
  section "E: hardening"
  for n in "$NODE_A" "$NODE_B"; do
    pid="$(main_pid "$n" fabercp)"
    out="$(dx "$n" "grep -E '^(Uid|Gid|CapInh|CapPrm|CapEff|CapBnd|CapAmb|NoNewPrivs):' /proc/$pid/status")"
    evidence "$out"
    assert_has "E $n: fabercp uid is faber's ($(dx "$n" 'id -u faber'))" "$out" "^Uid:[[:space:]]+$(dx "$n" 'id -u faber')[[:space:]]"
    assert_has "E $n: CapEff is CAP_NET_ADMIN only" "$out" "^CapEff:[[:space:]]+$CAP_NET_ADMIN_MASK$"
    assert_has "E $n: CapPrm is CAP_NET_ADMIN only" "$out" "^CapPrm:[[:space:]]+$CAP_NET_ADMIN_MASK$"
    assert_has "E $n: CapBnd is CAP_NET_ADMIN only" "$out" "^CapBnd:[[:space:]]+$CAP_NET_ADMIN_MASK$"
    assert_has "E $n: NoNewPrivs is set" "$out" '^NoNewPrivs:[[:space:]]+1$'
    assert_eq "E $n: /var/lib/faber is faber:faber 0700" "$(dx "$n" 'stat -c "%U:%G %a" /var/lib/faber')" "faber:faber 700"
    assert_eq "E $n: identity.json is faber:faber 0600" "$(dx "$n" 'stat -c "%U:%G %a" /var/lib/faber/identity.json')" "faber:faber 600"
    assert_eq "E $n: /etc/faber is root:faber 0750" "$(dx "$n" 'stat -c "%U:%G %a" /etc/faber')" "root:faber 750"
    assert_eq "E $n: /etc/cni/net.d is faber:faber 0755" "$(dx "$n" 'stat -c "%U:%G %a" /etc/cni/net.d')" "faber:faber 755"
    out="$(dx "$n" 'grep -E "^(ReadWritePaths|AmbientCapabilities|CapabilityBoundingSet)=" /etc/systemd/system/fabercp.service' | tr '\n' ' ')"
    assert_eq "E $n: the unit grants CAP_NET_ADMIN only and may write the containerd and CNI config directories" "$out" \
      "AmbientCapabilities=CAP_NET_ADMIN CapabilityBoundingSet=CAP_NET_ADMIN ReadWritePaths=/etc/containerd/conf.d ReadWritePaths=/etc/cni/net.d "
    out="$(dx "$n" 'systemd-analyze verify /etc/systemd/system/fabercp.service /etc/systemd/system/faber-containerd-reload.path /etc/systemd/system/faber-containerd-reload.service; echo "rc=$?"')"
    evidence "$out"
    assert_has "E $n: systemd-analyze verify exits 0 for the three units" "$out" '^rc=0$'
    assert_hasnt "E $n: systemd-analyze verify reports nothing about the faber units" "$out" 'faber'
  done
  out="$(dx "$NODE_B" 'journalctl -u fabercp --no-pager -o cat | grep -c "private_key\|PrivateKey = " || true')"
  assert_eq "E $NODE_B: fabercp journal has no private key lines" "$out" "0"
}

# --- phase F: failure paths -------------------------------------------------

# bootstrap_error NODE: content, owner and mode of the bootstrap-error file.
bootstrap_error() {
  dx "$1" 'f=/var/lib/faber/bootstrap-error; if [[ -f $f ]]; then echo "$(cat $f) $(stat -c "%U:%G %a" $f)"; else echo absent; fi'
}

# idle_token_checks TAG NODE CODE MESSAGE_REGEX: install.sh failed because
# fabercp idles on a token that cannot work.
idle_token_checks() {
  local t="$1" n="$2" code="$3" message="$4" out
  assert_fails "$t: install.sh exits non-zero" "$INSTALL_RC"
  assert_has "$t: install.sh prints 'cluster: failed'" "$INSTALL_OUT" '^cluster: failed$'
  assert_has "$t: install.sh says what is wrong with the token and how to recover" "$INSTALL_OUT" "$message"
  assert_hasnt "$t: install.sh does not suggest a plain re-run" "$INSTALL_OUT" 're-run this script to try again'
  assert_eq "$t: bootstrap-error is $code, faber:faber 0600" "$(bootstrap_error "$n")" "$code faber:faber 600"
  out="$(dx "$n" 'journalctl -u fabercp --no-pager -o cat | tail -n 3')"
  evidence "$out"
  assert_has "$t: fabercp journal says how to recover" "$out" 'faber mesh invite'
  assert_eq "$t: fabercp stays up, with no restart loop and no identity" \
    "$(dx "$n" 'systemctl is-active fabercp; systemctl show -p NRestarts --value fabercp; [[ -e /var/lib/faber/identity.json ]] && echo identity || echo no-identity' | tr '\n' ' ')" \
    "active 0 no-identity "
}

# stuck_join_checks TAG NODE: NODE replayed B's used token from an address
# the mesh did not invite, so its join can never finish.
stuck_join_checks() {
  local t="$1" n="$2" out body
  assert_fails "$t: install.sh exits non-zero" "$INSTALL_RC"
  assert_has "$t: install.sh prints 'cluster: failed'" "$INSTALL_OUT" '^cluster: failed$'
  assert_has "$t: install.sh explains what to check" "$INSTALL_OUT" 'has not joined the cluster|invite was rejected'
  out="$(dx "$n" 'journalctl -u fabercp --no-pager -o cat | tail -n 2')"
  evidence "$out"
  # Counts only; identity.json holds a private key and is never printed.
  assert_eq "$t: the node has an identity with a pending join that is not registered" \
    "$(dx "$n" 'f=/var/lib/faber/identity.json; echo "pending_join=$(grep -c "\"pending_join\"" $f) registered=$(grep -c "\"registered\"" $f)"')" \
    "pending_join=1 registered=0"

  # The raft gate: a node that has not registered answers raft RPCs with 503.
  body='{"vote":{"leader_id":{"term":1,"node_id":1},"committed":true},"prev_log_id":null,"entries":[],"leader_commit":null}'
  out="$(docker exec "$n" curl -sS --max-time 10 -w '\nhttp=%{http_code}\n' -X POST \
    -H 'content-type: application/json' -d "$body" "http://$MESH_B:$PEER_PORT/raft/append" 2>&1 | tr -d '\r')"
  evidence "$out"
  assert_has "$t: POST /raft/append on the unregistered node is 503" "$out" '^http=503$'
  assert_has "$t: POST /raft/append on the unregistered node says not_ready" "$out" 'not_ready'
  body='{"vote":{"leader_id":{"term":1,"node_id":1},"committed":false},"last_log_id":null}'
  out="$(docker exec "$n" curl -sS --max-time 10 -w '\nhttp=%{http_code}\n' -X POST \
    -H 'content-type: application/json' -d "$body" "http://$MESH_B:$PEER_PORT/raft/vote" 2>&1 | tr -d '\r')"
  assert_has "$t: POST /raft/vote on the unregistered node is 503" "$out" '^http=503$'
}

# mesh_intact_checks TAG A_STAMP: the mesh survived a machine that showed up
# with B's key. A_STAMP is unit_stamp of fabercp on A from before.
mesh_intact_checks() {
  local t="$1" stamp="$2" n out rc
  for n in "$NODE_A" "$NODE_B"; do
    out="$(dx "$n" 'journalctl -u fabercp --no-pager -o cat | grep -E "panicked|log reversion|storage is closed" | cut -c1-260 | head -n 4')"
    assert_hasnt "$t: no raft panic or closed storage in the journal of $n" "$out" 'panicked|log reversion|storage is closed'
  done
  assert_eq "$t: fabercp on $NODE_A was not restarted" "$(unit_stamp "$NODE_A" fabercp)" "$stamp"
  # WireGuard on A may have followed B's key to the other machine; B's own
  # traffic brings it back.
  wait_for 60 "$NODE_A" "wg show faber0 endpoints | grep -q '[[:space:]]$IP_B:$WG_PORT\$'"
  out="$(dx "$NODE_A" 'wg show faber0 endpoints')"
  evidence "$out"
  assert_has "$t: $NODE_A has B's endpoint as $IP_B:$WG_PORT (within 60 s)" "$out" "[[:space:]]${IP_B//./\\.}:$WG_PORT$"
  wait_for 30 "$NODE_A" "faber -o json cluster status | grep -q '\"leader_id\": [0-9]'"
  out="$(dx "$NODE_A" 'faber cluster status')"
  evidence "$out"
  assert_has "$t: B (node 2) is still a voter" "$out" "^ +2 +$NAME_B +${MESH_B//./\\.}:$PEER_PORT +voter "
  assert_hasnt "$t: the mesh has a leader" "$out" '^leader: +-$'
  # A's WireGuard session may still be the one it made with the other
  # machine; it lasts until A or B starts a new handshake.
  local t0=$SECONDS
  wait_for 60 "$NODE_A" "ping -c 1 -W 1 -I faber0 $MESH_B"
  info "$NODE_A could ping B again after $((SECONDS - t0)) s; raft $(dx "$NODE_A" 'faber cluster status | grep -E "^term:"' | tr -s ' ')"
  out="$(dx "$NODE_A" "ping -c 3 -W 2 -I faber0 $MESH_B")"
  assert_has "$t: $NODE_A reaches the real B over the mesh (within 60 s)" "$out" ' 0% packet loss'
  out="$(dx "$NODE_A" "curl -sS --max-time 5 http://$MESH_B:$PEER_PORT/api/v1/health")"
  assert_has "$t: $MESH_B:$PEER_PORT answers as node 2" "$out" '"node_id":2'
  # A write: an invite that expires at once (token discarded).
  docker exec "$NODE_A" faber mesh invite --name e2e-probe --wg-endpoint 198.51.100.77 --ttl 3s >/dev/null 2>&1
  rc=$?
  assert_ok "$t: writes still work (another 'faber mesh invite' succeeds)" "$rc"
  out="$(dx "$NODE_A" 'faber health')"
  assert_has "$t: faber health on $NODE_A is ok" "$out" '^status: +ok$'
}

# clean_start_checks TAG NODE RESTARTS_BEFORE SINCE_EPOCH: fabercp took its
# new identity on the first start.
clean_start_checks() {
  local t="$1" n="$2" before="$3" since="$4" out
  out="$(dx "$n" "journalctl -u fabercp --no-pager -o cat --since @$since | grep -E 'Error: |Cannot assign|Caused by|Failed with result' | head -n 4; echo \"NRestarts=\$(systemctl show -p NRestarts --value fabercp)\"")"
  evidence "$out"
  assert_has "$t: fabercp did not crash and restart on its new identity (NRestarts still $before)" "$out" "^NRestarts=$before$"
  assert_hasnt "$t: no 'Cannot assign requested address' on the new identity" "$out" 'Cannot assign requested address'
}

phase_f() {
  local out tok mesh_c mesh_e stamp restarts since
  section "F: failure paths on $NODE_C ($IP_C) and $NODE_E ($IP_E)"

  # F1: an interface called faber0 that faber did not create.
  dx "$NODE_C" 'ip link add faber0 type wireguard' >/dev/null
  run_install "$NODE_C" "install-c-foreign" "" -- --endpoint "$IP_C"
  assert_fails "F1 foreign faber0: install.sh exits non-zero" "$INSTALL_RC"
  assert_has "F1 foreign faber0: the message names the interface" "$INSTALL_OUT" 'interface faber0 already exists and was not created by faber'
  assert_eq "F1 foreign faber0: nothing was installed or written" \
    "$(dx "$NODE_C" '[[ -e /etc/faber || -e /opt/faber || -e /etc/systemd/system/fabercp.service ]] && echo changed || echo untouched; command -v containerd >/dev/null && echo containerd-installed')" "untouched"
  dx "$NODE_C" 'ip link del faber0' >/dev/null

  # F2: a token that expired before the node was installed.
  tok="$(docker exec "$NODE_A" faber mesh invite --name "$NAME_C" --wg-endpoint "$IP_C" --ttl 2s 2>/dev/null | tr -d '\r\n')"
  sleep 4
  run_install "$NODE_C" "install-c-expired" "$tok" -e FABER_HEALTH_TIMEOUT=15 -e FABER_JOIN_TIMEOUT=25 -- --join -
  idle_token_checks "F2 expired token" "$NODE_C" token_expired \
    'the join token has expired; create a new invite .*re-run with --join <token>'

  # F3: a token that has the right shape but does not decode.
  run_install "$NODE_C" "install-c-invalid" "faber1.bm90LWEtdG9rZW4" -e FABER_HEALTH_TIMEOUT=15 -e FABER_JOIN_TIMEOUT=25 -- --join -
  idle_token_checks "F3 invalid token" "$NODE_C" token_invalid \
    'the join token is not valid; create a new invite .*re-run with --join <token>'

  # F4: B's token again, from a host that was not invited. The mesh must
  # not let it in, and B must not notice. The node ends up in a pending join
  # that can never finish.
  run_install "$NODE_C" "install-c-used" "$TOKEN" -e FABER_JOIN_TIMEOUT=25 -- --join -
  stuck_join_checks "F4 used token from another host" "$NODE_C"
  assert_eq "F4 used token: the bootstrap-error of the earlier token is gone" "$(bootstrap_error "$NODE_C")" "absent"
  assert_eq "F4 mesh still has 2 voters (seen from $NODE_A)" "$(voters "$NODE_A")" "2"
  out="$(dx "$NODE_A" "ping -c 2 -W 2 -I faber0 $MESH_B")"
  assert_has "F4 $NODE_A still reaches the real B over the mesh" "$out" ' 0% packet loss'
  out="$(dx "$NODE_A" 'wg show faber0 endpoints')"
  evidence "$out"
  assert_has "F4 $NODE_A still has B's endpoint as $IP_B" "$out" "${IP_B//./\\.}:$WG_PORT"

  # F5: a fresh invite replaces the stuck join. No cleanup on the node. The
  # invite lets C's address through the firewall while C still holds B's key.
  stamp="$(unit_stamp "$NODE_A" fabercp)"
  restarts="$(dx "$NODE_C" 'systemctl show -p NRestarts --value fabercp')"
  since="$(dx "$NODE_C" 'date +%s')"
  tok="$(docker exec "$NODE_A" faber mesh invite --name "$NAME_C" --wg-endpoint "$IP_C" 2>/dev/null | tr -d '\r\n')"
  run_install "$NODE_C" "install-c-fresh" "$tok" -- --join -
  assert_ok "F5 fresh token over the stuck join, no manual cleanup: install.sh exits 0" "$INSTALL_RC"
  assert_has "F5 fresh token: install.sh prints 'cluster: joined'" "$INSTALL_OUT" '^cluster: joined$'
  out="$(dx "$NODE_C" "journalctl -u fabercp --no-pager -o cat --since @$since | grep -iE 'discard|removed the old identity|rejected|created node identity|joined the mesh' | cut -c1-300 | tail -n 5")"
  evidence "$out"
  clean_start_checks "F5 $NODE_C" "$NODE_C" "$restarts" "$since"
  wait_for 30 "$NODE_A" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 3"
  wait_for 30 "$NODE_B" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 3"
  assert_eq "F5 $NODE_A sees 3 voters" "$(voters "$NODE_A")" "3"
  assert_eq "F5 $NODE_B sees 3 voters" "$(voters "$NODE_B")" "3"
  assert_eq "F5 $NODE_C sees 3 voters" "$(voters "$NODE_C")" "3"
  mesh_intact_checks "F5" "$stamp"
  out="$(dx "$NODE_C" 'ip -o -4 addr show dev faber0')"
  evidence "$out"
  mesh_c="$(awk '{ split($4, a, "/"); print a[1] }' <<<"$out")"
  assert_eq "F5 $NODE_C has one address on faber0, and it is not B's" \
    "$(wc -l <<<"$out" | tr -d ' ') $([[ -n "$mesh_c" && "$mesh_c" != "$MESH_B" ]] && echo own || echo wrong)" "1 own"
  out="$(dx "$NODE_A" "ping -c 3 -W 2 -I faber0 $mesh_c")"
  assert_has "F5 ping $NODE_C ($mesh_c) from $NODE_A over the mesh" "$out" ' 0% packet loss'
  wait_for 20 "$NODE_B" "ping -c 1 -W 1 -I faber0 $mesh_c"
  out="$(dx "$NODE_B" "ping -c 3 -W 2 -I faber0 $mesh_c")"
  assert_has "F5 ping $NODE_C ($mesh_c) from $NODE_B over the mesh" "$out" ' 0% packet loss'
  out="$(dx "$NODE_C" 'cat /etc/faber/fabercp.env; ls /etc/faber')"
  assert_hasnt "F5 $NODE_C: bootstrap settings and token are gone" "$out" 'FABER_JOIN_TOKEN_FILE|join\.token'
  assert_eq "F5 $NODE_C: no bootstrap-error left" "$(bootstrap_error "$NODE_C")" "absent"

  # F6: a token is refused on a node that is already configured.
  run_install "$NODE_C" "install-c-rejoin" "$tok" -- --join -
  assert_fails "F6 --join on a configured node: install.sh exits non-zero" "$INSTALL_RC"
  assert_has "F6 --join on a configured node: clear message" "$INSTALL_OUT" 'already configured .*refusing --join'
  assert_eq "F6 mesh still has 3 voters" "$(voters "$NODE_A")" "3"

  # F7: the same replay on another node, but this time the operator creates
  # the invite and then waits. The stuck node, still holding B's key, now gets
  # through the firewall and talks to the mesh as B. The mesh must reject it
  # and carry on; the node must drop the key and the interface.
  run_install "$NODE_E" "install-e-used" "$TOKEN" -e FABER_JOIN_TIMEOUT=25 -- --join -
  stuck_join_checks "F7 used token on a second host" "$NODE_E"
  stamp="$(unit_stamp "$NODE_A" fabercp)"
  tok="$(docker exec "$NODE_A" faber mesh invite --name "$NAME_E" --wg-endpoint "$IP_E" 2>/dev/null | tr -d '\r\n')"
  if wait_for 90 "$NODE_E" '[[ ! -e /var/lib/faber/identity.json ]]'; then
    pass "F7 the mesh rejected the machine that used B's token (its identity is gone)"
  else
    fail "F7 the mesh rejected the machine that used B's token (its identity is gone)" \
      "$(dx "$NODE_E" 'journalctl -u fabercp --no-pager -o cat | cut -c1-300 | tail -n 6')"
  fi
  # The journal has colour codes between the fields, so count instead of cutting.
  assert_eq "F7 $NODE_E journal: the mesh rejected the invite with invite_used (matching lines)"     "$(dx "$NODE_E" 'journalctl -u fabercp --no-pager -o cat | grep -F "the mesh rejected this node" | grep -c "invite_used"')" "1"
  assert_eq "F7 $NODE_E: bootstrap-error is invite_rejected, faber:faber 0600" "$(bootstrap_error "$NODE_E")" "invite_rejected faber:faber 600"
  assert_eq "F7 $NODE_E: faber0 and its record are gone, fabercp idles without a restart" \
    "$(dx "$NODE_E" 'ip link show faber0 >/dev/null 2>&1 && echo link || echo no-link; [[ -e /var/lib/faber/iface ]] && echo record || echo no-record; systemctl is-active fabercp; systemctl show -p NRestarts --value fabercp' | tr '\n' ' ')" \
    "no-link no-record active 0 "
  mesh_intact_checks "F7" "$stamp"
  assert_eq "F7 mesh still has 3 voters" "$(voters "$NODE_A")" "3"

  # F8: the fresh invite then works on that node, again with no cleanup.
  restarts="$(dx "$NODE_E" 'systemctl show -p NRestarts --value fabercp')"
  since="$(dx "$NODE_E" 'date +%s')"
  run_install "$NODE_E" "install-e-fresh" "$tok" -- --join -
  assert_ok "F8 fresh token after the rejection, no manual cleanup: install.sh exits 0" "$INSTALL_RC"
  assert_has "F8 fresh token: install.sh prints 'cluster: joined'" "$INSTALL_OUT" '^cluster: joined$'
  clean_start_checks "F8 $NODE_E" "$NODE_E" "$restarts" "$since"
  wait_for 30 "$NODE_A" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx 4"
  out="$(dx "$NODE_A" 'faber cluster status')"
  evidence "$out"
  assert_eq "F8 $NODE_A sees 4 voters" "$(voters "$NODE_A")" "4"
  assert_eq "F8 $NODE_E sees 4 voters" "$(voters "$NODE_E")" "4"
  mesh_e="$(dx "$NODE_E" 'ip -o -4 addr show dev faber0' | awk '{ split($4, a, "/"); print a[1] }')"
  # The first packet also has to start the WireGuard handshake.
  wait_for 20 "$NODE_B" "ping -c 1 -W 1 -I faber0 $mesh_e"
  out="$(dx "$NODE_B" "ping -c 3 -W 2 -I faber0 $mesh_e")"
  assert_has "F8 ping $NODE_E ($mesh_e) from $NODE_B over the mesh" "$out" ' 0% packet loss'
  mesh_intact_checks "F8" "$stamp"
}

# --- phase I: names, lease and takeover -------------------------------------

status_json() { dx "$1" 'faber -o json cluster status'; }

# own_node_id NODE: the node id NODE reports for itself.
own_node_id() {
  status_json "$1" | awk '/"node_id":/ { gsub(/[^0-9]/, "", $2); print $2; exit }'
}

# member_id JSON NAME: the node id of the member called NAME.
member_id() {
  awk -v want="\"$2\"" '
    /"node_id":/ { id = $2; gsub(/[^0-9]/, "", id) }
    /"name":/ { v = $2; gsub(/,/, "", v); if (v == want) print id }' <<<"$1"
}

# member_ids JSON: the node ids of all members, each between spaces.
member_ids() {
  awk 'BEGIN { printf " " } /"members":/ { m = 1 } m && /"node_id":/ { id = $2; gsub(/[^0-9]/, "", id); printf "%s ", id }' <<<"$1"
}

# json_invite NAME ENDPOINT_IP: create an invite on $NODE_A. Sets INVITE_TOKEN
# and INVITE_REPLACES (empty when the name is free). The token is not printed.
json_invite() {
  local json
  json="$(docker exec "$NODE_A" faber -o json mesh invite --name "$1" --wg-endpoint "$2" 2>/dev/null | tr -d '\r')"
  INVITE_TOKEN="$(sed -n 's/.*"token": "\([^"]*\)".*/\1/p' <<<"$json")"
  INVITE_REPLACES="$(sed -n 's/.*"replaces_node_id": \([0-9]*\).*/\1/p' <<<"$json")"
}

# ensure_member NODE NAME IP: join NODE to the mesh unless it is a member.
ensure_member() {
  local n="$1" name="$2" ip="$3"
  if dxq "$n" '[[ -f /var/lib/faber/identity.json ]] && faber -o json cluster status | grep -q "\"joined\": true"'; then
    return 0
  fi
  info "$n is no member yet (phase F did not run): joining it as $name"
  json_invite "$name" "$ip"
  run_install "$n" "install-${n##*-}-i-join" "$INVITE_TOKEN" -- --join -
  assert_ok "I setup: $n joined as $name" "$INSTALL_RC"
}

# wait_voters NODE COUNT
wait_voters() {
  wait_for 60 "$1" "faber -o json cluster status | grep -c '\"voter\": true' | grep -qx $2"
}

phase_i() {
  local out json n role old_id new_id old_subnet new_subnet old_pub new_pub mesh_new restarts
  local -a members=("$NODE_A" "$NODE_B" "$NODE_C" "$NODE_E")
  section "I: names, lease and takeover"
  ensure_member "$NODE_C" "$NAME_C" "$IP_C"
  ensure_member "$NODE_E" "$NAME_E" "$IP_E"
  wait_voters "$NODE_A" 4
  assert_eq "I setup: the mesh has 4 voters ($NODE_A, $NODE_B, $NODE_C, $NODE_E)" "$(voters "$NODE_A")" "4"

  # I1: names.
  out="$(dx "$NODE_A" 'faber mesh invite --wg-endpoint 198.51.100.78; echo "rc=$?"')"
  evidence "$out"
  assert_has "I1 faber mesh invite without --name fails" "$out" '^rc=[1-9]'
  assert_has "I1 faber mesh invite without --name says that --name is missing" "$out" '--name'
  out="$(dx "$NODE_A" "curl -sS --max-time 10 -w '\nhttp=%{http_code}\n' -X POST -H 'content-type: application/json' -d '{\"name\":\"\",\"endpoint\":\"198.51.100.78\"}' http://127.0.0.1:7400/api/v1/mesh/invites")"
  evidence "$out"
  assert_has "I1 API: an invite with an empty name is 400" "$out" '^http=400$'
  assert_has "I1 API: an invite with an empty name is bad_request" "$out" 'bad_request'
  json="$(status_json "$NODE_A")"
  evidence "$(grep -E '"(node_id|name)":' <<<"$json")"
  assert_eq "I1 cluster status (JSON): the founder is named after its hostname, $NAME_A" "$(member_id "$json" "$NAME_A")" "1"
  assert_eq "I1 cluster status (JSON): node 2 is named $NAME_B, from its invite" "$(member_id "$json" "$NAME_B")" "2"
  assert_eq "I1 cluster status (JSON): $NODE_C is named $NAME_C" "$(member_id "$json" "$NAME_C")" "$(own_node_id "$NODE_C")"
  assert_eq "I1 cluster status (JSON): $NODE_E is named $NAME_E" "$(member_id "$json" "$NAME_E")" "$(own_node_id "$NODE_E")"
  out="$(dx "$NODE_A" 'faber cluster status')"
  evidence "$out"
  assert_has "I1 cluster status (text) shows the member names" "$out" "^ +2 +$NAME_B +${MESH_B//./\\.}:$PEER_PORT +voter "
  assert_has "I1 cluster status (text) has a name column" "$out" '^ +node_id +name +addr +membership '
  assert_has "I1 cluster status (text) names the founder" "$out" "^ +1 +$NAME_A +${MESH_A//./\\.}:$PEER_PORT "

  # I2: the lease setting.
  assert_eq "I2 faber mesh lease prints the default" "$(dx "$NODE_A" 'faber mesh lease')" "lease: 8h (28800s)"
  assert_eq "I2 faber mesh lease --set 10m prints the new value" "$(dx "$NODE_A" 'faber mesh lease --set 10m')" "lease: 10m (600s)"
  for n in "${members[@]}"; do
    wait_for 15 "$n" "faber mesh lease | grep -qxF 'lease: 10m (600s)'"
    role="$(status_json "$n" | sed -n 's/.*"role": "\([a-z]*\)".*/\1/p')"
    assert_eq "I2 $n ($role) sees the new lease" "$(dx "$n" 'faber mesh lease')" "lease: 10m (600s)"
  done
  out="$(dx "$NODE_A" 'faber mesh lease --set 1s; echo "rc=$?"')"
  evidence "$out"
  assert_has "I2 faber mesh lease --set 1s is refused" "$out" '^rc=[1-9]'
  assert_has "I2 the refusal carries the server's message" "$out" 'lease must be between 300 and 2592000 seconds'
  assert_eq "I2 the refused value changed nothing" "$(dx "$NODE_B" 'faber mesh lease')" "lease: 10m (600s)"
  assert_eq "I2 faber mesh lease --set 8h puts the default back" "$(dx "$NODE_A" 'faber mesh lease --set 8h')" "lease: 8h (28800s)"

  # I3: a new server, $NODE_F, joins with the name that $NODE_C holds while
  # $NODE_C is still running. It takes over the name and the subnet.
  old_id="$(own_node_id "$NODE_C")"
  old_subnet="$(node_subnet "$NODE_C")"
  old_pub="$(dx "$NODE_C" 'wg show faber0 public-key')"
  info "before: $NODE_C is node $old_id with subnet $old_subnet; $(dx "$NODE_A" 'faber cluster status | grep -E "^leader:"' | tr -s ' ')"
  INVITE_TOKEN="$(docker exec "$NODE_A" faber mesh invite --name "$NAME_C" --wg-endpoint "$IP_F" 2>"$LOG_DIR/invite-f-takeover.stderr" | tr -d '\r\n')"
  out="$(tr -d '\r' <"$LOG_DIR/invite-f-takeover.stderr")"
  evidence "$out"
  assert_has "I3 the invite for a held name prints the name on stderr" "$out" "^name: +$NAME_C\$"
  assert_has "I3 the invite for a held name warns which node it replaces" "$out" "^WARNING: this invite replaces node $old_id \($NAME_C\)"
  run_install "$NODE_F" "install-f-takeover" "$INVITE_TOKEN" -e FABER_JOIN_TIMEOUT=180 -- --join -
  assert_ok "I3 takeover: install.sh --join - on the new server exits 0" "$INSTALL_RC"
  assert_has "I3 takeover: install.sh prints 'cluster: joined'" "$INSTALL_OUT" '^cluster: joined$'
  new_id="$(own_node_id "$NODE_F")"
  assert_ne "I3 takeover: the new server has its own node id (old node: $old_id)" "$new_id" "$old_id"
  wait_for 30 "$NODE_F" "grep -q '\"subnet\"' $CNI_CONFLIST"
  assert_eq "I3 takeover: the new server has the old node's container subnet" "$(node_subnet "$NODE_F")" "$old_subnet"
  wait_for 30 "$NODE_A" "! faber -o json cluster status | grep -q '\"node_id\": $old_id,'"
  wait_voters "$NODE_A" 4
  json="$(status_json "$NODE_A")"
  evidence "$(dx "$NODE_A" 'faber cluster status')"
  assert_eq "I3 takeover: the name $NAME_C now belongs to node $new_id" "$(member_id "$json" "$NAME_C")" "$new_id"
  assert_hasnt "I3 takeover: node $old_id is gone from cluster status" "$(member_ids "$json")" " $old_id "
  assert_eq "I3 takeover: still 4 voters" "$(voters "$NODE_A")" "4"
  new_pub="$(dx "$NODE_F" 'wg show faber0 public-key')"
  mesh_new="$(mesh_ip "$NODE_F")"
  wait_for 30 "$NODE_B" "! wg show faber0 peers | grep -qF '$old_pub'"
  out="$(dx "$NODE_B" 'wg show faber0 allowed-ips')"
  evidence "$out"
  if grep -qF -- "$old_pub" <<<"$out"; then
    fail "I3 takeover: $NODE_B's WireGuard no longer has the old node's key" "$out"
  else
    pass "I3 takeover: $NODE_B's WireGuard no longer has the old node's key"
  fi
  assert_eq "I3 takeover: $NODE_B routes $old_subnet to the new node" "$(allowed_ips "$NODE_B" "$new_pub")" "$old_subnet $mesh_new/32 "
  wait_for 20 "$NODE_B" "ping -c 1 -W 1 -I faber0 $mesh_new"
  out="$(dx "$NODE_B" "ping -c 3 -W 2 -I faber0 $mesh_new")"
  assert_has "I3 takeover: ping the new node ($mesh_new) from $NODE_B over the mesh" "$out" ' 0% packet loss'

  # The old node is still running. It has to notice that the mesh removed it.
  if wait_for 120 "$NODE_C" '[[ ! -e /var/lib/faber/identity.json && -f /var/lib/faber/bootstrap-error ]]'; then
    pass "I3 the old node, still running, noticed its removal and discarded its identity"
    assert_eq "I3 the old node: bootstrap-error is node_removed, faber:faber 0600" "$(bootstrap_error "$NODE_C")" "node_removed faber:faber 600"
    assert_eq "I3 the old node: faber0 and its record are gone, fabercp idles" \
      "$(dx "$NODE_C" 'ip link show faber0 >/dev/null 2>&1 && echo link || echo no-link; [[ -e /var/lib/faber/iface ]] && echo record || echo no-record; systemctl is-active fabercp' | tr '\n' ' ')" \
      "no-link no-record active "
    assert_eq "I3 the old node: its CNI config is gone" "$(dx "$NODE_C" "[[ -e $CNI_CONFLIST ]] && echo present || echo absent")" "absent"
    out="$(dx "$NODE_C" 'nft list table inet faber >/dev/null 2>&1 && echo loaded || echo missing')"
    assert_eq "I3 the old node: the table inet faber is still loaded" "$out" "loaded"
    # A restart must leave fabercp idle, not in a restart loop.
    restarts="$(dx "$NODE_C" 'systemctl show -p NRestarts --value fabercp')"
    dx "$NODE_C" 'systemctl restart fabercp' >/dev/null
    sleep 10
    out="$(dx "$NODE_C" 'journalctl -u fabercp --no-pager -o cat | cut -c1-300 | tail -n 4')"
    evidence "$out"
    assert_eq "I3 the old node: after 'systemctl restart fabercp' the unit is active with no restarts (NRestarts was $restarts)" \
      "$(dx "$NODE_C" 'systemctl is-active fabercp; systemctl show -p NRestarts --value fabercp; systemctl show -p SubState --value fabercp' | tr '\n' ' ')" \
      "active $restarts running "
    assert_eq "I3 the old node: the restart kept the node_removed record and made no identity" \
      "$(bootstrap_error "$NODE_C") $(dx "$NODE_C" '[[ -e /var/lib/faber/identity.json ]] && echo identity || echo no-identity')" \
      "node_removed faber:faber 600 no-identity"
  else
    fail "I3 the old node, still running, noticed its removal and discarded its identity (within 120 s)" \
      "$(dx "$NODE_C" 'ls -la /var/lib/faber; journalctl -u fabercp --no-pager -o cat | cut -c1-300 | tail -n 8')"
    # The checks below need a removed node. Put this one into the state that
    # fabercp leaves behind.
    info "writing the node_removed state on $NODE_C by hand, so the install.sh checks still run"
    dx "$NODE_C" 'systemctl stop fabercp
      rm -f /var/lib/faber/identity.json
      printf "node_removed\n" > /var/lib/faber/bootstrap-error
      : > /var/lib/faber/join-failed
      chown faber:faber /var/lib/faber/bootstrap-error /var/lib/faber/join-failed
      chmod 0600 /var/lib/faber/bootstrap-error
      ip link del faber0 2>/dev/null; rm -f /var/lib/faber/iface' >/dev/null
  fi

  # I4: install.sh with no arguments on the removed node would start a
  # second mesh. It must refuse.
  out="$(dx "$NODE_C" 'md5sum < /etc/faber/fabercp.env')"
  run_install "$NODE_C" "install-c-removed-bare" "" --
  assert_fails "I4 removed node: install.sh without --join exits non-zero" "$INSTALL_RC"
  assert_has "I4 removed node: the message says the cluster removed the node" "$INSTALL_OUT" 'the cluster removed this node'
  assert_has "I4 removed node: the message names --join <token> --reset" "$INSTALL_OUT" '--join <token> --reset'
  assert_eq "I4 removed node: no new mesh was started and nothing changed" \
    "$(dx "$NODE_C" 'grep -c FABER_NEW_MESH /etc/faber/fabercp.env; [[ -e /var/lib/faber/identity.json ]] && echo identity || echo no-identity; md5sum < /etc/faber/fabercp.env' | tr '\n' ' ')" \
    "0 no-identity $out "

  # I5: the removed node comes back with a new invite and --join --reset.
  json_invite "${NAME_C}2" "$IP_C"
  assert_eq "I5 an invite for a free name replaces nobody" "$INVITE_REPLACES" ""
  run_install "$NODE_C" "install-c-rejoin-reset" "$INVITE_TOKEN" -e FABER_JOIN_TIMEOUT=180 -- --join - --reset
  assert_ok "I5 removed node: install.sh --join - --reset exits 0" "$INSTALL_RC"
  assert_has "I5 removed node: install.sh prints 'cluster: joined'" "$INSTALL_OUT" '^cluster: joined$'
  wait_voters "$NODE_A" 5
  assert_eq "I5 the mesh has 5 voters" "$(voters "$NODE_A")" "5"
  assert_eq "I5 removed node: no bootstrap-error left" "$(bootstrap_error "$NODE_C")" "absent"
  json="$(status_json "$NODE_A")"
  assert_eq "I5 the node is back as ${NAME_C}2" "$(member_id "$json" "${NAME_C}2")" "$(own_node_id "$NODE_C")"
  wait_for 30 "$NODE_C" "grep -q '\"subnet\"' $CNI_CONFLIST && ! grep -q '\"subnet\": \"$old_subnet\"' $CNI_CONFLIST"
  new_subnet="$(node_subnet "$NODE_C")"
  assert_ne "I5 the node has a subnet of its own, not the one it lost ($old_subnet)" "$new_subnet" "$old_subnet"

  # I6: --join --reset on a node that still has its identity. $NODE_E joins
  # again under its own name, which is a takeover of its old self.
  old_id="$(own_node_id "$NODE_E")"
  old_subnet="$(node_subnet "$NODE_E")"
  info "before: $NODE_E is node $old_id with subnet $old_subnet; $(dx "$NODE_A" 'faber cluster status | grep -E "^leader:"' | tr -s ' ')"
  json_invite "$NAME_E" "$IP_E"
  assert_eq "I6 the invite says it replaces node $old_id" "$INVITE_REPLACES" "$old_id"
  run_install "$NODE_E" "install-e-reset" "$INVITE_TOKEN" -e FABER_JOIN_TIMEOUT=180 -- --join - --reset
  assert_ok "I6 configured node: install.sh --join - --reset exits 0" "$INSTALL_RC"
  assert_has "I6 configured node: install.sh prints 'cluster: joined'" "$INSTALL_OUT" '^cluster: joined$'
  assert_has "I6 configured node: install.sh says it discards the identity" "$INSTALL_OUT" 'identity and raft data will be discarded'
  new_id="$(own_node_id "$NODE_E")"
  assert_ne "I6 the node has a new node id (was $old_id)" "$new_id" "$old_id"
  wait_for 30 "$NODE_E" "grep -q '\"subnet\": \"$old_subnet\"' $CNI_CONFLIST"
  assert_eq "I6 the node kept its container subnet" "$(node_subnet "$NODE_E")" "$old_subnet"
  wait_for 30 "$NODE_A" "! faber -o json cluster status | grep -q '\"node_id\": $old_id,'"
  wait_voters "$NODE_A" 5
  json="$(status_json "$NODE_A")"
  evidence "$(dx "$NODE_A" 'faber cluster status')"
  assert_eq "I6 the name $NAME_E belongs to node $new_id" "$(member_id "$json" "$NAME_E")" "$new_id"
  assert_hasnt "I6 node $old_id is gone from cluster status" "$(member_ids "$json")" " $old_id "
  assert_eq "I6 the mesh still has 5 voters" "$(voters "$NODE_A")" "5"
  mesh_new="$(mesh_ip "$NODE_E")"
  wait_for 20 "$NODE_A" "ping -c 1 -W 1 -I faber0 $mesh_new"
  out="$(dx "$NODE_A" "ping -c 3 -W 2 -I faber0 $mesh_new")"
  assert_has "I6 ping $NODE_E ($mesh_new) from $NODE_A over the mesh" "$out" ' 0% packet loss'
  out="$(dx "$NODE_A" 'faber cluster status')"
  assert_hasnt "I6 the mesh has a leader" "$out" '^leader: +-$'
}

# --- phase G: Docker's containerd.io ----------------------------------------

phase_g() {
  local before out
  section "G: new mesh on $NODE_D ($IP_D) with containerd.io preinstalled"
  before="$(dx "$NODE_D" "dpkg-query -W -f='\${Package} \${Version}' containerd.io; echo; containerd --version; systemctl is-active containerd")"
  evidence "$before"
  new_mesh_checks "G" "$NODE_D" "$IP_D" "install-d-1"
  assert_has "G install.sh chose the containerd.io package" "$INSTALL_OUT" 'containerd package: containerd\.io'
  out="$(dx "$NODE_D" "dpkg-query -W -f='\${Package} \${Status}\n' containerd.io containerd 2>&1")"
  evidence "$out"
  assert_has "G containerd.io is still installed" "$out" '^containerd\.io install ok installed$'
  assert_hasnt "G the distro containerd package was not installed" "$out" '^containerd install ok installed$'
  assert_has "G containerd is 2.x" "$(dx "$NODE_D" 'containerd --version')" ' v?2\.[0-9]+\.[0-9]+'
  out="$(dx "$NODE_D" 'ls /etc/containerd/')"
  evidence "$out"
  assert_has "G Docker's config.toml was backed up" "$out" '^config\.toml\.faber-bak\.[0-9]+$'

  # The container network on containerd.io, with one pod on this node.
  container_node_checks "G" "$NODE_D" "$SUBNET_A"
  if run_sandbox "G" "$NODE_D" e2e-pod-d; then
    if in_subnet24 "$POD_IP" "$SUBNET_A"; then pass "G pod on $NODE_D has an address in $SUBNET_A [$POD_IP]"; else fail "G pod on $NODE_D has an address in $SUBNET_A" "pod IP: $POD_IP"; fi
    out="$(dx "$NODE_D" "ping -c 3 -W 2 $POD_IP")"
    assert_has "G $NODE_D pings its pod" "$out" ' 0% packet loss'
    remove_sandbox "$NODE_D" "$POD_ID"
  fi
}

# refusal_warnings: a node whose raft RPCs a peer answers with 403 logs a
# warning, at most once a minute per peer. Nothing in this suite makes that
# happen on purpose, so this only reports what the journals hold.
refusal_warnings() {
  local n
  for n in "${STARTED[@]}"; do
    info "$n journal, \"refuses this node's raft RPCs\" warnings: $(dx "$n" 'journalctl -u fabercp --no-pager -o cat 2>/dev/null | grep -c "refuses this node.s raft RPCs"')"
  done
}

# --- main -------------------------------------------------------------------

has_phase() { [[ " $PHASES " == *" $1 "* ]]; }

main() {
  mkdir -p "$LOG_DIR"
  trap cleanup EXIT

  section "setup"
  build_binaries || { echo "building the binaries failed"; exit 2; }
  build_images || { echo "building the node images failed"; exit 2; }
  check_binaries || { echo "the binaries do not run on the node image (glibc?)"; exit 2; }

  cleanup
  docker network create "$NET" >/dev/null || { echo "cannot create network $NET"; exit 2; }
  start_node "$NODE_A" "$IMAGE"
  start_node "$NODE_B" "$IMAGE"
  start_node "$NODE_C" "$IMAGE"
  IP_A="$(node_ip "$NODE_A")"
  IP_B="$(node_ip "$NODE_B")"
  IP_C="$(node_ip "$NODE_C")"
  if has_phase F || has_phase I; then
    start_node "$NODE_E" "$IMAGE"
    IP_E="$(node_ip "$NODE_E")"
  fi
  if has_phase I; then
    start_node "$NODE_F" "$IMAGE"
    IP_F="$(node_ip "$NODE_F")"
  fi
  if has_phase G; then
    start_node "$NODE_D" "$IMAGE_CIO"
    IP_D="$(node_ip "$NODE_D")"
  fi
  info "docker: $(docker version --format '{{.Server.Version}}'), kernel $(dx "$NODE_A" 'uname -r')"
  info "wireguard link type: $(dx "$NODE_A" 'ip link add e2ewgtest type wireguard && ip link del e2ewgtest && echo available || echo MISSING')"

  has_phase A && phase_a
  has_phase B && phase_b
  has_phase H && phase_h
  has_phase J && phase_j
  has_phase C && phase_c
  has_phase D && phase_d
  has_phase E && phase_e
  has_phase F && phase_f
  has_phase I && phase_i
  has_phase G && phase_g
  refusal_warnings
  [[ -n "$SANDBOX_PATH" ]] && info "pod sandboxes: $SANDBOX_PATH"

  section "result"
  printf 'passed: %d   failed: %d\n' "$PASS" "$FAIL"
  if ((FAIL > 0)); then
    printf 'failed assertions:\n'
    printf '  - %s\n' "${FAILED[@]}"
    collect_logs
  fi
  info "logs: $LOG_DIR"
  ((FAIL == 0))
}

# One line, so bash has read all of it before main runs and an edit to this
# file during a run cannot change what the run does.
main "$@"; exit $?
