#!/usr/bin/env bats
# Unit tests for src/install.sh. Run with:
#   docker run --rm -v "$PWD:/w" -w /w bats/bats:latest tests/install

setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../.." && pwd)"
  SCRIPT="$REPO_ROOT/src/install.sh"
  FIXTURES="$BATS_TEST_DIRNAME/fixtures"
  # shellcheck source=../../src/install.sh
  source "$SCRIPT"
  # The script enables nounset/pipefail; bats internals are not nounset-safe.
  set +u
  TMP="$(mktemp -d)"
  export CNI_BIN_DIR="$TMP/cni"
  export FABER_ARCH=x86_64
  export FABER_HOME="$TMP/opt/faber"
  export FABER_CLI_DIR="$TMP/usr/local/bin"
  export FABER_NODE_INSTALL_DIR="$TMP/usr/local/sbin"
  # A kernel that passes preflight, whatever the host runs, and no sub-id files.
  export FABER_KERNEL_RELEASE="6.8.0-test"
  export SUBUID_FILE="$TMP/etc/subuid" SUBGID_FILE="$TMP/etc/subgid"
  export FABER_BIN_SRC_DIR="$TMP/src"
  export FABER_ETC="$TMP/etc/faber"
  export FABER_STATE_DIR="$TMP/var/lib/faber"
  export SYSTEMD_UNIT_DIR="$TMP/etc/systemd/system"
  # Absent unless a test creates it: no systemd by default.
  export SYSTEMD_RUN_DIR="$TMP/run/systemd/system"
  export CONTAINERD_ROOT_CONFIG="$TMP/etc/containerd/config.toml"
  export CONTAINERD_CONF_DIR="$TMP/etc/containerd/conf.d"
  export CNI_CONF_DIR="$TMP/etc/cni/net.d"
  export MODULES_LOAD_FILE="$TMP/etc/modules-load.d/faber.conf"
  export SYSCTL_FILE="$TMP/etc/sysctl.d/99-faber.conf"
  export FABER_HEALTH_TIMEOUT=1 FABER_JOIN_TIMEOUT=1 FABER_POLL_INTERVAL=0.1
  # A token-shaped value that must never show up in output or argv.
  TOKEN="faber1.eyJ2IjoxLCJwcml2YXRlX2tleSI6IlNFQ1JFVCJ9_TOPSECRET-xyz"
  make_fake_binary "$FABER_BIN_SRC_DIR/fabercp" "fabercp 0.1.0"
  make_fake_binary "$FABER_BIN_SRC_DIR/faber" "faber 0.1.0"
  make_fake_binary "$FABER_BIN_SRC_DIR/faber-node" "faber-node 0.1.0"
  # The default node name comes from `hostname`; see set_hostname.
  export HOSTNAME_STUB_DIR="$TMP/hostname-stub"
  export -f hostname
  set_hostname testnode testnode.example.com
}

# hostname: stands in for the hostname command, in these tests and (exported)
# in every `bash "$SCRIPT"` they start. `hostname -s` fails when set_hostname
# was given an empty short name.
hostname() {
  if [ "${1:-}" = -s ]; then
    [ -s "$HOSTNAME_STUB_DIR/short" ] || return 1
    cat "$HOSTNAME_STUB_DIR/short"
  else
    cat "$HOSTNAME_STUB_DIR/full"
  fi
}

# set_hostname SHORT [FULL]: what `hostname -s` and `hostname` print.
set_hostname() {
  mkdir -p "$HOSTNAME_STUB_DIR"
  if [ -n "$1" ]; then
    printf '%s\n' "$1" > "$HOSTNAME_STUB_DIR/short"
  else
    : > "$HOSTNAME_STUB_DIR/short"
  fi
  printf '%s\n' "${2-$1}" > "$HOSTNAME_STUB_DIR/full"
}

# make_fake_binary PATH VERSION_LINE: a script that prints VERSION_LINE.
make_fake_binary() {
  mkdir -p "$(dirname "$1")"
  printf '#!/bin/sh\necho "%s"\n' "$2" > "$1"
  chmod +x "$1"
}

teardown() {
  rm -rf "$TMP"
}

# --- detect_os ------------------------------------------------------------

@test "detect_os: ubuntu -> apt" {
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" run detect_os
  [ "$status" -eq 0 ]
  [ "$output" = "apt" ]
}

@test "detect_os: debian -> apt" {
  OS_RELEASE_FILE="$FIXTURES/os-release.debian" run detect_os
  [ "$status" -eq 0 ]
  [ "$output" = "apt" ]
}

@test "detect_os: fedora -> dnf" {
  OS_RELEASE_FILE="$FIXTURES/os-release.fedora" run detect_os
  [ "$status" -eq 0 ]
  [ "$output" = "dnf" ]
}

@test "detect_os: rocky -> dnf-el" {
  OS_RELEASE_FILE="$FIXTURES/os-release.rocky" run detect_os
  [ "$status" -eq 0 ]
  [ "$output" = "dnf-el" ]
}

@test "detect_os: arch -> pacman" {
  OS_RELEASE_FILE="$FIXTURES/os-release.arch" run detect_os
  [ "$status" -eq 0 ]
  [ "$output" = "pacman" ]
}

@test "detect_os: opensuse -> zypper" {
  OS_RELEASE_FILE="$FIXTURES/os-release.opensuse" run detect_os
  [ "$status" -eq 0 ]
  [ "$output" = "zypper" ]
}

@test "detect_os: ID_LIKE fallback (linuxmint -> apt)" {
  printf 'ID=linuxmint\nID_LIKE="ubuntu debian"\n' > "$TMP/os"
  OS_RELEASE_FILE="$TMP/os" run detect_os
  [ "$status" -eq 0 ]
  [ "$output" = "apt" ]
}

@test "detect_os: alpine is unsupported" {
  OS_RELEASE_FILE="$FIXTURES/os-release.alpine" run detect_os
  [ "$status" -eq 1 ]
  [[ "$output" == *"unsupported distro: alpine"* ]]
}

@test "detect_os: missing os-release file fails" {
  OS_RELEASE_FILE="$TMP/nope" run detect_os
  [ "$status" -ne 0 ]
}

@test "detect_os: does not pollute globals" {
  unset ID ID_LIKE NAME
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" detect_os >/dev/null
  [ -z "${ID:-}" ]
  [ -z "${ID_LIKE:-}" ]
  [ -z "${NAME:-}" ]
}

# --- arch mapping ---------------------------------------------------------

@test "cni_arch: maps machine names" {
  [ "$(FABER_ARCH=x86_64 cni_arch)" = "amd64" ]
  [ "$(FABER_ARCH=aarch64 cni_arch)" = "arm64" ]
  [ "$(FABER_ARCH=arm64 cni_arch)" = "arm64" ]
  [ "$(FABER_ARCH=armv7l cni_arch)" = "arm" ]
  [ "$(FABER_ARCH=riscv64 cni_arch)" = "riscv64" ]
  [ "$(FABER_ARCH=s390x cni_arch)" = "s390x" ]
  [ "$(FABER_ARCH=ppc64le cni_arch)" = "ppc64le" ]
}

@test "cni_arch: unknown machine fails" {
  FABER_ARCH=i686 run cni_arch
  [ "$status" -ne 0 ]
  [[ "$output" == *"unsupported architecture"* ]]
}

# --- flag parsing ---------------------------------------------------------

@test "parse_args: defaults" {
  parse_args
  [ "$DRY_RUN" = 0 ]
  [ "$CHECK" = 0 ]
}

@test "parse_args: supported flags" {
  parse_args --dry-run --check
  [ "$DRY_RUN" = 1 ]
  [ "$CHECK" = 1 ]
}

@test "script: unknown flag exits 2 with usage" {
  run bash "$SCRIPT" --bogus
  [ "$status" -eq 2 ]
  [[ "$output" == *"Usage"* ]]
}

@test "script: removed flags are rejected with exit 2" {
  for flag in --cni-version --cni-version=v1.8.0 --skip-cni --skip-containerd --no-start; do
    run bash "$SCRIPT" "$flag"
    [ "$status" -eq 2 ]
    [[ "$output" == *"unknown option: $flag"* ]]
  done
  run bash "$SCRIPT" --cni-version v1.8.0
  [ "$status" -eq 2 ]
}

@test "script: usage omits removed flags and documents new env vars" {
  run bash "$SCRIPT" --help
  [[ "$output" != *"--cni-version"* ]]
  [[ "$output" != *"--skip-"* ]]
  [[ "$output" != *"--no-start"* ]]
  [[ "$output" == *"FABER_BIN_SRC_DIR"* ]]
  [[ "$output" == *"FABER_HOME"* ]]
  [[ "$output" == *"FABER_CLI_DIR"* ]]
  [[ "$output" == *"--join"* ]]
  [[ "$output" == *"--endpoint"* ]]
  [[ "$output" == *"--mesh-cidr"* ]]
  [[ "$output" == *"--container-cidr <cidr>"* ]]
  [[ "$output" == *"--service-cidr <cidr>"* ]]
  [[ "$output" == *"10.201.0.0/16"* ]]
  [[ "$output" == *"10.200.0.0/16"* ]]
  [[ "$output" == *"--name <name>"* ]]
  [[ "$output" == *"--reset "* ]]
  [[ "$output" == *"--join <token> --reset"* ]]
  [[ "$output" == *"FABER_ETC"* ]]
  [[ "$output" == *"FABER_STATE_DIR"* ]]
  [[ "$output" == *"SYSTEMD_UNIT_DIR"* ]]
  [[ "$output" == *"CONTAINERD_CONF_DIR"* ]]
  [[ "$output" == *"CNI_CONF_DIR "*"/etc/cni/net.d"* ]]
}

@test "script: --help exits 0 with usage" {
  run bash "$SCRIPT" --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage"* ]]
}

# --- version parsing ------------------------------------------------------

@test "parse_version: containerd, docker containerd.io" {
  [ "$(parse_version 'containerd containerd.io 1.7.22 7f7fdf5fed64eb6a7caf99b3e12efcf9d60e311c')" = "1.7.22" ]
}

@test "parse_version: containerd, upstream v-prefixed" {
  [ "$(parse_version 'containerd github.com/containerd/containerd/v2 v2.0.2 c507a0257ea6462fbd6f5ba4f5c74facb04021f4')" = "2.0.2" ]
}

@test "parse_version: containerd, ubuntu package" {
  [ "$(parse_version 'containerd github.com/containerd/containerd 1.7.24 ')" = "1.7.24" ]
}

@test "parse_version: wg" {
  [ "$(parse_version 'wireguard-tools v1.0.20210914 - https://git.zx2c4.com/wireguard-tools/')" = "1.0.20210914" ]
}

@test "parse_version: nft" {
  [ "$(parse_version 'nftables v1.0.9 (Old Doc Yak #3)')" = "1.0.9" ]
}

@test "parse_version: garbage -> unknown" {
  [ "$(parse_version 'no version here')" = "unknown" ]
}

@test "component_version: missing binary -> missing" {
  PATH="$TMP/empty" run component_version wireguard
  [ "$output" = "missing" ]
}

@test "component_version: uses stub binary" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\necho "nftables v1.1.1 (Commodore Bullmoose #2)"\n' > "$TMP/bin/nft"
  chmod +x "$TMP/bin/nft"
  PATH="$TMP/bin:$PATH" run component_version nftables
  [ "$output" = "1.1.1" ]
}

@test "component_version: cni reads .version or missing" {
  run component_version cni
  [ "$output" = "missing" ]
  mkdir -p "$CNI_BIN_DIR"
  echo "v1.9.1" > "$CNI_BIN_DIR/.version"
  run component_version cni
  [ "$output" = "v1.9.1" ]
}

# --- status computation ---------------------------------------------------

@test "compute_status: missing -> installed" {
  [ "$(compute_status missing 1.7.24)" = "installed" ]
}

@test "compute_status: same -> up-to-date" {
  [ "$(compute_status 1.7.24 1.7.24)" = "up-to-date" ]
}

@test "compute_status: diff -> upgraded" {
  [ "$(compute_status 1.7.12 1.7.24)" = "upgraded" ]
}

@test "compute_status: missing after -> failed" {
  [ "$(compute_status missing missing)" = "failed" ]
}

# --- CNI ------------------------------------------------------------------

@test "install_cni: skips download when .version matches target" {
  mkdir -p "$CNI_BIN_DIR" "$TMP/bin"
  echo "v1.9.1" > "$CNI_BIN_DIR/.version"
  printf '#!/bin/sh\ntouch "%s/curl-called"\nexit 1\n' "$TMP" > "$TMP/bin/curl"
  chmod +x "$TMP/bin/curl"
  PATH="$TMP/bin:$PATH" run install_cni v1.9.1 amd64
  [ "$status" -eq 0 ]
  [[ "$output" == *"up-to-date"* ]]
  [ ! -e "$TMP/curl-called" ]
}

@test "install_cni: dry-run prints download commands without executing" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\ntouch "%s/curl-called"\nexit 1\n' "$TMP" > "$TMP/bin/curl"
  chmod +x "$TMP/bin/curl"
  DRY_RUN=1
  PATH="$TMP/bin:$PATH" run install_cni v1.9.1 amd64
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ curl -fsSL --retry 3"*"cni-plugins-linux-amd64-v1.9.1.tgz"* ]]
  [[ "$output" == *"+ tar -C $CNI_BIN_DIR -xzf"*" --no-same-owner --no-same-permissions"* ]]
  [[ "$output" == *"+ chmod -R u-s\,g-ws\,o-w $CNI_BIN_DIR"* ]]
  [ ! -e "$TMP/curl-called" ]
  [ ! -e "$CNI_BIN_DIR" ]
}

@test "verify_sha256: accepts matching checksum file with filename" {
  echo "hello" > "$TMP/f.tgz"
  hash="$(sha256sum "$TMP/f.tgz" | awk '{print $1}')"
  printf '%s  cni-plugins-linux-amd64-v1.9.1.tgz\n' "$hash" > "$TMP/f.tgz.sha256"
  run verify_sha256 "$TMP/f.tgz" "$TMP/f.tgz.sha256"
  [ "$status" -eq 0 ]
}

@test "verify_sha256: rejects mismatch" {
  echo "hello" > "$TMP/f.tgz"
  printf '%s  x.tgz\n' "0000000000000000000000000000000000000000000000000000000000000000" > "$TMP/f.tgz.sha256"
  run verify_sha256 "$TMP/f.tgz" "$TMP/f.tgz.sha256"
  [ "$status" -ne 0 ]
}

# --- end-to-end dry-run / check ------------------------------------------

make_stub_path() {
  mkdir -p "$TMP/bin"
  for cmd in apt-get dnf pacman zypper modprobe sysctl curl; do
    printf '#!/bin/sh\necho "%s $*" >> "%s/executed"\n' "$cmd" "$TMP" > "$TMP/bin/$cmd"
    chmod +x "$TMP/bin/$cmd"
  done
  stub_systemctl
  stub_ip
  stub_faber_user absent
  stub_containerd_io absent
}

# stub_systemctl: records every call except read-only is-active queries.
# `enable ... fabercp` and `try-restart fabercp` simulate fabercp creating
# identity.json when it starts (unless $TMP/no-identity exists; with the
# content of $TMP/identity-after when that exists) and, when
# $TMP/bootstrap-error exists, writing that as its bootstrap-error record.
# A start that finds $FABER_STATE_DIR/join-failed first does what fabercp does
# with it: records the marker's type, owner, mode and size in
# $TMP/marker-at-start, removes it together with identity.json and the content
# of data/, and adds a line to $TMP/discards. `stop fabercp` saves the token
# and the join setting it finds to $TMP/token-at-stop and $TMP/env-at-stop;
# try-restart does nothing while fabercp is stopped.
# `try-restart fabercp` also saves the token file fabercp would read to
# $TMP/token-at-restart.
stub_systemctl() {
  mkdir -p "$TMP/bin"
  cat > "$TMP/bin/systemctl" <<EOF
#!/bin/sh
case "\$1" in
  is-active) cat "$TMP/fabercp-state" 2>/dev/null || echo inactive; exit 0 ;;
esac
echo "systemctl \$*" >> "$TMP/executed"
case "\$*" in
  *stop*fabercp*)
    touch "$TMP/fabercp-stopped"
    cat "$FABER_ETC/join.token" > "$TMP/token-at-stop" 2>/dev/null
    grep '^FABER_JOIN_TOKEN_FILE=' "$FABER_ETC/fabercp.env" > "$TMP/env-at-stop" 2>/dev/null
    [ -e "$FABER_STATE_DIR/join-failed" ] && touch "$TMP/marker-before-stop"
    exit 0
    ;;
  *try-restart*fabercp*) [ -e "$TMP/fabercp-stopped" ] && exit 0 ;;
esac
case "\$*" in
  *enable*fabercp* | *try-restart*fabercp*)
    rm -f "$TMP/fabercp-stopped"
    if [ -e "$FABER_STATE_DIR/join-failed" ] || [ -L "$FABER_STATE_DIR/join-failed" ]; then
      stat -c '%F %U:%G %a %s' "$FABER_STATE_DIR/join-failed" > "$TMP/marker-at-start"
      rm -f "$FABER_STATE_DIR/join-failed" "$FABER_STATE_DIR/identity.json"
      rm -rf "$FABER_STATE_DIR/data"/*
      echo discarded >> "$TMP/discards"
    fi
    if [ -e "$TMP/identity-after" ]; then
      mkdir -p "$FABER_STATE_DIR" && cat "$TMP/identity-after" > "$FABER_STATE_DIR/identity.json"
    elif [ ! -e "$TMP/no-identity" ]; then
      mkdir -p "$FABER_STATE_DIR" && : > "$FABER_STATE_DIR/identity.json"
    fi
    if [ -e "$TMP/bootstrap-error" ] || [ -L "$TMP/bootstrap-error" ]; then
      mkdir -p "$FABER_STATE_DIR"
      rm -f "$FABER_STATE_DIR/bootstrap-error"
      cp -P "$TMP/bootstrap-error" "$FABER_STATE_DIR/bootstrap-error"
    fi
    ;;
esac
case "\$*" in
  *try-restart*fabercp*) cat "$FABER_ETC/join.token" > "$TMP/token-at-restart" 2>/dev/null ;;
esac
exit 0
EOF
  chmod +x "$TMP/bin/systemctl"
}

# stub_ip: `ip -4 route get 1.1.1.1` prints $TMP/ip-route (default fixture has
# src 203.0.113.7); `ip link show dev faber0` succeeds when $TMP/faber0 exists.
stub_ip() {
  mkdir -p "$TMP/bin"
  cp "$FIXTURES/ip-route-get" "$TMP/ip-route"
  cat > "$TMP/bin/ip" <<EOF
#!/bin/sh
echo "ip \$*" >> "$TMP/ip-calls"
case "\$*" in
  "-4 route get 1.1.1.1")
    { [ -e "$FABER_ETC" ] && echo etc=yes || echo etc=no
      grep -q 'apt-get.*install' "$TMP/executed" 2>/dev/null && echo apt=yes || echo apt=no
    } >> "$TMP/ip-context"
    [ -f "$TMP/ip-route" ] && { cat "$TMP/ip-route"; exit 0; }
    echo "RTNETLINK answers: Network is unreachable" >&2
    exit 2
    ;;
  *link*show*faber0*) [ -e "$TMP/faber0" ] && { echo "7: faber0: <POINTOPOINT,NOARP,UP>"; exit 0; }; exit 1 ;;
esac
exit 1
EOF
  chmod +x "$TMP/bin/ip"
}

# make_faber_cli_stub: source faber binary (installed as the CLI) that records
# its argv. health fails while $TMP/unhealthy exists; cluster init reports
# already_initialized when $TMP/init-already exists; cluster status reports
# joined when $TMP/joined exists; while $TMP/reject-invite exists it removes
# identity.json first, as fabercp does when the leader rejects the invite.
make_faber_cli_stub() {
  mkdir -p "$FABER_BIN_SRC_DIR"
  cat > "$FABER_BIN_SRC_DIR/faber" <<EOF
#!/bin/sh
[ "\$1" = --version ] && { echo "faber 0.1.0"; exit 0; }
echo "faber \$*" >> "$TMP/executed"
case "\$*" in
  health) [ -e "$TMP/unhealthy" ] && exit 1; echo "status   ok" ;;
  "cluster init")
    if [ -e "$TMP/init-already" ]; then
      echo "error: already_initialized: this node is already part of a cluster" >&2
      exit 1
    fi
    if [ -e "$TMP/init-fails" ]; then echo "error: internal: boom" >&2; exit 1; fi
    echo initialized
    ;;
  "-o json cluster status")
    [ -e "$TMP/reject-invite" ] && rm -f "$FABER_STATE_DIR/identity.json"
    if [ -e "$TMP/joined" ]; then printf '{\n  "joined": true\n}\n'; else printf '{\n  "joined": false\n}\n'; fi
    ;;
esac
exit 0
EOF
  chmod +x "$FABER_BIN_SRC_DIR/faber"
}

# stub_pkg_versions: containerd/wg/nft on PATH so a real run sees them installed.
stub_pkg_versions() {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\necho "containerd github.com/containerd/containerd %s "\n' "${1:-2.0.2}" > "$TMP/bin/containerd"
  printf '#!/bin/sh\necho "wireguard-tools v1.0.20210914 - https://git.zx2c4.com/wireguard-tools/"\n' > "$TMP/bin/wg"
  printf '#!/bin/sh\necho "nftables v1.0.9 (Old Doc Yak #3)"\n' > "$TMP/bin/nft"
  chmod +x "$TMP/bin/containerd" "$TMP/bin/wg" "$TMP/bin/nft"
}

# need_root_faber_account: real runs chown to faber, so they need root and a
# real faber account in the (disposable) test container.
need_root_faber_account() {
  [ "$(id -u)" = 0 ] || skip "needs root (real chown to faber)"
  if ! id -u faber >/dev/null 2>&1; then
    if command -v adduser >/dev/null 2>&1 && command -v addgroup >/dev/null 2>&1 &&
      addgroup -S faber >/dev/null 2>&1; then
      adduser -S -D -H -G faber -s /sbin/nologin faber >/dev/null 2>&1
    else
      groupadd -r faber && useradd -r -g faber -M -s /usr/sbin/nologin faber
    fi
  fi
}

# setup_full_run [systemd]: stubs for a real (non-dry) run as root.
setup_full_run() {
  need_root_faber_account
  make_stub_path
  stub_faber_user present
  stub_pkg_versions
  make_faber_cli_stub
  mkdir -p "$CNI_BIN_DIR"
  echo "v1.9.1" > "$CNI_BIN_DIR/.version"
  if [ "${1:-}" = systemd ]; then mkdir -p "$SYSTEMD_RUN_DIR"; fi
}

install_run() {
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" "$@"
}

# hide_ip [provide]: take `ip` off PATH (the stub moves aside and the sbin
# dirs are dropped). With "provide", the apt-get stub puts it back when it
# installs packages, like iproute2 would.
hide_ip() {
  mv "$TMP/bin/ip" "$TMP/ip.later"
  NOIP_PATH="$TMP/bin:/usr/local/bin:/usr/bin:/bin"
  if PATH="$NOIP_PATH" command -v ip >/dev/null 2>&1; then
    skip "ip is installed outside the sbin dirs on this host"
  fi
  cat > "$TMP/bin/apt-get" <<EOF
#!/bin/sh
echo "apt-get \$*" >> "$TMP/executed"
case "\$*" in
  *install*) [ "${1:-}" = provide ] && cp "$TMP/ip.later" "$TMP/bin/ip" ;;
esac
exit 0
EOF
  chmod +x "$TMP/bin/apt-get"
}

install_run_noip() {
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$NOIP_PATH" run bash "$SCRIPT" "$@"
}

count_calls() { grep -cF -- "$1" "$TMP/executed" 2>/dev/null || true; }

# stub_faber_user present|absent|wronggroup: stub `id` and `getent` for the
# faber account ("wronggroup": user exists, primary group 'users').
stub_faber_user() {
  local rc=2 group=faber
  case "$1" in
    present) rc=0 ;;
    wronggroup) rc=0 group=users ;;
  esac
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\n[ %s = 0 ] || exit %s\n[ "$1" = -gn ] && echo %s\n[ "$1" = -u ] && echo 990\n[ "$1" = -g ] && echo 989\nexit 0\n' \
    "$rc" "$rc" "$group" > "$TMP/bin/id"
  printf '#!/bin/sh\nexit %s\n' "$rc" > "$TMP/bin/getent"
  chmod +x "$TMP/bin/id" "$TMP/bin/getent"
}

# stub_containerd_io present|absent: stub dpkg-query and rpm.
stub_containerd_io() {
  mkdir -p "$TMP/bin"
  if [ "$1" = present ]; then
    printf '#!/bin/sh\nprintf "install ok installed"\n' > "$TMP/bin/dpkg-query"
    printf '#!/bin/sh\necho containerd.io-1.7.22-3.1.el9.x86_64\n' > "$TMP/bin/rpm"
  else
    printf '#!/bin/sh\nexit 1\n' > "$TMP/bin/dpkg-query"
    printf '#!/bin/sh\necho "package containerd.io is not installed"\nexit 1\n' > "$TMP/bin/rpm"
  fi
  chmod +x "$TMP/bin/dpkg-query" "$TMP/bin/rpm"
}

@test "dry-run (apt): prints apt-get install and executes nothing" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ env DEBIAN_FRONTEND=noninteractive apt-get update"* ]]
  [[ "$output" == *"apt-get install -y --no-install-recommends containerd wireguard-tools nftables ca-certificates curl tar"* ]]
  [[ "$output" == *"cni-plugins-linux-amd64-v1.9.1.tgz"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$CNI_BIN_DIR" ]
}

@test "dry-run (fedora): dnf install then upgrade" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.fedora" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ dnf install -y containerd wireguard-tools nftables "*"tar"* ]]
  [[ "$output" == *"+ dnf upgrade -y containerd wireguard-tools nftables"* ]]
  [ ! -e "$TMP/executed" ]
}

@test "dry-run (rocky): docker-ce repo and containerd.io" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.rocky" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"dnf-plugins-core"* ]]
  [[ "$output" == *"https://download.docker.com/linux/centos/docker-ce.repo"* ]]
  [[ "$output" == *"containerd.io wireguard-tools nftables"* ]]
  [ ! -e "$TMP/executed" ]
}

@test "dry-run (arch): pacman full sync" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.arch" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ pacman -Syu --needed --noconfirm containerd wireguard-tools nftables curl tar"* ]]
}

@test "dry-run (opensuse): zypper in then up" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.opensuse" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ zypper -n in containerd wireguard-tools nftables curl tar"* ]]
  [[ "$output" == *"+ zypper -n up containerd wireguard-tools nftables"* ]]
}

@test "dry-run: cni up-to-date when .version matches" {
  make_stub_path
  mkdir -p "$CNI_BIN_DIR"
  echo "v1.9.1" > "$CNI_BIN_DIR/.version"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" != *"cni-plugins-linux-amd64"* ]]
  echo "$output" | grep -Eq '^cni +\| +v1\.9\.1 +\| +v1\.9\.1 +\| +up-to-date'
}

@test "--check reports OS and versions without changes" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --check
  [ "$status" -eq 0 ]
  [[ "$output" == *"ubuntu"* ]]
  [[ "$output" == *"apt"* ]]
  [[ "$output" == *"containerd"* ]]
  [[ "$output" == *"wireguard"* ]]
  [[ "$output" == *"nftables"* ]]
  [[ "$output" == *"cni"* ]]
  echo "$output" | grep -Eq '^fabercp +\| +missing'
  echo "$output" | grep -Eq '^faber +\| +missing'
  [ ! -e "$TMP/executed" ]
}

@test "--check does not die when source binaries are missing" {
  make_stub_path
  FABER_BIN_SRC_DIR="$TMP/nowhere" OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" \
    PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --check
  [ "$status" -eq 0 ]
  [[ "$output" == *"$TMP/nowhere"* ]]
  echo "$output" | grep -Eq '^fabercp +\| '
  echo "$output" | grep -Eq '^faber +\| '
}

@test "--check reports source version when identical, 'present' when different" {
  mkdir -p "$FABER_HOME"
  cp "$TMP/src/fabercp" "$FABER_HOME/fabercp"
  make_fake_binary "$FABER_CLI_DIR/faber" "faber 0.3.0"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" run bash "$SCRIPT" --check
  [ "$status" -eq 0 ]
  echo "$output" | grep -Eq '^fabercp +\| +0\.1\.0'
  echo "$output" | grep -Eq '^faber +\| +present'
}

@test "--check on unsupported distro still exits 0" {
  OS_RELEASE_FILE="$FIXTURES/os-release.alpine" run bash "$SCRIPT" --check
  [ "$status" -eq 0 ]
  [[ "$output" == *"unsupported"* ]]
}

@test "require_root: non-root fails outside dry-run/check" {
  FABER_EUID=1000 run require_root
  [ "$status" -ne 0 ]
  [[ "$output" == *"root"* ]]
}

@test "require_root: non-root allowed in dry-run" {
  DRY_RUN=1 FABER_EUID=1000 run require_root
  [ "$status" -eq 0 ]
}

# --- file helpers ---------------------------------------------------------

@test "write_if_changed: writes new file, reports unchanged on second call" {
  run write_if_changed "$TMP/a.conf" "overlay"
  [ "$status" -eq 0 ]
  [ "$(cat "$TMP/a.conf")" = "overlay" ]
  run write_if_changed "$TMP/a.conf" "overlay"
  [[ "$output" == *"unchanged"* ]]
}

# --- removed options ------------------------------------------------------

@test "systemd_present: follows SYSTEMD_RUN_DIR" {
  run systemd_present
  [ "$status" -ne 0 ]
  mkdir -p "$SYSTEMD_RUN_DIR"
  run systemd_present
  [ "$status" -eq 0 ]
}

# --- containerd package choice -------------------------------------------

@test "containerd_package: apt and dnf keep Docker's containerd.io when installed, dnf-el always uses it" {
  local family
  for family in apt dnf; do
    stub_containerd_io present
    PATH="$TMP/bin:$PATH" run containerd_package "$family"
    [ "$output" = containerd.io ]
    stub_containerd_io absent
    PATH="$TMP/bin:$PATH" run containerd_package "$family"
    [ "$output" = containerd ]
  done
  stub_containerd_io absent
  PATH="$TMP/bin:$PATH" run containerd_package dnf-el
  [ "$output" = containerd.io ]
  PATH="$TMP/bin:$PATH" run containerd_package pacman
  [ "$output" = containerd ]
  PATH="$TMP/bin:$PATH" run containerd_package zypper
  [ "$output" = containerd ]
}

@test "dry-run (apt): installed containerd.io is requested instead of containerd" {
  make_stub_path
  stub_containerd_io present
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"apt-get install -y --no-install-recommends containerd.io wireguard-tools nftables"* ]]
  [[ "$output" != *" containerd wireguard-tools"* ]]
  [[ "$output" == *"containerd package: containerd.io"* ]]
}

@test "dry-run (fedora): installed containerd.io is requested and upgraded" {
  make_stub_path
  stub_containerd_io present
  OS_RELEASE_FILE="$FIXTURES/os-release.fedora" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ dnf install -y containerd.io wireguard-tools nftables"* ]]
  [[ "$output" == *"+ dnf upgrade -y containerd.io wireguard-tools nftables"* ]]
}

# --- faber binaries / user / home -----------------------------------------

@test "bin_src_dir: defaults to the script directory" {
  unset FABER_BIN_SRC_DIR
  [ "$(bin_src_dir)" = "$REPO_ROOT/src" ]
}

@test "bin_src_dir: FABER_BIN_SRC_DIR override" {
  [ "$(bin_src_dir)" = "$TMP/src" ]
}

@test "verify_binaries: ok when both present" {
  run verify_binaries
  [ "$status" -eq 0 ]
}

@test "verify_binaries: names the missing path" {
  rm "$TMP/src/faber"
  run verify_binaries
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/src/faber"* ]]
}

@test "verify_binaries: rejects a directory in place of a binary" {
  rm "$TMP/src/fabercp"
  mkdir "$TMP/src/fabercp"
  run verify_binaries
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/src/fabercp"* ]]
}

@test "dry-run: missing source binary dies before any package command" {
  make_stub_path
  FABER_BIN_SRC_DIR="$TMP/nowhere" OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" \
    PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/nowhere/fabercp"* ]]
  [[ "$output" != *"apt-get"* ]]
  [ ! -e "$TMP/executed" ]
}

@test "dry-run: symlinked FABER_HOME dies before any package command" {
  make_stub_path
  mkdir -p "$TMP/real" "$TMP/opt"
  ln -s "$TMP/real" "$FABER_HOME"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"symlink"* ]]
  [[ "$output" != *"apt-get"* ]]
}

@test "check_faber_home: non-directory dies" {
  mkdir -p "$TMP/opt"
  : > "$FABER_HOME"
  run check_faber_home
  [ "$status" -eq 1 ]
  [[ "$output" == *"not a directory"* ]]
}

@test "check_faber_account: absent user or user with faber group is fine" {
  stub_faber_user absent
  PATH="$TMP/bin:$PATH" run check_faber_account
  [ "$status" -eq 0 ]
  stub_faber_user present
  PATH="$TMP/bin:$PATH" run check_faber_account
  [ "$status" -eq 0 ]
}

@test "check_faber_account: faber user with another primary group dies" {
  stub_faber_user wronggroup
  PATH="$TMP/bin:$PATH" run check_faber_account
  [ "$status" -eq 1 ]
  [[ "$output" == *"primary group is 'users'"* ]]
}

@test "dry-run: faber user without faber group dies before package commands" {
  make_stub_path
  stub_faber_user wronggroup
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"primary group"* ]]
  [[ "$output" != *"apt-get"* ]]
  [[ "$output" != *"useradd"* ]]
}

@test "ensure_faber_user: dry-run prints useradd when user absent" {
  stub_faber_user absent
  DRY_RUN=1
  PATH="$TMP/bin:$PATH" run ensure_faber_user
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ useradd --system --home-dir $FABER_HOME --no-create-home --shell "*"nologin --user-group faber"* ]]
}

@test "ensure_faber_user: uses existing group" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\nexit 1\n' > "$TMP/bin/id"
  printf '#!/bin/sh\n[ "$1 $2" = "group faber" ] && exit 0\nexit 2\n' > "$TMP/bin/getent"
  chmod +x "$TMP/bin/id" "$TMP/bin/getent"
  DRY_RUN=1
  PATH="$TMP/bin:$PATH" run ensure_faber_user
  [[ "$output" == *"-g faber faber"* ]]
  [[ "$output" != *"--user-group"* ]]
}

@test "ensure_faber_user: skips useradd when user present" {
  stub_faber_user present
  DRY_RUN=1
  PATH="$TMP/bin:$PATH" run ensure_faber_user
  [ "$status" -eq 0 ]
  [[ "$output" != *"useradd"* ]]
}

@test "nologin_shell: returns an absolute nologin path" {
  run nologin_shell
  [[ "$output" == /*nologin ]]
}

@test "ensure_faber_home: dry-run prints install -d with owner and mode" {
  DRY_RUN=1
  run ensure_faber_home
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ install -d -o faber -g faber -m 0750 $FABER_HOME"* ]]
  [ ! -e "$FABER_HOME" ]
}

# --- binary state / version / status --------------------------------------

@test "binary_state: missing / same / perms / differs" {
  local src="$TMP/src/faber" dst="$TMP/copy" u g
  u="$(id -u)" g="$(id -g)"
  [ "$(binary_state "$src" "$dst" "$u" "$g" 0755)" = missing ]
  cp "$src" "$dst"
  chmod 0755 "$dst"
  [ "$(binary_state "$src" "$dst" "$u" "$g" 0755)" = same ]
  chmod 0700 "$dst"
  [ "$(binary_state "$src" "$dst" "$u" "$g" 0755)" = perms ]
  chmod 0755 "$dst"
  echo x >> "$dst"
  [ "$(binary_state "$src" "$dst" "$u" "$g" 0755)" = differs ]
}

@test "binary_state: symlink or directory at dest is never 'same'" {
  local src="$TMP/src/faber" u g
  u="$(id -u)" g="$(id -g)"
  cp "$src" "$TMP/identical"
  chmod 0755 "$TMP/identical"
  ln -s "$TMP/identical" "$TMP/link"
  [ "$(binary_state "$src" "$TMP/link" "$u" "$g" 0755)" = differs ]
  ln -s "$TMP/nowhere" "$TMP/dangling"
  [ "$(binary_state "$src" "$TMP/dangling" "$u" "$g" 0755)" = differs ]
  mkdir "$TMP/adir"
  [ "$(binary_state "$src" "$TMP/adir" "$u" "$g" 0755)" = differs ]
}

@test "binary_state: unsearchable directory -> unknown (non-root)" {
  [ "$(id -u)" = 0 ] && skip "root can search any directory"
  mkdir -p "$TMP/locked"
  cp "$TMP/src/faber" "$TMP/locked/faber"
  chmod 000 "$TMP/locked"
  run binary_state "$TMP/src/faber" "$TMP/locked/faber" "$(id -u)" "$(id -g)" 0755
  chmod 755 "$TMP/locked"
  [ "$output" = unknown ]
}

@test "binary_state: sha256sum fallback when cmp is absent" {
  have() { [ "$1" != cmp ] && command -v "$1" >/dev/null 2>&1; }
  local u g
  u="$(id -u)" g="$(id -g)"
  cp "$TMP/src/faber" "$TMP/copy"
  chmod 0755 "$TMP/copy"
  [ "$(binary_state "$TMP/src/faber" "$TMP/copy" "$u" "$g" 0755)" = same ]
  echo x >> "$TMP/copy"
  [ "$(binary_state "$TMP/src/faber" "$TMP/copy" "$u" "$g" 0755)" = differs ]
}

@test "binary_version: version comes from the source binary, never the installed one" {
  [ "$(binary_version missing "$TMP/src/fabercp")" = missing ]
  [ "$(binary_version unknown "$TMP/src/fabercp")" = unknown ]
  [ "$(binary_version same "$TMP/src/fabercp")" = 0.1.0 ]
  [ "$(binary_version perms "$TMP/src/fabercp")" = 0.1.0 ]
  [ "$(binary_version differs "$TMP/src/fabercp")" = present ]
}

@test "binary_status: real and dry-run mappings" {
  [ "$(binary_status missing 0.1.0)" = installed ]
  [ "$(binary_status differs 0.1.0)" = upgraded ]
  [ "$(binary_status perms 0.1.0)" = upgraded ]
  [ "$(binary_status same 0.1.0)" = up-to-date ]
  [ "$(binary_status missing missing)" = failed ]
  [ "$(binary_status unknown 0.1.0)" = unknown ]
  DRY_RUN=1
  [ "$(binary_status missing missing)" = would-install ]
  [ "$(binary_status differs present)" = would-upgrade ]
  [ "$(binary_status perms 0.1.0)" = would-upgrade ]
  [ "$(binary_status same 0.1.0)" = up-to-date ]
  [ "$(binary_status unknown unknown)" = unknown ]
}

@test "component_version: fabercp/faber compare against source, not --version of installed" {
  [ "$(component_version fabercp)" = missing ]
  [ "$(component_version faber)" = missing ]
  mkdir -p "$FABER_HOME"
  cp "$TMP/src/fabercp" "$FABER_HOME/fabercp"
  make_fake_binary "$FABER_CLI_DIR/faber" "faber 0.2.0"
  [ "$(component_version fabercp)" = 0.1.0 ]
  [ "$(component_version faber)" = present ]
}

# --- install_binary -------------------------------------------------------

@test "install_binary: dry-run prints staged copy and mv -T, no chown/chmod" {
  DRY_RUN=1
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" root root 0755
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ install -o root -g root -m 0755 $TMP/src/faber $TMP/usr/local/.faber-stage."*"/faber"* ]]
  [[ "$output" == *"+ mv -fT $TMP/usr/local/.faber-stage."*"/faber $FABER_CLI_DIR/faber"* ]]
  [[ "$output" != *"chown"* ]]
  [[ "$output" != *"chmod"* ]]
  [ ! -e "$FABER_CLI_DIR/faber" ]
}

@test "install_binary: same file -> no copy" {
  mkdir -p "$FABER_CLI_DIR"
  cp "$TMP/src/faber" "$FABER_CLI_DIR/faber"
  chmod 0755 "$FABER_CLI_DIR/faber"
  DRY_RUN=1
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ "$status" -eq 0 ]
  [[ "$output" == *"up-to-date"* ]]
  [[ "$output" != *"+ "* ]]
}

@test "install_binary: dry-run with unknown state claims no copy" {
  DRY_RUN=1
  run install_binary "$TMP/src/fabercp" "$FABER_HOME/fabercp" faber faber 0750 unknown
  [ "$status" -eq 0 ]
  [[ "$output" == *"root"* ]]
  [[ "$output" != *"+ install"* ]]
  [[ "$output" != *"+ mv"* ]]
}

@test "install_binary: real run installs a missing target and leaves no stage" {
  mkdir -p "$FABER_CLI_DIR"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ "$status" -eq 0 ]
  cmp -s "$TMP/src/faber" "$FABER_CLI_DIR/faber"
  [ "$(stat -c %a "$FABER_CLI_DIR/faber")" = 755 ]
  [ "$(ls -A "$FABER_CLI_DIR")" = "faber" ]
  [ "$(ls -A "$TMP/usr/local")" = "bin" ]
}

@test "install_binary: real run replaces a differing target" {
  mkdir -p "$FABER_CLI_DIR"
  make_fake_binary "$FABER_CLI_DIR/faber" "faber 0.0.1"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ "$status" -eq 0 ]
  cmp -s "$TMP/src/faber" "$FABER_CLI_DIR/faber"
  [ "$(ls -A "$FABER_CLI_DIR")" = "faber" ]
}

@test "install_binary: real run repairs wrong mode" {
  mkdir -p "$FABER_CLI_DIR"
  cp "$TMP/src/faber" "$FABER_CLI_DIR/faber"
  chmod 0777 "$FABER_CLI_DIR/faber"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ "$status" -eq 0 ]
  [ "$(stat -c %a "$FABER_CLI_DIR/faber")" = 755 ]
}

@test "install_binary: dest symlink to an outside file is replaced, target untouched" {
  mkdir -p "$FABER_CLI_DIR" "$TMP/outside"
  echo precious > "$TMP/outside/file"
  chmod 0600 "$TMP/outside/file"
  ln -s "$TMP/outside/file" "$FABER_CLI_DIR/faber"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ "$status" -eq 0 ]
  [ ! -L "$FABER_CLI_DIR/faber" ]
  [ -f "$FABER_CLI_DIR/faber" ]
  cmp -s "$TMP/src/faber" "$FABER_CLI_DIR/faber"
  [ "$(cat "$TMP/outside/file")" = precious ]
  [ "$(stat -c %a "$TMP/outside/file")" = 600 ]
}

@test "install_binary: dest symlink to an outside dir never writes there" {
  mkdir -p "$FABER_CLI_DIR" "$TMP/outside"
  chmod 0700 "$TMP/outside"
  ln -s "$TMP/outside" "$FABER_CLI_DIR/faber"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ -z "$(ls -A "$TMP/outside")" ]
  [ "$(stat -c %a "$TMP/outside")" = 700 ]
  # GNU mv -T replaces the link; busybox mv refuses. Either is safe.
  if [ "$status" -eq 0 ]; then
    [ ! -L "$FABER_CLI_DIR/faber" ]
    [ -f "$FABER_CLI_DIR/faber" ]
  fi
  [ "$(ls -A "$TMP/usr/local")" = "bin" ]
}

@test "install_binary: real directory at dest dies" {
  mkdir -p "$FABER_CLI_DIR/faber"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ "$status" -eq 1 ]
  [[ "$output" == *"directory"* ]]
  [ -d "$FABER_CLI_DIR/faber" ]
}

@test "install_binary: planted symlinks at temp-like names are harmless" {
  mkdir -p "$FABER_CLI_DIR" "$TMP/outside"
  ln -s "$TMP/outside/pwn" "$FABER_CLI_DIR/.faber.faber-tmp"
  ln -s "$TMP/outside" "$FABER_CLI_DIR/.faber-stage.AAAAAA"
  ln -s "$TMP/outside" "$FABER_CLI_DIR/faber.tmp"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  [ "$status" -eq 0 ]
  [ -z "$(ls -A "$TMP/outside")" ]
  cmp -s "$TMP/src/faber" "$FABER_CLI_DIR/faber"
}

@test "install_binary: refuses a group/world-writable staging parent" {
  mkdir -p "$FABER_CLI_DIR"
  chmod 0777 "$TMP/usr/local"
  run install_binary "$TMP/src/faber" "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755
  chmod 0755 "$TMP/usr/local"
  [ "$status" -eq 1 ]
  [[ "$output" == *"writable"* ]]
  [ ! -e "$FABER_CLI_DIR/faber" ]
}

@test "installed fabercp is never executed (version, state, install, dry-run, check)" {
  make_stub_path
  stub_faber_user present
  mkdir -p "$FABER_HOME" "$FABER_CLI_DIR"
  printf '#!/bin/sh\ntouch "%s/pwned"\necho "fabercp 9.9.9"\n' "$TMP" > "$FABER_HOME/fabercp"
  printf '#!/bin/sh\ntouch "%s/pwned"\necho "faber 9.9.9"\n' "$TMP" > "$FABER_CLI_DIR/faber"
  chmod 0755 "$FABER_HOME/fabercp" "$FABER_CLI_DIR/faber"
  run component_version fabercp
  [ "$output" = present ]
  run component_version faber
  [ "$output" = present ]
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --check
  [ "$status" -eq 0 ]
  run install_binary "$TMP/src/fabercp" "$FABER_HOME/fabercp" "$(id -u)" "$(id -g)" 0750
  [ "$status" -eq 0 ]
  [ ! -e "$TMP/pwned" ]
}

@test "install_faber_binaries: dry-run prints no redundant mkdir for FABER_HOME" {
  stub_faber_user present
  DRY_RUN=1
  PATH="$TMP/bin:$PATH" run install_faber_binaries
  [ "$status" -eq 0 ]
  [[ "$output" != *"+ install -d -m 0755 $FABER_HOME"* ]]
  [[ "$output" == *"+ install -d -m 0755 $FABER_CLI_DIR"* ]]
}

# --- end-to-end dry-run for binaries ---------------------------------------

@test "dry-run: binaries missing -> staged copies and would-install" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ useradd --system"* ]]
  [[ "$output" == *"+ install -d -o faber -g faber -m 0750 $FABER_HOME"* ]]
  [[ "$output" == *"+ install -o faber -g faber -m 0750 $TMP/src/fabercp $TMP/opt/.faber-stage."* ]]
  [[ "$output" == *"+ install -o root -g root -m 0755 $TMP/src/faber $TMP/usr/local/.faber-stage."* ]]
  echo "$output" | grep -Eq '^fabercp +\| +missing +\| +missing +\| +would-install'
  echo "$output" | grep -Eq '^faber +\| +missing +\| +missing +\| +would-install'
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_HOME" ]
}

@test "dry-run: identical -> up-to-date; wrong owner or content -> would-upgrade" {
  [ "$(id -u)" = 0 ] || skip "needs root to own files as root:root"
  make_stub_path
  stub_faber_user present
  mkdir -p "$FABER_HOME" "$FABER_CLI_DIR"
  cp "$TMP/src/faber" "$FABER_CLI_DIR/faber"
  chmod 0755 "$FABER_CLI_DIR/faber"
  cp "$TMP/src/fabercp" "$FABER_HOME/fabercp"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" != *"useradd"* ]]
  [[ "$output" != *"+ install -o root -g root -m 0755 $TMP/src/faber "* ]]
  echo "$output" | grep -Eq '^faber +\| +0\.1\.0 +\| +0\.1\.0 +\| +up-to-date'
  # content identical but owned by root rather than faber
  echo "$output" | grep -Eq '^fabercp +\| +0\.1\.0 +\| +0\.1\.0 +\| +would-upgrade'
  make_fake_binary "$FABER_CLI_DIR/faber" "faber 0.0.9"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  echo "$output" | grep -Eq '^faber +\| +present +\| +present +\| +would-upgrade'
}

@test "dry-run (non-root): unsearchable FABER_HOME -> unknown, no copy claimed" {
  [ "$(id -u)" = 0 ] && skip "root can search any directory"
  make_stub_path
  stub_faber_user present
  mkdir -p "$FABER_HOME"
  cp "$TMP/src/fabercp" "$FABER_HOME/fabercp"
  chmod 000 "$FABER_HOME"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  chmod 755 "$FABER_HOME"
  [ "$status" -eq 0 ]
  echo "$output" | grep -Eq '^fabercp +\| +unknown +\| +unknown +\| +unknown'
  [[ "$output" != *"+ install -o faber -g faber -m 0750 $TMP/src/fabercp"* ]]
  [[ "$output" == *"as root"* ]]
}

# --- mesh flags -----------------------------------------------------------

@test "parse_args: --join/--endpoint/--mesh-cidr, space and = forms" {
  parse_args --join "$TOKEN" --endpoint 198.51.100.2:51821
  [ "$JOIN_REQUESTED" = 1 ]
  [ "$JOIN_TOKEN" = "$TOKEN" ]
  [ "$ENDPOINT_ARG" = "198.51.100.2:51821" ]
  JOIN_REQUESTED=0 JOIN_TOKEN=""
  parse_args --mesh-cidr=10.50.0.0/16 --endpoint=198.51.100.2
  [ "$JOIN_REQUESTED" = 0 ]
  [ "$MESH_CIDR_ARG" = "10.50.0.0/16" ]
  [ "$ENDPOINT_ARG" = "198.51.100.2:51820" ]
}

@test "script: flags without a value exit 2" {
  for flag in --join --endpoint --mesh-cidr --container-cidr --service-cidr --name; do
    run bash "$SCRIPT" --dry-run "$flag"
    [ "$status" -eq 2 ]
    [[ "$output" == *"$flag needs a value"* ]]
    run bash "$SCRIPT" "$flag" --dry-run
    [ "$status" -eq 2 ]
    run bash "$SCRIPT" --dry-run "$flag="
    [ "$status" -eq 2 ]
  done
}

@test "script: malformed join tokens exit 2 without echoing the value" {
  local bad
  for bad in "faber2.abc" "faber1." "faber1.abc+def" "faber1.abc=" "xfaber1.abc" "faber1.a b" "SECRETVALUE"; do
    run bash "$SCRIPT" --dry-run --join "$bad"
    [ "$status" -eq 2 ]
    [[ "$output" == *"invalid join token"* ]]
    [[ "$output" != *"$bad"* ]]
  done
}

@test "script: a stray token-shaped argument is not echoed" {
  run bash "$SCRIPT" --dry-run "$TOKEN"
  [ "$status" -eq 2 ]
  [[ "$output" != *"$TOKEN"* ]]
  [[ "$output" == *"--join"* ]]
}

@test "parse_args: --join - reads the token from stdin" {
  parse_args --join - <<<"$TOKEN"
  [ "$JOIN_REQUESTED" = 1 ]
  [ "$JOIN_TOKEN" = "$TOKEN" ]
}

@test "parse_args: --join - tolerates CRLF and no trailing newline" {
  parse_args --join - < <(printf '%s\r\n' "$TOKEN")
  [ "$JOIN_TOKEN" = "$TOKEN" ]
  JOIN_TOKEN=""
  parse_args --join - < <(printf '%s' "$TOKEN")
  [ "$JOIN_TOKEN" = "$TOKEN" ]
}

@test "script: --join - with empty stdin exits 2" {
  run bash "$SCRIPT" --dry-run --join - </dev/null
  [ "$status" -eq 2 ]
  [[ "$output" == *"join token"* ]]
}

@test "normalize_endpoint: adds the default port, accepts IPv4 and [IPv6]" {
  [ "$(normalize_endpoint 198.51.100.2)" = "198.51.100.2:51820" ]
  [ "$(normalize_endpoint 198.51.100.2:4000)" = "198.51.100.2:4000" ]
  [ "$(normalize_endpoint '[2001:db8::1]')" = "[2001:db8::1]:51820" ]
  [ "$(normalize_endpoint '[2001:db8::1]:51821')" = "[2001:db8::1]:51821" ]
}

@test "normalize_endpoint: rejects junk" {
  local bad
  for bad in "" "host name" "1.2.3.4:0" "1.2.3.4:70000" "256.1.1.1" "1.2.3" "2001:db8::1" \
    "1.2.3.4:51820:1" "1.2.3.4
FABER_X=1" "example.com" "1.2.3.4:"; do
    run normalize_endpoint "$bad"
    [ "$status" -ne 0 ]
  done
}

@test "script: bad --endpoint or --mesh-cidr exits 2" {
  run bash "$SCRIPT" --dry-run --endpoint 'not an ip'
  [ "$status" -eq 2 ]
  [[ "$output" == *"--endpoint"* ]]
  run bash "$SCRIPT" --dry-run --mesh-cidr 10.0.0.0/33
  [ "$status" -eq 2 ]
  [[ "$output" == *"--mesh-cidr"* ]]
}

@test "valid_mesh_cidr: IPv4 CIDRs only" {
  valid_mesh_cidr 10.99.0.0/16
  valid_mesh_cidr 192.168.10.0/24
  ! valid_mesh_cidr 10.99.0.0
  ! valid_mesh_cidr 10.99.0.0/33
  ! valid_mesh_cidr 300.0.0.0/8
  ! valid_mesh_cidr fd00::/64
  ! valid_mesh_cidr "10.0.0.0/8 x"
}

# --- endpoint autodetect --------------------------------------------------

@test "detect_endpoint: uses the src address of ip -4 route get" {
  stub_ip
  PATH="$TMP/bin:$PATH" run detect_endpoint
  [ "$status" -eq 0 ]
  [ "$output" = "203.0.113.7:51820" ]
}

@test "detect_endpoint: fails when the route has no src or ip fails" {
  stub_ip
  cp "$FIXTURES/ip-route-get.nosrc" "$TMP/ip-route"
  PATH="$TMP/bin:$PATH" run detect_endpoint
  [ "$status" -ne 0 ]
  rm "$TMP/ip-route"
  PATH="$TMP/bin:$PATH" run detect_endpoint
  [ "$status" -ne 0 ]
}

@test "dry-run: new mesh dies with a clear message when autodetect fails" {
  make_stub_path
  rm "$TMP/ip-route"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"--endpoint"* ]]
  [[ "$output" != *"apt-get"* ]]
}

@test "dry-run: explicit --endpoint skips autodetect" {
  make_stub_path
  rm "$TMP/ip-route"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run --endpoint 198.51.100.9
  [ "$status" -eq 0 ]
  [[ "$output" == *"198.51.100.9:51820"* ]]
}

@test "resolve_endpoint: flag > env file > autodetect (new mesh only)" {
  stub_ip
  mkdir -p "$FABER_ETC"
  MODE=new ENDPOINT_ARG=""
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ "$WG_ENDPOINT" = "203.0.113.7:51820" ]
  printf 'FABER_WG_INTERFACE=faber0\nFABER_WG_ENDPOINT=198.51.100.4:51820\n' > "$FABER_ETC/fabercp.env"
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ "$WG_ENDPOINT" = "198.51.100.4:51820" ]
  ENDPOINT_ARG="198.51.100.5:51820"
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ "$WG_ENDPOINT" = "198.51.100.5:51820" ]
}

@test "resolve_endpoint: join without flag or env value leaves it to the token" {
  stub_ip
  MODE=join ENDPOINT_ARG=""
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ -z "$WG_ENDPOINT" ]
  [ ! -e "$TMP/ip-calls" ]
  MODE=existing
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ -z "$WG_ENDPOINT" ]
}

@test "resolve_endpoint: a joiner ignores an env value left by an earlier run" {
  stub_ip
  mkdir -p "$FABER_ETC"
  printf 'FABER_WG_ENDPOINT=198.51.100.4:51820\n' > "$FABER_ETC/fabercp.env"
  MODE=join ENDPOINT_ARG=""
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ -z "$WG_ENDPOINT" ]
  MODE=existing
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ "$WG_ENDPOINT" = "198.51.100.4:51820" ]
}

@test "resolve_endpoint: ignores a malformed value in the env file" {
  stub_ip
  mkdir -p "$FABER_ETC"
  printf 'FABER_WG_ENDPOINT=$(touch /tmp/x)\n' > "$FABER_ETC/fabercp.env"
  MODE=new ENDPOINT_ARG=""
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ "$WG_ENDPOINT" = "203.0.113.7:51820" ]
}

# --- configured detection -------------------------------------------------

@test "node_configured: identity.json regular file only" {
  [ "$(node_configured)" = no ]
  mkdir -p "$FABER_STATE_DIR"
  ln -s /etc/hostname "$FABER_STATE_DIR/identity.json"
  [ "$(node_configured)" = no ]
  rm "$FABER_STATE_DIR/identity.json"
  mkdir "$FABER_STATE_DIR/identity.json"
  [ "$(node_configured)" = no ]
  rmdir "$FABER_STATE_DIR/identity.json"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  [ "$(node_configured)" = yes ]
}

@test "dry-run: configured node + --join dies before any package command" {
  make_stub_path
  mkdir -p "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"already configured"* ]]
  [[ "$output" != *"apt-get"* ]]
  [[ "$output" != *"useradd"* ]]
  [[ "$output" != *"$TOKEN"* ]]
  [ ! -e "$TMP/executed" ]
}

@test "real run: configured node + --join changes nothing" {
  setup_full_run systemd
  mkdir -p "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$FABER_HOME" ]
  [ ! -e "$CONTAINERD_ROOT_CONFIG" ]
}

@test "dry-run: configured node + a new-mesh setting dies before any package command" {
  make_stub_path
  mkdir -p "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  local flag value
  for flag in "--mesh-cidr 10.50.0.0/16" "--container-cidr 10.210.0.0/16" "--service-cidr 10.77.0.0/16"; do
    value="${flag#* }"
    flag="${flag%% *}"
    OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
      run bash "$SCRIPT" --dry-run "$flag" "$value"
    echo "case: $flag" >&3
    [ "$status" -eq 1 ]
    [[ "$output" == *"$flag only applies to a new mesh"* ]]
    [[ "$output" != *"apt-get"* ]]
  done
}

@test "dry-run: symlinked FABER_ETC or conf.d dies before any package command" {
  make_stub_path
  mkdir -p "$TMP/real" "$TMP/etc/containerd"
  ln -s "$TMP/real" "$FABER_ETC"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"symlink"* ]]
  [[ "$output" != *"apt-get"* ]]
  rm "$FABER_ETC"
  ln -s "$TMP/real" "$CONTAINERD_CONF_DIR"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"symlink"* ]]
  [[ "$output" != *"apt-get"* ]]
}

@test "dry-run: symlinked or non-directory CNI conf dir dies before any package command" {
  make_stub_path
  mkdir -p "$TMP/real" "$TMP/etc/cni"
  ln -s "$TMP/real" "$CNI_CONF_DIR"
  install_run --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"$CNI_CONF_DIR is a symlink"* ]]
  [[ "$output" != *"apt-get"* ]]
  rm "$CNI_CONF_DIR"
  echo x > "$CNI_CONF_DIR"
  install_run --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"$CNI_CONF_DIR exists but is not a directory"* ]]
  [[ "$output" != *"apt-get"* ]]
}

# --- rendering ------------------------------------------------------------

@test "render_env: new mesh while unconfigured" {
  MODE=new WG_ENDPOINT="203.0.113.7:51820" MESH_CIDR="10.99.0.0/16"
  run render_env 1
  [ "$status" -eq 0 ]
  local expected
  expected="$(printf '%s\n' \
    '# managed by faber install.sh; changes are overwritten' \
    'FABER_WG_INTERFACE=faber0' \
    "FABER_STATE_DIR=$FABER_STATE_DIR" \
    "FABER_DATA_DIR=$FABER_STATE_DIR/data" \
    'FABER_WG_ENDPOINT=203.0.113.7:51820' \
    'FABER_WG_PORT=51820' \
    'FABER_MANAGE_HOST=1' \
    'FABER_NEW_MESH=1' \
    'FABER_MESH_CIDR=10.99.0.0/16' \
    'FABER_SERVICE_CIDR=10.201.0.0/16')"
  [ "$output" = "$expected" ]
}

@test "render_env: new mesh with --service-cidr writes it" {
  MODE=new WG_ENDPOINT="" MESH_CIDR="10.99.0.0/16" SERVICE_CIDR="10.77.0.0/20"
  run render_env 1
  [ "$status" -eq 0 ]
  grep -qxF 'FABER_SERVICE_CIDR=10.77.0.0/20' <<<"$output"
  [ "$(grep -c SERVICE_CIDR <<<"$output")" = 1 ]
}

@test "render_env: new mesh with --container-cidr adds FABER_CONTAINER_CIDR before the service range" {
  MODE=new WG_ENDPOINT="203.0.113.7:51820" MESH_CIDR="10.99.0.0/16" CONTAINER_CIDR="10.210.0.0/16"
  run render_env 1
  [ "$status" -eq 0 ]
  local expected
  expected="$(printf '%s\n' \
    '# managed by faber install.sh; changes are overwritten' \
    'FABER_WG_INTERFACE=faber0' \
    "FABER_STATE_DIR=$FABER_STATE_DIR" \
    "FABER_DATA_DIR=$FABER_STATE_DIR/data" \
    'FABER_WG_ENDPOINT=203.0.113.7:51820' \
    'FABER_WG_PORT=51820' \
    'FABER_MANAGE_HOST=1' \
    'FABER_NEW_MESH=1' \
    'FABER_MESH_CIDR=10.99.0.0/16' \
    'FABER_CONTAINER_CIDR=10.210.0.0/16' \
    'FABER_SERVICE_CIDR=10.201.0.0/16')"
  [ "$output" = "$expected" ]
}

@test "render_env: join points at the token file and never holds the token" {
  MODE=join WG_ENDPOINT="" JOIN_TOKEN="$TOKEN"
  run render_env 1
  [[ "$output" == *"FABER_JOIN_TOKEN_FILE=$FABER_ETC/join.token"* ]]
  [[ "$output" != *"FABER_NEW_MESH"* ]]
  [[ "$output" != *"FABER_MESH_CIDR"* ]]
  [[ "$output" != *"FABER_CONTAINER_CIDR"* ]]
  [[ "$output" != *"FABER_SERVICE_CIDR"* ]]
  [[ "$output" != *"FABER_WG_ENDPOINT"* ]]
  [[ "$output" != *"$TOKEN"* ]]
  [[ "$output" != *"CONF_DIR"* ]]
  WG_ENDPOINT="198.51.100.2:51820"
  run render_env 1
  [[ "$output" == *"FABER_WG_ENDPOINT=198.51.100.2:51820"* ]]
}

@test "render_env: bootstrap vars dropped once configured" {
  local mode
  for mode in new join existing; do
    MODE=$mode WG_ENDPOINT="203.0.113.7:51820" MESH_CIDR="10.99.0.0/16" CONTAINER_CIDR="10.210.0.0/16"
    run render_env 0
    [[ "$output" != *"FABER_NEW_MESH"* ]]
    [[ "$output" != *"FABER_MESH_CIDR"* ]]
    [[ "$output" != *"FABER_CONTAINER_CIDR"* ]]
    [[ "$output" != *"FABER_SERVICE_CIDR"* ]]
    [[ "$output" != *"FABER_JOIN_TOKEN_FILE"* ]]
    [[ "$output" == *"FABER_MANAGE_HOST=1"* ]]
    [[ "$output" != *"CONF_DIR"* ]]
  done
  MODE=existing
  run render_env 1
  [[ "$output" != *"FABER_NEW_MESH"* ]]
  [[ "$output" != *"FABER_JOIN_TOKEN_FILE"* ]]
}

@test "render_containerd_root_config: marker, version 2, imports, no grpc gid" {
  run render_containerd_root_config
  [ "$output" = "$(printf '%s\n' '# managed by faber' 'version = 2' \
    "imports = [\"$CONTAINERD_CONF_DIR/*.toml\"]")" ]
  [[ "$output" != *"grpc"* ]]
  [[ "$output" != *"gid"* ]]
}

@test "render_fabercp_unit: contract directives" {
  run render_fabercp_unit
  local line
  for line in '[Unit]' 'After=network-online.target containerd.service faber-node.service' \
    'Wants=network-online.target faber-node.service' '[Service]' 'User=faber' 'Group=faber' \
    "EnvironmentFile=$FABER_ETC/fabercp.env" "ExecStart=$FABER_HOME/fabercp" \
    'Restart=always' 'RestartSec=2' 'AmbientCapabilities=CAP_NET_ADMIN' \
    'CapabilityBoundingSet=CAP_NET_ADMIN' 'NoNewPrivileges=yes' \
    'StateDirectory=faber' 'StateDirectoryMode=0700' 'ProtectSystem=strict' \
    'ProtectHome=yes' 'PrivateTmp=yes' \
    'RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK' \
    '[Install]' 'WantedBy=multi-user.target'; do
    grep -qxF -- "$line" <<<"$output"
  done
}

@test "render_fabercp_unit: no ReadWritePaths, faber-node owns the conf dirs" {
  run render_fabercp_unit
  [[ "$output" != *"ReadWritePaths"* ]]
  [[ "$output" != *"$CONTAINERD_CONF_DIR"* ]]
  [[ "$output" != *"$CNI_CONF_DIR"* ]]
  [ "$(grep -A1 -xF 'ProtectSystem=strict' <<<"$output" | tail -n 1)" = 'ProtectHome=yes' ]
}

@test "render_reload units: path and oneshot service" {
  run render_reload_path_unit
  grep -qxF "PathChanged=$CONTAINERD_CONF_DIR/faber.toml" <<<"$output"
  grep -qxF 'Unit=faber-containerd-reload.service' <<<"$output"
  grep -qxF 'WantedBy=multi-user.target' <<<"$output"
  run render_reload_service_unit
  grep -qxF 'Type=oneshot' <<<"$output"
  grep -qxF 'ExecStart=/usr/bin/systemctl try-restart containerd.service' <<<"$output"
  grep -qxF 'StartLimitIntervalSec=60' <<<"$output"
  grep -qxF 'StartLimitBurst=5' <<<"$output"
  ! grep -q '^\[Install\]' <<<"$output"
}

# --- write_file / containerd root config ---------------------------------

@test "write_file: atomic write with owner/mode, unchanged on second call" {
  need_root_faber_account
  mkdir -p "$TMP/d"
  write_file "$TMP/d/f" "hello" root faber 0640
  [ "$FILE_CHANGED" = 1 ]
  [ "$(cat "$TMP/d/f")" = hello ]
  [ "$(stat -c '%U:%G %a' "$TMP/d/f")" = "root:faber 640" ]
  [ "$(ls -A "$TMP/d")" = f ]
  write_file "$TMP/d/f" "hello" root faber 0640
  [ "$FILE_CHANGED" = 0 ]
  chmod 0644 "$TMP/d/f"
  write_file "$TMP/d/f" "hello" root faber 0640
  [ "$FILE_CHANGED" = 1 ]
  [ "$(stat -c %a "$TMP/d/f")" = 640 ]
}

@test "write_file: replaces a symlink instead of writing through it" {
  need_root_faber_account
  mkdir -p "$TMP/d" "$TMP/outside"
  echo precious > "$TMP/outside/file"
  ln -s "$TMP/outside/file" "$TMP/d/f"
  write_file "$TMP/d/f" "hello" root root 0644
  [ ! -L "$TMP/d/f" ]
  [ "$(cat "$TMP/d/f")" = hello ]
  [ "$(cat "$TMP/outside/file")" = precious ]
}

@test "write_file: dry-run prints the label and writes nothing" {
  DRY_RUN=1
  run write_file "$TMP/d/secret" "$TOKEN" root faber 0640 "$TMP/d/secret (redacted)"
  [ "$status" -eq 0 ]
  [ "$output" = "+ write $TMP/d/secret (redacted)" ]
  [ ! -e "$TMP/d" ]
}

@test "configure_containerd_root: fresh host gets root config and conf.d" {
  need_root_faber_account
  configure_containerd_root
  [ "$ROOT_CONFIG_CHANGED" = 1 ]
  [ "$(cat "$CONTAINERD_ROOT_CONFIG")" = "$(render_containerd_root_config)" ]
  [ "$(stat -c '%U:%G %a' "$CONTAINERD_ROOT_CONFIG")" = "root:root 644" ]
  [ "$(stat -c '%U:%G %a' "$CONTAINERD_CONF_DIR")" = "root:root 755" ]
  ! ls "$TMP/etc/containerd" | grep -q faber-bak
  configure_containerd_root
  [ "$ROOT_CONFIG_CHANGED" = 0 ]
}

@test "configure_cni_conf_dir: fresh dir is root:root 0755 and stays empty" {
  configure_cni_conf_dir
  [ "$(stat -c '%U:%G %a' "$CNI_CONF_DIR")" = "root:root 755" ]
  [ -z "$(ls -A "$CNI_CONF_DIR")" ]
}

@test "configure_containerd_root: foreign config is backed up first" {
  need_root_faber_account
  mkdir -p "$TMP/etc/containerd"
  printf 'disabled_plugins = ["cri"]\n' > "$CONTAINERD_ROOT_CONFIG"
  configure_containerd_root
  [ "$ROOT_CONFIG_CHANGED" = 1 ]
  local bak
  bak="$(ls "$TMP/etc/containerd" | grep -E '^config\.toml\.faber-bak\.[0-9]+$')"
  [ -n "$bak" ]
  [ "$(cat "$TMP/etc/containerd/$bak")" = 'disabled_plugins = ["cri"]' ]
  grep -qxF '# managed by faber' "$CONTAINERD_ROOT_CONFIG"
}

@test "configure_containerd_root: an older managed config is rewritten without backup" {
  need_root_faber_account
  mkdir -p "$TMP/etc/containerd"
  printf '# managed by faber\nversion = 2\n' > "$CONTAINERD_ROOT_CONFIG"
  configure_containerd_root
  [ "$ROOT_CONFIG_CHANGED" = 1 ]
  ! ls "$TMP/etc/containerd" | grep -q faber-bak
  [ "$(cat "$CONTAINERD_ROOT_CONFIG")" = "$(render_containerd_root_config)" ]
}

@test "configure_containerd_root: dry-run plans backup and write, changes nothing" {
  mkdir -p "$TMP/etc/containerd"
  printf 'foreign\n' > "$CONTAINERD_ROOT_CONFIG"
  DRY_RUN=1
  run configure_containerd_root
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ cp -pP -- $CONTAINERD_ROOT_CONFIG $CONTAINERD_ROOT_CONFIG.faber-bak."* ]]
  [[ "$output" == *"+ write $CONTAINERD_ROOT_CONFIG"* ]]
  [[ "$output" == *"+ install -d -o root -g root -m 0755 $CONTAINERD_CONF_DIR"* ]]
  [ "$(cat "$CONTAINERD_ROOT_CONFIG")" = foreign ]
}

# --- end-to-end real runs (root, stubbed packages/systemctl/faber) --------

@test "real run (new mesh, systemd): files, owners, modes, units, init once" {
  setup_full_run systemd
  install_run
  [ "$status" -eq 0 ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC")" = "root:faber 750" ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/fabercp.env")" = "root:faber 640" ]
  [ ! -e "$FABER_ETC/join.token" ]
  # bootstrap finished, so bootstrap vars are gone and the endpoint is kept
  grep -qxF 'FABER_WG_ENDPOINT=203.0.113.7:51820' "$FABER_ETC/fabercp.env"
  ! grep -q 'FABER_NEW_MESH\|FABER_MESH_CIDR' "$FABER_ETC/fabercp.env"
  [ "$(stat -c '%U:%G %a' "$CONTAINERD_ROOT_CONFIG")" = "root:root 644" ]
  [ "$(stat -c '%U:%G %a' "$CONTAINERD_CONF_DIR")" = "root:root 755" ]
  [ "$(stat -c '%U:%G %a' "$CNI_CONF_DIR")" = "root:root 755" ]
  [ -z "$(ls -A "$CNI_CONF_DIR")" ]
  [ "$(ls -A "$TMP/etc/cni")" = net.d ]
  ! grep -q 'CONF_DIR' "$FABER_ETC/fabercp.env"
  ! grep -q 'ReadWritePaths' "$SYSTEMD_UNIT_DIR/fabercp.service"
  grep -qxF "FABER_NODE_CNI_CONF_DIR=$CNI_CONF_DIR" "$FABER_ETC/faber-node.env"
  grep -qxF "FABER_NODE_MESH_INTERFACE=faber0" "$FABER_ETC/faber-node.env"
  [[ "$output" != *"WARN"* ]]
  [[ "$(cat "$FABER_ETC/fabercp.env")" != *"FABER_CONTAINER_CIDR"* ]]
  local u
  for u in fabercp.service faber-containerd-reload.path faber-containerd-reload.service; do
    [ "$(stat -c '%U:%G %a' "$SYSTEMD_UNIT_DIR/$u")" = "root:root 644" ]
  done
  [ "$(count_calls 'systemctl daemon-reload')" = 1 ]
  [ "$(count_calls 'systemctl enable --now containerd faber-containerd-reload.path faber-node fabercp')" = 1 ]
  [ "$(count_calls 'faber health')" -ge 1 ]
  [ "$(count_calls 'faber cluster init')" = 1 ]
  [[ "$output" == *"cluster: created"* ]]
  [ -z "$(ls -A "$TMP/etc/containerd" | grep -v -e '^config.toml$' -e '^conf.d$')" ]
  [ -z "$(ls -A "$FABER_ETC" | grep -v -e '^fabercp.env$' -e '^faber-node.env$')" ]
}

@test "real run (new mesh): env file holds bootstrap vars until identity exists" {
  setup_full_run systemd
  touch "$TMP/no-identity"
  install_run --mesh-cidr 10.50.0.0/24
  grep -qxF 'FABER_NEW_MESH=1' "$FABER_ETC/fabercp.env"
  grep -qxF 'FABER_MESH_CIDR=10.50.0.0/24' "$FABER_ETC/fabercp.env"
}

@test "real run: re-run on a configured node is a no-op (no init, no reload)" {
  setup_full_run systemd
  install_run
  [ "$status" -eq 0 ]
  local env_before
  env_before="$(cat "$FABER_ETC/fabercp.env")"
  rm -f "$TMP/executed"
  rm "$TMP/ip-route"   # autodetect would fail: the kept endpoint must be used
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: existing"* ]]
  [ "$(cat "$FABER_ETC/fabercp.env")" = "$env_before" ]
  [ "$(count_calls 'faber cluster init')" = 0 ]
  [ "$(count_calls 'faber health')" = 0 ]
  [ "$(count_calls 'daemon-reload')" = 0 ]
  [ "$(count_calls 'try-restart')" = 0 ]
  [ "$(count_calls 'systemctl enable --now')" = 1 ]
}

@test "real run: re-run with --endpoint updates the env file and restarts fabercp" {
  setup_full_run systemd
  install_run
  rm -f "$TMP/executed"
  install_run --endpoint 198.51.100.77
  [ "$status" -eq 0 ]
  grep -qxF 'FABER_WG_ENDPOINT=198.51.100.77:51820' "$FABER_ETC/fabercp.env"
  [ "$(count_calls 'systemctl try-restart fabercp')" = 1 ]
  [ "$(count_calls 'try-restart containerd')" = 0 ]
}

@test "real run: already_initialized from cluster init counts as success" {
  setup_full_run systemd
  touch "$TMP/init-already"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
}

@test "real run: failing cluster init exits 1 after the summary" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  [[ "$output" == *"boom"* ]]
  echo "$output" | grep -Eq '^fabercp +\|'
  # identity exists, but the bootstrap did not finish: settings stay
  [ -f "$FABER_STATE_DIR/identity.json" ]
  grep -qxF 'FABER_NEW_MESH=1' "$FABER_ETC/fabercp.env"
  grep -qxF 'FABER_MESH_CIDR=10.99.0.0/16' "$FABER_ETC/fabercp.env"
}

@test "real run: fabercp never healthy -> warn and exit 1" {
  setup_full_run systemd
  touch "$TMP/unhealthy"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  [[ "$output" == *"journalctl -u fabercp"* ]]
  [ "$(count_calls 'faber cluster init')" = 0 ]
  grep -qxF 'FABER_NEW_MESH=1' "$FABER_ETC/fabercp.env"
}

@test "real run (join): joined, then token and bootstrap vars removed" {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [ ! -e "$FABER_ETC/join.token" ]
  ! grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  ! grep -q 'FABER_WG_ENDPOINT' "$FABER_ETC/fabercp.env"
  [ "$(count_calls 'faber -o json cluster status')" -ge 1 ]
  [ "$(count_calls 'faber cluster init')" = 0 ]
  [[ "$output" != *"$TOKEN"* ]]
  ! grep -qF "$TOKEN" "$TMP/executed"
  ! grep -q 'route get' "$TMP/ip-calls"
}

@test "real run (join, no systemd): token file root:faber 0640 and env points at it" {
  setup_full_run
  install_run --join - <<<"$TOKEN"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: skipped (no systemd)"* ]]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/join.token")" = "root:faber 640" ]
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  grep -qxF "FABER_JOIN_TOKEN_FILE=$FABER_ETC/join.token" "$FABER_ETC/fabercp.env"
  [ -z "$(ls -A "$FABER_ETC" | grep -v -e '^fabercp.env$' -e '^faber-node.env$' -e '^join.token$')" ]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "real run (join): --endpoint is passed through to the env file" {
  setup_full_run
  install_run --join "$TOKEN" --endpoint 198.51.100.2:51820
  [ "$status" -eq 0 ]
  grep -qxF 'FABER_WG_ENDPOINT=198.51.100.2:51820' "$FABER_ETC/fabercp.env"
}

@test "real run (join): timeout warns with hints and exits 1 after the summary" {
  setup_full_run systemd
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  [[ "$output" == *"UDP 51820"* ]]
  [[ "$output" == *"public IP"* ]]
  echo "$output" | grep -Eq '^fabercp +\|'
  # not joined yet: the token and the join setting stay for the next run
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/join.token")" = "root:faber 640" ]
  grep -qxF "FABER_JOIN_TOKEN_FILE=$FABER_ETC/join.token" "$FABER_ETC/fabercp.env"
  [[ "$output" != *"$TOKEN"* ]]
  ! grep -qF "$TOKEN" "$TMP/executed"
}

@test "real run: no systemd skips services and bootstrap but writes files" {
  setup_full_run
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"systemd not detected"* ]]
  [[ "$output" == *"cluster: skipped (no systemd)"* ]]
  [ "$(count_calls systemctl)" = 0 ]
  [ "$(count_calls 'faber ')" = 0 ]
  [ -f "$SYSTEMD_UNIT_DIR/fabercp.service" ]
  grep -qxF 'FABER_NEW_MESH=1' "$FABER_ETC/fabercp.env"
}

@test "real run: foreign containerd config is backed up and containerd restarted" {
  setup_full_run systemd
  mkdir -p "$TMP/etc/containerd"
  printf 'disabled_plugins = ["cri"]\n' > "$CONTAINERD_ROOT_CONFIG"
  install_run
  [ "$status" -eq 0 ]
  ls "$TMP/etc/containerd" | grep -Eq '^config\.toml\.faber-bak\.[0-9]+$'
  [ "$(count_calls 'systemctl try-restart containerd')" = 1 ]
}

@test "real run: restarts happen before enable --now" {
  setup_full_run systemd
  install_run
  grep -n . "$TMP/executed" > "$TMP/ordered"
  local restart enable
  restart="$(grep -m1 'try-restart' "$TMP/ordered" | cut -d: -f1)"
  enable="$(grep -m1 'enable --now' "$TMP/ordered" | cut -d: -f1)"
  [ -n "$restart" ]
  [ "$restart" -lt "$enable" ]
}

# --- dry-run / check with mesh --------------------------------------------

@test "dry-run (join): token redacted, systemctl and faber commands planned" {
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run --join - <<<"$TOKEN"
  [ "$status" -eq 0 ]
  [[ "$output" != *"$TOKEN"* ]]
  [[ "$output" != *"TOPSECRET"* ]]
  [[ "$output" == *"+ write $FABER_ETC/join.token (redacted)"* ]]
  [[ "$output" == *"+ write $FABER_ETC/fabercp.env"* ]]
  [[ "$output" == *"+ write $SYSTEMD_UNIT_DIR/fabercp.service"* ]]
  [[ "$output" == *"+ systemctl daemon-reload"* ]]
  [[ "$output" == *"+ systemctl enable --now containerd faber-containerd-reload.path faber-node fabercp"* ]]
  [[ "$output" == *"+ $FABER_CLI_DIR/faber health"* ]]
  [[ "$output" == *"+ $FABER_CLI_DIR/faber -o json cluster status"* ]]
  [[ "$output" == *"cluster: would-join"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_ETC" ]
}

@test "dry-run (new mesh): init planned, endpoint shown" {
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"203.0.113.7:51820"* ]]
  [[ "$output" == *"+ $FABER_CLI_DIR/faber cluster init"* ]]
  [[ "$output" == *"+ write $CONTAINERD_ROOT_CONFIG"* ]]
  [[ "$output" == *"+ install -d -o root -g root -m 0755 $CNI_CONF_DIR"* ]]
  [[ "$output" == *"cluster: would-create"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$TMP/etc/cni" ]
}

@test "--check reports configured, bootstrap, fabercp.service and faber0" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --check
  [ "$status" -eq 0 ]
  echo "$output" | grep -Eq '^configured: +no$'
  echo "$output" | grep -Eq '^fabercp.service: +inactive$'
  echo "$output" | grep -Eq '^bootstrap: +not started$'
  echo "$output" | grep -Eq '^faber0: +absent$'
  grep -qxF "cni conf dir:    $CNI_CONF_DIR (missing)" <<<"$output"
  mkdir -p "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  echo active > "$TMP/fabercp-state"
  touch "$TMP/faber0"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" \
    run bash "$SCRIPT" --check --join "$TOKEN"
  [ "$status" -eq 0 ]
  echo "$output" | grep -Eq '^configured: +yes$'
  echo "$output" | grep -Eq '^fabercp.service: +active$'
  echo "$output" | grep -Eq '^bootstrap: +complete$'
  echo "$output" | grep -Eq '^faber0: +present$'
  [[ "$output" != *"wg0"* ]]
  mkdir -p "$FABER_ETC"
  printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=10.99.0.0/16\n' > "$FABER_ETC/fabercp.env"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --check
  echo "$output" | grep -Eq '^bootstrap: +pending \(new mesh\)$'
  printf 'FABER_JOIN_TOKEN_FILE=%s\n' "$FABER_ETC/join.token" > "$FABER_ETC/fabercp.env"
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --check
  echo "$output" | grep -Eq '^bootstrap: +pending \(join\)$'
  [ ! -e "$TMP/executed" ]
}

# --- iproute2 and deferred endpoint autodetect ----------------------------

@test "dry-run: every family installs the ip command (iproute2 / iproute)" {
  make_stub_path
  OS_RELEASE_FILE="$FIXTURES/os-release.ubuntu" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --dry-run
  [[ "$output" == *"apt-get install -y --no-install-recommends "*" iproute2"* ]]
  OS_RELEASE_FILE="$FIXTURES/os-release.fedora" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --dry-run
  [[ "$output" == *"+ dnf install -y "*" iproute"* ]]
  [[ "$output" != *"iproute2"* ]]
  OS_RELEASE_FILE="$FIXTURES/os-release.rocky" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --dry-run
  [[ "$output" == *"+ dnf install -y containerd.io "*" iproute"* ]]
  [[ "$output" != *"iproute2"* ]]
  OS_RELEASE_FILE="$FIXTURES/os-release.arch" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --dry-run
  [[ "$output" == *"+ pacman -Syu --needed --noconfirm "*" iproute2"* ]]
  OS_RELEASE_FILE="$FIXTURES/os-release.opensuse" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --dry-run
  [[ "$output" == *"+ zypper -n in "*" iproute2"* ]]
}

@test "dry-run: every family installs e2fsprogs for faber-node's block volumes" {
  make_stub_path
  local fixture
  for fixture in ubuntu fedora rocky arch opensuse; do
    OS_RELEASE_FILE="$FIXTURES/os-release.$fixture" PATH="$TMP/bin:$PATH" run bash "$SCRIPT" --dry-run
    [[ "$output" == *" install "*" e2fsprogs"* || "$output" == *"zypper -n in "*" e2fsprogs"* || "$output" == *"pacman -Syu "*" e2fsprogs"* ]]
  done
}

@test "dry-run: ip missing and no --endpoint defers autodetect instead of dying" {
  make_stub_path
  hide_ip
  install_run_noip --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"autodetect"*"after"* ]]
  [ ! -e "$TMP/executed" ]
}

@test "real run: ip missing -> autodetect after packages, before /etc/faber" {
  setup_full_run
  hide_ip provide
  install_run_noip
  [ "$status" -eq 0 ]
  [ "$(cat "$TMP/ip-context")" = "$(printf 'etc=no\napt=yes')" ]
  grep -qxF 'FABER_WG_ENDPOINT=203.0.113.7:51820' "$FABER_ETC/fabercp.env"
}

@test "real run: ip still missing after packages -> dies before any faber write" {
  setup_full_run
  hide_ip
  install_run_noip
  [ "$status" -eq 1 ]
  [[ "$output" == *"--endpoint"* ]]
  grep -q 'apt-get.*install' "$TMP/executed"
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$FABER_HOME" ]
  [ ! -e "$FABER_CLI_DIR/faber" ]
  [ ! -e "$SYSTEMD_UNIT_DIR" ]
  [ ! -e "$CONTAINERD_ROOT_CONFIG" ]
  [ ! -e "$CONTAINERD_CONF_DIR" ]
  [ ! -e "$TMP/etc/cni" ]
}

@test "real run: ip present after packages but no route src -> dies before any faber write" {
  setup_full_run
  hide_ip provide
  cp "$FIXTURES/ip-route-get.nosrc" "$TMP/ip-route"
  install_run_noip
  [ "$status" -eq 1 ]
  [[ "$output" == *"--endpoint"* ]]
  [ "$(cat "$TMP/ip-context")" = "$(printf 'etc=no\napt=yes')" ]
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$CONTAINERD_ROOT_CONFIG" ]
}

@test "preflight: ip missing but --endpoint given needs no autodetect" {
  make_stub_path
  hide_ip
  install_run_noip --dry-run --endpoint 198.51.100.3
  [ "$status" -eq 0 ]
  [[ "$output" != *"autodetect"* ]]
  [ ! -e "$TMP/ip-context" ]
}

# --- WireGuard interface ownership ----------------------------------------

@test "check_wg_interface: only a node faber never ran on is checked" {
  stub_ip
  touch "$TMP/faber0"
  CONFIGURED=no PENDING=none
  PATH="$TMP/bin:$PATH" run check_wg_interface
  [ "$status" -eq 1 ]
  [[ "$output" == *"interface faber0 already exists and was not created by faber"* ]]
  CONFIGURED=yes
  PATH="$TMP/bin:$PATH" run check_wg_interface
  [ "$status" -eq 0 ]
  # an env file written by an earlier install.sh run must name the interface
  CONFIGURED=no
  mkdir -p "$FABER_ETC"
  printf 'FABER_WG_INTERFACE=wg0\n' > "$FABER_ETC/fabercp.env"
  PATH="$TMP/bin:$PATH" run check_wg_interface
  [ "$status" -eq 1 ]
  printf 'FABER_WG_INTERFACE=faber0\n' > "$FABER_ETC/fabercp.env"
  PATH="$TMP/bin:$PATH" run check_wg_interface
  [ "$status" -eq 0 ]
  rm "$FABER_ETC/fabercp.env" "$TMP/faber0"
  PATH="$TMP/bin:$PATH" run check_wg_interface
  [ "$status" -eq 0 ]
}

@test "dry-run: existing faber0 on an unconfigured node dies before any package command" {
  make_stub_path
  touch "$TMP/faber0"
  install_run --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"interface faber0 already exists and was not created by faber"* ]]
  [[ "$output" != *"apt-get"* ]]
  [ ! -e "$TMP/executed" ]
  install_run --dry-run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"interface faber0 already exists"* ]]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "real run: existing faber0 on an unconfigured node changes nothing" {
  setup_full_run systemd
  touch "$TMP/faber0"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"interface faber0 already exists and was not created by faber"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$FABER_HOME" ]
  [ ! -e "$CONTAINERD_ROOT_CONFIG" ]
}

@test "real run: faber0 on a configured node is expected" {
  setup_full_run systemd
  install_run
  [ "$status" -eq 0 ]
  touch "$TMP/faber0"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: existing"* ]]
}

@test "dry-run: ip missing defers the interface check" {
  make_stub_path
  hide_ip
  install_run_noip --dry-run --endpoint 198.51.100.3
  [ "$status" -eq 0 ]
  [[ "$output" == *"faber0 will be checked after installing packages"* ]]
}

@test "real run: ip missing -> faber0 is checked after packages, before any faber write" {
  setup_full_run systemd
  hide_ip provide
  touch "$TMP/faber0"
  install_run_noip --endpoint 198.51.100.3
  [ "$status" -eq 1 ]
  [[ "$output" == *"interface faber0 already exists and was not created by faber"* ]]
  grep -q 'apt-get.*install' "$TMP/executed"
  [ "$(count_calls systemctl)" = 0 ]
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$FABER_HOME" ]
  [ ! -e "$FABER_CLI_DIR/faber" ]
  [ ! -e "$SYSTEMD_UNIT_DIR" ]
  [ ! -e "$CONTAINERD_ROOT_CONFIG" ]
  [ ! -e "$CONTAINERD_CONF_DIR" ]
  [ ! -e "$TMP/etc/cni" ]
}

@test "real run: ip still missing after packages with --endpoint -> dies before any faber write" {
  setup_full_run systemd
  hide_ip
  install_run_noip --endpoint 198.51.100.3
  [ "$status" -eq 1 ]
  [[ "$output" == *"ip command"* ]]
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$FABER_HOME" ]
}

# --- bootstrap state: pending until created/joined ------------------------

@test "pending_bootstrap: read from the env file" {
  [ "$(pending_bootstrap)" = none ]
  mkdir -p "$FABER_ETC"
  printf 'FABER_WG_PORT=51820\n' > "$FABER_ETC/fabercp.env"
  [ "$(pending_bootstrap)" = none ]
  printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=10.99.0.0/16\n' > "$FABER_ETC/fabercp.env"
  [ "$(pending_bootstrap)" = new ]
  printf 'FABER_CONTAINER_CIDR=10.210.0.0/16\n' > "$FABER_ETC/fabercp.env"
  [ "$(pending_bootstrap)" = new ]
  printf 'FABER_NODE_NAME=web-01\n' > "$FABER_ETC/fabercp.env"
  [ "$(pending_bootstrap)" = new ]
  printf 'FABER_WG_PORT=51820\n' > "$FABER_ETC/fabercp.env"
  [ "$(pending_bootstrap)" = none ]
  printf 'FABER_JOIN_TOKEN_FILE=/etc/faber/join.token\n' > "$FABER_ETC/fabercp.env"
  [ "$(pending_bootstrap)" = join ]
  rm "$FABER_ETC/fabercp.env"
  ln -s /etc/hostname "$FABER_ETC/fabercp.env"
  [ "$(pending_bootstrap)" = none ]
}

@test "resolve_mode: bootstrapped only with identity.json and no bootstrap vars" {
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  printf 'FABER_WG_PORT=51820\n' > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ "$MODE" = existing ]
  [ "$RESUME" = 0 ]
  printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=10.50.0.0/24\n' > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ "$MODE" = new ]
  [ "$MESH_CIDR" = 10.50.0.0/24 ]
  [ "$RESUME" = 1 ]
  printf '%s\n' "$TOKEN" > "$FABER_ETC/join.token"
  printf 'FABER_JOIN_TOKEN_FILE=%s\n' "$FABER_ETC/join.token" > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ "$MODE" = join ]
  [ "$RESUME" = 1 ]
}

@test "real run: cluster init fails, the re-run retries it and finishes" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run --mesh-cidr 10.50.0.0/24
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  [ -f "$FABER_STATE_DIR/identity.json" ]
  grep -qxF 'FABER_MESH_CIDR=10.50.0.0/24' "$FABER_ETC/fabercp.env"
  rm "$TMP/init-fails" "$TMP/executed"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
  [ "$(count_calls 'faber cluster init')" = 1 ]
  # same settings as run 1 (stored mesh CIDR kept), so fabercp is not restarted
  [ "$(count_calls 'try-restart fabercp')" = 0 ]
  ! grep -q 'FABER_NEW_MESH\|FABER_MESH_CIDR' "$FABER_ETC/fabercp.env"
  rm "$TMP/executed"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: existing"* ]]
  [ "$(count_calls 'faber cluster init')" = 0 ]
}

@test "real run: health timeout, the re-run bootstraps the new mesh" {
  setup_full_run systemd
  touch "$TMP/unhealthy"
  install_run
  [ "$status" -eq 1 ]
  [ -f "$FABER_STATE_DIR/identity.json" ]
  rm "$TMP/unhealthy" "$TMP/executed"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
  [ "$(count_calls 'faber cluster init')" = 1 ]
  ! grep -q 'FABER_NEW_MESH' "$FABER_ETC/fabercp.env"
}

@test "real run: unfinished new mesh refuses --join and another --mesh-cidr" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run
  [ "$status" -eq 1 ]
  local env_before
  env_before="$(cat "$FABER_ETC/fabercp.env")"
  rm "$TMP/executed"
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"refusing --join"* ]]
  [[ "$output" != *"$TOKEN"* ]]
  install_run --mesh-cidr 10.77.0.0/16
  [ "$status" -eq 1 ]
  [[ "$output" == *"10.99.0.0/16"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_ETC/join.token" ]
  [ "$(cat "$FABER_ETC/fabercp.env")" = "$env_before" ]
  rm "$TMP/init-fails"
  install_run --mesh-cidr 10.99.0.0/16
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
}

@test "real run (join): timeout, the re-run without --join polls again and finishes" {
  setup_full_run systemd
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  [ -f "$FABER_STATE_DIR/identity.json" ]
  rm "$TMP/executed"
  touch "$TMP/joined"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [ "$(count_calls 'faber -o json cluster status')" -ge 1 ]
  [ "$(count_calls 'faber cluster init')" = 0 ]
  # nothing changed between the runs, so fabercp keeps running undisturbed
  [ "$(count_calls 'try-restart fabercp')" = 0 ]
  grep -qxF 'FABER_WG_ENDPOINT=198.51.100.2:51820' "$FABER_ETC/fabercp.env"
  ! grep -q 'FABER_JOIN_TOKEN_FILE\|FABER_NEW_MESH' "$FABER_ETC/fabercp.env"
  [ ! -e "$FABER_ETC/join.token" ]
  [[ "$output" != *"$TOKEN"* ]]
  ! grep -qF "$TOKEN" "$TMP/executed"
}

@test "real run (join): timeout, the re-run with the same token is allowed" {
  setup_full_run systemd
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  rm "$TMP/executed"
  touch "$TMP/joined"
  install_run --join - <<<"$TOKEN"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [ "$(count_calls 'try-restart fabercp')" = 0 ]
  [ ! -e "$FABER_ETC/join.token" ]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "real run (join): health timeout keeps the token for the re-run" {
  setup_full_run systemd
  touch "$TMP/unhealthy" "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  [ "$(count_calls 'faber -o json cluster status')" = 0 ]
  rm "$TMP/unhealthy"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
}

@test "real run (join): unfinished join accepts a different token and joins with it" {
  setup_full_run systemd
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 1 ]
  [ -f "$FABER_STATE_DIR/identity.json" ]
  grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  rm "$TMP/executed"
  touch "$TMP/joined"
  local other="faber1.b3RoZXI_OTHERSECRET"
  install_run --join - <<<"$other"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  # fabercp was restarted after the new token was in place
  [ "$(count_calls 'systemctl try-restart fabercp')" = 1 ]
  [ "$(cat "$TMP/token-at-restart")" = "$other" ]
  [ "$(count_calls 'faber health')" -ge 1 ]
  [ "$(count_calls 'faber -o json cluster status')" -ge 1 ]
  [ "$(count_calls 'faber cluster init')" = 0 ]
  [ ! -e "$FABER_ETC/join.token" ]
  ! grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  [ -z "$(ls -A "$FABER_ETC" | grep -v -e '^fabercp.env$' -e '^faber-node.env$')" ]
  [[ "$output" != *"$other"* ]]
  [[ "$output" != *"$TOKEN"* ]]
  ! grep -qF "$other" "$TMP/executed"
  [[ "$output" != *"was not used"* ]]
}

# write_identity FILE NODE_ID: identity.json as fabercp writes it (pretty JSON).
write_identity() {
  printf '{\n  "v": 1,\n  "node_id": %s,\n  "private_key": "SECRETKEY",\n  "mesh_ip": "10.99.0.%s"\n}\n' "$2" "$2" > "$1"
}

@test "identity_node_id: only a number from a regular identity.json" {
  mkdir -p "$FABER_STATE_DIR"
  run identity_node_id
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  write_identity "$FABER_STATE_DIR/identity.json" 12
  [ "$(identity_node_id)" = 12 ]
  echo '{"v":1,"node_id":7,"private_key":"SECRETKEY"}' > "$FABER_STATE_DIR/identity.json"
  [ "$(identity_node_id)" = 7 ]
  # nothing else from the file is ever printed
  for content in '' '{}' '{"node_id": "SECRETKEY"}' '{"node_id": -1}' \
    '{"node_id": 123456789012345678901}' 'node_id: 4'; do
    printf '%s\n' "$content" > "$FABER_STATE_DIR/identity.json"
    run identity_node_id
    [ "$status" -eq 0 ]
    [ -z "$output" ]
  done
  # a symlink is not read
  write_identity "$TMP/elsewhere.json" 5
  rm "$FABER_STATE_DIR/identity.json"
  ln -s "$TMP/elsewhere.json" "$FABER_STATE_DIR/identity.json"
  run identity_node_id
  [ -z "$output" ]
}

@test "real run (join): a different token on a node that had already joined says the new invite was not used" {
  setup_full_run systemd
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 1 ]
  # The node of the first invite. fabercp finds itself in the cluster after the
  # restart, so it keeps this identity whatever the token file holds.
  write_identity "$FABER_STATE_DIR/identity.json" 2
  write_identity "$TMP/identity-after" 2
  rm "$TMP/executed"
  touch "$TMP/joined"
  local other="faber1.b3RoZXI_OTHERSECRET"
  install_run --join - <<<"$other"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [[ "$output" == *"had already joined the cluster with its earlier invite (node 2)"* ]]
  [[ "$output" == *"the invite given with --join was not used"* ]]
  [ "$(count_calls 'systemctl try-restart fabercp')" = 1 ]
  # the bootstrap is finished all the same
  [ ! -e "$FABER_ETC/join.token" ]
  [ "$(grep -c 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env")" = 0 ]
  [[ "$output" != *"$other"* ]]
  [[ "$output" != *"SECRETKEY"* ]]
}

@test "real run (join): a different token that fabercp joins with says nothing about an unused invite" {
  setup_full_run systemd
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 1 ]
  write_identity "$FABER_STATE_DIR/identity.json" 2
  # fabercp discards the stuck identity and makes the one of the new invite
  write_identity "$TMP/identity-after" 3
  touch "$TMP/joined"
  install_run --join - <<<"faber1.b3RoZXI_OTHERSECRET"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [[ "$output" != *"was not used"* ]]
}

@test "real run (join): finishing an unfinished join with the same token says nothing about an unused invite" {
  setup_full_run systemd
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 1 ]
  write_identity "$FABER_STATE_DIR/identity.json" 2
  write_identity "$TMP/identity-after" 2
  touch "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [[ "$output" != *"was not used"* ]]
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" != *"was not used"* ]]
}

@test "real run (join): a replaced token is root:faber 0640 and its endpoint comes from --endpoint only" {
  setup_full_run systemd
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 1 ]
  grep -qxF 'FABER_WG_ENDPOINT=198.51.100.2:51820' "$FABER_ETC/fabercp.env"
  local other="faber1.b3RoZXI_OTHERSECRET"
  # still not joined: the new token and the join setting stay for the next run
  install_run --join "$other"
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  [ "$(cat "$FABER_ETC/join.token")" = "$other" ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/join.token")" = "root:faber 640" ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/fabercp.env")" = "root:faber 640" ]
  grep -qxF "FABER_JOIN_TOKEN_FILE=$FABER_ETC/join.token" "$FABER_ETC/fabercp.env"
  # the endpoint of the old invite does not carry over to the new one
  ! grep -q 'FABER_WG_ENDPOINT' "$FABER_ETC/fabercp.env"
  [ -z "$(ls -A "$FABER_ETC" | grep -v -e '^fabercp.env$' -e '^faber-node.env$' -e '^join.token$')" ]
  install_run --join "$TOKEN" --endpoint 198.51.100.3
  [ "$status" -eq 1 ]
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  grep -qxF 'FABER_WG_ENDPOINT=198.51.100.3:51820' "$FABER_ETC/fabercp.env"
  [[ "$output" != *"$other"* ]]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "dry-run (join): unfinished join + different token plans the token write and the restart" {
  setup_full_run systemd
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  rm "$TMP/executed"
  local other="faber1.b3RoZXI_OTHERSECRET"
  install_run --dry-run --join "$other"
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ write $FABER_ETC/join.token (redacted)"* ]]
  [[ "$output" == *"+ systemctl try-restart fabercp"* ]]
  [[ "$output" == *"cluster: would-join"* ]]
  [ ! -e "$TMP/executed" ]
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  [[ "$output" != *"$other"* ]]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "real run (join): unfinished join still refuses --mesh-cidr" {
  setup_full_run systemd
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  rm "$TMP/executed"
  install_run --mesh-cidr 10.77.0.0/16
  [ "$status" -eq 1 ]
  [[ "$output" == *"--mesh-cidr only applies to a new mesh"* ]]
  [ ! -e "$TMP/executed" ]
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
}

@test "resolve_mode: pending join with identity takes --join with any token" {
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  printf '%s\n' "$TOKEN" > "$FABER_ETC/join.token"
  printf 'FABER_JOIN_TOKEN_FILE=%s\n' "$FABER_ETC/join.token" > "$FABER_ETC/fabercp.env"
  JOIN_REQUESTED=1 JOIN_TOKEN="$TOKEN"
  resolve_mode
  [ "$MODE" = join ]
  [ "$RESUME" = 1 ]
  JOIN_TOKEN="faber1.b3RoZXI_OTHERSECRET"
  run resolve_mode
  [ "$status" -eq 0 ]
  [[ "$output" != *"$JOIN_TOKEN"* ]]
  [[ "$output" != *"$TOKEN"* ]]
  resolve_mode
  [ "$MODE" = join ]
  [ "$RESUME" = 0 ]
  # a new mesh that has not finished still refuses --join
  printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=10.50.0.0/24\n' > "$FABER_ETC/fabercp.env"
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"refusing --join"* ]]
  # and so does a bootstrapped node
  printf 'FABER_WG_PORT=51820\n' > "$FABER_ETC/fabercp.env"
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"already configured"* ]]
}

@test "real run (join): a joined node refuses --join, even with the same token" {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  rm "$TMP/executed"
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"already configured"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_ETC/join.token" ]
}

@test "real run (join): no identity yet, the re-run without --join keeps joining" {
  setup_full_run
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 0 ]
  [ ! -e "$FABER_STATE_DIR/identity.json" ]
  mkdir -p "$SYSTEMD_RUN_DIR"
  touch "$TMP/joined"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [ "$(count_calls 'faber cluster init')" = 0 ]
  ! grep -q 'FABER_NEW_MESH' "$FABER_ETC/fabercp.env"
  grep -qxF 'FABER_WG_ENDPOINT=198.51.100.2:51820' "$FABER_ETC/fabercp.env"
}

@test "real run (join): no identity and no stored token needs --join or --mesh-cidr" {
  setup_full_run
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  rm "$FABER_ETC/join.token" "$TMP/executed"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"--join"* ]]
  [ ! -e "$TMP/executed" ]
  grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  # --mesh-cidr asks for a new mesh explicitly
  install_run --mesh-cidr 10.50.0.0/24
  [ "$status" -eq 0 ]
  grep -qxF 'FABER_MESH_CIDR=10.50.0.0/24' "$FABER_ETC/fabercp.env"
  ! grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
}

# --- rejected invite ------------------------------------------------------

@test "real run (join): rejected invite stops the poll early with a clear error" {
  setup_full_run systemd
  touch "$TMP/reject-invite"
  local start=$SECONDS
  FABER_JOIN_TIMEOUT=60 install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [ $((SECONDS - start)) -lt 30 ]
  [[ "$output" == *"invite was rejected; create a new invite and re-run with --join"* ]]
  [[ "$output" == *"cluster: failed"* ]]
  [[ "$output" != *"UDP 51820"* ]]
  [ "$(count_calls 'faber -o json cluster status')" = 1 ]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "real run (join): --join with a new token recovers after a rejected invite" {
  setup_full_run systemd
  touch "$TMP/reject-invite"
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 1 ]
  [ ! -e "$FABER_STATE_DIR/identity.json" ]
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  # fabercp created the interface before the invite was rejected
  touch "$TMP/faber0"
  rm "$TMP/reject-invite" "$TMP/executed"
  touch "$TMP/joined"
  # from here on only a restart makes the (running) fabercp read the new token
  sed -i 's/\*enable\*fabercp\* | //' "$TMP/bin/systemctl"
  local new="faber1.bmV3LXRva2Vu_NEWSECRET"
  install_run --join - --endpoint 198.51.100.2 <<<"$new"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [ "$(count_calls 'systemctl try-restart fabercp')" = 1 ]
  [ -f "$FABER_STATE_DIR/identity.json" ]
  [ ! -e "$FABER_ETC/join.token" ]
  ! grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  [[ "$output" != *"$new"* ]]
  [[ "$output" != *"$TOKEN"* ]]
  ! grep -qF "$new" "$TMP/executed"
}

@test "real run (join, no systemd): stale env and token are overwritten by --join <new>" {
  setup_full_run
  mkdir -p "$FABER_ETC"
  printf 'FABER_WG_INTERFACE=faber0\nFABER_WG_ENDPOINT=198.51.100.9:51820\nFABER_JOIN_TOKEN_FILE=%s\n' \
    "$FABER_ETC/join.token" > "$FABER_ETC/fabercp.env"
  printf '%s\n' "$TOKEN" > "$FABER_ETC/join.token"
  chmod 0600 "$FABER_ETC/join.token"
  touch "$TMP/faber0"
  local new="faber1.bmV3LXRva2Vu_NEWSECRET"
  install_run --join - <<<"$new"
  [ "$status" -eq 0 ]
  [ "$(cat "$FABER_ETC/join.token")" = "$new" ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/join.token")" = "root:faber 640" ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/fabercp.env")" = "root:faber 640" ]
  grep -qxF "FABER_JOIN_TOKEN_FILE=$FABER_ETC/join.token" "$FABER_ETC/fabercp.env"
  # the endpoint of the old invite does not carry over to the new one
  ! grep -q 'FABER_WG_ENDPOINT' "$FABER_ETC/fabercp.env"
  [[ "$output" != *"$new"* ]]
  [[ "$output" != *"$TOKEN"* ]]
}

# --- bootstrap-error record ------------------------------------------------

@test "bootstrap_error_code: only the six fixed codes from a regular file" {
  mkdir -p "$FABER_STATE_DIR"
  local f="$FABER_STATE_DIR/bootstrap-error" code
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  for code in token_expired token_invalid invite_rejected node_removed key_mismatch key_unavailable; do
    printf '%s\n' "$code" > "$f"
    run bootstrap_error_code
    [ "$status" -eq 0 ]
    [ "$output" = "$code" ]
  done
  printf 'token_expired' > "$f"
  [ "$(bootstrap_error_code)" = token_expired ]
}

@test "bootstrap_error_code: anything else gives nothing and is never echoed" {
  mkdir -p "$FABER_STATE_DIR"
  local f="$FABER_STATE_DIR/bootstrap-error"
  printf 'EVIL-CONTENT\n' > "$f"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  printf 'token_expired\nEVIL-CONTENT\n' > "$f"
  run bootstrap_error_code
  [ -z "$output" ]
  printf 'token_expired EVIL\n' > "$f"
  run bootstrap_error_code
  [ -z "$output" ]
  # a code followed by more than the read limit
  { printf 'token_expired\n'; head -c 4096 /dev/zero | tr '\0' 'x'; } > "$f"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  # NUL bytes give no shell warning and no match
  printf 'token_\0expired\n' > "$f"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  : > "$f"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "bootstrap_error_code: a symlink, directory or fifo is not read" {
  mkdir -p "$FABER_STATE_DIR"
  local f="$FABER_STATE_DIR/bootstrap-error"
  printf 'token_expired\n' > "$TMP/elsewhere"
  ln -s "$TMP/elsewhere" "$f"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  rm "$f"
  mkdir "$f"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  rmdir "$f"
  mkfifo "$f"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "bootstrap_error_code: a record older than the token file is stale" {
  mkdir -p "$FABER_STATE_DIR" "$FABER_ETC"
  printf 'token_expired\n' > "$FABER_STATE_DIR/bootstrap-error"
  printf '%s\n' "$TOKEN" > "$FABER_ETC/join.token"
  touch -d '2030-01-01 00:00:00' "$FABER_ETC/join.token"
  run bootstrap_error_code
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  touch -d '2031-01-01 00:00:00' "$FABER_STATE_DIR/bootstrap-error"
  [ "$(bootstrap_error_code)" = token_expired ]
}

@test "real run (join): health timeout names an expired, invalid or rejected token, a cluster key problem, or a removed node" {
  setup_full_run systemd
  touch "$TMP/unhealthy"
  local code msg
  for code in token_expired token_invalid invite_rejected node_removed key_mismatch key_unavailable; do
    case "$code" in
      token_expired) msg="the join token has expired; create a new invite (faber mesh invite ...) on an existing node and re-run with --join <token>" ;;
      token_invalid) msg="the join token is not valid; create a new invite (faber mesh invite ...) on an existing node and re-run with --join <token>" ;;
      invite_rejected) msg="the invite was rejected by the cluster; create a new invite and re-run with --join <token>" ;;
      node_removed) msg="the cluster removed this node; create a new invite (faber mesh invite ...) on an existing node and rejoin with --join <token> --reset" ;;
      key_mismatch) msg="the cluster key sent to this node is not the cluster's, so it did not join; once the cluster key file is sound, create a new invite (faber mesh invite ...) on an existing node and re-run with --join <token>" ;;
      key_unavailable) msg="the cluster never sent its cluster key, so this node did not join; put the cluster key file on a mesh node, create a new invite (faber mesh invite ...) and re-run with --join <token>" ;;
    esac
    printf '%s\n' "$code" > "$TMP/bootstrap-error"
    install_run --join "$TOKEN"
    [ "$status" -eq 1 ]
    [[ "$output" == *"$msg"* ]]
    [[ "$output" != *"re-run this script"* ]]
    [[ "$output" == *"cluster: failed"* ]]
    [[ "$output" != *"$code"* ]]
    [[ "$output" != *"$TOKEN"* ]]
    echo "$output" | grep -Eq '^fabercp +\|'
  done
  # the token and the join setting stay, so --join <new token> replaces them
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
}

@test "real run (join): health timeout without a usable record keeps the generic message" {
  setup_full_run systemd
  touch "$TMP/unhealthy"
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"re-run this script to try again"* ]]
  [[ "$output" != *"create a new invite"* ]]
  # unknown content is not printed
  printf 'EVIL-CONTENT\n' > "$TMP/bootstrap-error"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"re-run this script to try again"* ]]
  [[ "$output" != *"EVIL-CONTENT"* ]]
  [[ "$output" != *"create a new invite"* ]]
  # a symlink is not followed
  printf 'token_expired\n' > "$TMP/elsewhere"
  rm "$TMP/bootstrap-error"
  ln -s "$TMP/elsewhere" "$TMP/bootstrap-error"
  install_run
  [ "$status" -eq 1 ]
  [ -L "$FABER_STATE_DIR/bootstrap-error" ]
  [[ "$output" == *"re-run this script to try again"* ]]
  [[ "$output" != *"has expired"* ]]
}

@test "real run (join): a record left by the previous token is ignored after --join <new>" {
  setup_full_run systemd
  touch "$TMP/unhealthy"
  printf 'token_expired\n' > "$TMP/bootstrap-error"
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"has expired"* ]]
  # fabercp now fails for another reason and never rewrites the record
  rm "$TMP/bootstrap-error"
  install_run --join "faber1.b3RoZXI_OTHERSECRET"
  [ "$status" -eq 1 ]
  [ -f "$FABER_STATE_DIR/bootstrap-error" ]
  [[ "$output" != *"has expired"* ]]
  [[ "$output" == *"re-run this script to try again"* ]]
}

@test "real run (new mesh): a bootstrap-error record does not change the health-timeout message" {
  setup_full_run systemd
  touch "$TMP/unhealthy"
  printf 'token_expired\n' > "$TMP/bootstrap-error"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"re-run this script to try again"* ]]
  [[ "$output" != *"has expired"* ]]
}

@test "real run (join): join timeout names the token problem instead of the network hints" {
  setup_full_run systemd
  printf 'token_invalid\n' > "$TMP/bootstrap-error"
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"the join token is not valid; create a new invite (faber mesh invite ...) on an existing node and re-run with --join <token>"* ]]
  [[ "$output" != *"re-run this script"* ]]
  [[ "$output" != *"UDP 51820"* ]]
  [[ "$output" == *"cluster: failed"* ]]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "real run (join): early stop names the token problem when fabercp recorded one" {
  setup_full_run systemd
  touch "$TMP/reject-invite"
  printf 'token_expired\n' > "$TMP/bootstrap-error"
  FABER_JOIN_TIMEOUT=60 install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"the join token has expired; create a new invite"* ]]
  [[ "$output" != *"re-run this script"* ]]
  [[ "$output" == *"cluster: failed"* ]]
  [ "$(count_calls 'faber -o json cluster status')" = 1 ]
  printf 'invite_rejected\n' > "$TMP/bootstrap-error"
  FABER_JOIN_TIMEOUT=60 install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"the invite was rejected by the cluster; create a new invite and re-run with --join <token>"* ]]
}

# --- interface without an ownership record ----------------------------------

@test "real run: health timeout hints at faber0 without an ownership record" {
  setup_full_run systemd
  touch "$TMP/unhealthy"
  local hint="interface faber0 exists but faber has no ownership record; remove it with: ip link delete faber0"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" != *"$hint"* ]]
  # the env file now names faber0, so preflight no longer refuses the link
  touch "$TMP/faber0"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"$hint"* ]]
  [[ "$output" == *"re-run this script to try again"* ]]
  [[ "$output" == *"cluster: failed"* ]]
  # fabercp's ownership record is there: no hint
  : > "$FABER_STATE_DIR/iface"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" != *"$hint"* ]]
  # no link: no hint
  rm "$FABER_STATE_DIR/iface" "$TMP/faber0"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" != *"$hint"* ]]
}

# --- staging parents in preflight ------------------------------------------

@test "real run: group/world-writable staging parent dies before any package command" {
  make_stub_path
  mkdir -p "$TMP/usr/local"
  chmod 0777 "$TMP/usr/local"
  FABER_EUID=0 install_run
  chmod 0755 "$TMP/usr/local"
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/usr/local must be owned by uid"*"writable"* ]]
  [[ "$output" == *"FABER_CLI_DIR=/usr/bin"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$FABER_HOME" ]
  mkdir -p "$TMP/opt"
  chmod 0770 "$TMP/opt"
  FABER_EUID=0 install_run --join "$TOKEN"
  chmod 0755 "$TMP/opt"
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/opt must be owned by uid"* ]]
  [ ! -e "$TMP/executed" ]
  [[ "$output" != *"$TOKEN"* ]]
}

@test "check_stage_parents: a group-writable parent of the CLI dir names the FABER_CLI_DIR workaround" {
  mkdir -p "$FABER_CLI_DIR" "$FABER_HOME" "$TMP/usr/bin"
  # /usr/local as some Debian releases ship it: root:staff 2775
  chmod 2775 "$TMP/usr/local"
  run check_stage_parents
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/usr/local must be owned by uid"*"group/world-writable"* ]]
  [[ "$output" == *"FABER_CLI_DIR set to a directory whose parent only root can write"* ]]
  [[ "$output" == *"FABER_CLI_DIR=/usr/bin"* ]]
  # the workaround works: the parent of that directory is root-only
  FABER_CLI_DIR="$TMP/usr/bin" FABER_NODE_INSTALL_DIR="$TMP/usr/bin" run check_stage_parents
  chmod 0755 "$TMP/usr/local"
  [ "$status" -eq 0 ]
  # fabercp is not installed through FABER_CLI_DIR, so its parent gets no such hint
  chmod 0775 "$TMP/opt"
  run check_stage_parents
  chmod 0755 "$TMP/opt"
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/opt must be owned by uid"* ]]
  [[ "$output" != *"FABER_CLI_DIR"* ]]
}

@test "check_stage_parents: only parents that will be staged into are checked" {
  # nothing exists yet: the parents are created later by root
  run check_stage_parents
  [ "$status" -eq 0 ]
  mkdir -p "$FABER_CLI_DIR" "$FABER_HOME" "$FABER_NODE_INSTALL_DIR"
  chmod 0777 "$TMP/usr/local"
  run check_stage_parents
  [ "$status" -eq 1 ]
  [[ "$output" == *"writable"* ]]
  # an up-to-date binary is not staged, so its parent is not checked
  install -m 0755 "$TMP/src/faber" "$FABER_CLI_DIR/faber"
  install -m 0755 "$TMP/src/faber-node" "$FABER_NODE_INSTALL_DIR/faber-node"
  binary_spec() {
    case "$1" in
      fabercp) printf '%s %s %s %s\n' "$FABER_HOME/fabercp" "$(id -u)" "$(id -g)" 0750 ;;
      faber) printf '%s %s %s %s\n' "$FABER_CLI_DIR/faber" "$(id -u)" "$(id -g)" 0755 ;;
      faber-node) printf '%s %s %s %s\n' "$FABER_NODE_INSTALL_DIR/faber-node" "$(id -u)" "$(id -g)" 0755 ;;
    esac
  }
  run check_stage_parents
  chmod 0755 "$TMP/usr/local"
  [ "$status" -eq 0 ]
}

# --- container CIDR -------------------------------------------------------

@test "parse_args: --container-cidr, space and = forms" {
  parse_args --container-cidr 10.210.0.0/16
  [ "$CONTAINER_CIDR_ARG" = "10.210.0.0/16" ]
  CONTAINER_CIDR_ARG=""
  parse_args --container-cidr=172.20.0.0/20 --mesh-cidr=10.50.0.0/16
  [ "$CONTAINER_CIDR_ARG" = "172.20.0.0/20" ]
  [ "$MESH_CIDR_ARG" = "10.50.0.0/16" ]
}

@test "valid_container_cidr: refuses ranges overlapping reserved blocks" {
  local bad ok
  for bad in 0.0.0.0/8 0.1.0.0/16 127.0.0.0/8 127.1.0.0/16 169.254.0.0/16 169.254.1.0/24 \
    224.0.0.0/4 230.0.0.0/8 239.0.0.0/24 240.0.0.0/4 250.0.0.0/8 255.255.255.0/24; do
    run valid_container_cidr "$bad"
    [ "$status" -ne 0 ]
  done
  # a range that holds a reserved block is refused too
  run valid_container_cidr 128.0.0.0/1
  [ "$status" -ne 0 ]
  run valid_container_cidr 0.0.0.0/0
  [ "$status" -ne 0 ]
  for ok in 126.0.0.0/8 128.0.0.0/8 169.253.0.0/16 169.255.0.0/16 223.0.0.0/8 8.8.8.0/24 172.16.0.0/12; do
    valid_container_cidr "$ok"
  done
}

@test "script: --container-cidr in a reserved range exits 2" {
  run bash "$SCRIPT" --dry-run --container-cidr 127.0.0.0/8
  [ "$status" -eq 2 ]
  [[ "$output" == *"--container-cidr must be"* ]]
}

@test "valid_container_cidr: IPv4 CIDRs with a prefix of at most 24" {
  valid_container_cidr 10.200.0.0/16
  valid_container_cidr 10.200.0.0/24
  valid_container_cidr 10.0.0.0/8
  run valid_container_cidr 10.200.0.0/25
  [ "$status" -ne 0 ]
  run valid_container_cidr 10.200.0.0/32
  [ "$status" -ne 0 ]
  run valid_container_cidr 10.200.0.0/33
  [ "$status" -ne 0 ]
  run valid_container_cidr 10.200.0.0
  [ "$status" -ne 0 ]
  run valid_container_cidr 300.0.0.0/8
  [ "$status" -ne 0 ]
  run valid_container_cidr fd00::/64
  [ "$status" -ne 0 ]
  run valid_container_cidr "10.200.0.0/16 x"
  [ "$status" -ne 0 ]
  run valid_container_cidr ""
  [ "$status" -ne 0 ]
}

@test "script: bad --container-cidr exits 2" {
  local bad
  for bad in 10.200.0.0 10.200.0.0/33 10.200.0.0/25 fd00::/64 300.1.0.0/16 "10.200.0.0/16 x"; do
    run bash "$SCRIPT" --dry-run --container-cidr "$bad"
    [ "$status" -eq 2 ]
    [[ "$output" == *"--container-cidr must be an IPv4 CIDR"* ]]
    [[ "$output" == *"/24"* ]]
  done
}

@test "cidrs_overlap: overlap by identity, nesting and host bits; none for adjacent or unrelated ranges" {
  # identical, and nested either way round
  cidrs_overlap 10.99.0.0/16 10.99.0.0/16
  cidrs_overlap 0.0.0.0/0 0.0.0.0/0
  cidrs_overlap 10.99.0.0/16 10.99.4.0/24
  cidrs_overlap 10.99.4.0/24 10.99.0.0/16
  cidrs_overlap 10.0.0.0/8 10.200.0.0/16
  cidrs_overlap 10.200.0.0/16 10.0.0.0/8
  cidrs_overlap 0.0.0.0/0 192.168.1.0/24
  # host bits do not hide an overlap, nor do leading zeros
  cidrs_overlap 10.99.3.7/16 10.99.200.0/24
  cidrs_overlap 10.99.200.9/24 10.99.3.7/16
  cidrs_overlap 010.099.000.000/16 10.99.8.0/24
  # adjacent and unrelated ranges do not overlap
  local pair
  for pair in "10.99.0.0/16 10.100.0.0/16" "10.100.0.0/16 10.99.0.0/16" "10.99.0.0/24 10.99.1.0/24" \
    "10.98.0.0/15 10.100.0.0/16" "192.168.255.0/24 192.169.0.0/16" "10.99.0.0/16 10.200.0.0/16" \
    "255.255.255.0/24 0.0.0.0/8"; do
    # shellcheck disable=SC2086
    run cidrs_overlap $pair
    echo "case: $pair" >&3
    [ "$status" -eq 1 ]
  done
}

@test "resolve_mode: --container-cidr overlapping the mesh CIDR dies" {
  CONTAINER_CIDR_ARG=10.99.0.0/16
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"container CIDR 10.99.0.0/16 overlaps the mesh CIDR 10.99.0.0/16"* ]]
  CONTAINER_CIDR_ARG=10.99.8.0/22
  run resolve_mode
  [ "$status" -eq 1 ]
  CONTAINER_CIDR_ARG=10.0.0.0/8
  run resolve_mode
  [ "$status" -eq 1 ]
  # the effective mesh CIDR counts, not the default
  CONTAINER_CIDR_ARG=10.99.0.0/16 MESH_CIDR_ARG=10.50.0.0/16
  resolve_mode
  [ "$CONTAINER_CIDR" = 10.99.0.0/16 ]
  CONTAINER_CIDR_ARG=10.50.4.0/24
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"overlaps the mesh CIDR 10.50.0.0/16"* ]]
}

@test "resolve_mode: a mesh CIDR overlapping the default container CIDR dies" {
  MESH_CIDR_ARG=10.200.4.0/24
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"container CIDR 10.200.0.0/16 overlaps the mesh CIDR 10.200.4.0/24"* ]]
  [[ "$output" == *"--container-cidr"* ]]
  CONTAINER_CIDR_ARG=10.210.0.0/16
  resolve_mode
  [ "$MODE" = new ]
}

@test "resolve_mode: container CIDR comes from the flag, else the env file, else stays empty" {
  resolve_mode
  [ "$MODE" = new ]
  [ -z "$CONTAINER_CIDR" ]
  CONTAINER_CIDR_ARG=10.210.0.0/16
  resolve_mode
  [ "$CONTAINER_CIDR" = 10.210.0.0/16 ]
  [ "$RESUME" = 0 ]
  CONTAINER_CIDR_ARG=""
  mkdir -p "$FABER_ETC"
  printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=10.50.0.0/24\nFABER_CONTAINER_CIDR=10.220.0.0/16\n' \
    > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ "$CONTAINER_CIDR" = 10.220.0.0/16 ]
  [ "$RESUME" = 1 ]
  CONTAINER_CIDR_ARG=10.230.0.0/16
  resolve_mode
  [ "$CONTAINER_CIDR" = 10.230.0.0/16 ]
  [ "$RESUME" = 0 ]
  # a malformed stored value is ignored
  CONTAINER_CIDR_ARG=""
  printf 'FABER_NEW_MESH=1\nFABER_CONTAINER_CIDR=$(touch /tmp/x)\n' > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ -z "$CONTAINER_CIDR" ]
}

@test "dry-run: --container-cidr or --service-cidr overlapping the mesh CIDR dies before any package command" {
  make_stub_path
  local flag
  for flag in --container-cidr --service-cidr; do
    install_run --dry-run "$flag" 10.99.128.0/17
    echo "case: $flag" >&3
    [ "$status" -eq 1 ]
    [[ "$output" == *"overlaps the mesh CIDR"* ]]
    [[ "$output" != *"apt-get"* ]]
  done
}

@test "real run (new mesh): --container-cidr is a bootstrap setting, stripped once created" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run --container-cidr 10.210.0.0/16
  [ "$status" -eq 1 ]
  grep -qxF 'FABER_NEW_MESH=1' "$FABER_ETC/fabercp.env"
  grep -qxF 'FABER_CONTAINER_CIDR=10.210.0.0/16' "$FABER_ETC/fabercp.env"
  rm "$TMP/init-fails" "$TMP/executed"
  # the re-run keeps the stored value, so fabercp is not restarted
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
  [ "$(count_calls 'try-restart fabercp')" = 0 ]
  run grep -q 'FABER_NEW_MESH\|FABER_MESH_CIDR\|FABER_CONTAINER_CIDR\|FABER_SERVICE_CIDR' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
  ! grep -q "CONF_DIR" "$FABER_ETC/fabercp.env"
  install_run --container-cidr 10.210.0.0/16
  [ "$status" -eq 1 ]
  [[ "$output" == *"--container-cidr only applies to a new mesh"* ]]
}

@test "real run: unfinished new mesh refuses another --container-cidr" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run --container-cidr 10.210.0.0/16
  [ "$status" -eq 1 ]
  local env_before
  env_before="$(cat "$FABER_ETC/fabercp.env")"
  rm "$TMP/executed"
  install_run --container-cidr 10.220.0.0/16
  [ "$status" -eq 1 ]
  [[ "$output" == *"10.210.0.0/16"* ]]
  [ ! -e "$TMP/executed" ]
  [ "$(cat "$FABER_ETC/fabercp.env")" = "$env_before" ]
  rm "$TMP/init-fails"
  install_run --container-cidr 10.210.0.0/16
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
}

@test "real run (join): no new-mesh settings, no conf dirs in fabercp.env, root-owned CNI dir" {
  setup_full_run
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  run grep -q 'FABER_CONTAINER_CIDR\|FABER_NODE_NAME\|FABER_SERVICE_CIDR' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
  ! grep -q "CONF_DIR" "$FABER_ETC/fabercp.env"
  [ "$(stat -c '%U:%G %a' "$CNI_CONF_DIR")" = "root:root 755" ]
}

@test "real run (join): unfinished join refuses --container-cidr" {
  setup_full_run systemd
  install_run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [ -f "$FABER_STATE_DIR/identity.json" ]
  install_run --container-cidr 10.210.0.0/16
  [ "$status" -eq 1 ]
  [[ "$output" == *"--container-cidr only applies to a new mesh"* ]]
}

# --- CNI conf dir ---------------------------------------------------------

@test "configure_cni_conf_dir: creates the directory as root:root 0755, repairs owner and mode" {
  need_root_faber_account
  run configure_cni_conf_dir
  [ "$status" -eq 0 ]
  [[ "$output" != *"WARN"* ]]
  [ "$(stat -c '%U:%G %a' "$CNI_CONF_DIR")" = "root:root 755" ]
  [ "$(stat -c '%U:%G' "$TMP/etc/cni")" = "root:root" ]
  chown root:root "$CNI_CONF_DIR"
  chmod 0700 "$CNI_CONF_DIR"
  run configure_cni_conf_dir
  [ "$status" -eq 0 ]
  [ "$(stat -c '%U:%G %a' "$CNI_CONF_DIR")" = "root:root 755" ]
}

@test "configure_cni_conf_dir: dry-run prints install -d and creates nothing" {
  DRY_RUN=1
  run configure_cni_conf_dir
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ install -d -o root -g root -m 0755 $CNI_CONF_DIR"* ]]
  [ ! -e "$TMP/etc/cni" ]
}

@test "foreign_cni_configs: lists config files other than faber's" {
  [ -z "$(foreign_cni_configs)" ]
  mkdir -p "$CNI_CONF_DIR/subdir.conf"
  # not loaded by containerd: a directory, other extensions, fabercp's temp file
  touch "$CNI_CONF_DIR/10-faber.conflist" "$CNI_CONF_DIR/README" "$CNI_CONF_DIR/.10-faber.conflist.42.tmp"
  [ -z "$(foreign_cni_configs)" ]
  touch "$CNI_CONF_DIR/87-podman-bridge.conflist" "$CNI_CONF_DIR/05-other.conf" \
    "$CNI_CONF_DIR/99-x.json" "$CNI_CONF_DIR/.hidden.conf"
  [ "$(foreign_cni_configs)" = "$(printf '%s\n' .hidden.conf 05-other.conf 87-podman-bridge.conflist 99-x.json)" ]
}

@test "configure_cni_conf_dir: warns about foreign configs by name, not about faber's own" {
  need_root_faber_account
  mkdir -p "$CNI_CONF_DIR"
  touch "$CNI_CONF_DIR/10-faber.conflist"
  run configure_cni_conf_dir
  [ "$status" -eq 0 ]
  [[ "$output" != *"WARN"* ]]
  touch "$CNI_CONF_DIR/05-other.conf" "$CNI_CONF_DIR/87-podman-bridge.conflist"
  run configure_cni_conf_dir
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARN: $CNI_CONF_DIR holds CNI configs that faber did not write: 05-other.conf 87-podman-bridge.conflist"* ]]
  [[ "$output" == *"lexically first"* ]]
  # nothing is removed or changed
  [ -e "$CNI_CONF_DIR/05-other.conf" ]
  [ -e "$CNI_CONF_DIR/87-podman-bridge.conflist" ]
}

@test "configure_cni_conf_dir: a hostile file name is printed quoted" {
  need_root_faber_account
  mkdir -p "$CNI_CONF_DIR"
  touch "$CNI_CONF_DIR/$(printf 'a\033[31m.conf')"
  run configure_cni_conf_dir
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARN"* ]]
  [[ "$output" != *$'\033'* ]]
}

@test "real run: foreign CNI config warns and the install still succeeds" {
  setup_full_run
  mkdir -p "$CNI_CONF_DIR"
  touch "$CNI_CONF_DIR/00-foreign.conflist"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"faber did not write: 00-foreign.conflist"* ]]
  [ -e "$CNI_CONF_DIR/00-foreign.conflist" ]
}

@test "--check reports the CNI conf dir state" {
  make_stub_path
  mkdir -p "$CNI_CONF_DIR"
  install_run --check
  [ "$status" -eq 0 ]
  grep -qxF "cni conf dir:    $CNI_CONF_DIR (ok)" <<<"$output"
  touch "$CNI_CONF_DIR/10-faber.conflist" "$CNI_CONF_DIR/00-foreign.conflist"
  install_run --check
  grep -qxF "cni conf dir:    $CNI_CONF_DIR (other configs: 00-foreign.conflist)" <<<"$output"
  rm -rf "$CNI_CONF_DIR"
  ln -s "$TMP" "$CNI_CONF_DIR"
  install_run --check
  [ "$status" -eq 0 ]
  grep -qxF "cni conf dir:    $CNI_CONF_DIR (symlink)" <<<"$output"
  rm "$CNI_CONF_DIR"
  echo x > "$CNI_CONF_DIR"
  install_run --check
  grep -qxF "cni conf dir:    $CNI_CONF_DIR (not a directory)" <<<"$output"
  [ ! -e "$TMP/executed" ]
}

# --- forward-policy warning -----------------------------------------------

# stub_nft_ruleset: `nft list ruleset` prints $TMP/ruleset (exit 1 when that
# file is absent); every call is recorded in $TMP/nft-calls.
stub_nft_ruleset() {
  mkdir -p "$TMP/bin"
  cat > "$TMP/bin/nft" <<EOF
#!/bin/sh
echo "nft \$*" >> "$TMP/nft-calls"
case "\$*" in
  "list ruleset") [ -f "$TMP/ruleset" ] && { cat "$TMP/ruleset"; exit 0; }; echo "Operation not permitted" >&2; exit 1 ;;
  --version) echo "nftables v1.0.9 (Old Doc Yak #3)" ;;
esac
exit 0
EOF
  chmod +x "$TMP/bin/nft"
}

ruleset_forward_drop() {
  cat > "$TMP/ruleset" <<'EOF'
table inet faber {
	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		iifname "faber0" drop
	}
}
table ip filter {
	chain INPUT {
		type filter hook input priority filter; policy drop;
	}

	chain FORWARD {
		type filter hook forward priority filter; policy drop;
		counter packets 0 bytes 0 jump DOCKER-USER
	}

	chain DOCKER-USER {
		counter packets 0 bytes 0 return
	}
}
EOF
}

ruleset_forward_accept() {
  cat > "$TMP/ruleset" <<'EOF'
table inet faber {
	chain forward {
		type filter hook forward priority filter - 10; policy accept;
		iifname "faber0" drop
	}
}
table inet filter {
	chain input {
		type filter hook input priority filter; policy drop;
	}

	chain forward {
		type filter hook forward priority filter; policy accept;
	}

	chain helper {
		ip saddr 10.0.0.0/8 drop
	}
}
EOF
}

@test "forward_drop_chains: names base chains on the forward hook with policy drop" {
  ruleset_forward_drop
  [ "$(forward_drop_chains < "$TMP/ruleset")" = "ip filter FORWARD" ]
  ruleset_forward_accept
  [ -z "$(forward_drop_chains < "$TMP/ruleset")" ]
  [ -z "$(forward_drop_chains < /dev/null)" ]
  printf '%s\n' 'table inet firewalld {' '	chain filter_FORWARD {' \
    '		type filter hook forward priority filter + 10; policy drop;' '	}' '}' \
    'table ip6 filter {' '	chain FORWARD {' \
    '		type filter hook forward priority filter; policy drop;' '	}' '}' > "$TMP/ruleset"
  [ "$(forward_drop_chains < "$TMP/ruleset")" = "$(printf '%s\n' \
    'inet firewalld filter_FORWARD' 'ip6 filter FORWARD')" ]
}

@test "warn_forward_drop: warns when a forward chain has policy drop" {
  stub_nft_ruleset
  ruleset_forward_drop
  PATH="$TMP/bin:$PATH" run warn_forward_drop
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARN:"*"ip filter FORWARD"* ]]
  [[ "$output" == *"faber0"* ]]
  [[ "$output" == *"faberbr0"* ]]
  # read-only: the only nft call lists the ruleset
  [ "$(cat "$TMP/nft-calls")" = "nft list ruleset" ]
}

@test "warn_forward_drop: silent when no forward chain drops by default" {
  stub_nft_ruleset
  ruleset_forward_accept
  PATH="$TMP/bin:$PATH" run warn_forward_drop
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "warn_forward_drop: silent in dry-run, without nft, and when nft fails" {
  stub_nft_ruleset
  ruleset_forward_drop
  DRY_RUN=1
  PATH="$TMP/bin:$PATH" run warn_forward_drop
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  [ ! -e "$TMP/nft-calls" ]
  DRY_RUN=0
  PATH="$TMP/empty" run warn_forward_drop
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  rm "$TMP/ruleset"
  PATH="$TMP/bin:$PATH" run warn_forward_drop
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "real run: forward policy drop warns after the kernel step and changes no firewall" {
  setup_full_run systemd
  stub_nft_ruleset
  ruleset_forward_drop
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARN:"*"ip filter FORWARD"*"faberbr0"* ]]
  [[ "$output" == *"cluster: created"* ]]
  # the warning comes after the sysctl step and before the services start
  local warn_line
  warn_line="$(grep -n 'faberbr0' <<<"$output" | head -n 1 | cut -d: -f1)"
  [ "$warn_line" -gt "$(grep -n 'sysctl.d/99-faber.conf' <<<"$output" | head -n 1 | cut -d: -f1)" ]
  [ "$warn_line" -lt "$(grep -n 'waiting up to' <<<"$output" | head -n 1 | cut -d: -f1)" ]
  # read-only: nft is only asked for its version and the ruleset
  run grep -v -e '^nft list ruleset$' -e '^nft --version$' "$TMP/nft-calls"
  [ "$status" -eq 1 ]
}

@test "dry-run: forward policy drop is not inspected" {
  make_stub_path
  stub_nft_ruleset
  ruleset_forward_drop
  install_run --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" != *"faberbr0"* ]]
  run grep -q 'list ruleset' "$TMP/nft-calls"
  [ "$status" -ne 0 ]
}

# --- container CIDR: host bits ---------------------------------------------

@test "valid_container_cidr: the address must be the network address" {
  local bad
  valid_container_cidr 10.200.0.0/16
  valid_container_cidr 10.128.0.0/9
  valid_container_cidr 10.200.4.0/22
  valid_container_cidr 128.0.0.0/3
  for bad in 10.200.1.5/16 10.200.1.0/16 10.200.0.1/24 10.200.5.0/22 10.0.0.0/0 010.200.000.000/16 010.200.001.000/16; do
    run valid_container_cidr "$bad"
    [ "$status" -ne 0 ]
  done
}

@test "script: --container-cidr with host bits exits 2" {
  local bad
  for bad in 10.200.1.5/16 10.200.1.0/16 10.200.0.1/24; do
    run bash "$SCRIPT" --dry-run --container-cidr "$bad"
    [ "$status" -eq 2 ]
    [[ "$output" == *"--container-cidr must be an IPv4 CIDR"* ]]
    [[ "$output" == *"network address"* ]]
  done
  run bash "$SCRIPT" --dry-run "--container-cidr=10.200.1.5/16"
  [ "$status" -eq 2 ]
}

@test "resolve_mode: a stored container CIDR with host bits is not used" {
  mkdir -p "$FABER_ETC"
  printf 'FABER_NEW_MESH=1\nFABER_CONTAINER_CIDR=10.220.1.5/16\n' > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ "$MODE" = new ]
  [ -z "$CONTAINER_CIDR" ]
}

# --- node name --------------------------------------------------------------

@test "valid_node_name: a DNS label" {
  local good bad long63 long64
  long63="$(printf 'a%.0s' $(seq 1 63))"
  long64="${long63}a"
  for good in a 7 web-01 node-1 0-0 a--b "$long63"; do
    valid_node_name "$good"
  done
  for bad in "" - -a a- Web web_01 web.example "web 01" "web-01 " $'web\n01' "wéb" "$long64" 'a;b' '$(id)'; do
    run valid_node_name "$bad"
    [ "$status" -ne 0 ]
  done
}

@test "valid_node_name: prints nothing" {
  run valid_node_name web-01
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  run valid_node_name 'Bad Name'
  [ "$status" -ne 0 ]
  [ -z "$output" ]
}

@test "parse_args: --name, space and = forms" {
  parse_args --name web-01
  [ "$NODE_NAME_ARG" = web-01 ]
  NODE_NAME_ARG=""
  parse_args --name=db-2 --mesh-cidr 10.50.0.0/16
  [ "$NODE_NAME_ARG" = db-2 ]
}

@test "script: bad --name exits 2" {
  local bad
  for bad in Web web_01 web.example.com a- "web 01" "$(printf 'a%.0s' $(seq 1 64))"; do
    run bash "$SCRIPT" --dry-run --name "$bad"
    [ "$status" -eq 2 ]
    [[ "$output" == *"--name must be a DNS label"* ]]
  done
  run bash "$SCRIPT" --dry-run --name -web
  [ "$status" -eq 2 ]
  run bash "$SCRIPT" --check --name Web
  [ "$status" -eq 2 ]
}

@test "default_node_name: short hostname in lower case" {
  set_hostname Web-01 Web-01.example.com
  run default_node_name
  [ "$status" -eq 0 ]
  [ "$output" = web-01 ]
  # some hostname commands print the FQDN even with -s
  set_hostname DB2.Example.com
  [ "$(default_node_name)" = db2 ]
}

@test "default_node_name: falls back to hostname with the domain stripped" {
  set_hostname "" Edge-7.example.com
  run default_node_name
  [ "$status" -eq 0 ]
  [ "$output" = edge-7 ]
}

@test "default_node_name: no hostname command gives nothing" {
  unset -f hostname
  mkdir -p "$TMP/empty"
  PATH="$TMP/empty" run default_node_name
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "resolve_mode: new mesh takes the name from --name, else the hostname" {
  resolve_mode
  [ "$MODE" = new ]
  [ "$NODE_NAME" = testnode ]
  NODE_NAME_ARG=web-01
  resolve_mode
  [ "$NODE_NAME" = web-01 ]
}

@test "resolve_mode: a hostname that is no DNS label dies and asks for --name" {
  local bad
  for bad in my_host -edge "$(printf 'a%.0s' $(seq 1 64))"; do
    set_hostname "$bad"
    run resolve_mode
    [ "$status" -eq 1 ]
    [[ "$output" == *"--name <name>"* ]]
  done
  set_hostname "" ""
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"--name <name>"* ]]
  # --name is the way out
  set_hostname my_host
  NODE_NAME_ARG=web-01
  resolve_mode
  [ "$NODE_NAME" = web-01 ]
}

@test "resolve_mode: a join never needs a node name" {
  set_hostname my_host
  JOIN_REQUESTED=1 JOIN_TOKEN="$TOKEN"
  resolve_mode
  [ "$MODE" = join ]
  [ -z "$NODE_NAME" ]
  # nor does a bootstrapped node
  JOIN_REQUESTED=0
  mkdir -p "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  resolve_mode
  [ "$MODE" = existing ]
  [ -z "$NODE_NAME" ]
}

@test "resolve_mode: the stored name is kept on a re-run of an unfinished new mesh" {
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=10.50.0.0/24\nFABER_NODE_NAME=web-01\n' > "$FABER_ETC/fabercp.env"
  # no identity yet: the stored name wins over the hostname, --name over both
  resolve_mode
  [ "$MODE" = new ]
  [ "$RESUME" = 1 ]
  [ "$NODE_NAME" = web-01 ]
  NODE_NAME_ARG=web-02
  resolve_mode
  [ "$NODE_NAME" = web-02 ]
  # identity there: another --name is refused, the same one is fine
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"already started a new mesh with --name web-01"* ]]
  NODE_NAME_ARG=web-01
  resolve_mode
  [ "$NODE_NAME" = web-01 ]
  NODE_NAME_ARG=""
  set_hostname my_host
  resolve_mode
  [ "$NODE_NAME" = web-01 ]
}

@test "resolve_mode: a malformed stored name is ignored" {
  mkdir -p "$FABER_ETC"
  printf 'FABER_NEW_MESH=1\nFABER_NODE_NAME=$(touch /tmp/x)\n' > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ "$NODE_NAME" = testnode ]
}

@test "resolve_mode: --name is refused on a bootstrapped or joining node" {
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  printf 'FABER_WG_PORT=51820\n' > "$FABER_ETC/fabercp.env"
  NODE_NAME_ARG=web-01
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"--name only applies to a new mesh"* ]]
  printf '%s\n' "$TOKEN" > "$FABER_ETC/join.token"
  printf 'FABER_JOIN_TOKEN_FILE=%s\n' "$FABER_ETC/join.token" > "$FABER_ETC/fabercp.env"
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"--name only applies to a new mesh"* ]]
}

@test "resolve_mode: --name on a node with a stored join and no identity starts a new mesh" {
  mkdir -p "$FABER_ETC"
  printf '%s\n' "$TOKEN" > "$FABER_ETC/join.token"
  printf 'FABER_JOIN_TOKEN_FILE=%s\n' "$FABER_ETC/join.token" > "$FABER_ETC/fabercp.env"
  NODE_NAME_ARG=web-01
  resolve_mode
  [ "$MODE" = new ]
  [ "$NODE_NAME" = web-01 ]
}

@test "render_env: new mesh carries FABER_NODE_NAME after the mesh CIDR" {
  MODE=new WG_ENDPOINT="203.0.113.7:51820" MESH_CIDR="10.99.0.0/16" NODE_NAME=web-01 CONTAINER_CIDR="10.210.0.0/16"
  run render_env 1
  [ "$status" -eq 0 ]
  [ "$(tail -n 5 <<<"$output")" = "$(printf '%s\n' \
    'FABER_NEW_MESH=1' \
    'FABER_MESH_CIDR=10.99.0.0/16' \
    'FABER_NODE_NAME=web-01' \
    'FABER_CONTAINER_CIDR=10.210.0.0/16' \
    'FABER_SERVICE_CIDR=10.201.0.0/16')" ]
  run render_env 0
  [[ "$output" != *"FABER_NODE_NAME"* ]]
  MODE=join
  run render_env 1
  [[ "$output" != *"FABER_NODE_NAME"* ]]
}

@test "dry-run: an unusable hostname dies before any package command" {
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR"
  set_hostname my_host
  install_run --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"--name <name>"* ]]
  [[ "$output" != *"apt-get"* ]]
  install_run --dry-run --name web-01
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: would-create"* ]]
  # a join does not look at the hostname
  install_run --dry-run --join "$TOKEN"
  [ "$status" -eq 0 ]
}

@test "real run (new mesh): FABER_NODE_NAME is a bootstrap setting, stripped once created" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run --name web-01
  [ "$status" -eq 1 ]
  grep -qxF 'FABER_NEW_MESH=1' "$FABER_ETC/fabercp.env"
  grep -qxF 'FABER_NODE_NAME=web-01' "$FABER_ETC/fabercp.env"
  rm "$TMP/init-fails" "$TMP/executed"
  # the hostname changed meanwhile: the re-run keeps the stored name
  set_hostname other
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
  [ "$(count_calls 'try-restart fabercp')" = 0 ]
  run grep -q 'FABER_NEW_MESH\|FABER_MESH_CIDR\|FABER_NODE_NAME' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
  install_run --name web-01
  [ "$status" -eq 1 ]
  [[ "$output" == *"--name only applies to a new mesh"* ]]
}

@test "real run (new mesh): the default name is the lower-case short hostname" {
  setup_full_run systemd
  set_hostname Web-01 Web-01.example.com
  touch "$TMP/no-identity"
  install_run
  grep -qxF 'FABER_NODE_NAME=web-01' "$FABER_ETC/fabercp.env"
}

@test "real run: unfinished new mesh refuses another --name" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run --name web-01
  [ "$status" -eq 1 ]
  rm "$TMP/executed"
  install_run --name web-02
  [ "$status" -eq 1 ]
  [[ "$output" == *"web-01"* ]]
  [ ! -e "$TMP/executed" ]
  grep -qxF 'FABER_NODE_NAME=web-01' "$FABER_ETC/fabercp.env"
}

# --- --reset ----------------------------------------------------------------

# write_joining_identity FILE NODE_ID: identity.json of a join that has not
# finished (it still has pending_join).
write_joining_identity() {
  printf '{\n  "v": 1,\n  "node_id": %s,\n  "private_key": "SECRETKEY",\n  "instance_id": "00",\n  "pending_join": {\n    "invite_id": "ab"\n  }\n}\n' "$2" > "$1"
}

@test "script: --reset without --join exits 2" {
  run bash "$SCRIPT" --dry-run --reset
  [ "$status" -eq 2 ]
  [[ "$output" == *"--reset needs --join"* ]]
  run bash "$SCRIPT" --check --reset
  [ "$status" -eq 2 ]
  run bash "$SCRIPT" --reset --mesh-cidr 10.50.0.0/16
  [ "$status" -eq 2 ]
  run bash "$SCRIPT" --reset=1 --join "$TOKEN"
  [ "$status" -eq 2 ]
}

@test "parse_args: --reset with --join" {
  parse_args --join "$TOKEN" --reset
  [ "$RESET_REQUESTED" = 1 ]
  [ "$JOIN_REQUESTED" = 1 ]
  RESET_REQUESTED=0
  parse_args --join "$TOKEN"
  [ "$RESET_REQUESTED" = 0 ]
}

@test "identity_pending_join: only for a regular identity.json that has pending_join" {
  mkdir -p "$FABER_STATE_DIR"
  local f="$FABER_STATE_DIR/identity.json"
  run identity_pending_join
  [ "$status" -ne 0 ]
  write_identity "$f" 2
  run identity_pending_join
  [ "$status" -ne 0 ]
  [ -z "$output" ]
  write_joining_identity "$f" 2
  run identity_pending_join
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  echo '{"node_id":2,"pending_join":{"invite_id":"ab"}}' > "$f"
  identity_pending_join
  echo '{"node_id":2,"pending_join":null}' > "$f"
  run identity_pending_join
  [ "$status" -ne 0 ]
  write_joining_identity "$TMP/elsewhere.json" 2
  rm "$f"
  ln -s "$TMP/elsewhere.json" "$f"
  run identity_pending_join
  [ "$status" -ne 0 ]
}

@test "resolve_mode: --join on a bootstrapped node dies unless --reset is given" {
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  write_identity "$FABER_STATE_DIR/identity.json" 2
  printf 'FABER_WG_PORT=51820\n' > "$FABER_ETC/fabercp.env"
  JOIN_REQUESTED=1 JOIN_TOKEN="$TOKEN"
  run resolve_mode
  [ "$status" -eq 1 ]
  [ "$output" = "[faber-install] ERROR: this node is already configured ($FABER_STATE_DIR/identity.json exists); refusing --join. Re-run without --join to upgrade it." ]
  RESET_REQUESTED=1
  run resolve_mode
  [ "$status" -eq 0 ]
  [[ "$output" != *"$TOKEN"* ]]
  resolve_mode
  [ "$MODE" = join ]
  [ "$RESET_IDENTITY" = 1 ]
  [ "$RESUME" = 0 ]
  [ -z "$REPLACED_JOIN_NODE_ID" ]
}

@test "resolve_mode: --reset without an identity has nothing to reset" {
  JOIN_REQUESTED=1 JOIN_TOKEN="$TOKEN" RESET_REQUESTED=1
  run resolve_mode
  [ "$status" -eq 0 ]
  [[ "$output" == *"nothing to reset"* ]]
  resolve_mode
  [ "$MODE" = join ]
  [ "$RESET_IDENTITY" = 0 ]
}

@test "resolve_mode: --reset with a stored join" {
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  printf '%s\n' "$TOKEN" > "$FABER_ETC/join.token"
  printf 'FABER_JOIN_TOKEN_FILE=%s\n' "$FABER_ETC/join.token" > "$FABER_ETC/fabercp.env"
  JOIN_REQUESTED=1 JOIN_TOKEN="$TOKEN" RESET_REQUESTED=1
  # same token, and the identity is the one that token made: keep waiting,
  # a second reset would give the node a new instance id for a used invite
  write_joining_identity "$FABER_STATE_DIR/identity.json" 2
  run resolve_mode
  [ "$status" -eq 0 ]
  [[ "$output" == *"nothing to reset"* ]]
  resolve_mode
  [ "$MODE" = join ]
  [ "$RESET_IDENTITY" = 0 ]
  [ "$RESUME" = 1 ]
  # same token, but the identity is still the old member's (an earlier
  # --reset run stopped before it got that far)
  write_identity "$FABER_STATE_DIR/identity.json" 2
  resolve_mode
  [ "$RESET_IDENTITY" = 1 ]
  [ "$RESUME" = 0 ]
  # another token
  write_joining_identity "$FABER_STATE_DIR/identity.json" 2
  JOIN_TOKEN="faber1.b3RoZXI_OTHERSECRET"
  resolve_mode
  [ "$RESET_IDENTITY" = 1 ]
  [ "$RESUME" = 0 ]
  [ -z "$REPLACED_JOIN_NODE_ID" ]
}

@test "resolve_mode: --reset does not lift the refusal on an unfinished new mesh" {
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  echo '{}' > "$FABER_STATE_DIR/identity.json"
  printf 'FABER_NEW_MESH=1\nFABER_MESH_CIDR=10.50.0.0/24\n' > "$FABER_ETC/fabercp.env"
  JOIN_REQUESTED=1 JOIN_TOKEN="$TOKEN" RESET_REQUESTED=1
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"refusing --join"* ]]
}

@test "dry-run (--join --reset): plans the stop and the marker, changes nothing" {
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR" "$FABER_ETC" "$FABER_STATE_DIR/data"
  write_identity "$FABER_STATE_DIR/identity.json" 2
  echo raft > "$FABER_STATE_DIR/data/CURRENT"
  printf 'FABER_WG_INTERFACE=faber0\nFABER_WG_PORT=51820\n' > "$FABER_ETC/fabercp.env"
  local before
  before="$(cd "$TMP" && find etc var -exec stat -c '%n %s %Y' {} + | sort)"
  install_run --dry-run --join "$TOKEN" --reset
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ systemctl stop fabercp"* ]]
  [[ "$output" == *"+ install -o faber -g faber -m 0644 /dev/null $TMP/var/lib/.faber-stage.XXXXXX/join-failed"* ]]
  [[ "$output" == *"+ mv -fT $TMP/var/lib/.faber-stage.XXXXXX/join-failed $FABER_STATE_DIR/join-failed"* ]]
  [[ "$output" == *"+ write $FABER_ETC/join.token (redacted)"* ]]
  [[ "$output" == *"cluster: would-join"* ]]
  [[ "$output" != *"$TOKEN"* ]]
  # the marker is planned after the token and before the services start
  local stop_line
  stop_line="$(grep -n 'systemctl stop fabercp' <<<"$output" | head -n 1 | cut -d: -f1)"
  [ "$stop_line" -gt "$(grep -n 'join.token (redacted)' <<<"$output" | head -n 1 | cut -d: -f1)" ]
  [ "$stop_line" -lt "$(grep -n 'mv -fT .*join-failed' <<<"$output" | head -n 1 | cut -d: -f1)" ]
  [ "$stop_line" -lt "$(grep -n 'systemctl enable --now' <<<"$output" | head -n 1 | cut -d: -f1)" ]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_STATE_DIR/join-failed" ]
  [ "$(cd "$TMP" && find etc var -exec stat -c '%n %s %Y' {} + | sort)" = "$before" ]
}

@test "dry-run: --join on a bootstrapped node without --reset still dies" {
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR" "$FABER_ETC" "$FABER_STATE_DIR"
  write_identity "$FABER_STATE_DIR/identity.json" 2
  install_run --dry-run --join "$TOKEN"
  [ "$status" -eq 1 ]
  [[ "$output" == *"already configured ($FABER_STATE_DIR/identity.json exists); refusing --join. Re-run without --join to upgrade it."* ]]
  [[ "$output" != *"apt-get"* ]]
  [[ "$output" != *"join-failed"* ]]
}

@test "real run (--join --reset): a bootstrapped node is stopped, marked and joins again" {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [ ! -e "$TMP/discards" ]
  mkdir -p "$FABER_STATE_DIR/data"
  echo raft > "$FABER_STATE_DIR/data/CURRENT"
  rm "$TMP/executed"
  local other="faber1.b3RoZXI_OTHERSECRET"
  install_run --join - --reset <<<"$other"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [[ "$output" != *"$other"* ]]
  [ "$(count_calls 'systemctl stop fabercp')" = 1 ]
  # fabercp was stopped with the new token and the join setting in place,
  # and the marker was written only after that
  [ "$(cat "$TMP/token-at-stop")" = "$other" ]
  grep -q '^FABER_JOIN_TOKEN_FILE=' "$TMP/env-at-stop"
  [ ! -e "$TMP/marker-before-stop" ]
  # the marker as fabercp found it at its next start: empty, faber:faber 0644
  [ "$(cat "$TMP/marker-at-start")" = "regular empty file faber:faber 644 0" ]
  [ "$(wc -l < "$TMP/discards")" = 1 ]
  [ ! -e "$FABER_STATE_DIR/data/CURRENT" ]
  [ ! -e "$FABER_STATE_DIR/join-failed" ]
  local stop start
  stop="$(grep -n 'systemctl stop fabercp' "$TMP/executed" | head -n 1 | cut -d: -f1)"
  start="$(grep -n 'systemctl enable --now' "$TMP/executed" | head -n 1 | cut -d: -f1)"
  [ "$stop" -lt "$start" ]
  # joined: the bootstrap settings and the token are gone again
  run grep -q 'FABER_JOIN_TOKEN_FILE' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
  [ ! -e "$FABER_ETC/join.token" ]
  # nothing is left in the staging parent
  [ -z "$(ls -A "$TMP/var/lib" | grep -v '^faber$')" ]
  run grep -F "$other" "$TMP/executed"
  [ "$status" -eq 1 ]
}

@test "real run (--join --reset): the stored endpoint of the old identity is dropped" {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN" --endpoint 198.51.100.2
  [ "$status" -eq 0 ]
  grep -qxF 'FABER_WG_ENDPOINT=198.51.100.2:51820' "$FABER_ETC/fabercp.env"
  install_run --join "faber1.b3RoZXI_OTHERSECRET" --reset
  [ "$status" -eq 0 ]
  run grep -q 'FABER_WG_ENDPOINT' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
}

@test "real run (--join --reset): re-running the same command after a join timeout does not reset twice" {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  rm "$TMP/joined" "$TMP/executed"
  # from here on fabercp writes the identity of a join that has not finished
  write_joining_identity "$TMP/identity-after" 5
  local other="faber1.b3RoZXI_OTHERSECRET"
  install_run --join "$other" --reset
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  [ "$(wc -l < "$TMP/discards")" = 1 ]
  rm "$TMP/executed"
  install_run --join "$other" --reset
  [ "$status" -eq 1 ]
  [[ "$output" == *"nothing to reset"* ]]
  [ "$(count_calls 'systemctl stop fabercp')" = 0 ]
  [ "$(wc -l < "$TMP/discards")" = 1 ]
  [ ! -e "$FABER_STATE_DIR/join-failed" ]
  touch "$TMP/joined"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [ "$(wc -l < "$TMP/discards")" = 1 ]
}

@test "real run (--join --reset): without an identity it is a plain join" {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN" --reset
  [ "$status" -eq 0 ]
  [[ "$output" == *"nothing to reset"* ]]
  [[ "$output" == *"cluster: joined"* ]]
  [ "$(count_calls 'systemctl stop fabercp')" = 0 ]
  [ ! -e "$TMP/discards" ]
  [ ! -e "$FABER_STATE_DIR/join-failed" ]
}

@test "real run (--join --reset, no systemd): the marker is written and nothing is stopped" {
  setup_full_run
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  write_identity "$FABER_STATE_DIR/identity.json" 2
  install_run --join "$TOKEN" --reset
  [ "$status" -eq 0 ]
  [ "$(count_calls systemctl)" = 0 ]
  [ "$(stat -c '%F %U:%G %a %s' "$FABER_STATE_DIR/join-failed")" = "regular empty file faber:faber 644 0" ]
  [[ "$output" == *"WARN:"*"stop fabercp"* ]]
  [[ "$output" == *"cluster: skipped (no systemd)"* ]]
  [ "$(cat "$FABER_ETC/join.token")" = "$TOKEN" ]
  # the identity itself is fabercp's to remove
  [ -f "$FABER_STATE_DIR/identity.json" ]
}

@test "real run (--join --reset): a symlink planted at the marker is replaced, not followed" {
  setup_full_run
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  write_identity "$FABER_STATE_DIR/identity.json" 2
  echo precious > "$TMP/precious"
  chmod 0600 "$TMP/precious"
  ln -s "$TMP/precious" "$FABER_STATE_DIR/join-failed"
  install_run --join "$TOKEN" --reset
  [ "$status" -eq 0 ]
  [ ! -L "$FABER_STATE_DIR/join-failed" ]
  [ -f "$FABER_STATE_DIR/join-failed" ]
  [ "$(cat "$TMP/precious")" = precious ]
  [ "$(stat -c '%U:%G %a' "$TMP/precious")" = "root:root 600" ]
}

@test "real run (--join --reset): a symlinked state directory is refused before anything changes" {
  setup_full_run systemd
  mkdir -p "$TMP/elsewhere" "$(dirname "$FABER_STATE_DIR")"
  write_identity "$TMP/elsewhere/identity.json" 2
  ln -s "$TMP/elsewhere" "$FABER_STATE_DIR"
  install_run --join "$TOKEN" --reset
  [ "$status" -eq 1 ]
  [[ "$output" == *"is a symlink"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$TMP/elsewhere/join-failed" ]
  [ ! -e "$FABER_ETC" ]
}

@test "real run (--join --reset): fabercp that cannot be stopped leaves no marker" {
  setup_full_run systemd
  mkdir -p "$FABER_ETC" "$FABER_STATE_DIR"
  write_identity "$FABER_STATE_DIR/identity.json" 2
  sed -i 's|^  \*stop\*fabercp\*)$|  *stop*fabercp*) exit 1 ;;\n  *never*)|' "$TMP/bin/systemctl"
  install_run --join "$TOKEN" --reset
  [ "$status" -eq 1 ]
  [[ "$output" == *"could not stop fabercp"* ]]
  [ ! -e "$FABER_STATE_DIR/join-failed" ]
  [ "$(count_calls 'systemctl enable --now')" = 0 ]
}

# --- node_removed -----------------------------------------------------------

@test "warn_bootstrap_error: node_removed points at --join --reset" {
  mkdir -p "$FABER_STATE_DIR"
  printf 'node_removed\n' > "$FABER_STATE_DIR/bootstrap-error"
  MODE=join
  run warn_bootstrap_error
  [ "$status" -eq 0 ]
  [[ "$output" == *"the cluster removed this node"* ]]
  [[ "$output" == *"--join <token> --reset"* ]]
  [[ "$output" != *"node_removed"* ]]
  MODE=new ENDPOINT_ARG=""
  run warn_bootstrap_error
  [ "$status" -eq 1 ]
  [ -z "$output" ]
}

# removed_node: a node that joined and that the cluster then removed. fabercp
# wrote the node_removed record, discarded the identity and left its marker.
removed_node() {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  rm "$FABER_STATE_DIR/identity.json" "$TMP/executed"
  printf 'node_removed\n' > "$FABER_STATE_DIR/bootstrap-error"
}

@test "real run: a node the cluster removed refuses to start a new mesh without --join" {
  removed_node
  cp "$FABER_ETC/fabercp.env" "$TMP/env-before"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"the cluster removed this node"* ]]
  [[ "$output" == *"--join <token> --reset"* ]]
  [[ "$output" == *"$FABER_STATE_DIR/bootstrap-error"* ]]
  [[ "$output" != *"mode: new"* ]]
  [[ "$output" != *"node_removed"* ]]
  # preflight: nothing ran and nothing changed
  [ ! -e "$TMP/executed" ]
  run cmp "$TMP/env-before" "$FABER_ETC/fabercp.env"
  [ "$status" -eq 0 ]
  run grep -q 'FABER_NEW_MESH' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
  [ ! -e "$FABER_STATE_DIR/identity.json" ]
}

@test "real run: a removed node refuses new-mesh flags and --dry-run too" {
  removed_node
  local args
  for args in "--mesh-cidr 10.50.0.0/16" "--name other" "--container-cidr 10.201.0.0/16" "--dry-run"; do
    # shellcheck disable=SC2086
    install_run $args
    [ "$status" -eq 1 ]
    [[ "$output" == *"the cluster removed this node"* ]]
    [ ! -e "$TMP/executed" ]
  done
}

@test "real run: a removed node on a server that never had faber files is refused as well" {
  setup_full_run systemd
  mkdir -p "$FABER_STATE_DIR"
  printf 'node_removed\n' > "$FABER_STATE_DIR/bootstrap-error"
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"the cluster removed this node"* ]]
  [ ! -e "$FABER_ETC" ]
  [ ! -e "$TMP/executed" ]
}

@test "real run: a removed node rejoins with --join" {
  removed_node
  install_run --join "faber1.b3RoZXI_OTHERSECRET"
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [[ "$output" != *"the cluster removed this node"* ]]
}

@test "real run: a removed node rejoins with --join --reset" {
  removed_node
  install_run --join "faber1.b3RoZXI_OTHERSECRET" --reset
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [[ "$output" != *"the cluster removed this node"* ]]
}

@test "real run: a removed node whose new join has not finished continues it without --join" {
  removed_node
  rm "$TMP/joined"
  touch -d '2020-01-01 00:00:00' "$FABER_STATE_DIR/bootstrap-error"
  install_run --join "faber1.b3RoZXI_OTHERSECRET"
  [ "$status" -eq 1 ]
  [[ "$output" == *"cluster: failed"* ]]
  # the record is older than the new token, so it is about the old membership
  [ "$FABER_ETC/join.token" -nt "$FABER_STATE_DIR/bootstrap-error" ]
  touch "$TMP/joined"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: joined"* ]]
  [[ "$output" != *"the cluster removed this node"* ]]
}

@test "real run: removing the record lets a removed node start a new mesh" {
  removed_node
  rm "$FABER_STATE_DIR/bootstrap-error"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
}

@test "real run: other bootstrap-error records and a symlinked one do not block a new mesh" {
  local code
  for code in token_expired token_invalid invite_rejected; do
    removed_node
    printf '%s\n' "$code" > "$FABER_STATE_DIR/bootstrap-error"
    install_run
    [ "$status" -eq 0 ]
    [[ "$output" == *"cluster: created"* ]]
    rm -rf "$FABER_STATE_DIR" "$FABER_ETC"
  done
  removed_node
  printf 'node_removed\n' > "$TMP/elsewhere"
  rm "$FABER_STATE_DIR/bootstrap-error"
  ln -s "$TMP/elsewhere" "$FABER_STATE_DIR/bootstrap-error"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
}

# --- leading zeros in IPv4 addresses ----------------------------------------

@test "valid_ipv4: an octet with a leading zero is refused, a lone 0 is fine" {
  local ip
  for ip in 0.0.0.0 10.0.0.5 203.0.113.7 255.255.255.255 100.200.0.10; do
    run valid_ipv4 "$ip"
    [ "$status" -eq 0 ]
  done
  for ip in 010.0.0.5 10.00.0.5 10.0.000.5 10.0.0.05 10.0.0.005 00.0.0.0 01.2.3.4 1.2.3.08; do
    run valid_ipv4 "$ip"
    [ "$status" -ne 0 ]
  done
}

@test "normalize_endpoint and the CIDR checks refuse leading zeros" {
  run normalize_endpoint 010.0.0.5
  [ "$status" -ne 0 ]
  run normalize_endpoint 10.0.0.05:51820
  [ "$status" -ne 0 ]
  run normalize_endpoint 10.0.0.5:0500
  [ "$status" -eq 0 ]
  [ "$output" = "10.0.0.5:500" ]
  run valid_mesh_cidr 010.99.0.0/16
  [ "$status" -ne 0 ]
  run valid_mesh_cidr 10.99.00.0/16
  [ "$status" -ne 0 ]
  run valid_mesh_cidr 10.0.0.0/8
  [ "$status" -eq 0 ]
  run valid_container_cidr 010.200.0.0/16
  [ "$status" -ne 0 ]
  run valid_container_cidr 10.200.0.00/16
  [ "$status" -ne 0 ]
  run valid_container_cidr 10.200.0.0/16
  [ "$status" -eq 0 ]
}

@test "script: --endpoint, --mesh-cidr and --container-cidr with a leading zero exit 2" {
  run bash "$SCRIPT" --dry-run --endpoint 010.0.0.5
  [ "$status" -eq 2 ]
  [[ "$output" == *"--endpoint"* ]]
  run bash "$SCRIPT" --dry-run --mesh-cidr 010.99.0.0/16
  [ "$status" -eq 2 ]
  [[ "$output" == *"--mesh-cidr"* ]]
  run bash "$SCRIPT" --dry-run --container-cidr 010.200.0.0/16
  [ "$status" -eq 2 ]
  [[ "$output" == *"--container-cidr"* ]]
}

@test "detect_endpoint and resolve_endpoint do not take an address with a leading zero" {
  make_stub_path
  sed 's/203\.0\.113\.7/203.0.113.07/' "$FIXTURES/ip-route-get" > "$TMP/ip-route"
  grep -q '203.0.113.07' "$TMP/ip-route"
  PATH="$TMP/bin:$PATH" run detect_endpoint
  [ "$status" -ne 0 ]
  # a stored value with a leading zero is not reused
  cp "$FIXTURES/ip-route-get" "$TMP/ip-route"
  mkdir -p "$FABER_ETC"
  printf 'FABER_WG_INTERFACE=faber0\nFABER_WG_ENDPOINT=010.0.0.5:51820\n' > "$FABER_ETC/fabercp.env"
  MODE=new ENDPOINT_ARG=""
  PATH="$TMP/bin:$PATH" resolve_endpoint
  [ "$WG_ENDPOINT" = "203.0.113.7:51820" ]
}

@test "real run: a stale node_removed record next to an identity does not block an upgrade re-run" {
  setup_full_run systemd
  touch "$TMP/joined"
  install_run --join "$TOKEN"
  [ "$status" -eq 0 ]
  # the node is configured and healthy; only the record is left over
  [ -f "$FABER_STATE_DIR/identity.json" ]
  printf 'node_removed\n' > "$FABER_STATE_DIR/bootstrap-error"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: existing"* ]]
  [[ "$output" != *"the cluster removed this node"* ]]
  install_run --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" != *"the cluster removed this node"* ]]
}

@test "check_node_removed: applies only to a node without an identity" {
  mkdir -p "$FABER_STATE_DIR"
  printf 'node_removed\n' > "$FABER_STATE_DIR/bootstrap-error"
  JOIN_REQUESTED=0 CONFIGURED=yes
  run check_node_removed
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  JOIN_REQUESTED=0 CONFIGURED=no
  run check_node_removed
  [ "$status" -eq 1 ]
  [[ "$output" == *"the cluster removed this node"* ]]
  JOIN_REQUESTED=1 CONFIGURED=no
  run check_node_removed
  [ "$status" -eq 0 ]
}

# --- --service-cidr -------------------------------------------------------

@test "parse_args: --service-cidr, space and = forms" {
  parse_args --service-cidr 10.77.0.0/16
  [ "$SERVICE_CIDR_ARG" = "10.77.0.0/16" ]
  SERVICE_CIDR_ARG=""
  parse_args --service-cidr=10.78.0.0/24
  [ "$SERVICE_CIDR_ARG" = "10.78.0.0/24" ]
}

@test "valid_service_cidr: network address with a prefix of at most 30" {
  valid_service_cidr 10.201.0.0/16
  valid_service_cidr 10.201.0.0/30
  run valid_service_cidr 10.201.0.0/31
  [ "$status" -eq 1 ]
  run valid_service_cidr 10.201.0.1/16
  [ "$status" -eq 1 ]
  run valid_service_cidr 10.201.0.0
  [ "$status" -eq 1 ]
  run valid_service_cidr fd00::/64
  [ "$status" -eq 1 ]
}

@test "script: bad --service-cidr exits 2" {
  local bad
  for bad in 10.201.0.0/31 10.201.0.1/16 10.201.0.0 nonsense; do
    run bash "$SCRIPT" --dry-run --service-cidr "$bad"
    [ "$status" -eq 2 ]
    [[ "$output" == *"--service-cidr must be an IPv4 CIDR"* ]]
  done
}

@test "script: --join with a bootstrap setting exits 2, token not echoed" {
  local flag value
  for flag in "--mesh-cidr 10.50.0.0/16" "--container-cidr 10.210.0.0/16" "--service-cidr 10.77.0.0/16" "--name web-01"; do
    value="${flag#* }"
    flag="${flag%% *}"
    run bash "$SCRIPT" --dry-run --join "$TOKEN" "$flag" "$value"
    echo "case: $flag" >&3
    [ "$status" -eq 2 ]
    [[ "$output" == *"--join and $flag cannot be combined"* ]]
    [[ "$output" != *"$TOKEN"* ]]
    run bash "$SCRIPT" --check "$flag" "$value" --join "$TOKEN"
    [ "$status" -eq 2 ]
  done
}

@test "resolve_mode: the service CIDR is the flag, else the stored value, else the default" {
  resolve_mode
  [ "$SERVICE_CIDR" = 10.201.0.0/16 ]
  SERVICE_CIDR_ARG=10.77.0.0/16
  resolve_mode
  [ "$SERVICE_CIDR" = 10.77.0.0/16 ]
  SERVICE_CIDR_ARG=""
  mkdir -p "$FABER_ETC"
  printf 'FABER_NEW_MESH=1\nFABER_SERVICE_CIDR=10.78.0.0/16\n' > "$FABER_ETC/fabercp.env"
  resolve_mode
  [ "$SERVICE_CIDR" = 10.78.0.0/16 ]
}

@test "resolve_mode: a service CIDR overlapping the mesh CIDR dies" {
  SERVICE_CIDR_ARG=10.99.4.0/24
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"service CIDR 10.99.4.0/24 overlaps the mesh CIDR 10.99.0.0/16"* ]]
  # the default range counts too
  MESH_CIDR_ARG=10.201.0.0/24 SERVICE_CIDR_ARG=""
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"service CIDR 10.201.0.0/16 overlaps the mesh CIDR 10.201.0.0/24"* ]]
}

@test "resolve_mode: a service CIDR overlapping the container CIDR dies" {
  SERVICE_CIDR_ARG=10.200.8.0/22
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"service CIDR 10.200.8.0/22 overlaps the container CIDR 10.200.0.0/16"* ]]
  CONTAINER_CIDR_ARG=10.201.0.0/20 SERVICE_CIDR_ARG=""
  run resolve_mode
  [ "$status" -eq 1 ]
  [[ "$output" == *"overlaps the container CIDR 10.201.0.0/20"* ]]
  CONTAINER_CIDR_ARG=10.210.0.0/16 SERVICE_CIDR_ARG=10.211.0.0/16
  resolve_mode
  [ "$SERVICE_CIDR" = 10.211.0.0/16 ]
}

@test "real run (new mesh): FABER_SERVICE_CIDR is always written, then stripped once created" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run --service-cidr 10.77.0.0/16
  [ "$status" -eq 1 ]
  grep -qxF 'FABER_SERVICE_CIDR=10.77.0.0/16' "$FABER_ETC/fabercp.env"
  rm "$TMP/init-fails" "$TMP/executed"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
  run grep -q 'FABER_SERVICE_CIDR' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
  install_run --service-cidr 10.77.0.0/16
  [ "$status" -eq 1 ]
  [[ "$output" == *"--service-cidr only applies to a new mesh"* ]]
}

@test "real run: unfinished new mesh refuses another --service-cidr" {
  setup_full_run systemd
  touch "$TMP/init-fails"
  install_run --service-cidr 10.77.0.0/16
  [ "$status" -eq 1 ]
  install_run --service-cidr 10.78.0.0/16
  [ "$status" -eq 1 ]
  [[ "$output" == *"--service-cidr 10.77.0.0/16"* ]]
  rm "$TMP/init-fails"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"cluster: created"* ]]
}

# --- faber-node -----------------------------------------------------------

@test "binary_spec: faber-node is root:root 0755 outside FABER_HOME" {
  run binary_spec faber-node
  [ "$output" = "$FABER_NODE_INSTALL_DIR/faber-node root root 0755" ]
  [[ "$output" != "$FABER_HOME"* ]]
}

@test "verify_binaries: a missing faber-node is refused" {
  rm "$FABER_BIN_SRC_DIR/faber-node"
  run verify_binaries
  [ "$status" -eq 1 ]
  [[ "$output" == *"missing faber binary"*"faber-node"* ]]
}

@test "render_node_env: uid range, faber ids and tool dirs" {
  FABER_ACCOUNT_UID=990 FABER_ACCOUNT_GID=989 run render_node_env
  [ "$status" -eq 0 ]
  [ "$output" = "$(printf '%s\n' \
    '# managed by faber' \
    'FABER_NODE_UID_MIN=1048576' \
    'FABER_NODE_UID_MAX=2147483648' \
    'FABER_NODE_FABER_UID=990' \
    'FABER_NODE_FABER_GID=989' \
    'FABER_NODE_BIN_DIR=/usr/bin' \
    'FABER_NODE_SBIN_DIR=/usr/sbin' \
    "FABER_NODE_CONTAINERD_CONF_DIR=$CONTAINERD_CONF_DIR" \
    "FABER_NODE_CNI_CONF_DIR=$CNI_CONF_DIR" \
    "FABER_NODE_CNI_BIN_DIR=$CNI_BIN_DIR" \
    'FABER_NODE_SANDBOX_IMAGE=registry.k8s.io/pause:3.10' \
    'FABER_NODE_MESH_INTERFACE=faber0')" ]
}

@test "render_node_env: the mesh interface is the one fabercp is given" {
  FABER_ACCOUNT_UID=990 FABER_ACCOUNT_GID=989 run render_node_env
  grep -qxF "FABER_NODE_MESH_INTERFACE=$WG_INTERFACE" <<<"$output"
  MODE=existing run render_env 0
  grep -qxF "FABER_WG_INTERFACE=$WG_INTERFACE" <<<"$output"
}

@test "render_node_env: the uid range is the one placement allocates from" {
  local place="$REPO_ROOT/src/fabercp/src/raft/store/place.rs"
  grep -qE "UID_RANGE_START: u32 = 1_048_576;" "$place"
  grep -qE "UID_RANGE_END: u32 = 2_147_483_648;" "$place"
  [ "$NODE_UID_MIN" = 1048576 ]
  [ "$NODE_UID_MAX" = 2147483648 ]
}

@test "render_node_env: a custom socket and tool dirs are written, the default socket is not" {
  FABER_ACCOUNT_UID=990 FABER_ACCOUNT_GID=989
  FABER_NODE_SOCKET=/srv/faber/node.sock FABER_NODE_BIN_DIR=/bin FABER_NODE_SBIN_DIR=/sbin \
    run render_node_env
  grep -qxF 'FABER_NODE_SOCKET=/srv/faber/node.sock' <<<"$output"
  grep -qxF 'FABER_NODE_BIN_DIR=/bin' <<<"$output"
  grep -qxF 'FABER_NODE_SBIN_DIR=/sbin' <<<"$output"
  run render_node_env
  [[ "$output" != *"FABER_NODE_SOCKET"* ]]
}

@test "resolve_faber_ids: dry-run before the faber user exists shows placeholders, a real run dies" {
  stub_faber_user absent
  DRY_RUN=1 PATH="$TMP/bin:$PATH" resolve_faber_ids
  [ "$FABER_ACCOUNT_UID" = "<uid of faber>" ]
  DRY_RUN=0 PATH="$TMP/bin:$PATH" run resolve_faber_ids
  [ "$status" -eq 1 ]
  [[ "$output" == *"cannot read the numeric uid of faber"* ]]
}

@test "render_faber_node_unit: root, after containerd, strict sandbox, explicit paths" {
  run render_faber_node_unit
  [ "$status" -eq 0 ]
  local line
  for line in '[Unit]' 'After=containerd.service' 'Wants=containerd.service' \
    '[Service]' 'User=root' \
    "ExecStart=$FABER_NODE_INSTALL_DIR/faber-node --env-file $FABER_ETC/faber-node.env" \
    'Restart=always' 'StateDirectory=faber-node' 'StateDirectoryMode=0755' \
    'RuntimeDirectory=faber' 'RuntimeDirectoryMode=0755' \
    'NoNewPrivileges=yes' \
    '[Install]' 'WantedBy=multi-user.target'; do
    grep -qxF -- "$line" <<<"$output"
  done
  [[ "$output" != *"User=faber"* ]]
  # mount namespacing would hide the volume mounts from containerd
  local directive
  for directive in ProtectSystem= ReadWritePaths= ReadOnlyPaths= InaccessiblePaths= \
    MountFlags= ProtectHome= PrivateTmp= PrivateMounts= ProtectKernel ProtectControlGroups= \
    PrivateDevices= BindPaths= BindReadOnlyPaths= TemporaryFileSystem= ProtectProc= ; do
    if grep -q "^$directive" <<<"$output"; then
      echo "unexpected $directive" >&2
      return 1
    fi
  done
}

@test "render_faber_node_unit: a custom socket has no RuntimeDirectory" {
  FABER_NODE_SOCKET=/srv/faber/node.sock run render_faber_node_unit
  [[ "$output" != *"RuntimeDirectory"* ]]
  run grep -q "^ReadWritePaths" <<<"$output"
  [ "$status" -eq 1 ]
}

@test "render_fabercp_unit: starts after faber-node and gains no capability" {
  run render_fabercp_unit
  grep -qxF 'After=network-online.target containerd.service faber-node.service' <<<"$output"
  grep -qxF 'Wants=network-online.target faber-node.service' <<<"$output"
  grep -qxF 'AmbientCapabilities=CAP_NET_ADMIN' <<<"$output"
  grep -qxF 'CapabilityBoundingSet=CAP_NET_ADMIN' <<<"$output"
}

@test "render_env: FABER_NODE_SOCKET only for a non-default socket" {
  MODE=existing WG_ENDPOINT=""
  run render_env 0
  [[ "$output" != *"FABER_NODE_SOCKET"* ]]
  FABER_NODE_SOCKET=/srv/faber/node.sock run render_env 0
  grep -qxF 'FABER_NODE_SOCKET=/srv/faber/node.sock' <<<"$output"
}

@test "real run: faber-node binary, env file and unit are installed with their modes" {
  setup_full_run systemd
  install_run
  [ "$status" -eq 0 ]
  [ "$(stat -c '%U:%G %a' "$FABER_NODE_INSTALL_DIR/faber-node")" = "root:root 755" ]
  [ "$(stat -c '%U:%G %a' "$FABER_ETC/faber-node.env")" = "root:root 640" ]
  [ "$(stat -c '%U:%G %a' "$SYSTEMD_UNIT_DIR/faber-node.service")" = "root:root 644" ]
  grep -qxF 'FABER_NODE_UID_MIN=1048576' "$FABER_ETC/faber-node.env"
  grep -qxF 'FABER_NODE_UID_MAX=2147483648' "$FABER_ETC/faber-node.env"
  grep -qxF 'FABER_NODE_FABER_UID=990' "$FABER_ETC/faber-node.env"
  grep -qxF 'FABER_NODE_FABER_GID=989' "$FABER_ETC/faber-node.env"
  grep -qxF 'FABER_NODE_BIN_DIR=/usr/bin' "$FABER_ETC/faber-node.env"
  grep -qxF 'FABER_NODE_SBIN_DIR=/usr/sbin' "$FABER_ETC/faber-node.env"
  [[ "$output" == *"faber-node "* ]]
  [ "$(count_calls 'systemctl enable --now containerd faber-containerd-reload.path faber-node fabercp')" = 1 ]
  run grep -q 'FABER_NODE_SOCKET' "$FABER_ETC/fabercp.env"
  [ "$status" -eq 1 ]
}


@test "real run: a changed faber-node binary or env restarts only faber-node, a re-run restarts nothing" {
  setup_full_run systemd
  install_run
  [ "$status" -eq 0 ]
  rm "$TMP/executed"
  install_run
  [ "$status" -eq 0 ]
  [ "$(count_calls try-restart)" = 0 ]
  [ "$(count_calls daemon-reload)" = 0 ]
  make_fake_binary "$FABER_BIN_SRC_DIR/faber-node" "faber-node 0.2.0"
  install_run
  [ "$status" -eq 0 ]
  [ "$(count_calls 'systemctl try-restart faber-node')" = 1 ]
  [ "$(count_calls 'systemctl try-restart fabercp')" = 0 ]
  rm "$TMP/executed"
  printf 'x\n' > "$FABER_ETC/faber-node.env"
  install_run
  [ "$(count_calls 'systemctl try-restart faber-node')" = 1 ]
}

@test "dry-run: faber-node is staged and its units are planned" {
  make_stub_path
  install_run --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"$FABER_NODE_INSTALL_DIR/faber-node"* ]]
  [[ "$output" == *"faber-node.env"* ]]
  [ ! -e "$FABER_NODE_INSTALL_DIR/faber-node" ]
}

# --- preflight: kernel, containerd, id ranges -----------------------------

@test "check_kernel_version: older than 6.3 is refused with a clear message" {
  local release
  for release in 5.15.0-91-generic 6.2.16 6.0 4.19.1; do
    FABER_KERNEL_RELEASE="$release" run check_kernel_version
    [ "$status" -eq 1 ]
    [[ "$output" == *"kernel $release is older than 6.3"* ]]
  done
}

@test "check_kernel_version: 6.3 and newer pass" {
  local release
  for release in 6.3.0 6.3 6.8.0-test 6.10.2 7.0.1 10.0; do
    FABER_KERNEL_RELEASE="$release" run check_kernel_version
    [ "$status" -eq 0 ]
    [ -z "$output" ]
  done
}

@test "check_kernel_version: an unreadable release warns but does not fail" {
  FABER_KERNEL_RELEASE="weird" run check_kernel_version
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARN"*"cannot read the kernel version"* ]]
}

@test "check_kernel_version: uses uname -r when FABER_KERNEL_RELEASE is empty" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\necho 5.10.0\n' > "$TMP/bin/uname"
  chmod +x "$TMP/bin/uname"
  FABER_KERNEL_RELEASE="" PATH="$TMP/bin:$PATH" run check_kernel_version
  [ "$status" -eq 1 ]
  [[ "$output" == *"kernel 5.10.0 is older than 6.3"* ]]
}

@test "dry-run: an old kernel dies before any package command" {
  make_stub_path
  FABER_KERNEL_RELEASE=5.15.0 install_run --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"older than 6.3"* ]]
  [[ "$output" != *"apt-get"* ]]
}

@test "real run: an old kernel changes nothing" {
  setup_full_run systemd
  FABER_KERNEL_RELEASE=6.1.0 install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"older than 6.3"* ]]
  [ ! -e "$TMP/executed" ]
  [ ! -e "$FABER_ETC" ]
}

@test "check_containerd_version: older than 2.0 is refused" {
  stub_pkg_versions 1.7.24
  PATH="$TMP/bin:$PATH" run check_containerd_version
  [ "$status" -eq 1 ]
  [[ "$output" == *"containerd 1.7.24 is older than 2.0"* ]]
  stub_pkg_versions 1.6.33
  PATH="$TMP/bin:$PATH" run check_containerd_version
  [ "$status" -eq 1 ]
}

@test "check_containerd_version: 2.0 and newer, or none yet, pass" {
  stub_pkg_versions 2.0.0
  PATH="$TMP/bin:$PATH" run check_containerd_version
  [ "$status" -eq 0 ]
  stub_pkg_versions 2.1.3
  PATH="$TMP/bin:$PATH" run check_containerd_version
  [ "$status" -eq 0 ]
  PATH="$TMP/empty" run check_containerd_version
  [ "$status" -eq 0 ]
}

@test "check_containerd_version: --dry-run only warns" {
  stub_pkg_versions 1.7.24
  DRY_RUN=1 PATH="$TMP/bin:$PATH" run check_containerd_version
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARN"*"containerd 1.7.24 is older than 2.0"* ]]
}

@test "real run: containerd older than 2.0 after the install stops before any faber write" {
  setup_full_run systemd
  stub_pkg_versions 1.7.24
  install_run
  [ "$status" -eq 1 ]
  [[ "$output" == *"containerd 1.7.24 is older than 2.0"* ]]
  [ ! -e "$FABER_ETC" ]
  [ "$(count_calls 'systemctl')" = 0 ]
}

@test "warn_uid_range_overlaps: subuid and subgid entries inside the pod range warn" {
  mkdir -p "$TMP/etc"
  printf 'alice:100000:65536\nbob:1048576:65536\n' > "$SUBUID_FILE"
  printf 'carol:1000000:65536\ndave:2147483648:65536\n' > "$SUBGID_FILE"
  run warn_uid_range_overlaps
  [ "$status" -eq 0 ]
  [[ "$output" == *"$SUBUID_FILE: 'bob' has ids 1048576-1114111"* ]]
  [[ "$output" == *"$SUBGID_FILE: 'carol' has ids 1000000-1065535"* ]]
  [[ "$output" != *"alice"* ]]
  [[ "$output" != *"dave"* ]]
}

@test "warn_uid_range_overlaps: nothing to warn about, missing files and junk lines" {
  mkdir -p "$TMP/etc"
  printf 'alice:100000:65536\n# comment\nbroken\nx:y:z\n' > "$SUBUID_FILE"
  run warn_uid_range_overlaps
  [ "$status" -eq 0 ]
  [ -z "$output" ]
  rm "$SUBUID_FILE"
  run warn_uid_range_overlaps
  [ -z "$output" ]
}

@test "warn_uid_range_overlaps: a range that ends just below, or starts at the end, does not warn" {
  mkdir -p "$TMP/etc"
  printf 'a:983040:65536\nb:2147483648:1\n' > "$SUBUID_FILE"
  run warn_uid_range_overlaps
  [ -z "$output" ]
  printf 'c:983041:65536\nd:2147483647:5\n' > "$SUBUID_FILE"
  run warn_uid_range_overlaps
  [[ "$output" == *"'c'"* ]]
  [[ "$output" == *"'d'"* ]]
}

@test "warn_uid_range_overlaps: systemd-nspawn on the host warns" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\nexit 0\n' > "$TMP/bin/systemd-nspawn"
  chmod +x "$TMP/bin/systemd-nspawn"
  PATH="$TMP/bin:$PATH" run warn_uid_range_overlaps
  [ "$status" -eq 0 ]
  [[ "$output" == *"systemd-nspawn"*"524288-1879048191"* ]]
}

@test "real run: an overlapping subuid entry warns and the install still succeeds" {
  setup_full_run systemd
  mkdir -p "$TMP/etc"
  printf 'bob:1048576:65536\n' > "$SUBUID_FILE"
  install_run
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARN"*"'bob' has ids 1048576-1114111"* ]]
  [[ "$output" == *"cluster: created"* ]]
}

# --- review fixes: hardening, paths, ids ----------------------------------

@test "real run: a custom socket reaches both env files, and the unit makes its directory at start" {
  setup_full_run systemd
  local dir="/srv/faber-bats-$$"
  FABER_NODE_SOCKET="$dir/node.sock" install_run
  rm -rf "$dir"
  [ "$status" -eq 0 ]
  grep -qxF "FABER_NODE_SOCKET=$dir/node.sock" "$FABER_ETC/fabercp.env"
  grep -qxF "FABER_NODE_SOCKET=$dir/node.sock" "$FABER_ETC/faber-node.env"
  grep -qxF "ExecStartPre=/usr/bin/install -d -o root -g root -m 0755 $dir" "$SYSTEMD_UNIT_DIR/faber-node.service"
}

@test "render_faber_node_unit: hardening without a mount namespace, caps with reasons" {
  run render_faber_node_unit
  local line
  for line in 'NoNewPrivileges=yes' 'LockPersonality=yes' 'RestrictRealtime=yes' \
    'SystemCallArchitectures=native' 'RestrictAddressFamilies=AF_UNIX AF_NETLINK' \
    'CapabilityBoundingSet=CAP_SYS_ADMIN CAP_CHOWN CAP_FOWNER CAP_DAC_OVERRIDE'; do
    grep -qxF -- "$line" <<<"$output"
  done
  run grep -E '^(CAP_MKNOD|CapabilityBoundingSet=.*MKNOD|Protect|Private|Inaccessible)' <<<"$output"
  [ "$status" -eq 1 ]
}

@test "render_faber_node_unit: a custom socket directory is created by ExecStartPre" {
  FABER_NODE_SOCKET=/srv/faber/node.sock run render_faber_node_unit
  grep -qxF 'ExecStartPre=/usr/bin/install -d -o root -g root -m 0755 /srv/faber' <<<"$output"
  run render_faber_node_unit
  [[ "$output" != *"ExecStartPre"* ]]
}

@test "check_cfg_paths: the conf dirs, the bin dir and their parents must be root's alone" {
  need_root_faber_account
  local name
  for name in CONTAINERD_CONF_DIR CNI_CONF_DIR CNI_BIN_DIR; do
    mkdir -p "$TMP/loose/$name/leaf"
    chmod 0777 "$TMP/loose/$name"
    printf -v "$name" '%s' "$TMP/loose/$name/leaf"
    run check_cfg_paths
    [ "$status" -eq 1 ]
    [[ "$output" == *"$TMP/loose/$name must be owned by root and not group/world-writable"* ]]
    [[ "$output" == *"faber-node refuses to start"* ]]
    chmod 0755 "$TMP/loose/$name"
    chown 1234 "$TMP/loose/$name"
    run check_cfg_paths
    [ "$status" -eq 1 ]
    chown 0 "$TMP/loose/$name"
    run check_cfg_paths
    [ "$status" -eq 0 ]
  done
}

@test "check_cfg_paths: a faber-owned conf dir from an older install passes, a writable bin dir does not" {
  need_root_faber_account
  mkdir -p "$CONTAINERD_CONF_DIR" "$CNI_CONF_DIR" "$CNI_BIN_DIR"
  chown faber:faber "$CONTAINERD_CONF_DIR" "$CNI_CONF_DIR"
  run check_cfg_paths
  [ "$status" -eq 0 ]
  chmod 0775 "$CNI_BIN_DIR"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"$CNI_BIN_DIR must be owned by root"* ]]
}

@test "check_cfg_paths: plugin files in the bin dir must be root's and not group/world-writable" {
  need_root_faber_account
  mkdir -p "$CNI_BIN_DIR"
  printf 'v1.9.1\n' > "$CNI_BIN_DIR/.version"
  printf '#!/bin/sh\n' > "$CNI_BIN_DIR/bridge"
  chmod 0755 "$CNI_BIN_DIR/bridge"
  run check_cfg_paths
  [ "$status" -eq 0 ]
  chmod 0775 "$CNI_BIN_DIR/bridge"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"$CNI_BIN_DIR/bridge must be owned by root and not group/world-writable"* ]]
  chmod 0755 "$CNI_BIN_DIR/bridge"
  chown faber "$CNI_BIN_DIR/bridge"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  chown root "$CNI_BIN_DIR/bridge"
  chmod 0666 "$CNI_BIN_DIR/.version"
  run check_cfg_paths
  [ "$status" -eq 1 ]
}

@test "check_cfg_paths: every entry of the bin dir must be root's and not group/world-writable, directories too" {
  need_root_faber_account
  mkdir -p "$CNI_BIN_DIR/sub"
  chmod 0755 "$CNI_BIN_DIR/sub"
  run check_cfg_paths
  [ "$status" -eq 0 ]
  chmod 0775 "$CNI_BIN_DIR/sub"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"$CNI_BIN_DIR/sub must be owned by root and not group/world-writable"* ]]
  chmod 0755 "$CNI_BIN_DIR/sub"
  chown faber "$CNI_BIN_DIR/sub"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"$CNI_BIN_DIR/sub must be owned by root"* ]]
}

@test "check_cfg_paths: a file in the containerd conf dir that root does not own is refused by name" {
  need_root_faber_account
  mkdir -p "$CONTAINERD_CONF_DIR"
  chown faber:faber "$CONTAINERD_CONF_DIR"
  touch "$CONTAINERD_CONF_DIR/faber.toml" "$CONTAINERD_CONF_DIR/ok.toml" "$CONTAINERD_CONF_DIR/planted.toml" \
    "$CONTAINERD_CONF_DIR/.hidden" "$CONTAINERD_CONF_DIR/link"
  chown faber:faber "$CONTAINERD_CONF_DIR/faber.toml" "$CONTAINERD_CONF_DIR/planted.toml" "$CONTAINERD_CONF_DIR/.hidden"
  chown -h faber:faber "$CONTAINERD_CONF_DIR/link"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"$CONTAINERD_CONF_DIR"* ]]
  [[ "$output" == *"planted.toml"* ]]
  [[ "$output" == *".hidden"* ]]
  [[ "$output" == *"link"* ]]
  [[ "$output" != *"ok.toml"* ]]
  [[ "$output" != *"faber.toml"* ]]
  [[ "$output" == *"remove"* ]]
  rm "$CONTAINERD_CONF_DIR/planted.toml" "$CONTAINERD_CONF_DIR/.hidden" "$CONTAINERD_CONF_DIR/link"
  run check_cfg_paths
  [ "$status" -eq 0 ]
}

@test "check_cfg_paths: a CNI config that root does not own is refused by name, other files are not" {
  need_root_faber_account
  mkdir -p "$CNI_CONF_DIR"
  touch "$CNI_CONF_DIR/10-faber.conflist" "$CNI_CONF_DIR/README" "$CNI_CONF_DIR/05-other.conf" \
    "$CNI_CONF_DIR/.hid.json" "$CNI_CONF_DIR/90-x.conflist"
  chown faber:faber "$CNI_CONF_DIR/10-faber.conflist" "$CNI_CONF_DIR/README" "$CNI_CONF_DIR/90-x.conflist"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"90-x.conflist"* ]]
  [[ "$output" != *"README"* ]]
  [[ "$output" != *"05-other.conf"* ]]
  [[ "$output" == *"remove"* ]]
  rm "$CNI_CONF_DIR/90-x.conflist"
  chown faber:faber "$CNI_CONF_DIR/.hid.json"
  run check_cfg_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *".hid.json"* ]]
  rm "$CNI_CONF_DIR/.hid.json"
  run check_cfg_paths
  [ "$status" -eq 0 ]
}

@test "take_over_fabercp_files: stops fabercp, then replaces the faber-owned files with nothing" {
  need_root_faber_account
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR" "$CONTAINERD_CONF_DIR" "$CNI_CONF_DIR"
  printf 'x\n' > "$CONTAINERD_CONF_DIR/faber.toml"
  printf 'x\n' > "$CNI_CONF_DIR/$CNI_CONFLIST"
  chown faber:faber "$CONTAINERD_CONF_DIR" "$CNI_CONF_DIR" "$CONTAINERD_CONF_DIR/faber.toml" "$CNI_CONF_DIR/$CNI_CONFLIST"
  PATH="$TMP/bin:$PATH" run take_over_fabercp_files
  [ "$status" -eq 0 ]
  [ "$(count_calls 'systemctl stop fabercp')" = 1 ]
  [ ! -e "$CONTAINERD_CONF_DIR/faber.toml" ]
  [ ! -e "$CNI_CONF_DIR/$CNI_CONFLIST" ]
  [ "$(stat -c '%U:%G %a' "$CONTAINERD_CONF_DIR")" = "root:root 755" ]
  [ "$(stat -c '%U:%G %a' "$CNI_CONF_DIR")" = "root:root 755" ]
}

@test "take_over_fabercp_files: a file planted after preflight is refused, nothing is removed" {
  need_root_faber_account
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR" "$CONTAINERD_CONF_DIR"
  printf 'x\n' > "$CONTAINERD_CONF_DIR/faber.toml"
  printf 'x\n' > "$CONTAINERD_CONF_DIR/evil.toml"
  chown faber:faber "$CONTAINERD_CONF_DIR" "$CONTAINERD_CONF_DIR/faber.toml" "$CONTAINERD_CONF_DIR/evil.toml"
  PATH="$TMP/bin:$PATH" run take_over_fabercp_files
  [ "$status" -eq 1 ]
  [[ "$output" == *"evil.toml"* ]]
  [ -e "$CONTAINERD_CONF_DIR/faber.toml" ]
}

@test "take_over_fabercp_files: root-owned files and missing directories change nothing" {
  need_root_faber_account
  make_stub_path
  mkdir -p "$SYSTEMD_RUN_DIR" "$CONTAINERD_CONF_DIR"
  printf 'x\n' > "$CONTAINERD_CONF_DIR/faber.toml"
  PATH="$TMP/bin:$PATH" run take_over_fabercp_files
  [ "$status" -eq 0 ]
  [ ! -e "$TMP/executed" ]
  [ -e "$CONTAINERD_CONF_DIR/faber.toml" ]
}

@test "take_over_fabercp_files: dry-run plans the stop and the removals" {
  need_root_faber_account
  mkdir -p "$SYSTEMD_RUN_DIR" "$CNI_CONF_DIR"
  printf 'x\n' > "$CNI_CONF_DIR/$CNI_CONFLIST"
  chown faber:faber "$CNI_CONF_DIR/$CNI_CONFLIST"
  DRY_RUN=1 run take_over_fabercp_files
  [ "$status" -eq 0 ]
  [[ "$output" == *"+ systemctl stop fabercp"* ]]
  [[ "$output" == *"+ rm -f -- $CNI_CONF_DIR/$CNI_CONFLIST"* ]]
  [ -e "$CNI_CONF_DIR/$CNI_CONFLIST" ]
}

@test "check_cfg_paths: missing directories pass" {
  run check_cfg_paths
  [ "$status" -eq 0 ]
}

@test "check_node_paths: plain absolute paths only" {
  local bad
  for bad in 'rel/dir' '/opt/a b' '/opt/a;b' '/opt/../etc' '/opt//x' '/opt/$x' ''; do
    FABER_NODE_INSTALL_DIR="$bad" run check_node_paths
    [ "$status" -eq 1 ]
    [[ "$output" == *"FABER_NODE_INSTALL_DIR must be an absolute path"* ]]
  done
  FABER_NODE_SOCKET='/srv/a b/node.sock' run check_node_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"FABER_NODE_SOCKET must be an absolute path"* ]]
  run check_node_paths
  [ "$status" -eq 0 ]
}

@test "check_node_paths: an install dir under a directory others can write is refused" {
  need_root_faber_account
  mkdir -p "$TMP/loose/sbin"
  chmod 0777 "$TMP/loose"
  FABER_NODE_INSTALL_DIR="$TMP/loose/sbin" run check_node_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"$TMP/loose must be owned by root and not group/world-writable"* ]]
  chmod 0755 "$TMP/loose"
  chown 1234 "$TMP/loose"
  FABER_NODE_INSTALL_DIR="$TMP/loose/sbin" run check_node_paths
  [ "$status" -eq 1 ]
  chown 0 "$TMP/loose"
  FABER_NODE_INSTALL_DIR="$TMP/loose/sbin" run check_node_paths
  [ "$status" -eq 0 ]
}

@test "check_node_paths: a custom socket needs a directory of its own, not /tmp" {
  local sock
  for sock in /run/node.sock /var/run/node.sock /tmp/node.sock /node.sock; do
    FABER_NODE_SOCKET="$sock" run check_node_paths
    [ "$status" -eq 1 ]
    [[ "$output" == *"shared directory"* ]]
  done
  for sock in /tmp/faber/node.sock /var/tmp/faber/node.sock; do
    FABER_NODE_SOCKET="$sock" run check_node_paths
    [ "$status" -eq 1 ]
    [[ "$output" == *"must not be under /tmp"* ]]
  done
  FABER_NODE_SOCKET=/run/faber2/node.sock run check_node_paths
  [ "$status" -eq 0 ]
}

@test "check_node_paths: an existing socket directory must be root's alone" {
  need_root_faber_account
  local dir="/srv/faber-bats-dir-$$"
  mkdir -p "$dir"
  chown 1234 "$dir"
  FABER_NODE_SOCKET="$dir/node.sock" run check_node_paths
  [ "$status" -eq 1 ]
  [[ "$output" == *"$dir must be owned by root"* ]]
  chown 0 "$dir"
  chmod 0777 "$dir"
  FABER_NODE_SOCKET="$dir/node.sock" run check_node_paths
  [ "$status" -eq 1 ]
  chmod 0755 "$dir"
  FABER_NODE_SOCKET="$dir/node.sock" run check_node_paths
  rm -rf "$dir"
  [ "$status" -eq 0 ]
}

@test "dry-run: a bad FABER_NODE_SOCKET dies before any package command" {
  make_stub_path
  FABER_NODE_SOCKET=/tmp/faber/node.sock install_run --dry-run
  [ "$status" -eq 1 ]
  [[ "$output" == *"FABER_NODE_SOCKET must not be under /tmp"* ]]
  [[ "$output" != *"apt-get"* ]]
}

@test "check_containerd_version: a containerd whose version cannot be read dies on a real run" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\necho "containerd garbage"\n' > "$TMP/bin/containerd"
  chmod +x "$TMP/bin/containerd"
  PATH="$TMP/bin:$PATH" run check_containerd_version
  [ "$status" -eq 1 ]
  [[ "$output" == *"cannot read the version of the installed containerd"* ]]
  DRY_RUN=1 PATH="$TMP/bin:$PATH" run check_containerd_version
  [ "$status" -eq 0 ]
}

@test "warn_uid_range_overlaps: an entry with a count of 0 is empty and does not warn" {
  mkdir -p "$TMP/etc"
  printf 'zed:1048576:0\nyan:1500000:0\n' > "$SUBUID_FILE"
  run warn_uid_range_overlaps
  [ -z "$output" ]
}

@test "warn_uid_range_overlaps: the nspawn warning names --private-users=pick" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\nexit 0\n' > "$TMP/bin/systemd-nspawn"
  chmod +x "$TMP/bin/systemd-nspawn"
  PATH="$TMP/bin:$PATH" run warn_uid_range_overlaps
  [[ "$output" == *"--private-users=pick"* ]]
}

@test "check_faber_account: a faber uid or gid inside the pod range is refused" {
  mkdir -p "$TMP/bin"
  printf '#!/bin/sh\ncase "$1" in -gn) echo faber ;; -u) echo 1048576 ;; -g) echo 989 ;; esac\nexit 0\n' > "$TMP/bin/id"
  chmod +x "$TMP/bin/id"
  PATH="$TMP/bin:$PATH" run check_faber_account
  [ "$status" -eq 1 ]
  [[ "$output" == *"has id 1048576, inside the pod id range"* ]]
  printf '#!/bin/sh\ncase "$1" in -gn) echo faber ;; -u) echo 990 ;; -g) echo 2147483647 ;; esac\nexit 0\n' > "$TMP/bin/id"
  PATH="$TMP/bin:$PATH" run check_faber_account
  [ "$status" -eq 1 ]
  printf '#!/bin/sh\ncase "$1" in -gn) echo faber ;; -u) echo 990 ;; -g) echo 2147483648 ;; esac\nexit 0\n' > "$TMP/bin/id"
  PATH="$TMP/bin:$PATH" run check_faber_account
  [ "$status" -eq 0 ]
}
