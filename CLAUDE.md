# faber

Rust workspace for a Raft cluster over a WireGuard mesh that stores container workloads and runs them as containers through containerd, with a virtual IP and a `.faber.local` DNS name for each service. `docs/contract.md` pins the names and formats that cross process, file or script boundaries.

## Crates

- `src/fabercp`: control plane. Raft (openraft, RocksDB), REST API, MCP endpoint, reconcile loop, placement, status, DNS and the runner.
- `src/faber-node`: root helper on each node. CRI calls, volumes, mounts and the containerd and CNI config, from typed requests over a Unix socket. Linux only.
- `src/faber`: CLI client for the control plane.
- `crates/api-types`: DTOs, workload model, error codes. HTTP client behind the `client` feature.
- `crates/node-protocol`: the typed frames between fabercp and faber-node.
- `crates/cri`: CRI v1 client trait, gRPC client over containerd's socket (unix only) and an in-memory fake. Generated code is checked in; `crates/cri/regen.sh` regenerates it.
- `crates/image`: image pulls through the CRI client, anonymous only.
- `crates/crypto`: envelope encryption (cluster key, data keys, sealed env, env digests).
- `crates/network-management`: WireGuard and nftables via `ip`, `wg`, `nft`.
- `crates/container-runtime`: containerd drop-in and CNI config writers, used by faber-node.
- `src/install.sh`: node installer.

## Toolchain

Stable Rust with rustfmt and clippy. Edition 2024, `rust-version` 1.89, resolver 3. The repo has no rustfmt, clippy or `.cargo` config and no CI workflow, so defaults apply.

## Verification

User-level agents (rust, verifier, reviewer, security-reviewer, explorer) read this file for commands.

- Author loop (plain `cargo test`, no nextest): `cargo test -p <crate> <filter>`, or `--test <file>` for one integration file. `-p faber-api-types` skips `tests/client.rs` unless you add `--features client`.
- Verifier: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo build --workspace`, `cargo test --workspace`. The `-D warnings` flag is a choice, not set by the repo.
- Slow: the first build compiles RocksDB (`bindgen-runtime`, needs libclang).
- faber-node and the other Linux-only code (sockets, mounts, CRI, DNS bind) need Linux to run their tests. They sit behind traits with in-memory fakes and are gated with `cfg(unix)` or `cfg(target_os = "linux")`, so `cargo build --workspace` also passes elsewhere, but those tests do not run there. On a host that cannot build or test them, run cargo in a Linux container: a Rust image with clang, libclang, cmake, rustfmt and clippy, the repo mounted, and `CARGO_TARGET_DIR` on a volume, for example `docker run --rm -v "$PWD:/w" -v faber-target:/target -w /w <image> cargo test -p faber-node`. Code must build on the host toolchain too. Where the host cannot run build scripts, `cargo check --target x86_64-unknown-linux-gnu` checks the Linux-only crates.
- Docker only: `docker run --rm -v "$PWD:/w" -w /w bats/bats:latest tests/install`.
- Opt-in: `tests/e2e/run.sh` needs Docker with WireGuard in the kernel and network access. It builds the binaries in a container and runs numbered phases on systemd nodes; the header of the script lists them and `E2E_PHASES` picks some. Phase M deploys redis from Docker Hub with `faber deploy spec` (two replicas, a block volume, a ramdisk and a probe) and checks that it becomes available, that its `.faber.local` name resolves to the virtual IP from containers on both nodes, that it survives a kill with its data, and that a redeploy keeps the address. Phase M needs a kernel of 6.3 or later and containerd 2.x on the nodes. Phase K calls the MCP endpoint on the loopback listener of node A with curl: initialize, the tool list and one tool call.
- No Rust test needs an external service (loopback ports and temp dirs). No benches exist.
