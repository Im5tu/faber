# faber-cri

CRI v1 client trait (`CriClient`), a gRPC client over containerd's Unix socket
(`GrpcCri`, unix only) and an in-memory `FakeCri` for tests.

## Vendored API

`proto/api.proto` is `kubernetes/cri-api` **v0.34.0**
(<https://raw.githubusercontent.com/kubernetes/cri-api/v0.34.0/pkg/apis/runtime/v1/api.proto>).
The generated Rust lives in `src/generated/runtime.v1.rs` and is checked in, so
normal builds need no `protoc`.

To regenerate after bumping the proto, run `crates/cri/regen.sh`. It builds
`gen/`, a helper crate outside the workspace that vendors `protoc`.

## Provenance

SHA-256 (LF line endings):

- Upstream file as downloaded from the URL above, without our 4-line header
  comment: `a1ac3e1b238569cee4f5e65cbd826c6fe316fe4721d1d45a8b6c1fbe435de7aa`
- `proto/api.proto` as checked in, including the header:
  `92bcd8a37928217372dc5bb48e7e67992ecc1497ec3a97d89bbf6d2d86611263`

Check upstream with `tail -n +5 proto/api.proto | sha256sum`.
