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
