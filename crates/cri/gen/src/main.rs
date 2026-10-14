//! Compiles `../proto/api.proto` into `../src/generated/`.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("no parent directory")?;
    // SAFETY: nothing else is running yet, so no concurrent environment access.
    unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    tonic_prost_build::configure()
        .build_server(false)
        .out_dir(root.join("src/generated"))
        .compile_protos(&[root.join("proto/api.proto")], &[root.join("proto")])?;
    Ok(())
}
