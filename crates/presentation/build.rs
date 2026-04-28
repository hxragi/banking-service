use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = "../../proto/bank.proto";

    if Path::new(proto_path).exists() {
        tonic_prost_build::compile_protos(proto_path)?;
        println!("cargo:rerun-if-changed={}", proto_path);
    } else {
        println!("cargo:warning=proto/bank.proto not found. skipping grpc codegen")
    }

    Ok(())
}
