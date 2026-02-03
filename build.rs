fn main() -> Result<(), Box<dyn std::error::Error>> {
    // MAINTAINER NOTE:
    // This build script is disabled by default to allow users to build the crate
    // without needing `protoc` installed or the external `chapaty-bq-export-proto` repo.
    //
    // The generated Rust code is committed to `src/proto_gen`.
    //
    // To regenerate the protobuf bindings (after updating .proto files), run:
    //     CHAPATY_GEN_PROTOS=1 cargo build
    //
    // Then commit the changes in `src/proto_gen`.
    if std::env::var("CHAPATY_GEN_PROTOS").is_err() {
        return Ok(());
    }

    let proto_root_path = std::fs::canonicalize("../chapaty-bq-export-proto/proto")?;
    let proto_root = proto_root_path.to_str().ok_or("Invalid path")?.to_string();

    let proto_files = [
        format!("{proto_root}/chapaty/bq_exporter/v1/service.proto"),
        format!("{proto_root}/chapaty/data/v1/common.proto"),
        format!("{proto_root}/chapaty/data/v1/economic_calendar.proto"),
        format!("{proto_root}/chapaty/data/v1/ohlcv_future.proto"),
        format!("{proto_root}/chapaty/data/v1/ohlcv_spot.proto"),
        format!("{proto_root}/chapaty/data/v1/tpo_future.proto"),
        format!("{proto_root}/chapaty/data/v1/tpo_spot.proto"),
        format!("{proto_root}/chapaty/data/v1/trades_spot.proto"),
        format!("{proto_root}/chapaty/data/v1/volume_profile_spot.proto"),
    ];

    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/proto_gen")
        .compile_protos(&proto_files, &[proto_root])?;

    std::process::Command::new("cargo")
        .args(["fmt", "--", "src/proto_gen/*.rs"])
        .status()
        .ok();

    Ok(())
}
