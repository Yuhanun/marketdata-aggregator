fn main() {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["../protos/orderbook.proto"], &["../protos"])
        .expect("Failed to compile protos");
}
