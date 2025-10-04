fn main() {
    tonic_prost_build::configure()
        .build_server(true)
        .compile_protos(&["../protos/orderbook.proto"], &["../protos"])
        .expect("Failed to compile protos");
}
