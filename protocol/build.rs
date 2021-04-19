use std::env;
use std::path::PathBuf;
use std::path::Path;

fn main() {
    let proto = "pserverpb.proto";
    let proto_path: &Path = proto.as_ref();
    let proto_dir = proto_path
        .parent()
        .expect("proto file should reside in a directory");

    tonic_build::configure()
        .out_dir("proto/src")
        .type_attribute(".", "#[derive(serde_derive::Serialize)]")
        .compile(&["proto/pserverpb.proto"], &["proto"])
        .unwrap();
}
