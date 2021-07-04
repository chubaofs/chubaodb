fn main() {
    tonic_build::configure()
        .out_dir("src")
        .type_attribute(
            ".",
            "#[derive(serde_derive::Serialize,serde_derive::Deserialize)]",
        )
        .compile(
            &[
                "proto/raft.proto",
                "proto/meta_raft.proto",
                "proto/pserver.proto",
            ],
            &["proto"],
        )
        .unwrap();
    println!("cargo:rerun-if-changed=proto");
}
