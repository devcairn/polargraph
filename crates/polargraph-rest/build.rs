fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Reuse the same proto file as polargraph-server — no duplication.
    tonic_build::configure()
        .build_server(false)
        .compile(
            &["../polargraph-server/proto/polargraph.proto"],
            &["../polargraph-server/proto"],
        )?;
    Ok(())
}
