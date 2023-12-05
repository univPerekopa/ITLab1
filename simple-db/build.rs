fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("/home/tr3tiakoff/University/ITLab1/proto/database.proto").unwrap();
    Ok(())
}