use std::env;

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    
    if target_os == "windows" {
        // Use direct EXPORT flags without /DEF
        println!("cargo:rustc-cdylib-link-arg=/EXPORT:JNI_OnLoad");
        println!("cargo:rustc-cdylib-link-arg=/EXPORT:Java_com_parquet_io_ParquetIO_stataCall");
    }
}