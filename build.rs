use std::env;

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    
    if target_os == "windows" {
        // These flags help with exporting symbols on Windows/MinGW
        println!("cargo:rustc-link-arg=-Wl,--export-all-symbols");
        println!("cargo:rustc-link-arg=-Wl,--enable-auto-import");
        println!("cargo:rustc-link-arg=-Wl,--allow-multiple-definition");
        
        // This makes sure DLL has the .plugin extension
        //  println!("cargo:rustc-cdylib-link-arg=-o=pq_win.plugin");
    } else if target_os == "macos" {
        //  println!("cargo:rustc-cdylib-link-arg=-o=pq_macos.plugin");
    } else {
        //  println!("cargo:rustc-cdylib-link-arg=-o=pq_unix.plugin");
    }
}