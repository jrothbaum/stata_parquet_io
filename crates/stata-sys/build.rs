use std::env;
use std::path::PathBuf;

fn main() {
    // 1. Tell Cargo to rerun if any vendor files change
    println!("cargo:rerun-if-changed=vendor/stplugin.cpp");
    println!("cargo:rerun-if-changed=vendor/stplugin.h");

    // 2. Compile the C++ code
    cc::Build::new()
        .cpp(true)
        .file("vendor/stplugin.cpp")
        .compile("stata_plugin");

    // 3. Where is the crate root?
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let vendor_dir = manifest_dir.join("vendor");

    // 4. Generate the bindings
    let bindings = bindgen::Builder::default()
        .header("wrapper_generated.h")  // crate-relative, not absolute
        .clang_arg(format!("-I{}", vendor_dir.display()))  // tell Clang where "vendor/" is
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate bindings");

    // 5. Write the bindings to OUT_DIR
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
