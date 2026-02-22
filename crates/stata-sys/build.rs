use std::env;
use std::path::PathBuf;

fn main() {
    // Detect the target OS
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    println!("cargo:warning=Building for OS: {}", target_os);
    
    // Set up C/C++ compilation
    let mut build = cc::Build::new();
    
    // Configure build based on target OS
    match target_os.as_str() {
        "windows" => {
            build.define("SYSTEM", "STWIN32")
                 .flag("-shared")
                 .flag("-fPIC");
        },
        "macos" => {
            build.define("SYSTEM", "APPLEMAC")
                 .flag("-bundle");
        },
        _ => { // Assume Linux/Unix
            build.define("SYSTEM", "OPUNIX")
                 .flag("-shared")
                 .flag("-fPIC")
                 .define("SYSTEM", "OPUNIX");
        },
    }
    
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=vendor/stplugin.h");
    //    println!("cargo:rerun-if-changed=vendor/stplugin.cpp");
    
    // Compile the minimal C/C++ code needed - this is just the support code
    //    build
    //        .cpp(true) // Compile as C++
    //        .file("vendor/stplugin.cpp")
    //        .compile("stplugin_support");
    
    // Generate bindings using bindgen - with more comprehensive allowlists
    let bindings = bindgen::Builder::default()
        .header("vendor/stplugin.h")
        // More comprehensive allowlists to expose the full Stata API
        .allowlist_function("pginit")
        .allowlist_type("ST_.*")          // Match all Stata types (ST_plugin, ST_retcode, etc.)
        .allowlist_var("_stata_")
        .allowlist_var("SD_.*")           // Match all Stata constants
        .allowlist_var("SF_.*")           // Match all Stata macros
        .allowlist_var("SW_.*")           // Additional Stata variables
        .allowlist_var("SV_.*")           // Additional Stata variables
        // Tell bindgen about platform-specific defines
        .clang_arg(match target_os.as_str() {
            "windows" => "-DSYSTEM=STWIN32",
            "macos" => "-DSYSTEM=APPLEMAC",
            _ => "-DSYSTEM=OPUNIX"
        })
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");
    
    // Write the bindings to the $OUT_DIR/bindings.rs file
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let binding_path = out_path.join("bindings.rs");
    println!("cargo:warning=Writing bindings to: {}", binding_path.display());
    
    bindings
        .write_to_file(binding_path)
        .expect("Couldn't write bindings!");
    
    // Don't set output library name here, as stata-sys is just a binding crate
    // The parent crate (stata_parquet_io) should handle naming the plugin file
}
