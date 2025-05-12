use std::env;
use std::path::PathBuf;
use std::fs;

fn main() {
    // 1. Tell Cargo to rerun if any vendor files change
    println!("cargo:rerun-if-changed=vendor/stplugin.cpp");
    println!("cargo:rerun-if-changed=vendor/stplugin.h");
    
    // Get target OS
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    println!("Target OS: {}", target_os);
    
    // 2. Define system type for stplugin.h based on target OS
    let system_define = match target_os.as_str() {
        "windows" => "STWIN32",
        "macos" => "APPLEMAC",
        "linux" => "OPUNIX",
        _ => "OPUNIX", // Default to UNIX-like for other platforms
    };
    
    // 3. Create a wrapper header that defines the system
    let wrapper_content = format!(
        "#define SYSTEM {}\n#include \"vendor/stplugin.h\"\n",
        system_define
    );
    fs::write("wrapper_generated.h", wrapper_content)
        .expect("Failed to write wrapper header");
    
    // 4. Compile the C++ code with platform-specific settings
    let mut build = cc::Build::new();
    
    // Common settings
    build.cpp(true)
         .file("vendor/stplugin.cpp");
         
    // Platform-specific settings
   if target_os == "linux" {
        // Linux-specific settings
        build.define("SYSTEM", "OPUNIX")
             .flag("-std=c++11")       // Use C++11 standard
             .flag("-DSPI=3.0");       // Define SPI version
        
        // Link with dynamic loading library needed by stplugin.h
        println!("cargo:rustc-link-lib=dylib=dl");
    } else if target_os == "macos" {
        // macOS-specific settings
        build.define("SYSTEM", "APPLEMAC")
             .flag("-std=c++11")       // Use C++11 standard
             .flag("-DSPI=3.0");       // Define SPI version
        
        // macOS may need specific framework linkage
        println!("cargo:rustc-link-arg=-framework");
        println!("cargo:rustc-link-arg=CoreFoundation");
    } else {
        // Windows or other platforms
        build.define("SYSTEM", system_define);
    }
    
    // Compile
    build.compile("stata_plugin");
    
    // 5. Where is the crate root?
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let vendor_dir = manifest_dir.join("vendor");

    // 6. Generate the bindings
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("OUT_DIR = {:?}", env::var("OUT_DIR"));
    
    // Initialize the bindings builder differently based on platform
    let bindings_result = if target_os == "linux" {
        // Linux-specific approach with fixes for clang issues
        println!("cargo:warning=Using Linux-specific bindgen configuration");
        
        // Create a more robust wrapper header with full paths for Linux
        let full_path_wrapper = format!(
            "#define SYSTEM OPUNIX\n#include \"{}/vendor/stplugin.h\"\n",
            manifest_dir.display()
        );
        let wrapper_path = out_dir.join("wrapper_linux.h");
        fs::write(&wrapper_path, full_path_wrapper)
            .expect("Failed to write Linux wrapper header");
        
        // Print debug info
        println!("cargo:warning=Using wrapper path: {}", wrapper_path.display());
        println!("cargo:warning=Include path: {}", manifest_dir.display());
        
        // Linux-specific bindgen configuration
        bindgen::Builder::default()
            .header(wrapper_path.to_str().unwrap())
            .clang_arg("-x")
            .clang_arg("c++")
            .clang_arg("-std=c++11")
            .clang_arg(format!("-I{}", manifest_dir.display()))
            .clang_arg("-DSPI=3.0")
            .detect_include_paths(true)
            .layout_tests(false)
            .parse_callbacks(Box::new(bindgen::CargoCallbacks))
            .generate()
    } else {
        // Original approach for non-Linux platforms
        bindgen::Builder::default()
            .header("wrapper_generated.h")
            .clang_arg(format!("-I{}", vendor_dir.display()))
            .clang_arg(format!("-DSYSTEM={}", system_define))
            .parse_callbacks(Box::new(bindgen::CargoCallbacks))
            .generate()
    };
}
