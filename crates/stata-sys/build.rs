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
    let mut bindgen_builder = bindgen::Builder::default()
        .header("wrapper_generated.h") // crate-relative, not absolute
        .clang_arg(format!("-I{}", vendor_dir.display())) // tell Clang where "vendor/" is
        .clang_arg(format!("-DSYSTEM={}", system_define)); // Define SYSTEM for clang too
    
    // Add platform-specific clang args
    if target_os == "linux" {
        // Add C++11 standard flag for clang/bindgen
        bindgen_builder = bindgen_builder
            .clang_arg("-std=c++11")
            .clang_arg("-DSPI=3.0");  // Add SPI version if needed
            
        // If you have system includes that might be needed
        if let Ok(gcc_include) = std::process::Command::new("g++")
            .args(&["-E", "-xc++", "-v", "/dev/null"])
            .output() 
        {
            // This will capture standard include directories from g++
            println!("cargo:warning=Checking g++ include paths");
        }
    }
    
    let bindings = bindgen_builder
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate bindings");
    
    // 7. Write the bindings to OUT_DIR
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
