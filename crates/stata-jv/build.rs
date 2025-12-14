use std::env;
use std::path::PathBuf;
use std::process::Command;
use walkdir::WalkDir;

fn main() {
    // The java directory is at the project root, two levels up from this crate
    let project_root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    
    let java_dir = project_root.join("java");
    
    println!("cargo:rerun-if-changed={}", java_dir.join("pom.xml").display());
    let java_src_dir = java_dir.join("src");
    if java_src_dir.exists() {
        for entry in walkdir::WalkDir::new(&java_src_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "java"))
        {
            println!("cargo:rerun-if-changed={}", entry.path().display());
        }
    }

    // Get the Stata SFI JAR path from environment variable
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace_root = manifest_dir.parent().unwrap().parent().unwrap();
    let env_path = workspace_root.join(".env");
    println!("cargo:warning=Looking for .env at: {:?}", env_path);
    // Load .env from workspace root
    if env_path.exists() {
        println!("cargo:warning=.env file exists, attempting to load...");
        match dotenvy::from_path(&env_path) {
            Ok(_) => println!("cargo:warning=.env loaded successfully"),
            Err(e) => println!("cargo:warning=.env load FAILED: {:?}", e),
        }
    } else {
        println!("cargo:warning=.env NOT FOUND at {}", env_path.display());
    }
    let sfi_jar = env::var("stata_parquet_io_stata_java_class_path").expect("stata_parquet_io_stata_java_class_path must be set");
    
    // Copy the SFI JAR to java/libs/ if it doesn't exist
    let libs_dir = java_dir.join("libs");
    std::fs::create_dir_all(&libs_dir).expect("Failed to create java/libs directory");
    
    let target_jar = libs_dir.join("sfi-api.jar");
    if !target_jar.exists() {
        std::fs::copy(&sfi_jar, &target_jar)
            .expect(&format!("Failed to copy {} to java/libs/", sfi_jar));
        println!("Copied SFI JAR to java/libs/");
    }
    
    // Run Maven build
    let mvn_command = if cfg!(target_os = "windows") {
        "mvn.cmd"
    } else {
        "mvn"
    };
    
    // Run Maven with output visible
    let output = Command::new(mvn_command)
        .current_dir(&java_dir)
        .args(&["clean", "package", "-X"])  // -X for debug output
        .output()  // Use output() instead of status() to capture stdout/stderr
        .expect("Failed to run Maven build");
    
    // Print Maven output
    println!("cargo:warning=Maven stdout: {}", String::from_utf8_lossy(&output.stdout));
    println!("cargo:warning=Maven stderr: {}", String::from_utf8_lossy(&output.stderr));
    
    if !output.status.success() {
        panic!("Maven build failed with status: {}", output.status);
    }
    
    println!("cargo:warning=Maven build completed successfully");
    
    // Check JAR timestamp
    let jar_path = java_dir.join("target").join("stata-parquet-io.jar");
    println!("cargo:warning=looking for jar at {}", jar_path.display());
    
    if !jar_path.exists() {
        panic!("Expected JAR not found at {:?}", jar_path);
    }
    
    // Print JAR metadata
    if let Ok(metadata) = std::fs::metadata(&jar_path) {
        if let Ok(modified) = metadata.modified() {
            println!("cargo:warning=JAR last modified: {:?}", modified);
        }
    }
    
    println!("cargo:rustc-env=JAVA_JAR_PATH={}", jar_path.display());
}