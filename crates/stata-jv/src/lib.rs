use jni::{JavaVM, JNIEnv, objects::{JClass, JObjectArray, JValue, JObject, GlobalRef}};
use jni::sys::jlong;
use std::sync::Mutex;
use std::os::raw::{c_int, c_char};
use std::cell::Cell;
use std::collections::HashMap;


use polars::datatypes::CompatLevel;
use polars::frame::DataFrame;
use polars_arrow::ffi::{export_array_to_c, export_field_to_c, ArrowArray, ArrowSchema};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::Field;


// Global storage for the JVM
static GLOBAL_JVM: Mutex<Option<JavaVM>> = Mutex::new(None);

// Global reference to the ParquetIO class
static PARQUET_CLASS_REF: Mutex<Option<GlobalRef>> = Mutex::new(None);

// Thread-local storage for the current JNI environment
thread_local! {
    static CURRENT_ENV: Cell<Option<*mut jni::sys::JNIEnv>> = Cell::new(None);
}

// Helper to set the current environment
pub fn set_current_env(env: *mut jni::sys::JNIEnv) {
    CURRENT_ENV.with(|e| e.set(Some(env)));
}

// Helper to use the current environment
pub fn with_current_env<F, R>(f: F) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce(&mut JNIEnv) -> Result<R, Box<dyn std::error::Error>>
{
    let env_ptr = CURRENT_ENV.with(|e| e.get())
        .ok_or("No JNI environment available")?;
    
    let mut env = unsafe { JNIEnv::from_raw(env_ptr)? };
    f(&mut env)
}


// Callback type that matches your stata_call signature with C ABI
pub type StataCallFn = extern "C" fn(argc: c_int, argv: *const *const c_char) -> c_int;

// Store the callback function
static STATA_CALL_CALLBACK: Mutex<Option<StataCallFn>> = Mutex::new(None);

static JAR_PATH: Mutex<Option<String>> = Mutex::new(None);


/// Register the callback function from the main crate
pub fn register_stata_call(callback: StataCallFn) {
    *STATA_CALL_CALLBACK.lock().unwrap() = Some(callback);
}

/// Execute a function with access to the global JVM
pub fn with_jvm<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&JavaVM) -> R,
{
    let guard = GLOBAL_JVM.lock().unwrap();
    guard.as_ref().map(|jvm| f(jvm))
}

/// JNI entry point called from Java
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_io_ParquetIO_stataCall( 
    mut env: JNIEnv,
    class: JClass,
    args: JObjectArray,
) -> jni::sys::jint {
    // Store the raw JNIEnv pointer for reuse
    set_current_env(env.get_raw());

    // Store JVM and class reference on first call
    let mut jvm_guard = GLOBAL_JVM.lock().unwrap();
    if jvm_guard.is_none() {
        match env.get_java_vm() {
            Ok(jvm) => {
                *jvm_guard = Some(jvm);
            }
            Err(e) => {
                eprintln!("Failed to get JavaVM: {:?}", e);
                return 198;
            }
        }
        drop(jvm_guard); // Release the lock
        
        // Store global reference to the class (borrow it, don't move it)
        match env.new_global_ref(&class) {  // <-- Add & here
            Ok(global_ref) => {
                *PARQUET_CLASS_REF.lock().unwrap() = Some(global_ref);
            }
            Err(e) => {
                eprintln!("Failed to create global ref: {:?}", e);
                return 198;
            }
        }
    
        // Store JAR path on first call
        if JAR_PATH.lock().unwrap().is_none() {
            // Get the ProtectionDomain
            let protection_domain = env.call_method(
                &class,  // <-- Add & here too
                "getProtectionDomain",
                "()Ljava/security/ProtectionDomain;",
                &[]
            ).ok().and_then(|r| r.l().ok());
            
            if let Some(pd) = protection_domain {
                // Get CodeSource
                let code_source = env.call_method(
                    pd,
                    "getCodeSource",
                    "()Ljava/security/CodeSource;",
                    &[]
                ).ok().and_then(|r| r.l().ok());
                
                if let Some(cs) = code_source {
                    // Get Location (URL)
                    let location = env.call_method(
                        cs,
                        "getLocation",
                        "()Ljava/net/URL;",
                        &[]
                    ).ok().and_then(|r| r.l().ok());
                    
                    if let Some(url) = location {
                        // Get path string
                        let path = env.call_method(
                            url,
                            "getPath",
                            "()Ljava/lang/String;",
                            &[]
                        ).ok().and_then(|r| r.l().ok());
                        
                        if let Some(path_str) = path {
                            if let Ok(java_string) = env.get_string(&path_str.into()) {
                                *JAR_PATH.lock().unwrap() = Some(java_string.to_str().unwrap_or("").to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Convert Java String[] to C-style argc/argv
    let argc = match env.get_array_length(&args) {
        Ok(len) => len,
        Err(_) => return 198,
    };
    
    // Convert to Vec<String> first to own the data
    let mut rust_strings: Vec<String> = Vec::with_capacity(argc as usize);
    for i in 0..argc {
        match env.get_object_array_element(&args, i) {
            Ok(jstring) => {
                match env.get_string(&jstring.into()) {
                    Ok(java_str) => {
                        rust_strings.push(java_str.to_str().unwrap_or("").to_string());
                    }
                    Err(_) => return 198,
                }
            }
            Err(_) => return 198,
        }
    }
    
    // Convert to C strings
    let c_strings: Vec<std::ffi::CString> = rust_strings
        .iter()
        .map(|s| std::ffi::CString::new(s.as_str()).unwrap())
        .collect();
    
    let c_ptrs: Vec<*const c_char> = c_strings
        .iter()
        .map(|s| s.as_ptr())
        .collect();
    
    // Call the registered stata_call function
    if let Some(callback) = *STATA_CALL_CALLBACK.lock().unwrap() {
        callback(argc, c_ptrs.as_ptr())
    } else {
        eprintln!("stata_call callback not registered!");
        198
    }
}


pub fn send_dataframe_to_java(
    df: DataFrame,
    start_index: usize,
    vars_to_stata_types: HashMap<String, i32>,
    n_threads: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("=== STEP 1: Using current JNI environment");
    

    // Load type constants
    let type_options = StataDataType::load()?;

    // Filter columns that are strL type
    let strl_columns: Vec<String> = vars_to_stata_types
        .iter()
        .filter(|(_, stata_type)| **stata_type == type_options.type_strl)
        .map(|(col_name, _)| col_name.clone())
        .collect();

    // Get all variable names and their indices
    let variables: Vec<String> = vars_to_stata_types.keys().cloned().collect();
    let indices: Vec<i32> = variables
        .iter()
        .map(|var_name| get_var_index(var_name).unwrap())
        .collect();

    // Don't attach - we're already in a JNI call!
    // Use the thread-local environment that was set during the original Java->Rust call
    with_current_env(|env| {
        eprintln!("=== STEP 2: Got current environment");
        
        // Get class reference in a limited scope
        let class = {
            eprintln!("=== STEP 3: Acquiring class lock");
            let class_guard = PARQUET_CLASS_REF.lock().unwrap();
            eprintln!("=== STEP 4: Class lock acquired");
            
            let class_ref = class_guard.as_ref().ok_or("Class reference not stored")?;
            unsafe { JClass::from_raw(class_ref.as_obj().as_raw()) }
        };
        eprintln!("=== STEP 5: Class lock released");
     
        eprintln!("=== STEP 6: Processing DataFrame");
        
        // Get the columns
        let columns = df.get_columns();
        
        // Convert each column to Arrow array
        let arrays: Vec<Box<dyn polars_arrow::array::Array>> = columns
            .iter()
            .map(|col| col.clone().rechunk_to_arrow(CompatLevel::newest()))
            .collect();
        
        // Get the schema fields
        let fields: Vec<Field> = df.schema()
            .iter()
            .map(|(name, dtype)| {
                Field::new(name.clone(), dtype.to_arrow(CompatLevel::newest()), false)
            })
            .collect();
        
        // Create struct datatype
        let struct_dtype = polars_arrow::datatypes::ArrowDataType::Struct(fields.clone());
        
        let len = df.height();
        let struct_array = StructArray::new(struct_dtype.clone(), len, arrays, None);
        
        // Create a field for the schema
        let field = Field::new("".into(), struct_dtype, false);
        
        eprintln!("=== STEP 7: Exporting to C Data Interface");
        
        // Export to C Data Interface (owned by this function)
        let schema: ArrowSchema = export_field_to_c(&field);
        let array: ArrowArray = export_array_to_c(Box::new(struct_array));
        
        // Get pointers WITHOUT transferring ownership
        let schema_ptr = &schema as *const ArrowSchema;
        let array_ptr = &array as *const ArrowArray;
        
        eprintln!("=== STEP 8: Schema ptr: {:p}, Array ptr: {:p}", schema_ptr, array_ptr);
        eprintln!("=== STEP 9: About to call Java assignToStata");
        
        // Prepare strL columns array
        let jstrl_columns = env.new_object_array(
            strl_columns.len() as i32,
            "java/lang/String",
            JObject::null()
        )?;
        for (i, col) in strl_columns.iter().enumerate() {
            let jstr = env.new_string(col)?;
            env.set_object_array_element(&jstrl_columns, i as i32, jstr)?;
        }

        // Prepare variables array
        let jvariables = env.new_object_array(
            variables.len() as i32,
            "java/lang/String",
            JObject::null()
        )?;
        for (i, var) in variables.iter().enumerate() {
            let jstr = env.new_string(var)?;
            env.set_object_array_element(&jvariables, i as i32, jstr)?;
        }

        // Prepare indices array
        let jindices = env.new_int_array(indices.len() as i32)?;
        env.set_int_array_region(&jindices, 0, &indices)?;
        
        // Call the method
        if let Err(e) = env.call_static_method(
            class,
            "assignToStata",
            "(JJJI[Ljava/lang/String;[Ljava/lang/String;[I)V",
            &[
                JValue::Long(schema_ptr as i64),
                JValue::Long(array_ptr as i64),
                JValue::Long(start_index as i64),
                JValue::Int(n_threads as i32),
                JValue::Object(&jstrl_columns),
                JValue::Object(&jvariables),
                JValue::Object(&jindices),
            ]
        ) {
            eprintln!("=== ERROR: Java call failed: {:?}", e);
            let _ = env.exception_describe();
            let _ = env.exception_clear();
            return Err("Java exception occurred - see stderr for details".into());
        }
        
        eprintln!("=== STEP 10: Java call completed");
        
        // Check for any pending exceptions
        if let Ok(true) = env.exception_check() {
            eprintln!("=== ERROR: Pending Java exception detected");
            let _ = env.exception_describe();
            let _ = env.exception_clear();
            return Err("Java exception occurred - see stderr for details".into());
        }
        
        eprintln!("=== STEP 11: All done");
        Ok(())
    })
}



/// Clean up FFI structures (call this from Java when done)
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_io_ParquetIO_releaseArrowArray(
    _env: JNIEnv,
    _class: JClass,
    array_ptr: jlong,
) {
    if array_ptr != 0 {
        unsafe {
            let _ = Box::from_raw(array_ptr as *mut polars_arrow::ffi::ArrowArray);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_io_ParquetIO_releaseArrowSchema(
    _env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
) {
    if schema_ptr != 0 {
        unsafe {
            let _ = Box::from_raw(schema_ptr as *mut polars_arrow::ffi::ArrowSchema);
        }
    }
}

/// Generate JNI wrapper functions for calling Java static methods
macro_rules! jni_static_method {
    // Pattern: String parameter -> void
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($param:ident: &str) -> $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($param: &str) -> Result<(), Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    let jstring = env.new_string($param)?;
                    
                    env.call_static_method(
                        class,
                        $java_method,
                        "(Ljava/lang/String;)V",
                        &[JValue::Object(&jstring)]
                    )?;
                    
                    Ok(())
                })
            }
        )*
    };
    
    // NEW: Pattern: (String, String) -> int
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: &str, $p2:ident: &str) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: &str, $p2: &str) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    let jstring1 = env.new_string($p1)?;
                    let jstring2 = env.new_string($p2)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(Ljava/lang/String;Ljava/lang/String;)I",
                        &[JValue::Object(&jstring1), JValue::Object(&jstring2)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };
    
    // Pattern: (String, int) -> int
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: &str, $p2:ident: i32) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: &str, $p2: i32) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    let jstring = env.new_string($p1)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(Ljava/lang/String;I)I",
                        &[JValue::Object(&jstring), JValue::Int($p2)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };
    
    // Pattern: String -> int
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($param:ident: &str) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($param: &str) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    let jstring = env.new_string($param)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(Ljava/lang/String;)I",
                        &[JValue::Object(&jstring)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };
    
    // Pattern: String -> String
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($param:ident: &str) -> String => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($param: &str) -> Result<String, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    let jstring = env.new_string($param)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(Ljava/lang/String;)Ljava/lang/String;",
                        &[JValue::Object(&jstring)]
                    )?;
                    
                    let jstr = result.l()?;
                    let rust_string = env.get_string(&jstr.into())?.into();
                    
                    Ok(rust_string)
                })
            }
        )*
    };
    
    // Pattern: no params -> i64
    (
        class: $java_class:expr,
        $(fn $rust_name:ident() -> i64 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name() -> Result<i64, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "()J",
                        &[]
                    )?;
                    
                    Ok(result.j()?)
                })
            }
        )*
    };

    // Pattern: no params -> i32
    (
        class: $java_class:expr,
        $(fn $rust_name:ident() -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name() -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "()I",
                        &[]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };
    
    // Pattern: (int, i64) -> f64  (for getNum)
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: i32, $p2:ident: i64) -> f64 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: i32, $p2: i64) -> Result<f64, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(IJ)D",
                        &[JValue::Int($p1), JValue::Long($p2)]
                    )?;
                    
                    Ok(result.d()?)
                })
            }
        )*
    };

    
    // Pattern: (int, i64) -> i32
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: i32, $p2:ident: i64) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: i32, $p2: i64) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(IJ)I",
                        &[JValue::Int($p1), JValue::Long($p2)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };

    
    // Pattern: (int, i64, f64) -> i32  (for storeNum)
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: i32, $p2:ident: i64, $p3:ident: f64) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: i32, $p2: i64, $p3: f64) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(IJD)I",
                        &[JValue::Int($p1), JValue::Long($p2), JValue::Double($p3)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };

    // Pattern: (int, i64, String) -> i32  (for storeStr)
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: i32, $p2:ident: i64, $p3:ident: &str) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: i32, $p2: i64, $p3: &str) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    let jstring = env.new_string($p3)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(IJLjava/lang/String;)I",
                        &[JValue::Int($p1), JValue::Long($p2), JValue::Object(&jstring)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };

    // Pattern: i64 -> int
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($param:ident: i64) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($param: i64) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(J)I",
                        &[JValue::Long($param)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };

    // Pattern: int -> int
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($param:ident: i32) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($param: i32) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(I)I",
                        &[JValue::Int($param)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };

    // Pattern: (int, String) -> int
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: i32, $p2:ident: &str) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: i32, $p2: &str) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    let jstring = env.new_string($p2)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(ILjava/lang/String;)I",
                        &[JValue::Int($p1), JValue::Object(&jstring)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };

    // Pattern: f64 -> int
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($param:ident: f64) -> i32 => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($param: f64) -> Result<i32, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(D)I",
                        &[JValue::Double($param)]
                    )?;
                    
                    Ok(result.i()?)
                })
            }
        )*
    };

    // Pattern: (int, i64) -> String
    (
        class: $java_class:expr,
        $(fn $rust_name:ident($p1:ident: i32, $p2:ident: i64) -> String => $java_method:expr;)*
    ) => {
        $(
            pub fn $rust_name($p1: i32, $p2: i64) -> Result<String, Box<dyn std::error::Error>> {
                with_current_env(|env| {
                    let class = env.find_class($java_class)?;
                    
                    let result = env.call_static_method(
                        class,
                        $java_method,
                        "(IJ)Ljava/lang/String;",
                        &[JValue::Int($p1), JValue::Long($p2)]
                    )?;
                    
                    let jstr = result.l()?;
                    let rust_string = env.get_string(&jstr.into())?.into();
                    
                    Ok(rust_string)
                })
            }
        )*
    };
}


// Some rust->java functions
// SFIToolkit methods
jni_static_method! {
    class: "com/stata/sfi/SFIToolkit",
    fn display(msg: &str) -> "display";
    fn displayln(msg: &str) -> "displayln";
    fn error(msg: &str) -> "error";
}

// Data methods - String -> int
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn add_var_double(name: &str) -> i32 => "addVarDouble";
    fn add_var_float(name: &str) -> i32 => "addVarFloat";
    fn add_var_int(name: &str) -> i32 => "addVarInt";
    fn add_var_long(name: &str) -> i32 => "addVarLong";
    fn add_var_byte(name: &str) -> i32 => "addVarByte";
    fn add_var_strl(name: &str) -> i32 => "addVarStrL";
    fn get_var_index(varname: &str) -> i32 => "getVarIndex";
}

// Data methods - (String, int) -> int
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn add_var_str(name: &str, length: i32) -> i32 => "addVarStr";
}

// Data methods - no params -> i64
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn get_obs_total() -> i64 => "getObsTotal";
    fn get_obs_parsed_in1() -> i64 => "getObsParsedIn1";
    fn get_obs_parsed_in2() -> i64 => "getObsParsedIn2";
}

// Data methods - no params -> i32
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn get_var_count() -> i32 => "getVarCount";
}

// Data methods - (int, i64) -> f64
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn get_num(var: i32, obs: i64) -> f64 => "getNum";
}

// Data methods - (int, i64, f64) -> i32
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn store_num(var: i32, obs: i64, value: f64) -> i32 => "storeNum";
    fn store_num_fast(var: i32, obs: i64, value: f64) -> i32 => "storeNumFast";
}

// Data methods - (int, i64, &str) -> i32
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn store_str_any(var: i32, obs: i64, value: &str) -> i32 => "storeStr";
    fn store_str(var: i32, obs: i64, value: &str) -> i32 => "storeStrf";
    fn store_str_fast(var: i32, obs: i64, value: &str) -> i32 => "storeStrfFast";
}

// Data methods - (i32) -> i32
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn get_type(obs: i32) -> i32 => "getType";
    fn get_str_var_width(obs: i32) -> i32 => "getStrVarWidth";
}

// Data methods - (i64) -> i32
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn set_obs_total(obs: i64) -> i32 => "setObsTotal";
}

// Data methods - (f64) -> i32
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn get_best_type(obs: f64) -> i32 => "getBestType";
}

// Data methods - (i32, i64) -> String
jni_static_method! {
    class: "com/stata/sfi/Data",
    fn get_str_any(row:i32, column:i64) -> String => "getStr";
    fn get_str(row:i32, column:i64) -> String => "getStrf";
}

jni_static_method! {
    class: "com/stata/sfi/Data",
    fn set_var_format(var: i32, format: &str) -> i32 => "setVarFormat";
    fn set_var_label(var: i32, label: &str) -> i32 => "setVarLabel";
}

// Macro methods
jni_static_method! {
    class: "com/stata/sfi/Macro",
    fn set_local(name: &str, value: &str) -> i32 => "setLocal";
    fn set_global(name: &str, value: &str) -> i32 => "setGlobal";
}

jni_static_method! {
    class: "com/stata/sfi/Macro",
    fn get_local(name: &str) -> String => "getLocalSafe";
    fn get_global(name: &str) -> String => "getGlobalSafe";
}




// Read a single static int field
fn get_java_constant(class_name: &str, field_name: &str) -> Result<i32, Box<dyn std::error::Error>> {
    with_current_env(|env| {
        let class = env.find_class(class_name)?;
        let value = env.get_static_field(&class, field_name, "I")?;
        Ok(value.i()?)
    })
}

// Struct with stata data type the constants
pub struct StataDataType {
    pub type_byte: i32,
    pub type_int: i32,
    pub type_long: i32,
    pub type_float: i32,
    pub type_double: i32,
    pub type_str: i32,
    pub type_strl: i32,
}

impl StataDataType {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        with_current_env(|env| {
            let class = env.find_class("com/stata/sfi/Data")?;
            
            Ok(StataDataType {
                type_byte: env.get_static_field(&class, "TYPE_BYTE", "I")?.i()?,
                type_int: env.get_static_field(&class, "TYPE_INT", "I")?.i()?,
                type_long: env.get_static_field(&class, "TYPE_LONG", "I")?.i()?,
                type_float: env.get_static_field(&class, "TYPE_FLOAT", "I")?.i()?,
                type_double: env.get_static_field(&class, "TYPE_DOUBLE", "I")?.i()?,
                type_str: env.get_static_field(&class, "TYPE_STR", "I")?.i()?,
                type_strl: env.get_static_field(&class, "TYPE_STRL", "I")?.i()?,
            })
        })
    }
}

// Usage:
// let types = StataDataType::load()?;
// println!("TYPE_DOUBLE = {}", types.type_double);