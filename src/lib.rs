// use tikv_jemallocator::Jemalloc;
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::slice;
use polars::io::partition;
use polars::prelude::*;
use utilities::ParallelizationStrategy;


pub mod read;
pub mod write;
pub mod mapping;
pub mod stata_interface;
pub mod describe;
pub mod sql_from_if;
pub mod utilities;

use std::ptr;


use stata_interface::{
    display,
    ST_retcode,
    ST_plugin,
    SD_PLUGINVER
};
use describe::file_summary;
use read::{
    data_exists,
    read_to_stata
};


#[no_mangle]
pub static mut _stata_: *mut stata_sys::ST_plugin = ptr::null_mut();

#[no_mangle]
pub extern "C" fn pginit(p: *mut stata_sys::ST_plugin) -> stata_sys::ST_retcode {
    unsafe {
        _stata_ = p;
    }
    stata_sys::SD_PLUGINVER
}

#[no_mangle]
pub extern "C" fn stata_call(argc: c_int, argv: *const *const c_char) -> ST_retcode {
    // Wrap the entire function body in catch_unwind
    std::panic::catch_unwind(|| {
    
        if argc < 1 || argv.is_null() {
            stata_interface::display("Error: No subfunction specified");
            return 198; // Syntax error
        }


        // Convert arguments to Rust strings
        let args: Vec<&str> = unsafe {
            let arg_ptrs = slice::from_raw_parts(argv, argc as usize);
            let mut rust_args = Vec::with_capacity(argc as usize);
            
            for arg_ptr in arg_ptrs {
                if arg_ptr.is_null() {
                    
                    stata_interface::display("Error: Null argument");
                    return 198; // Syntax error
                }
                
                match CStr::from_ptr(*arg_ptr).to_str() {
                    Ok(s) => rust_args.push(s),
                    Err(_) => {
                        stata_interface::display("Error: Invalid UTF-8 in argument");
                        return 198; // Syntax error
                    }
                }
            }
            
            rust_args
        };
        
        // display(&format!("{:?}",&args));
        // First argument is the subfunction name
        let subfunction_name = args[0];
        
        // Remaining arguments are passed to the subfunction
        let subfunction_args = &args[1..];
        //    println!("subfunction_args = {:?}",subfunction_args);
        
        
        // Call the appropriate subfunction
        match subfunction_name {
            "setup_check" => {
                return 0 as ST_retcode;
            }
            "read" => {
                if !data_exists(&subfunction_args[0]) {
                    stata_interface::display(&format!("File does not exist ({})",subfunction_args[0]));
                    return 601 as ST_retcode;
                }
                
                // for i in 0..9 {
                //     display(&format!("{} = {:?}", i,subfunction_args[i]));
                // }
                let parallel_strategy:Option<ParallelizationStrategy> = match subfunction_args[6] {
                    "columns" => Some(ParallelizationStrategy::ByColumn),
                    "rows" => Some(ParallelizationStrategy::ByRow),
                    //  Use default based on file dimensions
                    _ => None
                };

                
                let safe_relaxed = match subfunction_args[7] {
                    "0" => false,
                    "1" => true,
                    _ => false
                };

                let asterisk_to_variable_name = if subfunction_args[8].is_empty() {
                    None
                } else {
                    Some(subfunction_args[8])
                };


                let read_result = read_to_stata(
                    subfunction_args[0],
                    subfunction_args[1],
                    subfunction_args[2].parse::<usize>().unwrap(),
                    subfunction_args[3].parse::<usize>().unwrap(),
                    Some(subfunction_args[4]),
                    subfunction_args[5],
                    parallel_strategy,
                    safe_relaxed,
                    asterisk_to_variable_name,
                );
        
                // Use match to handle the Result
                match read_result {
                    Ok(_) => {
                        //  Do nothing
                    },
                    Err(e) => {
                        display(&format!("Error reading the file = {:?}",e));
                    }
                }

            },
            "describe" => {
                if !data_exists(&subfunction_args[0]) {
                    stata_interface::display(&format!("File does not exist ({})",subfunction_args[0]));
                    return 601 as ST_retcode;
                }

                let asterisk_to_variable_name = if subfunction_args[4].is_empty() {
                    None
                } else {
                    Some(subfunction_args[4])
                };
                return file_summary(
                        subfunction_args[0],
                        subfunction_args[1].parse::<u8>().unwrap() != 0,
                        subfunction_args[2].parse::<u8>().unwrap() != 0,
                        Some(subfunction_args[3].as_ref()),
                        true,
                        asterisk_to_variable_name,
                    ) as ST_retcode;
            },
            "save" => {
                let path = subfunction_args[0];
                let varlist = subfunction_args[1];
                let n_rows = subfunction_args[2];
                let offset =  subfunction_args[3];
                let sql_if =  subfunction_args[4];
                let mapping = subfunction_args[5];
                let partition_by = subfunction_args[6];
                let compression = subfunction_args[7];
                let compression_level_passed = subfunction_args[8].parse::<i32>().unwrap();
                let overwrite_partition = subfunction_args[9].parse::<i32>().unwrap() == 1;
                
                let compression_level = if compression_level_passed == -1 {
                    None
                } else {
                    Some(compression_level_passed as usize)
                };
                let output = match write::write_from_stata(
                    path,
                    varlist,
                    n_rows.parse::<usize>().unwrap(),
                    offset.parse::<usize>().unwrap(),
                    Some(sql_if),
                    mapping,
                    None,
                    partition_by,
                    compression,
                    compression_level,
                    overwrite_partition,
                ) {
                    Ok(_) => 0 as i32,
                    Err(_e) => 198 as i32
                };
                return output as ST_retcode;
            },
            "if" => {
                let sql_if = sql_from_if::stata_to_sql(subfunction_args[0] as &str);

                match sql_if {
                    Ok(sql) => {
                        stata_interface::set_macro("sql_if", &sql, false);
                    },
                    Err(e) => {
                        display(&format!("Error parsing if statement: {:?}", e));
                        return 198 as ST_retcode;
                    }
                }
            },
            _ => {
                stata_interface::display(&format!("Error: Unknown subfunction '{}'\n\0", subfunction_name));
                return 198 as ST_retcode;
            },
        }
        
        // Return success (0)
        0 as ST_retcode
    }).unwrap_or_else(|panic_error| {
        // Extract and display the panic message
        let panic_message = if let Some(string) = panic_error.downcast_ref::<String>() {
            format!("Panic occurred: {}", string)
        } else if let Some(str_slice) = panic_error.downcast_ref::<&str>() {
            format!("Panic occurred: {}", str_slice)
        } else {
            "Panic occurred with unknown error".to_string()
        };
        
        // Display the panic message
        stata_interface::display(&panic_message);
        
        // Return a specific error code for panics
        198 as ST_retcode
    })
}



