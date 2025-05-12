pub use stata_sys::{ST_retcode, ST_plugin};
use stata_sys;

use std::ffi::CString;
use std::os::raw::c_char;

#[inline]
pub fn display(msg: &str) -> i32 {
    stata_sys::display(&msg)    
}

#[inline]
pub fn set_macro(
    macro_name:&str,
    value:&str,
    global:bool
) -> i32 {
    stata_sys::set_macro(macro_name, value, global)
}

#[inline]
pub fn get_macro(
    macro_name:&str,
    global:bool,
    buffer_size: Option<usize>
) -> String {
    stata_sys::get_macro(macro_name, global, buffer_size).unwrap()
}

#[inline]
pub fn set_scalar(
    scalar_name:&str,
    value:&f64,
    //  global:bool
) -> i32 {
    stata_sys::set_scalar(scalar_name, value)
}


#[inline]
pub fn replace_number(
    value:Option<f64>,
    row:usize,
    column:usize
) -> i32 {
    stata_sys::replace_number(value,row,column)
}

#[inline]
pub fn replace_string(
    value:Option<String>,
    row:usize,
    column:usize
) -> i32 {
    stata_sys::replace_string(value,row,column)
}


#[inline]
pub fn n_obs() -> i32 {
    unsafe {
        stata_sys::SF_nobs()
    }
}

#[inline]
pub fn is_missing(value:f64) -> bool {
    unsafe {
        stata_sys::SF_is_missing(value)
    }
}

#[inline]
pub fn read_numeric(column: usize, row: usize) -> Option<f64> {
    // Create a mutable variable to store the result
    let mut result: f64 = 0.0;
    
    // Call the unsafe FFI function
    let status = unsafe {
        stata_sys::SF_vdata(column as i32, row as i32, &mut result)
    };

    // Return None if result is less than SV_MISSVAL, otherwise return Some(result)
    if (status == 0) & !is_missing(result) {
        Some(result)
    } else {
        None
    }
}

#[inline]
pub fn read_string(
    column: usize, 
    row: usize,
    string_length:usize
) -> String {
    //  No null value in stata strings (just "")

    // Allocate a buffer with the known string length plus 1 for null terminator
    let buffer_size = string_length + 1;
    let mut buffer = vec![0u8; buffer_size];
    
    // Call the unsafe FFI function with our buffer
    unsafe {
        stata_sys::SF_sdata(
            column as i32, 
            row as i32, 
            buffer.as_mut_ptr() as *mut std::os::raw::c_char
        );
        
        // The C function might still fill a shorter string than the allocated length,
        // so we need to find the actual null terminator
        let null_pos = buffer.iter().position(|&c| c == 0).unwrap_or(buffer_size);
        
        // Create a string from the buffer up to the null terminator
        String::from_utf8_lossy(&buffer[0..null_pos]).to_string()
    }
}
