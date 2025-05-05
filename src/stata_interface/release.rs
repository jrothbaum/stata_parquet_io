pub use stata_sys::ST_retcode;
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