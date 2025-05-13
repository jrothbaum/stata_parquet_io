pub type ST_retcode = i32;
pub type ST_plugin = u8;
use std::ffi::CString;
use std::os::raw::c_char;

// Add a global static counter
use std::sync::atomic::{AtomicUsize, Ordering};
static PRINT_COUNTER: AtomicUsize = AtomicUsize::new(0);
const MAX_PRINTS: usize = 250;

pub type pginit = u8;

// Helper function to check and increment the counter
fn should_print() -> bool {
    let current = PRINT_COUNTER.fetch_add(1, Ordering::SeqCst);
    if current < MAX_PRINTS {
        true
    } else if current == MAX_PRINTS {
        println!("Maximum print limit ({}) reached. Further output suppressed.", MAX_PRINTS);
        true
    } else {
        false
    }
}

pub fn display(msg: &str) -> i32 {
    if should_print() {
        println!("display:  {}", &msg);
    }
    0 as i32    
}

pub fn set_macro(
    macro_name: &str,
    value: &str,
    global: bool
) -> i32 {
    if should_print() {
        println!(
            "set macro {} = {} [global={}]",
            macro_name,
            value,
            global
        );
    }
    0 as i32
}


#[inline]
pub fn get_macro(
    macro_name:&str,
    global:bool,
    buffer_size: Option<usize>
) -> String {
    "macro value".to_string()
}

pub fn set_scalar(
    scalar_name: &str,
    value: &f64,
    //  global:bool
) -> i32 {
    if should_print() {
        println!(
            "set scalar {} = {}",
            scalar_name,
            value
        );
    }
    0 as i32
}

pub fn replace_number(
    value: Option<f64>,
    row: usize,
    column: usize
) -> i32 {
    if should_print() {
        println!(
            "Setting numeric value of {},{} = {:?}",
            row,
            column,
            value
        );
    }
    0 as i32
}

pub fn replace_string(
    value: Option<String>,
    row: usize,
    column: usize
) -> i32 {
    if should_print() {
        println!(
            "Setting string value of {},{} = {:?}",
            row,
            column,
            value
        );
    }
    0 as i32
}


pub fn n_obs() -> i32 {
    return 100;
}

pub fn read_numeric(column: usize, row: usize) -> Option<f64> {
    Some(1.0)
}

#[inline]
pub fn read_string(column: usize, row: usize,string_length:usize) -> String {
    "Hi".to_string()
}
