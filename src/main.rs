// use tikv_jemallocator::Jemalloc;
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

//  use log::{debug, info, warn, error};

#[cfg(debug_assertions)]
use env_logger::Builder;

#[cfg(debug_assertions)]
use std::io::Write;


pub mod read;
pub mod write;
pub mod describe;
pub mod mapping;
pub mod stata_interface;
pub mod utilities;
pub mod downcast;

#[cfg(debug_assertions)]
mod sql_from_if;

#[cfg(debug_assertions)]
use crate::read::data_exists;
 



#[cfg(not(debug_assertions))]
fn main() {
    //  Do nothing
}



#[cfg(debug_assertions)]
struct ReadParams {
    path:String,
    variables_as_str:String,
    n_rows:usize,
    offset:usize,
    sql_if:Option<String>,
    mapping:String,
}

#[cfg(debug_assertions)]
impl ReadParams {
    pub fn new(
        path:String,
        variables_as_str:String,
        n_rows:usize,
        offset:usize,
        sql_if:Option<String>,
        mapping:String,
    ) -> Self {
        ReadParams {
            path:path,
            variables_as_str: variables_as_str, 
            n_rows: n_rows,
            offset: offset,
            sql_if: sql_if,
            mapping: mapping,
        }
    }
}

#[cfg(debug_assertions)]
fn main() {
    //  env_logger::init();
    Builder::from_default_env()
        .format(|buf, record| {
            writeln!(buf, "[{}] {}", 
                record.level(),
                record.args()
            )
        })
        .init();

    _ = test_glob_path();
}


#[cfg(debug_assertions)]
fn test_glob_path() {
    let valid1 = data_exists("C:/Users/jonro/Downloads/random_types.parquet");
    let valid2 = data_exists("C:/Users/jonro/Downloads/random_types_*.parquet");
    let valid3 = data_exists("C:/Users/jonro/Downloads/random_types_partitioned.parquet");
    let valid4 = data_exists("C:/Users/jonro/Downloads/random_types_partitioned.parquet/**.parquet");

    let invalid1 = data_exists("C:/Users/jonro/Downloads/random_types_wrong.parquet");
    let invalid2 = data_exists("C:/Users/jonro/Downloads/random_types_*_test.parquet");
    let invalid3 = data_exists("C:/Users/jonro/Downloads/random_types__test.parquet/**.parquet");

    println!("valid1 = {:?}",valid1);
    println!("valid2 = {:?}",valid2);
    println!("valid3 = {:?}",valid3);
    println!("valid4 = {:?}",valid4);
    println!("invalid1 = {:?}",invalid1);
    println!("invalid2 = {:?}",invalid2);
    println!("invalid3 = {:?}",invalid3);
    
}


#[cfg(debug_assertions)]
fn test_stata_if_to_sql() {
    let test1 = sql_from_if::stata_to_sql("age > 30");
    let test2 = sql_from_if::stata_to_sql("age > 30 & gender == \"male\"");
    let test3 = sql_from_if::stata_to_sql("inrange(income, 1000, 5000)");
    let test4 = sql_from_if::stata_to_sql("inlist(country, \"USA\", \"Canada\")");
    let test5 = sql_from_if::stata_to_sql("missing(value)");
    let test6 = sql_from_if::stata_to_sql("inrange(age, 18, 65) & !missing(income) | status == \"active\"");
    let test7 = sql_from_if::stata_to_sql("a == 5 & (b == 10 | c >= 72)");
    let test8 = sql_from_if::stata_to_sql("a == 5 & ((b == 10 | c >= 72) == 1)");

    let test9 = sql_from_if::stata_to_sql("age > -30");
    
    println!("test1 = {:?}",test1);
    println!("test2 = {:?}",test2);
    println!("test3 = {:?}",test3);
    println!("test4 = {:?}",test4);
    println!("test5 = {:?}",test5);
    println!("test6 = {:?}",test6);
    println!("test7 = {:?}",test7);
    println!("test8 = {:?}",test8);
    println!("test9 = {:?}",test9);


}