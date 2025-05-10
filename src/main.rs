// use tikv_jemallocator::Jemalloc;
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

//  use log::{debug, info, warn, error};
use log::debug;
use env_logger::Builder;
use std::{env, io::Write};


pub mod read;
pub mod write;
pub mod describe;
pub mod mapping;
pub mod stata_interface;


#[cfg(debug_assertions)]
mod sql_from_if;


use describe::{
    get_schema,
    get_row_count,
    file_summary
}; 
use crate::read::{  
    scan_lazyframe,
    read_to_stata
};
 



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

        
    write_test();
}



#[cfg(debug_assertions)]
fn write_test() {
    let write_param1 = ReadParams::new(
        r#"C:/Users/jonro/Downloads/test_vs_code.parquet"#.to_owned(),
        "mychar mynum mydate dtime mylabl myord mytime".to_owned(),
        0,
        0,
        Some("".to_owned()),
        r#"[{"name":"mychar","dtype":"String","format":"%9s","str_length":1},{"name":"mynum","dtype":"Double","format":"%10.0g","str_length":0},{"name":"mydate","dtype":"Long","format":"%td","str_length":0},{"name":"dtime","dtype":"Double","format":"%tc","str_length":0},{"name":"mylabl","dtype":"Double","format":"%10.0g","str_length":0},{"name":"myord","dtype":"Double","format":"%10.0g","str_length":0},{"name":"mytime","dtype":"Double","format":"%tchh:mm:ss","str_length":0}]"#.to_owned(),
    );

    let result = write::write_from_stata(
        &write_param1.path,
        &write_param1.variables_as_str, 
        write_param1.n_rows,
        write_param1.offset,
        write_param1.sql_if.as_deref(), 
        &write_param1.mapping,
        None
    );
}


#[cfg(debug_assertions)]
fn read_test() {
    let path = "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet";
    let sql_if = sql_from_if::stata_to_sql("mynum > 0 | missing(mynum) | mytime > 1.1").unwrap();
    
    // let read_param1 = ReadParams::new(
    //     "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet".to_owned(),
    //     "mychar mynum mydate dtime mylabl myord mytime".to_owned(),
    //     6,
    //     0,
    //     Some("".to_owned()),
    //     r#"[{"name":"mychar","dtype":"String","stata_type":"string"},{"name":"mynum","dtype":"Float64","stata_type":"double"},{"name":"mydate","dtype":"Date","stata_type":"date"},{"name":"dtime","dtype":"Datetime(Milliseconds, None)","stata_type":"datetime"},{"name":"mylabl","dtype":"Float64","stata_type":"double"},{"name":"myord","dtype":"Float64","stata_type":"double"},{"name":"mytime","dtype":"Time","stata_type":"time"}]"#.to_owned(),
    // );

    let read_param1 = ReadParams::new(
        r#"C:\Users\jonro\Downloads\flights-1m.parquet"#.to_owned(),
        "FL_DATE DEP_DELAY ARR_DELAY AIR_TIME DISTANCE DEP_TIME ARR_TIME".to_owned(),
        10000,
        0,
        Some("".to_owned()),
        r#"[{"name":"FL_DATE","dtype":"Date","stata_type":"date"},{"name":"DEP_DELAY","dtype":"Int16","stata_type":"int"},{"name":"ARR_DELAY","dtype":"Int16","stata_type":"int"},{"name":"AIR_TIME","dtype":"Int16","stata_type":"int"},{"name":"DISTANCE","dtype":"Int16","stata_type":"int"},{"name":"DEP_TIME","dtype":"Float32","stata_type":"float"},{"name":"ARR_TIME","dtype":"Float32","stata_type":"float"}]"#.to_owned(),
    );

    file_summary(
        &read_param1.path,
        false,
        true,
        read_param1.sql_if.as_deref()
    );
    

    let df = scan_lazyframe(&path).unwrap().collect().unwrap();
    println!("{}", df);


    let read_result = read_to_stata(
        &read_param1.path,
        &read_param1.variables_as_str,
        read_param1.n_rows,
        read_param1.offset,
        read_param1.sql_if.as_deref(),
        &read_param1.mapping,
    );
    //  test_stata_if_to_sql();
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
    
    println!("test1 = {:?}",test1.unwrap());
    println!("test2 = {:?}",test2.unwrap());
    println!("test3 = {:?}",test3.unwrap());
    println!("test4 = {:?}",test4.unwrap());
    println!("test5 = {:?}",test5.unwrap());
    println!("test6 = {:?}",test6.unwrap());
    println!("test7 = {:?}",test7.unwrap());
    println!("test8 = {:?}",test8.unwrap());
    println!("test9 = {:?}",test9.unwrap());


}