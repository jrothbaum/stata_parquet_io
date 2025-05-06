use std::path::Path;
use std::env;
use polars::error::ErrString;
use polars::prelude::*;
use polars::lazy::dsl;
use polars_sql::SQLContext;
use polars::datatypes::AnyValue;
use std::{fs::File, path::PathBuf};
use std::sync::{Arc, Mutex};
use std::error::Error;
use std::thread;
use std::cmp::min;
use serde_json;


use crate::mapping::ColumnInfo;
use crate::stata_interface:: {
    ST_retcode,
    display,
    set_macro,
    set_scalar,
    replace_number,
    replace_string
};


pub const DAY_SHIFT_SAS_STATA: i32 = 3653;
pub const SEC_SHIFT_SAS_STATA: i64 = 315619200;

pub const SEC_MILLISECOND: i64 = 1_000;
pub const SEC_MICROSECOND: i64 = 1_000_000;
pub const SEC_NANOSECOND: i64 = 1_000_000_000;

pub fn file_exists_and_is_file(path: &str) -> bool {
    let path = Path::new(path);
    path.exists() && path.is_file()
}

pub fn scan_lazyframe(path:&str) -> Result<LazyFrame,PolarsError> {
    let mut df = LazyFrame::scan_parquet(path, ScanArgsParquet::default());

    let schema = df.as_mut().unwrap().collect_schema();

    println!("schema = {:?}", schema);

    df
}


pub fn read_to_stata(
    path:&str,
    variables_as_str:&str,
    n_rows:usize,
    offset:usize,
    sql_if:Option<&str>,
    mapping:&str,
) -> Result<i32,Box<dyn Error>> {

    let all_columns: Vec<ColumnInfo> = serde_json::from_str(mapping).unwrap();

    //  display(&format!("Column information: {:?}",all_columns));

    let mut df = match scan_lazyframe(&path) {
        Ok(df) => df,
        Err(e) => {
            display(&format!("Error scanning lazyframe: {:?}", e));
            return Ok(198);
        },
    };

    
    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", df);
        


        df = match ctx.execute(&format!("select * from df where {}",sql)) {
            Ok(lazyframe) => lazyframe,
            Err(e) => {
                display(&format!("Error in SQL if statement: {}", e));
                return Ok(198);
            }
        };
    }

    let columns: Vec<Expr> = variables_as_str.split_whitespace()
        .map(|s| col(s))
        .collect();

    

    

    

    let batch_size:usize = 1_000_000;
    let n_batches = (n_rows as f64 / batch_size as f64).ceil() as usize;


    //  display(&format!("n_batches: {}",n_batches));
    let n_threads = get_thread_count();
    for batchi in 0..n_batches {
        let mut df_batch = df.clone();

        let batch_offseti = (offset + batchi*batch_size);

        let batch_lengthi = if ((batchi+1)*batch_size > n_rows) {
            (n_rows - batchi*batch_size) as usize
        } else {
            batch_size
        } as u32;
        // display("");
        // display(&format!("Batch #{}",batchi+1));
        // display(&format!("Batch start: {}",batch_offseti));
        // display(&format!("Batch length: {}",batch_size));
        df_batch = df_batch.slice(
            batch_offseti as i64, 
            batch_lengthi
        );


        let _ = match process_batch(
            &df_batch.collect().unwrap(), 
            batch_offseti, 
            &all_columns,
            n_threads
        ) {
            Ok(_) => {
                //  Do nothing
            },
            Err(e) => {
                display(&format!("Error assigning values to stata data: {:?}", e));
                return Ok(198);
            },
        };    
        
        
    }

    Ok(0)
}

fn process_batch(
    batch: &DataFrame,
    start_index: usize,
    all_columns: &Vec<ColumnInfo>,
    n_threads: usize
) -> PolarsResult<()> {
    use std::thread;
    use std::sync::{Arc, Mutex};

    let row_count = batch.height();

    
    // If only 1 thread requested or batch is too small, use single-threaded version
    let min_multithreaded = 10000;
    if n_threads <= 1 || row_count < min_multithreaded {
        return process_batch_single_thread(batch, start_index, all_columns);
    }
    
    // Share the batch and columns across threads safely
    let batch = Arc::new(batch.clone());
    let all_columns = Arc::new(all_columns.clone());
    
    // Track errors across threads
    let error = Arc::new(Mutex::new(None));
    
    // Calculate rows per thread and prepare thread handles
    let mut handles = Vec::with_capacity(n_threads);
    let rows_per_thread = ((row_count as f64) / (n_threads as f64)).ceil() as usize;
    
    // Spawn threads to process row chunks
    for thread_idx in 0..n_threads {
        let thread_batch = Arc::clone(&batch);
        let thread_columns = Arc::clone(&all_columns);
        let thread_error = Arc::clone(&error);
        
        // Calculate row range for this thread
        let start_row = thread_idx * rows_per_thread;
        let end_row = std::cmp::min((thread_idx + 1) * rows_per_thread, row_count);
        
        // Skip if there's no work for this thread
        if start_row >= row_count {
            continue;
        }

        
        // Spawn a thread to process this chunk of rows
        let handle = thread::spawn(move || {
            // Process the range of rows for all columns
            match process_row_range(
                &thread_batch, 
                start_index,
                start_row, 
                end_row, 
                &thread_columns
            ) {
                Ok(_) => {},
                Err(e) => {
                    let mut err_lock = thread_error.lock().unwrap();
                    if err_lock.is_none() {
                        *err_lock = Some(e);
                    }
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        if let Err(_) = handle.join() {
            return Err(PolarsError::ComputeError(
                ErrString::from("Thread panicked during processing")
            ));
        }
    }
    
    // Check if any thread encountered an error
    let error_result = Arc::try_unwrap(error)
        .expect("There are still references to the error")
        .into_inner()
        .expect("Failed to unlock mutex");
    
    // Return the error if one occurred, otherwise return Ok
    match error_result {
        Some(e) => Err(e),
        None => Ok(())
    }
}


// Single-threaded implementation (fallback)
fn process_batch_single_thread(
    batch: &DataFrame,
    start_index: usize,
    all_columns: &Vec<ColumnInfo>
) -> PolarsResult<()> {
    // Process all rows for all columns in a single thread
    process_row_range(batch, start_index, 0, batch.height(), all_columns)
}

// Process a specific range of rows for all columns
fn process_row_range(
    batch: &DataFrame,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    all_columns: &Vec<ColumnInfo>
) -> PolarsResult<()> {
    // Iterate through each column
    for (col_idx, col_info) in all_columns.iter().enumerate() {
        // Get the column by name
        let col = batch.column(&col_info.name)?;
        
        // Process each value in the column based on its Stata type
        match col_info.stata_type.as_str() {
            "strl" => {
                return Err(PolarsError::SchemaMismatch(
                    ErrString::from("Strl assignment not implemented yet")
                ));                
            },
            "string" => {
                // Handle string types
                if let Ok(str_col) = col.str() {
                    for row_idx in start_row..end_row {
                        let global_row_idx = row_idx + start_index;

                        // Get the string value at this row position
                        let opt_val = match str_col.get(row_idx) {
                            Some(s) => Some(s.to_string()),
                            None => None
                        };
                        
                        replace_string(
                            opt_val, 
                            global_row_idx + 1, // +1 because replace_string expects 1-indexed
                            col_idx + 1        // +1 because replace functions expect 1-indexed
                        );
                    }
                }
            },
            "datetime" => {
                // Get the time_unit from the schema if it's a datetime column
                let time_unit = if col.dtype().is_temporal() {
                    match col.dtype() {
                        DataType::Datetime(time_unit, _) => Some(*time_unit),
                        _ => None
                    }
                } else {
                    None
                };

                if time_unit.is_none() {
                    return Err(PolarsError::SchemaMismatch(
                        ErrString::from(format!("No time unit specified for {}", &col_info.name))
                    ));
                }
                let time_unit_unwrapped = time_unit.unwrap();
                
                // Process each row based on the schema's time unit
                for row_idx in start_row..end_row {
                    let global_row_idx = row_idx + start_index;
                    let value: Option<f64> = match col.get(row_idx) {
                        Ok(AnyValue::Datetime(v, _, _)) => { 
                            // Use the time_unit from the schema
                            match time_unit_unwrapped {
                                TimeUnit::Nanoseconds => Some(v as f64 / 1_000_000.0 + (SEC_SHIFT_SAS_STATA as f64)*1000.0),
                                TimeUnit::Microseconds => Some(v as f64 / 1_000.0 + (SEC_SHIFT_SAS_STATA as f64)*1000.0),
                                TimeUnit::Milliseconds => Some(v as f64 + (SEC_SHIFT_SAS_STATA as f64)*1000.0),
                            }
                        },
                        _ => None
                    };

                    replace_number(
                        value, 
                        global_row_idx + 1,  // +1 because replace functions expect 1-indexed
                        col_idx + 1          // +1 because replace functions expect 1-indexed
                    );
                }
            },
            _ => {
                // Handle numeric types (including date/time which get converted to numeric)
                // Get the column's data type from the stored string representation
                let dtype_str = col_info.dtype.as_str();
                
                for row_idx in start_row..end_row {
                    let global_row_idx = row_idx + start_index;
                    
                    let value: Option<f64> = match col.get(row_idx) {
                        Ok(any_value) => match (dtype_str, any_value) {
                            ("Boolean", AnyValue::Boolean(b)) => Some(if b { 1.0 } else { 0.0 }),
                            ("Int8", AnyValue::Int8(v)) => Some(v as f64),
                            ("Int16", AnyValue::Int16(v)) => Some(v as f64),
                            ("Int32", AnyValue::Int32(v)) => Some(v as f64),
                            ("Int64", AnyValue::Int64(v)) => Some(v as f64),
                            ("UInt8", AnyValue::UInt8(v)) => Some(v as f64),
                            ("UInt16", AnyValue::UInt16(v)) => Some(v as f64),
                            ("UInt32", AnyValue::UInt32(v)) => Some(v as f64),
                            ("UInt64", AnyValue::UInt64(v)) => Some(v as f64),
                            ("Float32", AnyValue::Float32(v)) => Some(v as f64),
                            ("Float64", AnyValue::Float64(v)) => Some(v),
                            ("Date", AnyValue::Date(v)) => Some((v + DAY_SHIFT_SAS_STATA) as f64),
                            ("Time", AnyValue::Time(v)) => Some((v/SEC_MICROSECOND) as f64),
                            _ => None
                        },
                        Err(_) => None
                    };
                    
                    replace_number(
                        value, 
                        global_row_idx + 1,  // +1 because replace functions expect 1-indexed
                        col_idx + 1          // +1 because replace functions expect 1-indexed
                    );
                }
            }
        }
    }
    
    Ok(())
}

// fn process_batch_deprecated(
//     batch: &DataFrame,
//     start_index: usize,
//     all_columns: &Vec<ColumnInfo>,
//     n_threads:usize
// ) -> PolarsResult<()> {
//     // Iterate through each column in the batch
//     for (col_idx, col_info) in all_columns.iter().enumerate() {
//         // Get the column by name
//         let col = batch.column(&col_info.name)?;
//         //  display(&format!("{}:{},{}",col_idx,&col_info.name,col_info.stata_type.as_str()));
//         // Process each value in the column based on its Stata type
//         match col_info.stata_type.as_str() {
//             "strl" => {
//                 return Err(PolarsError::SchemaMismatch(ErrString::from("Strl assignment not implemented yet")));                
//             },
//             "string" => {
//                 // Handle string types
//                 if let Ok(str_col) = col.str() {
//                     for (row_idx, opt_val) in str_col.iter().enumerate() {
//                         replace_string(
//                             opt_val.map(|s| s.to_string()), 
//                             row_idx + start_index + 1,
//                              col_idx + 1
//                         );
//                     }
//                 }
//             },
//             "datetime" => {
//                 // Get the time_unit from the schema if it's a datetime column
//                 let time_unit = if col.dtype().is_temporal() {
//                     match col.dtype() {
//                         DataType::Datetime(time_unit, _) => Some(*time_unit),
//                         _ => None
//                     }
//                 } else {
//                     None
//                 };

//                 if time_unit.is_none() {
//                     return Err(PolarsError::SchemaMismatch(ErrString::from(format!("No time unit specified for {}",&col_info.name))));
//                 }
//                 let time_unit_unwrapped = time_unit.unwrap();
                
//                 // Process each row based on the schema's time unit
//                 for row_idx in 0..col.len() {
//                     let value: Option<f64> = match col.get(row_idx) {
//                         Ok(AnyValue::Datetime(v, _, _)) => { 
//                             // Use the time_unit from the schema
//                             match time_unit_unwrapped {
//                                 TimeUnit::Nanoseconds => Some(v as f64 / 1_000_000.0 + (SEC_SHIFT_SAS_STATA as f64)*1000.0),
//                                 TimeUnit::Microseconds => Some(v as f64 / 1_000.0 + (SEC_SHIFT_SAS_STATA as f64)*1000.0),
//                                 TimeUnit::Milliseconds => Some(v as f64 + (SEC_SHIFT_SAS_STATA as f64)*1000.0),
//                             }
//                         },
//                         _ => None
//                     };

//                     replace_number(
//                         value, 
//                         (row_idx + start_index + 1) as usize, 
//                         (col_idx + 1) as usize
//                     );
//                 }
//             },
//             _ => {
//                 // Handle numeric types (including date/time which get converted to numeric)
//                 // Get the column's data type from the stored string representation
//                 let dtype_str = col_info.dtype.as_str();
//                 //  display(dtype_str);
                 
                
//                 for row_idx in 0..col.len() {
//                     let value: Option<f64> = match col.get(row_idx) {
//                         Ok(any_value) => match (dtype_str, any_value) {
//                             ("Boolean", AnyValue::Boolean(b)) => Some(if b { 1.0 } else { 0.0 }),
//                             ("Int8", AnyValue::Int8(v)) => Some(v as f64),
//                             ("Int16", AnyValue::Int16(v)) => Some(v as f64),
//                             ("Int32", AnyValue::Int32(v)) => Some(v as f64),
//                             ("Int64", AnyValue::Int64(v)) => Some(v as f64),
//                             ("UInt8", AnyValue::UInt8(v)) => Some(v as f64),
//                             ("UInt16", AnyValue::UInt16(v)) => Some(v as f64),
//                             ("UInt32", AnyValue::UInt32(v)) => Some(v as f64),
//                             ("UInt64", AnyValue::UInt64(v)) => Some(v as f64),
//                             ("Float32", AnyValue::Float32(v)) => Some(v as f64),
//                             ("Float64", AnyValue::Float64(v)) => Some(v),
//                             ("Date", AnyValue::Date(v)) => Some((v + DAY_SHIFT_SAS_STATA) as f64),
//                             ("Time", AnyValue::Time(v)) => {
//                                 //  display(&format!("TIME = {}",v));
//                                 Some((v/SEC_MICROSECOND) as f64)
//                             },
//                             _ => None
//                         },
//                         Err(_) => None
//                     };
//                     //  display(&format!("Assigning numeric {},{} = {:?}",row_idx+1,col_idx+1,value));
//                     replace_number(
//                         value, 
//                         (row_idx + start_index + 1) as usize, 
//                         (col_idx + 1) as usize
//                     );
//                 }
//             }
//         }
//     }
    
//     Ok(())
// }




fn get_thread_count() -> usize {
    // First try to get the thread count from POLARS_MAX_THREADS env var
    match env::var("POLARS_MAX_THREADS") {
        Ok(threads_str) => {
            // Try to parse the environment variable as a usize
            match threads_str.parse::<usize>() {
                Ok(threads) => threads,
                Err(_) => {
                    // If parsing fails, fall back to system thread count
                    thread::available_parallelism()
                        .map(|p| p.get())
                        .unwrap_or(1)
                }
            }
        },
        Err(_) => {
            // If environment variable is not set, use system thread count
            thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        }
    }
}

// /// Process a Polars LazyFrame in parallel chunks
// /// 
// /// - `lazy_df`: The LazyFrame to process
// /// - `chunk_size`: Size of each chunk to process
// /// - `process_chunk_fn`: Function that receives (offset, chunk_dataframe)
// fn process_dataframe_in_chunks<F>(
//     lazy_df: LazyFrame,
//     chunk_size: usize,
//     process_chunk_fn: F
// ) -> Result<(), PolarsError>
// where
//     F: Fn(usize, DataFrame) + Send + Sync + Clone + 'static,
// {
//     // Get total row count to determine chunks
//     let row_count = lazy_df.clone().collect()?.height();
//     let chunks = (row_count + chunk_size - 1) / chunk_size; // Ceiling division
    
//     // Create thread handles
//     let mut handles = vec![];
    
//     for chunk_idx in 0..chunks {
//         let offset = chunk_idx * chunk_size;
//         let limit = std::cmp::min(chunk_size, row_count - offset);
        
//         // Clone the necessary data for the thread
//         let chunk_lazy = lazy_df.clone()
//             .slice(offset as i64, limit as u32)
//             .cache(); // Cache to avoid recomputation
        
//         let process_fn = process_chunk_fn.clone();
        
//         // Spawn thread for this chunk
//         let handle = thread::spawn(move || {
//             // Materialize the chunk
//             match chunk_lazy.collect() {
//                 Ok(df) => {
//                     // Process this chunk with the provided function
//                     process_fn(offset, df);
//                 },
//                 Err(e) => eprintln!("Error processing chunk {}: {}", chunk_idx, e),
//             }
//         });
        
//         handles.push(handle);
//     }
    
//     // Wait for all threads to complete
//     for handle in handles {
//         handle.join().unwrap();
//     }
    
//     Ok(())
// }



// fn process_chunk(offset: usize, df: DataFrame) {
//     // Your implementation here
//     println!("Processing chunk starting at offset: {}", offset);
//     println!("Chunk size: {} rows", df.height());
    
//     // Example: Iterate through each row in the chunk
//     for row_idx in 0..df.height() {
//         let global_idx = offset + row_idx;
        
//         // Access row data (example using get method)
//         // let value = df.get(row_idx, "column_name");
        
//         // Do your thread-safe processing here
//         // For example, update some external data structure
//         // your_target_data.thread_safe_update(global_idx, value);
//     }
// }


