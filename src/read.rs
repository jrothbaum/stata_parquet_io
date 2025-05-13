use std::path::Path;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use rayon::prelude::*;
use polars::error::ErrString;
use polars::prelude::*;
use polars_sql::SQLContext;
use polars::datatypes::{AnyValue, TimeUnit};
use std::error::Error;
use serde_json;

use crate::mapping::ColumnInfo;
use crate::stata_interface::{
    display,
    set_macro,
    replace_number,
    replace_string,
    get_macro
};

use crate::utilities::{
    determine_parallelization_strategy, get_thread_count, ParallelizationStrategy, DAY_SHIFT_SAS_STATA, SEC_MICROSECOND, SEC_MILLISECOND, SEC_NANOSECOND, SEC_SHIFT_SAS_STATA
};

// Trait for converting Polars values to Stata values
trait ToStataValue {
    fn to_stata_value(&self) -> Option<f64>;
}

// Implementations for different types
impl ToStataValue for bool {
    fn to_stata_value(&self) -> Option<f64> {
        Some(if *self { 1.0 } else { 0.0 })
    }
}

impl ToStataValue for i8 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for i16 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for i32 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for i64 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u8 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u16 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u32 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u64 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for f32 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for f64 {
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self)
    }
}

// Special type for handling dates
struct DateValue(i32);

impl ToStataValue for DateValue {
    fn to_stata_value(&self) -> Option<f64> {
        Some((self.0 + DAY_SHIFT_SAS_STATA) as f64)
    }
}

// Special type for handling time
struct TimeValue(i64);

impl ToStataValue for TimeValue {
    fn to_stata_value(&self) -> Option<f64> {
        Some((self.0 / SEC_MICROSECOND) as f64)
    }
}


// Special type for handling datetime
struct DatetimeValue(i64, TimeUnit);

impl ToStataValue for DatetimeValue {
    fn to_stata_value(&self) -> Option<f64> {
        let mills_factor = match self.1 {
            TimeUnit::Nanoseconds => (SEC_NANOSECOND/SEC_MILLISECOND) as f64,
            TimeUnit::Microseconds => (SEC_MICROSECOND/SEC_MILLISECOND) as f64,
            TimeUnit::Milliseconds => 1.0,
        };
        
        Some(self.0 as f64 / mills_factor + (SEC_SHIFT_SAS_STATA as f64) * (SEC_MILLISECOND as f64))
    }
}

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
    path: &str,
    variables_as_str: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    mapping: &str,
    parallel_strategy: Option<ParallelizationStrategy>,
) -> Result<i32, Box<dyn Error>> {
    // Handle empty variable list by getting from macros
    let variables_as_str = if variables_as_str.is_empty() || variables_as_str == "from_macro" {
        &get_macro("matched_vars", false, Some(1024 * 1024 * 10))
    } else {
        variables_as_str
    };

    // Get column info either from mapping or macros
    let all_columns: Vec<ColumnInfo> = if mapping.is_empty() || mapping == "from_macros" {
        let n_vars_str = get_macro("n_vars", false, None);
        let n_vars = match n_vars_str.parse::<usize>() {
            Ok(num) => num,
            Err(e) => {
                eprintln!("Failed to parse n_vars '{}' as usize: {}", n_vars_str, e);
                0
            }
        };
        column_info_from_macros(n_vars)
    } else {
        serde_json::from_str(mapping).unwrap()
    };

    //  display(&format!("Column information: {:?}", all_columns));

    // Scan the parquet file to get a LazyFrame
    let mut df = match scan_lazyframe(path) {
        Ok(df) => df,
        Err(e) => {
            display(&format!("Error scanning lazyframe: {:?}", e));
            return Ok(198);
        },
    };

    // Cast categorical columns to string
    df = cast_catenum_to_string(&df).unwrap();

    // Apply SQL filter if provided
    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", df);
        
        df = match ctx.execute(&format!("select * from df where {}", sql)) {
            Ok(lazyframe) => lazyframe,
            Err(e) => {
                display(&format!("Error in SQL if statement: {}", e));
                return Ok(198);
            }
        };
    }

    // Create column expressions from the provided variable list
    let columns: Vec<Expr> = variables_as_str.split_whitespace()
        .map(|s| col(s))
        .collect();

    // Configure batch processing
    let batch_size: usize = 1_000_000;
    let n_batches = (n_rows as f64 / batch_size as f64).ceil() as usize;

    // Determine thread count based on data size
    let n_threads = if n_rows < 1_000 {
        1
    } else {
        get_thread_count()
    };
    
    let strategy = parallel_strategy.unwrap_or_else(|| {
        determine_parallelization_strategy(
            columns.len(),
            n_rows,
            n_threads
        )
    });
    
    //  display(&format!("Processing with strategy: {:?}, threads: {}", strategy, n_threads));
    
    // Process data in batches
    set_macro("n_batches", &n_batches.to_string(), false);
    for batchi in 0..n_batches {
        let mut df_batch = df.clone();

        let batch_offseti = offset + batchi * batch_size;

        let batch_lengthi = if (batchi + 1) * batch_size > n_rows {
            n_rows - batchi * batch_size
        } else {
            batch_size
        } as u32;
        
        // display("");
        // display(&format!("Batch #{}", batchi + 1));
        // display(&format!("Batch start: {}", batch_offseti));
        // display(&format!("Batch length: {}", batch_lengthi));
        
        // Apply slice to get current batch
        df_batch = df_batch.slice(
            batch_offseti as i64, 
            batch_lengthi
        );

        // Collect the batch to a DataFrame
        let batch_df = match df_batch.collect() {
            Ok(df) => df,
            Err(e) => {
                display(&format!("Error collecting batch: {:?}", e));
                return Ok(198);
            }
        };

        // Process the batch with the selected parallelization strategy
        match process_batch_with_strategy(
            &batch_df, 
            batch_offseti, 
            &all_columns,
            strategy,
            n_threads,
            batchi,
        ) {
            Ok(_) => {
                // Do nothing on success
            },
            Err(e) => {
                display(&format!("Error assigning values to stata data: {:?}", e));
                return Ok(198);
            },
        };
    }

    Ok(0)
}


// To cast all categorical columns to string:
fn cast_catenum_to_string(lf: &LazyFrame) -> Result<LazyFrame, PolarsError> {
    // Collect the schema from the LazyFrame
    let mut lf_internal = lf.to_owned();
    let schema = lf_internal.collect_schema()?;
    
    // Identify categorical columns from the schema
    let cat_expressions: Vec<Expr> = schema.iter()
        .filter_map(|(name, dtype)| {
            if matches!(dtype, DataType::Categorical(_, _) | DataType::Enum(_, _)) {
                Some(col(name.clone()).cast(DataType::String))
            } else {
                None
            }
        })
        .collect();
    
    // If there are categorical columns, apply the transformations
    if !cat_expressions.is_empty() {
        Ok(lf_internal.with_columns(cat_expressions))
    } else {
        // If no categorical columns found, return the original LazyFrame
        Ok(lf_internal)
    }
}


// Create column info from Stata macros
fn column_info_from_macros(n_vars: usize) -> Vec<ColumnInfo> {
    let mut column_infos = Vec::with_capacity(n_vars);
    
    for i in 0..n_vars {
        let name = get_macro(&format!("name_{}", i+1), false, None);
        let dtype = get_macro(&format!("polars_type_{}", i+1), false, None);
        let stata_type = get_macro(&format!("type_{}", i+1), false, None);
        
        column_infos.push(ColumnInfo {
            name,
            dtype,
            stata_type,
        });
    }
    
    column_infos
}

// Main entry point that delegates to appropriate strategy
fn process_batch_with_strategy(
    batch: &DataFrame,
    start_index: usize,
    all_columns: &Vec<ColumnInfo>,
    strategy: ParallelizationStrategy,
    n_threads: usize,
    n_batch:usize,
) -> PolarsResult<()> {

    // If only 1 thread requested or batch is too small, use single-threaded version
    let row_count = batch.height();
    let min_multithreaded = 10000;
    
    if n_threads <= 1 || row_count < min_multithreaded {
        return process_batch_single_thread(batch, start_index, all_columns);
    }

    // Partition columns into special (strl/binary) and regular columns
    let (special_columns, regular_columns): (Vec<_>, Vec<_>) = all_columns.iter().enumerate()
        .partition(|(_, col_info)| {
            col_info.stata_type == "strl" || col_info.stata_type == "binary"
        });

    
    // Setup thread pool with rayon
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .map_err(|e| PolarsError::ComputeError(
            ErrString::from(format!("Failed to build thread pool: {}", e))
        ))?;

    pool.install(|| {
        // First, process regular columns with the chosen strategy
        if !regular_columns.is_empty() {
            // Create a vector of regular ColumnInfo objects
            let regular_column_infos: Vec<ColumnInfo> = regular_columns.iter()
                .map(|(_, col_info)| (*col_info).clone())
                .collect();
            
            match strategy {
                ParallelizationStrategy::ByRow => {
                    // Process regular columns by row
                    process_regular_by_row(batch, start_index, &regular_column_infos)?;
                },
                ParallelizationStrategy::ByColumn => {
                    // Process regular columns by column
                    process_regular_by_column(batch, start_index, &regular_column_infos)?;
                }
            }
        }

        // if !special_columns.is_empty() {
        //     display(&format!("Cannot process strL or binary columns: {:?}",special_columns));
        // }
         // Then, process special columns (strl/binary) in parallel threads but with sequential row processing
        if !special_columns.is_empty() {
            special_columns.into_par_iter()
                .try_for_each(|(col_idx, col_info)| {
                    //  let col = batch.column(&col_info.name)?;
                    
                    // Process special column sequentially by row
                    match col_info.stata_type.as_str() {
                        "strl" => {
                            process_strl_column(
                                batch,
                                &PlSmallStr::from(&col_info.name),
                                0 as usize,
                                batch.height(),
                                start_index,
                                n_batch + 1,
                                col_idx+1,
                            )
                        },
                        // "binary" => {
                        //     process_binary_column(col, 0, batch.height(), start_index, col_idx + 1)
                        // },
                        _ => {
                            // Should never get here due to partition filter
                            Ok(())
                        }
                    }
                })?;
        }
        
        Ok(())
    })
}


// Process regular columns with row-wise parallelization
fn process_regular_by_row(
    batch: &DataFrame,
    start_index: usize,
    columns: &Vec<ColumnInfo>
) -> PolarsResult<()> {
    let row_count = batch.height();
    
    // Calculate chunk size for processing
    let chunk_size = std::cmp::max(100, row_count / (rayon::current_num_threads() * 4));
    
    // Create chunks of row ranges and process them in parallel
    (0..row_count).into_par_iter()
        .chunks(chunk_size)
        .try_for_each(|chunk| {
            // Get the start and end row for this chunk
            let start_row = chunk[0];
            let end_row = chunk[chunk.len() - 1] + 1;
            
            // Process this range of rows for regular columns
            process_row_range(batch, start_index, start_row, end_row, columns)
        })
}

// Process regular columns with column-wise parallelization
fn process_regular_by_column(
    batch: &DataFrame,
    start_index: usize,
    columns: &Vec<ColumnInfo>
) -> PolarsResult<()> {
    // Process columns in parallel
    columns.par_iter().enumerate()
        .try_for_each(|(col_idx, col_info)| {
            // Get the column by name
            let col = match batch.column(&col_info.name) {
                Ok(c) => c,
                Err(e) => return Err(e)
            };
            
            // Process regular column based on its type
            match col_info.stata_type.as_str() {
                "string" => {
                    // Handle string types
                    if let Ok(str_col) = col.str() {
                        for row_idx in 0..batch.height() {
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
                    Ok(())
                },
                "datetime" => {
                    // Process datetime with appropriate time unit
                    process_datetime_column(col, 0, batch.height(), start_index, col_idx + 1)
                },
                _ => {
                    // Handle numeric types
                    process_numeric_column(col, col_info, 0, batch.height(), start_index, col_idx + 1)
                }
            }
        })
}


// Single-threaded implementation (fallback)
fn process_batch_single_thread(
    batch: &DataFrame,
    start_index: usize,
    all_columns: &Vec<ColumnInfo>
) -> PolarsResult<()> {
    // Process all rows for all columns in a single thread
    set_macro("n_batches", "1", false);

    let (special_columns, regular_columns): (Vec<_>, Vec<_>) = all_columns.iter().enumerate()
        .partition(|(_, col_info)| {
            col_info.stata_type == "strl" || col_info.stata_type == "binary"
        });
        
    let regular_column_infos: Vec<ColumnInfo> = regular_columns.iter()
                .map(|(_, col_info)| (*col_info).clone())
                .collect();
    let regular_process_out = process_row_range(batch, start_index, 0, batch.height(), &regular_column_infos);



    if !special_columns.is_empty() {
        special_columns.iter()
            .try_for_each(|(col_idx, col_info)| {
                //  let col = batch.column(&col_info.name)?;
                
                // Process special column sequentially by row
                match col_info.stata_type.as_str() {
                    "strl" => {
                        // Process and propagate any error
                        process_strl_column(
                            batch,
                            &PlSmallStr::from(&col_info.name),
                            0 as usize,
                            batch.height(),
                            start_index,
                            1 as usize,
                            col_idx + 1,
                        )
                    },
                    // "binary" => {
                    //     process_binary_column(col, 0, batch.height(), start_index, col_idx + 1)
                    // },
                    _ => {
                        // Should never get here due to partition filter
                        Ok(())
                    }
                }
            })?;
    }

    regular_process_out
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
                //  Do nothing, handled elsewhwere
                // let message= format!("Strl assignment not implemented yet: {}",col.name());
                // return Err(PolarsError::SchemaMismatch(
                //     ErrString::from(message)
                // ));                
            },
            "binary" => {
                let message= format!("Binary assignment not implemented yet: {}",col.name());
                return Err(PolarsError::SchemaMismatch(
                    ErrString::from(message)
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
                // Process datetime with appropriate time unit
                process_datetime_column(col, start_row, end_row, start_index, col_idx + 1)?;
            },
            _ => {
                // Handle numeric types (including date/time which get converted to numeric)
                process_numeric_column(col, col_info, start_row, end_row, start_index, col_idx + 1)?;
            }
        }
    }
    
    Ok(())
}

// Process a single column (for column-wise parallelization)
fn process_single_column(
    col: &Column,
    col_info: &ColumnInfo,
    start_index: usize,
    col_idx: usize
) -> PolarsResult<()> {
    let row_count = col.len();
    
    // Process each value in the column based on its Stata type
    match col_info.stata_type.as_str() {
        "strl" => {
            //  Do nothing, handled elsewhwere
            // return Err(PolarsError::SchemaMismatch(
            //     ErrString::from("Strl assignment not implemented yet")
            // ));                
        },
        "binary" => {
            return Err(PolarsError::SchemaMismatch(
                ErrString::from("Binary assignment not implemented yet")
            ));                
        },
        "string" => {
            // Handle string types
            if let Ok(str_col) = col.str() {
                for row_idx in 0..row_count {
                    let global_row_idx = row_idx + start_index;

                    // Get the string value at this row position
                    let opt_val = match str_col.get(row_idx) {
                        Some(s) => Some(s.to_string()),
                        None => None
                    };
                    
                    replace_string(
                        opt_val, 
                        global_row_idx + 1, // +1 because replace_string expects 1-indexed
                        col_idx             // col_idx is already 1-indexed
                    );
                }
            }
        },
        "datetime" => {
            // Process datetime with appropriate time unit
            process_datetime_column(col, 0, row_count, start_index, col_idx)?;
        },
        _ => {
            // Handle numeric types (including date/time which get converted to numeric)
            process_numeric_column(col, col_info, 0, row_count, start_index, col_idx)?;
        }
    }
    
    Ok(())
}

// Helper function to process datetime columns
fn process_datetime_column(
    col: &Column,
    start_row: usize,
    end_row: usize,
    start_index: usize,
    col_idx: usize
) -> PolarsResult<()> {
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
            ErrString::from(format!("No time unit specified for column {}", col.name()))
        ));
    }
    
    let time_unit_unwrapped = time_unit.unwrap();
    
    // Process each row based on the schema's time unit
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        let value: Option<f64> = match col.get(row_idx) {
            Ok(AnyValue::Datetime(v, _, _)) => { 
                // Convert to Stata datetime value using trait
                let datetime = DatetimeValue(v, time_unit_unwrapped);
                DatetimeValue::to_stata_value(&datetime)
            },
            _ => None
        };

        replace_number(
            value, 
            global_row_idx + 1,  // +1 because replace functions expect 1-indexed
            col_idx              // col_idx is already 1-indexed
        );
    }
    
    Ok(())
}

// Helper function to process numeric columns with appropriate type conversion
fn process_numeric_column(
    col: &Column,
    col_info: &ColumnInfo,
    start_row: usize,
    end_row: usize,
    start_index: usize,
    col_idx: usize
) -> PolarsResult<()> {
        // Get the column's data type from the stored string representation
    let dtype_str = col_info.dtype.as_str();
    
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        
        let value: Option<f64> = match col.get(row_idx) {
            Ok(any_value) => match (dtype_str, any_value) {
                ("Boolean", AnyValue::Boolean(b)) => b.to_stata_value(),
                ("Int8", AnyValue::Int8(v)) => v.to_stata_value(),
                ("Int16", AnyValue::Int16(v)) => v.to_stata_value(),
                ("Int32", AnyValue::Int32(v)) => v.to_stata_value(),
                ("Int64", AnyValue::Int64(v)) => v.to_stata_value(),
                ("UInt8", AnyValue::UInt8(v)) => v.to_stata_value(),
                ("UInt16", AnyValue::UInt16(v)) => v.to_stata_value(),
                ("UInt32", AnyValue::UInt32(v)) => v.to_stata_value(),
                ("UInt64", AnyValue::UInt64(v)) => v.to_stata_value(),
                ("Float32", AnyValue::Float32(v)) => v.to_stata_value(),
                ("Float64", AnyValue::Float64(v)) => v.to_stata_value(),
                ("Date", AnyValue::Date(v)) => DateValue(v).to_stata_value(),
                ("Time", AnyValue::Time(v)) => TimeValue(v).to_stata_value(),
                _ => None
            },
            Err(_) => None
        };
        
        replace_number(
            value, 
            global_row_idx + 1,  // +1 because replace functions expect 1-indexed
            col_idx              // col_idx is already 1-indexed
        );
    }
    
    Ok(())
}



fn process_strl_column(
    batch:&DataFrame,
    column_name: &PlSmallStr,
    start_row: usize,
    end_row: usize,
    start_index: usize,
    n_batch:usize,
    col_idx:usize,
) -> PolarsResult<()> {
    
    let path_stub = get_macro(
        &"temp_strl_stub",
        false,
        None
    );

    let path = format!(
        "{}_{}_{}.csv",
        path_stub,
        col_idx,
        n_batch,
    );
    
    set_macro(
        &format!(
            "strl_path_{}_{}",
            col_idx,
            n_batch
        ),
        &path,
        false,
    );
    set_macro(
        &format!(
            "strl_name_{}_{}",
            col_idx,
            n_batch
        ),
        &column_name,
        false,
    );
    set_macro(
        &format!(
            "strl_start_{}_{}",
            col_idx,
            n_batch
        ),
        &(start_row + start_index).to_string(),
        false,
    );

    set_macro(
        &format!(
            "strl_end_{}_{}",
            col_idx,
            n_batch
        ),
        &(end_row + start_index).to_string(),
        false,
    );
    let sink_target = SinkTarget::Path(Arc::new(PathBuf::from(path)));
    let mut csv_options = CsvWriterOptions::default();
    csv_options.include_header = false;
    match batch.select([&column_name.to_string()])
                                                  .unwrap()
                                                  .lazy()
                                                  .sink_csv(
                                                    sink_target,
                                                    csv_options,
                                                    None,
                                                    SinkOptions::default()
                                                  )
                                                  .unwrap()
                                                  .collect() {
            Err(e) => {
                display(&format!("Strl csv write error for {}: {}", column_name, e));
                Err(e)
            },
            Ok(_) => {
                Ok(())
            }
        }
}