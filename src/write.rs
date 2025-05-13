use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use log::debug;
use polars::prelude::{self, NamedFrom, TimeUnit};
use polars::prelude::*;
use polars_sql::SQLContext;
use rayon::prelude::*;
use std::error::Error;
use std::collections::{
    HashMap,
    HashSet
};

use crate::stata_interface;
use crate::stata_interface::{
    display,
    get_macro
};
use crate::mapping::{self, StataColumnInfo};

use crate::utilities::{
    DAY_SHIFT_SAS_STATA,
    SEC_SHIFT_SAS_STATA,
    SEC_MILLISECOND,
    SEC_MICROSECOND,
    SEC_NANOSECOND,
    get_thread_count,
    ParallelizationStrategy,
    determine_parallelization_strategy,
};



pub fn write_from_stata(
    path:&str,
    variables_as_str:&str,
    n_rows:usize,
    offset:usize,
    sql_if:Option<&str>,
    mapping:&str,
    parallel_strategy:Option<ParallelizationStrategy>,
) -> Result<i32,Box<dyn Error>> {
    let variables_as_str = if variables_as_str == "" || variables_as_str == "from_macro" {
        &get_macro("varlist", false,  Some(1024 * 1024 * 10))
    } else {
        variables_as_str
    };

    let rename_list = get_rename_list();
    let all_columns: Vec<PlSmallStr> = variables_as_str.split_whitespace()
    .map(|s| {
        let s_small = PlSmallStr::from(s);

        match rename_list.get(&s_small) {
            Some(renamed) => renamed.clone(),   // Clone the PlSmallStr we found
            None => s_small                                  // Use the original PlSmallStr
        }
    })
    .collect();
    
    let column_info: Vec<StataColumnInfo>= if (mapping == "" || mapping == "from_macros") {
        //  display("Reading column info from macros");

        let n_vars_str = get_macro(&"var_count", false, None);
        let n_vars = match n_vars_str.parse::<usize>() {
            Ok(num) => num,
            Err(e) => {
                eprintln!("Failed to parse n_vars '{}' as usize: {}", n_vars_str, e);
                0
            }
        };

        //  display(&format!("from n = {}",n_vars));
        column_info_from_macros(
            n_vars,
            rename_list
        )
    } else {
        serde_json::from_str(mapping).unwrap()
    };
    //    println!("columns     = {:?}", all_columns);
    //    println!("column info = {:?}", column_info);

    // Convert Option<&str> to Option<String>
    let sql_if_owned = sql_if.map(|s| s.to_string());
    
    let a_scan = StataDataScan::new(
        column_info,
        all_columns,
        None,
        offset,
        n_rows,
        sql_if_owned,
        parallel_strategy
    );

    
    let a_scan_arc = Arc::new(a_scan);

    let lf = LazyFrame::anonymous_scan(
        a_scan_arc,
        ScanArgsAnonymous::default()
    );
    let lf_unwrapped = lf.unwrap();
    
    let sink_target = SinkTarget::Path(Arc::new(PathBuf::from(path)));
    match lf_unwrapped.sink_parquet(
        sink_target, 
        ParquetWriteOptions::default(),
         None,
        SinkOptions::default()).unwrap().collect() {
            Err(e) => {
                display(&format!("Parquet write error: {}", e));
                Ok(198)
            },
            Ok(_) => {
                display(&format!("File saved to {}", path));
                Ok(0)
            }
        }
}

fn get_rename_list() -> HashMap<PlSmallStr,PlSmallStr> {
    let mut rename_list = HashMap::<PlSmallStr,PlSmallStr>::new();
    let n_rename_str = get_macro(
        &"n_rename",
        false, 
        None,
    );

    let n_rename = match n_rename_str.parse::<usize>() {
        Ok(num) => num,
        Err(e) => {
            eprintln!("Failed to parse n_vars '{}' as usize: {}", n_rename_str, e);
            0
        }
    };

    for i in 1..(n_rename+1) {
        let rename_from  = get_macro(
            &format!("rename_from_{}",i),
            false,
            None
        );
        let rename_to  = get_macro(
            &format!("rename_to_{}",i),
            false,
            None
        );

        rename_list.insert(rename_from.into(), rename_to.into());
    }
    rename_list
}


fn column_info_from_macros(
    n_vars: usize,
    rename_list: HashMap<PlSmallStr,PlSmallStr>,
) -> Vec<StataColumnInfo> {
    let mut column_infos = Vec::with_capacity(n_vars);
    
    for i in 0..n_vars {
        let name = get_macro(&format!("name_{}", i+1), false, None);

        let name = match rename_list.get(&PlSmallStr::from(&name)) {
            Some(renamed) => renamed.to_string(),       // Change the name to the renamed value
            None => name.clone()                                     // Use the original value
        };


        let dtype = get_macro(&format!("dtype_{}", i+1), false, None);
        let format = get_macro(&format!("format_{}", i+1), false, None);
        let str_length_str = get_macro(&format!("str_length_{}", i+1), false, None);
        let str_length = match str_length_str.parse::<usize>() {
            Ok(num) => num,
            Err(e) => {
                eprintln!("Failed to parse n_vars '{}' as usize: {}", str_length_str, e);
                0
            }
        };
        
        column_infos.push(StataColumnInfo {
            name,
            dtype,
            format,
            str_length
        });
    }
    
    column_infos
}



// Define a trait for converting f64 to different types
trait FromStataValue<T> {
    fn from_stata_value(value: f64) -> T;
}

// Implementations for different types
impl FromStataValue<bool> for bool {
    fn from_stata_value(value: f64) -> bool {
        value > 0.0
    }
}

impl FromStataValue<i8> for i8 {
    fn from_stata_value(value: f64) -> i8 {
        value as i8
    }
}

impl FromStataValue<i16> for i16 {
    fn from_stata_value(value: f64) -> i16 {
        value as i16
    }
}

impl FromStataValue<i32> for i32 {
    fn from_stata_value(value: f64) -> i32 {
        value as i32
    }
}

impl FromStataValue<f32> for f32 {
    fn from_stata_value(value: f64) -> f32 {
        value as f32
    }
}

impl FromStataValue<f64> for f64 {
    fn from_stata_value(value: f64) -> f64 {
        value
    }
}

// Special case for datetime milliseconds
struct DatetimeProcess(i64);

impl FromStataValue<DatetimeProcess> for DatetimeProcess {
    fn from_stata_value(value: f64) -> DatetimeProcess {
        DatetimeProcess((value - (SEC_SHIFT_SAS_STATA as f64) * 1000.0) as i64)
    }
}

// Special case for time
struct TimeProcess(i64);

impl FromStataValue<TimeProcess> for TimeProcess {
    fn from_stata_value(value: f64) -> TimeProcess {
        TimeProcess((value as i64) * SEC_MICROSECOND)
    }
}

struct DateProcess(i32);

impl FromStataValue<DateProcess> for DateProcess {
    fn from_stata_value(value: f64) -> DateProcess {
        DateProcess((value as i32) - DAY_SHIFT_SAS_STATA)
    }
}

fn process_numeric_data<T>(
    col_idx: usize,
    n_rows_to_read: usize,
    offset: usize,
    parallelize_rows: bool,
) -> Vec<Option<T>>
where
    T: Send + Sync + FromStataValue<T>,
{
    if parallelize_rows {
        // Process rows in parallel
        (0..n_rows_to_read)
            .into_par_iter()
            .map(|row_idx| {
                let row = offset + row_idx + 1;
                match stata_interface::read_numeric(col_idx + 1, row) {
                    Some(value) => Some(T::from_stata_value(value)),
                    None => None,
                }
            })
            .collect()
    } else {
        // Process rows sequentially
        (0..n_rows_to_read)
            .map(|row_idx| {
                let row = offset + row_idx + 1;
                match stata_interface::read_numeric(col_idx + 1, row) {
                    Some(value) => Some(T::from_stata_value(value)),
                    None => None,
                }
            })
            .collect()
    }
}








pub struct StataDataScan {
    current_offset: Arc<Mutex<usize>>,
    n_rows: usize,
    batch_size: usize,
    schema: Schema,
    column_info:Vec<mapping::StataColumnInfo>,
    all_columns:Vec<PlSmallStr>,
    sql_if:Option<String>,
    parallel_strategy:Option<ParallelizationStrategy>,
}


impl StataDataScan {
    pub fn new(
        column_info: Vec<mapping::StataColumnInfo>,
        all_columns: Vec<PlSmallStr>,
        batch_size: Option<usize>,
        initial_offset: usize,
        n_rows: usize,
        sql_if: Option<String>,
        parallel_strategy:Option<ParallelizationStrategy>,
    ) -> Self {
        let rows_to_read = if (n_rows > 0) {
            n_rows
        } else {
            stata_interface::n_obs() as usize
        };

        
        StataDataScan {
            current_offset: Arc::new(Mutex::new(initial_offset)),
            n_rows: rows_to_read,
            batch_size: batch_size.unwrap_or(10_000_000),
            schema: mapping::stata_column_info_to_schema(&column_info),
            column_info: column_info,
            all_columns: all_columns,
            sql_if: sql_if,
            parallel_strategy:parallel_strategy,
        }
    }
    
    pub fn get_current_offset(&self) -> usize {
        *self.current_offset.lock().unwrap()
    }
}

impl AnonymousScan for StataDataScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    

    fn schema(
        &self,
        _infer_schema_length: Option<usize>,
    ) -> Result<Arc<Schema>, PolarsError> {
        Ok(self.schema.clone().into())
    }

    #[allow(unused)]
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        // Reset current offset
        let mut offset = self.current_offset.lock().unwrap();
    
        // If no data, return an empty DataFrame
        if self.n_rows == 0 {
            return Ok(DataFrame::empty_with_schema(&self.schema));
        }
        
        // Update the offset for the next batch
        *offset += self.n_rows;

        // Call read_single_batch and handle errors with the ? operator
        let result = read_single_batch(
            self, 
            scan_opts,
            0,
            self.n_rows,
            self.parallel_strategy
        )?;

        // Now handle the Option<DataFrame>
        match result {
            Some(df) => Ok(df),
            None => Ok(DataFrame::empty_with_schema(&self.schema))
        }
    }
    
    #[allow(unused)]
    fn next_batch(
        &self,
        scan_opts: AnonymousScanArgs,
    ) -> PolarsResult<Option<DataFrame>> {
        let mut offset = self.current_offset.lock().unwrap();
    
        // If we've read all rows, return empty DataFrame
        if *offset >= self.n_rows {
            return Ok(None);
        }

        let initial_offset = offset.clone();

        // Update the offset for the next batch
        *offset += self.batch_size;

        
        read_single_batch(
            self, 
            scan_opts,
            initial_offset,
            self.batch_size,
            self.parallel_strategy)
    }
    
    fn allows_predicate_pushdown(&self) -> bool {
        false
    }
    fn allows_projection_pushdown(&self) -> bool {
        false
    }
    fn allows_slice_pushdown(&self) -> bool {
        false
    }
}


// Now the refactored process_column function would look like:
fn process_column(
    col_idx: usize,
    col_name: &PlSmallStr,
    n_rows_to_read: usize,
    offset: usize,
    parallelize_rows: bool,
    schema: &Schema,
    column_info: &Vec<mapping::StataColumnInfo>,
) -> PolarsResult<Option<Series>> {
    let dtype = match schema.get_field(col_name.as_str()) {
        Some(field) => field.dtype().clone(),
        None => {
            display(&format!("{} not getting saved", col_name));
            return Ok(None);
        }
    };

    // Create appropriate Series based on data type
    let series = match dtype {
        DataType::String => {
            let str_length = mapping::find_str_length_by_name(column_info, col_name).unwrap_or(0);
            let values: Vec<String> = if parallelize_rows {
                // Process rows in parallel
                (0..n_rows_to_read)
                    .into_par_iter()
                    .map(|row_idx| {
                        let row = offset + row_idx + 1;
                        stata_interface::read_string(col_idx + 1, row, str_length)
                    })
                    .collect()
            } else {
                // Process rows sequentially
                (0..n_rows_to_read)
                    .map(|row_idx| {
                        let row = offset + row_idx + 1;
                        stata_interface::read_string(col_idx + 1, row, str_length)
                    })
                    .collect()
            };
            Series::new(col_name.clone(), values)
        }
        DataType::Boolean => {
            let values = process_numeric_data::<bool>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Int8 => {
            let values = process_numeric_data::<i8>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Int16 => {
            let values = process_numeric_data::<i16>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Int32 => {
            let values = process_numeric_data::<i32>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Float32 => {
            let values = process_numeric_data::<f32>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Float64 => {
            let values = process_numeric_data::<f64>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Datetime(TimeUnit::Milliseconds, _) => {
            let values = process_numeric_data::<DatetimeProcess>(col_idx, n_rows_to_read, offset, parallelize_rows);
            // Convert the DatetimeProcess wrapper to i64 values
            let i64_values: Vec<Option<i64>> = values.into_iter().map(|opt| opt.map(|dm| dm.0)).collect();
            Series::new(col_name.clone(), i64_values).cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?
        }
        DataType::Time => {
            let values = process_numeric_data::<TimeProcess>(col_idx, n_rows_to_read, offset, parallelize_rows);
            // Convert the TimeProcess wrapper to i64 values
            let i64_values: Vec<Option<i64>> = values.into_iter().map(|opt| opt.map(|tm| tm.0)).collect();
            Series::new(col_name.clone(), i64_values).cast(&DataType::Time)?
        }
        DataType::Date => {
            let values = process_numeric_data::<DateProcess>(col_idx, n_rows_to_read, offset, parallelize_rows);
            // Convert the DateProcess wrapper to i32 values
            let i32_values: Vec<Option<i32>> = values.into_iter().map(|opt| opt.map(|dv| dv.0)).collect();
            Series::new(col_name.clone(), i32_values).cast(&DataType::Date)?
        }
        // Add more data types as needed
        _ => {
            return Err(PolarsError::ComputeError(
                format!("Unsupported data type: {:?}", dtype).into(),
            ))
        }
    };

    Ok(Some(series))
}

fn read_single_batch(
    sds: &StataDataScan,
    scan_opts: AnonymousScanArgs,
    offset: usize,
    n_rows: usize,
    parallel_strategy: Option<ParallelizationStrategy>,
) -> PolarsResult<Option<DataFrame>> {
    // Calculate how many rows to read in this batch
    let rows_remaining = sds.n_rows - offset;
    let n_rows_to_read = std::cmp::min(n_rows, rows_remaining);
    
    // Configure thread pool
    let n_threads = if n_rows_to_read < 100_000 {
        1 as usize
    } else {
        get_thread_count()
    };

    let strategy = parallel_strategy.unwrap_or_else(|| {
        determine_parallelization_strategy(
            sds.schema.len(),
            n_rows_to_read,
            n_threads
        )
    });
    
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to build thread pool: {}", e).into()))?;
    
    // Apply the strategy
    let columns_result: PolarsResult<Vec<Series>> = match strategy {
        ParallelizationStrategy::ByColumn => {
            // Process columns in parallel
            thread_pool.install(|| {
                sds.all_columns.par_iter().enumerate()
                    .map(|(col_idx, col_name)| {
                        match process_column(col_idx, col_name, n_rows_to_read, offset, false, &sds.schema, &sds.column_info)? {
                            Some(series) => Ok(series),
                            None => Err(PolarsError::ComputeError(
                                format!("Failed to process column: {}", col_name).into(),
                            ))
                        }
                    })
                    .collect()
            })
        },
        ParallelizationStrategy::ByRow => {
            // Process columns sequentially, but rows in parallel
            sds.all_columns.iter().enumerate()
                .map(|(col_idx, col_name)| {
                    match process_column(col_idx, col_name, n_rows_to_read, offset, true, &sds.schema, &sds.column_info)? {
                        Some(series) => Ok(series),
                        None => Err(PolarsError::ComputeError(
                            format!("Failed to process column: {}", col_name).into(),
                        ))
                    }
                })
                .collect()
        }
    };
    
    let columns = columns_result?;
    
    // Return the DataFrame built from columns
    let mut df = DataFrame::from_iter(columns).lazy();
    
    if let Some(sql_if) = &sds.sql_if {
        if !sql_if.is_empty() {
            let mut ctx = SQLContext::new();
            ctx.register("df", df);

            df = ctx.execute(&format!("select * from df where {}", sql_if))
                .map_err(|e| {
                    display(&format!("Error in SQL if statement: {}", e));
                    e
                })?;
        }
    }
    
    Ok(Some(df.collect()?))
}
