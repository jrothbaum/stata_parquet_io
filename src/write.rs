use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use log::debug;
use polars::prelude::{self, NamedFrom, TimeUnit};
use polars::prelude::*;
use polars_sql::SQLContext;
use rayon::prelude::*;
use std::error::Error;


use crate::stata_interface;
use crate::stata_interface::display;
use crate::mapping::{self, StataColumnInfo};

use crate::read::{
    SEC_SHIFT_SAS_STATA,
    DAY_SHIFT_SAS_STATA,
    SEC_MICROSECOND,
    get_thread_count
};

pub fn write_from_stata(
    path:&str,
    variables_as_str:&str,
    n_rows:usize,
    offset:usize,
    sql_if:Option<&str>,
    mapping:&str,
) -> Result<i32,Box<dyn Error>> {
    let all_columns: Vec<PlSmallStr> = variables_as_str.split_whitespace()
        .map(|s| PlSmallStr::from(s))
        .collect();
    let column_info: Vec<StataColumnInfo> = serde_json::from_str(mapping).unwrap();

    // println!("columns = {:?}", all_columns);
    // println!("columns = {:?}", column_info);

    // Convert Option<&str> to Option<String>
    let sql_if_owned = sql_if.map(|s| s.to_string());

    let a_scan = StataDataScan::new(
        column_info,
        all_columns,
        None,
        offset,
        n_rows,
        sql_if_owned,
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


pub struct StataDataScan {
    current_offset: Arc<Mutex<usize>>,
    n_rows: usize,
    batch_size: usize,
    schema: Schema,
    column_info:Vec<mapping::StataColumnInfo>,
    all_columns:Vec<PlSmallStr>,
    sql_if:Option<String>,
}


impl StataDataScan {
    pub fn new(
        column_info: Vec<mapping::StataColumnInfo>,
        all_columns: Vec<PlSmallStr>,
        batch_size: Option<usize>,
        initial_offset: usize,
        n_rows: usize,
        sql_if: Option<String>
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
            schema: mapping::StataColumnInfoToSchema(&column_info),
            column_info: column_info,
            all_columns: all_columns,
            sql_if: sql_if,
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
            self.n_rows
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
            self.batch_size)
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



fn read_single_batch(
    sds: &StataDataScan,
    scan_opts: AnonymousScanArgs,
    offset: usize,
    n_rows: usize
) -> PolarsResult<Option<DataFrame>> {
    // Get the current offset and calculate how many rows to read
    
    
    // Calculate how many rows to read in this batch
    let rows_remaining = sds.n_rows - offset;
    let n_rows_to_read = std::cmp::min(n_rows, rows_remaining);
    
    // Prepare vectors to store column data
    let mut columns: Vec<Series> = Vec::with_capacity(sds.schema.len());
    
    // Configure thread pool
    let n_threads = if n_rows_to_read < 1_000 {
        1 as usize
    } else {
        get_thread_count()
    };
    
    display(&format!("threads = {}", n_threads));
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap();
    
    
    
    // Process each column in the schema
    for (col_idx, col_name) in sds.all_columns.iter().enumerate() {
        let dtype = sds.schema.get_field(col_name.as_str()).map(|field| field.dtype().clone());
        
        match dtype {
            Some(dtype) => {
                // Create appropriate Series based on data type
                let series = match dtype {
                    DataType::String => {
                        // Process strings in parallel
                        let str_length = mapping::find_str_length_by_name(
                            &sds.column_info, 
                            &col_name
                        ).unwrap_or(0);


                        let string_values: Vec<String> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    stata_interface::read_string(col_idx+1, row,str_length)
                                })
                                .collect()
                        });

                        Series::new(col_name.clone(), string_values)
                    },
                    DataType::Boolean => {
                        // Process boolean values in parallel
                        let bool_values: Vec<Option<bool>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some(value > 0.0),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), bool_values)
                    },
                    DataType::Int8 => {
                        //  display(&format!("Reading byte"));
                        // Process integers in parallel
                        let int_values: Vec<Option<i8>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    // Handle the Option and convert f64 to i8
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some(value as i8),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), int_values)
                    },
                    DataType::Int16 => {
                        //  display(&format!("Reading int"));
                        // Process integers in parallel
                        let int_values: Vec<Option<i16>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    // Handle the Option and convert f64 to i16
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some(value as i16),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), int_values)
                    },
                    DataType::Int32 => {
                        // display(&format!("Reading long"));
                        // Process integers in parallel
                        let int_values: Vec<Option<i32>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    // Handle the Option and convert f64 to i32
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some(value as i32),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), int_values)
                    },
                    DataType::Float32 => {
                        //  display(&format!("Reading float"));
                        // Process floating point values in parallel
                        let float_values: Vec<Option<f32>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some(value as f32),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), float_values)
                    },
                    DataType::Float64 => {
                        // Process floating point values in parallel
                        // display(&format!("Reading double"));
                        let float_values: Vec<Option<f64>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    // display(&format!("Row {}, column {}",row,col_idx+1));
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => {
                                            // display(&format!("  value = {:?}",stata_interface::read_numeric(col_idx+1, row)));
                                            Some(value)
                                        },
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), float_values)
                    },
                    DataType::Datetime(TimeUnit::Milliseconds,_) => {
                        // Process floating point values in parallel
                        let float_values: Vec<Option<i64>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some((value - (SEC_SHIFT_SAS_STATA as f64)*1000.0)  as i64),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), float_values).cast(&DataType::Datetime(TimeUnit::Milliseconds,None)).unwrap()
                    },
                    DataType::Time => {
                        // Process floating point values in parallel
                        let float_values: Vec<Option<i64>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some((value  as i64)*SEC_MICROSECOND),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), float_values).cast(&DataType::Time).unwrap()
                    },
                    
                    DataType::Date => {
                        // Process floating point values in parallel
                        let float_values: Vec<Option<i32>> = thread_pool.install(|| {
                            (0..n_rows_to_read)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let row = offset + row_idx + 1;
                                    match stata_interface::read_numeric(col_idx+1, row) {
                                        Some(value) => Some((value as i32 - DAY_SHIFT_SAS_STATA)),
                                        None => None
                                    }
                                })
                                .collect()
                        });
                        Series::new(col_name.clone(), float_values).cast(&DataType::Date).unwrap()
                    },
                    // Add more data types as needed
                    _ => return Err(PolarsError::ComputeError(
                        format!("Unsupported data type: {:?}", dtype).into(),
                    )),
                };
                
                columns.push(series);
            },
            None => {
                display(&format!("{} not getting saved",col_name));
            }
        }
    }
    
    
    // Return the DataFrame built from columns
    let mut df = DataFrame::from_iter(columns).lazy();
    
    if sds.sql_if.is_some() {
        if !sds.sql_if.as_ref().unwrap().is_empty() {
            let mut ctx = SQLContext::new();
            ctx.register("df", df);

            df = match ctx.execute(&format!("select * from df where {}",sds.sql_if.as_ref().unwrap())) {
                Ok(lazyframe) => lazyframe,
                Err(e) => {
                    display(&format!("Error in SQL if statement: {}", e));
                    return Err(e);
                }
            };    
        }
    }
    
    Ok(Some(df.collect()?))
    
}