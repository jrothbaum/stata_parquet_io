use std::sync::{Arc, Mutex};
use polars::prelude::{NamedFrom, TimeUnit};
use polars::prelude::{
    AnonymousScan,
    AnonymousScanArgs,
    PolarsResult,
    Schema,
    DataFrame,
    Series,
    DataType,
    PolarsError
};
use rayon::prelude::*;

use crate::stata_interface;
use crate::mapping;

use crate::read::{
    SEC_SHIFT_SAS_STATA,
    DAY_SHIFT_SAS_STATA,
};
pub struct StataDataScan {
    current_offset: Arc<Mutex<usize>>,
    n_rows: usize,
    batch_size: usize,
    schema: Schema,
    column_info:Vec<mapping::StataColumnInfo>,
}

impl StataDataScan {
    pub fn new(
        column_info: Vec<mapping::StataColumnInfo>,
        batch_size: Option<usize>,
    ) -> Self {
        StataDataScan {
            current_offset: Arc::new(Mutex::new(0)),
            n_rows: stata_interface::n_obs() as usize,
            batch_size: batch_size.unwrap_or(100_000),
            schema: mapping::StataColumnInfoToSchema(&column_info),
            column_info: column_info,
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
        unimplemented!()
    }
    
    #[allow(unused)]
    fn next_batch(
        &self,
        scan_opts: AnonymousScanArgs,
    ) -> PolarsResult<Option<DataFrame>> {
        // Get the current offset and calculate how many rows to read
        let mut offset = self.current_offset.lock().unwrap();
        
        // If we've read all rows, return None
        if *offset >= self.n_rows {
            return Ok(None);
        }
        
        // Calculate how many rows to read in this batch
        let rows_remaining = self.n_rows - *offset;
        let n_rows_to_read = std::cmp::min(self.batch_size, rows_remaining);
        
        // Prepare vectors to store column data
        let mut columns: Vec<Series> = Vec::with_capacity(self.schema.len());
        
        // Configure number of threads
        let n_threads = 2;// scan_opts.n_threads.unwrap_or(1);
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(n_threads)
            .build()
            .unwrap();
        
        // Process each column in the schema
        for (col_idx, field) in self.schema.iter().enumerate() {
            let col_name = field.0;
            let dtype = field.1;
            
            // Create appropriate Series based on data type
            let series = match dtype {
                DataType::String => {
                    // Process strings in parallel
                    let start_offset = *offset;
                    let str_length = mapping::find_str_length_by_name(
                        &self.column_info, 
                        &col_name
                    ).unwrap_or(0);


                    let string_values: Vec<String> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
                                stata_interface::read_string(col_idx+1, row,str_length)
                            })
                            .collect()
                    });

                    Series::new(col_name.clone(), string_values)
                },
                DataType::Boolean => {
                    // Process boolean values in parallel
                    let start_offset = *offset;
                    let bool_values: Vec<Option<bool>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
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
                    // Process integers in parallel
                    let start_offset = *offset;
                    let int_values: Vec<Option<i8>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
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
                    // Process integers in parallel
                    let start_offset = *offset;
                    let int_values: Vec<Option<i16>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
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
                    // Process integers in parallel
                    let start_offset = *offset;
                    let int_values: Vec<Option<i32>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
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
                    // Process floating point values in parallel
                    let start_offset = *offset;
                    let float_values: Vec<Option<f32>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
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
                    let start_offset = *offset;
                    let float_values: Vec<Option<f64>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
                                match stata_interface::read_numeric(col_idx+1, row) {
                                    Some(value) => Some(value),
                                    None => None
                                }
                            })
                            .collect()
                    });
                    Series::new(col_name.clone(), float_values)
                },
                DataType::Datetime(TimeUnit::Milliseconds,_) => {
                    // Process floating point values in parallel
                    let start_offset = *offset;
                    let float_values: Vec<Option<i64>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
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
                    let start_offset = *offset;
                    let float_values: Vec<Option<i64>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
                                match stata_interface::read_numeric(col_idx+1, row) {
                                    Some(value) => Some(value  as i64),
                                    None => None
                                }
                            })
                            .collect()
                    });
                    Series::new(col_name.clone(), float_values).cast(&DataType::Time).unwrap()
                },
                
                DataType::Date => {
                    // Process floating point values in parallel
                    let start_offset = *offset;
                    let float_values: Vec<Option<i32>> = thread_pool.install(|| {
                        (0..n_rows_to_read)
                            .into_par_iter()
                            .map(|row_idx| {
                                let row = start_offset + row_idx;
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
        }
        
        // Update the offset for the next batch
        *offset += n_rows_to_read;
        
        // Create and return the DataFrame
        let df = DataFrame::from_iter(columns);
        Ok(Some(df))
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