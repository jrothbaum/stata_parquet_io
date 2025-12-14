use std::path::Path;
use std::sync::Arc;
use std::path::PathBuf;
use rayon::prelude::*;
use polars::error::ErrString;
use polars::prelude::*;
use polars_sql::SQLContext;
use polars::datatypes::{AnyValue, TimeUnit};
use std::error::Error;
use serde_json;
use std::collections::HashMap;
use glob::glob;
use regex::Regex;


use stata_jv::{
    send_dataframe_to_java,
    displayln,
    set_local,
    get_local,
    get_var_index,
    add_var_double,
    add_var_float,
    add_var_int,
    add_var_long,
    add_var_byte,
    add_var_str,
    add_var_strl,
    get_obs_total,
    set_obs_total,
    set_var_format,
    set_var_label,
    get_var_count,
    get_type,
    get_best_type,
    StataDataType,
    get_num,
    store_num,
    store_num_fast,
    get_str_var_width,
    get_str,
    get_str_any,
    store_str_any,
    store_str
};

use crate::utilities::{
    get_thread_count,
    DAY_SHIFT_SAS_STATA,
    SEC_MICROSECOND,
    SEC_MILLISECOND,
    SEC_NANOSECOND,
    SEC_SHIFT_SAS_STATA
};

use crate::downcast::apply_cast;



pub fn data_exists(path: &str) -> bool {
    let path_obj = Path::new(path);
    
    // displayln(&format!("=== DEBUG: Checking path: {}", path));
    // displayln(&format!("=== DEBUG: Path exists: {}", path_obj.exists()));
    // displayln(&format!("=== DEBUG: Is file: {}", path_obj.is_file()));
    // displayln(&format!("=== DEBUG: Is dir: {}", path_obj.is_dir()));
    
    // Check if it's a regular file
    if path_obj.exists() && path_obj.is_file() {
        //  displayln(&format!("=== DEBUG: Detected as regular file"));
        return true;
    }
    
    // Check if it's a hive partitioned directory with parquet files
    if path_obj.exists() && path_obj.is_dir() {
        //  displayln(&format!("=== DEBUG: Detected as directory, checking for hive structure"));
        let result = has_parquet_files_in_hive_structure(path);
        //  displayln(&format!("=== DEBUG: Hive structure check result: {}", result));
        return result;
    }
    
    // Check if it's a glob pattern that matches files
    //  displayln(&format!("=== DEBUG: Checking as glob pattern"));
    let result = is_valid_glob_pattern(path);
    //  displayln(&format!("=== DEBUG: Glob pattern check result: {}", result));
    result
}


fn has_parquet_files_in_hive_structure(dir_path: &str) -> bool {
    let mut glob_pattern = String::from(dir_path);
    
    // Remove trailing slash if present
    if glob_pattern.ends_with('/') {
        glob_pattern.pop();
    }
    
    // Normalize for Windows
    if cfg!(windows) {
        glob_pattern = glob_pattern.replace('\\', "/");
    }
    
    //  displayln(&format!("=== DEBUG: Checking hive structure in: {}", glob_pattern));
    
    // Check common hive patterns
    let test_patterns = vec![
        format!("{}/**/*.parquet", glob_pattern),
        format!("{}/*/*.parquet", glob_pattern),
        format!("{}/*/*/*.parquet", glob_pattern),
        format!("{}/*.parquet", glob_pattern), // Direct parquet files in directory
    ];
    
    // Return true if any pattern finds files
    for pattern in test_patterns {
        //  displayln(&format!("=== DEBUG: Testing hive pattern: {}", pattern));
        if let Ok(mut paths) = glob(&pattern) {
            if let Some(first_file) = paths.next() {
                match first_file {
                    Ok(file_path) => {
                        //  displayln(&format!("=== DEBUG: Found hive file: {:?}", file_path));
                        return true;
                    },
                    Err(e) => {
                        //  displayln(&format!("=== DEBUG: Error reading file in pattern {}: {:?}", pattern, e));
                    }
                }
            }
        } else {
            //  displayln(&format!("=== DEBUG: Pattern failed: {}", pattern));
        }
    }
    
    //  displayln(&format!("=== DEBUG: No parquet files found in hive structure"));
    false
}

fn is_valid_glob_pattern(glob_path: &str) -> bool {
    // Only check glob patterns (must contain glob characters)
    if !glob_path.contains('*') && !glob_path.contains('?') && !glob_path.contains('[') {
        return false;
    }
    
    // Fix common recursive wildcard pattern errors
    let mut normalized_pattern = if cfg!(windows) {
        glob_path.replace('\\', "/")
    } else {
        glob_path.to_string()
    };
    
    // Fix "**.ext" to "**/*.ext" (recursive wildcards must be their own component)
    if normalized_pattern.contains("**.") {
        normalized_pattern = normalized_pattern.replace("**.", "**/*.");
    }
    
    // Check if glob pattern matches any files
    match glob(&normalized_pattern) {
        Ok(paths) => {
            let found_files: Vec<_> = paths.filter_map(Result::ok).collect();
            
            // // Debug output (remove in production)
            // #[cfg(debug_assertions)]
            // {
            //     println!("Original pattern: {}", glob_path);
            //     println!("Normalized pattern: {}", normalized_pattern);
            //     println!("Found files: {:?}", found_files);
            // }
            
            !found_files.is_empty()
        },
        Err(e) => {
            #[cfg(debug_assertions)]
            println!("Glob error for pattern '{}': {:?}", normalized_pattern, e);
            false
        }
    }
}


pub fn scan_lazyframe(
    path: &str, 
    safe_relaxed: bool, 
    asterisk_to_variable_name: Option<&str>,
) -> Result<LazyFrame, PolarsError> {
    let path_obj = Path::new(path);
    
    // Check if it's a directory (hive partitioned dataset)
    if path_obj.is_dir() {
        return scan_hive_partitioned(path);
    }
    
    // Handle glob patterns with special options
    match (safe_relaxed, asterisk_to_variable_name) {
        (_, Some(var_name)) => scan_with_filename_extraction(path, var_name),
        (true, _) => scan_with_diagonal_relaxed(path),
        _ => {
            // Default behavior - direct scan_parquet on glob (with pattern normalization)
            let mut normalized_pattern = if cfg!(windows) {
                path.replace('\\', "/")
            } else {
                path.to_string()
            };
            
            // Fix "**.ext" to "**/*.ext"
            if normalized_pattern.contains("**.") {
                normalized_pattern = normalized_pattern.replace("**.", "**/*.");
            }
            
            let mut scan_args = ScanArgsParquet::default();
            scan_args.allow_missing_columns = true;
            scan_args.cache = false;
            LazyFrame::scan_parquet(
                PlPath::new(&normalized_pattern), scan_args.clone()
            )
        }
    }

    
}

fn scan_hive_partitioned(dir_path: &str) -> Result<LazyFrame, PolarsError> {
    // Detect hive partitioning structure and create appropriate glob
    let mut glob_pattern = String::from(dir_path);
    
    // Remove trailing slash if present
    if glob_pattern.ends_with('/') {
        glob_pattern.pop();
    }
    
    // Normalize for Windows
    if cfg!(windows) {
        glob_pattern = glob_pattern.replace('\\', "/");
    }
    
    // Check for common hive patterns
    let test_patterns = vec![
        format!("{}/**/*.parquet", glob_pattern),
        format!("{}/*/*.parquet", glob_pattern),
        format!("{}/*/*/*.parquet", glob_pattern),
    ];
    
    // Find the pattern that matches files
    for pattern in test_patterns {
        if let Ok(paths) = glob(&pattern) {
            let files: Vec<_> = paths.filter_map(Result::ok).collect();
            if !files.is_empty() {
                let mut scan_args = ScanArgsParquet::default();
                scan_args.allow_missing_columns = true;
                scan_args.cache = false;
                return LazyFrame::scan_parquet(PlPath::new(&pattern), scan_args.clone());
            }
        }
    }
    
    Err(PolarsError::ComputeError(format!("No parquet files found in hive partitioned structure: {}", dir_path).into()))
}

fn scan_with_diagonal_relaxed(glob_path: &str) -> Result<LazyFrame, PolarsError> {
    // Normalize pattern for Windows and fix recursive wildcards
    let mut normalized_pattern = if cfg!(windows) {
        glob_path.replace('\\', "/")
    } else {
        glob_path.to_string()
    };
    
    // Fix "**.ext" to "**/*.ext"
    if normalized_pattern.contains("**.") {
        normalized_pattern = normalized_pattern.replace("**.", "**/*.");
    }
    
    // Get all matching files
    let paths = glob(&normalized_pattern)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid glob pattern: {}", e).into()))?;
        
    let file_paths: Result<Vec<PathBuf>, _> = paths.collect();
    let file_paths = file_paths
        .map_err(|e| PolarsError::ComputeError(format!("Failed to read glob results: {}", e).into()))?;
    
    if file_paths.is_empty() {
        return Err(PolarsError::ComputeError(format!("No files found matching pattern: {}", normalized_pattern).into()));
    }
    
    // Create individual lazy frames for each file
    let mut scan_args = ScanArgsParquet::default();
    scan_args.allow_missing_columns = true;
    scan_args.cache = false;
    let lazy_frames: Result<Vec<LazyFrame>, PolarsError> = file_paths
        .iter()
        .map(|path| {
            LazyFrame::scan_parquet(
                PlPath::new(path.to_string_lossy().as_ref()), 
                scan_args.clone(),
            )
        })
        .collect();
    
    let lazy_frames = lazy_frames?;
    
    // Concatenate with diagonal relaxed
    concat(
        lazy_frames,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: true,
            diagonal: true,
            from_partitioned_ds: true,
            maintain_order: true,
        }
    )
}

fn scan_with_filename_extraction(
    glob_path: &str, 
    variable_name: &str
) -> Result<LazyFrame, PolarsError> {
    // Normalize pattern for Windows and fix recursive wildcards
    let mut normalized_pattern = if cfg!(windows) {
        glob_path.replace('\\', "/")
    } else {
        glob_path.to_string()
    };
    
    // Fix "**.ext" to "**/*.ext"
    if normalized_pattern.contains("**.") {
        normalized_pattern = normalized_pattern.replace("**.", "**/*.");
    }
    
    // Parse the normalized glob pattern to find asterisk position
    let asterisk_pos = normalized_pattern.find('*')
        .ok_or_else(|| PolarsError::ComputeError("No asterisk found in glob pattern".into()))?;
    
    // Create regex pattern from normalized glob
    let before_asterisk = &normalized_pattern[..asterisk_pos];
    let after_asterisk = &normalized_pattern[asterisk_pos + 1..];
    
    // Escape regex special characters in the parts before/after asterisk
    let before_escaped = regex::escape(before_asterisk);
    let after_escaped = regex::escape(after_asterisk);
    
    let regex_pattern = format!("{}(.+?){}", before_escaped, after_escaped);
    let re = Regex::new(&regex_pattern)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid regex pattern: {}", e).into()))?;
    
    // Get all matching files using normalized pattern
    let paths = glob(&normalized_pattern)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid glob pattern: {}", e).into()))?;
        
    let file_paths: Result<Vec<PathBuf>, _> = paths.collect();
    let file_paths = file_paths
        .map_err(|e| PolarsError::ComputeError(format!("Failed to read glob results: {}", e).into()))?;
    
    if file_paths.is_empty() {
        return Err(PolarsError::ComputeError(format!("No files found matching pattern: {}", normalized_pattern).into()));
    }
    
    // Create lazy frames with extracted values
    let lazy_frames: Result<Vec<LazyFrame>, PolarsError> = file_paths
        .iter()
        .map(|path| {
            let path_str = path.to_string_lossy();
            // Normalize the path string for regex matching
            let normalized_path_str = if cfg!(windows) {
                path_str.replace('\\', "/")
            } else {
                path_str.to_string()
            };
            
            // Extract value from filename using regex
            let extracted_value = re.captures(&normalized_path_str)
                .and_then(|caps| caps.get(1))
                .map(|m| m.as_str())
                .unwrap_or("unknown");
            
            // Create lazy frame with extracted column
            let mut scan_args = ScanArgsParquet::default();
            scan_args.allow_missing_columns = true;
            scan_args.cache = false;
            LazyFrame::scan_parquet(
                PlPath::new(path_str.as_ref()), 
                scan_args.clone()
            )
            .map(|lf| {
                //  displayln(&format!("Matched, {}: {}", variable_name, extracted_value));
                lf.with_columns([
                    smart_lit(extracted_value).alias(variable_name)
                ])
            })
        })
        .collect();
    
    let lazy_frames = lazy_frames?;
    
    // Concatenate all frames
    concat(
        lazy_frames,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: true,
            diagonal: true,
            from_partitioned_ds: true,
            maintain_order: true,
        }
    )
}


fn smart_lit(value: &str) -> Expr {
    let trimmed = value.trim();
    
    // Try integer
    if let Ok(int_val) = trimmed.parse::<i64>() {
        return lit(int_val);
    }
    
    // Try float
    if let Ok(float_val) = trimmed.parse::<f64>() {
        return lit(float_val);
    }
    
    // Default to string
    lit(value)
}

pub fn read_to_stata(
    path: &str,
    variables_as_str: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    safe_relaxed: bool, 
    asterisk_to_variable_name: Option<&str>,
    sort:&str,
    random_share:f64,
    random_seed:u64,
    batch_size:usize,
) -> Result<i32, Box<dyn Error>> {
    // displayln("read_to_stata: BEGIN");

    
    // Scan the parquet file to get a LazyFrame
    let mut df = match scan_lazyframe(
        path,
        safe_relaxed,
        asterisk_to_variable_name,
    ) {
        Ok(df) => df,
        Err(e) => {
            displayln(&format!("Error scanning lazyframe: {:?}", e));
            return Ok(198);
        },
    };

    let lf = df.clone().lazy();
    let selected_column_names: Vec<String> = if variables_as_str.trim().is_empty() || variables_as_str.trim() == "*" {
        // Select all columns
        lf.select([col("*")])
            .collect()?
            .get_column_names()
            .iter()
            .map(|&s| s.to_string())
            .collect()
    } else {
        // Build selector expressions for each pattern
        let patterns: Vec<&str> = variables_as_str.split_whitespace().collect();
        let mut exprs = Vec::new();
        
        for pattern in patterns {
            if pattern.contains('*') {
                // Convert to regex: "a*" -> "^a.*$", "*1" -> "^.*1$"
                let regex_pattern = format!("^{}$", pattern.replace("*", ".*"));
                exprs.push(col(&regex_pattern));
            } else {
                // Exact column name
                exprs.push(col(pattern));
            }
        }
        
        lf.select(exprs)
            .collect()?
            .get_column_names()
            .iter()
            .map(|&s| s.to_string())
            .collect()
    };


    //  displayln(&format!("df: {:?}", df.explain(true)));
    // Cast categorical columns to string
    df = cast_catenum_to_string(&df).unwrap();

    // Apply SQL filter if provided
    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", df);
        
        df = match ctx.execute(&format!("select * from df where {}", sql)) {
            Ok(lazyframe) => lazyframe,
            Err(e) => {
                displayln(&format!("Error in SQL if statement: {}", e));
                return Ok(198);
            }
        };
    }

    let sample_share = random_share > 0.0;
    if sample_share {
        let random_seed_option = if random_seed == 0 {
            None
        } else {
            Some(random_seed)
        };

        df = match df.collect() {
            Ok(df) => {
                df.sample_frac(
                        &Series::new("frac".into(), vec![random_share as f64]),
                        false,
                        false,
                        random_seed_option
                    )?.lazy()
            },
            Err(e) => {
                displayln(&format!("Error in SQL if statement: {}", e));
                return Ok(198);
            }
        };
    }
    df = if sort.is_empty() {
            df
        } else {
            
            let mut sort_options = SortMultipleOptions::default();
            let mut sort_cols: Vec<PlSmallStr> = Vec::new();
            let mut descending: Vec<bool> = Vec::new();

            for token in sort.split_whitespace() {
                if token.starts_with('-') && token.len() > 1 {
                    // Remove the '-' prefix and mark as descending
                    sort_cols.push(PlSmallStr::from(&token[1..]));
                    descending.push(true);
                } else {
                    // Use as-is and mark as ascending
                    sort_cols.push(PlSmallStr::from(token));
                    descending.push(false);
                }
            }
            sort_options.descending = descending;
            df.sort(
                sort_cols,
                sort_options
            )
        };

    // Create column expressions from the provided variable list
    let columns: Vec<Expr> = selected_column_names
        .iter()
        .map(|s| col(s.as_str()))
        .collect();
    

    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    // displayln(&format!("Offset: {}", offset));
    // displayln(&format!("Rows: {}", n_rows));
    // Track the current row offset
    let row_offset = Arc::new(AtomicUsize::new(
        if offset == 0 { 1 } else { offset }
    ));
    // displayln(&format!("row_offset: {}", row_offset.load(Ordering::SeqCst)));
    
    let row_offset_clone = row_offset.clone();
    let batch_counter = Arc::new(AtomicUsize::new(0));
    let batch_counter_clone = batch_counter.clone();

    // Clone what you need for the closure
    let selected_column_names_clone = selected_column_names.clone();

    // displayln(&format!("offset: {:?}", offset));
    // displayln(&format!("n_rows: {:?}", n_rows));

    let slice_offset = if offset > 0 { offset - 1 } else { 0 };
    if offset > 0 && n_rows > 0 {
        df = df.slice(slice_offset as i64, n_rows as u32);
    } else if offset > 0 {
        df = df.slice(slice_offset as i64, usize::MAX as u32);
    } else if n_rows > 0 {
        df = df.slice(0, n_rows as u32);
    }

    

    let height: usize = df
        .clone()
        .select([len().alias("height")])
        .collect()?
        .column("height")?
        .u32()?
        .get(0)
        .unwrap() as usize;

    // Configure batch processing
    let n_batches = (height as f64 / batch_size as f64).ceil() as usize;

    // Determine thread count based on data size
    let n_threads = if height < 1_000 {
        1
    } else {
        get_thread_count()
    };
    //  Add observations
    //  Add whatever variables are needed
    let existing_rows = get_obs_total().unwrap();
    // displayln(&format!("height={}",height));
    // displayln(&format!("existing height={}",existing_rows));
    let _ = set_obs_total((existing_rows as usize + height) as i64);
    let _ = 
    df.clone()
        .select(&columns)
        .sink_batches(
            PlanCallback::new(move |batch_df: DataFrame| -> PolarsResult<bool> {
                // Get current row offset and increment by batch size
                let current_row = row_offset_clone.fetch_add(
                    batch_df.height(), 
                    Ordering::SeqCst
                );
                
                // Get current batch index and increment
                let batch_idx = batch_counter_clone.fetch_add(1, Ordering::SeqCst);
                

                // Process the batch
                match process_batch(
                    batch_df,
                    current_row,
                    &selected_column_names_clone,
                    n_threads,
                    batch_idx,
                ) {
                    Ok(_) => Ok(false),
                    Err(e) => {
                        displayln(&format!("Error assigning values to stata data: {:?}", e));
                        Err(PolarsError::ComputeError(
                            format!("Batch processing error: {:?}", e).into()
                        ))
                    },
                }
            }),
            true,  // maintain_order
            NonZeroUsize::new(batch_size),
        )?
        .collect()?;

    // After completion, check how many batches were processed
    let total_batches = batch_counter.load(Ordering::SeqCst);

    // displayln("read_to_stata: END");
    Ok(0)
}



// To cast all categorical columns to string:
pub fn cast_catenum_to_string(lf: &LazyFrame) -> Result<LazyFrame, PolarsError> {
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


// Main entry point that delegates to appropriate strategy
fn process_batch(
    batch: DataFrame,
    start_index: usize,
    selected_columns: &Vec<String>,
    n_threads: usize,
    n_batch:usize,
) -> PolarsResult<()> {
    let vars_to_stata_types = vars_from_schema(&batch).unwrap();

    
    //  displayln(&format!("Sending data to java - BEGIN"));
    let _ = match send_dataframe_to_java(
        batch,
        start_index,
        vars_to_stata_types,
        n_threads
    ) {
        Ok(_) => {
            //  do nothing
        },
        Err(e) => {
            let _ = displayln(&format!("Sending data to java - ERROR: {:?}", e));
        }
    };
    //  displayln(&format!("Sending data to java - END"));
    
    Ok(())
}



fn vars_from_schema(
    df: &DataFrame,
) -> Result<HashMap<String, i32>, Box<dyn std::error::Error>> {

    let schema = &df.schema();
    let mut var_types: HashMap<String, i32> = HashMap::with_capacity(schema.len());

    let type_options = StataDataType::load()?;
    let n_vars_already = get_var_count().unwrap(); 

    for (col_name, dtype) in schema.iter() {
        //  displayln(&format!("col_name={}", col_name));
        //  displayln(&format!("dtype={}", dtype));
        
        let var_index = get_var_index(col_name).unwrap();
        //  displayln(&format!("var_index={}", var_index));

        if var_index > 0 && var_index <= n_vars_already {
            // Var exists already, no need to create it
            let mut stata_type = get_type(var_index).unwrap();
            var_types.insert(col_name.to_string(), stata_type);

            //  Some types need more information
            if (
                stata_type == type_options.type_byte
                || stata_type == type_options.type_int
                || stata_type == type_options.type_long
            ) {
                stata_type = check_type_int(
                    &df,
                    &col_name,
                    var_index,
                    stata_type,
                    &type_options,
                )?;
            } else if stata_type == type_options.type_str {
                stata_type = check_type_str(
                    &df,
                    &col_name,
                    var_index,
                    stata_type
                )?;
            }

        } else {
            let mut stata_type = match dtype {
                DataType::Int8 => type_options.type_byte,
                DataType::Int16 => type_options.type_int,
                DataType::Int32 => type_options.type_long,
                DataType::Int64 => type_options.type_long,
                DataType::UInt8 => type_options.type_byte,
                DataType::UInt16 => type_options.type_int,
                DataType::UInt32 => type_options.type_long,
                DataType::UInt64 => type_options.type_long,
                DataType::Float32 => type_options.type_float,
                DataType::Float64 => type_options.type_double,
                DataType::String => type_options.type_str,
                DataType::Boolean => type_options.type_strl,
                DataType::Date => type_options.type_long,
                DataType::Datetime(_, _) => type_options.type_double,
                DataType::Categorical(_, _) | DataType::Enum(_, _) => type_options.type_long,
                _ => {
                    return Err(format!("Unsupported data type for column {}: {:?}", col_name, dtype).into());
                }
            };

            
            let _ = match stata_type {
                t if t == type_options.type_byte =>      add_var_byte(col_name),
                t if t == type_options.type_int =>       add_var_int(col_name),
                t if t == type_options.type_long =>      add_var_long(col_name),
                t if t == type_options.type_float =>     add_var_float(col_name),
                t if t == type_options.type_double =>    add_var_double(col_name),
                t if t == type_options.type_str => {
                    let str_length = str_length_max(
                        &df,
                        &col_name
                    ).unwrap();

                    add_var_str(
                        col_name,
                        str_length
                    )
                },
                t if t == type_options.type_strl =>      add_var_strl(col_name),
                _ => return Err("Unknown Stata type".into()),
            };

            var_types.insert(col_name.to_string(), stata_type);
        }
    }
    
    Ok(var_types)
}

fn str_length_max(
    df: &DataFrame,
    col_name:&str,
) -> PolarsResult<i32> {
    let df_max = df.clone().lazy().select([
        //  col(col_name).min().alias("min"),
        col(col_name).str().len_bytes().max().alias("str_length"),
    ]).collect()?;

    Ok(df_max.column("str_length")?.u32()?.get(0).unwrap() as i32)
}


fn check_type_str(
    df: &DataFrame,
    col_name:&str,
    var_index:i32,
    stata_type:i32,
) -> PolarsResult<i32> {
    let existing_width = get_str_var_width(var_index).unwrap();
    let new_width = str_length_max(&df,&col_name).unwrap();

    if new_width > existing_width {
        let original_value = get_str(var_index,1).unwrap();

        let _ = store_str_any(
            var_index,
            1,
            &"a".repeat(new_width as usize)
        );

        let return_type = get_type(var_index).unwrap();
        
        let _ = store_str_any(
            var_index,
            1,
            &original_value
        );

        return Ok(return_type);
    }

    Ok(stata_type)
}

fn check_type_int(
    df: &DataFrame,
    col_name:&str,
    var_index:i32,
    var_type:i32,
    type_options: &StataDataType,
) -> PolarsResult<i32> {
    let df_max = df.clone().lazy().select([
        //  col(col_name).min().alias("min"),
        col(col_name).max().alias("max"),
    ]).collect()?;
    
    let max_val = df_max
        .column("max")?
        .cast(&DataType::Float64)?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let max_best = get_best_type(max_val).unwrap();

    let do_upcast = (
       (var_type == type_options.type_byte &&  max_best == type_options.type_int)
       || (var_type == type_options.type_int &&  max_best == type_options.type_long)
       || (var_type == type_options.type_long &&  max_best == type_options.type_double)
    );

    let return_type = if do_upcast {
        //  Needs upcasting
        let original_value = get_num(var_index,1).unwrap();
        let _ = store_num(
            var_index,
            1,
            max_val
        );

        let _ = store_num_fast(
            var_index,
            1,
            original_value
        );

        max_best
    } else {
        var_type
    };
 
    Ok(return_type)
}