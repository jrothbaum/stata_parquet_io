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
use std::collections::HashSet;
use glob::glob;
use regex::Regex;


use crate::mapping::ColumnInfo;
use crate::stata_interface::{
    display,
    set_macro,
    replace_number,
    replace_string,
    get_macro
};

use crate::utilities::{
    determine_parallelization_strategy,
    get_thread_count,
    ParallelizationStrategy,
    DAY_SHIFT_SAS_STATA,
    SEC_MICROSECOND,
    SEC_MILLISECOND,
    SEC_NANOSECOND,
    SEC_SHIFT_SAS_STATA
};

use crate::downcast::apply_cast;

// Trait for converting Polars values to Stata values

trait ToStataValue {
    fn to_stata_value(&self) -> Option<f64>;
}

// Implementations for different types
impl ToStataValue for bool {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(if *self { 1.0 } else { 0.0 })
    }
}

impl ToStataValue for i8 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for i16 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for i32 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for i64 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u8 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u16 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u32 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for u64 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for f32 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self as f64)
    }
}

impl ToStataValue for f64 {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some(*self)
    }
}

// Special type for handling dates
struct DateValue(i32);
impl ToStataValue for DateValue {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some((self.0 + DAY_SHIFT_SAS_STATA) as f64)
    }
}

// Special type for handling time
struct TimeValue(i64);

impl ToStataValue for TimeValue {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        Some((self.0 / SEC_MICROSECOND) as f64)
    }
}


// Special type for handling datetime
struct DatetimeValue(i64, TimeUnit);

impl ToStataValue for DatetimeValue {
    #[inline(always)]
    fn to_stata_value(&self) -> Option<f64> {
        let mills_factor = match self.1 {
            TimeUnit::Nanoseconds => (SEC_NANOSECOND/SEC_MILLISECOND) as f64,
            TimeUnit::Microseconds => (SEC_MICROSECOND/SEC_MILLISECOND) as f64,
            TimeUnit::Milliseconds => 1.0,
        };
        
        Some(self.0 as f64 / mills_factor + (SEC_SHIFT_SAS_STATA as f64) * (SEC_MILLISECOND as f64))
    }
}

pub fn data_exists(path: &str) -> bool {
    let path_obj = Path::new(path);
    
    // display(&format!("=== DEBUG: Checking path: {}", path));
    // display(&format!("=== DEBUG: Path exists: {}", path_obj.exists()));
    // display(&format!("=== DEBUG: Is file: {}", path_obj.is_file()));
    // display(&format!("=== DEBUG: Is dir: {}", path_obj.is_dir()));
    
    // Check if it's a regular file
    if path_obj.exists() && path_obj.is_file() {
        //  display(&format!("=== DEBUG: Detected as regular file"));
        return true;
    }
    
    // Check if it's a hive partitioned directory with parquet files
    if path_obj.exists() && path_obj.is_dir() {
        //  display(&format!("=== DEBUG: Detected as directory, checking for hive structure"));
        let result = has_parquet_files_in_hive_structure(path);
        //  display(&format!("=== DEBUG: Hive structure check result: {}", result));
        return result;
    }
    
    // Check if it's a glob pattern that matches files
    //  display(&format!("=== DEBUG: Checking as glob pattern"));
    let result = is_valid_glob_pattern(path);
    //  display(&format!("=== DEBUG: Glob pattern check result: {}", result));
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
    
    //  display(&format!("=== DEBUG: Checking hive structure in: {}", glob_pattern));
    
    // Check common hive patterns
    let test_patterns = vec![
        format!("{}/**/*.parquet", glob_pattern),
        format!("{}/*/*.parquet", glob_pattern),
        format!("{}/*/*/*.parquet", glob_pattern),
        format!("{}/*.parquet", glob_pattern), // Direct parquet files in directory
    ];
    
    // Return true if any pattern finds files
    for pattern in test_patterns {
        //  display(&format!("=== DEBUG: Testing hive pattern: {}", pattern));
        if let Ok(mut paths) = glob(&pattern) {
            if let Some(first_file) = paths.next() {
                match first_file {
                    Ok(file_path) => {
                        //  display(&format!("=== DEBUG: Found hive file: {:?}", file_path));
                        return true;
                    },
                    Err(e) => {
                        //  display(&format!("=== DEBUG: Error reading file in pattern {}: {:?}", pattern, e));
                    }
                }
            }
        } else {
            //  display(&format!("=== DEBUG: Pattern failed: {}", pattern));
        }
    }
    
    //  display(&format!("=== DEBUG: No parquet files found in hive structure"));
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
                //  display(&format!("Matched, {}: {}", variable_name, extracted_value));
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
    mapping: &str,
    parallel_strategy: Option<ParallelizationStrategy>,
    safe_relaxed: bool, 
    asterisk_to_variable_name: Option<&str>,
    sort:&str,
    stata_offset:usize,
    random_share:f64,
    random_seed:u64,
    batch_size:usize,
) -> Result<i32, Box<dyn Error>> {

    // Handle empty variable list by getting from macros
    let variables_as_str = if variables_as_str.is_empty() || variables_as_str == "from_macro" {
        &get_macro("matched_vars", false, Some(1024 * 1024 * 10))
    } else {
        variables_as_str
    };

    // Get column info either from mapping or macros
    let all_columns_unfiltered: Vec<ColumnInfo> = if mapping.is_empty() || mapping == "from_macros" {
        let n_vars_str = get_macro("n_matched_vars", false, None);
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


    // First, create a HashSet of column names from variables_as_str for efficient lookups
    let selected_column_names: HashSet<&str> = variables_as_str.split_whitespace().collect();

    // Then filter all_columns to only keep columns whose names are in the HashSet
    let all_columns: Vec<ColumnInfo> = all_columns_unfiltered
        .into_iter()
        .filter(|col_info| selected_column_names.contains(col_info.name.as_str()))
        .collect();

    //  display(&format!("Column information: {:?}", all_columns));

    // Scan the parquet file to get a LazyFrame
    let mut df = match scan_lazyframe(
        path,
        safe_relaxed,
        asterisk_to_variable_name,
    ) {
        Ok(df) => df,
        Err(e) => {
            display(&format!("Error scanning lazyframe: {:?}", e));
            return Ok(198);
        },
    };

    //  Set cast macro to empty
    let cast_json = get_macro(
        &"cast_json",
        false,
        None,
    );

    //  display(&format!("Cast: {}", cast_json));
    if !cast_json.is_empty() {
        df = match apply_cast(
            df,
            &cast_json,
        ) {
            Ok(lf_cast) => lf_cast,
            Err(e) => {
                display(&format!("Cast failed with error: {}", e));
                return Ok(198);
            }
        }
    }

    //  display(&format!("df: {:?}", df.explain(true)));
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
                display(&format!("Error in SQL if statement: {}", e));
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
        .map(|&s| col(s))
        .collect();

    //  display(&format!("columns: {:?}", columns));
    
    // Configure batch processing
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



    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    // display(&format!("Batches: {}", n_batches));
    // display(&format!("Offset: {}", offset));
    // display(&format!("Rows: {}", n_rows));
    // Track the current row offset
    let row_offset = Arc::new(AtomicUsize::new(0));
    let row_offset_clone = row_offset.clone();
    let batch_counter = Arc::new(AtomicUsize::new(0));
    let batch_counter_clone = batch_counter.clone();

    // Clone what you need for the closure
    let all_columns_clone = all_columns.clone();

    df.clone()
        .select(&columns)
        .slice(offset as i64, n_rows as u32)
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
                match process_batch_with_strategy(
                    &batch_df,
                    current_row,
                    &all_columns_clone,
                    strategy,
                    n_threads,
                    batch_idx,
                    stata_offset
                ) {
                    Ok(_) => Ok(false),
                    Err(e) => {
                        display(&format!("Error assigning values to stata data: {:?}", e));
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
    set_macro("n_batches", &total_batches.to_string(), false);

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


// Create column info from Stata macros
fn column_info_from_macros(n_vars: usize) -> Vec<ColumnInfo> {
    let mut column_infos = Vec::with_capacity(n_vars);
    
    for i in 0..n_vars {
        let index = get_macro(&format!("v_to_read_index_{}", i+1), false, None).parse::<usize>().unwrap() - 1;
        let name = get_macro(&format!("v_to_read_name_{}", i+1), false, None);
        let dtype = get_macro(&format!("v_to_read_p_type_{}", i+1), false, None);
        let stata_type = get_macro(&format!("v_to_read_type_{}", i+1), false, None);
        
        column_infos.push(ColumnInfo {
            index,
            name,
            dtype,
            stata_type,
        });
    }
    
    //  display(&format!("{:?}", column_infos));
            
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
    stata_offset:usize,
) -> PolarsResult<()> {

    // If only 1 thread requested or batch is too small, use single-threaded version
    let row_count = batch.height();
    let min_multithreaded = 10000;
    
    if n_threads <= 1 || row_count < min_multithreaded {
        return process_batch_single_thread(batch, start_index, all_columns, stata_offset);
    }

    // Partition columns into special (strl/binary) and regular columns
    let (special_columns, 
         regular_columns): (Vec<_>, Vec<_>) = all_columns.iter().enumerate()
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
                    process_regular_by_row(batch, start_index, &regular_column_infos, stata_offset)?;
                },
                ParallelizationStrategy::ByColumn => {
                    // Process regular columns by column
                    process_regular_by_column(batch, start_index, &regular_column_infos, stata_offset)?;
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
                                col_info.index+1,
                                stata_offset,
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
    columns: &Vec<ColumnInfo>,
    stata_offset: usize,
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
            process_row_range(batch, start_index, start_row, end_row, columns, stata_offset)
        })
}

// Process regular columns with column-wise parallelization
fn process_regular_by_column(
    batch: &DataFrame,
    start_index: usize,
    columns: &Vec<ColumnInfo>,
    stata_offset: usize,
) -> PolarsResult<()> {
    // Process columns in parallel
    columns.par_iter().enumerate()
        .try_for_each(|(col_idx, col_info)| {
            // Get the column by name
            let col = match batch.column(&col_info.name) {
                Ok(c) => c,
                Err(e) => return Err(e)
            };
            //  display(&format!("Index: {}", col_info.index));
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
                                global_row_idx + 1 + stata_offset, // +1 because replace_string expects 1-indexed
                                col_info.index + 1        // +1 because replace functions expect 1-indexed
                            );
                        }
                    }
                    Ok(())
                },
                "datetime" => {
                    // Process datetime with appropriate time unit
                    process_datetime_column(col, 0, batch.height(), start_index, col_info.index + 1, stata_offset)
                },
                _ => {
                    // Handle numeric types
                    process_numeric_column(col, col_info, 0, batch.height(), start_index, col_info.index + 1, stata_offset)
                }
            }
        })
}


// Single-threaded implementation (fallback)
fn process_batch_single_thread(
    batch: &DataFrame,
    start_index: usize,
    all_columns: &Vec<ColumnInfo>,
    stata_offset: usize,
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
    let regular_process_out = process_row_range(batch, start_index, 0, batch.height(), &regular_column_infos, stata_offset);



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
                            col_info.index + 1,
                            stata_offset,
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
    all_columns: &Vec<ColumnInfo>,
    stata_offset: usize,
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
                            global_row_idx + 1 + stata_offset, // +1 because replace_string expects 1-indexed
                            col_info.index + 1        // +1 because replace functions expect 1-indexed
                        );
                    }
                }
            },
            "datetime" => {
                // Process datetime with appropriate time unit
                process_datetime_column(col, start_row, end_row, start_index, col_info.index + 1, stata_offset)?;
            },
            _ => {
                // Handle numeric types (including date/time which get converted to numeric)
                process_numeric_column(col, col_info, start_row, end_row, start_index, col_info.index + 1, stata_offset)?;
            }
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
    col_idx: usize,
    stata_offset: usize
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
    
    let mills_factor = match time_unit_unwrapped {
        TimeUnit::Nanoseconds => (SEC_NANOSECOND/SEC_MILLISECOND) as f64,
        TimeUnit::Microseconds => (SEC_MICROSECOND/SEC_MILLISECOND) as f64,
        TimeUnit::Milliseconds => 1.0,
    };
    
    let sec_shift_scaled = (SEC_SHIFT_SAS_STATA as f64) * (SEC_MILLISECOND as f64);
    
    // Process each row based on the schema's time unit
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        let value: Option<f64> = match col.get(row_idx) {
            Ok(AnyValue::Datetime(v, _, _)) => { 
                Some(v as f64 / mills_factor + sec_shift_scaled)
            },
            _ => None
        };

        replace_number(
            value, 
            global_row_idx + 1 + stata_offset,  // +1 because replace functions expect 1-indexed
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
    col_idx: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    // Use function pointers for better performance
    let converter: fn(&AnyValue) -> Option<f64> = match col_info.dtype.as_str() {
        "Boolean" => |av| match av { AnyValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }), _ => None },
        "Int8" => |av| match av { AnyValue::Int8(v) => Some(*v as f64), _ => None },
        "Int16" => |av| match av { AnyValue::Int16(v) => Some(*v as f64), _ => None },
        "Int32" => |av| match av { AnyValue::Int32(v) => Some(*v as f64), _ => None },
        "Int64" => |av| match av { AnyValue::Int64(v) => Some(*v as f64), _ => None },
        "UInt8" => |av| match av { AnyValue::UInt8(v) => Some(*v as f64), _ => None },
        "UInt16" => |av| match av { AnyValue::UInt16(v) => Some(*v as f64), _ => None },
        "UInt32" => |av| match av { AnyValue::UInt32(v) => Some(*v as f64), _ => None },
        "UInt64" => |av| match av { AnyValue::UInt64(v) => Some(*v as f64), _ => None },
        "Float32" => |av| match av { AnyValue::Float32(v) => Some(*v as f64), _ => None },
        "Float64" => |av| match av { AnyValue::Float64(v) => Some(*v), _ => None },
        "Date" => |av| match av { AnyValue::Date(v) => Some((*v + DAY_SHIFT_SAS_STATA) as f64), _ => None },
        "Time" => |av| match av { AnyValue::Time(v) => Some((*v / SEC_MICROSECOND) as f64), _ => None },
        _ => return Ok(()) // Skip unknown types
    };

    // Get the column's data type from the stored string representation
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        let value = col.get(row_idx).ok().and_then(|av| converter(&av));
        replace_number(value, global_row_idx + 1 + stata_offset, col_idx);
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
    stata_offset:usize,
) -> PolarsResult<()> {
    let start_row = std::cmp::max(start_row,1);
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
        &(start_row + start_index + stata_offset).to_string(),
        false,
    );

    set_macro(
        &format!(
            "strl_end_{}_{}",
            col_idx,
            n_batch
        ),
        &(end_row + start_index + stata_offset).to_string(),
        false,
    );
    let sink_target = SinkTarget::Path(PlPath::new(path.as_ref()));
    let mut csv_options = CsvWriterOptions::default();
    csv_options.include_header = false;
    csv_options.serialize_options.quote_style = QuoteStyle::Never;

    let processed = batch.clone().lazy()
        .select([
            col(&column_name.to_string())
                // Encode internal newlines as visible escape sequences
                .str().replace_all(lit("\n"), lit("\\n"), false) 
                .str().replace_all(lit("\r"), lit("\\r"), false)
                .str().replace_all(lit("\""), lit("'"), false)
                .alias(&column_name.to_string())
        ])
        .collect()
        .unwrap();

    match processed.lazy().sink_csv(
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
