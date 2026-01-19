use std::path::Path;
use polars::prelude::*;
use polars_sql::SQLContext;
use std::error::Error;
use std::collections::HashMap;

use stata_jv::{
    displayln,
    get_local,
    get_var_index,
    get_obs_total,
    get_type,
    StataDataType,
    scan_stata,
};

use crate::downcast;
use crate::utilities::get_thread_count;


pub fn write_from_stata(
    path: &str,
    variables_as_str: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    partition_by_str: &str,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    compress: bool,
    compress_string: bool,
) -> Result<i32, Box<dyn Error>> {
    // Handle empty variable list by getting from local macro
    let variables_str = if variables_as_str.is_empty() || variables_as_str == "from_macro" {
        get_local("varlist").unwrap_or_default()
    } else {
        variables_as_str.to_string()
    };

    let rename_list = get_rename_list();
    
    // Build list of original variable names (for Stata access) and renamed names (for output)
    let original_columns: Vec<String> = variables_str
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    
    // Build list of variables with renaming applied (for output column names)
    let output_columns: Vec<String> = original_columns
        .iter()
        .map(|s| {
            match rename_list.get(s) {
                Some(renamed) => renamed.clone(),
                None => s.clone(),
            }
        })
        .collect();

    // Build partition columns (using output names)
    let partition_by: Vec<PlSmallStr> = if !partition_by_str.is_empty() {
        partition_by_str
            .split_whitespace()
            .map(|s| {
                let renamed = match rename_list.get(s) {
                    Some(r) => r.clone(),
                    None => s.to_string(),
                };
                PlSmallStr::from(renamed)
            })
            .collect()
    } else {
        Vec::new()
    };

    // Identify strL columns (using original Stata names)
    let type_options = StataDataType::load()?;
    let strl_columns: Vec<String> = original_columns
        .iter()
        .filter(|var_name| {
            if let Ok(var_idx) = get_var_index(var_name) {
                if var_idx > 0 {
                    if let Ok(var_type) = get_type(var_idx) {
                        return var_type == type_options.type_strl;
                    }
                }
            }
            false
        })
        .cloned()
        .collect();

    // Determine batch size and thread count
    let total_rows = if n_rows > 0 {
        n_rows
    } else {
        get_obs_total()? as usize
    };
    
    let batch_size = 100_000;
    let n_threads = if total_rows < 1_000 {
        1
    } else {
        get_thread_count()
    };

    // Create LazyFrame from Stata data using Java-based scanner
    // Use original column names for reading from Stata
    let mut lf = scan_stata(
        original_columns.clone(),
        strl_columns,
        n_threads as i32,
        batch_size as i32,
    )?;

    // Apply offset and row limit
    if offset > 0 || n_rows > 0 {
        let slice_offset = if offset > 0 { offset } else { 0 };
        let slice_len = if n_rows > 0 { n_rows } else { total_rows - slice_offset };
        lf = lf.slice(slice_offset as i64, slice_len as u32);
    }

    // Apply SQL filter if provided
    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", lf);
        
        lf = match ctx.execute(&format!("SELECT * FROM df WHERE {}", sql)) {
            Ok(filtered) => filtered,
            Err(e) => {
                let _ = displayln(&format!("Error in SQL if statement: {}", e));
                return Ok(198);
            }
        };
    }

    // Apply renaming if needed
    if !rename_list.is_empty() {
        let rename_exprs: Vec<Expr> = original_columns
            .iter()
            .zip(output_columns.iter())
            .map(|(orig, renamed)| {
                if orig != renamed {
                    col(orig.as_str()).alias(renamed.as_str())
                } else {
                    col(orig.as_str())
                }
            })
            .collect();
        lf = lf.select(rename_exprs);
    }

    // Delete existing files if overwrite is enabled
    let delete_error = delete_existing_files(path, overwrite_partition)?;
    if delete_error > 0 {
        return Ok(delete_error);
    }

    // Save to Parquet
    if !partition_by.is_empty() {
        save_partitioned(
            path,
            lf,
            compression,
            compression_level,
            &partition_by,
            compress,
            compress_string,
        )
    } else {
        save_no_partition(
            path,
            lf,
            compression,
            compression_level,
            compress,
            compress_string,
        )
    }
}


fn save_partitioned(
    path: &str,
    lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    partition_by: &Vec<PlSmallStr>,
    compress: bool,
    compress_string: bool,
) -> Result<i32, Box<dyn Error>> {
    let pqo = parquet_options(compression, compression_level);

    let mut df = match lf.collect() {
        Err(e) => {
            let _ = displayln(&format!("Parquet collect error: {}", e));
            return Ok(198);
        }
        Ok(df_collected) => df_collected,
    };

    if compress || compress_string {
        let cols_to_downcast: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|&name| name.to_string())
            .collect();

        let cols_not_boolean: Vec<String> = partition_by
            .iter()
            .map(|p| p.as_str().to_string())
            .collect();

        let mut down_config = downcast::DowncastConfig::default();
        down_config.check_strings = compress_string;
        down_config.prefer_int_over_float = compress;
        
        df = match downcast::intelligent_downcast_df(
            df,
            Some(cols_to_downcast),
            Some(cols_not_boolean),
            down_config,
        ) {
            Ok(df_ok) => df_ok,
            Err(e) => {
                let _ = displayln(&format!("Parquet downcast/compress error: {}", e));
                return Ok(198);
            }
        }
    }

    match write_partitioned_dataset(
        &mut df,
        PlPath::new(path).as_ref(),
        partition_by.clone(),
        &pqo,
        None,
        100_000_000_000,
    ) {
        Err(e) => {
            let _ = displayln(&format!(
                "Parquet write error during write_partitioned_dataset: {}",
                e
            ));
            Ok(198)
        }
        Ok(_) => {
            let _ = displayln(&format!("File saved to {}", path));
            Ok(0)
        }
    }
}


fn save_no_partition(
    path: &str,
    mut lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    compress: bool,
    compress_string: bool,
) -> Result<i32, Box<dyn Error>> {
    if compress || compress_string {
        let mut df = match lf.collect() {
            Ok(df_ok) => df_ok,
            Err(e) => {
                let _ = displayln(&format!("Parquet collect error: {}", e));
                return Ok(198);
            }
        };

        let mut down_config = downcast::DowncastConfig::default();
        down_config.check_strings = compress_string;
        down_config.prefer_int_over_float = compress;
        
        df = match downcast::intelligent_downcast_df(df, None, None, down_config) {
            Ok(df_ok) => df_ok,
            Err(e) => {
                let _ = displayln(&format!("Parquet downcast/compress error: {}", e));
                return Ok(198);
            }
        };

        lf = df.lazy();
    }

    let sink_target = SinkTarget::Path(PlPath::new(path));
    let pqo = parquet_options(compression, compression_level);

    // Set up the sink and handle potential errors
    let result_lf = match lf.sink_parquet(sink_target, pqo, None, SinkOptions::default()) {
        Err(e) => {
            let _ = displayln(&format!("Parquet sink setup error: {}", e));
            return Ok(198);
        }
        Ok(lf) => lf,
    };

    // Trigger execution with collect and handle errors
    match result_lf.collect() {
        Err(e) => {
            let _ = displayln(&format!("Parquet write error during collection: {}", e));
            Ok(198)
        }
        Ok(_) => {
            let _ = displayln(&format!("File saved to {}", path));
            Ok(0)
        }
    }
}


fn parquet_options(compression: &str, compression_level: Option<usize>) -> ParquetWriteOptions {
    let mut pqo = ParquetWriteOptions::default();
    pqo.compression = match compression {
        "lz4" => ParquetCompression::Lz4Raw,
        "uncompressed" => ParquetCompression::Uncompressed,
        "snappy" => ParquetCompression::Snappy,
        "gzip" => {
            let gzip_level = compression_level.and_then(|level| GzipLevel::try_new(level as u8).ok());
            ParquetCompression::Gzip(gzip_level)
        }
        "lzo" => ParquetCompression::Lzo,
        "brotli" => {
            let brotli_level =
                compression_level.and_then(|level| BrotliLevel::try_new(level as u32).ok());
            ParquetCompression::Brotli(brotli_level)
        }
        _ => {
            let zstd_level =
                compression_level.and_then(|level| ZstdLevel::try_new(level as i32).ok());
            ParquetCompression::Zstd(zstd_level)
        }
    };

    pqo
}


fn get_rename_list() -> HashMap<String, String> {
    let mut rename_list = HashMap::<String, String>::new();
    
    let n_rename_str = get_local("n_rename").unwrap_or_default();
    let n_rename = n_rename_str.parse::<usize>().unwrap_or(0);

    for i in 1..=n_rename {
        let rename_from = get_local(&format!("rename_from_{}", i)).unwrap_or_default();
        let rename_to = get_local(&format!("rename_to_{}", i)).unwrap_or_default();
        
        if !rename_from.is_empty() && !rename_to.is_empty() {
            rename_list.insert(rename_from, rename_to);
        }
    }
    
    rename_list
}


fn delete_existing_files(path: &str, overwrite_partition: bool) -> Result<i32, Box<dyn Error>> {
    if overwrite_partition {
        let path_obj = std::path::Path::new(path);

        if path_obj.is_file() {
            // If it's a .parquet file, delete it
            if path.ends_with(".parquet") {
                if let Err(e) = std::fs::remove_file(path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        let _ = displayln(&format!("Failed to remove parquet file {}: {}", path, e));
                        return Ok(198);
                    }
                }
            }
        } else if path_obj.is_dir() {
            // Only delete if all subdirectories are hive style and all files are .parquet
            if is_hive_style_parquet_directory(path_obj) {
                if let Err(e) = std::fs::remove_dir_all(path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        let _ = displayln(&format!("Failed to remove directory {}: {}", path, e));
                        return Ok(198);
                    }
                }
            } else {
                let _ = displayln(&format!(
                    "Error: {} is not a hive partition directory, not removed",
                    path
                ));
                return Ok(198);
            }
        }
    }

    Ok(0)
}


fn is_hive_style_parquet_directory(path: &Path) -> bool {
    fn check_recursive(dir: &Path) -> Result<bool, std::io::Error> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                // Check if directory name follows hive style (contains "=")
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if !name.contains('=') {
                        return Ok(false);
                    }
                }

                // Recursively check subdirectory
                if !check_recursive(&path)? {
                    return Ok(false);
                }
            } else if path.is_file() {
                // Check if file ends with .parquet
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if !name.ends_with(".parquet") {
                        return Ok(false);
                    }
                } else {
                    // If we can't get the filename, assume it's not a parquet file
                    return Ok(false);
                }
            }
            // Skip other file types (symlinks, etc.)
        }
        Ok(true)
    }

    check_recursive(path).unwrap_or(false)
}