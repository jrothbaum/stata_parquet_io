use polars::prelude::*;
use polars::prelude::{NamedFrom, TimeUnit};
use polars_parquet::write::{BrotliLevel, GzipLevel, ZstdLevel};
use polars_sql::SQLContext;
use rayon::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::mapping::{self, StataColumnInfo};
use crate::stata_interface::{display, get_macro};
use crate::stata_metadata;
use crate::{downcast, stata_interface};

use crate::utilities::{
    DAY_SHIFT_SAS_STATA,
    //  SEC_MILLISECOND,
    SEC_MICROSECOND,
    //  SEC_NANOSECOND,
    SEC_SHIFT_SAS_STATA,
};

static CONSOLIDATION_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)]
fn adaptive_write_batch_size(requested_rows: usize, n_cols: usize, n_rows: usize) -> usize {
    if n_rows == 0 {
        return 1;
    }
    let requested = requested_rows.max(10_000);
    let est_bytes_per_row = std::cmp::max(1, n_cols) * 16;
    let target_bytes = 64 * 1024 * 1024;
    let adaptive = (target_bytes / est_bytes_per_row).clamp(10_000, 1_000_000);
    requested.min(adaptive).min(n_rows)
}

pub fn write_from_stata(
    path: &str,
    variables_as_str: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    mapping: &str,
    partition_by_str: &str,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    compress: bool,
    compress_string: bool,
    quietly: bool,
    append_to_partition: bool,
    output_format: &str,
    stata_metadata_capsule: &str,
    stata_metadata_varlist_arg: &str,
) -> Result<i32, Box<dyn Error>> {
    let variables_as_str = if variables_as_str == "" || variables_as_str == "from_macro" {
        &get_macro("varlist", false, Some(1024 * 1024 * 10))
    } else {
        variables_as_str
    };

    let rename_list = get_rename_list();
    let metadata_rename_list = rename_list.clone();
    let saved_variables: std::collections::HashSet<String> = variables_as_str
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    let all_columns: Vec<PlSmallStr> = variables_as_str
        .split_whitespace()
        .map(|s| {
            let s_small = PlSmallStr::from(s);

            match rename_list.get(&s_small) {
                Some(renamed) => renamed.clone(), // Clone the PlSmallStr we found
                None => s_small,                  // Use the original PlSmallStr
            }
        })
        .collect();

    //  Default batch size
    let batch_size = Some(100_000 as usize);

    let partition_by: Vec<PlSmallStr> = if !partition_by_str.is_empty() {
        partition_by_str
            .split_whitespace()
            .map(|s| {
                let s_small = PlSmallStr::from(s);

                match rename_list.get(&s_small) {
                    Some(renamed) => renamed.clone(), // Clone the PlSmallStr we found
                    None => s_small,                  // Use the original PlSmallStr
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let column_info: Vec<StataColumnInfo> = if mapping == "" || mapping == "from_macros" {
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
        column_info_from_macros(n_vars, rename_list)
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
        batch_size,
        offset,
        n_rows,
        sql_if_owned,
    );

    let a_scan_arc = Arc::new(a_scan);

    let lf = LazyFrame::anonymous_scan(a_scan_arc, ScanArgsAnonymous::default());

    let lf_unwrapped = lf.unwrap();

    let output_format_normalized = output_format.to_ascii_lowercase();

    let stata_metadata_varlist_owned: String;
    let stata_metadata_varlist = if stata_metadata_varlist_arg == "from_macro" {
        stata_metadata_varlist_owned =
            get_macro("stata_metadata_varlist", false, Some(1024 * 1024 * 10));
        &stata_metadata_varlist_owned
    } else {
        stata_metadata_varlist_arg
    };
    let has_stata_metadata =
        !stata_metadata_capsule.is_empty() || !stata_metadata_varlist.is_empty();
    if has_stata_metadata && output_format_normalized != "parquet" {
        display("statametadata is only supported for Parquet output");
        return Ok(198);
    }
    let physical_names: HashMap<String, String> = metadata_rename_list
        .iter()
        .map(|(from, to)| (from.to_string(), to.to_string()))
        .collect();
    let mut prepared_stata_metadata = match stata_metadata::prepare_stata_metadata(
        stata_metadata_capsule,
        stata_metadata_varlist,
        &saved_variables,
        &physical_names,
    ) {
        Ok(metadata) => metadata,
        Err(e) => {
            display(&format!("Could not prepare embedded Stata metadata: {e}"));
            return Ok(198);
        }
    };

    if output_format_normalized == "parquet"
        && !partition_by.is_empty()
        && !overwrite_partition
        && Path::new(path).exists()
    {
        let existing_fragments = match stata_metadata::resolve_parquet_fragments(path) {
            Ok(fragments) => fragments,
            Err(e) => {
                display(&format!(
                    "Could not inspect the existing partitioned dataset before append: {e}"
                ));
                return Ok(198);
            }
        };
        let existing_metadata =
            match stata_metadata::validate_stata_metadata_fragments(&existing_fragments) {
                Ok(metadata) => metadata,
                Err(e) => {
                    display(&format!(
                        "Could not validate the existing partitioned dataset before append: {e}"
                    ));
                    return Ok(198);
                }
            };
        prepared_stata_metadata = match stata_metadata::reconcile_stata_metadata_for_append(
            existing_metadata,
            prepared_stata_metadata.as_ref(),
        ) {
            Ok(metadata) => metadata,
            Err(e) => {
                display(&format!(
                    "Could not append to the existing partitioned dataset: {e}"
                ));
                return Ok(198);
            }
        };
    }

    if output_format_normalized == "parquet" {
        let delete_error = delete_existing_files(path, overwrite_partition);

        if delete_error > 0 {
            return Ok(delete_error);
        }
        if partition_by.len() > 0 {
            save_partitioned(
                path,
                lf_unwrapped,
                compression,
                compression_level,
                &partition_by,
                compress,
                compress_string,
                quietly,
                append_to_partition,
                prepared_stata_metadata.as_ref(),
            )
        } else {
            save_no_partition(
                path,
                lf_unwrapped,
                compression,
                compression_level,
                compress,
                compress_string,
                quietly,
                prepared_stata_metadata.map(|prepared| prepared.key_value_metadata),
            )
        }
    } else {
        if !partition_by.is_empty() {
            display("partition_by() is only supported for parquet output");
            return Ok(198);
        }

        let delete_error = delete_existing_non_parquet(path);
        if delete_error > 0 {
            return Ok(delete_error);
        }

        let mut df = match lf_unwrapped.collect() {
            Ok(df) => df,
            Err(e) => {
                display(&format!(
                    "Collect error before {} write: {}",
                    output_format_normalized, e
                ));
                return Ok(198);
            }
        };

        if compress || compress_string {
            let mut down_config = downcast::DowncastConfig::default();
            down_config.check_strings = compress_string;
            down_config.prefer_int_over_float = compress;
            df = match downcast::intelligent_downcast_df(df, None, None, down_config) {
                Ok(df_ok) => df_ok,
                Err(e) => {
                    display(&format!(
                        "Downcast/compress error before {} write: {}",
                        output_format_normalized, e
                    ));
                    return Ok(198);
                }
            };
        }

        match output_format_normalized.as_str() {
            "spss" => {
                let writer = polars_readstat_rs::SpssWriter::new(path);
                if let Err(e) = writer.write_df(&df) {
                    display(&format!("SPSS write error: {}", e));
                    return Ok(198);
                }
            }
            "csv" => {
                let mut file = match File::create(path) {
                    Ok(f) => f,
                    Err(e) => {
                        display(&format!("CSV file create error: {}", e));
                        return Ok(198);
                    }
                };

                if let Err(e) = CsvWriter::new(&mut file)
                    .include_header(true)
                    .finish(&mut df)
                {
                    display(&format!("CSV write error: {}", e));
                    return Ok(198);
                }
            }
            _ => {
                display(&format!("Unsupported output format: {}", output_format));
                return Ok(198);
            }
        }

        if !quietly {
            let _ = display(&format!("File saved to {}", path));
        }
        Ok(0)
    }
}

fn delete_existing_non_parquet(path: &str) -> i32 {
    let path_obj = Path::new(path);
    if !path_obj.exists() {
        return 0;
    }

    if path_obj.is_dir() {
        display(&format!(
            "Error: {} is a directory; expected a file path",
            path
        ));
        return 198;
    }

    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            display(&format!("Failed to remove existing file {}: {}", path, e));
            return 198;
        }
    }

    0
}

fn save_partitioned(
    path: &str,
    lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    partition_by: &Vec<PlSmallStr>,
    compress: bool,
    compress_string: bool,
    quietly: bool,
    append_to_partition: bool,
    stata_metadata: Option<&stata_metadata::PreparedStataMetadata>,
) -> Result<i32, Box<dyn Error>> {
    let mut df = match lf.collect() {
        Err(e) => {
            display(&format!("Parquet collect error: {}", e));
            return Ok(198);
        }
        Ok(df_collected) => df_collected,
    };

    if compress | compress_string {
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
                display(&format!("Parquet downcast/compress error: {}", e));
                return Ok(198);
            }
        }
    }

    save_partitioned_sequential(
        path,
        df.lazy(),
        compression,
        compression_level,
        partition_by,
        false,
        false,
        quietly,
        append_to_partition,
        stata_metadata,
    )

    // let partition_variant = PartitionVariant::ByKey {
    //      key_exprs: partition_cols,
    //      include_key: false
    // };

    // let result_lf = match lf.sink_parquet_partitioned(
    //     Arc::new(PathBuf::from(path)),
    //     None,
    //     partition_variant,
    //     pqo,
    //     None,
    //     SinkOptions::default()) {
    //         Err(e) => {
    //             display(&format!("Parquet sink setup error: {}", e));
    //             return Ok(198);
    //         },
    //         Ok(lf) => lf,
    //     };

    // // Then trigger execution with collect and handle those errors
    // match result_lf.collect() {
    //     Err(e) => {
    //         display(&format!("Parquet write error during collection: {}", e));
    //         Ok(198)
    //     },
    //     Ok(_) => {
    //         display(&format!("File saved to {}", path));
    //         Ok(0)
    //     }
    // }
}

fn format_partition_float(value: f64) -> String {
    if value.is_finite() && value.fract().abs() < 1e-9 {
        format!("{:.1}", value)
    } else {
        let mut s = format!("{:.12}", value);
        while s.contains('.') && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
        s
    }
}

fn next_partition_file_index(partition_dir: &PathBuf) -> usize {
    let mut max_idx: Option<usize> = None;
    if let Ok(entries) = std::fs::read_dir(partition_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) {
                if let Some(stem) = name
                    .strip_prefix("data_")
                    .and_then(|s| s.strip_suffix(".parquet"))
                {
                    if let Ok(n) = stem.parse::<usize>() {
                        max_idx = Some(max_idx.map_or(n, |m: usize| m.max(n)));
                    }
                }
            }
        }
    }
    max_idx.map_or(0, |m| m + 1)
}

fn save_partitioned_sequential(
    path: &str,
    lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    partition_by: &Vec<PlSmallStr>,
    compress: bool,
    compress_string: bool,
    quietly: bool,
    append_to_partition: bool,
    stata_metadata: Option<&stata_metadata::PreparedStataMetadata>,
) -> Result<i32, Box<dyn Error>> {
    let pqo = parquet_options(
        compression,
        compression_level,
        stata_metadata.map(|metadata| metadata.key_value_metadata.clone()),
    );

    // First, get unique partition values by collecting only the partition columns
    let partition_values_df = lf
        .clone()
        .select(
            partition_by
                .iter()
                .map(|col_name| col(col_name.clone()))
                .collect::<Vec<_>>(),
        )
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .map_err(|e| {
            display(&format!("Error getting partition values: {}", e));
            e
        })?;

    let total_partitions = partition_values_df.height();
    if !quietly {
        _ = display(&format!(
            "Processing {} partitions sequentially",
            total_partitions
        ));
    }
    // Process each partition sequentially
    for partition_idx in 0..total_partitions {
        // Get the partition values for this row
        let mut partition_filters = Vec::new();
        let mut partition_path_parts = Vec::new();

        for col_name in partition_by {
            let series = partition_values_df.column(col_name.as_str()).map_err(|e| {
                display(&format!(
                    "Error accessing partition column {}: {}",
                    col_name, e
                ));
                e
            })?;

            let value = series.get(partition_idx).map_err(|e| {
                display(&format!("Error getting partition value: {}", e));
                e
            })?;

            let filter_expr = match value {
                AnyValue::String(s) => col(col_name.clone()).eq(lit(s)),
                AnyValue::Boolean(i) => col(col_name.clone()).eq(lit(i)),

                AnyValue::UInt8(i) => col(col_name.clone()).eq(lit(i)),
                AnyValue::UInt16(i) => col(col_name.clone()).eq(lit(i)),
                AnyValue::UInt32(i) => col(col_name.clone()).eq(lit(i)),
                AnyValue::UInt64(i) => col(col_name.clone()).eq(lit(i)),

                AnyValue::Int8(i) => col(col_name.clone()).eq(lit(i)),
                AnyValue::Int16(i) => col(col_name.clone()).eq(lit(i)),
                AnyValue::Int32(i) => col(col_name.clone()).eq(lit(i)),
                AnyValue::Int64(i) => col(col_name.clone()).eq(lit(i)),
                AnyValue::Float64(f) => col(col_name.clone()).eq(lit(f)),
                AnyValue::Float32(f) => col(col_name.clone()).eq(lit(f)),
                AnyValue::Date(d) => col(col_name.clone()).eq(lit(d)),
                AnyValue::Datetime(dt, _tu, _tz) => col(col_name.clone()).eq(lit(dt)),
                _ => {
                    return Err(format!(
                        "Unsupported partition value type for column {}: {:?}",
                        col_name, value
                    )
                    .into());
                }
            };
            partition_filters.push(filter_expr);

            // Create path component for this partition
            let path_component = match value {
                AnyValue::String(s) => format!("{}={}", col_name, s),
                AnyValue::Boolean(i) => format!("{}={}", col_name, i),
                AnyValue::Int8(i) => format!("{}={}", col_name, i),
                AnyValue::Int16(i) => format!("{}={}", col_name, i),
                AnyValue::Int32(i) => format!("{}={}", col_name, i),
                AnyValue::Int64(i) => format!("{}={}", col_name, i),

                AnyValue::UInt8(i) => format!("{}={}", col_name, i),
                AnyValue::UInt16(i) => format!("{}={}", col_name, i),
                AnyValue::UInt32(i) => format!("{}={}", col_name, i),
                AnyValue::UInt64(i) => format!("{}={}", col_name, i),

                AnyValue::Float64(f) => format!("{}={}", col_name, format_partition_float(f)),
                AnyValue::Float32(f) => {
                    format!("{}={}", col_name, format_partition_float(f as f64))
                }
                AnyValue::Date(d) => format!("{}={}", col_name, d),
                AnyValue::Datetime(dt, _tu, _tz) => format!("{}={}", col_name, dt),
                _ => format!("{}={:?}", col_name, value),
            };
            partition_path_parts.push(path_component);
        }

        // Build the full partition path
        let partition_dir = PathBuf::from(path).join(partition_path_parts.join("/"));

        if append_to_partition {
            // Just ensure the directory exists; keep existing files
            std::fs::create_dir_all(&partition_dir).map_err(|e| {
                display(&format!(
                    "Failed to create partition directory {}: {}",
                    partition_dir.display(),
                    e
                ));
                e
            })?;
        } else {
            // Delete existing partition directory if it exists
            if partition_dir.exists() {
                display(&format!(
                    "Removing existing partition: {}",
                    partition_dir.display()
                ));
                std::fs::remove_dir_all(&partition_dir).map_err(|e| {
                    display(&format!(
                        "Failed to remove partition directory {}: {}",
                        partition_dir.display(),
                        e
                    ));
                    e
                })?;
            }
            std::fs::create_dir_all(&partition_dir).map_err(|e| {
                display(&format!(
                    "Failed to create partition directory {}: {}",
                    partition_dir.display(),
                    e
                ));
                e
            })?;
        }

        // Filter data for this partition
        let mut partition_lf = lf.clone();
        for filter_expr in partition_filters {
            partition_lf = partition_lf.filter(filter_expr);
        }

        let mut partition_df = partition_lf.collect().map_err(|e| {
            display(&format!("Error collecting partition data: {}", e));
            e
        })?;

        // Apply compression if requested
        if compress || compress_string {
            let mut down_config = downcast::DowncastConfig::default();
            down_config.check_strings = compress_string;
            down_config.prefer_int_over_float = compress;

            partition_df = downcast::intelligent_downcast_df(partition_df, None, None, down_config)
                .map_err(|e| {
                    display(&format!("Partition downcast/compress error: {}", e));
                    e
                })?;
        }

        // Generate a unique filename for this partition
        let file_idx = if append_to_partition {
            next_partition_file_index(&partition_dir)
        } else {
            0
        };
        let partition_file = partition_dir.join(format!("data_{}.parquet", file_idx));
        let file = File::create(&partition_file).map_err(|e| {
            display(&format!(
                "Partition file create error for {}: {}",
                partition_dir.display(),
                e
            ));
            e
        })?;
        pqo.to_writer(file).finish(&mut partition_df).map_err(|e| {
            display(&format!(
                "Partition write error for {}: {}",
                partition_dir.display(),
                e
            ));
            e
        })?;

        if !quietly {
            _ = display(&format!(
                "Saved partition {}/{}: {}",
                partition_idx + 1,
                total_partitions,
                partition_dir.display()
            ));
        }
    }

    if !quietly {
        _ = display(&format!(
            "All {} partitions saved to {}",
            total_partitions, path
        ));
    }

    Ok(0)
}

pub fn delete_existing_files(path: &str, overwrite_partition: bool) -> i32 {
    if overwrite_partition {
        let path_obj = std::path::Path::new(path);

        if path_obj.is_file() {
            // If it's a .parquet file, delete it
            if path.ends_with(".parquet") {
                if let Err(e) = std::fs::remove_file(path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        display(&format!("Failed to remove parquet file {}: {}", path, e));
                        return 198;
                    }
                }
            }
        } else if path_obj.is_dir() {
            // Only delete if all subdirectories are hive style and all files are .parquet
            if is_hive_style_parquet_directory(&path_obj) {
                if let Err(e) = std::fs::remove_dir_all(path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        display(&format!("Failed to remove directory {}: {}", path, e));
                        return 198;
                    }
                }
            } else {
                display(&format!(
                    "Error: {} is not a hive partition directory, not removed",
                    path
                ));
                return 198;
            }
        }
    }

    0
}

pub fn consolidate_parquet_dir(path: &str) -> Result<i32, Box<dyn Error>> {
    let dir_path = Path::new(path);
    if !dir_path.is_dir() {
        display(&format!("Error: {} is not a directory", path));
        return Ok(198);
    }

    let fragments = match stata_metadata::resolve_parquet_fragments(path) {
        Ok(fragments) => fragments,
        Err(e) => {
            display(&format!("Error resolving consolidation input: {e}"));
            return Ok(198);
        }
    };
    let agreed_metadata = match stata_metadata::validate_stata_metadata_fragments(&fragments) {
        Ok(metadata) => metadata,
        Err(e) => {
            display(&format!("Error validating consolidation input: {e}"));
            return Ok(198);
        }
    };

    let scan_paths = match fragments
        .iter()
        .map(|fragment| PlRefPath::try_from_path(fragment))
        .collect::<PolarsResult<Vec<_>>>()
    {
        Ok(paths) => paths,
        Err(e) => {
            display(&format!("Error preparing consolidation paths: {e}"));
            return Ok(198);
        }
    };

    let scan_args = ScanArgsParquet::default();
    let lf = match LazyFrame::scan_parquet_files(scan_paths.into(), scan_args) {
        Ok(lf) => lf,
        Err(e) => {
            display(&format!("Error scanning parquet files in {}: {}", path, e));
            return Ok(198);
        }
    };

    let mut df = match lf.collect() {
        Ok(df) => df,
        Err(e) => {
            display(&format!("Error collecting parquet data: {}", e));
            return Ok(198);
        }
    };

    let (temp_path, file) = match create_consolidation_temp_file(dir_path) {
        Ok(value) => value,
        Err(e) => {
            display(&format!("Error creating consolidation staging file: {e}"));
            return Ok(198);
        }
    };
    let pqo = parquet_options(
        "zstd",
        None,
        agreed_metadata
            .as_ref()
            .map(|metadata| metadata.key_value_metadata.clone()),
    );
    if let Err(e) = pqo.to_writer(file).finish(&mut df) {
        let _ = std::fs::remove_file(&temp_path);
        display(&format!("Error writing consolidated parquet: {}", e));
        return Ok(198);
    }

    let completed_metadata =
        match stata_metadata::validate_stata_metadata_fragments(&[temp_path.clone()]) {
            Ok(metadata) => metadata,
            Err(e) => {
                let _ = std::fs::remove_file(&temp_path);
                display(&format!("Error validating consolidated parquet: {e}"));
                return Ok(198);
            }
        };
    if let Err(e) = stata_metadata::validate_stata_metadata_compatibility(
        agreed_metadata.as_ref(),
        completed_metadata.as_ref(),
    ) {
        let _ = std::fs::remove_file(&temp_path);
        display(&format!("Consolidated parquet metadata changed: {e}"));
        return Ok(198);
    }
    if let (Some(agreed), Some(completed)) = (agreed_metadata.as_ref(), completed_metadata.as_ref())
    {
        if agreed.envelope_json != completed.envelope_json {
            let _ = std::fs::remove_file(&temp_path);
            display("Consolidated parquet did not preserve the exact Stata metadata envelope");
            return Ok(198);
        }
    }

    let backup_path = match unused_consolidation_backup_path(dir_path) {
        Ok(path) => path,
        Err(e) => {
            let _ = std::fs::remove_file(&temp_path);
            display(&format!("Error preparing consolidation swap: {e}"));
            return Ok(198);
        }
    };
    if let Err(e) = swap_consolidated_output(dir_path, &temp_path, &backup_path) {
        let _ = std::fs::remove_file(&temp_path);
        display(&format!("Error installing consolidated parquet: {e}"));
        return Ok(198);
    }

    Ok(0)
}

fn create_consolidation_temp_file(source: &Path) -> Result<(PathBuf, File), String> {
    let parent = source.parent().unwrap_or_else(|| Path::new("."));
    let name = source
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "Consolidation path has no valid file name".to_string())?;
    for _ in 0..100 {
        let sequence = CONSOLIDATION_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{name}.consolidating.{}.{}.parquet",
            std::process::id(),
            sequence
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("Could not allocate a unique consolidation staging path".to_string())
}

fn unused_consolidation_backup_path(source: &Path) -> Result<PathBuf, String> {
    let parent = source.parent().unwrap_or_else(|| Path::new("."));
    let name = source
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "Consolidation path has no valid file name".to_string())?;
    for _ in 0..100 {
        let sequence = CONSOLIDATION_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{name}.consolidation-backup.{}.{}",
            std::process::id(),
            sequence
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err("Could not allocate a unique consolidation backup path".to_string())
}

fn swap_consolidated_output(source: &Path, staging: &Path, backup: &Path) -> Result<(), String> {
    std::fs::rename(source, backup).map_err(|e| {
        format!(
            "Could not stage original directory {} as {}: {e}",
            source.display(),
            backup.display()
        )
    })?;

    if let Err(install_error) = std::fs::rename(staging, source) {
        return match std::fs::rename(backup, source) {
            Ok(()) => Err(format!(
                "Could not rename staging file {} to {}: {install_error}; original directory restored",
                staging.display(),
                source.display()
            )),
            Err(restore_error) => Err(format!(
                "Could not rename staging file {} to {}: {install_error}; restoring {} also failed: {restore_error}",
                staging.display(),
                source.display(),
                backup.display()
            )),
        };
    }

    std::fs::remove_dir_all(backup).map_err(|e| {
        format!(
            "Consolidated output is installed, but the staged original directory {} could not be removed: {e}",
            backup.display()
        )
    })
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
fn save_no_partition(
    path: &str,
    mut lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    compress: bool,
    compress_string: bool,
    quietly: bool,
    key_value_metadata: Option<KeyValueMetadata>,
) -> Result<i32, Box<dyn Error>> {
    if compress | compress_string {
        let df = match lf.collect() {
            Ok(df_ok) => df_ok,
            Err(e) => {
                display(&format!("Parquet collect error: {}", e));
                return Ok(198);
            }
        };

        let mut down_config = downcast::DowncastConfig::default();
        down_config.check_strings = compress_string;
        down_config.prefer_int_over_float = compress;
        let df = match downcast::intelligent_downcast_df(df, None, None, down_config) {
            Ok(df_ok) => df_ok,
            Err(e) => {
                display(&format!("Parquet downcast/compress error: {}", e));
                return Ok(198);
            }
        };

        lf = df.lazy();
    }

    let pqo = parquet_options(compression, compression_level, key_value_metadata);
    match lf.collect() {
        Err(e) => {
            display(&format!("Parquet collect error: {}", e));
            Ok(198)
        }
        Ok(mut df) => {
            let file = match File::create(path) {
                Ok(f) => f,
                Err(e) => {
                    display(&format!("Parquet file create error: {}", e));
                    return Ok(198);
                }
            };
            if let Err(e) = pqo.to_writer(file).finish(&mut df) {
                display(&format!("Parquet write error: {}", e));
                return Ok(198);
            }

            if !quietly {
                let _ = display(&format!("File saved to {}", path));
            }
            Ok(0)
        }
    }
}

fn parquet_options(
    compression: &str,
    compression_level: Option<usize>,
    key_value_metadata: Option<KeyValueMetadata>,
) -> ParquetWriteOptions {
    let mut pqo = ParquetWriteOptions::default();
    pqo.key_value_metadata = key_value_metadata;
    pqo.compression = match compression {
        "lz4" => ParquetCompression::Lz4Raw,
        "uncompressed" => ParquetCompression::Uncompressed,
        "snappy" => ParquetCompression::Snappy,
        "gzip" => {
            let gzip_level = match compression_level {
                None => None,
                Some(level) => GzipLevel::try_new(level as u8).ok(),
            };

            ParquetCompression::Gzip(gzip_level)
        }
        "lzo" => ParquetCompression::Zstd(None),
        "brotli" => {
            let brotli_level = match compression_level {
                None => None,
                Some(level) => BrotliLevel::try_new(level as u32).ok(),
            };

            ParquetCompression::Brotli(brotli_level)
        }
        _ => {
            let zstd_level = match compression_level {
                None => None,
                Some(level) => ZstdLevel::try_new(level as i32).ok(),
            };

            ParquetCompression::Zstd(zstd_level)
        }
    };

    pqo
}

fn get_rename_list() -> HashMap<PlSmallStr, PlSmallStr> {
    let mut rename_list = HashMap::<PlSmallStr, PlSmallStr>::new();
    let n_rename_str = get_macro(&"n_rename", false, None);

    let n_rename = match n_rename_str.parse::<usize>() {
        Ok(num) => num,
        Err(e) => {
            eprintln!("Failed to parse n_vars '{}' as usize: {}", n_rename_str, e);
            0
        }
    };

    for i in 1..(n_rename + 1) {
        let rename_from = get_macro(&format!("rename_from_{}", i), false, None);
        let rename_to = get_macro(&format!("rename_to_{}", i), false, None);

        rename_list.insert(rename_from.into(), rename_to.into());
    }
    rename_list
}

fn column_info_from_macros(
    n_vars: usize,
    rename_list: HashMap<PlSmallStr, PlSmallStr>,
) -> Vec<StataColumnInfo> {
    let mut column_infos = Vec::with_capacity(n_vars);

    for i in 0..n_vars {
        let name = get_macro(&format!("name_{}", i + 1), false, None);

        let name = match rename_list.get(&PlSmallStr::from(&name)) {
            Some(renamed) => renamed.to_string(), // Change the name to the renamed value
            None => name.clone(),                 // Use the original value
        };

        let dtype = get_macro(&format!("dtype_{}", i + 1), false, None);
        let format = get_macro(&format!("format_{}", i + 1), false, None);
        let str_length_str = get_macro(&format!("str_length_{}", i + 1), false, None);
        let str_length = match str_length_str.parse::<usize>() {
            Ok(num) => num,
            Err(e) => {
                eprintln!(
                    "Failed to parse n_vars '{}' as usize: {}",
                    str_length_str, e
                );
                0
            }
        };

        let stata_col_str = get_macro(&format!("col_{}", i + 1), false, None);
        let stata_col = stata_col_str.parse::<usize>().unwrap_or(0);

        column_infos.push(StataColumnInfo {
            name,
            dtype,
            format,
            str_length,
            stata_col,
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
    #[allow(dead_code)]
    batch_size: usize,
    schema: Schema,
    column_info: Vec<mapping::StataColumnInfo>,
    all_columns: Vec<PlSmallStr>,
    sql_if: Option<String>,
}

impl StataDataScan {
    pub fn new(
        column_info: Vec<mapping::StataColumnInfo>,
        all_columns: Vec<PlSmallStr>,
        batch_size: Option<usize>,
        initial_offset: usize,
        n_rows: usize,
        sql_if: Option<String>,
    ) -> Self {
        let rows_to_read = if n_rows > 0 {
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

    fn schema(&self, _infer_schema_length: Option<usize>) -> Result<Arc<Schema>, PolarsError> {
        Ok(self.schema.clone().into())
    }

    #[allow(unused)]
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        // If no data, return an empty DataFrame
        if self.n_rows == 0 {
            return Ok(DataFrame::empty_with_schema(&self.schema));
        }

        let n_rows = scan_opts.n_rows.unwrap_or(self.n_rows);
        let n_rows = std::cmp::min(n_rows, self.n_rows);

        // Call read_single_batch and handle errors with the ? operator
        let result = read_single_batch(self, scan_opts, 0, n_rows)?;

        // Now handle the Option<DataFrame>
        match result {
            Some(df) => Ok(df),
            None => Ok(DataFrame::empty_with_schema(&self.schema)),
        }
    }

    fn allows_predicate_pushdown(&self) -> bool {
        false
    }
    fn allows_projection_pushdown(&self) -> bool {
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

            //  display(&format!("{}:{}, {}",col_name,dtype,str_length));

            let s_series = if str_length > 0 {
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
            } else {
                let error_found = AtomicBool::new(false);

                let values: Vec<Option<String>> = if parallelize_rows {
                    //  Never parallelize strl reads
                    (0..n_rows_to_read)
                        .map(|row_idx| {
                            let row = offset + row_idx + 1;

                            match stata_interface::read_string_strl(col_idx + 1, row) {
                                Ok(val) => Some(val),
                                Err(_) => {
                                    error_found.store(true, Ordering::Relaxed);
                                    display(
                                        &format!("{} ({},{}): binary value found where string expected in strl variable, saving as blank",
                                        col_name,
                                        row,
                                        col_idx + 1
                                    ));
                                    None
                                }
                            }
                        })
                        .collect()
                } else {
                    // Process rows sequentially
                    (0..n_rows_to_read)
                        .map(|row_idx| {
                            let row = offset + row_idx + 1;

                            match stata_interface::read_string_strl(col_idx + 1, row) {
                                Ok(val) => Some(val),
                                Err(_) => {
                                    display(
                                        &format!("{} ({},{}): binary value found where string expected in strl variable, saving as blank",
                                        col_name,
                                        row,
                                        col_idx + 1
                                    ));
                                    None
                                }
                            }
                        })
                        .collect()
                };

                if error_found.load(Ordering::Relaxed) {
                    display(
                        &format!("*****{}: binary value(s) found where string expected in strl variable, saving as blank*****",
                        col_name
                    ));
                }

                Series::new(col_name.clone(), values)
            };

            s_series
        }
        DataType::Boolean => {
            let values =
                process_numeric_data::<bool>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Int8 => {
            let values =
                process_numeric_data::<i8>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Int16 => {
            let values =
                process_numeric_data::<i16>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Int32 => {
            let values =
                process_numeric_data::<i32>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Float32 => {
            let values =
                process_numeric_data::<f32>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Float64 => {
            let values =
                process_numeric_data::<f64>(col_idx, n_rows_to_read, offset, parallelize_rows);
            Series::new(col_name.clone(), values)
        }
        DataType::Datetime(TimeUnit::Milliseconds, _) => {
            let values = process_numeric_data::<DatetimeProcess>(
                col_idx,
                n_rows_to_read,
                offset,
                parallelize_rows,
            );
            // Convert the DatetimeProcess wrapper to i64 values
            let i64_values: Vec<Option<i64>> =
                values.into_iter().map(|opt| opt.map(|dm| dm.0)).collect();
            Series::new(col_name.clone(), i64_values)
                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?
        }
        DataType::Time => {
            let values = process_numeric_data::<TimeProcess>(
                col_idx,
                n_rows_to_read,
                offset,
                parallelize_rows,
            );
            // Convert the TimeProcess wrapper to i64 values
            let i64_values: Vec<Option<i64>> =
                values.into_iter().map(|opt| opt.map(|tm| tm.0)).collect();
            Series::new(col_name.clone(), i64_values).cast(&DataType::Time)?
        }
        DataType::Date => {
            let values = process_numeric_data::<DateProcess>(
                col_idx,
                n_rows_to_read,
                offset,
                parallelize_rows,
            );
            // Convert the DateProcess wrapper to i32 values
            let i32_values: Vec<Option<i32>> =
                values.into_iter().map(|opt| opt.map(|dv| dv.0)).collect();
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
    _scan_opts: AnonymousScanArgs,
    offset: usize,
    n_rows: usize,
) -> PolarsResult<Option<DataFrame>> {
    // Calculate how many rows to read in this batch
    let rows_remaining = sds.n_rows - offset;
    let n_rows_to_read = std::cmp::min(n_rows, rows_remaining);

    // Process columns sequentially, rows in parallel
    let columns_result: PolarsResult<Vec<Series>> = sds
        .all_columns
        .iter()
        .enumerate()
        .map(|(varlist_idx, col_name)| {
            // stata_col is the 1-based column position in the full dataset.
            // Fall back to the varlist position+1 when stata_col is 0 (unset/old ado).
            let col_idx = match sds.column_info.get(varlist_idx) {
                Some(ci) if ci.stata_col > 0 => ci.stata_col - 1,
                _ => varlist_idx,
            };
            match process_column(
                col_idx,
                col_name,
                n_rows_to_read,
                offset,
                true,
                &sds.schema,
                &sds.column_info,
            )? {
                Some(series) => Ok(series),
                None => Err(PolarsError::ComputeError(
                    format!("Failed to process column: {}", col_name).into(),
                )),
            }
        })
        .collect();

    let columns = columns_result?;

    // Return the DataFrame built from columns
    let columns: Vec<Column> = columns.into_iter().map(Column::from).collect();
    let mut df = DataFrame::new_infer_height(columns)?.lazy();

    if let Some(sql_if) = &sds.sql_if {
        if !sql_if.is_empty() {
            let mut ctx = SQLContext::new();
            ctx.register("df", df);

            df = ctx
                .execute(&format!("select * from df where {}", sql_if))
                .map_err(|e| {
                    display(&format!("Error in SQL if statement: {}", e));
                    e
                })?;
        }
    }

    Ok(Some(df.collect()?))
}

#[cfg(test)]
mod consolidation_tests {
    use super::*;

    #[test]
    fn failed_consolidation_install_restores_original_directory() {
        let sequence = CONSOLIDATION_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "stata_parquet_io_consolidation_swap_{}_{}",
            std::process::id(),
            sequence
        ));
        let source = root.join("dataset.parquet");
        let staging = root.join("missing-staging.parquet");
        let backup = root.join("backup");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("sentinel.txt"), b"original").unwrap();

        let error = swap_consolidated_output(&source, &staging, &backup).unwrap_err();
        assert!(error.contains("original directory restored"));
        assert_eq!(
            std::fs::read(source.join("sentinel.txt")).unwrap(),
            b"original"
        );
        assert!(!backup.exists());
        assert!(!staging.exists());

        std::fs::remove_dir_all(root).unwrap();
    }
}
