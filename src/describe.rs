use polars::prelude::*;
use polars_sql::SQLContext;
use polars_readstat_rs::{readstat_metadata_json, ReadStatFormat};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use glob::glob;

use crate::fast_cache::{self, FastCacheKey, resolve_varlist};
use crate::mapping::{is_string_type, schema_with_stata_types};
use crate::stata_interface::{
    ST_retcode,
    display,
    set_macro,
};
use crate::utilities::{ms, normalize_path_separators, profile_timing_enabled};

use crate::read::{
    InputFormat,
    cast_catenum_to_string,
    filtered_row_count_readstat_with_sql,
    scan_lazyframe_with_options,
};

use crate::downcast::{
    intelligent_downcast,
    DowncastConfig
};

pub fn file_summary(
    path:&str,
    quietly:bool,
    detailed:bool,
    sql_if:Option<&str>,
    safe_relaxed: bool,
    asterisk_to_variable_name: Option<&str>,
    compress: bool,
    compress_string_to_numeric: bool,
    input_format: InputFormat,
    infer_schema_length: usize,
    parse_dates: bool,
    fast: bool,
    auto_fast_limit_mb: u64,
    columns_varlist: &str,
    drop_list: &str,
) -> i32 {
    let prof = profile_timing_enabled();
    let t_total = Instant::now();
    let mut t_scan = Duration::ZERO;
    let mut t_downcast = Duration::ZERO;
    let mut t_schema = Duration::ZERO;
    let mut t_sql = Duration::ZERO;
    let mut t_cat_cast = Duration::ZERO;
    let mut t_stats = Duration::ZERO;
    let mut t_macros = Duration::ZERO;

    // Always clear any stale cache at the start of describe. This ensures that
    // if a previous describe stored a DataFrame but the ADO code then failed
    // before the read plugin call ran, the stale entry is not held indefinitely.
    // The cache will be repopulated below if effective_fast is true.
    fast_cache::clear();

    // Determine whether to use fast (collect+cache) mode.
    // auto_fast_limit_mb is compared against *estimated RAM*, not on-disk size.
    // Parquet is typically 4–8x compressed on disk, so multiply by an expansion
    // factor so that a 25 MB parquet file counts as ~100 MB of estimated RAM.
    // CSV, SAS, and SPSS are roughly 1:1 (on-disk ≈ in-memory).
    const PARQUET_RAM_EXPANSION: u64 = 4;
    let file_bytes = total_file_size_bytes(path);
    let estimated_ram_mb = match input_format {
        InputFormat::Parquet => (file_bytes / (1024 * 1024)).saturating_mul(PARQUET_RAM_EXPANSION),
        _ => file_bytes / (1024 * 1024),
    };
    let effective_fast = fast || estimated_ram_mb < auto_fast_limit_mb;

    let csv_infer_schema_length = if matches!(input_format, InputFormat::Csv) {
        if infer_schema_length == 0 {
            None
        } else {
            Some(infer_schema_length)
        }
    } else {
        None
    };
    let csv_try_parse_dates = matches!(input_format, InputFormat::Csv) && parse_dates;
    
    let t0 = Instant::now();
    let mut df = match scan_lazyframe_with_options(
        &path,
        safe_relaxed,
        asterisk_to_variable_name,
        input_format,
        false,
        csv_infer_schema_length,
        csv_try_parse_dates,
        None,
    ) {
        Ok(df) => df,
        Err(e) => {
            display(&format!("Error scanning lazyframe: {:?}", e));
            return 198
        },
    };
    if prof {
        t_scan += t0.elapsed();
    }

    //  Set cast macro to empty
    set_macro(
        &"cast_json",
        &"",
        false
    );
    
    if compress | compress_string_to_numeric {
        let t0 = Instant::now();
        let mut downcast_config = DowncastConfig::default();
        downcast_config.check_strings = compress_string_to_numeric;
        downcast_config.prefer_int_over_float = compress;
        
        df = match intelligent_downcast(
            df,
            None,
            None,
            downcast_config
        ) {
            Ok(lf) => lf,
            Err(_e) => {
                display("Error on compress");
                return 198;
            }
        };
        if prof {
            t_downcast += t0.elapsed();
        }
    }
    let t0 = Instant::now();
    let schema = match df.collect_schema() {
        Ok(schema) => schema,
        Err(e) => {
            display(&format!("Error collecting schema: {:?}", e));
            return 198
        },
    };
    if prof {
        t_schema += t0.elapsed();
    }

    // Resolve varlist (with Stata-style wildcards) against the actual schema columns,
    // then apply the drop list. This replicates pq_match_variables in Rust so that
    // the cache key uses exact resolved names and matched_vars is set for the ADO code.
    let schema_col_strs: Vec<&str> = schema.iter_names().map(|s| s.as_str()).collect();
    let matched_cols = match resolve_varlist(columns_varlist, &schema_col_strs, drop_list) {
        Ok(v) => v,
        Err(e) => {
            display(&e);
            return 198 as ST_retcode;
        }
    };
    // Set matched vars macros for use by ADO and read().
    let _ = set_macro("matched_vars", &matched_cols.join(" "), false);
    let _ = set_macro("matched_var_count", &matched_cols.len().to_string(), false);
    for (idx, name) in matched_cols.iter().enumerate() {
        let _ = set_macro(&format!("matched_var_{}", idx + 1), name, false);
    }
    // Sorted list used as the cache key (order-invariant).
    let mut matched_cols_sorted = matched_cols.clone();
    matched_cols_sorted.sort();

    // Build a filtered schema with only matched columns (preserves file order for macros).
    let matched_schema: Schema = Schema::from_iter(
        matched_cols.iter()
            .filter_map(|name| {
                schema.get(name.as_str())
                    .map(|dtype| Field::new(PlSmallStr::from(name.as_str()), dtype.clone()))
            })
    );

    //  display(&format!("schema: {:?}", schema));
    let sql_filter = sql_if.filter(|s| !s.trim().is_empty());
    if let Some(sql) = sql_filter {
        let t0 = Instant::now();
        let mut ctx = SQLContext::new();
        ctx.register("df", df);
        


        df = match ctx.execute(&format!("select * from df where {}", sql)) {
            Ok(lazyframe) => lazyframe,
            Err(e) => {
                display(&format!("Error in SQL if statement: {}", e));
                return 198 as ST_retcode;
            }
        };
        if prof {
            t_sql += t0.elapsed();
        }
    }

    // Project to matched columns AFTER the SQL filter (which may reference any column).
    // For parquet this pushes column pruning into the file reader.
    // For all formats it reduces cast, collect, and stats to matched columns only.
    if matched_cols.len() < schema_col_strs.len() {
        let col_exprs: Vec<Expr> = matched_cols.iter().map(|s| col(s.as_str())).collect();
        df = df.select(col_exprs);
    }

    let t0 = Instant::now();
    df = cast_catenum_to_string(&df).unwrap();
    if prof {
        t_cat_cast += t0.elapsed();
    }

    let t0 = Instant::now();
    let (n_rows, string_lengths) = if effective_fast {
        // Fast path: collect the full DataFrame once, compute stats in memory, cache for read.
        let full_df = match df.clone().collect() {
            Ok(d) => d,
            Err(e) => {
                display(&format!("Error collecting DataFrame for fast cache: {:?}", e));
                fast_cache::clear();
                return 198 as ST_retcode;
            }
        };
        // df was already projected to matched_cols before collect, so full_df has matched cols only.
        let stats = collect_row_count_and_string_lengths_from_df(&full_df, &matched_schema);
        let cache_df = full_df;

        let cache_key = FastCacheKey {
            path: path.to_string(),
            sql_if: sql_filter.unwrap_or("").to_string(),
            columns: matched_cols_sorted,
            format: input_format.as_str().to_string(),
            parse_dates,
            infer_schema_length,
        };
        fast_cache::store(cache_key, cache_df);
        stats
    } else {
        // Non-fast path: cache already cleared at top of function.
        if detailed {
            match collect_row_count_and_string_lengths(&df, &matched_schema) {
                Ok(v) => v,
                Err(e) => {
                    display(&format!("Error collecting detailed describe stats: {:?}", e));
                    return 198 as ST_retcode;
                }
            }
        } else {
            let n_rows = if let Some(sql) = sql_filter {
                if matches!(input_format, InputFormat::Sas | InputFormat::Spss) {
                    filtered_row_count_readstat_with_sql(path, input_format, sql)
                        .unwrap_or_else(|| get_row_count(&df).unwrap())
                } else {
                    get_row_count(&df).unwrap()
                }
            } else {
                get_metadata_row_count(path, input_format).unwrap_or_else(|| get_row_count(&df).unwrap())
            };
            (n_rows, HashMap::new())
        }
    };
    if prof {
        t_stats += t0.elapsed();
    }

    let t0 = Instant::now();
    schema_with_stata_types(
        &df,
        &matched_schema,
        quietly,
        detailed,
        if detailed { Some(&string_lengths) } else { None },
    );

    let n_vars = matched_schema.len();
    
    //  Return scalars of the number of columns and rows 
    let _ = set_macro("n_columns", &(format!("{}",n_vars)), false);
    let _ = set_macro("n_rows", &(format!("{}",n_rows)),false);
    if prof {
        t_macros += t0.elapsed();
    }

    if !quietly {
        display(&"");
        display(&format!("n columns = {}", n_vars));
        display(&format!("n rows = {}", n_rows));
    }

    if prof {
        display(&format!(
            "[pq profile describe format({:?})] total={:.2}ms scan={:.2}ms downcast={:.2}ms schema={:.2}ms sql={:.2}ms cat_cast={:.2}ms stats={:.2}ms macros={:.2}ms detailed={}",
            input_format,
            ms(t_total.elapsed()),
            ms(t_scan),
            ms(t_downcast),
            ms(t_schema),
            ms(t_sql),
            ms(t_cat_cast),
            ms(t_stats),
            ms(t_macros),
            detailed
        ));
    }

    return 0 as ST_retcode;
}

fn collect_row_count_and_string_lengths(
    df: &LazyFrame,
    schema: &Schema,
) -> Result<(usize, HashMap<PlSmallStr, usize>), PolarsError> {
    let string_columns: Vec<PlSmallStr> = schema
        .iter()
        .filter_map(|(name, dtype)| if is_string_type(dtype) { Some(name.clone()) } else { None })
        .collect();

    let mut exprs: Vec<Expr> = Vec::with_capacity(1 + string_columns.len());
    exprs.push(len().alias("__n_rows"));
    for col_name in &string_columns {
        exprs.push(
            col(col_name.as_str())
                .str()
                .len_bytes()
                .max()
                .alias(col_name.as_str()),
        );
    }

    let result_df = df.clone().select(exprs).collect()?;

    let n_rows = result_df
        .column("__n_rows")?
        .get(0)?
        .try_extract::<usize>()
        .map_err(|_| PolarsError::ComputeError("failed to extract __n_rows as usize".into()))?;

    let mut string_lengths = HashMap::new();
    for col_name in string_columns {
        let av = result_df.column(col_name.as_str())?.get(0)?;
        let len = match av {
            AnyValue::UInt32(v) => v as usize,
            AnyValue::UInt64(v) => v as usize,
            AnyValue::Int32(v) => v.max(0) as usize,
            AnyValue::Int64(v) => v.max(0) as usize,
            AnyValue::Null => 0usize,
            _ => 0usize,
        };
        string_lengths.insert(col_name, len);
    }

    Ok((n_rows, string_lengths))
}

fn readstat_format_for_input(input_format: InputFormat) -> Option<ReadStatFormat> {
    match input_format {
        InputFormat::Sas => Some(ReadStatFormat::Sas),
        InputFormat::Spss => Some(ReadStatFormat::Spss),
        _ => None,
    }
}

fn get_metadata_row_count(path: &str, input_format: InputFormat) -> Option<usize> {
    let format = readstat_format_for_input(input_format)?;
    let metadata_json = readstat_metadata_json(path, Some(format)).ok()?;
    let metadata: Value = serde_json::from_str(&metadata_json).ok()?;
    metadata
        .get("row_count")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
}

pub fn get_schema(path:&str) -> PolarsResult<Schema> {
    let mut scan_args = ScanArgsParquet::default();
    scan_args.allow_missing_columns = true;
    scan_args.cache = false;
    let mut df = LazyFrame::scan_parquet(path.into(), scan_args.clone())?;

    let schema = df.collect_schema()?;
    
    Ok(schema.as_ref().clone())
}

pub fn get_row_count(lazy_df: &LazyFrame) -> Result<usize, PolarsError> {
    // Create a new LazyFrame with just the count

    let count_df = lazy_df.clone()
                                .select([len().alias("n_rows")])
                                .collect()
                                .unwrap();

    let count = count_df.column("n_rows").unwrap().get(0).unwrap().try_extract::<usize>().unwrap();
    Ok(count)
}

/// Compute row count and max string lengths from an already-collected DataFrame.
/// No lazy execution or disk I/O — all in memory.
fn collect_row_count_and_string_lengths_from_df(
    df: &DataFrame,
    schema: &Schema,
) -> (usize, HashMap<PlSmallStr, usize>) {
    let n_rows = df.height();

    let mut string_lengths = HashMap::new();
    for (name, dtype) in schema.iter() {
        if is_string_type(dtype) {
            let len = df.column(name.as_str())
                .ok()
                .and_then(|col| col.str().ok().map(|ca| {
                    ca.into_iter()
                        .filter_map(|s| s.map(|s| s.len()))
                        .max()
                        .unwrap_or(0)
                }))
                .unwrap_or(0);
            string_lengths.insert(name.clone(), len);
        }
    }

    (n_rows, string_lengths)
}

/// Sum file sizes for path (supports glob patterns).
fn total_file_size_bytes(path: &str) -> u64 {
    let normalized = normalize_path_separators(path);
    if let Ok(paths) = glob(&normalized) {
        let total: u64 = paths
            .filter_map(|p| p.ok())
            .filter_map(|p| std::fs::metadata(&p).ok())
            .map(|m| m.len())
            .sum();
        if total > 0 { return total; }
    }
    // Fallback: single-file stat (handles non-glob paths that didn't match above)
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(u64::MAX)
}
