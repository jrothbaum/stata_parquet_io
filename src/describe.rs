use polars::prelude::*;
use polars_sql::SQLContext;
use polars_readstat_rs::{readstat_metadata_json, ReadStatFormat};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use glob::glob;

use crate::fast_cache::{self, FastCacheKey, resolve_varlist};
use crate::mapping::{is_string_type, schema_with_stata_types, widen_with_recorded_type, StataType};
use crate::stata_interface::{
    ST_retcode,
    display,
    set_macro,
};
use crate::stata_metadata::read_metadata_validated;
use crate::parquet_stats::integer_ranges_for_path;
use crate::utilities::{ms, normalize_path_separators, profile_timing_enabled};

use crate::read::{
    InputFormat,
    cast_catenum_to_string,
    filtered_row_count_readstat_with_sql,
    scan_lazyframe_with_options,
};

use crate::downcast::{
    apply_user_cast,
    find_optimal_integer_type,
    intelligent_downcast,
    polars_type_to_stata_type,
    validate_user_type,
    DowncastConfig,
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
    user_cast_json: &str,
    binary_to_string: bool,
    cast_strict: bool,
    safe_int64: bool,
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

    set_macro("cast_json", "", false);
    set_macro("pq_user_cast_json", "", false);
    set_macro("pq_cast_strict", if cast_strict { "1" } else { "0" }, false);
    set_macro("pq_cast_error", "", false);

    // Apply user cast (binary_to_string + cast option) BEFORE compress and schema computation
    // so that string lengths, types, and the fast cache all reflect the cast types.
    // Schema is needed regardless of these options to check for Int64/UInt64 precision
    // overflow below, so it's always computed here.
    let scan_schema = match df.collect_schema() {
        Ok(s) => s,
        Err(e) => {
            display(&format!("Error reading schema for cast: {:?}", e));
            return 198;
        }
    };

    let mut cast_map: HashMap<String, String> = HashMap::new();

    if binary_to_string {
        for (name, dtype) in scan_schema.iter() {
            if matches!(dtype, DataType::Binary) {
                cast_map.insert(name.to_string(), "string".to_string());
            }
        }
    }

    if !user_cast_json.is_empty() {
        let col_to_type: HashMap<String, Value> = match serde_json::from_str(user_cast_json) {
            Ok(m) => m,
            Err(e) => {
                let msg = format!("cast: invalid JSON: {}", e);
                display(&msg);
                set_macro("pq_cast_error", &msg, false);
                return 198;
            }
        };
        for (col_name, type_val) in col_to_type {
            let type_str = match type_val.as_str() {
                Some(s) => s.to_lowercase(),
                None => {
                    let msg = format!("cast: type for '{}' must be a string", col_name);
                    display(&msg);
                    set_macro("pq_cast_error", &msg, false);
                    return 198;
                }
            };
            if let Err(e) = validate_user_type(&type_str) {
                let msg = format!("cast({}): {}", col_name, e);
                display(&msg);
                set_macro("pq_cast_error", &msg, false);
                return 198;
            }
            if scan_schema.get(col_name.as_str()).is_none() {
                let msg = format!("cast: column '{}' not found in file", col_name);
                display(&msg);
                set_macro("pq_cast_error", &msg, false);
                return 198;
            }
            cast_map.insert(col_name, type_str);
        }
    }

    // Stata has no native 64-bit integer type, so Int64/UInt64 columns are stored as
    // doubles. Doubles only preserve integer precision up to +/-2^53, so values
    // outside that range silently collide (distinct ids can become indistinguishable).
    // Columns the user already cast explicitly above are left alone.
    let int64_candidates: Vec<PlSmallStr> = scan_schema
        .iter()
        .filter_map(|(name, dtype)| {
            if matches!(dtype, DataType::Int64 | DataType::UInt64)
                && !cast_map.contains_key(name.as_str())
            {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    if !int64_candidates.is_empty() {
        match find_int64_precision_overflow_columns(&df, &int64_candidates) {
            Ok(overflow_cols) if !overflow_cols.is_empty() => {
                if safe_int64 {
                    for name in &overflow_cols {
                        cast_map.insert(name.to_string(), "string".to_string());
                    }
                } else {
                    let col_list = overflow_cols
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let msg = format!(
                        "Column(s) {} contain Int64/UInt64 values outside +/-2^53 \
                         (9,007,199,254,740,992). Stata has no 64-bit integer type, so these \
                         values would silently lose precision as a double (distinct values can \
                         become indistinguishable). Use cast({{\"col\":\"string\"}}) to load the \
                         affected column(s) as strings, or pass the safe_int64 option to do this \
                         automatically.",
                        col_list
                    );
                    display(&msg);
                    set_macro("pq_cast_error", &msg, false);
                    return 198;
                }
            }
            Ok(_) => {}
            Err(e) => {
                display(&format!("Error checking Int64 precision range: {:?}", e));
                return 198;
            }
        }
    }

    if !cast_map.is_empty() {
        let combined_json = serde_json::to_string(&cast_map).unwrap_or_default();
        df = match apply_user_cast(df, &combined_json, cast_strict) {
            Ok(lf) => lf,
            Err(e) => {
                let msg = format!("cast failed: {}", e);
                display(&msg);
                set_macro("pq_cast_error", &msg, false);
                return 198;
            }
        };
        set_macro("pq_user_cast_json", &combined_json, false);
        // Invalidate cached data that doesn't have this cast applied
        fast_cache::clear();
    }

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

    // `compress` already ran a real scan (intelligent_downcast, above) to
    // pick the tightest safe integer DataType per column - that's strictly
    // more authoritative than footer stats, so map it directly (Int8->Byte,
    // not the conservative Int8->Int upcast `map_polars_to_stata` uses when
    // it has no evidence about the actual data). Without this, `compress`'s
    // narrowing was silently re-widened one level by that same default
    // mapping when schema_with_stata_types ran on the already-narrowed
    // schema. Parquet without `compress` uses the cheap footer-stats path.
    let type_overrides = if compress {
        direct_integer_type_overrides(&matched_schema, &cast_map)
    } else if matches!(input_format, InputFormat::Parquet) {
        safe_integer_type_overrides(path, &matched_schema, &cast_map)
    } else {
        HashMap::new()
    };

    let t0 = Instant::now();
    schema_with_stata_types(
        &df,
        &matched_schema,
        quietly,
        detailed,
        if detailed { Some(&string_lengths) } else { None },
        if type_overrides.is_empty() { None } else { Some(&type_overrides) },
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

/// Maps every already-narrowed integer column straight to its Stata type
/// (Int8->Byte, Int16->Int, Int32->Long - no upcast), for use after
/// `compress` has already picked that DataType via a real min/max scan.
/// Unlike `safe_integer_type_overrides`, no independent verification is
/// needed here: `intelligent_downcast` only ever narrows to a type the
/// actual observed values already fit. Excludes: Int64/UInt64 (Stata has no
/// native 64-bit integer - stays on the existing double/safe_int64-string
/// path unchanged) and any column the caller already cast explicitly
/// (`cast()` or the safe_int64 auto-promotion), whose requested type must
/// win outright rather than be silently re-decided here.
fn direct_integer_type_overrides(
    schema: &Schema,
    cast_map: &HashMap<String, String>,
) -> HashMap<String, StataType> {
    schema
        .iter()
        .filter_map(|(name, dtype)| {
            if cast_map.contains_key(name.as_str()) {
                return None;
            }
            if !matches!(
                dtype,
                DataType::Int8 | DataType::Int16 | DataType::Int32 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32
            ) {
                return None;
            }
            let type_str = polars_type_to_stata_type(&format!("{:?}", dtype).to_lowercase());
            StataType::from_str(type_str).map(|t| (name.to_string(), t))
        })
        .collect()
}

/// PARQUET, non-compress reads only: a safe, tighter-than-default integer
/// type per column, computed from Parquet row-group footer statistics (no
/// data scan) and optionally widened by a Stata type recorded in the file's
/// own metadata footer. A column appears here ONLY when the footer stats
/// independently verify the range is safe - a stale, wrong, or absent
/// recorded type can never narrow a column below that verified floor, and a
/// column whose stats can't be cheaply verified is simply absent, leaving it
/// to fall back to the existing conservative default mapping in
/// `map_polars_to_stata` (see mapping::schema_with_stata_types). Excludes:
/// Int64/UInt64 (Stata has no native 64-bit integer - the existing
/// double/safe_int64-string handling in `find_int64_precision_overflow_columns`
/// is a separate, deliberate safety mechanism this doesn't touch) and any
/// column the caller already cast explicitly, same reasoning as above.
fn safe_integer_type_overrides(
    path: &str,
    schema: &Schema,
    cast_map: &HashMap<String, String>,
) -> HashMap<String, StataType> {
    let integer_cols: Vec<String> = schema
        .iter()
        .filter_map(|(name, dtype)| {
            if cast_map.contains_key(name.as_str()) {
                return None;
            }
            if matches!(
                dtype,
                DataType::Int8 | DataType::Int16 | DataType::Int32 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32
            ) {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect();

    if integer_cols.is_empty() {
        return HashMap::new();
    }

    let verified_ranges = integer_ranges_for_path(path, &integer_cols);
    if verified_ranges.is_empty() {
        return HashMap::new();
    }

    // Best-effort: a conflicting or unreadable metadata footer just means no
    // recorded type to widen with - never an error surfaced from `describe`,
    // which has never required metadata to be present or consistent.
    let recorded_types: HashMap<String, StataType> = read_metadata_validated(path)
        .ok()
        .flatten()
        .map(|envelope| {
            envelope
                .variables
                .into_iter()
                .filter_map(|(name, meta)| {
                    meta.stata_type
                        .as_deref()
                        .and_then(StataType::from_str)
                        .map(|t| (name, t))
                })
                .collect()
        })
        .unwrap_or_default();

    let mut overrides = HashMap::with_capacity(verified_ranges.len());
    for (name, (min_val, max_val)) in verified_ranges {
        let optimal_dtype = find_optimal_integer_type(min_val, max_val, true);
        let type_str = polars_type_to_stata_type(&format!("{:?}", optimal_dtype).to_lowercase());
        let Some(verified_safe) = StataType::from_str(type_str) else { continue };
        let recorded = recorded_types.get(&name).copied();
        overrides.insert(name, widen_with_recorded_type(verified_safe, recorded));
    }
    overrides
}

/// Stata doubles only preserve integer precision up to 2^53. Return the subset of
/// `candidates` (Int64/UInt64 columns) whose min or max, cast to f64, falls outside
/// that range. Uses a single min/max aggregation over just the candidate columns
/// rather than collecting the full frame.
fn find_int64_precision_overflow_columns(
    df: &LazyFrame,
    candidates: &[PlSmallStr],
) -> Result<Vec<PlSmallStr>, PolarsError> {
    const MAX_SAFE_INT_AS_DOUBLE: f64 = 9_007_199_254_740_992.0; // 2^53

    let stats_exprs: Vec<Expr> = candidates
        .iter()
        .flat_map(|name| {
            vec![
                col(name.as_str()).cast(DataType::Float64).min().alias(&format!("{}_min", name)),
                col(name.as_str()).cast(DataType::Float64).max().alias(&format!("{}_max", name)),
            ]
        })
        .collect();

    let stats_df = df.clone().select(stats_exprs).collect()?;

    let mut overflow_cols = Vec::new();
    for name in candidates {
        let min_val = stats_df.column(&format!("{}_min", name))?.f64()?.get(0);
        let max_val = stats_df.column(&format!("{}_max", name))?.f64()?.get(0);
        let overflows = min_val.map(|v| v < -MAX_SAFE_INT_AS_DOUBLE).unwrap_or(false)
            || max_val.map(|v| v > MAX_SAFE_INT_AS_DOUBLE).unwrap_or(false);
        if overflows {
            overflow_cols.push(name.clone());
        }
    }

    Ok(overflow_cols)
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
