use std::collections::HashMap;
use serde_json::{Map, Value};


use polars::prelude::*;
use stata_sys::{
    display,
    set_macro
};


pub struct DowncastConfig {
    pub check_strings: bool,
    pub prefer_int_over_float: bool,
}

impl Default for DowncastConfig {
    fn default() -> Self {
        Self {
            check_strings: true,
            prefer_int_over_float: true,
        }
    }
}

/// Efficiently downcast columns: strings->numeric, floats->ints, then shrink integers
pub fn intelligent_downcast(
    mut df: LazyFrame,
    cols: Option<Vec<String>>,
    cols_not_boolean: Option<Vec<String>>,
    config: DowncastConfig,
) -> PolarsResult<LazyFrame> {
    
    let schema = df.collect_schema()?;
    let columns_to_process = cols.unwrap_or_else(|| {
        schema.iter().map(|(name, _)| name.to_string()).collect()
    });

    let columns_not_boolean = cols_not_boolean.as_deref().unwrap_or(&[]);

    let mut df = df;
    
    
    // Step 1: Handle string to numeric conversions
    if config.check_strings {
        df = convert_numeric_strings(
            df,
            &schema,
            &columns_to_process
        )?;
    }
    
    // Step 2: Convert floats to integers where possible
    if config.prefer_int_over_float {
        df = convert_floats_to_integers(
            df,
            &schema,
            &columns_to_process
        )?;
    }
    
    // Step 3: Let Polars handle integer downcasting efficiently
    df = safe_shrink_integers(
        df, 
        &columns_to_process,
        &columns_not_boolean,
    )?;

    let schema_new = df.collect_schema()?;
    let json_return = match build_type_mapping(
        &schema,
        &schema_new,
        &columns_to_process,
    ) {
        Ok(json_out) => json_out,
        Err(e) => {
            display(&format!("Failed to serialize compress type mapping: {}", e));
            return Err(e);
        }
    };

    set_macro(
        &"cast_json",
        &json_return,
        false
    );

    
    Ok(df)
}


/// Safely shrink integer columns by checking actual min/max values
fn safe_shrink_integers(
    mut df: LazyFrame,
    columns: &[String],
    columns_not_boolean: &[String],
) -> PolarsResult<LazyFrame> {
    
    let schema = df.collect_schema()?;
    let int_columns: Vec<String> = columns
        .iter()
        .filter(|col| {
            matches!(
                schema.get(col.as_str()),
                Some(DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 |
                     DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8)
            )
        })
        .cloned()
        .collect();
    
    if int_columns.is_empty() {
        return Ok(df);
    }
    
    // Get min/max for all integer columns at once
    let stats_exprs: Vec<Expr> = int_columns
        .iter()
        .flat_map(|col_name| {
            vec![
                col(col_name).min().cast(DataType::Int64).alias(&format!("{}_min", col_name)),
                col(col_name).max().cast(DataType::Int64).alias(&format!("{}_max", col_name)),
            ]
        })
        .collect();
    
    let stats_df = df.clone().select(stats_exprs).collect()?;
    
    // Determine optimal type for each column
    let mut cast_exprs = Vec::new();
    
    for col_name in &int_columns {
        let min_col = format!("{}_min", col_name);
        let max_col = format!("{}_max", col_name);
        
        let min_val = stats_df.column(&min_col)?.i64()?.get(0);
        let max_val = stats_df.column(&max_col)?.i64()?.get(0);
        
        if let (Some(min), Some(max)) = (min_val, max_val) {
            // Check if this column should NOT be compressed to a boolean
            let not_boolean = columns_not_boolean.contains(col_name);
            
            let optimal_type = find_optimal_integer_type(min, max,not_boolean);
            let current_type = schema.get(col_name.as_str()).unwrap();
            
            // Only cast if it's actually a smaller/better type
            if optimal_type != *current_type && is_better_type(&optimal_type, current_type) {
                cast_exprs.push(
                    col(col_name).cast(optimal_type).alias(col_name)
                );
            }
        }
    }
    
    let mut result_df = df;
    
    // Apply integer casts first
    if !cast_exprs.is_empty() {
        result_df = result_df.with_columns(cast_exprs);
    }
    
    Ok(result_df)
}


/// Find the smallest integer type that can hold the given range
fn find_optimal_integer_type(
    min_val: i64,
    max_val: i64,
    not_boolean: bool,
) -> DataType {
    // Check if values fit in smaller types, considering both signed and unsigned
    
    // Boolean (0, 1)
    if min_val >= 0 && max_val <= 1 && !not_boolean {
        return DataType::Boolean;
    }
    
    // // UInt8 (0 to 255)
    // if min_val >= 0 && max_val <= 255 {
    //     return DataType::UInt8;
    // }

    // Int8: Stata byte valid range -127 to 100 (both ends truncated for stata)
    if min_val >= -127 && max_val <= 100 {
        return DataType::Int8;
    }

    // // UInt16 (0 to 65,535)
    // if min_val >= 0 && max_val <= 65535 {
    //     return DataType::UInt16;
    // }

    // Int16: Stata int valid range -32,767 to 32,740 (both ends truncated for stata)
    if min_val >= -32767 && max_val <= 32740 {
        return DataType::Int16;
    }

    // // UInt32 (0 to 4,294,967,295)
    // if min_val >= 0 && max_val <= 4294967295 {
    //     return DataType::UInt32;
    // }

    // Int32: Stata long valid range -2,147,483,647 to 2,147,483,620 (both ends truncated for stata)
    if min_val >= -2147483647 && max_val <= 2147483620 {
        return DataType::Int32;
    }
    
    // // UInt64 (0 to 18,446,744,073,709,551,615)
    // if min_val >= 0 {
    //     return DataType::UInt64;
    // }
    
    // Fall back to Int64
    DataType::Int64
}

/// Check if the new type is actually better (smaller) than the current type
fn is_better_type(new_type: &DataType, current_type: &DataType) -> bool {
    let type_size = |dt: &DataType| match dt {
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 => 16,
        DataType::Int32 | DataType::UInt32 => 32,
        DataType::Int64 | DataType::UInt64 => 64,
        _ => 64, // Default to largest
    };
    
    type_size(new_type) < type_size(current_type)
}


/// Build a JSON mapping of type changes grouped by target type
/// Example output: {"int8":["var1","var2"],"float64":["var3"],"int16":["var4"]}
fn build_type_mapping(
    schema_original: &Schema,
    schema_new: &Schema, 
    columns: &[String]
) -> PolarsResult<String> {
    
    let mut type_groups: HashMap<String, Vec<String>> = HashMap::new();
    
    for col_name in columns {
        if let (Some(original_type), Some(new_type)) = (
            schema_original.get(col_name.as_str()),
            schema_new.get(col_name.as_str())
        ) {
            // Only include columns that actually changed type
            if original_type != new_type {
                //  display(&format!("Casting {} from {} to {}", col_name,original_type,new_type));
                let type_key = format!("{:?}", new_type).to_lowercase();
                type_groups
                    .entry(type_key)
                    .or_insert_with(Vec::new)
                    .push(col_name.clone());
            }
        }
    }
    
    // Convert to JSON
    let json_map: Map<String, Value> = type_groups
        .into_iter()
        .map(|(type_name, columns)| {
            (type_name, Value::Array(
                columns.into_iter().map(Value::String).collect()
            ))
        })
        .collect();
    
    Ok(serde_json::to_string(&json_map).unwrap_or_else(|_| "{}".to_string()))
}

/// Detect and convert string columns that contain only numeric values
fn convert_numeric_strings(
    df: LazyFrame,
    schema:&Schema, 
    columns: &[String]
) -> PolarsResult<LazyFrame> {
    
    let string_columns: Vec<String> = columns
        .iter()
        .filter(|col| {
            matches!(
                schema.get(col.as_str()), 
                Some(DataType::String)
            )
        })
        .cloned()
        .collect();
    
    if string_columns.is_empty() {
        return Ok(df);
    }
    
    // Build expressions to check which string columns are all numeric
    let check_exprs: Vec<Expr> = string_columns
        .iter()
        .map(|col_name| {
            let original_col = col(col_name);
            let as_float = original_col.clone().cast(DataType::Float64);
            
            // Check if casting preserves null count (no new nulls from failed parsing)
            original_col.null_count()
                .eq(as_float.null_count())
                .alias(&format!("{}_can_convert", col_name))
        })
        .collect();
    
    // Evaluate which columns can be converted
    let check_results = df.clone().select(check_exprs).collect()?;
    
    // Collect columns that can be safely converted
    let mut columns_to_convert = Vec::new();
    for col_name in &string_columns {
        let check_col = format!("{}_can_convert", col_name);
        let can_convert = check_results
            .column(&check_col)?
            .bool()?
            .get(0)
            .unwrap_or(false);
        
        if can_convert {
            columns_to_convert.push(col_name.clone());
        }
    }
    
    // Apply simple casts to convertible columns
    if !columns_to_convert.is_empty() {
        let cast_exprs: Vec<Expr> = columns_to_convert
            .iter()
            .map(|col_name| col(col_name).cast(DataType::Float64).alias(col_name))
            .collect();
        
        Ok(df.with_columns(cast_exprs))
    } else {
        Ok(df)
    }
}
/// Convert float columns to integers where all values are whole numbers
fn convert_floats_to_integers(
    df: LazyFrame,
    schema:&Schema,
    columns: &[String]
) -> PolarsResult<LazyFrame> {

    let float_columns: Vec<String> = columns
        .iter()
        .filter(|col| {
            matches!(
                schema.get(col.as_str()),
                Some(DataType::Float64)
            )
        })
        .cloned()
        .collect();
    
    if float_columns.is_empty() {
        return Ok(df);
    }
    
    // Build expressions to check which columns can be converted
    let check_exprs: Vec<Expr> = float_columns
        .iter()
        .map(|col_name| {
            let original_col = col(col_name);
            let as_int = original_col.clone().cast(DataType::Int64);
            let back_to_float = as_int.cast(DataType::Float64);

            // Check if round-trip float64->int64->float64 preserves all non-null values
            original_col.eq(back_to_float).all(true).alias(&format!("{}_can_convert", col_name))
        })
        .collect();
    
    // Evaluate which columns can be converted
    let check_results = df.clone().select(check_exprs).collect()?;
    
    // Collect columns that can be safely converted
    let mut columns_to_convert = Vec::new();
    for col_name in &float_columns {
        let check_col = format!("{}_can_convert", col_name);
        let can_convert = check_results
            .column(&check_col)?
            .bool()?
            .get(0)
            .unwrap_or(false);
        
        if can_convert {
            columns_to_convert.push(col_name.clone());
        }
    }
    
    // Apply simple casts to convertible columns
    if !columns_to_convert.is_empty() {
        let cast_exprs: Vec<Expr> = columns_to_convert
            .iter()
            .map(|col_name| col(col_name).cast(DataType::Int64).alias(col_name))
            .collect();
        
        Ok(df.with_columns(cast_exprs))
    } else {
        Ok(df)
    }
}

/// Convenience function for DataFrames
pub fn intelligent_downcast_df(
    df: DataFrame,
    cols: Option<Vec<String>>,
    cols_not_boolean: Option<Vec<String>>,
    config: DowncastConfig,
) -> PolarsResult<DataFrame> {
    intelligent_downcast(
        df.lazy(), 
        cols, 
        cols_not_boolean,
        config)?.collect()
}


/// Apply a previously captured type mapping to a new LazyFrame
/// Only casts columns that actually exist in the dataframe, silently skips missing ones
pub fn apply_cast(
    mut df: LazyFrame,
    type_mapping_json: &str
) -> PolarsResult<LazyFrame> {
    
    let type_mapping: Map<String, Value> = serde_json::from_str(type_mapping_json)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid JSON: {}", e).into()))?;
    
    // Get the schema to check which columns exist
    let schema = df.collect_schema()?;
    let mut cast_exprs = Vec::new();
    
    for (type_str, columns_value) in type_mapping {
        if let Value::Array(columns_array) = columns_value {
            let target_type:Option<DataType> = match parse_data_type(&type_str) {
                Ok(valid_type) => Some(valid_type),
                Err(e) => {
                    display(&format!("Invalid cast type: {}, {}",type_str,e));
                    None
                }
            };
            
            match target_type {
                Some(valid_type) => {
                    for column_value in columns_array {
                        if let Value::String(col_name) = column_value {
                            // Check if column exists in the dataframe
                            if schema.get(&col_name).is_some() {
                                //  display(&format!("casting {} to {}",col_name,valid_type));
                                cast_exprs.push(
                                    col(&col_name).cast(valid_type.clone()).alias(&col_name)
                                );
                            }
                            // Silently skip missing columns
                        }
                    }
                },
                None => {
                    //  Do nothing
                }
                
            }
            
        }
    }
    
    if cast_exprs.is_empty() {
        Ok(df)
    } else {
        Ok(df.with_columns(cast_exprs))
    }
}

/// Parse string representation back to DataType
fn parse_data_type(type_str: &str) -> PolarsResult<DataType> {
    match type_str {
        "boolean" => Ok(DataType::Boolean),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16), 
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "string" => Ok(DataType::String),
        _ => Err(PolarsError::ComputeError(
            format!("Unknown data type: {}", type_str).into()
        ))
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
    
//     #[test]
//     fn test_intelligent_downcast() {
//         let df = df! {
//             "string_ints" => ["1", "2", "3", "127"],
//             "string_floats" => ["1.5", "2.7", "3.14"],
//             "string_mixed" => ["1", "hello", "3"],
//             "float_ints" => [1.0, 2.0, 3.0, 127.0],
//             "real_floats" => [1.1, 2.2, 3.3],
//             "big_ints" => [1000i64, 2000, 3000],
//         }.unwrap();
        
//         let config = DowncastConfig::default();
//         let result = intelligent_downcast_df(df, None, config).unwrap();
        
//         println!("Result dtypes: {:?}", result.dtypes());
        
//         // string_ints should become Int8 (via string->int->shrink)
//         // float_ints should become Int8 (via float->int->shrink)  
//         // big_ints should become Int16 (via shrink)
//         // string_mixed should stay String
//         // real_floats should stay Float
//     }
    
//     #[test]
//     fn test_mixed_string_columns() {
//         let df = df! {
//             "all_numeric_strings" => ["1", "2", "3", "127"],      // Should convert
//             "mixed_strings" => ["1", "hello", "3"],               // Should stay string
//             "empty_and_numeric" => [Some("1"), None, Some("3")],  // Should convert
//             "text_only" => ["hello", "world", "test"],            // Should stay string
//         }.unwrap();
        
//         let config = DowncastConfig::default();
//         let result = intelligent_downcast_df(df, None, config).unwrap();
        
//         // all_numeric_strings should become Int8
//         // mixed_strings should stay String (because of "hello")
//         // empty_and_numeric should become Int8 (nulls are ignored)
//         // text_only should stay String
        
//         assert!(matches!(result.column("mixed_strings").unwrap().dtype(), DataType::String));
//         assert!(matches!(result.column("text_only").unwrap().dtype(), DataType::String));
//         println!("Mixed strings test result: {:?}", result.dtypes());
//     }
// }
