use std::collections::{
    HashMap,
    HashSet
};
use serde::{Serialize, Deserialize};
use polars::prelude::*;

use crate::stata_interface::{
    display,
    set_macro,
};


// Enum representing Stata data types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StataType {
    Byte,    // 1-byte signed integer
    Int,     // 2-byte signed integer
    Long,    // 4-byte signed integer
    Float,   // 4-byte floating point
    Double,  // 8-byte floating point
    Date,    // Stata date format
    Time,    // Stata time format
    DateTime, // Stata datetime format
    String,     //  Regular string
    Strl,       //  Long strings
    Binary,     //  Binary data (actually strl in stata, also)
}


impl StataType {
    pub fn to_string(&self) -> &'static str {
        match self {
            StataType::Byte => "byte",
            StataType::Int => "int",
            StataType::Long => "long",
            StataType::Float => "float",
            StataType::Double => "double",
            StataType::Date => "date",
            StataType::Time => "time",
            StataType::DateTime => "datetime",
            StataType::String => "string",
            StataType::Strl => "strl",
            StataType::Binary => "binary",
        }
    }

    pub fn from_str(s: &str) -> Option<StataType> {
        match s.to_lowercase().as_str() {
            "byte" => Some(StataType::Byte),
            "int" => Some(StataType::Int),
            "long" => Some(StataType::Long),
            "float" => Some(StataType::Float),
            "double" => Some(StataType::Double),
            "date" => Some(StataType::Date),
            "time" => Some(StataType::Time),
            "datetime" => Some(StataType::DateTime),
            "string" => Some(StataType::String),
            "strl" => Some(StataType::Strl),
            "binary" => Some(StataType::Binary),
            _ => None,
        }
    }

    /// Rank within the plain integer family (byte < int < long < double);
    /// `None` for anything outside it (float, string, date/time, ...).
    fn integer_rank(&self) -> Option<u8> {
        match self {
            StataType::Byte => Some(1),
            StataType::Int => Some(2),
            StataType::Long => Some(3),
            StataType::Double => Some(4),
            _ => None,
        }
    }
}

/// Widens `verified_safe` (an integer type already confirmed safe by Parquet
/// footer statistics) with an optionally recorded metadata type - returns
/// whichever is WIDER, never narrower than what's verified. `recorded` is
/// ignored outright when it isn't itself a plain integer-family type: an
/// incompatible category is a sign the recorded value doesn't describe this
/// column's current data, so it can't be trusted to widen anything either.
pub fn widen_with_recorded_type(verified_safe: StataType, recorded: Option<StataType>) -> StataType {
    match recorded.and_then(|t| t.integer_rank()) {
        Some(recorded_rank) => {
            let safe_rank = verified_safe.integer_rank().unwrap_or(4);
            if recorded_rank > safe_rank { recorded.unwrap() } else { verified_safe }
        }
        None => verified_safe,
    }
}

/// Returns true for Polars string-like types that need length measurement.
pub fn is_string_type(dtype: &DataType) -> bool {
    matches!(dtype, DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _))
}

// Function to map Polars DataType to StataType
pub fn map_polars_to_stata(
    dtype: &DataType,
    str_length: usize,
) -> StataType {

    match dtype {
        //  Boolean
        DataType::Boolean => StataType::Byte,
        // Integers
        DataType::Int8 => StataType::Int,    // byte's reserved range (101-127) is below Int8 max
        DataType::Int16 => StataType::Long, // int's reserved range (32741-32767) is below Int16 max
        DataType::Int32 => StataType::Double, // long's reserved range (2147483621-2147483647) is below Int32 max
        DataType::Int64 => StataType::Double, // Only double can contain the set of possible values
        DataType::UInt8 => StataType::Int, 
        DataType::UInt16 => StataType::Long,
        DataType::UInt32 => StataType::Long,
        DataType::UInt64 => StataType::Double,
        
        // Floating point
        DataType::Float32 => StataType::Float,
        DataType::Float64 => StataType::Double,
        
        // Date/Time types
        DataType::Date => StataType::Date,
        DataType::Time => StataType::Time,
        DataType::Datetime(_, _) => StataType::DateTime,
        DataType::String 
        | DataType::Categorical(_,_)
        | DataType::Enum(_,_) => {
            if str_length > 2045 {
                StataType::Strl
            } else {
                StataType::String
            }
        },
        DataType::Binary => StataType::Binary,
        
        // Other types default to Double (most flexible numeric type)
        _ => {
            display(&format!("Undefined parquet type: {}", dtype));

            StataType::Double
        },
    }
}



pub fn map_stata_to_polars(
    stata_type: &StataType
) -> DataType {
    match stata_type {
        //  Boolean
        StataType::Byte => DataType::Int8,
        StataType::Int => DataType::Int16,
        StataType::Long => DataType::Int32,
        StataType::Float => DataType::Float32,
        StataType::Double => DataType::Float64,
        
        // Date/Time types
        StataType::Date => DataType::Date,
        StataType::Time => DataType::Time,
        StataType::DateTime => DataType::Datetime(TimeUnit::Milliseconds, None),
        StataType::String | StataType::Strl => DataType::String,
        StataType::Binary => DataType::Binary,
    }
}



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnInfo {
    pub index: usize,
    pub name: String,
    pub dtype: String,
    pub stata_type: String,
}




#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StataColumnInfo {
    pub name: String,
    pub dtype: String,
    pub format: String,
    pub str_length: usize,
    #[serde(default)]
    pub stata_col: usize,  // 1-based position in the Stata dataset; 0 = unset (use enumerate index)
}

/// Resolves a Stata dtype string (as staged by pq.ado: "String"/"StrL"/
/// "Binary"/"Byte"/"Int"/"Long"/"Float"/"Double", case-insensitive) plus its
/// display format into the exact `StataType`, including the date/time/
/// datetime sub-classification carried by the format rather than the dtype.
/// Shared by save-side schema building and Stata metadata recording so the
/// two can never resolve the same column differently.
pub fn resolve_stata_type(dtype: &str, format: &str) -> StataType {
    match (dtype.to_lowercase().as_ref(), format) {
        ("string",_) => StataType::String,
        ("strl",_) => StataType::Strl,
        ("binary",_) => StataType::Binary,
        ("byte",_) => StataType::Byte,
        ("int",_) => match_var_format_stata(format).unwrap_or(StataType::Int),
        ("long",_) => match_var_format_stata(format).unwrap_or(StataType::Long),
        ("float",_) => match_var_format_stata(format).unwrap_or(StataType::Float),
        ("double",_) => match_var_format_stata(format).unwrap_or(StataType::Double),
        (_,_) => panic!("Unknown Stata type: {}", dtype),
    }
}

pub fn stata_column_info_to_schema(
    column_info: &Vec<StataColumnInfo>
) -> Schema {
    let fields: Vec<Field> = column_info.iter().map(|col| {
        let stata_type = resolve_stata_type(&col.dtype, &col.format);

        // Map StataType to Polars DataType
        let polars_dtype = map_stata_to_polars(&stata_type);

        // Create a Field with the column name and data type
        Field::new(PlSmallStr::from(&col.name), polars_dtype)
    }).collect();

    Schema::from_iter(fields)
}


pub fn find_str_length_by_name(columns: &Vec<StataColumnInfo>, target_name: &str) -> Option<usize> {
    columns.iter()
        .find(|col| col.name == target_name)
        .map(|col| col.str_length)
}

fn match_var_format_stata(format_str: &str) -> Option<StataType> {
    // Convert to lowercase for case-insensitive matching
    let format_lower = format_str.to_lowercase();
    
    // 1. Check for TIME formats first (most specific)
    if format_lower.contains("hh:mm:ss") || format_lower.contains("hh:mm") {
        return Some(StataType::Time);
    }
    
    // 2. Check for DATETIME formats
    if format_lower.starts_with("%tc") || format_lower.starts_with("%c") ||
       format_lower.starts_with("%tn") || format_lower.starts_with("%n") ||
       format_lower.starts_with("%tu") || format_lower.starts_with("%u") {
        return Some(StataType::DateTime);
    }
    
    // 3. Check for DATE formats
    if format_lower.starts_with("%td") || format_lower.starts_with("%d") ||
       format_lower.starts_with("%tw") || format_lower.starts_with("%tm") || 
       format_lower.starts_with("%tq") || format_lower.starts_with("%th") || 
       format_lower.starts_with("%ty") || format_lower.starts_with("%tb") ||
       format_lower == "%tdd_m_y" || format_lower == "%tdccyy-nn-dd" ||
       format_lower == "%d"{
        return Some(StataType::Date);
    }
    
    // No match found
    None
}
// Function to print schema with type mappings
pub fn schema_with_stata_types(
    df:&LazyFrame,
    schema: &Schema,
    quietly:bool,
    detailed:bool,
    precomputed_string_lengths: Option<&HashMap<PlSmallStr, usize>>,
    type_overrides: Option<&HashMap<String, StataType>>,
) {

    if !quietly {
        display(&String::from("Variable Name                    | Polars Type                      | Stata Type"));
        display(&String::from("-------------------------------- | -------------------------------- | -------------------- "));
    }

    let hash_strings = if detailed {
            // Use precomputed lengths when available to avoid a second scan.
            precomputed_string_lengths
                .cloned()
                .unwrap_or_else(|| get_string_column_lengths(&df, &schema).unwrap())
        } else {
            HashMap::<PlSmallStr, usize>::new()
        };

    //  display(&format!("hash_strings = {:?}", hash_strings));

    let rename_map = generate_rename_map(&schema);
    let mut all_columns:Vec<ColumnInfo> = Vec::with_capacity(schema.len());
    for (i,(name, dtype)) in schema.iter().enumerate() {
        let char_length = hash_strings.get(name).unwrap_or(&0);
        // A verified-safe override (footer-stats-checked, optionally widened
        // by recorded metadata - see parquet_stats/describe) replaces the
        // conservative default mapping for that column only. Absent for any
        // column whose safety couldn't be verified cheaply, which just
        // falls through to the same upcast-by-default behavior as always.
        let stata_type = type_overrides
            .and_then(|m| m.get(name.as_str()))
            .copied()
            .unwrap_or_else(|| map_polars_to_stata(dtype, *char_length));

        let column_info = ColumnInfo {
            index: i,
            name: name.to_string(),
            dtype: format!("{:?}", dtype),
            stata_type: stata_type.to_string().to_owned(),
        };


        all_columns.push(column_info);
        if !quietly {
            let msg = format!("{:<32} | {:<32} | {}", 
                                    name, 
                                    format!("{:?}", dtype), 
                                    stata_type.to_string());
            display(&msg);
        }

        //  Variable information macros
        //      Name
        let _ = set_macro(
            &format!("name_{}",i+1),
            &name,
            false
        );

        //      Stata type
        let _ = set_macro(
            &format!("type_{}",i+1),
            &stata_type.to_string(),
            false
        );

        
        //      Polars type
        let _ = set_macro(
            &format!("polars_type_{}",i+1),
            &format!("{:?}", dtype),
            false
        );

        //      String length (if applicable)
        let _ = set_macro(
            &format!("string_length_{}",i+1),
            &(format!("{}",char_length)),
            false
        );

        //      Rename, if needed
        let _ = set_macro(
            &format!("rename_{}",i+1),
            rename_map.get(&name.to_string()).unwrap_or(&"".to_string()),
            false
        );
        

        //      Variable name->type lookup
        let _ = set_macro(
            &name,
            &(format!(
                "{}|{}",
                stata_type.to_string(),
                char_length)),
            false
        );
    }

    //      Variable name->type lookup
    let _ = set_macro(
        &"n_vars",
        &(format!(
            "{}",
            schema.len())),
        false
    );
}



fn generate_rename_map(schema: &Schema) -> HashMap<String, String> {
    let mut rename_map: HashMap<String, String> = HashMap::new();
    
    let reserved_words: HashSet<&str> = [
        "aggregate",
        "array",
        "boolean",
        "break",
        "byte",
        "case",
        "catch",
        "class",
        "colvector",
        "complex",
        "const",
        "continue",
        "default",
        "delegate",
        "delete",
        "do",
        "double",
        "else",
        "eltypedef",
        "end",
        "enum",
        "explicit",
        "export",
        "external",
        "float",
        "for",
        "friend",
        "function",
        "global",
        "goto",
        "if",
        "inline",
        "int",
        "local",
        "long",
        "NULL",
        "pragma",
        "protected",
        "quad",
        "rowvector",
        "short",
        "typedef",
        "typename",
        "virtual",
        "_all",
        "_N",
        "_skip",
        "_b",
        "_pi",
        "str#",
        "in",
        "_pred",
        "strL",
        "_coef",
        "_rc",
        "using",
        "_cons",
        "_se",
        "with",
        "_n",
    ].iter().cloned().collect();

    // First pass: Process each name to create sanitized and shortened versions
    let mut processed_names: HashMap<String, String> = HashMap::new();
    
    for (pl_name, _) in schema.iter() {
        let original_name = pl_name.to_string();
        
        // Replace invalid characters with underscores
        let sanitized_name = original_name.chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();
        
        // Ensure name starts with a letter or underscore
        let mut new_name = if !sanitized_name.is_empty() && 
                             !sanitized_name.chars().next().unwrap().is_alphabetic() && 
                             sanitized_name.chars().next().unwrap() != '_' {
            format!("_{}", sanitized_name)
        } else {
            sanitized_name
        };
        
        // Check if name is reserved
        if reserved_words.contains(new_name.as_str()) {
            new_name = format!("_{}", new_name);
        }
        
        // Truncate if needed
        if new_name.len() > 32 {
            new_name = new_name[0..32].to_string();
        }
        
        processed_names.insert(original_name, new_name);
    }
    
    // Second pass: Find collisions
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    
    for new_name in processed_names.values() {
        *name_counts.entry(new_name.clone()).or_insert(0) += 1;
    }
    
    // Third pass: Resolve collisions and build final rename map. Iterates
    // in schema (file column) order rather than draining the processed_names
    // HashMap directly - HashMap iteration order is randomized per-process
    // in Rust, so which colliding column got _001 vs _002 was not stable
    // across separate loads/saves of the same file.
    let mut used_final_names: HashSet<String> = HashSet::new();

    for (pl_name, _) in schema.iter() {
        let original_name = pl_name.to_string();
        let processed_name = processed_names.get(&original_name).unwrap().clone();
        let name_count = *name_counts.get(&processed_name).unwrap();
        
        // If the original name doesn't need modification, skip it
        if original_name == processed_name {
            continue;
        }
        
        // If there's only one variable with this processed name, use it as-is
        if name_count == 1 {
            rename_map.insert(original_name, processed_name.clone());
            used_final_names.insert(processed_name.clone());
            continue;
        }
        
        // For collisions, we need to add numeric suffixes
        let base_name = if processed_name.len() > 28 {
            processed_name[0..28].to_string()
        } else {
            processed_name
        };
        
        // Find a unique suffix
        let mut counter = 1;
        let mut final_name;
        
        loop {
            final_name = format!("{}_{:03}", base_name, counter);
            if !used_final_names.contains(&final_name) {
                break;
            }
            counter += 1;
        }
        
        used_final_names.insert(final_name.clone());
        rename_map.insert(original_name, final_name);
    }
    
    rename_map
}

fn get_string_column_lengths(
    df:&LazyFrame,
    schema: &Schema
) -> PolarsResult<HashMap<PlSmallStr,usize>> {
    // Find all string columns
    let string_columns: Vec<PlSmallStr> = schema
        .iter()
        .filter_map(|(name, dtype)| {
            if matches!(dtype, DataType::String) 
                | matches!(dtype,DataType::Categorical(_,_))
                | matches!(dtype,DataType::Enum(_,_)) {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();
    
    // If there are no string columns, return an empty HashMap
    if string_columns.is_empty() {
        return Ok(HashMap::<PlSmallStr, usize>::new());
    }

    // Create expressions to get max length for each string column
    let exprs: Vec<Expr> = string_columns
        .iter()
        .map(|col_name| {
            col(col_name.as_str()).str().len_bytes().max().alias(col_name.as_str())
        })
        .collect();


    // Execute the query and get the result DataFrame
    let result_df = df.clone().select(exprs).collect()?;

    
    //  display(&format!("df: {:?}", result_df));
    
    // Convert the DataFrame to a HashMap
    let row = result_df.get_row(0)?;

    let mut result_map = HashMap::new();
    for (i, col_name) in string_columns.iter().enumerate() {
        // Get the value and convert it to usize
        if let Some(value) = row.0.get(i) {
            if let AnyValue::UInt32(len) = value {
                result_map.insert(col_name.clone(), *len as usize);
            } else if let AnyValue::UInt64(len) = value {
                result_map.insert(col_name.clone(), *len as usize);
            } else if let AnyValue::Int32(len) = value {
                result_map.insert(col_name.clone(), *len as usize);
            } else if let AnyValue::Int64(len) = value {
                result_map.insert(col_name.clone(), *len as usize);
            }
            // Add other numeric types if needed
        }
    }
    
    Ok(result_map)
}
